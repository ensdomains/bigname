use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ResolverProfileInputChange, acknowledge_resolver_profile_input_changes,
    load_pending_resolver_profile_input_changes_excluding, load_raw_log_staging_input_version,
};
use tracing::{info, warn};

#[path = "resolver_profile_convergence/authority.rs"]
mod authority;
#[path = "resolver_profile_convergence/invalidations.rs"]
mod invalidations;

use authority::ResolverProfileAuthorityEntry;
pub(crate) use authority::{
    ResolverProfileAuthoritySnapshot, capture_resolver_profile_authority,
    journal_resolver_profile_authority, journal_resolver_profile_authority_if_epoch_changed,
};
use invalidations::{
    enqueue_resolver_profile_projection_invalidations,
    load_resolver_profile_projection_invalidation_plan,
};

// Preserve the previous bounded-drain budget while loading one aggregate pass.
// Resolver-profile reconciliation replays chain-global context, so splitting
// these inputs into 128-row pages repeats the same authority and event scans.
const MAX_DRAIN_INPUTS: usize = 128 * 1_024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResolverProfileConvergenceSummary {
    pub(crate) loaded_input_count: usize,
    pub(crate) reconciled_target_count: usize,
    pub(crate) invalidated_projection_key_count: u64,
    pub(crate) acknowledged_input_count: usize,
    pub(crate) concurrent_input_count: usize,
    pub(crate) deferred_input_count: usize,
    pub(crate) deferred_chains: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct ResolverProfileAuthorityIndex {
    entries_by_chain_and_address:
        BTreeMap<String, BTreeMap<String, Vec<ResolverProfileAuthorityEntry>>>,
    addresses_by_chain_and_source_family: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    #[cfg(test)]
    indexed_entry_count: usize,
}

impl ResolverProfileAuthorityIndex {
    fn from_snapshot(authority: ResolverProfileAuthoritySnapshot) -> Self {
        let mut index = Self::default();
        for entry in authority.entries {
            index
                .addresses_by_chain_and_source_family
                .entry(entry.chain.clone())
                .or_default()
                .entry(entry.source_family.clone())
                .or_default()
                .insert(entry.address.clone());
            index
                .entries_by_chain_and_address
                .entry(entry.chain.clone())
                .or_default()
                .entry(entry.address.clone())
                .or_default()
                .push(entry);
            #[cfg(test)]
            {
                index.indexed_entry_count += 1;
            }
        }
        index
    }

    fn entries_for(&self, chain: &str, address: &str) -> Option<&[ResolverProfileAuthorityEntry]> {
        self.entries_by_chain_and_address
            .get(chain)?
            .get(address)
            .map(Vec::as_slice)
    }

    fn addresses_for_family(&self, chain: &str, source_family: &str) -> Option<&BTreeSet<String>> {
        self.addresses_by_chain_and_source_family
            .get(chain)?
            .get(source_family)
    }
}

impl ResolverProfileConvergenceSummary {
    pub(crate) fn ensure_chain_completion_allowed(
        &self,
        chain: &str,
        completion: &str,
    ) -> Result<()> {
        if self.deferred_chains.contains(chain) {
            bail!(
                "resolver-profile reconciliation on chain {chain} remains deferred without resolver absence-replay authority; refusing {completion} until the database is fully rebuilt as a generation-zero raw-log corpus"
            );
        }
        Ok(())
    }
}

/// Converge every currently eligible durable resolver-profile input change.
/// A crash or error before the final generation CAS leaves the input dirty, so
/// the full repair is safe to retry. Exact generations whose retained corpus
/// cannot authorize absence replay are skipped only for this bounded drain and
/// remain durably pending.
pub(crate) async fn drain_resolver_profile_input_changes(
    pool: &sqlx::PgPool,
) -> Result<ResolverProfileConvergenceSummary> {
    let mut aggregate = ResolverProfileConvergenceSummary::default();
    let mut deferred_inputs = Vec::<ResolverProfileInputChange>::new();
    let mut deferred_chains = BTreeMap::<String, i64>::new();
    let mut authority_index = None;

    while aggregate.loaded_input_count < MAX_DRAIN_INPUTS {
        let remaining_input_count = MAX_DRAIN_INPUTS - aggregate.loaded_input_count;
        let pending = load_pending_resolver_profile_input_changes_excluding(
            pool,
            i64::try_from(remaining_input_count)
                .context("resolver-profile drain input budget does not fit i64")?,
            &deferred_inputs,
        )
        .await?;
        if pending.is_empty() {
            if aggregate.loaded_input_count > 0 {
                info!(
                    service = "indexer",
                    command = "resolver-profile-convergence",
                    loaded_input_count = aggregate.loaded_input_count,
                    reconciled_target_count = aggregate.reconciled_target_count,
                    invalidated_projection_key_count = aggregate.invalidated_projection_key_count,
                    acknowledged_input_count = aggregate.acknowledged_input_count,
                    concurrent_input_count = aggregate.concurrent_input_count,
                    deferred_input_count = aggregate.deferred_input_count,
                    deferred_chain_count = aggregate.deferred_chains.len(),
                    "resolver-profile input-change drain completed"
                );
            }
            return Ok(aggregate);
        }

        aggregate.loaded_input_count += pending.len();
        // One authority snapshot is sufficient for the bounded drain. A
        // concurrent authority mutation is itself journaled as forced target
        // work, and the adapter reloads current admission while reconciling.
        if authority_index.is_none() {
            authority_index = Some(ResolverProfileAuthorityIndex::from_snapshot(
                capture_resolver_profile_authority(pool).await?,
            ));
        }
        let authority_index = authority_index
            .as_ref()
            .context("resolver-profile authority was not captured for pending work")?;
        let targets_by_chain = expanded_reconciliation_targets(&pending, authority_index);

        let mut eligible_targets_by_chain = BTreeMap::new();
        for (chain, addresses) in &targets_by_chain {
            let retention_generation = if let Some(generation) = deferred_chains.get(chain) {
                *generation
            } else {
                load_raw_log_staging_input_version(pool, chain)
                    .await?
                    .retention_generation
            };
            if retention_generation == 0 {
                eligible_targets_by_chain.insert(chain.clone(), addresses.clone());
                continue;
            }
            if deferred_chains
                .insert(chain.clone(), retention_generation)
                .is_none()
            {
                warn!(
                    service = "indexer",
                    command = "resolver-profile-convergence",
                    chain,
                    retention_generation,
                    "resolver-profile reconciliation is deferred because this retained raw-log corpus has no resolver absence-replay authority; pending generations remain unacknowledged and require a full generation-zero database rebootstrap"
                );
            }
        }
        aggregate.deferred_chains = deferred_chains.keys().cloned().collect();
        aggregate.reconciled_target_count += eligible_targets_by_chain
            .values()
            .map(BTreeSet::len)
            .sum::<usize>();

        let invalidation_plan =
            load_resolver_profile_projection_invalidation_plan(pool, &eligible_targets_by_chain)
                .await?;
        for (chain, addresses) in &eligible_targets_by_chain {
            let addresses = addresses.iter().cloned().collect::<Vec<_>>();
            let summary =
                bigname_adapters::reconcile_resolver_profile_events(pool, chain, &addresses)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to reconcile resolver-profile events for {} targets on {chain}",
                            addresses.len()
                        )
                    })?;
            info!(
                service = "indexer",
                command = "resolver-profile-convergence",
                chain,
                resolver_address_count = summary.resolver_address_count,
                retained_block_hash_count = summary.block_hash_count,
                scanned_log_count = summary.scanned_log_count,
                matched_log_count = summary.matched_log_count,
                normalized_event_count = summary.normalized_event_count,
                normalized_event_inserted_count = summary.normalized_event_inserted_count,
                orphaned_normalized_event_count = summary.orphaned_normalized_event_count,
                "resolver-profile event reconciliation completed"
            );
        }
        aggregate.invalidated_projection_key_count +=
            enqueue_resolver_profile_projection_invalidations(pool, &invalidation_plan).await?;

        let mut acknowledgement_candidates = Vec::new();
        for input in pending {
            if input_requires_reconciliation(&input, authority_index)
                && deferred_chains.contains_key(&input.chain_id)
            {
                aggregate.deferred_input_count += 1;
                deferred_inputs.push(input);
            } else {
                acknowledgement_candidates.push(input);
            }
        }
        let acknowledged =
            acknowledge_resolver_profile_input_changes(pool, &acknowledgement_candidates).await?;
        aggregate.acknowledged_input_count += acknowledged;
        // A concurrent generation remains dirty and is selected by a later
        // pass. Never acknowledge work we did not observe.
        aggregate.concurrent_input_count += acknowledgement_candidates.len() - acknowledged;
    }

    if load_pending_resolver_profile_input_changes_excluding(pool, 1, &deferred_inputs)
        .await?
        .is_empty()
    {
        return Ok(aggregate);
    }
    bail!(
        "resolver-profile convergence exceeded its bounded {MAX_DRAIN_INPUTS}-input budget; pending generations remain durable"
    )
}

fn input_requires_reconciliation(
    input: &ResolverProfileInputChange,
    authority: &ResolverProfileAuthorityIndex,
) -> bool {
    input.force_reconciliation
        || authority
            .entries_for(&input.chain_id, &input.contract_address)
            .is_some()
}

fn expanded_reconciliation_targets(
    pending: &[ResolverProfileInputChange],
    authority: &ResolverProfileAuthorityIndex,
) -> BTreeMap<String, BTreeSet<String>> {
    expanded_reconciliation_targets_with_family_count(pending, authority).0
}

fn expanded_reconciliation_targets_with_family_count(
    pending: &[ResolverProfileInputChange],
    authority: &ResolverProfileAuthorityIndex,
) -> (BTreeMap<String, BTreeSet<String>>, usize) {
    let mut targets = BTreeMap::<String, BTreeSet<String>>::new();
    let mut seed_families = BTreeSet::<(String, String)>::new();

    for input in pending {
        let current_entries = authority.entries_for(&input.chain_id, &input.contract_address);
        // Raw-code triggers observe every watched contract. Ordinary changes
        // outside resolver-profile authority are acknowledged as irrelevant;
        // an explicit authority kick retains removed targets for cleanup.
        if current_entries.is_none() && !input.force_reconciliation {
            continue;
        }
        targets
            .entry(input.chain_id.clone())
            .or_default()
            .insert(input.contract_address.clone());

        if let Some(current_entries) = current_entries {
            for seed in current_entries.iter().filter(|entry| entry.is_seed) {
                seed_families.insert((seed.chain.clone(), seed.source_family.clone()));
            }
        }
    }

    let expanded_seed_family_count = seed_families.len();
    for (chain, source_family) in seed_families {
        let Some(addresses) = authority.addresses_for_family(&chain, &source_family) else {
            continue;
        };
        targets
            .entry(chain)
            .or_default()
            .extend(addresses.iter().cloned());
    }

    (targets, expanded_seed_family_count)
}

#[cfg(test)]
#[path = "resolver_profile_convergence/tests.rs"]
mod tests;
