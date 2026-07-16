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

pub(crate) use authority::{
    ResolverProfileAuthoritySnapshot, capture_resolver_profile_authority,
    journal_resolver_profile_authority, journal_resolver_profile_authority_if_epoch_changed,
};
use invalidations::{
    enqueue_resolver_profile_projection_invalidations,
    load_resolver_profile_projection_invalidation_plan,
};

const INPUT_CHANGE_BATCH_SIZE: i64 = 128;
const MAX_DRAIN_BATCHES: usize = 1_024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResolverProfileConvergenceSummary {
    pub(crate) loaded_input_count: usize,
    pub(crate) reconciled_target_count: usize,
    pub(crate) invalidated_projection_key_count: u64,
    pub(crate) acknowledged_input_count: usize,
    pub(crate) concurrent_input_count: usize,
    pub(crate) deferred_input_count: usize,
    pub(crate) deferred_chain_count: usize,
    pub(crate) deferred_chains: BTreeSet<String>,
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

    for _ in 0..MAX_DRAIN_BATCHES {
        let pending = load_pending_resolver_profile_input_changes_excluding(
            pool,
            INPUT_CHANGE_BATCH_SIZE,
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
                    deferred_chain_count = aggregate.deferred_chain_count,
                    "resolver-profile input-change drain completed"
                );
            }
            return Ok(aggregate);
        }

        aggregate.loaded_input_count += pending.len();
        let authority = capture_resolver_profile_authority(pool).await?;
        let targets_by_chain = expanded_reconciliation_targets(&pending, &authority);

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
        aggregate.deferred_chain_count = deferred_chains.len();
        aggregate.deferred_chains = deferred_chains.keys().cloned().collect();
        aggregate.reconciled_target_count += eligible_targets_by_chain
            .values()
            .map(BTreeSet::len)
            .sum::<usize>();

        for (chain, addresses) in &eligible_targets_by_chain {
            let chain_targets = BTreeMap::from([(chain.clone(), addresses.clone())]);
            let invalidation_plan =
                load_resolver_profile_projection_invalidation_plan(pool, &chain_targets).await?;
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
            aggregate.invalidated_projection_key_count +=
                enqueue_resolver_profile_projection_invalidations(pool, &invalidation_plan).await?;
        }

        let mut acknowledgement_candidates = Vec::new();
        for input in pending {
            if input_requires_reconciliation(&input, &authority)
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
        "resolver-profile convergence exceeded {MAX_DRAIN_BATCHES} batches; pending generations remain durable"
    )
}

fn input_requires_reconciliation(
    input: &ResolverProfileInputChange,
    authority: &ResolverProfileAuthoritySnapshot,
) -> bool {
    input.force_reconciliation
        || authority
            .entries
            .iter()
            .any(|entry| entry.chain == input.chain_id && entry.address == input.contract_address)
}

fn expanded_reconciliation_targets(
    pending: &[ResolverProfileInputChange],
    authority: &ResolverProfileAuthoritySnapshot,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut targets = BTreeMap::<String, BTreeSet<String>>::new();

    for input in pending {
        let current_entries = authority
            .entries
            .iter()
            .filter(|entry| {
                entry.chain == input.chain_id && entry.address == input.contract_address
            })
            .collect::<Vec<_>>();
        // Raw-code triggers observe every watched contract. Ordinary changes
        // outside resolver-profile authority are acknowledged as irrelevant;
        // an explicit authority kick retains removed targets for cleanup.
        if current_entries.is_empty() && !input.force_reconciliation {
            continue;
        }
        targets
            .entry(input.chain_id.clone())
            .or_default()
            .insert(input.contract_address.clone());

        for seed in current_entries.into_iter().filter(|entry| entry.is_seed) {
            for candidate in authority.entries.iter().filter(|candidate| {
                candidate.chain == seed.chain && candidate.source_family == seed.source_family
            }) {
                targets
                    .entry(candidate.chain.clone())
                    .or_default()
                    .insert(candidate.address.clone());
            }
        }
    }

    targets
}

#[cfg(test)]
#[path = "resolver_profile_convergence/tests.rs"]
mod tests;
