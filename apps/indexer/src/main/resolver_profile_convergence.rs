use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ResolverProfileInputChange, acknowledge_resolver_profile_input_change,
    load_pending_resolver_profile_input_changes,
};
use tracing::info;

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
}

/// Converge every durable resolver-profile input change that is currently
/// pending. A crash or error before the final generation CAS leaves the input
/// dirty, so the full repair is safe to retry.
pub(crate) async fn drain_resolver_profile_input_changes(
    pool: &sqlx::PgPool,
) -> Result<ResolverProfileConvergenceSummary> {
    let mut aggregate = ResolverProfileConvergenceSummary::default();

    for _ in 0..MAX_DRAIN_BATCHES {
        let pending =
            load_pending_resolver_profile_input_changes(pool, INPUT_CHANGE_BATCH_SIZE).await?;
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
                    "resolver-profile input changes converged"
                );
            }
            return Ok(aggregate);
        }

        aggregate.loaded_input_count += pending.len();
        let authority = capture_resolver_profile_authority(pool).await?;
        let targets_by_chain = expanded_reconciliation_targets(&pending, &authority);
        aggregate.reconciled_target_count +=
            targets_by_chain.values().map(BTreeSet::len).sum::<usize>();
        let invalidation_plan =
            load_resolver_profile_projection_invalidation_plan(pool, &targets_by_chain).await?;

        for (chain, addresses) in &targets_by_chain {
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

        for input in pending {
            if acknowledge_resolver_profile_input_change(
                pool,
                &input.chain_id,
                &input.contract_address,
                input.generation,
            )
            .await?
            {
                aggregate.acknowledged_input_count += 1;
            } else {
                // A concurrent generation remains dirty and is selected by a
                // later pass. Never acknowledge work we did not observe.
                aggregate.concurrent_input_count += 1;
            }
        }
    }

    if load_pending_resolver_profile_input_changes(pool, 1)
        .await?
        .is_empty()
    {
        return Ok(aggregate);
    }
    bail!(
        "resolver-profile convergence exceeded {MAX_DRAIN_BATCHES} batches; pending generations remain durable"
    )
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
