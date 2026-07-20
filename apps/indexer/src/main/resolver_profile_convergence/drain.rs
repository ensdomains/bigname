use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use bigname_storage::{
    RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE, ResolverProfileInputChange,
    ResolverProfileReconciliationTarget, acknowledge_resolver_profile_input_changes,
    load_pending_resolver_profile_input_changes_excluding, load_raw_log_staging_input_version,
    load_resolver_profile_authority_entries_for_targets,
    load_resolver_profile_authority_family_target_page,
};
use tracing::{info, warn};

use super::authority::ResolverProfileAuthorityEntry;
use super::{
    ResolverProfileAuthorityIndex, ResolverProfileConvergenceSummary,
    input_requires_reconciliation,
    invalidations::{
        enqueue_resolver_profile_projection_invalidations,
        load_resolver_profile_projection_invalidation_plan,
    },
    reconciliation_scope,
};

// Preserve the previous durable input budget while making every authority and
// reconciliation query inside that budget independently bounded.
const MAX_DRAIN_INPUTS: usize = 128 * 1_024;
const RECONCILIATION_TARGET_PAGE_SIZE: usize = 250;
const MIN_CONVERGENCE_POOL_CONNECTIONS: u32 = 3;

/// Converge every currently eligible durable resolver-profile input change.
/// A crash or error before the final generation CAS leaves the input dirty, so
/// every page is safe to retry. Exact generations whose retained corpus cannot
/// authorize absence replay remain durably pending.
pub(crate) async fn drain_resolver_profile_input_changes(
    pool: &sqlx::PgPool,
) -> Result<ResolverProfileConvergenceSummary> {
    let mut aggregate = ResolverProfileConvergenceSummary::default();
    let mut deferred_inputs = Vec::<ResolverProfileInputChange>::new();
    let mut deferred_chains = BTreeMap::<String, i64>::new();

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
            log_completed_drain(&aggregate);
            return Ok(aggregate);
        }
        ensure!(
            pool.options().get_max_connections() >= MIN_CONVERGENCE_POOL_CONNECTIONS,
            "resolver-profile convergence requires at least {MIN_CONVERGENCE_POOL_CONNECTIONS} \
             database connections (runtime writer guard, reconciliation guard, and bounded \
             authority/event reads), but the pool allows only {}",
            pool.options().get_max_connections()
        );

        aggregate.loaded_input_count += pending.len();
        let authority_index = load_scoped_authority_index(pool, &pending, &mut aggregate).await?;
        let scope = reconciliation_scope(&pending, &authority_index);
        classify_deferred_chains(pool, &scope, &mut deferred_chains).await?;
        aggregate.deferred_chains = deferred_chains.keys().cloned().collect();

        let eligible_direct_targets = scope
            .direct_targets_by_chain
            .into_iter()
            .filter(|(chain, _)| !deferred_chains.contains_key(chain))
            .collect::<BTreeMap<_, _>>();
        reconcile_direct_target_pages(pool, &eligible_direct_targets, &mut aggregate).await?;

        let eligible_seed_families = scope
            .seed_families
            .into_iter()
            .filter(|(chain, _)| !deferred_chains.contains_key(chain))
            .collect::<Vec<_>>();
        reconcile_seed_family_target_pages(
            pool,
            &eligible_seed_families,
            &eligible_direct_targets,
            &mut aggregate,
        )
        .await?;

        acknowledge_inputs(
            pool,
            pending,
            &authority_index,
            &deferred_chains,
            &mut deferred_inputs,
            &mut aggregate,
        )
        .await?;
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

async fn load_scoped_authority_index(
    pool: &sqlx::PgPool,
    pending: &[ResolverProfileInputChange],
    aggregate: &mut ResolverProfileConvergenceSummary,
) -> Result<ResolverProfileAuthorityIndex> {
    let targets = pending
        .iter()
        .map(|input| ResolverProfileReconciliationTarget {
            chain_id: input.chain_id.clone(),
            contract_address: input.contract_address.clone(),
        })
        .collect::<Vec<_>>();
    let mut authority_index = ResolverProfileAuthorityIndex::default();
    for target_page in targets.chunks(RESOLVER_PROFILE_AUTHORITY_JOURNAL_ENTRY_BATCH_SIZE) {
        aggregate.authority_target_read_statement_count += 1;
        aggregate.max_authority_target_read_batch_size = aggregate
            .max_authority_target_read_batch_size
            .max(target_page.len());
        for entry in load_resolver_profile_authority_entries_for_targets(pool, target_page).await? {
            authority_index.insert(
                serde_json::from_value::<ResolverProfileAuthorityEntry>(entry.entry_payload)
                    .context("failed to decode scoped resolver-profile authority entry")?,
            );
        }
    }
    Ok(authority_index)
}

async fn classify_deferred_chains(
    pool: &sqlx::PgPool,
    scope: &super::ResolverProfileReconciliationScope,
    deferred_chains: &mut BTreeMap<String, i64>,
) -> Result<()> {
    let chains = scope
        .direct_targets_by_chain
        .keys()
        .cloned()
        .chain(scope.seed_families.iter().map(|(chain, _)| chain.clone()))
        .collect::<BTreeSet<_>>();
    for chain in chains {
        if deferred_chains.contains_key(&chain) {
            continue;
        }
        let retention_generation = load_raw_log_staging_input_version(pool, &chain)
            .await?
            .retention_generation;
        if retention_generation == 0 {
            continue;
        }
        deferred_chains.insert(chain.clone(), retention_generation);
        warn!(
            service = "indexer",
            command = "resolver-profile-convergence",
            chain,
            retention_generation,
            "resolver-profile reconciliation is deferred because this retained raw-log corpus has no resolver absence-replay authority; pending generations remain unacknowledged and require a full generation-zero database rebootstrap"
        );
    }
    Ok(())
}

async fn reconcile_direct_target_pages(
    pool: &sqlx::PgPool,
    targets_by_chain: &BTreeMap<String, BTreeSet<String>>,
    aggregate: &mut ResolverProfileConvergenceSummary,
) -> Result<()> {
    for (chain, addresses) in targets_by_chain {
        let addresses = addresses.iter().cloned().collect::<Vec<_>>();
        for page in addresses.chunks(RECONCILIATION_TARGET_PAGE_SIZE) {
            reconcile_target_page(
                pool,
                &BTreeMap::from([(chain.clone(), page.iter().cloned().collect())]),
                aggregate,
            )
            .await?;
        }
    }
    Ok(())
}

async fn reconcile_seed_family_target_pages(
    pool: &sqlx::PgPool,
    seed_families: &[(String, String)],
    direct_targets_by_chain: &BTreeMap<String, BTreeSet<String>>,
    aggregate: &mut ResolverProfileConvergenceSummary,
) -> Result<()> {
    if seed_families.is_empty() {
        return Ok(());
    }
    let mut after = None::<ResolverProfileReconciliationTarget>;
    loop {
        let page = load_resolver_profile_authority_family_target_page(
            pool,
            seed_families,
            after.as_ref(),
            RECONCILIATION_TARGET_PAGE_SIZE,
        )
        .await?;
        aggregate.family_target_read_statement_count += 1;
        aggregate.max_family_target_page_size =
            aggregate.max_family_target_page_size.max(page.len());
        let Some(last) = page.last() else {
            return Ok(());
        };
        after = Some(last.clone());
        let mut targets_by_chain = BTreeMap::<String, BTreeSet<String>>::new();
        for target in page {
            if direct_targets_by_chain
                .get(&target.chain_id)
                .is_some_and(|addresses| addresses.contains(&target.contract_address))
            {
                continue;
            }
            targets_by_chain
                .entry(target.chain_id)
                .or_default()
                .insert(target.contract_address);
        }
        reconcile_target_page(pool, &targets_by_chain, aggregate).await?;
    }
}

async fn reconcile_target_page(
    pool: &sqlx::PgPool,
    targets_by_chain: &BTreeMap<String, BTreeSet<String>>,
    aggregate: &mut ResolverProfileConvergenceSummary,
) -> Result<()> {
    if targets_by_chain.is_empty() {
        return Ok(());
    }
    let invalidation_plan =
        load_resolver_profile_projection_invalidation_plan(pool, targets_by_chain).await?;
    for (chain, addresses) in targets_by_chain {
        let addresses = addresses.iter().cloned().collect::<Vec<_>>();
        let summary = bigname_adapters::reconcile_resolver_profile_events(pool, chain, &addresses)
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
    aggregate.reconciled_target_count +=
        targets_by_chain.values().map(BTreeSet::len).sum::<usize>();
    aggregate.invalidated_projection_key_count +=
        enqueue_resolver_profile_projection_invalidations(pool, &invalidation_plan).await?;
    Ok(())
}

async fn acknowledge_inputs(
    pool: &sqlx::PgPool,
    pending: Vec<ResolverProfileInputChange>,
    authority_index: &ResolverProfileAuthorityIndex,
    deferred_chains: &BTreeMap<String, i64>,
    deferred_inputs: &mut Vec<ResolverProfileInputChange>,
    aggregate: &mut ResolverProfileConvergenceSummary,
) -> Result<()> {
    let mut candidates = Vec::new();
    for input in pending {
        if input_requires_reconciliation(&input, authority_index)
            && deferred_chains.contains_key(&input.chain_id)
        {
            aggregate.deferred_input_count += 1;
            deferred_inputs.push(input);
        } else {
            candidates.push(input);
        }
    }
    let acknowledged = acknowledge_resolver_profile_input_changes(pool, &candidates).await?;
    aggregate.acknowledged_input_count += acknowledged;
    aggregate.concurrent_input_count += candidates.len() - acknowledged;
    Ok(())
}

fn log_completed_drain(aggregate: &ResolverProfileConvergenceSummary) {
    if aggregate.loaded_input_count == 0 {
        return;
    }
    info!(
        service = "indexer",
        command = "resolver-profile-convergence",
        loaded_input_count = aggregate.loaded_input_count,
        authority_target_read_statement_count = aggregate.authority_target_read_statement_count,
        max_authority_target_read_batch_size = aggregate.max_authority_target_read_batch_size,
        family_target_read_statement_count = aggregate.family_target_read_statement_count,
        max_family_target_page_size = aggregate.max_family_target_page_size,
        reconciled_target_count = aggregate.reconciled_target_count,
        invalidated_projection_key_count = aggregate.invalidated_projection_key_count,
        acknowledged_input_count = aggregate.acknowledged_input_count,
        concurrent_input_count = aggregate.concurrent_input_count,
        deferred_input_count = aggregate.deferred_input_count,
        deferred_chain_count = aggregate.deferred_chains.len(),
        "resolver-profile input-change drain completed"
    );
}
