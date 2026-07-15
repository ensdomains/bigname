use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

use super::{
    ScopedDiscoveryChronology,
    bulk::{
        deactivate_reconciled_discovery_edge, insert_reconciled_discovery_edges,
        reconcile_historical_discovery_edges,
    },
    bump_discovery_admission_epochs,
    cascade::cascade_deactivation_terminal_states,
    edge_starts_after_terminal,
    existing::{
        load_active_reconciled_discovery_edge_chains, load_active_reconciled_discovery_edge_count,
        load_active_reconciled_discovery_edges,
    },
    fence_discovery_admission_epoch_writes, lock_discovery_reconciliation,
    observation_terminal_states, resolve_reconciled_discovery_edge_specs,
    safe_deactivation_terminal,
};
use crate::{
    DiscoveryObservation, DiscoveryReconciliationSummary,
    discovery::{
        loading::load_discovery_admission_state_with_excluded_source as load_admission_state,
        provenance::observation_key,
        types::{ExistingReconciledDiscoveryEdge, ObservationTerminalState},
    },
    reconcile_active_contract_instance_addresses,
};

pub async fn reconcile_discovery_observations(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    reconcile_discovery_observations_with_boundary(pool, discovery_source, observations, None, None)
        .await
}

/// Full-source reconciliation fenced to one chain's expected discovery
/// admission epoch. The comparison happens after the writer lock is acquired
/// and before any absence-based mutation.
pub async fn reconcile_discovery_observations_with_expected_admission_epoch(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
    chain: &str,
    expected_admission_epoch: i64,
) -> Result<DiscoveryReconciliationSummary> {
    reconcile_discovery_observations_with_boundary(
        pool,
        discovery_source,
        observations,
        None,
        Some((chain, expected_admission_epoch)),
    )
    .await
}

/// Reconcile the complete retained observation history through an inclusive
/// block boundary. Active edges which start later than the boundary are
/// preserved unless their start block is explicitly orphaned.
pub async fn reconcile_discovery_observations_through_block(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
    through_block_number: i64,
) -> Result<DiscoveryReconciliationSummary> {
    reconcile_discovery_observations_with_boundary(
        pool,
        discovery_source,
        observations,
        Some(through_block_number),
        None,
    )
    .await
}

/// Boundary-limited full-source reconciliation fenced to one chain's expected
/// discovery admission epoch.
pub async fn reconcile_discovery_observations_through_block_with_expected_admission_epoch(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
    through_block_number: i64,
    chain: &str,
    expected_admission_epoch: i64,
) -> Result<DiscoveryReconciliationSummary> {
    reconcile_discovery_observations_with_boundary(
        pool,
        discovery_source,
        observations,
        Some(through_block_number),
        Some((chain, expected_admission_epoch)),
    )
    .await
}

async fn reconcile_discovery_observations_with_boundary(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
    through_block_number: Option<i64>,
    expected_admission_epoch: Option<(&str, i64)>,
) -> Result<DiscoveryReconciliationSummary> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start discovery-edge reconciliation transaction")?;
    lock_discovery_reconciliation(transaction.as_mut(), discovery_source).await?;
    let mut candidate_chains = observations
        .iter()
        .map(|observation| observation.chain.clone())
        .collect::<BTreeSet<_>>();
    candidate_chains.extend(
        load_active_reconciled_discovery_edge_chains(transaction.as_mut(), discovery_source)
            .await?,
    );
    if let Some((chain, _)) = expected_admission_epoch {
        candidate_chains.insert(chain.to_owned());
    }
    fence_discovery_admission_epoch_writes(transaction.as_mut(), &candidate_chains).await?;
    if let Some((chain, expected_epoch)) = expected_admission_epoch {
        ensure!(
            candidate_chains.iter().all(|candidate| candidate == chain),
            "discovery source {discovery_source} expected epoch fence for {chain} cannot reconcile observations from another chain"
        );
        let current_epoch = sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_optional(transaction.as_mut())
        .await
        .with_context(|| {
            format!("failed to read discovery admission epoch for {chain} under the writer fence")
        })?
        .unwrap_or(0);
        ensure!(
            current_epoch == expected_epoch,
            "discovery admission epoch changed before full-source reconciliation for {chain}: expected {expected_epoch}, observed {current_epoch}"
        );
    }

    let admission_state =
        load_admission_state(transaction.as_mut(), Some(discovery_source)).await?;
    let direct_terminal_states_by_key = observation_terminal_states(observations)?;
    let observations_by_key = observations
        .iter()
        .map(|observation| Ok((observation_key(observation)?, observation)))
        .collect::<Result<HashMap<_, _>>>()?;

    let (desired_edges, admitted_edges) = resolve_reconciled_discovery_edge_specs(
        &admission_state,
        transaction.as_mut(),
        observations,
    )
    .await?;
    let existing_edges =
        load_active_reconciled_discovery_edges(transaction.as_mut(), discovery_source).await?;

    let desired_set = desired_edges.iter().cloned().collect::<HashSet<_>>();
    let chronology = ScopedDiscoveryChronology::classify(
        &desired_edges,
        &existing_edges,
        &direct_terminal_states_by_key,
    );
    let deactivation_terminal_states_by_key = cascade_deactivation_terminal_states(
        &existing_edges,
        &desired_set,
        &observations_by_key,
        &direct_terminal_states_by_key,
    )?;

    let mut deactivated_edge_count = 0;
    let mut mutated_chains = BTreeSet::new();
    for existing_edge in &existing_edges {
        if desired_set.contains(&existing_edge.spec)
            || chronology.retains_newer_edge(existing_edge.discovery_edge_id)
        {
            continue;
        }
        let terminal_state =
            deactivation_terminal_states_by_key.get(&existing_edge.spec.observation_key);
        if protects_non_orphaned_newer_edge(existing_edge, terminal_state, through_block_number) {
            continue;
        }
        let terminal_state = terminal_state
            .cloned()
            .map(|terminal_state| safe_deactivation_terminal(existing_edge, terminal_state));
        let deactivated = deactivate_reconciled_discovery_edge(
            transaction.as_mut(),
            existing_edge.discovery_edge_id,
            terminal_state.as_ref(),
        )
        .await?;
        if deactivated {
            mutated_chains.insert(existing_edge.spec.chain.clone());
            deactivated_edge_count += 1;
        }
    }

    let new_edges = &chronology.current_new_edges;
    let historical_edges = &chronology.historical_edges;
    let edge_insert = insert_reconciled_discovery_edges(transaction.as_mut(), new_edges).await?;
    let historical_edge_reconciliation =
        reconcile_historical_discovery_edges(transaction.as_mut(), historical_edges).await?;
    let inserted_edge_count = edge_insert.inserted_count
        + edge_insert.reactivated_count
        + historical_edge_reconciliation.inserted_count;
    mutated_chains.extend(new_edges.iter().map(|edge| edge.chain.clone()));
    if historical_edge_reconciliation.inserted_count > 0
        || historical_edge_reconciliation.updated_count > 0
    {
        mutated_chains.extend(historical_edges.iter().map(|(edge, _)| edge.chain.clone()));
    }

    if inserted_edge_count > 0
        || historical_edge_reconciliation.updated_count > 0
        || deactivated_edge_count > 0
    {
        reconcile_active_contract_instance_addresses(transaction.as_mut()).await?;
    }
    let active_edge_count =
        load_active_reconciled_discovery_edge_count(transaction.as_mut(), discovery_source).await?;
    let admission_epoch_bump_count = mutated_chains.len();
    bump_discovery_admission_epochs(transaction.as_mut(), &mutated_chains).await?;

    transaction
        .commit()
        .await
        .context("failed to commit discovery-edge reconciliation transaction")?;

    Ok(DiscoveryReconciliationSummary {
        active_edge_count,
        admitted_edge_count: admitted_edges.len(),
        inserted_edge_count,
        deactivated_edge_count,
        admission_epoch_bump_count,
        admitted_edges,
    })
}

fn protects_non_orphaned_newer_edge(
    edge: &ExistingReconciledDiscoveryEdge,
    terminal_state: Option<&ObservationTerminalState>,
    through_block_number: Option<i64>,
) -> bool {
    if edge.active_from_block_is_orphaned {
        return false;
    }
    let Some(active_from_block_number) = edge.spec.active_from_block_number else {
        return false;
    };
    terminal_state.is_some_and(|terminal| edge_starts_after_terminal(edge, terminal))
        || through_block_number.is_some_and(|through| active_from_block_number > through)
}
