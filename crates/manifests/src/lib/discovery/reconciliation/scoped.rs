use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::PgPool;

use super::existing::load_active_reconciled_discovery_edge_count;
use super::{
    bump_discovery_admission_epochs, empty_reconciliation_summary,
    fence_discovery_admission_epoch_writes, lock_discovery_reconciliation,
    reconcile_scoped_discovery_observations_in_transaction,
};
use crate::{DiscoveryObservation, DiscoveryReconciliationSummary};

pub async fn reconcile_scoped_discovery_observations(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start scoped discovery-edge reconciliation transaction")?;
    lock_discovery_reconciliation(transaction.as_mut(), discovery_source).await?;
    let candidate_chains = observations
        .iter()
        .map(|observation| observation.chain.clone())
        .collect::<BTreeSet<_>>();
    fence_discovery_admission_epoch_writes(transaction.as_mut(), &candidate_chains).await?;

    let (mut summary, mutated_chains) = reconcile_scoped_discovery_observations_in_transaction(
        transaction.as_mut(),
        discovery_source,
        observations,
    )
    .await?;
    summary.active_edge_count =
        load_active_reconciled_discovery_edge_count(transaction.as_mut(), discovery_source).await?;
    summary.admission_epoch_bump_count = mutated_chains.len();
    bump_discovery_admission_epochs(transaction.as_mut(), &mutated_chains).await?;

    transaction
        .commit()
        .await
        .context("failed to commit scoped discovery-edge reconciliation transaction")?;

    Ok(summary)
}

/// Reconcile an ordered sequence of scoped discovery states while holding one
/// transaction and one source lock. Each state is still applied in order, so
/// historical intervals and recursive admission match one-at-a-time replay,
/// but a long retained history does not pay a transaction/lock round trip for
/// every transition.
pub async fn reconcile_scoped_discovery_observation_transitions(
    pool: &PgPool,
    discovery_source: &str,
    transitions: &[Vec<DiscoveryObservation>],
) -> Result<DiscoveryReconciliationSummary> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start scoped discovery-transition reconciliation transaction")?;
    lock_discovery_reconciliation(transaction.as_mut(), discovery_source).await?;
    let candidate_chains = transitions
        .iter()
        .flatten()
        .map(|observation| observation.chain.clone())
        .collect::<BTreeSet<_>>();
    fence_discovery_admission_epoch_writes(transaction.as_mut(), &candidate_chains).await?;

    let mut summary = empty_reconciliation_summary();
    let mut mutated_chains = BTreeSet::new();
    for observations in transitions {
        let (transition_summary, transition_mutated_chains) =
            reconcile_scoped_discovery_observations_in_transaction(
                transaction.as_mut(),
                discovery_source,
                observations,
            )
            .await?;
        summary.active_edge_count = transition_summary.active_edge_count;
        summary.admitted_edge_count = transition_summary.admitted_edge_count;
        summary.inserted_edge_count += transition_summary.inserted_edge_count;
        summary.deactivated_edge_count += transition_summary.deactivated_edge_count;
        summary.admission_epoch_bump_count += transition_summary.admission_epoch_bump_count;
        summary.admitted_edges = transition_summary.admitted_edges;
        mutated_chains.extend(transition_mutated_chains);
    }
    summary.active_edge_count =
        load_active_reconciled_discovery_edge_count(transaction.as_mut(), discovery_source).await?;
    summary.admission_epoch_bump_count = mutated_chains.len();
    bump_discovery_admission_epochs(transaction.as_mut(), &mutated_chains).await?;

    transaction
        .commit()
        .await
        .context("failed to commit scoped discovery-transition reconciliation transaction")?;

    Ok(summary)
}
