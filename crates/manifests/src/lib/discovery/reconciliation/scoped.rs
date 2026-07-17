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

fn accumulate_transition_summary(
    summary: &mut DiscoveryReconciliationSummary,
    transition_summary: DiscoveryReconciliationSummary,
) {
    summary.active_edge_count = transition_summary.active_edge_count;
    summary.admitted_edge_count += transition_summary.admitted_edge_count;
    summary.inserted_edge_count += transition_summary.inserted_edge_count;
    summary.deactivated_edge_count += transition_summary.deactivated_edge_count;
    summary
        .admitted_edges
        .extend(transition_summary.admitted_edges);
}

pub async fn reconcile_scoped_discovery_observations(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    let transitions = [observations.to_vec()];
    reconcile_scoped_discovery_observation_transitions(pool, discovery_source, &transitions).await
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
        accumulate_transition_summary(&mut summary, transition_summary);
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

#[cfg(test)]
mod tests {
    use sqlx::types::Uuid;

    use super::*;
    use crate::AdmittedDiscoveryEdge;

    fn admitted_edge() -> AdmittedDiscoveryEdge {
        AdmittedDiscoveryEdge {
            source_manifest_id: 1,
            chain: "ethereum-sepolia".to_owned(),
            from_contract_instance_id: Uuid::nil(),
            to_contract_instance_id: Some(Uuid::from_u128(1)),
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: "0x0000000000000000000000000000000000000002".to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "test".to_owned(),
            admission: "reachable_from_root".to_owned(),
            from_role: "registry".to_owned(),
        }
    }

    #[test]
    fn transition_summary_accumulates_admissions_before_empty_final_state() {
        let mut summary = empty_reconciliation_summary();
        let admitted_edge = admitted_edge();
        accumulate_transition_summary(
            &mut summary,
            DiscoveryReconciliationSummary {
                active_edge_count: 1,
                admitted_edge_count: 1,
                inserted_edge_count: 1,
                deactivated_edge_count: 0,
                admission_epoch_bump_count: 0,
                admitted_edges: vec![admitted_edge.clone()],
            },
        );
        accumulate_transition_summary(&mut summary, empty_reconciliation_summary());

        assert_eq!(summary.admitted_edge_count, 1);
        assert_eq!(summary.inserted_edge_count, 1);
        assert_eq!(summary.admitted_edges, vec![admitted_edge]);
    }
}
