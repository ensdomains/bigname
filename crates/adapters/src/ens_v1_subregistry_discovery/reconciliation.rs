use anyhow::Result;
use bigname_manifests::{
    DiscoveryObservation, DiscoveryReconciliationSummary, reconcile_discovery_observations,
    reconcile_discovery_observations_through_block,
    reconcile_discovery_observations_through_block_with_expected_admission_epoch,
};
use sqlx::PgPool;

use super::{
    EnsV1SubregistryDiscoverySyncSummary,
    checkpoint::{RECONCILIATION_PAGE_LIMIT, SubregistryReplayCheckpoint},
};

pub(super) async fn reconcile_subregistry_discovery_from_checkpoint(
    pool: &PgPool,
    checkpoint: &SubregistryReplayCheckpoint,
    discovery_sources: &[String],
    reconciliation: &mut EnsV1SubregistryDiscoverySyncSummary,
) -> Result<()> {
    for discovery_source in discovery_sources {
        let mut source_observations = Vec::new();
        let mut after_key = None::<String>;
        loop {
            let page = checkpoint
                .load_assignment_page(
                    pool,
                    discovery_source,
                    after_key.as_deref(),
                    RECONCILIATION_PAGE_LIMIT,
                )
                .await?;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            after_key = Some(last_key.clone());
            source_observations.extend(
                page.iter()
                    .map(|(_, assignment)| assignment.discovery_observation())
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        let source_reconciliation =
            reconcile_discovery_observations(pool, discovery_source, &source_observations).await?;
        reconciliation.active_edge_count += source_reconciliation.active_edge_count;
        reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
        reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
        reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
    }
    Ok(())
}

pub(super) async fn reconcile_subregistry_discovery_source_through_block(
    pool: &PgPool,
    chain: &str,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
    through_block: i64,
    expected_admission_epoch: Option<i64>,
) -> Result<DiscoveryReconciliationSummary> {
    if let Some(expected_epoch) = expected_admission_epoch {
        reconcile_discovery_observations_through_block_with_expected_admission_epoch(
            pool,
            discovery_source,
            observations,
            through_block,
            chain,
            expected_epoch,
        )
        .await
    } else {
        reconcile_discovery_observations_through_block(
            pool,
            discovery_source,
            observations,
            through_block,
        )
        .await
    }
}
