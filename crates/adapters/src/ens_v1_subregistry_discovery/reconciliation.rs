use anyhow::Result;
use bigname_manifests::{
    DiscoveryObservation, DiscoveryObservationPageSource, DiscoveryReconciliationSummary,
    ExpectedDiscoveryAdmissionEpoch, FullDiscoveryReconciliationOptions,
    reconcile_discovery_observations, reconcile_discovery_observations_streamed,
};
use sqlx::PgPool;

use super::{
    EnsV1SubregistryDiscoverySyncSummary,
    checkpoint::{RECONCILIATION_PAGE_LIMIT, SubregistryReplayCheckpoint},
};

/// Pages one discovery source's staged latest-per-key assignments straight
/// from the checkpoint items, so the finalize reconcile never materializes a
/// source's observations in memory (#168).
struct CheckpointAssignmentPageSource<'a> {
    pool: &'a PgPool,
    checkpoint: &'a SubregistryReplayCheckpoint,
    discovery_source: &'a str,
}

impl DiscoveryObservationPageSource for CheckpointAssignmentPageSource<'_> {
    async fn load_page(
        &self,
        after_key: Option<&str>,
        limit: i64,
    ) -> Result<Vec<(String, DiscoveryObservation)>> {
        let limit = limit.min(RECONCILIATION_PAGE_LIMIT);
        self.checkpoint
            .load_assignment_page(self.pool, self.discovery_source, after_key, limit)
            .await?
            .into_iter()
            .map(|(item_key, assignment)| Ok((item_key, assignment.discovery_observation()?)))
            .collect()
    }
}

pub(super) async fn reconcile_subregistry_discovery_from_checkpoint(
    pool: &PgPool,
    checkpoint: &SubregistryReplayCheckpoint,
    discovery_sources: &[String],
    reconciliation: &mut EnsV1SubregistryDiscoverySyncSummary,
) -> Result<()> {
    for discovery_source in discovery_sources {
        let page_source = CheckpointAssignmentPageSource {
            pool,
            checkpoint,
            discovery_source,
        };
        let source_reconciliation =
            reconcile_discovery_observations_streamed(pool, discovery_source, &page_source).await?;
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
    reconcile_discovery_observations(
        pool,
        discovery_source,
        observations,
        FullDiscoveryReconciliationOptions {
            through_block_number: Some(through_block),
            expected_admission_epoch: expected_admission_epoch
                .map(|epoch| ExpectedDiscoveryAdmissionEpoch { chain, epoch }),
        },
    )
    .await
}
