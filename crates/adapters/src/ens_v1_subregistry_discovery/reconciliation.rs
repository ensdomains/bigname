use std::{collections::BTreeMap, ops::Bound};

use anyhow::{Context, Result};
use bigname_manifests::{
    DiscoveryObservation, DiscoveryObservationPageSource, ExpectedDiscoveryAdmissionEpoch,
    FullDiscoveryReconciliationOptions, reconcile_discovery_observations_streamed,
    reconcile_discovery_observations_streamed_with_full_options,
};
use sqlx::PgPool;

use super::hex_topic::{ZERO_ADDRESS, normalize_address};
use super::{
    EnsV1SubregistryDiscoverySyncSummary,
    assignment::ObservedRegistryAssignment,
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

    async fn record_progress(&self) -> Result<()> {
        Ok(())
    }
}

struct AssignmentMapPageSource<'a> {
    assignments: &'a BTreeMap<String, ObservedRegistryAssignment>,
    discovery_source: &'a str,
}

impl DiscoveryObservationPageSource for AssignmentMapPageSource<'_> {
    async fn load_page(
        &self,
        after_key: Option<&str>,
        limit: i64,
    ) -> Result<Vec<(String, DiscoveryObservation)>> {
        let prefix = format!("{}:", self.discovery_source);
        let start = match after_key {
            Some(after_key) => Bound::Excluded(format!("{prefix}{after_key}")),
            None => Bound::Included(prefix.clone()),
        };
        self.assignments
            .range((start, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(&prefix))
            .take(usize::try_from(limit.clamp(1, RECONCILIATION_PAGE_LIMIT))?)
            .map(|(_, assignment)| {
                Ok((
                    assignment.observation_key.clone(),
                    assignment.discovery_observation()?,
                ))
            })
            .collect()
    }

    async fn record_progress(&self) -> Result<()> {
        Ok(())
    }
}

pub(super) fn count_active_assignments(
    assignments: &BTreeMap<String, ObservedRegistryAssignment>,
) -> usize {
    assignments
        .values()
        .filter(|assignment| normalize_address(&assignment.to_address) != ZERO_ADDRESS)
        .count()
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

pub(super) async fn reconcile_subregistry_discovery_from_assignments_through_block(
    pool: &PgPool,
    chain: &str,
    assignments: &BTreeMap<String, ObservedRegistryAssignment>,
    discovery_sources: &[String],
    through_block: i64,
    mut expected_admission_epoch: Option<i64>,
    reconciliation: &mut EnsV1SubregistryDiscoverySyncSummary,
) -> Result<()> {
    for discovery_source in discovery_sources {
        let page_source = AssignmentMapPageSource {
            assignments,
            discovery_source,
        };
        let source_reconciliation = reconcile_discovery_observations_streamed_with_full_options(
            pool,
            discovery_source,
            &page_source,
            FullDiscoveryReconciliationOptions {
                through_block_number: Some(through_block),
                expected_admission_epoch: expected_admission_epoch
                    .map(|epoch| ExpectedDiscoveryAdmissionEpoch { chain, epoch }),
            },
        )
        .await?;
        reconciliation.active_edge_count += source_reconciliation.active_edge_count;
        reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
        reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
        reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
        if let Some(expected_epoch) = expected_admission_epoch.as_mut() {
            *expected_epoch = expected_epoch
                .checked_add(i64::try_from(
                    source_reconciliation.admission_epoch_bump_count,
                )?)
                .context("legacy registry reconciliation admission epoch overflowed")?;
        }
    }
    Ok(())
}
