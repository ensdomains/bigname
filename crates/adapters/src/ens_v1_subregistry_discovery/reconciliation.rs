use std::{collections::BTreeMap, ops::Bound};

use anyhow::{Context, Result};
use bigname_manifests::{
    DiscoveryObservation, DiscoveryObservationPageSource, ExpectedDiscoveryAdmissionEpoch,
    FullDiscoveryReconciliationOptions, reconcile_discovery_observations_streamed,
    reconcile_discovery_observations_streamed_with_full_options,
};
use sqlx::PgPool;

use super::{
    EnsV1SubregistryDiscoverySyncSummary,
    assignment::ObservedRegistryAssignment,
    checkpoint::{RECONCILIATION_PAGE_LIMIT, SubregistryReplayCheckpoint},
};
use crate::checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress};

use super::hex_topic::{ZERO_ADDRESS, normalize_address};

/// Pages one discovery source's staged latest-per-key assignments straight
/// from the checkpoint items, so the finalize reconcile never materializes a
/// source's observations in memory (#168).
struct CheckpointAssignmentPageSource<'a, 'progress> {
    pool: &'a PgPool,
    checkpoint: &'a SubregistryReplayCheckpoint,
    discovery_source: &'a str,
    startup_progress: &'a tokio::sync::Mutex<Option<&'progress mut dyn StartupAdapterProgress>>,
}

impl DiscoveryObservationPageSource for CheckpointAssignmentPageSource<'_, '_> {
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
        let mut startup_progress = self.startup_progress.lock().await;
        record_startup_adapter_progress(self.pool, &mut startup_progress).await
    }
}

struct AssignmentMapPageSource<'a, 'progress> {
    pool: &'a PgPool,
    assignments: &'a BTreeMap<String, ObservedRegistryAssignment>,
    discovery_source: &'a str,
    startup_progress: &'a tokio::sync::Mutex<Option<&'progress mut dyn StartupAdapterProgress>>,
}

impl DiscoveryObservationPageSource for AssignmentMapPageSource<'_, '_> {
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
        let mut startup_progress = self.startup_progress.lock().await;
        record_startup_adapter_progress(self.pool, &mut startup_progress).await
    }
}

pub(super) async fn count_active_assignments_with_progress(
    pool: &PgPool,
    assignments: &BTreeMap<String, ObservedRegistryAssignment>,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<usize> {
    let mut active_count = 0usize;
    for (index, assignment) in assignments.values().enumerate() {
        if normalize_address(&assignment.to_address) != ZERO_ADDRESS {
            active_count += 1;
        }
        if (index + 1).is_multiple_of(super::checkpoint::PAGE_LIMIT as usize) {
            record_startup_adapter_progress(pool, startup_progress).await?;
        }
    }
    Ok(active_count)
}

pub(super) async fn reconcile_subregistry_discovery_from_checkpoint(
    pool: &PgPool,
    checkpoint: &SubregistryReplayCheckpoint,
    discovery_sources: &[String],
    reconciliation: &mut EnsV1SubregistryDiscoverySyncSummary,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let progress = tokio::sync::Mutex::new(startup_progress.take());
    let result = async {
        for discovery_source in discovery_sources {
            let page_source = CheckpointAssignmentPageSource {
                pool,
                checkpoint,
                discovery_source,
                startup_progress: &progress,
            };
            let source_reconciliation =
                reconcile_discovery_observations_streamed(pool, discovery_source, &page_source)
                    .await?;
            reconciliation.active_edge_count += source_reconciliation.active_edge_count;
            reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
            reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
            reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
        }
        Ok(())
    }
    .await;
    *startup_progress = progress.into_inner();
    result
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn reconcile_subregistry_discovery_from_assignments_through_block(
    pool: &PgPool,
    chain: &str,
    assignments: &BTreeMap<String, ObservedRegistryAssignment>,
    discovery_sources: &[String],
    through_block: i64,
    mut expected_admission_epoch: Option<i64>,
    reconciliation: &mut EnsV1SubregistryDiscoverySyncSummary,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let progress = tokio::sync::Mutex::new(startup_progress.take());
    let result = async {
        for discovery_source in discovery_sources {
            let page_source = AssignmentMapPageSource {
                pool,
                assignments,
                discovery_source,
                startup_progress: &progress,
            };
            let source_reconciliation =
                reconcile_discovery_observations_streamed_with_full_options(
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
    .await;
    *startup_progress = progress.into_inner();
    result
}
