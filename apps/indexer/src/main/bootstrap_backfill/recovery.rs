use anyhow::{Context, Result};
use bigname_manifests::{
    ManifestBootstrapTarget, WatchedSourceSelector, WatchedTargetIdentity,
    load_discovery_admission_epoch, load_ens_v2_authoritative_discovery_bootstrap_targets,
    load_historical_watched_source_selector_plan,
    load_manifest_declared_watched_source_selector_plan, load_required_watched_tuples,
};
use sqlx::Row;

use crate::backfill::{BackfillAdapterSyncMode, BackfillBlockRange};

const MAX_RETENTION_AUTHORITY_RETRIES: usize = 4;
const MAX_ENS_V2_DISCOVERY_EXPANSION_PASSES: usize = 1_024;

#[cfg(test)]
static FORCED_RETENTION_ROTATION_CHAIN: std::sync::OnceLock<std::sync::Mutex<Option<String>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) struct ForcedRetentionRotationGuard {
    chain: String,
}

#[cfg(test)]
impl Drop for ForcedRetentionRotationGuard {
    fn drop(&mut self) {
        let mut forced = FORCED_RETENTION_ROTATION_CHAIN
            .get_or_init(Default::default)
            .lock()
            .expect("forced retention-rotation test lock must not be poisoned");
        if forced.as_deref() == Some(self.chain.as_str()) {
            *forced = None;
        }
    }
}

#[cfg(test)]
pub(crate) fn install_forced_retention_rotation(chain: &str) -> ForcedRetentionRotationGuard {
    let mut forced = FORCED_RETENTION_ROTATION_CHAIN
        .get_or_init(Default::default)
        .lock()
        .expect("forced retention-rotation test lock must not be poisoned");
    assert!(
        forced.is_none(),
        "only one forced rotation may be installed"
    );
    *forced = Some(chain.to_owned());
    ForcedRetentionRotationGuard {
        chain: chain.to_owned(),
    }
}

#[cfg(test)]
async fn maybe_force_retention_rotation(pool: &sqlx::PgPool, chain: &str) -> Result<()> {
    let should_rotate = {
        let mut forced = FORCED_RETENTION_ROTATION_CHAIN
            .get_or_init(Default::default)
            .lock()
            .expect("forced retention-rotation test lock must not be poisoned");
        if forced.as_deref() == Some(chain) {
            *forced = None;
            true
        } else {
            false
        }
    };
    if !should_rotate {
        return Ok(());
    }
    let updated = sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = retention_generation + 1,
            retained_history_complete = false,
            incomplete_since = now(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?
    .rows_affected();
    anyhow::ensure!(
        updated == 1,
        "forced retention rotation found no state for {chain}"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BootstrapRetentionSnapshot {
    pub(crate) generation: i64,
    pub(crate) discovery_admission_epoch: i64,
    pub(crate) has_ens_v2_history_requirements: bool,
    pub(crate) requires_ens_v2_history_recovery: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BootstrapPassStatus {
    Stable,
    DiscoveryExpanded,
    RetentionAuthorityChanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EnsV2DiscoveryProgress {
    generation: i64,
    targets: Vec<ManifestBootstrapTarget>,
}

#[derive(Default)]
pub(super) struct BootstrapConvergenceTracker {
    consecutive_retention_authority_retries: usize,
    discovery_expansion_passes: usize,
    last_discovery_progress: Option<EnsV2DiscoveryProgress>,
}

impl BootstrapConvergenceTracker {
    pub(super) async fn record_retry(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
        through_block: i64,
        status: BootstrapPassStatus,
    ) -> Result<()> {
        match status {
            BootstrapPassStatus::Stable => Ok(()),
            BootstrapPassStatus::RetentionAuthorityChanged => {
                self.consecutive_retention_authority_retries += 1;
                self.last_discovery_progress = None;
                if self.consecutive_retention_authority_retries >= MAX_RETENTION_AUTHORITY_RETRIES {
                    anyhow::bail!(
                        "raw-log retention authority for chain {chain} changed during {} consecutive automatic bootstrap planning passes",
                        self.consecutive_retention_authority_retries
                    );
                }
                Ok(())
            }
            BootstrapPassStatus::DiscoveryExpanded => {
                self.consecutive_retention_authority_retries = 0;
                let snapshot =
                    load_bootstrap_retention_snapshot(pool, chain, through_block).await?;
                let progress = EnsV2DiscoveryProgress {
                    generation: snapshot.generation,
                    targets: load_ens_v2_authoritative_discovery_bootstrap_targets(
                        pool,
                        chain,
                        through_block,
                    )
                    .await?,
                };
                self.record_discovery_progress(chain, progress)
            }
        }
    }

    fn record_discovery_progress(
        &mut self,
        chain: &str,
        progress: EnsV2DiscoveryProgress,
    ) -> Result<()> {
        self.discovery_expansion_passes += 1;
        if self.discovery_expansion_passes > MAX_ENS_V2_DISCOVERY_EXPANSION_PASSES {
            anyhow::bail!(
                "ENSv2 discovery on chain {chain} did not reach a fixed point within {MAX_ENS_V2_DISCOVERY_EXPANSION_PASSES} provider-backed bootstrap passes"
            );
        }
        if self.last_discovery_progress.as_ref() == Some(&progress) {
            anyhow::bail!(
                "ENSv2 discovery bootstrap on chain {chain} requested another pass without changing its retention generation or authoritative known-start target set"
            );
        }
        self.last_discovery_progress = Some(progress);
        Ok(())
    }
}

const ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES: [&str; 2] = ["ens_v2_root_l1", "ens_v2_registry_l1"];

/// Capture the retention generation once for one chain planning pass. An
/// absent row is the ordinary fresh-bootstrap state: generation zero with no
/// historical recovery corpus yet.
pub(crate) async fn load_bootstrap_retention_snapshot(
    pool: &sqlx::PgPool,
    chain: &str,
    through_block: i64,
) -> Result<BootstrapRetentionSnapshot> {
    let source_families = ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES
        .iter()
        .map(|source_family| (*source_family).to_owned())
        .collect::<Vec<_>>();
    let has_ens_v2_history_requirements =
        !load_required_watched_tuples(pool, chain, 0, through_block, &source_families)
            .await?
            .is_empty();
    let discovery_admission_epoch = load_discovery_admission_epoch(pool, chain).await?;
    let row = sqlx::query(
        r#"
        SELECT
            state.retention_generation,
            state.retained_history_complete,
            state.proven_retention_generation,
            state.proven_discovery_admission_epoch,
            state.proven_through_block
        FROM raw_log_staging_input_revisions state
        WHERE state.chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load bootstrap raw-log retention snapshot for chain {chain}")
    })?;

    let Some(row) = row else {
        return Ok(BootstrapRetentionSnapshot {
            generation: 0,
            discovery_admission_epoch,
            has_ens_v2_history_requirements,
            requires_ens_v2_history_recovery: false,
        });
    };
    let generation = row
        .try_get::<i64, _>("retention_generation")
        .context("failed to read bootstrap retention_generation")?;
    let retained_history_complete = row
        .try_get::<bool, _>("retained_history_complete")
        .context("failed to read bootstrap retained_history_complete")?;
    let proven_retention_generation = row
        .try_get::<Option<i64>, _>("proven_retention_generation")
        .context("failed to read bootstrap proven_retention_generation")?;
    let proven_discovery_admission_epoch = row
        .try_get::<Option<i64>, _>("proven_discovery_admission_epoch")
        .context("failed to read bootstrap proven_discovery_admission_epoch")?;
    let proven_through_block = row
        .try_get::<Option<i64>, _>("proven_through_block")
        .context("failed to read bootstrap proven_through_block")?;

    Ok(BootstrapRetentionSnapshot {
        generation,
        discovery_admission_epoch,
        has_ens_v2_history_requirements,
        requires_ens_v2_history_recovery: has_ens_v2_history_requirements
            && (!retained_history_complete
                || proven_retention_generation != Some(generation)
                || proven_discovery_admission_epoch != Some(discovery_admission_epoch)
                || proven_through_block.is_none_or(|proven| proven < through_block)),
    })
}

pub(crate) async fn automatic_backfill_retention_snapshot_is_stable(
    pool: &sqlx::PgPool,
    chain: &str,
    through_block: i64,
    planned: BootstrapRetentionSnapshot,
) -> Result<bool> {
    let current = load_bootstrap_retention_snapshot(pool, chain, through_block).await?;
    Ok(retention_snapshots_are_stable(planned, current))
}

fn retention_snapshots_are_stable(
    planned: BootstrapRetentionSnapshot,
    current: BootstrapRetentionSnapshot,
) -> bool {
    current.generation == planned.generation
        && current.discovery_admission_epoch == planned.discovery_admission_epoch
        && (!current.requires_ens_v2_history_recovery || planned.requires_ens_v2_history_recovery)
}

/// Run the full-source ENSv2 registry reconciliation which turns complete,
/// generation-bound provider coverage into a retained-history proof. A `true`
/// result means reconciliation admitted additional historical authority whose
/// coverage must be planned in another pass.
pub(crate) async fn converge_ens_v2_retained_history_through_block(
    pool: &sqlx::PgPool,
    chain: &str,
    through_block: i64,
    has_ens_v2_history_requirements: bool,
) -> Result<bool> {
    if !has_ens_v2_history_requirements {
        return Ok(false);
    }

    match bigname_adapters::sync_ens_v2_registry_resource_surface_through_block(
        pool,
        chain,
        through_block,
    )
    .await
    {
        Ok(_) => Ok(false),
        Err(error) if bigname_adapters::is_ens_v2_newly_required_coverage(&error) => Ok(true),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to converge ENSv2 full-source authority on chain {chain} through block {through_block}"
            )
        }),
    }
}

pub(super) async fn finish_bootstrap_convergence_pass(
    pool: &sqlx::PgPool,
    chain: &str,
    through_block: i64,
    planned: BootstrapRetentionSnapshot,
    requested_adapter_sync_mode: BackfillAdapterSyncMode,
) -> Result<BootstrapPassStatus> {
    #[cfg(test)]
    maybe_force_retention_rotation(pool, chain).await?;

    let newly_required_coverage = if requested_adapter_sync_mode == BackfillAdapterSyncMode::Auto {
        converge_ens_v2_retained_history_through_block(
            pool,
            chain,
            through_block,
            planned.has_ens_v2_history_requirements,
        )
        .await?
    } else {
        false
    };

    let current = load_bootstrap_retention_snapshot(pool, chain, through_block).await?;
    if !newly_required_coverage && retention_snapshots_are_stable(planned, current) {
        Ok(BootstrapPassStatus::Stable)
    } else if current.generation != planned.generation {
        Ok(BootstrapPassStatus::RetentionAuthorityChanged)
    } else if newly_required_coverage
        || current.discovery_admission_epoch != planned.discovery_admission_epoch
    {
        Ok(BootstrapPassStatus::DiscoveryExpanded)
    } else {
        Ok(BootstrapPassStatus::RetentionAuthorityChanged)
    }
}

pub(super) async fn load_bootstrap_source_plan(
    pool: &sqlx::PgPool,
    chain: &str,
    targets: &[ManifestBootstrapTarget],
    range: BackfillBlockRange,
    include_historical_recovery_targets: bool,
) -> Result<bigname_manifests::WatchedSourceSelectorPlan> {
    let selector = WatchedSourceSelector::WatchedTargetSet(
        targets
            .iter()
            .map(|target| WatchedTargetIdentity {
                contract_instance_id: target.contract_instance_id,
            })
            .collect(),
    );
    if include_historical_recovery_targets {
        load_historical_watched_source_selector_plan(
            pool,
            chain,
            selector,
            range.from_block,
            range.to_block,
        )
        .await
    } else {
        load_manifest_declared_watched_source_selector_plan(
            pool,
            chain,
            selector,
            range.from_block,
            range.to_block,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::Uuid;

    fn progress(target_count: u128) -> EnsV2DiscoveryProgress {
        EnsV2DiscoveryProgress {
            generation: 0,
            targets: (1..=target_count)
                .map(|id| ManifestBootstrapTarget {
                    source_family: "ens_v2_registry_l1".to_owned(),
                    contract_instance_id: Uuid::from_u128(id),
                    address: format!("0x{id:040x}"),
                    effective_from_block: i64::try_from(id).expect("test id fits i64"),
                    effective_to_block: Some(100),
                })
                .collect(),
        }
    }

    #[test]
    fn discovery_progress_accepts_more_than_four_genuine_target_expansions() -> Result<()> {
        let mut tracker = BootstrapConvergenceTracker::default();
        for target_count in 1..=6 {
            tracker.record_discovery_progress("ethereum-sepolia", progress(target_count))?;
        }
        assert_eq!(tracker.discovery_expansion_passes, 6);
        Ok(())
    }

    #[test]
    fn discovery_progress_rejects_epoch_only_retries_with_the_same_targets() -> Result<()> {
        let mut tracker = BootstrapConvergenceTracker::default();
        tracker.record_discovery_progress("ethereum-sepolia", progress(1))?;

        let error = tracker
            .record_discovery_progress("ethereum-sepolia", progress(1))
            .expect_err("an admission-epoch bump alone must not count as target progress");
        assert!(error.to_string().contains("without changing"));
        Ok(())
    }
}
