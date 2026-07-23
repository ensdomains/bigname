use anyhow::{Context, Result, ensure};
use bigname_manifests::{
    RequiredWatchedTuple, load_required_watched_tuples_in_transaction,
    load_required_watched_tuples_in_transaction_with_progress,
};
use sqlx::PgPool;

use super::{
    FullSourceRawLogHistoryGuard, RawLogClosureProof, ens_v2_closure_source_families,
    ens_v2_discovery_history_source_families, ensure_newly_required_generation_bound_coverage,
    ensure_retained_semantic_witnesses_with_optional_progress, load_locked_retained_history_state,
    requirement_intervals_not_covered_by, requirement_intervals_not_covered_by_with_progress,
};
use crate::ens_v2_registry::live::checkpoint::{
    StagedLiveRegistryReplayCheckpoint, finalize_live_registry_replay_checkpoint,
};
use crate::{
    checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress},
    startup_progress::StartupManifestProgress,
};

impl FullSourceRawLogHistoryGuard {
    /// Advance the proof after a successful full-source reconciliation or a
    /// complete live-path extension, publish an optional matching durable
    /// snapshot, then release the raw-log table fence.
    pub(in crate::ens_v2_registry) async fn finish(
        self,
        pool: &PgPool,
        proof: RawLogClosureProof,
        through_block: i64,
        own_discovery_epoch_bumps: usize,
        pre_sync_requirements: &[RequiredWatchedTuple],
        staged_checkpoint: Option<&StagedLiveRegistryReplayCheckpoint>,
    ) -> Result<()> {
        self.finish_inner(
            pool,
            proof,
            through_block,
            own_discovery_epoch_bumps,
            pre_sync_requirements,
            staged_checkpoint,
            None,
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    pub(in crate::ens_v2_registry) async fn finish_with_progress(
        self,
        pool: &PgPool,
        proof: RawLogClosureProof,
        through_block: i64,
        own_discovery_epoch_bumps: usize,
        pre_sync_requirements: &[RequiredWatchedTuple],
        staged_checkpoint: Option<&StagedLiveRegistryReplayCheckpoint>,
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<()> {
        self.finish_inner(
            pool,
            proof,
            through_block,
            own_discovery_epoch_bumps,
            pre_sync_requirements,
            staged_checkpoint,
            Some(progress),
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    async fn finish_inner(
        self,
        pool: &PgPool,
        proof: RawLogClosureProof,
        through_block: i64,
        own_discovery_epoch_bumps: usize,
        pre_sync_requirements: &[RequiredWatchedTuple],
        staged_checkpoint: Option<&StagedLiveRegistryReplayCheckpoint>,
        mut progress: Option<&mut dyn StartupAdapterProgress>,
    ) -> Result<()> {
        let expected_bumps = i64::try_from(own_discovery_epoch_bumps)
            .context("ENSv2 discovery admission-epoch bump count exceeds i64")?;
        let expected_epoch = proof
            .discovery_admission_epoch
            .checked_add(expected_bumps)
            .context("ENSv2 discovery admission epoch overflow")?;
        let mut finish_transaction = pool
            .begin()
            .await
            .context("failed to begin ENSv2 retained-history proof advance")?;
        let current_epoch = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT epoch
            FROM discovery_admission_epochs
            WHERE chain_id = $1
            FOR SHARE
            "#,
        )
        .bind(&self.chain)
        .fetch_one(finish_transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to lock ENSv2 discovery-admission epoch while finishing {}",
                self.chain
            )
        })?;
        ensure!(
            current_epoch == expected_epoch,
            "ENSv2 discovery admission changed during full-source reconciliation on {}: expected epoch {expected_epoch}, observed {current_epoch}",
            self.chain
        );
        let state =
            load_locked_retained_history_state(finish_transaction.as_mut(), &self.chain).await?;
        ensure!(
            state.retention_generation == proof.retention_generation,
            "ENSv2 raw-log retention generation changed during reconciliation on {}",
            self.chain
        );
        ensure!(
            state.retained_history_complete
                && state.proven_retention_generation == Some(proof.retention_generation),
            "ENSv2 retained-history proof was invalidated during reconciliation on {}",
            self.chain
        );

        let discovery_families = ens_v2_discovery_history_source_families();
        let post_sync_discovery_history_requirements =
            if let Some(progress) = progress.as_deref_mut() {
                let mut manifest_progress = StartupManifestProgress::new(progress);
                load_required_watched_tuples_in_transaction_with_progress(
                    finish_transaction.as_mut(),
                    pool,
                    &self.chain,
                    0,
                    through_block,
                    &discovery_families,
                    &mut manifest_progress,
                )
                .await?
            } else {
                load_required_watched_tuples_in_transaction(
                    finish_transaction.as_mut(),
                    &self.chain,
                    0,
                    through_block,
                    &discovery_families,
                )
                .await?
            };
        let newly_required_intervals = if let Some(progress) = progress.as_deref_mut() {
            requirement_intervals_not_covered_by_with_progress(
                pool,
                &post_sync_discovery_history_requirements,
                pre_sync_requirements,
                progress,
            )
            .await?
        } else {
            requirement_intervals_not_covered_by(
                &post_sync_discovery_history_requirements,
                pre_sync_requirements,
            )
        };
        if progress.is_some() {
            for page in newly_required_intervals.chunks(super::RETAINED_REQUIREMENT_PROGRESS_ROWS) {
                ensure_newly_required_generation_bound_coverage(
                    finish_transaction.as_mut(),
                    &self.chain,
                    page,
                    proof.retention_generation,
                )
                .await?;
                record_startup_adapter_progress(pool, &mut progress).await?;
            }
        } else {
            ensure_newly_required_generation_bound_coverage(
                finish_transaction.as_mut(),
                &self.chain,
                &newly_required_intervals,
                proof.retention_generation,
            )
            .await?;
        }
        let closure_families = ens_v2_closure_source_families();
        let post_sync_closure_requirements = if let Some(progress) = progress.as_deref_mut() {
            let mut manifest_progress = StartupManifestProgress::new(progress);
            load_required_watched_tuples_in_transaction_with_progress(
                finish_transaction.as_mut(),
                pool,
                &self.chain,
                0,
                through_block,
                &closure_families,
                &mut manifest_progress,
            )
            .await?
        } else {
            load_required_watched_tuples_in_transaction(
                finish_transaction.as_mut(),
                &self.chain,
                0,
                through_block,
                &closure_families,
            )
            .await?
        };
        ensure_retained_semantic_witnesses_with_optional_progress(
            pool,
            finish_transaction.as_mut(),
            &self.chain,
            &post_sync_closure_requirements,
            through_block,
            &mut progress,
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE raw_log_staging_input_revisions
            SET proven_discovery_admission_epoch = $2,
                proven_through_block = GREATEST(proven_through_block, $3)
            WHERE chain_id = $1
              AND retention_generation = $4
              AND retained_history_complete = true
            "#,
        )
        .bind(&self.chain)
        .bind(current_epoch)
        .bind(through_block)
        .bind(proof.retention_generation)
        .execute(finish_transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to advance ENSv2 retained-history proof for {}",
                self.chain
            )
        })?;
        if let Some(checkpoint) = staged_checkpoint {
            finalize_live_registry_replay_checkpoint(finish_transaction.as_mut(), checkpoint)
                .await?;
        }
        finish_transaction
            .commit()
            .await
            .context("failed to commit ENSv2 retained-history proof advance")?;
        self.transaction
            .commit()
            .await
            .context("failed to release ENSv2 raw-log read fence")?;
        record_startup_adapter_progress(pool, &mut progress).await?;
        Ok(())
    }
}
