use super::*;
use sqlx::types::Json;

pub(crate) struct StagingBatchInputFence {
    pub(super) watermark: crate::projection_apply::ProjectionStagingInputWatermark,
}

impl ProjectionStagingCheckpoint {
    pub(crate) async fn prepare_next_batch(&self, pool: &PgPool) -> Result<StagingBatchInputFence> {
        ensure!(
            !self.staging_complete,
            "cannot prepare a batch for completed {} staging",
            self.projection
        );
        let mut transaction = pool.begin().await.with_context(|| {
            format!(
                "failed to open {} staging input-fence transaction",
                self.projection
            )
        })?;
        lock_projection_checkpoint(&mut transaction, self.projection).await?;
        ensure_current_projection_full_replay_input_revision_in_transaction(
            &mut transaction,
            self.full_replay_input_revision,
        )
        .await?;
        let stored = load_checkpoint(&mut transaction, self.projection)
            .await?
            .with_context(|| format!("{} staging checkpoint disappeared", self.projection))?;
        ensure!(
            stored.replay_version == CURRENT_PROJECTION_REPLAY_VERSION
                && stored.staging_schema_version == CURRENT_PROJECTION_STAGING_SCHEMA_VERSION
                && stored.normalized_target_block == self.normalized_target_block
                && stored.full_replay_input_revision == self.full_replay_input_revision
                && stored.validated_normalized_change_id == self.validated_normalized_change_id
                && stored.validated_direct_invalidation_revision
                    == self.validated_direct_invalidation_revision
                && stored.last_source_key == self.last_source_key
                && stored.completed_source_count == self.completed_source_count
                && stored.status == "running",
            "{} staging checkpoint changed before the next source batch",
            self.projection
        );
        let upper =
            crate::projection_apply::capture_projection_staging_input_watermark_in_transaction(
                &mut transaction,
            )
            .await?;
        let completed_sources_changed = if let Some(last_source_key) = self.last_source_key.as_ref()
        {
            crate::projection_apply::completed_projection_sources_changed(
                &mut transaction,
                self.projection,
                self.validated_input_watermark(),
                upper,
                crate::projection_apply::CompletedProjectionSourceRange::Through(last_source_key),
            )
            .await?
        } else {
            false
        };
        if completed_sources_changed {
            drop_stage_tables(&mut transaction, &stored.stage_tables).await?;
            sqlx::query("DELETE FROM current_projection_staging_checkpoints WHERE projection = $1")
                .bind(self.projection)
                .execute(&mut *transaction)
                .await
                .with_context(|| {
                    format!(
                        "failed to discard changed {} staging checkpoint",
                        self.projection
                    )
                })?;
            transaction.commit().await?;
            anyhow::bail!(
                "{} completed staging source range changed after its last durable page; discarded the stage for a correctness-first fresh restage",
                self.projection
            );
        }
        transaction
            .commit()
            .await
            .with_context(|| format!("failed to commit {} staging input fence", self.projection))?;
        Ok(StagingBatchInputFence { watermark: upper })
    }

    /// Returns `true` when the stage became complete and `false` when final-fence drift replaced
    /// it with a fresh running checkpoint.
    pub(crate) async fn mark_staging_complete(
        &mut self,
        pool: &PgPool,
        input_fence: StagingBatchInputFence,
    ) -> Result<bool> {
        if self.staging_complete {
            return Ok(true);
        }
        let mut transaction = pool.begin().await.with_context(|| {
            format!(
                "failed to open {} staging completion transaction",
                self.projection
            )
        })?;
        lock_projection_checkpoint(&mut transaction, self.projection).await?;
        ensure_current_projection_full_replay_input_revision_in_transaction(
            &mut transaction,
            self.full_replay_input_revision,
        )
        .await?;
        let stored = load_checkpoint(&mut transaction, self.projection)
            .await?
            .with_context(|| format!("{} staging checkpoint disappeared", self.projection))?;
        ensure!(
            stored.replay_version == CURRENT_PROJECTION_REPLAY_VERSION
                && stored.staging_schema_version == CURRENT_PROJECTION_STAGING_SCHEMA_VERSION
                && stored.normalized_target_block == self.normalized_target_block
                && stored.full_replay_input_revision == self.full_replay_input_revision
                && stored.validated_normalized_change_id == self.validated_normalized_change_id
                && stored.validated_direct_invalidation_revision
                    == self.validated_direct_invalidation_revision
                && stored.last_source_key == self.last_source_key
                && stored.completed_source_count == self.completed_source_count
                && stored.staged_row_count == self.staged_row_count
                && stored.staged_aux_row_count == self.staged_aux_row_count
                && stored.status == "running",
            "{} staging checkpoint changed before completion",
            self.projection
        );

        // This fence is intentionally captured after the empty source-page query. The capture
        // locks both generation journals through this transaction, so the full-range check and
        // completion update share one finite input boundary.
        let final_watermark =
            crate::projection_apply::capture_projection_staging_input_watermark_in_transaction(
                &mut transaction,
            )
            .await?;
        ensure!(
            final_watermark.normalized_change_id >= input_fence.watermark.normalized_change_id
                && final_watermark.direct_invalidation_revision
                    >= input_fence.watermark.direct_invalidation_revision,
            "{} staging input watermarks moved backwards before completion",
            self.projection
        );
        let completed_sources_changed =
            crate::projection_apply::completed_projection_sources_changed(
                &mut transaction,
                self.projection,
                self.validated_input_watermark(),
                final_watermark,
                crate::projection_apply::CompletedProjectionSourceRange::Full,
            )
            .await?;
        if completed_sources_changed {
            let stale_stage_tables = stored.stage_tables.clone();
            drop_stage_tables(&mut transaction, &stale_stage_tables).await?;
            let deleted = sqlx::query(
                "DELETE FROM current_projection_staging_checkpoints WHERE projection = $1",
            )
            .bind(self.projection)
            .execute(&mut *transaction)
            .await
            .with_context(|| {
                format!(
                    "failed to discard changed {} staging checkpoint at completion",
                    self.projection
                )
            })?
            .rows_affected();
            ensure!(
                deleted == 1,
                "{} staging checkpoint changed while final drift was discarded",
                self.projection
            );
            transaction.commit().await.with_context(|| {
                format!(
                    "failed to commit changed {} staging checkpoint discard",
                    self.projection
                )
            })?;
            let replacement =
                Self::load_or_start(pool, self.projection, self.normalized_target_block).await?;
            info!(
                service = "worker",
                replay = "all_current_projections",
                projection = self.projection,
                stale_stage_tables = ?stale_stage_tables,
                final_normalized_change_id = final_watermark.normalized_change_id,
                final_direct_invalidation_revision =
                    final_watermark.direct_invalidation_revision,
                "all-current projection final staging fence detected drift; fresh restage started"
            );
            *self = replacement;
            return Ok(false);
        }

        let updated = sqlx::query(
            r#"
            UPDATE current_projection_staging_checkpoints
            SET
                status = 'staging_complete',
                staging_completed_at = now(),
                validated_normalized_change_id = $6,
                validated_direct_invalidation_revision = $7,
                updated_at = now()
            WHERE projection = $1
              AND replay_version = $2
              AND staging_schema_version = $3
              AND completed_normalized_target_block IS NOT DISTINCT FROM $4
              AND full_replay_input_revision = $5
              AND validated_normalized_change_id = $8
              AND validated_direct_invalidation_revision = $9
              AND status = 'running'
            "#,
        )
        .bind(self.projection)
        .bind(CURRENT_PROJECTION_REPLAY_VERSION)
        .bind(CURRENT_PROJECTION_STAGING_SCHEMA_VERSION)
        .bind(self.normalized_target_block)
        .bind(self.full_replay_input_revision)
        .bind(final_watermark.normalized_change_id)
        .bind(final_watermark.direct_invalidation_revision)
        .bind(self.validated_normalized_change_id)
        .bind(self.validated_direct_invalidation_revision)
        .execute(&mut *transaction)
        .await
        .with_context(|| {
            format!(
                "failed to mark {} projection staging complete",
                self.projection
            )
        })?
        .rows_affected();
        ensure!(
            updated == 1,
            "{} projection staging checkpoint changed before completion",
            self.projection
        );
        transaction
            .commit()
            .await
            .with_context(|| format!("failed to commit {} staging completion", self.projection))?;
        self.staging_complete = true;
        self.validated_normalized_change_id = final_watermark.normalized_change_id;
        self.validated_direct_invalidation_revision = final_watermark.direct_invalidation_revision;
        Ok(true)
    }

    pub(crate) async fn persist_progress(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        progress: &StagingBatchProgress,
        input_fence: &StagingBatchInputFence,
    ) -> Result<()> {
        ensure_current_projection_full_replay_input_revision_in_transaction(
            transaction,
            self.full_replay_input_revision,
        )
        .await?;
        let updated = sqlx::query(
            r#"
            UPDATE current_projection_staging_checkpoints
            SET
                last_source_key = $5,
                completed_source_count = $6,
                staged_row_count = $7,
                staged_aux_row_count = $8,
                validated_normalized_change_id = $9,
                validated_direct_invalidation_revision = $10,
                updated_at = now()
            WHERE projection = $1
              AND replay_version = $2
              AND staging_schema_version = $3
              AND full_replay_input_revision = $4
              AND completed_source_count = $11
              AND validated_normalized_change_id = $12
              AND validated_direct_invalidation_revision = $13
              AND status = 'running'
            "#,
        )
        .bind(self.projection)
        .bind(CURRENT_PROJECTION_REPLAY_VERSION)
        .bind(CURRENT_PROJECTION_STAGING_SCHEMA_VERSION)
        .bind(self.full_replay_input_revision)
        .bind(Json(&progress.last_source_key))
        .bind(progress.completed_source_count)
        .bind(progress.staged_row_count)
        .bind(progress.staged_aux_row_count)
        .bind(input_fence.watermark.normalized_change_id)
        .bind(input_fence.watermark.direct_invalidation_revision)
        .bind(self.completed_source_count)
        .bind(self.validated_normalized_change_id)
        .bind(self.validated_direct_invalidation_revision)
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to persist {} projection staging progress",
                self.projection
            )
        })?
        .rows_affected();
        ensure!(
            updated == 1,
            "{} projection staging checkpoint changed concurrently",
            self.projection
        );
        Ok(())
    }

    pub(crate) fn accept_progress(
        &mut self,
        progress: StagingBatchProgress,
        input_fence: StagingBatchInputFence,
    ) {
        self.last_source_key = Some(progress.last_source_key);
        self.completed_source_count = progress.completed_source_count;
        self.staged_row_count = progress.staged_row_count;
        self.staged_aux_row_count = progress.staged_aux_row_count;
        self.validated_normalized_change_id = input_fence.watermark.normalized_change_id;
        self.validated_direct_invalidation_revision =
            input_fence.watermark.direct_invalidation_revision;
    }
}
