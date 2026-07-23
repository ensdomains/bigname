use super::*;
use sqlx::types::Json;

pub(crate) struct StagingBatchInputFence {
    pub(super) validated_normalized_change_id: i64,
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
                && stored.last_source_key == self.last_source_key
                && stored.completed_source_count == self.completed_source_count
                && stored.status == "running",
            "{} staging checkpoint changed before the next source batch",
            self.projection
        );
        let upper =
            crate::projection_apply::capture_normalized_event_change_watermark_in_transaction(
                &mut transaction,
            )
            .await?
            .change_id;
        let completed_sources_changed = if let Some(last_source_key) = self.last_source_key.as_ref()
        {
            crate::projection_apply::completed_projection_sources_changed(
                &mut transaction,
                self.projection,
                self.validated_normalized_change_id,
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
        Ok(StagingBatchInputFence {
            validated_normalized_change_id: upper,
        })
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
                updated_at = now()
            WHERE projection = $1
              AND replay_version = $2
              AND staging_schema_version = $3
              AND full_replay_input_revision = $4
              AND completed_source_count = $10
              AND validated_normalized_change_id = $11
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
        .bind(input_fence.validated_normalized_change_id)
        .bind(self.completed_source_count)
        .bind(self.validated_normalized_change_id)
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
        self.validated_normalized_change_id = input_fence.validated_normalized_change_id;
    }
}
