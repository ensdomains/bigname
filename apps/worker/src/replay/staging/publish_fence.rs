use super::*;

impl ProjectionStagingCheckpoint {
    /// Open a replacement transaction only while the completed stage still covers every input
    /// change. `None` means drift was found and `self` now refers to a fresh running checkpoint.
    pub(crate) async fn begin_fenced_publish_transaction(
        &mut self,
        pool: &PgPool,
    ) -> Result<Option<Transaction<'static, Postgres>>> {
        ensure!(
            self.staging_complete,
            "cannot publish incomplete {} staging",
            self.projection
        );
        let mut transaction = pool.begin().await.with_context(|| {
            format!(
                "failed to open {} staging publication transaction",
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
                && stored.stage_tables == self.stage_tables
                && stored.last_source_key == self.last_source_key
                && stored.completed_source_count == self.completed_source_count
                && stored.staged_row_count == self.staged_row_count
                && stored.staged_aux_row_count == self.staged_aux_row_count
                && stored.status == "staging_complete",
            "{} staging checkpoint changed before publication",
            self.projection
        );

        // The capture functions take SHARE locks on both generation journals. Retaining those
        // locks in the replacement transaction prevents an in-flight invalidation writer from
        // landing between this full-range check and the live-table commit.
        let publish_watermark =
            crate::projection_apply::capture_projection_staging_input_watermark_in_transaction(
                &mut transaction,
            )
            .await?;
        let completed_sources_changed =
            crate::projection_apply::completed_projection_sources_changed(
                &mut transaction,
                self.projection,
                self.validated_input_watermark(),
                publish_watermark,
                crate::projection_apply::CompletedProjectionSourceRange::Full,
            )
            .await?;
        if !completed_sources_changed {
            return Ok(Some(transaction));
        }

        let stale_stage_tables = stored.stage_tables.clone();
        drop_stage_tables(&mut transaction, &stale_stage_tables).await?;
        let deleted =
            sqlx::query("DELETE FROM current_projection_staging_checkpoints WHERE projection = $1")
                .bind(self.projection)
                .execute(&mut *transaction)
                .await
                .with_context(|| {
                    format!(
                        "failed to discard changed {} staging checkpoint before publication",
                        self.projection
                    )
                })?
                .rows_affected();
        ensure!(
            deleted == 1,
            "{} staging checkpoint changed while publication drift was discarded",
            self.projection
        );
        transaction.commit().await.with_context(|| {
            format!(
                "failed to commit changed {} staging checkpoint discard before publication",
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
            publish_normalized_change_id = publish_watermark.normalized_change_id,
            publish_direct_invalidation_revision =
                publish_watermark.direct_invalidation_revision,
            "all-current projection publication fence detected drift; fresh restage started"
        );
        *self = replacement;
        Ok(None)
    }
}
