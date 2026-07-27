use super::*;
use crate::checkpoint_context::FULL_CLOSURE_CHECKPOINT_SCOPE;

pub async fn clear_replay_adapter_checkpoints(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .bind(ADAPTER)
    .bind(FULL_CLOSURE_CHECKPOINT_SCOPE)
    .execute(pool)
    .await;
    if is_undefined_table_error(&result) {
        return Ok(());
    }
    result.with_context(|| {
        format!(
            "failed to clear unwrapped-authority replay adapter checkpoints for {deployment_profile}/{chain}/{cursor_kind}"
        )
    })?;
    Ok(())
}

fn is_undefined_table_error<T>(result: &std::result::Result<T, sqlx::Error>) -> bool {
    matches!(
        result,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01")
    )
}

pub(super) async fn load_checkpoint_row(
    pool: &PgPool,
    chain: &str,
    context: &AdapterCheckpointContext,
) -> Result<Option<UnwrappedAuthorityReplayCheckpoint>> {
    let row = sqlx::query(
        r#"
        SELECT
            replay_start_block_number,
            replay_target_block_number,
            last_block_number,
            scanned_log_count,
            matched_log_count,
            status,
            state_payload,
            raw_log_retention_generation,
            raw_log_input_revision,
            adapter_semantic_version,
            schema_migration_count,
            schema_migration_max_version
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&context.deployment_profile)
    .bind(chain)
    .bind(&context.cursor_kind)
    .bind(ADAPTER)
    .bind(context.checkpoint_scope)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load {ADAPTER} replay checkpoint for {}/{}",
            context.deployment_profile, chain
        )
    })?;

    row.map(|row| checkpoint_from_row(chain, context, row))
        .transpose()
}

fn checkpoint_from_row(
    chain: &str,
    context: &AdapterCheckpointContext,
    row: sqlx::postgres::PgRow,
) -> Result<UnwrappedAuthorityReplayCheckpoint> {
    let state_payload: Value = row.try_get("state_payload")?;
    let flushed_events = flushed_events_from_payload(&state_payload)?;
    Ok(UnwrappedAuthorityReplayCheckpoint {
        context: AdapterCheckpointContext {
            deployment_profile: context.deployment_profile.clone(),
            cursor_kind: context.cursor_kind.clone(),
            checkpoint_scope: context.checkpoint_scope,
            range_start_block_number: row.try_get("replay_start_block_number")?,
            target_block_number: row.try_get("replay_target_block_number")?,
            startup_discovery_admission_epoch: context.startup_discovery_admission_epoch,
            startup_adapter_semantic_version: context.startup_adapter_semantic_version,
            startup_schema_migration_state: context.startup_schema_migration_state,
        },
        chain: chain.to_owned(),
        status: row.try_get("status")?,
        last_block_number: row.try_get("last_block_number")?,
        scanned_log_count: usize::try_from(row.try_get::<i64, _>("scanned_log_count")?)
            .context("checkpoint scanned log count overflowed usize")?,
        matched_log_count: usize::try_from(row.try_get::<i64, _>("matched_log_count")?)
            .context("checkpoint matched log count overflowed usize")?,
        state_payload,
        flushed_events,
        raw_log_input_version: RawLogStagingInputVersion {
            retention_generation: row.try_get("raw_log_retention_generation")?,
            revision: row.try_get("raw_log_input_revision")?,
        },
        adapter_semantic_version: row.try_get("adapter_semantic_version")?,
        schema_migration_count: row.try_get("schema_migration_count")?,
        schema_migration_max_version: row.try_get("schema_migration_max_version")?,
    })
}

pub(super) async fn delete_checkpoint(
    pool: &PgPool,
    chain: &str,
    context: &AdapterCheckpointContext,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&context.deployment_profile)
    .bind(chain)
    .bind(&context.cursor_kind)
    .bind(ADAPTER)
    .bind(context.checkpoint_scope)
    .execute(pool)
    .await
    .context("failed to reset stale unwrapped-authority replay checkpoint")?;
    Ok(())
}

impl UnwrappedAuthorityReplayCheckpoint {
    pub(in crate::ens_v1_unwrapped_authority) async fn mark_stream_complete(
        &mut self,
        pool: &PgPool,
        scanned_log_count: usize,
        matched_log_count: usize,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE normalized_replay_adapter_checkpoints
            SET
                status = 'stream_complete',
                scanned_log_count = $6,
                matched_log_count = $7,
                raw_log_retention_generation = $8,
                raw_log_input_revision = $9,
                adapter_semantic_version = $10,
                schema_migration_count = $11,
                schema_migration_max_version = $12,
                updated_at = now(),
                last_failure_reason = NULL
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(self.context.checkpoint_scope)
        .bind(i64::try_from(scanned_log_count).context("scanned log count overflowed i64")?)
        .bind(i64::try_from(matched_log_count).context("matched log count overflowed i64")?)
        .bind(self.raw_log_input_version.retention_generation)
        .bind(self.raw_log_input_version.revision)
        .bind(self.context.startup_adapter_semantic_version())
        .bind(self.context.startup_schema_migration_count())
        .bind(self.context.startup_schema_migration_max_version())
        .execute(pool)
        .await
        .context("failed to mark unwrapped-authority replay checkpoint stream complete")?;

        self.status = "stream_complete".to_owned();
        self.scanned_log_count = scanned_log_count;
        self.matched_log_count = matched_log_count;
        Ok(())
    }

    pub(in crate::ens_v1_unwrapped_authority) async fn mark_completed(
        &mut self,
        pool: &PgPool,
        summary: &EnsV1UnwrappedAuthoritySyncSummary,
    ) -> Result<()> {
        let state_payload = self.context.bind_startup_authority(json!({
            "version": SNAPSHOT_VERSION,
            "summary": summary_payload(summary),
        }))?;
        sqlx::query(
            r#"
            UPDATE normalized_replay_adapter_checkpoints
            SET
                status = 'completed',
                scanned_log_count = $6,
                matched_log_count = $7,
                state_payload = $8,
                raw_log_retention_generation = $9,
                raw_log_input_revision = $10,
                adapter_semantic_version = $11,
                schema_migration_count = $12,
                schema_migration_max_version = $13,
                completed_at = now(),
                updated_at = now(),
                last_failure_reason = NULL
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(self.context.checkpoint_scope)
        .bind(i64::try_from(summary.scanned_log_count).context("scanned log count overflowed i64")?)
        .bind(i64::try_from(summary.matched_log_count).context("matched log count overflowed i64")?)
        .bind(&state_payload)
        .bind(self.raw_log_input_version.retention_generation)
        .bind(self.raw_log_input_version.revision)
        .bind(self.context.startup_adapter_semantic_version())
        .bind(self.context.startup_schema_migration_count())
        .bind(self.context.startup_schema_migration_max_version())
        .execute(pool)
        .await
        .context("failed to mark unwrapped-authority replay checkpoint completed")?;

        self.status = "completed".to_owned();
        self.scanned_log_count = summary.scanned_log_count;
        self.matched_log_count = summary.matched_log_count;
        self.flushed_events = UnwrappedAuthorityReplayFlushedEvents::default();
        self.state_payload = state_payload;
        Ok(())
    }
}
