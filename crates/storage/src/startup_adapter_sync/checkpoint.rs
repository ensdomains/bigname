use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgConnection;

use crate::raw_staging_revision::raw_log_staging_block_range_changed_since_from_connection;

use super::{
    STARTUP_ADAPTER_CHECKPOINT_SCOPE, STARTUP_ADAPTER_CURSOR_KIND,
    STARTUP_CANONICAL_LINEAGE_HEAD_FIELD, STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD,
    STARTUP_LINEAGE_MUTATION_REVISION_FIELD, STARTUP_LINEAGE_SCAN_EXTENT_FIELD,
    StartupAdapterLineageTailPolicy, StartupAdapterSyncKey, StartupCanonicalLineageHead,
    lineage::{
        CompletedLineageExtentDecision, StartupAdapterLineageState,
        completed_lineage_extent_decision,
    },
};

pub(super) async fn completed_checkpoint_matches(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    key: &StartupAdapterSyncKey,
    tail_policy: StartupAdapterLineageTailPolicy,
) -> Result<CompletedLineageExtentDecision> {
    let checkpoint = sqlx::query_as::<_, (Value, i64, i64)>(
        r#"
        SELECT
            state_payload,
            raw_log_input_revision,
            replay_target_block_number
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
          AND status = 'completed'
          AND completed_at IS NOT NULL
          AND raw_log_retention_generation = $6
          AND adapter_semantic_version = $7
          AND schema_migration_count = $8
          AND schema_migration_max_version = $9
          AND state_payload -> $10 = to_jsonb($11::BIGINT)
          AND state_payload ? $12
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .bind(key.raw_log_input_version.retention_generation)
    .bind(key.adapter_semantic_version)
    .bind(key.schema_migration_count)
    .bind(key.schema_migration_max_version)
    .bind(STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD)
    .bind(key.discovery_admission_epoch)
    .bind(STARTUP_CANONICAL_LINEAGE_HEAD_FIELD)
    .fetch_optional(&mut *connection)
    .await
    .with_context(|| {
        format!(
            "failed to load completed startup adapter checkpoint for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })?;
    let Some((state_payload, recorded_raw_log_revision, recorded_target)) = checkpoint else {
        return Ok(CompletedLineageExtentDecision::Reject);
    };
    let Some(recorded_revision) = state_payload
        .get(STARTUP_LINEAGE_MUTATION_REVISION_FIELD)
        .and_then(Value::as_i64)
    else {
        return Ok(CompletedLineageExtentDecision::Reject);
    };
    let Some(recorded_head_payload) = state_payload.get(STARTUP_CANONICAL_LINEAGE_HEAD_FIELD)
    else {
        return Ok(CompletedLineageExtentDecision::Reject);
    };
    let Ok(recorded_head) = serde_json::from_value::<Option<StartupCanonicalLineageHead>>(
        recorded_head_payload.clone(),
    ) else {
        return Ok(CompletedLineageExtentDecision::Reject);
    };
    let Some(scanned_extent_payload) = state_payload.get(STARTUP_LINEAGE_SCAN_EXTENT_FIELD) else {
        return Ok(CompletedLineageExtentDecision::Reject);
    };
    let Ok(scanned_extent) = serde_json::from_value::<Option<StartupCanonicalLineageHead>>(
        scanned_extent_payload.clone(),
    ) else {
        return Ok(CompletedLineageExtentDecision::Reject);
    };
    let scanned_through_block = scanned_extent.as_ref().map_or(0, |head| head.block_number);
    if recorded_target != scanned_through_block {
        return Ok(CompletedLineageExtentDecision::Reject);
    }
    if raw_log_staging_block_range_changed_since_from_connection(
        connection,
        chain,
        recorded_raw_log_revision,
        0,
        scanned_through_block,
    )
    .await?
    {
        return Ok(CompletedLineageExtentDecision::Reject);
    }
    completed_lineage_extent_decision(
        connection,
        chain,
        recorded_revision,
        recorded_head.as_ref(),
        scanned_extent.as_ref(),
        &StartupAdapterLineageState {
            mutation_revision: key.lineage_mutation_revision,
            canonical_lineage_head: key.canonical_lineage_head.clone(),
        },
        tail_policy,
    )
    .await
}

pub(super) async fn downgrade_completed_checkpoint_to_boundary_resume(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    key: &StartupAdapterSyncKey,
) -> Result<bool> {
    let state_payload = json!({
        (STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD): key.discovery_admission_epoch,
        (STARTUP_LINEAGE_MUTATION_REVISION_FIELD): key.lineage_mutation_revision,
        (STARTUP_CANONICAL_LINEAGE_HEAD_FIELD): key.canonical_lineage_head,
    });
    let result = sqlx::query(
        r#"
        UPDATE normalized_replay_adapter_checkpoints
        SET
            status = 'stream_complete',
            last_block_number = replay_target_block_number,
            last_transaction_index = 0,
            last_log_index = 0,
            last_emitting_address = 'block-boundary',
            state_payload = (
                CASE
                    WHEN jsonb_typeof(state_payload) = 'object' THEN state_payload
                    ELSE '{}'::JSONB
                END
            ) || $6,
            raw_log_retention_generation = $7,
            raw_log_input_revision = $8,
            adapter_semantic_version = $9,
            schema_migration_count = $10,
            schema_migration_max_version = $11,
            last_failure_reason = NULL,
            completed_at = NULL,
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
          AND status = 'completed'
          AND completed_at IS NOT NULL
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .bind(state_payload)
    .bind(key.raw_log_input_version.retention_generation)
    .bind(key.raw_log_input_version.revision)
    .bind(key.adapter_semantic_version)
    .bind(key.schema_migration_count)
    .bind(key.schema_migration_max_version)
    .execute(connection)
    .await
    .with_context(|| {
        format!(
            "failed to downgrade completed startup adapter checkpoint to boundary resume for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn publish_completed_checkpoint(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    key: &StartupAdapterSyncKey,
    scanned_extent: Option<&StartupCanonicalLineageHead>,
) -> Result<()> {
    let replay_target_block_number = scanned_extent.map_or(0, |head| head.block_number);
    let state_payload = json!({
        (STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD): key.discovery_admission_epoch,
        (STARTUP_LINEAGE_MUTATION_REVISION_FIELD): key.lineage_mutation_revision,
        (STARTUP_CANONICAL_LINEAGE_HEAD_FIELD): key.canonical_lineage_head,
        (STARTUP_LINEAGE_SCAN_EXTENT_FIELD): scanned_extent,
    });
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_adapter_checkpoints (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope,
            replay_start_block_number,
            replay_target_block_number,
            status,
            state_payload,
            raw_log_retention_generation,
            raw_log_input_revision,
            adapter_semantic_version,
            schema_migration_count,
            schema_migration_max_version,
            completed_at
        )
        VALUES ($1, $2, $3, $4, $5, 0, $6, 'completed', $7, $8, $9, $10, $11, $12, now())
        ON CONFLICT (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope
        )
        DO UPDATE SET
            replay_target_block_number = EXCLUDED.replay_target_block_number,
            status = 'completed',
            state_payload = (
                CASE
                    WHEN jsonb_typeof(normalized_replay_adapter_checkpoints.state_payload) = 'object'
                        THEN normalized_replay_adapter_checkpoints.state_payload
                    ELSE '{}'::JSONB
                END
            ) || EXCLUDED.state_payload,
            raw_log_retention_generation = EXCLUDED.raw_log_retention_generation,
            raw_log_input_revision = EXCLUDED.raw_log_input_revision,
            adapter_semantic_version = EXCLUDED.adapter_semantic_version,
            schema_migration_count = EXCLUDED.schema_migration_count,
            schema_migration_max_version = EXCLUDED.schema_migration_max_version,
            last_failure_reason = NULL,
            completed_at = now(),
            updated_at = now()
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .bind(replay_target_block_number)
    .bind(state_payload)
    .bind(key.raw_log_input_version.retention_generation)
    .bind(key.raw_log_input_version.revision)
    .bind(key.adapter_semantic_version)
    .bind(key.schema_migration_count)
    .bind(key.schema_migration_max_version)
    .execute(connection)
    .await
    .with_context(|| {
        format!(
            "failed to publish completed startup adapter checkpoint for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })?;
    Ok(())
}

pub(super) async fn invalidate_startup_adapter_checkpoint(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
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
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .execute(connection)
    .await
    .with_context(|| {
        format!(
            "failed to invalidate startup adapter checkpoint for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })?;
    Ok(())
}

pub(super) async fn invalidate_completed_startup_adapter_checkpoint(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
          AND status = 'completed'
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .execute(connection)
    .await
    .with_context(|| {
        format!(
            "failed to invalidate non-matching completed startup adapter checkpoint for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })?;
    Ok(())
}
