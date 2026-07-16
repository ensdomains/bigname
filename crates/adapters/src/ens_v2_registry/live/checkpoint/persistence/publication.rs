use anyhow::{Context, Result, ensure};

use super::StagedLiveRegistryReplayCheckpoint;
use crate::ens_v2_registry::live::checkpoint::{
    LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER, LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND,
    LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE, LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE,
};

pub(in crate::ens_v2_registry) async fn finalize_live_registry_replay_checkpoint(
    connection: &mut sqlx::PgConnection,
    checkpoint: &StagedLiveRegistryReplayCheckpoint,
) -> Result<()> {
    // This runs in the retained-history finish transaction. Any publication
    // error rolls the delete back, preserving the prior completed checkpoint.
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
    .bind(&checkpoint.deployment_profile)
    .bind(&checkpoint.chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE)
    .execute(&mut *connection)
    .await
    .context("failed to replace prior ENSv2 live checkpoint")?;
    let header_result = sqlx::query(
        r#"
        INSERT INTO normalized_replay_adapter_checkpoints (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope,
            replay_start_block_number,
            replay_target_block_number,
            last_block_number,
            last_transaction_index,
            last_log_index,
            last_emitting_address,
            staged_item_count,
            staged_aux_item_count,
            scanned_log_count,
            matched_log_count,
            status,
            state_payload,
            last_failure_reason,
            started_at,
            updated_at,
            completed_at,
            raw_log_retention_generation,
            raw_log_input_revision
        )
        SELECT
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            $5,
            replay_start_block_number,
            replay_target_block_number,
            last_block_number,
            last_transaction_index,
            last_log_index,
            last_emitting_address,
            staged_item_count,
            staged_aux_item_count,
            scanned_log_count,
            matched_log_count,
            'completed',
            state_payload,
            NULL,
            started_at,
            now(),
            now(),
            raw_log_retention_generation,
            raw_log_input_revision
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $6
          AND status = 'running'
          AND replay_target_block_number = $7
          AND raw_log_retention_generation = $8
          AND raw_log_input_revision = $9
          AND state_payload = $10
          AND staged_item_count = $11
        "#,
    )
    .bind(&checkpoint.deployment_profile)
    .bind(&checkpoint.chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE)
    .bind(checkpoint.through_block_number)
    .bind(checkpoint.raw_log_retention_generation)
    .bind(checkpoint.raw_log_input_revision)
    .bind(&checkpoint.state_payload)
    .bind(checkpoint.item_count)
    .execute(&mut *connection)
    .await
    .context("failed to publish ENSv2 live checkpoint header")?;
    ensure!(
        header_result.rows_affected() == 1,
        "ENSv2 live checkpoint publication did not match its staged row"
    );
    let item_result = sqlx::query(
        r#"
        INSERT INTO normalized_replay_adapter_checkpoint_items (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope,
            item_kind,
            item_key,
            item_payload,
            updated_at
        )
        SELECT
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            $5,
            item_kind,
            item_key,
            item_payload,
            now()
        FROM normalized_replay_adapter_checkpoint_items
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $6
        "#,
    )
    .bind(&checkpoint.deployment_profile)
    .bind(&checkpoint.chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE)
    .execute(&mut *connection)
    .await
    .context("failed to publish ENSv2 live checkpoint items")?;
    ensure!(
        item_result.rows_affected() == u64::try_from(checkpoint.item_count)?,
        "ENSv2 live checkpoint publication copied an unexpected item count"
    );
    let staging_result = sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&checkpoint.deployment_profile)
    .bind(&checkpoint.chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE)
    .execute(connection)
    .await
    .context("failed to clear staged ENSv2 live checkpoint")?;
    ensure!(
        staging_result.rows_affected() == 1,
        "ENSv2 live checkpoint staging row disappeared during publication"
    );
    Ok(())
}
