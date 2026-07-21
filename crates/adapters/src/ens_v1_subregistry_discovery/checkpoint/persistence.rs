use anyhow::{Context, Result};
use bigname_storage::RawLogStagingInputVersion;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row};

use crate::checkpoint_context::AdapterCheckpointContext;

use super::{ADAPTER, RegistryRawLogPosition, SubregistryReplayCheckpoint};

pub(super) async fn load_checkpoint_row(
    pool: &PgPool,
    chain: &str,
    context: &AdapterCheckpointContext,
) -> Result<Option<SubregistryReplayCheckpoint>> {
    let row = sqlx::query(
        r#"
        SELECT
            replay_start_block_number,
            replay_target_block_number,
            last_block_number,
            last_transaction_index,
            last_log_index,
            last_emitting_address,
            scanned_log_count,
            matched_log_count,
            staged_item_count,
            status,
            state_payload,
            raw_log_retention_generation,
            raw_log_input_revision
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
) -> Result<SubregistryReplayCheckpoint> {
    let range_start_block_number = row.try_get("replay_start_block_number")?;
    let target_block_number = row.try_get("replay_target_block_number")?;
    let last_block_number: Option<i64> = row.try_get("last_block_number")?;
    let last_transaction_index: Option<i64> = row.try_get("last_transaction_index")?;
    let last_log_index: Option<i64> = row.try_get("last_log_index")?;
    let last_emitting_address: Option<String> = row.try_get("last_emitting_address")?;
    let last_position = match (
        last_block_number,
        last_transaction_index,
        last_log_index,
        last_emitting_address,
    ) {
        (Some(block_number), Some(transaction_index), Some(log_index), Some(emitting_address)) => {
            Some(RegistryRawLogPosition {
                block_number,
                transaction_index,
                log_index,
                emitting_address,
            })
        }
        _ => None,
    };

    Ok(SubregistryReplayCheckpoint {
        context: AdapterCheckpointContext {
            deployment_profile: context.deployment_profile.clone(),
            cursor_kind: context.cursor_kind.clone(),
            checkpoint_scope: context.checkpoint_scope,
            range_start_block_number,
            target_block_number,
            startup_discovery_admission_epoch: context.startup_discovery_admission_epoch,
        },
        chain: chain.to_owned(),
        status: row.try_get("status")?,
        last_position,
        scanned_log_count: usize::try_from(row.try_get::<i64, _>("scanned_log_count")?)
            .context("checkpoint scanned log count overflowed usize")?,
        matched_log_count: usize::try_from(row.try_get::<i64, _>("matched_log_count")?)
            .context("checkpoint matched log count overflowed usize")?,
        staged_item_count: usize::try_from(row.try_get::<i64, _>("staged_item_count")?)
            .context("checkpoint staged item count overflowed usize")?,
        state_payload: row.try_get("state_payload")?,
        raw_log_input_version: RawLogStagingInputVersion {
            retention_generation: row.try_get("raw_log_retention_generation")?,
            revision: row.try_get("raw_log_input_revision")?,
        },
    })
}

pub(super) async fn update_checkpoint_progress(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &SubregistryReplayCheckpoint,
    status: &str,
    last_position: Option<&RegistryRawLogPosition>,
    scanned_log_count: usize,
    matched_log_count: usize,
    staged_item_count: usize,
    staged_aux_item_count: usize,
    state_payload: Value,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE normalized_replay_adapter_checkpoints
        SET
            status = $6,
            last_block_number = $7,
            last_transaction_index = $8,
            last_log_index = $9,
            last_emitting_address = $10,
            staged_item_count = $11,
            staged_aux_item_count = $12,
            scanned_log_count = $13,
            matched_log_count = $14,
            state_payload = $15,
            raw_log_retention_generation = $16,
            raw_log_input_revision = $17,
            updated_at = now(),
            last_failure_reason = NULL
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&checkpoint.context.deployment_profile)
    .bind(&checkpoint.chain)
    .bind(&checkpoint.context.cursor_kind)
    .bind(ADAPTER)
    .bind(checkpoint.context.checkpoint_scope)
    .bind(status)
    .bind(last_position.map(|position| position.block_number))
    .bind(last_position.map(|position| position.transaction_index))
    .bind(last_position.map(|position| position.log_index))
    .bind(last_position.map(|position| position.emitting_address.as_str()))
    .bind(i64::try_from(staged_item_count).context("staged item count overflowed i64")?)
    .bind(i64::try_from(staged_aux_item_count).context("staged aux item count overflowed i64")?)
    .bind(i64::try_from(scanned_log_count).context("scanned log count overflowed i64")?)
    .bind(i64::try_from(matched_log_count).context("matched log count overflowed i64")?)
    .bind(state_payload)
    .bind(checkpoint.raw_log_input_version.retention_generation)
    .bind(checkpoint.raw_log_input_version.revision)
    .execute(transaction.as_mut())
    .await
    .context("failed to update replay adapter checkpoint progress")?;
    Ok(())
}
