use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{Postgres, QueryBuilder};

use crate::checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress};

use super::{
    ADAPTER, CHECKPOINT_CODEC, CHECKPOINT_ITEM_DELETE_BATCH_SIZE,
    CHECKPOINT_ITEM_INSERT_BATCH_SIZE, ITEM_KIND_HISTORY, ITEM_KIND_KNOWN_NAME,
    ITEM_KIND_KNOWN_NAME_REF, ITEM_KIND_MIGRATED_NODE, ITEM_KIND_NAMEHASH_LABELHASH,
    ITEM_KIND_PENDING_OBSERVATIONS, ITEM_KIND_REVERSE_HISTORY, UnwrappedAuthorityReplayCheckpoint,
    UnwrappedAuthorityReplayCheckpointDelta, UnwrappedAuthorityReplayCheckpointStateRef,
    encode_item,
};

impl UnwrappedAuthorityReplayCheckpointDelta {
    pub(in crate::ens_v1_unwrapped_authority) fn mark_history(&mut self, key: impl Into<String>) {
        self.history_keys.insert(key.into());
    }

    pub(in crate::ens_v1_unwrapped_authority) fn mark_reverse_history(
        &mut self,
        key: impl Into<String>,
    ) {
        self.reverse_history_keys.insert(key.into());
    }

    pub(in crate::ens_v1_unwrapped_authority) fn mark_known_name(
        &mut self,
        key: impl Into<String>,
    ) {
        self.known_name_keys.insert(key.into());
    }

    pub(in crate::ens_v1_unwrapped_authority) fn mark_known_name_ref(
        &mut self,
        key: impl Into<String>,
    ) {
        self.known_name_ref_keys.insert(key.into());
    }

    pub(in crate::ens_v1_unwrapped_authority) fn mark_namehash_labelhash(
        &mut self,
        key: impl Into<String>,
    ) {
        self.namehash_labelhash_keys.insert(key.into());
    }

    pub(in crate::ens_v1_unwrapped_authority) fn mark_pending_observations(
        &mut self,
        key: impl Into<String>,
    ) {
        self.pending_observation_keys.insert(key.into());
    }

    pub(in crate::ens_v1_unwrapped_authority) fn mark_migrated_node(
        &mut self,
        node: impl Into<String>,
    ) {
        self.migrated_nodes.insert(node.into());
    }

    pub(in crate::ens_v1_unwrapped_authority) fn clear(&mut self) {
        self.history_keys.clear();
        self.reverse_history_keys.clear();
        self.known_name_keys.clear();
        self.known_name_ref_keys.clear();
        self.namehash_labelhash_keys.clear();
        self.pending_observation_keys.clear();
        self.migrated_nodes.clear();
    }
}

#[cfg(test)]
pub(super) fn checkpoint_item_rows(
    state: &UnwrappedAuthorityReplayCheckpointStateRef<'_>,
    delta: &UnwrappedAuthorityReplayCheckpointDelta,
) -> Result<Vec<(&'static str, String, Value)>> {
    let mut rows = Vec::new();
    for key in &delta.history_keys {
        if let Some(value) = state.histories.get(key) {
            rows.push((ITEM_KIND_HISTORY, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.reverse_history_keys {
        if let Some(value) = state.reverse_histories.get(key) {
            rows.push((ITEM_KIND_REVERSE_HISTORY, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.known_name_keys {
        if let Some(value) = state.known_names_by_namehash.get(key) {
            rows.push((ITEM_KIND_KNOWN_NAME, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.known_name_ref_keys {
        if let Some(value) = state.known_name_refs_by_namehash.get(key) {
            rows.push((ITEM_KIND_KNOWN_NAME_REF, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.namehash_labelhash_keys {
        if let Some(labelhash) = state.namehash_to_labelhash.get(key) {
            rows.push((
                ITEM_KIND_NAMEHASH_LABELHASH,
                key.clone(),
                CHECKPOINT_CODEC.encode(json!({ "labelhash": labelhash })),
            ));
        }
    }
    for key in &delta.pending_observation_keys {
        if let Some(observations) = state
            .pending_namehash_observations
            .get(key)
            .filter(|observations| !observations.is_empty())
        {
            rows.push((
                ITEM_KIND_PENDING_OBSERVATIONS,
                key.clone(),
                encode_item(observations.as_slice())?,
            ));
        }
    }
    for node in &delta.migrated_nodes {
        rows.push((
            ITEM_KIND_MIGRATED_NODE,
            node.clone(),
            CHECKPOINT_CODEC.encode(json!({ "node": node })),
        ));
    }
    Ok(rows)
}

#[cfg(test)]
pub(super) fn checkpoint_pending_observation_delete_keys(
    state: &UnwrappedAuthorityReplayCheckpointStateRef<'_>,
    delta: &UnwrappedAuthorityReplayCheckpointDelta,
) -> Vec<String> {
    delta
        .pending_observation_keys
        .iter()
        .filter(|key| {
            state
                .pending_namehash_observations
                .get(*key)
                .is_none_or(Vec::is_empty)
        })
        .cloned()
        .collect()
}

pub(super) async fn checkpoint_item_rows_with_progress(
    pool: &sqlx::PgPool,
    state: &UnwrappedAuthorityReplayCheckpointStateRef<'_>,
    delta: &UnwrappedAuthorityReplayCheckpointDelta,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<Vec<(&'static str, String, Value)>> {
    let mut rows = Vec::new();
    let mut examined = 0usize;
    macro_rules! push_encoded {
        ($keys:expr, $values:expr, $kind:expr) => {
            for key in $keys {
                if let Some(value) = $values.get(key) {
                    rows.push(($kind, key.clone(), encode_item(value)?));
                }
                examined += 1;
                if examined.is_multiple_of(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
                    record_startup_adapter_progress(pool, progress).await?;
                }
            }
        };
    }
    push_encoded!(
        delta.history_keys.iter(),
        state.histories,
        ITEM_KIND_HISTORY
    );
    push_encoded!(
        delta.reverse_history_keys.iter(),
        state.reverse_histories,
        ITEM_KIND_REVERSE_HISTORY
    );
    push_encoded!(
        delta.known_name_keys.iter(),
        state.known_names_by_namehash,
        ITEM_KIND_KNOWN_NAME
    );
    push_encoded!(
        delta.known_name_ref_keys.iter(),
        state.known_name_refs_by_namehash,
        ITEM_KIND_KNOWN_NAME_REF
    );
    for key in &delta.namehash_labelhash_keys {
        if let Some(labelhash) = state.namehash_to_labelhash.get(key) {
            rows.push((
                ITEM_KIND_NAMEHASH_LABELHASH,
                key.clone(),
                CHECKPOINT_CODEC.encode(json!({ "labelhash": labelhash })),
            ));
        }
        examined += 1;
        if examined.is_multiple_of(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
            record_startup_adapter_progress(pool, progress).await?;
        }
    }
    for key in &delta.pending_observation_keys {
        if let Some(observations) = state
            .pending_namehash_observations
            .get(key)
            .filter(|observations| !observations.is_empty())
        {
            rows.push((
                ITEM_KIND_PENDING_OBSERVATIONS,
                key.clone(),
                encode_item(observations.as_slice())?,
            ));
        }
        examined += 1;
        if examined.is_multiple_of(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
            record_startup_adapter_progress(pool, progress).await?;
        }
    }
    for node in &delta.migrated_nodes {
        rows.push((
            ITEM_KIND_MIGRATED_NODE,
            node.clone(),
            CHECKPOINT_CODEC.encode(json!({ "node": node })),
        ));
        examined += 1;
        if examined.is_multiple_of(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
            record_startup_adapter_progress(pool, progress).await?;
        }
    }
    if examined > 0 && !examined.is_multiple_of(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
        record_startup_adapter_progress(pool, progress).await?;
    }
    Ok(rows)
}

pub(super) async fn checkpoint_pending_observation_delete_keys_with_progress(
    pool: &sqlx::PgPool,
    state: &UnwrappedAuthorityReplayCheckpointStateRef<'_>,
    delta: &UnwrappedAuthorityReplayCheckpointDelta,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for (index, key) in delta.pending_observation_keys.iter().enumerate() {
        if state
            .pending_namehash_observations
            .get(key)
            .is_none_or(Vec::is_empty)
        {
            keys.push(key.clone());
        }
        if index + 1 == delta.pending_observation_keys.len()
            || (index + 1).is_multiple_of(CHECKPOINT_ITEM_DELETE_BATCH_SIZE)
        {
            record_startup_adapter_progress(pool, progress).await?;
        }
    }
    Ok(keys)
}

pub(super) async fn delete_checkpoint_items_with_progress(
    pool: &sqlx::PgPool,
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    item_kind: &str,
    item_keys: &[String],
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    for chunk in item_keys.chunks(CHECKPOINT_ITEM_DELETE_BATCH_SIZE) {
        delete_checkpoint_items(transaction, checkpoint, item_kind, chunk).await?;
        record_startup_adapter_progress(pool, progress).await?;
    }
    Ok(())
}

pub(super) async fn insert_checkpoint_items_with_progress(
    pool: &sqlx::PgPool,
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    item_rows: &[(&'static str, String, Value)],
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    for chunk in item_rows.chunks(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
        insert_checkpoint_items(transaction, checkpoint, chunk).await?;
        record_startup_adapter_progress(pool, progress).await?;
    }
    Ok(())
}

pub(super) async fn delete_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    item_kind: &str,
    item_keys: &[String],
) -> Result<()> {
    for chunk in item_keys.chunks(CHECKPOINT_ITEM_DELETE_BATCH_SIZE) {
        if chunk.is_empty() {
            continue;
        }
        sqlx::query(
            r#"
            DELETE FROM normalized_replay_adapter_checkpoint_items
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
              AND item_kind = $6
              AND item_key = ANY($7)
            "#,
        )
        .bind(&checkpoint.context.deployment_profile)
        .bind(&checkpoint.chain)
        .bind(&checkpoint.context.cursor_kind)
        .bind(ADAPTER)
        .bind(checkpoint.context.checkpoint_scope)
        .bind(item_kind)
        .bind(chunk)
        .execute(transaction.as_mut())
        .await
        .context("failed to delete cleared unwrapped-authority replay checkpoint items")?;
    }
    Ok(())
}

pub(super) async fn insert_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    item_rows: &[(&'static str, String, Value)],
) -> Result<()> {
    for chunk in item_rows.chunks(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
        if chunk.is_empty() {
            continue;
        }
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO normalized_replay_adapter_checkpoint_items (
                deployment_profile,
                chain_id,
                cursor_kind,
                adapter,
                checkpoint_scope,
                item_kind,
                item_key,
                item_payload
            )
            "#,
        );
        builder.push_values(
            chunk.iter(),
            |mut row, (item_kind, item_key, item_payload)| {
                row.push_bind(&checkpoint.context.deployment_profile)
                    .push_bind(&checkpoint.chain)
                    .push_bind(&checkpoint.context.cursor_kind)
                    .push_bind(ADAPTER)
                    .push_bind(checkpoint.context.checkpoint_scope)
                    .push_bind(*item_kind)
                    .push_bind(item_key)
                    .push_bind(item_payload);
            },
        );
        builder.push(
            r#"
            ON CONFLICT (
                deployment_profile,
                chain_id,
                cursor_kind,
                adapter,
                checkpoint_scope,
                item_kind,
                item_key
            ) DO UPDATE
            SET item_payload = EXCLUDED.item_payload,
                updated_at = now()
            "#,
        );
        builder
            .build()
            .execute(transaction.as_mut())
            .await
            .context("failed to upsert unwrapped-authority replay checkpoint items")?;
    }
    Ok(())
}

pub(super) async fn update_checkpoint_progress(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    status: &str,
    last_block_number: Option<i64>,
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
            last_transaction_index = CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE 0 END,
            last_log_index = CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE 0 END,
            last_emitting_address = CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE 'block-boundary' END,
            staged_item_count = $8,
            staged_aux_item_count = $9,
            scanned_log_count = $10,
            matched_log_count = $11,
            state_payload = $12,
            raw_log_retention_generation = $13,
            raw_log_input_revision = $14,
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
    .bind(last_block_number)
    .bind(i64::try_from(staged_item_count).context("staged item count overflowed i64")?)
    .bind(i64::try_from(staged_aux_item_count).context("staged aux item count overflowed i64")?)
    .bind(i64::try_from(scanned_log_count).context("scanned log count overflowed i64")?)
    .bind(i64::try_from(matched_log_count).context("matched log count overflowed i64")?)
    .bind(state_payload)
    .bind(checkpoint.raw_log_input_version.retention_generation)
    .bind(checkpoint.raw_log_input_version.revision)
    .execute(transaction.as_mut())
    .await
    .context("failed to update unwrapped-authority replay checkpoint progress")?;
    Ok(())
}
