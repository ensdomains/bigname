use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};

use super::{
    LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER, LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND,
    LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE, LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE,
    payload::{SnapshotItemCounts, decode_metadata, decode_replay_state, encode_snapshot},
};
use crate::ens_v2_registry::live::cache::CachedLiveRegistryReplayState;

mod publication;
pub(in crate::ens_v2_registry) use publication::finalize_live_registry_replay_checkpoint;

const CHECKPOINT_ITEM_INSERT_BATCH_SIZE: usize = 256;

pub(in crate::ens_v2_registry::live) enum LiveRegistryReplayCheckpointLoad<T> {
    Missing,
    Invalid(String),
    Ready(T),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ens_v2_registry::live) struct LiveRegistryReplayCheckpointHeader {
    pub(in crate::ens_v2_registry::live) deployment_profile: String,
    pub(in crate::ens_v2_registry::live) chain: String,
    pub(in crate::ens_v2_registry::live) through_block_number: i64,
    pub(in crate::ens_v2_registry::live) through_block_hash: String,
    pub(in crate::ens_v2_registry::live) raw_log_input_revision: i64,
    pub(in crate::ens_v2_registry::live) raw_log_retention_generation: i64,
    pub(in crate::ens_v2_registry::live) discovery_admission_epoch: i64,
    item_counts: SnapshotItemCounts,
}

pub(in crate::ens_v2_registry) struct StagedLiveRegistryReplayCheckpoint {
    deployment_profile: String,
    chain: String,
    through_block_number: i64,
    raw_log_input_revision: i64,
    raw_log_retention_generation: i64,
    item_count: i64,
    state_payload: Value,
}

pub(in crate::ens_v2_registry::live) async fn load_live_registry_replay_checkpoint_header(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<LiveRegistryReplayCheckpointLoad<LiveRegistryReplayCheckpointHeader>> {
    let row = load_checkpoint_row(pool, deployment_profile, chain, false).await?;
    let Some(row) = row else {
        return Ok(LiveRegistryReplayCheckpointLoad::Missing);
    };
    Ok(
        match decode_checkpoint_header(deployment_profile, chain, row) {
            Ok(header) => LiveRegistryReplayCheckpointLoad::Ready(header),
            Err(error) => LiveRegistryReplayCheckpointLoad::Invalid(format!("{error:#}")),
        },
    )
}

pub(in crate::ens_v2_registry::live) async fn load_live_registry_replay_checkpoint(
    pool: &PgPool,
    expected: &LiveRegistryReplayCheckpointHeader,
) -> Result<LiveRegistryReplayCheckpointLoad<CachedLiveRegistryReplayState>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin ENSv2 live checkpoint load")?;
    let row = load_checkpoint_row(
        transaction.as_mut(),
        &expected.deployment_profile,
        &expected.chain,
        true,
    )
    .await?;
    let Some(row) = row else {
        transaction.rollback().await?;
        return Ok(LiveRegistryReplayCheckpointLoad::Invalid(
            "ENSv2 live checkpoint disappeared during load".to_owned(),
        ));
    };
    let observed =
        match decode_checkpoint_header(&expected.deployment_profile, &expected.chain, row) {
            Ok(header) => header,
            Err(error) => {
                transaction.rollback().await?;
                return Ok(LiveRegistryReplayCheckpointLoad::Invalid(format!(
                    "{error:#}"
                )));
            }
        };
    if observed != *expected {
        transaction.rollback().await?;
        return Ok(LiveRegistryReplayCheckpointLoad::Invalid(
            "ENSv2 live checkpoint changed during load".to_owned(),
        ));
    }
    let rows = sqlx::query_as::<_, (String, String, Value)>(
        r#"
        SELECT item_kind, item_key, item_payload
        FROM normalized_replay_adapter_checkpoint_items
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        ORDER BY item_kind, item_key
        "#,
    )
    .bind(&expected.deployment_profile)
    .bind(&expected.chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE)
    .fetch_all(transaction.as_mut())
    .await
    .context("failed to load ENSv2 live checkpoint items")?;
    transaction
        .commit()
        .await
        .context("failed to release ENSv2 live checkpoint read")?;
    Ok(
        match decode_replay_state(&expected.chain, rows, expected.item_counts) {
            Ok(replay_state) => {
                LiveRegistryReplayCheckpointLoad::Ready(CachedLiveRegistryReplayState {
                    through_block_number: expected.through_block_number,
                    through_block_hash: expected.through_block_hash.clone(),
                    raw_log_input_revision: expected.raw_log_input_revision,
                    raw_log_retention_generation: expected.raw_log_retention_generation,
                    discovery_admission_epoch: expected.discovery_admission_epoch,
                    replay_state,
                })
            }
            Err(error) => LiveRegistryReplayCheckpointLoad::Invalid(format!("{error:#}")),
        },
    )
}

pub(in crate::ens_v2_registry::live) async fn stage_live_registry_replay_checkpoint(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    snapshot: &CachedLiveRegistryReplayState,
) -> Result<StagedLiveRegistryReplayCheckpoint> {
    ensure!(
        !deployment_profile.trim().is_empty(),
        "ENSv2 live checkpoint deployment profile is empty"
    );
    let (state_payload, items, counts) = encode_snapshot(snapshot)?;
    let staged_item_count =
        i64::try_from(counts.total()?).context("ENSv2 live checkpoint item count exceeds i64")?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin ENSv2 live checkpoint staging")?;
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
            staged_item_count,
            staged_aux_item_count,
            scanned_log_count,
            matched_log_count,
            status,
            state_payload,
            raw_log_retention_generation,
            raw_log_input_revision,
            completed_at
        )
        VALUES ($1, $2, $3, $4, $5, 0, $6, $7, 0, 0, 0, 'running', $8, $9, $10, NULL)
        ON CONFLICT (deployment_profile, chain_id, cursor_kind, adapter, checkpoint_scope)
        DO UPDATE SET
            replay_start_block_number = 0,
            replay_target_block_number = EXCLUDED.replay_target_block_number,
            last_block_number = NULL,
            last_transaction_index = NULL,
            last_log_index = NULL,
            last_emitting_address = NULL,
            staged_item_count = EXCLUDED.staged_item_count,
            staged_aux_item_count = 0,
            scanned_log_count = 0,
            matched_log_count = 0,
            status = 'running',
            state_payload = EXCLUDED.state_payload,
            raw_log_retention_generation = EXCLUDED.raw_log_retention_generation,
            raw_log_input_revision = EXCLUDED.raw_log_input_revision,
            last_failure_reason = NULL,
            updated_at = now(),
            completed_at = NULL
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE)
    .bind(snapshot.through_block_number)
    .bind(staged_item_count)
    .bind(&state_payload)
    .bind(snapshot.raw_log_retention_generation)
    .bind(snapshot.raw_log_input_revision)
    .execute(transaction.as_mut())
    .await
    .with_context(|| {
        format!("failed to stage ENSv2 live checkpoint header for {deployment_profile}/{chain}")
    })?;
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoint_items
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE)
    .execute(transaction.as_mut())
    .await
    .context("failed to clear prior ENSv2 live checkpoint items")?;
    for chunk in items.chunks(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
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
        builder.push_values(chunk, |mut row, item| {
            row.push_bind(deployment_profile)
                .push_bind(chain)
                .push_bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
                .push_bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
                .push_bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE)
                .push_bind(item.item_kind)
                .push_bind(&item.item_key)
                .push_bind(&item.item_payload);
        });
        builder
            .build()
            .execute(transaction.as_mut())
            .await
            .context("failed to insert ENSv2 live checkpoint items")?;
    }
    transaction.commit().await.with_context(|| {
        format!("failed to stage ENSv2 live checkpoint for {deployment_profile}/{chain}")
    })?;
    Ok(StagedLiveRegistryReplayCheckpoint {
        deployment_profile: deployment_profile.to_owned(),
        chain: chain.to_owned(),
        through_block_number: snapshot.through_block_number,
        raw_log_input_revision: snapshot.raw_log_input_revision,
        raw_log_retention_generation: snapshot.raw_log_retention_generation,
        item_count: staged_item_count,
        state_payload,
    })
}

pub(in crate::ens_v2_registry::live) async fn clear_live_registry_replay_checkpoint(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope IN ($5, $6)
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE)
    .execute(pool)
    .await
    .context("failed to clear invalid ENSv2 live checkpoint")?;
    Ok(())
}

pub(in crate::ens_v2_registry) async fn clear_live_registry_replay_checkpoints_for_chain(
    pool: &PgPool,
    chain: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE chain_id = $1
          AND cursor_kind = $2
          AND adapter = $3
        "#,
    )
    .bind(chain)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
    .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
    .execute(pool)
    .await
    .with_context(|| format!("failed to clear ENSv2 live checkpoints for {chain}"))?;
    Ok(())
}

async fn load_checkpoint_row<'e, E>(
    executor: E,
    deployment_profile: &str,
    chain: &str,
    for_share: bool,
) -> Result<Option<PgRow>>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let lock = if for_share { " FOR SHARE" } else { "" };
    let query = format!(
        r#"
        SELECT
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
            raw_log_retention_generation,
            raw_log_input_revision
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5{lock}
        "#
    );
    sqlx::query(&query)
        .bind(deployment_profile)
        .bind(chain)
        .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND)
        .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER)
        .bind(LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE)
        .fetch_optional(executor)
        .await
        .with_context(|| {
            format!("failed to load ENSv2 live checkpoint for {deployment_profile}/{chain}")
        })
}

fn decode_checkpoint_header(
    deployment_profile: &str,
    chain: &str,
    row: PgRow,
) -> Result<LiveRegistryReplayCheckpointHeader> {
    let replay_start_block_number: i64 = row.try_get("replay_start_block_number")?;
    let replay_target_block_number: i64 = row.try_get("replay_target_block_number")?;
    let last_block_number: Option<i64> = row.try_get("last_block_number")?;
    let last_transaction_index: Option<i64> = row.try_get("last_transaction_index")?;
    let last_log_index: Option<i64> = row.try_get("last_log_index")?;
    let last_emitting_address: Option<String> = row.try_get("last_emitting_address")?;
    let staged_item_count: i64 = row.try_get("staged_item_count")?;
    let staged_aux_item_count: i64 = row.try_get("staged_aux_item_count")?;
    let scanned_log_count: i64 = row.try_get("scanned_log_count")?;
    let matched_log_count: i64 = row.try_get("matched_log_count")?;
    let status: String = row.try_get("status")?;
    let state_payload: Value = row.try_get("state_payload")?;
    let raw_log_retention_generation: i64 = row.try_get("raw_log_retention_generation")?;
    let raw_log_input_revision: i64 = row.try_get("raw_log_input_revision")?;
    ensure!(
        status == "completed",
        "ENSv2 live checkpoint is not completed"
    );
    ensure!(
        replay_start_block_number == 0,
        "ENSv2 live checkpoint replay start is not zero"
    );
    ensure!(
        replay_target_block_number >= 0,
        "ENSv2 live checkpoint target is negative"
    );
    ensure!(
        last_block_number.is_none()
            && last_transaction_index.is_none()
            && last_log_index.is_none()
            && last_emitting_address.is_none(),
        "ENSv2 live checkpoint unexpectedly carries a scan cursor"
    );
    ensure!(
        staged_aux_item_count == 0 && scanned_log_count == 0 && matched_log_count == 0,
        "ENSv2 live checkpoint carries unexpected auxiliary counts"
    );
    ensure!(
        raw_log_retention_generation >= 0 && raw_log_input_revision >= 0,
        "ENSv2 live checkpoint raw-log version is negative"
    );
    let metadata = decode_metadata(state_payload)?;
    ensure!(
        staged_item_count == i64::try_from(metadata.item_counts.total()?)?,
        "ENSv2 live checkpoint staged item count does not match metadata"
    );
    Ok(LiveRegistryReplayCheckpointHeader {
        deployment_profile: deployment_profile.to_owned(),
        chain: chain.to_owned(),
        through_block_number: replay_target_block_number,
        through_block_hash: metadata.through_block_hash,
        raw_log_input_revision,
        raw_log_retention_generation,
        discovery_admission_epoch: metadata.discovery_admission_epoch,
        item_counts: metadata.item_counts,
    })
}
