use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    RawLogStagingInputVersion, load_raw_log_staging_input_version,
    raw_log_staging_block_range_changed_since,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

use crate::registry_migration_cache::MigratedRegistryNodes;

use super::{
    EnsV1SubregistryDiscoverySyncSummary, assignment::ObservedRegistryAssignment,
    loader::RegistryRawLogPosition,
};

mod payload;

use payload::{assignment_from_payload, assignment_payload, summary_from_payload, summary_payload};

const ADAPTER: &str = "ens_v1_subregistry_discovery";
const CHECKPOINT_SCOPE: &str = "full_closure";
const ITEM_KIND_LATEST_ASSIGNMENT: &str = "latest_assignment";
const ITEM_KIND_MIGRATED_NODE: &str = "migrated_registry_node";
const CHECKPOINT_ITEM_INSERT_BATCH_SIZE: usize = 500;
pub(super) const EVENT_PAGE_LIMIT: i64 = 20_000;
pub(super) const PAGE_LIMIT: i64 = 10_000;
pub(super) const RECONCILIATION_PAGE_LIMIT: i64 = 50_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayAdapterCheckpointContext {
    pub deployment_profile: String,
    pub cursor_kind: String,
    pub range_start_block_number: i64,
    pub target_block_number: i64,
}

#[derive(Clone, Debug)]
pub(super) struct StagedSubregistryState {
    pub(super) latest_assignments: BTreeMap<String, ObservedRegistryAssignment>,
    pub(super) migrated_registry_nodes: MigratedRegistryNodes,
}

#[derive(Clone, Debug)]
pub(super) struct SubregistryReplayCheckpoint {
    pub(super) context: ReplayAdapterCheckpointContext,
    pub(super) chain: String,
    status: String,
    last_position: Option<RegistryRawLogPosition>,
    scanned_log_count: usize,
    matched_log_count: usize,
    state_payload: Value,
    raw_log_input_version: RawLogStagingInputVersion,
}

impl SubregistryReplayCheckpoint {
    pub(super) async fn load_or_start(
        pool: &PgPool,
        chain: &str,
        context: &ReplayAdapterCheckpointContext,
    ) -> Result<Self> {
        let raw_log_input_version = load_raw_log_staging_input_version(pool, chain).await?;
        let existing = load_checkpoint_row(pool, chain, context).await?;
        let reset_existing = match existing.as_ref() {
            Some(checkpoint) => {
                checkpoint.context.range_start_block_number != context.range_start_block_number
                    || checkpoint
                        .raw_log_input_requires_reset(pool, raw_log_input_version)
                        .await?
            }
            None => false,
        };
        if reset_existing {
            delete_checkpoint(pool, chain, context).await?;
        }

        if existing.is_none() || reset_existing {
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
                    raw_log_retention_generation,
                    raw_log_input_revision
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(&context.deployment_profile)
            .bind(chain)
            .bind(&context.cursor_kind)
            .bind(ADAPTER)
            .bind(CHECKPOINT_SCOPE)
            .bind(context.range_start_block_number)
            .bind(context.target_block_number)
            .bind(raw_log_input_version.retention_generation)
            .bind(raw_log_input_version.revision)
            .execute(pool)
            .await
            .with_context(|| {
                format!(
                    "failed to start {ADAPTER} replay checkpoint for {}/{}",
                    context.deployment_profile, chain
                )
            })?;
        } else {
            sqlx::query(
                r#"
                UPDATE normalized_replay_adapter_checkpoints
                SET
                    replay_target_block_number = GREATEST(replay_target_block_number, $6),
                    status = CASE
                        WHEN status = 'completed' AND replay_target_block_number < $6 THEN 'running'
                        ELSE status
                    END,
                    completed_at = CASE
                        WHEN status = 'completed' AND replay_target_block_number < $6 THEN NULL
                        ELSE completed_at
                    END,
                    raw_log_retention_generation = $7,
                    raw_log_input_revision = $8,
                    updated_at = now()
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
            .bind(CHECKPOINT_SCOPE)
            .bind(context.target_block_number)
            .bind(raw_log_input_version.retention_generation)
            .bind(raw_log_input_version.revision)
            .execute(pool)
            .await
            .with_context(|| {
                format!(
                    "failed to refresh {ADAPTER} replay checkpoint for {}/{}",
                    context.deployment_profile, chain
                )
            })?;
        }

        load_checkpoint_row(pool, chain, context)
            .await?
            .context("started replay checkpoint row was not found")
    }

    async fn raw_log_input_requires_reset(
        &self,
        pool: &PgPool,
        current: RawLogStagingInputVersion,
    ) -> Result<bool> {
        if self.raw_log_input_version.retention_generation != current.retention_generation
            || self.raw_log_input_version.revision > current.revision
        {
            return Ok(true);
        }
        if self.raw_log_input_version.revision == current.revision {
            return Ok(false);
        }
        let consumed_through = if self.stream_complete() {
            Some(self.context.target_block_number)
        } else {
            self.last_position
                .as_ref()
                .map(|position| position.block_number)
        };
        let Some(consumed_through) = consumed_through else {
            return Ok(false);
        };
        raw_log_staging_block_range_changed_since(
            pool,
            &self.chain,
            self.raw_log_input_version.revision,
            self.context.range_start_block_number,
            consumed_through,
        )
        .await
    }

    pub(super) async fn ensure_raw_log_input_current(&self, pool: &PgPool) -> Result<()> {
        let observed = load_raw_log_staging_input_version(pool, &self.chain).await?;
        ensure!(
            observed == self.raw_log_input_version,
            "{ADAPTER} raw-log input changed before checkpoint publication on {}: expected generation {} revision {}, observed generation {} revision {}",
            self.chain,
            self.raw_log_input_version.retention_generation,
            self.raw_log_input_version.revision,
            observed.retention_generation,
            observed.revision
        );
        Ok(())
    }

    pub(super) fn completed_summary(&self) -> Result<Option<EnsV1SubregistryDiscoverySyncSummary>> {
        if self.status != "completed" {
            return Ok(None);
        }
        let summary = self
            .state_payload
            .get("summary")
            .context("completed subregistry replay checkpoint is missing summary")?;
        Ok(Some(summary_from_payload(summary)?))
    }

    pub(super) async fn load_staged_state(&self, pool: &PgPool) -> Result<StagedSubregistryState> {
        let rows = sqlx::query(
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
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(CHECKPOINT_SCOPE)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load staged {ADAPTER} replay checkpoint items for {}/{}",
                self.context.deployment_profile, self.chain
            )
        })?;

        let mut latest_assignments = BTreeMap::new();
        let mut migrated_nodes = HashSet::new();
        for row in rows {
            let item_kind: String = row.try_get("item_kind")?;
            let item_key: String = row.try_get("item_key")?;
            let item_payload: Value = row.try_get("item_payload")?;
            match item_kind.as_str() {
                ITEM_KIND_LATEST_ASSIGNMENT => {
                    latest_assignments.insert(item_key, assignment_from_payload(&item_payload)?);
                }
                ITEM_KIND_MIGRATED_NODE => {
                    migrated_nodes.insert(item_key);
                }
                _ => {}
            }
        }

        Ok(StagedSubregistryState {
            latest_assignments,
            migrated_registry_nodes: MigratedRegistryNodes::from_delta(migrated_nodes),
        })
    }

    pub(super) async fn active_assignment_count(
        &self,
        pool: &PgPool,
        discovery_sources: &[String],
    ) -> Result<usize> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_replay_adapter_checkpoint_items
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
              AND item_kind = $6
              AND item_payload ->> 'discovery_source' = ANY($7)
              AND lower(item_payload ->> 'to_address') <> $8
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(CHECKPOINT_SCOPE)
        .bind(ITEM_KIND_LATEST_ASSIGNMENT)
        .bind(discovery_sources)
        .bind(super::ZERO_ADDRESS)
        .fetch_one(pool)
        .await
        .context("failed to count active staged subregistry checkpoint assignments")?;
        usize::try_from(count).context("active staged assignment count overflowed usize")
    }

    pub(super) async fn load_assignment_page(
        &self,
        pool: &PgPool,
        discovery_source: &str,
        after_key: Option<&str>,
        limit: i64,
    ) -> Result<Vec<(String, ObservedRegistryAssignment)>> {
        let rows = sqlx::query(
            r#"
            SELECT item_key, item_payload
            FROM normalized_replay_adapter_checkpoint_items
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
              AND item_kind = $6
              AND item_payload ->> 'discovery_source' = $7
              AND ($8::TEXT IS NULL OR item_key > $8)
            ORDER BY item_key
            LIMIT $9
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(CHECKPOINT_SCOPE)
        .bind(ITEM_KIND_LATEST_ASSIGNMENT)
        .bind(discovery_source)
        .bind(after_key)
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load staged subregistry checkpoint assignment page")?;

        rows.into_iter()
            .map(|row| {
                let item_key: String = row.try_get("item_key")?;
                let item_payload: Value = row.try_get("item_payload")?;
                Ok((item_key, assignment_from_payload(&item_payload)?))
            })
            .collect()
    }

    pub(super) fn stream_complete(&self) -> bool {
        self.status == "stream_complete" || self.status == "completed"
    }

    pub(super) fn scanned_log_count(&self) -> usize {
        self.scanned_log_count
    }

    pub(super) fn matched_log_count(&self) -> usize {
        self.matched_log_count
    }

    pub(super) fn last_position(&self) -> Option<RegistryRawLogPosition> {
        self.last_position.clone()
    }

    pub(super) fn range_start_block_number(&self) -> i64 {
        self.context.range_start_block_number
    }

    pub(super) fn target_block_number(&self) -> i64 {
        self.context.target_block_number
    }

    pub(super) async fn save_progress(
        &mut self,
        pool: &PgPool,
        last_position: &RegistryRawLogPosition,
        scanned_log_count: usize,
        matched_log_count: usize,
        latest_assignments: &BTreeMap<String, ObservedRegistryAssignment>,
        changed_assignment_keys: &[String],
        migrated_nodes: &[String],
        staged_aux_item_count: usize,
    ) -> Result<()> {
        let assignment_keys = changed_assignment_keys
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let migrated_nodes = migrated_nodes.iter().cloned().collect::<BTreeSet<_>>();
        let mut item_rows = Vec::<(&'static str, String, Value)>::new();
        for assignment_key in assignment_keys {
            if let Some(assignment) = latest_assignments.get(&assignment_key) {
                item_rows.push((
                    ITEM_KIND_LATEST_ASSIGNMENT,
                    assignment_key,
                    assignment_payload(assignment),
                ));
            }
        }
        for node in migrated_nodes {
            item_rows.push((
                ITEM_KIND_MIGRATED_NODE,
                node.clone(),
                json!({ "node": node }),
            ));
        }

        let mut transaction = pool
            .begin()
            .await
            .context("failed to start subregistry replay checkpoint transaction")?;
        insert_checkpoint_items(&mut transaction, self, &item_rows).await?;
        update_checkpoint_progress(
            &mut transaction,
            self,
            "running",
            Some(last_position),
            scanned_log_count,
            matched_log_count,
            latest_assignments.len(),
            staged_aux_item_count,
            self.state_payload.clone(),
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit subregistry replay checkpoint progress")?;

        self.last_position = Some(last_position.clone());
        self.scanned_log_count = scanned_log_count;
        self.matched_log_count = matched_log_count;
        self.status = "running".to_owned();
        Ok(())
    }

    pub(super) async fn mark_stream_complete(
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
        .bind(CHECKPOINT_SCOPE)
        .bind(i64::try_from(scanned_log_count).context("scanned log count overflowed i64")?)
        .bind(i64::try_from(matched_log_count).context("matched log count overflowed i64")?)
        .bind(self.raw_log_input_version.retention_generation)
        .bind(self.raw_log_input_version.revision)
        .execute(pool)
        .await
        .context("failed to mark subregistry replay checkpoint stream complete")?;

        self.status = "stream_complete".to_owned();
        self.scanned_log_count = scanned_log_count;
        self.matched_log_count = matched_log_count;
        Ok(())
    }

    pub(super) async fn mark_completed(
        &mut self,
        pool: &PgPool,
        summary: &EnsV1SubregistryDiscoverySyncSummary,
    ) -> Result<()> {
        let state_payload = json!({ "summary": summary_payload(summary) });
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
        .bind(CHECKPOINT_SCOPE)
        .bind(i64::try_from(summary.scanned_log_count).context("scanned log count overflowed i64")?)
        .bind(i64::try_from(summary.matched_log_count).context("matched log count overflowed i64")?)
        .bind(&state_payload)
        .bind(self.raw_log_input_version.retention_generation)
        .bind(self.raw_log_input_version.revision)
        .execute(pool)
        .await
        .context("failed to mark subregistry replay checkpoint completed")?;

        self.status = "completed".to_owned();
        self.scanned_log_count = summary.scanned_log_count;
        self.matched_log_count = summary.matched_log_count;
        self.state_payload = state_payload;
        Ok(())
    }
}

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
    .bind(CHECKPOINT_SCOPE)
    .execute(pool)
    .await;
    if is_undefined_table_error(&result) {
        return Ok(());
    }
    result.with_context(|| {
        format!(
            "failed to clear replay adapter checkpoints for {deployment_profile}/{chain}/{cursor_kind}"
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

async fn load_checkpoint_row(
    pool: &PgPool,
    chain: &str,
    context: &ReplayAdapterCheckpointContext,
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
    .bind(CHECKPOINT_SCOPE)
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
    context: &ReplayAdapterCheckpointContext,
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
        context: ReplayAdapterCheckpointContext {
            deployment_profile: context.deployment_profile.clone(),
            cursor_kind: context.cursor_kind.clone(),
            range_start_block_number,
            target_block_number,
        },
        chain: chain.to_owned(),
        status: row.try_get("status")?,
        last_position,
        scanned_log_count: usize::try_from(row.try_get::<i64, _>("scanned_log_count")?)
            .context("checkpoint scanned log count overflowed usize")?,
        matched_log_count: usize::try_from(row.try_get::<i64, _>("matched_log_count")?)
            .context("checkpoint matched log count overflowed usize")?,
        state_payload: row.try_get("state_payload")?,
        raw_log_input_version: RawLogStagingInputVersion {
            retention_generation: row.try_get("raw_log_retention_generation")?,
            revision: row.try_get("raw_log_input_revision")?,
        },
    })
}

pub(super) async fn delete_checkpoint(
    pool: &PgPool,
    chain: &str,
    context: &ReplayAdapterCheckpointContext,
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
    .bind(CHECKPOINT_SCOPE)
    .execute(pool)
    .await
    .context("failed to reset stale subregistry replay checkpoint")?;
    Ok(())
}

async fn insert_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &SubregistryReplayCheckpoint,
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
                    .push_bind(CHECKPOINT_SCOPE)
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
            .context("failed to upsert replay adapter checkpoint items")?;
    }
    Ok(())
}

async fn update_checkpoint_progress(
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
    .bind(CHECKPOINT_SCOPE)
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
