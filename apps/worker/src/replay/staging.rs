#[path = "staging/cleanup.rs"]
pub(super) mod cleanup;
#[path = "staging/cursor.rs"]
mod cursor;
#[cfg(test)]
#[path = "staging/fingerprint.rs"]
pub(crate) mod fingerprint;
#[path = "staging/input_fence.rs"]
mod input_fence;
#[path = "staging/tables.rs"]
mod tables;

pub(crate) use cleanup::cleanup_projection_checkpoint;

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    CURRENT_PROJECTION_REPLAY_VERSION,
    projection_staging::{
        ensure_current_projection_full_replay_input_revision_in_transaction,
        load_current_projection_full_replay_input_revision_in_transaction,
    },
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tables::{create_stage_tables, drop_stage_tables, projection_stage_specs, stage_tables_exist};
use tracing::info;

/// Compatibility version for durable full-rebuild staging checkpoints.
///
/// Bump this for any incompatible source ordering, staged-row construction, stage-table shape,
/// publication, or completed-range classification change. The full bump contract lives in
/// `docs/projections.md` under "Replay status tracking".
const CURRENT_PROJECTION_STAGING_SCHEMA_VERSION: i32 = 2;

struct StoredCheckpoint {
    replay_version: i32,
    staging_schema_version: i32,
    normalized_target_block: Option<i64>,
    full_replay_input_revision: i64,
    validated_normalized_change_id: i64,
    stage_tables: Vec<String>,
    last_source_key: Option<Value>,
    completed_source_count: i64,
    staged_row_count: i64,
    staged_aux_row_count: i64,
    status: String,
}

pub(crate) struct ProjectionStagingCheckpoint {
    projection: &'static str,
    normalized_target_block: Option<i64>,
    full_replay_input_revision: i64,
    validated_normalized_change_id: i64,
    stage_tables: Vec<String>,
    last_source_key: Option<Value>,
    completed_source_count: i64,
    staged_row_count: i64,
    staged_aux_row_count: i64,
    staging_complete: bool,
}

pub(crate) struct StagingBatchProgress {
    last_source_key: Value,
    completed_source_count: i64,
    staged_row_count: i64,
    staged_aux_row_count: i64,
}

pub(crate) use input_fence::StagingBatchInputFence;

impl ProjectionStagingCheckpoint {
    pub(crate) async fn load_or_start(
        pool: &PgPool,
        projection: &'static str,
        normalized_target_block: Option<i64>,
    ) -> Result<Self> {
        let specs = projection_stage_specs(projection)?;
        let mut transaction = pool.begin().await.with_context(|| {
            format!("failed to open {projection} staging checkpoint transaction")
        })?;
        lock_projection_checkpoint(&mut transaction, projection).await?;
        let full_replay_input_revision =
            load_current_projection_full_replay_input_revision_in_transaction(&mut transaction)
                .await?;
        let current_change_id =
            crate::projection_apply::capture_normalized_event_change_watermark_in_transaction(
                &mut transaction,
            )
            .await?
            .change_id;
        let existing = load_checkpoint(&mut transaction, projection).await?;
        let structurally_reusable = match existing.as_ref() {
            Some(checkpoint) => {
                checkpoint.replay_version == CURRENT_PROJECTION_REPLAY_VERSION
                    && checkpoint.staging_schema_version
                        == CURRENT_PROJECTION_STAGING_SCHEMA_VERSION
                    && checkpoint.normalized_target_block == normalized_target_block
                    && checkpoint.full_replay_input_revision == full_replay_input_revision
                    && checkpoint.stage_tables.len() == specs.len()
                    && checkpoint_source_key_is_valid(projection, checkpoint)
                    && stage_tables_exist(&mut transaction, &checkpoint.stage_tables).await?
            }
            None => false,
        };
        let completed_sources_changed = if structurally_reusable {
            let checkpoint = existing
                .as_ref()
                .context("reusable staging checkpoint disappeared")?;
            match checkpoint.status.as_str() {
                "running" => {
                    if let Some(last_source_key) = checkpoint.last_source_key.as_ref() {
                        crate::projection_apply::completed_projection_sources_changed(
                            &mut transaction,
                            projection,
                            checkpoint.validated_normalized_change_id,
                            current_change_id,
                            crate::projection_apply::CompletedProjectionSourceRange::Through(
                                last_source_key,
                            ),
                        )
                        .await?
                    } else {
                        false
                    }
                }
                "staging_complete" => {
                    crate::projection_apply::completed_projection_sources_changed(
                        &mut transaction,
                        projection,
                        checkpoint.validated_normalized_change_id,
                        current_change_id,
                        crate::projection_apply::CompletedProjectionSourceRange::Full,
                    )
                    .await?
                }
                _ => unreachable!("structurally reusable checkpoint has a supported status"),
            }
        } else {
            false
        };
        let reusable = structurally_reusable && !completed_sources_changed;

        if reusable {
            let checkpoint = existing.context("reusable staging checkpoint disappeared")?;
            transaction.commit().await.with_context(|| {
                format!("failed to commit {projection} staging checkpoint resume")
            })?;
            info!(
                service = "worker",
                replay = "all_current_projections",
                projection,
                completed_source_count = checkpoint.completed_source_count,
                staged_row_count = checkpoint.staged_row_count,
                staged_aux_row_count = checkpoint.staged_aux_row_count,
                staging_complete = checkpoint.status == "staging_complete",
                "all-current projection staging resumed from durable checkpoint"
            );
            return Self::from_stored(projection, checkpoint);
        }

        if let Some(checkpoint) = existing {
            drop_stage_tables(&mut transaction, &checkpoint.stage_tables).await?;
            sqlx::query("DELETE FROM current_projection_staging_checkpoints WHERE projection = $1")
                .bind(projection)
                .execute(&mut *transaction)
                .await
                .with_context(|| {
                    format!("failed to discard stale {projection} staging checkpoint")
                })?;
            info!(
                service = "worker",
                replay = "all_current_projections",
                projection,
                old_replay_version = checkpoint.replay_version,
                old_staging_schema_version = checkpoint.staging_schema_version,
                old_normalized_target_block = checkpoint.normalized_target_block,
                old_full_replay_input_revision = checkpoint.full_replay_input_revision,
                old_validated_normalized_change_id = checkpoint.validated_normalized_change_id,
                current_replay_version = CURRENT_PROJECTION_REPLAY_VERSION,
                current_staging_schema_version = CURRENT_PROJECTION_STAGING_SCHEMA_VERSION,
                normalized_target_block,
                full_replay_input_revision,
                current_normalized_change_id = current_change_id,
                completed_sources_changed,
                "stale all-current projection staging checkpoint discarded"
            );
        }

        let stage_tables = create_stage_tables(&mut transaction, projection, &specs).await?;
        sqlx::query(
            r#"
            INSERT INTO current_projection_staging_checkpoints (
                projection,
                replay_version,
                staging_schema_version,
                completed_normalized_target_block,
                full_replay_input_revision,
                validated_normalized_change_id,
                stage_tables
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(projection)
        .bind(CURRENT_PROJECTION_REPLAY_VERSION)
        .bind(CURRENT_PROJECTION_STAGING_SCHEMA_VERSION)
        .bind(normalized_target_block)
        .bind(full_replay_input_revision)
        .bind(current_change_id)
        .bind(&stage_tables)
        .execute(&mut *transaction)
        .await
        .with_context(|| format!("failed to start {projection} staging checkpoint"))?;
        transaction
            .commit()
            .await
            .with_context(|| format!("failed to commit new {projection} staging checkpoint"))?;

        Ok(Self {
            projection,
            normalized_target_block,
            full_replay_input_revision,
            validated_normalized_change_id: current_change_id,
            stage_tables,
            last_source_key: None,
            completed_source_count: 0,
            staged_row_count: 0,
            staged_aux_row_count: 0,
            staging_complete: false,
        })
    }

    fn from_stored(projection: &'static str, checkpoint: StoredCheckpoint) -> Result<Self> {
        ensure!(
            checkpoint.status == "running" || checkpoint.status == "staging_complete",
            "unsupported {projection} staging checkpoint status {}",
            checkpoint.status
        );
        Ok(Self {
            projection,
            normalized_target_block: checkpoint.normalized_target_block,
            full_replay_input_revision: checkpoint.full_replay_input_revision,
            validated_normalized_change_id: checkpoint.validated_normalized_change_id,
            stage_tables: checkpoint.stage_tables,
            last_source_key: checkpoint.last_source_key,
            completed_source_count: checkpoint.completed_source_count,
            staged_row_count: checkpoint.staged_row_count,
            staged_aux_row_count: checkpoint.staged_aux_row_count,
            staging_complete: checkpoint.status == "staging_complete",
        })
    }

    pub(crate) fn stage_table(&self, index: usize) -> Result<&str> {
        self.stage_tables
            .get(index)
            .map(String::as_str)
            .with_context(|| {
                format!(
                    "{} staging checkpoint has no stage table at index {index}",
                    self.projection
                )
            })
    }

    pub(crate) fn last_source_key(&self) -> Option<&Value> {
        self.last_source_key.as_ref()
    }

    pub(crate) fn completed_source_count(&self) -> Result<usize> {
        usize::try_from(self.completed_source_count)
            .context("completed projection staging source count must fit usize")
    }

    pub(crate) fn staged_row_count(&self) -> Result<usize> {
        usize::try_from(self.staged_row_count)
            .context("projection staging row count must fit usize")
    }

    pub(crate) fn staged_aux_row_count(&self) -> Result<usize> {
        usize::try_from(self.staged_aux_row_count)
            .context("projection auxiliary staging row count must fit usize")
    }

    pub(crate) fn staging_complete(&self) -> bool {
        self.staging_complete
    }

    pub(crate) fn full_replay_input_revision(&self) -> i64 {
        self.full_replay_input_revision
    }

    pub(crate) fn progress_after_batch(
        &self,
        completed_source_count: usize,
        last_source_key: Value,
        staged_row_count: u64,
        staged_aux_row_count: u64,
    ) -> Result<StagingBatchProgress> {
        ensure!(
            !self.staging_complete,
            "cannot advance completed {} staging checkpoint",
            self.projection
        );
        ensure!(
            completed_source_count > 0,
            "{} staging checkpoint batch must complete at least one source",
            self.projection
        );
        Ok(StagingBatchProgress {
            last_source_key,
            completed_source_count: self
                .completed_source_count
                .checked_add(i64::try_from(completed_source_count)?)
                .context("completed projection staging source count overflow")?,
            staged_row_count: self
                .staged_row_count
                .checked_add(i64::try_from(staged_row_count)?)
                .context("projection staging row count overflow")?,
            staged_aux_row_count: self
                .staged_aux_row_count
                .checked_add(i64::try_from(staged_aux_row_count)?)
                .context("projection auxiliary staging row count overflow")?,
        })
    }

    pub(crate) async fn mark_staging_complete(
        &mut self,
        pool: &PgPool,
        input_fence: StagingBatchInputFence,
    ) -> Result<()> {
        if self.staging_complete {
            return Ok(());
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
        let updated = sqlx::query(
            r#"
            UPDATE current_projection_staging_checkpoints
            SET
                status = 'staging_complete',
                staging_completed_at = now(),
                validated_normalized_change_id = $6,
                updated_at = now()
            WHERE projection = $1
              AND replay_version = $2
              AND staging_schema_version = $3
              AND completed_normalized_target_block IS NOT DISTINCT FROM $4
              AND full_replay_input_revision = $5
              AND validated_normalized_change_id = $7
              AND status = 'running'
            "#,
        )
        .bind(self.projection)
        .bind(CURRENT_PROJECTION_REPLAY_VERSION)
        .bind(CURRENT_PROJECTION_STAGING_SCHEMA_VERSION)
        .bind(self.normalized_target_block)
        .bind(self.full_replay_input_revision)
        .bind(input_fence.validated_normalized_change_id)
        .bind(self.validated_normalized_change_id)
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
        self.validated_normalized_change_id = input_fence.validated_normalized_change_id;
        Ok(())
    }
}

async fn lock_projection_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    projection: &str,
) -> Result<()> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('current_projection_staging:' || $1, 0))",
    )
    .bind(projection)
    .execute(&mut **transaction)
    .await
    .with_context(|| format!("failed to lock {projection} staging checkpoint"))?;
    Ok(())
}

async fn load_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    projection: &str,
) -> Result<Option<StoredCheckpoint>> {
    let row = sqlx::query(
        r#"
        SELECT
            replay_version,
            staging_schema_version,
            completed_normalized_target_block,
            full_replay_input_revision,
            validated_normalized_change_id,
            stage_tables,
            last_source_key,
            completed_source_count,
            staged_row_count,
            staged_aux_row_count,
            status
        FROM current_projection_staging_checkpoints
        WHERE projection = $1
        FOR UPDATE
        "#,
    )
    .bind(projection)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| format!("failed to load {projection} staging checkpoint"))?;
    row.map(|row| {
        Ok(StoredCheckpoint {
            replay_version: row.try_get("replay_version")?,
            staging_schema_version: row.try_get("staging_schema_version")?,
            normalized_target_block: row.try_get("completed_normalized_target_block")?,
            full_replay_input_revision: row.try_get("full_replay_input_revision")?,
            validated_normalized_change_id: row.try_get("validated_normalized_change_id")?,
            stage_tables: row.try_get("stage_tables")?,
            last_source_key: row.try_get("last_source_key")?,
            completed_source_count: row.try_get("completed_source_count")?,
            staged_row_count: row.try_get("staged_row_count")?,
            staged_aux_row_count: row.try_get("staged_aux_row_count")?,
            status: row.try_get("status")?,
        })
    })
    .transpose()
}

fn checkpoint_source_key_is_valid(projection: &str, checkpoint: &StoredCheckpoint) -> bool {
    if checkpoint.status != "running" && checkpoint.status != "staging_complete" {
        return false;
    }
    if checkpoint.completed_source_count == 0 {
        return checkpoint.last_source_key.is_none();
    }
    let Some(source_key) = checkpoint.last_source_key.as_ref() else {
        return false;
    };
    cursor::source_key_is_valid(projection, source_key)
}

#[cfg(test)]
pub(crate) fn current_staging_schema_version() -> i32 {
    CURRENT_PROJECTION_STAGING_SCHEMA_VERSION
}
