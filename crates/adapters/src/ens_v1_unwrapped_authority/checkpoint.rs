use super::*;
use crate::checkpoint_context::AdapterCheckpointContext;
use anyhow::{Context, Result, ensure};
use bigname_storage::{
    RawLogStagingInputVersion, load_raw_log_staging_input_version,
    raw_log_staging_block_range_changed_since,
};
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

mod items;
mod payload;
mod persistence;
mod startup_events;
use crate::checkpoint_codec::JsonbCheckpointCodec;
use items::{
    checkpoint_item_rows, checkpoint_pending_observation_delete_keys, delete_checkpoint_items,
    insert_checkpoint_items, update_checkpoint_progress,
};
pub(super) use payload::{decode_item, encode_item};
use payload::{flushed_events_from_payload, summary_from_payload, summary_payload};
pub use persistence::clear_replay_adapter_checkpoints;
use persistence::{delete_checkpoint, load_checkpoint_row};

const ADAPTER: &str = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY;
const SNAPSHOT_VERSION: i64 = 1;
const ITEM_KIND_HISTORY: &str = "name_history";
const ITEM_KIND_REVERSE_HISTORY: &str = "reverse_history";
const ITEM_KIND_KNOWN_NAME: &str = "known_name";
const ITEM_KIND_KNOWN_NAME_REF: &str = "known_name_ref";
const ITEM_KIND_NAMEHASH_LABELHASH: &str = "namehash_labelhash";
const ITEM_KIND_PENDING_OBSERVATIONS: &str = "pending_observations";
const ITEM_KIND_MIGRATED_NODE: &str = "migrated_registry_node";
const CHECKPOINT_ITEM_INSERT_BATCH_SIZE: usize = 250;
const CHECKPOINT_ITEM_DELETE_BATCH_SIZE: usize = 1_000;
const CHECKPOINT_CODEC: JsonbCheckpointCodec = JsonbCheckpointCodec::new(
    "__bigname_unwrapped_authority_checkpoint_string_v1_hex",
    "__bigname_unwrapped_authority_checkpoint_object_v1_hex",
);

#[derive(Default)]
pub(super) struct UnwrappedAuthorityReplayCheckpointDelta {
    pub(super) history_keys: BTreeSet<String>,
    pub(super) reverse_history_keys: BTreeSet<String>,
    pub(super) known_name_keys: BTreeSet<String>,
    pub(super) known_name_ref_keys: BTreeSet<String>,
    pub(super) namehash_labelhash_keys: BTreeSet<String>,
    pub(super) pending_observation_keys: BTreeSet<String>,
    pub(super) migrated_nodes: BTreeSet<String>,
}

impl UnwrappedAuthorityReplayCheckpointDelta {
    pub(super) fn mark_history(&mut self, key: impl Into<String>) {
        self.history_keys.insert(key.into());
    }

    pub(super) fn mark_reverse_history(&mut self, key: impl Into<String>) {
        self.reverse_history_keys.insert(key.into());
    }

    pub(super) fn mark_known_name(&mut self, key: impl Into<String>) {
        self.known_name_keys.insert(key.into());
    }

    pub(super) fn mark_known_name_ref(&mut self, key: impl Into<String>) {
        self.known_name_ref_keys.insert(key.into());
    }

    pub(super) fn mark_namehash_labelhash(&mut self, key: impl Into<String>) {
        self.namehash_labelhash_keys.insert(key.into());
    }

    pub(super) fn mark_pending_observations(&mut self, key: impl Into<String>) {
        self.pending_observation_keys.insert(key.into());
    }

    pub(super) fn mark_migrated_node(&mut self, node: impl Into<String>) {
        self.migrated_nodes.insert(node.into());
    }

    pub(super) fn clear(&mut self) {
        self.history_keys.clear();
        self.reverse_history_keys.clear();
        self.known_name_keys.clear();
        self.known_name_ref_keys.clear();
        self.namehash_labelhash_keys.clear();
        self.pending_observation_keys.clear();
        self.migrated_nodes.clear();
    }
}

pub(super) struct UnwrappedAuthorityReplayCheckpointState {
    pub(super) histories: BTreeMap<String, NameHistory>,
    pub(super) reverse_histories: BTreeMap<String, ReverseClaimSourceHistory>,
    pub(super) known_names_by_namehash: HashMap<String, NameMetadata>,
    pub(super) known_name_refs_by_namehash: HashMap<String, ObservationRef>,
    pub(super) namehash_to_labelhash: HashMap<String, String>,
    pub(super) pending_namehash_observations: HashMap<String, Vec<AuthorityObservation>>,
    pub(super) migrated_registry_nodes: MigratedRegistryNodes,
}

pub(super) struct UnwrappedAuthorityReplayCheckpointStateRef<'a> {
    pub(super) histories: &'a BTreeMap<String, NameHistory>,
    pub(super) reverse_histories: &'a BTreeMap<String, ReverseClaimSourceHistory>,
    pub(super) known_names_by_namehash: &'a HashMap<String, NameMetadata>,
    pub(super) known_name_refs_by_namehash: &'a HashMap<String, ObservationRef>,
    pub(super) namehash_to_labelhash: &'a HashMap<String, String>,
    pub(super) pending_namehash_observations: &'a HashMap<String, Vec<AuthorityObservation>>,
    pub(super) migrated_registry_nodes: &'a MigratedRegistryNodes,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct UnwrappedAuthorityReplayFlushedEvents {
    pub(super) total_count: usize,
    pub(super) inserted_count: usize,
    pub(super) by_kind: BTreeMap<String, usize>,
}

pub(super) struct UnwrappedAuthorityReplayCheckpoint {
    context: AdapterCheckpointContext,
    chain: String,
    status: String,
    last_block_number: Option<i64>,
    scanned_log_count: usize,
    matched_log_count: usize,
    flushed_events: UnwrappedAuthorityReplayFlushedEvents,
    state_payload: Value,
    raw_log_input_version: RawLogStagingInputVersion,
}

impl UnwrappedAuthorityReplayCheckpoint {
    pub(super) async fn load_or_start(
        pool: &PgPool,
        chain: &str,
        context: &AdapterCheckpointContext,
    ) -> Result<Self> {
        let raw_log_input_version = load_raw_log_staging_input_version(pool, chain).await?;
        let existing = load_checkpoint_row(pool, chain, context).await?;
        let reset_existing = match existing.as_ref() {
            Some(checkpoint) => {
                checkpoint.context.range_start_block_number != context.range_start_block_number
                    || !checkpoint.snapshot_version_is_current()
                    || context.startup_authority_changed(&checkpoint.state_payload)
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
            let state_payload =
                context.bind_startup_authority(json!({ "version": SNAPSHOT_VERSION }))?;
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
                    state_payload,
                    raw_log_retention_generation,
                    raw_log_input_revision
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
            )
            .bind(&context.deployment_profile)
            .bind(chain)
            .bind(&context.cursor_kind)
            .bind(ADAPTER)
            .bind(context.checkpoint_scope)
            .bind(context.range_start_block_number)
            .bind(context.target_block_number)
            .bind(state_payload)
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
                        WHEN replay_target_block_number < $6 AND (status = 'completed' OR (status = 'stream_complete' AND $5 = 'startup_adapter_sync')) THEN 'running'
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
            .bind(context.checkpoint_scope)
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
            .context("started unwrapped-authority replay checkpoint row was not found")
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
        let consumed_through = if self.status == "stream_complete" || self.status == "completed" {
            Some(self.context.target_block_number)
        } else {
            self.last_block_number
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

    pub(super) fn completed_summary(&self) -> Result<Option<EnsV1UnwrappedAuthoritySyncSummary>> {
        if self.status != "completed" {
            return Ok(None);
        }
        let summary = self
            .state_payload
            .get("summary")
            .context("completed unwrapped-authority replay checkpoint is missing summary")?;
        Ok(Some(summary_from_payload(summary)?))
    }

    pub(super) fn last_block_number(&self) -> Option<i64> {
        self.last_block_number
    }

    pub(super) fn target_block_number(&self) -> i64 {
        self.context.target_block_number
    }

    pub(super) fn needs_replay_auxiliary_state(&self) -> bool {
        self.last_block_number
            .is_none_or(|last_block_number| last_block_number < self.context.target_block_number)
    }

    pub(super) fn scanned_log_count(&self) -> usize {
        self.scanned_log_count
    }

    pub(super) fn matched_log_count(&self) -> usize {
        self.matched_log_count
    }

    pub(super) fn flushed_events(&self) -> &UnwrappedAuthorityReplayFlushedEvents {
        &self.flushed_events
    }

    pub(super) async fn load_state(
        &self,
        pool: &PgPool,
        include_replay_auxiliary_state: bool,
    ) -> Result<Option<UnwrappedAuthorityReplayCheckpointState>> {
        if self.last_block_number.is_none() {
            return Ok(None);
        }

        let materialization_item_kinds = vec![
            ITEM_KIND_HISTORY.to_owned(),
            ITEM_KIND_REVERSE_HISTORY.to_owned(),
        ];
        let mut rows = sqlx::query(
            r#"
            SELECT item_kind, item_key, item_payload
            FROM normalized_replay_adapter_checkpoint_items
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
              AND NOT (
                  item_kind = 'pending_observations'
                  AND item_payload = '[]'::jsonb
              )
              AND (
                  $6::BOOLEAN
                  OR item_kind = ANY($7::TEXT[])
              )
            ORDER BY item_kind, item_key
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(self.context.checkpoint_scope)
        .bind(include_replay_auxiliary_state)
        .bind(&materialization_item_kinds)
        .fetch(pool);

        let mut histories = BTreeMap::new();
        let mut reverse_histories = BTreeMap::new();
        let mut known_names_by_namehash = HashMap::new();
        let mut known_name_refs_by_namehash = HashMap::new();
        let mut namehash_to_labelhash = HashMap::new();
        let mut pending_namehash_observations = HashMap::new();
        let mut migrated_nodes = HashSet::new();

        while let Some(row) = rows.try_next().await.with_context(|| {
            format!(
                "failed to load staged {ADAPTER} replay checkpoint items for {}/{}",
                self.context.deployment_profile, self.chain
            )
        })? {
            let item_kind: String = row.try_get("item_kind")?;
            let item_key: String = row.try_get("item_key")?;
            let item_payload: Value = row.try_get("item_payload")?;
            match item_kind.as_str() {
                ITEM_KIND_HISTORY => {
                    histories.insert(item_key, decode_item(item_payload, ITEM_KIND_HISTORY)?);
                }
                ITEM_KIND_REVERSE_HISTORY => {
                    reverse_histories.insert(
                        item_key,
                        decode_item(item_payload, ITEM_KIND_REVERSE_HISTORY)?,
                    );
                }
                ITEM_KIND_KNOWN_NAME => {
                    known_names_by_namehash
                        .insert(item_key, decode_item(item_payload, ITEM_KIND_KNOWN_NAME)?);
                }
                ITEM_KIND_KNOWN_NAME_REF => {
                    known_name_refs_by_namehash.insert(
                        item_key,
                        decode_item(item_payload, ITEM_KIND_KNOWN_NAME_REF)?,
                    );
                }
                ITEM_KIND_NAMEHASH_LABELHASH => {
                    if let Some(labelhash) = item_payload.get("labelhash").and_then(Value::as_str) {
                        namehash_to_labelhash.insert(item_key, labelhash.to_owned());
                    }
                }
                ITEM_KIND_PENDING_OBSERVATIONS => {
                    let observations = decode_item::<Vec<AuthorityObservation>>(
                        item_payload,
                        ITEM_KIND_PENDING_OBSERVATIONS,
                    )?;
                    if !observations.is_empty() {
                        pending_namehash_observations.insert(item_key, observations);
                    }
                }
                ITEM_KIND_MIGRATED_NODE => {
                    migrated_nodes.insert(item_key);
                }
                _ => {}
            }
        }

        Ok(Some(UnwrappedAuthorityReplayCheckpointState {
            histories,
            reverse_histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            migrated_registry_nodes: MigratedRegistryNodes::from_delta(migrated_nodes),
        }))
    }

    pub(super) async fn save_progress(
        &mut self,
        pool: &PgPool,
        boundary_block_number: i64,
        scanned_log_count: usize,
        matched_log_count: usize,
        state: UnwrappedAuthorityReplayCheckpointStateRef<'_>,
        delta: &UnwrappedAuthorityReplayCheckpointDelta,
        flushed_events: &UnwrappedAuthorityReplayFlushedEvents,
    ) -> Result<()> {
        let item_rows = checkpoint_item_rows(&state, delta)?;
        let pending_observation_delete_keys =
            checkpoint_pending_observation_delete_keys(&state, delta);
        let staged_item_count = state.histories.len() + state.reverse_histories.len();
        let staged_aux_item_count = state.known_names_by_namehash.len()
            + state.known_name_refs_by_namehash.len()
            + state.namehash_to_labelhash.len()
            + state.pending_namehash_observations.len()
            + state.migrated_registry_nodes.delta_nodes().count();
        let state_payload = self.context.bind_startup_authority(json!({
            "version": SNAPSHOT_VERSION,
            "last_block_number": boundary_block_number,
            "flushed_normalized_event_count": flushed_events.total_count,
            "flushed_normalized_event_inserted_count": flushed_events.inserted_count,
            "flushed_by_kind": &flushed_events.by_kind,
        }))?;

        let mut transaction = pool
            .begin()
            .await
            .context("failed to start unwrapped-authority replay checkpoint transaction")?;
        delete_checkpoint_items(
            &mut transaction,
            self,
            ITEM_KIND_PENDING_OBSERVATIONS,
            &pending_observation_delete_keys,
        )
        .await?;
        insert_checkpoint_items(&mut transaction, self, &item_rows).await?;
        update_checkpoint_progress(
            &mut transaction,
            self,
            "running",
            Some(boundary_block_number),
            scanned_log_count,
            matched_log_count,
            staged_item_count,
            staged_aux_item_count,
            state_payload.clone(),
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit unwrapped-authority replay checkpoint progress")?;

        self.last_block_number = Some(boundary_block_number);
        self.scanned_log_count = scanned_log_count;
        self.matched_log_count = matched_log_count;
        self.flushed_events = flushed_events.clone();
        self.status = "running".to_owned();
        self.state_payload = state_payload;
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
        .bind(self.context.checkpoint_scope)
        .bind(i64::try_from(scanned_log_count).context("scanned log count overflowed i64")?)
        .bind(i64::try_from(matched_log_count).context("matched log count overflowed i64")?)
        .bind(self.raw_log_input_version.retention_generation)
        .bind(self.raw_log_input_version.revision)
        .execute(pool)
        .await
        .context("failed to mark unwrapped-authority replay checkpoint stream complete")?;

        self.status = "stream_complete".to_owned();
        self.scanned_log_count = scanned_log_count;
        self.matched_log_count = matched_log_count;
        Ok(())
    }

    pub(super) async fn mark_completed(
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

    fn snapshot_version_is_current(&self) -> bool {
        self.state_payload
            .get("version")
            .and_then(Value::as_i64)
            .is_none_or(|version| version == SNAPSHOT_VERSION)
    }
}

#[cfg(test)]
mod tests;
