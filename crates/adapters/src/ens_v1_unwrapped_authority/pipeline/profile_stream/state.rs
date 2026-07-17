use super::super::*;
use anyhow::ensure;
use serde_json::value::RawValue;
use sqlx::{Postgres, QueryBuilder};

pub(super) const ITEM_KIND_EVENT: &str = "normalized_event";
const ITEM_KIND_HISTORY: &str = "name_history";
const ITEM_KIND_REVERSE_HISTORY: &str = "reverse_history";
const ITEM_KIND_KNOWN_NAME: &str = "known_name";
const ITEM_KIND_KNOWN_NAME_REF: &str = "known_name_ref";
const ITEM_KIND_NAMEHASH_LABELHASH: &str = "namehash_labelhash";
const ITEM_KIND_PENDING_OBSERVATIONS: &str = "pending_observations";
const ITEM_KIND_MIGRATED_NODE: &str = "migrated_registry_node";
const STATE_INSERT_BATCH_SIZE: usize = 250;
pub(super) const MAX_LIVE_STATE_ITEM_COUNT: usize = 10_000;
pub(super) const MAX_LIVE_STATE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

struct SerializedStateItem {
    kind: &'static str,
    key: String,
    payload: Box<RawValue>,
}

pub(super) struct ProfilePageState {
    pub(super) histories: BTreeMap<String, NameHistory>,
    pub(super) reverse_histories: BTreeMap<String, ReverseClaimSourceHistory>,
    pub(super) known_names_by_namehash: HashMap<String, NameMetadata>,
    pub(super) known_name_refs_by_namehash: HashMap<String, ObservationRef>,
    pub(super) namehash_to_labelhash: HashMap<String, String>,
    pub(super) pending_namehash_observations: HashMap<String, Vec<AuthorityObservation>>,
    pub(super) migrated_registry_nodes: MigratedRegistryNodes,
}

impl Default for ProfilePageState {
    fn default() -> Self {
        Self {
            histories: BTreeMap::new(),
            reverse_histories: BTreeMap::new(),
            known_names_by_namehash: HashMap::new(),
            known_name_refs_by_namehash: HashMap::new(),
            namehash_to_labelhash: HashMap::new(),
            pending_namehash_observations: HashMap::new(),
            migrated_registry_nodes: MigratedRegistryNodes::empty(),
        }
    }
}

impl ProfilePageState {
    pub(super) async fn load(pool: &PgPool, run_id: Uuid, keys: &BTreeSet<String>) -> Result<Self> {
        ensure!(
            keys.len() <= MAX_LIVE_STATE_ITEM_COUNT,
            "resolver-profile page needs {} state keys, exceeding hard live-state bound {}",
            keys.len(),
            MAX_LIVE_STATE_ITEM_COUNT
        );
        if keys.is_empty() {
            return Ok(Self::default());
        }
        let keys = keys.iter().cloned().collect::<Vec<_>>();
        let (item_count, payload_bytes) = sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT COUNT(*)::BIGINT, COALESCE(SUM(pg_column_size(item_payload)), 0)::BIGINT
            FROM resolver_profile_reconciliation_state_items
            WHERE run_id = $1
              AND item_kind <> $2
              AND item_key = ANY($3::TEXT[])
            "#,
        )
        .bind(run_id)
        .bind(ITEM_KIND_EVENT)
        .bind(&keys)
        .fetch_one(pool)
        .await
        .context("failed to size resolver-profile page state")?;
        ensure!(
            usize::try_from(item_count).unwrap_or(usize::MAX) <= MAX_LIVE_STATE_ITEM_COUNT,
            "resolver-profile persisted page state exceeds hard item bound"
        );
        ensure!(
            usize::try_from(payload_bytes).unwrap_or(usize::MAX) <= MAX_LIVE_STATE_PAYLOAD_BYTES,
            "resolver-profile persisted page state exceeds hard payload bound"
        );

        let rows = sqlx::query(
            r#"
            SELECT item_kind, item_key, item_payload
            FROM resolver_profile_reconciliation_state_items
            WHERE run_id = $1
              AND item_kind <> $2
              AND item_key = ANY($3::TEXT[])
            ORDER BY item_kind, item_key
            "#,
        )
        .bind(run_id)
        .bind(ITEM_KIND_EVENT)
        .bind(&keys)
        .fetch_all(pool)
        .await
        .context("failed to load resolver-profile page state")?;

        let mut state = Self::default();
        let mut migrated_nodes = HashSet::new();
        for row in rows {
            let kind: String = row.try_get("item_kind")?;
            let key: String = row.try_get("item_key")?;
            let payload: Value = row.try_get("item_payload")?;
            match kind.as_str() {
                ITEM_KIND_HISTORY => {
                    state.histories.insert(key, decode_item(payload, &kind)?);
                }
                ITEM_KIND_REVERSE_HISTORY => {
                    state
                        .reverse_histories
                        .insert(key, decode_item(payload, &kind)?);
                }
                ITEM_KIND_KNOWN_NAME => {
                    state
                        .known_names_by_namehash
                        .insert(key, decode_item(payload, &kind)?);
                }
                ITEM_KIND_KNOWN_NAME_REF => {
                    state
                        .known_name_refs_by_namehash
                        .insert(key, decode_item(payload, &kind)?);
                }
                ITEM_KIND_NAMEHASH_LABELHASH => {
                    let labelhash = payload
                        .get("labelhash")
                        .and_then(Value::as_str)
                        .context("resolver-profile labelhash state is malformed")?;
                    state
                        .namehash_to_labelhash
                        .insert(key, labelhash.to_owned());
                }
                ITEM_KIND_PENDING_OBSERVATIONS => {
                    state
                        .pending_namehash_observations
                        .insert(key, decode_item(payload, &kind)?);
                }
                ITEM_KIND_MIGRATED_NODE => {
                    migrated_nodes.insert(key);
                }
                _ => bail!("unknown resolver-profile state item kind {kind}"),
            }
        }
        state.migrated_registry_nodes = MigratedRegistryNodes::from_delta(migrated_nodes);
        Ok(state)
    }

    pub(super) fn live_item_count(&self) -> usize {
        self.histories.len()
            + self.reverse_histories.len()
            + self.known_names_by_namehash.len()
            + self.known_name_refs_by_namehash.len()
            + self.namehash_to_labelhash.len()
            + self.pending_namehash_observations.len()
            + self.migrated_registry_nodes.delta_nodes().count()
    }

    pub(super) fn drain_resolver_events(&mut self) -> Vec<NormalizedEvent> {
        let mut events = Vec::new();
        for history in self.histories.values_mut() {
            events.append(&mut history.events);
        }
        for history in self.reverse_histories.values_mut() {
            events.append(&mut history.events);
        }
        events.retain(|event| {
            matches!(
                event.source_family.as_str(),
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1 | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
            )
        });
        events
    }

    pub(super) async fn persist(
        &self,
        pool: &PgPool,
        run_id: Uuid,
        affected_keys: &BTreeSet<String>,
        events: &[NormalizedEvent],
    ) -> Result<usize> {
        let item_rows = self.item_rows()?;
        ensure!(
            item_rows.len() <= MAX_LIVE_STATE_ITEM_COUNT,
            "resolver-profile page produced {} live state items, exceeding hard bound {}",
            item_rows.len(),
            MAX_LIVE_STATE_ITEM_COUNT
        );
        let payload_bytes = item_rows.iter().try_fold(0usize, |total, item| {
            let bytes = item.payload.get().len();
            total
                .checked_add(bytes)
                .context("resolver-profile state payload size overflow")
        })?;
        ensure!(
            payload_bytes <= MAX_LIVE_STATE_PAYLOAD_BYTES,
            "resolver-profile page produced {payload_bytes} state payload bytes, exceeding hard bound {MAX_LIVE_STATE_PAYLOAD_BYTES}"
        );

        let mut transaction = pool
            .begin()
            .await
            .context("failed to start resolver-profile page-state transaction")?;
        let affected_keys = affected_keys.iter().cloned().collect::<Vec<_>>();
        if !affected_keys.is_empty() {
            sqlx::query(
                r#"
                DELETE FROM resolver_profile_reconciliation_state_items
                WHERE run_id = $1
                  AND item_kind <> $2
                  AND item_key = ANY($3::TEXT[])
                "#,
            )
            .bind(run_id)
            .bind(ITEM_KIND_EVENT)
            .bind(&affected_keys)
            .execute(transaction.as_mut())
            .await
            .context("failed to evict prior resolver-profile page state")?;
        }
        insert_items(&mut transaction, run_id, &item_rows).await?;
        let event_rows = events
            .iter()
            .map(|event| {
                serialized_state_item(
                    ITEM_KIND_EVENT,
                    event.event_identity.clone(),
                    encode_item(event)?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        insert_items(&mut transaction, run_id, &event_rows).await?;
        let result = sqlx::query(
            r#"
            UPDATE resolver_profile_reconciliation_runs
            SET updated_at = now()
            WHERE run_id = $1 AND status = 'running'
            "#,
        )
        .bind(run_id)
        .execute(transaction.as_mut())
        .await
        .context("failed to refresh resolver-profile run after page staging")?;
        ensure!(
            result.rows_affected() == 1,
            "resolver-profile run disappeared during replay"
        );
        transaction
            .commit()
            .await
            .context("failed to commit resolver-profile page state")?;
        Ok(payload_bytes)
    }

    fn item_rows(&self) -> Result<Vec<SerializedStateItem>> {
        let mut rows = Vec::new();
        for (key, value) in &self.histories {
            rows.push(serialized_state_item(
                ITEM_KIND_HISTORY,
                key.clone(),
                encode_item(value)?,
            )?);
        }
        for (key, value) in &self.reverse_histories {
            rows.push(serialized_state_item(
                ITEM_KIND_REVERSE_HISTORY,
                key.clone(),
                encode_item(value)?,
            )?);
        }
        for (key, value) in &self.known_names_by_namehash {
            rows.push(serialized_state_item(
                ITEM_KIND_KNOWN_NAME,
                key.clone(),
                encode_item(value)?,
            )?);
        }
        for (key, value) in &self.known_name_refs_by_namehash {
            rows.push(serialized_state_item(
                ITEM_KIND_KNOWN_NAME_REF,
                key.clone(),
                encode_item(value)?,
            )?);
        }
        for (key, labelhash) in &self.namehash_to_labelhash {
            rows.push(serialized_state_item(
                ITEM_KIND_NAMEHASH_LABELHASH,
                key.clone(),
                json!({ "labelhash": labelhash }),
            )?);
        }
        for (key, value) in &self.pending_namehash_observations {
            rows.push(serialized_state_item(
                ITEM_KIND_PENDING_OBSERVATIONS,
                key.clone(),
                encode_item(value)?,
            )?);
        }
        for node in self.migrated_registry_nodes.delta_nodes() {
            rows.push(serialized_state_item(
                ITEM_KIND_MIGRATED_NODE,
                node.clone(),
                json!({ "node": node }),
            )?);
        }
        Ok(rows)
    }
}

fn serialized_state_item(
    kind: &'static str,
    key: String,
    payload: Value,
) -> Result<SerializedStateItem> {
    let payload = serde_json::value::to_raw_value(&payload)
        .context("failed to serialize resolver-profile state payload")?;
    Ok(SerializedStateItem { kind, key, payload })
}

async fn insert_items(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    run_id: Uuid,
    rows: &[SerializedStateItem],
) -> Result<()> {
    for chunk in rows.chunks(STATE_INSERT_BATCH_SIZE) {
        if chunk.is_empty() {
            continue;
        }
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO resolver_profile_reconciliation_state_items (run_id, item_kind, item_key, item_payload) ",
        );
        builder.push_values(chunk, |mut row, item| {
            row.push_bind(run_id)
                .push_bind(item.kind)
                .push_bind(&item.key)
                .push_bind(sqlx::types::Json(item.payload.as_ref()));
        });
        builder.push(
            " ON CONFLICT (run_id, item_kind, item_key) DO UPDATE SET item_payload = EXCLUDED.item_payload, updated_at = now()",
        );
        builder
            .build()
            .execute(transaction.as_mut())
            .await
            .context("failed to stage resolver-profile state items")?;
    }
    Ok(())
}
