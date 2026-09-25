//! Shared fixture for the owned key family tests: a phase schema installed from the baseline,
//! a canonical lineage, and helpers that write events, run the family loop and snapshot every
//! family table.
#![allow(dead_code)]

use anyhow::Result;
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions, FamilyOutcome},
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

pub const CHAIN: &str = "ethereum-sepolia";
pub const CONTENT_HASH: &str = "families-fixture-hash";

pub fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

pub fn marker(block: i64) -> Marker {
    Marker {
        number: block,
        hash: hash(block),
    }
}

pub fn uuid(n: u32) -> String {
    format!("00000000-0000-0000-0000-{n:012x}")
}

/// Journalled family tables, compared by the undo and rebuild tests.
pub const FAMILY_TABLES: &[&str] = &[
    "project_name_state",
    "project_binding_candidate",
    "project_lifecycle_key_state",
    "project_lifecycle_triple_summary",
    "project_lifecycle_association",
    "project_lifecycle_event",
    "project_child_registration_state",
    "project_wrapper_state",
    "project_registry_node_state",
    "project_registry_binding_observation",
    "project_resolver_classification",
    "project_registry_pointer",
    "project_resource_pointer",
    "project_node_record_partition",
    "project_node_record_value",
    "project_record_id_value",
    "project_resolver_link",
    "project_grant",
    "project_resource_admin_aggregate",
    "project_account_approval",
    "project_name_alias",
    "project_resolver_alias",
    "project_child_edge_candidate",
    "project_parent_subregistry",
    "project_reverse_tuple",
    "project_reverse_node_claim",
    "project_claim_normalization",
    "project_address_name_fold",
    "project_address_controller_candidate",
    "project_address_name_index",
    "project_address_record_node_index",
    "project_address_record_id_index",
];

pub struct Fixture {
    pub database: TestDatabase,
    pub pool: PgPool,
}

impl Fixture {
    /// A phase schema with canonical blocks `0..=blocks`.
    pub async fn new(prefix: &str, blocks: i64) -> Result<Self> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
        let pool = database.pool().clone();
        let name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await?;
        let mut tx = pool.begin().await?;
        sqlx::query("CREATE SCHEMA bigname_phase")
            .execute(&mut *tx)
            .await?;
        raw_sql(&format!(
            "ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public",
            name.replace('"', r#""""#)
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query("SET LOCAL search_path TO bigname_phase, public")
            .execute(&mut *tx)
            .await?;
        for script in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
            include_str!("../../../../schema-v2/baseline/06_projections.sql"),
            include_str!("../../../../schema-v2/baseline/07_labels.sql"),
            include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
            include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
            include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
        ] {
            raw_sql(script).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        pool.set_connect_options(
            pool.connect_options()
                .as_ref()
                .clone()
                .options([("search_path", "bigname_phase,public")]),
        );
        let mut connection = pool.acquire().await?;
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        drop(connection);
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state)
             SELECT $1, '0x' || lpad(to_hex(block), 64, '0'),
                    CASE WHEN block > 0 THEN '0x' || lpad(to_hex(block - 1), 64, '0') END,
                    block, to_timestamp(1800000000 + block * 12), 'canonical'
             FROM generate_series(0, $2) block",
        )
        .bind(CHAIN)
        .bind(blocks)
        .execute(&pool)
        .await?;
        Ok(Self { database, pool })
    }

    pub async fn cleanup(self) -> Result<()> {
        self.database.cleanup().await
    }

    pub async fn apply(&self, target: i64, mode: FamilyMode) -> FamilyOutcome {
        self.apply_with(target, mode, &FamilyOptions::new(CONTENT_HASH))
            .await
    }

    pub async fn apply_with(
        &self,
        target: i64,
        mode: FamilyMode,
        options: &FamilyOptions,
    ) -> FamilyOutcome {
        // The Project phase reads the token right after its batch; the tests read it the same way.
        let token = families::input_token(&self.pool, CHAIN)
            .await
            .expect("the input token reads");
        families::apply(&self.pool, CHAIN, &marker(target), mode, &token, options).await
    }

    /// The family marker: block, hash and sequence.
    pub async fn marker(&self) -> Result<(Option<i64>, Option<String>, i64)> {
        Ok(sqlx::query_as(
            "SELECT current_block_number, current_block_hash, sequence
             FROM project_family_marker WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_one(&self.pool)
        .await?)
    }

    /// Blocks that have a marker journal row.
    pub async fn journalled_blocks(&self) -> Result<Vec<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT block_number FROM project_family_undo
             WHERE chain_id = $1 AND family = 'marker' ORDER BY 1",
        )
        .bind(CHAIN)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Every family table as JSON, rows ordered by their full text.
    pub async fn snapshot(&self) -> Result<Value> {
        let mut tables = serde_json::Map::new();
        for table in FAMILY_TABLES {
            let rows: Vec<Value> = sqlx::query_scalar(&format!(
                "SELECT to_jsonb(family_row) FROM {table} family_row
                 ORDER BY to_jsonb(family_row)::text"
            ))
            .fetch_all(&self.pool)
            .await?;
            tables.insert((*table).to_owned(), Value::Array(rows));
        }
        Ok(Value::Object(tables))
    }

    /// One event row. `position` is `(transaction_index, log_index)`; `None` writes a
    /// synthesised event with no transaction.
    pub async fn event(&self, event: Event<'_>) -> Result<i64> {
        let (transaction_hash, transaction_index, log_index) = match event.position {
            Some((transaction, log)) => (
                Some(format!("0xtx{}_{transaction}", event.block)),
                Some(transaction),
                Some(log),
            ),
            None => (None, None, None),
        };
        Ok(sqlx::query_scalar(
            "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
                 resource_id, event_kind, source_family, manifest_version, chain_id,
                 block_number, block_hash, transaction_hash, transaction_index, log_index,
                 derivation_kind, canonicality_state, before_state, after_state, raw_fact_ref)
             VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8, $9, $10, $11,
                     'ens_v2_registry_resource_surface', 'canonical', $12, $13, $14)
             RETURNING normalized_event_id",
        )
        .bind(event.identity)
        .bind(event.name)
        .bind(event.resource)
        .bind(event.kind)
        .bind(event.family)
        .bind(CHAIN)
        .bind(event.block)
        .bind(hash(event.block))
        .bind(transaction_hash)
        .bind(transaction_index)
        .bind(log_index)
        .bind(event.before)
        .bind(event.after)
        .bind(event.raw)
        .fetch_one(&self.pool)
        .await?)
    }

    /// A name surface, which events that carry a logical name reference.
    pub async fn surface(&self, logical_name_id: &str, namehash: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state)
             VALUES ($1, 'ens', $1, ARRAY[$1], '\\x00', $2, ARRAY[$2], 'ensip15', 'active',
                     $3, $4, 0, 'canonical')
             ON CONFLICT DO NOTHING",
        )
        .bind(logical_name_id)
        .bind(namehash)
        .bind(CHAIN)
        .bind(hash(0))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// A resource row, which events that carry a resource reference.
    pub async fn resource(&self, resource_id: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ($1::uuid, $2, $3, 0, 'canonical')
             ON CONFLICT DO NOTHING",
        )
        .bind(resource_id)
        .bind(CHAIN)
        .bind(hash(0))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

pub struct Event<'a> {
    pub identity: &'a str,
    pub block: i64,
    pub position: Option<(i64, i64)>,
    pub kind: &'a str,
    pub family: &'a str,
    pub name: Option<&'a str>,
    pub resource: Option<&'a str>,
    pub before: Value,
    pub after: Value,
    pub raw: Value,
}

impl<'a> Event<'a> {
    pub fn new(identity: &'a str, block: i64, log: i64, kind: &'a str, family: &'a str) -> Self {
        Self {
            identity,
            block,
            position: Some((0, log)),
            kind,
            family,
            name: None,
            resource: None,
            before: json!({}),
            after: json!({}),
            raw: json!({"emitting_address": "0x00000000000000000000000000000000000000a4"}),
        }
    }

    pub fn name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    pub fn resource(mut self, resource: &'a str) -> Self {
        self.resource = Some(resource);
        self
    }

    pub fn after(mut self, after: Value) -> Self {
        self.after = after;
        self
    }

    pub fn before(mut self, before: Value) -> Self {
        self.before = before;
        self
    }

    pub fn raw(mut self, raw: Value) -> Self {
        self.raw = raw;
        self
    }

    pub fn synthesised(mut self) -> Self {
        self.position = None;
        self
    }

    pub fn at(mut self, transaction: i64, log: i64) -> Self {
        self.position = Some((transaction, log));
        self
    }
}

impl Fixture {
    /// The Project row of `chain_phase_state` at redo attempt `attempt`. With `redo`, the row is
    /// inside an open redo of that range carrying that last error, as the runner leaves it while
    /// the redo batch commits.
    pub async fn project_row(&self, attempt: i64, redo: Option<(i64, i64, &str)>) -> Result<()> {
        sqlx::query("DELETE FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'")
            .bind(CHAIN)
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "INSERT INTO chain_phase_state (chain_id, phase_name, phase_status,
                 redo_attempt_generation, redo_in_progress, redo_mode,
                 redo_previous_phase_status, redo_from_block_number, redo_to_block_number,
                 last_error, started_at)
             VALUES ($1, 'project', CASE WHEN $3 THEN 'running' ELSE 'idle' END, $2, $3,
                     CASE WHEN $3 THEN 'redo' END, CASE WHEN $3 THEN 'idle' END, $4, $5, $6,
                     CASE WHEN $3 THEN now() END)",
        )
        .bind(CHAIN)
        .bind(attempt)
        .bind(redo.is_some())
        .bind(redo.map(|(from, _, _)| from))
        .bind(redo.map(|(_, to, _)| to))
        .bind(redo.map(|(_, _, error)| error))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The repair record as JSON, without its update time.
    pub async fn repair_record(&self) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(record) - 'updated_at' - 'chain_id'
             FROM project_repair_record record WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// A ResolverChanged at `block` pointing the name numbered `name` at `resolver`.
    pub async fn resolver_changed(
        &self,
        block: i64,
        log: i64,
        name: u64,
        resolver: &str,
    ) -> Result<i64> {
        let node = format!("0x{name:064x}");
        let logical_name_id = format!("ens:{node}");
        self.surface(&logical_name_id, &node).await?;
        let identity = format!("resolver-{block}-{log}");
        self.event(
            Event::new(
                &identity,
                block,
                log,
                "ResolverChanged",
                "ens_v1_registry_l1",
            )
            .name(&logical_name_id)
            .after(json!({"resolver": resolver, "node": node})),
        )
        .await
    }
}

impl Fixture {
    /// The Interpret row of `chain_phase_state` with its content hash and redo attempt, inside an
    /// open redo of blocks 0 to 1 when `in_redo`.
    pub async fn interpret_row(&self, hash: &str, attempt: i64, in_redo: bool) -> Result<()> {
        sqlx::query(
            "DELETE FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'interpret'",
        )
        .bind(CHAIN)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_phase_state (chain_id, phase_name, phase_status,
                 input_content_hash, redo_attempt_generation, redo_in_progress, redo_mode,
                 redo_previous_phase_status, redo_from_block_number, redo_to_block_number,
                 started_at)
             VALUES ($1, 'interpret', CASE WHEN $4 THEN 'running' ELSE 'idle' END, $2, $3, $4,
                     CASE WHEN $4 THEN 'redo' END, CASE WHEN $4 THEN 'idle' END,
                     CASE WHEN $4 THEN 0 END, CASE WHEN $4 THEN 1 END,
                     CASE WHEN $4 THEN now() END)",
        )
        .bind(CHAIN)
        .bind(hash)
        .bind(attempt)
        .bind(in_redo)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The chain's published safe and finalized blocks, which bound undo retention.
    pub async fn heads(&self, latest: i64, safe: i64, finalized: i64) -> Result<()> {
        for (state, through) in [("safe", safe), ("finalized", finalized)] {
            sqlx::query(
                "UPDATE chain_lineage SET canonicality_state = $2::canonicality_state
                 WHERE chain_id = $1 AND block_number <= $3
                   AND canonicality_state IN ('canonical', 'safe')
                   AND canonicality_state::text <> $2",
            )
            .bind(CHAIN)
            .bind(state)
            .bind(through)
            .execute(&self.pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number,
                 safe_block_hash, safe_block_number, finalized_block_hash,
                 finalized_block_number)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (chain_id) DO UPDATE SET
                 latest_block_hash = EXCLUDED.latest_block_hash,
                 latest_block_number = EXCLUDED.latest_block_number,
                 safe_block_hash = EXCLUDED.safe_block_hash,
                 safe_block_number = EXCLUDED.safe_block_number,
                 finalized_block_hash = EXCLUDED.finalized_block_hash,
                 finalized_block_number = EXCLUDED.finalized_block_number",
        )
        .bind(CHAIN)
        .bind(hash(latest))
        .bind(latest)
        .bind(hash(safe))
        .bind(safe)
        .bind(hash(finalized))
        .bind(finalized)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The input revision the family marker records.
    pub async fn marker_revision(&self) -> Result<(Option<String>, Option<i64>)> {
        Ok(sqlx::query_as(
            "SELECT interpret_input_content_hash, interpret_redo_attempt
             FROM project_family_marker WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_one(&self.pool)
        .await?)
    }
}

impl Fixture {
    /// One family table's rows as JSON, ordered by their text.
    pub async fn rows(&self, table: &str) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT to_jsonb(family_row) - 'chain_id' FROM {table} family_row
             ORDER BY to_jsonb(family_row)::text"
        ))
        .fetch_all(&self.pool)
        .await?)
    }

    /// Every family table and the marker without its sequence, as text.
    pub async fn exact(&self) -> Result<Vec<(String, String)>> {
        let mut tables = Vec::new();
        for table in families::family_tables() {
            let rows: String = sqlx::query_scalar(&format!(
                "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), '[]')::text
                 FROM {table} t"
            ))
            .fetch_one(&self.pool)
            .await?;
            tables.push((table.to_owned(), rows));
        }
        let marker: Option<String> = sqlx::query_scalar(
            "SELECT (to_jsonb(m) - 'sequence')::text FROM project_family_marker m
             WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_optional(&self.pool)
        .await?;
        tables.push(("marker".to_owned(), marker.unwrap_or_default()));
        Ok(tables)
    }

    /// Apply through `last - 1`, keep the families, apply `last`, undo it and require the
    /// families byte for byte as they were; then apply `last` again.
    pub async fn assert_undo_restores(&self, last: i64) -> Result<()> {
        self.apply(last - 1, FamilyMode::Normal).await;
        let before = self.exact().await?;
        let applied = self.apply(last, FamilyMode::Normal).await;
        anyhow::ensure!(
            applied.skipped.is_none(),
            "block {last}: {:?}",
            applied.skipped
        );
        let undone = families::undo_to(&self.pool, CHAIN, last - 1).await?;
        anyhow::ensure!(undone == 1, "undid {undone} blocks, not block {last}");
        let after = self.exact().await?;
        for ((table, was), (_, now)) in before.iter().zip(&after) {
            anyhow::ensure!(
                was == now,
                "undo of {last} left {table} as {now}, not {was}"
            );
        }
        self.apply(last, FamilyMode::Normal).await;
        Ok(())
    }

    /// The families after the incremental run must equal a rebuild from scratch at `target`.
    pub async fn assert_rebuild_equal(&self, target: i64) -> Result<()> {
        let incremental = self.exact().await?;
        let rebuilt = self.apply(target, FamilyMode::Rebuild).await;
        anyhow::ensure!(rebuilt.skipped.is_none(), "rebuild: {:?}", rebuilt.skipped);
        let fresh = self.exact().await?;
        for ((table, was), (_, now)) in incremental.iter().zip(&fresh) {
            anyhow::ensure!(
                was == now,
                "a rebuild at {target} left {table} as {now}, not {was}"
            );
        }
        Ok(())
    }

    /// An event with its namespace, family and payloads set, at `block` and log `log` in
    /// transaction 0.
    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        block: i64,
        log: i64,
        kind: &str,
        family: &str,
        name: Option<&str>,
        resource: Option<&str>,
        after: Value,
        emitter: &str,
    ) -> Result<i64> {
        if let Some(resource) = resource {
            self.resource(resource).await?;
        }
        if let Some(name) = name {
            let namehash = name.split_once(':').map_or(name, |(_, hash)| hash);
            self.surface(name, namehash).await?;
        }
        let identity = format!("{kind}:{block}:{log}");
        let mut event = Event::new(&identity, block, log, kind, family)
            .after(after)
            .raw(json!({"emitting_address": emitter}));
        event.name = name;
        event.resource = resource;
        self.event(event).await
    }
}

impl Fixture {
    /// A surface binding created at `block`, with its provenance position, closed when the next
    /// binding of its name and arm opens at `closed_at`.
    #[allow(clippy::too_many_arguments)]
    pub async fn binding(
        &self,
        id: &str,
        name: &str,
        resource: &str,
        arm: &str,
        block: i64,
        log: i64,
        closed_at: Option<i64>,
    ) -> Result<()> {
        let namehash = name.split_once(':').map_or(name, |(_, hash)| hash);
        self.surface(name, namehash).await?;
        self.resource(resource).await?;
        sqlx::query(
            "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
                 binding_kind, authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, provenance, canonicality_state)
             VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4,
                     to_timestamp(1800000000 + $6 * 12), to_timestamp(1800000000 + $9 * 12),
                     $5, $7, $6, jsonb_build_object('transaction_index', 0, 'log_index', $8),
                     'canonical')",
        )
        .bind(id)
        .bind(name)
        .bind(resource)
        .bind(arm)
        .bind(CHAIN)
        .bind(block)
        .bind(hash(block))
        .bind(log)
        .bind(closed_at.map(|block| block as f64))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
