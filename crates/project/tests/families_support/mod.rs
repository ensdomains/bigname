//! Shared fixture for the owned key family tests: a phase schema installed from the baseline,
//! a canonical lineage, and helpers that write events, run the family loop and snapshot every
//! family table.
#![allow(dead_code)]

use anyhow::Result;
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions, FamilyOutcome, RebuildRanges},
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

pub const CHAIN: &str = "ethereum-sepolia";
/// The interpreter hash of this build: the composed name reader serves only a marker written by
/// it, as the publication fence does.
pub const CONTENT_HASH: &str = bigname_test_support::INTERPRETER_CONTENT_HASH;

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

/// The retired F10 alias tables. Historical reset migrations still name them in their literal
/// lists; 20261001100000_retire_resolver_alias_families.sql drops them.
pub const RETIRED_FAMILY_TABLES: [&str; 2] = ["project_name_alias", "project_resolver_alias"];

/// Journalled family tables, compared by the undo and rebuild tests.
pub const FAMILY_TABLES: &[&str] = &[
    "child_registration_events",
    "project_name_state",
    "project_binding_candidate",
    "project_lifecycle_key_state",
    "project_lifecycle_triple_summary",
    "project_lifecycle_association",
    "project_lifecycle_event",
    "project_child_registration_state",
    "project_wrapper_state",
    "project_registry_node_state",
    "project_registry_owner_event",
    "project_registry_binding_observation",
    "project_resolver_classification",
    "project_registry_pointer",
    "project_resource_pointer",
    "project_named_resource_pointer",
    "project_universal_resolver_proxy",
    "project_node_record_partition",
    "project_node_record_value",
    "project_record_id_value",
    "project_resolver_link",
    "project_grant",
    "project_resource_admin_aggregate",
    "project_account_approval",
    "project_ens_v2_entry_owner",
    "project_ens_v2_registry_parent",
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
    "project_name_history",
    "project_name_summary",
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
            include_str!("../../../storage/schema/baseline/01_chain.sql"),
            include_str!("../../../storage/schema/baseline/02_raw_facts.sql"),
            include_str!("../../../storage/schema/baseline/03_identity.sql"),
            include_str!("../../../storage/schema/baseline/04_manifests.sql"),
            include_str!("../../../storage/schema/baseline/05_normalized_events.sql"),
            include_str!("../../../storage/schema/baseline/06_projections.sql"),
            include_str!("../../../storage/schema/baseline/07_labels.sql"),
            include_str!("../../../storage/schema/baseline/08_heartbeats.sql"),
            include_str!("../../../storage/schema/baseline/09_divergence.sql"),
            include_str!("../../../storage/schema/baseline/10_phase_state.sql"),
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
        let fixture = Self { database, pool };
        fixture.lineage(CHAIN, blocks).await?;
        Ok(fixture)
    }

    /// Canonical blocks `0..=blocks` of `chain`, with the same hashes and times as `CHAIN`'s.
    pub async fn lineage(&self, chain: &str, blocks: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state)
             SELECT $1, '0x' || lpad(to_hex(block), 64, '0'),
                    CASE WHEN block > 0 THEN '0x' || lpad(to_hex(block - 1), 64, '0') END,
                    block, to_timestamp(1800000000 + block * 12), 'canonical'
             FROM generate_series(0, $2) block",
        )
        .bind(chain)
        .bind(blocks)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// One family run of `chain` to `target` in normal mode.
    pub async fn apply_on(
        &self,
        chain: &str,
        target: i64,
    ) -> bigname_project::Result<FamilyOutcome> {
        let token = families::input_token(&self.pool, chain).await?;
        let outcome = families::apply(
            &self.pool,
            chain,
            &marker(target),
            FamilyMode::Normal,
            &token,
            &FamilyOptions::new(CONTENT_HASH),
        )
        .await?;
        self.check_expiry_selector(&outcome, target).await;
        Ok(outcome)
    }

    /// Every publication a family test makes must leave an exact expiry selector.
    async fn check_expiry_selector(&self, outcome: &FamilyOutcome, target: i64) {
        if outcome.marker.is_some() {
            self.assert_expiry_selector()
                .await
                .unwrap_or_else(|error| panic!("expiry selector at {target}: {error:#}"));
        }
    }

    pub async fn cleanup(self) -> Result<()> {
        self.database.cleanup().await
    }

    pub async fn apply(
        &self,
        target: i64,
        mode: FamilyMode,
    ) -> bigname_project::Result<FamilyOutcome> {
        self.apply_with(target, mode, &FamilyOptions::new(CONTENT_HASH))
            .await
    }

    pub async fn apply_with(
        &self,
        target: i64,
        mode: FamilyMode,
        options: &FamilyOptions,
    ) -> bigname_project::Result<FamilyOutcome> {
        // The Project phase reads the token right after its batch; the tests read it the same way.
        let token = families::input_token(&self.pool, CHAIN).await?;
        let outcome =
            families::apply(&self.pool, CHAIN, &marker(target), mode, &token, options).await?;
        self.check_expiry_selector(&outcome, target).await;
        Ok(outcome)
    }

    /// Check the stored expiry selector (`project_name_summary.expiry_listable`, `expires_at`
    /// and `public_authority`) against every composed name row of the published chains, two
    /// ways. First against the rows themselves: a row is listable when its coverage is not
    /// unsupported and its registration carries a finite decimal expiry, at exactly that expiry
    /// and under the public authority its provenance maps to. Then against what the expiry
    /// listing serves for each namespace, unfiltered and for each authority value. Returns the
    /// listable names as (name, expiry, authority), in name order; none when no chain is
    /// published for this build.
    pub async fn assert_expiry_selector(&self) -> Result<Vec<(String, String, Option<String>)>> {
        use bigname_storage::{
            NameCurrentExpiringFilter, NameCurrentListOrder, UnixSeconds,
            families::name::{
                is_publication_unavailable, load_family_expiring_page,
                load_family_names_by_logical_name_ids,
            },
            name_current_public_authority,
        };
        use std::collections::{BTreeMap, BTreeSet};

        // A session of its own: tests that measure a statement's plan depend on what the
        // fixture's one pooled session has already run.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.pool.connect_options().as_ref().clone())
            .await?;
        let pool = &pool;
        let surfaces: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT surface.logical_name_id, surface.namespace, surface.chain_id
             FROM name_surfaces surface
             JOIN project_family_marker marker ON marker.chain_id = surface.chain_id
             WHERE marker.current_block_number IS NOT NULL",
        )
        .fetch_all(pool)
        .await?;
        let names: Vec<String> = surfaces.iter().map(|(name, ..)| name.clone()).collect();
        let composed = match load_family_names_by_logical_name_ids(pool, &names).await {
            Ok(composed) => composed,
            Err(error) if is_publication_unavailable(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut expected: BTreeMap<String, (UnixSeconds, Option<String>)> = BTreeMap::new();
        for (name, row) in &composed {
            let expiry = match &row.declared_summary["registration"]["expiry"] {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            }
            .filter(|text| {
                let digits = text.strip_prefix('-').unwrap_or(text);
                let (whole, fraction) = digits.split_once('.').unwrap_or((digits, "0"));
                [whole, fraction]
                    .iter()
                    .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
            });
            let Some(expiry) = expiry else { continue };
            if row.coverage["status"] == "unsupported" {
                continue;
            }
            let expiry: UnixSeconds = expiry
                .parse()
                .map_err(|_| anyhow::anyhow!("{name}: expiry {expiry} is not exact seconds"))?;
            expected.insert(
                name.clone(),
                (
                    expiry,
                    name_current_public_authority(&row.provenance).map(str::to_owned),
                ),
            );
        }
        let stored: Vec<(String, bool, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT summary.logical_name_id, summary.expiry_listable, summary.expires_at::text,
                    summary.public_authority
             FROM project_name_summary summary
             JOIN project_family_marker marker ON marker.chain_id = summary.chain_id
             WHERE marker.current_block_number IS NOT NULL
             ORDER BY summary.logical_name_id",
        )
        .fetch_all(pool)
        .await?;
        let mut listable = Vec::new();
        for (name, flag, expiry, authority) in stored {
            let want = expected.get(&name);
            anyhow::ensure!(
                flag == want.is_some(),
                "{name}: expiry_listable is {flag}, its composed row says {want:?}"
            );
            if let Some(row) = composed.get(&name) {
                anyhow::ensure!(
                    authority.as_deref() == name_current_public_authority(&row.provenance),
                    "{name}: public_authority is {authority:?}, its row serves {}",
                    row.provenance["authority_selection"]
                );
            } else {
                anyhow::ensure!(
                    authority.is_none(),
                    "{name}: authority with no composed row"
                );
            }
            let Some((want_expiry, _)) = want else {
                continue;
            };
            let expiry =
                expiry.ok_or_else(|| anyhow::anyhow!("{name}: listable without expiry"))?;
            anyhow::ensure!(
                expiry.parse::<UnixSeconds>().ok() == Some(*want_expiry),
                "{name}: expires_at is {expiry}, its row serves {want_expiry}"
            );
            listable.push((name, expiry, authority));
        }
        anyhow::ensure!(
            listable.len() == expected.len(),
            "listable rows without a summary: {expected:?} against {listable:?}"
        );

        let mut namespaces: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (_, namespace, chain) in surfaces {
            namespaces.entry(namespace).or_default().insert(chain);
        }
        let zero = UnixSeconds::from_seconds(0).expect("zero seconds");
        for (namespace, chains) in namespaces {
            let chains: Vec<String> = chains.into_iter().collect();
            let prefix = format!("{namespace}:");
            for authority in [None, Some("ens_v0"), Some("ens_v1"), Some("ens_v2")] {
                let mut listed = BTreeMap::new();
                for (after, before) in [(Some(zero), None), (None, Some(zero))] {
                    let filter = NameCurrentExpiringFilter {
                        namespace: namespace.clone(),
                        windows: vec![bigname_storage::NameCurrentExpiryWindow {
                            expires_after: after,
                            expires_before: before,
                        }],
                        authorities: authority.map(|value| vec![value.to_owned()]),
                        parent: None,
                    };
                    let mut cursor = None;
                    loop {
                        let page = load_family_expiring_page(
                            pool,
                            &filter,
                            NameCurrentListOrder::Asc,
                            cursor.as_ref(),
                            200,
                            &chains,
                        )
                        .await?;
                        for row in page.rows {
                            listed.insert(row.row.logical_name_id.clone(), row.expiry_date);
                        }
                        cursor = page.next_cursor;
                        if cursor.is_none() {
                            break;
                        }
                    }
                }
                let selected: BTreeMap<String, Option<UnixSeconds>> = listable
                    .iter()
                    .filter(|(name, _, stored)| {
                        name.starts_with(&prefix)
                            && authority.is_none_or(|value| stored.as_deref() == Some(value))
                    })
                    .map(|(name, expiry, _)| (name.clone(), expiry.parse().ok()))
                    .collect();
                anyhow::ensure!(
                    listed == selected,
                    "{namespace} authority {authority:?}: the listing serves {listed:?}, the \
                     selector holds {selected:?}"
                );
            }
        }
        Ok(listable)
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
        .bind(event.chain)
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
        self.surface_on(CHAIN, logical_name_id, namehash).await
    }

    /// A name surface of `chain`.
    pub async fn surface_on(
        &self,
        chain: &str,
        logical_name_id: &str,
        namehash: &str,
    ) -> Result<()> {
        // These fold fixtures use synthetic identity keys; their visible names still obey
        // the same normalization contract as surfaces written by Interpret.
        let labels = namehash.trim_start_matches("0x");
        let (first, last) = labels.split_at(labels.len() / 2);
        let normalized =
            bigname_domain::normalization::normalize_name(&format!("n{first}.n{last}.eth"))?;
        let labelhashes: Vec<String> = normalized
            .normalized_labels
            .iter()
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
            .collect();
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state)
             VALUES ($1, $10, $5, $6, $7, $2, $8, $9, 'active',
                     $3, $4, 0, 'canonical')
             ON CONFLICT DO NOTHING",
        )
        .bind(logical_name_id)
        .bind(namehash)
        .bind(chain)
        .bind(hash(0))
        .bind(normalized.normalized_name)
        .bind(normalized.normalized_labels)
        .bind(normalized.dns_encoded_name)
        .bind(labelhashes)
        .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
        .bind(
            logical_name_id
                .split_once(':')
                .expect("fixture namespace")
                .0,
        )
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
    pub chain: &'a str,
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
            chain: CHAIN,
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

    pub fn on(mut self, chain: &'a str) -> Self {
        self.chain = chain;
        self
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
        self.apply(last - 1, FamilyMode::Normal).await?;
        let before = self.exact().await?;
        self.apply(last, FamilyMode::Normal).await?;
        let undone = families::undo_to(&self.pool, CHAIN, last - 1).await?;
        anyhow::ensure!(undone == 1, "undid {undone} blocks, not block {last}");
        self.assert_expiry_selector().await?;
        let after = self.exact().await?;
        for ((table, was), (_, now)) in before.iter().zip(&after) {
            anyhow::ensure!(
                was == now,
                "undo of {last} left {table} as {now}, not {was}"
            );
        }
        self.apply(last, FamilyMode::Normal).await?;
        Ok(())
    }

    /// The families after the incremental run must equal a rebuild from scratch at `target`,
    /// both a rebuild that applies every work block below the target in ranges and one that
    /// applies each block in a transaction of its own. The per-block rebuild runs last, so the
    /// fixture is left as a per-block rebuild leaves it.
    pub async fn assert_rebuild_equal(&self, target: i64) -> Result<()> {
        let incremental = self.exact().await?;
        for (label, ranges) in [
            ("in ranges", RebuildRanges::Through(target)),
            ("block by block", RebuildRanges::Off),
        ] {
            let options = FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(ranges);
            let rebuilt = self
                .apply_with(target, FamilyMode::Rebuild, &options)
                .await
                .map_err(|error| anyhow::anyhow!("rebuild {label}: {error}"))?;
            // Every rebuild visits the target on its own; any other work block below it goes
            // into a range when ranges are on.
            let expected_ranges = matches!(ranges, RebuildRanges::Through(_)) && rebuilt.blocks > 1;
            anyhow::ensure!(
                (rebuilt.ranges > 0) == expected_ranges,
                "rebuild {label} at {target} applied {} blocks in {} ranges",
                rebuilt.blocks,
                rebuilt.ranges
            );
            let fresh = self.exact().await?;
            for ((table, was), (_, now)) in incremental.iter().zip(&fresh) {
                anyhow::ensure!(
                    was == now,
                    "a rebuild {label} at {target} left {table} as {now}, not {was}"
                );
            }
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
