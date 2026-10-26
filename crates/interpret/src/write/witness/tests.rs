//! The lost-witness sets against the full scans they replaced. Every row runs a redo's steps in
//! the order `write::batch` runs them and checks both statements against frozen copies of the
//! full-scan forms on the same rows, at the points the redo runs them: the names statement 1
//! returns, the repair targets, and every surface row the repair leaves.
use std::collections::BTreeSet;

use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};

use super::super::{prepare_redo_range, reanchor, search};
use super::{arms, lost_witnesses};
use bigname_adapters::schema_v2::seam::{
    NAME_IDENTITY_OBSERVED_KEY, PREIMAGE_OBSERVATION_EVENT_KIND,
};

mod frozen;
mod ordering;
mod scans;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const CHAIN: &str = "ethereum";

const BASELINE: [&str; 14] = [
    include_str!("../../../../storage/schema/baseline/01_chain.sql"),
    include_str!("../../../../storage/schema/baseline/03_identity.sql"),
    include_str!("../../../../storage/schema/baseline/04_manifests.sql"),
    include_str!("../../../../storage/schema/baseline/05_normalized_events.sql"),
    include_str!("../../../../storage/schema/baseline/02_raw_facts.sql"),
    include_str!("../../../../storage/schema/baseline/06_projections.sql"),
    include_str!("../../../../storage/schema/baseline/07_labels.sql"),
    include_str!("../../../../storage/schema/baseline/08_heartbeats.sql"),
    include_str!("../../../../storage/schema/baseline/09_divergence.sql"),
    include_str!("../../../../storage/schema/baseline/10_phase_state.sql"),
    include_str!("../../../../storage/schema/baseline/11_manifest_authority_attestations.sql"),
    include_str!("../../../../storage/schema/baseline/12_project_generation_failures.sql"),
    include_str!("../../../../storage/schema/baseline/13_interpret_decode_skips.sql"),
    include_str!("../../../../storage/schema/baseline/14_discovery_watch_admissions.sql"),
];

/// A database with canonical blocks `1..=blocks`, block `n` hashed `hn` at time `n`. Autovacuum
/// is off on the three tables the statements read, so statistics exist only where a row
/// gathers them.
async fn database(name: &str, blocks: i64) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    database.create_phase_schema().await?;
    for sql in BASELINE {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::raw_sql(
        "ALTER TABLE name_surfaces SET (autovacuum_enabled = false);
         ALTER TABLE normalized_events SET (autovacuum_enabled = false);
         ALTER TABLE chain_lineage SET (autovacuum_enabled = false);",
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) SELECT $1, 'h' || number, number, to_timestamp(number), 'canonical'
           FROM generate_series(1, $2::bigint) number",
    )
    .bind(CHAIN)
    .bind(blocks)
    .execute(database.pool())
    .await?;
    Ok(database)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    /// Raw labels, active.
    Plain,
    /// Raw labels, deactivated by the normalization gate.
    Shadow,
    /// A byte-oriented shadow: empty raw name and labels, a witness it must keep.
    ByteShadow,
    /// No raw bytes, identity from the label-hash path.
    HashPath,
}

fn id(name: &str) -> String {
    format!("ens:{name}")
}

/// A surface named `name`, anchored at canonical block `anchor`.
async fn surface(
    pool: &PgPool,
    name: &str,
    anchor: i64,
    witness: Option<&str>,
    shape: Shape,
) -> TestResult {
    let (raw_name, raw_labels, dns): (Option<String>, Option<Vec<String>>, Option<Vec<u8>>) =
        match shape {
            Shape::Plain | Shape::Shadow => (
                Some(format!("{name}.eth")),
                Some(vec![name.to_owned(), "eth".to_owned()]),
                Some(vec![5]),
            ),
            Shape::ByteShadow => (Some(String::new()), Some(Vec::new()), Some(Vec::new())),
            Shape::HashPath => (None, None, None),
        };
    let visibility = match shape {
        Shape::Shadow | Shape::ByteShadow => "shadow",
        Shape::Plain | Shape::HashPath => "active",
    };
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, deactivation_reason,
             deactivated_at, chain_id, block_hash, block_number, provenance,
             canonicality_state, preimage_event_identity
         ) VALUES (
             'ens:' || $1, 'ens', $2, $3, $4, $1, ARRAY[$1 || '-label', '0xeth'], 'test', $5,
             CASE WHEN $5 = 'shadow' THEN 'normalization_gate' END,
             CASE WHEN $5 = 'shadow' THEN to_timestamp($6) END,
             $7, 'h' || $6, $6, '{}', 'canonical', $8
         )",
    )
    .bind(name)
    .bind(raw_name)
    .bind(raw_labels)
    .bind(dns)
    .bind(visibility)
    .bind(anchor)
    .bind(CHAIN)
    .bind(witness)
    .execute(pool)
    .await?;
    Ok(())
}

async fn event(
    pool: &PgPool,
    identity: &str,
    name: &str,
    kind: &str,
    (block, hash): (i64, &str),
    after: Value,
) -> TestResult {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             after_state
         ) VALUES ($1, 'ens', 'ens:' || $2, $3, 'ens_v1_registry_l1', 1, $4, $5, $6,
                   '0xtx' || $5, 0, 0, jsonb_build_object('event', $1),
                   'raw_log_preimage_observation', 'canonical', $7)",
    )
    .bind(identity)
    .bind(name)
    .bind(kind)
    .bind(CHAIN)
    .bind(block)
    .bind(hash)
    .bind(after)
    .execute(pool)
    .await?;
    Ok(())
}

/// A preimage observation of `name` on canonical block `block`.
async fn preimage(pool: &PgPool, identity: &str, name: &str, block: i64) -> TestResult {
    preimage_on(pool, identity, name, (block, &format!("h{block}"))).await
}

async fn preimage_on(pool: &PgPool, identity: &str, name: &str, at: (i64, &str)) -> TestResult {
    let after = json!({"raw_name": format!("{name}.eth"), "raw_labels": [name, "eth"]});
    event(
        pool,
        identity,
        name,
        PREIMAGE_OBSERVATION_EVENT_KIND,
        at,
        after,
    )
    .await
}

/// A label-hash-path observation that establishes `name` on canonical block `block`.
async fn hash_path(pool: &PgPool, name: &str, block: i64) -> TestResult {
    let after = json!({"source_event": "NewOwner", (NAME_IDENTITY_OBSERVED_KEY): true});
    let identity = format!("path-{name}-{block}");
    let hash = format!("h{block}");
    event(
        pool,
        &identity,
        name,
        "SubregistryChanged",
        (block, &hash),
        after,
    )
    .await
}

/// Orphan canonical block `block` and admit an empty replacement `rn` at its height.
async fn orphan(pool: &PgPool, block: i64) -> TestResult {
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = 'h' || $2",
    )
    .bind(CHAIN)
    .bind(block)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, 'r' || $2, $2, to_timestamp($2), 'canonical')",
    )
    .bind(CHAIN)
    .bind(block)
    .execute(pool)
    .await?;
    Ok(())
}

/// Reorg back to the original block `block`: its replacement is orphaned and `hn` is canonical.
async fn restore(pool: &PgPool, block: i64) -> TestResult {
    for (hash, state) in [
        (format!("r{block}"), "orphaned"),
        (format!("h{block}"), "canonical"),
    ] {
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = $3::canonicality_state
             WHERE chain_id = $1 AND block_hash = $2",
        )
        .bind(CHAIN)
        .bind(hash)
        .bind(state)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// The arm a wrong bound would drop.
#[derive(Clone, Copy, Debug)]
enum Mutant {
    WithoutReleased,
    WithoutOrphaned,
    WithoutByteShadow,
    /// The range arms only.
    RangeOnly,
}

fn mutant_lost(mutant: Mutant) -> String {
    let dropped = match mutant {
        Mutant::WithoutReleased => 0,
        Mutant::WithoutOrphaned => 1,
        Mutant::WithoutByteShadow => 2,
        Mutant::RangeOnly => {
            return "SELECT NULL::text AS logical_name_id WHERE false".to_owned();
        }
    };
    let mut kept = arms().to_vec();
    kept.remove(dropped);
    kept.join("\n UNION ALL\n ")
}

fn mutate(statement: &str, mutant: Mutant) -> String {
    let lost = lost_witnesses();
    assert!(statement.contains(&lost));
    statement.replace(&lost, &mutant_lost(mutant))
}

async fn names(
    conn: &mut PgConnection,
    statement: &str,
    range: (i64, i64),
) -> TestResult<Vec<String>> {
    Ok(sqlx::query_scalar(statement)
        .bind(CHAIN)
        .bind(range.0)
        .bind(range.1)
        .fetch_all(conn)
        .await?)
}

async fn targets(
    conn: &mut PgConnection,
    targets: &str,
    range: (i64, i64),
) -> TestResult<BTreeSet<String>> {
    let statement = format!("SELECT target.logical_name_id FROM ({targets}) target");
    Ok(names(conn, &statement, range).await?.into_iter().collect())
}

/// Every surface row, all columns. `observed_at = now()` is the transaction's start time, so two
/// runs of a statement in one transaction write the same value.
async fn surfaces(conn: &mut PgConnection) -> TestResult<Value> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(surface) ORDER BY logical_name_id), '[]')
         FROM name_surfaces surface",
    )
    .fetch_one(conn)
    .await?)
}

async fn run(conn: &mut PgConnection, statement: &str, range: (i64, i64)) -> TestResult {
    sqlx::query(statement)
        .bind(CHAIN)
        .bind(range.0)
        .bind(range.1)
        .execute(conn)
        .await?;
    Ok(())
}

/// One batch of a redo of `range`, its steps called in `write::batch`'s order.
struct Batch {
    transaction: Transaction<'static, Postgres>,
    range: (i64, i64),
}

impl Batch {
    async fn begin(pool: &PgPool, range: (i64, i64)) -> TestResult<Self> {
        Ok(Self {
            transaction: pool.begin().await?,
            range,
        })
    }

    /// Statement 1, which the redo's first and completing batches run before anything else,
    /// checked against the frozen full scan.
    async fn names(&mut self) -> TestResult<Vec<String>> {
        let current = names(&mut self.transaction, &search::redo_names(), self.range).await?;
        let frozen = names(&mut self.transaction, &frozen::redo_names(), self.range).await?;
        assert_eq!(current, frozen, "statement 1 over {:?}", self.range);
        Ok(current)
    }

    async fn mutant_names(&mut self, mutant: Mutant) -> TestResult<Vec<String>> {
        let statement = mutate(&search::redo_names(), mutant);
        names(&mut self.transaction, &statement, self.range).await
    }

    async fn prepare(&mut self) -> TestResult {
        prepare_redo_range(&mut self.transaction, CHAIN, self.range.0, self.range.1).await?;
        Ok(())
    }

    /// The replay's own writes, as SQL.
    async fn write(&mut self, sql: &str) -> TestResult {
        sqlx::raw_sql(sql).execute(&mut *self.transaction).await?;
        Ok(())
    }

    async fn reanchor(&mut self) -> TestResult {
        reanchor::reanchor_identities(&mut self.transaction, CHAIN, self.range.0, self.range.1)
            .await?;
        Ok(())
    }

    /// What `statement` would leave, run in a savepoint that is then rolled back.
    async fn trial(&mut self, statement: &str) -> TestResult<Value> {
        sqlx::raw_sql("SAVEPOINT trial")
            .execute(&mut *self.transaction)
            .await?;
        run(&mut self.transaction, statement, self.range).await?;
        let rows = surfaces(&mut self.transaction).await?;
        sqlx::raw_sql("ROLLBACK TO SAVEPOINT trial; RELEASE SAVEPOINT trial")
            .execute(&mut *self.transaction)
            .await?;
        Ok(rows)
    }

    /// The targets and the rows the repair would leave under `mutant`, left unapplied.
    async fn mutant_repair(&mut self, mutant: Mutant) -> TestResult<(BTreeSet<String>, Value)> {
        let targets = targets(
            &mut self.transaction,
            &mutate(&reanchor::repair_targets(), mutant),
            self.range,
        )
        .await?;
        let rows = self
            .trial(&mutate(&reanchor::repair_statement(), mutant))
            .await?;
        Ok((targets, rows))
    }

    /// The completion repair. Its targets and every surface row it leaves must be the frozen
    /// statement's on the same rows.
    async fn repair(&mut self) -> TestResult<BTreeSet<String>> {
        let current = targets(
            &mut self.transaction,
            &reanchor::repair_targets(),
            self.range,
        )
        .await?;
        let frozen_targets =
            targets(&mut self.transaction, &frozen::repair_targets(), self.range).await?;
        assert_eq!(
            current, frozen_targets,
            "repair targets over {:?}",
            self.range
        );
        let expected = self.trial(&frozen::repair()).await?;
        reanchor::repair_preimage_witnesses(
            &mut self.transaction,
            CHAIN,
            self.range.0,
            self.range.1,
        )
        .await?;
        assert_eq!(
            surfaces(&mut self.transaction).await?,
            expected,
            "repaired rows over {:?}",
            self.range
        );
        Ok(current)
    }

    /// The repair targets now, checked against the frozen statement's, without repairing.
    async fn targets(&mut self) -> TestResult<BTreeSet<String>> {
        let current = targets(
            &mut self.transaction,
            &reanchor::repair_targets(),
            self.range,
        )
        .await?;
        let frozen_targets =
            targets(&mut self.transaction, &frozen::repair_targets(), self.range).await?;
        assert_eq!(
            current, frozen_targets,
            "repair targets over {:?}",
            self.range
        );
        Ok(current)
    }

    async fn rows(&mut self) -> TestResult<Value> {
        surfaces(&mut self.transaction).await
    }

    async fn count(&mut self, statement: &str) -> TestResult<i64> {
        Ok(sqlx::query_scalar(statement)
            .fetch_one(&mut *self.transaction)
            .await?)
    }

    async fn commit(self) -> TestResult {
        self.transaction.commit().await?;
        Ok(())
    }

    async fn rollback(self) -> TestResult {
        self.transaction.rollback().await?;
        Ok(())
    }
}

/// The replay writing a preimage observation of `name` on canonical block `block`.
fn replayed_preimage(identity: &str, name: &str, block: i64) -> String {
    format!(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             after_state
         ) VALUES ('{identity}', 'ens', 'ens:{name}', '{PREIMAGE_OBSERVATION_EVENT_KIND}',
                   'ens_v1_registry_l1', 1, '{CHAIN}', {block}, 'h{block}', '0xtx{block}', 0, 0,
                   '{{}}', 'raw_log_preimage_observation', 'canonical', '{{}}');"
    )
}

/// A one-block or single-batch redo: statement 1, the range preparation, the replay's writes,
/// reanchoring and the repair in one transaction.
async fn single_batch_redo(
    pool: &PgPool,
    range: (i64, i64),
    writes: &str,
) -> TestResult<(Vec<String>, BTreeSet<String>)> {
    let mut batch = Batch::begin(pool, range).await?;
    let names = batch.names().await?;
    batch.prepare().await?;
    if !writes.is_empty() {
        batch.write(writes).await?;
    }
    batch.reanchor().await?;
    let targets = batch.repair().await?;
    batch.commit().await?;
    Ok((names, targets))
}

/// Non-shadow surfaces whose witness names no event: the scope's first invariant count.
const DANGLING_OUTSIDE_BYTE_SHADOWS: &str = "SELECT count(*) FROM name_surfaces surface
    WHERE surface.preimage_event_identity IS NOT NULL
      AND NOT (surface.raw_name = '' AND surface.visibility_state = 'shadow')
      AND NOT EXISTS (
          SELECT 1 FROM normalized_events event
          WHERE event.event_identity = surface.preimage_event_identity)";

/// Events whose own state is not readable: the second.
const UNREADABLE_EVENTS: &str = "SELECT count(*) FROM normalized_events
    WHERE canonicality_state NOT IN ('canonical', 'safe', 'finalized')";

async fn count(pool: &PgPool, statement: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar(statement).fetch_one(pool).await?)
}

type Raw = (Option<String>, Option<String>, String);

/// A surface's raw name, witness and visibility.
async fn raw(pool: &PgPool, name: &str) -> TestResult<Raw> {
    Ok(sqlx::query_as(
        "SELECT raw_name, preimage_event_identity, visibility_state
         FROM name_surfaces WHERE logical_name_id = $1",
    )
    .bind(id(name))
    .fetch_one(pool)
    .await?)
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| id(name)).collect()
}

fn list(names: &[&str]) -> Vec<String> {
    set(names).into_iter().collect()
}
