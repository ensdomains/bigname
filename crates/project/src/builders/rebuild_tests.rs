//! Full-rebuild statements, checked on the seeded database of `tests/rebuild_performance/`.
//!
//! Two kinds of test. The equality tests run a rewritten statement and the statement it replaced
//! against the same staged tables and compare the rows both ways. The plan tests run a statement
//! under `EXPLAIN ANALYZE` on enough names that a per-name scan of a whole table would show up,
//! and assert the table is read a bounded number of times.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{Postgres, Transaction, raw_sql};

use crate::{Marker, scope, stage};

const CHAIN: &str = "ethereum-sepolia";
const TARGET_BLOCK: i64 = 300;
const SEED: &str = include_str!("../../tests/rebuild_performance/seed.sql");
const BASELINE: &[&str] = &[
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
];
/// Enough names that reading a staged table once per name costs visibly more than a keyed lookup.
const PLAN_NAMES: i64 = 3_000;

/// The builders in the order `build_all` runs them.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum Builder {
    Staged,
    NameAuthority,
    Permissions,
    NameCurrent,
    RecordInventory,
    AddressNames,
    PrimaryNames,
}

struct Rebuild {
    database: TestDatabase,
    transaction: Transaction<'static, Postgres>,
    target: Marker,
}

impl Rebuild {
    /// Seeds `names` names and runs a from-zero Project pass up to and including `last`.
    async fn through(prefix: &str, names: i64, last: Builder) -> Result<Self> {
        let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
        let mut transaction = database.pool().begin().await?;
        raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
            .execute(&mut *transaction)
            .await?;
        for script in BASELINE {
            raw_sql(script).execute(&mut *transaction).await?;
        }
        raw_sql(
            &SEED
                .replace("__NAMES__", &names.to_string())
                .replace("__CHAIN__", CHAIN),
        )
        .execute(&mut *transaction)
        .await?;
        let target = Marker {
            number: TARGET_BLOCK,
            hash: format!("0x{TARGET_BLOCK:064x}"),
        };
        let tx = &mut transaction;
        stage::prepare(tx, CHAIN, &target).await?;
        let window = scope::Window {
            previous: None,
            from_block: 1,
            to_block: TARGET_BLOCK,
            full_rebuild: true,
            retain_retracted: false,
        };
        scope::initialize(tx, CHAIN, &target, window).await?;
        stage::inputs(tx, CHAIN, &target, true).await?;
        // Keep this in step with `build_all`.
        if last >= Builder::NameAuthority {
            super::name_authority::build(tx, CHAIN, &target).await?;
        }
        if last >= Builder::Permissions {
            super::account_permissions::build(tx, CHAIN, &target).await?;
            super::permissions::build(tx, CHAIN, &target, true).await?;
        }
        if last >= Builder::NameCurrent {
            super::name_current::build(tx, CHAIN, &target).await?;
        }
        if last >= Builder::RecordInventory {
            super::permission_resources::build_registry_binding(tx).await?;
            super::resolver::build(tx, CHAIN, &target, true).await?;
            super::linked_records::build(tx).await?;
            super::record_inventory::build(tx, CHAIN, &target).await?;
        }
        if last >= Builder::AddressNames {
            super::name_topology::build(tx, CHAIN, &target).await?;
            super::children::build(tx, CHAIN, &target).await?;
            super::address_names::build(tx, CHAIN, &target).await?;
        }
        if last >= Builder::PrimaryNames {
            super::address_records::build(tx, CHAIN, &target).await?;
            super::primary_names::build(tx, CHAIN, &target).await?;
        }
        Ok(Self {
            database,
            transaction,
            target,
        })
    }

    /// Runs a statement that takes the chain and target as `$1`, `$2`, `$3` (and the full-rebuild
    /// flag as `$4` when it has one).
    async fn execute(&mut self, statement: &str) -> Result<()> {
        let mut query = sqlx::query(statement);
        if statement.contains("$1") {
            query = query
                .bind(CHAIN)
                .bind(self.target.number)
                .bind(&self.target.hash);
        }
        if statement.contains("$4") {
            query = query.bind(true);
        }
        query.execute(&mut *self.transaction).await?;
        Ok(())
    }

    async fn explain(&mut self, statement: &str) -> Result<Value> {
        let explain = format!("EXPLAIN (ANALYZE, FORMAT JSON) {statement}");
        let mut query = sqlx::query_scalar::<_, Value>(&explain);
        if statement.contains("$1") {
            query = query
                .bind(CHAIN)
                .bind(self.target.number)
                .bind(&self.target.hash);
        }
        if statement.contains("$4") {
            query = query.bind(true);
        }
        Ok(query.fetch_one(&mut *self.transaction).await?[0]["Plan"].take())
    }

    /// Both statements wrote the same rows: nothing is left of either side once the other is
    /// taken away, duplicates included.
    async fn assert_same_rows(&mut self, current: &str, previous: &str) -> Result<()> {
        let (rows, only_current, only_previous): (i64, i64, i64) = sqlx::query_as(&format!(
            "SELECT (SELECT count(*) FROM {current}),
                    (SELECT count(*) FROM (TABLE {current} EXCEPT ALL TABLE {previous}) extra),
                    (SELECT count(*) FROM (TABLE {previous} EXCEPT ALL TABLE {current}) missing)"
        ))
        .fetch_one(&mut *self.transaction)
        .await?;
        ensure!(
            rows > 0,
            "{current} is empty, so the comparison proves nothing"
        );
        ensure!(
            only_current == 0 && only_previous == 0,
            "{current} differs from {previous}: {only_current} extra, {only_previous} missing"
        );
        Ok(())
    }

    async fn finish(self) -> Result<()> {
        self.transaction.rollback().await?;
        self.database.cleanup().await?;
        Ok(())
    }
}

/// How many rows of `relation` (a table, or a CTE of that name) the plan handled, counting every
/// repeat: the largest `rows x loops` over the scans of the relation and over the hash, sort,
/// aggregate or materialize nodes stacked directly on such a scan. A keyed lookup per name stays
/// near the number of names; reading or re-reading the whole relation per name grows with names
/// times rows.
fn rows_read(plan: &Value, relation: &str) -> f64 {
    fn visit(node: &Value, relation: &str) -> (f64, bool) {
        let number = |key: &str| node[key].as_f64().unwrap_or(0.0);
        let handled = (number("Actual Rows")
            + number("Rows Removed by Filter")
            + number("Rows Removed by Join Filter"))
            * number("Actual Loops");
        let children = node["Plans"].as_array().map(Vec::as_slice).unwrap_or(&[]);
        let scans_relation = children.is_empty()
            && (node["Relation Name"] == relation || node["CTE Name"] == relation);
        let visited: Vec<_> = children
            .iter()
            .map(|child| visit(child, relation))
            .collect();
        let below = visited.iter().map(|(rows, _)| *rows).fold(0.0, f64::max);
        let only_relation = scans_relation || matches!(visited.as_slice(), [(_, true)]);
        if only_relation {
            (below.max(handled), true)
        } else {
            (below, false)
        }
    }
    visit(plan, relation).0
}

/// Selecting each name's binding checks that the binding's resource is staged. That check runs
/// once per candidate binding, so it has to be a key lookup, not a read of every resource.
#[tokio::test]
async fn name_authority_looks_resources_up_by_key() -> Result<()> {
    let mut rebuild =
        Rebuild::through("rebuild_plan_authority", PLAN_NAMES, Builder::NameAuthority).await?;
    let statement = include_str!("name_authority/build.sql").replace(
        "TABLE project_name_authority ",
        "TABLE explained_name_authority ",
    );
    let plan = rebuild.explain(&statement).await?;
    let rows = rows_read(&plan, "project_resources");
    ensure!(
        rows <= 20.0 * PLAN_NAMES as f64,
        "project_resources rows handled: {rows}; {plan}"
    );
    rebuild.finish().await
}
