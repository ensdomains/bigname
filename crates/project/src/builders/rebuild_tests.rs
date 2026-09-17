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
        // The schema and the seed are committed first: one transaction that also created every
        // staging table would hold more locks than the server allows.
        let mut setup = database.pool().begin().await?;
        raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
            .execute(&mut *setup)
            .await?;
        for script in BASELINE {
            raw_sql(script).execute(&mut *setup).await?;
        }
        raw_sql(
            &SEED
                .replace("__NAMES__", &names.to_string())
                .replace("__CHAIN__", CHAIN),
        )
        .execute(&mut *setup)
        .await?;
        setup.commit().await?;
        let mut transaction = database.pool().begin().await?;
        raw_sql("SET LOCAL search_path TO bigname_phase, public")
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

    /// Rows only in `current` and rows only in `previous`, duplicates included.
    async fn row_differences(&mut self, current: &str, previous: &str) -> Result<(i64, i64)> {
        let (rows, extra, missing): (i64, i64, i64) = sqlx::query_as(&format!(
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
        Ok((extra, missing))
    }

    /// Both statements wrote the same rows: nothing is left of either side once the other is
    /// taken away.
    async fn assert_same_rows(&mut self, current: &str, previous: &str) -> Result<()> {
        let (extra, missing) = self.row_differences(current, previous).await?;
        ensure!(
            extra == 0 && missing == 0,
            "{current} differs from {previous}: {extra} extra, {missing} missing"
        );
        Ok(())
    }

    /// Keeps what the current statement wrote to `stage` as `current_rows`, empties the stage,
    /// and lets `previous` fill it again from the same staged inputs.
    async fn rerun_with(&mut self, stage: &str, previous: &str) -> Result<()> {
        raw_sql(&format!(
            "DROP TABLE IF EXISTS current_rows;
             CREATE TEMP TABLE current_rows AS TABLE {stage};
             TRUNCATE {stage}"
        ))
        .execute(&mut *self.transaction)
        .await?;
        self.execute(previous).await
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

/// Whether any node of the plan satisfies `matches`.
fn any_node(plan: &Value, matches: &dyn Fn(&Value) -> bool) -> bool {
    matches(plan)
        || plan["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|child| any_node(child, matches))
}

const AUTHORITY_EVENTS: &str = include_str!("name_authority/authority_events.sql");

/// The statement used to de-duplicate and sort its output by event id. Every event joins at most
/// one name, so both were no-ops that sorted each wide event row.
#[tokio::test]
async fn authority_events_match_the_deduplicated_and_sorted_statement() -> Result<()> {
    let mut rebuild =
        Rebuild::through("rebuild_equal_authority", 600, Builder::NameAuthority).await?;
    let previous = format!(
        "{}\nORDER BY event.normalized_event_id",
        AUTHORITY_EVENTS
            .replacen(
                "TABLE project_authority_events ",
                "TABLE previous_authority_events ",
                1
            )
            .replacen(
                "SELECT event.*",
                "SELECT DISTINCT ON (event.normalized_event_id) event.*",
                1
            )
    );
    ensure!(previous.contains("previous_authority_events") && previous.contains("DISTINCT ON"));
    rebuild.execute(&previous).await?;
    rebuild
        .assert_same_rows("project_authority_events", "previous_authority_events")
        .await?;
    rebuild.finish().await
}

#[tokio::test]
async fn authority_events_are_staged_without_sorting_them() -> Result<()> {
    let mut rebuild =
        Rebuild::through("rebuild_plan_events", PLAN_NAMES, Builder::NameAuthority).await?;
    let statement = AUTHORITY_EVENTS.replacen(
        "TABLE project_authority_events ",
        "TABLE explained_authority_events ",
        1,
    );
    let plan = rebuild.explain(&statement).await?;
    ensure!(
        !any_node(&plan, &|node| node["Node Type"] == "Sort"
            || node["Node Type"] == "Unique"),
        "the staged events were sorted or de-duplicated: {plan}"
    );
    let rows = rows_read(&plan, "project_events");
    ensure!(
        rows <= 40.0 * PLAN_NAMES as f64,
        "project_events rows handled: {rows}; {plan}"
    );
    rebuild.finish().await
}

const PREVIOUS_V2_LIFECYCLE_CTE: &str =
    include_str!("../../tests/rebuild_performance/previous_v2_lifecycle_cte.sql");

fn without_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect()
}

/// `name_current` is one statement of nearly nine hundred lines, too entangled to rewrite piece by
/// piece. Its `v2_lifecycle_events` CTE became a staged table built by the same SELECT, and the
/// rest of the statement is untouched. Putting the old CTE back in front (a CTE hides a table of
/// the same name) gives the statement as it was, and both must write the same rows.
#[tokio::test]
async fn name_current_matches_the_statement_with_the_lifecycle_cte() -> Result<()> {
    use super::name_current::query::{BUILD_NAME_CURRENT, STAGE_V2_LIFECYCLE_EVENTS};
    let key = |sql: &str| {
        let sql = without_whitespace(sql);
        let from = sql
            .find("COALESCE(event.resource_id::text")
            .expect("lifecycle key");
        sql[from..].trim_end_matches(')').to_owned()
    };
    assert_eq!(
        key(STAGE_V2_LIFECYCLE_EVENTS[0]),
        key(PREVIOUS_V2_LIFECYCLE_CTE),
        "the staged table must compute the lifecycle key over the same rows as the CTE did"
    );

    let mut rebuild = Rebuild::through("rebuild_equal_names", 600, Builder::NameCurrent).await?;
    let previous = format!(
        "{PREVIOUS_V2_LIFECYCLE_CTE}{}",
        BUILD_NAME_CURRENT.replace("project_v2_lifecycle_events", "v2_lifecycle_events")
    );
    rebuild
        .rerun_with("project_stage_name_current", &previous)
        .await?;
    rebuild
        .assert_same_rows("current_rows", "project_stage_name_current")
        .await?;
    let (v2_rows, released): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE declared_summary #>> '{registration,authority_kind}'
                                       = 'ens_v2_registry'),
                count(*) FILTER (WHERE declared_summary #>> '{registration,status}' = 'released')
         FROM current_rows",
    )
    .fetch_one(&mut *rebuild.transaction)
    .await?;
    ensure!(
        v2_rows > 0 && released > 0,
        "the seed must reach the ENSv2 lifecycle paths: {v2_rows} registered, {released} released"
    );
    rebuild.finish().await
}

/// Each name reads its own ENSv2 lifecycle rows, authority events and resource by key. Before,
/// every name scanned the whole lifecycle CTE several times and the whole resource stage once.
#[tokio::test]
async fn name_current_reads_each_name_by_key() -> Result<()> {
    let mut rebuild =
        Rebuild::through("rebuild_plan_names", PLAN_NAMES, Builder::NameCurrent).await?;
    let plan = rebuild
        .explain(super::name_current::query::BUILD_NAME_CURRENT)
        .await?;
    ensure!(
        !any_node(&plan, &|node| node["Node Type"] == "CTE Scan"),
        "name_current scans a CTE again: {plan}"
    );
    for relation in [
        "project_v2_lifecycle_events",
        "project_authority_events",
        "project_registration_events",
        "project_resources",
    ] {
        let rows = rows_read(&plan, relation);
        ensure!(
            rows <= 100.0 * PLAN_NAMES as f64,
            "{relation} rows handled: {rows}; {plan}"
        );
    }
    rebuild.finish().await
}

/// `sql` with the text from `from` through `through` swapped for `previous`.
fn with_previous(sql: &str, from: &str, through: &str, previous: &str) -> String {
    let start = sql.find(from).expect("start of the rewritten fragment");
    let end = start
        + sql[start..]
            .find(through)
            .expect("end of the rewritten fragment")
        + through.len();
    format!("{}{previous}{}", &sql[..start], &sql[end..])
}

/// The token holder used to be looked up again in `project_authority_events` by event id, which
/// no index served. It is the row already joined as `registration`.
#[tokio::test]
async fn address_names_match_the_statement_that_looked_the_transfer_up_again() -> Result<()> {
    use super::address_names::BUILD_ADDRESS_NAMES;
    let previous = with_previous(
        BUILD_ADDRESS_NAMES,
        "            -- The token holder is read from the registrant event",
        ") token_holder ON TRUE\n",
        include_str!("../../tests/rebuild_performance/previous_address_names_token_holder.sql"),
    );
    let mut rebuild =
        Rebuild::through("rebuild_equal_addresses", 600, Builder::AddressNames).await?;
    rebuild
        .rerun_with("project_stage_address_names_current", &previous)
        .await?;
    rebuild
        .assert_same_rows("current_rows", "project_stage_address_names_current")
        .await?;
    let transfers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_stage_name_current name
         JOIN project_authority_events event
           ON event.normalized_event_id = (name.provenance ->> 'registrant_event_id')::bigint
         WHERE event.event_kind = 'TokenControlTransferred'",
    )
    .fetch_one(&mut *rebuild.transaction)
    .await?;
    ensure!(
        transfers > 0,
        "the seed has no registrant that is a token transfer"
    );
    rebuild.finish().await
}

#[tokio::test]
async fn address_names_read_authority_events_once() -> Result<()> {
    let mut rebuild =
        Rebuild::through("rebuild_plan_addresses", PLAN_NAMES, Builder::AddressNames).await?;
    let plan = rebuild
        .explain(super::address_names::BUILD_ADDRESS_NAMES)
        .await?;
    let rows = rows_read(&plan, "project_authority_events");
    ensure!(
        rows <= 40.0 * PLAN_NAMES as f64,
        "project_authority_events rows handled: {rows}; {plan}"
    );
    rebuild.finish().await
}

/// `sql` with exactly one occurrence of `current` swapped for `previous`.
fn swapped(sql: &str, current: &str, previous: &str) -> String {
    assert_eq!(sql.matches(current).count(), 1, "rewritten text: {current}");
    sql.replacen(current, previous, 1)
}

/// `locked_roles` asked, per resource and role, whether an admin row exists for the resource or
/// its root. The `IN (resource, root)` test kept that from being a join, so every resource read
/// all staged permission rows. It is now two joins on the per-resource admin set.
#[tokio::test]
async fn resource_summary_matches_the_correlated_admin_lookup() -> Result<()> {
    let current = super::permissions::resource_summary::query();
    let previous = swapped(
        &swapped(
            &swapped(&current, "v2_admin_powers AS MATERIALIZED (", "v2_admin_powers AS ("),
            "        LEFT JOIN v2_admin_powers own_admins ON own_admins.resource_id = resource.resource_id
        LEFT JOIN v2_admin_powers root_admins
          ON root_admins.resource_id = root_resource.resource_id\n",
            "",
        ),
        "            WHERE NOT COALESCE(role.admin = ANY(own_admins.admins), false)
              AND NOT COALESCE(role.admin = ANY(root_admins.admins), false)\n",
        include_str!("../../tests/rebuild_performance/previous_resource_summary_locks.sql"),
    );
    let mut rebuild = Rebuild::through("rebuild_equal_summary", 600, Builder::Permissions).await?;
    let stage = "project_stage_permissions_current_resource_summary";
    rebuild.rerun_with(stage, &previous).await?;
    rebuild.assert_same_rows("current_rows", stage).await?;
    // Registrations with their own admin holder, with only the root's, and the root itself.
    let lock_sets: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT resource_restrictions -> 'locked_roles') FROM current_rows
         WHERE resource_restrictions ->> 'kind' = 'ens_v2_registry'",
    )
    .fetch_one(&mut *rebuild.transaction)
    .await?;
    ensure!(
        lock_sets >= 2,
        "the seed yields {lock_sets} distinct locked-role sets"
    );
    rebuild.finish().await
}

#[tokio::test]
async fn resource_summary_reads_the_staged_permissions_a_fixed_number_of_times() -> Result<()> {
    let mut rebuild =
        Rebuild::through("rebuild_plan_summary", PLAN_NAMES, Builder::Permissions).await?;
    let plan = rebuild
        .explain(&super::permissions::resource_summary::query())
        .await?;
    for relation in ["project_stage_permissions_current", "v2_admin_powers"] {
        let rows = rows_read(&plan, relation);
        ensure!(
            rows <= 20.0 * PLAN_NAMES as f64,
            "{relation} rows handled: {rows}; {plan}"
        );
    }
    rebuild.finish().await
}
