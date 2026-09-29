//! Plan-shape tests for the family loaders that look rows up by a list of uuid ids. Each
//! statement must compare the uuid column itself with the ids, so the planner can probe the
//! column's index; a `column::text` comparison can only filter a scan of the whole chain. The
//! fixture is small, so `enable_seqscan` is off to stand in for a large table: the assertions
//! are about which access paths the planner can use at all, not about costs.

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgConnection, Row, raw_sql};
use uuid::Uuid;

use super::{
    control::{lifecycle::LEASE_CANDIDATES_SQL, wrapper::WRAPPER_ROWS_SQL},
    name::RESOURCES_SQL,
};

const CHAIN: &str = "ethereum-sepolia";
const ROWS: i64 = 2_000;

#[tokio::test]
async fn family_id_lookups_probe_the_id_indexes() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("family_id_index_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_fixture(&mut connection).await?;
        check_plans(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn check_plans(connection: &mut PgConnection) -> Result<()> {
    // One id with rows in every table, one id that is only a wrapped registrar resource, one
    // id with no rows at all. The loaders bind ids as text, as read from uuid columns.
    let ids = [fixture_id(3), wrapped_id(3), fixture_id(ROWS + 1)].map(|id| id.to_string());
    let ids_literal = format!("'{{{}}}'", ids.join(","));
    // The two arms of the lease candidates' OR each probe their own index, under a BitmapOr.
    let both = ["resource_id", "wrapped_registrar_resource_id"];
    // Each statement is prepared with the parameter types the loader binds.
    let statements = [
        (
            "resources",
            RESOURCES_SQL,
            "text[], text, bigint",
            format!("{ids_literal}, '{CHAIN}', {ROWS}"),
            &["resource_id"][..],
        ),
        (
            "wrapper_rows",
            WRAPPER_ROWS_SQL,
            "text, text[]",
            format!("'{CHAIN}', {ids_literal}"),
            &["resource_id"][..],
        ),
        (
            "lease_candidates",
            LEASE_CANDIDATES_SQL,
            "text, text[]",
            format!("'{CHAIN}', {ids_literal}"),
            &both[..],
        ),
    ];
    let mut failures = Vec::new();
    for (label, sql, types, values, columns) in &statements {
        raw_sql(&format!("PREPARE {label} ({types}) AS {sql}"))
            .execute(&mut *connection)
            .await
            .with_context(|| format!("prepare {label}"))?;
        // The generic plan is the one a prepared statement settles on after its first calls;
        // the custom plan is the one planned for the bound values.
        for mode in ["force_generic_plan", "force_custom_plan"] {
            let plan = raw_sql(&format!(
                "SET plan_cache_mode = {mode}; EXPLAIN (COSTS OFF) EXECUTE {label} ({values})"
            ))
            .fetch_all(&mut *connection)
            .await
            .with_context(|| format!("{label} {mode}"))?
            .iter()
            .map(|row| row.try_get(0))
            .collect::<Result<Vec<String>, _>>()?;
            failures.extend(missing_probes(&format!("{label} ({mode})"), &plan, columns));
        }
    }
    ensure!(
        failures.is_empty(),
        "family id plans:\n{}",
        failures.join("\n\n")
    );

    // The statements still return the rows of exactly the ids asked for.
    let resources: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT resource_id FROM ({RESOURCES_SQL}) resource"
    ))
    .bind(&ids)
    .bind(CHAIN)
    .bind(ROWS)
    .fetch_all(&mut *connection)
    .await?;
    ensure!(
        resources == [fixture_id(3).to_string()],
        "resources returned {resources:?}"
    );
    let wrappers: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT wrapper ->> 'resource_id' FROM ({WRAPPER_ROWS_SQL}) row (wrapper)"
    ))
    .bind(CHAIN)
    .bind(&ids)
    .fetch_all(&mut *connection)
    .await?;
    ensure!(
        wrappers == [fixture_id(3).to_string()],
        "wrapper rows returned {wrappers:?}"
    );
    // Name 3 holds resource 3 and wrapped registrar resource 2^64 + 3; it matches once.
    let candidates: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT candidate ->> 'logical_name_id' FROM ({LEASE_CANDIDATES_SQL}) row (candidate)"
    ))
    .bind(CHAIN)
    .bind(&ids)
    .fetch_all(&mut *connection)
    .await?;
    ensure!(
        candidates == ["ens:3"],
        "lease candidates returned {candidates:?}"
    );
    Ok(())
}

/// Resource `n` of the fixture.
fn fixture_id(n: i64) -> Uuid {
    Uuid::from_u128(n as u128)
}

/// The wrapped registrar resource of name `n`, on names divisible by three.
fn wrapped_id(n: i64) -> Uuid {
    Uuid::from_u128((1_u128 << 64) | n as u128)
}

/// Each of `columns` must be an index condition somewhere in `plan`, compared with the ids
/// as a uuid.
fn missing_probes(label: &str, plan: &[String], columns: &[&str]) -> Option<String> {
    let missing: Vec<&str> = columns
        .iter()
        .copied()
        .filter(|column| {
            !plan.iter().any(|line| {
                line.contains("Index Cond:") && line.contains(&format!("({column} = ANY "))
            })
        })
        .collect();
    (!missing.is_empty()).then(|| {
        format!(
            "{label}: no index probe on {missing:?}\n{}",
            plan.join("\n")
        )
    })
}

async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
    ] {
        raw_sql(baseline).execute(&mut *connection).await?;
    }
    // Resource n is uuid n; name n holds resource n, and on every third name also wrapped
    // registrar resource 2^64 + n.
    raw_sql(&format!(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT '{CHAIN}', 'block-' || n, n, to_timestamp(n), 'canonical'
         FROM generate_series(1, {ROWS}) n;

         INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT lpad(to_hex(n), 32, '0')::uuid, '{CHAIN}', 'block-' || n, n, 'canonical'
         FROM generate_series(1, {ROWS}) n;

         INSERT INTO project_wrapper_state (chain_id, resource_id, block_number, event_identity)
         SELECT '{CHAIN}', lpad(to_hex(n), 32, '0')::uuid, n, 'wrapper:' || n
         FROM generate_series(1, {ROWS}) n;

         INSERT INTO project_binding_candidate
             (surface_binding_id, logical_name_id, namespace, chain_id, authority_arm,
              resource_id, binding_kind, canonicality_state, block_number, event_identity,
              wrapped_registrar_resource_id)
         SELECT lpad(to_hex(n), 32, '0')::uuid, 'ens:' || n, 'ens', '{CHAIN}', 'ens_v1',
                lpad(to_hex(n), 32, '0')::uuid, 'registration', 'canonical', n,
                'candidate:' || n,
                CASE WHEN n % 3 = 0
                     THEN ('0000000000000001' || lpad(to_hex(n), 16, '0'))::uuid END
         FROM generate_series(1, {ROWS}) n;

         ANALYZE; SET enable_seqscan = off; SET jit = off"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
