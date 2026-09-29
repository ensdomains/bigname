//! Plan-shape tests for the family loaders that look rows up by a list of uuid ids.
//!
//! The selective lookups (readable resources, wrapper rows, lease candidates) must compare the
//! uuid column itself with the ids, so the planner can probe the column's index; a
//! `column::text` comparison can only filter a scan of the whole chain. Their fixture is small,
//! so `enable_seqscan` is off to stand in for a large table: the assertions are about which
//! access paths the planner can use at all, not about costs. The name summary loaders that look
//! rows up by name id (lifecycle key states, authority starts, name migrations) are checked the
//! same way: each must probe an index on `(chain_id, logical_name_id)`, the key states' by-name
//! arm beside the primary key under a BitmapOr.
//!
//! The resource pointer lookup ORs a by-resource arm with a root-registry arm. Its test keeps
//! sequential scans enabled and pins, in the generic plan, a BitmapOr of the primary key and
//! the partial root-node index, with the ids reaching the plan as the bound uuid array rather
//! than a text array converted per row.

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, Row, raw_sql};
use uuid::Uuid;

use super::{
    control::{
        lifecycle::{AUTHORITY_STARTS_SQL, KEY_STATES_SQL, LEASE_CANDIDATES_SQL},
        wrapper::WRAPPER_ROWS_SQL,
    },
    name::{MIGRATIONS_SQL, RESOURCE_POINTERS_SQL, RESOURCES_SQL, canonical_uuid},
};

const CHAIN: &str = "ethereum-sepolia";
const OTHER_CHAIN: &str = "base-sepolia";
const ROWS: i64 = 2_000;
const POINTERS: i64 = 20_000;
const PLAN_MODES: [&str; 2] = ["force_generic_plan", "force_custom_plan"];

/// An index a plan must probe, and what its index condition must compare.
struct Probe {
    index: &'static str,
    conditions: &'static [&'static str],
}

#[tokio::test]
async fn family_id_lookups_probe_the_id_indexes() -> Result<()> {
    with_database("family_id_index_plan", async |connection| {
        install_id_fixture(connection).await?;
        check_id_lookups(connection).await
    })
    .await
}

#[tokio::test]
async fn resource_pointer_lookup_probes_both_indexes_with_bound_uuids() -> Result<()> {
    with_database("family_pointer_plan", async |connection| {
        install_pointer_fixture(connection).await?;
        check_pointer_lookup(connection).await
    })
    .await
}

async fn with_database(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_schema(&mut connection).await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn check_id_lookups(connection: &mut PgConnection) -> Result<()> {
    // Resource 3 has rows in every table, and name 3 also matches through its wrapped
    // registrar resource; name 6 matches only through its wrapped registrar resource; the last
    // id has no rows. The loaders bind ids as text, as read from uuid columns.
    let ids = [
        fixture_id(3),
        wrapped_id(3),
        wrapped_id(6),
        fixture_id(ROWS + 1),
    ]
    .map(|id| id.to_string());
    let ids_literal = format!("'{{{}}}'", ids.join(","));
    // The name summary loaders look names up by id: names 3, 4 and 5, and one with no rows.
    let names = ["ens:3", "ens:4", "ens:5", "ens:4000"].map(str::to_owned);
    let names_literal = format!("'{{{}}}'", names.join(","));
    let resource_probe = &["(resource_id = ANY "][..];
    let keyed_probe = &["(chain_id = ", "(resource_id = ANY "][..];
    let wrapped_probe = &["(chain_id = ", "(wrapped_registrar_resource_id = ANY "][..];
    let named_probe = &["(chain_id = ", "(logical_name_id = ANY "][..];
    // Each statement is prepared with the parameter types the loader binds.
    let statements = [
        (
            "resources",
            RESOURCES_SQL,
            "text[], text, bigint",
            format!("{ids_literal}, '{CHAIN}', {ROWS}"),
            vec![Probe {
                index: "resources_pkey",
                conditions: resource_probe,
            }],
        ),
        (
            "wrapper_rows",
            WRAPPER_ROWS_SQL,
            "text, text[]",
            format!("'{CHAIN}', {ids_literal}"),
            vec![Probe {
                index: "project_wrapper_state_pkey",
                conditions: keyed_probe,
            }],
        ),
        (
            // The two arms of the OR each probe their own index, under a BitmapOr.
            "lease_candidates",
            LEASE_CANDIDATES_SQL,
            "text, text[]",
            format!("'{CHAIN}', {ids_literal}"),
            vec![
                Probe {
                    index: "project_binding_candidate_resource_idx",
                    conditions: keyed_probe,
                },
                Probe {
                    index: "project_binding_candidate_wrapped_lease_idx",
                    conditions: wrapped_probe,
                },
            ],
        ),
        (
            // By name through the partial name index, by resource through the primary key.
            "key_states",
            KEY_STATES_SQL,
            "text, text[], text[]",
            format!("'{CHAIN}', {names_literal}, {ids_literal}"),
            vec![
                Probe {
                    index: "project_lifecycle_key_state_name_idx",
                    conditions: named_probe,
                },
                Probe {
                    index: "project_lifecycle_key_state_pkey",
                    conditions: keyed_probe,
                },
            ],
        ),
        (
            "authority_starts",
            AUTHORITY_STARTS_SQL,
            "text, text[]",
            format!("'{CHAIN}', {names_literal}"),
            vec![Probe {
                index: "project_name_state_name_idx",
                conditions: named_probe,
            }],
        ),
        (
            "name_migrations",
            MIGRATIONS_SQL,
            "text, text[]",
            format!("'{CHAIN}', {names_literal}"),
            vec![Probe {
                index: "project_name_state_name_idx",
                conditions: named_probe,
            }],
        ),
    ];
    let mut failures = Vec::new();
    for (label, sql, types, values, probes) in &statements {
        raw_sql(&format!("PREPARE {label} ({types}) AS {sql}"))
            .execute(&mut *connection)
            .await
            .with_context(|| format!("prepare {label}"))?;
        // A prepared statement may switch to the generic plan after its first calls, when that
        // plan is estimated no worse than planning each call; both must probe the index.
        for mode in PLAN_MODES {
            let plan = explain_execute(connection, mode, label, values).await?;
            let label = format!("{label} ({mode})");
            failures.extend(missing_probes(&label, &plan, probes));
            if probes.len() > 1 && !plan.iter().any(|line| line.contains("BitmapOr")) {
                failures.push(format!("{label}: no BitmapOr\n{}", plan.join("\n")));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "family id plans:\n{}",
        failures.join("\n\n")
    );

    // The statements still return the rows of exactly the ids asked for, under either plan.
    for mode in PLAN_MODES {
        raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await?;
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
            "{mode}: resources returned {resources:?}"
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
            "{mode}: wrapper rows returned {wrappers:?}"
        );
        // Name 3 matches through both arms and is returned once; name 6 matches through its
        // wrapped registrar resource only. Names without one (a null column) match only by
        // their own resource.
        let mut candidates: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT candidate ->> 'logical_name_id' FROM ({LEASE_CANDIDATES_SQL}) row (candidate)"
        ))
        .bind(CHAIN)
        .bind(&ids)
        .fetch_all(&mut *connection)
        .await?;
        candidates.sort();
        ensure!(
            candidates == ["ens:3", "ens:6"],
            "{mode}: lease candidates returned {candidates:?}"
        );
        // Names 3 and 5 by name (name 4 has a key state with no name), and resource 3 again
        // by id, returned once.
        let mut key_states: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT state ->> 'resource_id' FROM ({KEY_STATES_SQL}) row (state)"
        ))
        .bind(CHAIN)
        .bind(&names)
        .bind(&ids)
        .fetch_all(&mut *connection)
        .await?;
        key_states.sort();
        ensure!(
            key_states == [fixture_id(3).to_string(), fixture_id(5).to_string()],
            "{mode}: key states returned {key_states:?}"
        );
        let mut starts: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT logical_name_id FROM ({AUTHORITY_STARTS_SQL}) start"
        ))
        .bind(CHAIN)
        .bind(&names)
        .fetch_all(&mut *connection)
        .await?;
        starts.sort();
        ensure!(
            starts == ["ens:3", "ens:4", "ens:5"],
            "{mode}: authority starts returned {starts:?}"
        );
        // Only even names carry a MigrationApplied.
        let migrations: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT logical_name_id FROM ({MIGRATIONS_SQL}) migration"
        ))
        .bind(CHAIN)
        .bind(&names)
        .fetch_all(&mut *connection)
        .await?;
        ensure!(
            migrations == ["ens:4"],
            "{mode}: name migrations returned {migrations:?}"
        );
    }
    Ok(())
}

async fn check_pointer_lookup(connection: &mut PgConnection) -> Result<()> {
    // 500 odd resources, one root-registry resource asked for by id and by node, twenty ids
    // with no pointer, and resource 0xac in upper case, which never matched
    // `resource_id::text`.
    let mut ids: Vec<String> = (1..1_000)
        .step_by(2)
        .chain([1_000])
        .chain(POINTERS + 1..=POINTERS + 20)
        .map(|n| fixture_id(n).to_string())
        .collect();
    ids.push(fixture_id(0xac).to_string().to_uppercase());
    // Twenty root-registry nodes not asked for by id, the one asked for by both, fifteen
    // nodes of non-root pointers, ten nodes with no pointer, and five root nodes in another
    // namespace.
    let nodes: Vec<(String, String)> = (100..=2_100)
        .step_by(100)
        .filter(|n| *n != 1_000)
        .chain([1_000])
        .map(|n| ("ens".to_owned(), namehash(n)))
        .chain((2..=30).step_by(2).map(|n| ("ens".to_owned(), namehash(n))))
        .chain((POINTERS + 1..=POINTERS + 10).map(|n| ("ens".to_owned(), namehash(n))))
        .chain(
            (2_200..=2_600)
                .step_by(100)
                .map(|n| ("basenames".to_owned(), namehash(n))),
        )
        .collect();
    let (namespaces, namehashes): (Vec<String>, Vec<String>) = nodes.iter().cloned().unzip();
    let uuids: Vec<Uuid> = ids.iter().filter_map(|id| canonical_uuid(id)).collect();
    ensure!(uuids.len() == 521, "{} ids parsed", uuids.len());

    // The generic plan, prepared with the types the loader binds: the ids as uuid[], the
    // nodes as text[]. Sequential scans stay enabled. Each arm must probe its own index, and
    // the ids must reach them as the bound uuid array, with no conversion left in the plan.
    let text_list = |values: &[String]| format!("'{{{}}}'", values.join(","));
    raw_sql(&format!(
        "PREPARE resource_pointers (text, uuid[], text[], text[]) AS {RESOURCE_POINTERS_SQL}"
    ))
    .execute(&mut *connection)
    .await
    .context("prepare resource_pointers")?;
    let uuid_list = text_list(&uuids.iter().map(Uuid::to_string).collect::<Vec<_>>());
    let values = format!(
        "'{CHAIN}', {uuid_list}, {}, {}",
        text_list(&namespaces),
        text_list(&namehashes)
    );
    let plan = explain_execute(
        connection,
        "force_generic_plan",
        "resource_pointers",
        &values,
    )
    .await?;
    let mut failures = missing_probes(
        "resource pointers (force_generic_plan)",
        &plan,
        &[
            Probe {
                index: "project_resource_pointer_pkey",
                conditions: &["(chain_id = $1)", "(resource_id = ANY ($2))"],
            },
            Probe {
                index: "project_resource_pointer_root_node_idx",
                conditions: &[
                    "(chain_id = $1)",
                    "(namespace = ANY ($3))",
                    "(namehash = ANY ($4))",
                ],
            },
        ],
    );
    if !plan.iter().any(|line| line.contains("BitmapOr")) {
        failures.push("resource pointers: no BitmapOr".to_owned());
    }
    if plan
        .iter()
        .any(|line| line.contains("::uuid[]") || line.contains("(resource_id)::text"))
    {
        failures.push("resource pointers: the ids are converted in the plan".to_owned());
    }
    ensure!(
        failures.is_empty(),
        "{}\n{}",
        failures.join("\n"),
        plan.join("\n")
    );

    // The rows are the ones the statement returned before, with the text comparison and
    // without the index conditions, under either plan.
    let before_sql = RESOURCE_POINTERS_SQL
        .replace(
            "pointer.resource_id = ANY($2)",
            "pointer.resource_id::text = ANY($2::text[])",
        )
        .replace(
            "AND pointer.namespace = ANY($3::text[])
                AND pointer.namehash = ANY($4::text[])
                ",
            "",
        );
    ensure!(
        !before_sql.contains("namehash = ANY") && before_sql.contains("::text = ANY"),
        "the pointer predicate moved"
    );
    let rows_sql = |sql: &str| format!("SELECT to_jsonb(pointer) FROM ({sql}) pointer ORDER BY 1");
    for mode in PLAN_MODES {
        raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await?;
        let rows: Vec<Value> = sqlx::query_scalar(&rows_sql(RESOURCE_POINTERS_SQL))
            .bind(CHAIN)
            .bind(&uuids)
            .bind(&namespaces)
            .bind(&namehashes)
            .fetch_all(&mut *connection)
            .await?;
        let before: Vec<Value> = sqlx::query_scalar(&rows_sql(&before_sql))
            .bind(CHAIN)
            .bind(&ids)
            .bind(&namespaces)
            .bind(&namehashes)
            .fetch_all(&mut *connection)
            .await?;
        ensure!(
            rows == before,
            "{mode}: pointer rows differ from the statement before"
        );
        // 500 odd resources, twenty root nodes, and resource 1000 once.
        ensure!(rows.len() == 521, "{mode}: {} pointer rows", rows.len());
    }
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

/// The namehash of pointer `n` of the fixture.
fn namehash(n: i64) -> String {
    format!("0x{n:064x}")
}

async fn explain_execute(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    values: &str,
) -> Result<Vec<String>> {
    raw_sql(&format!(
        "SET plan_cache_mode = {mode}; EXPLAIN (COSTS OFF) EXECUTE {statement} ({values})"
    ))
    .fetch_all(&mut *connection)
    .await
    .with_context(|| format!("{statement} {mode}"))?
    .iter()
    .map(|row| row.try_get(0).map_err(Into::into))
    .collect()
}

/// Each probe's index must be scanned with an index condition holding all its conditions. In
/// the text plan the condition is the line right after the scan node's own line.
fn missing_probes(label: &str, plan: &[String], probes: &[Probe]) -> Vec<String> {
    probes
        .iter()
        .filter(|probe| {
            !plan.windows(2).any(|lines| {
                let names_index = lines[0].contains(&format!("using {} on", probe.index))
                    || lines[0].ends_with(&format!("on {}", probe.index));
                names_index
                    && lines[1].contains("Index Cond:")
                    && probe
                        .conditions
                        .iter()
                        .all(|condition| lines[1].contains(condition))
            })
        })
        .map(|probe| {
            format!(
                "{label}: no probe of {} on {:?}\n{}",
                probe.index,
                probe.conditions,
                plan.join("\n")
            )
        })
        .collect()
}

async fn install_schema(connection: &mut PgConnection) -> Result<()> {
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
    raw_sql("SET jit = off").execute(&mut *connection).await?;
    Ok(())
}

async fn install_id_fixture(connection: &mut PgConnection) -> Result<()> {
    // Resource n is uuid n; name n holds resource n, and on every third name also wrapped
    // registrar resource 2^64 + n. Resource n has a lifecycle key state, carrying name n
    // unless n is a multiple of four, and name n a name state, with a MigrationApplied position
    // when n is even.
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

         INSERT INTO project_lifecycle_key_state
             (chain_id, resource_id, logical_name_id, block_number, event_identity)
         SELECT '{CHAIN}', lpad(to_hex(n), 32, '0')::uuid,
                CASE WHEN n % 4 <> 0 THEN 'ens:' || n END, n, 'key-state:' || n
         FROM generate_series(1, {ROWS}) n;

         INSERT INTO project_name_state
             (namespace, logical_name_id, chain_id, block_number, event_identity,
              migration_position)
         SELECT 'ens', 'ens:' || n, '{CHAIN}', n, 'name-state:' || n,
                CASE WHEN n % 2 = 0
                     THEN jsonb_build_object('event_identity', 'migration:' || n) END
         FROM generate_series(1, {ROWS}) n;

         ANALYZE; SET enable_seqscan = off"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn install_pointer_fixture(connection: &mut PgConnection) -> Result<()> {
    // Pointer n is resource n with node n; every hundredth is a root-registry pointer. The first
    // hundred resources also have a pointer on another chain, which never matches.
    raw_sql(&format!(
        "INSERT INTO project_resource_pointer
             (chain_id, resource_id, block_number, event_identity, resolver_address,
              pointer_position, namespace, source_family, namehash)
         SELECT chain, lpad(to_hex(n), 32, '0')::uuid, n, 'pointer:' || n,
                '0x' || lpad(to_hex(n), 40, '0'),
                jsonb_build_object('event_identity', 'pointer:' || n, 'block_number', n),
                'ens',
                CASE WHEN n % 100 = 0 THEN 'ens_v2_root_l1' ELSE 'ens_v1_registry_l1' END,
                '0x' || lpad(to_hex(n), 64, '0')
         FROM generate_series(1, {POINTERS}) n
         CROSS JOIN LATERAL (VALUES ('{CHAIN}'), ('{OTHER_CHAIN}')) chains (chain)
         WHERE chain = '{CHAIN}' OR n <= 100;

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
