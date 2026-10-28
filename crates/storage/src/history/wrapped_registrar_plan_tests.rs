//! Plan and row tests for the registrar-grant lease lookup of name history
//! (`push_registrar_grant_resources_query`).
//!
//! The node probe index's key is an expression and the index is partial. The planner never reads a
//! partial index's expression statistics, so it estimates the probe at the default equality
//! selectivity whatever the node and whatever `ANALYZE` collected. The
//! previous statement joined that expression to the surface's, and against the estimate the
//! planner ANDed the probe with the event-kind index over every `RegistrationGranted` row of the
//! chain. The fixture reproduces that: unrelated ENSv1 rows interleaved as on a live chain, 3.2%
//! of them grants. The plan test checks that the previous statement still takes the `BitmapAnd`
//! here with statistics off, so the fixture keeps proving something, and that the fenced
//! statement probes only the node index in every cell. Sequential and bitmap scans stay enabled:
//! the shape is chosen on cost. The row test pins every input class to its rows, in order, and
//! to the previous statement's rows.

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row, postgres::PgRow, raw_sql};
use uuid::Uuid;

use super::wrapped_registrar::push_registrar_grant_resources_query;

const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];
const PROBE_INDEX: &str = "normalized_events_v1_direct_node_probe_idx";
const MAINNET: &str = "ethereum-mainnet";
const SEPOLIA: &str = "ethereum-sepolia";

/// The fixture's names, by the number their namehash encodes.
const NO_GRANT: u32 = 0xa0;
const ONE_GRANT: u32 = 0xa1;
const ROLLED_OVER: u32 = 0xa2;
const WRAPPED: u32 = 0xa3;
const SEPOLIA_NAME: u32 = 0xa5;
const LEASE_REORG: u32 = 0xa6;
const SURFACE_REORG: u32 = 0xa7;
const FILTER_EDGES: u32 = 0xa8;
const HOT_NODE: u32 = 0xa9;
const NO_SURFACE: u32 = 0xee;

/// An expected row: chain, block and the lease resource as a number.
type GrantRow = (&'static str, i64, u128);

/// The previous statement, verbatim with `$1`, split where `canonical_only` adds its filters.
const BEFORE_HEAD: &str = r#"
        SELECT DISTINCT
            lease.chain_id,
            lease.block_number,
            lease.resource_id AS registrar_resource_id
        FROM bigname_phase.name_surfaces surface
        LEFT JOIN bigname_phase.chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
        JOIN bigname_phase.normalized_events lease
          ON lease.chain_id = surface.chain_id
         AND COALESCE(
                 lease.namespace || ':' || lower(COALESCE(
                     lease.after_state ->> 'child_node',
                     lease.after_state ->> 'namehash',
                     lease.after_state ->> 'node',
                     lease.after_state #>> '{grant_source,node}',
                     lease.after_state #>> '{revocation_source,node}'
                 )),
                 lease.logical_name_id
             ) = surface.namespace || ':' || lower(surface.namehash)
        LEFT JOIN bigname_phase.chain_lineage lease_lineage
          ON lease_lineage.chain_id = lease.chain_id
         AND lease_lineage.block_hash = lease.block_hash
        WHERE surface.logical_name_id = $1
          AND lease.resource_id IS NOT NULL
          AND lease.source_family LIKE 'ens\_v1\_%'
          AND lease.source_family = 'ens_v1_registrar_l1'
          AND lease.event_kind = 'RegistrationGranted'
          AND lease.consumer_visibility = 'activated'
          AND lower(lease.after_state ->> 'namehash') = lower(surface.namehash)
          AND COALESCE(NULLIF(lease.after_state ->> 'authority_kind', ''), 'registrar')
              = 'registrar'
        "#;
const BEFORE_CANONICAL: &str = concat!(
    r#"
        AND surface.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
        AND (
            surface.block_hash IS NULL
            OR surface_lineage.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
            )
        )
        "#,
    r#"
        AND lease.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
        AND (
            lease.block_hash IS NULL
            OR lease_lineage.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
            )
        )
        "#,
);

#[tokio::test]
async fn registrar_grants_probe_only_the_node_index() -> Result<()> {
    with_fixture("registrar_grant_plan", async |connection| {
        let mut failures = Vec::new();
        for statistics in [Statistics::Off, Statistics::On] {
            statistics.apply(connection).await?;
            prepare(connection, "grants", &grants_sql(true)).await?;
            prepare(connection, "grants_before", &before_sql(true)).await?;
            for mode in PLAN_MODES {
                for (label, id) in [
                    ("cold node", name(ONE_GRANT)),
                    ("hot node", name(HOT_NODE)),
                    ("no surface", name(NO_SURFACE)),
                ] {
                    let cell = format!("{} statistics, {mode}, {label}", statistics.label());
                    let plan = explain(connection, mode, "grants", &id).await?;
                    failures.extend(probe_failures(&cell, &plan));
                    let before = explain(connection, mode, "grants_before", &id).await?;
                    println!("{cell}, previous statement:\n{}\n", before.join("\n"));
                    if statistics == Statistics::Off
                        && (probe_failures(&cell, &before).is_empty()
                            || !before.iter().any(|line| line.contains("BitmapAnd"))
                            || !before
                                .iter()
                                .any(|line| line.contains("normalized_events_projection_idx")))
                    {
                        failures.push(format!(
                            "{cell}: the previous statement no longer ANDs the event-kind index, \
                             so this fixture proves nothing about it\n{}",
                            before.join("\n")
                        ));
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "registrar grant plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn registrar_grants_rows_and_oracle_parity() -> Result<()> {
    with_fixture("registrar_grant_rows", async |connection| {
        let hot_node = (1..=100_u16)
            .map(|n| (MAINNET, 200 + i64::from(n), 0x9000 + u128::from(n)))
            .collect();
        let cases: [(&str, u32, Vec<GrantRow>); 10] = [
            ("no grant", NO_GRANT, vec![]),
            ("one grant", ONE_GRANT, vec![(MAINNET, 100, 0xa10)]),
            (
                "lease rolled over",
                ROLLED_OVER,
                vec![
                    (MAINNET, 100, 0xa2a),
                    (MAINNET, 200, 0xa2b),
                    (MAINNET, 300, 0xa2b),
                ],
            ),
            ("wrapped", WRAPPED, vec![(MAINNET, 100, 0xa30)]),
            ("sepolia", SEPOLIA_NAME, vec![(SEPOLIA, 100, 0xa50)]),
            ("lease reorg", LEASE_REORG, vec![(MAINNET, 100, 0xa60)]),
            ("surface reorg", SURFACE_REORG, vec![]),
            (
                "filter edges",
                FILTER_EDGES,
                vec![
                    (MAINNET, 130, 0xa80),
                    (MAINNET, 130, 0xa81),
                    (MAINNET, 130, 0xa82),
                ],
            ),
            ("hot node", HOT_NODE, hot_node),
            ("no surface", NO_SURFACE, vec![]),
        ];
        // Without `canonical_only`, which no reader passes, rows on orphaned blocks stay in.
        let all_rows = [ONE_GRANT, LEASE_REORG, SURFACE_REORG];
        let mut failures = Vec::new();
        for statistics in [Statistics::Off, Statistics::On] {
            statistics.apply(connection).await?;
            prepare(connection, "grants", &grants_sql(true)).await?;
            prepare(connection, "grants_before", &before_sql(true)).await?;
            prepare(connection, "all_grants", &grants_sql(false)).await?;
            prepare(connection, "all_grants_before", &before_sql(false)).await?;
            for mode in PLAN_MODES {
                let cell = format!("{} statistics, {mode}", statistics.label());
                for (label, k, expected) in &cases {
                    let id = name(*k);
                    let expected = expected
                        .iter()
                        .map(|(chain, block, resource)| {
                            ((*chain).to_owned(), *block, Uuid::from_u128(*resource))
                        })
                        .collect::<Vec<_>>();
                    let rows = execute(connection, mode, "grants", &id).await?;
                    let before = execute(connection, mode, "grants_before", &id).await?;
                    if rows != expected || rows != before {
                        failures.push(format!(
                            "{cell}, {label}: rows {rows:?}\nexpected {expected:?}\nprevious \
                             {before:?}"
                        ));
                    }
                }
                for k in all_rows {
                    let id = name(k);
                    let rows = execute(connection, mode, "all_grants", &id).await?;
                    let before = execute(connection, mode, "all_grants_before", &id).await?;
                    if rows != before || rows.is_empty() {
                        failures.push(format!(
                            "{cell}, {id} without canonical_only: rows {rows:?}\nprevious \
                             {before:?}"
                        ));
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "registrar grant rows:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Statistics {
    /// No statistics on the probe expression. The planner ignores them on a partial index either
    /// way, so both variants should plan alike. The pair guards against that ever changing.
    Off,
    On,
}

impl Statistics {
    fn label(self) -> &'static str {
        match self {
            Self::Off => "no expression",
            Self::On => "expression",
        }
    }

    async fn apply(self, connection: &mut PgConnection) -> Result<()> {
        let target = match self {
            Self::Off => 0,
            Self::On => -1,
        };
        raw_sql(&format!(
            "DEALLOCATE ALL;
             ALTER INDEX {PROBE_INDEX} ALTER COLUMN 2 SET STATISTICS {target};
             ANALYZE normalized_events, name_surfaces, chain_lineage;"
        ))
        .execute(&mut *connection)
        .await?;
        Ok(())
    }
}

fn name(k: u32) -> String {
    format!("ens:0x{k:064x}")
}

fn grants_sql(canonical_only: bool) -> String {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_registrar_grant_resources_query(&mut builder, "", canonical_only);
    builder.sql().to_owned()
}

fn before_sql(canonical_only: bool) -> String {
    let canonical = if canonical_only { BEFORE_CANONICAL } else { "" };
    format!("{BEFORE_HEAD}{canonical} ORDER BY 3, 1, 2")
}

async fn prepare(connection: &mut PgConnection, statement: &str, sql: &str) -> Result<()> {
    raw_sql(&format!("PREPARE {statement} (text) AS {sql}"))
        .execute(&mut *connection)
        .await
        .with_context(|| format!("prepare {statement}"))?;
    Ok(())
}

async fn explain(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    id: &str,
) -> Result<Vec<String>> {
    run(
        connection,
        mode,
        &format!("EXPLAIN (COSTS OFF) EXECUTE {statement}('{id}')"),
    )
    .await?
    .iter()
    .map(|row| row.try_get(0).map_err(Into::into))
    .collect()
}

async fn execute(
    connection: &mut PgConnection,
    mode: &str,
    statement: &str,
    id: &str,
) -> Result<Vec<(String, i64, Uuid)>> {
    run(connection, mode, &format!("EXECUTE {statement}('{id}')"))
        .await?
        .iter()
        .map(|row| -> Result<_> { Ok((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?)) })
        .collect()
}

async fn run(connection: &mut PgConnection, mode: &str, sql: &str) -> Result<Vec<PgRow>> {
    raw_sql(&format!("SET plan_cache_mode = {mode}; {sql}"))
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("{sql} under {mode}"))
}

/// The lease rows must come from one scan of `normalized_events`, through the probe index alone,
/// keyed by the surface's chain and node, and the surface by its primary key.
fn probe_failures(cell: &str, plan: &[String]) -> Vec<String> {
    let mut failures = Vec::new();
    let table_scans = plan
        .iter()
        .filter(|line| line.contains(" on normalized_events "))
        .count();
    if table_scans != 1 {
        failures.push(format!("{table_scans} scans of normalized_events"));
    }
    for line in plan {
        if line.contains("BitmapAnd") || line.contains("BitmapOr") {
            failures.push(format!("a combined bitmap: {}", line.trim()));
        }
        if line.contains("Seq Scan on normalized_events ") {
            failures.push(format!("a sequential scan: {}", line.trim()));
        }
        let other_index = line
            .match_indices("normalized_events_")
            .map(|(at, _)| {
                line[at..]
                    .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .next()
                    .unwrap_or_default()
            })
            .find(|index| *index != PROBE_INDEX);
        if let Some(index) = other_index {
            failures.push(format!("another normalized_events index: {index}"));
        }
    }
    let probed = plan.windows(2).any(|lines| {
        (lines[0].contains(&format!(
            "Index Scan using {PROBE_INDEX} on normalized_events "
        )) || lines[0].ends_with(&format!("Bitmap Index Scan on {PROBE_INDEX}")))
            && lines[1].contains("Index Cond:")
            && lines[1].contains("(chain_id = surface.chain_id)")
            && lines[1].contains("surface.namespace")
            && lines[1].contains("lower(surface.namehash)")
    });
    if !probed {
        failures.push(format!(
            "no probe of {PROBE_INDEX} on the surface's chain and node"
        ));
    }
    let surface_by_key = plan.windows(2).any(|lines| {
        lines[0].contains("Index Scan using name_surfaces_pkey on name_surfaces surface")
            && lines[1].contains("Index Cond: (logical_name_id = ")
    });
    if !surface_by_key {
        failures.push("the surface is not read by its primary key".to_owned());
    }
    if failures.is_empty() {
        return failures;
    }
    vec![format!(
        "{cell}: {}\n{}",
        failures.join("; "),
        plan.join("\n")
    )]
}

async fn with_fixture(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_schema(&mut connection).await?;
        raw_sql(FIXTURE_SQL)
            .execute(&mut *connection)
            .await
            .context("install the registrar grant fixture")?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn install_schema(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
        include_str!("../../schema/baseline/06_projections.sql"),
    ] {
        raw_sql(baseline).execute(&mut *connection).await?;
    }
    raw_sql("SET jit = off").execute(&mut *connection).await?;
    Ok(())
}

/// Lineage for blocks 1..=400 on two chains, each with one orphaned block hash at 120. Name k
/// has namehash k as 32 bytes of hex and resource k is uuid k. Autovacuum is off so the
/// statistics are only the ones a test collects.
const FIXTURE_SQL: &str = r#"
ALTER TABLE normalized_events SET (autovacuum_enabled = false);
CREATE FUNCTION pg_temp.node(k bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT '0x' || lpad(to_hex(k), 64, '0') $$;
CREATE FUNCTION pg_temp.rid(k bigint) RETURNS uuid LANGUAGE sql IMMUTABLE
    AS $$ SELECT lpad(to_hex(k), 32, '0')::uuid $$;
CREATE FUNCTION pg_temp.hash(chain text, block bigint) RETURNS text LANGUAGE sql IMMUTABLE
    AS $$ SELECT chain || ':' || block $$;
CREATE FUNCTION pg_temp.ev(
    identity text, chain text, block bigint, family text, kind text, resource uuid,
    state jsonb, canon canonicality_state DEFAULT 'canonical',
    visibility text DEFAULT 'activated', hash text DEFAULT NULL, name text DEFAULT NULL
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
    SELECT resource, chain, pg_temp.hash(chain, 1), 1, 'canonical'
    WHERE resource IS NOT NULL
    ON CONFLICT DO NOTHING;
    INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
        event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
        derivation_kind, canonicality_state, consumer_visibility, migration_correlation_ids,
        after_state)
    VALUES (identity, 'ens', name, resource, kind, family, 1, chain, block,
        COALESCE(hash, pg_temp.hash(chain, block)), 'ens_v1_unwrapped_authority', canon,
        visibility,
        CASE visibility WHEN 'candidate' THEN ARRAY['fixture'] ELSE ARRAY[]::text[] END,
        state);
$$;

INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
SELECT chain, pg_temp.hash(chain, n), n, to_timestamp(n), 'canonical'::canonicality_state
FROM unnest(ARRAY['ethereum-mainnet', 'ethereum-sepolia']) chain, generate_series(1, 400) n
UNION ALL
SELECT chain, chain || ':orphan', 120, to_timestamp(120), 'orphaned'
FROM unnest(ARRAY['ethereum-mainnet', 'ethereum-sepolia']) chain;

INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
    visibility_state, chain_id, block_hash, block_number, canonicality_state)
SELECT 'ens:' || pg_temp.node(k), 'ens', pg_temp.node(k), ARRAY[pg_temp.node(k)], 'fixture',
    'active', chain, hash, block, 'canonical'
FROM (VALUES
    (160, 'ethereum-mainnet', 'ethereum-mainnet:1', 1),
    (161, 'ethereum-mainnet', 'ethereum-mainnet:1', 1),
    (162, 'ethereum-mainnet', 'ethereum-mainnet:1', 1),
    (163, 'ethereum-mainnet', 'ethereum-mainnet:1', 1),
    (165, 'ethereum-sepolia', 'ethereum-sepolia:1', 1),
    (166, 'ethereum-mainnet', 'ethereum-mainnet:1', 1),
    (167, 'ethereum-mainnet', 'ethereum-mainnet:orphan', 120),
    (168, 'ethereum-mainnet', 'ethereum-mainnet:1', 1),
    (169, 'ethereum-mainnet', 'ethereum-mainnet:1', 1)
) surface(k, chain, hash, block);

-- Unrelated names and ENSv1 rows, interleaved as on a live chain: 5,000 nodes with 25 rows
-- each. Of every 125 rows, 4 are registrar grants (3.2%), 12 are other rows of the granted
-- leases, and the rest are registry and resolver rows of the nodes' own resources.
INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes, normalizer_version,
    visibility_state, chain_id, block_hash, block_number, canonicality_state)
SELECT 'ens:' || pg_temp.node(1048576 + n), 'ens', pg_temp.node(1048576 + n),
    ARRAY[pg_temp.node(1048576 + n)], 'fixture', 'active', 'ethereum-mainnet',
    'ethereum-mainnet:1', 1, 'canonical'
FROM generate_series(0, 1999) n;
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(16777216 + n), 'ethereum-mainnet', 'ethereum-mainnet:1', 1, 'canonical'
FROM generate_series(0, 8999) n;
INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind, source_family,
    manifest_version, chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    after_state)
SELECT 'noise:' || n, 'ens',
    pg_temp.rid(16777216 + CASE WHEN slot < 16 THEN lease ELSE 4000 + n % 5000 END),
    CASE WHEN slot < 4 THEN 'RegistrationGranted'
         WHEN slot < 10 THEN 'RegistrationRenewed'
         WHEN slot < 16 THEN 'TokenControlTransferred'
         WHEN n % 2 = 0 THEN 'ResolverChanged' ELSE 'RecordChanged' END,
    CASE WHEN slot < 16 THEN 'ens_v1_registrar_l1'
         WHEN n % 2 = 0 THEN 'ens_v1_registry_l1' ELSE 'ens_v1_resolver_l1' END,
    1, 'ethereum-mainnet', 1 + n % 400, pg_temp.hash('ethereum-mainnet', 1 + n % 400),
    'ens_v1_unwrapped_authority', 'canonical',
    CASE WHEN slot < 4 THEN jsonb_build_object('namehash', pg_temp.node(1048576 + lease),
             'registrant', '0x' || left(md5(n::text), 40))
         WHEN slot < 16 THEN jsonb_build_object('namehash', pg_temp.node(1048576 + lease))
         WHEN n % 5 = 0 THEN jsonb_build_object('child_node', pg_temp.node(1048576 + n % 5000),
             'node', pg_temp.node(1))
         ELSE jsonb_build_object('node', pg_temp.node(1048576 + n % 5000)) END
FROM generate_series(0, 124999) n,
    LATERAL (SELECT n % 125 AS slot, n / 125 * 4 + n % 125 % 4 AS lease) noise;

-- N9, a hot node: 19,900 registry and resolver rows and 100 grants.
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT pg_temp.rid(36864 + n), 'ethereum-mainnet', 'ethereum-mainnet:1', 1, 'canonical'
FROM generate_series(1, 100) n;
INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind, source_family,
    manifest_version, chain_id, block_number, block_hash, derivation_kind, canonicality_state,
    after_state)
SELECT 'n9:' || n, 'ens', CASE WHEN n <= 100 THEN pg_temp.rid(36864 + n) END,
    CASE WHEN n <= 100 THEN 'RegistrationGranted'
         WHEN n % 2 = 0 THEN 'ResolverChanged' ELSE 'RecordChanged' END,
    CASE WHEN n <= 100 THEN 'ens_v1_registrar_l1'
         WHEN n % 2 = 0 THEN 'ens_v1_registry_l1' ELSE 'ens_v1_resolver_l1' END,
    1, 'ethereum-mainnet', CASE WHEN n <= 100 THEN 200 + n ELSE 1 + n % 400 END,
    pg_temp.hash('ethereum-mainnet', CASE WHEN n <= 100 THEN 200 + n ELSE 1 + n % 400 END),
    'ens_v1_unwrapped_authority', 'canonical',
    CASE WHEN n <= 100 THEN jsonb_build_object('namehash', pg_temp.node(169))
         ELSE jsonb_build_object('node', pg_temp.node(169)) END
FROM generate_series(1, 20000) n;

-- N0: registry rows only.
SELECT pg_temp.ev('n0:owner', 'ethereum-mainnet', 10, 'ens_v1_registry_l1', 'AuthorityTransferred',
    NULL, jsonb_build_object('node', pg_temp.node(1), 'child_node', pg_temp.node(160)));
SELECT pg_temp.ev('n0:resolver:' || n, 'ethereum-mainnet', 10 + n, 'ens_v1_registry_l1',
    'ResolverChanged', NULL, jsonb_build_object('node', pg_temp.node(160)))
FROM generate_series(1, 2) n;
-- N1: one grant, two registry rows, and a grant of the same namehash on another chain.
SELECT pg_temp.ev('n1:grant', 'ethereum-mainnet', 100, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2576),
    jsonb_build_object('namehash', pg_temp.node(161), 'authority_kind', 'registrar'));
SELECT pg_temp.ev('n1:resolver:' || n, 'ethereum-mainnet', 100 + n, 'ens_v1_registry_l1',
    'ResolverChanged', NULL, jsonb_build_object('node', pg_temp.node(161)))
FROM generate_series(1, 2) n;
SELECT pg_temp.ev('n1:sepolia-grant', 'ethereum-sepolia', 100, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2577), jsonb_build_object('namehash', pg_temp.node(161)));
-- N2: a lease released and a second lease granted twice.
SELECT pg_temp.ev('n2:' || label, 'ethereum-mainnet', block, 'ens_v1_registrar_l1', kind,
    pg_temp.rid(resource), jsonb_build_object('namehash', pg_temp.node(162)))
FROM (VALUES
    ('grant-a', 100, 'RegistrationGranted', 2602),
    ('release-a', 150, 'RegistrationReleased', 2602),
    ('grant-b', 200, 'RegistrationGranted', 2603),
    ('renew-b', 250, 'RegistrationRenewed', 2603),
    ('regrant-b', 300, 'RegistrationGranted', 2603)
) grants(label, block, kind, resource);
-- N3: a wrapped lease, with its NameWrapper rows filed under the node.
SELECT pg_temp.ev('n3:grant', 'ethereum-mainnet', 100, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2608), jsonb_build_object('namehash', pg_temp.node(163)));
SELECT pg_temp.ev('n3:wrapped', 'ethereum-mainnet', 101, 'ens_v1_wrapper_l1', 'SurfaceBound',
    pg_temp.rid(2609), jsonb_build_object('node', pg_temp.node(163),
        'wrapped_registrar_resource_id', pg_temp.rid(2608)),
    name => 'ens:' || pg_temp.node(163));
SELECT pg_temp.ev('n3:wrapper:' || kind, 'ethereum-mainnet', block, 'ens_v1_wrapper_l1', kind,
    pg_temp.rid(2609), jsonb_build_object('node', pg_temp.node(163)))
FROM (VALUES (102, 'TokenControlTransferred'), (103, 'ExpiryChanged')) wrapper(block, kind);
-- N5: a Sepolia name, and a grant of its namehash on Mainnet where it has no surface.
SELECT pg_temp.ev('n5:grant', 'ethereum-sepolia', 100, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2640), jsonb_build_object('namehash', pg_temp.node(165)));
SELECT pg_temp.ev('n5:mainnet-grant', 'ethereum-mainnet', 100, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2641), jsonb_build_object('namehash', pg_temp.node(165)));
-- N6: grants on a readable block, an orphaned row, and a canonical row on an orphaned block.
SELECT pg_temp.ev('n6:grant', 'ethereum-mainnet', 100, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2656), jsonb_build_object('namehash', pg_temp.node(166)));
SELECT pg_temp.ev('n6:orphaned-row', 'ethereum-mainnet', 110, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2657), jsonb_build_object('namehash', pg_temp.node(166)),
    canon => 'orphaned');
SELECT pg_temp.ev('n6:orphaned-block', 'ethereum-mainnet', 120, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2658), jsonb_build_object('namehash', pg_temp.node(166)),
    hash => 'ethereum-mainnet:orphan');
-- N7: a canonical grant of a name whose surface sits on an orphaned block.
SELECT pg_temp.ev('n7:grant', 'ethereum-mainnet', 100, 'ens_v1_registrar_l1',
    'RegistrationGranted', pg_temp.rid(2672), jsonb_build_object('namehash', pg_temp.node(167)));
-- N8: the filter edges at one block. Resources 2688..2690 are kept, 2691.. are not.
SELECT pg_temp.ev('n8:' || label, 'ethereum-mainnet', 130, family, kind, pg_temp.rid(resource),
    state, visibility => visibility)
FROM (VALUES
    ('empty-kind', 2690, 'ens_v1_registrar_l1', 'RegistrationGranted', 'activated',
        jsonb_build_object('namehash', pg_temp.node(168), 'authority_kind', '')),
    ('no-kind', 2689, 'ens_v1_registrar_l1', 'RegistrationGranted', 'activated',
        jsonb_build_object('namehash', pg_temp.node(168))),
    ('upper-case', 2688, 'ens_v1_registrar_l1', 'RegistrationGranted', 'activated',
        jsonb_build_object('namehash', '0x' || upper(substr(pg_temp.node(168), 3)))),
    ('candidate', 2691, 'ens_v1_registrar_l1', 'RegistrationGranted', 'candidate',
        jsonb_build_object('namehash', pg_temp.node(168))),
    ('registry-only', 2692, 'ens_v1_registrar_l1', 'RegistrationGranted', 'activated',
        jsonb_build_object('namehash', pg_temp.node(168), 'authority_kind', 'registry_only')),
    ('renewal', 2693, 'ens_v1_registrar_l1', 'RegistrationRenewed', 'activated',
        jsonb_build_object('namehash', pg_temp.node(168))),
    ('registry-family', 2694, 'ens_v1_registry_l1', 'RegistrationGranted', 'activated',
        jsonb_build_object('namehash', pg_temp.node(168))),
    ('child-node', 2695, 'ens_v1_registrar_l1', 'RegistrationGranted', 'activated',
        jsonb_build_object('namehash', pg_temp.node(168), 'child_node', pg_temp.node(175)))
) edge(label, resource, family, kind, visibility, state);
SELECT pg_temp.ev('n8:no-resource', 'ethereum-mainnet', 130, 'ens_v1_registrar_l1',
    'RegistrationGranted', NULL, jsonb_build_object('namehash', pg_temp.node(168)));
"#;
