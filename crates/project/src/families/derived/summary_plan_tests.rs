//! Plan-shape and row tests for the name summary work list
//! (`project:families.derived.summary_names`).
//!
//! A follow block composes again the names of the registry events and name surfaces of the
//! blocks after the family marker's, up to the block itself. The marker's block is bound as a
//! parameter, so a one-block follow reads that one block through the `(chain_id, block_number)`
//! indexes of `normalized_events` and `name_surfaces`, and looks up the other registry events of
//! each resource it finds by the resource index. The fixture holds 200,000 registry events and
//! 40,000 name surfaces across 20,000 blocks with sequential scans enabled, so a statement that
//! could not bound the range from below reads the chain's whole history instead. A plain join
//! to the resource's events is not enough: a generic plan estimates the bound range at a fixed
//! share of the chain and may hash-join it to a scan of every event, which is why the lookup is
//! a parameterized subquery.
//!
//! The test pins access paths, not the cost of the whole statement. It checks three scans (the
//! block's registry events, its surfaces and the per-resource lookup) and makes no row or
//! buffer assertions. It does not cover the other CTEs' plans, the `clocked` lookup against a
//! populated `project_name_summary`, or how long a resource with a long history takes to probe.
//!
//! The rows are those of the statement before the fix, which joined the marker row for the
//! lower bound, for a one-block follow, a multi-block rebuild range, and a rebuild from a reset
//! family marker.

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, raw_sql};

use super::summary::WORK_LIST;

const CHAIN: &str = "ethereum-sepolia";
/// The follow block; the fixture's history is blocks 1 to `FOLLOW - 1`.
const FOLLOW: i64 = 20_000;
const REGISTRY_EVENTS: i64 = 200_000;
const OTHER_EVENTS: i64 = 60_000;
const NAMES: i64 = 40_000;
const PLAN_MODES: [&str; 2] = ["force_generic_plan", "force_custom_plan"];
const BLOCK_INDEXES: &[&str] = &[
    "normalized_events_chain_block_number_idx",
    "normalized_events_chain_block_number_desc_idx",
];

#[tokio::test]
async fn the_summary_work_list_reads_one_follow_block_by_index() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("family_summary_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_schema(&mut connection).await?;
        install_fixture(&mut connection).await?;
        check_plans(&mut connection).await?;
        check_rows(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

async fn check_plans(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "PREPARE summary_names (text, bigint, bigint, bigint) AS {WORK_LIST}"
    ))
    .execute(&mut *connection)
    .await
    .context("prepare summary_names")?;
    let bounded = &["(chain_id = ", "(block_number > ", "(block_number <= "][..];
    let mut failures = Vec::new();
    // A prepared statement may switch to the generic plan after its first calls, when that plan
    // is estimated no worse than planning each call; both must read only the follow block.
    for mode in PLAN_MODES {
        raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await?;
        let plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (FORMAT JSON, COSTS OFF) EXECUTE summary_names \
             ('{CHAIN}', {FOLLOW}, {}, {})",
            block_time(FOLLOW),
            FOLLOW - 1
        ))
        .fetch_one(&mut *connection)
        .await
        .with_context(|| format!("explain summary_names {mode}"))?;
        let mut scans = Vec::new();
        collect_scans(&plan[0]["Plan"], &mut scans);
        let checks: [(&str, &str, &[&str], &[&str]); 3] = [
            (
                "normalized_events",
                "registry_event",
                BLOCK_INDEXES,
                bounded,
            ),
            (
                "name_surfaces",
                "surface",
                &["name_surfaces_chain_block_number_idx"],
                bounded,
            ),
            (
                "normalized_events",
                "carried_event",
                &["normalized_events_resource_history_idx"],
                &["(resource_id = registry_event.resource_id)"],
            ),
        ];
        for (relation, alias, indexes, conditions) in checks {
            if let Some(failure) = probe_failure(&scans, relation, alias, indexes, conditions) {
                failures.push(format!("{mode}: {failure}"));
            }
        }
        if !failures.is_empty() {
            failures.push(serde_json::to_string_pretty(&plan)?);
        }
    }
    ensure!(
        failures.is_empty(),
        "summary work list plans:\n{}",
        failures.join("\n")
    );
    raw_sql("DEALLOCATE summary_names; SET plan_cache_mode = auto")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn check_rows(connection: &mut PgConnection) -> Result<()> {
    let before = before_fix()?;
    // (marker block, block): a one-block follow, a rebuild range of a thousand blocks, and
    // rebuild ranges from a reset marker (no block) to the follow block and to a block of the
    // history.
    let cases = [
        (Some(FOLLOW - 1), FOLLOW),
        (Some(FOLLOW - 1_000), FOLLOW),
        (None, FOLLOW),
        (None, 5_000),
    ];
    for (marker, number) in cases {
        sqlx::query(
            "UPDATE project_family_marker
             SET current_block_number = $2,
                 current_block_hash = CASE WHEN $2::bigint IS NOT NULL
                                           THEN '0x' || lpad(to_hex($2::bigint), 64, '0') END
             WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .bind(marker)
        .execute(&mut *connection)
        .await?;
        let expected: Vec<String> = sqlx::query_scalar(&before)
            .bind(CHAIN)
            .bind(number)
            .bind(block_time(number))
            .fetch_all(&mut *connection)
            .await?;
        ensure!(
            !expected.is_empty(),
            "marker {marker:?}, block {number}: the fixture names nothing"
        );
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *connection)
                .await?;
            let names: Vec<String> = sqlx::query_scalar(WORK_LIST)
                .bind(CHAIN)
                .bind(number)
                .bind(block_time(number))
                .bind(marker.unwrap_or(-1))
                .fetch_all(&mut *connection)
                .await?;
            ensure!(
                names == expected,
                "{mode}, marker {marker:?}, block {number}: {} names, {} before the fix",
                names.len(),
                expected.len()
            );
        }
        raw_sql("SET plan_cache_mode = auto")
            .execute(&mut *connection)
            .await?;
    }
    // The follow block names what the registry events of resources 7 and 4 carry, and its new
    // surface.
    let follow: Vec<String> = sqlx::query_scalar(WORK_LIST)
        .bind(CHAIN)
        .bind(FOLLOW)
        .bind(block_time(FOLLOW))
        .bind(FOLLOW - 1)
        .fetch_all(&mut *connection)
        .await?;
    let expected: Vec<String> = [4, 7, NAMES + 1].into_iter().map(name).collect();
    ensure!(follow == expected, "the follow block named {follow:?}");
    Ok(())
}

/// The statement before the fix: `linked` joined every registry event up to `$2` to the other
/// events of its resource, and it and `surfaced` took their lower bound from a join to the family
/// marker row. The rest of the statement is unchanged.
fn before_fix() -> Result<String> {
    const LINKED: &str = "    linked AS (
        SELECT DISTINCT carried.logical_name_id
        FROM normalized_events registry_event
        LEFT JOIN project_family_marker marker ON marker.chain_id = registry_event.chain_id
        JOIN normalized_events carried
          ON carried.resource_id = registry_event.resource_id
         AND carried.chain_id = $1
         AND carried.source_family = registry_event.source_family
         AND carried.logical_name_id IS NOT NULL
         AND carried.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE registry_event.chain_id = $1 AND registry_event.block_number <= $2
          AND registry_event.block_number > COALESCE(marker.current_block_number, -1)
          AND registry_event.resource_id IS NOT NULL
          AND registry_event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
    ),
";
    const SURFACED: &str = "    surfaced AS (
        SELECT surface.logical_name_id
        FROM name_surfaces surface
        LEFT JOIN project_family_marker marker ON marker.chain_id = surface.chain_id
        WHERE surface.chain_id = $1 AND surface.block_number <= $2
          AND surface.block_number > COALESCE(marker.current_block_number, -1)
    )
";
    let mut sql = WORK_LIST.to_owned();
    for (from, to, before) in [
        (
            "    linked AS (\n",
            "    -- A name whose composition the clock changes",
            LINKED,
        ),
        (
            "    surfaced AS (\n",
            "    SELECT logical_name_id FROM (",
            SURFACED,
        ),
    ] {
        ensure!(
            sql.matches(from).count() == 1 && sql.matches(to).count() == 1,
            "the work list no longer holds {from:?} before {to:?}"
        );
        let start = sql.find(from).expect("counted above");
        let end = sql.find(to).expect("counted above");
        ensure!(start < end, "{from:?} follows {to:?}");
        sql.replace_range(start..end, before);
    }
    ensure!(!sql.contains("$4"), "the work list binds $4 elsewhere");
    Ok(sql)
}

/// Every scan node of a JSON plan, children included.
fn collect_scans<'a>(node: &'a Value, scans: &mut Vec<&'a Value>) {
    if node["Node Type"]
        .as_str()
        .is_some_and(|kind| kind.contains("Scan"))
    {
        scans.push(node);
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        collect_scans(child, scans);
    }
}

/// Why the scans of `relation` under `alias` are not all index probes of one of `indexes` whose
/// index condition holds every one of `conditions`, or `None` when they are. A bitmap heap scan
/// passes when a bitmap index scan below it does.
fn probe_failure(
    scans: &[&Value],
    relation: &str,
    alias: &str,
    indexes: &[&str],
    conditions: &[&str],
) -> Option<String> {
    let probes = |node: &Value| {
        indexes.contains(&node["Index Name"].as_str().unwrap_or_default())
            && node["Index Cond"]
                .as_str()
                .is_some_and(|cond| conditions.iter().all(|part| cond.contains(part)))
    };
    let of_relation: Vec<&Value> = scans
        .iter()
        .copied()
        .filter(|node| node["Relation Name"] == relation && node["Alias"] == alias)
        .collect();
    if of_relation.is_empty() {
        return Some(format!("no scan of {relation} {alias}"));
    }
    for node in of_relation {
        let kind = node["Node Type"].as_str().unwrap_or_default();
        let passes = match kind {
            "Index Scan" | "Index Only Scan" => probes(node),
            "Bitmap Heap Scan" => {
                let mut below = Vec::new();
                collect_scans(node, &mut below);
                below
                    .iter()
                    .any(|child| child["Node Type"] == "Bitmap Index Scan" && probes(child))
            }
            _ => false,
        };
        if !passes {
            return Some(format!(
                "{kind} of {relation} {alias} is not a probe of {indexes:?} on {conditions:?}"
            ));
        }
    }
    None
}

/// Block times in the fixture: `1800000000 + 12 * block` seconds.
fn block_time(block: i64) -> i64 {
    1_800_000_000 + 12 * block
}

/// Name `n` of the fixture, carried by resource `n`.
fn name(n: i64) -> String {
    format!("ens:0x{n:064x}")
}

async fn install_schema(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
    ] {
        raw_sql(baseline).execute(&mut *connection).await?;
    }
    raw_sql("SET jit = off").execute(&mut *connection).await?;
    Ok(())
}

/// Blocks 0 to `FOLLOW`. Resource n carries name n. Registry event i sits at block
/// `1 + i % (FOLLOW - 1)` on resource `1 + i % NAMES`, under Basenames' registry family when
/// `i % 10 = 3` and with no name (an unnamed Transfer) when `i % 5 = 0`; the other events are
/// resolver events on the same resources. The follow block holds a registry event of resource 7
/// and one of resource 4, each in its resource's family; one of resource 8 in the other family
/// and one of resource 1, whose events carry no name, which add nothing; a resolver event; and
/// the surface of a new name. The family marker is at the block before it.
async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    let history = FOLLOW - 1;
    raw_sql(&format!(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state)
         SELECT '{CHAIN}', '0x' || lpad(to_hex(b), 64, '0'),
                CASE WHEN b > 0 THEN '0x' || lpad(to_hex(b - 1), 64, '0') END,
                b, to_timestamp(1800000000 + 12 * b), 'canonical'
         FROM generate_series(0, {FOLLOW}) b;

         INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT lpad(to_hex(n), 32, '0')::uuid, '{CHAIN}', '0x' || lpad(to_hex(1), 64, '0'), 1,
                'canonical'
         FROM generate_series(1, {NAMES}) n;

         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         SELECT 'ens:0x' || lpad(to_hex(n), 64, '0'), 'ens', 'n' || n || '.eth',
                ARRAY['n' || n, 'eth'], '\\x00'::bytea, '0x' || lpad(to_hex(n), 64, '0'),
                ARRAY['a', 'b'], 'v1', 'active', '{CHAIN}',
                '0x' || lpad(to_hex(block), 64, '0'), block, 'canonical'
         FROM generate_series(1, {NAMES} + 1) n
         CROSS JOIN LATERAL (
             SELECT CASE WHEN n > {NAMES} THEN {FOLLOW} ELSE 1 + (n * 7) % {history} END
         ) surface (block);

         INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
             resource_id, event_kind, source_family, manifest_version, chain_id, block_number,
             block_hash, transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state)
         SELECT 'registry:' || i, 'ens',
                CASE WHEN i % 5 <> 0 THEN 'ens:0x' || lpad(to_hex(1 + i % {NAMES}), 64, '0') END,
                lpad(to_hex(1 + i % {NAMES}), 32, '0')::uuid, 'AuthorityTransferred',
                CASE WHEN i % 10 = 3 THEN 'basenames_base_registry'
                     ELSE 'ens_v1_registry_l1' END,
                1, '{CHAIN}', 1 + i % {history}, '0x' || lpad(to_hex(1 + i % {history}), 64, '0'),
                '0xregistry' || i, 0, i / {history}, 'ens_v1_unwrapped_authority', 'canonical'
         FROM generate_series(0, {REGISTRY_EVENTS} - 1) i;

         INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
             resource_id, event_kind, source_family, manifest_version, chain_id, block_number,
             block_hash, transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state)
         SELECT 'resolver:' || i, 'ens', 'ens:0x' || lpad(to_hex(1 + i % {NAMES}), 64, '0'),
                lpad(to_hex(1 + i % {NAMES}), 32, '0')::uuid, 'RecordChanged',
                'ens_v1_resolver_l1', 1, '{CHAIN}', 1 + i % {history},
                '0x' || lpad(to_hex(1 + i % {history}), 64, '0'), '0xresolver' || i, 1,
                i / {history}, 'ens_v1_unwrapped_authority', 'canonical'
         FROM generate_series(0, {OTHER_EVENTS} - 1) i;

         INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
             resource_id, event_kind, source_family, manifest_version, chain_id, block_number,
             block_hash, transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state)
         SELECT 'follow:' || n, 'ens', NULL, lpad(to_hex(n), 32, '0')::uuid, kind, family, 1,
                '{CHAIN}', {FOLLOW}, '0x' || lpad(to_hex({FOLLOW}), 64, '0'), '0xfollow' || n, 0,
                n, 'ens_v1_unwrapped_authority', 'canonical'
         FROM (VALUES (7, 'AuthorityTransferred', 'ens_v1_registry_l1'),
                      (4, 'AuthorityTransferred', 'basenames_base_registry'),
                      (8, 'AuthorityTransferred', 'basenames_base_registry'),
                      (1, 'AuthorityTransferred', 'ens_v1_registry_l1'),
                      (10, 'RecordChanged', 'ens_v1_resolver_l1')) follow (n, kind, family);

         INSERT INTO project_family_marker
             (chain_id, current_block_number, current_block_hash, sequence, state)
         VALUES ('{CHAIN}', {history}, '0x' || lpad(to_hex({history}), 64, '0'), {history},
                 'live');

         ANALYZE"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
