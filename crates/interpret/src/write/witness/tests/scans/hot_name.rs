//! The repair of one target whose name has a long event history. Its earliest observation
//! survives, so the repair reads the name's history once, for that observation, as main's
//! statement does, and not again for label-hash-path evidence.
use super::*;

/// Events of the hot name after its surviving observation, none of them label-hash-path
/// evidence, as for an ENSv2 or Basenames name.
const HISTORY: i64 = 2_000;

/// `hot`, anchored outside the range with its witness at block 3, which the range releases. Its
/// other observation is at block 1, and `HISTORY` events follow it on blocks 10 on.
async fn hot_name(name: &str) -> TestResult<TestDatabase> {
    let database = database(name, BLOCKS).await?;
    let pool = database.pool();
    background(pool, 1_000).await?;
    surface(pool, "hot", 20, Some("pre-hot-3"), Shape::Plain).await?;
    preimage(pool, "pre-hot-1", "hot", 1).await?;
    preimage(pool, "pre-hot-3", "hot", 3).await?;
    sqlx::raw_sql(&format!(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             after_state
         ) SELECT 'hot-record-' || n, 'ens', 'ens:hot', 'RecordChanged', 'ens_v2_resolver', 1,
                  '{CHAIN}', 9 + n, 'h' || (9 + n), '0xtx', 0, n, '{{}}', 'ens_v2_resolver',
                  'canonical', '{{}}'
           FROM generate_series(1, {HISTORY}) n"
    ))
    .execute(pool)
    .await?;
    Ok(database)
}

/// Rows the scans of `normalized_events` aliased `alias` read, as `add_reads` counts them.
fn alias_reads(node: &Value, alias: &str) -> i64 {
    let own = if node["Relation Name"] == "normalized_events" && node["Alias"] == alias {
        let loops = node["Actual Loops"].as_i64().unwrap_or(0);
        [
            "Actual Rows",
            "Rows Removed by Filter",
            "Rows Removed by Index Recheck",
        ]
        .into_iter()
        .filter_map(|field| node[field].as_i64())
        .sum::<i64>()
            * loops
    } else {
        0
    };
    own + node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|child| alias_reads(child, alias))
        .sum::<i64>()
}

/// The repair moves the witness to block 1, equal to the frozen statement, and never reads the
/// name's events for label-hash-path evidence, in both plan modes. After ANALYZE it reads the
/// name's events once, for the earliest observation, and only the target's own surface and
/// block. Before it, the planner may scan the chain's surfaces and walk its readable lineage for
/// the earliest observation, plans main's statement shares, so only the evidence probe, the
/// read this row measures, is held to its bound there.
async fn hot_name_reads_only_its_earliest_observation(name: &str, statistics: bool) -> TestResult {
    let database = hot_name(name).await?;
    let pool = database.pool();
    if statistics {
        sqlx::raw_sql("ANALYZE").execute(pool).await?;
    }
    let mut batch = Batch::begin(pool, RANGE).await?;
    let names: BTreeSet<String> = batch.names().await?.into_iter().collect();
    assert_eq!(names, set(&["hot"]));
    batch.prepare().await?;
    batch.reanchor().await?;
    assert_eq!(batch.repair().await?, set(&["hot"]));
    let moved = "SELECT count(*) FROM name_surfaces
                 WHERE logical_name_id = 'ens:hot' AND preimage_event_identity = 'pre-hot-1'";
    assert_eq!(batch.count(moved).await?, 1);
    batch.rollback().await?;
    for mode in PLAN_MODES {
        let mut batch = Batch::begin(pool, RANGE).await?;
        batch.prepare().await?;
        batch.reanchor().await?;
        sqlx::raw_sql(&format!(
            "SET plan_cache_mode = {mode};
             PREPARE measured (text, bigint, bigint) AS {}",
            reanchor::repair_statement()
        ))
        .execute(&mut *batch.transaction)
        .await?;
        let plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE measured ('{CHAIN}', {}, {})",
            RANGE.0, RANGE.1
        ))
        .fetch_one(&mut *batch.transaction)
        .await?;
        batch.rollback().await?;
        let mut reads = Reads::default();
        add_reads(&plan[0]["Plan"], &mut reads);
        outline(&plan[0]["Plan"], 0, &mut reads.outline);
        let evidence = alias_reads(&plan[0]["Plan"], "observation");
        let mut bounds = vec![("label-hash-path evidence", evidence, 0)];
        if statistics {
            bounds.extend([
                (
                    "normalized_events",
                    read(&reads, "normalized_events"),
                    HISTORY + 2,
                ),
                ("name_surfaces", read(&reads, "name_surfaces"), 1),
                ("chain_lineage", read(&reads, "chain_lineage"), 1),
            ]);
        }
        for (what, rows, bound) in bounds {
            assert!(
                rows <= bound + SLACK,
                "statistics {statistics} {mode}: {rows} rows of {what}, bound {bound}{}",
                reads.outline
            );
        }
    }
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_hot_name_repair_reads_only_its_earliest_observation_without_statistics() -> TestResult {
    hot_name_reads_only_its_earliest_observation("witness_hot_unanalyzed", false).await
}

#[tokio::test]
async fn a_hot_name_repair_reads_only_its_earliest_observation_after_analyze() -> TestResult {
    hot_name_reads_only_its_earliest_observation("witness_hot_analyzed", true).await
}
