//! The input-class rows: what each statement reads, measured from `EXPLAIN (ANALYZE)` as rows
//! returned plus rows removed by a filter or an index recheck, times loops, for each relation.
//! A chain of readable surfaces outside the range must cost nothing. Each set costs its own
//! size, at 0, 1 and many, in both plan modes: statement 1 and the repair targets before and
//! after `ANALYZE`, the whole repair after it.
use std::collections::BTreeMap;

use bigname_adapters::schema_v2::BatchOutput;
use bigname_test_support::StatementCounter;

use super::*;

const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];
const RANGE: (i64, i64) = (3, 3);
const BLOCKS: i64 = 5_100;
/// Rows a statement may read beyond what its sets and range hold.
const SLACK: i64 = 16;

/// Rows read per relation, and the plan's outline for a failure message.
#[derive(Debug, Default)]
struct Reads {
    rows: BTreeMap<String, i64>,
    outline: String,
}

/// One line per plan node: its type, relation or index, rows and loops.
fn outline(node: &Value, depth: usize, out: &mut String) {
    let text = |field: &str| node[field].as_str().unwrap_or("").to_owned();
    let number = |field: &str| node[field].as_i64().unwrap_or(0);
    out.push_str(&format!(
        "\n{}{} {} {} rows={} removed={} loops={}",
        "  ".repeat(depth),
        text("Node Type"),
        text("Relation Name"),
        text("Index Name"),
        number("Actual Rows"),
        number("Rows Removed by Filter"),
        number("Actual Loops"),
    ));
    for child in node["Plans"].as_array().into_iter().flatten() {
        outline(child, depth + 1, out);
    }
}

fn add_reads(node: &Value, reads: &mut Reads) {
    let scan = node["Node Type"]
        .as_str()
        .is_some_and(|kind| kind.ends_with("Scan"));
    if let (true, Some(relation)) = (scan, node["Relation Name"].as_str()) {
        let loops = node["Actual Loops"].as_i64().unwrap_or(0);
        let rows = [
            "Actual Rows",
            "Rows Removed by Filter",
            "Rows Removed by Index Recheck",
        ]
        .into_iter()
        .filter_map(|field| node[field].as_i64())
        .sum::<i64>();
        *reads.rows.entry(relation.to_owned()).or_default() += rows * loops;
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        add_reads(child, reads);
    }
}

/// Run `statement` once under `mode` and report what each relation's scans read.
async fn measure(conn: &mut PgConnection, statement: &str, mode: &str) -> TestResult<Reads> {
    sqlx::raw_sql(&format!(
        "SET plan_cache_mode = {mode};
         PREPARE measured (text, bigint, bigint) AS {statement}"
    ))
    .execute(&mut *conn)
    .await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE measured ('{CHAIN}', {}, {})",
        RANGE.0, RANGE.1
    ))
    .fetch_one(&mut *conn)
    .await?;
    sqlx::raw_sql("DEALLOCATE measured; RESET plan_cache_mode")
        .execute(&mut *conn)
        .await?;
    let mut reads = Reads::default();
    add_reads(&plan[0]["Plan"], &mut reads);
    outline(&plan[0]["Plan"], 0, &mut reads.outline);
    Ok(reads)
}

fn read(reads: &Reads, relation: &str) -> i64 {
    reads.rows.get(relation).copied().unwrap_or(0)
}

/// What each statement reads under `mode`.
struct Measured {
    names: Reads,
    targets: Reads,
    repair: Option<Reads>,
}

/// Statement 1 on the rows as they stand. The repair targets alone and the whole repair on the
/// rows the range preparation and reanchoring leave, in a rolled-back transaction.
async fn measure_all(pool: &PgPool, mode: &str, whole_repair: bool) -> TestResult<Measured> {
    let mut connection = pool.acquire().await?;
    let names = measure(&mut connection, &search::redo_names(), mode).await?;
    drop(connection);
    let mut batch = Batch::begin(pool, RANGE).await?;
    batch.prepare().await?;
    batch.reanchor().await?;
    let only_targets = format!(
        "SELECT target.logical_name_id FROM ({}) target",
        reanchor::repair_targets()
    );
    let targets = measure(&mut batch.transaction, &only_targets, mode).await?;
    let repair = if whole_repair {
        Some(measure(&mut batch.transaction, &reanchor::repair_statement(), mode).await?)
    } else {
        None
    };
    batch.rollback().await?;
    Ok(Measured {
        names,
        targets,
        repair,
    })
}

/// `count` readable plain surfaces `bg1..`, each anchored and witnessed on its own block from
/// 51 on, outside every range a row redoes.
async fn background(pool: &PgPool, count: i64) -> TestResult {
    sqlx::raw_sql(&format!(
        r"INSERT INTO name_surfaces (
              logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
              labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
              block_number, canonicality_state, preimage_event_identity
          ) SELECT 'ens:bg' || n, 'ens', 'bg' || n || '.eth', ARRAY['bg' || n, 'eth'],
                   '\x05'::bytea, 'bg' || n, ARRAY['bg' || n || '-label', '0xeth'], 'test',
                   'active', '{CHAIN}', 'h' || (50 + n % 5000), 50 + n % 5000, 'canonical',
                   'bg-pre-' || n
            FROM generate_series(1, {count}) n;
          INSERT INTO normalized_events (
              event_identity, namespace, logical_name_id, event_kind, source_family,
              manifest_version, chain_id, block_number, block_hash, transaction_hash,
              transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
              after_state
          ) SELECT 'bg-pre-' || n, 'ens', 'ens:bg' || n, '{PREIMAGE_OBSERVATION_EVENT_KIND}',
                   'ens_v1_registry_l1', 1, '{CHAIN}', 50 + n % 5000, 'h' || (50 + n % 5000),
                   '0xtx', 0, n, '{{}}', 'raw_log_preimage_observation', 'canonical', '{{}}'
            FROM generate_series(1, {count}) n;"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Surfaces `{prefix}{n}` for `n` in `from..to`, anchored on block `anchor`, witnessed by the
/// SQL expression `witness` of `n`.
async fn surfaces_of(
    pool: &PgPool,
    prefix: &str,
    (from, to): (i64, i64),
    anchor: i64,
    witness: &str,
    shape: Shape,
) -> TestResult {
    let (raw_name, raw_labels, dns, visibility, reason, at) = match shape {
        Shape::Plain => (
            format!("'{prefix}' || n || '.eth'"),
            format!("ARRAY['{prefix}' || n, 'eth']"),
            r"'\x05'::bytea",
            "active",
            "NULL",
            "NULL".to_owned(),
        ),
        Shape::ByteShadow => (
            "''".to_owned(),
            "'{}'::text[]".to_owned(),
            "''::bytea",
            "shadow",
            "'normalization_gate'",
            format!("to_timestamp({anchor})"),
        ),
        Shape::Shadow | Shape::HashPath => unreachable!("not a measured class"),
    };
    sqlx::raw_sql(&format!(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, deactivation_reason,
             deactivated_at, chain_id, block_hash, block_number, canonicality_state,
             preimage_event_identity
         ) SELECT 'ens:{prefix}' || n, 'ens', {raw_name}, {raw_labels}, {dns}, '{prefix}' || n,
                  ARRAY['{prefix}' || n || '-label', '0xeth'], 'test', '{visibility}', {reason},
                  {at}, '{CHAIN}', 'h{anchor}', {anchor}, 'canonical', {witness}
           FROM generate_series({from}, {to} - 1) n"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Events `{identity}{n}` of kind `kind` naming `ens:{name}{n}`, for `n` in `from..to`, on the
/// block the SQL expressions `number` and `hash` of `n` give.
async fn events_of(
    pool: &PgPool,
    (identity, name, kind): (&str, &str, &str),
    (from, to): (i64, i64),
    number: &str,
    hash: &str,
) -> TestResult {
    sqlx::raw_sql(&format!(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind, canonicality_state,
             after_state
         ) SELECT '{identity}' || n, 'ens', 'ens:{name}' || n, '{kind}', 'ens_v1_registry_l1',
                  1, '{CHAIN}', {number}, {hash}, '0xtx', 0, n, '{{}}',
                  'raw_log_preimage_observation', 'canonical', '{{}}'
           FROM generate_series({from}, {to} - 1) n"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Orphaned blocks `{prefix}{n}` at height `height + n`, for `n` in `from..to`.
async fn orphaned_blocks(
    pool: &PgPool,
    prefix: &str,
    (from, to): (i64, i64),
    height: i64,
) -> TestResult {
    sqlx::raw_sql(&format!(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) SELECT '{CHAIN}', '{prefix}' || n, {height} + n, to_timestamp({height} + n), 'orphaned'
           FROM generate_series({from}, {to} - 1) n"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// One readable surface anchored and observed in the range.
async fn in_range(pool: &PgPool) -> TestResult {
    surface(pool, "anchored", 3, Some("pre-anchored-3"), Shape::Plain).await?;
    preimage(pool, "pre-anchored-3", "anchored", 3).await
}

/// One surface of each lost-witness class, outside the range.
async fn one_of_each(pool: &PgPool) -> TestResult {
    surface(pool, "released", 20, None, Shape::Plain).await?;
    surface(pool, "orphaned", 21, Some("pre-orphaned"), Shape::Plain).await?;
    orphaned_blocks(pool, "fork-one-", (0, 1), 29).await?;
    preimage_on(pool, "pre-orphaned", "orphaned", (29, "fork-one-0")).await?;
    surface(
        pool,
        "dangling",
        22,
        Some("pre-dangling-gone"),
        Shape::ByteShadow,
    )
    .await
}

/// The surfaces each set serves, and the rows each statement may read for them: every member,
/// every byte shadow probed, every event on an orphaned block and every orphaned block.
struct Sizes {
    members: BTreeSet<String>,
    released: i64,
    byte_shadows: i64,
    orphaned_blocks: i64,
    orphaned_events: i64,
    orphaned_preimages: i64,
}

async fn sizes(pool: &PgPool, members: &BTreeSet<String>) -> TestResult<Sizes> {
    let (released, byte_shadows, orphaned_blocks, orphaned_events, orphaned_preimages): (
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(&format!(
        "SELECT
             (SELECT count(*) FROM name_surfaces
              WHERE raw_name IS NOT NULL AND preimage_event_identity IS NULL),
             (SELECT count(*) FROM name_surfaces
              WHERE raw_name = '' AND visibility_state = 'shadow'),
             (SELECT count(*) FROM chain_lineage WHERE canonicality_state = 'orphaned'),
             (SELECT count(*) FROM normalized_events event JOIN chain_lineage lineage
                USING (chain_id, block_hash, block_number)
              WHERE lineage.canonicality_state = 'orphaned'),
             (SELECT count(*) FROM normalized_events event JOIN chain_lineage lineage
                USING (chain_id, block_hash, block_number)
              WHERE lineage.canonicality_state = 'orphaned'
                AND event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}')"
    ))
    .fetch_one(pool)
    .await?;
    Ok(Sizes {
        members: members.clone(),
        released,
        byte_shadows,
        orphaned_blocks,
        orphaned_events,
        orphaned_preimages,
    })
}

/// Both statements return their sets, equal to the frozen full scans, and read no more than
/// the sets and the range hold, in both plan modes.
async fn check(pool: &PgPool, label: &str, sizes: &Sizes, statistics: bool) -> TestResult {
    let mut batch = Batch::begin(pool, RANGE).await?;
    let names: BTreeSet<String> = batch.names().await?.into_iter().collect();
    assert_eq!(names, sizes.members, "{label}: statement 1");
    batch.prepare().await?;
    batch.reanchor().await?;
    assert_eq!(
        batch.targets().await?,
        sizes.members,
        "{label}: repair targets"
    );
    batch.rollback().await?;

    let targets = sizes.members.len() as i64;
    // Statement 1 also reads the in-range surface and its observation.
    let surfaces = sizes.released + sizes.orphaned_preimages + sizes.byte_shadows + 1;
    let events = sizes.orphaned_events + sizes.byte_shadows + 1;
    let blocks = sizes.orphaned_blocks + sizes.byte_shadows;
    for mode in PLAN_MODES {
        let measured = measure_all(pool, mode, statistics).await?;
        let mut reads = vec![
            ("statement 1", &measured.names, "name_surfaces", surfaces),
            ("statement 1", &measured.names, "normalized_events", events),
            ("statement 1", &measured.names, "chain_lineage", blocks),
            (
                "repair targets",
                &measured.targets,
                "name_surfaces",
                surfaces,
            ),
            (
                "repair targets",
                &measured.targets,
                "normalized_events",
                events,
            ),
            ("repair targets", &measured.targets, "chain_lineage", blocks),
        ];
        // The rest of the repair probes each target's row and its observations. Before ANALYZE
        // the planner may walk the readable lineage for each target's earliest observation, a
        // fast-start plan for its LIMIT 1 that main's statement shares, so only the targets,
        // the part this change bounds, are held to their sizes there.
        if let Some(repair) = &measured.repair {
            reads.extend([
                ("repair", repair, "name_surfaces", surfaces + targets),
                ("repair", repair, "normalized_events", events + 4 * targets),
                ("repair", repair, "chain_lineage", blocks + 4 * targets),
            ]);
        }
        for (statement, measured, relation, bound) in reads {
            assert!(
                read(measured, relation) <= bound + SLACK,
                "{label} {mode}: {statement} read {} rows of {relation}, bound {bound}: {:?}{}",
                read(measured, relation),
                measured.rows,
                measured.outline
            );
        }
    }
    Ok(())
}

const BACKGROUND: i64 = 20_000;
const MANY: i64 = 1_000;

/// Grow each set from 0 to 1 to many on a chain of readable surfaces, checking at each step.
async fn grow_each_set(name: &str, statistics: bool) -> TestResult {
    let database = database(name, BLOCKS).await?;
    let pool = database.pool();
    background(pool, BACKGROUND).await?;
    in_range(pool).await?;
    let mut members = set(&["anchored"]);
    let half = MANY / 2;
    let steps: [(&str, i64); 7] = [
        ("empty sets", 0),
        ("one released", 1),
        ("many released", MANY),
        ("one orphaned", 1),
        ("many orphaned", MANY),
        ("one byte shadow", 1),
        ("many byte shadows", MANY),
    ];
    for (step, (label, size)) in steps.into_iter().enumerate() {
        let kind = PREIMAGE_OBSERVATION_EVENT_KIND;
        let added: Vec<String> = match step {
            0 => Vec::new(),
            1 | 2 => {
                let from = if step == 1 { 0 } else { 1 };
                surfaces_of(pool, "released", (from, size), 20, "NULL", Shape::Plain).await?;
                (from..size).map(|n| id(&format!("released{n}"))).collect()
            }
            3 => {
                orphaned_blocks(pool, "fork-one-", (0, 1), 29).await?;
                let witness = "'orphaned-pre-' || n";
                surfaces_of(pool, "orphaned", (0, 1), 21, witness, Shape::Plain).await?;
                let ids = ("orphaned-pre-", "orphaned", kind);
                events_of(pool, ids, (0, 1), "29", "'fork-one-0'").await?;
                vec![id("orphaned0")]
            }
            4 => {
                // Half on one hot orphaned block, the rest four to a block, with observations of
                // names whose witness is readable and other events on the same blocks.
                orphaned_blocks(pool, "fork-hot-", (0, 1), 30).await?;
                orphaned_blocks(pool, "fork-", (0, half / 4), 31).await?;
                let witness = "'orphaned-pre-' || n";
                surfaces_of(pool, "orphaned", (1, size), 21, witness, Shape::Plain).await?;
                let ids = ("orphaned-pre-", "orphaned", kind);
                events_of(pool, ids, (1, half), "30", "'fork-hot-0'").await?;
                let number = format!("31 + (n - {half}) / 4");
                let hash = format!("'fork-' || (n - {half}) / 4");
                events_of(pool, ids, (half, size), &number, &hash).await?;
                let hot = (1, 201);
                events_of(pool, ("ghost-", "bg", kind), hot, "30", "'fork-hot-0'").await?;
                let noise = ("noise-", "bg", "SubregistryChanged");
                events_of(pool, noise, hot, "30", "'fork-hot-0'").await?;
                (1..size).map(|n| id(&format!("orphaned{n}"))).collect()
            }
            5 => {
                let witness = "'bytes-gone-' || n";
                surfaces_of(pool, "bytes", (0, 1), 22, witness, Shape::ByteShadow).await?;
                vec![id("bytes0")]
            }
            _ => {
                // One in five names no event, the rest a readable observation.
                let witness = "CASE WHEN n % 5 = 0 THEN 'bytes-gone-' ELSE 'bytes-pre-' END || n";
                surfaces_of(pool, "bytes", (1, size), 22, witness, Shape::ByteShadow).await?;
                events_of(
                    pool,
                    ("bytes-pre-", "bytes", kind),
                    (1, size),
                    "22",
                    "'h22'",
                )
                .await?;
                sqlx::raw_sql(
                    "DELETE FROM normalized_events WHERE event_identity ~ '^bytes-pre-[0-9]*[05]$'",
                )
                .execute(pool)
                .await?;
                (1..size)
                    .filter(|n| n % 5 == 0)
                    .map(|n| id(&format!("bytes{n}")))
                    .collect()
            }
        };
        members.extend(added);
        if statistics {
            sqlx::raw_sql("ANALYZE").execute(pool).await?;
        }
        let sizes = sizes(pool, &members).await?;
        check(pool, label, &sizes, statistics).await?;
    }
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn each_set_costs_its_own_size_without_statistics() -> TestResult {
    grow_each_set("witness_sets_unanalyzed", false).await
}

#[tokio::test]
async fn each_set_costs_its_own_size_after_analyze() -> TestResult {
    grow_each_set("witness_sets_analyzed", true).await
}

/// A chain of readable surfaces outside a one-block range, with one surface of each class.
async fn seeded_chain(name: &str) -> TestResult<TestDatabase> {
    let database = database(name, BLOCKS).await?;
    background(database.pool(), 5_000).await?;
    in_range(database.pool()).await?;
    one_of_each(database.pool()).await?;
    sqlx::raw_sql("ANALYZE").execute(database.pool()).await?;
    Ok(database)
}

fn one_of_each_names() -> BTreeSet<String> {
    set(&["anchored", "released", "orphaned", "dangling"])
}

/// The statements a one-block redo sends that contain `marker`.
async fn sent(pool: &PgPool, marker: &str) -> TestResult<usize> {
    let counter = StatementCounter::connect(pool).await?;
    let loaded = vec![(RANGE.0, format!("h{}", RANGE.0))];
    let output = BatchOutput::default();
    let (result, counts) = counter
        .count(super::super::super::batch(
            counter.pool(),
            CHAIN,
            Some(RANGE),
            true,
            true,
            0,
            &loaded,
            &output,
        ))
        .await;
    result?;
    counter.close().await;
    Ok(counts
        .statements()
        .iter()
        .filter(|statement| statement.contains(marker))
        .count())
}

/// Statement 1 of a one-block redo on a seeded chain reads the four surfaces it returns, not
/// the chain's.
#[tokio::test]
async fn redo_names_one_block_range_reads_only_the_witness_sets() -> TestResult {
    let database = seeded_chain("witness_redo_names_scan").await?;
    let pool = database.pool();
    let mut batch = Batch::begin(pool, RANGE).await?;
    let names: BTreeSet<String> = batch.names().await?.into_iter().collect();
    assert_eq!(names, one_of_each_names());
    batch.rollback().await?;
    for mode in PLAN_MODES {
        let mut connection = pool.acquire().await?;
        let reads = measure(&mut connection, &search::redo_names(), mode).await?;
        for relation in ["name_surfaces", "normalized_events", "chain_lineage"] {
            let rows = read(&reads, relation);
            assert!(
                rows <= SLACK,
                "{mode}: {rows} rows of {relation}{}",
                reads.outline
            );
        }
    }
    assert_eq!(sent(pool, "interpret:identity_search.redo_names").await?, 1);
    database.cleanup().await?;
    Ok(())
}

/// The repair of a one-block redo on a seeded chain reads the four surfaces it targets.
#[tokio::test]
async fn repair_targets_one_block_range_reads_only_the_witness_sets() -> TestResult {
    let database = seeded_chain("witness_repair_scan").await?;
    let pool = database.pool();
    let mut batch = Batch::begin(pool, RANGE).await?;
    batch.prepare().await?;
    batch.reanchor().await?;
    assert_eq!(batch.targets().await?, one_of_each_names());
    batch.rollback().await?;
    for mode in PLAN_MODES {
        let reads = measure_all(pool, mode, true)
            .await?
            .repair
            .unwrap_or_default();
        for relation in ["name_surfaces", "normalized_events", "chain_lineage"] {
            let rows = read(&reads, relation);
            assert!(
                rows <= SLACK,
                "{mode}: {rows} rows of {relation}{}",
                reads.outline
            );
        }
    }
    assert_eq!(sent(pool, "WITH targets AS").await?, 1);
    database.cleanup().await?;
    Ok(())
}
