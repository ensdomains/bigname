//! End-to-end Project benchmark: each target runs as one committed Normal batch through the
//! runner's Project phase, so the engine commit, hydration and the progress marker all happen,
//! and the clock stops only when a storage reader serves the new publication. The rollback
//! benchmark in `bigname-project` stops before commit and never writes the marker; this one
//! measures what a client waits for.
//!
//! The timed path is the engine commit, the batch log, hydration, the progress write and the
//! synchronous metrics handoff: the phase records the write summary on the metrics feed, and the
//! feed is notified that the batch committed. It does not include the runner's `confirm_progress`,
//! which production awaits before that notification, nor the metrics worker applying the summary
//! or a scrape reading it; this test starts no metrics worker.
//!
//! It commits, so it runs only against a disposable copy carrying the marker of
//! docs/runbooks/benchmark-gate.md. The copy must be paused with Project published at
//! `BIGNAME_BENCHMARK_PREVIOUS`. A target above the stored head moves the head there first, as
//! Live does; a target below it must be within the publication lag tolerance, because a
//! publication further behind the head is never served. On a paused copy that means one target at
//! its head, unless the copy was rewound.
//!
//! With `BIGNAME_END_TO_END_COMPARE=1` each target is then rebuilt from scratch and committed at
//! the same block on the same copy, and the name and subname readers must serve the same values
//! for every name and every subname in either state, apart from rows the batch was allowed to
//! keep (`project_end_to_end/endpoint.rs`). That is a reader-value comparison against a full
//! rebuild, not the rollback benchmark's contract oracle. The next target continues from that
//! rebuilt state.
//!
//! In the same mode, once the owned key families reach each target, the step 5 family readers
//! (`bigname_storage::families::topology`) must serve what the served subname, topology, resolver
//! and resolver collection readers serve at that publication (`project_end_to_end/shadow.rs`).
#[path = "project_end_to_end/endpoint.rs"]
mod endpoint;
#[allow(dead_code)]
#[path = "project_end_to_end/shadow.rs"]
mod shadow;
#[allow(dead_code)]
mod support;

use std::{str::FromStr, time::Instant};

use anyhow::{Context, Result, ensure};
use bigname_lookup::ChainRpcUrls;
use bigname_storage::{
    PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS, load_name_current, load_served_project_generation,
};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    heads::{BlockMarker, HeadMarkers, publish_heads},
    metrics::RunnerMetricsFeed,
    phase::{Phase, PhaseContext, PhaseName, PhaseResume, RunMode},
    project_phase::ProjectPhase,
    state::PhaseStore,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use support::ScratchDatabase;

const CHAIN: &str = "ethereum-sepolia";
/// Subname page sizes for the rebuild comparison: small on the fixture so pages are traversed,
/// larger on a copy.
const FIXTURE_CHILDREN_PAGE: u64 = 1;
const COPY_CHILDREN_PAGE: u64 = 200;
/// Registry subnames for the fixture, which has none of its own: every registry-only name becomes
/// a child of the first `.eth` name, so that parent's subnames span several pages.
const FIXTURE_SUBNAMES: &str = "
INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id,
    event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
    transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
    after_state, raw_fact_ref)
SELECT 'fixture:subname:' || child.i, 'ens', child.logical_name_id, child.registry_node,
       'SubregistryChanged', 'ens_v1_registry_l1', 1, '__CHAIN__', child.block, child.block_hash,
       '0x' || md5('s' || child.i), 0, 12, 'ens_v1_unwrapped_authority',
       'canonical'::canonicality_state,
       jsonb_build_object('source_event', 'NewOwner', 'node', parent.namehash,
           'child_node', child.namehash,
           'labelhash', '0x' || md5('a' || child.i) || md5('b' || child.i),
           'owner', child.owner),
       '{\"emitting_address\":\"0x00000000000000000000000000000000000000a3\"}'
FROM seed child
JOIN seed parent ON parent.i = 1
WHERE child.known AND child.shape = 'registry_only';
";

#[tokio::test]
#[ignore = "commits to an explicitly configured disposable copy"]
async fn disposable_copy_publishes_hydrates_and_reads_each_target() -> Result<()> {
    ensure!(
        std::env::var("BIGNAME_END_TO_END").as_deref() == Ok("1"),
        "set BIGNAME_END_TO_END=1: this benchmark commits"
    );
    let options = PgConnectOptions::from_str(&std::env::var("BIGNAME_BENCHMARK_DATABASE_URL")?)?
        .application_name("bigname-project-end-to-end-benchmark")
        .options([("search_path", "bigname_phase,public")]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;
    require_disposable_copy(&pool).await?;
    let previous = std::env::var("BIGNAME_BENCHMARK_PREVIOUS")?.parse()?;
    let targets = parse_targets(&std::env::var("BIGNAME_BENCHMARK_TARGETS")?)?;
    let compare = (std::env::var("BIGNAME_END_TO_END_COMPARE").as_deref() == Ok("1"))
        .then_some(COPY_CHILDREN_PAGE);
    run(&pool, previous, &targets, compare).await?;
    pool.close().await;
    Ok(())
}

/// The same mode on the rebuild-performance seed, so it keeps working and can be timed locally:
/// `BIGNAME_END_TO_END_FIXTURE_NAMES`, `_PREVIOUS` and `_TARGETS` size it (defaults 400, 30 and
/// 35,40).
///
/// The seed's registry-only names (`i % 10 = 9`) become subnames of one parent once their block
/// is published, and the subname reader serves about half of them (the seed gives half a zero
/// owner). The defaults serve 3 subnames at block 35 and 4 at block 40, so with pages of one row
/// every target reads that parent over several pages, which the test requires. More names or
/// later targets serve more: 5,000 names at blocks 241 to 245 serve 240.
#[tokio::test]
async fn fixture_corpus_publishes_hydrates_reads_and_matches_a_rebuild() -> Result<()> {
    let setting = |name: &str, default: &str| {
        std::env::var(format!("BIGNAME_END_TO_END_FIXTURE_{name}"))
            .unwrap_or_else(|_| default.to_owned())
    };
    let names: u32 = setting("NAMES", "400").parse()?;
    let previous: i64 = setting("PREVIOUS", "30").parse()?;
    let targets = parse_targets(&setting("TARGETS", "35,40"))?;
    let scratch = ScratchDatabase::create("phase_runner_project_end_to_end").await?;
    let pool = scratch.pool();
    // Head publication walks parent links, which the seed's lineage leaves out, and blocks past
    // the published head are observed but not yet canonical: Live promotes each target before
    // Project follows it.
    let seed = include_str!("../../../crates/project/tests/rebuild_performance/seed.sql")
        .replacen(
            "canonicality_state)\nSELECT '__CHAIN__', '0x' || lpad(to_hex(block), 64, '0'),",
            "canonicality_state, parent_hash)\nSELECT '__CHAIN__', '0x' || lpad(to_hex(block), 64, '0'),",
            1,
        )
        .replacen(
            "'canonical'::canonicality_state\n",
            &format!(
                "(CASE WHEN block <= {previous} THEN 'canonical' ELSE 'observed' END)\
                 ::canonicality_state,\n       \
                 CASE WHEN block > 1 THEN '0x' || lpad(to_hex(block - 1), 64, '0') END\n"
            ),
            1,
        );
    let seed = seed.replacen(
        "\nDROP TABLE seed;",
        &format!("{FIXTURE_SUBNAMES}\nDROP TABLE seed;"),
        1,
    );
    ensure!(
        seed.contains("parent_hash")
            && seed.contains("block > 1")
            && seed.contains("fixture:subname"),
        "the seed changed shape"
    );
    sqlx::raw_sql(
        &seed
            .replace("__NAMES__", &names.to_string())
            .replace("__CHAIN__", CHAIN),
    )
    .execute(pool)
    .await?;
    prepare_fixture(pool, previous, *targets.last().context("targets")?).await?;
    sqlx::raw_sql(
        "CREATE SCHEMA bigname_benchmark;
         CREATE TABLE bigname_benchmark.disposable_copy_marker (
             marker uuid PRIMARY KEY,
             database_name text NOT NULL UNIQUE,
             prepared_at timestamptz NOT NULL DEFAULT now()
         );
         INSERT INTO bigname_benchmark.disposable_copy_marker (marker, database_name)
         VALUES (gen_random_uuid(), current_database());",
    )
    .execute(pool)
    .await?;
    require_disposable_copy(pool).await?;
    let compared = run(pool, previous, &targets, Some(FIXTURE_CHILDREN_PAGE)).await?;
    ensure!(compared.len() == targets.len(), "every target is compared");
    for compared in &compared {
        // The harness cannot tell whether a dropped key was in the batch's full scope (see
        // `endpoint::Outcome::dropped`), so the fixture must produce none.
        ensure!(
            compared.outcome.dropped == 0,
            "target {} dropped {} baseline keys",
            compared.target,
            compared.outcome.dropped
        );
        // The subname comparison must have traversed a parent over more than one page, not just
        // printed that it could.
        ensure!(
            compared.subname_rows > 0 && compared.subname_pages > compared.subname_parents,
            "target {} served {} subnames over {} pages for {} parents; raise \
             BIGNAME_END_TO_END_FIXTURE_NAMES so a parent spans several pages",
            compared.target,
            compared.subname_rows,
            compared.subname_pages,
            compared.subname_parents
        );
    }
    scratch.cleanup().await
}

fn parse_targets(value: &str) -> Result<Vec<i64>> {
    value
        .split(',')
        .map(|target| target.trim().parse().context("target block"))
        .collect()
}

/// The copy marker of docs/runbooks/benchmark-gate.md, for this database, prepared within the
/// last twelve hours and not more than five minutes ahead of the database clock.
async fn require_disposable_copy(pool: &PgPool) -> Result<()> {
    let fresh: Option<bool> = sqlx::query_scalar(
        "SELECT prepared_at > now() - interval '12 hours'
                AND prepared_at <= now() + interval '5 minutes'
         FROM bigname_benchmark.disposable_copy_marker
         WHERE database_name = current_database()",
    )
    .fetch_optional(pool)
    .await
    .context("the end-to-end benchmark needs the disposable-copy marker; it commits")?;
    ensure!(
        fresh == Some(true),
        "the disposable-copy marker is missing, stale or ahead of the database clock"
    );
    Ok(())
}

/// Seed state a runner would have left: Ingest and Interpret complete through the last target,
/// the head and Project at `previous`, Project published by a full rebuild.
async fn prepare_fixture(pool: &PgPool, previous: i64, interpreted_through: i64) -> Result<()> {
    let store = PhaseStore::new(pool.clone());
    store.initialize_chain(CHAIN).await?;
    let interpreted_hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(interpreted_through)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = $2, current_block_hash = $3,
             target_block_number = $2, target_block_hash = $3, input_content_hash = $4,
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret')",
    )
    .bind(CHAIN)
    .bind(interpreted_through)
    .bind(&interpreted_hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    let marker = follow_head(pool, previous).await?;
    store
        .start_phase(CHAIN, PhaseName::Project, &RunMode::Normal)
        .await?;
    let outcome = ProjectPhase::new(pool.clone())
        .run_batch(context(&marker, None))
        .await?;
    store
        .record_progress(
            CHAIN,
            PhaseName::Project,
            &RunMode::Normal,
            None,
            outcome.progress(),
        )
        .await?;
    Ok(())
}

/// What the rebuild comparison saw at one target.
struct Compared {
    target: i64,
    outcome: endpoint::Outcome,
    subname_rows: usize,
    subname_pages: usize,
    subname_parents: usize,
}

/// `compare` is the subname page size of the rebuild comparison, when it runs; it returns what
/// each comparison saw.
async fn run(
    pool: &PgPool,
    previous: i64,
    targets: &[i64],
    compare: Option<u64>,
) -> Result<Vec<Compared>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("bigname_project::batch=info,bigname_project::families=info")
        .with_target(false)
        .with_ansi(false)
        .try_init();
    ensure!(!targets.is_empty(), "at least one target is required");
    let interpreted: (Option<i64>, bool) = sqlx::query_as(
        "SELECT current_block_number, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    ensure!(
        !interpreted.1 && interpreted.0.is_some(),
        "Interpret must have a completed frontier without redo"
    );
    let mut last = previous;
    for &number in targets {
        ensure!(
            number > last && Some(number) <= interpreted.0,
            "targets must increase and remain within the Interpret frontier"
        );
        last = number;
    }
    let mut resume = load_marker(pool, previous).await?;
    let mut compared = Vec::new();
    ensure!(
        publication(pool).await? == (Some(previous), Some(resume.hash.clone()), false),
        "Project is not published at the requested previous marker"
    );
    let store = PhaseStore::new(pool.clone());
    // The runner starts the phase once and then runs batches; starting sets the running status
    // and this binary's interpreter hash, which the publication fence requires.
    store
        .start_phase(CHAIN, PhaseName::Project, &RunMode::Normal)
        .await?;
    // As main.rs builds it, so the batch log and the metrics handoff fall inside the clock. No
    // metrics worker consumes the feed here: applying the summary and scraping it are not timed.
    let metrics_feed = RunnerMetricsFeed::default();
    let project = ProjectPhase::with_hydration(pool.clone(), ChainRpcUrls::default())
        .with_metrics_feed(metrics_feed.clone());
    for &number in targets {
        let target = follow_head(pool, number).await?;
        let baseline = match compare {
            Some(children_page) => Some(endpoint::Served::read(pool, children_page).await?),
            None => None,
        };
        let batch_started: String = sqlx::query_scalar("SELECT clock_timestamp()::text")
            .fetch_one(pool)
            .await?;
        let started = Instant::now();
        let outcome = project.run_batch(context(&target, Some(&resume))).await?;
        store
            .record_progress(
                CHAIN,
                PhaseName::Project,
                &RunMode::Normal,
                None,
                outcome.progress(),
            )
            .await?;
        // The runner notifies here too, after `confirm_progress`, which this test skips.
        metrics_feed.batch_committed();
        while load_served_project_generation(pool, CHAIN, number, &target.hash, true, true)
            .await?
            .is_none()
        {
            ensure!(
                started.elapsed().as_secs() < 60,
                "the publication at {number} never became servable"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let rewritten: Vec<String> = sqlx::query_scalar(
            "SELECT logical_name_id FROM name_current
             WHERE last_recomputed_at >= $1::timestamptz ORDER BY logical_name_id",
        )
        .bind(&batch_started)
        .fetch_all(pool)
        .await?;
        let read = match rewritten.first() {
            Some(name) => {
                load_name_current(pool, name)
                    .await?
                    .with_context(|| format!("the name reader did not serve {name}"))?;
                "observed"
            }
            None => "nothing_rewritten",
        };
        let elapsed = started.elapsed();
        eprintln!(
            "SEPOLIA_END_TO_END target={number} elapsed_ms={} commit=included hydration=invoked \
             marker=written read={read} rewritten_names={}",
            elapsed.as_millis(),
            rewritten.len()
        );
        // The owned key families follow in their own transactions once progress is recorded,
        // as the runner calls them; their time is outside the served clock above.
        let families_started = Instant::now();
        project.after_progress_recorded(CHAIN).await;
        let families_ms = families_started.elapsed().as_millis();
        let family_marker: Option<i64> = sqlx::query_scalar(
            "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_optional(pool)
        .await?
        .flatten();
        eprintln!("SEPOLIA_END_TO_END_FAMILIES target={number} families_ms={families_ms}");
        ensure!(
            family_marker == Some(number),
            "the owned key families stopped at {family_marker:?}, not at target {number}"
        );
        ensure!(
            publication(pool).await? == (Some(number), Some(target.hash.clone()), false),
            "the progress marker is not at target {number}"
        );
        let hash: Option<String> = sqlx::query_scalar(
            "SELECT input_content_hash FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'project'",
        )
        .bind(CHAIN)
        .fetch_one(pool)
        .await?;
        ensure!(
            hash.as_deref() == Some(INTERPRETER_CONTENT_HASH),
            "the publication does not carry this binary's interpreter hash"
        );
        if let Some(children_page) = compare {
            let report = shadow::compare(
                pool,
                CHAIN,
                shadow::Settings {
                    children_page,
                    collection_page: children_page,
                    every_child_filter: children_page == FIXTURE_CHILDREN_PAGE,
                },
            )
            .await?;
            eprintln!("{}", report.line());
            ensure!(
                report.mismatches.is_empty(),
                "the family readers differ from the served readers at {number}: {:#}",
                shadow::describe(&report)
            );
        }
        if let (Some(children_page), Some(baseline)) = (compare, baseline) {
            let retention = endpoint::Retention::load(
                pool,
                baseline,
                (resume.number + 1, number),
                resume.number,
            )
            .await?;
            compared.push(
                compare_with_rebuild(pool, &project, &target, children_page, &retention).await?,
            );
        }
        resume = target;
    }
    Ok(compared)
}

/// Reads every name and subname the batch left, rebuilds the target from scratch and commits it,
/// reads them again and compares. A row that differs must be one the batch was allowed to keep
/// (see [`endpoint::Retention`]).
async fn compare_with_rebuild(
    pool: &PgPool,
    project: &ProjectPhase,
    target: &BlockMarker,
    children_page: u64,
    retention: &endpoint::Retention,
) -> Result<Compared> {
    let candidate = endpoint::Served::read(pool, children_page).await?;
    let incremental_families = families(pool).await?;
    let started = Instant::now();
    project.run_batch(context(target, None)).await?;
    let rebuild_ms = started.elapsed().as_millis();
    // The rebuilt batch rebuilds the owned key families from scratch; they must equal the
    // families the incremental blocks left, row for row, the marker's sequence aside.
    let families_started = Instant::now();
    project.after_progress_recorded(CHAIN).await;
    let families_rebuild_ms = families_started.elapsed().as_millis();
    let rebuilt_families = families(pool).await?;
    let differing: Vec<&str> = incremental_families
        .iter()
        .zip(&rebuilt_families)
        .filter(|(incremental, rebuilt)| incremental != rebuilt)
        .map(|((table, _), _)| table.as_str())
        .collect();
    ensure!(
        differing.is_empty(),
        "the owned key families differ from a rebuild at {}: {differing:?}",
        target.number
    );
    eprintln!(
        "SEPOLIA_END_TO_END_FAMILIES_COMPARE target={} tables={} rebuild_ms={families_rebuild_ms} \
         result=equal",
        target.number,
        rebuilt_families.len()
    );
    let rebuilt = endpoint::Served::read(pool, children_page).await?;
    let stamp = endpoint::Target::load(pool, target.number, &target.hash).await?;
    let outcome = endpoint::compare(&candidate, &rebuilt, &stamp, retention)?;
    eprintln!(
        "SEPOLIA_END_TO_END_COMPARE target={} names={} subname_rows={} subname_pages={} \
         subname_parents={} exact={} retained={} removed={} dropped={} rebuild_ms={rebuild_ms} \
         result=equal",
        target.number,
        candidate.names.len(),
        candidate.subname_rows(),
        candidate.pages_read,
        candidate.parents,
        outcome.exact,
        outcome.retained,
        outcome.removed,
        outcome.dropped,
    );
    Ok(Compared {
        target: target.number,
        outcome,
        subname_rows: candidate.subname_rows(),
        subname_pages: candidate.pages_read,
        subname_parents: candidate.parents,
    })
}

/// Every owned key family table as ordered JSON text, then the family marker without its
/// sequence, which every block and undo advances.
async fn families(pool: &PgPool) -> Result<Vec<(String, String)>> {
    let mut tables = Vec::new();
    for table in bigname_project::families::family_tables() {
        let rows: String = sqlx::query_scalar(&format!(
            "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), '[]')::text
             FROM {table} t"
        ))
        .fetch_one(pool)
        .await?;
        tables.push((table.to_owned(), rows));
    }
    let marker: Option<String> = sqlx::query_scalar(
        "SELECT (to_jsonb(m) - 'sequence')::text FROM project_family_marker m
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_optional(pool)
    .await?;
    tables.push((
        "project_family_marker".to_owned(),
        marker.unwrap_or_default(),
    ));
    Ok(tables)
}

async fn publication(pool: &PgPool) -> Result<(Option<i64>, Option<String>, bool)> {
    Ok(sqlx::query_as(
        "SELECT current_block_number, current_block_hash, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?)
}

async fn load_marker(pool: &PgPool, number: i64) -> Result<BlockMarker> {
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(CHAIN)
    .bind(number)
    .fetch_one(pool)
    .await
    .with_context(|| format!("block {number} is not readable"))?;
    Ok(BlockMarker::new(number, hash)?)
}

/// Moves the stored head up to the target, as Live does before Project follows it, and returns
/// the target's marker. It never moves the head down.
async fn follow_head(pool: &PgPool, number: i64) -> Result<BlockMarker> {
    let head: Option<i64> =
        sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_optional(pool)
            .await?;
    if head.is_some_and(|head| head >= number) {
        let head = head.unwrap_or(number);
        ensure!(
            head - number <= PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS,
            "the stored head is {head}; a publication at {number} would never be served"
        );
        return load_marker(pool, number).await;
    }
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('observed', 'canonical')",
    )
    .bind(CHAIN)
    .bind(number)
    .fetch_one(pool)
    .await
    .with_context(|| format!("block {number} has no single hash to publish"))?;
    let heads = HeadMarkers {
        latest: BlockMarker::new(number, hash)?,
        safe: None,
        finalized: None,
    };
    publish_heads(pool, CHAIN, &heads).await?;
    load_marker(pool, number).await
}

fn context(target: &BlockMarker, resume: Option<&BlockMarker>) -> PhaseContext {
    PhaseContext {
        chain_id: CHAIN.to_owned(),
        phase: PhaseName::Project,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: Vec::new().into(),
        available_heads: Some(HeadMarkers {
            latest: target.clone(),
            safe: None,
            finalized: None,
        }),
        live_handoff: None,
        resume: PhaseResume {
            current: resume.cloned(),
            ..PhaseResume::default()
        },
    }
}
