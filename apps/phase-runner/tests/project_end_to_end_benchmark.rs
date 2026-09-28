//! Permanent family publication corpus. The real Project phase publishes each target, then
//! all registered family rows and the route readers' name/subname values are compared strictly
//! with a rebuild at that same target. No served-table oracle or retained-row allowance remains.
//! Disposable-copy mode still requires the existing explicit fresh copy marker before writing.
#[path = "project_end_to_end/endpoint.rs"]
mod endpoint;
#[path = "project_end_to_end/families_mode.rs"]
mod families_mode;
#[path = "project_end_to_end/served_batch.rs"]
mod family_metrics;
#[allow(dead_code)]
mod support;

use std::{str::FromStr, time::Instant};

use anyhow::{Context, Result, ensure};
use bigname_lookup::ChainRpcUrls;
use bigname_project::families::RebuildRanges;
use bigname_storage::{PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS, load_served_project_generation};
use phase_runner::{
    INTERPRETER_CONTENT_HASH, RunnerPhaseProgress,
    heads::{BlockMarker, HeadMarkers, publish_heads},
    metrics::{RunnerLoopHeartbeat, RunnerMetricsFeed},
    phase::{Phase, PhaseBatchOutcome, PhaseContext, PhaseName, PhaseResume, RunMode},
    project_phase::{FamilySettings, ProjectPhase},
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
    run(&pool, previous, &targets, compare, false).await?;
    pool.close().await;
    Ok(())
}

async fn seed_fixture() -> Result<(ScratchDatabase, i64, Vec<i64>)> {
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
    Ok((scratch, previous, targets))
}

#[tokio::test]
async fn fixture_corpus_publishes_hydrates_reads_and_matches_a_rebuild() -> Result<()> {
    let (scratch, previous, targets) = seed_fixture().await?;
    let compared = run(
        scratch.pool(),
        previous,
        &targets,
        Some(FIXTURE_CHILDREN_PAGE),
        true,
    )
    .await?;
    ensure!(compared == targets.len(), "every target must be compared");
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
    let project = ProjectPhase::new(pool.clone()).with_family_settings(FamilySettings {
        rebuild_ranges: RebuildRanges::Through(i64::MAX),
        ..FamilySettings::default()
    });
    let outcome = project.run_batch(context(&marker, None)).await?;
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

async fn run(
    pool: &PgPool,
    previous: i64,
    targets: &[i64],
    compare: Option<u64>,
    corpus: bool,
) -> Result<usize> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("bigname_project::families=info")
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
    ensure!(
        publication(pool).await? == (Some(previous), Some(resume.hash.clone()), false),
        "Project is not published at the requested previous marker"
    );
    let store = PhaseStore::new(pool.clone());
    store
        .start_phase(CHAIN, PhaseName::Project, &RunMode::Normal)
        .await?;
    let metrics_feed = RunnerMetricsFeed::default();
    let project = ProjectPhase::with_hydration(pool.clone(), ChainRpcUrls::default())
        .with_family_settings(FamilySettings {
            rebuild_ranges: if corpus {
                RebuildRanges::Through(i64::MAX)
            } else {
                RebuildRanges::BelowSafe
            },
            ..FamilySettings::default()
        })
        .with_metrics_feed(metrics_feed.clone());
    let metrics_stop = tokio_util::sync::CancellationToken::new();
    let metrics_address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        pool.clone(),
        metrics_stop.clone(),
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
        metrics_feed.clone(),
    )
    .await?;
    let _metrics_stop = metrics_stop.drop_guard();
    let mut block_seconds = family_metrics::FamilyBlockSeconds::default();
    let mut compared = 0;
    for &number in targets {
        let target = follow_head(pool, number).await?;
        let started = Instant::now();
        let mut batch_resume = Some(resume.clone());
        loop {
            let outcome = project
                .run_batch(context(&target, batch_resume.as_ref()))
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
            metrics_feed.batch_committed();
            match outcome {
                PhaseBatchOutcome::Continue(progress) => {
                    batch_resume =
                        Some(progress.current.context("a continued batch has progress")?);
                }
                _ => break,
            }
        }
        ensure!(
            load_served_project_generation(pool, CHAIN, number, &target.hash, true, true)
                .await?
                .is_some(),
            "the family publication is not servable at {number}"
        );
        ensure!(
            publication(pool).await? == (Some(number), Some(target.hash.clone()), false),
            "the runner progress is not at target {number}"
        );
        eprintln!(
            "SEPOLIA_END_TO_END target={number} elapsed_ms={} commit=included hydration=invoked marker=written source=families",
            started.elapsed().as_millis()
        );
        let before = block_seconds;
        block_seconds =
            family_metrics::FamilyBlockSeconds::scrape(metrics_address, CHAIN, before).await?;
        eprintln!("{}", block_seconds.line(before, number));
        let (page_size, max_pages) = if corpus {
            (FIXTURE_CHILDREN_PAGE, u64::MAX)
        } else {
            (COPY_CHILDREN_PAGE, 20)
        };
        let (search, expiring) =
            families_mode::measure_walks(pool, number, CHAIN, "ens", page_size, max_pages).await?;
        ensure!(
            search.rows > 0 && expiring.rows > 0,
            "the composed listings served nothing at {number}"
        );
        if corpus {
            ensure!(
                !search.capped && !expiring.capped,
                "fixture walks must complete"
            );
        }
        if let Some(children_page) = compare {
            compare_with_rebuild(pool, &project, &target, children_page, corpus).await?;
            compared += 1;
        }
        resume = target;
    }
    Ok(compared)
}

async fn compare_with_rebuild(
    pool: &PgPool,
    project: &ProjectPhase,
    target: &BlockMarker,
    children_page: u64,
    corpus: bool,
) -> Result<()> {
    let candidate = endpoint::Served::read(pool, children_page).await?;
    ensure!(
        !candidate.names.is_empty(),
        "no names read at {}",
        target.number
    );
    if corpus {
        ensure!(
            candidate.subname_rows() > 0 && candidate.pages_read > candidate.parents,
            "the fixture must traverse nonempty subname pages"
        );
    }
    let incremental = families(pool).await?;
    let started = Instant::now();
    let token = bigname_project::families::input_token(pool, CHAIN).await?;
    let mut options = bigname_project::families::FamilyOptions::new(INTERPRETER_CONTENT_HASH);
    options.max_blocks_per_run = 0;
    let reset = bigname_project::families::apply(
        pool,
        CHAIN,
        &bigname_project::Marker {
            number: target.number,
            hash: target.hash.clone(),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &options,
    )
    .await?;
    ensure!(
        reset.reset && reset.marker.is_none(),
        "the comparison must start from an actual empty rebuild"
    );
    let mut resume = None;
    while let PhaseBatchOutcome::Continue(progress) =
        project.run_batch(context(target, resume.as_ref())).await?
    {
        resume = progress.current;
    }
    let rebuilt = families(pool).await?;
    ensure!(
        incremental == rebuilt,
        "family rows differ after rebuild at {}",
        target.number
    );
    let rebuilt_reads = endpoint::Served::read(pool, children_page).await?;
    ensure!(
        candidate.names == rebuilt_reads.names,
        "name reader values differ at {}",
        target.number
    );
    ensure!(
        candidate.children == rebuilt_reads.children,
        "subname reader values differ at {}",
        target.number
    );
    eprintln!(
        "SEPOLIA_END_TO_END_COMPARE target={} tables={} names={} subname_rows={} subname_pages={} rebuild_ms={} result=equal",
        target.number,
        rebuilt.len(),
        candidate.names.len(),
        candidate.subname_rows(),
        candidate.pages_read,
        started.elapsed().as_millis()
    );
    Ok(())
}

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
