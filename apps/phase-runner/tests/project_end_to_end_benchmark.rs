//! End-to-end Project benchmark: each target runs as one committed Normal batch through the
//! runner's Project phase, so the engine commit, hydration and the progress marker all happen,
//! and the clock stops only when a storage reader serves the new publication. The rollback
//! benchmark in `bigname-project` stops before commit and never writes the marker; this one
//! measures what a client waits for.
//!
//! It commits, so it runs only against a disposable copy carrying the marker of
//! docs/runbooks/benchmark-gate.md. The copy must be paused with Project published at
//! `BIGNAME_BENCHMARK_PREVIOUS`. A target above the stored head moves the head there first, as
//! Live does; a target below it must be within the publication lag tolerance, because a
//! publication further behind the head is never served. On a paused copy that means one target at
//! its head, unless the copy was rewound.
//!
//! With `BIGNAME_END_TO_END_COMPARE=1` each target is then rebuilt from scratch and committed at
//! the same block on the same copy, and the name and subname readers must serve the same rows
//! for every name the batch rewrote. The next target continues from that rebuilt state.
#[allow(dead_code)]
mod support;

use std::{str::FromStr, time::Instant};

use anyhow::{Context, Result, ensure};
use bigname_lookup::ChainRpcUrls;
use bigname_storage::{
    ChildrenCurrentPageFilter, PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS,
    load_children_current_page_filtered, load_name_current, load_name_current_by_logical_name_ids,
    load_served_project_generation,
};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    heads::{BlockMarker, HeadMarkers, publish_heads},
    phase::{Phase, PhaseContext, PhaseName, PhaseResume, RunMode},
    project_phase::ProjectPhase,
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use support::ScratchDatabase;

const CHAIN: &str = "ethereum-sepolia";
const CHILDREN_PAGE: u64 = 1_000;

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
    let compare = std::env::var("BIGNAME_END_TO_END_COMPARE").as_deref() == Ok("1");
    run(&pool, previous, &targets, compare).await?;
    pool.close().await;
    Ok(())
}

/// The same mode on the rebuild-performance seed, so it keeps working and can be timed locally:
/// `BIGNAME_END_TO_END_FIXTURE_NAMES`, `_PREVIOUS` and `_TARGETS` size it (defaults 40, 30 and
/// 35,40).
#[tokio::test]
async fn fixture_corpus_publishes_hydrates_reads_and_matches_a_rebuild() -> Result<()> {
    let setting = |name: &str, default: &str| {
        std::env::var(format!("BIGNAME_END_TO_END_FIXTURE_{name}"))
            .unwrap_or_else(|_| default.to_owned())
    };
    let names: u32 = setting("NAMES", "40").parse()?;
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
    ensure!(
        seed.contains("parent_hash") && seed.contains("block > 1"),
        "the seed lineage changed shape"
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
    run(pool, previous, &targets, true).await?;
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

async fn run(pool: &PgPool, previous: i64, targets: &[i64], compare: bool) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("bigname_project::batch=info")
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
    // The runner starts the phase once and then runs batches; starting sets the running status
    // and this binary's interpreter hash, which the publication fence requires.
    store
        .start_phase(CHAIN, PhaseName::Project, &RunMode::Normal)
        .await?;
    let project = ProjectPhase::with_hydration(pool.clone(), ChainRpcUrls::default());
    for &number in targets {
        let target = follow_head(pool, number).await?;
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
        if compare {
            compare_with_rebuild(pool, &project, &target, &rewritten).await?;
        }
        resume = target;
    }
    Ok(())
}

/// Reads every rewritten name and its subname page, rebuilds the target from scratch and commits
/// it, and requires the same rows again. A name the batch rewrote must match exactly; a subname
/// row the batch kept may differ only in the target block it names, the one difference a kept
/// row is allowed.
async fn compare_with_rebuild(
    pool: &PgPool,
    project: &ProjectPhase,
    target: &BlockMarker,
    rewritten: &[String],
) -> Result<()> {
    let candidate = endpoint_rows(pool, rewritten).await?;
    let candidate_keys = name_keys(pool).await?;
    let started = Instant::now();
    project.run_batch(context(target, None)).await?;
    let rebuild_ms = started.elapsed().as_millis();
    let reference = endpoint_rows(pool, rewritten).await?;
    ensure!(
        candidate_keys == name_keys(pool).await?,
        "the batch left a different set of names than a rebuild at {}",
        target.number
    );
    ensure!(candidate.names == reference.names, "name rows differ");
    let mut retained = 0;
    for ((parent, candidate), (_, reference)) in candidate.children.iter().zip(&reference.children)
    {
        ensure!(
            candidate.len() == reference.len(),
            "subname pages of {parent} differ in length"
        );
        for (kept, rebuilt) in candidate.iter().zip(reference) {
            if kept == rebuilt {
                continue;
            }
            let mut refreshed = kept.clone();
            refreshed["chain_positions"]["target_block_number"] = json!(target.number);
            refreshed["chain_positions"]["target_block_hash"] = json!(target.hash);
            ensure!(
                &refreshed == rebuilt,
                "subname row under {parent} differs from the rebuild: {kept} != {rebuilt}"
            );
            retained += 1;
        }
    }
    eprintln!(
        "SEPOLIA_END_TO_END_COMPARE target={} names={} name_keys={} subname_rows={} \
         retained={retained} rebuild_ms={rebuild_ms} result=equal",
        target.number,
        candidate.names.len(),
        candidate_keys.len(),
        candidate
            .children
            .iter()
            .map(|(_, rows)| rows.len())
            .sum::<usize>()
    );
    Ok(())
}

struct EndpointRows {
    names: Vec<String>,
    children: Vec<(String, Vec<Value>)>,
}

async fn endpoint_rows(pool: &PgPool, rewritten: &[String]) -> Result<EndpointRows> {
    let names = load_name_current_by_logical_name_ids(pool, rewritten)
        .await?
        .into_values()
        .map(|mut row| {
            row.last_recomputed_at = sqlx::types::time::OffsetDateTime::UNIX_EPOCH;
            format!("{row:?}")
        })
        .collect();
    let mut children = Vec::new();
    for parent in rewritten {
        let page = load_children_current_page_filtered(
            pool,
            parent,
            &ChildrenCurrentPageFilter::default(),
            None,
            CHILDREN_PAGE,
        )
        .await?;
        let rows = page
            .rows
            .into_iter()
            .map(|row| {
                json!({
                    "child": row.child_logical_name_id,
                    "surface_class": row.surface_class,
                    "canonical_display_name": row.canonical_display_name,
                    "labelhash": row.labelhash,
                    "owner": row.owner,
                    "registrant": row.registrant,
                    "provenance": row.provenance,
                    "chain_positions": row.chain_positions,
                    "canonicality_summary": row.canonicality_summary,
                    "manifest_version": row.manifest_version,
                })
            })
            .collect();
        children.push((parent.clone(), rows));
    }
    Ok(EndpointRows { names, children })
}

async fn name_keys(pool: &PgPool) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar("SELECT logical_name_id FROM name_current ORDER BY logical_name_id")
            .fetch_all(pool)
            .await?,
    )
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
