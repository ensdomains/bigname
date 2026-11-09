//! The planner statistics of the tables the permission reads join, refreshed between Project
//! batches. The fixture is the rebuild seed without its closing `ANALYZE`: a one-block rebuild,
//! which analyzes no family table this file reads, then a batch of a new `ProjectPhase` that
//! writes the rest of the names, the way a restarted process follows the head. Autovacuum is off
//! on these tables, so only the code under test moves their statistics. `pg_class.reltuples` is
//! -1 on a table never analyzed. The last test runs the same batch through the phase runner.
#[allow(dead_code)]
mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use phase_runner::{
    INTERPRETER_CONTENT_HASH, PROJECTION_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    heads::{BlockMarker, HeadMarkers},
    phase::{
        AfterProgress, AfterProgressFuture, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext,
        PhaseFuture, PhaseName, PhaseResume, PhaseSet, RunMode,
    },
    project_phase::{FamilySettings, ProjectPhase},
    runner::PhaseRunner,
    state::PhaseStore,
};
use sqlx::PgPool;
use support::ScratchDatabase;
use tokio_util::sync::CancellationToken;

const CHAIN: &str = "ethereum-sepolia";
/// A chain the runner runs with a test source. The production chains require trusted
/// verification sources.
const RUNNER_CHAIN: &str = "project-statistics-runner";
const OTHER_CHAIN: &str = "base-mainnet";
const REBUILT_TO: i64 = 1;
const HEAD: i64 = 40;
/// The top of the seed's lineage, which the runner reads as the readable head.
const RUNNER_HEAD: i64 = 300;
const TABLES: [&str; 7] = [
    "project_grant",
    "project_account_approval",
    "project_ens_v2_entry_owner",
    "project_ens_v2_registry_parent",
    "project_registry_binding_observation",
    "project_binding_candidate",
    "project_lifecycle_key_state",
];

async fn seeded(prefix: &str) -> Result<ScratchDatabase> {
    seeded_on(prefix, CHAIN).await
}

async fn seeded_on(prefix: &str, chain: &str) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(prefix).await?;
    let seed = include_str!("../../../crates/project/tests/rebuild_performance/seed.sql")
        .replace("__NAMES__", "40")
        .replace("__CHAIN__", chain);
    let seed = seed
        .strip_suffix("ANALYZE;\n")
        .context("the seed ends with an ANALYZE of the whole database")?;
    sqlx::raw_sql(seed).execute(scratch.pool()).await?;
    for table in TABLES {
        sqlx::query(&format!(
            "ALTER TABLE {table} SET (autovacuum_enabled = false)"
        ))
        .execute(scratch.pool())
        .await?;
    }
    ProjectPhase::new(scratch.pool().clone())
        .run_batch(context_on(chain, REBUILT_TO, None)?)
        .await?;
    Ok(scratch)
}

fn block_hash(block: i64) -> String {
    format!("0x{block:064x}")
}

fn context(target: i64, resume: Option<i64>) -> Result<PhaseContext> {
    context_on(CHAIN, target, resume)
}

fn context_on(chain: &str, target: i64, resume: Option<i64>) -> Result<PhaseContext> {
    let marker = |block: i64| BlockMarker::new(block, block_hash(block));
    Ok(PhaseContext {
        chain_id: chain.to_owned(),
        phase: PhaseName::Project,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: Vec::new().into(),
        available_heads: Some(HeadMarkers {
            latest: marker(target)?,
            safe: None,
            finalized: None,
        }),
        live_handoff: None,
        resume: PhaseResume {
            current: resume.map(marker).transpose()?,
            ..PhaseResume::default()
        },
    })
}

/// A new process's batch from the rebuilt block to the head.
async fn restarted(pool: &PgPool) -> Result<Arc<ProjectPhase>> {
    let project = Arc::new(ProjectPhase::new(pool.clone()));
    project.run_batch(context(HEAD, Some(REBUILT_TO))?).await?;
    Ok(project)
}

/// The table's row estimate and its rows, for each table that holds a row.
async fn estimates(pool: &PgPool) -> Result<Vec<(&'static str, i64, i64)>> {
    let mut estimates = Vec::new();
    for table in TABLES {
        let (estimate, rows): (i64, i64) = sqlx::query_as(&format!(
            "SELECT (SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass('{table}')),
                    (SELECT count(*) FROM {table})"
        ))
        .fetch_one(pool)
        .await?;
        if rows > 0 {
            estimates.push((table, estimate, rows));
        }
    }
    ensure!(
        estimates
            .iter()
            .any(|(table, ..)| *table == "project_grant"),
        "the seed writes the role grants: {estimates:?}"
    );
    Ok(estimates)
}

async fn run_due_refreshes(project: &ProjectPhase) -> Result<()> {
    while project.has_after_progress_work(CHAIN) {
        project.after_progress_recorded(CHAIN).await?;
    }
    Ok(())
}

async fn ensure_refreshed(pool: &PgPool) -> Result<()> {
    let estimates = estimates(pool).await?;
    ensure!(
        estimates.iter().all(|(_, estimate, rows)| estimate == rows),
        "every written table's statistics hold its rows: {estimates:?}"
    );
    Ok(())
}

/// Holds the lock `ANALYZE` needs on every table, so a refresh waits inside the statement.
async fn hold_analyze_locks(pool: &PgPool) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut transaction = pool.begin().await?;
    sqlx::query(&format!(
        "LOCK TABLE {} IN SHARE UPDATE EXCLUSIVE MODE",
        TABLES.join(", ")
    ))
    .execute(&mut *transaction)
    .await?;
    Ok(transaction)
}

async fn waiting_analyze_statements(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND pid <> pg_backend_pid()
           AND wait_event_type = 'Lock' AND query LIKE '%ANALYZE%'",
    )
    .fetch_one(pool)
    .await?)
}

/// Starts a refresh in a task and returns once its `ANALYZE` waits on the held lock. Well inside
/// the statement's 10-second lock timeout.
async fn refresh_blocked_on_its_lock(
    project: &Arc<ProjectPhase>,
    pool: &PgPool,
) -> Result<tokio::task::JoinHandle<Result<AfterProgress>>> {
    let running = Arc::clone(project);
    let refresh = tokio::spawn(async move { Ok(running.after_progress_recorded(CHAIN).await?) });
    tokio::time::timeout(Duration::from_secs(5), async {
        while waiting_analyze_statements(pool).await? == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(refresh)
}

#[tokio::test]
async fn a_table_never_analyzed_is_refreshed_after_the_first_write() -> Result<()> {
    let scratch = seeded("project_statistics_never_analyzed").await?;
    let project = restarted(scratch.pool()).await?;
    let before = estimates(scratch.pool()).await?;
    ensure!(
        before.iter().all(|(_, estimate, _)| *estimate == -1),
        "no table of the fixture has been analyzed: {before:?}"
    );
    ensure!(
        project.has_after_progress_work(CHAIN),
        "a table never analyzed was written"
    );
    run_due_refreshes(&project).await?;
    ensure_refreshed(scratch.pool()).await?;
    ensure!(
        !project.has_after_progress_work(CHAIN),
        "nothing is due once every written table is refreshed"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn statistics_older_than_the_process_are_refreshed_after_the_first_write() -> Result<()> {
    let scratch = seeded("project_statistics_stale").await?;
    for table in TABLES {
        sqlx::query(&format!("ANALYZE {table}"))
            .execute(scratch.pool())
            .await?;
    }
    let project = restarted(scratch.pool()).await?;
    let before = estimates(scratch.pool()).await?;
    ensure!(
        before
            .iter()
            .all(|(_, estimate, rows)| *estimate >= 0 && estimate < rows),
        "every table's statistics pre-date the batch: {before:?}"
    );
    ensure!(
        project.has_after_progress_work(CHAIN),
        "tables with stale statistics were written"
    );
    run_due_refreshes(&project).await?;
    ensure_refreshed(scratch.pool()).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn one_refresh_runs_at_a_time_and_another_chain_does_not_wait_for_it() -> Result<()> {
    let scratch = seeded("project_statistics_bound").await?;
    let project = restarted(scratch.pool()).await?;
    let locks = hold_analyze_locks(scratch.pool()).await?;
    let refresh = refresh_blocked_on_its_lock(&project, scratch.pool()).await?;

    ensure!(
        !project.has_after_progress_work(OTHER_CHAIN),
        "the other chain has nothing to do while a refresh runs"
    );
    let step = tokio::time::timeout(
        Duration::from_secs(2),
        project.after_progress_recorded(OTHER_CHAIN),
    )
    .await??;
    ensure!(step == AfterProgress::Done, "{step:?}");
    ensure!(
        waiting_analyze_statements(scratch.pool()).await? == 1,
        "the other chain's call started no statement"
    );

    locks.rollback().await?;
    refresh.await??;
    run_due_refreshes(&project).await?;
    ensure_refreshed(scratch.pool()).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn a_stop_during_a_refresh_leaves_the_table_due() -> Result<()> {
    let scratch = seeded("project_statistics_stop").await?;
    let project = restarted(scratch.pool()).await?;
    let locks = hold_analyze_locks(scratch.pool()).await?;
    let refresh = refresh_blocked_on_its_lock(&project, scratch.pool()).await?;

    // The runner's stop drops the call the same way.
    refresh.abort();
    ensure!(refresh.await.is_err_and(|error| error.is_cancelled()));
    ensure!(
        project.has_after_progress_work(CHAIN),
        "the dropped call gave its permit back and refreshed nothing"
    );

    locks.rollback().await?;
    run_due_refreshes(&project).await?;
    ensure_refreshed(scratch.pool()).await?;
    scratch.cleanup().await
}

/// Project, raising the stop once a batch completes, and passing the follow-up work on.
struct StopOnCompleteBatch {
    inner: Arc<ProjectPhase>,
    stop: CancellationToken,
    completed: AtomicBool,
    batches: AtomicUsize,
}

impl Phase for StopOnCompleteBatch {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.batches.fetch_add(1, Ordering::SeqCst);
            let outcome = self.inner.run_batch(context).await?;
            if matches!(outcome, PhaseBatchOutcome::Complete(_)) {
                self.completed.store(true, Ordering::SeqCst);
                self.stop.cancel();
            }
            Ok(outcome)
        })
    }

    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        self.inner.after_progress_recorded(chain_id)
    }

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.inner.has_after_progress_work(chain_id)
    }
}

fn runner_chain() -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        RUNNER_CHAIN,
        vec![SourceConfig::new(
            RUNNER_CHAIN,
            "source",
            "test",
            SeedBasis::NewSignatureRange,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}

/// Ingest and Interpret completed through the top of the lineage, the heads published there, and
/// Project completed at the rebuilt block, where the family marker stands.
async fn ready_for_the_runner(pool: &PgPool) -> Result<()> {
    PhaseStore::new(pool.clone())
        .initialize_chain(RUNNER_CHAIN)
        .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(RUNNER_CHAIN)
    .bind(block_hash(RUNNER_HEAD))
    .bind(RUNNER_HEAD)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = CASE phase_name WHEN 'project' THEN $4 ELSE $2 END,
             current_block_hash = CASE phase_name WHEN 'project' THEN $5 ELSE $3 END,
             target_block_number = CASE phase_name WHEN 'project' THEN $4 ELSE $2 END,
             target_block_hash = CASE phase_name WHEN 'project' THEN $5 ELSE $3 END,
             live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
             input_content_hash = CASE phase_name WHEN 'project' THEN $7
                 WHEN 'interpret' THEN $6 END,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(RUNNER_CHAIN)
    .bind(RUNNER_HEAD)
    .bind(block_hash(RUNNER_HEAD))
    .bind(REBUILT_TO)
    .bind(block_hash(REBUILT_TO))
    .bind(INTERPRETER_CONTENT_HASH)
    .bind(PROJECTION_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0, $2, $3, $3, $4)",
    )
    .bind(RUNNER_CHAIN)
    .bind(RUNNER_HEAD + 1)
    .bind(RUNNER_HEAD)
    .bind(block_hash(RUNNER_HEAD))
    .execute(pool)
    .await?;
    Ok(())
}

// A stop that arrives once a normal Project batch has completed, while a refresh is due. The
// runner records the batch's progress, then its after-batch check sees the waiting refresh and
// the stop, abandons both and reports the run cancelled. So the batch is not reported complete
// and the phase stays running until the next start, the same as for Interpret's refresh. With
// nothing waiting the same stop would let the phase complete.
#[tokio::test]
async fn a_stop_after_a_completed_batch_with_a_refresh_due_leaves_the_phase_running() -> Result<()>
{
    let scratch = seeded_on("project_statistics_runner_stop", RUNNER_CHAIN).await?;
    ready_for_the_runner(scratch.pool()).await?;
    let stop = CancellationToken::new();
    // One batch reaches the head, so the batch that writes the tables is the one that completes.
    let inner = Arc::new(
        ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
            max_blocks_per_run: 512,
            ..FamilySettings::default()
        }),
    );
    let project = Arc::new(StopOnCompleteBatch {
        inner: Arc::clone(&inner),
        stop: stop.clone(),
        completed: AtomicBool::new(false),
        batches: AtomicUsize::new(0),
    });
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            project.clone(),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "project-statistics-runner",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let chain = runner_chain()?;
    let logs = support::CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(logs.clone())
        .finish();
    let run = tokio::time::timeout(
        Duration::from_secs(45),
        tracing::instrument::WithSubscriber::with_subscriber(
            runner.run_chain(&chain, stop.clone()),
            subscriber,
        ),
    )
    .await;
    let text = logs.text();
    let lines: Vec<&str> = text.lines().collect();
    let head: Vec<String> = lines.iter().take(150).map(|line| line.chars().take(400).collect()).collect();
    let tail: Vec<String> = lines
        .iter()
        .skip(lines.len().saturating_sub(150))
        .map(|line| line.chars().take(400).collect())
        .collect();
    eprintln!("DIAG {} log lines; first:\n{}\nDIAG last:\n{}", lines.len(), head.join("\n"), tail.join("\n"));
    let (status, current): (String, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, current_block_number FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(RUNNER_CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    eprintln!(
        "DIAG project {status} {current:?} batches {} completed {}",
        project.batches.load(Ordering::SeqCst),
        project.completed.load(Ordering::SeqCst)
    );
    let states: Vec<(String, String, Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT phase_name, phase_status, current_block_number, failure_reason FROM chain_phase_state
         WHERE chain_id = $1 ORDER BY phase_name",
    )
    .bind(RUNNER_CHAIN)
    .fetch_all(scratch.pool())
    .await
    .unwrap_or_default();
    eprintln!("DIAG states {states:?}");
    run.context("the chain run did not stop within 45 seconds")??;

    ensure!(
        project.batches.load(Ordering::SeqCst) == 1 && project.completed.load(Ordering::SeqCst),
        "one Project batch ran and completed"
    );
    let (status, current): (String, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, current_block_number FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(RUNNER_CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    ensure!(
        current == Some(RUNNER_HEAD),
        "the completed batch's progress is recorded: {current:?}"
    );
    ensure!(
        status == "running",
        "the run is reported cancelled, so the phase stays running: {status}"
    );
    ensure!(
        inner.has_after_progress_work(RUNNER_CHAIN),
        "the refresh is still due for the next start"
    );
    let analyzed: i64 = sqlx::query_scalar(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass('project_grant')",
    )
    .fetch_one(scratch.pool())
    .await?;
    ensure!(analyzed == -1, "no refresh ran: {analyzed}");
    scratch.cleanup().await
}
