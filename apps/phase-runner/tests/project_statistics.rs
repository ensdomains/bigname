//! The planner statistics of the tables the permission reads join, refreshed between Project
//! batches. The fixture is the rebuild seed without its closing `ANALYZE`: a one-block rebuild,
//! which analyzes no family table this file reads, then a batch of a new `ProjectPhase` that
//! writes the rest of the names, the way a restarted process follows the head. Autovacuum is off
//! on these tables, so only the code under test moves their statistics. `pg_class.reltuples` is
//! -1 on a table never analyzed.
#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use phase_runner::{
    heads::{BlockMarker, HeadMarkers},
    phase::{AfterProgress, Phase, PhaseContext, PhaseName, PhaseResume, RunMode},
    project_phase::ProjectPhase,
};
use sqlx::PgPool;
use support::ScratchDatabase;

const CHAIN: &str = "ethereum-sepolia";
const OTHER_CHAIN: &str = "base-mainnet";
const REBUILT_TO: i64 = 1;
const HEAD: i64 = 40;
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
    let scratch = ScratchDatabase::create(prefix).await?;
    let seed = include_str!("../../../crates/project/tests/rebuild_performance/seed.sql")
        .replace("__NAMES__", "40")
        .replace("__CHAIN__", CHAIN);
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
        .run_batch(context(REBUILT_TO, None)?)
        .await?;
    Ok(scratch)
}

fn context(target: i64, resume: Option<i64>) -> Result<PhaseContext> {
    let marker = |block: i64| BlockMarker::new(block, format!("0x{block:064x}"));
    Ok(PhaseContext {
        chain_id: CHAIN.to_owned(),
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
