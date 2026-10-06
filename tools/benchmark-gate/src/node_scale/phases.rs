use std::time::Instant;

use anyhow::{Result, ensure};
use bigname_interpret::{BatchRequest, Engine, RunMode as InterpretMode};
use phase_runner::{
    heads::BlockMarker,
    phase::{PhaseName, PhaseProgress, RunMode},
    phase_lock::PhaseLock,
    state::{PhaseStore, StartDisposition},
};
use serde::Serialize;
use sqlx::PgPool;

use super::{manifests::CHAIN, seed::block_hash};

#[derive(Serialize)]
pub(super) struct PhaseReport {
    pub(super) phase: &'static str,
    pub(super) head: i64,
    pub(super) elapsed_seconds: f64,
    pub(super) batches: u64,
    pub(super) estimated_write_bytes: Option<u64>,
    pub(super) database_bytes_before: i64,
    pub(super) database_bytes_after: i64,
    pub(super) wal_bytes: String,
    pub(super) relations: serde_json::Value,
    pub(super) identity_counts: super::oracle::IdentityCounts,
    pub(super) project_batches: Vec<serde_json::Value>,
    pub(super) process_and_cgroup: serde_json::Value,
    pub(super) feature_gate_complete: bool,
}

fn progress(current: i64, head: i64) -> PhaseProgress {
    PhaseProgress {
        current: Some(BlockMarker {
            number: current,
            hash: block_hash(current),
        }),
        target: Some(BlockMarker {
            number: head,
            hash: block_hash(head),
        }),
        ..PhaseProgress::default()
    }
}

async fn lock(pool: &PgPool, phase: PhaseName) -> Result<PhaseLock> {
    Ok(PhaseLock::acquire(pool.connect_options().as_ref().clone(), CHAIN, phase).await?)
}

pub(super) async fn record_intake(pool: &PgPool, head: i64, previous: Option<i64>) -> Result<()> {
    let store = PhaseStore::new(pool.clone());
    store.initialize_chain(CHAIN).await?;
    let mut lock = lock(pool, PhaseName::Ingest).await?;
    if let Some(previous) = previous {
        store
            .record_progress(
                CHAIN,
                PhaseName::Ingest,
                &RunMode::Normal,
                None,
                &progress(previous, head),
            )
            .await?;
    }
    ensure!(
        matches!(
            store
                .start_phase(CHAIN, PhaseName::Ingest, &RunMode::Normal)
                .await?,
            StartDisposition::Started
        ),
        "intake phase was already used"
    );
    store
        .complete_phase_with_lock(&mut lock, CHAIN, PhaseName::Ingest, &progress(head, head))
        .await?;
    lock.release().await?;
    Ok(())
}

pub(super) async fn run(
    pool: &PgPool,
    phase: PhaseName,
    directory: &std::path::Path,
    head: i64,
) -> Result<PhaseReport> {
    let expected = super::oracle::expected(directory, head)?;
    ensure!(
        matches!(phase, PhaseName::Interpret | PhaseName::Project),
        "unsupported measured phase"
    );
    let store = PhaseStore::new(pool.clone());
    let mut lock = lock(pool, phase).await?;
    ensure!(
        matches!(
            store.start_phase(CHAIN, phase, &RunMode::Normal).await?,
            StartDisposition::Started
        ),
        "phase was already completed; use a declared replay command"
    );
    let before: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await?;
    let wal: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(pool)
        .await?;
    let started = Instant::now();
    let outcome = match phase {
        PhaseName::Interpret => interpret(pool, &store, &mut lock, head).await,
        PhaseName::Project => project(pool, &store, &mut lock, head).await,
        _ => unreachable!(),
    };
    let (batches, estimated_write_bytes, project_batches) = match outcome {
        Ok(value) => value,
        Err(error) => {
            store.fail_phase(CHAIN, phase, &error.to_string()).await?;
            lock.release().await?;
            return Err(error);
        }
    };
    store
        .complete_phase_with_lock(&mut lock, CHAIN, phase, &progress(head, head))
        .await?;
    let identity_counts = super::oracle::identities(pool, &expected).await?;
    if phase == PhaseName::Project {
        crate::indexing::publication::require_published_head(pool, CHAIN, head).await?;
    }
    let elapsed_seconds = started.elapsed().as_secs_f64();
    lock.release().await?;
    let after = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await?;
    let wal_bytes =
        sqlx::query_scalar("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::text")
            .bind(wal)
            .fetch_one(pool)
            .await?;
    let relations = sqlx::query_scalar("SELECT COALESCE(jsonb_agg(jsonb_build_object('relation',c.relname,'heap_bytes',pg_relation_size(c.oid),'indexes_bytes',pg_indexes_size(c.oid),'toast_bytes',CASE WHEN c.reltoastrelid=0 THEN 0 ELSE pg_total_relation_size(c.reltoastrelid) END,'total_bytes',pg_total_relation_size(c.oid),'estimated_rows',c.reltuples) ORDER BY c.relname),'[]'::jsonb)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='bigname_phase' AND c.relkind='r'")
        .fetch_one(pool).await?;
    Ok(PhaseReport {
        phase: phase.as_str(),
        head,
        elapsed_seconds,
        batches,
        estimated_write_bytes,
        database_bytes_before: before,
        database_bytes_after: after,
        wal_bytes,
        relations,
        identity_counts,
        project_batches,
        process_and_cgroup: process_metrics(),
        feature_gate_complete: false,
    })
}

fn process_metrics() -> serde_json::Value {
    let read = |path| std::fs::read_to_string(path).ok();
    serde_json::json!({
        "proc_self_status": read("/proc/self/status"),
        "proc_self_stat": read("/proc/self/stat"),
        "cgroup_memory_peak": read("/sys/fs/cgroup/memory.peak"),
        "cgroup_memory_max": read("/sys/fs/cgroup/memory.max"),
        "cgroup_memory_events": read("/sys/fs/cgroup/memory.events"),
        "cgroup_cpu_stat": read("/sys/fs/cgroup/cpu.stat"),
        "cgroup_cpu_max": read("/sys/fs/cgroup/cpu.max"),
        "cgroup_io_stat": read("/sys/fs/cgroup/io.stat"),
        "unavailable_is_null": true,
    })
}

async fn interpret(
    pool: &PgPool,
    store: &PhaseStore,
    lock: &mut PhaseLock,
    head: i64,
) -> Result<(u64, Option<u64>, Vec<serde_json::Value>)> {
    let engine = Engine::with_state_cache_capacity(pool.clone(), 65_536);
    let resume = store
        .phase_resume(CHAIN, PhaseName::Interpret, &RunMode::Normal)
        .await?;
    let mut resume_current = resume.current.map(|marker| bigname_interpret::Marker {
        number: marker.number,
        hash: marker.hash,
    });
    let mut batches = 0;
    let mut bytes = 0;
    loop {
        lock.check_alive().await?;
        let result = engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: 0,
                to_block: head,
                resume_current,
                mode: InterpretMode::Normal,
            })
            .await?;
        batches += 1;
        bytes += result.estimated_write_bytes;
        store
            .record_progress(
                CHAIN,
                PhaseName::Interpret,
                &RunMode::Normal,
                None,
                &progress(result.current.number, head),
            )
            .await?;
        eprintln!("node-scale interpret {}/{}", result.current.number, head);
        if result.complete {
            break;
        }
        resume_current = Some(result.current);
    }
    Ok((batches, Some(bytes), Vec::new()))
}

async fn project(
    pool: &PgPool,
    store: &PhaseStore,
    lock: &mut PhaseLock,
    head: i64,
) -> Result<(u64, Option<u64>, Vec<serde_json::Value>)> {
    use bigname_project::families::{self, FamilyMode, FamilyOptions};
    let target = bigname_project::Marker {
        number: head,
        hash: block_hash(head),
    };
    let options = FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH);
    let resume = store
        .phase_resume(CHAIN, PhaseName::Project, &RunMode::Normal)
        .await?;
    let mut mode = if resume.current.is_some() {
        FamilyMode::Normal
    } else {
        FamilyMode::Rebuild
    };
    let mut batches = 0;
    let mut details = Vec::new();
    loop {
        lock.check_alive().await?;
        let token = families::input_token(pool, CHAIN).await?;
        let outcome = families::apply(pool, CHAIN, &target, mode, &token, &options).await?;
        batches += 1;
        details.push(serde_json::json!({ "blocks": outcome.blocks, "ranges": outcome.ranges,
            "rows_by_family": outcome.rows, "undo_rows": outcome.undo_rows, "elapsed_ms": outcome.elapsed_ms,
            "statistics_refreshes": outcome.statistics_refreshes, "duplicate_anomalies": outcome.duplicate_anomalies }));
        ensure!(
            outcome.duplicate_anomalies == 0,
            "Project detected duplicate event disagreements"
        );
        if let Some(marker) = &outcome.marker {
            store
                .record_progress(
                    CHAIN,
                    PhaseName::Project,
                    &RunMode::Normal,
                    None,
                    &progress(marker.number, head),
                )
                .await?;
            eprintln!("node-scale project {}/{}", marker.number, head);
        }
        if outcome.marker.as_ref() == Some(&target) {
            break;
        }
        ensure!(
            outcome.budget_exhausted && outcome.blocks > 0,
            "Project made no progress"
        );
        mode = FamilyMode::Normal;
    }
    Ok((batches, None, details))
}
