use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, Marker as InterpretMarker,
    RunMode as InterpretMode,
};
use bigname_lookup::ChainRpcUrls;
use bigname_project::{
    BatchRequest as ProjectRequest, Engine as ProjectEngine, Marker as ProjectMarker,
    RunMode as ProjectMode,
};
use serde::Serialize;
use sqlx::PgPool;

use crate::{budgets::GateBudgets, database};

const PROJECTION_NAME_COUNT_SQL: &str = "SELECT count(*) FROM name_current WHERE provenance ->> 'chain_id' = $1 AND support_status = 'supported'";
const PROC_SELF_STATUS: &str = "/proc/self/status";
const PROC_SELF_CLEAR_REFS: &str = "/proc/self/clear_refs";
static HWM_RESET_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug)]
pub struct IndexingInput {
    pub chain_id: String,
    pub head_block: i64,
    pub walk_from_block: i64,
    pub walk_to_block: i64,
    pub hydration_rpc_urls: Option<ChainRpcUrls>,
}

#[derive(Clone, Debug, Serialize)]
pub struct IndexingReport {
    pub preflight_passed: bool,
    pub database_instance_identity: String,
    pub postflight_database_instance_identity: String,
    pub chain_id: String,
    pub head_block: i64,
    pub walk_from_block: i64,
    pub walk_to_block: i64,
    pub min_walk_blocks: u64,
    pub raw_logs: i64,
    pub raw_logs_per_1000_blocks: f64,
    pub min_raw_logs_per_1000_blocks: u64,
    pub pre_rebuild_name_current_rows: u64,
    pub post_rebuild_name_current_rows: u64,
    pub min_name_current_rows: u64,
    pub project_tick_ms: u128,
    pub project_tick_max_ms: u64,
    pub project_tick_hydration_updated_rows: usize,
    pub project_rebuild_seconds: f64,
    pub project_rebuild_max_seconds: u64,
    pub project_rebuild_completed: bool,
    pub project_hydration_updated_rows: usize,
    pub interpret_walk_seconds: f64,
    pub interpret_blocks_per_hour: f64,
    pub interpret_min_blocks_per_hour: u64,
    pub interpret_peak_rss_mib: f64,
    pub interpret_kernel_hwm_rss_mib: f64,
    pub interpret_sampled_peak_rss_mib: f64,
    pub interpret_max_peak_rss_mib: u64,
    pub interpret_state_cache_entries: usize,
    pub green: bool,
    pub failures: Vec<String>,
}

pub async fn run(
    pool: &PgPool,
    input: &IndexingInput,
    budgets: &GateBudgets,
) -> Result<IndexingReport> {
    let database_instance_identity = database::database_instance_identity(pool).await?;
    validate_input(pool, input, budgets).await?;
    let name_current_rows = projection_name_count(pool, &input.chain_id).await?;
    if name_current_rows < budgets.project_min_name_current_rows {
        let postflight_database_instance_identity =
            database::database_instance_identity(pool).await?;
        return Ok(scale_failure_report(
            input,
            budgets,
            name_current_rows,
            database_instance_identity,
            postflight_database_instance_identity,
        ));
    }
    let walk_blocks = input.walk_to_block - input.walk_from_block + 1;
    let raw_logs: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM raw_logs log
         JOIN chain_lineage lineage
           ON lineage.chain_id = log.chain_id
          AND lineage.block_hash = log.block_hash
          AND lineage.block_number = log.block_number
         WHERE log.chain_id = $1
           AND log.block_number BETWEEN $2 AND $3
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(&input.chain_id)
    .bind(input.walk_from_block)
    .bind(input.walk_to_block)
    .fetch_one(pool)
    .await
    .context("failed to count dense-era raw logs")?;
    let density = raw_logs as f64 * 1_000.0 / walk_blocks as f64;

    let tick_from = input.head_block;
    let resume = project_marker(pool, &input.chain_id, tick_from - 1).await?;
    let tick_started = Instant::now();
    let project_tick_hydration_updated_rows = tokio::time::timeout(
        Duration::from_millis(budgets.project_tick_max_ms.saturating_mul(2)),
        run_project_tick(pool, input, tick_from, resume),
    )
    .await
    .context("incremental projection tick and canonical-head hydration exceeded twice their release budget")??;
    let project_tick_ms = tick_started.elapsed().as_millis();

    let interpret_walk =
        run_interpret_walk(pool, input, budgets.interpret_state_cache_entries).await?;
    let interpret_walk_seconds = interpret_walk.elapsed_seconds;
    let peak_rss_mib = interpret_walk.budget_peak_rss_mib;
    let interpret_blocks_per_hour =
        walk_blocks as f64 * 3_600.0 / interpret_walk_seconds.max(0.000_001);

    let rebuild_started = Instant::now();
    let rebuild_result = tokio::time::timeout(
        Duration::from_secs(budgets.project_rebuild_max_seconds),
        run_full_project_rebuild(pool, input),
    )
    .await;
    let project_rebuild_seconds = rebuild_started.elapsed().as_secs_f64();
    let (project_hydration_updated_rows, project_rebuild_completed) = match rebuild_result {
        Ok(result) => (result?, true),
        Err(_) => (0, false),
    };
    let post_rebuild_name_current_rows = projection_name_count(pool, &input.chain_id).await?;
    let postflight_database_instance_identity = database::database_instance_identity(pool).await?;

    let mut failures = Vec::new();
    failures.extend(database_instance_identity_failures(
        &database_instance_identity,
        &postflight_database_instance_identity,
    ));
    failures.extend(projection_scale_failures(
        name_current_rows,
        post_rebuild_name_current_rows,
        budgets.project_min_name_current_rows,
    ));
    if density < budgets.dense_min_raw_logs_per_1000_blocks as f64 {
        failures.push(format!(
            "walk density {density:.1} raw logs/1000 blocks is below {:.1}",
            budgets.dense_min_raw_logs_per_1000_blocks
        ));
    }
    if project_tick_ms > u128::from(budgets.project_tick_max_ms) {
        failures.push(format!(
            "incremental projection tick took {project_tick_ms}ms; budget is {}ms",
            budgets.project_tick_max_ms
        ));
    }
    if !project_rebuild_completed {
        failures.push(format!(
            "full projection rebuild did not complete within {}s",
            budgets.project_rebuild_max_seconds
        ));
    } else if project_rebuild_seconds > budgets.project_rebuild_max_seconds as f64 {
        failures.push(format!(
            "full projection rebuild took {project_rebuild_seconds:.3}s; budget is {}s",
            budgets.project_rebuild_max_seconds
        ));
    }
    if interpret_blocks_per_hour < budgets.interpret_min_blocks_per_hour as f64 {
        failures.push(format!(
            "Interpret walk achieved {interpret_blocks_per_hour:.0} blocks/hour; floor is {}",
            budgets.interpret_min_blocks_per_hour
        ));
    }
    if peak_rss_mib > budgets.interpret_max_peak_rss_mib as f64 {
        failures.push(format!(
            "Interpret walk peaked at {peak_rss_mib:.1} MiB RSS; budget is {} MiB",
            budgets.interpret_max_peak_rss_mib
        ));
    }

    Ok(IndexingReport {
        preflight_passed: true,
        database_instance_identity,
        postflight_database_instance_identity,
        chain_id: input.chain_id.clone(),
        head_block: input.head_block,
        walk_from_block: input.walk_from_block,
        walk_to_block: input.walk_to_block,
        min_walk_blocks: budgets.interpret_min_walk_blocks,
        raw_logs,
        raw_logs_per_1000_blocks: density,
        min_raw_logs_per_1000_blocks: budgets.dense_min_raw_logs_per_1000_blocks,
        pre_rebuild_name_current_rows: name_current_rows,
        post_rebuild_name_current_rows,
        min_name_current_rows: budgets.project_min_name_current_rows,
        project_tick_ms,
        project_tick_max_ms: budgets.project_tick_max_ms,
        project_tick_hydration_updated_rows,
        project_rebuild_seconds,
        project_rebuild_max_seconds: budgets.project_rebuild_max_seconds,
        project_rebuild_completed,
        project_hydration_updated_rows,
        interpret_walk_seconds,
        interpret_blocks_per_hour,
        interpret_min_blocks_per_hour: budgets.interpret_min_blocks_per_hour,
        interpret_peak_rss_mib: peak_rss_mib,
        interpret_kernel_hwm_rss_mib: interpret_walk.kernel_hwm_rss_mib,
        interpret_sampled_peak_rss_mib: interpret_walk.sampled_peak_rss_mib,
        interpret_max_peak_rss_mib: budgets.interpret_max_peak_rss_mib,
        interpret_state_cache_entries: budgets.interpret_state_cache_entries,
        green: failures.is_empty(),
        failures,
    })
}

fn scale_failure_report(
    input: &IndexingInput,
    budgets: &GateBudgets,
    name_current_rows: u64,
    database_instance_identity: String,
    postflight_database_instance_identity: String,
) -> IndexingReport {
    let mut failures = vec![format!(
        "name_current has {name_current_rows} rows; release profile requires {} before projection benchmarking",
        budgets.project_min_name_current_rows
    )];
    failures.extend(database_instance_identity_failures(
        &database_instance_identity,
        &postflight_database_instance_identity,
    ));
    IndexingReport {
        preflight_passed: false,
        database_instance_identity,
        postflight_database_instance_identity,
        chain_id: input.chain_id.clone(),
        head_block: input.head_block,
        walk_from_block: input.walk_from_block,
        walk_to_block: input.walk_to_block,
        min_walk_blocks: budgets.interpret_min_walk_blocks,
        raw_logs: 0,
        raw_logs_per_1000_blocks: 0.0,
        min_raw_logs_per_1000_blocks: budgets.dense_min_raw_logs_per_1000_blocks,
        pre_rebuild_name_current_rows: name_current_rows,
        post_rebuild_name_current_rows: 0,
        min_name_current_rows: budgets.project_min_name_current_rows,
        project_tick_ms: 0,
        project_tick_max_ms: budgets.project_tick_max_ms,
        project_tick_hydration_updated_rows: 0,
        project_rebuild_seconds: 0.0,
        project_rebuild_max_seconds: budgets.project_rebuild_max_seconds,
        project_rebuild_completed: false,
        project_hydration_updated_rows: 0,
        interpret_walk_seconds: 0.0,
        interpret_blocks_per_hour: 0.0,
        interpret_min_blocks_per_hour: budgets.interpret_min_blocks_per_hour,
        interpret_peak_rss_mib: 0.0,
        interpret_kernel_hwm_rss_mib: 0.0,
        interpret_sampled_peak_rss_mib: 0.0,
        interpret_max_peak_rss_mib: budgets.interpret_max_peak_rss_mib,
        interpret_state_cache_entries: budgets.interpret_state_cache_entries,
        green: false,
        failures,
    }
}

async fn run_project_tick(
    pool: &PgPool,
    input: &IndexingInput,
    tick_from: i64,
    resume: ProjectMarker,
) -> Result<usize> {
    let outcome = ProjectEngine::new(pool.clone())
        .run_batch(ProjectRequest {
            chain_id: input.chain_id.clone(),
            target_block: input.head_block,
            affected_from_block: tick_from,
            affected_to_block: input.head_block,
            resume_current: Some(resume),
            mode: ProjectMode::Normal,
        })
        .await
        .context("incremental projection tick failed")?;
    ensure!(
        outcome.complete,
        "incremental projection tick did not complete"
    );
    hydrate_project_head(pool, input, &outcome.current).await
}

async fn run_full_project_rebuild(pool: &PgPool, input: &IndexingInput) -> Result<usize> {
    let rebuild = ProjectEngine::new(pool.clone())
        .run_batch(ProjectRequest {
            chain_id: input.chain_id.clone(),
            target_block: input.head_block,
            affected_from_block: 0,
            affected_to_block: input.head_block,
            resume_current: None,
            mode: ProjectMode::Normal,
        })
        .await
        .context("full projection rebuild failed")?;
    ensure!(rebuild.complete, "full projection rebuild did not complete");

    hydrate_project_head(pool, input, &rebuild.current).await
}

async fn hydrate_project_head(
    pool: &PgPool,
    input: &IndexingInput,
    head: &ProjectMarker,
) -> Result<usize> {
    let Some(rpc_urls) = &input.hydration_rpc_urls else {
        return Ok(0);
    };
    let hydrator = bigname_project::Hydrator::new(pool.clone(), rpc_urls.clone());
    hydrator.require_rpc_configuration(&input.chain_id)?;
    let hydration = hydrator
        .hydrate_if_canonical_head(&input.chain_id, head)
        .await
        .context("canonical-head projection hydration failed")?
        .context("selected rebuild head is not the current canonical head")?;
    require_completed_hydration(hydration)
}

fn require_completed_hydration(hydration: bigname_project::HydrationOutcome) -> Result<usize> {
    ensure!(
        !hydration.deferred_for_redo,
        "canonical-head projection hydration was deferred for an Interpret redo"
    );
    Ok(hydration.updated_rows)
}

struct InterpretWalkMetrics {
    elapsed_seconds: f64,
    budget_peak_rss_mib: f64,
    kernel_hwm_rss_mib: f64,
    sampled_peak_rss_mib: f64,
}

async fn run_interpret_walk(
    pool: &PgPool,
    input: &IndexingInput,
    state_cache_entries: usize,
) -> Result<InterpretWalkMetrics> {
    let _hwm_reset_guard = HWM_RESET_LOCK.lock().await;
    reset_peak_rss_hwm()?;
    let initial_rss_kib = rss_kib().context(
        "failed to read process RSS from /proc/self/status; the memory gate requires Linux procfs",
    )?;
    let running = Arc::new(AtomicBool::new(true));
    let peak_kib = Arc::new(AtomicU64::new(initial_rss_kib));
    let sampler_running = running.clone();
    let sampler_peak = peak_kib.clone();
    let sampler = tokio::spawn(async move {
        while sampler_running.load(Ordering::Relaxed) {
            if let Some(sample) = rss_kib() {
                sampler_peak.fetch_max(sample, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if let Some(sample) = rss_kib() {
            sampler_peak.fetch_max(sample, Ordering::Relaxed);
        }
    });

    let engine = InterpretEngine::with_state_cache_capacity(pool.clone(), state_cache_entries);
    let mut resume_current: Option<InterpretMarker> = None;
    let started = Instant::now();
    let walk_result: Result<()> = async {
        loop {
            let outcome = engine
                .run_batch(InterpretRequest {
                    chain_id: input.chain_id.clone(),
                    from_block: input.walk_from_block,
                    to_block: input.walk_to_block,
                    resume_current,
                    mode: InterpretMode::Redo,
                })
                .await
                .context("Interpret walk batch failed")?;
            if outcome.complete {
                break;
            }
            resume_current = Some(outcome.current);
        }
        Ok(())
    }
    .await;
    let elapsed = started.elapsed().as_secs_f64();
    running.store(false, Ordering::Relaxed);
    sampler.await.context("Interpret RSS sampler failed")?;
    walk_result?;
    let sampled_peak_kib = peak_kib.load(Ordering::Relaxed);
    let kernel_hwm_kib = peak_rss_hwm_kib()
        .context("failed to read process VmHWM from /proc/self/status after the Interpret walk")?;
    Ok(InterpretWalkMetrics {
        elapsed_seconds: elapsed,
        budget_peak_rss_mib: sampled_peak_kib.max(kernel_hwm_kib) as f64 / 1_024.0,
        kernel_hwm_rss_mib: kernel_hwm_kib as f64 / 1_024.0,
        sampled_peak_rss_mib: sampled_peak_kib as f64 / 1_024.0,
    })
}

async fn validate_input(pool: &PgPool, input: &IndexingInput, budgets: &GateBudgets) -> Result<()> {
    ensure!(
        !input.chain_id.trim().is_empty(),
        "chain ID must not be empty"
    );
    ensure!(
        input.head_block > 0,
        "head block must be greater than zero for an incremental tick"
    );
    ensure!(
        input.walk_from_block >= 0 && input.walk_to_block >= input.walk_from_block,
        "invalid Interpret walk range"
    );
    ensure!(
        input.walk_to_block <= input.head_block,
        "Interpret walk range exceeds the selected head"
    );
    let walk_blocks = input.walk_to_block - input.walk_from_block + 1;
    require_minimum_walk_blocks(walk_blocks, budgets.interpret_min_walk_blocks)?;
    let canonical_blocks: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM chain_lineage
         WHERE chain_id = $1
           AND block_number BETWEEN $2 AND $3
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(&input.chain_id)
    .bind(input.walk_from_block)
    .bind(input.walk_to_block)
    .fetch_one(pool)
    .await
    .context("failed to validate benchmark lineage")?;
    ensure!(
        canonical_blocks == walk_blocks,
        "Interpret walk requires one canonical lineage block at every height; found {canonical_blocks} of {walk_blocks}"
    );
    if input.hydration_rpc_urls.is_some() {
        let selected_is_current_head: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM chain_heads
                 WHERE chain_id = $1 AND latest_block_number = $2
             )",
        )
        .bind(&input.chain_id)
        .bind(input.head_block)
        .fetch_one(pool)
        .await
        .context("failed to validate the selected canonical hydration head")?;
        ensure!(
            selected_is_current_head,
            "selected rebuild head is not chain_heads.latest_block_number"
        );
    }
    Ok(())
}

async fn project_marker(pool: &PgPool, chain_id: &str, block_number: i64) -> Result<ProjectMarker> {
    let block_hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2 AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_one(pool)
    .await
    .context("failed to load incremental projection resume marker")?;
    Ok(ProjectMarker {
        number: block_number,
        hash: block_hash,
    })
}

fn rss_kib() -> Option<u64> {
    fs::read_to_string(PROC_SELF_STATUS)
        .ok()
        .and_then(|status| parse_status_memory_kib(&status, "VmRSS"))
}

fn peak_rss_hwm_kib() -> Option<u64> {
    fs::read_to_string(PROC_SELF_STATUS)
        .ok()
        .and_then(|status| parse_status_memory_kib(&status, "VmHWM"))
}

fn parse_status_memory_kib(status: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}:");
    status.lines().find_map(|line| {
        line.strip_prefix(&prefix)?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
    })
}

fn reset_peak_rss_hwm() -> Result<()> {
    fs::write(PROC_SELF_CLEAR_REFS, "5").context(
        "failed to reset process VmHWM through /proc/self/clear_refs before the Interpret walk; the memory gate requires writable Linux procfs",
    )
}

fn require_minimum_walk_blocks(walk_blocks: i64, minimum: u64) -> Result<()> {
    ensure!(
        u64::try_from(walk_blocks).unwrap_or_default() >= minimum,
        "Interpret walk contains {walk_blocks} blocks; release minimum is {minimum}"
    );
    Ok(())
}

async fn projection_name_count(pool: &PgPool, chain_id: &str) -> Result<u64> {
    let count: i64 = sqlx::query_scalar(PROJECTION_NAME_COUNT_SQL)
        .bind(chain_id)
        .fetch_one(pool)
        .await
        .context("failed to count selected-chain names during projection benchmarking")?;
    u64::try_from(count).context("name_current returned a negative row count")
}

fn projection_scale_failures(pre_rebuild: u64, post_rebuild: u64, minimum: u64) -> Vec<String> {
    let mut failures = Vec::new();
    if pre_rebuild < minimum {
        failures.push(format!(
            "name_current had {pre_rebuild} supported rows before rebuild; release profile requires {minimum}"
        ));
    }
    if post_rebuild < minimum {
        failures.push(format!(
            "name_current has {post_rebuild} supported rows after rebuild; release profile requires {minimum}"
        ));
    }
    failures
}

fn database_instance_identity_failures(preflight: &str, postflight: &str) -> Vec<String> {
    if preflight == postflight {
        Vec::new()
    } else {
        vec!["database instance identity changed during the indexing benchmark".to_owned()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    #[test]
    fn parses_kernel_high_water_mark_from_proc_status() {
        let status = "Name:\tbenchmark\nVmRSS:\t  1024 kB\nVmHWM:\t  8192 kB\n";
        assert_eq!(parse_status_memory_kib(status, "VmHWM"), Some(8_192));
        assert_eq!(parse_status_memory_kib(status, "VmRSS"), Some(1_024));
        assert_eq!(parse_status_memory_kib(status, "VmPeak"), None);
    }

    #[tokio::test]
    async fn kernel_high_water_mark_captures_a_freed_transient_allocation() {
        const CHILD_ENV: &str = "BIGNAME_BENCHMARK_HWM_PROBE_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            run_kernel_high_water_mark_probe().await;
            return;
        }

        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let unrelated = std::thread::spawn(move || {
            let allocation = vec![1_u8; 64 * 1_024 * 1_024];
            std::hint::black_box(&allocation);
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(allocation);
        });
        ready_rx.recv().unwrap();

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "indexing::tests::kernel_high_water_mark_captures_a_freed_transient_allocation",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        release_tx.send(()).unwrap();
        unrelated.join().unwrap();
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        print!("{}", String::from_utf8_lossy(&output.stdout));
        assert!(
            output.status.success(),
            "isolated VmHWM probe failed with {}",
            output.status
        );
    }

    async fn run_kernel_high_water_mark_probe() {
        let _hwm_reset_guard = HWM_RESET_LOCK.lock().await;
        reset_peak_rss_hwm().unwrap();
        let baseline_rss = rss_kib().unwrap();
        let reset_hwm = peak_rss_hwm_kib().unwrap();
        assert!(reset_hwm >= baseline_rss);
        assert!(reset_hwm - baseline_rss < 16 * 1_024);

        const TRANSIENT_BYTES: usize = 96 * 1_024 * 1_024;
        let mut transient = vec![0_u8; TRANSIENT_BYTES];
        for page in transient.chunks_mut(4_096) {
            page[0] = 1;
        }
        std::hint::black_box(&transient);
        let allocated_hwm = peak_rss_hwm_kib().unwrap();
        assert!(allocated_hwm >= reset_hwm + 80 * 1_024);
        drop(transient);

        let mut current_rss = rss_kib().unwrap();
        for _ in 0..50 {
            if current_rss <= baseline_rss + 32 * 1_024 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            current_rss = rss_kib().unwrap();
        }
        let final_hwm = peak_rss_hwm_kib().unwrap();
        eprintln!(
            "VmRSS baseline={baseline_rss}kB current={current_rss}kB; VmHWM reset={reset_hwm}kB allocated={allocated_hwm}kB final={final_hwm}kB"
        );
        assert!(current_rss <= baseline_rss + 32 * 1_024);
        assert!(final_hwm >= reset_hwm + 80 * 1_024);
    }

    #[test]
    fn dense_walk_cannot_use_a_trivial_range() {
        assert!(require_minimum_walk_blocks(100_000, 100_000).is_ok());
        assert!(require_minimum_walk_blocks(99_999, 100_000).is_err());
        assert!(require_minimum_walk_blocks(1, 100_000).is_err());
    }

    #[test]
    fn hydration_deferred_for_redo_cannot_count_as_complete() {
        let deferred = bigname_project::HydrationOutcome {
            head: ProjectMarker {
                number: 16,
                hash: "block-16".to_owned(),
            },
            deferred_for_redo: true,
            reverse_candidates: 0,
            text_candidates: 0,
            updated_rows: 0,
        };
        assert!(require_completed_hydration(deferred).is_err());
    }

    #[test]
    fn undersized_projection_returns_a_red_preflight_report() {
        let budgets = crate::budgets::BudgetsFile::load(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../benchmarks/release-gate.toml"),
        )
        .unwrap();
        let report = scale_failure_report(
            &IndexingInput {
                chain_id: "ethereum-mainnet".to_owned(),
                head_block: 16,
                walk_from_block: 1,
                walk_to_block: 16,
                hydration_rpc_urls: None,
            },
            budgets.profile(crate::budgets::BudgetProfile::Production),
            50_000,
            "database-before".to_owned(),
            "database-before".to_owned(),
        );
        assert!(!report.preflight_passed);
        assert!(!report.green);
        assert_eq!(report.pre_rebuild_name_current_rows, 50_000);
        assert_eq!(report.post_rebuild_name_current_rows, 0);
        assert_eq!(report.min_name_current_rows, 3_000_000);
        assert_eq!(report.project_tick_ms, 0);
    }

    #[test]
    fn rebuild_that_drops_projection_scale_is_red() {
        assert!(!projection_scale_failures(3_500_000, 2_900_000, 3_000_000).is_empty());
    }

    #[test]
    fn projection_scale_uses_selected_project_ownership() {
        assert!(PROJECTION_NAME_COUNT_SQL.contains("provenance ->> 'chain_id' = $1"));
        assert!(PROJECTION_NAME_COUNT_SQL.contains("support_status = 'supported'"));
    }

    #[tokio::test]
    async fn unsupported_projection_rows_do_not_satisfy_the_scale_floor() {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "benchmark_supported_projection_scale",
        ))
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE name_current (
                 provenance jsonb NOT NULL,
                 support_status text NOT NULL
             )",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO name_current
             SELECT jsonb_build_object('chain_id', 'ethereum-mainnet'), 'unsupported'
             FROM generate_series(1, 8)",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO name_current VALUES
                 (jsonb_build_object('chain_id', 'ethereum-mainnet'), 'supported')",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let count = projection_name_count(database.pool(), "ethereum-mainnet")
            .await
            .unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn database_instance_change_is_red() {
        assert!(database_instance_identity_failures("same", "same").is_empty());
        assert_eq!(
            database_instance_identity_failures("before", "after"),
            ["database instance identity changed during the indexing benchmark"]
        );
    }
}
