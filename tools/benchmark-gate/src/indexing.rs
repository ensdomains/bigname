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

mod deadline;
mod publication;
mod report;
mod verdict;
use deadline::{InterpretWalkMetrics, InterpretWalkOutcome};
use publication::projection_name_count;
use report::{head_reapply_failure_report, scale_failure_report, walk_failure_report};
use verdict::{database_instance_identity_failures, projection_scale_failures};

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
    pub project_head_reapply_ms: u128,
    pub project_head_reapply_max_ms: u64,
    pub project_head_reapply_hydration_updated_rows: usize,
    pub project_rebuild_seconds: f64,
    pub project_rebuild_max_seconds: u64,
    pub project_rebuild_completed: bool,
    pub project_hydration_updated_rows: usize,
    pub interpret_walk_completed: bool,
    pub interpret_walk_seconds: f64,
    pub interpret_walk_deadline_seconds: u64,
    pub interpret_walk_deadline_multiplier: u64,
    pub interpret_walk_max_seconds: u64,
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
    let name_current_rows = publication::projection_name_count(pool, &input.chain_id).await?;
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

    // This re-applies the published head; a true head-1 rewind needs Project capability
    // tracked by https://github.com/ensdomains/bigname/issues/467.
    let reapply_from = input.head_block;
    let resume = publication::project_marker(pool, &input.chain_id, reapply_from - 1).await?;
    let reapply_started = Instant::now();
    let head_reapply_timeout =
        Duration::from_millis(budgets.project_head_reapply_max_ms.saturating_mul(2));
    let project_head_reapply_result = tokio::time::timeout(
        head_reapply_timeout,
        run_project_head_reapply(pool, input, reapply_from, resume),
    )
    .await;
    let project_head_reapply_elapsed = reapply_started.elapsed();
    let project_head_reapply_ms = project_head_reapply_elapsed.as_millis();
    let project_head_reapply_hydration_updated_rows = match project_head_reapply_result {
        Ok(result) => result?,
        Err(_) => {
            let postflight_database_instance_identity =
                database::database_instance_identity(pool).await?;
            return Ok(head_reapply_failure_report(
                input,
                budgets,
                database_instance_identity,
                postflight_database_instance_identity,
                name_current_rows,
                raw_logs,
                density,
                project_head_reapply_elapsed,
            ));
        }
    };

    let walk_deadline = deadline::from_throughput_floor(
        walk_blocks,
        budgets.interpret_min_blocks_per_hour,
        budgets.interpret_walk_deadline_multiplier,
        budgets.interpret_walk_max_seconds,
    );
    let interpret_walk = run_interpret_walk(
        pool,
        input,
        budgets.interpret_state_cache_entries,
        walk_deadline,
        budgets.interpret_min_blocks_per_hour,
        budgets.interpret_walk_deadline_multiplier,
        budgets.interpret_walk_max_seconds,
    )
    .await?;
    let interpret_walk = match interpret_walk {
        InterpretWalkOutcome::Completed(metrics) => metrics,
        InterpretWalkOutcome::TimedOut { metrics, failure } => {
            let postflight_database_instance_identity =
                database::database_instance_identity(pool).await?;
            return Ok(walk_failure_report(
                input,
                budgets,
                database_instance_identity,
                postflight_database_instance_identity,
                name_current_rows,
                raw_logs,
                density,
                project_head_reapply_ms,
                project_head_reapply_hydration_updated_rows,
                walk_deadline,
                metrics,
                failure,
            ));
        }
    };
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
    if project_head_reapply_ms > u128::from(budgets.project_head_reapply_max_ms) {
        failures.push(format!(
            "published-head projection re-apply took {project_head_reapply_ms}ms; budget is {}ms",
            budgets.project_head_reapply_max_ms
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
        project_head_reapply_ms,
        project_head_reapply_max_ms: budgets.project_head_reapply_max_ms,
        project_head_reapply_hydration_updated_rows,
        project_rebuild_seconds,
        project_rebuild_max_seconds: budgets.project_rebuild_max_seconds,
        project_rebuild_completed,
        project_hydration_updated_rows,
        interpret_walk_completed: true,
        interpret_walk_seconds,
        interpret_walk_deadline_seconds: walk_deadline.as_secs(),
        interpret_walk_deadline_multiplier: budgets.interpret_walk_deadline_multiplier,
        interpret_walk_max_seconds: budgets.interpret_walk_max_seconds,
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

async fn run_project_head_reapply(
    pool: &PgPool,
    input: &IndexingInput,
    reapply_from: i64,
    resume: ProjectMarker,
) -> Result<usize> {
    let outcome = ProjectEngine::new(pool.clone())
        .run_batch(ProjectRequest {
            chain_id: input.chain_id.clone(),
            target_block: input.head_block,
            affected_from_block: reapply_from,
            affected_to_block: input.head_block,
            resume_current: Some(resume),
            mode: ProjectMode::Normal,
        })
        .await
        .context("published-head projection re-apply failed")?;
    ensure!(outcome.complete, "published-head re-apply did not complete");
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

async fn run_interpret_walk(
    pool: &PgPool,
    input: &IndexingInput,
    state_cache_entries: usize,
    walk_deadline: Duration,
    minimum_blocks_per_hour: u64,
    deadline_multiplier: u64,
    maximum_seconds: u64,
) -> Result<InterpretWalkOutcome> {
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
    let walk_result: Option<Result<()>> = deadline::complete_within(walk_deadline, async {
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
    })
    .await;
    let elapsed = started.elapsed().as_secs_f64();
    running.store(false, Ordering::Relaxed);
    sampler.await.context("Interpret RSS sampler failed")?;
    let timed_out = match walk_result {
        Some(result) => {
            result?;
            false
        }
        None => true,
    };
    let sampled_peak_kib = peak_kib.load(Ordering::Relaxed);
    let kernel_hwm_kib = peak_rss_hwm_kib()
        .context("failed to read process VmHWM from /proc/self/status after the Interpret walk")?;
    let metrics = InterpretWalkMetrics {
        elapsed_seconds: elapsed,
        budget_peak_rss_mib: sampled_peak_kib.max(kernel_hwm_kib) as f64 / 1_024.0,
        kernel_hwm_rss_mib: kernel_hwm_kib as f64 / 1_024.0,
        sampled_peak_rss_mib: sampled_peak_kib as f64 / 1_024.0,
    };
    Ok(if timed_out {
        InterpretWalkOutcome::TimedOut {
            metrics,
            failure: deadline::failure(
                walk_deadline,
                minimum_blocks_per_hour,
                deadline_multiplier,
                maximum_seconds,
            ),
        }
    } else {
        InterpretWalkOutcome::Completed(metrics)
    })
}

async fn validate_input(pool: &PgPool, input: &IndexingInput, budgets: &GateBudgets) -> Result<()> {
    ensure!(
        !input.chain_id.trim().is_empty(),
        "chain ID must not be empty"
    );
    ensure!(
        input.head_block > 0,
        "head block must be greater than zero for a published-head projection re-apply"
    );
    publication::require_published_head(pool, &input.chain_id, input.head_block).await?;
    ensure!(
        input.walk_from_block >= 0 && input.walk_to_block >= input.walk_from_block,
        "invalid Interpret walk range"
    );
    ensure!(
        input.walk_to_block <= input.head_block,
        "Interpret walk range exceeds the selected head"
    );
    let walk_blocks = input.walk_to_block - input.walk_from_block + 1;
    publication::require_minimum_walk_blocks(walk_blocks, budgets.interpret_min_walk_blocks)?;
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
    Ok(())
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

#[cfg(test)]
mod publication_tests;

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
        const CHILD_SENTINEL: &str = "BIGNAME_BENCHMARK_HWM_PROBE_RAN";
        if std::env::var_os(CHILD_ENV).is_some() {
            run_kernel_high_water_mark_probe().await;
            println!("{CHILD_SENTINEL}");
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
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(CHILD_SENTINEL),
            "isolated VmHWM child exited successfully without running the exact probe test"
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
        assert!(publication::require_minimum_walk_blocks(100_000, 100_000).is_ok());
        assert!(publication::require_minimum_walk_blocks(99_999, 100_000).is_err());
        assert!(publication::require_minimum_walk_blocks(1, 100_000).is_err());
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
        assert_eq!(report.project_head_reapply_ms, 0);
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
