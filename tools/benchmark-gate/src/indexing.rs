use std::{
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

use crate::budgets::GateBudgets;

const PROJECTION_NAME_COUNT_SQL: &str =
    "SELECT count(*) FROM name_current WHERE provenance ->> 'chain_id' = $1";

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
    validate_input(pool, input, budgets).await?;
    let name_current_rows = projection_name_count(pool, &input.chain_id).await?;
    if name_current_rows < budgets.project_min_name_current_rows {
        return Ok(scale_failure_report(input, budgets, name_current_rows));
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

    let (interpret_walk_seconds, peak_rss_mib) =
        run_interpret_walk(pool, input, budgets.interpret_state_cache_entries).await?;
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

    let mut failures = Vec::new();
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
) -> IndexingReport {
    IndexingReport {
        preflight_passed: false,
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
        interpret_max_peak_rss_mib: budgets.interpret_max_peak_rss_mib,
        interpret_state_cache_entries: budgets.interpret_state_cache_entries,
        green: false,
        failures: vec![format!(
            "name_current has {name_current_rows} rows; release profile requires {} before projection benchmarking",
            budgets.project_min_name_current_rows
        )],
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

async fn run_interpret_walk(
    pool: &PgPool,
    input: &IndexingInput,
    state_cache_entries: usize,
) -> Result<(f64, f64)> {
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
    Ok((elapsed, peak_kib.load(Ordering::Relaxed) as f64 / 1_024.0))
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
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
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
            "name_current had {pre_rebuild} rows before rebuild; release profile requires {minimum}"
        ));
    }
    if post_rebuild < minimum {
        failures.push(format!(
            "name_current has {post_rebuild} rows after rebuild; release profile requires {minimum}"
        ));
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
