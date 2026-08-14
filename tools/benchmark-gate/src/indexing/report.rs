use std::time::Duration;

use crate::budgets::GateBudgets;

use super::{
    IndexingInput, IndexingReport,
    deadline::{self, InterpretWalkMetrics},
    verdict::database_instance_identity_failures,
};

pub(super) fn scale_failure_report(
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
        project_head_reapply_ms: 0,
        project_head_reapply_max_ms: budgets.project_head_reapply_max_ms,
        project_head_reapply_hydration_updated_rows: 0,
        project_rebuild_seconds: 0.0,
        project_rebuild_max_seconds: budgets.project_rebuild_max_seconds,
        project_rebuild_completed: false,
        project_hydration_updated_rows: 0,
        interpret_walk_completed: false,
        interpret_walk_seconds: 0.0,
        interpret_walk_deadline_seconds: deadline::from_throughput_floor(
            input.walk_to_block - input.walk_from_block + 1,
            budgets.interpret_min_blocks_per_hour,
            budgets.interpret_walk_deadline_multiplier,
            budgets.interpret_walk_max_seconds,
        )
        .as_secs(),
        interpret_walk_deadline_multiplier: budgets.interpret_walk_deadline_multiplier,
        interpret_walk_max_seconds: budgets.interpret_walk_max_seconds,
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

#[allow(clippy::too_many_arguments)]
pub(super) fn walk_failure_report(
    input: &IndexingInput,
    budgets: &GateBudgets,
    database_instance_identity: String,
    postflight_database_instance_identity: String,
    name_current_rows: u64,
    raw_logs: i64,
    density: f64,
    project_head_reapply_ms: u128,
    project_head_reapply_hydration_updated_rows: usize,
    walk_deadline: Duration,
    interpret_walk: InterpretWalkMetrics,
    failure: String,
) -> IndexingReport {
    let mut failures = vec![failure];
    failures.extend(database_instance_identity_failures(
        &database_instance_identity,
        &postflight_database_instance_identity,
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
    if interpret_walk.budget_peak_rss_mib > budgets.interpret_max_peak_rss_mib as f64 {
        failures.push(format!(
            "Interpret walk peaked at {:.1} MiB RSS; budget is {} MiB",
            interpret_walk.budget_peak_rss_mib, budgets.interpret_max_peak_rss_mib
        ));
    }
    IndexingReport {
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
        post_rebuild_name_current_rows: 0,
        min_name_current_rows: budgets.project_min_name_current_rows,
        project_head_reapply_ms,
        project_head_reapply_max_ms: budgets.project_head_reapply_max_ms,
        project_head_reapply_hydration_updated_rows,
        project_rebuild_seconds: 0.0,
        project_rebuild_max_seconds: budgets.project_rebuild_max_seconds,
        project_rebuild_completed: false,
        project_hydration_updated_rows: 0,
        interpret_walk_completed: false,
        interpret_walk_seconds: interpret_walk.elapsed_seconds,
        interpret_walk_deadline_seconds: walk_deadline.as_secs(),
        interpret_walk_deadline_multiplier: budgets.interpret_walk_deadline_multiplier,
        interpret_walk_max_seconds: budgets.interpret_walk_max_seconds,
        interpret_blocks_per_hour: 0.0,
        interpret_min_blocks_per_hour: budgets.interpret_min_blocks_per_hour,
        interpret_peak_rss_mib: interpret_walk.budget_peak_rss_mib,
        interpret_kernel_hwm_rss_mib: interpret_walk.kernel_hwm_rss_mib,
        interpret_sampled_peak_rss_mib: interpret_walk.sampled_peak_rss_mib,
        interpret_max_peak_rss_mib: budgets.interpret_max_peak_rss_mib,
        interpret_state_cache_entries: budgets.interpret_state_cache_entries,
        green: false,
        failures,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn head_reapply_failure_report(
    input: &IndexingInput,
    budgets: &GateBudgets,
    database_instance_identity: String,
    postflight_database_instance_identity: String,
    name_current_rows: u64,
    raw_logs: i64,
    density: f64,
    elapsed: Duration,
) -> IndexingReport {
    let timeout_ms = budgets.project_head_reapply_max_ms.saturating_mul(2);
    let mut failures = vec![format!(
        "published-head Project re-apply and hydration did not complete within {timeout_ms}ms (twice the {}ms budget)",
        budgets.project_head_reapply_max_ms
    )];
    failures.extend(database_instance_identity_failures(
        &database_instance_identity,
        &postflight_database_instance_identity,
    ));
    IndexingReport {
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
        post_rebuild_name_current_rows: 0,
        min_name_current_rows: budgets.project_min_name_current_rows,
        project_head_reapply_ms: elapsed.as_millis(),
        project_head_reapply_max_ms: budgets.project_head_reapply_max_ms,
        project_head_reapply_hydration_updated_rows: 0,
        project_rebuild_seconds: 0.0,
        project_rebuild_max_seconds: budgets.project_rebuild_max_seconds,
        project_rebuild_completed: false,
        project_hydration_updated_rows: 0,
        interpret_walk_completed: false,
        interpret_walk_seconds: 0.0,
        interpret_walk_deadline_seconds: deadline::from_throughput_floor(
            input.walk_to_block - input.walk_from_block + 1,
            budgets.interpret_min_blocks_per_hour,
            budgets.interpret_walk_deadline_multiplier,
            budgets.interpret_walk_max_seconds,
        )
        .as_secs(),
        interpret_walk_deadline_multiplier: budgets.interpret_walk_deadline_multiplier,
        interpret_walk_max_seconds: budgets.interpret_walk_max_seconds,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budgets::{BudgetProfile, BudgetsFile};
    use std::path::Path;

    #[test]
    fn timed_out_walk_does_not_claim_throughput_or_run_rebuild() {
        let budgets = BudgetsFile::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
        )
        .unwrap();
        let budgets = budgets.profile(BudgetProfile::Smoke);
        let report = walk_failure_report(
            &IndexingInput {
                chain_id: "ethereum-mainnet".to_owned(),
                head_block: 16,
                walk_from_block: 1,
                walk_to_block: 16,
                hydration_rpc_urls: None,
            },
            budgets,
            "database-before".to_owned(),
            "database-before".to_owned(),
            8,
            16,
            1_000.0,
            5,
            0,
            Duration::from_secs(30),
            InterpretWalkMetrics {
                elapsed_seconds: 30.0,
                budget_peak_rss_mib: 64.0,
                kernel_hwm_rss_mib: 64.0,
                sampled_peak_rss_mib: 63.0,
            },
            "Interpret walk timed out".to_owned(),
        );

        assert!(!report.interpret_walk_completed);
        assert_eq!(report.interpret_blocks_per_hour, 0.0);
        assert!(!report.project_rebuild_completed);
        assert_eq!(report.project_rebuild_seconds, 0.0);
        assert_eq!(report.post_rebuild_name_current_rows, 0);
        assert_eq!(report.failures, ["Interpret walk timed out"]);
    }

    #[test]
    fn timed_out_head_reapply_returns_named_red_evidence() {
        let budgets = BudgetsFile::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
        )
        .unwrap();
        let budgets = budgets.profile(BudgetProfile::Smoke);
        let report = head_reapply_failure_report(
            &IndexingInput {
                chain_id: "ethereum-mainnet".to_owned(),
                head_block: 16,
                walk_from_block: 1,
                walk_to_block: 16,
                hydration_rpc_urls: None,
            },
            budgets,
            "database-before".to_owned(),
            "database-before".to_owned(),
            8,
            16,
            1_000.0,
            Duration::from_secs(10),
        );

        assert!(!report.green);
        assert_eq!(report.project_head_reapply_ms, 10_000);
        assert!(!report.interpret_walk_completed);
        assert!(!report.project_rebuild_completed);
        assert!(report.failures[0].contains("re-apply and hydration did not complete"));
    }

    #[test]
    fn indexing_orchestrator_routes_head_reapply_timeout_into_the_red_report() {
        let source = include_str!("../indexing.rs");
        assert!(source.contains("let project_head_reapply_result = tokio::time::timeout("));
        assert!(source.contains("return Ok(head_reapply_failure_report("));
        assert!(source.contains("project_head_reapply_elapsed,"));
        assert!(!source.contains("reapply_started.elapsed(),"));
    }
}
