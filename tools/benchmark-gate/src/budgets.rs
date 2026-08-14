use std::{collections::BTreeSet, fs, path::Path};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

const PRODUCTION_ENDPOINTS: &[&str] = &[
    "lookup",
    "status",
    "name",
    "records",
    "subnames",
    "name_history",
    "permissions",
    "address_names",
    "primary_name",
    "address_history",
    "search",
    "events",
    "resolver",
    "namespace",
];

const SMOKE_ENDPOINTS: &[&str] = &[
    "lookup",
    "status",
    "name",
    "records",
    "subnames",
    "name_history",
    "permissions",
    "address_names",
    "primary_name",
    "address_history",
    "search",
    "events",
    "resolver",
    "namespace",
];

#[derive(Clone, Debug, Deserialize)]
pub struct BudgetsFile {
    pub version: u32,
    pub production: GateBudgets,
    pub smoke: GateBudgets,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GateBudgets {
    pub project_head_reapply_max_ms: u64,
    pub project_rebuild_max_seconds: u64,
    pub interpret_min_blocks_per_hour: u64,
    pub interpret_walk_deadline_multiplier: u64,
    pub interpret_walk_max_seconds: u64,
    pub interpret_max_peak_rss_mib: u64,
    pub interpret_state_cache_entries: usize,
    pub interpret_min_walk_blocks: u64,
    pub dense_min_raw_logs_per_1000_blocks: u64,
    pub project_min_name_current_rows: u64,
    pub api_target_qps: u64,
    pub api_min_achieved_qps: u64,
    pub api_duration_seconds: u64,
    pub api_warmup_seconds: u64,
    pub api_corpus_size: usize,
    pub api_min_specialized_corpus_size: usize,
    pub api_min_name_current_rows: u64,
    pub api_min_address_names_current_rows: u64,
    pub api_min_success_percent: f64,
    pub api_records_min_populated_percent: f64,
    pub api_cursor_seed_count: usize,
    pub api_cursor_weight_percent: usize,
    pub api_validation_sample_every: usize,
    pub api_require_populated_probes: bool,
    pub api_require_cursor_variants: bool,
    pub api_require_resolver_cursor_variant: bool,
    pub endpoints: Vec<EndpointBudget>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EndpointBudget {
    pub name: String,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum BudgetProfile {
    Production,
    Smoke,
}

impl BudgetsFile {
    pub fn load(path: &Path) -> Result<Self> {
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read benchmark budgets from {}", path.display()))?;
        let parsed: Self = toml::from_str(&source).with_context(|| {
            format!("failed to parse benchmark budgets from {}", path.display())
        })?;
        parsed.validate()?;
        Ok(parsed)
    }

    pub fn profile(&self, profile: BudgetProfile) -> &GateBudgets {
        match profile {
            BudgetProfile::Production => &self.production,
            BudgetProfile::Smoke => &self.smoke,
        }
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1,
            "unsupported benchmark budget version {}",
            self.version
        );
        self.production
            .validate("production", PRODUCTION_ENDPOINTS)?;
        self.smoke.validate("smoke", SMOKE_ENDPOINTS)?;
        Ok(())
    }
}

impl GateBudgets {
    fn validate(&self, profile: &str, expected_endpoints: &[&str]) -> Result<()> {
        for (name, value) in [
            (
                "project_head_reapply_max_ms",
                self.project_head_reapply_max_ms,
            ),
            (
                "project_rebuild_max_seconds",
                self.project_rebuild_max_seconds,
            ),
            (
                "interpret_min_blocks_per_hour",
                self.interpret_min_blocks_per_hour,
            ),
            (
                "interpret_walk_deadline_multiplier",
                self.interpret_walk_deadline_multiplier,
            ),
            (
                "interpret_walk_max_seconds",
                self.interpret_walk_max_seconds,
            ),
            (
                "interpret_max_peak_rss_mib",
                self.interpret_max_peak_rss_mib,
            ),
            ("interpret_min_walk_blocks", self.interpret_min_walk_blocks),
            (
                "dense_min_raw_logs_per_1000_blocks",
                self.dense_min_raw_logs_per_1000_blocks,
            ),
            (
                "project_min_name_current_rows",
                self.project_min_name_current_rows,
            ),
            ("api_target_qps", self.api_target_qps),
            ("api_min_achieved_qps", self.api_min_achieved_qps),
            ("api_duration_seconds", self.api_duration_seconds),
            ("api_min_name_current_rows", self.api_min_name_current_rows),
            (
                "api_min_address_names_current_rows",
                self.api_min_address_names_current_rows,
            ),
        ] {
            ensure!(value > 0, "{profile}.{name} must be positive");
        }
        ensure!(
            self.api_min_achieved_qps <= self.api_target_qps,
            "{profile}.api_min_achieved_qps exceeds its target"
        );
        ensure!(
            (0.0..=100.0).contains(&self.api_min_success_percent),
            "{profile}.api_min_success_percent must be between 0 and 100"
        );
        ensure!(
            (0.0..=100.0).contains(&self.api_records_min_populated_percent),
            "{profile}.api_records_min_populated_percent must be between 0 and 100"
        );
        ensure!(
            self.api_corpus_size > 0,
            "{profile}.api_corpus_size must be positive"
        );
        ensure!(
            self.api_min_specialized_corpus_size <= self.api_corpus_size,
            "{profile}.api_min_specialized_corpus_size exceeds api_corpus_size"
        );
        ensure!(
            (1..100).contains(&self.api_cursor_weight_percent),
            "{profile}.api_cursor_weight_percent must be between 1 and 99"
        );
        ensure!(
            self.api_validation_sample_every > 0,
            "{profile}.api_validation_sample_every must be positive"
        );
        ensure!(
            self.interpret_state_cache_entries > 0,
            "{profile}.interpret_state_cache_entries must be positive"
        );
        ensure!(
            !self.endpoints.is_empty(),
            "{profile}.endpoints must not be empty"
        );
        let mut names = BTreeSet::new();
        for endpoint in &self.endpoints {
            ensure!(
                names.insert(endpoint.name.as_str()),
                "{profile} repeats endpoint budget {:?}",
                endpoint.name
            );
            ensure!(
                endpoint.p50_ms > 0
                    && endpoint.p50_ms <= endpoint.p95_ms
                    && endpoint.p95_ms <= endpoint.p99_ms,
                "{profile} endpoint {:?} must have positive ordered percentile budgets",
                endpoint.name
            );
        }
        let expected = expected_endpoints.iter().copied().collect::<BTreeSet<_>>();
        ensure!(
            names == expected,
            "{profile} endpoint budgets must cover exactly {expected:?}; found {names:?}"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_budgets_parse_and_validate() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
        let budgets = BudgetsFile::load(&path).expect("checked-in budgets must be valid");
        assert_eq!(budgets.production.api_target_qps, 2_000);
        assert_eq!(budgets.production.project_head_reapply_max_ms, 1_000);
        assert_eq!(budgets.production.interpret_walk_deadline_multiplier, 2);
        assert_eq!(budgets.smoke.interpret_walk_max_seconds, 30);
        assert_eq!(budgets.production.project_min_name_current_rows, 3_000_000);
        assert_eq!(budgets.production.api_min_name_current_rows, 3_000_000);
        assert_eq!(
            budgets.production.api_min_address_names_current_rows,
            3_000_000
        );
        assert_eq!(budgets.production.api_records_min_populated_percent, 1.0);
        assert!(budgets.smoke.api_require_resolver_cursor_variant);
    }
}
