use std::collections::BTreeMap;

use serde::Serialize;

use super::{
    ApiTargetIdentity,
    corpus::{Corpus, TableScale},
};
use crate::budgets::GateBudgets;

#[derive(Clone, Debug, Serialize)]
pub struct ResolverManifestCoverage {
    pub chain_id: String,
    pub source_family: String,
    pub declared_addresses: usize,
    pub applicable_addresses: usize,
    pub exercised_addresses: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiReport {
    pub api_build_sha: String,
    pub expected_api_build_sha: Option<String>,
    pub api_interpreter_content_hash: String,
    pub api_database_identity: String,
    pub corpus_database_identity: String,
    pub postflight_api_build_sha: Option<String>,
    pub postflight_api_interpreter_content_hash: Option<String>,
    pub postflight_api_database_identity: Option<String>,
    pub postflight_corpus_database_identity: Option<String>,
    pub name_current_rows: u64,
    pub min_name_current_rows: u64,
    pub address_names_current_rows: u64,
    pub min_address_names_current_rows: u64,
    pub corpus_names: usize,
    pub corpus_names_by_namespace: BTreeMap<String, usize>,
    pub corpus_addresses: usize,
    pub corpus_parents: usize,
    pub corpus_parents_by_namespace: BTreeMap<String, usize>,
    pub corpus_permission_subjects: usize,
    pub corpus_primary_names: usize,
    pub corpus_resolvers: usize,
    pub resolver_manifest_coverage: Vec<ResolverManifestCoverage>,
    pub default_primary_name_probe_requests: usize,
    pub default_primary_name_probe_outcomes: BTreeMap<String, usize>,
    pub endpoints: Vec<EndpointReport>,
    pub green: bool,
    pub failures: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EndpointReport {
    pub endpoint: String,
    pub target_qps: u64,
    pub min_achieved_qps: u64,
    pub requests: usize,
    pub successful_requests: usize,
    pub min_success_percent: f64,
    pub success_percent: f64,
    pub populated_responses: Option<usize>,
    pub min_populated_percent: Option<f64>,
    pub populated_percent: Option<f64>,
    pub achieved_qps: f64,
    pub target_p50_ms: u64,
    pub p50_ms: f64,
    pub target_p95_ms: u64,
    pub p95_ms: f64,
    pub target_p99_ms: u64,
    pub p99_ms: f64,
    pub request_variants: usize,
    pub base_request_variants: usize,
    pub unique_resumed_cursor_variants: usize,
    pub weighted_resumed_cursor_requests: usize,
    pub target_resumed_cursor_percent: usize,
    pub actual_resumed_cursor_percent: f64,
    pub validation_sample_every: Option<usize>,
    pub validated_responses: usize,
    pub invalid_sampled_responses: usize,
    pub outcomes: BTreeMap<String, usize>,
    pub green: bool,
    pub failures: Vec<String>,
}

pub(super) fn endpoint_failure_report(
    endpoint: &crate::budgets::EndpointBudget,
    budgets: &GateBudgets,
    failure: String,
) -> EndpointReport {
    EndpointReport {
        endpoint: endpoint.name.clone(),
        target_qps: budgets.api_target_qps,
        min_achieved_qps: budgets.api_min_achieved_qps,
        requests: 0,
        successful_requests: 0,
        min_success_percent: budgets.api_min_success_percent,
        success_percent: 0.0,
        populated_responses: (endpoint.name == "records").then_some(0),
        min_populated_percent: (endpoint.name == "records")
            .then_some(budgets.api_records_min_populated_percent),
        populated_percent: (endpoint.name == "records").then_some(0.0),
        achieved_qps: 0.0,
        target_p50_ms: endpoint.p50_ms,
        p50_ms: 0.0,
        target_p95_ms: endpoint.p95_ms,
        p95_ms: 0.0,
        target_p99_ms: endpoint.p99_ms,
        p99_ms: 0.0,
        request_variants: 0,
        base_request_variants: 0,
        unique_resumed_cursor_variants: 0,
        weighted_resumed_cursor_requests: 0,
        target_resumed_cursor_percent: budgets.api_cursor_weight_percent,
        actual_resumed_cursor_percent: 0.0,
        validation_sample_every: None,
        validated_responses: 0,
        invalid_sampled_responses: 0,
        outcomes: BTreeMap::new(),
        green: false,
        failures: vec![failure],
    }
}

pub(super) fn preflight_failure_report(
    scale: TableScale,
    budgets: &GateBudgets,
    identity: ApiTargetIdentity,
    corpus_database_identity: String,
    expected_build_sha: Option<&str>,
    failures: Vec<String>,
) -> ApiReport {
    ApiReport {
        api_build_sha: identity.build_sha,
        expected_api_build_sha: expected_build_sha.map(str::to_owned),
        api_interpreter_content_hash: identity.interpreter_content_hash,
        api_database_identity: identity.database_identity,
        corpus_database_identity,
        postflight_api_build_sha: None,
        postflight_api_interpreter_content_hash: None,
        postflight_api_database_identity: None,
        postflight_corpus_database_identity: None,
        name_current_rows: scale.name_current_rows,
        min_name_current_rows: budgets.api_min_name_current_rows,
        address_names_current_rows: scale.address_names_current_rows,
        min_address_names_current_rows: budgets.api_min_address_names_current_rows,
        corpus_names: 0,
        corpus_names_by_namespace: BTreeMap::new(),
        corpus_addresses: 0,
        corpus_parents: 0,
        corpus_parents_by_namespace: BTreeMap::new(),
        corpus_permission_subjects: 0,
        corpus_primary_names: 0,
        corpus_resolvers: 0,
        resolver_manifest_coverage: Vec::new(),
        default_primary_name_probe_requests: 0,
        default_primary_name_probe_outcomes: BTreeMap::new(),
        endpoints: Vec::new(),
        green: false,
        failures,
    }
}

pub(super) fn corpus_failure_report(
    scale: TableScale,
    budgets: &GateBudgets,
    identity: ApiTargetIdentity,
    corpus_database_identity: String,
    expected_build_sha: Option<&str>,
    corpus: Corpus,
    failures: Vec<String>,
) -> ApiReport {
    let resolver_manifest_coverage = corpus
        .resolver_manifest_coverage
        .into_iter()
        .map(|mut count| {
            count.exercised_addresses = 0;
            count
        })
        .collect();
    ApiReport {
        api_build_sha: identity.build_sha,
        expected_api_build_sha: expected_build_sha.map(str::to_owned),
        api_interpreter_content_hash: identity.interpreter_content_hash,
        api_database_identity: identity.database_identity,
        corpus_database_identity,
        postflight_api_build_sha: None,
        postflight_api_interpreter_content_hash: None,
        postflight_api_database_identity: None,
        postflight_corpus_database_identity: None,
        name_current_rows: scale.name_current_rows,
        min_name_current_rows: budgets.api_min_name_current_rows,
        address_names_current_rows: scale.address_names_current_rows,
        min_address_names_current_rows: budgets.api_min_address_names_current_rows,
        corpus_names: corpus.names.len(),
        corpus_names_by_namespace: corpus.names_by_namespace,
        corpus_addresses: corpus.address_names.len(),
        corpus_parents: corpus.parents.len(),
        corpus_parents_by_namespace: corpus.parents_by_namespace,
        corpus_permission_subjects: corpus.permission_subjects.len(),
        corpus_primary_names: corpus.primary_names.len(),
        corpus_resolvers: corpus.resolvers.len(),
        resolver_manifest_coverage,
        default_primary_name_probe_requests: 0,
        default_primary_name_probe_outcomes: BTreeMap::new(),
        endpoints: Vec::new(),
        green: false,
        failures,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::budgets::{BudgetProfile, BudgetsFile};

    #[test]
    fn resolver_coverage_refusal_keeps_counts_in_the_red_report() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
        let budgets = BudgetsFile::load(&path).unwrap();
        let report = corpus_failure_report(
            TableScale {
                name_current_rows: 3_000_000,
                address_names_current_rows: 3_000_000,
            },
            budgets.profile(BudgetProfile::Production),
            ApiTargetIdentity {
                build_sha: "release".to_owned(),
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
                database_identity: "keccak256:database".to_owned(),
            },
            "keccak256:database".to_owned(),
            Some("release"),
            Corpus {
                names: Vec::new(),
                address_names: Vec::new(),
                parents: Vec::new(),
                permission_subjects: Vec::new(),
                primary_names: Vec::new(),
                resolvers: Vec::new(),
                namespaces: vec!["ens".to_owned()],
                names_by_namespace: BTreeMap::new(),
                parents_by_namespace: BTreeMap::new(),
                resolver_manifest_coverage: vec![ResolverManifestCoverage {
                    chain_id: "ethereum-sepolia".to_owned(),
                    source_family: "ens_v2_resolver_l1".to_owned(),
                    declared_addresses: 0,
                    applicable_addresses: 0,
                    exercised_addresses: 7,
                }],
            },
            vec!["resolver coverage is empty".to_owned()],
        );

        assert!(!report.green);
        assert_eq!(report.failures, ["resolver coverage is empty"]);
        assert_eq!(report.corpus_resolvers, 0);
        assert_eq!(report.resolver_manifest_coverage.len(), 1);
        assert_eq!(report.resolver_manifest_coverage[0].exercised_addresses, 0);
        assert_eq!(
            report.resolver_manifest_coverage[0].source_family,
            "ens_v2_resolver_l1"
        );
        assert!(report.endpoints.is_empty());
    }
}
