use std::path::Path;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use super::*;
use crate::budgets::{BudgetProfile, BudgetsFile};

const ADDRESS: &str = "0x0000000000000000000000000000000000000001";

fn empty_corpus() -> Corpus {
    Corpus {
        names: Vec::new(),
        address_names: Vec::new(),
        parents: Vec::new(),
        permission_subjects: Vec::new(),
        primary_names: Vec::new(),
        resolvers: Vec::new(),
        namespaces: vec!["ens".to_owned()],
        names_by_namespace: Default::default(),
        parents_by_namespace: Default::default(),
        resolver_manifest_coverage: Vec::new(),
    }
}

fn classified_failure(status: StatusCode, body: &Value) -> String {
    default_primary_name_failure("ens", ADDRESS, "60", status, Some(body))
        .expect("mutation fixture must be rejected")
}

#[tokio::test]
async fn production_budget_reaches_the_default_source_probe_invocation() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
    let budgets = BudgetsFile::load(&path).unwrap();
    let production = budgets.profile(BudgetProfile::Production);
    assert!(production.api_require_populated_probes);
    let base = super::super::workload::normalized_base_url("http://127.0.0.1:1").unwrap();

    let report = probe_default_primary_name(&Client::new(), &base, &empty_corpus(), production)
        .await
        .unwrap();

    assert!(
        report
            .failures
            .iter()
            .any(|failure| failure.contains("ens/coin-60")),
        "run-time production budgets must make a vacuous probe red"
    );
}

#[tokio::test]
async fn smoke_budget_exempts_an_empty_probe_through_the_public_wrapper() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
    let budgets = BudgetsFile::load(&path).unwrap();
    let smoke = budgets.profile(BudgetProfile::Smoke);
    assert!(!smoke.api_require_populated_probes);
    let base = super::super::workload::normalized_base_url("http://127.0.0.1:1").unwrap();

    let report = probe_default_primary_name(&Client::new(), &base, &empty_corpus(), smoke)
        .await
        .unwrap();
    assert_eq!(report.requests_sent, 0);
    assert!(report.failures.is_empty());

    let mut cursor_required = smoke.clone();
    cursor_required.api_require_cursor_variants = true;
    let report =
        probe_default_primary_name(&Client::new(), &base, &empty_corpus(), &cursor_required)
            .await
            .unwrap();
    assert_eq!(report.requests_sent, 0);
    assert!(
        report.failures.is_empty(),
        "the unrelated cursor requirement must not control default-source probe vacuity"
    );
}

#[test]
fn default_source_probe_rejects_any_answer_count_other_than_two() {
    let indexed_only = json!({
        "data": {"answers": [{"source": "indexed", "status": "ok"}]}
    });
    let three_answers = json!({
        "data": {"answers": [
            {"source": "indexed", "status": "ok"},
            {"source": "verified", "status": "ok"},
            {"source": "verified", "status": "ok"}
        ]}
    });

    for body in [&indexed_only, &three_answers] {
        assert!(
            classified_failure(StatusCode::OK, body)
                .contains("documented indexed-then-verified answer pair")
        );
    }
}

#[test]
fn default_source_probe_rejects_swapped_or_duplicated_sources() {
    let swapped = json!({
        "data": {"answers": [
            {"source": "verified", "status": "ok"},
            {"source": "indexed", "status": "ok"}
        ]}
    });
    let duplicated = json!({
        "data": {"answers": [
            {"source": "indexed", "status": "ok"},
            {"source": "indexed", "status": "ok"}
        ]}
    });

    assert!(
        classified_failure(StatusCode::OK, &swapped)
            .contains("documented indexed-then-verified answer pair")
    );
    assert!(
        classified_failure(StatusCode::OK, &duplicated)
            .contains("documented indexed-then-verified answer pair")
    );
}

#[test]
fn default_source_probe_rejects_non_stale_conflicts() {
    let body = json!({
        "error": {"code": "conflict", "message": "snapshot selection failed", "details": {}}
    });

    let failure = classified_failure(StatusCode::CONFLICT, &body);
    assert!(failure.contains("HTTP 409"));
    assert!(failure.contains("conflict"));
    assert!(failure.contains(ADDRESS));
}
