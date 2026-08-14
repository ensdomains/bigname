use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    budgets::{EndpointBudget, GateBudgets},
    database,
};
use anyhow::{Context, Result, ensure};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use tokio::task::JoinSet;
mod corpus;
mod preflight;
mod probes;
mod report;
mod seeding;
mod validation;
pub(crate) mod workload;
use corpus::{Corpus, load_table_scale};
use preflight::{ApiBoundaryPreflight, load_api_database_snapshot, recheck_api_boundary};
use probes::require_seed_probe;
pub use report::{ApiReport, EndpointReport, ResolverManifestCoverage};
use report::{corpus_failure_report, endpoint_failure_report, preflight_failure_report};
#[cfg(test)]
use seeding::{SeedProbe, cursor_variants, endpoint_requires_cursor, response_is_populated};
use seeding::{
    aggregate_records_are_populated, prime_cursor_variants, requested_records_are_populated,
};
use workload::{RequestSpec, endpoint_requests, get, normalized_base_url};
#[derive(Debug)]
struct Sample {
    elapsed_micros: u128,
    success: bool,
    outcome: String,
    validation_failure: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct ApiTargetIdentity {
    build_sha: String,
    interpreter_content_hash: String,
    database_identity: String,
}

#[derive(Debug, Deserialize)]
struct HealthResponse {
    identity: HealthIdentity,
    database: HealthDatabase,
}

#[derive(Debug, Deserialize)]
struct HealthIdentity {
    build_sha: String,
    interpreter_content_hash: String,
}

#[derive(Debug, Deserialize)]
struct HealthDatabase {
    identity: Option<String>,
}

fn api_identity_failures(
    actual: &ApiTargetIdentity,
    expected_build_sha: Option<&str>,
    expected_database_identity: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    if expected_build_sha.is_some_and(|expected| actual.build_sha != expected) {
        failures.push(format!(
            "target API build SHA {:?} does not match expected {:?}",
            actual.build_sha,
            expected_build_sha.unwrap_or_default()
        ));
    }
    if actual.interpreter_content_hash != bigname_content_hash::INTERPRETER_CONTENT_HASH {
        failures.push(format!(
            "target API interpreter content hash {:?} does not match harness {:?}",
            actual.interpreter_content_hash,
            bigname_content_hash::INTERPRETER_CONTENT_HASH
        ));
    }
    if actual.database_identity != expected_database_identity {
        failures.push(format!(
            "target API database identity {:?} does not match corpus database identity {:?}",
            actual.database_identity, expected_database_identity
        ));
    }
    failures
}

pub async fn run(
    pool: &PgPool,
    api_base_url: &str,
    expected_build_sha: Option<&str>,
    budgets: &GateBudgets,
) -> Result<ApiReport> {
    let base = normalized_base_url(api_base_url)?;
    let client = Client::builder()
        .pool_max_idle_per_host((budgets.api_target_qps / 2).clamp(32, 1_024) as usize)
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to build API benchmark client")?;
    let (identity, health_failures) = load_api_preflight_identity(&client, &base).await?;
    let database_identity = database::database_instance_identity(pool).await?;
    let database_snapshot = load_api_database_snapshot(pool).await?;
    let boundary_preflight = ApiBoundaryPreflight::new(
        &identity,
        &database_snapshot,
        expected_build_sha,
        &database_identity,
    );
    let scale = load_table_scale(pool).await?;
    let mut preflight_failures = health_failures;
    preflight_failures.extend(api_identity_failures(
        &identity,
        expected_build_sha,
        &database_identity,
    ));
    preflight_failures.extend(database_snapshot.active_failures());
    preflight_failures.extend(scale.failures(budgets));
    if !preflight_failures.is_empty() {
        return Ok(preflight_failure_report(
            scale,
            budgets,
            identity,
            database_identity,
            expected_build_sha,
            preflight_failures,
        ));
    }
    let (mut corpus, resolver_manifest_failures) = Corpus::load(pool, budgets).await?;
    if !resolver_manifest_failures.is_empty() {
        return Ok(corpus_failure_report(
            scale,
            budgets,
            identity,
            database_identity,
            expected_build_sha,
            corpus,
            resolver_manifest_failures,
        ));
    }
    let default_primary_name_probe =
        probes::probe_default_primary_name(&client, &base, &corpus, budgets).await?;
    let mut failures = default_primary_name_probe.failures;
    let mut endpoint_reports = Vec::with_capacity(budgets.endpoints.len());
    let mut postflight_identity = None;
    let mut postflight_database_identity = None;
    for endpoint in &budgets.endpoints {
        let requests = endpoint_requests(&base, &mut corpus, &endpoint.name)?;
        let report = if requests.is_empty() {
            endpoint_failure_report(
                endpoint,
                budgets,
                format!("endpoint {:?} has no request variants", endpoint.name),
            )
        } else {
            match prime_cursor_variants(
                &client,
                &endpoint.name,
                requests,
                budgets.api_cursor_seed_count,
                budgets.api_cursor_weight_percent,
            )
            .await
            {
                Err(error) => endpoint_failure_report(
                    endpoint,
                    budgets,
                    format!("seed probing failed: {error:#}"),
                ),
                Ok(primed) => match require_seed_probe(budgets, &endpoint.name, primed.probe) {
                    Err(error) => endpoint_failure_report(
                        endpoint,
                        budgets,
                        format!("seed probing did not establish required evidence: {error:#}"),
                    ),
                    Ok(()) => {
                        let requests = primed.requests;
                        if budgets.api_warmup_seconds > 0 {
                            let _ = execute_window(
                                &client,
                                Arc::from(requests.clone()),
                                &endpoint.name,
                                budgets.api_target_qps,
                                budgets.api_warmup_seconds,
                                None,
                            )
                            .await?;
                        }
                        let request_variants = requests.len();
                        let (samples, elapsed) = execute_window(
                            &client,
                            Arc::from(requests),
                            &endpoint.name,
                            budgets.api_target_qps,
                            budgets.api_duration_seconds,
                            Some(budgets.api_validation_sample_every),
                        )
                        .await?;
                        build_endpoint_report(
                            endpoint,
                            (
                                request_variants,
                                primed.base_variants,
                                primed.unique_cursor_variants,
                                primed.weighted_cursor_requests,
                            ),
                            samples,
                            elapsed,
                            budgets,
                        )
                    }
                },
            }
        };
        failures.extend(
            report
                .failures
                .iter()
                .map(|failure| format!("{}: {failure}", endpoint.name)),
        );
        endpoint_reports.push(report);
        let (boundary_identity, boundary_database_identity, boundary_failures) =
            recheck_api_boundary(&client, &base, pool, &endpoint.name, &boundary_preflight).await;
        failures.extend(boundary_failures);
        postflight_identity = boundary_identity;
        postflight_database_identity = boundary_database_identity;
    }

    Ok(ApiReport {
        api_build_sha: identity.build_sha,
        expected_api_build_sha: expected_build_sha.map(str::to_owned),
        api_interpreter_content_hash: identity.interpreter_content_hash,
        api_database_identity: identity.database_identity,
        corpus_database_identity: database_identity,
        postflight_api_build_sha: postflight_identity
            .as_ref()
            .map(|identity| identity.build_sha.clone()),
        postflight_api_interpreter_content_hash: postflight_identity
            .as_ref()
            .map(|identity| identity.interpreter_content_hash.clone()),
        postflight_api_database_identity: postflight_identity
            .map(|identity| identity.database_identity),
        postflight_corpus_database_identity: postflight_database_identity,
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
        resolver_manifest_coverage: corpus.resolver_manifest_coverage,
        default_primary_name_probe_requests: default_primary_name_probe.requests_sent,
        default_primary_name_probe_outcomes: default_primary_name_probe.outcomes,
        green: failures.is_empty(),
        failures,
        endpoints: endpoint_reports,
    })
}

async fn load_api_target_identity(
    client: &Client,
    base: &reqwest::Url,
) -> Result<ApiTargetIdentity> {
    let request = get(base, &["healthz"], &[])?;
    let response = send(client, &request).await?;
    ensure!(
        response.status().is_success(),
        "target API /healthz returned {}",
        response.status()
    );
    let health: HealthResponse = response
        .json()
        .await
        .context("failed to parse target API /healthz response")?;
    health_identity(health, true).map(|(identity, _)| identity)
}

async fn load_api_preflight_identity(
    client: &Client,
    base: &reqwest::Url,
) -> Result<(ApiTargetIdentity, Vec<String>)> {
    let request = get(base, &["healthz"], &[])?;
    let response = send(client, &request).await?;
    ensure!(
        response.status().is_success(),
        "target API /healthz returned {}",
        response.status()
    );
    let health: HealthResponse = response
        .json()
        .await
        .context("failed to parse target API /healthz response")?;
    health_identity(health, false)
}

fn health_identity(
    health: HealthResponse,
    require_database_identity: bool,
) -> Result<(ApiTargetIdentity, Vec<String>)> {
    let mut failures = Vec::new();
    let database_identity = match health.database.identity {
        Some(identity) => identity,
        None if require_database_identity => {
            anyhow::bail!("target API /healthz did not identify its database")
        }
        None => {
            failures.push(
                "target API /healthz was reachable but omitted database.identity; ensure its readiness and serving pools use the same direct PostgreSQL instance and rerun the gate"
                    .to_owned(),
            );
            "<missing>".to_owned()
        }
    };
    Ok(ApiTargetIdentity {
        build_sha: health.identity.build_sha,
        interpreter_content_hash: health.identity.interpreter_content_hash,
        database_identity,
    })
    .map(|identity| (identity, failures))
}

async fn execute_window(
    client: &Client,
    requests: Arc<[RequestSpec]>,
    endpoint: &str,
    target_qps: u64,
    duration_seconds: u64,
    validation_sample_every: Option<usize>,
) -> Result<(Vec<Sample>, Duration)> {
    let total = target_qps.saturating_mul(duration_seconds);
    let total = usize::try_from(total).context("API request count exceeds platform size")?;
    let tick = Duration::from_millis(10);
    let tick_count = duration_seconds.saturating_mul(100);
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    let mut samples = Vec::with_capacity(total);
    let mut sent = 0usize;
    let endpoint: Arc<str> = Arc::from(endpoint);
    for tick_number in 1..=tick_count {
        let expected_sent = usize::try_from(
            u64::try_from(total)
                .unwrap_or(u64::MAX)
                .saturating_mul(tick_number)
                / tick_count,
        )
        .unwrap_or(total)
        .min(total);
        let batch = expected_sent.saturating_sub(sent);
        for _ in 0..batch {
            let request = requests[sent % requests.len()].clone();
            let client = client.clone();
            let endpoint = endpoint.clone();
            let validate = validation_sample_every.is_some_and(|every| {
                validation_sample_is_due(sent, every)
                    && request.known_good_evidence
                    && validation::endpoint_is_sampled(endpoint.as_ref())
            });
            let scheduled_start = Instant::now();
            tasks.spawn(async move {
                sample_request(&client, &request, &endpoint, scheduled_start, validate).await
            });
            sent += 1;
        }
        while let Some(joined) = tasks.try_join_next() {
            samples.push(joined.context("API benchmark request task panicked")?);
        }
        tokio::time::sleep_until(
            tokio::time::Instant::from_std(started)
                + tick * u32::try_from(tick_number).unwrap_or(u32::MAX),
        )
        .await;
    }
    while let Some(joined) = tasks.join_next().await {
        samples.push(joined.context("API benchmark request task panicked")?);
    }
    Ok((samples, started.elapsed()))
}

fn validation_sample_is_due(sent: usize, every: usize) -> bool {
    let block = sent / every;
    sent % every == block % every
}

fn build_endpoint_report(
    endpoint: &EndpointBudget,
    variant_counts: (usize, usize, usize, usize),
    samples: Vec<Sample>,
    elapsed: Duration,
    budgets: &GateBudgets,
) -> EndpointReport {
    let (
        request_variants,
        base_request_variants,
        unique_resumed_cursor_variants,
        weighted_resumed_cursor_requests,
    ) = variant_counts;
    let requests = samples.len();
    let successful_requests = samples.iter().filter(|sample| sample.success).count();
    let success_percent = successful_requests as f64 * 100.0 / requests.max(1) as f64;
    let populated_responses = (endpoint.name == "records").then(|| {
        samples
            .iter()
            .filter(|sample| sample.outcome.ends_with(":records_populated"))
            .count()
    });
    let populated_percent =
        populated_responses.map(|populated| populated as f64 * 100.0 / requests.max(1) as f64);
    let achieved_qps = requests as f64 / elapsed.as_secs_f64().max(0.000_001);
    let mut outcomes = BTreeMap::new();
    let validation_failures = samples
        .iter()
        .filter_map(|sample| sample.validation_failure.as_ref())
        .cloned()
        .collect::<Vec<_>>();
    let validated_responses = samples
        .iter()
        .filter(|sample| {
            sample.validation_failure.is_some() || sample.outcome.contains(":validated")
        })
        .count();
    for sample in &samples {
        *outcomes.entry(sample.outcome.clone()).or_insert(0) += 1;
    }
    let mut durations = samples
        .into_iter()
        .map(|sample| sample.elapsed_micros)
        .collect::<Vec<_>>();
    durations.sort_unstable();
    let p50_ms = percentile(&durations, 50.0) / 1_000.0;
    let p95_ms = percentile(&durations, 95.0) / 1_000.0;
    let p99_ms = percentile(&durations, 99.0) / 1_000.0;
    let mut failures = Vec::new();
    if !validation_failures.is_empty() {
        let first = validation_failures.first().cloned().unwrap_or_default();
        failures.push(format!(
            "{} of {validated_responses} sampled response validations failed; first failure: {first}",
            validation_failures.len()
        ));
    }
    if success_percent < budgets.api_min_success_percent {
        failures.push(format!(
            "success rate {success_percent:.3}% is below {:.3}%",
            budgets.api_min_success_percent
        ));
    }
    if achieved_qps < budgets.api_min_achieved_qps as f64 {
        failures.push(format!(
            "achieved {achieved_qps:.1} QPS; floor is {}",
            budgets.api_min_achieved_qps
        ));
    }
    if populated_percent.is_some_and(|actual| actual < budgets.api_records_min_populated_percent) {
        failures.push(format!(
            "populated records share {:.3}% is below {:.3}%",
            populated_percent.unwrap_or_default(),
            budgets.api_records_min_populated_percent
        ));
    }
    for (name, actual, budget) in [
        ("p50", p50_ms, endpoint.p50_ms),
        ("p95", p95_ms, endpoint.p95_ms),
        ("p99", p99_ms, endpoint.p99_ms),
    ] {
        if actual > budget as f64 {
            failures.push(format!("{name} is {actual:.3}ms; budget is {budget}ms"));
        }
    }
    EndpointReport {
        endpoint: endpoint.name.clone(),
        target_qps: budgets.api_target_qps,
        min_achieved_qps: budgets.api_min_achieved_qps,
        requests,
        successful_requests,
        min_success_percent: budgets.api_min_success_percent,
        success_percent,
        populated_responses,
        min_populated_percent: (endpoint.name == "records")
            .then_some(budgets.api_records_min_populated_percent),
        populated_percent,
        achieved_qps,
        target_p50_ms: endpoint.p50_ms,
        p50_ms,
        target_p95_ms: endpoint.p95_ms,
        p95_ms,
        target_p99_ms: endpoint.p99_ms,
        p99_ms,
        request_variants,
        base_request_variants,
        unique_resumed_cursor_variants,
        weighted_resumed_cursor_requests,
        target_resumed_cursor_percent: budgets.api_cursor_weight_percent,
        actual_resumed_cursor_percent: weighted_resumed_cursor_requests as f64 * 100.0
            / request_variants.max(1) as f64,
        validation_sample_every: validation::endpoint_is_sampled(&endpoint.name)
            .then_some(budgets.api_validation_sample_every),
        validated_responses,
        invalid_sampled_responses: validation_failures.len(),
        outcomes,
        green: failures.is_empty(),
        failures,
    }
}

fn percentile(sorted_micros: &[u128], percentile: f64) -> f64 {
    if sorted_micros.is_empty() {
        return 0.0;
    }
    let rank = ((percentile / 100.0) * sorted_micros.len() as f64).ceil() as usize;
    sorted_micros[rank.saturating_sub(1).min(sorted_micros.len() - 1)] as f64
}

async fn send(client: &Client, request: &RequestSpec) -> Result<reqwest::Response> {
    let mut builder = client.request(request.method.clone(), request.url.clone());
    if let Some(body) = &request.body {
        builder = builder.json(body);
    }
    builder.send().await.context("API benchmark request failed")
}

async fn sample_request(
    client: &Client,
    request: &RequestSpec,
    endpoint: &str,
    scheduled_start: Instant,
    validate: bool,
) -> Sample {
    let (elapsed_micros, success, outcome, validation_failure) = match send(client, request).await {
        Ok(response) => {
            let status = response.status();
            match response.bytes().await {
                Ok(body) => {
                    let elapsed_micros = scheduled_start.elapsed().as_micros();
                    let success = status.is_success();
                    let mut outcome = status.as_u16().to_string();
                    let validation_failure = (validate && success)
                        .then(|| validation::validate_timed_response(endpoint, request, &body))
                        .flatten();
                    if validate && success && validation_failure.is_none() {
                        outcome.push_str(":validated");
                    }
                    if !success
                        && let Ok(body) = serde_json::from_slice::<Value>(&body)
                        && let Some(code) = body
                            .pointer("/error/code")
                            .or_else(|| body.get("code"))
                            .and_then(Value::as_str)
                    {
                        outcome.push(':');
                        outcome.push_str(code);
                    }
                    if success && request.url.path().ends_with("/records") {
                        let keyed = request.url.query_pairs().any(|(key, _)| key == "keys");
                        let populated =
                            serde_json::from_slice::<Value>(&body)
                                .ok()
                                .is_some_and(|body| {
                                    if keyed {
                                        requested_records_are_populated(request, &body)
                                    } else {
                                        aggregate_records_are_populated(&body)
                                    }
                                });
                        outcome.push_str(if (keyed, populated) == (true, true) {
                            ":records_populated"
                        } else if keyed {
                            ":records_empty"
                        } else if populated {
                            ":records_aggregate_populated"
                        } else {
                            ":records_aggregate_empty"
                        });
                    }
                    (elapsed_micros, success, outcome, validation_failure)
                }
                Err(_) => (
                    scheduled_start.elapsed().as_micros(),
                    false,
                    "response_body_error".to_owned(),
                    None,
                ),
            }
        }
        Err(_) => (
            scheduled_start.elapsed().as_micros(),
            false,
            "transport_error".to_owned(),
            None,
        ),
    };
    Sample {
        elapsed_micros,
        success,
        outcome,
        validation_failure,
    }
}

#[cfg(test)]
mod expansion_cursor_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budgets::{BudgetProfile, BudgetsFile};
    use serde_json::json;
    use std::path::Path;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use workload::get;

    mod cursor_priming;
    mod reporting;
    mod timing;

    #[test]
    fn percentile_uses_nearest_rank() {
        let values = [1, 2, 3, 4, 5];
        assert_eq!(percentile(&values, 50.0), 3.0);
        assert_eq!(percentile(&values, 99.0), 5.0);
    }

    #[test]
    fn production_probe_classifies_populated_rows_and_cursor_routes() {
        assert!(response_is_populated(
            "permissions",
            &json!({"data": [{"address": "0x01"}]})
        ));
        assert!(!response_is_populated("permissions", &json!({"data": []})));
        assert!(response_is_populated(
            "primary_name",
            &json!({"data": {"answers": [{"source": "indexed", "status": "ok"}]}})
        ));
        assert!(endpoint_requires_cursor("lookup"));
        assert!(endpoint_requires_cursor("resolver"));
        assert!(!endpoint_requires_cursor("primary_name"));
    }

    #[test]
    fn lookup_probe_requires_a_nonempty_address_result() {
        assert!(!response_is_populated(
            "lookup",
            &json!({
                "data": [{
                    "kind": "address",
                    "status": "ok",
                    "records": []
                }]
            })
        ));
        for status in ["stale", "unsupported"] {
            assert!(!response_is_populated(
                "lookup",
                &json!({
                    "data": [{
                        "kind": "address",
                        "status": status,
                        "records": [{"name": format!("{status}.eth")}]
                    }]
                })
            ));
        }
        assert!(!response_is_populated(
            "lookup",
            &json!({
                "data": [{
                    "kind": "name",
                    "status": "ok",
                    "record": {"name": "forward.eth"}
                }]
            })
        ));
        assert!(response_is_populated(
            "lookup",
            &json!({
                "data": [{
                    "kind": "address",
                    "status": "ok",
                    "records": [{"name": "visible.eth"}]
                }]
            })
        ));
        assert!(!response_is_populated(
            "lookup",
            &json!({
                "data": [{
                    "kind": "address",
                    "status": "failed",
                    "records": [{"name": "failed.eth", "status": "failed"}]
                }]
            })
        ));
    }

    #[test]
    fn seed_evidence_rejects_degraded_status_and_missing_indexed_claims() {
        assert!(!response_is_populated(
            "status",
            &json!({
                "data": {
                    "status": "degraded",
                    "chains": {"1": {"status": "ready"}}
                }
            })
        ));
        assert!(!response_is_populated(
            "status",
            &json!({
                "data": {
                    "status": "ready",
                    "chains": {"1": {"status": "stale"}}
                }
            })
        ));
        assert!(response_is_populated(
            "status",
            &json!({
                "data": {
                    "status": "ready",
                    "chains": {"1": {"status": "ready"}}
                }
            })
        ));

        assert!(!response_is_populated(
            "primary_name",
            &json!({
                "data": {"answers": [
                    {"source": "indexed", "status": "not_found"},
                    {"source": "verified", "status": "ok"}
                ]}
            })
        ));
    }

    #[test]
    fn production_api_identity_must_match_the_release() {
        let failures = api_identity_failures(
            &ApiTargetIdentity {
                build_sha: "old-release".to_owned(),
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
                database_identity: "database".to_owned(),
            },
            Some("new-release"),
            "database",
        );
        assert_eq!(failures.len(), 1);

        let failures = api_identity_failures(
            &ApiTargetIdentity {
                build_sha: "new-release".to_owned(),
                interpreter_content_hash: "keccak256:old-interpreter".to_owned(),
                database_identity: "database".to_owned(),
            },
            Some("new-release"),
            "database",
        );
        assert_eq!(failures.len(), 1);

        let before = ApiTargetIdentity {
            build_sha: "new-release".to_owned(),
            interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
            database_identity: "database-before".to_owned(),
        };
        let mut after = before.clone();
        after.database_identity = "database-after".to_owned();
        assert_eq!(
            preflight::api_postflight_failures(&before, &after, "corpus-before", "corpus-after",)
                .len(),
            2
        );
        let boundary = preflight::api_boundary_failures(
            "records",
            &before,
            &after,
            Some("new-release"),
            "corpus-before",
            "corpus-after",
        );
        assert!(!boundary.is_empty());
        assert!(
            boundary
                .iter()
                .all(|failure| failure.starts_with("after records endpoint:"))
        );
    }

    #[test]
    fn boundary_probe_errors_are_endpoint_named_report_failures() {
        let preflight = ApiTargetIdentity {
            build_sha: "release".to_owned(),
            interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
            database_identity: "database".to_owned(),
        };
        let (target, database, failures) = preflight::classify_api_boundary(
            "records",
            &preflight,
            Some("release"),
            "database",
            Err(anyhow::anyhow!("connection closed")),
            Ok("database".to_owned()),
        );

        assert!(target.is_none());
        assert_eq!(database.as_deref(), Some("database"));
        assert_eq!(failures.len(), 1);
        assert!(failures[0].starts_with("after records endpoint:"));
        assert!(failures[0].contains("connection closed"));
    }

    #[tokio::test]
    async fn timed_records_sample_classifies_an_all_not_found_response_as_empty() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            let body = r#"{"data":{"records":{"addr:60":{"status":"not_found"},"text:avatar":{"status":"not_found"}}}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let base = normalized_base_url(&format!("http://{address}")).unwrap();
        let request = get(
            &base,
            &["v2", "names", "empty.eth", "records"],
            &[("keys", "addr:60,text:avatar")],
        )
        .unwrap();

        let sample =
            sample_request(&Client::new(), &request, "records", Instant::now(), false).await;
        server.await.unwrap();

        assert!(sample.success);
        assert_eq!(sample.outcome, "200:records_empty");
    }

    #[test]
    fn timed_records_report_enforces_the_populated_share_floor() {
        let budgets = BudgetsFile::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
        )
        .unwrap();
        let production = budgets.profile(BudgetProfile::Production);
        let endpoint = production
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "records")
            .unwrap();
        let samples = (0..100)
            .map(|_| Sample {
                elapsed_micros: 1_000,
                success: true,
                outcome: "200:records_empty".to_owned(),
                validation_failure: None,
            })
            .collect();

        let report = build_endpoint_report(
            endpoint,
            (100, 100, 0, 0),
            samples,
            Duration::from_millis(1),
            production,
        );

        assert_eq!(report.populated_responses, Some(0));
        assert_eq!(report.populated_percent, Some(0.0));
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.contains("populated records share"))
        );
    }

    #[test]
    fn sampled_in_band_validation_failure_makes_the_endpoint_red() {
        let budgets = BudgetsFile::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
        )
        .unwrap();
        let production = budgets.profile(BudgetProfile::Production);
        let endpoint = production
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == "lookup")
            .unwrap();
        let samples = vec![Sample {
            elapsed_micros: 1_000,
            success: true,
            outcome: "200".to_owned(),
            validation_failure: Some(
                "sampled lookup input \"forward\" did not return name-kind status ok".to_owned(),
            ),
        }];
        let report = build_endpoint_report(
            endpoint,
            (1, 1, 0, 0),
            samples,
            Duration::from_secs_f64(1.0 / production.api_min_achieved_qps as f64),
            production,
        );

        assert!(!report.green);
        assert_eq!(report.validated_responses, 1);
        assert_eq!(report.invalid_sampled_responses, 1);
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.contains("sampled response validations failed"))
        );
    }
}
