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
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use tokio::task::JoinSet;
mod corpus;
mod workload;
use corpus::{Corpus, TableScale, load_table_scale};
use workload::{RequestSpec, get, normalized_base_url, request_variants};
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
    pub min_corpus_resolvers: usize,
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
    pub outcomes: BTreeMap<String, usize>,
    pub green: bool,
    pub failures: Vec<String>,
}
#[derive(Debug)]
struct Sample {
    elapsed_micros: u128,
    success: bool,
    outcome: String,
}

#[derive(Clone, Copy, Debug, Default)]
struct SeedProbe {
    populated: bool,
    cursor_variants: usize,
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
    let identity = load_api_target_identity(&client, &base).await?;
    let database_identity = database::database_instance_identity(pool).await?;
    let scale = load_table_scale(pool).await?;
    let mut preflight_failures =
        api_identity_failures(&identity, expected_build_sha, &database_identity);
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
    let corpus = Corpus::load(pool, budgets).await?;

    let mut endpoint_reports = Vec::with_capacity(budgets.endpoints.len());
    let mut failures = Vec::new();
    let mut postflight_identity = None;
    let mut postflight_database_identity = None;
    for endpoint in &budgets.endpoints {
        let mut requests = request_variants(&base, &corpus, &endpoint.name)?;
        ensure!(
            !requests.is_empty(),
            "endpoint {:?} has no request variants",
            endpoint.name
        );
        let probe = prime_cursor_variants(
            &client,
            &endpoint.name,
            &mut requests,
            budgets.api_cursor_seed_count,
        )
        .await?;
        require_seed_probe(budgets, &endpoint.name, probe)?;

        if budgets.api_warmup_seconds > 0 {
            let _ = execute_window(
                &client,
                Arc::from(requests.clone()),
                budgets.api_target_qps,
                budgets.api_warmup_seconds,
            )
            .await?;
        }
        let request_variants = requests.len();
        let (samples, elapsed) = execute_window(
            &client,
            Arc::from(requests),
            budgets.api_target_qps,
            budgets.api_duration_seconds,
        )
        .await?;
        let report = build_endpoint_report(endpoint, request_variants, samples, elapsed, budgets);
        failures.extend(
            report
                .failures
                .iter()
                .map(|failure| format!("{}: {failure}", endpoint.name)),
        );
        endpoint_reports.push(report);
        let (boundary_identity, boundary_database_identity, boundary_failures) =
            recheck_api_boundary(
                &client,
                &base,
                pool,
                &endpoint.name,
                &identity,
                expected_build_sha,
                &database_identity,
            )
            .await;
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
        min_corpus_resolvers: budgets.api_min_resolver_corpus_size,
        green: failures.is_empty(),
        failures,
        endpoints: endpoint_reports,
    })
}

fn api_postflight_failures(
    preflight: &ApiTargetIdentity,
    postflight: &ApiTargetIdentity,
    preflight_database_identity: &str,
    postflight_database_identity: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    if preflight != postflight {
        failures.push("target API identity changed during the load benchmark".to_owned());
    }
    if preflight_database_identity != postflight_database_identity {
        failures.push("corpus database identity changed during the load benchmark".to_owned());
    }
    failures
}

fn api_boundary_failures(
    endpoint: &str,
    preflight: &ApiTargetIdentity,
    boundary: &ApiTargetIdentity,
    expected_build_sha: Option<&str>,
    preflight_database_identity: &str,
    boundary_database_identity: &str,
) -> Vec<String> {
    let failures = api_identity_failures(boundary, expected_build_sha, boundary_database_identity)
        .into_iter()
        .chain(api_postflight_failures(
            preflight,
            boundary,
            preflight_database_identity,
            boundary_database_identity,
        ));
    failures
        .map(|failure| format!("after {endpoint} endpoint: {failure}"))
        .collect()
}

async fn recheck_api_boundary(
    client: &Client,
    base: &reqwest::Url,
    pool: &PgPool,
    endpoint: &str,
    preflight: &ApiTargetIdentity,
    expected_build_sha: Option<&str>,
    preflight_database_identity: &str,
) -> (Option<ApiTargetIdentity>, Option<String>, Vec<String>) {
    let (target, database) = tokio::join!(
        load_api_target_identity(client, base),
        database::database_instance_identity(pool)
    );
    classify_api_boundary(
        endpoint,
        preflight,
        expected_build_sha,
        preflight_database_identity,
        target,
        database,
    )
}

fn classify_api_boundary(
    endpoint: &str,
    preflight: &ApiTargetIdentity,
    expected_build_sha: Option<&str>,
    preflight_database_identity: &str,
    target: Result<ApiTargetIdentity>,
    database: Result<String>,
) -> (Option<ApiTargetIdentity>, Option<String>, Vec<String>) {
    let mut failures = Vec::new();
    match (&target, &database) {
        (Ok(target), Ok(database)) => failures.extend(api_boundary_failures(
            endpoint,
            preflight,
            target,
            expected_build_sha,
            preflight_database_identity,
            database,
        )),
        (Ok(target), Err(error)) => {
            failures.extend(
                api_identity_failures(target, expected_build_sha, preflight_database_identity)
                    .into_iter()
                    .map(|failure| format!("after {endpoint} endpoint: {failure}")),
            );
            if target != preflight {
                failures.push(format!(
                    "after {endpoint} endpoint: target API identity changed during the load benchmark"
                ));
            }
            failures.push(format!(
                "after {endpoint} endpoint: corpus database identity recheck failed: {error:#}"
            ));
        }
        (Err(error), Ok(database)) => {
            failures.push(format!(
                "after {endpoint} endpoint: target API identity recheck failed: {error:#}"
            ));
            if database != preflight_database_identity {
                failures.push(format!(
                    "after {endpoint} endpoint: corpus database identity changed during the load benchmark"
                ));
            }
        }
        (Err(target_error), Err(database_error)) => {
            failures.push(format!(
                "after {endpoint} endpoint: target API identity recheck failed: {target_error:#}"
            ));
            failures.push(format!(
                "after {endpoint} endpoint: corpus database identity recheck failed: {database_error:#}"
            ));
        }
    }
    (target.ok(), database.ok(), failures)
}

fn requires_cursor_probe(budgets: &GateBudgets, endpoint: &str) -> bool {
    budgets.api_require_cursor_variants
        || (budgets.api_require_resolver_cursor_variant && endpoint == "resolver")
}

fn require_seed_probe(budgets: &GateBudgets, endpoint: &str, probe: SeedProbe) -> Result<()> {
    ensure!(
        !budgets.api_require_populated_probes || probe.populated,
        "endpoint {endpoint:?} returned no populated seed response"
    );
    ensure!(
        !requires_cursor_probe(budgets, endpoint)
            || !endpoint_requires_cursor(endpoint)
            || probe.cursor_variants > 0,
        "endpoint {endpoint:?} produced no real continuation cursor"
    );
    Ok(())
}

fn preflight_failure_report(
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
        min_corpus_resolvers: budgets.api_min_resolver_corpus_size,
        endpoints: Vec::new(),
        green: false,
        failures,
    }
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
    Ok(ApiTargetIdentity {
        build_sha: health.identity.build_sha,
        interpreter_content_hash: health.identity.interpreter_content_hash,
        database_identity: health
            .database
            .identity
            .context("target API /healthz did not identify its database")?,
    })
}

async fn prime_cursor_variants(
    client: &Client,
    endpoint: &str,
    requests: &mut Vec<RequestSpec>,
    limit: usize,
) -> Result<SeedProbe> {
    let seeds = requests.clone();
    let mut cursors = Vec::new();
    let mut populated = false;
    for (index, seed) in seeds.into_iter().enumerate() {
        let prefix_complete = index >= limit;
        let cursor_requirement_met = !endpoint_requires_cursor(endpoint) || !cursors.is_empty();
        if prefix_complete && populated && cursor_requirement_met {
            break;
        }
        let response = send(client, &seed).await?;
        if !response.status().is_success() {
            continue;
        }
        let body: Value = response
            .json()
            .await
            .context("failed to parse cursor-seed response")?;
        populated |= response_is_populated(endpoint, &body);
        cursors.extend(cursor_variants(&seed, &body));
    }
    let cursor_variants = cursors.len();
    requests.extend(cursors);
    Ok(SeedProbe {
        populated,
        cursor_variants,
    })
}

fn endpoint_requires_cursor(endpoint: &str) -> bool {
    matches!(
        endpoint,
        "lookup"
            | "subnames"
            | "name_history"
            | "permissions"
            | "address_names"
            | "address_history"
            | "search"
            | "events"
            | "resolver"
    )
}

fn response_is_populated(endpoint: &str, body: &Value) -> bool {
    match endpoint {
        "lookup" => body
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|results| {
                results.iter().any(|result| {
                    result.get("kind").and_then(Value::as_str) == Some("address")
                        && result.get("status").and_then(Value::as_str) == Some("ok")
                        && result
                            .get("records")
                            .and_then(Value::as_array)
                            .is_some_and(|records| !records.is_empty())
                })
            }),
        "subnames" | "name_history" | "permissions" | "address_names" | "address_history"
        | "search" | "events" => body
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|rows| !rows.is_empty()),
        "primary_name" => body
            .pointer("/data/answers")
            .and_then(Value::as_array)
            .is_some_and(|answers| {
                answers
                    .iter()
                    .any(|answer| answer.get("status").and_then(Value::as_str) == Some("ok"))
            }),
        "resolver" => body
            .pointer("/data/address")
            .and_then(Value::as_str)
            .is_some(),
        "records" => body
            .pointer("/data/records")
            .and_then(Value::as_object)
            .is_some_and(|records| {
                records
                    .values()
                    .any(|record| record.get("status").and_then(Value::as_str) == Some("ok"))
            }),
        "status" | "name" | "namespace" => body.get("data").is_some(),
        _ => false,
    }
}

async fn execute_window(
    client: &Client,
    requests: Arc<[RequestSpec]>,
    target_qps: u64,
    duration_seconds: u64,
) -> Result<(Vec<Sample>, Duration)> {
    let total = target_qps.saturating_mul(duration_seconds);
    let total = usize::try_from(total).context("API request count exceeds platform size")?;
    let tick = Duration::from_millis(10);
    let tick_count = duration_seconds.saturating_mul(100);
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    let mut samples = Vec::with_capacity(total);
    let mut sent = 0usize;
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
            tasks.spawn(async move { sample_request(&client, &request).await });
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

fn build_endpoint_report(
    endpoint: &EndpointBudget,
    request_variants: usize,
    samples: Vec<Sample>,
    elapsed: Duration,
    budgets: &GateBudgets,
) -> EndpointReport {
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

async fn sample_request(client: &Client, request: &RequestSpec) -> Sample {
    let started = Instant::now();
    let (elapsed_micros, success, outcome) = match send(client, request).await {
        Ok(response) => {
            let status = response.status();
            match response.bytes().await {
                Ok(body) => {
                    let elapsed_micros = started.elapsed().as_micros();
                    let success = status.is_success();
                    let mut outcome = status.as_u16().to_string();
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
                        let populated = serde_json::from_slice::<Value>(&body)
                            .ok()
                            .is_some_and(|body| response_is_populated("records", &body));
                        outcome.push_str(if populated {
                            ":records_populated"
                        } else {
                            ":records_empty"
                        });
                    }
                    (elapsed_micros, success, outcome)
                }
                Err(_) => (
                    started.elapsed().as_micros(),
                    false,
                    "response_body_error".to_owned(),
                ),
            }
        }
        Err(_) => (
            started.elapsed().as_micros(),
            false,
            "transport_error".to_owned(),
        ),
    };
    Sample {
        elapsed_micros,
        success,
        outcome,
    }
}

fn cursor_variants(seed: &RequestSpec, body: &Value) -> Vec<RequestSpec> {
    if seed.method == Method::POST && seed.url.path().ends_with("/v2/lookup") {
        let Some(results) = body.get("data").and_then(Value::as_array) else {
            return Vec::new();
        };
        return results
            .iter()
            .enumerate()
            .find_map(|(index, result)| {
                let cursor = result.pointer("/page/next_cursor")?.as_str()?;
                let mut resumed = seed.clone();
                resumed
                    .body
                    .as_mut()?
                    .pointer_mut(&format!("/inputs/{index}"))?
                    .as_object_mut()?
                    .insert("cursor".to_owned(), Value::String(cursor.to_owned()));
                Some(vec![resumed])
            })
            .unwrap_or_default();
    }

    let cursor = body
        .pointer("/page/next_cursor")
        .or_else(|| body.pointer("/data/bound_names/page/next_cursor"))
        .and_then(Value::as_str);
    let Some(cursor) = cursor else {
        return Vec::new();
    };
    let mut resumed = seed.clone();
    resumed.url.query_pairs_mut().append_pair("cursor", cursor);
    vec![resumed]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budgets::{BudgetProfile, BudgetsFile};
    use serde_json::json;
    use std::path::Path;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use workload::{get, post};

    #[test]
    fn percentile_uses_nearest_rank() {
        let values = [1, 2, 3, 4, 5];
        assert_eq!(percentile(&values, 50.0), 3.0);
        assert_eq!(percentile(&values, 99.0), 5.0);
    }

    #[test]
    fn cursor_variants_cover_lookup_and_nested_resolver_pages() {
        let base = normalized_base_url("http://127.0.0.1:3000").unwrap();
        let lookup = post(
            &base,
            &["v2", "lookup"],
            json!({"inputs": [{"address": "0x01"}]}),
        )
        .unwrap();
        let resumed = cursor_variants(
            &lookup,
            &json!({"data": [{"page": {"next_cursor": "lookup-next"}}]}),
        );
        assert_eq!(
            resumed[0].body.as_ref().unwrap()["inputs"][0]["cursor"],
            "lookup-next"
        );

        let resolver = get(&base, &["v2", "resolvers", "1", "0x01"], &[]).unwrap();
        let resumed = cursor_variants(
            &resolver,
            &json!({"data": {"bound_names": {"page": {"next_cursor": "resolver-next"}}}}),
        );
        assert_eq!(resumed[0].url.query(), Some("cursor=resolver-next"));
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
            &json!({"data": {"answers": [{"status": "ok"}]}})
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
    fn records_probe_requires_a_found_requested_record() {
        assert!(!response_is_populated(
            "records",
            &json!({
                "data": {
                    "records": {
                        "addr:60": {"status": "not_found"},
                        "text:avatar": {"status": "not_found"}
                    }
                }
            })
        ));
        assert!(response_is_populated(
            "records",
            &json!({
                "data": {
                    "records": {
                        "addr:60": {"status": "not_found"},
                        "text:avatar": {"status": "ok", "value": "ipfs://avatar"}
                    }
                }
            })
        ));
    }

    #[tokio::test]
    async fn cursor_priming_continues_past_the_fixed_prefix() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for index in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request).await.unwrap();
                let body = if index < 2 {
                    r#"{"data":[{"name":"one-page.eth"}]}"#
                } else {
                    r#"{"data":[{"name":"paginated.eth"}],"page":{"next_cursor":"later"}}"#
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let base = normalized_base_url(&format!("http://{address}")).unwrap();
        let mut requests = (0..3)
            .map(|index| get(&base, &["v2", "events", &index.to_string()], &[]).unwrap())
            .collect::<Vec<_>>();

        let probe = prime_cursor_variants(&Client::new(), "events", &mut requests, 2)
            .await
            .unwrap();
        server.abort();

        assert!(probe.populated);
        assert_eq!(probe.cursor_variants, 1);
        assert_eq!(requests.len(), 4);
        assert_eq!(requests.last().unwrap().url.query(), Some("cursor=later"));
    }

    #[tokio::test]
    async fn cursor_priming_exhausts_the_corpus_without_inventing_a_cursor() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_served = Arc::clone(&served);
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request).await.unwrap();
                server_served.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let body = r#"{"data":[{"name":"one-page.eth"}]}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let base = normalized_base_url(&format!("http://{address}")).unwrap();
        let mut requests = (0..3)
            .map(|index| get(&base, &["v2", "events", &index.to_string()], &[]).unwrap())
            .collect::<Vec<_>>();

        let probe = prime_cursor_variants(&Client::new(), "events", &mut requests, 2)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("cursor-exhaustion mock did not receive the bounded corpus within two seconds")
            .unwrap();

        assert!(probe.populated);
        assert_eq!(served.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert_eq!(probe.cursor_variants, 0);
        assert_eq!(requests.len(), 3);
        let budgets = BudgetsFile::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
        )
        .unwrap();
        assert!(
            require_seed_probe(budgets.profile(BudgetProfile::Production), "events", probe)
                .is_err()
        );
    }

    #[test]
    fn preflight_failure_report_records_observed_totals_and_floors() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
        let budgets = BudgetsFile::load(&path).unwrap();
        let production = budgets.profile(BudgetProfile::Production);
        let report = preflight_failure_report(
            corpus::TableScale {
                name_current_rows: 50_000,
                address_names_current_rows: 75_000,
            },
            production,
            ApiTargetIdentity {
                build_sha: "release".to_owned(),
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
                database_identity: "keccak256:database".to_owned(),
            },
            "keccak256:database".to_owned(),
            Some("release"),
            vec!["name_current is below its floor".to_owned()],
        );

        assert!(!report.green);
        assert_eq!(report.name_current_rows, 50_000);
        assert_eq!(report.min_name_current_rows, 3_000_000);
        assert_eq!(report.address_names_current_rows, 75_000);
        assert_eq!(report.min_address_names_current_rows, 3_000_000);
        assert!(report.endpoints.is_empty());
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
            api_postflight_failures(&before, &after, "corpus-before", "corpus-after").len(),
            2
        );
        let boundary = api_boundary_failures(
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
        let (target, database, failures) = classify_api_boundary(
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
    async fn response_timing_includes_the_complete_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            stream.write_all(b"done").await.unwrap();
        });
        let base = normalized_base_url(&format!("http://{address}")).unwrap();
        let request = get(&base, &["slow"], &[]).unwrap();
        let sample = sample_request(&Client::new(), &request).await;
        server.await.unwrap();
        assert!(sample.success);
        assert!(sample.elapsed_micros >= 40_000);
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
        let request = get(&base, &["v2", "names", "empty.eth", "records"], &[]).unwrap();

        let sample = sample_request(&Client::new(), &request).await;
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
            })
            .collect();

        let report =
            build_endpoint_report(endpoint, 100, samples, Duration::from_millis(1), production);

        assert_eq!(report.populated_responses, Some(0));
        assert_eq!(report.populated_percent, Some(0.0));
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.contains("populated records share"))
        );
    }
}
