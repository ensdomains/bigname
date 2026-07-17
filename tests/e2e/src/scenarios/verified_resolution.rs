use alloy_primitives::Address;
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use sqlx::types::Uuid;

use super::support;
use crate::harness::responses::pointer;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

fn query_by_record_key<'a>(
    queries: &'a Value,
    record_key: &str,
    context: &str,
) -> Result<&'a Value> {
    queries
        .as_array()
        .and_then(|queries| {
            queries
                .iter()
                .find(|query| query.get("record_key").and_then(Value::as_str) == Some(record_key))
        })
        .ok_or_else(|| anyhow!("{context} missing query {record_key}; queries: {queries}"))
}

fn verified_query<'a>(body: &'a Value, record_key: &str) -> Result<&'a Value> {
    let queries = body
        .pointer("/verified_state/verified_queries")
        .ok_or_else(|| {
            anyhow!("verified profile response missing verified_queries; full body: {body}")
        })?;
    query_by_record_key(queries, record_key, "verified profile response")
}

fn payload_verified_query<'a>(
    payload: &'a Value,
    record_key: &str,
    context: &str,
) -> Result<&'a Value> {
    let queries = payload
        .pointer("/verified_queries")
        .ok_or_else(|| anyhow!("{context} missing verified_queries; payload: {payload}"))?;
    query_by_record_key(queries, record_key, context)
}

fn assert_compact_addr_success(body: &Value, target: Address, context: &str) {
    let expected = format!("{target:#x}");
    assert_eq!(
        pointer(body, "/data/coin_addresses/60/status"),
        "success",
        "{context} should return addr:60 success; full body: {body}"
    );
    assert_eq!(
        pointer(body, "/data/coin_addresses/60/value"),
        expected,
        "{context} should return the on-chain addr:60 value; full body: {body}"
    );
}

fn assert_verified_addr_query(body: &Value, query: &Value, target: Address) -> Result<String> {
    let expected = format!("{target:#x}");
    assert_eq!(
        query.get("status"),
        Some(&json!("success")),
        "verified addr:60 query should succeed; full body: {body}"
    );
    assert_eq!(
        query.pointer("/value/value"),
        Some(&json!(expected)),
        "verified addr:60 query should match declared on-chain value; full body: {body}"
    );
    let trace_id = query
        .pointer("/provenance/execution_trace_id")
        .and_then(Value::as_str)
        .context("verified addr:60 query missing execution_trace_id provenance")?;
    assert_eq!(
        body.pointer("/provenance/execution_trace_id")
            .and_then(Value::as_str),
        Some(trace_id),
        "top-level provenance should carry the same execution_trace_id; full body: {body}"
    );
    Ok(trace_id.to_owned())
}

fn contains_universal_resolver_call(contracts_called: &Value) -> bool {
    contracts_called.as_array().is_some_and(|calls| {
        calls.iter().any(|call| {
            call.get("contract_address")
                .and_then(Value::as_str)
                .is_some_and(|address| {
                    address.eq_ignore_ascii_case(ens_v1::EXECUTION_UNIVERSAL_RESOLVER_ADDRESS)
                })
        })
    })
}

async fn assert_execution_artifacts(
    run: &support::PipelineRun,
    trace_id: &str,
    target: Address,
) -> Result<()> {
    let trace_uuid = Uuid::parse_str(trace_id).context("parse execution_trace_id from response")?;
    let (request_type, namespace, contracts_called, final_payload, request_metadata): (
        String,
        String,
        Value,
        Option<Value>,
        Value,
    ) = sqlx::query_as(
        "SELECT request_type, namespace, contracts_called, final_payload, request_metadata \
         FROM execution_traces WHERE execution_trace_id = $1",
    )
    .bind(trace_uuid)
    .fetch_one(&run.db.pool)
    .await
    .with_context(|| format!("load execution_traces row {trace_id}"))?;

    assert_eq!(
        request_type, "verified_resolution",
        "execution trace {trace_id} should use verified_resolution request_type"
    );
    assert_eq!(
        namespace, "ens",
        "execution trace {trace_id} should use ENS namespace"
    );
    assert_eq!(
        request_metadata.get("entrypoint"),
        Some(&json!("universal_resolver")),
        "trace {trace_id} should record the Universal Resolver entrypoint; metadata: {request_metadata}"
    );
    assert!(
        contains_universal_resolver_call(&contracts_called),
        "trace {trace_id} contracts_called should include the execution UniversalResolver {}; contracts_called: {contracts_called}",
        ens_v1::EXECUTION_UNIVERSAL_RESOLVER_ADDRESS
    );
    let final_payload =
        final_payload.context("successful verified-resolution trace should set final_payload")?;
    let final_query =
        payload_verified_query(&final_payload, "addr:60", "execution trace final_payload")?;
    assert_eq!(
        final_query.get("status"),
        Some(&json!("success")),
        "trace {trace_id} final_payload should include a successful addr:60 query; final_payload: {final_payload}"
    );
    assert_eq!(
        final_query.pointer("/value/value"),
        Some(&json!(format!("{target:#x}"))),
        "trace {trace_id} final_payload should carry the resolved addr:60 value; final_payload: {final_payload}"
    );

    let steps: Vec<(String, Value)> = sqlx::query_as(
        "SELECT step_kind, step_payload FROM execution_steps \
         WHERE execution_trace_id = $1 ORDER BY step_index",
    )
    .bind(trace_uuid)
    .fetch_all(&run.db.pool)
    .await
    .with_context(|| format!("load execution_steps rows {trace_id}"))?;
    assert!(
        steps
            .iter()
            .any(|(kind, _)| kind == "load_declared_topology"),
        "trace {trace_id} should persist a load_declared_topology step; steps: {steps:?}"
    );
    assert!(
        steps.iter().any(|(kind, payload)| {
            kind == "call_universal_resolver"
                && payload.get("entrypoint") == Some(&json!("universal_resolver"))
                && payload.get("record_key") == Some(&json!("addr:60"))
        }),
        "trace {trace_id} should persist a call_universal_resolver addr:60 step; steps: {steps:?}"
    );

    let cache_rows: Vec<(Option<Value>,)> = sqlx::query_as(
        "SELECT outcome_payload FROM execution_cache_outcomes WHERE execution_trace_id = $1",
    )
    .bind(trace_uuid)
    .fetch_all(&run.db.pool)
    .await
    .with_context(|| format!("load execution_cache_outcomes row {trace_id}"))?;
    assert_eq!(
        cache_rows.len(),
        1,
        "trace {trace_id} should have exactly one execution_cache_outcomes row; rows: {cache_rows:?}"
    );
    let outcome_payload = cache_rows[0]
        .0
        .as_ref()
        .context("cache outcome should set outcome_payload")?;
    let outcome_query = payload_verified_query(
        outcome_payload,
        "addr:60",
        "execution cache outcome_payload",
    )?;
    assert_eq!(
        outcome_query.pointer("/value/value"),
        Some(&json!(format!("{target:#x}"))),
        "cache outcome for trace {trace_id} should carry the resolved addr:60 value; outcome_payload: {outcome_payload}"
    );
    Ok(())
}

#[tokio::test]
async fn direct_path_verified_query_via_local_universal_resolver_persists_trace() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let universal_resolver =
        ens_v1::install_local_universal_resolver(&rpc, &root, &deployment).await?;
    let accounts = rpc.accounts().await?;
    let owner = accounts[1];
    let target = accounts[2];

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "verified",
        owner,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::set_addr_record(
        &rpc,
        deployment.public_resolver.address,
        owner,
        "verified.eth",
        target,
    )
    .await?;

    let ready_sql =
        support::canonical_event_ready_sql("ens:verified.eth", "RecordChanged", Some("addr:60"));
    let run = support::ingest_and_serve_with_ens_execution(
        &anvil,
        &deployment,
        &universal_resolver,
        Some(&ready_sql),
    )
    .await?;

    let (declared_status, declared_body) = run
        .api
        .get_json("/v1/names/ens/verified.eth/records?mode=declared&coin_types=60&meta=full")
        .await?;
    assert_eq!(
        declared_status, 200,
        "declared records lookup should succeed; full body: {declared_body}"
    );
    assert_compact_addr_success(&declared_body, target, "declared records lookup");

    let (verified_status, verified_body) = run
        .api
        .get_json("/v1/profiles/names/verified.eth?mode=both&meta=full")
        .await?;
    assert_eq!(
        verified_status, 200,
        "verified profile lookup should succeed; full body: {verified_body}"
    );
    let query = verified_query(&verified_body, "addr:60")?;
    let trace_id = assert_verified_addr_query(&verified_body, query, target)?;

    // The explain route must reconstruct the same selected-snapshot cache
    // identity used by the on-demand profile write. The name projection may
    // predate the selected head, so deriving requested positions from
    // name_current rather than the route snapshot would miss this outcome.
    let (explain_status, explain_body) = run
        .api
        .get_json("/v1/explain/resolutions/ens/verified.eth/execution?records=addr:60,contenthash")
        .await?;
    assert_eq!(
        explain_status, 200,
        "on-demand persisted outcome should be explain-readable; full body: {explain_body}"
    );
    let explain_query = verified_query(&explain_body, "addr:60")?;
    let explain_trace_id = assert_verified_addr_query(&explain_body, explain_query, target)?;
    assert_eq!(
        explain_trace_id, trace_id,
        "explain should return the trace persisted by the profile request; full body: {explain_body}"
    );
    assert_eq!(
        pointer(
            &explain_body,
            "/verified_state/execution/execution_trace_id"
        ),
        trace_id,
        "execution summary should use the persisted profile trace; full body: {explain_body}"
    );

    assert_execution_artifacts(&run, &trace_id, target).await?;

    run.db.cleanup().await?;
    Ok(())
}
