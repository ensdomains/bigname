use anyhow::{Result, bail};
use bigname_storage::{ExecutionTrace, ExecutionTraceStep};
use serde_json::{Value, json};

use super::{
    BuildOnDemandEnsVerifiedPrimaryNameRequest, RouteLocalEnsPrimaryNameClaim,
    RouteLocalEnsPrimaryNameExecution, route_local_claim_value,
};
use crate::ETHEREUM_MAINNET_CHAIN_ID;
use crate::ens_resolution_abi::digest_json;

pub(super) fn validate_route_local_steps(
    trace: &ExecutionTrace,
    execution: &RouteLocalEnsPrimaryNameExecution,
) -> Result<()> {
    let mut expected = vec!["call_ens_reverse_lookup"];
    if matches!(
        execution.claim,
        RouteLocalEnsPrimaryNameClaim::Found { .. }
            | RouteLocalEnsPrimaryNameClaim::InvalidName { .. }
    ) {
        expected.push("normalize_claimed_name");
    }
    if execution.forward_call_attempted {
        expected.push("call_universal_resolver");
        expected.extend(
            trace
                .steps
                .iter()
                .filter(|step| step.step_kind == "ccip_offchain_lookup")
                .map(|_| "ccip_offchain_lookup"),
        );
    }
    let actual = trace
        .steps
        .iter()
        .map(|step| step.step_kind.as_str())
        .collect::<Vec<_>>();
    if actual != expected {
        bail!(
            "route-local ENS primary-name trace {} steps {:?} do not match expected {:?}",
            trace.execution_trace_id,
            actual,
            expected
        );
    }
    let ccip_steps = trace
        .steps
        .iter()
        .filter(|step| step.step_kind == "ccip_offchain_lookup")
        .collect::<Vec<_>>();
    if ccip_steps.iter().any(|step| {
        !step.step_payload.is_object() || step.latency_ms.is_none_or(|latency_ms| latency_ms < 0)
    }) {
        bail!(
            "route-local ENS primary-name trace {} has malformed CCIP steps",
            trace.execution_trace_id
        );
    }
    let gateway_digests = trace
        .gateway_digests
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("route-local gateway_digests must be an array"))?;
    if gateway_digests
        .iter()
        .any(|digest| digest.as_str().is_none_or(str::is_empty))
        || (!gateway_digests.is_empty() && ccip_steps.is_empty())
    {
        bail!(
            "route-local ENS primary-name trace {} has malformed gateway evidence",
            trace.execution_trace_id
        );
    }
    Ok(())
}

pub(super) fn route_local_contracts_called(
    request: &BuildOnDemandEnsVerifiedPrimaryNameRequest<'_>,
) -> Value {
    Value::Array(request.execution_evidence.contracts_called.clone())
}

pub(super) fn route_local_steps(
    request: &BuildOnDemandEnsVerifiedPrimaryNameRequest<'_>,
) -> Vec<ExecutionTraceStep> {
    let dependency = canonicality_dependency(request);
    let claim_value = route_local_claim_value(request.claim);
    let mut steps = vec![ExecutionTraceStep {
        step_index: 0,
        step_kind: "call_ens_reverse_lookup".to_owned(),
        input_digest: Some(digest_json(&json!({
            "normalized_address": request.normalized_address,
            "block_hash": request.block_hash,
        }))),
        output_digest: Some(digest_json(&claim_value)),
        latency_ms: Some(request.reverse_latency_ms),
        canonicality_dependency: dependency.clone(),
        step_payload: claim_value,
    }];
    match request.claim {
        RouteLocalEnsPrimaryNameClaim::Found {
            raw_name,
            normalized_name,
            ..
        } => steps.push(normalization_step(
            steps.len() as i64,
            raw_name,
            json!({
                "normalized_name": normalized_name,
                "claim_name_is_normalized": raw_name == normalized_name,
            }),
            &dependency,
        )),
        RouteLocalEnsPrimaryNameClaim::InvalidName { raw_name, .. } => {
            steps.push(normalization_step(
                steps.len() as i64,
                raw_name,
                json!({ "failure_reason": "claim_name_not_normalizable" }),
                &dependency,
            ));
        }
        RouteLocalEnsPrimaryNameClaim::NotFound
        | RouteLocalEnsPrimaryNameClaim::ExecutionFailed { .. } => {}
    }
    if request.forward_call_attempted {
        steps.push(ExecutionTraceStep {
            step_index: steps.len() as i64,
            step_kind: "call_universal_resolver".to_owned(),
            input_digest: Some(digest_json(&json!({
                "normalized_address": request.normalized_address,
                "claim": route_local_claim_value(request.claim),
            }))),
            output_digest: Some(digest_json(&request.verified_primary_name)),
            latency_ms: request.forward_latency_ms,
            canonicality_dependency: dependency.clone(),
            step_payload: json!({
                "name": normalized_claim_name(request.claim),
                "coin_type": "60",
                "block_selector": {
                    "blockHash": request.block_hash,
                    "requireCanonical": true,
                }
            }),
        });
        for payload in &request.execution_evidence.ccip_step_payloads {
            steps.push(ccip_step(steps.len() as i64, payload, &dependency));
        }
    }
    steps
}

fn ccip_step(step_index: i64, payload: &Value, dependency: &Value) -> ExecutionTraceStep {
    ExecutionTraceStep {
        step_index,
        step_kind: "ccip_offchain_lookup".to_owned(),
        input_digest: None,
        output_digest: Some(digest_json(payload)),
        latency_ms: payload
            .get("latency_ms")
            .and_then(Value::as_i64)
            .filter(|latency_ms| *latency_ms >= 0)
            .or(Some(0)),
        canonicality_dependency: dependency.clone(),
        step_payload: payload.clone(),
    }
}

fn normalization_step(
    step_index: i64,
    raw_name: &str,
    step_payload: Value,
    dependency: &Value,
) -> ExecutionTraceStep {
    ExecutionTraceStep {
        step_index,
        step_kind: "normalize_claimed_name".to_owned(),
        input_digest: Some(digest_json(&json!(raw_name))),
        output_digest: Some(digest_json(&step_payload)),
        latency_ms: Some(0),
        canonicality_dependency: dependency.clone(),
        step_payload,
    }
}

fn canonicality_dependency(request: &BuildOnDemandEnsVerifiedPrimaryNameRequest<'_>) -> Value {
    json!({
        ETHEREUM_MAINNET_CHAIN_ID: {
            "block_hash": request.block_hash,
            "block_number": request.block_number,
            "state": "canonical",
        }
    })
}

fn normalized_claim_name(claim: &RouteLocalEnsPrimaryNameClaim) -> Option<&str> {
    match claim {
        RouteLocalEnsPrimaryNameClaim::Found {
            normalized_name, ..
        } => Some(normalized_name),
        RouteLocalEnsPrimaryNameClaim::NotFound
        | RouteLocalEnsPrimaryNameClaim::InvalidName { .. }
        | RouteLocalEnsPrimaryNameClaim::ExecutionFailed { .. } => None,
    }
}
