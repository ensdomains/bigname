use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, SELECTED_CHECKPOINT_BOUNDARY_KIND,
};
use serde_json::{Map, Value, json};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::persistence::PersistEnsVerifiedPrimaryNameRequest;
use crate::{
    ENS_EXECUTION_SOURCE_FAMILY, ENS_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID,
    OnDemandEnsPrimaryNameExecutionEvidence, VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
};

const ROUTE_LOCAL_CLAIM_METADATA_KEY: &str = "route_local_claim";
const FORWARD_CALL_ATTEMPTED_METADATA_KEY: &str = "forward_call_attempted";

#[path = "route_fallback/trace.rs"]
mod trace;
use trace::{route_local_contracts_called, route_local_steps};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteLocalEnsPrimaryNameClaim {
    Found {
        raw_name: String,
        normalized_name: String,
        resolver_address: String,
    },
    NotFound,
    InvalidName {
        raw_name: String,
        resolver_address: String,
    },
    ExecutionFailed {
        failure_reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteLocalEnsPrimaryNameExecution {
    pub claim: RouteLocalEnsPrimaryNameClaim,
    pub forward_call_attempted: bool,
}

pub struct BuildOnDemandEnsVerifiedPrimaryNameRequest<'a> {
    pub normalized_address: &'a str,
    pub claim: &'a RouteLocalEnsPrimaryNameClaim,
    pub verified_primary_name: Value,
    pub block_number: i64,
    pub block_hash: &'a str,
    pub block_timestamp: &'a str,
    pub manifest_versions: Value,
    pub forward_call_attempted: bool,
    pub reverse_latency_ms: i64,
    pub forward_latency_ms: Option<i64>,
    pub execution_evidence: &'a OnDemandEnsPrimaryNameExecutionEvidence,
}

pub fn build_on_demand_ens_verified_primary_name_request(
    request: BuildOnDemandEnsVerifiedPrimaryNameRequest<'_>,
) -> Result<PersistEnsVerifiedPrimaryNameRequest> {
    validate_build_request(&request)?;

    let execution_trace_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let finished_at = OffsetDateTime::from_unix_timestamp(now.unix_timestamp())
        .context("failed to normalize on-demand ENS primary-name execution timestamp")?;
    let request_key = format!("{ENS_NAMESPACE}:{}:60", request.normalized_address);
    let requested_chain_positions = json!([{
        "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
        "block_number": request.block_number,
        "block_hash": request.block_hash,
    }]);
    let boundary = route_local_boundary(&request);
    let cache_key = ExecutionCacheKey {
        request_key: request_key.clone(),
        requested_chain_positions: requested_chain_positions.clone(),
        manifest_versions: request.manifest_versions.clone(),
        topology_version_boundary: boundary.clone(),
        record_version_boundary: boundary,
    };
    let route_local_claim = route_local_claim_value(request.claim);
    let failure_reason = request
        .verified_primary_name
        .get("failure_reason")
        .and_then(Value::as_str);
    let execution_failed = request
        .verified_primary_name
        .get("status")
        .and_then(Value::as_str)
        == Some("execution_failed");
    let failure_stage = matches!(
        request.claim,
        RouteLocalEnsPrimaryNameClaim::ExecutionFailed { .. }
    )
    .then_some("call_ens_reverse_lookup")
    .unwrap_or("call_universal_resolver");
    let failure_payload = execution_failed.then(|| {
        json!({
            "failure_reason": failure_reason.unwrap_or("resolver_call_failed"),
            "stage": failure_stage,
        })
    });
    let final_payload = (!execution_failed).then(|| {
        json!({
            "verified_primary_name": request.verified_primary_name.clone(),
        })
    });
    let outcome_payload = json!({
        "verified_primary_name": request.verified_primary_name.clone(),
    });
    let outcome_failure_payload = execution_failed.then(|| {
        json!({
            "failure_reason": failure_reason.unwrap_or("resolver_call_failed"),
            "stage": failure_stage,
        })
    });

    let trace = ExecutionTrace {
        execution_trace_id,
        request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        request_key: request_key.clone(),
        namespace: ENS_NAMESPACE.to_owned(),
        chain_context: json!({
            "requested_positions": requested_chain_positions,
        }),
        manifest_context: json!({
            "manifest_versions": request.manifest_versions.clone(),
        }),
        contracts_called: route_local_contracts_called(&request),
        gateway_digests: json!(request.execution_evidence.gateway_digests.clone()),
        final_payload,
        failure_payload,
        request_metadata: json!({
            "normalized_address": request.normalized_address,
            "coin_type": "60",
            "namespace": ENS_NAMESPACE,
            ROUTE_LOCAL_CLAIM_METADATA_KEY: route_local_claim,
            FORWARD_CALL_ATTEMPTED_METADATA_KEY: request.forward_call_attempted,
            "cache_identity": {
                "requested_chain_positions": cache_key.requested_chain_positions.clone(),
                "manifest_versions": cache_key.manifest_versions.clone(),
                "topology_version_boundary": cache_key.topology_version_boundary.clone(),
                "record_version_boundary": cache_key.record_version_boundary.clone(),
            }
        }),
        finished_at: Some(finished_at),
        steps: route_local_steps(&request),
    };
    let outcome = ExecutionOutcome {
        cache_key,
        execution_trace_id,
        request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        outcome_payload: Some(outcome_payload),
        failure_payload: outcome_failure_payload,
        finished_at,
    };

    Ok(PersistEnsVerifiedPrimaryNameRequest { trace, outcome })
}

pub fn route_local_ens_primary_name_execution(
    trace: &ExecutionTrace,
) -> Result<Option<RouteLocalEnsPrimaryNameExecution>> {
    let Some(metadata) = trace.request_metadata.as_object() else {
        return Ok(None);
    };
    let Some(claim) = metadata.get(ROUTE_LOCAL_CLAIM_METADATA_KEY) else {
        return Ok(None);
    };
    if trace.namespace != ENS_NAMESPACE {
        bail!("route-local ENS primary-name claim metadata requires namespace {ENS_NAMESPACE}");
    }
    if metadata.get("coin_type").and_then(Value::as_str) != Some("60") {
        bail!("route-local ENS primary-name claim metadata requires coin_type 60");
    }
    let forward_call_attempted = metadata
        .get(FORWARD_CALL_ATTEMPTED_METADATA_KEY)
        .and_then(Value::as_bool)
        .context("route-local ENS primary-name metadata must include forward_call_attempted")?;
    let claim = parse_route_local_claim(claim)?;
    validate_claim_call_order(&claim, forward_call_attempted)?;

    Ok(Some(RouteLocalEnsPrimaryNameExecution {
        claim,
        forward_call_attempted,
    }))
}

pub(crate) fn validate_route_local_ens_primary_name_execution(
    trace: &ExecutionTrace,
    verified_primary_name: &Value,
) -> Result<Option<RouteLocalEnsPrimaryNameExecution>> {
    let execution = route_local_ens_primary_name_execution(trace)?;
    if let Some(execution) = execution.as_ref() {
        trace::validate_route_local_steps(trace, execution)?;
        validate_claim_outcome(&execution.claim, verified_primary_name)?;
    }
    Ok(execution)
}

fn validate_build_request(request: &BuildOnDemandEnsVerifiedPrimaryNameRequest<'_>) -> Result<()> {
    if request.block_number < 0 || request.block_hash.trim().is_empty() {
        bail!("on-demand ENS primary-name persistence requires a selected block");
    }
    if request.block_timestamp.trim().is_empty() {
        bail!("on-demand ENS primary-name persistence requires a selected block timestamp");
    }
    if request.reverse_latency_ms < 0 || request.forward_latency_ms.is_some_and(|value| value < 0) {
        bail!("on-demand ENS primary-name persistence latencies must be non-negative");
    }
    if request
        .manifest_versions
        .as_array()
        .is_none_or(|versions| versions.is_empty())
    {
        bail!("on-demand ENS primary-name persistence requires manifest_versions");
    }
    let has_ens_execution = request
        .manifest_versions
        .as_array()
        .is_some_and(|versions| {
            versions.iter().any(|version| {
                version.get("source_family").and_then(Value::as_str)
                    == Some(ENS_EXECUTION_SOURCE_FAMILY)
            })
        });
    if !has_ens_execution {
        bail!(
            "on-demand ENS primary-name persistence requires source_family {ENS_EXECUTION_SOURCE_FAMILY}"
        );
    }
    validate_claim_call_order(request.claim, request.forward_call_attempted)?;
    validate_claim_outcome(request.claim, &request.verified_primary_name)?;
    validate_execution_evidence(request)?;
    if request.forward_call_attempted != request.forward_latency_ms.is_some() {
        bail!("forward_call_attempted must match forward_latency_ms presence");
    }
    Ok(())
}

fn validate_execution_evidence(
    request: &BuildOnDemandEnsVerifiedPrimaryNameRequest<'_>,
) -> Result<()> {
    if !request.forward_call_attempted
        && (!request.execution_evidence.gateway_digests.is_empty()
            || !request.execution_evidence.ccip_step_payloads.is_empty())
    {
        bail!("CCIP evidence requires a route-local forward call");
    }
    if !request.execution_evidence.gateway_digests.is_empty()
        && request.execution_evidence.ccip_step_payloads.is_empty()
    {
        bail!("route-local gateway digests require CCIP trace steps");
    }
    if request
        .execution_evidence
        .gateway_digests
        .iter()
        .any(|digest| digest.is_empty())
        || request
            .execution_evidence
            .ccip_step_payloads
            .iter()
            .any(|payload| !payload.is_object())
    {
        bail!("route-local CCIP evidence is malformed");
    }

    let contracts = &request.execution_evidence.contracts_called;
    let Some(reverse_contract_count) = contracts
        .len()
        .checked_sub(usize::from(request.forward_call_attempted))
    else {
        bail!("route-local forward call is missing contract evidence");
    };
    let reverse_contract_count_is_valid = match request.claim {
        RouteLocalEnsPrimaryNameClaim::Found { .. }
        | RouteLocalEnsPrimaryNameClaim::InvalidName { .. } => reverse_contract_count == 2,
        RouteLocalEnsPrimaryNameClaim::NotFound => matches!(reverse_contract_count, 1 | 2),
        RouteLocalEnsPrimaryNameClaim::ExecutionFailed { .. } => reverse_contract_count <= 2,
    };
    if !reverse_contract_count_is_valid {
        bail!("route-local reverse contract evidence does not match its claim outcome");
    }
    for (index, contract) in contracts.iter().enumerate() {
        let contract = contract
            .as_object()
            .context("route-local contract evidence must be an object")?;
        if contract.get("chain_id").and_then(Value::as_str) != Some(ETHEREUM_MAINNET_CHAIN_ID) {
            bail!("route-local contract evidence must use Ethereum Mainnet");
        }
        let address = contract
            .get("contract_address")
            .and_then(Value::as_str)
            .context("route-local contract evidence must include contract_address")?;
        let selector = contract
            .get("selector")
            .and_then(Value::as_str)
            .context("route-local contract evidence must include selector")?;
        match index {
            0 if reverse_contract_count > 0 => {
                if !address.eq_ignore_ascii_case(crate::ENS_REGISTRY_ADDRESS)
                    || selector != "0x0178b8bf"
                {
                    bail!("route-local reverse evidence must begin with ENS registry resolver");
                }
            }
            1 if reverse_contract_count > 1 => {
                if selector != "0x691f3431" {
                    bail!("route-local reverse resolver evidence has the wrong selector");
                }
                if let RouteLocalEnsPrimaryNameClaim::Found {
                    resolver_address, ..
                }
                | RouteLocalEnsPrimaryNameClaim::InvalidName {
                    resolver_address, ..
                } = request.claim
                    && !address.eq_ignore_ascii_case(resolver_address)
                {
                    bail!("route-local reverse resolver evidence has the wrong address");
                }
            }
            _ if index == reverse_contract_count && request.forward_call_attempted => {
                if !address.eq_ignore_ascii_case(crate::ENS_UNIVERSAL_RESOLVER_ADDRESS)
                    || selector != "0x9061b923"
                {
                    bail!("route-local forward evidence must call the ENS Universal Resolver");
                }
            }
            _ => bail!("route-local contract evidence has an unexpected call"),
        }
    }
    Ok(())
}

fn validate_claim_call_order(
    claim: &RouteLocalEnsPrimaryNameClaim,
    forward_call_attempted: bool,
) -> Result<()> {
    let may_call_forward = matches!(
        claim,
        RouteLocalEnsPrimaryNameClaim::Found {
            raw_name,
            normalized_name,
            ..
        } if raw_name == normalized_name
    );
    if forward_call_attempted != may_call_forward {
        bail!(
            "route-local ENS primary-name forward execution must run exactly for a normalized successful claim"
        );
    }
    Ok(())
}

fn validate_claim_outcome(claim: &RouteLocalEnsPrimaryNameClaim, outcome: &Value) -> Result<()> {
    let status = outcome
        .get("status")
        .and_then(Value::as_str)
        .context("verified_primary_name.status must be set")?;
    let failure_reason = outcome.get("failure_reason").and_then(Value::as_str);
    match claim {
        RouteLocalEnsPrimaryNameClaim::NotFound if status != "not_found" => {
            bail!("route-local missing reverse claim must persist verified not_found")
        }
        RouteLocalEnsPrimaryNameClaim::InvalidName { .. }
            if status != "invalid_name"
                || failure_reason != Some("claim_name_not_normalizable") =>
        {
            bail!("route-local unnormalizable claim must persist claim_name_not_normalizable")
        }
        RouteLocalEnsPrimaryNameClaim::ExecutionFailed {
            failure_reason: claim_reason,
        } if status != "execution_failed" || failure_reason != Some(claim_reason) => {
            bail!("route-local reverse execution failure must persist the same failure reason")
        }
        RouteLocalEnsPrimaryNameClaim::Found {
            raw_name,
            normalized_name,
            ..
        } if raw_name != normalized_name
            && (status != "invalid_name"
                || failure_reason != Some(VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON)) =>
        {
            bail!("route-local non-normalized claim must persist claim_not_normalized")
        }
        RouteLocalEnsPrimaryNameClaim::Found {
            raw_name,
            normalized_name,
            ..
        } if raw_name == normalized_name
            && !matches!(
                status,
                "success" | "not_found" | "mismatch" | "execution_failed"
            ) =>
        {
            bail!("route-local normalized claim has unsupported verified status {status}")
        }
        _ => {}
    }
    Ok(())
}

fn route_local_boundary(request: &BuildOnDemandEnsVerifiedPrimaryNameRequest<'_>) -> Value {
    json!({
        "boundary_kind": SELECTED_CHECKPOINT_BOUNDARY_KIND,
        "chain_position": {
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": request.block_number,
            "block_hash": request.block_hash,
            "timestamp": request.block_timestamp,
        }
    })
}

fn route_local_claim_value(claim: &RouteLocalEnsPrimaryNameClaim) -> Value {
    match claim {
        RouteLocalEnsPrimaryNameClaim::Found {
            raw_name,
            normalized_name,
            resolver_address,
        } => json!({
            "status": "success",
            "raw_name": raw_name,
            "normalized_name": normalized_name,
            "resolver_address": resolver_address,
        }),
        RouteLocalEnsPrimaryNameClaim::NotFound => json!({ "status": "not_found" }),
        RouteLocalEnsPrimaryNameClaim::InvalidName {
            raw_name,
            resolver_address,
        } => json!({
            "status": "invalid_name",
            "raw_name": raw_name,
            "resolver_address": resolver_address,
        }),
        RouteLocalEnsPrimaryNameClaim::ExecutionFailed { failure_reason } => json!({
            "status": "execution_failed",
            "failure_reason": failure_reason,
        }),
    }
}

fn parse_route_local_claim(value: &Value) -> Result<RouteLocalEnsPrimaryNameClaim> {
    let claim = value
        .as_object()
        .context("route-local ENS primary-name claim metadata must be an object")?;
    let status = required_string(claim, "status")?;
    match status {
        "success" => Ok(RouteLocalEnsPrimaryNameClaim::Found {
            raw_name: required_string(claim, "raw_name")?.to_owned(),
            normalized_name: required_string(claim, "normalized_name")?.to_owned(),
            resolver_address: required_string(claim, "resolver_address")?.to_owned(),
        }),
        "not_found" => Ok(RouteLocalEnsPrimaryNameClaim::NotFound),
        "invalid_name" => Ok(RouteLocalEnsPrimaryNameClaim::InvalidName {
            raw_name: required_string(claim, "raw_name")?.to_owned(),
            resolver_address: required_string(claim, "resolver_address")?.to_owned(),
        }),
        "execution_failed" => Ok(RouteLocalEnsPrimaryNameClaim::ExecutionFailed {
            failure_reason: required_string(claim, "failure_reason")?.to_owned(),
        }),
        other => bail!("unsupported route-local ENS primary-name claim status {other}"),
    }
}

fn required_string<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("route-local ENS primary-name claim must include {field}"))
}
