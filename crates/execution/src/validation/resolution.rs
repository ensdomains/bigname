use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionOutcome, ExecutionTrace, RawCallSnapshot,
    SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey,
    parse_supported_verified_resolution_record_key,
};
use serde_json::Value;

use crate::json_helpers::{
    ensure_absent, optional_nonempty_string_field, required_array, required_coin_type_field,
    required_nonempty_string_field, required_object, required_string,
};
use crate::persistence::PersistEnsExactNameVerifiedResolutionRequest;
use crate::{
    BASENAMES_EXECUTION_SOURCE_FAMILY, BASENAMES_NAMESPACE, ENS_EXECUTION_SOURCE_FAMILY,
    ENS_NAMESPACE, VERIFIED_RESOLUTION_REQUEST_TYPE,
};

use super::path::{
    ensure_basenames_requested_positions,
    ensure_steps_are_supported_basenames_transport_direct_path,
    ensure_steps_are_supported_exact_surface_path,
};
use super::{
    RequestedSelectorSet, VerifiedQueryStatus, VerifiedQuerySummary,
    ensure_contains_basenames_l1_resolver_call, ensure_contains_universal_resolver_call,
    ensure_single_ethereum_mainnet_position, manifest_versions_include_source_family_for_context,
    normalized_request_key, required_chain_positions,
};

pub(crate) fn validate_direct_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<Vec<VerifiedQuerySummary>> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    ensure_requested_selectors_match_queries(&requested_selectors, &queries)?;
    validate_trace(
        &request.trace,
        &request.outcome,
        &requested_selectors,
        &queries,
    )?;
    validate_outcome(&request.outcome, &request.trace, &queries)?;
    validate_raw_call_snapshots(
        &request.raw_call_snapshots,
        &request.outcome,
        &requested_selectors,
    )?;
    Ok(queries)
}

pub(crate) fn validate_basenames_transport_direct_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<Vec<VerifiedQuerySummary>> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    ensure_requested_selectors_match_queries(&requested_selectors, &queries)?;
    validate_basenames_transport_direct_trace(
        &request.trace,
        &request.outcome,
        &requested_selectors,
        &queries,
    )?;
    validate_basenames_transport_direct_outcome(
        &request.outcome,
        &request.trace,
        &requested_selectors,
        &queries,
    )?;
    if !request.raw_call_snapshots.is_empty() {
        bail!(
            "Basenames transport-assisted direct persistence does not admit raw_call_snapshots yet"
        );
    }
    Ok(queries)
}

fn validate_basenames_transport_direct_trace(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "Basenames transport-direct verified resolution trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if trace.namespace != BASENAMES_NAMESPACE {
        bail!(
            "Basenames transport-direct verified resolution trace {} must use namespace {}",
            trace.execution_trace_id,
            BASENAMES_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "Basenames transport-direct verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let expected_request_key = normalized_request_key(
        BASENAMES_NAMESPACE,
        &requested_selectors.surface,
        &requested_selectors.ordered_record_keys,
    );
    if trace.request_key != expected_request_key {
        bail!(
            "Basenames transport-direct verified resolution trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "Basenames transport-direct verified resolution trace.chain_context.requested_positions",
    )?;
    ensure_basenames_requested_positions(
        &requested_positions,
        "Basenames transport-direct verified resolution trace.chain_context.requested_positions",
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        "Basenames transport-direct verified resolution trace.gateway_digests",
    )?;
    if gateway_digests.is_empty() {
        bail!(
            "Basenames transport-direct verified resolution must record gateway_digests for CCIP readback"
        );
    }

    if !manifest_versions_include_source_family_for_context(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
        BASENAMES_EXECUTION_SOURCE_FAMILY,
        "Basenames transport-direct verified resolution",
    )? {
        bail!(
            "Basenames transport-direct verified resolution must include source_family {} in manifest context or cache key",
            BASENAMES_EXECUTION_SOURCE_FAMILY
        );
    }

    ensure_contains_basenames_l1_resolver_call(
        &trace.contracts_called,
        trace.execution_trace_id,
        "Basenames transport-direct verified resolution",
    )?;
    ensure_steps_are_supported_basenames_transport_direct_path(
        trace,
        requested_selectors,
        trace.execution_trace_id,
    )?;
    validate_trace_terminal_payloads(trace, queries)?;

    Ok(())
}

fn validate_basenames_transport_direct_outcome(
    outcome: &ExecutionOutcome,
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "Basenames transport-direct verified resolution outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if outcome.namespace != BASENAMES_NAMESPACE {
        bail!(
            "Basenames transport-direct verified resolution outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            BASENAMES_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "Basenames transport-direct verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "Basenames transport-direct verified resolution trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "Basenames transport-direct verified resolution outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    let expected_request_key = normalized_request_key(
        BASENAMES_NAMESPACE,
        &requested_selectors.surface,
        &requested_selectors.ordered_record_keys,
    );
    if outcome.cache_key.request_key != expected_request_key {
        bail!(
            "Basenames transport-direct verified resolution outcome request_key {} does not match expected {}",
            outcome.cache_key.request_key,
            expected_request_key
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "Basenames transport-direct verified resolution outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "Basenames transport-direct verified resolution cache_key.requested_chain_positions",
    )?;
    ensure_basenames_requested_positions(
        &requested_positions,
        "Basenames transport-direct verified resolution cache_key.requested_chain_positions",
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "Basenames transport-direct verified resolution trace.chain_context.requested_positions",
    )?;
    if trace_positions != requested_positions {
        bail!(
            "Basenames transport-direct verified resolution trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    if queries
        .iter()
        .all(|query| query.status == VerifiedQueryStatus::ExecutionFailed)
    {
        required_object(
            outcome.failure_payload.as_ref(),
            "Basenames transport-direct verified resolution execution_failed outcome.failure_payload",
        )?;
    } else if outcome.failure_payload.is_some() {
        bail!(
            "Basenames transport-direct verified resolution outcome for request_key {} must not set failure_payload unless every selector status is execution_failed",
            outcome.cache_key.request_key
        );
    }

    Ok(())
}

pub(crate) fn extract_requested_selectors(trace: &ExecutionTrace) -> Result<RequestedSelectorSet> {
    let request_metadata = required_object(
        Some(&trace.request_metadata),
        "ENS direct-path verified resolution trace.request_metadata",
    )?;
    let surface = required_string(
        request_metadata,
        "surface",
        "ENS direct-path verified resolution trace.request_metadata",
    )?
    .to_owned();

    let ordered_record_keys = match (
        request_metadata.get("record_keys"),
        request_metadata.get("record_key"),
    ) {
        (Some(record_keys), Some(record_key)) => {
            let parsed_record_keys = parse_requested_record_keys(
                record_keys,
                "ENS direct-path verified resolution trace.request_metadata.record_keys",
            )?;
            let singular_record_key = record_key
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .context(
                    "ENS direct-path verified resolution trace.request_metadata must include non-empty string field record_key",
                )?;
            if parsed_record_keys.len() != 1 || parsed_record_keys[0] != singular_record_key {
                bail!(
                    "ENS direct-path verified resolution trace.request_metadata.record_key must match record_keys when both are present"
                );
            }
            parsed_record_keys
        }
        (Some(record_keys), None) => parse_requested_record_keys(
            record_keys,
            "ENS direct-path verified resolution trace.request_metadata.record_keys",
        )?,
        (None, Some(_)) => vec![
            required_string(
                request_metadata,
                "record_key",
                "ENS direct-path verified resolution trace.request_metadata",
            )?
            .to_owned(),
        ],
        (None, None) => bail!(
            "ENS direct-path verified resolution trace.request_metadata must include record_key or record_keys"
        ),
    };

    validate_ordered_record_keys(
        &ordered_record_keys,
        "ENS direct-path verified resolution trace.request_metadata",
    )?;

    let binding_kind = optional_nonempty_string_field(
        request_metadata,
        "binding_kind",
        "ENS direct-path verified resolution trace.request_metadata",
    )?;

    Ok(RequestedSelectorSet {
        surface,
        ordered_record_keys,
        binding_kind,
    })
}

fn parse_requested_record_keys(value: &Value, context: &str) -> Result<Vec<String>> {
    let items = required_array(Some(value), context)?;
    if items.is_empty() {
        bail!("{context} must include at least one selector");
    }

    let mut record_keys = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        record_keys.push(
            item.as_str()
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("{context}[{index}] must be a non-empty string"))?
                .to_owned(),
        );
    }
    Ok(record_keys)
}

fn validate_ordered_record_keys(record_keys: &[String], context: &str) -> Result<()> {
    if record_keys.is_empty() {
        bail!("{context} must include at least one selector");
    }

    let mut seen = BTreeSet::new();
    for record_key in record_keys {
        parse_supported_verified_record_key(record_key)?;
        if !seen.insert(record_key.clone()) {
            bail!("{context} must not contain duplicate selectors ({record_key})");
        }
    }

    Ok(())
}

pub(crate) fn extract_supported_verified_queries(
    outcome: &ExecutionOutcome,
) -> Result<Vec<VerifiedQuerySummary>> {
    let outcome_payload = outcome
        .outcome_payload
        .as_ref()
        .context("ENS direct-path verified resolution outcome must set outcome_payload")?;
    extract_verified_queries_from_payload(
        outcome_payload,
        "ENS direct-path verified resolution outcome_payload",
    )
}

fn extract_verified_queries_from_payload(
    payload: &Value,
    context: &str,
) -> Result<Vec<VerifiedQuerySummary>> {
    let payload = required_object(Some(payload), context)?;
    let verified_queries = required_array(
        payload.get("verified_queries"),
        &format!("{context}.verified_queries"),
    )?;
    if verified_queries.is_empty() {
        bail!("{context} must include at least one verified query");
    }

    let mut queries = Vec::with_capacity(verified_queries.len());
    let mut seen_record_keys = BTreeSet::new();
    for (index, query) in verified_queries.iter().enumerate() {
        let query_context = format!("{context}.verified_queries[{index}]");
        let query = required_object(Some(query), &query_context)?;
        if query.contains_key("unsupported_reason") {
            bail!("ENS direct-path verified resolution does not persist unsupported selectors");
        }

        let record_key = required_string(query, "record_key", &query_context)?.to_owned();
        if !seen_record_keys.insert(record_key.clone()) {
            bail!("{context}.verified_queries must not contain duplicate selectors ({record_key})");
        }

        let selector = parse_supported_verified_record_key(&record_key)?;
        let (status, value, failure_reason) = match required_string(
            query,
            "status",
            &query_context,
        )? {
            "success" => {
                let value = required_object(query.get("value"), &format!("{query_context}.value"))?;
                if let SupportedVerifiedRecordKey::Addr { coin_type } = &selector {
                    let value_coin_type =
                        required_string(value, "coin_type", &format!("{query_context}.value"))?;
                    if value_coin_type != coin_type {
                        bail!(
                            "ENS direct-path verified resolution query value coin_type {} does not match record_key {}",
                            value_coin_type,
                            record_key
                        );
                    }
                }
                let resolved_value = required_nonempty_string_field(
                    value,
                    "value",
                    &format!("{query_context}.value"),
                )?;
                if query.contains_key("failure_reason") {
                    bail!(
                        "ENS direct-path verified resolution success query must not set failure_reason"
                    );
                }
                (VerifiedQueryStatus::Success, Some(resolved_value), None)
            }
            "not_found" => {
                ensure_absent(query, "value", &query_context)?;
                let failure_reason =
                    optional_nonempty_string_field(query, "failure_reason", &query_context)?;
                (VerifiedQueryStatus::NotFound, None, failure_reason)
            }
            "execution_failed" => {
                ensure_absent(query, "value", &query_context)?;
                let failure_reason =
                    required_nonempty_string_field(query, "failure_reason", &query_context)?;
                (
                    VerifiedQueryStatus::ExecutionFailed,
                    None,
                    Some(failure_reason),
                )
            }
            status => bail!(
                "ENS direct-path verified resolution only supports success, not_found, and execution_failed selector results; found {status}"
            ),
        };

        queries.push(VerifiedQuerySummary {
            record_key,
            selector,
            status,
            value,
            failure_reason,
        });
    }

    Ok(queries)
}

fn ensure_requested_selectors_match_queries(
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if requested_selectors.ordered_record_keys.len() != queries.len() {
        bail!(
            "ENS direct-path verified resolution trace.request_metadata selectors {} do not match outcome verified query count {}",
            requested_selectors.ordered_record_keys.len(),
            queries.len()
        );
    }

    for (index, (requested_record_key, query)) in requested_selectors
        .ordered_record_keys
        .iter()
        .zip(queries.iter())
        .enumerate()
    {
        if requested_record_key != &query.record_key {
            bail!(
                "ENS direct-path verified resolution trace.request_metadata.record_keys[{index}] {} does not match outcome verified_queries[{index}] {}",
                requested_record_key,
                query.record_key
            );
        }
    }

    Ok(())
}

fn validate_trace(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "ENS direct-path verified resolution trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if trace.namespace != ENS_NAMESPACE {
        bail!(
            "ENS direct-path verified resolution trace {} must use namespace {}",
            trace.execution_trace_id,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS direct-path verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let expected_request_key = normalized_request_key(
        ENS_NAMESPACE,
        &requested_selectors.surface,
        &requested_selectors.ordered_record_keys,
    );
    if trace.request_key != expected_request_key {
        bail!(
            "ENS direct-path verified resolution trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        "ENS direct-path verified resolution trace.gateway_digests",
    )?;
    if !gateway_digests.is_empty() {
        bail!("ENS direct-path verified resolution must keep gateway_digests empty");
    }

    if !manifest_versions_include_source_family_for_context(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
        ENS_EXECUTION_SOURCE_FAMILY,
        "ENS direct-path verified resolution",
    )? {
        bail!(
            "ENS direct-path verified resolution must include source_family {} in manifest context or cache key",
            ENS_EXECUTION_SOURCE_FAMILY
        );
    }

    ensure_contains_universal_resolver_call(
        &trace.contracts_called,
        trace.execution_trace_id,
        "ENS direct-path verified resolution",
    )?;
    ensure_steps_are_supported_exact_surface_path(
        trace,
        requested_selectors,
        trace.execution_trace_id,
    )?;
    validate_trace_terminal_payloads(trace, queries)?;

    Ok(())
}

fn validate_outcome(
    outcome: &ExecutionOutcome,
    trace: &ExecutionTrace,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if outcome.namespace != ENS_NAMESPACE {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS direct-path verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "ENS direct-path verified resolution trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "ENS direct-path verified resolution outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "ENS direct-path verified resolution outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;
    if trace_positions != requested_positions {
        bail!(
            "ENS direct-path verified resolution trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    if queries
        .iter()
        .all(|query| query.status == VerifiedQueryStatus::ExecutionFailed)
    {
        required_object(
            outcome.failure_payload.as_ref(),
            "ENS direct-path verified resolution execution_failed outcome.failure_payload",
        )?;
    } else if outcome.failure_payload.is_some() {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must not set failure_payload unless every selector status is execution_failed",
            outcome.cache_key.request_key
        );
    }

    Ok(())
}

fn validate_trace_terminal_payloads(
    trace: &ExecutionTrace,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    let all_execution_failed = queries
        .iter()
        .all(|query| query.status == VerifiedQueryStatus::ExecutionFailed);

    if all_execution_failed {
        if trace.final_payload.is_some() {
            bail!(
                "ENS direct-path verified resolution execution_failed trace {} must not set final_payload",
                trace.execution_trace_id
            );
        }
        required_object(
            trace.failure_payload.as_ref(),
            "ENS direct-path verified resolution execution_failed trace.failure_payload",
        )?;
        return Ok(());
    }

    if trace.failure_payload.is_some() {
        bail!(
            "ENS direct-path verified resolution trace {} must not set failure_payload unless every selector status is execution_failed",
            trace.execution_trace_id
        );
    }

    let final_payload = trace.final_payload.as_ref().with_context(|| {
        format!(
            "ENS direct-path verified resolution trace {} must set final_payload when any selector resolves or returns not_found",
            trace.execution_trace_id
        )
    })?;
    if final_payload_contains_verified_queries(final_payload)? {
        let final_queries = extract_verified_queries_from_payload(
            final_payload,
            "ENS direct-path verified resolution trace.final_payload",
        )?;
        if final_queries != queries {
            bail!(
                "ENS direct-path verified resolution trace.final_payload.verified_queries must match outcome_payload.verified_queries"
            );
        }
        return Ok(());
    }

    if queries.len() != 1 {
        bail!(
            "ENS direct-path verified resolution multi-selector trace {} final_payload must include verified_queries",
            trace.execution_trace_id
        );
    }

    match queries[0].status {
        VerifiedQueryStatus::Success => validate_success_final_payload(final_payload, &queries[0]),
        VerifiedQueryStatus::NotFound => {
            validate_not_found_final_payload(final_payload, &queries[0])
        }
        VerifiedQueryStatus::ExecutionFailed => unreachable!("all execution_failed handled above"),
    }
}

fn validate_raw_call_snapshots(
    raw_call_snapshots: &[RawCallSnapshot],
    outcome: &ExecutionOutcome,
    requested_selectors: &RequestedSelectorSet,
) -> Result<()> {
    if raw_call_snapshots.is_empty() {
        return Ok(());
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;
    let requested_position = requested_positions
        .first()
        .context("ENS direct-path verified resolution must include one requested chain position")?;

    for snapshot in raw_call_snapshots {
        if snapshot.chain_id != requested_position.chain_id
            || snapshot.block_hash != requested_position.block_hash
            || snapshot.block_number != requested_position.block_number
        {
            bail!(
                "ENS direct-path verified resolution raw call snapshot for request {} must align with requested chain position {} {} {}",
                normalized_request_key(
                    ENS_NAMESPACE,
                    &requested_selectors.surface,
                    &requested_selectors.ordered_record_keys,
                ),
                requested_position.chain_id,
                requested_position.block_number,
                requested_position.block_hash
            );
        }
    }

    Ok(())
}

fn parse_supported_verified_record_key(record_key: &str) -> Result<SupportedVerifiedRecordKey> {
    parse_supported_verified_resolution_record_key(record_key)
}

fn validate_success_final_payload(
    final_payload: &Value,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    let object = required_object(
        Some(final_payload),
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    let record_kind = required_string(
        object,
        "record_kind",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    match &query.selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => {
            if record_kind != "addr" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be addr, found {}",
                    record_kind
                );
            }
            let payload_coin_type = required_coin_type_field(
                object,
                "coin_type",
                "ENS direct-path verified resolution success trace.final_payload",
            )?;
            if &payload_coin_type != coin_type {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.coin_type {} does not match outcome record_key {}",
                    payload_coin_type,
                    query.record_key
                );
            }
        }
        SupportedVerifiedRecordKey::Contenthash => {
            if record_kind != "contenthash" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be contenthash, found {}",
                    record_kind
                );
            }
        }
        SupportedVerifiedRecordKey::Avatar => {
            if record_kind != "avatar" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be avatar, found {}",
                    record_kind
                );
            }
        }
        SupportedVerifiedRecordKey::Text => {
            if record_kind != "text" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be text, found {}",
                    record_kind
                );
            }
        }
    }
    let value = required_nonempty_string_field(
        object,
        "value",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    if query
        .value
        .as_deref()
        .is_some_and(|expected_value| expected_value != value)
    {
        bail!(
            "ENS direct-path verified resolution success trace.final_payload.value {} does not match outcome query value {}",
            value,
            query.value.as_deref().unwrap_or_default()
        );
    }
    Ok(())
}

fn validate_not_found_final_payload(
    final_payload: &Value,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    let final_payload_object = required_object(
        Some(final_payload),
        "ENS direct-path verified resolution not_found trace.final_payload",
    )?;
    let failure_reason = optional_nonempty_string_field(
        final_payload_object,
        "failure_reason",
        "ENS direct-path verified resolution not_found trace.final_payload",
    )?;
    if failure_reason != query.failure_reason {
        bail!(
            "ENS direct-path verified resolution not_found trace.final_payload.failure_reason {:?} does not match outcome query failure_reason {:?}",
            failure_reason,
            query.failure_reason
        );
    }
    Ok(())
}

fn final_payload_contains_verified_queries(final_payload: &Value) -> Result<bool> {
    Ok(required_object(
        Some(final_payload),
        "ENS direct-path verified resolution trace.final_payload",
    )?
    .contains_key("verified_queries"))
}
