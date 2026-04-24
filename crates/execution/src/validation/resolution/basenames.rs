use anyhow::{Context, Result, bail};
use bigname_storage::{ExecutionOutcome, ExecutionTrace};

use super::terminal::validate_trace_terminal_payloads;
use crate::json_helpers::{required_array, required_object};
use crate::validation::path::{
    ensure_basenames_requested_positions, ensure_contains_basenames_l1_resolver_call,
    ensure_steps_are_supported_basenames_transport_direct_path,
    manifest_versions_include_source_family_for_context,
};
use crate::validation::{
    RequestedSelectorSet, VerifiedQueryStatus, VerifiedQuerySummary, normalized_request_key,
    required_chain_positions,
};
use crate::{
    BASENAMES_EXECUTION_SOURCE_FAMILY, BASENAMES_NAMESPACE, VERIFIED_RESOLUTION_REQUEST_TYPE,
};

pub(super) fn validate_basenames_transport_direct_trace(
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

pub(super) fn validate_basenames_transport_direct_outcome(
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
