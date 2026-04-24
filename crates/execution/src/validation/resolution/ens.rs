use anyhow::{Context, Result, bail};
use bigname_storage::{ExecutionOutcome, ExecutionTrace, RawCallSnapshot};

use super::terminal::validate_trace_terminal_payloads;
use crate::json_helpers::{required_array, required_object};
use crate::validation::path::{
    ensure_contains_universal_resolver_call, ensure_single_ethereum_mainnet_position,
    ensure_steps_are_supported_exact_surface_path,
    manifest_versions_include_source_family_for_context,
};
use crate::validation::{
    RequestedSelectorSet, VerifiedQueryStatus, VerifiedQuerySummary, normalized_request_key,
    required_chain_positions,
};
use crate::{ENS_EXECUTION_SOURCE_FAMILY, ENS_NAMESPACE, VERIFIED_RESOLUTION_REQUEST_TYPE};

pub(super) fn validate_trace(
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

pub(super) fn validate_outcome(
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

pub(super) fn validate_raw_call_snapshots(
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
