use anyhow::{Context, Result, bail};
use bigname_storage::{ExecutionOutcome, ExecutionTrace};

use crate::VERIFIED_PRIMARY_NAME_REQUEST_TYPE;
use crate::json_helpers::required_object;
use crate::validation::{ensure_single_ethereum_mainnet_position, required_chain_positions};

use super::context::verified_primary_context_label;
use super::{
    VerifiedPrimaryNameSection, VerifiedPrimaryNameStatus, VerifiedPrimaryNameTuple,
    normalized_verified_primary_name_request_key,
};

pub(super) fn validate_verified_primary_outcome(
    outcome: &ExecutionOutcome,
    trace: &ExecutionTrace,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if outcome.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE {
        bail!(
            "{context} outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        );
    }
    if outcome.namespace != tuple.namespace {
        bail!(
            "{context} outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            tuple.namespace
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "{context} outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "{context} trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "{context} outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    let expected_request_key = normalized_verified_primary_name_request_key(
        &tuple.namespace,
        &tuple.normalized_address,
        &tuple.coin_type,
    );
    if outcome.cache_key.request_key != expected_request_key {
        bail!(
            "{context} outcome request_key {} does not match expected {}",
            outcome.cache_key.request_key,
            expected_request_key
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "{context} outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        &format!("{context} cache_key.requested_chain_positions"),
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        &format!("{context} cache_key.requested_chain_positions"),
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        &format!("{context} trace.chain_context.requested_positions"),
    )?;
    if trace_positions != requested_positions {
        bail!(
            "{context} trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    match verified_primary_name.status {
        VerifiedPrimaryNameStatus::ExecutionFailed => {
            required_object(
                outcome.failure_payload.as_ref(),
                &format!("{context} execution_failed outcome.failure_payload"),
            )?;
        }
        _ if outcome.failure_payload.is_some() => {
            bail!(
                "{context} outcome for request_key {} must not set failure_payload unless status is execution_failed",
                outcome.cache_key.request_key
            );
        }
        _ => {}
    }

    Ok(())
}
