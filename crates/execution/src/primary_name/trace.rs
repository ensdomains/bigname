use anyhow::{Context, Result, bail};
use bigname_storage::{ExecutionOutcome, ExecutionTrace};
use uuid::Uuid;

use crate::json_helpers::{required_array, required_object};
use crate::validation::{
    SupportedResolutionPathClass, SupportedResolutionStepSummary,
    ensure_contains_basenames_l1_resolver_call, ensure_contains_universal_resolver_call,
    ensure_single_ethereum_mainnet_position, ensure_steps_do_not_use_deferred_execution_paths,
    manifest_versions_include_source_family_for_context, required_chain_positions,
};
use crate::{BASENAMES_NAMESPACE, ENS_NAMESPACE, VERIFIED_PRIMARY_NAME_REQUEST_TYPE};

use super::context::{verified_primary_context_label, verified_primary_execution_source_family};
use super::payload::extract_verified_primary_name_section;
use super::{
    VerifiedPrimaryNameSection, VerifiedPrimaryNameStatus, VerifiedPrimaryNameTuple,
    normalized_verified_primary_name_request_key,
};

pub(super) fn validate_verified_primary_trace(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if trace.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE {
        bail!(
            "{context} trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        );
    }
    if trace.namespace != tuple.namespace {
        bail!(
            "{context} trace {} must use namespace {}",
            trace.execution_trace_id,
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

    let expected_request_key = normalized_verified_primary_name_request_key(
        &tuple.namespace,
        &tuple.normalized_address,
        &tuple.coin_type,
    );
    if trace.request_key != expected_request_key {
        bail!(
            "{context} trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        &format!("{context} trace.chain_context.requested_positions"),
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        &format!("{context} trace.chain_context.requested_positions"),
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        &format!("{context} trace.gateway_digests"),
    )?;
    if tuple.namespace == ENS_NAMESPACE && !gateway_digests.is_empty() {
        bail!("{context} must keep gateway_digests empty");
    }

    if !manifest_versions_include_source_family_for_context(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
        verified_primary_execution_source_family(&tuple.namespace)?,
        context,
    )? {
        bail!(
            "{context} must include source_family {} in manifest context or cache key",
            verified_primary_execution_source_family(&tuple.namespace)?
        );
    }

    let step_summary = if tuple.namespace == ENS_NAMESPACE {
        ensure_steps_do_not_use_deferred_execution_paths(
            &trace.steps,
            trace.execution_trace_id,
            context,
            SupportedResolutionPathClass::Direct,
        )?
    } else {
        ensure_steps_are_supported_basenames_verified_primary_path(
            trace,
            trace.execution_trace_id,
            matches!(
                verified_primary_name.status,
                VerifiedPrimaryNameStatus::Success
                    | VerifiedPrimaryNameStatus::Mismatch
                    | VerifiedPrimaryNameStatus::ExecutionFailed
            ),
        )?
    };
    if matches!(
        verified_primary_name.status,
        VerifiedPrimaryNameStatus::Success | VerifiedPrimaryNameStatus::Mismatch
    ) {
        if tuple.namespace == ENS_NAMESPACE && !step_summary.saw_universal_resolver_call {
            bail!(
                "{context} trace {} must include step_kind call_universal_resolver for status {:?}",
                trace.execution_trace_id,
                verified_primary_name.status
            );
        }
        match tuple.namespace.as_str() {
            ENS_NAMESPACE => ensure_contains_universal_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            BASENAMES_NAMESPACE => ensure_contains_basenames_l1_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            _ => unreachable!("unsupported verified-primary namespace already rejected"),
        }
    } else if !required_array(
        Some(&trace.contracts_called),
        &format!("{context} trace.contracts_called"),
    )?
    .is_empty()
    {
        match tuple.namespace.as_str() {
            ENS_NAMESPACE => ensure_contains_universal_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            BASENAMES_NAMESPACE => ensure_contains_basenames_l1_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            _ => unreachable!("unsupported verified-primary namespace already rejected"),
        }
    }

    validate_verified_primary_trace_terminal_payloads(trace, verified_primary_name)?;

    Ok(())
}

fn validate_verified_primary_trace_terminal_payloads(
    trace: &ExecutionTrace,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&trace.namespace)?;
    match verified_primary_name.status {
        VerifiedPrimaryNameStatus::ExecutionFailed => {
            if trace.final_payload.is_some() {
                bail!(
                    "{context} execution_failed trace {} must not set final_payload",
                    trace.execution_trace_id
                );
            }
            required_object(
                trace.failure_payload.as_ref(),
                &format!("{context} execution_failed trace.failure_payload"),
            )?;
        }
        _ => {
            if trace.failure_payload.is_some() {
                bail!(
                    "{context} trace {} must not set failure_payload unless status is execution_failed",
                    trace.execution_trace_id
                );
            }
            let final_payload = trace.final_payload.as_ref().with_context(|| {
                format!(
                    "{context} trace {} must set final_payload when status is not execution_failed",
                    trace.execution_trace_id
                )
            })?;
            let final_verified_primary_name = extract_verified_primary_name_section(
                Some(final_payload),
                &format!("{context} trace.final_payload"),
                &trace.namespace,
            )?;
            if final_verified_primary_name != *verified_primary_name {
                bail!(
                    "{context} trace.final_payload.verified_primary_name must match outcome_payload.verified_primary_name"
                );
            }
        }
    }

    Ok(())
}

fn ensure_steps_are_supported_basenames_verified_primary_path(
    trace: &ExecutionTrace,
    execution_trace_id: Uuid,
    require_l1_resolver_step: bool,
) -> Result<SupportedResolutionStepSummary> {
    let mut saw_l1_resolver_call = false;
    for step in &trace.steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("alias")
            || normalized.contains("wildcard")
            || normalized.contains("subregistry")
            || normalized.contains("ancestor")
            || normalized.contains("universal_resolver")
        {
            bail!(
                "Basenames verified-primary trace {} must not persist out-of-class step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("l1_resolver") {
            saw_l1_resolver_call = true;
        }
    }

    if require_l1_resolver_step && !saw_l1_resolver_call {
        bail!(
            "Basenames verified-primary trace {} must include an L1 resolver step",
            execution_trace_id
        );
    }

    Ok(SupportedResolutionStepSummary::default())
}
