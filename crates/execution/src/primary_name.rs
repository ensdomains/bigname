use anyhow::{Context, Result, bail};
use bigname_storage::{ExecutionOutcome, ExecutionTrace, load_primary_name_current};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::json_helpers::{
    ensure_absent, ensure_only_allowed_fields, optional_nonempty_string_field, required_array,
    required_coin_type_field, required_nonempty_string_field, required_object, required_string,
};
use crate::persistence::{
    PersistEnsVerifiedPrimaryNameRequest, VerifiedPrimaryNameReadbackProvenance,
};
use crate::validation::{
    SupportedResolutionPathClass, SupportedResolutionStepSummary,
    ensure_contains_basenames_l1_resolver_call, ensure_contains_universal_resolver_call,
    ensure_single_ethereum_mainnet_position, ensure_steps_do_not_use_deferred_execution_paths,
    manifest_versions_include_source_family_for_context, normalize_address,
    required_chain_positions,
};
use crate::{
    BASENAMES_EXECUTION_SOURCE_FAMILY, BASENAMES_NAMESPACE, ENS_EXECUTION_SOURCE_FAMILY,
    ENS_NAMESPACE, VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerifiedPrimaryNameStatus {
    Success,
    NotFound,
    Mismatch,
    InvalidName,
    ExecutionFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedPrimaryNameTuple {
    pub(crate) namespace: String,
    pub(crate) normalized_address: String,
    pub(crate) coin_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedPrimaryNameSection {
    pub(crate) section: Value,
    pub(crate) status: VerifiedPrimaryNameStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedVerifiedPrimaryName {
    pub(crate) tuple: VerifiedPrimaryNameTuple,
    pub(crate) verified_primary_name: VerifiedPrimaryNameSection,
}

pub(crate) fn verified_primary_context_label(namespace: &str) -> Result<&'static str> {
    match namespace {
        ENS_NAMESPACE => Ok("ENS verified-primary"),
        BASENAMES_NAMESPACE => Ok("Basenames verified-primary"),
        other => bail!("verified-primary namespace {other} is unsupported"),
    }
}

fn verified_primary_execution_source_family(namespace: &str) -> Result<&'static str> {
    match namespace {
        ENS_NAMESPACE => Ok(ENS_EXECUTION_SOURCE_FAMILY),
        BASENAMES_NAMESPACE => Ok(BASENAMES_EXECUTION_SOURCE_FAMILY),
        other => bail!("verified-primary namespace {other} is unsupported"),
    }
}

pub(crate) fn validate_verified_primary_request(
    request: &PersistEnsVerifiedPrimaryNameRequest,
) -> Result<ValidatedVerifiedPrimaryName> {
    let tuple = extract_verified_primary_tuple(&request.trace)?;
    let verified_primary_name = extract_verified_primary_name_section(
        request.outcome.outcome_payload.as_ref(),
        "verified-primary outcome_payload",
        &tuple.namespace,
    )?;
    validate_verified_primary_trace(
        &request.trace,
        &request.outcome,
        &tuple,
        &verified_primary_name,
    )?;
    validate_verified_primary_outcome(
        &request.outcome,
        &request.trace,
        &tuple,
        &verified_primary_name,
    )?;

    Ok(ValidatedVerifiedPrimaryName {
        tuple,
        verified_primary_name,
    })
}

pub(crate) fn validate_verified_primary_trace_and_outcome(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<ValidatedVerifiedPrimaryName> {
    let tuple = extract_verified_primary_tuple(trace)?;
    let verified_primary_name = extract_verified_primary_name_section(
        outcome.outcome_payload.as_ref(),
        "verified-primary outcome_payload",
        &tuple.namespace,
    )?;
    validate_verified_primary_trace(trace, outcome, &tuple, &verified_primary_name)?;
    validate_verified_primary_outcome(outcome, trace, &tuple, &verified_primary_name)?;

    Ok(ValidatedVerifiedPrimaryName {
        tuple,
        verified_primary_name,
    })
}

pub(crate) fn extract_verified_primary_readback_provenance(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<VerifiedPrimaryNameReadbackProvenance> {
    let context = verified_primary_context_label(&trace.namespace)?;
    let cache_manifest_versions = required_array(
        Some(&outcome.cache_key.manifest_versions),
        &format!("{context} cache_key.manifest_versions"),
    )?;
    if let Some(trace_manifest_versions) = trace.manifest_context.get("manifest_versions") {
        let trace_manifest_versions = required_array(
            Some(trace_manifest_versions),
            &format!("{context} trace.manifest_context.manifest_versions"),
        )?;
        if trace_manifest_versions != cache_manifest_versions {
            bail!(
                "{context} trace.manifest_context.manifest_versions must match cache_key.manifest_versions"
            );
        }
    }

    Ok(VerifiedPrimaryNameReadbackProvenance {
        execution_trace_id: trace.execution_trace_id,
        manifest_versions: Value::Array(cache_manifest_versions.clone()),
    })
}

fn extract_verified_primary_tuple(trace: &ExecutionTrace) -> Result<VerifiedPrimaryNameTuple> {
    let trace_namespace = trace.namespace.as_str();
    let context = verified_primary_context_label(trace_namespace)?;
    let request_metadata = required_object(
        Some(&trace.request_metadata),
        &format!("{context} trace.request_metadata"),
    )?;
    let normalized_address = required_string(
        request_metadata,
        "normalized_address",
        &format!("{context} trace.request_metadata"),
    )?
    .to_owned();
    if normalized_address != normalize_address(&normalized_address) {
        bail!("{context} trace.request_metadata.normalized_address must already be lowercase");
    }

    let coin_type = required_coin_type_field(
        request_metadata,
        "coin_type",
        &format!("{context} trace.request_metadata"),
    )?;
    let namespace = if let Some(namespace) = optional_nonempty_string_field(
        request_metadata,
        "namespace",
        &format!("{context} trace.request_metadata"),
    )? {
        if namespace != trace_namespace {
            bail!(
                "{context} trace.request_metadata.namespace must be {}",
                trace_namespace
            );
        }
        namespace.to_owned()
    } else {
        trace.namespace.clone()
    };

    Ok(VerifiedPrimaryNameTuple {
        namespace,
        normalized_address,
        coin_type,
    })
}

fn extract_verified_primary_name_section(
    payload: Option<&Value>,
    context: &str,
    namespace: &str,
) -> Result<VerifiedPrimaryNameSection> {
    let payload = required_object(payload, context)?;
    ensure_only_allowed_fields(payload, &["verified_primary_name"], context)?;

    let section_context = format!("{context}.verified_primary_name");
    let section = required_object(payload.get("verified_primary_name"), &section_context)?;
    ensure_only_allowed_fields(
        section,
        &["status", "name", "failure_reason"],
        &section_context,
    )?;

    let status = match required_string(section, "status", &section_context)? {
        "success" => {
            validate_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            ensure_absent(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::Success
        }
        "not_found" => {
            ensure_absent(section, "name", &section_context)?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::NotFound
        }
        "mismatch" => {
            validate_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::Mismatch
        }
        "invalid_name" => {
            ensure_absent(section, "name", &section_context)?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::InvalidName
        }
        "execution_failed" => {
            ensure_absent(section, "name", &section_context)?;
            required_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::ExecutionFailed
        }
        status => bail!(
            "verified-primary only supports success, not_found, mismatch, invalid_name, and execution_failed; found {status}"
        ),
    };

    Ok(VerifiedPrimaryNameSection {
        section: Value::Object(section.clone()),
        status,
    })
}

pub(crate) fn validate_verified_primary_name_ref(
    value: Option<&Value>,
    context: &str,
    expected_namespace: &str,
) -> Result<()> {
    let name = required_object(value, context)?;
    ensure_only_allowed_fields(
        name,
        &[
            "logical_name_id",
            "namespace",
            "normalized_name",
            "canonical_display_name",
            "namehash",
            "resource_id",
            "binding_kind",
        ],
        context,
    )?;

    let logical_name_id = required_string(name, "logical_name_id", context)?;
    let namespace = required_string(name, "namespace", context)?;
    let normalized_name = required_string(name, "normalized_name", context)?;
    required_string(name, "canonical_display_name", context)?;
    required_string(name, "namehash", context)?;
    optional_nonempty_string_field(name, "resource_id", context)?;
    optional_nonempty_string_field(name, "binding_kind", context)?;

    if namespace != expected_namespace {
        bail!("{context}.namespace must be {expected_namespace}");
    }
    if logical_name_id != format!("{expected_namespace}:{normalized_name}") {
        bail!(
            "{context}.logical_name_id {} does not match normalized_name {}",
            logical_name_id,
            normalized_name
        );
    }

    Ok(())
}

fn validate_verified_primary_trace(
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

fn validate_verified_primary_outcome(
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

pub(crate) async fn ensure_primary_name_anchor_exists(
    pool: &PgPool,
    tuple: &VerifiedPrimaryNameTuple,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if load_primary_name_current(
        pool,
        &tuple.normalized_address,
        &tuple.namespace,
        &tuple.coin_type,
    )
    .await?
    .is_some()
    {
        return Ok(());
    }

    bail!(
        "{context} persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
        tuple.normalized_address,
        tuple.namespace,
        tuple.coin_type
    )
}

pub(crate) fn normalized_verified_primary_name_request_key(
    namespace: &str,
    normalized_address: &str,
    coin_type: &str,
) -> String {
    format!(
        "{namespace}:{}:{coin_type}",
        normalize_address(normalized_address)
    )
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
