use anyhow::{Result, bail};
use bigname_storage::{ExecutionTrace, ExecutionTraceStep};
use serde_json::Value;
use uuid::Uuid;

use super::common::persisted_trace_detail_object;
use crate::DECLARED_REGISTRY_PATH_BINDING_KIND;
use crate::json_helpers::{ensure_only_allowed_fields, required_array, required_object};
use crate::primary_name::validate_verified_primary_name_ref;
use crate::validation::{
    RequestedSelectorSet, SupportedResolutionPathClass, SupportedResolutionStepSummary,
};
use crate::{
    LINKED_SUBREGISTRY_PATH_BINDING_KIND, MIGRATION_REBIND_BINDING_KIND,
    OBSERVED_ONLY_BINDING_KIND, OBSERVED_WILDCARD_PATH_BINDING_KIND,
    RESOLVER_ALIAS_PATH_BINDING_KIND,
};

pub(super) fn ensure_steps_are_supported_exact_surface_path(
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    execution_trace_id: Uuid,
) -> Result<()> {
    let path_class = classify_supported_resolution_path(
        requested_selectors.binding_kind.as_deref(),
        execution_trace_id,
    )?;
    let step_summary = ensure_steps_do_not_use_deferred_execution_paths(
        &trace.steps,
        execution_trace_id,
        "ENS direct-path verified resolution",
        path_class,
    )?;
    if !step_summary.saw_universal_resolver_call {
        bail!(
            "ENS direct-path verified resolution trace {} must include step_kind call_universal_resolver",
            execution_trace_id
        );
    }
    ensure_universal_resolver_steps_anchor_to_surface(
        &trace.steps,
        &requested_selectors.surface,
        execution_trace_id,
        "ENS direct-path verified resolution",
    )?;
    validate_supported_exact_surface_runtime_details(trace, path_class, execution_trace_id)?;
    match path_class {
        SupportedResolutionPathClass::Direct => {
            if step_summary.saw_alias_step {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias steps without binding_kind {}",
                    execution_trace_id,
                    RESOLVER_ALIAS_PATH_BINDING_KIND
                );
            }
        }
        SupportedResolutionPathClass::AliasOnly => {}
        SupportedResolutionPathClass::WildcardDerived => {
            if step_summary.saw_alias_step {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias steps when binding_kind is {}",
                    execution_trace_id,
                    OBSERVED_WILDCARD_PATH_BINDING_KIND
                );
            }
        }
    }

    Ok(())
}

pub(crate) fn ensure_steps_do_not_use_deferred_execution_paths(
    steps: &[ExecutionTraceStep],
    execution_trace_id: Uuid,
    context: &str,
    path_class: SupportedResolutionPathClass,
) -> Result<SupportedResolutionStepSummary> {
    let mut summary = SupportedResolutionStepSummary::default();
    for step in steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("wildcard")
            && path_class != SupportedResolutionPathClass::WildcardDerived
        {
            bail!(
                "{context} trace {} must not persist wildcard traversal step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("ccip")
            || normalized.contains("transport")
            || normalized.contains("subregistry")
            || normalized.contains("ancestor")
            || normalized.contains("basename")
        {
            bail!(
                "{context} trace {} must not persist non-direct step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("alias") {
            summary.saw_alias_step = true;
        }
        if step.step_kind == "call_universal_resolver" {
            summary.saw_universal_resolver_call = true;
        }
    }

    Ok(summary)
}

pub(crate) fn classify_supported_resolution_path(
    binding_kind: Option<&str>,
    execution_trace_id: Uuid,
) -> Result<SupportedResolutionPathClass> {
    match binding_kind {
        None | Some(DECLARED_REGISTRY_PATH_BINDING_KIND) => {
            Ok(SupportedResolutionPathClass::Direct)
        }
        Some(RESOLVER_ALIAS_PATH_BINDING_KIND) => Ok(SupportedResolutionPathClass::AliasOnly),
        Some(OBSERVED_WILDCARD_PATH_BINDING_KIND) => {
            Ok(SupportedResolutionPathClass::WildcardDerived)
        }
        Some(LINKED_SUBREGISTRY_PATH_BINDING_KIND) => bail!(
            "ENS direct-path verified resolution trace {} must not persist non-alias ancestor-selected binding_kind {}",
            execution_trace_id,
            LINKED_SUBREGISTRY_PATH_BINDING_KIND
        ),
        Some(MIGRATION_REBIND_BINDING_KIND | OBSERVED_ONLY_BINDING_KIND) => bail!(
            "ENS direct-path verified resolution trace {} must not persist unsupported binding_kind {}",
            execution_trace_id,
            binding_kind.unwrap_or_default()
        ),
        Some(other) => bail!(
            "ENS direct-path verified resolution trace {} must use binding_kind {}, {}, or omit binding_kind; found {}",
            execution_trace_id,
            DECLARED_REGISTRY_PATH_BINDING_KIND,
            RESOLVER_ALIAS_PATH_BINDING_KIND,
            other
        ),
    }
}

fn validate_supported_exact_surface_runtime_details(
    trace: &ExecutionTrace,
    path_class: SupportedResolutionPathClass,
    execution_trace_id: Uuid,
) -> Result<()> {
    let alias_present =
        persisted_alias_detail_is_present(trace, "ENS direct-path verified resolution")?;
    ensure_wildcard_detail_matches_path_class(
        trace,
        path_class,
        "ENS direct-path verified resolution",
        execution_trace_id,
    )?;
    ensure_transport_detail_absent(trace, "ENS direct-path verified resolution")?;

    match path_class {
        SupportedResolutionPathClass::Direct => {
            if alias_present {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias detail unless binding_kind is {}",
                    execution_trace_id,
                    RESOLVER_ALIAS_PATH_BINDING_KIND
                );
            }
        }
        SupportedResolutionPathClass::AliasOnly => {
            if !alias_present {
                bail!(
                    "ENS direct-path verified resolution trace {} must persist alias.final_target and non-empty alias.hops for binding_kind {}",
                    execution_trace_id,
                    RESOLVER_ALIAS_PATH_BINDING_KIND
                );
            }
        }
        SupportedResolutionPathClass::WildcardDerived => {
            if alias_present {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias detail when binding_kind is {}",
                    execution_trace_id,
                    OBSERVED_WILDCARD_PATH_BINDING_KIND
                );
            }
        }
    }

    Ok(())
}

fn ensure_universal_resolver_steps_anchor_to_surface(
    steps: &[ExecutionTraceStep],
    surface: &str,
    execution_trace_id: Uuid,
    context: &str,
) -> Result<()> {
    for step in steps {
        if step.step_kind != "call_universal_resolver" {
            continue;
        }

        let payload = required_object(
            Some(&step.step_payload),
            &format!("{context} trace.steps.call_universal_resolver.step_payload"),
        )?;
        if let Some(name) = payload.get("name").and_then(Value::as_str)
            && name != surface
        {
            bail!(
                "{context} trace {} must anchor call_universal_resolver name {} to request surface {}",
                execution_trace_id,
                name,
                surface
            );
        }
    }

    Ok(())
}

fn persisted_alias_detail_is_present(trace: &ExecutionTrace, context: &str) -> Result<bool> {
    let Some(alias) = persisted_trace_detail_object(trace, "alias") else {
        return Ok(false);
    };

    let alias_context = format!("{context} trace alias detail");
    let alias = required_object(Some(&alias), &alias_context)?;
    ensure_only_allowed_fields(alias, &["final_target", "hops"], &alias_context)?;

    let final_target = match alias.get("final_target") {
        None | Some(Value::Null) => None,
        Some(value) => {
            validate_verified_primary_name_ref(
                Some(value),
                &format!("{alias_context}.final_target"),
                &trace.namespace,
            )?;
            Some(value)
        }
    };
    let hops = required_array(alias.get("hops"), &format!("{alias_context}.hops"))?;

    if final_target.is_none() && hops.is_empty() {
        return Ok(false);
    }
    if final_target.is_none() || hops.is_empty() {
        bail!("{alias_context} must set final_target and non-empty hops together");
    }

    for (index, hop) in hops.iter().enumerate() {
        validate_verified_primary_name_ref(
            Some(hop),
            &format!("{alias_context}.hops[{index}]"),
            &trace.namespace,
        )?;
    }
    if hops.last() != final_target {
        bail!("{alias_context}.hops last element must match final_target");
    }

    Ok(true)
}

fn ensure_wildcard_detail_matches_path_class(
    trace: &ExecutionTrace,
    path_class: SupportedResolutionPathClass,
    context: &str,
    execution_trace_id: Uuid,
) -> Result<()> {
    let Some(wildcard) = persisted_trace_detail_object(trace, "wildcard") else {
        if path_class == SupportedResolutionPathClass::WildcardDerived {
            bail!(
                "{context} trace {} must persist wildcard.source non-null with matched_labels non-empty for binding_kind {}",
                execution_trace_id,
                OBSERVED_WILDCARD_PATH_BINDING_KIND
            );
        }
        return Ok(());
    };

    let wildcard_context = format!("{context} trace wildcard detail");
    let wildcard = required_object(Some(&wildcard), &wildcard_context)?;
    ensure_only_allowed_fields(wildcard, &["source", "matched_labels"], &wildcard_context)?;

    let source_present = match wildcard.get("source") {
        None | Some(Value::Null) => false,
        Some(source) => {
            validate_verified_primary_name_ref(
                Some(source),
                &format!("{wildcard_context}.source"),
                &trace.namespace,
            )?;
            true
        }
    };
    let matched_labels = required_array(
        wildcard.get("matched_labels"),
        &format!("{wildcard_context}.matched_labels"),
    )?;
    match path_class {
        SupportedResolutionPathClass::Direct | SupportedResolutionPathClass::AliasOnly => {
            if source_present || !matched_labels.is_empty() {
                bail!(
                    "{context} only supports wildcard.source=null with matched_labels=[] for persisted exact-surface requests"
                );
            }
        }
        SupportedResolutionPathClass::WildcardDerived => {
            if !source_present || matched_labels.is_empty() {
                bail!(
                    "{context} trace {} must persist wildcard.source non-null with matched_labels non-empty for binding_kind {}",
                    execution_trace_id,
                    OBSERVED_WILDCARD_PATH_BINDING_KIND
                );
            }
        }
    }

    Ok(())
}

fn ensure_transport_detail_absent(trace: &ExecutionTrace, context: &str) -> Result<()> {
    let Some(transport) = persisted_trace_detail_object(trace, "transport") else {
        return Ok(());
    };

    let transport_context = format!("{context} trace transport detail");
    let transport = required_object(Some(&transport), &transport_context)?;
    ensure_only_allowed_fields(
        transport,
        &[
            "source_chain_id",
            "target_chain_id",
            "contract_address",
            "latest_event_kind",
        ],
        &transport_context,
    )?;

    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        if !matches!(transport.get(field_name), None | Some(Value::Null)) {
            bail!("{context} transport-assisted persisted requests remain unsupported");
        }
    }

    Ok(())
}
