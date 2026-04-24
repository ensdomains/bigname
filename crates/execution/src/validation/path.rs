use anyhow::{Context, Result, bail};
use bigname_storage::ExecutionTrace;
use serde_json::Value;
use uuid::Uuid;

use crate::json_helpers::{
    ensure_only_allowed_fields, required_array, required_object, required_string,
};
use crate::primary_name::validate_verified_primary_name_ref;
use crate::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_L1_RESOLVER_ROLE,
    DECLARED_REGISTRY_PATH_BINDING_KIND, ENS_UNIVERSAL_RESOLVER_ADDRESS,
    ENS_UNIVERSAL_RESOLVER_ROLE, ETHEREUM_MAINNET_CHAIN_ID, LINKED_SUBREGISTRY_PATH_BINDING_KIND,
    MIGRATION_REBIND_BINDING_KIND, OBSERVED_ONLY_BINDING_KIND, OBSERVED_WILDCARD_PATH_BINDING_KIND,
    RESOLVER_ALIAS_PATH_BINDING_KIND,
};

use super::{
    RequestedChainPosition, RequestedSelectorSet, SupportedResolutionPathClass,
    SupportedResolutionStepSummary,
};

pub(crate) fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}

pub(crate) fn manifest_versions_include_source_family_for_context(
    manifest_context: Option<&Value>,
    cache_manifest_versions: Option<&Value>,
    expected_source_family: &str,
    context: &str,
) -> Result<bool> {
    if let Some(manifest_context) = manifest_context {
        let object = required_object(
            Some(manifest_context),
            &format!("{context} trace.manifest_context"),
        )?;
        if contains_source_family(
            object.get("manifest_versions"),
            expected_source_family,
            context,
        )? {
            return Ok(true);
        }
    }

    contains_source_family(cache_manifest_versions, expected_source_family, context)
}

fn contains_source_family(
    value: Option<&Value>,
    expected_source_family: &str,
    context: &str,
) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let items = required_array(Some(value), &format!("{context} manifest_versions"))?;
    for (index, item) in items.iter().enumerate() {
        let object = required_object(Some(item), &format!("{context} manifest_versions[{index}]"))?;
        if object
            .get("source_family")
            .and_then(Value::as_str)
            .is_some_and(|value| value == expected_source_family)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn ensure_contains_universal_resolver_call(
    contracts_called: &Value,
    execution_trace_id: Uuid,
    context: &str,
) -> Result<()> {
    let calls = required_array(
        Some(contracts_called),
        &format!("{context} trace.contracts_called"),
    )?;
    for (index, call) in calls.iter().enumerate() {
        let object = required_object(
            Some(call),
            &format!("{context} trace.contracts_called[{index}]"),
        )?;
        let chain_id = required_string(
            object,
            "chain_id",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let contract_address = required_string(
            object,
            "contract_address",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let selector = required_string(
            object,
            "selector",
            &format!("{context} trace.contracts_called entry"),
        )?;
        if chain_id == ETHEREUM_MAINNET_CHAIN_ID
            && contract_address.eq_ignore_ascii_case(ENS_UNIVERSAL_RESOLVER_ADDRESS)
            && !selector.is_empty()
        {
            return Ok(());
        }
    }

    bail!(
        "{context} trace {} must include one {} contract call on {}",
        execution_trace_id,
        ENS_UNIVERSAL_RESOLVER_ROLE,
        ETHEREUM_MAINNET_CHAIN_ID
    )
}

pub(crate) fn ensure_contains_basenames_l1_resolver_call(
    contracts_called: &Value,
    execution_trace_id: Uuid,
    context: &str,
) -> Result<()> {
    let calls = required_array(
        Some(contracts_called),
        &format!("{context} trace.contracts_called"),
    )?;
    for (index, call) in calls.iter().enumerate() {
        let object = required_object(
            Some(call),
            &format!("{context} trace.contracts_called[{index}]"),
        )?;
        let chain_id = required_string(
            object,
            "chain_id",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let contract_address = required_string(
            object,
            "contract_address",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let selector = required_string(
            object,
            "selector",
            &format!("{context} trace.contracts_called entry"),
        )?;
        if chain_id == ETHEREUM_MAINNET_CHAIN_ID
            && contract_address.eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS)
            && !selector.is_empty()
        {
            return Ok(());
        }
    }

    bail!(
        "{context} trace {} must include one {} contract call on {}",
        execution_trace_id,
        BASENAMES_L1_RESOLVER_ROLE,
        ETHEREUM_MAINNET_CHAIN_ID
    )
}

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

pub(super) fn ensure_steps_are_supported_basenames_transport_direct_path(
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    execution_trace_id: Uuid,
) -> Result<()> {
    match requested_selectors.binding_kind.as_deref() {
        None | Some(DECLARED_REGISTRY_PATH_BINDING_KIND) => {}
        Some(other) => bail!(
            "Basenames transport-direct verified resolution trace {} must use binding_kind {} or omit binding_kind; found {}",
            execution_trace_id,
            DECLARED_REGISTRY_PATH_BINDING_KIND,
            other
        ),
    }

    let mut saw_l1_resolver_call = false;
    let mut saw_ccip_or_proof = false;
    for step in &trace.steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("alias")
            || normalized.contains("wildcard")
            || normalized.contains("subregistry")
            || normalized.contains("ancestor")
            || normalized.contains("universal_resolver")
        {
            bail!(
                "Basenames transport-direct verified resolution trace {} must not persist out-of-class step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("l1_resolver") {
            saw_l1_resolver_call = true;
            let payload = required_object(
                Some(&step.step_payload),
                "Basenames transport-direct verified resolution trace.steps.l1_resolver.step_payload",
            )?;
            if let Some(name) = payload.get("name").and_then(Value::as_str)
                && name != requested_selectors.surface
            {
                bail!(
                    "Basenames transport-direct verified resolution trace {} must anchor L1 resolver name {} to request surface {}",
                    execution_trace_id,
                    name,
                    requested_selectors.surface
                );
            }
        }
        if normalized.contains("ccip")
            || normalized.contains("offchain")
            || normalized.contains("resolve_with_proof")
            || normalized.contains("proof")
        {
            saw_ccip_or_proof = true;
        }
    }

    if !saw_l1_resolver_call {
        bail!(
            "Basenames transport-direct verified resolution trace {} must include an L1 resolver step",
            execution_trace_id
        );
    }
    if !saw_ccip_or_proof {
        bail!(
            "Basenames transport-direct verified resolution trace {} must include CCIP or proof-completion steps",
            execution_trace_id
        );
    }

    ensure_basenames_alias_detail_absent(trace, "Basenames transport-direct verified resolution")?;
    ensure_basenames_wildcard_detail_absent(
        trace,
        "Basenames transport-direct verified resolution",
    )?;
    ensure_basenames_transport_detail_supported(
        trace,
        "Basenames transport-direct verified resolution",
    )?;

    Ok(())
}

pub(crate) fn ensure_steps_do_not_use_deferred_execution_paths(
    steps: &[bigname_storage::ExecutionTraceStep],
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
    steps: &[bigname_storage::ExecutionTraceStep],
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

fn ensure_basenames_alias_detail_absent(trace: &ExecutionTrace, context: &str) -> Result<()> {
    let Some(alias) = persisted_trace_detail_object(trace, "alias") else {
        return Ok(());
    };
    let alias = required_object(Some(&alias), &format!("{context} trace alias detail"))?;
    let final_target_present = !matches!(alias.get("final_target"), None | Some(Value::Null));
    let hops = required_array(
        alias.get("hops"),
        &format!("{context} trace alias detail.hops"),
    )?;
    if final_target_present || !hops.is_empty() {
        bail!("{context} must keep alias.final_target null with alias.hops empty");
    }
    Ok(())
}

fn ensure_basenames_wildcard_detail_absent(trace: &ExecutionTrace, context: &str) -> Result<()> {
    let Some(wildcard) = persisted_trace_detail_object(trace, "wildcard") else {
        return Ok(());
    };
    let wildcard = required_object(Some(&wildcard), &format!("{context} trace wildcard detail"))?;
    let source_present = !matches!(wildcard.get("source"), None | Some(Value::Null));
    let matched_labels = required_array(
        wildcard.get("matched_labels"),
        &format!("{context} trace wildcard detail.matched_labels"),
    )?;
    if source_present || !matched_labels.is_empty() {
        bail!("{context} must keep wildcard.source null with matched_labels empty");
    }
    Ok(())
}

fn ensure_basenames_transport_detail_supported(
    trace: &ExecutionTrace,
    context: &str,
) -> Result<()> {
    let transport = persisted_trace_detail_object(trace, "transport")
        .context(format!("{context} must persist transport detail"))?;
    let transport = required_object(
        Some(&transport),
        &format!("{context} trace transport detail"),
    )?;
    ensure_only_allowed_fields(
        transport,
        &[
            "source_chain_id",
            "target_chain_id",
            "contract_address",
            "latest_event_kind",
        ],
        &format!("{context} trace transport detail"),
    )?;

    let source_chain_id = required_string(
        transport,
        "source_chain_id",
        &format!("{context} trace transport detail"),
    )?;
    let target_chain_id = required_string(
        transport,
        "target_chain_id",
        &format!("{context} trace transport detail"),
    )?;
    let contract_address = required_string(
        transport,
        "contract_address",
        &format!("{context} trace transport detail"),
    )?;

    if source_chain_id != BASE_MAINNET_CHAIN_ID
        || target_chain_id != ETHEREUM_MAINNET_CHAIN_ID
        || !contract_address.eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS)
    {
        bail!(
            "{context} must use transport {} -> {} via {}",
            BASE_MAINNET_CHAIN_ID,
            ETHEREUM_MAINNET_CHAIN_ID,
            BASENAMES_L1_RESOLVER_ADDRESS
        );
    }

    Ok(())
}

pub(crate) fn persisted_trace_detail_object(trace: &ExecutionTrace, key: &str) -> Option<Value> {
    trace
        .request_metadata
        .get(key)
        .filter(|value| value.is_object())
        .cloned()
        .or_else(|| {
            trace.steps.iter().find_map(|step| {
                step.step_payload
                    .get(key)
                    .filter(|value| value.is_object())
                    .cloned()
            })
        })
}

pub(crate) fn ensure_single_ethereum_mainnet_position(
    positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    if positions.len() != 1 {
        bail!(
            "{context} must include exactly one chain position, found {}",
            positions.len()
        );
    }
    let position = &positions[0];
    if position.chain_id != ETHEREUM_MAINNET_CHAIN_ID {
        bail!(
            "{context} must target chain_id {}, found {}",
            ETHEREUM_MAINNET_CHAIN_ID,
            position.chain_id
        );
    }
    Ok(())
}

pub(super) fn ensure_basenames_requested_positions(
    positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    if positions.len() != 2 {
        bail!(
            "{context} must include exactly two chain positions for {} -> {}, found {}",
            BASE_MAINNET_CHAIN_ID,
            ETHEREUM_MAINNET_CHAIN_ID,
            positions.len()
        );
    }

    let mut saw_base = false;
    let mut saw_ethereum = false;
    for position in positions {
        match position.chain_id.as_str() {
            BASE_MAINNET_CHAIN_ID => saw_base = true,
            ETHEREUM_MAINNET_CHAIN_ID => saw_ethereum = true,
            other => {
                bail!(
                    "{context} only supports chain_id {} and {}, found {}",
                    BASE_MAINNET_CHAIN_ID,
                    ETHEREUM_MAINNET_CHAIN_ID,
                    other
                )
            }
        }
    }

    if !saw_base {
        bail!("{context} must include chain_id {}", BASE_MAINNET_CHAIN_ID);
    }
    if !saw_ethereum {
        bail!(
            "{context} must include chain_id {}",
            ETHEREUM_MAINNET_CHAIN_ID
        );
    }
    Ok(())
}

pub(crate) fn required_chain_positions(
    value: Option<&Value>,
    context: &str,
) -> Result<Vec<RequestedChainPosition>> {
    let items = required_array(value, context)?;
    let mut positions = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = required_object(Some(item), &format!("{context}[{index}]"))?;
        let block_number = object
            .get("block_number")
            .and_then(Value::as_i64)
            .with_context(|| {
                format!("{context}[{index}] must include integer field block_number")
            })?;
        positions.push(RequestedChainPosition {
            chain_id: required_string(object, "chain_id", &format!("{context}[{index}]"))?
                .to_owned(),
            block_number,
            block_hash: required_string(object, "block_hash", &format!("{context}[{index}]"))?
                .to_owned(),
        });
    }
    Ok(positions)
}
