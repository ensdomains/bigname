use anyhow::{Context, Result, bail};
use bigname_storage::ExecutionTrace;
use serde_json::Value;
use uuid::Uuid;

use super::common::persisted_trace_detail_object;
use crate::json_helpers::{
    ensure_only_allowed_fields, required_array, required_object, required_string,
};
use crate::validation::{RequestedChainPosition, RequestedSelectorSet};
use crate::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, DECLARED_REGISTRY_PATH_BINDING_KIND,
    ETHEREUM_MAINNET_CHAIN_ID,
};

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
