use anyhow::{Result, bail};
use serde_json::Value;
use uuid::Uuid;

use crate::json_helpers::{required_array, required_object, required_string};
use crate::{
    BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_L1_RESOLVER_ROLE, ENS_UNIVERSAL_RESOLVER_ADDRESS,
    ENS_UNIVERSAL_RESOLVER_ROLE, ETHEREUM_MAINNET_CHAIN_ID,
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
