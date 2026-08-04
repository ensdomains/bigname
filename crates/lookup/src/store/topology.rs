use serde_json::Value;

use crate::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID, LookupError, Result,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutionPathClass {
    EnsDirectOrAlias,
    EnsWildcard,
    BasenamesDirect,
}

pub(super) fn ensure_supported_execution_path(
    namespace: &str,
    logical_name_id: &str,
    topology: &Value,
) -> Result<ExecutionPathClass> {
    if topology.get("status").and_then(Value::as_str) == Some("unsupported") {
        return Err(unsupported("projected topology is unsupported"));
    }
    let resolver_name = topology
        .get("resolver_path")
        .and_then(Value::as_array)
        .and_then(|path| path.first())
        .and_then(|hop| hop.get("logical_name_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| unsupported("projected topology has no resolver path anchor"))?;
    let alias_present = alias_present(topology)?;
    let wildcard_source = wildcard_source(topology)?;
    let transport_is_null = transport_is_null(topology);

    if namespace == BASENAMES_NAMESPACE {
        if transport_is_null
            || !subregistry_path_is_empty(topology)
            || resolver_name != logical_name_id
            || alias_present
            || wildcard_source.is_some()
            || !transport_matches_basenames(topology)
        {
            return Err(unsupported(
                "projected Basenames topology is outside the supported direct transport path",
            ));
        }
        return Ok(ExecutionPathClass::BasenamesDirect);
    }

    if transport_is_null {
        if let Some(source) = wildcard_source {
            if !alias_present && subregistry_path_is_empty(topology) && resolver_name == source {
                return Ok(ExecutionPathClass::EnsWildcard);
            }
        } else if resolver_name == logical_name_id {
            return Ok(ExecutionPathClass::EnsDirectOrAlias);
        }
    }
    Err(unsupported(
        "projected ENS topology is outside the supported direct, alias, or wildcard paths",
    ))
}

fn alias_present(topology: &Value) -> Result<bool> {
    let alias = topology
        .get("alias")
        .ok_or_else(|| unsupported("projected topology has no alias detail"))?;
    let final_target = !matches!(alias.get("final_target"), None | Some(Value::Null));
    let hops = alias
        .get("hops")
        .and_then(Value::as_array)
        .ok_or_else(|| unsupported("projected topology alias has no hops"))?;
    if final_target == hops.is_empty() {
        return Err(unsupported(
            "projected topology alias target and hops disagree",
        ));
    }
    Ok(final_target)
}

fn wildcard_source(topology: &Value) -> Result<Option<&str>> {
    let wildcard = topology
        .get("wildcard")
        .ok_or_else(|| unsupported("projected topology has no wildcard detail"))?;
    let labels = wildcard
        .get("matched_labels")
        .and_then(Value::as_array)
        .ok_or_else(|| unsupported("projected topology wildcard has no matched labels"))?;
    match wildcard.get("source") {
        None | Some(Value::Null) if labels.is_empty() => Ok(None),
        Some(source) if !labels.is_empty() => source
            .get("logical_name_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(Some)
            .ok_or_else(|| unsupported("projected wildcard source has no logical name")),
        _ => Err(unsupported(
            "projected wildcard source and matched labels disagree",
        )),
    }
}

fn subregistry_path_is_empty(topology: &Value) -> bool {
    topology
        .get("subregistry_path")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn transport_is_null(topology: &Value) -> bool {
    let Some(transport) = topology.get("transport") else {
        return true;
    };
    [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ]
    .iter()
    .all(|field| matches!(transport.get(field), None | Some(Value::Null)))
}

fn transport_matches_basenames(topology: &Value) -> bool {
    let Some(transport) = topology.get("transport").and_then(Value::as_object) else {
        return false;
    };
    if transport.iter().any(|(field, value)| {
        !matches!(
            field.as_str(),
            "source_chain_id" | "target_chain_id" | "contract_address" | "latest_event_kind"
        ) && !value.is_null()
    }) {
        return false;
    }
    transport.get("source_chain_id").and_then(Value::as_str) == Some(BASE_MAINNET_CHAIN_ID)
        && transport.get("target_chain_id").and_then(Value::as_str)
            == Some(ETHEREUM_MAINNET_CHAIN_ID)
        // The store compares this value with the selected execution manifest.
        && transport
            .get("contract_address")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
}

fn unsupported(message: &'static str) -> LookupError {
    LookupError::unsupported(message)
}
