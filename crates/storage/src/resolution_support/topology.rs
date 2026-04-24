use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::name_current::NameCurrentRow;

use super::support_classes::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_NAMESPACE,
    ETHEREUM_MAINNET_CHAIN_ID, VerifiedResolutionPathClass, json_field, json_string_field,
    resolution_projection_chain_position_from_value, summary_is_unsupported,
};

pub fn projected_resolution_topology(summary: &Value) -> Option<Value> {
    json_field(summary, "topology")
        .filter(|value| value.is_object())
        .cloned()
}

pub fn classify_supported_resolution_topology(
    namespace: &str,
    logical_name_id: &str,
    topology: &Value,
) -> Option<VerifiedResolutionPathClass> {
    if summary_is_unsupported(Some(topology)) {
        return None;
    }

    let resolver_logical_name_id = resolution_topology_resolver_logical_name_id(topology)?;
    let alias_present = resolution_topology_alias_is_present(topology).ok()?;
    let wildcard_source_logical_name_id = resolution_topology_wildcard_state(topology).ok()?;
    let transport_is_null = resolution_topology_transport_is_null(topology);

    if namespace == BASENAMES_NAMESPACE {
        if !transport_is_null {
            return resolution_topology_subregistry_path_is_empty(topology)
                .then_some(())
                .filter(|_| resolver_logical_name_id == logical_name_id)
                .filter(|_| !alias_present)
                .filter(|_| wildcard_source_logical_name_id.is_none())
                .filter(|_| {
                    resolution_topology_transport_matches_basenames_supported_class(topology)
                })
                .map(|_| VerifiedResolutionPathClass::BasenamesTransportDirect);
        }
        return None;
    }

    if !transport_is_null {
        return None;
    }

    if wildcard_source_logical_name_id.is_some() {
        if alias_present || !resolution_topology_subregistry_path_is_empty(topology) {
            return None;
        }
        return (resolver_logical_name_id == wildcard_source_logical_name_id?)
            .then_some(VerifiedResolutionPathClass::WildcardDerived);
    }

    if resolver_logical_name_id != logical_name_id {
        return None;
    }

    if alias_present {
        Some(VerifiedResolutionPathClass::AliasOnly)
    } else {
        Some(VerifiedResolutionPathClass::Direct)
    }
}

pub fn try_classify_supported_resolution_topology(
    namespace: &str,
    logical_name_id: &str,
    topology: &Value,
) -> Result<VerifiedResolutionPathClass> {
    if summary_is_unsupported(Some(topology)) {
        bail!("projected topology is unsupported");
    }

    let resolver_logical_name_id = resolution_topology_resolver_logical_name_id(topology)
        .with_context(|| {
            "projected topology must include resolver_path[0].logical_name_id".to_owned()
        })?;
    let alias_present = resolution_topology_alias_is_present(topology)?;
    let wildcard_source_logical_name_id = resolution_topology_wildcard_state(topology)?;
    let transport_is_null = resolution_topology_transport_is_null(topology);

    if namespace == BASENAMES_NAMESPACE {
        if transport_is_null {
            bail!("projected Basenames topology must include supported transport detail");
        }
        if !resolution_topology_subregistry_path_is_empty(topology) {
            bail!("projected Basenames topology must keep subregistry_path empty");
        }
        if resolver_logical_name_id != logical_name_id {
            bail!("projected Basenames topology must anchor resolver_path[0] to the request name");
        }
        if alias_present {
            bail!("projected Basenames topology must keep alias detail empty");
        }
        if wildcard_source_logical_name_id.is_some() {
            bail!("projected Basenames topology must keep wildcard detail empty");
        }
        if !resolution_topology_transport_matches_basenames_supported_class(topology) {
            bail!("projected Basenames topology transport is outside the supported class");
        }
        return Ok(VerifiedResolutionPathClass::BasenamesTransportDirect);
    }

    if !transport_is_null {
        bail!("projected ENS topology must keep transport detail null");
    }

    if let Some(wildcard_source_logical_name_id) = wildcard_source_logical_name_id {
        if alias_present || !resolution_topology_subregistry_path_is_empty(topology) {
            bail!(
                "projected wildcard-derived ENS topology must keep alias detail empty and subregistry_path empty"
            );
        }
        if resolver_logical_name_id != wildcard_source_logical_name_id {
            bail!(
                "projected wildcard-derived ENS topology must anchor resolver_path[0] to wildcard.source.logical_name_id"
            );
        }
        return Ok(VerifiedResolutionPathClass::WildcardDerived);
    }

    if resolver_logical_name_id != logical_name_id {
        bail!("projected ENS topology must anchor resolver_path[0] to the request name");
    }

    if alias_present {
        Ok(VerifiedResolutionPathClass::AliasOnly)
    } else {
        Ok(VerifiedResolutionPathClass::Direct)
    }
}

pub fn row_has_basenames_supported_chain_positions(row: &NameCurrentRow) -> bool {
    let Some(chain_positions) = row.chain_positions.as_object() else {
        return false;
    };

    let mut saw_base = false;
    let mut saw_ethereum = false;
    for position in chain_positions.values() {
        match resolution_projection_chain_position_from_value(position)
            .map(|position| position.chain_id)
        {
            Some(chain_id) if chain_id == BASE_MAINNET_CHAIN_ID => saw_base = true,
            Some(chain_id) if chain_id == ETHEREUM_MAINNET_CHAIN_ID => saw_ethereum = true,
            Some(_) | None => {}
        }
    }

    saw_base && saw_ethereum
}

pub(crate) fn row_has_basenames_supported_chain_positions_for_revalidation(
    row: &NameCurrentRow,
) -> bool {
    row_has_basenames_supported_chain_positions(row)
}

fn resolution_topology_resolver_logical_name_id(topology: &Value) -> Option<String> {
    json_field(topology, "resolver_path")
        .and_then(Value::as_array)
        .and_then(|resolver_path| resolver_path.first())
        .and_then(|hop| json_string_field(json_field(hop, "logical_name_id")))
}

fn resolution_topology_alias_is_present(topology: &Value) -> Result<bool> {
    let alias = json_field(topology, "alias")
        .with_context(|| "projected topology must include alias".to_owned())?;
    let final_target_present =
        !matches!(json_field(alias, "final_target"), None | Some(Value::Null));
    let hops = json_field(alias, "hops")
        .and_then(Value::as_array)
        .with_context(|| "projected topology alias must include hops".to_owned())?;
    let hops_present = !hops.is_empty();
    if final_target_present != hops_present {
        bail!("projected topology alias must set final_target and non-empty hops together");
    }
    Ok(final_target_present)
}

fn resolution_topology_wildcard_state(topology: &Value) -> Result<Option<String>> {
    let wildcard = json_field(topology, "wildcard")
        .with_context(|| "projected topology must include wildcard".to_owned())?;
    let matched_labels = json_field(wildcard, "matched_labels")
        .and_then(Value::as_array)
        .with_context(|| "projected topology wildcard must include matched_labels".to_owned())?;
    let source = json_field(wildcard, "source");

    match source {
        None | Some(Value::Null) => {
            if matched_labels.is_empty() {
                Ok(None)
            } else {
                bail!("projected topology wildcard with null source must keep matched_labels empty")
            }
        }
        Some(_) if matched_labels.is_empty() => {
            bail!(
                "projected topology wildcard must keep matched_labels non-empty when source is present"
            )
        }
        Some(source) => Ok(Some(
            json_string_field(json_field(source, "logical_name_id")).with_context(|| {
                "projected topology wildcard source must include logical_name_id".to_owned()
            })?,
        )),
    }
}

fn resolution_topology_subregistry_path_is_empty(topology: &Value) -> bool {
    json_field(topology, "subregistry_path")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn resolution_topology_transport_is_null(topology: &Value) -> bool {
    let Some(transport) = json_field(topology, "transport") else {
        return true;
    };

    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        if !matches!(json_field(transport, field_name), None | Some(Value::Null)) {
            return false;
        }
    }

    true
}

fn resolution_topology_transport_matches_basenames_supported_class(topology: &Value) -> bool {
    let Some(transport) = json_field(topology, "transport").and_then(Value::as_object) else {
        return false;
    };
    if transport.iter().any(|(field_name, value)| {
        !matches!(
            field_name.as_str(),
            "source_chain_id" | "target_chain_id" | "contract_address" | "latest_event_kind"
        ) && !value.is_null()
    }) {
        return false;
    }
    json_string_field(transport.get("source_chain_id"))
        .is_some_and(|value| value == BASE_MAINNET_CHAIN_ID)
        && json_string_field(transport.get("target_chain_id"))
            .is_some_and(|value| value == ETHEREUM_MAINNET_CHAIN_ID)
        && json_string_field(transport.get("contract_address"))
            .is_some_and(|value| value.eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS))
}
