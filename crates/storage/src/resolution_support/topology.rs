use anyhow::{Context, Result};
use bigname_domain::resolution_topology::{ResolutionRoutePolicy, ResolutionTopology};
use serde_json::Value;

use crate::name_current::NameCurrentRow;

use super::support_classes::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_EXPECTED_TRANSPORT, BASENAMES_NAMESPACE, ENS_NAMESPACE,
    ETHEREUM_MAINNET_CHAIN_ID, VerifiedResolutionPathClass, json_field,
    resolution_projection_chain_position_from_value,
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
    let topology = serde_json::from_value::<ResolutionTopology>(topology.clone()).ok()?;
    topology
        .classify(logical_name_id, route_policy(namespace)?)
        .ok()
}

pub fn try_classify_supported_resolution_topology(
    namespace: &str,
    logical_name_id: &str,
    topology: &Value,
) -> Result<VerifiedResolutionPathClass> {
    let topology = serde_json::from_value::<ResolutionTopology>(topology.clone())
        .with_context(|| "projected topology does not match ResolutionTopology")?;
    topology
        .classify(
            logical_name_id,
            route_policy(namespace).with_context(|| {
                format!("namespace {namespace} has no verified resolution route policy")
            })?,
        )
        .map_err(Into::into)
}

fn route_policy(namespace: &str) -> Option<ResolutionRoutePolicy> {
    match namespace {
        ENS_NAMESPACE => Some(ResolutionRoutePolicy::Ens),
        BASENAMES_NAMESPACE => Some(ResolutionRoutePolicy::Basenames {
            expected_transport: BASENAMES_EXPECTED_TRANSPORT,
        }),
        _ => None,
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
