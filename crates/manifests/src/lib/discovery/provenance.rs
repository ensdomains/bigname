use anyhow::{Context, Result, bail, ensure};

use super::types::{DiscoveryObservation, EvmEventPosition};
use crate::{PROPAGATED_ROLE_PROVENANCE_FIELD, ZERO_ADDRESS, normalize_address};

pub(super) const TRANSITIVE_DISCOVERY_EDGE_KIND: &str = "subregistry";

pub(super) fn observation_key(observation: &DiscoveryObservation) -> Result<String> {
    observation
        .provenance
        .get("observation_key")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| {
            format!(
                "discovery observation for {} {} is missing provenance.observation_key",
                observation.discovery_source, observation.from_address
            )
        })
}

pub fn discovery_observation_evm_event_position(
    provenance: &serde_json::Value,
) -> Result<Option<(i64, i64)>> {
    let object = provenance
        .as_object()
        .context("discovery observation provenance must be a JSON object")?;
    let (transaction_index, log_index) = match (
        object.get("transaction_index"),
        object.get("log_index"),
    ) {
        (None, None) => return Ok(None),
        (Some(transaction_index), Some(log_index)) => (transaction_index, log_index),
        _ => bail!(
            "discovery observation provenance must carry both transaction_index and log_index when either is present"
        ),
    };
    let transaction_index = transaction_index
        .as_i64()
        .context("discovery observation provenance.transaction_index must be an integer")?;
    let log_index = log_index
        .as_i64()
        .context("discovery observation provenance.log_index must be an integer")?;
    ensure!(
        transaction_index >= 0,
        "discovery observation provenance.transaction_index must be non-negative"
    );
    ensure!(
        log_index >= 0,
        "discovery observation provenance.log_index must be non-negative"
    );

    Ok(Some((transaction_index, log_index)))
}

pub(super) fn evm_event_position(
    provenance: &serde_json::Value,
) -> Result<Option<EvmEventPosition>> {
    Ok(discovery_observation_evm_event_position(provenance)?.map(
        |(transaction_index, log_index)| EvmEventPosition {
            transaction_index,
            log_index,
        },
    ))
}

pub(super) fn is_zero_address(value: &str) -> bool {
    normalize_address(value) == ZERO_ADDRESS
}

pub(super) fn discovery_edge_provenance(
    provenance: &serde_json::Value,
    edge_kind: &str,
    from_role: &str,
) -> Result<serde_json::Value> {
    let mut provenance = provenance.clone();
    let Some(object) = provenance.as_object_mut() else {
        bail!("discovery observation provenance must be a JSON object");
    };
    if discovery_edge_propagates_role(edge_kind) {
        object.insert(
            PROPAGATED_ROLE_PROVENANCE_FIELD.to_owned(),
            serde_json::Value::String(from_role.to_owned()),
        );
    }
    Ok(provenance)
}

pub(super) fn discovery_edge_propagates_role(edge_kind: &str) -> bool {
    edge_kind == TRANSITIVE_DISCOVERY_EDGE_KIND
}
