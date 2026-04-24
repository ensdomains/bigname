use anyhow::{Context, Result, bail};

use super::types::DiscoveryObservation;
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
