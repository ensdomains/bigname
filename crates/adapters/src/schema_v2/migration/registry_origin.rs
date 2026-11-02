//! Independent factory evidence for the permission reader's explicitly supported code models.
//! A retained origin is not a registry admission or a migration correlation.
use serde_json::Value;

use super::{CANDIDATE, Catalog, MIGRATION_FAMILY, NormalizedEvent};

// The registry implementations whose factory proxies the permission reader supports are the
// active migration manifest's declarations under the two implementation roles, each from its
// declared start (docs/manifests.md § Registry implementation declarations). The manifest
// review is what holds an address to reviewed code, as it is for the root and the registry.
const IMPLEMENTATION_ROLES: [&str; 2] = [
    bigname_manifests::WRAPPER_REGISTRY_IMPLEMENTATION_ROLE,
    bigname_manifests::USER_REGISTRY_IMPLEMENTATION_ROLE,
];

pub(super) fn retain(catalog: &Catalog, events: &mut Vec<NormalizedEvent>) {
    // Preserve independent known registry origins without changing any correlated visibility.
    // Their eventual RegistryCreated announcement still owns ordinary registry admission.
    events.retain(|event| {
        event.source_family != MIGRATION_FAMILY
            || event.consumer_visibility == CANDIDATE
            || supported_origin(catalog, event)
    });
}

fn supported_origin(catalog: &Catalog, event: &NormalizedEvent) -> bool {
    let Some(source) = catalog.source_for_family(MIGRATION_FAMILY) else {
        return false;
    };
    let Some(block) = event.block_number else {
        return false;
    };
    if event.event_kind != "ContractDiscovered"
        || event
            .after_state
            .get("source_event")
            .and_then(Value::as_str)
            != Some("ContractDiscovered")
        || source.namespace != event.namespace
        || source.chain_id != event.chain_id
        || event.source_manifest_id != Some(source.manifest_id)
    {
        return false;
    }
    let Some(emitter) = event
        .raw_fact_ref
        .get("emitting_address")
        .and_then(Value::as_str)
    else {
        return false;
    };
    if !catalog.declares_at(MIGRATION_FAMILY, "verifiable_factory", emitter, block) {
        return false;
    }
    let Some(implementation) = event
        .after_state
        .get("implementation")
        .and_then(Value::as_str)
    else {
        return false;
    };
    IMPLEMENTATION_ROLES
        .iter()
        .any(|role| catalog.declares_at(MIGRATION_FAMILY, role, implementation, block))
}
