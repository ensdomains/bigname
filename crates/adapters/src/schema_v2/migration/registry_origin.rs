//! Independent factory evidence for the permission reader's explicitly supported code models.
//! A retained origin is not a registry admission or a migration correlation.
use serde_json::Value;

use super::{Catalog, MIGRATION_FAMILY, NormalizedEvent};

// The exact reviewed implementations, never a role-only assertion about arbitrary future code.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/WrapperRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const IMPLEMENTATIONS: [(&str, &str); 2] = [
    (
        "wrapper_registry_implementation",
        "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880",
    ),
    (
        "user_registry_implementation",
        "0x9bd8a88719068d09ecee662f36c0e3856708366a",
    ),
];

pub(super) fn supported_origin(catalog: &Catalog, event: &NormalizedEvent) -> bool {
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
        || source.deployment_label != "ens_v2_sepolia_20261001"
        || event.source_manifest_id != Some(source.manifest_id)
    {
        return false;
    }
    let declared = |role: &str, address: &str| {
        catalog
            .declared_address_for_role(MIGRATION_FAMILY, role)
            .is_some_and(|value| value.eq_ignore_ascii_case(address))
            && catalog
                .declared_start_block_for_role(MIGRATION_FAMILY, role)
                .is_some_and(|start| start <= block)
    };
    let Some(emitter) = event
        .raw_fact_ref
        .get("emitting_address")
        .and_then(Value::as_str)
    else {
        return false;
    };
    if !emitter.eq_ignore_ascii_case("0xda70306c98e97ece36f997a21368e53298572991")
        || !declared("verifiable_factory", emitter)
    {
        return false;
    }
    let Some(implementation) = event
        .after_state
        .get("implementation")
        .and_then(Value::as_str)
    else {
        return false;
    };
    IMPLEMENTATIONS
        .iter()
        .any(|(role, exact)| implementation.eq_ignore_ascii_case(exact) && declared(role, exact))
}
