//! Independent factory evidence for the permission reader's explicitly supported code models.
//! A retained origin is not a registry admission or a migration correlation.
use serde_json::Value;

use super::{CANDIDATE, Catalog, MIGRATION_FAMILY, NormalizedEvent};

// The exact reviewed implementations, never a role-only assertion about arbitrary future code.
// One row per reviewed deployment, keyed by its chain and deployment label: the
// VerifiableFactory and the registry implementations it deploys. A row is inert until that
// deployment's migration manifest on that chain declares the factory and the implementation.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/VerifiableFactory.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/WrapperRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/VerifiableFactory.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
type Deployment = (
    &'static str,
    &'static str,
    &'static str,
    [(&'static str, &'static str); 2],
);
const DEPLOYMENTS: [Deployment; 2] = [
    (
        "ethereum-sepolia",
        "ens_v2_sepolia_20261001",
        "0xda70306c98e97ece36f997a21368e53298572991",
        [
            (
                "wrapper_registry_implementation",
                "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880",
            ),
            (
                "user_registry_implementation",
                "0x9bd8a88719068d09ecee662f36c0e3856708366a",
            ),
        ],
    ),
    (
        "ethereum-mainnet",
        "ens_v2_mainnet_20261002",
        "0x77510ba162dfea3d8989e3f0928a691b345730d2",
        [
            (
                "wrapper_registry_implementation",
                "0xcf3edb3f5501de63b57254d910b6782ee370039b",
            ),
            (
                "user_registry_implementation",
                "0x75ff9adbcfce2469b097ac68f6ae650eae1351bb",
            ),
        ],
    ),
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
    let Some((.., factory, implementations)) = DEPLOYMENTS
        .iter()
        .find(|(chain, label, ..)| *chain == source.chain_id && *label == source.deployment_label)
    else {
        return false;
    };
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
    if !emitter.eq_ignore_ascii_case(factory) || !declared("verifiable_factory", emitter) {
        return false;
    }
    let Some(implementation) = event
        .after_state
        .get("implementation")
        .and_then(Value::as_str)
    else {
        return false;
    };
    implementations
        .iter()
        .any(|(role, exact)| implementation.eq_ignore_ascii_case(exact) && declared(role, exact))
}
