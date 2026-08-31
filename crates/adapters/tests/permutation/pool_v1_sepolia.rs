use alloy_primitives::{Address, U256};
use alloy_sol_types::SolEvent;

use super::{
    events::{V1RegistrarToken, V1Registry, V1Resolver, V1Wrapper},
    names::{dns_encode, labelhash, namehash},
    scenario::{Action, Dimensions, action, emission, stage},
    world::Wiring,
};

const LABELS: [&str; 3] = ["alpha", "bravo", "charlie"];
const REGISTRY: &str = "ens_v1_registry_l1";
const REGISTRAR: &str = "ens_v1_registrar_l1";
const WRAPPER: &str = "ens_v1_wrapper_l1";
const RESOLVER: &str = "ens_v1_resolver_l1";
const GRACE_PERIOD: i64 = 90 * 24 * 60 * 60;

/// Generates the ordinary ENSv1 authority path declared by the checked-in Sepolia manifests.
/// Dedicated tests cover correlation-dependent BaseRegistrar events; this generated event world
/// exercises the registry, resolver, wrapper, and registrar Transfer that restores unwrap authority.
pub fn build(wiring: &Wiring, dimensions: &Dimensions, settle_timestamp: i64) -> Vec<Action> {
    let registry = wiring.address(REGISTRY, "registry");
    let registrar = wiring.address(REGISTRAR, "registrar");
    let wrapper = wiring.address(WRAPPER, "name_wrapper");
    let resolver = wiring.address(RESOLVER, "public_resolver");
    let wrapper_address = address(wrapper);
    let resolver_address = address(resolver);
    let eth_node = namehash(&["eth"]);
    let expiry = u64::try_from(settle_timestamp + GRACE_PERIOD + 31_536_000)
        .expect("Sepolia fixture expiry fits u64");
    let mut actions = Vec::new();

    for (index, label) in LABELS.iter().take(dimensions.name_count).enumerate() {
        let owner = actor(u64::try_from(index).expect("name index fits u64") * 3);
        let successor = actor(u64::try_from(index).expect("name index fits u64") * 3 + 1);
        let operator = actor(u64::try_from(index).expect("name index fits u64") * 3 + 2);
        let hash = labelhash(label);
        let node = namehash(&[label, "eth"]);
        let child_hash = labelhash("sub");
        let child_node = namehash(&["sub", label, "eth"]);

        actions.push(action(
            format!("{label}:registry-setup"),
            stage::REGISTER,
            vec![emission(
                registry,
                V1Registry::NewOwner {
                    node: eth_node,
                    label: hash,
                    owner: wrapper_address,
                }
                .encode_log_data(),
            )],
        ));
        actions.push(action(
            format!("{label}:wrap"),
            stage::LINK,
            vec![
                emission(
                    registry,
                    V1Registry::Transfer {
                        node,
                        owner: wrapper_address,
                    }
                    .encode_log_data(),
                ),
                emission(
                    wrapper,
                    V1Wrapper::NameWrapped {
                        node,
                        name: dns_encode(&[label, "eth"]).into(),
                        owner,
                        fuses: (1 << 16) | (1 << 17),
                        expiry,
                    }
                    .encode_log_data(),
                ),
            ],
        ));
        actions.push(action(
            format!("{label}:resolver"),
            stage::LINK,
            vec![
                emission(
                    registry,
                    V1Registry::NewResolver {
                        node,
                        resolver: resolver_address,
                    }
                    .encode_log_data(),
                ),
                emission(
                    resolver,
                    V1Resolver::TextChanged {
                        node,
                        indexedKey: labelhash("url"),
                        key: "url".to_owned(),
                        value: "https://example.test".to_owned(),
                    }
                    .encode_log_data(),
                ),
                emission(
                    resolver,
                    V1Resolver::VersionChanged {
                        node,
                        newVersion: 1,
                    }
                    .encode_log_data(),
                ),
            ],
        ));
        actions.push(action(
            format!("{label}:wrapper-lifecycle"),
            stage::WRITE,
            vec![
                emission(
                    registry,
                    V1Registry::NewOwner {
                        node,
                        label: child_hash,
                        owner: wrapper_address,
                    }
                    .encode_log_data(),
                ),
                emission(
                    wrapper,
                    V1Wrapper::NameWrapped {
                        node: child_node,
                        name: dns_encode(&["sub", label, "eth"]).into(),
                        owner,
                        fuses: 1 << 16,
                        expiry: expiry - 86_400,
                    }
                    .encode_log_data(),
                ),
                emission(
                    wrapper,
                    V1Wrapper::ExpiryExtended {
                        node: child_node,
                        expiry,
                    }
                    .encode_log_data(),
                ),
                emission(
                    wrapper,
                    V1Wrapper::TransferSingle {
                        operator,
                        from: owner,
                        to: successor,
                        id: U256::from_be_bytes(node.0),
                        value: U256::from(1_u64),
                    }
                    .encode_log_data(),
                ),
            ],
        ));
        actions.push(action(
            format!("{label}:unwrap"),
            stage::LATE,
            vec![
                emission(
                    registry,
                    V1Registry::Transfer {
                        node,
                        owner: successor,
                    }
                    .encode_log_data(),
                ),
                emission(
                    wrapper,
                    V1Wrapper::NameUnwrapped {
                        node,
                        owner: successor,
                    }
                    .encode_log_data(),
                ),
                emission(
                    registrar,
                    V1RegistrarToken::Transfer {
                        from: wrapper_address,
                        to: successor,
                        tokenId: U256::from_be_bytes(hash.0),
                    }
                    .encode_log_data(),
                ),
            ],
        ));
    }
    actions
}

fn address(value: &str) -> Address {
    value.parse().expect("world address is well formed")
}

fn actor(index: u64) -> Address {
    format!("0x{:040x}", 0xe000_0000_u64 + index)
        .parse()
        .expect("actor address is well formed")
}
