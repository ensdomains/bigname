use alloy_primitives::{Address, U256};
use alloy_sol_types::SolEvent;

use super::{
    events::{V1RegistrarToken, V1Registry, V1Resolver, V1Reverse, V1Wrapper},
    names::{dns_encode, labelhash, namehash, reverse_labels},
    scenario::{Action, Dimensions, ExpiryWindow, Perturbation, action, emission, stage},
    world::Wiring,
};

const LABELS: [&str; 3] = ["alpha", "bravo", "charlie"];
const REGISTRY: &str = "ens_v1_registry_l1";
const REGISTRAR: &str = "ens_v1_registrar_l1";
const WRAPPER: &str = "ens_v1_wrapper_l1";
const RESOLVER: &str = "ens_v1_resolver_l1";
const GRACE_PERIOD: i64 = 90 * 24 * 60 * 60;

/// Generates the ordinary ENSv1 authority path declared by the checked-in Sepolia manifests.
/// Numeric BaseRegistrar grants and renewals establish a lease even without an admitted controller
/// label event. Registration emits the mint, registry ownership, then the numeric grant.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L167 @ ens_v1@91c966f)
pub fn build(wiring: &Wiring, dimensions: &Dimensions, settle_timestamp: i64) -> Vec<Action> {
    let registry = wiring.address(REGISTRY, "registry");
    let registrar = wiring.address(REGISTRAR, "registrar");
    let wrapper = wiring.address(WRAPPER, "name_wrapper");
    let resolver = wiring.address(RESOLVER, "public_resolver");
    let wrapper_address = address(wrapper);
    let resolver_address = address(resolver);
    let eth_node = namehash(&["eth"]);
    let lease_expiry = u64::try_from(match dimensions.expiry_window {
        ExpiryWindow::Active => settle_timestamp + 31_536_000,
        ExpiryWindow::JustExpired => settle_timestamp - 3_600,
        ExpiryWindow::PastGrace => settle_timestamp - 17_280_000,
    })
    .expect("Sepolia fixture lease expiry fits u64");
    let expiry =
        lease_expiry + u64::try_from(GRACE_PERIOD).expect("Sepolia fixture expiry fits u64");
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
            vec![
                emission(
                    registrar,
                    V1RegistrarToken::Transfer {
                        from: Address::ZERO,
                        to: owner,
                        tokenId: U256::from_be_bytes(hash.0),
                    }
                    .encode_log_data(),
                ),
                emission(
                    registry,
                    V1Registry::NewOwner {
                        node: eth_node,
                        label: hash,
                        owner,
                    }
                    .encode_log_data(),
                ),
                emission(
                    registrar,
                    V1RegistrarToken::NameRegistered {
                        id: U256::from_be_bytes(hash.0),
                        owner,
                        expires: U256::from(lease_expiry),
                    }
                    .encode_log_data(),
                ),
            ],
        ));
        actions.push(action(
            format!("{label}:wrap"),
            stage::LINK,
            vec![
                emission(
                    registrar,
                    V1RegistrarToken::Transfer {
                        from: owner,
                        to: wrapper_address,
                        tokenId: U256::from_be_bytes(hash.0),
                    }
                    .encode_log_data(),
                ),
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
        if dimensions.has(Perturbation::RenewalAfterExpiry) {
            actions.push(action(
                format!("{label}:renewal"),
                stage::WRITE,
                vec![emission(
                    registrar,
                    V1RegistrarToken::NameRenewed {
                        id: U256::from_be_bytes(hash.0),
                        expires: U256::from(lease_expiry + 31_536_000),
                    }
                    .encode_log_data(),
                )],
            ));
        }
        if dimensions.has(Perturbation::ReverseClaim)
            && let Some(reverse) = wiring.optional_address("ens_v1_reverse_l1", "reverse_registrar")
        {
            let reverse_labels = reverse_labels(&format!("{owner:#x}"));
            let reverse_node = namehash(
                &reverse_labels
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            );
            actions.push(action(
                format!("{label}:reverse"),
                stage::LATE,
                vec![
                    emission(
                        reverse,
                        V1Reverse::ReverseClaimed {
                            addr: owner,
                            node: reverse_node,
                        }
                        .encode_log_data(),
                    ),
                    emission(
                        registry,
                        V1Registry::NewResolver {
                            node: reverse_node,
                            resolver: resolver_address,
                        }
                        .encode_log_data(),
                    ),
                    emission(
                        resolver,
                        V1Resolver::NameChanged {
                            node: reverse_node,
                            name: format!("{label}.eth"),
                        }
                        .encode_log_data(),
                    ),
                ],
            ));
        }
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
