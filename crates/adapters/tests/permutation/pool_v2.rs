use alloy_primitives::{Address, B256, U256};
use alloy_sol_types::SolEvent;

use super::{
    events::{V2Registrar, V2Registry, V2Resolver},
    names::{labelhash, namehash},
    scenario::{
        Action, AuthorityShape, Dimensions, ExpiryWindow, Perturbation, RecordState,
        RegistrationPath, SubnameShape, WrapState, action, emission, stage,
    },
    world::Wiring,
};

const LABELS: [&str; 3] = ["alpha", "bravo", "charlie"];
const ROOT: &str = "ens_v2_root_l1";
const REGISTRY: &str = "ens_v2_registry_l1";
const REGISTRAR: &str = "ens_v2_registrar_l1";
const RESOLVER: &str = "ens_v2_resolver_l1";

struct Wires<'a> {
    root: &'a str,
    registry: &'a str,
    registrar: &'a str,
    resolver: &'a str,
}

pub fn build(wiring: &Wiring, dimensions: &Dimensions, settle_timestamp: i64) -> Vec<Action> {
    let wires = Wires {
        root: wiring.address(ROOT, "root_registry"),
        registry: wiring.address(REGISTRY, "registry"),
        registrar: wiring.address(REGISTRAR, "registrar"),
        resolver: wiring.address(RESOLVER, "resolver"),
    };
    let registry_address = address(wires.registry);
    let resolver_address = address(wires.resolver);
    let expiry = u64::try_from(expiry_for(dimensions.expiry_window, settle_timestamp))
        .expect("expiry fits u64");

    let eth_hash = labelhash("eth");
    let eth_token = U256::from_be_bytes(eth_hash.0);
    let mut actions = vec![
        action(
            "root:eth-label",
            stage::REGISTER,
            vec![emission(
                wires.root,
                V2Registry::LabelRegistered {
                    tokenId: eth_token,
                    labelHash: eth_hash,
                    label: "eth".to_owned(),
                    owner: actor(0),
                    expiry: 0,
                    sender: actor(0),
                }
                .encode_log_data(),
            )],
        ),
        action(
            "root:eth-subregistry",
            stage::LINK,
            vec![emission(
                wires.root,
                V2Registry::SubregistryUpdated {
                    tokenId: eth_token,
                    subregistry: registry_address,
                    sender: actor(0),
                }
                .encode_log_data(),
            )],
        ),
    ];

    for (index, label) in LABELS.iter().take(dimensions.name_count).enumerate() {
        let seat = u64::try_from(index).expect("name index fits u64");
        let owner = actor(seat * 4 + 1);
        let successor = actor(seat * 4 + 2);
        let operator = actor(seat * 4 + 3);
        let hash = labelhash(label);
        let token = U256::from_be_bytes(hash.0);
        let resource = U256::from(0x0100_u64 + seat);
        let node = namehash(&[label, "eth"]);

        actions.push(action(
            format!("{label}:label-registered"),
            stage::REGISTER,
            vec![emission(
                wires.registry,
                V2Registry::LabelRegistered {
                    tokenId: token,
                    labelHash: hash,
                    label: (*label).to_owned(),
                    owner,
                    expiry,
                    sender: owner,
                }
                .encode_log_data(),
            )],
        ));
        // TokenRegenerated requires a retained TokenResource predecessor, so the pair stays in one
        // transaction and permutation reorders it as a unit.
        let mut token_link = vec![emission(
            wires.registry,
            V2Registry::TokenResource {
                tokenId: token,
                resource,
            }
            .encode_log_data(),
        )];
        if dimensions.wrap_state != WrapState::Unwrapped {
            token_link.push(emission(
                wires.registry,
                V2Registry::TokenRegenerated {
                    oldTokenId: token,
                    newTokenId: token + U256::from(1_u64),
                }
                .encode_log_data(),
            ));
        }
        actions.push(action(
            format!("{label}:token-resource"),
            stage::IDENTITY,
            token_link,
        ));

        if dimensions.registration_path != RegistrationPath::Legacy {
            actions.push(action(
                format!("{label}:registrar-registered"),
                stage::REGISTRAR,
                vec![emission(
                    wires.registrar,
                    V2Registrar::NameRegistered {
                        tokenId: token,
                        label: (*label).to_owned(),
                        owner,
                        subregistry: Address::ZERO,
                        resolver: resolver_address,
                        duration: 31_536_000,
                        paymentToken: Address::ZERO,
                        referrer: B256::ZERO,
                        base: U256::from(1_u64),
                        premium: U256::ZERO,
                    }
                    .encode_log_data(),
                )],
            ));
        }
        if dimensions.registration_path == RegistrationPath::Unwrapped {
            actions.push(action(
                format!("{label}:registrar-renewed"),
                stage::WRITE,
                vec![emission(
                    wires.registrar,
                    V2Registrar::NameRenewed {
                        tokenId: token,
                        label: (*label).to_owned(),
                        duration: 31_536_000,
                        newExpiry: expiry + 31_536_000,
                        paymentToken: Address::ZERO,
                        referrer: B256::ZERO,
                        amount: U256::from(1_u64),
                    }
                    .encode_log_data(),
                )],
            ));
        }

        match dimensions.record_state {
            RecordState::NoResolver => {}
            RecordState::ResolverWithRecords => {
                actions.push(action(
                    format!("{label}:resolver-set"),
                    stage::LINK,
                    vec![emission(
                        wires.registry,
                        V2Registry::ResolverUpdated {
                            tokenId: token,
                            resolver: resolver_address,
                            sender: owner,
                        }
                        .encode_log_data(),
                    )],
                ));
                actions.push(action(
                    format!("{label}:records"),
                    stage::WRITE,
                    vec![
                        emission(
                            wires.resolver,
                            V2Resolver::AddressChanged {
                                node,
                                coinType: U256::from(60_u64),
                                newAddress: owner.to_vec().into(),
                            }
                            .encode_log_data(),
                        ),
                        emission(
                            wires.resolver,
                            V2Resolver::TextChanged {
                                node,
                                indexedKey: labelhash("url"),
                                key: "url".to_owned(),
                                value: format!("https://{label}.example"),
                            }
                            .encode_log_data(),
                        ),
                    ],
                ));
            }
            RecordState::CustomResolverNoRecords => {
                actions.push(action(
                    format!("{label}:custom-resolver"),
                    stage::LINK,
                    vec![emission(
                        wires.registry,
                        V2Registry::ResolverUpdated {
                            tokenId: token,
                            resolver: actor(0x200 + seat),
                            sender: owner,
                        }
                        .encode_log_data(),
                    )],
                ));
            }
        }

        match dimensions.subname_shape {
            SubnameShape::None => {}
            SubnameShape::RegistrySubnode | SubnameShape::DeepSubnode => {
                actions.push(action(
                    format!("{label}:subregistry"),
                    stage::LINK,
                    vec![emission(
                        wires.registry,
                        V2Registry::SubregistryUpdated {
                            tokenId: token,
                            subregistry: actor(0x300 + seat),
                            sender: owner,
                        }
                        .encode_log_data(),
                    )],
                ));
            }
            SubnameShape::WrappedChild => {
                actions.push(action(
                    format!("{label}:parent-updated"),
                    stage::LINK,
                    vec![emission(
                        wires.registry,
                        V2Registry::ParentUpdated {
                            parent: address(wires.root),
                            label: (*label).to_owned(),
                            sender: owner,
                        }
                        .encode_log_data(),
                    )],
                ));
            }
        }

        match dimensions.authority_shape {
            AuthorityShape::SelfOwned => {}
            AuthorityShape::OperatorTransfer => {
                actions.push(action(
                    format!("{label}:transfer"),
                    stage::CONTROL,
                    vec![emission(
                        wires.registry,
                        V2Registry::TransferSingle {
                            operator,
                            from: owner,
                            to: successor,
                            id: token,
                            value: U256::from(1_u64),
                        }
                        .encode_log_data(),
                    )],
                ));
            }
            AuthorityShape::GiveAway => {
                actions.push(action(
                    format!("{label}:roles"),
                    stage::WRITE,
                    vec![emission(
                        wires.registry,
                        V2Registry::EACRolesChanged {
                            resource,
                            account: successor,
                            oldRoleBitmap: U256::ZERO,
                            newRoleBitmap: U256::from(0b11_u64),
                        }
                        .encode_log_data(),
                    )],
                ));
            }
        }

        if dimensions.has(Perturbation::RenewalAfterExpiry) {
            actions.push(action(
                format!("{label}:expiry-updated"),
                stage::WRITE,
                vec![emission(
                    wires.registry,
                    V2Registry::ExpiryUpdated {
                        tokenId: token,
                        newExpiry: expiry + 63_072_000,
                        sender: owner,
                    }
                    .encode_log_data(),
                )],
            ));
        }
        if dimensions.has(Perturbation::Reregistration) {
            actions.push(action(
                format!("{label}:unregistered"),
                stage::LATE,
                vec![emission(
                    wires.registry,
                    V2Registry::LabelUnregistered {
                        tokenId: token,
                        sender: operator,
                    }
                    .encode_log_data(),
                )],
            ));
        }
        if dimensions.has(Perturbation::LateRecordWrite) {
            actions.push(action(
                format!("{label}:late-record"),
                stage::LATE,
                vec![emission(
                    wires.resolver,
                    V2Resolver::NameChanged {
                        node,
                        name: format!("{label}.eth"),
                    }
                    .encode_log_data(),
                )],
            ));
        }
    }

    if dimensions.has(Perturbation::RegistryAnnouncement) {
        actions.push(action(
            "registry:announcement",
            stage::ANNOUNCE,
            vec![emission(
                &format!("{:?}", actor(0x400)).to_ascii_lowercase(),
                V2Registry::RegistryCreated {}.encode_log_data(),
            )],
        ));
    }
    if dimensions.has(Perturbation::ProxyUpgrade) {
        actions.push(action(
            "registry:upgraded",
            stage::LATE,
            vec![emission(
                wires.registry,
                V2Registry::Upgraded {
                    implementation: actor(0x401),
                }
                .encode_log_data(),
            )],
        ));
    }
    if dimensions.has(Perturbation::LateRegistryWrite) {
        actions.push(action(
            "root:late-expiry",
            stage::LATE,
            vec![emission(
                wires.root,
                V2Registry::ExpiryUpdated {
                    tokenId: eth_token,
                    newExpiry: expiry,
                    sender: actor(0),
                }
                .encode_log_data(),
            )],
        ));
    }
    actions
}

fn expiry_for(window: ExpiryWindow, settle_timestamp: i64) -> i64 {
    match window {
        ExpiryWindow::Active => settle_timestamp + 31_536_000,
        ExpiryWindow::JustExpired => settle_timestamp - 3_600,
        ExpiryWindow::PastGrace => settle_timestamp - 17_280_000,
    }
}

fn address(value: &str) -> Address {
    value.parse().expect("world address is well formed")
}

fn actor(index: u64) -> Address {
    format!("0x{:040x}", 0xe000_0000_u64 + index)
        .parse()
        .expect("actor address is well formed")
}
