use alloy_primitives::{Address, B256, U256};
use alloy_sol_types::SolEvent;

use super::{
    events::{V2RecordResolver, V2Registrar, V2Registry, V2Resolver},
    names::{dns_encode, labelhash, namehash},
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
/// The resolver `created_resolver` deploys: no declared role and no registry pointer names it.
pub const CREATED_RESOLVER: &str = "0x00000000000000000000000000000000e0000402";

struct Wires<'a> {
    root: &'a str,
    registry: &'a str,
    registrar: &'a str,
    resolver: &'a str,
    public_resolver: Option<&'a str>,
}

pub fn build(wiring: &Wiring, dimensions: &Dimensions, settle_timestamp: i64) -> Vec<Action> {
    let wires = Wires {
        root: wiring.address(ROOT, "root_registry"),
        registry: wiring.address(REGISTRY, "registry"),
        registrar: wiring.address(REGISTRAR, "registrar"),
        resolver: wiring.address(RESOLVER, "resolver"),
        public_resolver: wiring.optional_address(RESOLVER, "public_resolver_v2"),
    };
    let registry_address = address(wires.registry);
    let resolver_address = address(wires.resolver);
    let expiry = u64::try_from(expiry_for(dimensions.expiry_window, settle_timestamp))
        .expect("expiry fits u64");

    let eth_hash = labelhash("eth");
    let eth_token = U256::from_be_bytes(eth_hash.0);
    let mut actions = vec![
        action(
            "root:reserved",
            stage::REGISTER,
            vec![emission(
                wires.root,
                V2Registry::LabelReserved {
                    tokenId: U256::from_be_bytes(labelhash("reserved").0),
                    labelHash: labelhash("reserved"),
                    label: "reserved".to_owned(),
                    expiry: u64::try_from(settle_timestamp + 31_536_000)
                        .expect("reservation expiry fits u64"),
                    sender: actor(0),
                }
                .encode_log_data(),
            )],
        ),
        action(
            "root:roles",
            stage::WRITE,
            vec![emission(
                wires.root,
                V2Registry::EACRolesChanged {
                    resource: U256::ZERO,
                    account: actor(0),
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1_u64),
                }
                .encode_log_data(),
            )],
        ),
        action(
            "resolver:version-reset",
            stage::WRITE,
            vec![emission(
                wires.public_resolver.unwrap_or(wires.resolver),
                V2Resolver::VersionChanged {
                    node: namehash(&["version", "eth"]),
                    newVersion: 1,
                }
                .encode_log_data(),
            )],
        ),
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
        let alias_label = format!("alias-{label}");
        let alias_node = namehash(&[alias_label.as_str(), "eth"]);

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

        if wires.public_resolver.is_some() {
            // The record-ID deployment links both names to one record, then writes through that
            // ID. It has no AliasChanged event.
            // (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PermissionedResolver.sol:L352-L366 @ ens_v2_sepolia_20260903@5da83f6a)
            let record_id = U256::from(seat + 1);
            actions.push(action(
                format!("{label}:record-link"),
                stage::WRITE,
                vec![
                    emission(
                        wires.resolver,
                        V2RecordResolver::Linked {
                            recordId: record_id,
                            node,
                            name: dns_encode(&[label, "eth"]).into(),
                        }
                        .encode_log_data(),
                    ),
                    emission(
                        wires.resolver,
                        V2RecordResolver::Linked {
                            recordId: record_id,
                            node: alias_node,
                            name: dns_encode(&[alias_label.as_str(), "eth"]).into(),
                        }
                        .encode_log_data(),
                    ),
                ],
            ));
            actions.push(action(
                format!("{label}:record-write"),
                stage::LATE,
                vec![
                    emission(
                        wires.resolver,
                        V2RecordResolver::AddressUpdated {
                            recordId: record_id,
                            coinType: U256::from(60_u64),
                            addressBytes: owner.to_vec().into(),
                        }
                        .encode_log_data(),
                    ),
                    emission(
                        wires.resolver,
                        V2RecordResolver::NameUpdated {
                            recordId: record_id,
                            primaryName: format!("{label}.eth"),
                        }
                        .encode_log_data(),
                    ),
                ],
            ));
            // Setter argument evidence precedes the matching role grant.
            // (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PermissionedResolver.sol:L253-L261 @ ens_v2_sepolia_20260903@5da83f6a)
            let argument = format!("key-{label}").into_bytes();
            let permission_resource = U256::from_be_bytes(alloy_primitives::keccak256(&argument).0);
            actions.push(action(
                format!("{label}:resolver-permission"),
                stage::WRITE,
                vec![
                    emission(
                        wires.resolver,
                        V2RecordResolver::ResourceArgument {
                            resource: permission_resource,
                            arg: argument.into(),
                        }
                        .encode_log_data(),
                    ),
                    emission(
                        wires.resolver,
                        V2Resolver::EACRolesChanged {
                            resource: permission_resource,
                            account: owner,
                            oldRoleBitmap: U256::ZERO,
                            newRoleBitmap: U256::from(1_u64 << 4),
                        }
                        .encode_log_data(),
                    ),
                ],
            ));
        } else {
            actions.push(action(
                format!("{label}:alias"),
                stage::WRITE,
                vec![emission(
                    wires.resolver,
                    V2Resolver::AliasChanged {
                        indexedFromName: alloy_primitives::keccak256(dns_encode(&[label, "eth"])),
                        indexedToName: alloy_primitives::keccak256(dns_encode(&[
                            alias_label.as_str(),
                            "eth",
                        ])),
                        fromName: dns_encode(&[label, "eth"]).into(),
                        toName: dns_encode(&[alias_label.as_str(), "eth"]).into(),
                    }
                    .encode_log_data(),
                )],
            ));
            actions.push(action(
                format!("{label}:alias-record"),
                stage::LATE,
                vec![emission(
                    wires.resolver,
                    V2Resolver::AddressChanged {
                        node: alias_node,
                        coinType: U256::from(60_u64),
                        newAddress: owner.to_vec().into(),
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
                            wires.public_resolver.unwrap_or(wires.resolver),
                            V2Resolver::AddressChanged {
                                node,
                                coinType: U256::from(60_u64),
                                newAddress: owner.to_vec().into(),
                            }
                            .encode_log_data(),
                        ),
                        emission(
                            wires.public_resolver.unwrap_or(wires.resolver),
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
                    if wires.public_resolver.is_some() {
                        V2RecordResolver::NameUpdated {
                            recordId: U256::from(seat + 1),
                            primaryName: format!("{label}.eth"),
                        }
                        .encode_log_data()
                    } else {
                        V2Resolver::NameChanged {
                            node,
                            name: format!("{label}.eth"),
                        }
                        .encode_log_data()
                    },
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
    if dimensions.resolver_creation {
        actions.extend(created_resolver());
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

/// A resolver that no manifest role and no registry pointer admits: its own `ResolverCreated()` is
/// the only thing that lets its record events derive. The initializer emits the creation first,
/// then its role grants, then the record writes of its multicall, all in one transaction; a write
/// in a later transaction follows, which a split may place in a later batch than the creation.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/PermissionedResolver.sol:L119-L126 @ ens_v2_sepolia_20260916@366de741)
fn created_resolver() -> Vec<Action> {
    let resolver = CREATED_RESOLVER;
    let admin = actor(0x403);
    let record_id = U256::from(1_u64);
    vec![
        action(
            "created-resolver:initialized",
            stage::ANNOUNCE,
            vec![
                emission(
                    resolver,
                    V2RecordResolver::ResolverCreated {}.encode_log_data(),
                ),
                emission(
                    resolver,
                    V2Resolver::EACRolesChanged {
                        resource: U256::ZERO,
                        account: admin,
                        oldRoleBitmap: U256::ZERO,
                        newRoleBitmap: U256::from(1_u64),
                    }
                    .encode_log_data(),
                ),
                emission(
                    resolver,
                    V2RecordResolver::Linked {
                        recordId: record_id,
                        node: namehash(&["created", "eth"]),
                        name: dns_encode(&["created", "eth"]).into(),
                    }
                    .encode_log_data(),
                ),
                emission(
                    resolver,
                    V2RecordResolver::AddressUpdated {
                        recordId: record_id,
                        coinType: U256::from(60_u64),
                        addressBytes: admin.to_vec().into(),
                    }
                    .encode_log_data(),
                ),
            ],
        ),
        action(
            "created-resolver:late-record",
            stage::LATE,
            vec![emission(
                resolver,
                V2RecordResolver::NameUpdated {
                    recordId: record_id,
                    primaryName: "created.eth".to_owned(),
                }
                .encode_log_data(),
            )],
        ),
    ]
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
