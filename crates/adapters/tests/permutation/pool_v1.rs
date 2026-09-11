use alloy_primitives::{Address, B256, U256};
use alloy_sol_types::SolEvent;

use super::{
    events::{
        V1BaseRegistrar, V1LegacyController, V1RegistrarToken, V1Registry, V1Resolver, V1Reverse,
        V1UnwrappedController, V1WrappedController, V1Wrapper,
    },
    names::{child_node, dns_encode, labelhash, namehash, reverse_labels},
    scenario::{
        Action, AuthorityShape, BurstPhase, Dimensions, ExpiryWindow, Perturbation, RecordState,
        RegistrationPath, SubnameShape, WrapState, action, emission, stage,
    },
    world::Wiring,
};

const LABELS: [&str; 3] = ["alpha", "bravo", "charlie"];
/// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
const CANNOT_UNWRAP: u32 = 1;
/// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18 @ ens_v1@91c966f)
const PARENT_CANNOT_CONTROL: u32 = 1 << 16;
/// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L19 @ ens_v1@91c966f)
const IS_DOT_ETH: u32 = 1 << 17;
/// Wrapping a .eth 2LD always burns both, so no other word is reachable for one
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1013 @ ens_v1@91c966f).
const WRAPPED_2LD_FUSES: u32 = PARENT_CANNOT_CONTROL | IS_DOT_ETH;
const REGISTRY: &str = "ens_v1_registry_l1";
const REGISTRAR: &str = "ens_v1_registrar_l1";
const WRAPPER: &str = "ens_v1_wrapper_l1";
const RESOLVER: &str = "ens_v1_resolver_l1";
const REVERSE: &str = "ens_v1_reverse_l1";

struct Wires<'a> {
    registry: &'a str,
    registrar: &'a str,
    legacy_controller: &'a str,
    wrapped_controller: &'a str,
    unwrapped_controller: &'a str,
    wrapper: &'a str,
    resolver: &'a str,
    reverse: &'a str,
}

pub fn build(wiring: &Wiring, dimensions: &Dimensions, settle_timestamp: i64) -> Vec<Action> {
    let wires = Wires {
        registry: wiring.address(REGISTRY, "registry"),
        registrar: wiring.address(REGISTRAR, "registrar"),
        legacy_controller: wiring.address(REGISTRAR, "legacy_registrar_controller"),
        wrapped_controller: wiring.address(REGISTRAR, "wrapped_registrar_controller"),
        unwrapped_controller: wiring.address(REGISTRAR, "unwrapped_registrar_controller"),
        wrapper: wiring.address(WRAPPER, "name_wrapper"),
        resolver: wiring.address(RESOLVER, "public_resolver"),
        reverse: wiring.address(REVERSE, "reverse_registrar"),
    };
    let wrapper_address = address(wires.wrapper);
    let resolver_address = address(wires.resolver);
    let eth_node = namehash(&["eth"]);
    let expires = expiry(dimensions.expiry_window, settle_timestamp);

    let mut actions = Vec::new();
    for (index, label) in LABELS.iter().take(dimensions.name_count).enumerate() {
        let seat = u64::try_from(index).expect("name index fits u64");
        let owner = actor(seat * 4);
        let successor = actor(seat * 4 + 1);
        let operator = actor(seat * 4 + 2);
        let node = namehash(&[label, "eth"]);
        let hash = labelhash(label);
        let token = U256::from_be_bytes(hash.0);
        let registrant = match dimensions.authority_shape {
            AuthorityShape::GiveAway => successor,
            _ => owner,
        };

        let mut onboarding = registration(
            &wires,
            dimensions.registration_path,
            label,
            hash,
            eth_node,
            registrant,
            wrapper_address,
            expires,
            stage::REGISTER,
        );
        if dimensions.pre_registration_burst {
            burst_around_registration(&mut onboarding, &wires, node, successor, operator);
            // The same selector written after registration: whether the rewrite lands in the burst
            // block or a later one is the layout's seeded choice, and both placements matter.
            let mut rewrite = emission(
                wires.resolver,
                V1Resolver::AddrChanged { node, a: successor }.encode_log_data(),
            );
            rewrite.burst = Some(BurstPhase::PostRegistrationRewrite);
            actions.push(action(
                format!("{label}:rewrite-after-registration"),
                stage::WRITE,
                vec![rewrite],
            ));
        }
        actions.push(onboarding);

        if dimensions.registration_path != RegistrationPath::Wrapped {
            actions.push(action(
                format!("{label}:token-mint"),
                stage::IDENTITY,
                vec![emission(
                    wires.registrar,
                    V1RegistrarToken::Transfer {
                        from: Address::ZERO,
                        to: registrant,
                        tokenId: token,
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
                        V1Registry::NewResolver {
                            node,
                            resolver: resolver_address,
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
                            V1Resolver::AddrChanged { node, a: owner }.encode_log_data(),
                        ),
                        emission(
                            wires.resolver,
                            V1Resolver::TextChanged {
                                node,
                                indexedKey: labelhash("url"),
                                key: "url".to_owned(),
                                value: format!("https://{label}.example"),
                            }
                            .encode_log_data(),
                        ),
                    ],
                ));
                actions.push(action(
                    format!("{label}:contenthash"),
                    stage::WRITE,
                    vec![emission(
                        wires.resolver,
                        V1Resolver::ContenthashChanged {
                            node,
                            hash: vec![0xe3, 0x01, seat as u8].into(),
                        }
                        .encode_log_data(),
                    )],
                ));
            }
            RecordState::CustomResolverNoRecords => {
                actions.push(action(
                    format!("{label}:custom-resolver"),
                    stage::LINK,
                    vec![emission(
                        wires.registry,
                        V1Registry::NewResolver {
                            node,
                            resolver: actor(0x100 + seat),
                        }
                        .encode_log_data(),
                    )],
                ));
            }
        }

        let born_wrapped = dimensions.registration_path == RegistrationPath::Wrapped;
        if dimensions.wrap_state != WrapState::Unwrapped || born_wrapped {
            let fuses = match dimensions.wrap_state {
                WrapState::WrappedLocked => WRAPPED_2LD_FUSES | CANNOT_UNWRAP,
                _ => WRAPPED_2LD_FUSES,
            };
            actions.push(action(
                format!("{label}:wrap"),
                stage::LINK,
                vec![
                    // Only an unwrapped name is handed to the wrapper; one registered through the
                    // wrapped controller is already wrapper-owned, and re-emitting the registry
                    // Transfer would acquire it twice.
                    emission(
                        wires.registry,
                        V1Registry::Transfer {
                            node,
                            owner: if born_wrapped {
                                registrant
                            } else {
                                wrapper_address
                            },
                        }
                        .encode_log_data(),
                    ),
                    emission(
                        wires.wrapper,
                        V1Wrapper::NameWrapped {
                            node,
                            name: dns_encode(&[label, "eth"]).into(),
                            owner: registrant,
                            fuses,
                            expiry: u64::try_from(expires).expect("expiry fits u64"),
                        }
                        .encode_log_data(),
                    ),
                ],
            ));
            actions.push(action(
                format!("{label}:wrapper-expiry"),
                stage::WRITE,
                vec![emission(
                    wires.wrapper,
                    V1Wrapper::ExpiryExtended {
                        node,
                        expiry: u64::try_from(expires + 86_400).expect("expiry fits u64"),
                    }
                    .encode_log_data(),
                )],
            ));
            actions.push(action(
                format!("{label}:wrapper-transfer"),
                stage::WRITE,
                vec![emission(
                    wires.wrapper,
                    V1Wrapper::TransferSingle {
                        operator,
                        from: registrant,
                        to: successor,
                        id: U256::from_be_bytes(node.0),
                        value: U256::from(1_u64),
                    }
                    .encode_log_data(),
                )],
            ));
            if dimensions.wrap_state == WrapState::WrappedUnlocked {
                actions.push(action(
                    format!("{label}:unwrap"),
                    stage::LATE,
                    vec![
                        emission(
                            wires.wrapper,
                            V1Wrapper::NameUnwrapped {
                                node,
                                owner: successor,
                            }
                            .encode_log_data(),
                        ),
                        emission(
                            wires.registry,
                            V1Registry::Transfer {
                                node,
                                owner: successor,
                            }
                            .encode_log_data(),
                        ),
                    ],
                ));
            }
        }

        match dimensions.authority_shape {
            AuthorityShape::SelfOwned => {}
            AuthorityShape::OperatorTransfer => {
                actions.push(action(
                    format!("{label}:registrar-transfer"),
                    stage::CONTROL,
                    vec![emission(
                        wires.registrar,
                        V1RegistrarToken::Transfer {
                            from: registrant,
                            to: successor,
                            tokenId: token,
                        }
                        .encode_log_data(),
                    )],
                ));
            }
            AuthorityShape::GiveAway => {
                actions.push(action(
                    format!("{label}:registry-transfer"),
                    stage::CONTROL,
                    vec![emission(
                        wires.registry,
                        V1Registry::Transfer {
                            node,
                            owner: operator,
                        }
                        .encode_log_data(),
                    )],
                ));
            }
        }

        match dimensions.subname_shape {
            SubnameShape::None => {}
            SubnameShape::RegistrySubnode => {
                actions.push(subnode(&wires, label, "sub", node, owner, stage::IDENTITY));
            }
            SubnameShape::WrappedChild => {
                let child_node = child_node(node, "kid");
                let child = child_wrap(dimensions.wrap_state, expires);
                actions.push(subnode(
                    &wires,
                    label,
                    "kid",
                    node,
                    wrapper_address,
                    stage::IDENTITY,
                ));
                actions.push(action(
                    format!("{label}:wrapped-child"),
                    stage::WRITE,
                    vec![emission(
                        wires.wrapper,
                        V1Wrapper::NameWrapped {
                            node: child_node,
                            name: dns_encode(&["kid", label, "eth"]).into(),
                            owner: successor,
                            fuses: child.0,
                            expiry: child.1,
                        }
                        .encode_log_data(),
                    )],
                ));
            }
            SubnameShape::DeepSubnode => {
                let child = child_node(node, "sub");
                actions.push(subnode(&wires, label, "sub", node, owner, stage::IDENTITY));
                actions.push(subnode(
                    &wires,
                    label,
                    "deep",
                    child,
                    successor,
                    stage::LINK,
                ));
            }
        }

        if dimensions.has(Perturbation::RenewalAfterExpiry) {
            actions.push(renewal(
                &wires,
                dimensions.registration_path,
                label,
                hash,
                expires + 31_536_000,
            ));
        }
        if dimensions.has(Perturbation::LateRegistryWrite) {
            actions.push(action(
                format!("{label}:late-registry-write"),
                stage::LATE,
                vec![emission(
                    wires.registry,
                    V1Registry::NewOwner {
                        node: eth_node,
                        label: hash,
                        owner: operator,
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
                    V1Resolver::AddrChanged { node, a: successor }.encode_log_data(),
                )],
            ));
        }
        if dimensions.has(Perturbation::Reregistration) {
            actions.push(registration(
                &wires,
                RegistrationPath::Unwrapped,
                label,
                hash,
                eth_node,
                operator,
                wrapper_address,
                expires + 63_072_000,
                stage::LATE,
            ));
        }
        if dimensions.has(Perturbation::ReverseClaim) {
            let labels = reverse_labels(&format!("{registrant:?}"));
            let reverse = namehash(&labels.iter().map(String::as_str).collect::<Vec<_>>());
            actions.push(action(
                format!("{label}:reverse-claim"),
                stage::LATE,
                vec![
                    emission(
                        wires.reverse,
                        V1Reverse::ReverseClaimed {
                            addr: registrant,
                            node: reverse,
                        }
                        .encode_log_data(),
                    ),
                    emission(
                        wires.resolver,
                        V1Resolver::NameChanged {
                            node: reverse,
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

/// The burst shape the stage ordering otherwise keeps unreachable: resolver record writes for a
/// node inside its registration's own transaction, log-ordered before the controller's
/// `NameRegistered`. Upstream-legal on both legs: the resolver authorises record writes on the
/// registry's owner of the node, never on the registrar's lease, so an expired name whose registry
/// record still names the previous registrant emits record events before any registration the
/// interpreter has seen (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114-L129
/// @ ens_v1@91c966f); and the controller itself runs resolver writes inside `register` before its
/// own event (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L301-L341
/// @ ens_v1@91c966f). Stays one action — and therefore one transaction — at stage::REGISTER, so
/// the repair still orders it before everything the registration enables.
fn burst_around_registration(
    onboarding: &mut Action,
    wires: &Wires<'_>,
    node: B256,
    first: Address,
    second: Address,
) {
    let burst = |a: Address, phase: BurstPhase| {
        let mut emission = emission(
            wires.resolver,
            V1Resolver::AddrChanged { node, a }.encode_log_data(),
        );
        emission.burst = Some(phase);
        emission
    };
    // The controller's event is the last emission of every registration path, so popping and
    // re-pushing it keeps the registry setup between the two burst writes.
    let controller = onboarding.emissions.pop().expect("registration emits");
    onboarding
        .emissions
        .insert(0, burst(first, BurstPhase::PreOwnership));
    onboarding
        .emissions
        .push(burst(second, BurstPhase::RetargetWindow));
    onboarding.emissions.push(controller);
}

#[allow(clippy::too_many_arguments)]
fn registration(
    wires: &Wires<'_>,
    path: RegistrationPath,
    label: &str,
    hash: B256,
    eth_node: B256,
    registrant: Address,
    wrapper: Address,
    expires: i64,
    stage: u8,
) -> Action {
    let expires = U256::from(u64::try_from(expires).expect("expiry fits u64"));
    let mut emissions = match path {
        RegistrationPath::Legacy => vec![
            emission(
                wires.registry,
                V1Registry::NewOwner {
                    node: eth_node,
                    label: hash,
                    owner: registrant,
                }
                .encode_log_data(),
            ),
            emission(
                wires.legacy_controller,
                V1LegacyController::NameRegistered {
                    name: label.to_owned(),
                    label: hash,
                    owner: registrant,
                    cost: U256::from(1_u64),
                    expires,
                }
                .encode_log_data(),
            ),
        ],
        RegistrationPath::Wrapped => vec![
            // The registrar mints to the wrapper in the same transaction, then the registry names
            // the wrapper as owner (upstream:
            // .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @
            // ens_v1@91c966f). Without the mint the wrapped path had no token lineage at all.
            emission(
                wires.registrar,
                V1RegistrarToken::Transfer {
                    from: Address::ZERO,
                    to: wrapper,
                    tokenId: U256::from_be_bytes(hash.0),
                }
                .encode_log_data(),
            ),
            emission(
                wires.registry,
                V1Registry::NewOwner {
                    node: eth_node,
                    label: hash,
                    owner: wrapper,
                }
                .encode_log_data(),
            ),
            emission(
                wires.wrapped_controller,
                V1WrappedController::NameRegistered {
                    name: label.to_owned(),
                    label: hash,
                    owner: registrant,
                    baseCost: U256::from(1_u64),
                    premium: U256::ZERO,
                    expires,
                }
                .encode_log_data(),
            ),
        ],
        RegistrationPath::Unwrapped => vec![
            emission(
                wires.registry,
                V1Registry::NewOwner {
                    node: eth_node,
                    label: hash,
                    owner: registrant,
                }
                .encode_log_data(),
            ),
            emission(
                wires.unwrapped_controller,
                V1UnwrappedController::NameRegistered {
                    name: label.to_owned(),
                    label: hash,
                    owner: registrant,
                    baseCost: U256::from(1_u64),
                    premium: U256::ZERO,
                    expires,
                    referrer: B256::ZERO,
                }
                .encode_log_data(),
            ),
        ],
    };
    let registrar_owner = if path == RegistrationPath::Wrapped {
        wrapper
    } else {
        registrant
    };
    let controller = emissions
        .pop()
        .expect("registration emits controller event");
    emissions.push(emission(
        wires.registrar,
        V1BaseRegistrar::NameRegistered {
            id: U256::from_be_bytes(hash.0),
            owner: registrar_owner,
            expires,
        }
        .encode_log_data(),
    ));
    emissions.push(controller);
    action(format!("{label}:register-{path:?}"), stage, emissions)
}

fn renewal(
    wires: &Wires<'_>,
    path: RegistrationPath,
    label: &str,
    hash: B256,
    expires: i64,
) -> Action {
    let expires = U256::from(u64::try_from(expires).expect("expiry fits u64"));
    let controller = match path {
        RegistrationPath::Unwrapped => emission(
            wires.unwrapped_controller,
            V1UnwrappedController::NameRenewed {
                name: label.to_owned(),
                label: hash,
                cost: U256::from(1_u64),
                expires,
                referrer: B256::ZERO,
            }
            .encode_log_data(),
        ),
        // The registrar manifest admits this same four-argument renewal on the wrapped controller
        // role as well, so a wrapped registration renewing through the legacy controller would
        // leave the wrapped-controller admission path ungenerated.
        RegistrationPath::Wrapped => emission(
            wires.wrapped_controller,
            V1LegacyController::NameRenewed {
                name: label.to_owned(),
                label: hash,
                cost: U256::from(1_u64),
                expires,
            }
            .encode_log_data(),
        ),
        _ => emission(
            wires.legacy_controller,
            V1LegacyController::NameRenewed {
                name: label.to_owned(),
                label: hash,
                cost: U256::from(1_u64),
                expires,
            }
            .encode_log_data(),
        ),
    };
    action(
        format!("{label}:renew"),
        stage::WRITE,
        vec![
            emission(
                wires.registrar,
                V1BaseRegistrar::NameRenewed {
                    id: U256::from_be_bytes(hash.0),
                    expires,
                }
                .encode_log_data(),
            ),
            controller,
        ],
    )
}

fn subnode(
    wires: &Wires<'_>,
    label: &str,
    child: &str,
    parent: B256,
    owner: Address,
    stage: u8,
) -> Action {
    action(
        format!("{label}:subnode-{child}"),
        stage,
        vec![emission(
            wires.registry,
            V1Registry::NewOwner {
                node: parent,
                label: labelhash(child),
                owner,
            }
            .encode_log_data(),
        )],
    )
}

/// The fuses and wrapper expiry a wrapped subname can carry, given its parent's state. Burning a
/// parent-controlled fuse on a subname reverts unless the parent has burned CANNOT_UNWRAP
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L968-L975 @ ens_v1@91c966f). Expiry is
/// not gated the same way — it is only clamped to the parent's maximum (upstream:
/// .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L978-L994 @ ens_v1@91c966f) — so pairing the two
/// here is this lane's choice of a realistic locked-parent shape, not an upstream requirement.
/// Wrapping under any other parent goes through the plain wrap path, which burns no fuses and sets
/// no expiry (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L374 @ ens_v1@91c966f).
fn child_wrap(wrap_state: WrapState, expires: i64) -> (u32, u64) {
    match wrap_state {
        WrapState::WrappedLocked => (
            PARENT_CANNOT_CONTROL,
            u64::try_from(expires).expect("expiry fits u64"),
        ),
        _ => (0, 0),
    }
}

fn expiry(window: ExpiryWindow, settle_timestamp: i64) -> i64 {
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
    format!("0x{:040x}", 0xf000_0000_u64 + index)
        .parse()
        .expect("actor address is well formed")
}
