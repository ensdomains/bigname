//! A wrapped `.eth` name whose resolver was set through `NameWrapper.setResolver` in one
//! registration lifetime, then registered again after expiry and grace with a new resolver: the
//! later resolver write replaces the earlier one, live and restored.
use super::node_record_events::{declared_address, profile};
use super::v1_pre_surface_resolver::compact_prior;
use super::*;

mod events {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
        event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
        event NameUnwrapped(bytes32 indexed node, address owner);
    }

    pub mod registry {
        alloy_sol_types::sol! {
            event Transfer(bytes32 indexed node, address owner);
        }
    }
}

const FIRST_OWNER: &str = "0x00000000000000000000000000000000000000a3";
const SECOND_OWNER: &str = "0x00000000000000000000000000000000000000a4";
const CONTROLLER: &str = "0x00000000000000000000000000000000000000c1";
const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a1";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b1";
const GRACE_PERIOD: i64 = 90 * 24 * 60 * 60;
/// `PARENT_CANNOT_CONTROL | IS_DOT_ETH`.
const DOT_ETH_FUSES: u32 = 0x10000 | 0x20000;
const REGISTERED: i64 = 4_000_000;
const FIRST_EXPIRY: i64 = REGISTERED + 1_000;
const SET_RESOLVER: i64 = REGISTERED + 10;
const REREGISTERED: i64 = FIRST_EXPIRY + GRACE_PERIOD + 100;
const SECOND_EXPIRY: i64 = REREGISTERED + 1_000_000;
const CLEARED: i64 = REREGISTERED + 10;
const UNWRAPPED: i64 = REREGISTERED + 20;
const REWRAPPED: i64 = REREGISTERED + 30;

struct Contracts {
    registry: String,
    registrar: String,
    wrapper: String,
}

impl Contracts {
    fn new(admissions: &[AddressAdmissionInput]) -> Self {
        Self {
            registry: declared_address(admissions, "registry"),
            registrar: declared_address(admissions, "registrar"),
            wrapper: declared_address(admissions, "name_wrapper"),
        }
    }
}

fn label() -> (B256, U256, B256) {
    let labelhash = keccak256(b"relinked");
    let node = super::common::namehash(&["relinked".to_owned(), "eth".to_owned()])
        .parse()
        .expect("namehash");
    (labelhash, U256::from_be_bytes(labelhash.0), node)
}

fn address(value: &str) -> Address {
    value.parse().expect("address")
}

fn new_resolver(contracts: &Contracts, block: i64, log_index: i64, resolver: &str) -> RawLogInput {
    raw_at(
        events::NewResolver {
            node: label().2,
            resolver: address(resolver),
        }
        .encode_log_data(),
        block,
        log_index,
        &contracts.registry,
    )
}

fn wrapper_mint(
    contracts: &Contracts,
    block: i64,
    log_index: i64,
    owner: &str,
    expiry: i64,
) -> [RawLogInput; 2] {
    let node = label().2;
    [
        raw_at(
            events::TransferSingle {
                operator: address(CONTROLLER),
                from: Address::ZERO,
                to: address(owner),
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            block,
            log_index,
            &contracts.wrapper,
        ),
        raw_at(
            events::NameWrapped {
                node,
                name: b"\x08relinked\x03eth\0".to_vec().into(),
                owner: address(owner),
                fuses: DOT_ETH_FUSES,
                expiry: u64::try_from(expiry + GRACE_PERIOD).expect("expiry"),
            }
            .encode_log_data(),
            block,
            log_index + 1,
            &contracts.wrapper,
        ),
    ]
}

/// `registerAndWrapETH2LD`: the BaseRegistrar burns an expired token, mints to the NameWrapper,
/// names it the registry owner and emits `NameRegistered`; `_mint` burns the expired ERC-1155
/// token with `NameUnwrapped(node, 0)`, mints the new one, emits `NameWrapped`, then sets a
/// non-zero resolver.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L143-L150 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L892 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1020 @ ens_v1@91c966f)
fn register_and_wrap(
    contracts: &Contracts,
    block: i64,
    owner: &str,
    expiry: i64,
    previous_owner: Option<&str>,
    resolver: Option<&str>,
) -> Vec<RawLogInput> {
    let (labelhash, token, node) = label();
    let wrapper = address(&contracts.wrapper);
    let mut logs = Vec::new();
    if previous_owner.is_some() {
        logs.push(raw_at(
            events::Transfer {
                from: wrapper,
                to: Address::ZERO,
                tokenId: token,
            }
            .encode_log_data(),
            block,
            0,
            &contracts.registrar,
        ));
    }
    logs.push(raw_at(
        events::Transfer {
            from: Address::ZERO,
            to: wrapper,
            tokenId: token,
        }
        .encode_log_data(),
        block,
        1,
        &contracts.registrar,
    ));
    logs.push(raw_at(
        events::NewOwner {
            node: super::common::namehash(&["eth".to_owned()])
                .parse()
                .expect("eth node"),
            label: labelhash,
            owner: wrapper,
        }
        .encode_log_data(),
        block,
        2,
        &contracts.registry,
    ));
    logs.push(raw_at(
        events::NameRegistered {
            id: token,
            owner: wrapper,
            expires: U256::from(expiry),
        }
        .encode_log_data(),
        block,
        3,
        &contracts.registrar,
    ));
    if let Some(previous_owner) = previous_owner {
        logs.push(raw_at(
            events::TransferSingle {
                operator: address(CONTROLLER),
                from: address(previous_owner),
                to: Address::ZERO,
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            block,
            4,
            &contracts.wrapper,
        ));
        logs.push(raw_at(
            events::NameUnwrapped {
                node,
                owner: Address::ZERO,
            }
            .encode_log_data(),
            block,
            5,
            &contracts.wrapper,
        ));
    }
    logs.extend(wrapper_mint(contracts, block, 6, owner, expiry));
    if let Some(resolver) = resolver {
        logs.push(new_resolver(contracts, block, 8, resolver));
    }
    logs
}

/// `unwrapETH2LD(labelhash, owner, owner)`: `_unwrap` burns the token, sets the registry owner
/// and emits `NameUnwrapped`, then the registrar token moves to the registrant.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
fn unwrap(contracts: &Contracts, block: i64, owner: &str) -> Vec<RawLogInput> {
    let (_, token, node) = label();
    vec![
        raw_at(
            events::TransferSingle {
                operator: address(owner),
                from: address(owner),
                to: Address::ZERO,
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            block,
            0,
            &contracts.wrapper,
        ),
        raw_at(
            events::registry::Transfer {
                node,
                owner: address(owner),
            }
            .encode_log_data(),
            block,
            1,
            &contracts.registry,
        ),
        raw_at(
            events::NameUnwrapped {
                node,
                owner: address(owner),
            }
            .encode_log_data(),
            block,
            2,
            &contracts.wrapper,
        ),
        raw_at(
            events::Transfer {
                from: address(&contracts.wrapper),
                to: address(owner),
                tokenId: token,
            }
            .encode_log_data(),
            block,
            3,
            &contracts.registrar,
        ),
    ]
}

/// `wrapETH2LD(label, owner, 0, address(0))`: the registrar token moves to the NameWrapper,
/// `reclaim` names it the registry owner, and `_wrapETH2LD` writes no resolver.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
fn rewrap(contracts: &Contracts, block: i64, owner: &str, expiry: i64) -> Vec<RawLogInput> {
    let (labelhash, token, _) = label();
    let wrapper = address(&contracts.wrapper);
    let mut logs = vec![
        raw_at(
            events::Transfer {
                from: address(owner),
                to: wrapper,
                tokenId: token,
            }
            .encode_log_data(),
            block,
            0,
            &contracts.registrar,
        ),
        raw_at(
            events::NewOwner {
                node: super::common::namehash(&["eth".to_owned()])
                    .parse()
                    .expect("eth node"),
                label: labelhash,
                owner: wrapper,
            }
            .encode_log_data(),
            block,
            1,
            &contracts.registry,
        ),
    ];
    logs.extend(wrapper_mint(contracts, block, 2, owner, expiry));
    logs
}

fn batch(
    chain: &str,
    manifests: &[ManifestInput],
    admissions: &[AddressAdmissionInput],
    prior_events: Vec<PriorEventInput>,
    mut raw_logs: Vec<RawLogInput>,
) -> BatchInput {
    for log in &mut raw_logs {
        log.chain_id = chain.to_owned();
    }
    BatchInput {
        chain_id: chain.to_owned(),
        manifests: manifests.to_vec(),
        discovery_rules: Vec::new(),
        admissions: admissions.to_vec(),
        prior_events,
        blocks: Vec::new(),
        raw_logs,
    }
}

/// `NameWrapper.setResolver` writes resolver A outside any registration, so its registry-read
/// copy stays on the registry-read resource. After expiry and grace the name is registered
/// again, wrapped, with resolver B, whose rows land on the new wrapper and registrar resources.
/// B replaces A, a later clear removes it, and an unwrap plus a zero-resolver rewrap brings
/// nothing back; every step agrees with a restore from all rows and from compacted rows.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666-L671 @ ens_v1@91c966f)
#[test]
fn reregistered_resolver_replaces_a_prior_lifetime_registry_read_link() -> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let contracts = Contracts::new(&admissions);
    let node = super::common::namehash(&["relinked".to_owned(), "eth".to_owned()]);
    let steps: Vec<(Vec<RawLogInput>, Option<&str>)> = vec![
        (
            register_and_wrap(
                &contracts,
                REGISTERED,
                FIRST_OWNER,
                FIRST_EXPIRY,
                None,
                None,
            ),
            None,
        ),
        (
            vec![new_resolver(&contracts, SET_RESOLVER, 0, RESOLVER_A)],
            Some(RESOLVER_A),
        ),
        (
            register_and_wrap(
                &contracts,
                REREGISTERED,
                SECOND_OWNER,
                SECOND_EXPIRY,
                Some(FIRST_OWNER),
                Some(RESOLVER_B),
            ),
            Some(RESOLVER_B),
        ),
        (
            vec![new_resolver(&contracts, CLEARED, 0, ZERO_ADDRESS)],
            None,
        ),
        (unwrap(&contracts, UNWRAPPED, SECOND_OWNER), None),
        (
            rewrap(&contracts, REWRAPPED, SECOND_OWNER, SECOND_EXPIRY),
            None,
        ),
    ];
    let link = |session: &AdapterSession| {
        session
            .v1_resolver_link("ens", &node)
            .map(|link| link.resolver_address.to_ascii_lowercase())
    };
    let mut session = None;
    let mut events = Vec::new();
    let mut all_logs = Vec::new();
    for (index, (logs, expected)) in steps.into_iter().enumerate() {
        all_logs.extend(logs.clone());
        let (output, live) = interpret_test_batch_incremental(
            batch(&chain, &manifests, &admissions, Vec::new(), logs),
            session,
        )?;
        events.extend(output.normalized_events);
        assert_eq!(
            link(&live).as_deref(),
            expected,
            "live link after step {index}"
        );
        for (form, prior) in [
            (
                "all rows",
                events.iter().map(prior_event).collect::<Vec<_>>(),
            ),
            ("compacted rows", compact_prior(&events)),
        ] {
            let (_, restored) = interpret_test_batch_incremental(
                batch(&chain, &manifests, &admissions, prior, Vec::new()),
                None,
            )?;
            assert_eq!(
                link(&restored),
                link(&live),
                "restored from {form} after step {index}"
            );
            assert_eq!(
                restored, live,
                "restored session from {form} after step {index}"
            );
        }
        session = Some(live);
    }

    let stale = events
        .iter()
        .filter(|event| event.block_number.is_some_and(|block| block > REREGISTERED))
        .filter(|event| {
            event.after_state["resolver"]
                .as_str()
                .or_else(|| event.after_state["scope"]["resolver_address"].as_str())
                .is_some_and(|resolver| resolver.eq_ignore_ascii_case(RESOLVER_A))
        })
        .map(|event| {
            (
                event.block_number,
                event.event_kind.clone(),
                event.resource_id,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        stale,
        Vec::new(),
        "resolver A reappears after resolver B replaced it"
    );

    let one_shot =
        interpret_test_batch(batch(&chain, &manifests, &admissions, Vec::new(), all_logs))?;
    let identities = |events: &[NormalizedEvent]| {
        events
            .iter()
            .map(|event| {
                (
                    event.event_identity.clone(),
                    event.resource_id,
                    event.after_state.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(identities(&one_shot.normalized_events), identities(&events));
    Ok(())
}

const FIRST_UNWRAPPED: i64 = REGISTERED + 20;

/// `registerAndWrapETH2LD` in transaction `transaction` of `block`, for a name whose previous
/// registration was unwrapped: the BaseRegistrar burns the expired token from its registrant,
/// mints to the NameWrapper, names it the registry owner and emits `NameRegistered`; `_mint`
/// finds no old ERC-1155 token, mints the new one, emits `NameWrapped`, then sets the resolver.
/// Log indexes run on from `first_log`, as they do across one block.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L143-L150 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L892 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1019 @ ens_v1@91c966f)
#[allow(clippy::too_many_arguments)]
fn register_and_wrap_after_unwrap(
    contracts: &Contracts,
    block: i64,
    transaction: i64,
    first_log: i64,
    previous_registrant: &str,
    owner: &str,
    expiry: i64,
    resolver: &str,
) -> Vec<RawLogInput> {
    let (labelhash, token, node) = label();
    let wrapper = address(&contracts.wrapper);
    let at = |encoded, offset: i64, emitter: &str| {
        raw_at_transaction(encoded, block, transaction, first_log + offset, emitter)
    };
    vec![
        at(
            events::Transfer {
                from: address(previous_registrant),
                to: Address::ZERO,
                tokenId: token,
            }
            .encode_log_data(),
            0,
            &contracts.registrar,
        ),
        at(
            events::Transfer {
                from: Address::ZERO,
                to: wrapper,
                tokenId: token,
            }
            .encode_log_data(),
            1,
            &contracts.registrar,
        ),
        at(
            events::NewOwner {
                node: super::common::namehash(&["eth".to_owned()])
                    .parse()
                    .expect("eth node"),
                label: labelhash,
                owner: wrapper,
            }
            .encode_log_data(),
            2,
            &contracts.registry,
        ),
        at(
            events::NameRegistered {
                id: token,
                owner: wrapper,
                expires: U256::from(expiry),
            }
            .encode_log_data(),
            3,
            &contracts.registrar,
        ),
        at(
            events::TransferSingle {
                operator: address(CONTROLLER),
                from: Address::ZERO,
                to: address(owner),
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            4,
            &contracts.wrapper,
        ),
        at(
            events::NameWrapped {
                node,
                name: b"\x08relinked\x03eth\0".to_vec().into(),
                owner: address(owner),
                fuses: DOT_ETH_FUSES,
                expiry: u64::try_from(expiry + GRACE_PERIOD).expect("expiry"),
            }
            .encode_log_data(),
            5,
            &contracts.wrapper,
        ),
        at(
            events::NewResolver {
                node,
                resolver: address(resolver),
            }
            .encode_log_data(),
            6,
            &contracts.registry,
        ),
    ]
}

/// The compacted rows, as `compact_prior` keeps them, with the rows of each block restored in
/// reverse: a block's stored rows come back in no fixed order.
fn compact_prior_reversed_within_blocks(events: &[NormalizedEvent]) -> Vec<PriorEventInput> {
    let prior = events
        .iter()
        .map(|event| (event.block_number, prior_event(event)))
        .collect::<Vec<_>>();
    let mut last_index = std::collections::HashMap::new();
    for (index, (_, event)) in prior.iter().enumerate() {
        last_index.insert(event.retained_state_key.clone(), index);
    }
    let mut kept = prior
        .into_iter()
        .enumerate()
        .filter(|(index, (_, event))| last_index[&event.retained_state_key] == *index)
        .map(|(_, row)| row)
        .collect::<Vec<_>>();
    kept.reverse();
    kept.sort_by_key(|(block, _)| *block);
    kept.into_iter().map(|(_, event)| event).collect()
}

/// A `.eth` name unwrapped during its first registration keeps an ordinary registry owner, who
/// may still call `ENSRegistry.setResolver` after expiry, since registry authorisation checks
/// only the recorded owner. In one block, that owner sets resolver A in transaction 0 and a
/// controller registers the name again, wrapped, with resolver B in transaction 1. A's row stays
/// on the registry-read resource while B's land on the new wrapper and registrar resources, all
/// with the block's timestamp. B is the later write, so it is selected live and after restoring
/// all rows, compacted rows, and compacted rows in reverse within each block; a later unwrap
/// and zero-resolver rewrap leave B, since the registry still holds it.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L20 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L95 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1019 @ ens_v1@91c966f)
#[test]
fn same_block_wrapped_reregistration_replaces_an_earlier_registry_write() -> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let contracts = Contracts::new(&admissions);
    let node = super::common::namehash(&["relinked".to_owned(), "eth".to_owned()]);
    let mut reregistration = vec![raw_at_transaction(
        events::NewResolver {
            node: label().2,
            resolver: address(RESOLVER_A),
        }
        .encode_log_data(),
        REREGISTERED,
        0,
        0,
        &contracts.registry,
    )];
    reregistration.extend(register_and_wrap_after_unwrap(
        &contracts,
        REREGISTERED,
        1,
        1,
        FIRST_OWNER,
        SECOND_OWNER,
        SECOND_EXPIRY,
        RESOLVER_B,
    ));
    let steps: Vec<(Vec<RawLogInput>, Option<&str>)> = vec![
        (
            register_and_wrap(
                &contracts,
                REGISTERED,
                FIRST_OWNER,
                FIRST_EXPIRY,
                None,
                None,
            ),
            None,
        ),
        (unwrap(&contracts, FIRST_UNWRAPPED, FIRST_OWNER), None),
        (reregistration, Some(RESOLVER_B)),
        (
            unwrap(&contracts, UNWRAPPED, SECOND_OWNER),
            Some(RESOLVER_B),
        ),
        (
            rewrap(&contracts, REWRAPPED, SECOND_OWNER, SECOND_EXPIRY),
            Some(RESOLVER_B),
        ),
    ];
    let link = |session: &AdapterSession| {
        session
            .v1_resolver_link("ens", &node)
            .map(|link| link.resolver_address.to_ascii_lowercase())
    };
    let mut session = None;
    let mut events = Vec::new();
    for (index, (logs, expected)) in steps.into_iter().enumerate() {
        let (output, live) = interpret_test_batch_incremental(
            batch(&chain, &manifests, &admissions, Vec::new(), logs),
            session,
        )?;
        events.extend(output.normalized_events);
        assert_eq!(
            link(&live).as_deref(),
            expected,
            "live link after step {index}"
        );
        for (form, prior) in [
            (
                "all rows",
                events.iter().map(prior_event).collect::<Vec<_>>(),
            ),
            ("compacted rows", compact_prior(&events)),
            (
                "compacted rows reversed within blocks",
                compact_prior_reversed_within_blocks(&events),
            ),
        ] {
            let (_, restored) = interpret_test_batch_incremental(
                batch(&chain, &manifests, &admissions, prior, Vec::new()),
                None,
            )?;
            assert_eq!(
                link(&restored).as_deref(),
                expected,
                "restored from {form} after step {index}"
            );
            // Other retained state follows production row order within a block, so the reversed
            // form checks only the resolver link.
            if !form.contains("reversed") {
                assert_eq!(
                    restored, live,
                    "restored session from {form} after step {index}"
                );
            }
        }
        session = Some(live);
    }

    let stale = events
        .iter()
        .filter(|event| event.block_number.is_some_and(|block| block > REREGISTERED))
        .filter(|event| {
            event.after_state["resolver"]
                .as_str()
                .or_else(|| event.after_state["scope"]["resolver_address"].as_str())
                .is_some_and(|resolver| resolver.eq_ignore_ascii_case(RESOLVER_A))
        })
        .map(|event| {
            (
                event.block_number,
                event.event_kind.clone(),
                event.resource_id,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        stale,
        Vec::new(),
        "resolver A reappears after resolver B replaced it"
    );
    Ok(())
}
