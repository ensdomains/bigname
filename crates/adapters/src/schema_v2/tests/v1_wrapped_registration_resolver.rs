//! A `.eth` name registered through the Sepolia wrapped controller with a resolver: the registry
//! `NewResolver` written after `NameWrapped` in the same transaction belongs to the wrapper
//! resource that `NameWrapped` bound, not to the registrar resource the NameWrapper holds.
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
}

const OWNER: &str = "0x00000000000000000000000000000000000000a3";
const RESOLVER: &str = "0x8fade66b79cc9f707ab26799354482eb93a5b7dd";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b4";
const BLOCK: i64 = 4_052_977;
const REGISTRAR_EXPIRY: u64 = 1_900_000_000;
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;
/// `PARENT_CANNOT_CONTROL | IS_DOT_ETH`.
const DOT_ETH_FUSES: u32 = 0x10000 | 0x20000;

/// The wrapped controller's `register` calls `NameWrapper.registerAndWrapETH2LD`: the
/// BaseRegistrar mints to the NameWrapper, names it the registry owner and emits `NameRegistered`;
/// `_wrapETH2LD` mints the ERC-1155 token, emits `NameWrapped`, then sets the registry resolver.
/// (upstream: .refs/basenames/lib/ens-contracts/contracts/ethregistrar/ETHRegistrarController.sol:L178-L184 @ basenames@1809bbc)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1019 @ ens_v1@91c966f)
fn registration_logs(
    chain: &str,
    admissions: &[AddressAdmissionInput],
) -> anyhow::Result<Vec<RawLogInput>> {
    let registry = declared_address(admissions, "registry");
    let registrar = declared_address(admissions, "registrar");
    let wrapper = declared_address(admissions, "name_wrapper");
    let label = b"taytems";
    let labelhash = keccak256(label);
    let node: B256 = super::common::namehash(&["taytems".to_owned(), "eth".to_owned()]).parse()?;
    let token = U256::from_be_bytes(labelhash.0);
    let wrapper_address: Address = wrapper.parse()?;
    let mut logs = vec![
        raw_at(
            events::Transfer {
                from: Address::ZERO,
                to: wrapper_address,
                tokenId: token,
            }
            .encode_log_data(),
            BLOCK,
            0,
            &registrar,
        ),
        raw_at(
            events::NewOwner {
                node: super::common::namehash(&["eth".to_owned()]).parse()?,
                label: labelhash,
                owner: wrapper_address,
            }
            .encode_log_data(),
            BLOCK,
            1,
            &registry,
        ),
        raw_at(
            events::NameRegistered {
                id: token,
                owner: wrapper_address,
                expires: U256::from(REGISTRAR_EXPIRY),
            }
            .encode_log_data(),
            BLOCK,
            2,
            &registrar,
        ),
        raw_at(
            events::TransferSingle {
                operator: wrapper_address,
                from: Address::ZERO,
                to: OWNER.parse()?,
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            BLOCK,
            3,
            &wrapper,
        ),
        raw_at(
            events::NameWrapped {
                node,
                name: b"\x07taytems\x03eth\0".to_vec().into(),
                owner: OWNER.parse()?,
                fuses: DOT_ETH_FUSES,
                expiry: REGISTRAR_EXPIRY + GRACE_PERIOD,
            }
            .encode_log_data(),
            BLOCK,
            4,
            &wrapper,
        ),
        raw_at(
            events::NewResolver {
                node,
                resolver: RESOLVER.parse()?,
            }
            .encode_log_data(),
            BLOCK,
            5,
            &registry,
        ),
    ];
    for log in &mut logs {
        log.chain_id = chain.to_owned();
    }
    Ok(logs)
}

/// taytems.eth on Sepolia (block 4052977): `NameWrapped` at log 21, registry `NewResolver` at log
/// 22. The resolver pointer must land on the wrapper resource the name is served from; its
/// registry-read copy moves with the registration as before, and the selected resolver is the
/// same live and restored.
#[test]
fn registry_resolver_set_after_wrapping_stays_on_the_wrapper_resource() -> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let raw_logs = registration_logs(&chain, &admissions)?;
    let (output, live) = interpret_test_batch_incremental(
        sepolia_input(&chain, &manifests, &admissions, Vec::new(), raw_logs),
        None,
    )?;
    let resource_of = |kind: &str, family: &str| {
        output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == kind && event.source_family == family)
            .and_then(|event| event.resource_id)
    };
    let wrapper = resource_of("SurfaceBound", "ens_v1_wrapper_l1").expect("wrapper resource");
    let registrar =
        resource_of("RegistrationGranted", "ens_v1_registrar_l1").expect("registrar resource");
    assert_ne!(wrapper, registrar);
    let mut rows = output
        .normalized_events
        .iter()
        .filter(|event| event.log_index == Some(5))
        .map(|event| {
            (
                event.event_kind.as_str(),
                event.resource_id,
                event.after_state["resolver"]
                    .as_str()
                    .or_else(|| event.after_state["scope"]["resolver_address"].as_str()),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    let mut expected = vec![
        ("PermissionChanged", Some(wrapper), Some(RESOLVER)),
        ("ResolverChanged", Some(wrapper), Some(RESOLVER)),
        ("ResolverChanged", Some(registrar), Some(RESOLVER)),
    ];
    expected.sort();
    assert_eq!(rows, expected, "wrapper {wrapper}, registrar {registrar}");

    let node = super::common::namehash(&["taytems".to_owned(), "eth".to_owned()]);
    let link = |session: &AdapterSession| {
        session
            .v1_resolver_link("ens", &node)
            .map(|link| link.resolver_address.to_ascii_lowercase())
    };
    assert_eq!(link(&live).as_deref(), Some(RESOLVER));
    for prior in [
        output.normalized_events.iter().map(prior_event).collect(),
        compact_prior(&output.normalized_events),
    ] {
        let (_, restored) = interpret_test_batch_incremental(
            sepolia_input(&chain, &manifests, &admissions, prior, Vec::new()),
            None,
        )?;
        assert_eq!(link(&restored).as_deref(), Some(RESOLVER));
    }
    Ok(())
}

fn sepolia_input(
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

/// An owner-controlled contract registers `taytems.eth` wrapped to itself with resolver A, then
/// in the same transaction calls `unwrapETH2LD(labelhash, itself, NameWrapper)`, reclaims the
/// registry node through the BaseRegistrar and sets resolver `replacement` through the registry.
/// `unwrapETH2LD` rejects only a registrant equal to the NameWrapper; `_unwrap` burns the token,
/// writes the registry owner, emits `NameUnwrapped`, then the registrar token moves.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
fn unwrap_reclaim_and_replace_logs(
    admissions: &[AddressAdmissionInput],
    replacement: &str,
) -> anyhow::Result<Vec<RawLogInput>> {
    let registry = declared_address(admissions, "registry");
    let registrar = declared_address(admissions, "registrar");
    let wrapper = declared_address(admissions, "name_wrapper");
    let wrapper_address: Address = wrapper.parse()?;
    let owner: Address = OWNER.parse()?;
    let labelhash = keccak256(b"taytems");
    let node: B256 = super::common::namehash(&["taytems".to_owned(), "eth".to_owned()]).parse()?;
    Ok(vec![
        raw_at(
            events::TransferSingle {
                operator: owner,
                from: owner,
                to: Address::ZERO,
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            BLOCK,
            6,
            &wrapper,
        ),
        raw_at(
            v1_registry::Transfer {
                node,
                owner: wrapper_address,
            }
            .encode_log_data(),
            BLOCK,
            7,
            &registry,
        ),
        raw_at(
            events::NameUnwrapped {
                node,
                owner: wrapper_address,
            }
            .encode_log_data(),
            BLOCK,
            8,
            &wrapper,
        ),
        raw_at(
            events::Transfer {
                from: wrapper_address,
                to: owner,
                tokenId: U256::from_be_bytes(labelhash.0),
            }
            .encode_log_data(),
            BLOCK,
            9,
            &registrar,
        ),
        raw_at(
            events::NewOwner {
                node: super::common::namehash(&["eth".to_owned()]).parse()?,
                label: labelhash,
                owner,
            }
            .encode_log_data(),
            BLOCK,
            10,
            &registry,
        ),
        raw_at(
            events::NewResolver {
                node,
                resolver: replacement.parse()?,
            }
            .encode_log_data(),
            BLOCK,
            11,
            &registry,
        ),
    ])
}

/// A later `wrapETH2LD(label, owner, 0, address(0))` from the same contract: the registrar token
/// moves to the NameWrapper, the registry node is reclaimed for it, and `NameWrapped` follows
/// with no resolver write.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
fn rewrap_logs(admissions: &[AddressAdmissionInput]) -> anyhow::Result<Vec<RawLogInput>> {
    let registry = declared_address(admissions, "registry");
    let registrar = declared_address(admissions, "registrar");
    let wrapper = declared_address(admissions, "name_wrapper");
    let wrapper_address: Address = wrapper.parse()?;
    let owner: Address = OWNER.parse()?;
    let labelhash = keccak256(b"taytems");
    let node: B256 = super::common::namehash(&["taytems".to_owned(), "eth".to_owned()]).parse()?;
    let block = BLOCK + 1;
    Ok(vec![
        raw_at(
            events::Transfer {
                from: owner,
                to: wrapper_address,
                tokenId: U256::from_be_bytes(labelhash.0),
            }
            .encode_log_data(),
            block,
            0,
            &registrar,
        ),
        raw_at(
            events::NewOwner {
                node: super::common::namehash(&["eth".to_owned()]).parse()?,
                label: labelhash,
                owner: wrapper_address,
            }
            .encode_log_data(),
            block,
            1,
            &registry,
        ),
        raw_at(
            events::TransferSingle {
                operator: owner,
                from: Address::ZERO,
                to: owner,
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            block,
            2,
            &wrapper,
        ),
        raw_at(
            events::NameWrapped {
                node,
                name: b"\x07taytems\x03eth\0".to_vec().into(),
                owner,
                fuses: DOT_ETH_FUSES,
                expiry: REGISTRAR_EXPIRY + GRACE_PERIOD,
            }
            .encode_log_data(),
            block,
            3,
            &wrapper,
        ),
    ])
}

/// A resolver write after a same-transaction unwrap and reclaim must replace the resolver set
/// while the name was wrapped, both live and when state is restored from the retained rows, and
/// a later wrap without a resolver must not bring the earlier resolver back.
fn assert_replacement_survives_restore(replacement: &str) -> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let node = super::common::namehash(&["taytems".to_owned(), "eth".to_owned()]);
    let expected = (replacement != ZERO_ADDRESS).then(|| replacement.to_owned());
    let mut first = registration_logs(&chain, &admissions)?;
    first.extend(unwrap_reclaim_and_replace_logs(&admissions, replacement)?);
    let (registered, live) = interpret_test_batch_incremental(
        sepolia_input(&chain, &manifests, &admissions, Vec::new(), first),
        None,
    )?;
    let link = |session: &AdapterSession| {
        session
            .v1_resolver_link("ens", &node)
            .map(|link| link.resolver_address.to_ascii_lowercase())
    };
    assert_eq!(link(&live), expected, "live resolver after the replacement");
    let mut sessions = vec![("live", live)];
    for (shape, prior) in [
        (
            "restored",
            registered
                .normalized_events
                .iter()
                .map(prior_event)
                .collect(),
        ),
        ("compacted", compact_prior(&registered.normalized_events)),
    ] {
        let (_, restored) = interpret_test_batch_incremental(
            sepolia_input(&chain, &manifests, &admissions, prior, Vec::new()),
            None,
        )?;
        assert_eq!(
            link(&restored),
            expected,
            "{shape} resolver after the replacement"
        );
        sessions.push((shape, restored));
    }
    for (shape, session) in sessions {
        let (rewrapped, _) = interpret_test_batch_incremental(
            sepolia_input(
                &chain,
                &manifests,
                &admissions,
                Vec::new(),
                rewrap_logs(&admissions)?,
            ),
            Some(session),
        )?;
        let pointers = rewrapped
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "ResolverChanged")
            .map(|event| event.after_state["resolver"].clone())
            .collect::<Vec<_>>();
        assert!(
            pointers.iter().all(|resolver| *resolver != RESOLVER),
            "{shape}: the rewrap revived the resolver replaced while unwrapped: {pointers:?}"
        );
    }
    Ok(())
}

#[test]
fn resolver_replaced_after_same_transaction_unwrap_survives_restore() -> anyhow::Result<()> {
    assert_replacement_survives_restore(RESOLVER_B)
}

#[test]
fn resolver_cleared_after_same_transaction_unwrap_survives_restore() -> anyhow::Result<()> {
    assert_replacement_survives_restore(ZERO_ADDRESS)
}
