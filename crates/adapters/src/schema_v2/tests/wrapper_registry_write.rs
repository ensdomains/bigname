//! A current ENSv1 registry owner write over a wrapped name ends the NameWrapper authority unless
//! the NameWrapper's own `NameUnwrapped` for the node follows in the transaction or the new owner
//! is the NameWrapper.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L612-L660 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L892 @ ens_v1@91c966f)

use super::*;

const REGISTRY_MANIFEST: i64 = 7401;
const WRAPPER_MANIFEST: i64 = 7402;
const REGISTRY: &str = "0x00000000000000000000000000000000000000c1";
const NAME_WRAPPER: &str = "0x00000000000000000000000000000000000000c2";
const HOLDER: &str = "0x00000000000000000000000000000000000000a1";
const NEW_OWNER: &str = "0x00000000000000000000000000000000000000b1";
const NEXT_HOLDER: &str = "0x00000000000000000000000000000000000000b2";
const REWRAPPED_HOLDER: &str = "0x00000000000000000000000000000000000000d1";
const LAST_OWNER: &str = "0x00000000000000000000000000000000000000e1";

fn manifests() -> Vec<ManifestInput> {
    vec![
        manifest_with_events(
            REGISTRY_MANIFEST,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "NewOwner",
                    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                    &["registry"],
                    &["SubregistryChanged", "AuthorityTransferred"],
                ),
                (
                    "Transfer",
                    "event Transfer(bytes32 indexed node, address owner)",
                    &["registry"],
                    &["AuthorityTransferred"],
                ),
            ],
        ),
        manifest_with_events(
            WRAPPER_MANIFEST,
            "ens",
            "ens_v1_wrapper_l1",
            &[
                (
                    "NameWrapped",
                    "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                    &["name_wrapper"],
                    &[
                        "TokenControlTransferred",
                        "ExpiryChanged",
                        "PermissionScopeChanged",
                        "PermissionChanged",
                        "SurfaceBound",
                        "AuthorityEpochChanged",
                    ],
                ),
                (
                    "TransferSingle",
                    "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                    &["name_wrapper"],
                    &["TokenControlTransferred", "PermissionChanged"],
                ),
                (
                    "FusesSet",
                    "event FusesSet(bytes32 indexed node, uint32 fuses)",
                    &["name_wrapper"],
                    &["PermissionScopeChanged"],
                ),
                (
                    "NameUnwrapped",
                    "event NameUnwrapped(bytes32 indexed node, address owner)",
                    &["name_wrapper"],
                    &[
                        "SurfaceUnbound",
                        "AuthorityEpochChanged",
                        "PermissionChanged",
                    ],
                ),
            ],
        ),
    ]
}

fn admissions() -> Vec<AddressAdmissionInput> {
    let mut registry = admission(REGISTRY_MANIFEST, "registry");
    registry.address = REGISTRY.to_owned();
    let mut wrapper = admission(WRAPPER_MANIFEST, "name_wrapper");
    wrapper.address = NAME_WRAPPER.to_owned();
    vec![registry, wrapper]
}

fn interpret(raw_logs: Vec<RawLogInput>) -> anyhow::Result<Vec<NormalizedEvent>> {
    Ok(interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    })?
    .normalized_events)
}

fn parent() -> B256 {
    super::common::namehash(&["parent".to_owned(), "eth".to_owned()])
        .parse()
        .expect("namehash parses")
}

fn child() -> B256 {
    super::common::namehash(&["child".to_owned(), "parent".to_owned(), "eth".to_owned()])
        .parse()
        .expect("namehash parses")
}

fn addr(value: &str) -> Address {
    value.parse().expect("address literal parses")
}

fn parent_sets_owner(block: i64, log_index: i64, owner: &str) -> RawLogInput {
    raw_at(
        v1_registry::NewOwner {
            node: parent(),
            label: keccak256(b"child"),
            owner: addr(owner),
        }
        .encode_log_data(),
        block,
        log_index,
        REGISTRY,
    )
}

fn registry_transfer(block: i64, log_index: i64, owner: &str) -> RawLogInput {
    raw_at(
        v1_registry::Transfer {
            node: child(),
            owner: addr(owner),
        }
        .encode_log_data(),
        block,
        log_index,
        REGISTRY,
    )
}

fn name_wrapped(block: i64, log_index: i64, owner: &str) -> RawLogInput {
    raw_at(
        NameWrapped {
            node: child(),
            name: b"\x05child\x06parent\x03eth\0".to_vec().into(),
            owner: addr(owner),
            fuses: 0,
            expiry: 0,
        }
        .encode_log_data(),
        block,
        log_index,
        NAME_WRAPPER,
    )
}

fn name_unwrapped(block: i64, log_index: i64, owner: &str) -> RawLogInput {
    raw_at(
        NameUnwrapped {
            node: child(),
            owner: addr(owner),
        }
        .encode_log_data(),
        block,
        log_index,
        NAME_WRAPPER,
    )
}

fn fuses_set(block: i64, log_index: i64) -> RawLogInput {
    raw_at(
        FusesSet {
            node: child(),
            fuses: 0,
        }
        .encode_log_data(),
        block,
        log_index,
        NAME_WRAPPER,
    )
}

fn token_transfer(block: i64, log_index: i64, from: &str, to: &str) -> RawLogInput {
    raw_at(
        v2_registry::TransferSingle {
            operator: addr(from),
            from: addr(from),
            to: addr(to),
            id: U256::from_be_bytes(child().0),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        log_index,
        NAME_WRAPPER,
    )
}

/// The parent creates the child for HOLDER (block 1), who wraps it with the generic `wrap`
/// (block 2).
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L375 @ ens_v1@91c966f)
fn wrapped_child() -> Vec<RawLogInput> {
    vec![
        parent_sets_owner(1, 0, HOLDER),
        registry_transfer(2, 0, NAME_WRAPPER),
        name_wrapped(2, 1, HOLDER),
    ]
}

fn in_block(events: &[NormalizedEvent], block: i64) -> Vec<&NormalizedEvent> {
    events
        .iter()
        .filter(|event| event.block_number == Some(block))
        .collect()
}

fn find<'a>(
    events: &'a [NormalizedEvent],
    block: i64,
    log_index: i64,
    event_kind: &str,
) -> Option<&'a NormalizedEvent> {
    events.iter().find(|event| {
        event.block_number == Some(block)
            && event.log_index == Some(log_index)
            && event.event_kind == event_kind
    })
}

fn registry_only_resource(events: &[NormalizedEvent]) -> Uuid {
    find(events, 1, 0, "AuthorityTransferred")
        .and_then(|event| event.resource_id)
        .expect("the parent's first write anchors the registry-only resource")
}

fn wrapper_resource(events: &[NormalizedEvent]) -> Uuid {
    find(events, 2, 1, "TokenControlTransferred")
        .and_then(|event| event.resource_id)
        .expect("NameWrapped anchors the wrapper resource")
}

fn resource_control(
    events: &[NormalizedEvent],
    block: i64,
    resource: Uuid,
    subject: &str,
) -> Option<serde_json::Value> {
    in_block(events, block)
        .into_iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .filter(|event| event.resource_id == Some(resource))
        .filter(|event| event.after_state["subject"] == subject)
        .filter(|event| event.after_state["scope"]["kind"] == "resource")
        .map(|event| event.after_state["effective_powers"].clone())
        .next_back()
}

#[test]
fn parent_registry_write_over_a_wrapped_child_ends_the_wrapper_authority() -> anyhow::Result<()> {
    let mut logs = wrapped_child();
    logs.push(parent_sets_owner(3, 0, NEW_OWNER));
    logs.push(token_transfer(4, 0, HOLDER, NEXT_HOLDER));
    let events = interpret(logs)?;
    let registry_only = registry_only_resource(&events);
    let wrapper = wrapper_resource(&events);

    let transferred =
        find(&events, 3, 0, "AuthorityTransferred").expect("the parent's write moves the owner");
    assert_eq!(transferred.resource_id, Some(registry_only));
    assert_eq!(transferred.after_state["authority_kind"], "registry_only");
    assert_eq!(transferred.after_state["owner"], NEW_OWNER);
    assert_eq!(
        find(&events, 3, 0, "SurfaceUnbound").and_then(|event| event.resource_id),
        Some(wrapper)
    );
    assert_eq!(
        find(&events, 3, 0, "SurfaceBound").and_then(|event| event.resource_id),
        Some(registry_only)
    );
    assert_eq!(
        resource_control(&events, 3, wrapper, HOLDER),
        Some(json!([]))
    );
    assert_eq!(
        resource_control(&events, 3, registry_only, NEW_OWNER),
        Some(json!(["resource_control"]))
    );
    assert!(
        in_block(&events, 4).is_empty(),
        "the stale token's transfer moves nothing: {:#?}",
        in_block(&events, 4)
    );
    Ok(())
}

#[test]
fn the_old_holders_set_fuses_after_the_parents_write_changes_no_resource() -> anyhow::Result<()> {
    // `setFuses` checks only the surviving token's owner, not registry custody, so the old
    // holder can still emit `FusesSet` after the parent's write.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421-L435 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1034-L1042 @ ens_v1@91c966f)
    let mut logs = wrapped_child();
    logs.extend([parent_sets_owner(3, 0, NEW_OWNER), fuses_set(4, 0)]);
    let events = interpret(logs)?;

    let fuses = find(&events, 4, 0, "PermissionScopeChanged").expect("FusesSet is recorded");
    assert_eq!(fuses.resource_id, None);
    assert_eq!(fuses.logical_name_id, None);
    Ok(())
}

#[test]
fn consecutive_parent_writes_in_one_transaction_leave_the_last_owner() -> anyhow::Result<()> {
    let mut logs = wrapped_child();
    logs.push(parent_sets_owner(3, 0, NEW_OWNER));
    logs.push(parent_sets_owner(3, 1, LAST_OWNER));
    let events = interpret(logs)?;
    let registry_only = registry_only_resource(&events);

    for (log_index, owner) in [(0, NEW_OWNER), (1, LAST_OWNER)] {
        let transferred = find(&events, 3, log_index, "AuthorityTransferred")
            .expect("each write moves the owner");
        assert_eq!(transferred.resource_id, Some(registry_only));
        assert_eq!(transferred.after_state["authority_kind"], "registry_only");
        assert_eq!(transferred.after_state["owner"], owner);
    }
    assert_eq!(
        resource_control(&events, 3, registry_only, LAST_OWNER),
        Some(json!(["resource_control"]))
    );
    Ok(())
}

#[test]
fn the_name_wrappers_own_unwrap_write_keeps_the_wrapper_until_name_unwrapped() -> anyhow::Result<()>
{
    let mut logs = wrapped_child();
    logs.extend([
        token_transfer(3, 0, HOLDER, ZERO_ADDRESS),
        registry_transfer(3, 1, NEW_OWNER),
        name_unwrapped(3, 2, NEW_OWNER),
    ]);
    let events = interpret(logs)?;
    let wrapper = wrapper_resource(&events);

    let transferred =
        find(&events, 3, 1, "AuthorityTransferred").expect("the unwrap's registry write");
    assert_eq!(transferred.resource_id, Some(wrapper));
    assert_eq!(transferred.after_state["authority_kind"], "wrapper");
    let epochs = in_block(&events, 3)
        .into_iter()
        .filter(|event| event.event_kind == "AuthorityEpochChanged")
        .collect::<Vec<_>>();
    assert_eq!(epochs.len(), 1, "{epochs:#?}");
    assert_eq!(epochs[0].log_index, Some(2));
    assert_eq!(epochs[0].before_state["authority_kind"], "wrapper");
    Ok(())
}

#[test]
fn a_name_unwrapped_shields_only_the_registry_write_just_before_it() -> anyhow::Result<()> {
    // A contract parent takes the child, re-wraps it to REWRAPPED_HOLDER and then hands the
    // registry record to LAST_OWNER, all in one transaction. Only the write to the NameWrapper
    // directly precedes the `NameUnwrapped` of the re-mint's burn.
    let mut logs = wrapped_child();
    logs.extend([
        parent_sets_owner(3, 0, NEW_OWNER),
        registry_transfer(3, 1, NAME_WRAPPER),
        token_transfer(3, 2, HOLDER, ZERO_ADDRESS),
        name_unwrapped(3, 3, ZERO_ADDRESS),
        name_wrapped(3, 4, REWRAPPED_HOLDER),
        parent_sets_owner(3, 5, LAST_OWNER),
    ]);
    let events = interpret(logs)?;
    let registry_only = registry_only_resource(&events);

    let taken = find(&events, 3, 0, "AuthorityTransferred").expect("the parent's first write");
    assert_eq!(taken.resource_id, Some(registry_only));
    assert_eq!(taken.after_state["authority_kind"], "registry_only");
    assert_eq!(taken.after_state["owner"], NEW_OWNER);
    let last = find(&events, 3, 5, "AuthorityTransferred").expect("the write after the re-wrap");
    assert_eq!(last.resource_id, Some(registry_only));
    assert_eq!(last.after_state["authority_kind"], "registry_only");
    assert_eq!(last.after_state["owner"], LAST_OWNER);
    Ok(())
}

#[test]
fn a_rewrap_after_the_parents_write_binds_the_new_holder() -> anyhow::Result<()> {
    let mut logs = wrapped_child();
    logs.push(parent_sets_owner(3, 0, NEW_OWNER));
    // NEW_OWNER wraps the child again: `_mint` burns the live token first.
    logs.extend([
        registry_transfer(4, 0, NAME_WRAPPER),
        token_transfer(4, 1, HOLDER, ZERO_ADDRESS),
        name_unwrapped(4, 2, ZERO_ADDRESS),
        name_wrapped(4, 3, REWRAPPED_HOLDER),
        token_transfer(5, 0, REWRAPPED_HOLDER, NEXT_HOLDER),
    ]);
    let events = interpret(logs)?;

    let unwrap_epoch = find(&events, 4, 2, "AuthorityEpochChanged").expect("re-mint releases");
    assert_eq!(
        unwrap_epoch.before_state["authority_kind"], "registry_only",
        "the parent's write already ended the old holder's wrapper authority"
    );
    assert!(
        in_block(&events, 4).iter().all(|event| {
            event.event_kind != "PermissionChanged" || event.after_state["subject"] != HOLDER
        }),
        "the burn of the stale token changes no grant of the old holder"
    );
    let rewrapped = find(&events, 4, 3, "TokenControlTransferred").expect("re-wrap");
    assert_eq!(rewrapped.after_state["to"], REWRAPPED_HOLDER);
    let moved = find(&events, 5, 0, "TokenControlTransferred").expect("the new token moves");
    assert_eq!(moved.resource_id, rewrapped.resource_id);
    assert_eq!(moved.after_state["to"], NEXT_HOLDER);
    Ok(())
}

#[test]
fn name_wrapper_set_record_keeps_the_wrapper_and_moves_the_token() -> anyhow::Result<()> {
    // `setRecord` rewrites the registry record to the NameWrapper, then `_transfer`s the token.
    let mut logs = wrapped_child();
    logs.extend([
        registry_transfer(3, 0, NAME_WRAPPER),
        token_transfer(3, 1, HOLDER, NEXT_HOLDER),
    ]);
    let events = interpret(logs)?;
    let wrapper = wrapper_resource(&events);

    assert!(find(&events, 3, 0, "SurfaceUnbound").is_none());
    let moved = find(&events, 3, 1, "TokenControlTransferred").expect("the token moves");
    assert_eq!(moved.resource_id, Some(wrapper));
    assert_eq!(moved.after_state["to"], NEXT_HOLDER);
    Ok(())
}

#[test]
fn name_wrapper_set_subnode_record_over_a_wrapped_child_keeps_the_wrapper() -> anyhow::Result<()> {
    // A wrapped parent created the child through the NameWrapper's `setSubnodeOwner`; its
    // `setSubnodeRecord` over the still-wrapped child rewrites the registry record to the
    // NameWrapper, then `_updateName` transfers the token.
    let logs = vec![
        parent_sets_owner(1, 0, NAME_WRAPPER),
        name_wrapped(1, 1, HOLDER),
        parent_sets_owner(2, 0, NAME_WRAPPER),
        token_transfer(2, 1, HOLDER, NEXT_HOLDER),
    ];
    let events = interpret(logs)?;
    let wrapper = find(&events, 1, 1, "TokenControlTransferred")
        .and_then(|event| event.resource_id)
        .expect("NameWrapped anchors the wrapper resource");

    assert!(find(&events, 2, 0, "SurfaceUnbound").is_none());
    let moved = find(&events, 2, 1, "TokenControlTransferred").expect("the token moves");
    assert_eq!(moved.resource_id, Some(wrapper));
    assert_eq!(moved.after_state["to"], NEXT_HOLDER);
    Ok(())
}

#[test]
fn a_parents_zero_owner_write_over_a_wrapped_child_ends_the_wrapper_authority() -> anyhow::Result<()>
{
    let mut logs = wrapped_child();
    logs.push(parent_sets_owner(3, 0, ZERO_ADDRESS));
    logs.push(token_transfer(4, 0, HOLDER, NEXT_HOLDER));
    let events = interpret(logs)?;
    let wrapper = wrapper_resource(&events);

    let transferred =
        find(&events, 3, 0, "AuthorityTransferred").expect("the parent's write clears the owner");
    assert!(transferred.after_state["authority_kind"].is_null());
    assert_eq!(
        find(&events, 3, 0, "SurfaceUnbound").and_then(|event| event.resource_id),
        Some(wrapper)
    );
    assert_eq!(
        resource_control(&events, 3, wrapper, HOLDER),
        Some(json!([]))
    );
    assert!(
        in_block(&events, 4).is_empty(),
        "the stale token's transfer moves nothing: {:#?}",
        in_block(&events, 4)
    );
    Ok(())
}

#[test]
fn the_name_wrappers_unwrap_to_zero_keeps_the_wrapper_until_name_unwrapped() -> anyhow::Result<()> {
    // `setRecord(node, 0, …)`: the record is rewritten to the NameWrapper, then `_unwrap(node, 0)`
    // burns the token, clears the registry owner and emits `NameUnwrapped`.
    let mut logs = wrapped_child();
    logs.extend([
        registry_transfer(3, 0, NAME_WRAPPER),
        token_transfer(3, 1, HOLDER, ZERO_ADDRESS),
        registry_transfer(3, 2, ZERO_ADDRESS),
        name_unwrapped(3, 3, ZERO_ADDRESS),
    ]);
    let events = interpret(logs)?;

    let epochs = in_block(&events, 3)
        .into_iter()
        .filter(|event| event.event_kind == "AuthorityEpochChanged")
        .collect::<Vec<_>>();
    assert_eq!(epochs.len(), 1, "{epochs:#?}");
    assert_eq!(epochs[0].log_index, Some(3));
    assert_eq!(epochs[0].before_state["authority_kind"], "wrapper");
    Ok(())
}

#[test]
fn a_lapsed_wrapped_eth_name_registered_again_without_the_name_wrapper_drops_the_old_token()
-> anyhow::Result<()> {
    // `registerAndWrapETH2LD`, then a plain `register` once the lease is past grace: `_register`
    // burns and mints the registrar token and writes the registry to the new owner.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
    const REGISTRAR_MANIFEST: i64 = 7403;
    const REGISTRAR: &str = "0x00000000000000000000000000000000000000c3";
    const EXPIRY: u64 = 10;
    const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;
    const REREGISTERED: i64 = 8_000_000;
    let label = keccak256(b"alice");
    let eth = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let node = super::common::namehash(&["alice".to_owned(), "eth".to_owned()]).parse::<B256>()?;
    let lifecycle = &[
        "RegistrationGranted",
        "ExpiryChanged",
        "PermissionChanged",
        "SurfaceUnbound",
        "SurfaceBound",
        "AuthorityEpochChanged",
        "ResolverChanged",
        "RegistrationReleased",
        "TokenControlTransferred",
    ];
    let mut manifests = manifests();
    manifests.push(manifest_with_events(
        REGISTRAR_MANIFEST,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "NameRegistered",
                "event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)",
                &["registrar"],
                lifecycle,
            ),
            (
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                lifecycle,
            ),
        ],
    ));
    let mut admissions = admissions();
    let mut registrar = admission(REGISTRAR_MANIFEST, "registrar");
    registrar.address = REGISTRAR.to_owned();
    admissions.push(registrar);
    let token = U256::from_be_bytes(label.0);
    let registrar_transfer = |block, log_index, from: &str, to: &str| {
        raw_at(
            v1_registrar::Transfer {
                from: addr(from),
                to: addr(to),
                tokenId: token,
            }
            .encode_log_data(),
            block,
            log_index,
            REGISTRAR,
        )
    };
    let registered = |block, log_index, owner: &str, expires: u64| {
        raw_at(
            with_topic0(
                v1_registrar::BaseNameRegistered {
                    id: token,
                    owner: addr(owner),
                    expires: U256::from(expires),
                }
                .encode_log_data(),
                keccak256(b"NameRegistered(uint256,address,uint256)"),
            ),
            block,
            log_index,
            REGISTRAR,
        )
    };
    let registry_owner = |block, log_index, owner: &str| {
        raw_at(
            v1_registry::NewOwner {
                node: eth,
                label,
                owner: addr(owner),
            }
            .encode_log_data(),
            block,
            log_index,
            REGISTRY,
        )
    };
    let raw_logs = vec![
        registrar_transfer(1, 0, ZERO_ADDRESS, NAME_WRAPPER),
        registry_owner(1, 1, NAME_WRAPPER),
        registered(1, 2, NAME_WRAPPER, EXPIRY),
        raw_at(
            NameWrapped {
                node,
                name: b"\x05alice\x03eth\0".to_vec().into(),
                owner: addr(HOLDER),
                fuses: (1 << 16) | (1 << 17),
                expiry: EXPIRY + GRACE_PERIOD,
            }
            .encode_log_data(),
            1,
            3,
            NAME_WRAPPER,
        ),
        registrar_transfer(REREGISTERED, 0, NAME_WRAPPER, ZERO_ADDRESS),
        registrar_transfer(REREGISTERED, 1, ZERO_ADDRESS, NEW_OWNER),
        registry_owner(REREGISTERED, 2, NEW_OWNER),
        registered(REREGISTERED, 3, NEW_OWNER, REREGISTERED as u64 + 1_000),
        // Robustness input, not a chain flow: with PARENT_CANNOT_CONTROL burnt, the expired
        // token has no holder to transfer it.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L176-L183 @ ens_v1@91c966f)
        raw_at(
            v2_registry::TransferSingle {
                operator: addr(HOLDER),
                from: addr(HOLDER),
                to: addr(NEXT_HOLDER),
                id: U256::from_be_bytes(node.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            REREGISTERED + 1,
            0,
            NAME_WRAPPER,
        ),
    ];
    let events = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    })?
    .normalized_events;

    let transferred = find(&events, REREGISTERED, 2, "AuthorityTransferred")
        .expect("the registration writes the registry owner");
    assert_ne!(transferred.after_state["authority_kind"], "wrapper");
    assert!(
        in_block(&events, REREGISTERED + 1).is_empty(),
        "the old token's transfer moves nothing: {:#?}",
        in_block(&events, REREGISTERED + 1)
    );
    Ok(())
}

/// The per-token approved address of a wrapped name loses its NameWrapper permission at the same
/// registry write that ends the holder's. These fixtures are full call sequences against the
/// checked-in Sepolia contracts: an unwrapped `dao.eth`, its child `team.dao.eth` wrapped with
/// `wrap` by its owner, then an `approve` on the NameWrapper.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L373 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L902 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L127-L135 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L375-L377 @ ens_v1@91c966f)
mod token_approval {
    use super::super::wrap_registry_interval::Fixture;
    use super::*;

    mod events {
        alloy_sol_types::sol! {
            event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
            event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
            event ControllerAdded(address indexed controller);
            event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
            event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
            event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
            event NameUnwrapped(bytes32 indexed node, address owner);
            event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
        }
        pub mod registry {
            alloy_sol_types::sol! { event Transfer(bytes32 indexed node, address owner); }
        }
    }

    const DAO: &str = "0x00000000000000000000000000000000000000d0";
    const HOLDER: &str = "0x00000000000000000000000000000000000000d1";
    const DELEGATE: &str = "0x00000000000000000000000000000000000000d2";
    const NEW_OWNER: &str = "0x00000000000000000000000000000000000000d3";
    const REWRAPPED: &str = "0x00000000000000000000000000000000000000d5";
    const NEXT_DELEGATE: &str = "0x00000000000000000000000000000000000000d6";
    const GRACE_PERIOD: i64 = 90 * 24 * 60 * 60;
    /// After the Sepolia Graveyard's declared start block, so the migration context applies.
    const START: i64 = 11_830_000;
    const PARENT_EXPIRY: i64 = START + 1_000_000;
    const CREATED: i64 = START + 1;
    const WRAPPED: i64 = START + 2;
    const APPROVED: i64 = START + 3;
    const WRITTEN: i64 = START + 4;
    const AFTER: i64 = START + 5;
    const LATEST: i64 = START + 6;

    fn address(value: &str) -> Address {
        value.parse().expect("address")
    }

    fn eth() -> B256 {
        super::super::common::namehash(&["eth".to_owned()])
            .parse()
            .expect("eth")
    }

    fn dao() -> B256 {
        super::super::common::namehash(&["dao".to_owned(), "eth".to_owned()])
            .parse()
            .expect("dao.eth")
    }

    fn team() -> B256 {
        super::super::common::namehash(&["team".to_owned(), "dao".to_owned(), "eth".to_owned()])
            .parse()
            .expect("team.dao.eth")
    }

    fn token(label: &[u8]) -> U256 {
        U256::from_be_bytes(keccak256(label).0)
    }

    struct Calls<'a> {
        fixture: &'a Fixture,
    }

    impl Calls<'_> {
        fn registrar(&self, data: alloy_primitives::LogData, block: i64, log: i64) -> RawLogInput {
            raw_at(data, block, log, &self.fixture.registrar)
        }

        fn registry(&self, data: alloy_primitives::LogData, block: i64, log: i64) -> RawLogInput {
            raw_at(data, block, log, &self.fixture.registry)
        }

        fn wrapper(&self, data: alloy_primitives::LogData, block: i64, log: i64) -> RawLogInput {
            raw_at(data, block, log, &self.fixture.wrapper)
        }

        /// `BaseRegistrar.register(dao, DAO, duration)` from an un-admitted controller.
        /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
        fn register_dao(&self) -> Vec<RawLogInput> {
            vec![
                self.registrar(
                    events::Transfer {
                        from: Address::ZERO,
                        to: address(DAO),
                        tokenId: token(b"dao"),
                    }
                    .encode_log_data(),
                    START,
                    0,
                ),
                self.registry(
                    events::NewOwner {
                        node: eth(),
                        label: keccak256(b"dao"),
                        owner: address(DAO),
                    }
                    .encode_log_data(),
                    START,
                    1,
                ),
                self.registrar(
                    events::NameRegistered {
                        id: token(b"dao"),
                        owner: address(DAO),
                        expires: U256::from(PARENT_EXPIRY),
                    }
                    .encode_log_data(),
                    START,
                    2,
                ),
            ]
        }

        /// The parent's `ENSRegistry.setSubnodeOwner(dao, team, owner)`.
        /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
        fn parent_write(&self, block: i64, log: i64, owner: &str) -> RawLogInput {
            self.registry(
                events::NewOwner {
                    node: dao(),
                    label: keccak256(b"team"),
                    owner: address(owner),
                }
                .encode_log_data(),
                block,
                log,
            )
        }

        /// `NameWrapper.wrap(team.dao.eth, wrapped, 0)` by the registry owner `caller`:
        /// `setOwner` to the NameWrapper, then `_mint`, which first burns a live old token
        /// held by `old` with `NameUnwrapped(node, 0)`, then `NameWrapped` with no fuses and no
        /// expiry.
        /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L373 @ ens_v1@91c966f)
        /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L902 @ ens_v1@91c966f)
        /// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L278 @ ens_v1@91c966f)
        fn wrap(
            &self,
            block: i64,
            caller: &str,
            old: Option<&str>,
            wrapped: &str,
        ) -> Vec<RawLogInput> {
            let id = U256::from_be_bytes(team().0);
            let mut logs = vec![
                self.registry(
                    events::registry::Transfer {
                        node: team(),
                        owner: address(&self.fixture.wrapper),
                    }
                    .encode_log_data(),
                    block,
                    0,
                ),
            ];
            if let Some(old) = old {
                logs.push(
                    self.wrapper(
                        events::TransferSingle {
                            operator: address(caller),
                            from: address(old),
                            to: Address::ZERO,
                            id,
                            value: U256::from(1),
                        }
                        .encode_log_data(),
                        block,
                        1,
                    ),
                );
                logs.push(
                    self.wrapper(
                        events::NameUnwrapped {
                            node: team(),
                            owner: Address::ZERO,
                        }
                        .encode_log_data(),
                        block,
                        2,
                    ),
                );
            }
            let next = i64::try_from(logs.len()).expect("log count");
            logs.push(
                self.wrapper(
                    events::TransferSingle {
                        operator: address(caller),
                        from: Address::ZERO,
                        to: address(wrapped),
                        id,
                        value: U256::from(1),
                    }
                    .encode_log_data(),
                    block,
                    next,
                ),
            );
            logs.push(
                self.wrapper(
                    events::NameWrapped {
                        node: team(),
                        name: b"\x04team\x03dao\x03eth\0".to_vec().into(),
                        owner: address(wrapped),
                        fuses: 0,
                        expiry: 0,
                    }
                    .encode_log_data(),
                    block,
                    next + 1,
                ),
            );
            logs
        }

        fn approve(&self, block: i64, owner: &str, approved: &str) -> RawLogInput {
            self.wrapper(
                events::Approval {
                    owner: address(owner),
                    approved: address(approved),
                    tokenId: U256::from_be_bytes(team().0),
                }
                .encode_log_data(),
                block,
                0,
            )
        }

        /// The dao's child, wrapped by its holder, with `approve(DELEGATE)`.
        fn approved_child(&self) -> Vec<Vec<RawLogInput>> {
            vec![
                self.register_dao(),
                vec![self.parent_write(CREATED, 0, HOLDER)],
                self.wrap(WRAPPED, HOLDER, None, HOLDER),
                vec![self.approve(APPROVED, HOLDER, DELEGATE)],
            ]
        }
    }

    fn wrapper_resource(output: &BatchOutput, block: i64) -> Uuid {
        output
            .normalized_events
            .iter()
            .find(|event| {
                event.block_number == Some(block)
                    && event.source_family == "ens_v1_wrapper_l1"
                    && event.event_kind == "TokenControlTransferred"
                    && event.after_state["authority_kind"] == "wrapper"
            })
            .and_then(|event| event.resource_id)
            .expect("the wrap anchors a wrapper resource")
    }

    fn relation(event: &NormalizedEvent) -> Option<&str> {
        event
            .after_state
            .pointer("/grant_source/relation_kind")
            .or_else(|| {
                event
                    .after_state
                    .pointer("/revocation_source/relation_kind")
            })
            .and_then(serde_json::Value::as_str)
    }

    /// (log index, subject, resource, powers, authority contract) of the token-approval rows of
    /// `block`.
    fn approval_rows(
        output: &BatchOutput,
        block: i64,
    ) -> Vec<(i64, String, Option<Uuid>, serde_json::Value, String)> {
        output
            .normalized_events
            .iter()
            .filter(|event| event.block_number == Some(block))
            .filter(|event| event.event_kind == "PermissionChanged")
            .filter(|event| relation(event) == Some("token_approval"))
            .map(|event| {
                let source = event
                    .after_state
                    .get("revocation_source")
                    .filter(|source| !source.is_null())
                    .unwrap_or(&event.after_state["grant_source"]);
                (
                    event.log_index.unwrap_or(-1),
                    event.after_state["subject"]
                        .as_str()
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    event.resource_id,
                    event.after_state["effective_powers"].clone(),
                    source["authority_contract"]
                        .as_str()
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                )
            })
            .collect()
    }

    /// The delegate revoke comes before the holder's revoke on the same resource.
    fn assert_delegate_revoked_first(output: &BatchOutput, block: i64, resource: Uuid) {
        let positions = output
            .normalized_events
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                event.block_number == Some(block)
                    && event.event_kind == "PermissionChanged"
                    && event.resource_id == Some(resource)
            })
            .map(|(index, event)| (index, event.after_state["subject"].clone()))
            .collect::<Vec<_>>();
        let delegate = positions
            .iter()
            .position(|(_, subject)| subject == DELEGATE);
        let holder = positions.iter().position(|(_, subject)| subject == HOLDER);
        assert!(
            delegate.is_some() && holder.is_some() && delegate < holder,
            "the delegate revoke precedes the holder revoke: {positions:?}"
        );
    }

    fn newest_delegate_powers(output: &BatchOutput, resource: Uuid) -> Option<serde_json::Value> {
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "PermissionChanged")
            .filter(|event| event.resource_id == Some(resource))
            .filter(|event| event.after_state["subject"] == DELEGATE)
            .map(|event| event.after_state["effective_powers"].clone())
            .next_back()
    }

    fn revoked(
        wrapper: &Fixture,
        resource: Uuid,
    ) -> Vec<(i64, String, Option<Uuid>, serde_json::Value, String)> {
        vec![(
            0,
            DELEGATE.to_owned(),
            Some(resource),
            json!([]),
            wrapper.wrapper.to_ascii_lowercase(),
        )]
    }

    #[test]
    fn a_parents_write_to_a_new_owner_revokes_the_token_approval() -> anyhow::Result<()> {
        let fixture = Fixture::new()?;
        let calls = Calls { fixture: &fixture };
        let mut steps = calls.approved_child();
        steps.push(vec![calls.parent_write(WRITTEN, 0, NEW_OWNER)]);
        let output = fixture.run(steps)?;
        let resource = wrapper_resource(&output, WRAPPED);
        assert_eq!(approval_rows(&output, WRITTEN), revoked(&fixture, resource));
        assert_delegate_revoked_first(&output, WRITTEN, resource);
        Ok(())
    }

    #[test]
    fn a_parents_zero_owner_write_revokes_the_token_approval() -> anyhow::Result<()> {
        let fixture = Fixture::new()?;
        let calls = Calls { fixture: &fixture };
        let mut steps = calls.approved_child();
        steps.push(vec![calls.parent_write(WRITTEN, 0, ZERO_ADDRESS)]);
        let output = fixture.run(steps)?;
        let resource = wrapper_resource(&output, WRAPPED);
        assert_eq!(approval_rows(&output, WRITTEN), revoked(&fixture, resource));
        assert_delegate_revoked_first(&output, WRITTEN, resource);
        Ok(())
    }

    /// The new owner wraps the child again, which burns the old token, and the new holder then
    /// approves someone else. Nothing brings the old delegate back, live or restored.
    #[test]
    fn a_rewrap_by_the_new_owner_leaves_the_old_delegate_revoked() -> anyhow::Result<()> {
        let fixture = Fixture::new()?;
        let calls = Calls { fixture: &fixture };
        let mut steps = calls.approved_child();
        steps.push(vec![calls.parent_write(WRITTEN, 0, NEW_OWNER)]);
        steps.push(calls.wrap(AFTER, NEW_OWNER, Some(HOLDER), REWRAPPED));
        steps.push(vec![calls.approve(LATEST, REWRAPPED, NEXT_DELEGATE)]);
        let output = fixture.run(steps)?;
        let old = wrapper_resource(&output, WRAPPED);
        let new = wrapper_resource(&output, AFTER);
        assert_eq!(newest_delegate_powers(&output, old), Some(json!([])));
        assert_eq!(approval_rows(&output, AFTER), Vec::new());
        assert_eq!(
            approval_rows(&output, LATEST),
            vec![(
                0,
                NEXT_DELEGATE.to_owned(),
                Some(new),
                json!(["extend_subname_expiry"]),
                fixture.wrapper.to_ascii_lowercase(),
            )],
            "the new approval revokes no stale delegate"
        );
        Ok(())
    }

    #[test]
    fn a_parents_write_with_no_delegate_emits_no_token_approval_row() -> anyhow::Result<()> {
        let fixture = Fixture::new()?;
        let calls = Calls { fixture: &fixture };
        let mut steps = calls.approved_child();
        steps.pop();
        steps.push(vec![calls.parent_write(WRITTEN, 0, NEW_OWNER)]);
        let output = fixture.run(steps)?;
        assert_eq!(approval_rows(&output, WRITTEN), Vec::new());

        let mut steps = calls.approved_child();
        steps.push(vec![calls.approve(WRITTEN, HOLDER, ZERO_ADDRESS)]);
        steps.push(vec![calls.parent_write(AFTER, 0, NEW_OWNER)]);
        let output = fixture.run(steps)?;
        assert_eq!(
            approval_rows(&output, AFTER),
            Vec::new(),
            "a cleared approval stays cleared"
        );
        Ok(())
    }

    /// The holder's own `unwrap(dao, team, NEW_OWNER)`: `_unwrap` burns the token, which clears
    /// the approval, then writes the registry and emits `NameUnwrapped`. The registry write keeps
    /// the wrapper authority, so the burn's revoke is the only one.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
    #[test]
    fn the_holders_unwrap_revokes_the_token_approval_once_at_the_burn() -> anyhow::Result<()> {
        let fixture = Fixture::new()?;
        let calls = Calls { fixture: &fixture };
        let mut steps = calls.approved_child();
        steps.push(vec![
            calls.wrapper(
                events::TransferSingle {
                    operator: address(HOLDER),
                    from: address(HOLDER),
                    to: Address::ZERO,
                    id: U256::from_be_bytes(team().0),
                    value: U256::from(1),
                }
                .encode_log_data(),
                WRITTEN,
                0,
            ),
            calls.registry(
                events::registry::Transfer {
                    node: team(),
                    owner: address(NEW_OWNER),
                }
                .encode_log_data(),
                WRITTEN,
                1,
            ),
            calls.wrapper(
                events::NameUnwrapped {
                    node: team(),
                    owner: address(NEW_OWNER),
                }
                .encode_log_data(),
                WRITTEN,
                2,
            ),
        ]);
        let output = fixture.run(steps)?;
        let resource = wrapper_resource(&output, WRAPPED);
        assert_eq!(approval_rows(&output, WRITTEN), revoked(&fixture, resource));
        Ok(())
    }

    /// A registry write that names the NameWrapper keeps the wrapper authority, so the approval
    /// stays until the holder's next transfer clears it.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
    #[test]
    fn a_registry_write_naming_the_name_wrapper_keeps_the_token_approval() -> anyhow::Result<()> {
        let fixture = Fixture::new()?;
        let calls = Calls { fixture: &fixture };
        let mut steps = calls.approved_child();
        steps.push(vec![calls.parent_write(WRITTEN, 0, &fixture.wrapper)]);
        steps.push(vec![
            calls.wrapper(
                events::TransferSingle {
                    operator: address(HOLDER),
                    from: address(HOLDER),
                    to: address(NEW_OWNER),
                    id: U256::from_be_bytes(team().0),
                    value: U256::from(1),
                }
                .encode_log_data(),
                AFTER,
                0,
            ),
        ]);
        let output = fixture.run(steps)?;
        let resource = wrapper_resource(&output, WRAPPED);
        assert_eq!(approval_rows(&output, WRITTEN), Vec::new());
        assert_eq!(approval_rows(&output, AFTER), revoked(&fixture, resource));
        Ok(())
    }

    /// `registerAndWrapETH2LD` of `alice.eth` to the holder, an `approve`, then, once the lease
    /// is past grace, a plain `register` to a new owner: `_register` burns and mints the
    /// registrar token and writes the registry to the new owner. The lapsed wrapper authority
    /// is no longer the active one at that write, so the write ends nothing and this rule does
    /// not apply. The approved address is treated like the holder: neither gets a row on the
    /// old NameWrapper resource there, and the read masks both as lapsed.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
    #[test]
    fn a_lapsed_wrapped_eth_name_registered_without_the_name_wrapper_is_outside_this_rule()
    -> anyhow::Result<()> {
        let fixture = Fixture::new()?;
        let calls = Calls { fixture: &fixture };
        let alice = super::super::common::namehash(&["alice".to_owned(), "eth".to_owned()])
            .parse::<B256>()?;
        let lease = START + 100;
        let reregistered = lease + GRACE_PERIOD + 10;
        let wrapper = address(&fixture.wrapper);
        let registration = vec![
            calls.registrar(
                events::Transfer {
                    from: Address::ZERO,
                    to: wrapper,
                    tokenId: token(b"alice"),
                }
                .encode_log_data(),
                START,
                0,
            ),
            calls.registry(
                events::NewOwner {
                    node: eth(),
                    label: keccak256(b"alice"),
                    owner: wrapper,
                }
                .encode_log_data(),
                START,
                1,
            ),
            calls.registrar(
                events::NameRegistered {
                    id: token(b"alice"),
                    owner: wrapper,
                    expires: U256::from(lease),
                }
                .encode_log_data(),
                START,
                2,
            ),
            calls.wrapper(
                events::TransferSingle {
                    operator: address(&fixture.controller),
                    from: Address::ZERO,
                    to: address(HOLDER),
                    id: U256::from_be_bytes(alice.0),
                    value: U256::from(1),
                }
                .encode_log_data(),
                START,
                3,
            ),
            calls.wrapper(
                events::NameWrapped {
                    node: alice,
                    name: b"\x05alice\x03eth\0".to_vec().into(),
                    owner: address(HOLDER),
                    fuses: 0x10000 | 0x20000,
                    expiry: u64::try_from(lease + GRACE_PERIOD)?,
                }
                .encode_log_data(),
                START,
                4,
            ),
        ];
        let approval = vec![
            calls.wrapper(
                events::Approval {
                    owner: address(HOLDER),
                    approved: address(DELEGATE),
                    tokenId: U256::from_be_bytes(alice.0),
                }
                .encode_log_data(),
                APPROVED,
                0,
            ),
        ];
        let again = vec![
            calls.registrar(
                events::Transfer {
                    from: wrapper,
                    to: Address::ZERO,
                    tokenId: token(b"alice"),
                }
                .encode_log_data(),
                reregistered,
                0,
            ),
            calls.registrar(
                events::Transfer {
                    from: Address::ZERO,
                    to: address(NEW_OWNER),
                    tokenId: token(b"alice"),
                }
                .encode_log_data(),
                reregistered,
                1,
            ),
            calls.registry(
                events::NewOwner {
                    node: eth(),
                    label: keccak256(b"alice"),
                    owner: address(NEW_OWNER),
                }
                .encode_log_data(),
                reregistered,
                2,
            ),
            calls.registrar(
                events::NameRegistered {
                    id: token(b"alice"),
                    owner: address(NEW_OWNER),
                    expires: U256::from(reregistered + 1_000_000),
                }
                .encode_log_data(),
                reregistered,
                3,
            ),
        ];
        let output = fixture.run(vec![registration, approval, again])?;
        let resource = wrapper_resource(&output, START);
        assert_eq!(
            newest_delegate_powers(&output, resource),
            Some(json!(["extend_subname_expiry"]))
        );
        let rows_at_write = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.block_number == Some(reregistered)
                    && event.resource_id == Some(resource)
                    && event.event_kind == "PermissionChanged"
            })
            .count();
        assert_eq!(
            rows_at_write, 0,
            "the re-registration writes no row on the old resource"
        );
        Ok(())
    }

    /// Sepolia's Graveyard `clear([team.dao.eth])` once `dao.eth` is past grace: it registers
    /// `dao.eth` to itself through the BaseRegistrar, then `setSubnodeRecord` takes the wrapped
    /// child to itself in the registry with the resolver and TTL already zero, so only
    /// `NewOwner` is emitted.
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L98-L102 @ ens_v2_sepolia_20261001@07e55a05)
    /// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20261001@07e55a05)
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L58 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L188 @ ens_v1@91c966f)
    #[test]
    fn the_graveyards_clear_over_a_wrapped_child_revokes_the_token_approval() -> anyhow::Result<()>
    {
        let fixture = Fixture::with(&["ens_v2_migration_l1"])?;
        let calls = Calls { fixture: &fixture };
        let graveyard =
            super::super::node_record_events::declared_address(&fixture.admissions, "graveyard");
        let cleared = PARENT_EXPIRY + GRACE_PERIOD + 10;
        let mut steps = calls.approved_child();
        steps.push(vec![
            calls.registrar(
                events::ControllerAdded {
                    controller: address(&graveyard),
                }
                .encode_log_data(),
                WRITTEN,
                0,
            ),
        ]);
        steps.push(vec![
            calls.registrar(
                events::Transfer {
                    from: address(DAO),
                    to: Address::ZERO,
                    tokenId: token(b"dao"),
                }
                .encode_log_data(),
                cleared,
                0,
            ),
            calls.registrar(
                events::Transfer {
                    from: Address::ZERO,
                    to: address(&graveyard),
                    tokenId: token(b"dao"),
                }
                .encode_log_data(),
                cleared,
                1,
            ),
            calls.registry(
                events::NewOwner {
                    node: eth(),
                    label: keccak256(b"dao"),
                    owner: address(&graveyard),
                }
                .encode_log_data(),
                cleared,
                2,
            ),
            calls.registrar(
                events::NameRegistered {
                    id: token(b"dao"),
                    owner: address(&graveyard),
                    expires: U256::from(u64::MAX - u64::try_from(GRACE_PERIOD)?),
                }
                .encode_log_data(),
                cleared,
                3,
            ),
            calls.parent_write(cleared, 4, &graveyard),
        ]);
        let output = fixture.run(steps)?;
        let resource = wrapper_resource(&output, WRAPPED);
        let rows = approval_rows(&output, cleared);
        assert_eq!(
            rows,
            vec![(
                4,
                DELEGATE.to_owned(),
                Some(resource),
                json!([]),
                fixture.wrapper.to_ascii_lowercase(),
            )]
        );
        assert_delegate_revoked_first(&output, cleared, resource);
        Ok(())
    }
    /// The permission rows of the parent's registry write over a wrapped child with an approved
    /// address, against the NameWrapper resource and the registry-only resource.
    mod registry_write_rows {
        use super::*;

        /// (subject, resource, granted) of a permission row.
        type Row = (String, Option<Uuid>, bool);

        /// The permission rows of `block`, in output order.
        fn rows_at(output: &BatchOutput, block: i64) -> Vec<Row> {
            output
                .normalized_events
                .iter()
                .filter(|event| event.block_number == Some(block))
                .filter(|event| event.event_kind == "PermissionChanged")
                .map(|event| {
                    (
                        event.after_state["subject"]
                            .as_str()
                            .unwrap_or_default()
                            .to_ascii_lowercase(),
                        event.resource_id,
                        event.after_state["effective_powers"]
                            .as_array()
                            .is_some_and(|powers| !powers.is_empty()),
                    )
                })
                .collect()
        }

        /// The `token_approval` rows of `block` as (family, subject).
        fn approvals_at(output: &BatchOutput, block: i64) -> Vec<(String, String)> {
            output
                .normalized_events
                .iter()
                .filter(|event| event.block_number == Some(block))
                .filter(|event| relation(event) == Some("token_approval"))
                .map(|event| {
                    (
                        event.source_family.clone(),
                        event.after_state["subject"]
                            .as_str()
                            .unwrap_or_default()
                            .to_ascii_lowercase(),
                    )
                })
                .collect()
        }

        /// The parent's `setSubnodeOwner` to a new owner writes four rows in this order: the
        /// approved address's revoke on the NameWrapper resource, the holder's revoke there, the
        /// new owner's grant on the registry-only resource, then the NameWrapper's revoke there.
        /// The approved address's revoke is the one `token_approval` row, from the registry.
        #[test]
        fn a_parents_write_revokes_the_delegate_once_and_grants_the_new_owner_once()
        -> anyhow::Result<()> {
            let fixture = Fixture::new()?;
            let calls = Calls { fixture: &fixture };
            let mut steps = calls.approved_child();
            steps.push(vec![calls.parent_write(WRITTEN, 0, NEW_OWNER)]);
            let output = fixture.run(steps)?;
            let wrapped = wrapper_resource(&output, WRAPPED);
            let rows = rows_at(&output, WRITTEN);
            let registry_only = rows.get(2).and_then(|row| row.1);
            assert!(registry_only.is_some_and(|resource| resource != wrapped));
            assert_eq!(
                rows,
                vec![
                    (DELEGATE.to_owned(), Some(wrapped), false),
                    (HOLDER.to_owned(), Some(wrapped), false),
                    (NEW_OWNER.to_owned(), registry_only, true),
                    (fixture.wrapper.to_ascii_lowercase(), registry_only, false),
                ]
            );
            assert_eq!(
                approvals_at(&output, WRITTEN),
                vec![("ens_v1_registry_l1".to_owned(), DELEGATE.to_owned())]
            );
            Ok(())
        }

        /// The parent's zero-owner write writes three rows in this order: the approved address's
        /// revoke on the NameWrapper resource, the holder's revoke there, then the NameWrapper's
        /// revoke on the registry-only resource. No one is granted.
        #[test]
        fn a_parents_zero_write_grants_no_one() -> anyhow::Result<()> {
            let fixture = Fixture::new()?;
            let calls = Calls { fixture: &fixture };
            let mut steps = calls.approved_child();
            steps.push(vec![calls.parent_write(WRITTEN, 0, ZERO_ADDRESS)]);
            let output = fixture.run(steps)?;
            let wrapped = wrapper_resource(&output, WRAPPED);
            let rows = rows_at(&output, WRITTEN);
            let registry_only = rows.get(2).and_then(|row| row.1);
            assert!(registry_only.is_some_and(|resource| resource != wrapped));
            assert_eq!(
                rows,
                vec![
                    (DELEGATE.to_owned(), Some(wrapped), false),
                    (HOLDER.to_owned(), Some(wrapped), false),
                    (fixture.wrapper.to_ascii_lowercase(), registry_only, false),
                ]
            );
            assert_eq!(
                approvals_at(&output, WRITTEN),
                vec![("ens_v1_registry_l1".to_owned(), DELEGATE.to_owned())]
            );
            Ok(())
        }

        /// The parent's write naming the NameWrapper keeps the NameWrapper authority, so it
        /// writes no registry `token_approval` row.
        #[test]
        fn a_write_naming_the_name_wrapper_writes_no_registry_token_approval() -> anyhow::Result<()>
        {
            let fixture = Fixture::new()?;
            let calls = Calls { fixture: &fixture };
            let mut steps = calls.approved_child();
            steps.push(vec![calls.parent_write(WRITTEN, 0, &fixture.wrapper)]);
            let output = fixture.run(steps)?;
            let approvals = approvals_at(&output, WRITTEN);
            assert!(
                approvals
                    .iter()
                    .all(|(family, _)| family != "ens_v1_registry_l1"),
                "{approvals:#?}"
            );
            Ok(())
        }
    }
}
