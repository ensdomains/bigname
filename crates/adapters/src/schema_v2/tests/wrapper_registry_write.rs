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
fn a_name_unwrapped_earlier_in_the_transaction_does_not_shield_a_later_write() -> anyhow::Result<()>
{
    // A contract parent takes the child, re-wraps it to REWRAPPED_HOLDER and then hands the
    // registry record to LAST_OWNER, all in one transaction.
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
    let wrapper = wrapper_resource(&events);

    let shielded =
        find(&events, 3, 0, "AuthorityTransferred").expect("the write before the re-wrap");
    assert_eq!(shielded.resource_id, Some(wrapper));
    assert_eq!(shielded.after_state["authority_kind"], "wrapper");
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
