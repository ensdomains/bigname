//! TYR-334 reproduction: an unwrapped ENSv1 subname (registry `NewOwner` to an EOA) whose
//! parent is NameWrapper-owned must still receive a registry-only surface binding, in either
//! creation order. Without a binding the composed row has no `surface_binding_id`,
//! `resource_id` or `binding_kind`, its lifecycle status composes as `unregistered`, and
//! `relations()` (crates/storage/src/families/records/address_relations.rs) serves no
//! relations for its owner; the surface-less registry-children fallback cannot rescue it
//! once a surface exists. Wrapped siblings bind through `NameWrapped` and serve `active`.
//!
//! A child created before its parent's label path was ever observed gets no
//! surface. That shape is tracked separately as TYR-335 and is not covered here.

use super::*;

const REGISTRY_MANIFEST: i64 = 7401;
const WRAPPER_MANIFEST: i64 = 7402;
const REGISTRAR_MANIFEST: i64 = 7403;
const REGISTRY: &str = "0x00000000000000000000000000000000000000c1";
const NAME_WRAPPER: &str = "0x00000000000000000000000000000000000000c2";
const HOLDER: &str = "0x00000000000000000000000000000000000000a1";
const CHILD_OWNER: &str = "0x00000000000000000000000000000000000000b1";
const RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

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
                (
                    "NewResolver",
                    "event NewResolver(bytes32 indexed node, address resolver)",
                    &["registry"],
                    &["ResolverChanged", "PermissionChanged"],
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
                        "SurfaceBound",
                        "AuthorityEpochChanged",
                        "ResolverChanged",
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

fn node(labels: &[&str]) -> B256 {
    super::common::namehash(
        &labels
            .iter()
            .map(|label| (*label).to_owned())
            .collect::<Vec<_>>(),
    )
    .parse()
    .expect("namehash parses")
}

fn parent() -> B256 {
    node(&["parent", "eth"])
}

fn child() -> B256 {
    node(&["child", "parent", "eth"])
}

fn sibling() -> B256 {
    node(&["sibling", "parent", "eth"])
}

fn child_id() -> String {
    format!("ens:{:#x}", child())
}

fn sibling_id() -> String {
    format!("ens:{:#x}", sibling())
}

fn addr(value: &str) -> Address {
    value.parse().expect("address literal parses")
}

/// The registry writes a subnode: `setSubnodeOwner(parent, label, owner)`.
fn new_owner(block: i64, log_index: i64, label: &str, owner: &str) -> RawLogInput {
    new_owner_under(parent(), block, log_index, label, owner)
}

fn new_owner_under(
    node: B256,
    block: i64,
    log_index: i64,
    label: &str,
    owner: &str,
) -> RawLogInput {
    raw_at(
        v1_registry::NewOwner {
            node,
            label: keccak256(label.as_bytes()),
            owner: addr(owner),
        }
        .encode_log_data(),
        block,
        log_index,
        REGISTRY,
    )
}

fn new_resolver(block: i64, log_index: i64, node: B256, resolver: &str) -> RawLogInput {
    raw_at(
        v1_registry::NewResolver {
            node,
            resolver: addr(resolver),
        }
        .encode_log_data(),
        block,
        log_index,
        REGISTRY,
    )
}

fn transfer_single(
    block: i64,
    log_index: i64,
    operator: &str,
    from: &str,
    to: &str,
    id: B256,
) -> RawLogInput {
    raw_at(
        v2_registry::TransferSingle {
            operator: addr(operator),
            from: addr(from),
            to: addr(to),
            id: U256::from_be_bytes(id.0),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        log_index,
        NAME_WRAPPER,
    )
}

fn name_unwrapped(block: i64, log_index: i64, node: B256, owner: &str) -> RawLogInput {
    raw_at(
        NameUnwrapped {
            node,
            owner: addr(owner),
        }
        .encode_log_data(),
        block,
        log_index,
        NAME_WRAPPER,
    )
}

fn registry_transfer(block: i64, log_index: i64, node: B256, owner: &str) -> RawLogInput {
    raw_at(
        v1_registry::Transfer {
            node,
            owner: addr(owner),
        }
        .encode_log_data(),
        block,
        log_index,
        REGISTRY,
    )
}

fn name_wrapped(block: i64, log_index: i64, node: B256, dns: &[u8], owner: &str) -> RawLogInput {
    raw_at(
        NameWrapped {
            node,
            name: dns.to_vec().into(),
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

/// The parent is wrapped: its registry record moves to the NameWrapper, which mints the
/// parent token to HOLDER.
fn wrap_parent(block: i64) -> [RawLogInput; 2] {
    [
        registry_transfer(block, 0, parent(), NAME_WRAPPER),
        name_wrapped(block, 1, parent(), b"\x06parent\x03eth\0", HOLDER),
    ]
}

/// A wrapped sibling created under the wrapped parent, for contrast.
fn wrap_sibling(block: i64) -> [RawLogInput; 3] {
    [
        new_owner(block, 1, "sibling", NAME_WRAPPER),
        registry_transfer(block, 2, sibling(), NAME_WRAPPER),
        name_wrapped(
            block,
            3,
            sibling(),
            b"\x07sibling\x06parent\x03eth\0",
            HOLDER,
        ),
    ]
}

fn batch(logs: Vec<RawLogInput>) -> BatchInput {
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: logs,
    }
}

fn interpret(logs: Vec<RawLogInput>) -> anyhow::Result<BatchOutput> {
    interpret_test_batch(batch(logs))
}

/// The Sepolia log sequence: wrapped parent (block 1), child created wrapped through
/// the wrapper (block 2), child unwrapped (block 3). Split after block 2 for the
/// restore/incremental equivalence check.
fn sepolia_logs() -> (Vec<RawLogInput>, Vec<RawLogInput>) {
    let mut setup = wrap_parent(1).to_vec();
    setup.push(new_owner(2, 0, "child", NAME_WRAPPER));
    setup.push(new_resolver(2, 1, child(), RESOLVER));
    setup.push(transfer_single(2, 2, HOLDER, ZERO, CHILD_OWNER, child()));
    setup.push(name_wrapped(
        2,
        3,
        child(),
        b"\x05child\x06parent\x03eth\0",
        CHILD_OWNER,
    ));
    let tail = vec![
        transfer_single(3, 0, CHILD_OWNER, CHILD_OWNER, ZERO, child()),
        registry_transfer(3, 1, child(), CHILD_OWNER),
        name_unwrapped(3, 2, child(), CHILD_OWNER),
    ];
    (setup, tail)
}

fn assert_bound(output: &BatchOutput, logical_name_id: &str) {
    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.logical_name_id == logical_name_id),
        "no name surface for {logical_name_id}: {:?}",
        output
            .name_surfaces
            .iter()
            .map(|surface| &surface.logical_name_id)
            .collect::<Vec<_>>()
    );
    assert!(
        output
            .surface_bindings
            .iter()
            .any(|binding| binding.logical_name_id == logical_name_id),
        "no surface binding for {logical_name_id}: bindings are {:?}, events are {:?}",
        output
            .surface_bindings
            .iter()
            .map(|binding| &binding.logical_name_id)
            .collect::<Vec<_>>(),
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "SurfaceBound")
            .map(|event| (&event.logical_name_id, &event.after_state["authority_kind"]))
            .collect::<Vec<_>>()
    );
}

#[test]
fn unwrapped_child_created_when_parent_already_wrapped_binds_registry_only() -> anyhow::Result<()> {
    let mut logs = wrap_parent(1).to_vec();
    logs.push(new_owner(2, 0, "child", CHILD_OWNER));
    logs.extend(wrap_sibling(2));
    let output = interpret(logs)?;

    assert_bound(&output, &child_id());
    assert_bound(&output, &sibling_id());
    let child_bound = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(child_id().as_str())
        })
        .expect("the unwrapped child has a SurfaceBound");
    assert_eq!(child_bound.after_state["authority_kind"], "registry_only");
    Ok(())
}

/// Sepolia shape: the child is created wrapped through the NameWrapper
/// under an already-wrapped parent, then unwrapped in the next block. The registry owner
/// after the unwrap is a nonzero EOA and there is no registrar lease to reactivate, so the
/// unwrap must hand the known surface to a registry-only authority instead of closing the
/// wrapped binding with no successor.
#[test]
fn wrapped_child_unwrapped_without_a_lease_binds_registry_only() -> anyhow::Result<()> {
    let (setup, tail) = sepolia_logs();
    let mut logs = setup;
    logs.extend(tail);
    let output = interpret(logs)?;

    assert_bound(&output, &child_id());
    let binding = output
        .surface_bindings
        .iter()
        .find(|binding| binding.logical_name_id == child_id())
        .expect("the unwrapped child keeps a surface binding");
    assert_eq!(binding.binding_kind, "declared_registry_path");
    let child_bound = output
        .normalized_events
        .iter()
        .rfind(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(child_id().as_str())
        })
        .expect("the unwrapped child has a SurfaceBound");
    assert_eq!(child_bound.after_state["authority_kind"], "registry_only");
    // The registry-only successor must carry the same authority permission
    // handoff a plain registry-owned name gets: the registry owner holds
    // `resource_control` (served as `registration_control`) and
    // `resolver_control` on the registry-only resource.
    let registry_resource = binding.resource_id;
    for (power, scope_kind) in [
        ("resource_control", "resource"),
        ("resolver_control", "resolver"),
    ] {
        let grant = output
            .normalized_events
            .iter()
            .filter(|event| event.block_number == Some(3))
            .find(|event| {
                event.event_kind == "PermissionChanged"
                    && event.logical_name_id.as_deref() == Some(child_id().as_str())
                    && event.resource_id == Some(registry_resource)
                    && event.after_state["subject"] == CHILD_OWNER
                    && event.after_state["scope"]["kind"] == scope_kind
                    && event.after_state["effective_powers"]
                        .as_array()
                        .is_some_and(|powers| powers.iter().any(|held| held == power))
            })
            .unwrap_or_else(|| {
                panic!(
                    "unwrapped child has no {power} grant on the registry-only resource: {:?}",
                    output
                        .normalized_events
                        .iter()
                        .filter(|event| event.event_kind == "PermissionChanged")
                        .map(|event| &event.after_state)
                        .collect::<Vec<_>>()
                )
            });
        assert_eq!(
            grant.after_state["grant_source"]["authority_kind"],
            "registry_only"
        );
        if scope_kind == "resolver" {
            assert_eq!(grant.after_state["scope"]["resolver_address"], RESOLVER);
        }
    }
    Ok(())
}

/// The unwrap successor must survive a batch boundary: replaying block 3 from a
/// restored session or from folded prior events emits the same registry-only
/// `SurfaceBound` and binding as the single-batch run.
#[test]
fn wrapped_child_unwrap_binds_registry_only_across_restore() -> anyhow::Result<()> {
    let (setup, tail) = sepolia_logs();
    let mut all = setup.clone();
    all.extend(tail.clone());
    let full = interpret(all)?;
    let full_tail_events: Vec<_> = full
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(3))
        .cloned()
        .collect();
    assert!(full_tail_events.iter().any(|event| {
        event.event_kind == "SurfaceBound" && event.after_state["authority_kind"] == "registry_only"
    }));

    let assert_tail_bound = |output: &BatchOutput| {
        assert!(
            output
                .surface_bindings
                .iter()
                .any(|binding| binding.logical_name_id == child_id()
                    && binding.binding_kind == "declared_registry_path"),
            "tail keeps no registry binding for the child: {:?}",
            output.surface_bindings
        );
    };
    let (setup_output, session) = interpret_test_batch_incremental(batch(setup.clone()), None)?;
    let (live, _) = interpret_test_batch_incremental(batch(tail.clone()), Some(session))?;
    assert_eq!(live.normalized_events, full_tail_events, "incremental tail");
    assert_tail_bound(&live);

    for prior in [
        setup_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect::<Vec<_>>(),
        seam::fold_prior_events(
            Vec::new(),
            &setup_output.normalized_events,
            &[1, 2]
                .into_iter()
                .map(|number| RawBlockInput {
                    chain_id: CHAIN.to_owned(),
                    block_hash: format!("block-{number}"),
                    block_number: number,
                    block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
                    canonicality_state: "canonical".to_owned(),
                })
                .collect::<Vec<_>>(),
        )?,
    ] {
        let mut cold_input = batch(tail.clone());
        cold_input.prior_events = prior;
        let cold = interpret_test_batch(cold_input)?;
        assert_eq!(cold.normalized_events, full_tail_events, "restored tail");
        assert_tail_bound(&cold);
    }
    Ok(())
}

/// Realistic created-before-wrapped shape: the parent is created and known
/// while unwrapped, the child is created by `NewOwner` under it, and only then does
/// the parent wrap. The child received a plain registry binding at creation and must
/// keep it across the parent's wrap. The parent is made known the way
/// `v1_node_identity` seeds nested paths: the root node has an empty path, so
/// `NewOwner(root, "eth")` seeds `eth`, then `NewOwner(eth, "parent")` seeds the
/// parent; without the `eth` step `observe_identity` finds no parent path.
#[test]
fn unwrapped_child_created_under_known_unwrapped_parent_keeps_registry_binding()
-> anyhow::Result<()> {
    let eth = node(&["eth"]);
    let mut prefix = vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", HOLDER),
        new_owner_under(eth, 1, 1, "parent", HOLDER),
    ];
    prefix.push(new_owner(2, 0, "child", CHILD_OWNER));
    let prefix_output = interpret(prefix.clone())?;
    assert_bound(&prefix_output, &child_id());
    let prefix_bound = prefix_output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(child_id().as_str())
        })
        .expect("the child binds in its creation block, before the parent wraps");
    assert_eq!(prefix_bound.block_number, Some(2));
    assert_eq!(prefix_bound.after_state["authority_kind"], "registry_only");
    let mut logs = prefix;
    logs.extend(wrap_parent(3));
    let output = interpret(logs)?;

    assert_bound(&output, &child_id());
    let binding = output
        .surface_bindings
        .iter()
        .find(|binding| binding.logical_name_id == child_id())
        .expect("the child keeps a surface binding");
    assert_eq!(binding.binding_kind, "declared_registry_path");
    let child_bound = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(child_id().as_str())
        })
        .expect("the child has a SurfaceBound");
    assert_eq!(child_bound.block_number, Some(2));
    assert_eq!(child_bound.after_state["authority_kind"], "registry_only");
    Ok(())
}

/// After-block-3 boundary (TYR-334 restore blocker): the unwrap has already completed
/// in an earlier batch, so the registry-only successor exists only as folded/restored
/// prior state. Re-wrapping the child in the next block must behave exactly as in
/// the live incremental session and in the single-batch run: the restored successor
/// is the authority the wrap transitions from, so its `declared_registry_path`
/// binding is the one closed and replaced.
#[test]
fn wrapped_child_unwrap_registry_only_survives_restore_after_unwrap() -> anyhow::Result<()> {
    let (setup, tail) = sepolia_logs();
    let mut history = setup.clone();
    history.extend(tail.clone());
    let probe = vec![name_wrapped(
        4,
        0,
        child(),
        b"\x05child\x06parent\x03eth\0",
        CHILD_OWNER,
    )];

    let mut all = history.clone();
    all.extend(probe.clone());
    let full = interpret(all)?;
    let full_probe_events: Vec<_> = full
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(4))
        .cloned()
        .collect();
    assert!(
        full_probe_events.iter().any(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(child_id().as_str())
                && event.after_state["authority_kind"] == "wrapper"
        }),
        "single-batch probe re-wraps the child: {full_probe_events:?}"
    );
    let (history_output, session) = interpret_test_batch_incremental(batch(history.clone()), None)?;
    let (live, _) = interpret_test_batch_incremental(batch(probe.clone()), Some(session))?;
    assert_eq!(
        live.normalized_events, full_probe_events,
        "incremental probe"
    );

    for prior in [
        history_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect::<Vec<_>>(),
        seam::fold_prior_events(
            Vec::new(),
            &history_output.normalized_events,
            &[1, 2, 3]
                .into_iter()
                .map(|number| RawBlockInput {
                    chain_id: CHAIN.to_owned(),
                    block_hash: format!("block-{number}"),
                    block_number: number,
                    block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
                    canonicality_state: "canonical".to_owned(),
                })
                .collect::<Vec<_>>(),
        )?,
    ] {
        let mut cold_input = batch(probe.clone());
        cold_input.prior_events = prior;
        let cold = interpret_test_batch(cold_input)?;
        assert_eq!(cold.normalized_events, full_probe_events, "restored probe");
        assert_eq!(
            cold.surface_bindings, live.surface_bindings,
            "restored probe bindings"
        );
        assert_eq!(
            cold.binding_closures, live.binding_closures,
            "restored probe closures"
        );
        // The wrap transitions FROM the unwrap's registry-only successor: closing
        // that binding is what proves the successor survived the cold restore.
        assert!(
            cold.normalized_events.iter().any(|event| {
                event.event_kind == "SurfaceUnbound"
                    && event.logical_name_id.as_deref() == Some(child_id().as_str())
                    && event.before_state["authority_kind"] == "registry_only"
            }),
            "restored probe lost the child's registry-only successor: {:?}",
            cold.normalized_events
        );
    }
    Ok(())
}

/// Lease boundary (TYR-334 restore fix): when the unwrap DOES reactivate a registrar
/// lease, the successor is the registrar authority. The registry-only restore branch
/// must not fire for that shape, live or restored: the binding stays on the
/// registrar resource and no `registry_only` successor is minted.
#[test]
fn wrapped_name_unwrapped_with_a_live_lease_reactivates_registrar_not_registry_only()
-> anyhow::Result<()> {
    let leased = node(&["leased", "eth"]);
    let leased_id = format!("ens:{leased:#x}");
    let lease_manifests = || {
        let mut all = manifests();
        all.push(manifest(
            REGISTRAR_MANIFEST,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        ));
        all
    };
    let lease_admissions = || {
        let mut all = admissions();
        all.push(admission(REGISTRAR_MANIFEST, "registrar"));
        all
    };
    let lease_batch = |logs: Vec<RawLogInput>| BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: lease_manifests(),
        discovery_rules: Vec::new(),
        admissions: lease_admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: logs,
    };
    let setup = vec![
        raw_at(
            NameRegistered {
                name: "leased".to_owned(),
                label: keccak256(b"leased"),
                owner: addr(HOLDER),
                expires: U256::from(42),
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        ),
        name_wrapped(2, 0, leased, b"\x06leased\x03eth\0", HOLDER),
    ];
    let tail = vec![name_unwrapped(3, 0, leased, HOLDER)];

    let mut all = setup.clone();
    all.extend(tail.clone());
    let full = interpret_test_batch(lease_batch(all))?;
    let registrar_resource = full
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar resource");
    let assert_lease_successor = |output: &BatchOutput| {
        let bound = output
            .normalized_events
            .iter()
            .rfind(|event| {
                event.event_kind == "SurfaceBound"
                    && event.logical_name_id.as_deref() == Some(leased_id.as_str())
            })
            .expect("the unwrapped leased name has a SurfaceBound");
        assert_eq!(bound.after_state["authority_kind"], "registrar");
        assert_eq!(bound.resource_id, Some(registrar_resource));
        assert!(
            !output.normalized_events.iter().any(|event| {
                event.event_kind == "SurfaceBound"
                    && event.logical_name_id.as_deref() == Some(leased_id.as_str())
                    && event.after_state["authority_kind"] == "registry_only"
            }),
            "registry-only successor fired for a lease-bearing unwrap: {:?}",
            output.normalized_events
        );
        let binding = output
            .surface_bindings
            .iter()
            .find(|binding| binding.logical_name_id == leased_id)
            .expect("the unwrapped leased name keeps a surface binding");
        assert_eq!(binding.binding_kind, "declared_registry_path");
        assert_eq!(binding.resource_id, registrar_resource);
    };
    assert_lease_successor(&full);

    // The same successor must come out of a cold restore split before the unwrap.
    let full_tail_events: Vec<_> = full
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(3))
        .cloned()
        .collect();
    let (setup_output, session) =
        interpret_test_batch_incremental(lease_batch(setup.clone()), None)?;
    let (live, _) = interpret_test_batch_incremental(lease_batch(tail.clone()), Some(session))?;
    assert_eq!(live.normalized_events, full_tail_events, "incremental tail");
    assert_lease_successor(&live);
    for prior in [
        setup_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect::<Vec<_>>(),
        seam::fold_prior_events(
            Vec::new(),
            &setup_output.normalized_events,
            &[1, 2]
                .into_iter()
                .map(|number| RawBlockInput {
                    chain_id: CHAIN.to_owned(),
                    block_hash: format!("block-{number}"),
                    block_number: number,
                    block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
                    canonicality_state: "canonical".to_owned(),
                })
                .collect::<Vec<_>>(),
        )?,
    ] {
        let mut cold_input = lease_batch(tail.clone());
        cold_input.prior_events = prior;
        let cold = interpret_test_batch(cold_input)?;
        assert_eq!(cold.normalized_events, full_tail_events, "restored tail");
        assert_lease_successor(&cold);
    }
    Ok(())
}
