//! An unwrapped ENSv1 subname whose parent is NameWrapper-owned must still receive a
//! registry-only surface binding, whether it was created under the parent before the parent
//! wrapped, or created wrapped under the wrapped parent and then unwrapped. Without a binding
//! the composed row has no `surface_binding_id`, `resource_id` or `binding_kind`, its
//! lifecycle status composes as `unregistered`, and `relations()`
//! (crates/storage/src/families/records/address_relations.rs) serves no relations for its
//! owner. The surface-less registry-children fallback cannot rescue it once a surface exists.
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
/// PARENT_CANNOT_CONTROL | IS_DOT_ETH, which `_wrapETH2LD` ORs into every `.eth` wrap.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1015 @ ens_v1@91c966f)
const DOT_ETH_FUSES: u32 = 0x30000;
/// The registrar expiry plus the 90-day grace that `wrapETH2LD` gives the wrapped `.eth` name.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L270 @ ens_v1@91c966f)
const DOT_ETH_WRAP_EXPIRY: u64 = 2_000_000_000 + 90 * 86_400;

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

fn child_id() -> String {
    format!("ens:{:#x}", child())
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

/// `NameWrapped` with fuses 0 and expiry 0, as `wrap` emits it, and as a fuse-0
/// `setSubnodeOwner` or `setSubnodeRecord` emits it when the caller passes expiry 0.
fn name_wrapped(block: i64, log_index: i64, node: B256, dns: &[u8], owner: &str) -> RawLogInput {
    name_wrapped_with(block, log_index, node, dns, owner, (0, 0))
}

fn name_wrapped_with(
    block: i64,
    log_index: i64,
    node: B256,
    dns: &[u8],
    owner: &str,
    (fuses, expiry): (u32, u64),
) -> RawLogInput {
    raw_at(
        NameWrapped {
            node,
            name: dns.to_vec().into(),
            owner: addr(owner),
            fuses,
            expiry,
        }
        .encode_log_data(),
        block,
        log_index,
        NAME_WRAPPER,
    )
}

/// HOLDER's `wrapETH2LD` of `parent.eth`: the registrar token moves to the NameWrapper (a
/// registrar log these manifests do not watch), `reclaim` points the registry at the
/// NameWrapper, and the parent token is minted to HOLDER.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
fn wrap_parent(block: i64) -> [RawLogInput; 3] {
    [
        new_owner_under(node(&["eth"]), block, 0, "parent", NAME_WRAPPER),
        transfer_single(block, 1, HOLDER, ZERO, HOLDER, parent()),
        name_wrapped_with(
            block,
            2,
            parent(),
            b"\x06parent\x03eth\0",
            HOLDER,
            (DOT_ETH_FUSES, DOT_ETH_WRAP_EXPIRY),
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

/// Control for the creation path, in the realistic created-before-wrapped shape. It never sees
/// `NameUnwrapped`, so it guards the existing creation binding, not the unwrap successor. The
/// parent is created and known while unwrapped, the child is created by `NewOwner` under it,
/// and only then does the parent wrap. The child received a plain registry binding at creation
/// and must keep it across the parent's wrap. The parent is made known the way
/// `v1_node_identity` seeds nested paths: the root node has an empty path, so
/// `NewOwner(root, "eth")` seeds `eth`, then `NewOwner(eth, "parent")` seeds the
/// parent. Without the `eth` step `observe_identity` finds no parent path.
#[test]
fn unwrapped_child_created_under_known_unwrapped_parent_keeps_registry_binding()
-> anyhow::Result<()> {
    let eth = node(&["eth"]);
    let mut prefix = vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
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

/// After-block-3 boundary, across a restore: the unwrap has already completed
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
    // CHILD_OWNER's `wrap`: the registry write to the NameWrapper, the mint, then `NameWrapped`.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372-L374 @ ens_v1@91c966f)
    let probe = vec![
        registry_transfer(4, 0, child(), NAME_WRAPPER),
        transfer_single(4, 1, CHILD_OWNER, ZERO, CHILD_OWNER, child()),
        name_wrapped(4, 2, child(), b"\x05child\x06parent\x03eth\0", CHILD_OWNER),
    ];

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
        // The wrap transitions FROM the unwrap's registry-only successor. The wrap's own
        // registry write moves that authority and binds nothing new, so a registry-only
        // `SurfaceBound` in block 4 would mean the restore lost the successor and the write
        // opened a fresh one. The `NameWrapped` then closes the successor.
        assert!(
            !cold.normalized_events.iter().any(|event| {
                event.event_kind == "SurfaceBound"
                    && event.block_number == Some(4)
                    && event.logical_name_id.as_deref() == Some(child_id().as_str())
                    && event.after_state["authority_kind"] == "registry_only"
            }),
            "restored probe lost the child's registry-only successor: {:?}",
            cold.normalized_events
        );
        assert!(
            cold.normalized_events.iter().any(|event| {
                event.event_kind == "SurfaceUnbound"
                    && event.logical_name_id.as_deref() == Some(child_id().as_str())
                    && event.before_state["authority_kind"] == "registry_only"
            }),
            "restored probe closed no registry-only successor: {:?}",
            cold.normalized_events
        );
    }
    Ok(())
}

/// Lease boundary, live and restored: when the unwrap DOES reactivate a registrar
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
    // The registration writes the registry owner before the controller's `NameRegistered`.
    // `wrapETH2LD` and `unwrapETH2LD` move the registrar token too, a registrar log this
    // manifest does not watch.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L150 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390-L395 @ ens_v1@91c966f)
    let setup = vec![
        new_owner_under(node(&["eth"]), 1, 0, "leased", HOLDER),
        raw_at(
            NameRegistered {
                name: "leased".to_owned(),
                label: keccak256(b"leased"),
                owner: addr(HOLDER),
                expires: U256::from(42),
            }
            .encode_log_data(),
            1,
            1,
            CONTRACT,
        ),
        new_owner_under(node(&["eth"]), 2, 0, "leased", NAME_WRAPPER),
        transfer_single(2, 1, HOLDER, ZERO, HOLDER, leased),
        name_wrapped_with(
            2,
            2,
            leased,
            b"\x06leased\x03eth\0",
            HOLDER,
            (DOT_ETH_FUSES, 42 + 90 * 86_400),
        ),
    ];
    let tail = vec![
        transfer_single(3, 0, HOLDER, HOLDER, ZERO, leased),
        registry_transfer(3, 1, leased, HOLDER),
        name_unwrapped(3, 2, leased, HOLDER),
    ];

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
        // The unwrap at block 3. The wrap at block 2 binds a registry-only authority for its
        // `reclaim` write before `NameWrapped` replaces it, which this test does not cover.
        assert!(
            !output.normalized_events.iter().any(|event| {
                event.event_kind == "SurfaceBound"
                    && event.block_number == Some(3)
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

/// A wrapped second-level name under a non-eth suffix has no BaseRegistrar
/// and no following registrar transfer. Its holder's `wrap` emits expiry 0,
/// and the name is not under eth, so the `.eth` prediction cannot apply. With
/// no registrar identity its unwrap must activate the registry-only successor
/// instead of deferring to a transfer that cannot follow.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372-L374 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
#[test]
fn wrapped_second_level_non_eth_unwrapped_without_a_lease_binds_registry_only() -> anyhow::Result<()>
{
    let wrapped = node(&["direct", "xyz"]);
    let wrapped_id = format!("ens:{wrapped:#x}");
    let logs = vec![
        registry_transfer(1, 0, wrapped, NAME_WRAPPER),
        transfer_single(1, 1, HOLDER, ZERO, HOLDER, wrapped),
        name_wrapped(1, 2, wrapped, b"\x06direct\x03xyz\0", HOLDER),
        transfer_single(2, 0, HOLDER, HOLDER, ZERO, wrapped),
        registry_transfer(2, 1, wrapped, HOLDER),
        name_unwrapped(2, 2, wrapped, HOLDER),
    ];
    let output = interpret(logs)?;

    assert_bound(&output, &wrapped_id);
    let binding = output
        .surface_bindings
        .iter()
        .find(|binding| binding.logical_name_id == wrapped_id)
        .expect("the unwrapped non-eth name keeps a surface binding");
    assert_eq!(binding.binding_kind, "declared_registry_path");
    let bound = output
        .normalized_events
        .iter()
        .rfind(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(wrapped_id.as_str())
        })
        .expect("the unwrapped non-eth name has a SurfaceBound");
    assert_eq!(bound.after_state["authority_kind"], "registry_only");
    Ok(())
}

const AWAY_OWNER: &str = "0x00000000000000000000000000000000000000e1";
const TRANSFEREE: &str = "0x00000000000000000000000000000000000000e2";
const PROBE_OWNER: &str = "0x00000000000000000000000000000000000000e3";

/// Blocks 1-2: a known unwrapped parent, and its child created for CHILD_OWNER and then
/// wrapped by CHILD_OWNER through `wrap`, which moves the registry record to the NameWrapper
/// and mints the token.
fn custody_setup() -> Vec<RawLogInput> {
    vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
        new_owner_under(node(&["eth"]), 1, 1, "parent", HOLDER),
        new_owner(2, 0, "child", CHILD_OWNER),
        new_resolver(2, 1, child(), RESOLVER),
        registry_transfer(2, 2, child(), NAME_WRAPPER),
        transfer_single(2, 3, CHILD_OWNER, ZERO, CHILD_OWNER, child()),
        name_wrapped(2, 4, child(), b"\x05child\x06parent\x03eth\0", CHILD_OWNER),
    ]
}

/// The unwrap transaction `_unwrap` emits: the burn, the registry owner write, then
/// `NameUnwrapped`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
fn unwrap_by(block: i64, owner: &str) -> Vec<RawLogInput> {
    vec![
        transfer_single(block, 0, owner, owner, ZERO, child()),
        registry_transfer(block, 1, child(), owner),
        name_unwrapped(block, 2, child(), owner),
    ]
}

/// The custody paths into the final unwrap at block 5, with the owner that unwrap leaves.
fn custody_paths() -> Vec<(&'static str, Vec<RawLogInput>, &'static str)> {
    let rewrap = vec![
        registry_transfer(4, 0, child(), NAME_WRAPPER),
        transfer_single(4, 1, CHILD_OWNER, ZERO, CHILD_OWNER, child()),
        name_wrapped(4, 2, child(), b"\x05child\x06parent\x03eth\0", CHILD_OWNER),
    ];
    vec![
        // The parent owner's `setSubnodeOwner` moves the registry record away and back
        // without touching the token, so the registry writes select the registry-only
        // authority before the unwrap.
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
        (
            "away and back",
            vec![
                new_owner(3, 0, "child", AWAY_OWNER),
                new_owner(4, 0, "child", NAME_WRAPPER),
            ],
            CHILD_OWNER,
        ),
        (
            "unwrap and rewrap",
            unwrap_by(3, CHILD_OWNER)
                .into_iter()
                .chain(rewrap)
                .collect(),
            CHILD_OWNER,
        ),
        (
            "transfer through the wrapper",
            vec![transfer_single(
                3,
                0,
                CHILD_OWNER,
                CHILD_OWNER,
                TRANSFEREE,
                child(),
            )],
            TRANSFEREE,
        ),
        // `wrap` had the NameWrapper write the registry for the holder, so the holder approved
        // it as a registry operator. When the parent owner hands the record to the holder, the
        // holder's unwrap writes the registry through that approval, and the record never
        // returns to the NameWrapper.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L364-L372 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L21 @ ens_v1@91c966f)
        (
            "away, unwrapped while away",
            vec![new_owner(3, 0, "child", CHILD_OWNER)],
            CHILD_OWNER,
        ),
        // A different owner wraps the record it received. `_mint` burns the earlier token
        // and emits `NameUnwrapped` with the zero owner before it mints the new one.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L891 @ ens_v1@91c966f)
        (
            "away, then wrapped by its new owner",
            vec![
                new_owner(3, 0, "child", AWAY_OWNER),
                registry_transfer(4, 0, child(), NAME_WRAPPER),
                transfer_single(4, 1, AWAY_OWNER, CHILD_OWNER, ZERO, child()),
                name_unwrapped(4, 2, child(), ZERO),
                transfer_single(4, 3, AWAY_OWNER, ZERO, AWAY_OWNER, child()),
                name_wrapped(4, 4, child(), b"\x05child\x06parent\x03eth\0", AWAY_OWNER),
            ],
            AWAY_OWNER,
        ),
        ("plain unwrap", Vec::new(), CHILD_OWNER),
    ]
}

/// The unwrap at block 5 leaves the registry-only authority bound: any binding it closes it
/// reopens for the registry-only successor in the same block, and the later registry owner
/// change at block 6 transfers that authority instead of binding the name again.
fn assert_unwrap_keeps_registry_authority(output: &BatchOutput, path: &str) {
    let child_events = |block: i64| {
        output
            .normalized_events
            .iter()
            .filter(move |event| {
                event.block_number == Some(block)
                    && event.logical_name_id.as_deref() == Some(child_id().as_str())
                    && matches!(event.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound")
            })
            .collect::<Vec<_>>()
    };
    let at_unwrap = child_events(5);
    if let Some(last) = at_unwrap.last() {
        assert!(
            last.event_kind == "SurfaceBound"
                && last.after_state["authority_kind"] == "registry_only",
            "{path}: the unwrap left the child unbound: {at_unwrap:?}"
        );
    }
    assert!(
        child_events(6).is_empty(),
        "{path}: the registry owner change after the unwrap bound the child again: {:?}",
        child_events(6)
    );
    assert!(
        output.surface_bindings.iter().any(|binding| {
            binding.logical_name_id == child_id()
                && binding.binding_kind == "declared_registry_path"
        }),
        "{path}: the child has no registry binding: {:?}",
        output.surface_bindings
    );
}

fn custody_blocks(numbers: &[i64]) -> Vec<RawBlockInput> {
    numbers
        .iter()
        .map(|&number| RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: format!("block-{number}"),
            block_number: number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
            canonicality_state: "canonical".to_owned(),
        })
        .collect()
}

/// Every custody path ends in a successful unwrap that leaves an ordinary registry owner, so
/// the name stays bound to the registry-only authority: in one batch, split before the unwrap
/// and restored after it, live and from prior events.
#[test]
fn every_custody_path_into_a_no_lease_unwrap_keeps_the_registry_authority() -> anyhow::Result<()> {
    for (path, custody, owner) in custody_paths() {
        let mut head = custody_setup();
        head.extend(custody);
        let unwrap = unwrap_by(5, owner);
        let probe = vec![registry_transfer(6, 0, child(), PROBE_OWNER)];
        let all: Vec<_> = head.iter().chain(&unwrap).chain(&probe).cloned().collect();
        let full = interpret(all)?;
        assert_unwrap_keeps_registry_authority(&full, path);
        for (before, after) in [
            (head.clone(), [unwrap.clone(), probe.clone()].concat()),
            ([head.clone(), unwrap.clone()].concat(), probe.clone()),
        ] {
            let from = after[0].block_number;
            let expected: Vec<_> = full
                .normalized_events
                .iter()
                .filter(|event| event.block_number >= Some(from))
                .cloned()
                .collect();
            let (before_output, session) = interpret_test_batch_incremental(batch(before), None)?;
            let (live, _) = interpret_test_batch_incremental(batch(after.clone()), Some(session))?;
            assert_eq!(
                live.normalized_events, expected,
                "{path}: live split at {from}"
            );
            let held: Vec<i64> = (1..from).collect();
            for prior in [
                before_output
                    .normalized_events
                    .iter()
                    .map(prior_event)
                    .collect::<Vec<_>>(),
                seam::fold_prior_events(
                    Vec::new(),
                    &before_output.normalized_events,
                    &custody_blocks(&held),
                )?,
            ] {
                let mut cold_input = batch(after.clone());
                cold_input.prior_events = prior;
                let cold = interpret_test_batch(cold_input)?;
                assert_eq!(
                    cold.normalized_events, expected,
                    "{path}: restored split at {from}"
                );
                assert_eq!(
                    cold.surface_bindings, live.surface_bindings,
                    "{path}: restored bindings at {from}"
                );
            }
        }
    }
    Ok(())
}

/// A full `unwrapETH2LD` of a wrapped `.eth` name with no registrar identity: the burn, the
/// registry write and `NameUnwrapped`, then the BaseRegistrar token moves to the registrant.
/// The registrar transfer takes the name over, so no registry-only interval opens at the
/// unwrap, not even one closed again in the same transaction. The name was wrapped by
/// `wrapETH2LD` at block 1, before the registrar's admission starts at block 2, so its
/// registrar transfer into the NameWrapper materializes no registrar identity.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390-L395 @ ens_v1@91c966f)
#[test]
fn eth_second_level_unwrap_without_a_registrar_identity_opens_no_registry_only_interval()
-> anyhow::Result<()> {
    let wrapped = node(&["direct", "eth"]);
    let wrapped_id = format!("ens:{wrapped:#x}");
    let mut manifests = manifests();
    manifests.push(manifest(
        REGISTRAR_MANIFEST,
        "ens_v1_registrar_l1",
        "Transfer",
        "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
        &["registrar"],
        &[
            "TokenControlTransferred",
            "PermissionChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "ResolverChanged",
        ],
    ));
    let mut admissions = admissions();
    let mut registrar = admission(REGISTRAR_MANIFEST, "registrar");
    registrar.active_from_block = Some(2);
    admissions.push(registrar);
    let registrar_transfer = |block: i64, log_index: i64, from: &str, to: &str| {
        raw_at(
            v1_registrar::Transfer {
                from: addr(from),
                to: addr(to),
                tokenId: U256::from_be_bytes(keccak256(b"direct").0),
            }
            .encode_log_data(),
            block,
            log_index,
            CONTRACT,
        )
    };
    let logs = vec![
        registrar_transfer(1, 0, HOLDER, NAME_WRAPPER),
        new_owner_under(node(&["eth"]), 1, 1, "direct", NAME_WRAPPER),
        transfer_single(1, 2, HOLDER, ZERO, HOLDER, wrapped),
        name_wrapped_with(
            1,
            3,
            wrapped,
            b"\x06direct\x03eth\0",
            HOLDER,
            (DOT_ETH_FUSES, 1_000_000_000),
        ),
        transfer_single(2, 0, HOLDER, HOLDER, ZERO, wrapped),
        registry_transfer(2, 1, wrapped, HOLDER),
        name_unwrapped(2, 2, wrapped, HOLDER),
        registrar_transfer(2, 3, NAME_WRAPPER, HOLDER),
    ];
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: logs,
    })?;
    assert!(
        !output.normalized_events.iter().any(|event| {
            event.block_number == Some(2)
                && event.logical_name_id.as_deref() == Some(wrapped_id.as_str())
                && event.after_state["authority_kind"] == "registry_only"
        }),
        "a registry-only interval opened at the .eth unwrap: {:?}",
        output.normalized_events
    );
    let bound = output
        .normalized_events
        .iter()
        .rfind(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(wrapped_id.as_str())
        })
        .expect("the unwrapped .eth name is bound after the registrar transfer");
    assert_eq!(bound.block_number, Some(2));
    assert_eq!(bound.after_state["authority_kind"], "registrar");
    Ok(())
}
