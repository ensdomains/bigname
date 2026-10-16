//! An unwrapped ENSv1 subname whose parent is NameWrapper-owned must still receive a
//! registry-only surface binding, whether it was created under the parent before the parent
//! wrapped, or created wrapped under the wrapped parent and then unwrapped. Without a binding
//! the composed row has no `surface_binding_id`, `resource_id` or `binding_kind`, its
//! lifecycle status composes as `unregistered`, and `relations()`
//! (crates/storage/src/families/records/address_relations.rs) serves no relations for its
//! owner. The surface-less registry-children fallback cannot rescue it once a surface exists.
//!
//! A child created before its parent's label path was ever observed gets no
//! surface. That shape is out of scope here and is not covered.

use super::*;

const REGISTRY_MANIFEST: i64 = 7401;
const WRAPPER_MANIFEST: i64 = 7402;
const REGISTRAR_MANIFEST: i64 = 7403;
const REGISTRY: &str = "0x00000000000000000000000000000000000000c1";
const NAME_WRAPPER: &str = "0x00000000000000000000000000000000000000c2";
const HOLDER: &str = "0x00000000000000000000000000000000000000a1";
const TLD_OWNER: &str = "0x00000000000000000000000000000000000000a2";
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
                (
                    "FusesSet",
                    "event FusesSet(bytes32 indexed node, uint32 fuses)",
                    &["name_wrapper"],
                    &["PermissionScopeChanged"],
                ),
                (
                    "ExpiryExtended",
                    "event ExpiryExtended(bytes32 indexed node, uint64 expiry)",
                    &["name_wrapper"],
                    &["ExpiryChanged"],
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
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
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
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372-L374 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L577-L581 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L610-L619 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L978-L994 @ ens_v1@91c966f)
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

/// The NameWrapper's wrap is the first registry write of `direct.xyz` seen, so the
/// registry-only resource is remembered before the name's surface is known, and `NameWrapped`
/// then makes it known. The unwrap's registry write grants HOLDER the registry-only resource,
/// and those grant rows carry the name id, in one batch and cut after the wrap, resumed warm
/// and restored cold, plain and folded.
#[test]
fn unwrap_grant_on_the_registry_only_resource_names_a_known_surface() -> anyhow::Result<()> {
    let wrapped = node(&["direct", "xyz"]);
    let wrapped_id = format!("ens:{wrapped:#x}");
    let registry_only =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{wrapped:#x}"));
    let wrap = vec![
        registry_transfer(1, 0, wrapped, NAME_WRAPPER),
        transfer_single(1, 1, HOLDER, ZERO, HOLDER, wrapped),
        name_wrapped(1, 2, wrapped, b"\x06direct\x03xyz\0", HOLDER),
    ];
    let unwrap = vec![
        transfer_single(2, 0, HOLDER, HOLDER, ZERO, wrapped),
        registry_transfer(2, 1, wrapped, HOLDER),
        name_unwrapped(2, 2, wrapped, HOLDER),
    ];
    let mut failures = Vec::new();
    for (layout, outputs) in every_layout(&batch, &wrap, &[unwrap])? {
        let grants = permission_rows_at(&outputs, 2, |event| {
            event.resource_id == Some(registry_only)
                && event.after_state["subject"] == HOLDER
                && event.after_state["effective_powers"]
                    .as_array()
                    .is_some_and(|powers| !powers.is_empty())
        });
        if grants.is_empty()
            || grants
                .iter()
                .any(|event| event.logical_name_id.as_deref() != Some(wrapped_id.as_str()))
        {
            failures.push(format!("{layout}: {grants:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The `.eth` prediction is limited to names under eth, and a non-eth second-level name can
/// carry a live wrapper expiry. The registry owner of a top-level name wraps it, which stores
/// expiry 0. For a child of the root, `setChildFuses` checks the name's own owner, and the
/// root's stored expiry is the maximum, so that owner raises its own expiry. It then creates a
/// wrapped second-level name with a live expiry, capped at its own. With no registrar identity,
/// the unwrap of that name must still activate the registry-only successor.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L76-L81 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372-L374 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L532-L545 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1041-L1045 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L577-L581 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1028-L1031 @ ens_v1@91c966f)
#[test]
fn non_eth_second_level_name_with_a_live_wrapper_expiry_unwraps_to_registry_only()
-> anyhow::Result<()> {
    const LIVE_EXPIRY: u64 = 4_000_000_000;
    let tld = node(&["xyz"]);
    let wrapped = node(&["direct", "xyz"]);
    let wrapped_id = format!("ens:{wrapped:#x}");
    let logs = vec![
        registry_transfer(1, 0, tld, NAME_WRAPPER),
        transfer_single(1, 1, TLD_OWNER, ZERO, TLD_OWNER, tld),
        name_wrapped(1, 2, tld, b"\x03xyz\0", TLD_OWNER),
        raw_at(
            FusesSet {
                node: tld,
                fuses: 0,
            }
            .encode_log_data(),
            2,
            0,
            NAME_WRAPPER,
        ),
        raw_at(
            ExpiryExtended {
                node: tld,
                expiry: LIVE_EXPIRY,
            }
            .encode_log_data(),
            2,
            1,
            NAME_WRAPPER,
        ),
        new_owner_under(tld, 3, 0, "direct", NAME_WRAPPER),
        transfer_single(3, 1, TLD_OWNER, ZERO, HOLDER, wrapped),
        name_wrapped_with(
            3,
            2,
            wrapped,
            b"\x06direct\x03xyz\0",
            HOLDER,
            (0, LIVE_EXPIRY),
        ),
        transfer_single(4, 0, HOLDER, HOLDER, ZERO, wrapped),
        registry_transfer(4, 1, wrapped, HOLDER),
        name_unwrapped(4, 2, wrapped, HOLDER),
    ];
    let output = interpret(logs)?;

    assert_bound(&output, &wrapped_id);
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "SurfaceBound"
                && event.block_number == Some(4)
                && event.logical_name_id.as_deref() == Some(wrapped_id.as_str())
                && event.after_state["authority_kind"] == "registry_only"
        }),
        "the unwrap of a non-eth name with a live wrapper expiry opened no registry-only successor: {:?}",
        output.normalized_events
    );
    Ok(())
}

const AWAY_OWNER: &str = "0x00000000000000000000000000000000000000e1";
const TRANSFEREE: &str = "0x00000000000000000000000000000000000000e2";
const PROBE_OWNER: &str = "0x00000000000000000000000000000000000000e3";

/// Blocks 1-2: a known unwrapped parent, and its child created for CHILD_OWNER and then
/// wrapped by CHILD_OWNER through `wrap`, which moves the registry record to the NameWrapper
/// and mints the token.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L364-L374 @ ens_v1@91c966f)
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
    let build = |raw_logs: Vec<RawLogInput>| BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    };
    let output = interpret_test_batch(build(logs.clone()))?;
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
    // The NameWrapper's block 1 registry write grants it the registry-only resource, since no
    // registrar identity is selected yet. The unwrap's registry write ends those grants, and
    // the registrar transfer selects the lease, so that resource holds nothing afterwards, in
    // one batch and cut after the wrap, resumed warm and restored cold.
    let registry_only =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{wrapped:#x}"));
    let mut failures = Vec::new();
    for (layout, grants) in
        grants_in_every_layout(&build, &logs[..4], &[logs[4..].to_vec()], registry_only)?
    {
        if !grants.is_empty() {
            failures.push(format!("{layout}: {grants:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Live direct grants on one resource, as (subject, scope kind, powers).
pub(super) type LiveGrants = std::collections::BTreeSet<(String, String, Vec<String>)>;

/// The child's registry-only resource: the one its first registry write binds.
fn registry_only_resource(output: &BatchOutput) -> uuid::Uuid {
    output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(child_id().as_str())
                && event.after_state["authority_kind"] == "registry_only"
        })
        .and_then(|event| event.resource_id)
        .expect("the child's registry write binds a registry-only resource")
}

/// Every subject's live direct grants on `resource` after `outputs`, read in order, as
/// (subject, scope kind, powers). The newest `PermissionChanged` row of each subject and
/// scope wins, and an empty power list is a revocation.
fn live_grants_on(outputs: &[&BatchOutput], resource: uuid::Uuid) -> LiveGrants {
    let mut newest = std::collections::BTreeMap::new();
    for event in outputs
        .iter()
        .flat_map(|output| output.normalized_events.iter())
        .filter(|event| {
            event.event_kind == "PermissionChanged" && event.resource_id == Some(resource)
        })
    {
        let state = &event.after_state;
        newest.insert(
            (
                state["subject"].as_str().unwrap_or_default().to_owned(),
                state["scope"].to_string(),
            ),
            (
                state["scope"]["kind"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                state["effective_powers"]
                    .as_array()
                    .map(|powers| {
                        powers
                            .iter()
                            .filter_map(|power| power.as_str().map(str::to_owned))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
            ),
        );
    }
    newest
        .into_iter()
        .filter(|(_, (_, powers))| !powers.is_empty())
        .map(|((subject, _), (kind, powers))| (subject, kind, powers))
        .collect()
}

/// `owner`'s registry-only grants on the child: control of the resource and of its resolver.
fn registry_owner_grants(owner: &str) -> LiveGrants {
    [
        (
            owner.to_owned(),
            "resource".to_owned(),
            vec!["resource_control".to_owned()],
        ),
        (
            owner.to_owned(),
            "resolver".to_owned(),
            vec!["resolver_control".to_owned()],
        ),
    ]
    .into_iter()
    .collect()
}

/// The child's setup without its wrap: the known unwrapped parent of HOLDER, and the child
/// created for CHILD_OWNER with a resolver.
fn unwrapped_child_setup() -> Vec<RawLogInput> {
    custody_setup().into_iter().take(4).collect()
}

/// CHILD_OWNER's `wrap` of the child to itself at `block`: the registry write to the
/// NameWrapper, the mint, then `NameWrapped`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L364-L374 @ ens_v1@91c966f)
fn wrap_child(block: i64) -> Vec<RawLogInput> {
    vec![
        registry_transfer(block, 0, child(), NAME_WRAPPER),
        transfer_single(block, 1, CHILD_OWNER, ZERO, CHILD_OWNER, child()),
        name_wrapped(
            block,
            2,
            child(),
            b"\x05child\x06parent\x03eth\0",
            CHILD_OWNER,
        ),
    ]
}

/// `holder`'s `unwrap` of the child to `to` at `block`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
fn unwrap_child(block: i64, holder: &str, to: &str) -> Vec<RawLogInput> {
    vec![
        transfer_single(block, 0, holder, holder, ZERO, child()),
        registry_transfer(block, 1, child(), to),
        name_unwrapped(block, 2, child(), to),
    ]
}

/// The child's registry-only resource carries the grants of the child's ENSv1 registry owner,
/// whichever authority is selected. `wrap` makes the NameWrapper the registry owner, so while
/// the child is wrapped that resource holds the NameWrapper's grants and nobody else's.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
#[test]
fn wrap_moves_the_registry_only_grants_to_the_name_wrapper() -> anyhow::Result<()> {
    let setup = interpret(unwrapped_child_setup())?;
    let resource = registry_only_resource(&setup);
    assert_eq!(
        live_grants_on(&[&setup], resource),
        registry_owner_grants(CHILD_OWNER),
        "the unwrapped child's registry owner holds its resource"
    );
    let mut logs = unwrapped_child_setup();
    logs.extend(wrap_child(3));
    let wrapped = interpret(logs)?;
    assert_eq!(
        live_grants_on(&[&wrapped], resource),
        registry_owner_grants(NAME_WRAPPER),
        "while wrapped, the registry-only resource holds more than the registry owner's grants"
    );
    Ok(())
}

/// The same through a wrapped parent's `setSubnodeOwner` over the existing unwrapped child:
/// the NameWrapper writes the registry to itself with `NewOwner`, then mints and wraps. The
/// holder's later unwrap to TRANSFEREE leaves that resource with TRANSFEREE's grants only, in
/// one batch and cut after the wrap, resumed warm and restored cold.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L577-L581 @ ens_v1@91c966f)
#[test]
fn set_subnode_owner_wrap_keeps_the_registry_only_grants_on_the_registry_owner()
-> anyhow::Result<()> {
    let mut logs = vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
        new_owner_under(node(&["eth"]), 1, 1, "parent", HOLDER),
        new_owner(2, 0, "child", CHILD_OWNER),
        new_resolver(2, 1, child(), RESOLVER),
    ];
    logs.extend(wrap_parent(3));
    let before = interpret(logs.clone())?;
    let resource = registry_only_resource(&before);
    logs.extend([
        new_owner(4, 0, "child", NAME_WRAPPER),
        transfer_single(4, 1, HOLDER, ZERO, CHILD_OWNER, child()),
        name_wrapped(4, 2, child(), b"\x05child\x06parent\x03eth\0", CHILD_OWNER),
    ]);
    let wrapped = interpret(logs.clone())?;
    assert_eq!(
        live_grants_on(&[&wrapped], resource),
        registry_owner_grants(NAME_WRAPPER),
        "while wrapped, the registry-only resource holds more than the registry owner's grants"
    );
    let mut failures = Vec::new();
    for (layout, grants) in grants_in_every_layout(
        &batch,
        &logs,
        &[unwrap_child(5, CHILD_OWNER, TRANSFEREE)],
        resource,
    )? {
        if grants != scoped_registry_owner_grants(TRANSFEREE, RESOLVER) {
            failures.push(format!("{layout}: {grants:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Ways authority leaves the registry-only resource that keep no grants behind: a registry
/// owner change to another account, a zero-owner write, and a registrar registration taking
/// over a registry-only `.eth` name. Each control ends with only the expected holder.
#[test]
fn registry_only_handoffs_without_a_wrap_keep_no_stale_grants() -> anyhow::Result<()> {
    let setup = interpret(unwrapped_child_setup())?;
    let resource = registry_only_resource(&setup);
    let mut moved = unwrapped_child_setup();
    moved.push(registry_transfer(3, 0, child(), AWAY_OWNER));
    assert_eq!(
        live_grants_on(&[&interpret(moved)?], resource),
        registry_owner_grants(AWAY_OWNER),
        "owner change"
    );
    let mut closed = unwrapped_child_setup();
    closed.push(registry_transfer(3, 0, child(), ZERO));
    assert_eq!(
        live_grants_on(&[&interpret(closed.clone())?], resource),
        std::collections::BTreeSet::new(),
        "zero-owner close"
    );
    closed.push(new_owner(4, 0, "child", AWAY_OWNER));
    assert_eq!(
        live_grants_on(&[&interpret(closed)?], resource),
        registry_owner_grants(AWAY_OWNER),
        "reopened after a zero-owner close"
    );
    Ok(())
}

const NEXT_RESOLVER: &str = "0x00000000000000000000000000000000000000d3";

/// Every subject's live direct grants on `resource` after `outputs`, as in `live_grants_on`,
/// with a resolver grant's scope naming its resolver.
pub(super) fn scoped_grants_on(outputs: &[&BatchOutput], resource: uuid::Uuid) -> LiveGrants {
    let mut newest = std::collections::BTreeMap::new();
    for event in outputs
        .iter()
        .flat_map(|output| output.normalized_events.iter())
        .filter(|event| {
            event.event_kind == "PermissionChanged" && event.resource_id == Some(resource)
        })
    {
        let state = &event.after_state;
        let scope = match state["scope"]["resolver_address"].as_str() {
            Some(resolver) => format!("resolver:{}", resolver.to_ascii_lowercase()),
            None => state["scope"]["kind"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        };
        let powers = state["effective_powers"]
            .as_array()
            .map(|powers| {
                powers
                    .iter()
                    .filter_map(|power| power.as_str().map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        newest.insert(
            (
                state["subject"].as_str().unwrap_or_default().to_owned(),
                scope,
            ),
            powers,
        );
    }
    newest
        .into_iter()
        .filter(|(_, powers)| !powers.is_empty())
        .map(|((subject, scope), powers)| (subject, scope, powers))
        .collect()
}

/// `owner`'s registry-only grants on the child with `resolver` set, scoped as in
/// `scoped_grants_on`.
fn scoped_registry_owner_grants(owner: &str, resolver: &str) -> LiveGrants {
    [
        (
            owner.to_owned(),
            "resource".to_owned(),
            vec!["resource_control".to_owned()],
        ),
        (
            owner.to_owned(),
            format!("resolver:{resolver}"),
            vec!["resolver_control".to_owned()],
        ),
    ]
    .into_iter()
    .collect()
}

/// The blocks of `logs`, once each, in log order.
fn blocks_of(logs: &[RawLogInput]) -> Vec<RawBlockInput> {
    let mut seen = std::collections::BTreeSet::new();
    logs.iter()
        .filter(|raw| seen.insert(raw.block_number))
        .map(|raw| RawBlockInput {
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            block_timestamp: raw.block_timestamp,
            canonicality_state: raw.canonicality_state.clone(),
        })
        .collect()
}

/// The outputs of every layout of `prefix` followed by `steps`, each batch built by `build`:
/// one batch, and a cut after the prefix and after each step but the last, each resumed warm,
/// restored cold from the head's rows, and restored cold from those rows folded as the
/// retained state keeps them.
pub(super) fn every_layout(
    build: &dyn Fn(Vec<RawLogInput>) -> BatchInput,
    prefix: &[RawLogInput],
    steps: &[Vec<RawLogInput>],
) -> anyhow::Result<Vec<(String, Vec<BatchOutput>)>> {
    let all = |range: std::ops::Range<usize>| -> Vec<RawLogInput> {
        steps[range].iter().flatten().cloned().collect()
    };
    let mut whole = prefix.to_vec();
    whole.extend(all(0..steps.len()));
    let mut layouts = vec![(
        "one batch".to_owned(),
        vec![interpret_test_batch(build(whole))?],
    )];
    for cut in 0..steps.len() {
        let point = if cut == 0 {
            "the wrap".to_owned()
        } else {
            format!("step {cut}")
        };
        let mut head_logs = prefix.to_vec();
        head_logs.extend(all(0..cut));
        let tail_logs = all(cut..steps.len());
        let (head, session) = interpret_test_batch_incremental(build(head_logs.clone()), None)?;
        let (warm, _) = interpret_test_batch_incremental(build(tail_logs.clone()), Some(session))?;
        let mut cold_input = build(tail_logs.clone());
        cold_input.prior_events = head.normalized_events.iter().map(prior_event).collect();
        let cold = interpret_test_batch(cold_input)?;
        let mut folded_input = build(tail_logs);
        folded_input.prior_events = crate::schema_v2::seam::fold_prior_events(
            Vec::new(),
            &head.normalized_events,
            &blocks_of(&head_logs),
        )?;
        let folded = interpret_test_batch(folded_input)?;
        layouts.push((format!("cut after {point}, warm"), vec![head.clone(), warm]));
        layouts.push((format!("cut after {point}, cold"), vec![head.clone(), cold]));
        layouts.push((format!("cut after {point}, folded"), vec![head, folded]));
    }
    Ok(layouts)
}

/// The grants on `resource` in every layout of `prefix` followed by `steps`, as in
/// `every_layout`.
fn grants_in_every_layout(
    build: &dyn Fn(Vec<RawLogInput>) -> BatchInput,
    prefix: &[RawLogInput],
    steps: &[Vec<RawLogInput>],
    resource: uuid::Uuid,
) -> anyhow::Result<Vec<(String, LiveGrants)>> {
    Ok(every_layout(build, prefix, steps)?
        .into_iter()
        .map(|(layout, outputs)| {
            let outputs = outputs.iter().collect::<Vec<_>>();
            (layout, scoped_grants_on(&outputs, resource))
        })
        .collect())
}

/// The `PermissionChanged` rows of `outputs` at `block` that `filter` selects.
fn permission_rows_at(
    outputs: &[BatchOutput],
    block: i64,
    filter: impl Fn(&NormalizedEvent) -> bool,
) -> Vec<&NormalizedEvent> {
    outputs
        .iter()
        .flat_map(|output| output.normalized_events.iter())
        .filter(|event| {
            event.event_kind == "PermissionChanged" && event.block_number == Some(block)
        })
        .filter(|event| filter(event))
        .collect()
}

/// Every way the registry-only resource reopens after a wrap carries only its new owner's
/// grants, because the registry record has left the NameWrapper: the holder's unwrap to
/// another account or to itself, the parent's registry write handing the wrapped record to
/// another account, the parent's zero-owner write followed by a new owner, and an unwrap after
/// a resolver change made while wrapped. Each handoff is followed by the new owner's resolver
/// change, so a cut after the handoff carries the moved grants into a later write. Every row
/// runs in one batch and cut after the wrap and after each step, resumed warm and restored
/// cold.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
#[test]
fn registry_only_resource_reopened_after_a_wrap_holds_only_its_new_owner() -> anyhow::Result<()> {
    let setup = interpret(unwrapped_child_setup())?;
    let resource = registry_only_resource(&setup);
    let mut wrapped = unwrapped_child_setup();
    wrapped.extend(wrap_child(3));
    let rows = [
        (
            "unwrap to another account",
            vec![
                unwrap_child(4, CHILD_OWNER, TRANSFEREE),
                vec![new_resolver(6, 0, child(), NEXT_RESOLVER)],
            ],
            TRANSFEREE,
            NEXT_RESOLVER,
        ),
        (
            "unwrap to the holder",
            vec![
                unwrap_child(4, CHILD_OWNER, CHILD_OWNER),
                vec![new_resolver(6, 0, child(), NEXT_RESOLVER)],
            ],
            CHILD_OWNER,
            NEXT_RESOLVER,
        ),
        (
            "parent hands the wrapped record to another account",
            vec![
                vec![new_owner(4, 0, "child", AWAY_OWNER)],
                vec![new_resolver(6, 0, child(), NEXT_RESOLVER)],
            ],
            AWAY_OWNER,
            NEXT_RESOLVER,
        ),
        (
            "parent zero-owner write, then a new owner",
            vec![
                vec![new_owner(4, 0, "child", ZERO)],
                vec![new_owner(5, 0, "child", AWAY_OWNER)],
                vec![new_resolver(6, 0, child(), NEXT_RESOLVER)],
            ],
            AWAY_OWNER,
            NEXT_RESOLVER,
        ),
        (
            "resolver change while wrapped, then an unwrap to another account",
            vec![
                vec![new_resolver(4, 0, child(), NEXT_RESOLVER)],
                unwrap_child(5, CHILD_OWNER, TRANSFEREE),
            ],
            TRANSFEREE,
            NEXT_RESOLVER,
        ),
    ];
    let mut failures = Vec::new();
    for (row, steps, owner, resolver) in rows {
        for (layout, grants) in grants_in_every_layout(&batch, &wrapped, &steps, resource)? {
            if grants != scoped_registry_owner_grants(owner, resolver) {
                failures.push(format!("{row}, {layout}: {grants:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// While the child stays wrapped, the registry-only resource follows the NameWrapper's own
/// registry writes: a resolver change moves its resolver control to the new resolver, in
/// every layout.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666-L671 @ ens_v1@91c966f)
#[test]
fn resolver_change_while_wrapped_moves_the_name_wrappers_resolver_grant() -> anyhow::Result<()> {
    let setup = interpret(unwrapped_child_setup())?;
    let resource = registry_only_resource(&setup);
    let mut wrapped = unwrapped_child_setup();
    wrapped.extend(wrap_child(3));
    let mut failures = Vec::new();
    for (layout, grants) in grants_in_every_layout(
        &batch,
        &wrapped,
        &[vec![new_resolver(4, 0, child(), NEXT_RESOLVER)]],
        resource,
    )? {
        if grants != scoped_registry_owner_grants(NAME_WRAPPER, NEXT_RESOLVER) {
            failures.push(format!("{layout}: {grants:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The NameWrapper's `setRecord` rewrites the registry record to itself before it moves the
/// token, so that write keeps the wrapper authority and the NameWrapper stays the registry
/// owner. It emits no permission row at all, no token approval row among them, and the
/// registry-only resource keeps the NameWrapper's grants, with resolver control moved to the
/// new resolver by the resolver write. With the zero owner, `setRecord` then unwraps to the
/// zero address, and that write ends the NameWrapper's grants on the registry-only resource.
/// Every row runs in one batch and cut after the wrap, resumed warm and restored cold.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L637-L660 @ ens_v1@91c966f)
#[test]
fn name_wrapper_record_rewrite_keeps_the_registry_only_grants_on_the_name_wrapper()
-> anyhow::Result<()> {
    let setup = interpret(unwrapped_child_setup())?;
    let resource = registry_only_resource(&setup);
    let mut wrapped = unwrapped_child_setup();
    wrapped.extend(wrap_child(3));
    let rewrite = vec![
        registry_transfer(4, 0, child(), NAME_WRAPPER),
        new_resolver(4, 1, child(), NEXT_RESOLVER),
    ];
    let mut failures = Vec::new();
    for (layout, outputs) in every_layout(&batch, &wrapped, &[rewrite])? {
        let rewrite_rows = permission_rows_at(&outputs, 4, |event| {
            event.log_index == Some(0)
                || event.after_state["effective_powers"]
                    .as_array()
                    .is_some_and(|powers| powers.iter().any(|power| power == "token_approval"))
        });
        if !rewrite_rows.is_empty() {
            failures.push(format!("rewrite, {layout}: {rewrite_rows:?}"));
        }
        let outputs = outputs.iter().collect::<Vec<_>>();
        let grants = scoped_grants_on(&outputs, resource);
        if grants != scoped_registry_owner_grants(NAME_WRAPPER, NEXT_RESOLVER) {
            failures.push(format!("rewrite, {layout}: {grants:?}"));
        }
    }
    let unwrap_to_zero = vec![
        registry_transfer(4, 0, child(), NAME_WRAPPER),
        transfer_single(4, 1, CHILD_OWNER, CHILD_OWNER, ZERO, child()),
        registry_transfer(4, 2, child(), ZERO),
        name_unwrapped(4, 3, child(), ZERO),
    ];
    for (layout, grants) in grants_in_every_layout(&batch, &wrapped, &[unwrap_to_zero], resource)? {
        if !grants.is_empty() {
            failures.push(format!("zero-owner setRecord, {layout}: {grants:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// A restore reads the registry owner the registry-only grants follow from the registry's
/// owner rows, not from permission rows, so a registry-family `token_approval` row among the
/// prior rows changes nothing: the cold unwrap after the wrap emits the same rows with and
/// without it.
#[test]
fn restore_ignores_registry_family_token_approval_rows() -> anyhow::Result<()> {
    let mut wrapped = unwrapped_child_setup();
    wrapped.extend(wrap_child(3));
    let head = interpret(wrapped)?;
    let prior = head
        .normalized_events
        .iter()
        .map(prior_event)
        .collect::<Vec<_>>();
    let mut approval = prior
        .iter()
        .find(|event| {
            event.event_kind == "PermissionChanged" && event.source_family == "ens_v1_registry_l1"
        })
        .cloned()
        .expect("the wrap's registry write emits a registry-family permission row");
    approval.retained_state_key = format!("{}:token-approval", approval.retained_state_key);
    approval.after_state["subject"] = json!(PROBE_OWNER);
    approval.after_state["scope"] = json!({"kind":"token"});
    approval.after_state["effective_powers"] = json!(["token_approval"]);
    let cold = |prior_events: Vec<PriorEventInput>| {
        let mut input = batch(unwrap_child(4, CHILD_OWNER, TRANSFEREE));
        input.prior_events = prior_events;
        interpret_test_batch(input)
    };
    let mut with_approval = prior.clone();
    with_approval.push(approval);
    assert_eq!(
        cold(with_approval)?.normalized_events,
        cold(prior)?.normalized_events,
        "a registry-family token_approval row changed the restored registry-only grants"
    );
    Ok(())
}

/// The batch for `logs`, with the BaseRegistrar watched as the lease tests watch it.
fn lease_batch(logs: Vec<RawLogInput>) -> BatchInput {
    let mut input = batch(logs);
    input.manifests.push(manifest_with_events(
        REGISTRAR_MANIFEST,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            (
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
            ),
        ],
    ));
    input
        .admissions
        .push(admission(REGISTRAR_MANIFEST, "registrar"));
    input
}

/// The BaseRegistrar's `Transfer` of the `leased` token from `from` to `to`.
fn lease_transfer(block: i64, log_index: i64, from: &str, to: &str) -> RawLogInput {
    raw_at(
        v1_registrar::Transfer {
            from: addr(from),
            to: addr(to),
            tokenId: U256::from_be_bytes(keccak256(b"leased").0),
        }
        .encode_log_data(),
        block,
        log_index,
        CONTRACT,
    )
}

/// HOLDER's registration of `leased.eth` with a resolver at block 1, then its `wrapETH2LD` at
/// block 2: the registrar token moves to the NameWrapper, `reclaim` points the registry at it,
/// and the wrapped token is minted.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
fn wrapped_lease_logs() -> Vec<RawLogInput> {
    let leased = node(&["leased", "eth"]);
    vec![
        new_owner_under(B256::ZERO, 1, 0, "eth", CONTRACT),
        new_owner_under(node(&["eth"]), 1, 1, "leased", HOLDER),
        raw_at(
            NameRegistered {
                name: "leased".to_owned(),
                label: keccak256(b"leased"),
                owner: addr(HOLDER),
                expires: U256::from(4_000_000_000_u64),
            }
            .encode_log_data(),
            1,
            2,
            CONTRACT,
        ),
        new_resolver(1, 3, leased, RESOLVER),
        lease_transfer(2, 0, HOLDER, NAME_WRAPPER),
        new_owner_under(node(&["eth"]), 2, 1, "leased", NAME_WRAPPER),
        transfer_single(2, 2, HOLDER, ZERO, HOLDER, leased),
        name_wrapped_with(
            2,
            3,
            leased,
            b"\x06leased\x03eth\0",
            HOLDER,
            (DOT_ETH_FUSES, 4_000_000_000 + 90 * 86_400),
        ),
    ]
}

/// The lease side of `wrapETH2LD`, as a control. The NameWrapper holds the BaseRegistrar
/// token and, after `reclaim`, the registry record, so its grants on the lease match the chain
/// while the name is wrapped. `unwrapETH2LD` moves both to the new owner, and the lease then
/// holds only that owner's grants, in one batch and cut after the wrap, resumed warm and
/// restored cold. The lease or the NameWrapper authority stays selected through every
/// registry write of the wrap and the unwrap transactions, so none of them moves registry-only
/// grants, and the registry-only resource holds no grants at the end, in any of them.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
#[test]
fn wrap_eth2ld_lease_grants_follow_the_registrar_token() -> anyhow::Result<()> {
    let leased = node(&["leased", "eth"]);
    let logs = wrapped_lease_logs();
    let wrapped = interpret_test_batch(lease_batch(logs.clone()))?;
    let lease = wrapped
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("the registration has a lease resource");
    assert_eq!(
        live_grants_on(&[&wrapped], lease),
        registry_owner_grants(NAME_WRAPPER),
        "while wrapped, the lease's grants are the NameWrapper's"
    );
    let unwrap = vec![
        transfer_single(3, 0, HOLDER, HOLDER, ZERO, leased),
        registry_transfer(3, 1, leased, TRANSFEREE),
        name_unwrapped(3, 2, leased, TRANSFEREE),
        lease_transfer(3, 3, NAME_WRAPPER, TRANSFEREE),
    ];
    let registry_only =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{leased:#x}"));
    let mut failures = Vec::new();
    for (layout, grants) in grants_in_every_layout(&lease_batch, &logs, &[unwrap.clone()], lease)? {
        if grants != scoped_registry_owner_grants(TRANSFEREE, RESOLVER) {
            failures.push(format!("lease, {layout}: {grants:?}"));
        }
    }
    for (layout, outputs) in every_layout(&lease_batch, &logs, &[unwrap])? {
        for block in [2, 3] {
            let rows = permission_rows_at(&outputs, block, |event| {
                event.resource_id == Some(registry_only)
            });
            if !rows.is_empty() {
                failures.push(format!(
                    "registry-only rows, block {block}, {layout}: {rows:?}"
                ));
            }
        }
        let outputs = outputs.iter().collect::<Vec<_>>();
        let grants = scoped_grants_on(&outputs, registry_only);
        if !grants.is_empty() {
            failures.push(format!("registry-only resource, {layout}: {grants:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

const MIGRATION_MANIFEST: i64 = 7404;
const GRAVEYARD: &str = "0x00000000000000000000000000000000000000f7";

/// `input` with an ENSv1→ENSv2 migration manifest that admits GRAVEYARD as its Graveyard, with
/// the controller and factory declarations that ENSv1→ENSv2 migration correlation needs.
fn with_graveyard(mut input: BatchInput) -> BatchInput {
    let mut manifest = manifest(
        MIGRATION_MANIFEST,
        "ens_v2_migration_l1",
        "ProxyDeployed",
        "event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation)",
        &["verifiable_factory"],
        &["ContractDiscovered"],
    );
    let mut payload: serde_json::Value = serde_json::from_str(&manifest.payload_json).unwrap();
    payload["correlation_addresses"] = serde_json::json!({
        "ens_v1_name_wrapper": NAME_WRAPPER,
        "ens_v1_base_registrar": CONTRACT,
    });
    manifest.payload_json = payload.to_string();
    input.manifests.push(manifest);
    input.admissions.extend(
        [
            ("graveyard", GRAVEYARD),
            (
                "unlocked_migration_controller",
                "0x00000000000000000000000000000000000000f8",
            ),
            (
                "locked_migration_controller",
                "0x00000000000000000000000000000000000000f9",
            ),
            (
                "verifiable_factory",
                "0x00000000000000000000000000000000000000fa",
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (role, address))| {
            let mut value = admission(MIGRATION_MANIFEST, role);
            value.address = address.to_owned();
            value.contract_instance_id = uuid::Uuid::from_u128(0xf700 + index as u128);
            value
        }),
    );
    input
}

/// `unwrapETH2LD` of `leased.eth` into the admitted Graveyard after `setResolver` to zero, then
/// the BaseRegistrar transfer to it, as the unlocked controller migrates a wrapped `.eth` name
/// in an ENSv1→ENSv2 migration. The name has a registrar identity, whose lease takes every
/// registry write of the wrap and the unwrap over: none of them makes a row on the registry-only
/// resource, and that resource holds no grants at the end, the Graveyard's among them. The
/// registry write naming the Graveyard is marked with the Graveyard's getter reason, so the
/// Graveyard is admitted. Every layout runs in one batch and cut after the wrap, resumed warm,
/// restored cold and restored from folded rows.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L146-L147 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
#[test]
fn unwrap_eth2ld_into_the_graveyard_moves_no_registry_only_grants() -> anyhow::Result<()> {
    let leased = node(&["leased", "eth"]);
    let build = |logs: Vec<RawLogInput>| with_graveyard(lease_batch(logs));
    let unwrap = vec![
        new_resolver(3, 0, leased, ZERO),
        transfer_single(3, 1, HOLDER, HOLDER, ZERO, leased),
        registry_transfer(3, 2, leased, GRAVEYARD),
        name_unwrapped(3, 3, leased, GRAVEYARD),
        lease_transfer(3, 4, NAME_WRAPPER, GRAVEYARD),
    ];
    let registry_only =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{leased:#x}"));
    let mut failures = Vec::new();
    for (layout, outputs) in every_layout(&build, &wrapped_lease_logs(), &[unwrap])? {
        if !outputs
            .iter()
            .flat_map(|output| output.normalized_events.iter())
            .any(|event| {
                event.block_number == Some(3)
                    && event.after_state["owner_getter_reason"] == "graveyard"
            })
        {
            failures.push(format!("{layout}: the Graveyard is not admitted"));
        }
        for block in [2, 3] {
            let rows = permission_rows_at(&outputs, block, |event| {
                event.resource_id == Some(registry_only)
            });
            if !rows.is_empty() {
                failures.push(format!(
                    "registry-only rows, block {block}, {layout}: {rows:?}"
                ));
            }
        }
        let outputs = outputs.iter().collect::<Vec<_>>();
        let grants = scoped_grants_on(&outputs, registry_only);
        if !grants.is_empty() {
            failures.push(format!("registry-only resource, {layout}: {grants:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[path = "registry_only_holder.rs"]
pub(in crate::schema_v2::tests) mod registry_only_holder;
