//! TYR-400: an ENSv2 registry token's resolver pointer is a discovery edge only while the
//! registry is reachable, that is while its suffix walk reaches an anchor. `PARENT` is a declared
//! registry, so it is the anchored `eth` registry. Every other registry here announces itself
//! with `RegistryCreated()` and is reachable only while a live `PARENT` token, or a reachable
//! registry's token, points at it.
use super::*;

const MANIFEST: i64 = 61;
const PARENT: &str = "0x00000000000000000000000000000000000000c0";
const CHILD: &str = "0x00000000000000000000000000000000000000c1";
const GRANDCHILD: &str = "0x00000000000000000000000000000000000000c2";
const ELSEWHERE: &str = "0x00000000000000000000000000000000000000c3";
const PROXY: &str = "0x00000000000000000000000000000000000000c4";
const RESOLVER_X: &str = "0x00000000000000000000000000000000000000e1";
const RESOLVER_Y: &str = "0x00000000000000000000000000000000000000e2";
const RESOLVER_Z: &str = "0x00000000000000000000000000000000000000e3";
const SENDER: &str = "0x00000000000000000000000000000000000000a2";
/// Far beyond every block timestamp here, which is the block number in seconds.
const LIVE: u64 = 1_000_000;

fn registry_manifest() -> ManifestInput {
    const ALL: &[&str] = &[
        "SurfaceUnbound",
        "RegistrationReleased",
        "RegistrationGranted",
        "RegistrationReserved",
        "RegistrationRenewed",
        "PreimageObserved",
        "SurfaceBound",
        "AuthorityTransferred",
        "ExpiryChanged",
        "ResolverChanged",
        "SubregistryChanged",
        "ParentChanged",
        "RegistryCreated",
        "TokenResourceLinked",
        "TokenRegenerated",
        "Upgraded",
    ];
    manifest_with_events(
        MANIFEST,
        "ens",
        "ens_v2_registry_l1",
        &[
            ("RegistryCreated", "event RegistryCreated()", &[], ALL),
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                ALL,
            ),
            (
                "ExpiryUpdated",
                "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                &["registry"],
                ALL,
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                ALL,
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                ALL,
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                ALL,
            ),
            (
                "Upgraded",
                "event Upgraded(address indexed implementation)",
                &["registry"],
                ALL,
            ),
            (
                "TokenRegenerated",
                "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                &["registry"],
                ALL,
            ),
        ],
    )
}

fn rules() -> Vec<DiscoveryRuleInput> {
    [
        ("registry_announcement", "reachable_from_root"),
        ("subregistry", "linked_subregistry_event"),
        ("resolver", "reachable_from_root"),
    ]
    .into_iter()
    .map(|(edge_kind, admission)| DiscoveryRuleInput {
        manifest_id: MANIFEST,
        edge_kind: edge_kind.to_owned(),
        from_role: Some("registry".to_owned()),
        admission: admission.to_owned(),
    })
    .collect()
}

fn instance(address: &str) -> Uuid {
    crate::schema_v2::common::contract_id(CHAIN, address)
}

fn parent_admission() -> AddressAdmissionInput {
    let mut parent = admission(MANIFEST, "registry");
    parent.address = PARENT.to_owned();
    parent
}

/// The admission an earlier batch's announcement of `registry` at `block` left behind.
fn announced(registry: &str, block: i64) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: registry.to_owned(),
        contract_instance_id: instance(registry),
        source_manifest_id: Some(MANIFEST),
        role: None,
        discovery_edge_kind: Some("registry_announcement".to_owned()),
        discovery_from_contract_instance_id: Some(instance(registry)),
        discovery_observation_key: Some(format!("registry-announcement:{registry}")),
        active_from_block: Some(block),
        active_to_block: None,
    }
}

fn batch(logs: Vec<RawLogInput>) -> BatchInput {
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest()],
        discovery_rules: rules(),
        admissions: vec![parent_admission()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: logs,
    }
}

fn announce(registry: &str, block: i64, log: i64) -> RawLogInput {
    raw_at(RegistryCreated {}.encode_log_data(), block, log, registry)
}

fn register(registry: &str, label: &str, expiry: u64, block: i64, log: i64) -> RawLogInput {
    raw_at(
        v2_registry::LabelRegistered {
            tokenId: versioned_token(label, 1),
            labelHash: keccak256(label.as_bytes()),
            label: label.to_owned(),
            owner: SENDER.parse().expect("address"),
            expiry,
            sender: SENDER.parse().expect("address"),
        }
        .encode_log_data(),
        block,
        log,
        registry,
    )
}

fn set_resolver(registry: &str, label: &str, resolver: &str, block: i64, log: i64) -> RawLogInput {
    raw_at(
        v2_registry::ResolverUpdated {
            tokenId: versioned_token(label, 1),
            resolver: resolver.parse().expect("address"),
            sender: SENDER.parse().expect("address"),
        }
        .encode_log_data(),
        block,
        log,
        registry,
    )
}

fn mount(parent: &str, label: &str, child: &str, block: i64, log: i64) -> RawLogInput {
    raw_at(
        v2_registry::SubregistryUpdated {
            tokenId: versioned_token(label, 1),
            subregistry: child.parse().expect("address"),
            sender: SENDER.parse().expect("address"),
        }
        .encode_log_data(),
        block,
        log,
        parent,
    )
}

/// The key `registry`'s own `ResolverUpdated` for `label` writes and closes.
fn key(registry: &str, label: &str) -> String {
    format!("resolver:{registry}:{:#x}", versioned_token(label, 0))
}

/// `(target, key, block, transaction index, log index)` of each resolver edge from `registry`.
fn resolver_edges(output: &BatchOutput, registry: &str) -> Vec<(Uuid, String, i64, i64, i64)> {
    output
        .discovery_edges
        .iter()
        .filter(|edge| {
            edge.edge_kind == "resolver" && edge.from_contract_instance_id == instance(registry)
        })
        .map(|edge| {
            (
                edge.to_contract_instance_id,
                edge.observation_key.clone(),
                edge.active_from_block_number,
                edge.provenance["transaction_index"]
                    .as_i64()
                    .unwrap_or(i64::MIN),
                edge.provenance["log_index"].as_i64().unwrap_or(i64::MIN),
            )
        })
        .collect()
}

/// `(key, block, transaction index, log index)` of each resolver close from `registry`, every
/// edge under the key ending there, or with `caps` each cap that ends every edge under the key
/// except the one its log opens.
fn resolver_closures_of(
    output: &BatchOutput,
    registry: &str,
    caps: bool,
) -> Vec<(String, i64, i64, i64)> {
    output
        .discovery_edge_closures
        .iter()
        .filter(|closure| {
            closure.edge_kind == "resolver"
                && closure.from_contract_instance_id == instance(registry)
                && closure.except_to_contract_instance_id.is_some() == caps
        })
        .map(|closure| {
            (
                closure.observation_key.clone(),
                closure.active_to_block_number,
                closure.transaction_index,
                closure.log_index,
            )
        })
        .collect()
}

/// The resolver closes from `registry` from block 5 on. A registration closes its token's stale
/// keys at its own log, before any edge exists, and every case here registers before block 5.
fn resolver_closures(output: &BatchOutput, registry: &str) -> Vec<(String, i64, i64, i64)> {
    resolver_closures_of(output, registry, false)
        .into_iter()
        .filter(|closure| closure.1 >= 5)
        .collect()
}

fn named(output: &BatchOutput, name: &str) -> bool {
    output
        .name_surfaces
        .iter()
        .any(|surface| surface.raw_name() == Some(name))
}

/// `CHILD` announces at block 1, registers `a` at block 2 and points it at `RESOLVER_X` at
/// block 3. `PARENT` registers `n` at block 4.
fn unmounted_child() -> Vec<RawLogInput> {
    vec![
        announce(CHILD, 1, 0),
        register(CHILD, "a", LIVE, 2, 0),
        set_resolver(CHILD, "a", RESOLVER_X, 3, 0),
        register(PARENT, "n", LIVE, 4, 0),
    ]
}

/// N1: a registry nothing reachable mounts opens no resolver edge, whatever labels it holds.
#[test]
fn n1_an_unmounted_registry_opens_no_resolver_edge() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(vec![
        announce(CHILD, 1, 0),
        register(CHILD, "eth", LIVE, 2, 0),
        set_resolver(CHILD, "eth", RESOLVER_X, 3, 0),
        mount(CHILD, "eth", GRANDCHILD, 4, 0),
    ]))?;
    assert_eq!(resolver_edges(&output, CHILD), Vec::new());
    assert!(
        output
            .discovery_edges
            .iter()
            .any(|edge| edge.edge_kind == "registry_announcement"
                && edge.from_contract_instance_id == instance(CHILD)),
        "the registry is still admitted"
    );
    assert!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.raw_fact_ref["emitting_address"] == serde_json::json!(CHILD))
            .all(|event| event.logical_name_id.is_none()),
        "its events stay nameless"
    );
    Ok(())
}

/// F2: an implementation contract announces itself from its constructor and is never mounted.
/// It is an N1 registry.
#[test]
fn f2_an_implementation_self_announcement_opens_no_resolver_edge() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(unmounted_child()))?;
    assert_eq!(resolver_edges(&output, CHILD), Vec::new());
    assert!(!named(&output, "a.n.eth"));
    Ok(())
}

/// M1: the mount opens the registry's resolver edge with the registry as `from` and the key
/// the registry's own pointer log writes, so the registry's next `ResolverUpdated` closes it.
#[test]
fn m1_a_mount_opens_the_edge_under_the_registrys_own_key() -> anyhow::Result<()> {
    let mut logs = unmounted_child();
    logs.push(mount(PARENT, "n", CHILD, 5, 0));
    logs.push(set_resolver(CHILD, "a", RESOLVER_Y, 6, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert!(named(&output, "a.n.eth"));
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![
            (instance(RESOLVER_X), key(CHILD, "a"), 5, 0, 0),
            (instance(RESOLVER_Y), key(CHILD, "a"), 6, 0, 0),
        ]
    );
    let opened = output
        .discovery_edges
        .iter()
        .find(|edge| edge.to_contract_instance_id == instance(RESOLVER_X))
        .expect("the edge to X");
    assert_eq!(opened.source_manifest_id, MANIFEST);
    assert_eq!(opened.admission_basis, "reachable_from_root");
    assert!(
        resolver_closures_of(&output, CHILD, true).contains(&(key(CHILD, "a"), 6, 0, 0)),
        "{:?}",
        output.discovery_edge_closures
    );
    Ok(())
}

/// M2: M1 with a batch cut between the pointer and the mount. The second batch, restored from
/// the first one's events or continued in the same session, opens the edge at the mount, and
/// the first opens none.
#[test]
fn m2_a_mount_in_a_later_batch_opens_the_edge_after_a_cold_restore() -> anyhow::Result<()> {
    let first_input = batch(unmounted_child());
    let first = interpret_test_batch(first_input.clone())?;
    assert_eq!(resolver_edges(&first, CHILD), Vec::new());
    let mut second_input = batch(vec![
        mount(PARENT, "n", CHILD, 5, 0),
        set_resolver(CHILD, "a", RESOLVER_Y, 6, 0),
    ]);
    second_input.admissions.push(announced(CHILD, 1));
    let mut restored_input = second_input.clone();
    restored_input.prior_events = first.normalized_events.iter().map(prior_event).collect();
    let restored = interpret_test_batch(restored_input)?;
    let expected = vec![
        (instance(RESOLVER_X), key(CHILD, "a"), 5, 0, 0),
        (instance(RESOLVER_Y), key(CHILD, "a"), 6, 0, 0),
    ];
    assert_eq!(resolver_edges(&restored, CHILD), expected);

    let (incremental_first, session) = interpret_test_batch_incremental(first_input, None)?;
    assert_eq!(resolver_edges(&incremental_first, CHILD), Vec::new());
    let (incremental, _) = interpret_test_batch_incremental(second_input, Some(session))?;
    assert_eq!(incremental.discovery_edges, restored.discovery_edges);
    assert_eq!(
        incremental.discovery_edge_closures,
        restored.discovery_edge_closures
    );
    Ok(())
}

/// M3: a resolver changed while the registry was unreachable. Only the current one gets an
/// edge, from the mount.
#[test]
fn m3_a_pointer_changed_while_unreachable_opens_only_the_current_one() -> anyhow::Result<()> {
    let mut logs = unmounted_child();
    logs.push(set_resolver(CHILD, "a", RESOLVER_Y, 4, 1));
    logs.push(mount(PARENT, "n", CHILD, 6, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_Y), key(CHILD, "a"), 6, 0, 0)]
    );
    Ok(())
}

/// M4: announcement, entry and mount in one transaction, as the migration factory writes them,
/// with the entry's pointer after the mount. The edge opens at the pointer's own log.
#[test]
fn m4_a_pointer_after_a_same_transaction_mount_opens_at_its_own_log() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(vec![
        register(PARENT, "n", LIVE, 2, 0),
        announce(CHILD, 3, 0),
        register(CHILD, "a", LIVE, 3, 1),
        mount(PARENT, "n", CHILD, 3, 2),
        set_resolver(CHILD, "a", RESOLVER_X, 3, 3),
    ]))?;
    assert!(named(&output, "a.n.eth"));
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_X), key(CHILD, "a"), 3, 0, 3)]
    );
    Ok(())
}

/// M5: the pointer's log comes before the mount's in one transaction. The edge opens at the
/// mount's log.
#[test]
fn m5_a_pointer_before_a_same_transaction_mount_opens_at_the_mount() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(vec![
        announce(CHILD, 1, 0),
        register(CHILD, "a", LIVE, 2, 0),
        register(PARENT, "n", LIVE, 2, 1),
        set_resolver(CHILD, "a", RESOLVER_X, 3, 0),
        mount(PARENT, "n", CHILD, 3, 1),
    ]))?;
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_X), key(CHILD, "a"), 3, 0, 1)]
    );
    Ok(())
}

/// P1: the registry announces after its own pointer log in the block that mounts it. The first
/// pass admits it and is discarded. The final pass opens one edge, at the mount.
#[test]
fn p1_the_final_pass_opens_the_edge_at_the_mount() -> anyhow::Result<()> {
    crate::schema_v2::session::reset_block_passes();
    let output = interpret_test_batch(batch(vec![
        register(PARENT, "n", LIVE, 4, 0),
        register(CHILD, "a", LIVE, 5, 1),
        set_resolver(CHILD, "a", RESOLVER_X, 5, 2),
        announce(CHILD, 5, 5),
        mount(PARENT, "n", CHILD, 5, 8),
    ]))?;
    assert_eq!(crate::schema_v2::session::block_passes().get(&5), Some(&2));
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_X), key(CHILD, "a"), 5, 0, 8)]
    );
    Ok(())
}

/// `CHILD` as in M1, mounted under `n.eth` at block 5.
fn mounted_child() -> Vec<RawLogInput> {
    let mut logs = unmounted_child();
    logs.push(mount(PARENT, "n", CHILD, 5, 0));
    logs
}

/// U1: clearing the mount closes every resolver edge of the registry at that log.
#[test]
fn u1_clearing_the_mount_closes_the_registry_edges() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 6, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_closures(&output, CHILD),
        vec![(key(CHILD, "a"), 6, 0, 0)]
    );
    Ok(())
}

/// U2: pointing the mount elsewhere closes the registry's edges the same way.
#[test]
fn u2_repointing_the_mount_closes_the_registry_edges() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(announce(ELSEWHERE, 6, 0));
    logs.push(mount(PARENT, "n", ELSEWHERE, 6, 1));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_closures(&output, CHILD),
        vec![(key(CHILD, "a"), 6, 0, 1)]
    );
    assert_eq!(resolver_edges(&output, ELSEWHERE), Vec::new());
    Ok(())
}

/// U3: the mounting token expires between blocks. The next block's boundary closes the
/// registry's edges at the stand-in position, with no log.
#[test]
fn u3_an_expiry_crossing_closes_the_registry_edges_at_the_boundary() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(vec![
        announce(CHILD, 1, 0),
        register(CHILD, "a", LIVE, 2, 0),
        set_resolver(CHILD, "a", RESOLVER_X, 3, 0),
        register(PARENT, "n", 10, 4, 0),
        mount(PARENT, "n", CHILD, 5, 0),
        register(PARENT, "later", LIVE, 12, 0),
    ]))?;
    assert_eq!(
        resolver_closures(&output, CHILD),
        vec![(key(CHILD, "a"), 12, -1, -1)]
    );
    Ok(())
}

/// U4: mounting again reopens each token's current resolver, one open interval at a time.
#[test]
fn u4_mounting_again_reopens_the_current_resolver() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 6, 0));
    logs.push(set_resolver(CHILD, "a", RESOLVER_Y, 7, 0));
    logs.push(mount(PARENT, "n", CHILD, 8, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![
            (instance(RESOLVER_X), key(CHILD, "a"), 5, 0, 0),
            (instance(RESOLVER_Y), key(CHILD, "a"), 8, 0, 0),
        ]
    );
    assert!(resolver_closures(&output, CHILD).contains(&(key(CHILD, "a"), 6, 0, 0)));
    Ok(())
}

/// E1: a token's own expiry on a reachable registry leaves its edge alone. Reachability of the
/// registry gates the edge, not the token's name.
#[test]
fn e1_a_tokens_own_expiry_leaves_its_edge_open() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(vec![
        announce(CHILD, 1, 0),
        register(CHILD, "a", 10, 2, 0),
        set_resolver(CHILD, "a", RESOLVER_X, 3, 0),
        register(PARENT, "n", LIVE, 4, 0),
        mount(PARENT, "n", CHILD, 5, 0),
        register(PARENT, "later", LIVE, 12, 0),
    ]))?;
    assert_eq!(resolver_edges(&output, CHILD).len(), 1);
    assert_eq!(resolver_closures(&output, CHILD), Vec::new());
    Ok(())
}

/// S2: a label that does not normalize gives its token only a shadow name. The registry is
/// reachable, so the token's edge opens at the mount all the same.
#[test]
fn s2_an_unnormalizable_label_keeps_its_edge_on_a_reachable_registry() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(vec![
        announce(CHILD, 1, 0),
        register(CHILD, "a b", LIVE, 2, 0),
        set_resolver(CHILD, "a b", RESOLVER_X, 3, 0),
        register(PARENT, "n", LIVE, 4, 0),
        mount(PARENT, "n", CHILD, 5, 0),
    ]))?;
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_X), key(CHILD, "a b"), 5, 0, 0)]
    );
    Ok(())
}

/// `GRANDCHILD` sits under `CHILD`'s `b`, and nothing mounts `CHILD` until `PARENT`'s `n` points
/// at it at block 6.
fn transitive() -> Vec<RawLogInput> {
    vec![
        announce(GRANDCHILD, 1, 0),
        announce(CHILD, 1, 1),
        register(GRANDCHILD, "c", LIVE, 2, 0),
        register(CHILD, "b", LIVE, 2, 1),
        set_resolver(GRANDCHILD, "c", RESOLVER_X, 3, 0),
        mount(CHILD, "b", GRANDCHILD, 4, 0),
        set_resolver(CHILD, "b", RESOLVER_Y, 4, 1),
        register(PARENT, "n", LIVE, 5, 0),
        mount(PARENT, "n", CHILD, 6, 0),
    ]
}

/// T1: mounting a registry makes the registry below it reachable in the same log. Both open
/// their edges there.
#[test]
fn t1_mounting_an_ancestor_opens_both_registries_edges() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(transitive()))?;
    assert!(named(&output, "c.b.n.eth"));
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_Y), key(CHILD, "b"), 6, 0, 0)]
    );
    assert_eq!(
        resolver_edges(&output, GRANDCHILD),
        vec![(instance(RESOLVER_X), key(GRANDCHILD, "c"), 6, 0, 0)]
    );
    Ok(())
}

/// T2: when the ancestor loses its mount, both registries close their edges in that log.
#[test]
fn t2_unmounting_an_ancestor_closes_both_registries_edges() -> anyhow::Result<()> {
    let mut logs = transitive();
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 7, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_closures(&output, CHILD),
        vec![(key(CHILD, "b"), 7, 0, 0)]
    );
    assert_eq!(
        resolver_closures(&output, GRANDCHILD),
        vec![(key(GRANDCHILD, "c"), 7, 0, 0)]
    );
    Ok(())
}

/// F1: a factory proxy announces itself when initialized. The proxy is the registry a parent
/// mounts. Its implementation's own constructor announcement is a registry nothing mounts.
#[test]
fn f1_a_mounted_proxy_opens_edges_and_its_implementation_does_not() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(vec![
        announce(ELSEWHERE, 1, 0),
        register(ELSEWHERE, "a", LIVE, 2, 0),
        set_resolver(ELSEWHERE, "a", RESOLVER_Y, 2, 1),
        raw_at(
            v2_registry::Upgraded {
                implementation: ELSEWHERE.parse()?,
            }
            .encode_log_data(),
            3,
            0,
            PROXY,
        ),
        announce(PROXY, 3, 1),
        register(PROXY, "a", LIVE, 4, 0),
        set_resolver(PROXY, "a", RESOLVER_X, 4, 1),
        register(PARENT, "n", LIVE, 4, 2),
        mount(PARENT, "n", PROXY, 5, 0),
    ]))?;
    assert_eq!(resolver_edges(&output, ELSEWHERE), Vec::new());
    assert_eq!(
        resolver_edges(&output, PROXY),
        vec![(instance(RESOLVER_X), key(PROXY, "a"), 5, 0, 0)]
    );
    Ok(())
}

/// R1: a registry that announces itself again changes nothing about its resolver edges.
#[test]
fn r1_a_second_announcement_leaves_the_edges_alone() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(announce(CHILD, 6, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_X), key(CHILD, "a"), 5, 0, 0)]
    );
    assert_eq!(resolver_closures(&output, CHILD), Vec::new());
    Ok(())
}

/// S1, adapter half: `n` announced registries, each pointing one token at a resolver in a later
/// block, add no resolver edge and exactly one announcement edge each.
#[test]
fn s1_announcement_spam_adds_no_resolver_edge() -> anyhow::Result<()> {
    for registries in [1_u64, 1_000] {
        let addresses = (0..registries)
            .map(|index| format!("0x{:040x}", 0xd000_0000_u64 + index))
            .collect::<Vec<_>>();
        let mut logs = Vec::new();
        for (index, address) in addresses.iter().enumerate() {
            logs.push(announce(address, 1, index as i64));
        }
        for (index, address) in addresses.iter().enumerate() {
            logs.push(set_resolver(address, "a", RESOLVER_X, 2, index as i64));
        }
        let output = interpret_test_batch(batch(logs))?;
        let count = |kind: &str| {
            output
                .discovery_edges
                .iter()
                .filter(|edge| edge.edge_kind == kind)
                .count() as u64
        };
        assert_eq!(count("resolver"), 0, "{registries} registries");
        assert_eq!(count("registry_announcement"), registries);
        assert_eq!(
            output
                .discovery_edge_closures
                .iter()
                .filter(|closure| closure.edge_kind == "resolver")
                .count(),
            0
        );
        assert!(output.name_surfaces.is_empty());
    }
    Ok(())
}

fn parent_claim(registry: &str, parent: &str, label: &str, block: i64, log: i64) -> RawLogInput {
    raw_at(
        v2_registry::ParentUpdated {
            parent: parent.parse().expect("address"),
            label: label.to_owned(),
            sender: SENDER.parse().expect("address"),
        }
        .encode_log_data(),
        block,
        log,
        registry,
    )
}

/// `CHILD` holds `a` pointing at `RESOLVER_X` and claims `ELSEWHERE`'s `p`, which points back
/// until it expires at 10. `ELSEWHERE` is announced and unreachable. `PARENT` mounts `CHILD` at
/// block 4, but the claim that points back wins, so `CHILD` is unreachable until `p` expires.
fn claimed_by_unreachable_parent() -> Vec<RawLogInput> {
    vec![
        announce(ELSEWHERE, 1, 0),
        announce(CHILD, 1, 1),
        register(ELSEWHERE, "p", 10, 2, 0),
        mount(ELSEWHERE, "p", CHILD, 2, 1),
        register(CHILD, "a", LIVE, 2, 2),
        set_resolver(CHILD, "a", RESOLVER_X, 2, 3),
        parent_claim(CHILD, ELSEWHERE, "p", 3, 0),
        register(PARENT, "n", LIVE, 4, 0),
        mount(PARENT, "n", CHILD, 4, 1),
    ]
}

/// U5: when the claimed entry expires, the walk falls back to `PARENT`'s mount, so a block
/// boundary makes `CHILD` reachable. Its edge opens at the boundary with a raw-block position,
/// which sorts before the block's logs.
#[test]
fn u5_an_expired_parent_claim_opens_the_edge_at_the_boundary() -> anyhow::Result<()> {
    let mut logs = claimed_by_unreachable_parent();
    logs.push(register(PARENT, "later", LIVE, 12, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(
            instance(RESOLVER_X),
            key(CHILD, "a"),
            12,
            i64::MIN,
            i64::MIN
        )]
    );
    let opened = output
        .discovery_edges
        .iter()
        .find(|edge| edge.edge_kind == "resolver")
        .expect("the boundary edge");
    assert!(crate::schema_v2::seam::is_raw_block_provenance(
        &opened.provenance
    ));
    assert_eq!(opened.discovery_source, "RegistryPathExpired");
    assert_eq!(opened.source_manifest_id, MANIFEST);
    assert!(named(&output, "a.n.eth"));
    Ok(())
}

/// U6: a parent claim that points back at an unreachable parent takes reachability away from a
/// mounted registry at the claim's log.
#[test]
fn u6_a_parent_claim_to_an_unreachable_parent_closes_at_its_log() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(announce(ELSEWHERE, 6, 0));
    logs.push(register(ELSEWHERE, "p", LIVE, 6, 1));
    logs.push(mount(ELSEWHERE, "p", CHILD, 6, 2));
    logs.push(parent_claim(CHILD, ELSEWHERE, "p", 7, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD),
        vec![(instance(RESOLVER_X), key(CHILD, "a"), 5, 0, 0)]
    );
    assert_eq!(
        resolver_closures(&output, CHILD),
        vec![(key(CHILD, "a"), 7, 0, 0)]
    );
    Ok(())
}

/// U4 with a regenerated token: a regeneration to a token id that differs beyond the version
/// bits leaves the survivor carrying its predecessor's key. Losing reachability closes that key
/// and the survivor's own, and regaining it reopens only the carried key, which is the one the
/// survivor's edge was left under.
#[test]
fn u7_a_regenerated_token_reopens_its_carried_key_once() -> anyhow::Result<()> {
    let survivor = versioned_token("a-regenerated", 1);
    let mut logs = mounted_child();
    logs.push(raw_at(
        v2_registry::TokenRegenerated {
            oldTokenId: versioned_token("a", 1),
            newTokenId: survivor,
        }
        .encode_log_data(),
        6,
        0,
        CHILD,
    ));
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 7, 0));
    logs.push(mount(PARENT, "n", CHILD, 8, 0));
    let output = interpret_test_batch(batch(logs))?;
    let reopened = resolver_edges(&output, CHILD)
        .into_iter()
        .filter(|edge| edge.2 == 8)
        .collect::<Vec<_>>();
    assert_eq!(
        reopened,
        vec![(instance(RESOLVER_X), key(CHILD, "a"), 8, 0, 0)]
    );
    let closed = resolver_closures(&output, CHILD)
        .into_iter()
        .filter(|closure| closure.1 == 7)
        .map(|closure| closure.0)
        .collect::<std::collections::BTreeSet<_>>();
    let mut survivor_key = survivor.to_be_bytes::<32>();
    survivor_key[28..].fill(0);
    assert_eq!(
        closed,
        [
            key(CHILD, "a"),
            format!("resolver:{CHILD}:{:#x}", U256::from_be_bytes(survivor_key)),
        ]
        .into_iter()
        .collect()
    );
    Ok(())
}

fn set_versioned_resolver(
    registry: &str,
    label: &str,
    version: u32,
    resolver: &str,
    block: i64,
    log: i64,
) -> RawLogInput {
    raw_at(
        v2_registry::ResolverUpdated {
            tokenId: versioned_token(label, version),
            resolver: resolver.parse().expect("address"),
            sender: SENDER.parse().expect("address"),
        }
        .encode_log_data(),
        block,
        log,
        registry,
    )
}

/// `mounted_child`, then a `ResolverUpdated` for another version of `a`'s token id clears the
/// link they share at block 6. Version 1 still holds `RESOLVER_X`.
fn cleared_through_another_version() -> Vec<RawLogInput> {
    let mut logs = mounted_child();
    logs.push(set_versioned_resolver(CHILD, "a", 2, ZERO_ADDRESS, 6, 0));
    logs
}

/// K1: two token versions write one link, nonzero and then zero. The clear closes the link, and
/// unmounting and mounting again leaves it closed: a mount reopens a link from its latest write.
#[test]
fn k1_a_link_cleared_through_another_version_stays_closed_after_a_remount() -> anyhow::Result<()> {
    let mut logs = cleared_through_another_version();
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 7, 0));
    logs.push(mount(PARENT, "n", CHILD, 8, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert!(resolver_closures(&output, CHILD).contains(&(key(CHILD, "a"), 6, 0, 0)));
    assert_eq!(
        resolver_edges(&output, CHILD)
            .into_iter()
            .filter(|edge| edge.2 >= 6)
            .collect::<Vec<_>>(),
        Vec::new()
    );
    Ok(())
}

/// K2: after that clear, a nonzero write through the first version reopens the link at its log
/// as usual, and a remount reopens it with that resolver.
#[test]
fn k2_a_later_write_through_the_other_version_reopens_the_link() -> anyhow::Result<()> {
    let mut logs = cleared_through_another_version();
    logs.push(set_versioned_resolver(CHILD, "a", 1, RESOLVER_Y, 7, 0));
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 8, 0));
    logs.push(mount(PARENT, "n", CHILD, 9, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD)
            .into_iter()
            .filter(|edge| edge.2 >= 6)
            .collect::<Vec<_>>(),
        vec![
            (instance(RESOLVER_Y), key(CHILD, "a"), 7, 0, 0),
            (instance(RESOLVER_Y), key(CHILD, "a"), 9, 0, 0),
        ]
    );
    Ok(())
}

/// K3: the same collision with the versions swapped, the greater token id writing the nonzero
/// value first and the lesser one clearing it, also stays closed after a remount.
#[test]
fn k3_a_link_cleared_through_a_lesser_version_stays_closed_after_a_remount() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(set_versioned_resolver(CHILD, "a", 2, RESOLVER_Y, 6, 0));
    logs.push(set_versioned_resolver(CHILD, "a", 1, ZERO_ADDRESS, 7, 0));
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 8, 0));
    logs.push(mount(PARENT, "n", CHILD, 9, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD)
            .into_iter()
            .filter(|edge| edge.2 >= 6)
            .collect::<Vec<_>>(),
        vec![(instance(RESOLVER_Y), key(CHILD, "a"), 6, 0, 0)]
    );
    Ok(())
}

/// K4: two versions write one link nonzero, the greater token id first. A remount reopens the
/// link to the latest write, the lesser version's resolver.
#[test]
fn k4_a_remount_reopens_a_shared_link_to_its_latest_write() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(set_versioned_resolver(CHILD, "a", 2, RESOLVER_X, 6, 0));
    logs.push(set_versioned_resolver(CHILD, "a", 1, RESOLVER_Y, 7, 0));
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 8, 0));
    logs.push(mount(PARENT, "n", CHILD, 9, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        resolver_edges(&output, CHILD)
            .into_iter()
            .filter(|edge| edge.2 == 9)
            .collect::<Vec<_>>(),
        vec![(instance(RESOLVER_Y), key(CHILD, "a"), 9, 0, 0)]
    );
    Ok(())
}

/// `first` in one batch through `last_block`, then an unmount and a remount in the next: once
/// continued in the session, once restored from the first batch's events and once restored from
/// them folded as the loader keeps them. All three agree, and the restored output is returned.
fn remount_in_a_later_batch(
    first_logs: Vec<RawLogInput>,
    last_block: i64,
) -> anyhow::Result<BatchOutput> {
    let first_input = batch(first_logs);
    let first = interpret_test_batch(first_input.clone())?;
    let mut second_input = batch(vec![
        mount(PARENT, "n", ZERO_ADDRESS, last_block + 1, 0),
        mount(PARENT, "n", CHILD, last_block + 2, 0),
    ]);
    second_input.admissions.push(announced(CHILD, 1));
    let mut restored_input = second_input.clone();
    restored_input.prior_events = first.normalized_events.iter().map(prior_event).collect();
    let restored = interpret_test_batch(restored_input)?;
    let mut folded_input = second_input.clone();
    folded_input.prior_events = crate::schema_v2::seam::fold_prior_events(
        Vec::new(),
        &first.normalized_events,
        &(1..=last_block).map(test_block).collect::<Vec<_>>(),
    )?;
    let folded = interpret_test_batch(folded_input)?;
    let (_, session) = interpret_test_batch_incremental(first_input, None)?;
    let (incremental, _) = interpret_test_batch_incremental(second_input, Some(session))?;
    for output in [&folded, &incremental] {
        assert_eq!(output.discovery_edges, restored.discovery_edges);
        assert_eq!(
            output.discovery_edge_closures,
            restored.discovery_edge_closures
        );
    }
    Ok(restored)
}

/// `remount_in_a_later_batch`, where no resolver edge reopens at the remount.
fn assert_a_cleared_link_stays_closed_across_batches(
    first_logs: Vec<RawLogInput>,
    last_block: i64,
) -> anyhow::Result<()> {
    let restored = remount_in_a_later_batch(first_logs, last_block)?;
    assert_eq!(resolver_edges(&restored, CHILD), Vec::new());
    Ok(())
}

/// K1r: K1's clear in one batch and the remount in the next.
#[test]
fn k1r_a_link_cleared_in_an_earlier_batch_stays_closed_after_a_remount() -> anyhow::Result<()> {
    assert_a_cleared_link_stays_closed_across_batches(cleared_through_another_version(), 6)
}

/// K3r: K3's swapped clear in one batch and the remount in the next.
#[test]
fn k3r_a_link_cleared_through_a_lesser_version_in_an_earlier_batch_stays_closed()
-> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(set_versioned_resolver(CHILD, "a", 2, RESOLVER_Y, 6, 0));
    logs.push(set_versioned_resolver(CHILD, "a", 1, ZERO_ADDRESS, 7, 0));
    assert_a_cleared_link_stays_closed_across_batches(logs, 7)
}

fn unregister(registry: &str, token_id: U256, block: i64, log: i64) -> RawLogInput {
    raw_at(
        v2_registry::LabelUnregistered {
            tokenId: token_id,
            sender: SENDER.parse().expect("address"),
        }
        .encode_log_data(),
        block,
        log,
        registry,
    )
}

fn regenerate(registry: &str, old: U256, new: U256, block: i64, log: i64) -> RawLogInput {
    raw_at(
        v2_registry::TokenRegenerated {
            oldTokenId: old,
            newTokenId: new,
        }
        .encode_log_data(),
        block,
        log,
        registry,
    )
}

/// `mounted_child`, where `a`'s token B holds `RESOLVER_X`, then: B is unregistered, token A
/// (label `p`) is registered and writes `RESOLVER_Y`, A is regenerated to B's id, and that token to C (label
/// `carrier`). C holds `RESOLVER_Y` and carries the keys of `p` and `a`. No event after B's
/// unregistration selected `RESOLVER_X`.
fn released_writer_then_reused_key() -> Vec<RawLogInput> {
    let (a, b, c) = (
        versioned_token("p", 1),
        versioned_token("a", 1),
        versioned_token("carrier", 1),
    );
    let mut logs = mounted_child();
    logs.push(unregister(CHILD, b, 6, 0));
    logs.push(register(CHILD, "p", LIVE, 6, 1));
    logs.push(set_resolver(CHILD, "p", RESOLVER_Y, 6, 2));
    logs.push(regenerate(CHILD, a, b, 7, 0));
    logs.push(regenerate(CHILD, b, c, 7, 1));
    logs
}

/// The resolver edges from `CHILD` that open at `block`, by target and key.
fn reopened_at(output: &BatchOutput, block: i64) -> std::collections::BTreeSet<(Uuid, String)> {
    resolver_edges(output, CHILD)
        .into_iter()
        .filter(|edge| edge.2 == block)
        .map(|edge| (edge.0, edge.1))
        .collect()
}

/// K5: a released token's write does not survive it. After B's key is carried onto C by
/// regeneration, a remount reopens both of C's keys to C's resolver, and nothing to B's old one.
#[test]
fn k5_a_remount_does_not_reopen_a_released_writers_resolver() -> anyhow::Result<()> {
    let mut logs = released_writer_then_reused_key();
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 8, 0));
    logs.push(mount(PARENT, "n", CHILD, 9, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        reopened_at(&output, 9),
        [
            (instance(RESOLVER_Y), key(CHILD, "p")),
            (instance(RESOLVER_Y), key(CHILD, "a")),
        ]
        .into_iter()
        .collect()
    );
    Ok(())
}

/// K6: K5's sequence, then another version of `a`'s token id writes `RESOLVER_Z` on the reused
/// key. That write is still held, so the remount reopens the key to it, not to C's resolver.
/// C's token id is the greater one, so following the holders instead would pick C's.
#[test]
fn k6_a_held_write_after_the_reuse_still_wins_the_remount() -> anyhow::Result<()> {
    let mut logs = released_writer_then_reused_key();
    logs.push(set_versioned_resolver(CHILD, "a", 2, RESOLVER_Z, 7, 2));
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 8, 0));
    logs.push(mount(PARENT, "n", CHILD, 9, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        reopened_at(&output, 9),
        [
            (instance(RESOLVER_Y), key(CHILD, "p")),
            (instance(RESOLVER_Z), key(CHILD, "a")),
        ]
        .into_iter()
        .collect()
    );
    Ok(())
}

/// K7: K1's clear, then the clearing version is unregistered. The clear still stands, so the
/// link stays closed after a remount although version 1 still holds `RESOLVER_X`.
#[test]
fn k7_a_clear_outlives_the_token_that_wrote_it() -> anyhow::Result<()> {
    let mut logs = cleared_through_another_version();
    logs.push(unregister(CHILD, versioned_token("a", 2), 7, 0));
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 8, 0));
    logs.push(mount(PARENT, "n", CHILD, 9, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(reopened_at(&output, 9), std::collections::BTreeSet::new());
    Ok(())
}

/// K8: version 2 of `a`'s token id writes `RESOLVER_Y` over version 1's `RESOLVER_X`, then
/// version 2 is unregistered. A remount follows the surviving holder, version 1.
#[test]
fn k8_a_remount_follows_the_surviving_version_after_the_writer_is_released() -> anyhow::Result<()> {
    let mut logs = mounted_child();
    logs.push(set_versioned_resolver(CHILD, "a", 2, RESOLVER_Y, 6, 0));
    logs.push(unregister(CHILD, versioned_token("a", 2), 7, 0));
    logs.push(mount(PARENT, "n", ZERO_ADDRESS, 8, 0));
    logs.push(mount(PARENT, "n", CHILD, 9, 0));
    let output = interpret_test_batch(batch(logs))?;
    assert_eq!(
        reopened_at(&output, 9),
        [(instance(RESOLVER_X), key(CHILD, "a"))]
            .into_iter()
            .collect()
    );
    Ok(())
}

/// K5r: K5's sequence in one batch and the remount in the next, three ways.
#[test]
fn k5r_a_released_writers_resolver_stays_out_of_a_remount_in_a_later_batch() -> anyhow::Result<()> {
    let restored = remount_in_a_later_batch(released_writer_then_reused_key(), 7)?;
    assert_eq!(
        reopened_at(&restored, 9),
        [
            (instance(RESOLVER_Y), key(CHILD, "p")),
            (instance(RESOLVER_Y), key(CHILD, "a")),
        ]
        .into_iter()
        .collect()
    );
    Ok(())
}
