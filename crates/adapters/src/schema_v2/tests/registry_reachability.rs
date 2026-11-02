//! TYR-400: an ENSv2 registry token's resolver pointer is a discovery edge only while the
//! registry is reachable, that is while its suffix walk reaches an anchor. `PARENT` is a declared
//! registry, so it is the anchored `eth` registry. Every other registry here announces itself
//! with `RegistryCreated()` and is reachable only while a live `PARENT` token, or a reachable
//! registry's token, points at it.
#![allow(dead_code)]

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
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
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

/// `CHILD` as in M1, mounted under `n.eth` at block 5.
fn mounted_child() -> Vec<RawLogInput> {
    let mut logs = unmounted_child();
    logs.push(mount(PARENT, "n", CHILD, 5, 0));
    logs
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

/// `first` in one batch through `last_block`, then an unmount and a remount in the next.
fn remount_in_a_later_batch(
    first_logs: Vec<RawLogInput>,
    last_block: i64,
) -> anyhow::Result<BatchOutput> {
    let second_logs = vec![
        mount(PARENT, "n", ZERO_ADDRESS, last_block + 1, 0),
        mount(PARENT, "n", CHILD, last_block + 2, 0),
    ];
    continued_in_a_later_batch(first_logs, last_block, second_logs)
}

/// `first` in one batch through `last_block`, then `second` in the next: once continued in the
/// session, once restored from the first batch's events and once restored from them folded as
/// the loader keeps them. All three agree, and the restored output is returned.
fn continued_in_a_later_batch(
    first_logs: Vec<RawLogInput>,
    last_block: i64,
    second_logs: Vec<RawLogInput>,
) -> anyhow::Result<BatchOutput> {
    let first_input = batch(first_logs);
    let first = interpret_test_batch(first_input.clone())?;
    let mut second_input = batch(second_logs);
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

/// K10 on the reconcile tip without the reachability change: the mounting token is
/// unregistered in one batch, then registered again and mounting in the next. All three paths
/// must agree.
#[test]
fn k10_bare_a_released_mount_stays_released_across_a_restore() -> anyhow::Result<()> {
    let mut first = mounted_child();
    first.push(unregister(PARENT, versioned_token("n", 1), 6, 0));
    continued_in_a_later_batch(
        first,
        6,
        vec![
            register(PARENT, "n", LIVE, 7, 0),
            mount(PARENT, "n", CHILD, 8, 0),
        ],
    )?;
    Ok(())
}
