use super::*;
use serde_json::Value;
use std::collections::BTreeSet;

fn block(number: i64) -> RawBlockInput {
    RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    }
}

fn input(
    manifests: Vec<ManifestInput>,
    admissions: Vec<AddressAdmissionInput>,
    logs: Vec<RawLogInput>,
) -> BatchInput {
    let blocks = logs
        .iter()
        .map(|log| log.block_number)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(block)
        .collect();
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        admissions,
        discovery_rules: vec![],
        prior_events: vec![],
        blocks,
        raw_logs: logs,
    }
}

fn restore(input: &BatchInput, prior: Vec<PriorEventInput>) -> anyhow::Result<AdapterSession> {
    let mut restore = begin_schema_v2_adapter_restore(
        input.chain_id.clone(),
        input.manifests.clone(),
        input.discovery_rules.clone(),
        input.admissions.clone(),
        StateCacheCapacity::Unlimited,
    )?;
    restore.apply_prior_events(prior)?;
    Ok(restore.finish(
        input
            .blocks
            .first()
            .map(|b| b.block_timestamp - time::Duration::seconds(1)),
    ))
}

fn complete(
    prepared: PreparedAdapterBatch,
    prior: &[PriorEventInput],
) -> anyhow::Result<(BatchOutput, AdapterSession)> {
    let tails = prepared
        .state_value_requests()
        .iter()
        .map(|request| InterpreterStateValue {
            state_key: request.state_key.clone(),
            after_state: prior
                .iter()
                .rev()
                .find(|e| e.retained_state_key == request.state_key)
                .map_or_else(|| json!({}), |e| e.after_state.clone()),
        })
        .collect();
    prepared.finish(tails)
}

thread_local! {
    /// Scoped comparisons run on this test thread, so a test can prove the fixture hook ran.
    static SCOPED_COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn scoped_comparisons() -> usize {
    SCOPED_COMPARISONS.get()
}

/// An input the lookahead loader would be chosen for: every manifest family is covered and
/// the retained history holds only ENSv1 events, which is all the production query reads.
pub(super) fn is_ensv1_only(input: &BatchInput) -> bool {
    !input.manifests.is_empty()
        && input
            .manifests
            .iter()
            .all(|manifest| v1_lookahead_supports_family(&manifest.source_family))
        && input
            .prior_events
            .iter()
            .all(|event| event.source_family.starts_with("ens_v1_"))
}

/// The name an event is filed under by `normalized_events_v1_direct_node_probe_idx` and
/// `events.sql`: its first node field in the pinned order, else its logical name.
fn routed_name(event: &PriorEventInput) -> Option<String> {
    seam::v1_event_node(&event.after_state)
        .or_else(|| {
            ["/grant_source/node", "/revocation_source/node"]
                .iter()
                .find_map(|path| event.after_state.pointer(path).and_then(Value::as_str))
        })
        .map(|node| format!("{}:{}", event.namespace, node.to_lowercase()))
        .or_else(|| event.logical_name_id.clone())
}

/// Mirrors `due_names.sql`: registrar grants, renewals and token transfers whose expiry plus
/// the grace period falls after the predecessor timestamp and before the last block's.
fn due_names(prior: &[PriorEventInput], predecessor: Option<i64>, last: i64) -> Vec<V1NodeRequest> {
    let grace = i128::from(super::super::state::ENS_GRACE_PERIOD_SECS);
    prior
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && matches!(
                    event.event_kind.as_str(),
                    "RegistrationGranted" | "RegistrationRenewed" | "TokenControlTransferred"
                )
        })
        .filter_map(|event| {
            let text = match event.after_state.get("expiry")? {
                Value::Number(number) => number.to_string(),
                Value::String(text) => text.clone(),
                _ => return None,
            };
            let digits = text.strip_prefix(['+', '-']).unwrap_or(&text);
            if digits.is_empty()
                || !digits.bytes().all(|byte| byte.is_ascii_digit())
                || digits.trim_start_matches('0').len() > 19
            {
                return None;
            }
            let expiry: i128 = text.parse().ok()?;
            let in_i64 = |value: i128| i64::try_from(value).is_ok();
            // Second branch of the query: an event in the block just before the batch has no
            // lower bound, because its expiry may have lapsed before it was recorded. The
            // latest retained timestamp stands in for that block.
            let in_previous_block = predecessor.is_some()
                && event.block_timestamp.map(OffsetDateTime::unix_timestamp) == predecessor;
            let due = in_i64(expiry)
                && in_i64(expiry + grace)
                && (in_previous_block
                    || predecessor.is_none_or(|at| expiry >= i128::from(at) - grace))
                && expiry < i128::from(last) - grace;
            let node = seam::v1_event_node(&event.after_state)?;
            due.then(|| V1NodeRequest {
                namespace: event.namespace.clone(),
                node: node.to_lowercase(),
            })
        })
        .collect()
}

/// In-memory stand-in for the production selection in `events.sql`, run to the same closure
/// as the Interpret loader. It files each event under exactly one name, as the index does,
/// and reads an event by resource only when it has no name at all. Fixture history is already
/// folded to the latest event per state key, so the SQL's per-key winner step has nothing
/// left to choose; that step has its own database tests.
fn scope(
    mut deps: V1BatchDependencies,
    prior: &[PriorEventInput],
) -> anyhow::Result<(V1BatchDependencies, Vec<PriorEventInput>)> {
    loop {
        let names: BTreeSet<String> = deps
            .nodes
            .iter()
            .map(|request| format!("{}:{}", request.namespace, request.node))
            .collect();
        let rows = prior
            .iter()
            .filter(|event| event.source_family.starts_with("ens_v1_"))
            .filter(|event| match routed_name(event) {
                Some(name) => names.contains(&name),
                None => event
                    .resource_id
                    .is_some_and(|resource| deps.resource_ids.contains(&resource)),
            })
            .cloned()
            .collect::<Vec<_>>();
        let before = deps.clone();
        deps.include_prior_events(&rows)?;
        if before == deps {
            return Ok((deps, rows));
        }
    }
}

pub(super) fn assert_scoped_matches(mut input: BatchInput) -> anyhow::Result<AdapterSession> {
    if input.blocks.is_empty() {
        input.blocks = input
            .raw_logs
            .iter()
            .map(|raw| {
                (
                    (raw.block_number, raw.block_hash.clone()),
                    RawBlockInput {
                        chain_id: raw.chain_id.clone(),
                        block_hash: raw.block_hash.clone(),
                        block_number: raw.block_number,
                        block_timestamp: raw.block_timestamp,
                        canonicality_state: raw.canonicality_state.clone(),
                    },
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_values()
            .collect();
    }
    let prior = std::mem::take(&mut input.prior_events);
    let provenance = input.manifests.clone();
    let expected = complete(
        prepare_schema_v2_batch_incremental(
            input.clone(),
            Some(restore(&input, prior.clone())?),
            StateCacheCapacity::Unlimited,
        )?,
        &prior,
    )?
    .0;
    let predecessor = input
        .blocks
        .first()
        .map(|block| block.block_timestamp - time::Duration::seconds(1));
    let mut dependencies = collect_v1_batch_dependencies(&input, &provenance)?;
    if let Some(last) = input.blocks.last() {
        // Interpret bounds the due window below by the timestamp of the block before the
        // batch. Fixture inputs carry no such block; the latest retained event stands in for
        // it, because no earlier batch can have settled releases after that point. The
        // synthetic `predecessor` above is often far later and would hide names that fall
        // due in the gap a fixture leaves between its history and its batch.
        let history_end = prior
            .iter()
            .filter_map(|event| event.block_timestamp)
            .max()
            .map(OffsetDateTime::unix_timestamp);
        dependencies.nodes.extend(due_names(
            &prior,
            history_end,
            last.block_timestamp.unix_timestamp(),
        ));
    }
    let (dependencies, rows) = scope(dependencies, &prior)?;
    assert!(dependencies.unsupported.is_empty());
    let session = restore_schema_v2_lookahead_session(
        begin_schema_v2_adapter_restore(
            input.chain_id.clone(),
            input.manifests.clone(),
            input.discovery_rules.clone(),
            input.admissions.clone(),
            StateCacheCapacity::Unlimited,
        )?,
        rows,
        predecessor,
        &dependencies.nodes,
    )?;
    let prepared = prepare_schema_v2_batch_lookahead(
        input.clone(),
        provenance,
        session,
        &dependencies.nodes,
        StateCacheCapacity::Unlimited,
    )?;
    let (actual, session) = complete(prepared, &prior)?;
    assert_eq!(actual, expected, "complete scoped suffix output differs");
    SCOPED_COMPARISONS.set(SCOPED_COMPARISONS.get() + 1);
    Ok(session)
}

pub(super) fn registrar_manifest() -> ManifestInput {
    manifest_with_events(
        81,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar_controller"],
                &["RegistrationGranted", "ExpiryChanged"],
            ),
            (
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
                &["registrar_controller"],
                &[
                    "RegistrationGranted",
                    "RegistrationRenewed",
                    "ExpiryChanged",
                ],
            ),
        ],
    )
}

#[test]
fn scoped_suffix_matches_producer_history_and_drops_disjoint_names() -> anyhow::Result<()> {
    let manifests = vec![registrar_manifest()];
    let admissions = vec![admission(81, "registrar_controller")];
    let register = |name: &str, index| {
        raw_at(
            NameRegistered {
                name: name.to_owned(),
                label: keccak256(name),
                owner: CONTRACT.parse().unwrap(),
                expires: U256::from(1_000_000),
            }
            .encode_log_data(),
            1,
            index,
            CONTRACT,
        )
    };
    let prefix = input(
        manifests.clone(),
        admissions.clone(),
        vec![register("alice", 0), register("bob", 1)],
    );
    let output = interpret_schema_v2_batch(prefix.clone())?;
    let prior = seam::fold_prior_events(vec![], &output.normalized_events, &prefix.blocks)?;
    for (name, other) in [("alice", "bob"), ("bob", "alice"), ("alice", "bob")] {
        let mut suffix = input(
            manifests.clone(),
            admissions.clone(),
            vec![raw_at(
                NameRenewed {
                    name: name.to_owned(),
                    label: keccak256(name),
                    expires: U256::from(2_000_000),
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            )],
        );
        suffix.prior_events = prior.clone();
        let session = assert_scoped_matches(suffix)?;
        assert!(
            session
                .v1_name(
                    "ens",
                    &super::super::common::namehash(&[name.to_owned(), "eth".to_owned()])
                )
                .is_some()
        );
        assert!(
            session
                .v1_name(
                    "ens",
                    &super::super::common::namehash(&[other.to_owned(), "eth".to_owned()])
                )
                .is_none()
        );
    }
    Ok(())
}

#[test]
fn known_absent_node_is_distinct_from_unloaded_node() -> anyhow::Result<()> {
    let input = input(
        vec![registrar_manifest()],
        vec![admission(81, "registrar_controller")],
        vec![raw(NameRegistered {
            name: "alice".to_owned(),
            label: keccak256("alice"),
            owner: CONTRACT.parse()?,
            expires: U256::from(1_000_000),
        }
        .encode_log_data())],
    );
    let deps = collect_v1_batch_dependencies(&input, &input.manifests)?;
    let error = prepare_schema_v2_batch_lookahead(
        input.clone(),
        input.manifests.clone(),
        restore(&input, vec![])?,
        &BTreeSet::new(),
        StateCacheCapacity::Unlimited,
    )
    .err()
    .expect("unloaded must fail");
    assert!(error.to_string().contains("not completely loaded"));
    let output = complete(
        prepare_schema_v2_batch_lookahead(
            input.clone(),
            input.manifests.clone(),
            restore(&input, vec![])?,
            &deps.nodes,
            StateCacheCapacity::Unlimited,
        )?,
        &[],
    )?
    .0;
    assert!(
        output
            .normalized_events
            .iter()
            .any(|e| e.event_kind == "RegistrationGranted")
    );
    Ok(())
}

#[test]
fn quiet_v2_manifest_is_explicitly_unsupported() -> anyhow::Result<()> {
    let input = input(
        vec![manifest(
            1,
            "ens_v2_registry_l1",
            "RegistryCreated",
            "event RegistryCreated()",
            &["registry"],
            &["RegistryCreated"],
        )],
        vec![],
        vec![],
    );
    let deps = collect_v1_batch_dependencies(&input, &input.manifests)?;
    assert!(
        deps.unsupported
            .iter()
            .any(|reason| reason.contains("ens_v2_registry_l1"))
    );
    Ok(())
}

#[test]
fn wrapper_batch_transfers_request_every_token_before_state_exists() -> anyhow::Result<()> {
    use super::super::protocol::v1::wrapper::{NameWrapped, TransferBatch};
    let manifests = vec![manifest_with_events(
        91,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "TransferBatch",
                "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                &["name_wrapper"],
                &["TokenControlTransferred"],
            ),
        ],
    )];
    let admissions = vec![admission(91, "name_wrapper")];
    let nodes = ["alice", "bob"]
        .map(|label| super::super::common::namehash(&[label.to_owned(), "eth".to_owned()]));
    let logs = ["alice", "bob"]
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            let mut dns = vec![label.len() as u8];
            dns.extend(label.as_bytes());
            dns.extend(b"\x03eth\0");
            raw_at(
                NameWrapped {
                    node: nodes[index].parse().unwrap(),
                    name: dns.into(),
                    owner: CONTRACT.parse().unwrap(),
                    fuses: 1,
                    expiry: 1_000_000,
                }
                .encode_log_data(),
                1,
                index as i64,
                CONTRACT,
            )
        })
        .collect();
    let prefix = input(manifests.clone(), admissions.clone(), logs);
    let output = interpret_schema_v2_batch(prefix.clone())?;
    let mut suffix = input(
        manifests,
        admissions,
        vec![raw_at(
            TransferBatch {
                operator: CONTRACT.parse()?,
                from: CONTRACT.parse()?,
                to: "0x0000000000000000000000000000000000000099".parse()?,
                ids: nodes
                    .iter()
                    .map(|node| U256::from_be_bytes(node.parse::<B256>().unwrap().0))
                    .collect(),
                values: vec![U256::from(1); 2],
            }
            .encode_log_data(),
            2,
            0,
            CONTRACT,
        )],
    );
    let deps = collect_v1_batch_dependencies(&suffix, &suffix.manifests)?;
    for node in &nodes {
        assert!(deps.nodes.contains(&V1NodeRequest {
            namespace: "ens".to_owned(),
            node: node.clone()
        }));
    }
    suffix.prior_events =
        seam::fold_prior_events(vec![], &output.normalized_events, &prefix.blocks)?;
    assert_scoped_matches(suffix)?;
    Ok(())
}

#[test]
fn registry_parent_request_does_not_expand_unrelated_children() -> anyhow::Result<()> {
    sol! { event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner); }
    let parent = super::super::common::namehash(&["eth".to_owned()]);
    let manifest = manifest(
        92,
        "ens_v1_registry_l1",
        "NewOwner",
        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        &["registry"],
        &[
            "SubregistryChanged",
            "AuthorityTransferred",
            "AuthorityEpochChanged",
        ],
    );
    let prefix = input(
        vec![manifest.clone()],
        vec![admission(92, "registry")],
        vec![raw_at(
            NewOwner {
                node: parent.parse()?,
                label: keccak256("unrelated"),
                owner: CONTRACT.parse()?,
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    );
    let prefix_output = interpret_schema_v2_batch(prefix.clone())?;
    let prior = seam::fold_prior_events(vec![], &prefix_output.normalized_events, &prefix.blocks)?;
    let suffix = input(
        vec![manifest],
        vec![admission(92, "registry")],
        vec![raw_at(
            NewOwner {
                node: parent.parse()?,
                label: keccak256("wanted"),
                owner: CONTRACT.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            CONTRACT,
        )],
    );
    let deps = collect_v1_batch_dependencies(&suffix, &suffix.manifests)?;
    assert!(deps.nodes.contains(&V1NodeRequest {
        namespace: "ens".to_owned(),
        node: parent
    }));
    let (_, selected) = scope(deps, &prior)?;
    assert!(
        selected.is_empty(),
        "parent authority request swept unrelated child observations"
    );
    let mut suffix = suffix;
    suffix.prior_events = prior;
    assert_scoped_matches(suffix)?;
    Ok(())
}

#[test]
fn quiet_due_node_requires_a_loaded_certificate_before_publication() -> anyhow::Result<()> {
    let prefix = input(
        vec![registrar_manifest()],
        vec![admission(81, "registrar_controller")],
        vec![raw(NameRegistered {
            name: "quiet".to_owned(),
            label: keccak256("quiet"),
            owner: CONTRACT.parse()?,
            expires: U256::from(1_000),
        }
        .encode_log_data())],
    );
    let output = interpret_schema_v2_batch(prefix.clone())?;
    let prior = seam::fold_prior_events(vec![], &output.normalized_events, &prefix.blocks)?;
    let mut quiet = input(prefix.manifests, prefix.admissions, vec![]);
    let mut next = block(2);
    next.block_timestamp =
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1_000 + 90 * 24 * 60 * 60 + 1);
    quiet.blocks.push(next);
    let error = prepare_schema_v2_batch_lookahead(
        quiet.clone(),
        quiet.manifests.clone(),
        restore(&quiet, prior.clone())?,
        &BTreeSet::new(),
        StateCacheCapacity::Unlimited,
    )
    .err()
    .expect("a quiet expiry outside certified nodes must reject publication");
    assert!(error.to_string().contains("accessed unloaded nodes"));
    let mut deps = V1BatchDependencies::default();
    deps.include_prior_events(&prior)?;
    let output = complete(
        prepare_schema_v2_batch_lookahead(
            quiet.clone(),
            quiet.manifests.clone(),
            restore(&quiet, prior.clone())?,
            &deps.nodes,
            StateCacheCapacity::Unlimited,
        )?,
        &prior,
    )?
    .0;
    assert!(
        output
            .normalized_events
            .iter()
            .any(|e| e.event_kind == "RegistrationReleased")
    );
    Ok(())
}

#[test]
fn child_only_history_does_not_supply_parent_surface_membership() -> anyhow::Result<()> {
    sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
    }
    const REGISTRY: &str = "0x0000000000000000000000000000000000000098";
    let parent = super::super::common::namehash(&["parent".to_owned(), "eth".to_owned()]);
    let child =
        super::super::common::namehash(&["kid".to_owned(), "parent".to_owned(), "eth".to_owned()]);
    let manifests = vec![
        manifest(
            95,
            "ens_v1_wrapper_l1",
            "NameWrapped",
            "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            &["name_wrapper"],
            &[
                "TokenControlTransferred",
                "ExpiryChanged",
                "PermissionScopeChanged",
                "AuthorityEpochChanged",
            ],
        ),
        manifest_with_events(
            96,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "NewOwner",
                    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                    &["registry"],
                    &[
                        "SubregistryChanged",
                        "AuthorityTransferred",
                        "AuthorityEpochChanged",
                    ],
                ),
                (
                    "NewResolver",
                    "event NewResolver(bytes32 indexed node, address resolver)",
                    &["registry"],
                    &["ResolverChanged"],
                ),
            ],
        ),
    ];
    let mut registry = admission(96, "registry");
    registry.address = REGISTRY.to_owned();
    let admissions = vec![admission(95, "name_wrapper"), registry];
    let prefix = input(
        manifests.clone(),
        admissions.clone(),
        vec![
            raw_at(
                NameWrapped {
                    node: child.parse()?,
                    name: b"\x03kid\x06parent\x03eth\0".to_vec().into(),
                    owner: CONTRACT.parse()?,
                    fuses: 1,
                    expiry: 1_000_000,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                NewOwner {
                    node: parent.parse()?,
                    label: keccak256("kid"),
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                1,
                REGISTRY,
            ),
        ],
    );
    let output = interpret_schema_v2_batch(prefix.clone())?;
    let edge = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "SubregistryChanged")
        .expect("child edge");
    assert_eq!(
        edge.logical_name_id.as_deref(),
        Some(format!("ens:{child}").as_str())
    );
    assert_eq!(edge.after_state["node"], parent);
    assert_eq!(edge.after_state["child_node"], child);
    let prior = seam::fold_prior_events(vec![], &output.normalized_events, &prefix.blocks)?;
    let mut suffix = input(
        manifests,
        admissions,
        vec![raw_at(
            NewResolver {
                node: parent.parse()?,
                resolver: CONTRACT.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        )],
    );
    let (_, selected) = scope(
        collect_v1_batch_dependencies(&suffix, &suffix.manifests)?,
        &prior,
    )?;
    assert!(
        selected.is_empty(),
        "parent-only request must not retain child history"
    );
    suffix.prior_events = prior;
    let session = assert_scoped_matches(suffix)?;
    assert!(
        session.v1_name("ens", &parent).is_none(),
        "a known child must not invent parent authority"
    );
    Ok(())
}

#[test]
fn restore_reads_outside_loaded_nodes_fail() -> anyhow::Result<()> {
    let manifests = vec![registrar_manifest()];
    let admissions = vec![admission(81, "registrar_controller")];
    let register = |name: &str, index| {
        raw_at(
            NameRegistered {
                name: name.to_owned(),
                label: keccak256(name),
                owner: CONTRACT.parse().unwrap(),
                expires: U256::from(1_000_000),
            }
            .encode_log_data(),
            1,
            index,
            CONTRACT,
        )
    };
    let prefix = input(
        manifests.clone(),
        admissions.clone(),
        vec![register("alice", 0), register("bob", 1)],
    );
    let output = interpret_schema_v2_batch(prefix.clone())?;
    let prior = seam::fold_prior_events(vec![], &output.normalized_events, &prefix.blocks)?;
    let node = |name: &str| V1NodeRequest {
        namespace: "ens".to_owned(),
        node: super::super::common::namehash(&[name.to_owned(), "eth".to_owned()]),
    };
    let begin = || {
        begin_schema_v2_adapter_restore(
            CHAIN.to_owned(),
            manifests.clone(),
            vec![],
            admissions.clone(),
            StateCacheCapacity::Unlimited,
        )
    };
    // Both names loaded: every restore read is inside the loaded set.
    let both = BTreeSet::from([node("alice"), node("bob")]);
    let session = restore_schema_v2_lookahead_session(begin()?, prior.clone(), None, &both)?;
    assert!(session.v1_name("ens", &node("bob").node).is_some());
    // Bob's events handed to a restore that loaded only alice would rebuild bob from an
    // arbitrary part of his history. Restore must refuse instead.
    let alice_only = BTreeSet::from([node("alice")]);
    let error = restore_schema_v2_lookahead_session(begin()?, prior, None, &alice_only)
        .err()
        .expect("restore outside the loaded names must fail");
    assert!(
        error.to_string().contains("accessed unloaded nodes")
            && error.to_string().contains(&node("bob").node),
        "{error:#}"
    );
    Ok(())
}

/// The lookahead index files an event under one name; restore must apply it to that same
/// name, or a loaded name would miss an event that the full-state loader applies to it.
#[test]
fn resolver_changed_node_precedence_matches_restore() -> anyhow::Result<()> {
    assert_eq!(
        seam::V1_EVENT_NODE_FIELDS,
        ["child_node", "namehash", "node"]
    );
    let hash = |byte: u8| format!("{:#x}", alloy_primitives::B256::repeat_byte(byte));
    let (parent, child, by_namehash, by_node) = (hash(1), hash(2), hash(3), hash(4));
    let resolver_changed = |after_state: Value, name: &str| PriorEventInput {
        retained_state_key: format!("test:{name}"),
        chain_id: CHAIN.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(format!("ens:{name}")),
        resource_id: Some(Uuid::from_u128(7)),
        event_kind: "ResolverChanged".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        emitting_address: None,
        state_scope: None,
        block_timestamp: None,
        after_state,
    };
    for (after_state, filed_under) in [
        // No adapter emits differing `namehash` and `node`; if one ever does, the index
        // files the event under `namehash`, so restore must read `namehash` first too.
        (
            json!({"node": by_node, "namehash": by_namehash, "resolver": CONTRACT}),
            &by_namehash,
        ),
        // A registry NewOwner carries its parent in `node` and the created name in `child_node`.
        (
            json!({"node": parent, "child_node": child, "resolver": CONTRACT}),
            &child,
        ),
    ] {
        assert_eq!(
            seam::v1_event_node(&after_state),
            Some(filed_under.as_str())
        );
        let event = resolver_changed(after_state, filed_under);
        assert_eq!(
            routed_name(&event).as_deref(),
            Some(format!("ens:{filed_under}").as_str())
        );
        // Restoring with only that name loaded proves restore touches no other name.
        let loaded = BTreeSet::from([V1NodeRequest {
            namespace: "ens".to_owned(),
            node: filed_under.clone(),
        }]);
        restore_schema_v2_lookahead_session(
            begin_schema_v2_adapter_restore(
                CHAIN.to_owned(),
                vec![registrar_manifest()],
                vec![],
                vec![],
                StateCacheCapacity::Unlimited,
            )?,
            vec![event],
            None,
            &loaded,
        )?;
    }
    Ok(())
}

/// Lookahead reads retained events of the `ens_v1_*` families only, yet it also covers these
/// families. That is sound because they keep no state: a manifest of one of them cannot
/// declare an event, so no log is ever interpreted under it and no event of it is stored.
#[test]
fn covered_families_outside_ens_v1_cannot_interpret_a_log() {
    for family in [
        "basenames_l1_compat",
        "basenames_execution",
        "ens_execution",
    ] {
        assert!(v1_lookahead_supports_family(family));
        let mut manifest = registrar_manifest();
        manifest.source_family = family.to_owned();
        let error = interpret_schema_v2_batch(input(vec![manifest], vec![], vec![]))
            .err()
            .expect("a declared event must be refused");
        assert!(
            format!("{error:#}").contains("has no typed schema-v2 adapter"),
            "{family}: {error:#}"
        );
    }
    for family in ["basenames_base_registry", "ens_v2_registry_l1"] {
        assert!(!v1_lookahead_supports_family(family));
    }
}
