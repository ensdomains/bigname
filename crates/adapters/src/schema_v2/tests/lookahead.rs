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

// In-memory oracle for direct-node/resource SQL selection. Production SQL has its own tests.
fn scope(
    mut deps: V1BatchDependencies,
    prior: &[PriorEventInput],
) -> anyhow::Result<(V1BatchDependencies, Vec<PriorEventInput>)> {
    loop {
        let rows = prior
            .iter()
            .filter(|event| {
                let node = ["child_node", "namehash", "node"]
                    .iter()
                    .find_map(|field| event.after_state.get(field).and_then(Value::as_str))
                    .or_else(|| {
                        event
                            .after_state
                            .pointer("/grant_source/node")
                            .and_then(Value::as_str)
                    })
                    .or_else(|| {
                        event
                            .after_state
                            .pointer("/revocation_source/node")
                            .and_then(Value::as_str)
                    });
                let direct = node.map(|node| V1NodeRequest {
                    namespace: event.namespace.clone(),
                    node: node.to_lowercase(),
                });
                let name = event
                    .logical_name_id
                    .as_deref()
                    .and_then(|s| s.split_once(':'))
                    .map(|(ns, n)| V1NodeRequest {
                        namespace: ns.to_owned(),
                        node: n.to_lowercase(),
                    });
                direct.as_ref().is_some_and(|n| deps.nodes.contains(n))
                    || (direct.as_ref().is_none_or(|n| deps.nodes.contains(n))
                        && (name.as_ref().is_some_and(|n| deps.nodes.contains(n))
                            || event
                                .resource_id
                                .is_some_and(|r| deps.resource_ids.contains(&r))))
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
            .map(|r| r.block_number)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(block)
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
    let (dependencies, rows) = scope(collect_v1_batch_dependencies(&input, &provenance)?, &prior)?;
    assert!(dependencies.unsupported.is_empty());
    let prepared = prepare_schema_v2_batch_lookahead(
        input.clone(),
        provenance,
        restore(&input, rows)?,
        &dependencies.nodes,
        StateCacheCapacity::Unlimited,
    )?;
    let (actual, session) = complete(prepared, &prior)?;
    assert_eq!(actual, expected, "complete scoped suffix output differs");
    Ok(session)
}

fn registrar_manifest() -> ManifestInput {
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
