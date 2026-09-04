use super::*;

const REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
const OLD_REGISTRY: &str = "0x00000000000000000000000000000000000000a0";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
const WRAPPER: &str = "0x00000000000000000000000000000000000000a6";
const OWNER: &str = "0x00000000000000000000000000000000000000a3";
const OWNER_2: &str = "0x00000000000000000000000000000000000000a5";
const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a4";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b4";
const REGISTRY_MANIFEST_ID: i64 = 6131;
const REGISTRAR_MANIFEST_ID: i64 = 6132;
const WRAPPER_MANIFEST_ID: i64 = 6133;

fn fixture() -> (Vec<ManifestInput>, Vec<AddressAdmissionInput>, B256) {
    let registry_manifest = manifest_with_events(
        REGISTRY_MANIFEST_ID,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry", "registry_old"],
                &[
                    "SubregistryChanged",
                    "AuthorityTransferred",
                    "ResolverChanged",
                    "PermissionChanged",
                ],
            ),
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry", "registry_old"],
                &[
                    "AuthorityTransferred",
                    "ResolverChanged",
                    "PermissionChanged",
                ],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry", "registry_old"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    );
    let registrar_manifest = manifest_with_events(
        REGISTRAR_MANIFEST_ID,
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
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
                &["registrar"],
                &[
                    "RegistrationGranted",
                    "RegistrationRenewed",
                    "ExpiryChanged",
                    "PreimageObserved",
                ],
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
    );
    let wrapper_manifest = manifest_with_events(
        WRAPPER_MANIFEST_ID,
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
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                    "PermissionChanged",
                    "PreimageObserved",
                ],
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
                ],
            ),
        ],
    );
    let mut registry = admission(REGISTRY_MANIFEST_ID, "registry");
    registry.address = REGISTRY.to_owned();
    let mut old_registry = admission(REGISTRY_MANIFEST_ID, "registry_old");
    old_registry.address = OLD_REGISTRY.to_owned();
    old_registry.contract_instance_id = Uuid::from_u128(6_130);
    let mut registrar = admission(REGISTRAR_MANIFEST_ID, "registrar");
    registrar.address = REGISTRAR.to_owned();
    let mut wrapper = admission(WRAPPER_MANIFEST_ID, "name_wrapper");
    wrapper.address = WRAPPER.to_owned();
    wrapper.contract_instance_id = Uuid::from_u128(6_133);
    let node = super::common::namehash(&["pointer".to_owned(), "eth".to_owned()])
        .parse()
        .expect("fixture node");
    (
        vec![registry_manifest, registrar_manifest, wrapper_manifest],
        vec![registry, old_registry, registrar, wrapper],
        node,
    )
}

fn prefix(owner: &str, node: B256, clear: bool) -> anyhow::Result<Vec<RawLogInput>> {
    let mut logs = vec![raw_at(
        v1_registry::NewResolver {
            node,
            resolver: RESOLVER_A.parse()?,
        }
        .encode_log_data(),
        1,
        0,
        REGISTRY,
    )];
    if clear {
        logs.push(raw_at(
            v1_registry::NewResolver {
                node,
                resolver: ZERO_ADDRESS.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        ));
    }
    logs.push(raw_at(
        v1_registry::Transfer {
            node,
            owner: owner.parse()?,
        }
        .encode_log_data(),
        if clear { 3 } else { 2 },
        0,
        REGISTRY,
    ));
    Ok(logs)
}

fn renewal(block: i64) -> RawLogInput {
    renewal_with_expiry(block, 9_999)
}

fn renewal_with_expiry(block: i64, expiry: u64) -> RawLogInput {
    raw_at(
        NameRenewed {
            name: "pointer".to_owned(),
            label: keccak256(b"pointer"),
            expires: U256::from(expiry),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    )
}

fn registration(block: i64, expiry: u64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        NameRegistered {
            name: "pointer".to_owned(),
            label: keccak256(b"pointer"),
            owner: OWNER_2.parse()?,
            expires: U256::from(expiry),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    ))
}

fn wrapped(block: i64) -> anyhow::Result<RawLogInput> {
    let node = super::common::namehash(&["pointer".to_owned(), "eth".to_owned()]).parse()?;
    Ok(raw_at(
        NameWrapped {
            node,
            name: b"\x07pointer\x03eth\0".to_vec().into(),
            owner: OWNER_2.parse()?,
            fuses: 1,
            expiry: 9_999,
        }
        .encode_log_data(),
        block,
        0,
        WRAPPER,
    ))
}

fn unwrapped(block: i64) -> anyhow::Result<RawLogInput> {
    let node = super::common::namehash(&["pointer".to_owned(), "eth".to_owned()]).parse()?;
    Ok(raw_at(
        NameUnwrapped {
            node,
            owner: OWNER_2.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        WRAPPER,
    ))
}

fn registrar_transfer(from: &str, to: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registrar::Transfer {
            from: from.parse()?,
            to: to.parse()?,
            tokenId: U256::from_be_slice(keccak256(b"pointer").as_slice()),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    ))
}

fn current_new_owner(owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::NewOwner {
            node: super::common::namehash(&["eth".to_owned()]).parse()?,
            label: keccak256(b"pointer"),
            owner: owner.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        REGISTRY,
    ))
}

fn old_new_owner(owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    let mut log = current_new_owner(owner, block)?;
    log.emitting_address = OLD_REGISTRY.to_owned();
    Ok(log)
}

fn current_transfer(node: B256, owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::Transfer {
            node,
            owner: owner.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        REGISTRY,
    ))
}

fn resolver_selection(
    registry: &str,
    node: B256,
    resolver: &str,
    block: i64,
) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::NewResolver {
            node,
            resolver: resolver.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        registry,
    ))
}

fn old_transfer(node: B256, owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::Transfer {
            node,
            owner: owner.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        OLD_REGISTRY,
    ))
}

fn input(
    manifests: Vec<ManifestInput>,
    admissions: Vec<AddressAdmissionInput>,
    prior_events: Vec<PriorEventInput>,
    raw_logs: Vec<RawLogInput>,
) -> BatchInput {
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events,
        blocks: Vec::new(),
        raw_logs,
    }
}

fn run(owner: &str, clear: bool) -> anyhow::Result<(BatchOutput, BatchOutput)> {
    let (manifests, admissions, node) = fixture();
    let prefix_output = interpret_test_batch(input(
        manifests.clone(),
        admissions.clone(),
        Vec::new(),
        prefix(owner, node, clear)?,
    ))?;
    let mut logs = prefix(owner, node, clear)?;
    logs.push(renewal(if clear { 4 } else { 3 }));
    let output = interpret_test_batch(input(manifests, admissions, Vec::new(), logs))?;
    Ok((prefix_output, output))
}

fn run_live_and_restored(owner: &str, clear: bool) -> anyhow::Result<(BatchOutput, BatchOutput)> {
    let (manifests, admissions, node) = fixture();
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            prefix(owner, node, clear)?,
        ),
        None,
    )?;
    let suffix = renewal(if clear { 4 } else { 3 });
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let (restored, _) =
        interpret_test_batch_incremental(input(manifests, admissions, prior, vec![suffix]), None)?;
    Ok((live, restored))
}

fn prepare_with_provenance(
    mut batch: BatchInput,
    provenance: Vec<ManifestInput>,
    session: Option<AdapterSession>,
) -> anyhow::Result<(BatchOutput, AdapterSession)> {
    if batch.blocks.is_empty() {
        let mut blocks = std::collections::BTreeMap::new();
        for raw in &batch.raw_logs {
            blocks
                .entry((raw.block_number, raw.block_hash.clone()))
                .or_insert_with(|| RawBlockInput {
                    chain_id: raw.chain_id.clone(),
                    block_hash: raw.block_hash.clone(),
                    block_number: raw.block_number,
                    block_timestamp: raw.block_timestamp,
                    canonicality_state: raw.canonicality_state.clone(),
                });
        }
        batch.blocks = blocks.into_values().collect();
    }
    super::super::prepare_schema_v2_batch_incremental_with_provenance(
        batch,
        provenance,
        session,
        StateCacheCapacity::Unlimited,
    )?
    .finish(Vec::new())
}

fn state_derived_pointer(output: &BatchOutput) -> Option<&NormalizedEvent> {
    output.normalized_events.iter().find(|event| {
        event.event_kind == "ResolverChanged"
            && event.after_state["state_derived"] == true
            && event.after_state["surface_materialization"] == true
    })
}

fn compact_prior(events: &[NormalizedEvent]) -> Vec<PriorEventInput> {
    let prior = events.iter().map(prior_event).collect::<Vec<_>>();
    let mut last_index = std::collections::HashMap::new();
    for (index, event) in prior.iter().enumerate() {
        last_index.insert(event.retained_state_key.clone(), index);
    }
    prior
        .into_iter()
        .enumerate()
        .filter(|(index, event)| last_index[&event.retained_state_key] == *index)
        .map(|(_, event)| event)
        .collect()
}

fn run_batches(
    manifests: &[ManifestInput],
    admissions: &[AddressAdmissionInput],
    batches: Vec<Vec<RawLogInput>>,
) -> anyhow::Result<Vec<NormalizedEvent>> {
    let mut session = None;
    let mut events = Vec::new();
    for raw_logs in batches {
        let (output, next) = interpret_test_batch_incremental(
            input(
                manifests.to_vec(),
                admissions.to_vec(),
                Vec::new(),
                raw_logs,
            ),
            session,
        )?;
        events.extend(output.normalized_events);
        session = Some(next);
    }
    Ok(events)
}

fn assert_four_way_and_restore_parity(
    history: &[RawLogInput],
    prefix_len: usize,
) -> anyhow::Result<(Vec<NormalizedEvent>, BatchOutput)> {
    let (manifests, admissions, _) = fixture();
    let single = run_batches(&manifests, &admissions, vec![history.to_vec()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        history.iter().cloned().map(|event| vec![event]).collect(),
    )?;
    let split_at_prefix = run_batches(
        &manifests,
        &admissions,
        vec![
            history[..prefix_len].to_vec(),
            history[prefix_len..].to_vec(),
        ],
    )?;
    let alternate_split = run_batches(
        &manifests,
        &admissions,
        vec![history[..1].to_vec(), history[1..].to_vec()],
    )?;
    assert_eq!(single, per_block, "per-block replay drift");
    assert_eq!(single, split_at_prefix, "prefix/suffix replay drift");
    assert_eq!(single, alternate_split, "alternate split replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            history[..prefix_len].to_vec(),
        ),
        None,
    )?;
    let suffix = history[prefix_len..].to_vec();
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            suffix.clone(),
        ),
        Some(session),
    )?;
    for prior in [
        prefix_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        compact_prior(&prefix_output.normalized_events),
    ] {
        let (restored, _) = interpret_test_batch_incremental(
            input(manifests.clone(), admissions.clone(), prior, suffix.clone()),
            None,
        )?;
        assert_eq!(live, restored, "cold restore drift");
    }
    Ok((single, live))
}

fn assert_current_registry_reassignment_replays(
    reassigned_owner: &str,
    transfer_suffix: bool,
) -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let prefix = vec![
        current_new_owner(OWNER, 1)?,
        raw_at(
            v1_registry::NewResolver {
                node,
                resolver: RESOLVER_A.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        ),
        current_new_owner(reassigned_owner, 3)?,
    ];
    let suffix = if transfer_suffix {
        current_transfer(node, OWNER_2, 4)?
    } else {
        renewal(4)
    };
    let mut history = prefix.clone();
    history.push(suffix.clone());

    let single = run_batches(&manifests, &admissions, vec![history.clone()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        history.iter().cloned().map(|event| vec![event]).collect(),
    )?;
    let split_three_one = run_batches(
        &manifests,
        &admissions,
        vec![history[..3].to_vec(), history[3..].to_vec()],
    )?;
    let split_two_two = run_batches(
        &manifests,
        &admissions,
        vec![history[..2].to_vec(), history[2..].to_vec()],
    )?;
    assert_eq!(single, per_block, "per-block replay drift");
    assert_eq!(single, split_three_one, "3|1 replay drift");
    assert_eq!(single, split_two_two, "2|2 replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(manifests.clone(), admissions.clone(), Vec::new(), prefix),
        None,
    )?;
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let full_prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let compacted_prior = compact_prior(&prefix_output.normalized_events);
    let (restored_full, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            full_prior,
            vec![suffix.clone()],
        ),
        None,
    )?;
    let (restored_compacted, _) = interpret_test_batch_incremental(
        input(manifests, admissions, compacted_prior, vec![suffix]),
        None,
    )?;
    assert_eq!(live, restored_full, "full restore drift");
    assert_eq!(live, restored_compacted, "compacted restore drift");
    Ok(())
}

#[test]
fn current_registry_reassignment_preserves_resolver_across_every_replay_shape() -> anyhow::Result<()>
{
    assert_current_registry_reassignment_replays(OWNER_2, false)?;
    assert_current_registry_reassignment_replays(OWNER, false)?;
    assert_current_registry_reassignment_replays(OWNER_2, true)
}

#[test]
fn registered_pre_surface_registry_authority_reactivates_after_expiry_in_every_replay_shape()
-> anyhow::Result<()> {
    const EXPIRY: u64 = 100;
    const RELEASE_BLOCK: i64 = 7_776_101;
    let (manifests, admissions, node) = fixture();
    let prefix = vec![current_new_owner(OWNER, 1)?, registration(2, EXPIRY)?];
    let release_trigger = current_transfer(B256::ZERO, OWNER_2, RELEASE_BLOCK)?;
    let mut history = prefix.clone();
    history.push(release_trigger.clone());

    let single = run_batches(&manifests, &admissions, vec![history.clone()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        history.iter().cloned().map(|event| vec![event]).collect(),
    )?;
    let split = run_batches(
        &manifests,
        &admissions,
        vec![prefix.clone(), vec![release_trigger.clone()]],
    )?;
    assert_eq!(single, per_block, "per-block replay drift");
    assert_eq!(single, split, "incremental replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(manifests.clone(), admissions.clone(), Vec::new(), prefix),
        None,
    )?;
    let registry_resource = prefix_output
        .normalized_events
        .iter()
        .find(|event| event.block_number == Some(1) && event.event_kind == "AuthorityTransferred")
        .and_then(|event| event.resource_id)
        .expect("retained registry authority resource");
    let (live, live_session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![release_trigger.clone()],
        ),
        Some(session),
    )?;
    for prior in [
        prefix_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        compact_prior(&prefix_output.normalized_events),
    ] {
        let (restored, restored_session) = interpret_test_batch_incremental(
            input(
                manifests.clone(),
                admissions.clone(),
                prior,
                vec![release_trigger.clone()],
            ),
            None,
        )?;
        assert_eq!(live, restored, "cold restore output drift");
        assert_eq!(live_session, restored_session, "cold restore state drift");
    }
    let rebound = live
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(RELEASE_BLOCK)
                && event.event_kind == "SurfaceBound"
                && event.resource_id == Some(registry_resource)
        })
        .expect("expiry must reactivate the retained registry surface");
    assert_eq!(rebound.after_state["authority_kind"], "registry_only");
    assert_eq!(
        live_session
            .v1_name("ens", &format!("{node:#x}"))
            .and_then(|authority| authority.owner),
        Some(OWNER.to_owned()),
        "known name must retain its direct-registry owner after registrar expiry"
    );
    Ok(())
}

fn assert_original_unchanged(prefix: &BatchOutput, complete: &BatchOutput) {
    let original = prefix
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ResolverChanged" && event.after_state["resolver"] == RESOLVER_A
        })
        .expect("original pre-surface pointer");
    assert_eq!(original.logical_name_id, None);
    assert_eq!(original.resource_id, None);
    assert_eq!(
        complete
            .normalized_events
            .iter()
            .find(|event| event.event_identity == original.event_identity),
        Some(original),
        "surface materialization must not rewrite the raw-derived pointer"
    );
}

#[test]
fn pre_surface_registry_resolver_materialization_links_current_authority() -> anyhow::Result<()> {
    let (prefix, output) = run(OWNER, false)?;
    assert_original_unchanged(&prefix, &output);
    let authority_resource = prefix
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .and_then(|event| event.resource_id)
        .expect("registry-only authority resource");
    let pointer = state_derived_pointer(&output).expect("linked materialization pointer");
    assert_eq!(pointer.source_family, "ens_v1_registry_l1");
    assert_eq!(pointer.source_manifest_id, Some(REGISTRY_MANIFEST_ID));
    assert_eq!(pointer.resource_id, Some(authority_resource));
    assert_eq!(pointer.after_state["resolver"], RESOLVER_A);
    assert_eq!(pointer.block_number, Some(3));
    assert_eq!(pointer.transaction_hash.as_deref(), Some("transaction-3"));
    assert_eq!(pointer.log_index, Some(0));
    assert_eq!(output.surface_bindings.len(), 1);
    let surface = output
        .normalized_events
        .iter()
        .find(|event| event.block_number == Some(3) && event.event_kind == "SurfaceBound")
        .expect("surface materialization must make the retained owner projectable");
    assert_eq!(surface.resource_id, Some(authority_resource));
    assert_eq!(surface.after_state["owner"], OWNER);
    assert!(output.normalized_events.iter().all(|event| {
        !(event.block_number == Some(3)
            && matches!(
                event.event_kind.as_str(),
                "AuthorityEpochChanged" | "PermissionChanged"
            ))
    }));
    Ok(())
}

#[test]
fn pre_surface_ownerless_resolver_materialization_links_read_anchor_without_control()
-> anyhow::Result<()> {
    let (prefix, output) = run(REGISTRY, false)?;
    assert_original_unchanged(&prefix, &output);
    let pointer = state_derived_pointer(&output).expect("ownerless retained-anchor pointer");
    assert_eq!(pointer.source_family, "ens_v1_registry_l1");
    assert_eq!(pointer.source_manifest_id, Some(REGISTRY_MANIFEST_ID));
    assert!(pointer.logical_name_id.is_some());
    assert!(pointer.resource_id.is_some());
    assert!(output.surface_bindings.is_empty());
    assert!(output.normalized_events.iter().all(|event| {
        !(event.block_number == Some(3)
            && matches!(
                event.event_kind.as_str(),
                "SurfaceBound" | "AuthorityEpochChanged" | "PermissionChanged"
            ))
    }));
    Ok(())
}

#[test]
fn old_registry_resolver_is_not_materialized_after_current_registry_migration() -> anyhow::Result<()>
{
    let (manifests, admissions, node) = fixture();
    let parent = super::common::namehash(&["eth".to_owned()]).parse()?;
    let label = keccak256(b"pointer");
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![
                raw_at(
                    v1_registry::NewResolver {
                        node,
                        resolver: RESOLVER_A.parse()?,
                    }
                    .encode_log_data(),
                    1,
                    0,
                    OLD_REGISTRY,
                ),
                raw_at(
                    v1_registry::NewOwner {
                        node: parent,
                        label,
                        owner: OWNER.parse()?,
                    }
                    .encode_log_data(),
                    2,
                    0,
                    REGISTRY,
                ),
            ],
        ),
        None,
    )?;
    let suffix = renewal(3);
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    assert!(prefix_output.normalized_events.iter().any(|event| {
        event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == RESOLVER_A
            && event.block_number == Some(1)
    }));
    let (restored, _) =
        interpret_test_batch_incremental(input(manifests, admissions, prior, vec![suffix]), None)?;
    assert!(
        state_derived_pointer(&live).is_none(),
        "the old fallback registry resolver must not surface after current-registry migration"
    );
    assert_eq!(live, restored);
    Ok(())
}

#[test]
fn current_registry_transfer_invalidates_only_old_registry_resolver_links_in_every_replay_shape()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let migrated_history = vec![
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 1)?,
        current_transfer(node, OWNER, 2)?,
        renewal(3),
    ];
    let (single, live) = assert_four_way_and_restore_parity(&migrated_history, 2)?;
    assert!(single.iter().any(|event| {
        event.block_number == Some(1)
            && event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == RESOLVER_A
    }));
    assert!(
        state_derived_pointer(&live).is_none(),
        "a current-registry Transfer must end an old-registry fallback pointer"
    );

    let current_pointer_history = vec![
        current_new_owner(OWNER, 1)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 2)?,
        old_transfer(node, OWNER_2, 3)?,
        renewal(4),
    ];
    let (_, mirror_live) = assert_four_way_and_restore_parity(&current_pointer_history, 3)?;
    let pointer = state_derived_pointer(&mirror_live)
        .expect("an old-registry Transfer must not clear a current-registry resolver pointer");
    assert_eq!(pointer.after_state["resolver"], RESOLVER_A);
    Ok(())
}

#[test]
fn same_owner_transfer_retracts_selected_old_registry_fallback_across_every_replay_shape()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        current_transfer(node, OWNER, 3)?,
        renewal(4),
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 3)?;
    assert!(state_derived_pointer(&live).is_none());
    assert!(single.iter().any(|event| {
        event.block_number == Some(3)
            && event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == ZERO_ADDRESS
            && event.after_state["registry_fallback_handoff"] == true
    }));
    Ok(())
}

#[test]
fn same_owner_transfer_without_a_pointer_still_suppresses_later_old_registry_logs()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        current_transfer(node, OWNER, 2)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 3)?,
        renewal(4),
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 2)?;
    assert!(state_derived_pointer(&live).is_none());
    assert!(single.iter().all(|event| {
        !(event.block_number == Some(3)
            && event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == RESOLVER_A)
    }));
    assert!(single.iter().any(|event| {
        event.block_number == Some(2)
            && event.event_kind == "AuthorityTransferred"
            && event.after_state["source_event"] == "Transfer"
    }));
    Ok(())
}

#[test]
fn current_registry_record_retracts_a_surfaced_old_registry_resolver() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    for ownership in [
        current_transfer(node, OWNER, 4)?,
        current_new_owner(OWNER, 4)?,
    ] {
        let history = vec![
            old_new_owner(OWNER, 1)?,
            resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
            renewal(3),
            ownership,
            renewal(5),
        ];
        let (single, live) = assert_four_way_and_restore_parity(&history, 4)?;
        assert!(state_derived_pointer(&live).is_none());
        let clear = single
            .iter()
            .find(|event| {
                event.block_number == Some(4)
                    && event.event_kind == "ResolverChanged"
                    && event.after_state["registry_fallback_handoff"] == true
            })
            .expect("current registry ownership must retract the surfaced old pointer");
        assert_eq!(clear.after_state["resolver"], ZERO_ADDRESS);
        assert_eq!(clear.after_state["previous_resolver"], RESOLVER_A);
        assert!(clear.logical_name_id.is_some());
        assert!(clear.resource_id.is_some());
        assert!(single.iter().any(|event| {
            event.block_number == Some(4)
                && event.event_kind == "PermissionChanged"
                && event.after_state["effective_powers"] == json!([])
                && event.after_state["scope"]["resolver_address"] == RESOLVER_A
        }));
    }
    Ok(())
}

#[test]
fn current_registry_handoff_retracts_old_resolver_from_every_linked_resource() -> anyhow::Result<()>
{
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        registrar_transfer(OWNER, OWNER, 4)?,
        current_new_owner(OWNER, 5)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 4)?;
    let linked_nonzero = single
        .iter()
        .filter(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_A
                && event.resource_id.is_some()
        })
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        linked_nonzero.len(),
        2,
        "fixture must create dual-resource pointers"
    );
    for resource_id in linked_nonzero {
        assert!(
            single.iter().any(|event| {
                event.block_number == Some(5)
                    && event.event_kind == "ResolverChanged"
                    && event.resource_id == Some(resource_id)
                    && event.after_state["resolver"] == ZERO_ADDRESS
                    && event.after_state["registry_fallback_handoff"] == true
            }),
            "current-registry handoff did not retract resolver from {resource_id}"
        );
    }
    Ok(())
}

#[test]
fn same_transaction_registration_keeps_fallback_clear_on_registry_resource() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let mut ownership = current_new_owner(OWNER_2, 4)?;
    ownership.log_index = 0;
    let mut registered = registration(4, 9_999)?;
    registered.log_index = 1;
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        ownership,
        registered,
    ];
    let output = interpret_test_batch(input(fixture().0, fixture().1, Vec::new(), history))?;
    let registry_resource = output
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_A
                && event.after_state["state_derived"] == true
        })
        .and_then(|event| event.resource_id)
        .expect("materialization must link the old-registry resolver to the registry resource");
    assert!(
        output.normalized_events.iter().any(|event| {
            event.block_number == Some(4)
                && event.event_kind == "ResolverChanged"
                && event.resource_id == Some(registry_resource)
                && event.after_state["resolver"] == ZERO_ADDRESS
                && event.after_state["registry_fallback_handoff"] == true
        }),
        "same-transaction reconciliation moved the fallback clear off the registry resource"
    );
    Ok(())
}

#[test]
fn same_transaction_transient_setup_reinserts_each_fallback_clear() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let mut controller_ownership = current_new_owner(REGISTRAR, 5)?;
    controller_ownership.log_index = 0;
    let mut resolver_clear = resolver_selection(REGISTRY, node, ZERO_ADDRESS, 5)?;
    resolver_clear.log_index = 1;
    let mut final_ownership = current_new_owner(OWNER_2, 5)?;
    final_ownership.log_index = 2;
    let mut registered = registration(5, 9_999)?;
    registered.log_index = 3;
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        registrar_transfer(OWNER, OWNER, 4)?,
        controller_ownership,
        resolver_clear,
        final_ownership,
        registered,
    ];
    let output = interpret_test_batch(input(fixture().0, fixture().1, Vec::new(), history))?;
    let linked_resources = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number < Some(5)
                && event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_A
        })
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(linked_resources.len(), 2, "fixture must link two resources");
    for resource_id in linked_resources {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.block_number == Some(5)
                    && event.event_kind == "ResolverChanged"
                    && event.resource_id == Some(resource_id)
                    && event.after_state["resolver"] == ZERO_ADDRESS
                    && event.after_state["registry_fallback_handoff"] == true
            }),
            "same-transaction transient setup dropped the fallback clear for {resource_id}"
        );
    }
    Ok(())
}

#[test]
fn current_registry_handoff_retracts_old_resolver_from_wrapper_resource_across_replay_shapes()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        wrapped(4)?,
        current_new_owner(OWNER_2, 5)?,
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 4)?;
    let wrapper_resource = single
        .iter()
        .find(|event| {
            event.block_number == Some(4)
                && event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .and_then(|event| event.resource_id)
        .expect("wrapper authority resource");
    assert!(single.iter().any(|event| {
        event.block_number == Some(4)
            && event.event_kind == "ResolverChanged"
            && event.resource_id == Some(wrapper_resource)
            && event.after_state["resolver"] == RESOLVER_A
    }));
    assert!(live.normalized_events.iter().any(|event| {
        event.event_kind == "ResolverChanged"
            && event.resource_id == Some(wrapper_resource)
            && event.after_state["resolver"] == ZERO_ADDRESS
            && event.after_state["registry_fallback_handoff"] == true
    }));
    Ok(())
}

#[test]
fn current_registry_handoff_retracts_old_resolver_from_historical_wrapper_resource()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        wrapped(4)?,
        unwrapped(5)?,
        current_new_owner(OWNER_2, 6)?,
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 5)?;
    let wrapper_resource = single
        .iter()
        .find(|event| {
            event.block_number == Some(4)
                && event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .and_then(|event| event.resource_id)
        .expect("wrapper authority resource");
    assert!(single.iter().any(|event| {
        event.block_number == Some(4)
            && event.event_kind == "ResolverChanged"
            && event.resource_id == Some(wrapper_resource)
            && event.after_state["resolver"] == RESOLVER_A
    }));
    assert!(
        live.normalized_events.iter().any(|event| {
            event.event_kind == "ResolverChanged"
                && event.resource_id == Some(wrapper_resource)
                && event.after_state["resolver"] == ZERO_ADDRESS
                && event.after_state["registry_fallback_handoff"] == true
        }),
        "current-registry handoff left a stale pointer on the historical wrapper resource"
    );
    Ok(())
}

#[test]
fn wrapper_first_surface_links_the_retained_registry_read_resource() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        current_transfer(node, OWNER, 1)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 2)?,
        wrapped(3)?,
        current_transfer(node, ZERO_ADDRESS, 4)?,
        unwrapped(5)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 3)?;
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node:#x}"));
    assert!(
        single.iter().any(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.source_family == "ens_v1_registry_l1"
                && event.logical_name_id.is_some()
                && event.resource_id == Some(registry_resource)
                && event.after_state["resolver"] == RESOLVER_A
                && event.after_state["surface_materialization"] == true
        }),
        "wrapper-first materialization did not link the retained registry read resource"
    );
    assert!(
        single.iter().all(|event| {
            !(event.block_number == Some(3)
                && event.event_kind == "SurfaceBound"
                && event.resource_id == Some(registry_resource))
        }),
        "wrapper-first materialization must not bind the dormant registry resource"
    );
    Ok(())
}

#[test]
fn same_transaction_registration_keeps_wrapper_surface_pointer_on_registry_resource()
-> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let mut wrapper_surface = wrapped(3)?;
    wrapper_surface.log_index = 0;
    let mut registered = registration(3, 9_999)?;
    registered.log_index = 1;
    let history = vec![
        current_transfer(node, OWNER, 1)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 2)?,
        wrapper_surface,
        registered,
        current_transfer(node, ZERO_ADDRESS, 4)?,
        unwrapped(5)?,
    ];
    let single = run_batches(&manifests, &admissions, vec![history.clone()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        vec![
            history[..1].to_vec(),
            history[1..2].to_vec(),
            history[2..4].to_vec(),
            history[4..5].to_vec(),
            history[5..].to_vec(),
        ],
    )?;
    let split = run_batches(
        &manifests,
        &admissions,
        vec![
            history[..2].to_vec(),
            history[2..4].to_vec(),
            history[4..].to_vec(),
        ],
    )?;
    assert_eq!(single, per_block, "physical-block replay drift");
    assert_eq!(single, split, "split replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            history[..4].to_vec(),
        ),
        None,
    )?;
    let suffix = history[4..].to_vec();
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            suffix.clone(),
        ),
        Some(session),
    )?;
    for prior in [
        prefix_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        compact_prior(&prefix_output.normalized_events),
    ] {
        let (restored, _) = interpret_test_batch_incremental(
            input(manifests.clone(), admissions.clone(), prior, suffix.clone()),
            None,
        )?;
        assert_eq!(live, restored, "cold restore drift");
    }

    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node:#x}"));
    assert!(
        single.iter().any(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.source_family == "ens_v1_registry_l1"
                && event.resource_id == Some(registry_resource)
                && event.after_state["resolver"] == RESOLVER_A
                && event.after_state["surface_materialization"] == true
                && event.after_state["source_event"] == "NameWrapped"
        }),
        "same-transaction registration moved the wrapper surface pointer off the registry resource"
    );
    Ok(())
}

#[test]
fn current_registry_ownership_preserves_the_old_registry_root_resolver() -> anyhow::Result<()> {
    const ROOT_NODE: B256 = B256::ZERO;
    let history = vec![
        resolver_selection(OLD_REGISTRY, ROOT_NODE, RESOLVER_A, 1)?,
        current_transfer(ROOT_NODE, OWNER, 2)?,
        resolver_selection(OLD_REGISTRY, ROOT_NODE, RESOLVER_B, 3)?,
    ];
    let (_, live) = assert_four_way_and_restore_parity(&history, 2)?;
    assert!(
        live.normalized_events.iter().all(|event| {
            !(event.event_kind == "ResolverChanged"
                && event.after_state["registry_fallback_handoff"] == true)
        }),
        "current-registry ownership incorrectly cleared the old-registry root resolver exception"
    );
    let replacement = live
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_B
        })
        .expect("later old-registry root resolver update");
    assert_eq!(replacement.before_state["resolver"], RESOLVER_A);
    Ok(())
}

#[test]
fn latest_pre_surface_zero_resolver_clear_suppresses_materialization_pointer() -> anyhow::Result<()>
{
    for owner in [OWNER, REGISTRY] {
        let (output, restored) = run_live_and_restored(owner, true)?;
        assert_eq!(output, restored, "owner case {owner} restore drift");
        assert_eq!(
            output
                .normalized_events
                .iter()
                .filter(|event| event.after_state["state_derived"] == true
                    && event.event_kind == "ResolverChanged")
                .count(),
            0,
            "owner case {owner} resurrected resolver A"
        );
        assert!(output.normalized_events.iter().all(|event| {
            !(event.block_number == Some(4) && event.after_state["resolver"] == RESOLVER_A)
        }));
        if owner == OWNER {
            assert!(output.normalized_events.iter().any(|event| {
                event.block_number == Some(4)
                    && event.event_kind == "SurfaceBound"
                    && event.after_state["state_derived"] == true
            }));
        } else {
            assert!(output.surface_bindings.is_empty());
        }
    }
    Ok(())
}

fn assert_restore(owner: &str) -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            prefix(owner, node, false)?,
        ),
        None,
    )?;
    let suffix = renewal(3);
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let (restored, _) =
        interpret_test_batch_incremental(input(manifests, admissions, prior, vec![suffix]), None)?;
    assert!(state_derived_pointer(&live).is_some());
    assert_eq!(live, restored);
    Ok(())
}

#[test]
fn pre_surface_registry_resolver_surface_promotion_restores_exactly() -> anyhow::Result<()> {
    assert_restore(OWNER)
}

#[test]
fn pre_surface_ownerless_resolver_surface_promotion_restores_exactly() -> anyhow::Result<()> {
    assert_restore(REGISTRY)
}

#[test]
fn deprecated_registry_manifest_remains_available_for_materialization_provenance()
-> anyhow::Result<()> {
    let (provenance, admissions, node) = fixture();
    let active = vec![provenance[1].clone()];
    let registrar_admissions = vec![admissions[2].clone()];
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            provenance.clone(),
            admissions,
            Vec::new(),
            prefix(OWNER, node, false)?,
        ),
        None,
    )?;
    let suffix = renewal(3);
    let (live, _) = prepare_with_provenance(
        input(
            active.clone(),
            registrar_admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        provenance.clone(),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let (restored, _) = prepare_with_provenance(
        input(active, registrar_admissions, prior, vec![suffix]),
        provenance,
        None,
    )?;
    let pointer = state_derived_pointer(&live).expect("deprecated-manifest pointer");
    assert_eq!(pointer.source_manifest_id, Some(REGISTRY_MANIFEST_ID));
    assert_eq!(pointer.source_family, "ens_v1_registry_l1");
    assert_eq!(live, restored);
    Ok(())
}

#[test]
fn unknown_registry_manifest_fails_live_and_cold_restore_with_context() -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let active = vec![manifests[1].clone()];
    let registrar_admissions = vec![admissions[2].clone()];
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            prefix(OWNER, node, false)?,
        ),
        None,
    )?;
    let live_error = prepare_with_provenance(
        input(
            active.clone(),
            registrar_admissions.clone(),
            Vec::new(),
            vec![renewal(3)],
        ),
        active.clone(),
        Some(session),
    )
    .expect_err("live materialization must reject unknown provenance");

    let mut history = prefix(OWNER, node, false)?;
    history.push(renewal(3));
    let complete = interpret_test_batch(input(manifests, admissions, Vec::new(), history))?;
    let mut restore = super::super::begin_schema_v2_adapter_restore_with_provenance(
        CHAIN.to_owned(),
        active.clone(),
        active,
        Vec::new(),
        registrar_admissions,
        StateCacheCapacity::Unlimited,
    )?;
    let restore_error = restore
        .apply_prior_events(complete.normalized_events.iter().map(prior_event).collect())
        .expect_err("cold restore must reject unknown provenance");
    let expected = format!(
        "state-derived source manifest is missing for namespace ens, namehash {node:#x}, manifest {REGISTRY_MANIFEST_ID}"
    );
    assert!(
        format!("{live_error:#}").contains(&expected),
        "{live_error:#}"
    );
    assert!(
        restore_error.to_string().contains(&expected),
        "{restore_error:#}"
    );
    assert!(prefix_output.normalized_events.iter().any(|event| {
        event.event_kind == "ResolverChanged"
            && event.source_manifest_id == Some(REGISTRY_MANIFEST_ID)
    }));
    Ok(())
}

#[test]
fn surfaced_transfer_fallback_without_manifest_never_requires_materialization_provenance()
-> anyhow::Result<()> {
    let mut state = super::super::state::State::new(Vec::new(), Vec::new());
    let namehash = format!("{:#x}", B256::ZERO);
    let authority = super::super::state::V1NameState {
        logical_name_id: format!("ens:{namehash}"),
        surface_known: true,
        resource_id: Uuid::from_u128(613),
        token_lineage_id: None,
        authority_source_family: "ens_v1_registry_l1".to_owned(),
        source_manifest_id: None,
        labelhash: Some(format!("{:#x}", B256::ZERO)),
        expiry: None,
        owner: Some(OWNER.to_owned()),
        authority_key: Some("transfer-fallback".to_owned()),
        wrapper_fallback: false,
    };
    state.remember_v1_registry_authority("ens", &namehash, authority.clone());
    state.activate_v1_authority("ens", &namehash, Some(authority));
    assert_eq!(
        state.materialize_v1_active_surface(
            "ens",
            &namehash,
            &format!("ens:{namehash}"),
            &format!("{:#x}", B256::ZERO),
        )?,
        super::super::state::V1SurfaceMaterialization::AlreadyMaterialized
    );
    Ok(())
}

#[test]
fn ownerless_renewal_refreshes_already_current_registrar_state() -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let (initial, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![renewal_with_expiry(1, 100)],
        ),
        None,
    )?;
    let (ownerless, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![current_transfer(node, ZERO_ADDRESS, 2)?],
        ),
        Some(session),
    )?;
    let (output, session) = interpret_test_batch_incremental(
        input(
            manifests,
            admissions,
            Vec::new(),
            vec![renewal_with_expiry(3, 9_999)],
        ),
        Some(session),
    )?;
    let current = session
        .v1_name("ens", &format!("{node:#x}"))
        .expect("current registrar");
    assert_eq!(current.expiry, Some(9_999));
    assert_eq!(current.authority_source_family, "ens_v1_registrar_l1");
    assert!(output.normalized_events.iter().all(|event| {
        !(event.block_number == Some(3)
            && matches!(
                event.event_kind.as_str(),
                "SurfaceBound" | "AuthorityEpochChanged" | "PermissionChanged"
            ))
    }));
    let complete = initial
        .normalized_events
        .iter()
        .chain(&ownerless.normalized_events)
        .chain(&output.normalized_events)
        .cloned()
        .collect::<Vec<_>>();
    for prior in [
        complete.iter().map(prior_event).collect(),
        compact_prior(&complete),
    ] {
        let (_, restored) = interpret_test_batch_incremental(
            input(fixture().0, fixture().1, prior, Vec::new()),
            None,
        )?;
        assert_eq!(
            restored
                .v1_name("ens", &format!("{node:#x}"))
                .and_then(|current| current.expiry),
            Some(9_999)
        );
    }
    Ok(())
}
