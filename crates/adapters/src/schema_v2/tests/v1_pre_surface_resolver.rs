use super::*;

const REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
const OLD_REGISTRY: &str = "0x00000000000000000000000000000000000000a0";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
const OWNER: &str = "0x00000000000000000000000000000000000000a3";
const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a4";
const REGISTRY_MANIFEST_ID: i64 = 6131;
const REGISTRAR_MANIFEST_ID: i64 = 6132;

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
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry"],
                &["AuthorityTransferred", "PermissionChanged"],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry", "registry_old"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    );
    let registrar_manifest = manifest(
        REGISTRAR_MANIFEST_ID,
        "ens_v1_registrar_l1",
        "NameRenewed",
        "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted", "PreimageObserved"],
    );
    let mut registry = admission(REGISTRY_MANIFEST_ID, "registry");
    registry.address = REGISTRY.to_owned();
    let mut old_registry = admission(REGISTRY_MANIFEST_ID, "registry_old");
    old_registry.address = OLD_REGISTRY.to_owned();
    old_registry.contract_instance_id = Uuid::from_u128(6_130);
    let mut registrar = admission(REGISTRAR_MANIFEST_ID, "registrar");
    registrar.address = REGISTRAR.to_owned();
    let node = super::common::namehash(&["pointer".to_owned(), "eth".to_owned()])
        .parse()
        .expect("fixture node");
    (
        vec![registry_manifest, registrar_manifest],
        vec![registry, old_registry, registrar],
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
    raw_at(
        NameRenewed {
            name: "pointer".to_owned(),
            label: keccak256(b"pointer"),
            expires: U256::from(9_999_u64),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    )
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

fn state_derived_pointer(output: &BatchOutput) -> Option<&NormalizedEvent> {
    output.normalized_events.iter().find(|event| {
        event.event_kind == "ResolverChanged"
            && event.after_state["state_derived"] == true
            && event.after_state["surface_materialization"] == true
    })
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
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "SurfaceBound" && event.block_number == Some(3))
            .count(),
        1
    );
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
