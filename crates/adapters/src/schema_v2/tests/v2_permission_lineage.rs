//! A role change on an ENSv2 registry resource that no token was linked to still names the
//! resource's token lineage, and drafts it once. Adapter state restored from the batch's
//! normalized events must know that lineage was drafted, or the next role change on the same
//! resource drafts it again. Under the checked-in Sepolia registry manifest, a contract no
//! manifest declares admits itself with `RegistryCreated()`.

use super::*;

const REGISTRY_MANIFEST: i64 = 7601;
const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
/// A contract no manifest declares. It admits itself with `RegistryCreated()`.
const ANNOUNCED_REGISTRY: &str = "0x00000000000000000000000000000000000000a7";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";

fn sepolia_registry_manifest() -> ManifestInput {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .unwrap();
    let manifest = &repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v2_registry_l1")
        .unwrap()
        .manifest;
    ManifestInput {
        manifest_id: REGISTRY_MANIFEST,
        manifest_version: i64::try_from(manifest.manifest_version).unwrap(),
        namespace: manifest.namespace.clone(),
        source_family: "ens_v2_registry_l1".to_owned(),
        chain_id: CHAIN.to_owned(),
        deployment_label: manifest.deployment_epoch.clone(),
        normalizer_version: manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(manifest).unwrap(),
    }
}

fn rules() -> Vec<DiscoveryRuleInput> {
    vec![DiscoveryRuleInput {
        manifest_id: REGISTRY_MANIFEST,
        edge_kind: "registry_announcement".to_owned(),
        from_role: Some("registry".to_owned()),
        admission: "reachable_from_root".to_owned(),
    }]
}

fn declared_registry() -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: ETH_REGISTRY.to_owned(),
        contract_instance_id: crate::schema_v2::common::contract_id(CHAIN, ETH_REGISTRY),
        source_manifest_id: Some(REGISTRY_MANIFEST),
        role: Some("registry".to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

/// The admission an earlier batch's announcement left behind, read from that batch's rows.
fn announced_admission(output: &BatchOutput) -> AddressAdmissionInput {
    let address = output
        .contract_addresses
        .iter()
        .find(|address| address.address == ANNOUNCED_REGISTRY)
        .expect("the announcement admits its emitter");
    let edge = output
        .discovery_edges
        .iter()
        .find(|edge| edge.to_contract_instance_id == address.contract_instance_id)
        .expect("the announcement records its discovery edge");
    AddressAdmissionInput {
        address: ANNOUNCED_REGISTRY.to_owned(),
        contract_instance_id: address.contract_instance_id,
        source_manifest_id: Some(address.source_manifest_id),
        role: None,
        discovery_edge_kind: Some(edge.edge_kind.clone()),
        discovery_from_contract_instance_id: Some(edge.from_contract_instance_id),
        discovery_observation_key: Some(edge.observation_key.clone()),
        active_from_block: Some(address.active_from_block_number),
        active_to_block: None,
    }
}

fn batch(
    admissions: Vec<AddressAdmissionInput>,
    prior_events: Vec<PriorEventInput>,
    block: i64,
    raw_logs: Vec<RawLogInput>,
) -> BatchInput {
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![sepolia_registry_manifest()],
        discovery_rules: rules(),
        admissions,
        prior_events,
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: format!("block-{block}"),
            block_number: block,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(block),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs,
    }
}

fn role_change(block: i64, log: i64, old: u64, new: u64) -> RawLogInput {
    raw_at(
        EACRolesChanged {
            resource: versioned_token("tyr507", 1),
            account: OWNER.parse().unwrap(),
            oldRoleBitmap: U256::from(old),
            newRoleBitmap: U256::from(new),
        }
        .encode_log_data(),
        block,
        log,
        ANNOUNCED_REGISTRY,
    )
}

#[test]
fn self_announced_registry_role_change_lineage_is_known_after_a_restore() -> anyhow::Result<()> {
    let input = batch(
        vec![declared_registry()],
        Vec::new(),
        1,
        vec![
            raw_at(
                RegistryCreated {}.encode_log_data(),
                1,
                0,
                ANNOUNCED_REGISTRY,
            ),
            role_change(1, 1, 0, 1),
        ],
    );
    let (first, _) = interpret_test_batch_incremental(input.clone(), None)?;
    assert!(first.decode_skips.is_empty(), "{:?}", first.decode_skips);
    let change = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionChanged")
        .expect("the role change is interpreted");
    let resource_id = change.resource_id.expect("a resource");
    let lineage = first
        .resources
        .iter()
        .find(|resource| resource.resource_id == resource_id)
        .and_then(|resource| resource.token_lineage_id)
        .expect("a non-zero resource has a token lineage");
    assert_eq!(
        first
            .token_lineages
            .iter()
            .map(|row| row.token_lineage_id)
            .collect::<Vec<_>>(),
        vec![lineage],
        "the role change drafts its resource's lineage once"
    );
    assert_eq!(
        change.after_state.get(seam::TOKEN_LINEAGE_ID_KEY),
        Some(&json!(lineage.to_string())),
        "a restore rebuilds drafted lineages from after_states, so the role change must carry \
         the lineage it drafted: {}",
        change.after_state
    );

    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &input.blocks)?;
    let admissions = vec![declared_registry(), announced_admission(&first)];
    let catalog = crate::schema_v2::catalog::Catalog::new(
        vec![sepolia_registry_manifest()],
        rules(),
        admissions.clone(),
    )?;
    let mut restored = crate::schema_v2::state::State::with_cache_capacity_and_manifest_ids(
        prior.clone(),
        catalog.v2_suffix_anchors(),
        StateCacheCapacity::Unlimited,
        Some(catalog.provenance_ids()),
    );
    restored.ensure_restore_succeeded()?;
    assert!(
        !restored.materialize_token_lineage(lineage),
        "the restored state must know the lineage was already drafted"
    );

    let (second, _) = interpret_test_batch_incremental(
        batch(admissions, prior, 2, vec![role_change(2, 0, 1, 3)]),
        None,
    )?;
    assert!(second.decode_skips.is_empty(), "{:?}", second.decode_skips);
    assert!(
        second.token_lineages.is_empty(),
        "a role change after a restore drafted its lineage again: {:?}",
        second.token_lineages
    );
    Ok(())
}
