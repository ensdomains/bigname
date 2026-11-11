//! An ENSv2 registry that reserves a label drafts the reserved token's resource and token
//! lineage. Registering the reserved label keeps that token id, so the token keeps the resource
//! and lineage. A contract no manifest declares can admit itself with `RegistryCreated()`,
//! reserve a label, then register it without `TokenResource`. Adapter state restored from the
//! batch's normalized events must hold the same lineage, or the token's next event drafts its
//! resource with none.
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L455-L472 @ ens_v2@a971bd64)

use super::*;

const REGISTRY_MANIFEST: i64 = 7602;
const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
/// A contract no manifest declares. It admits itself with `RegistryCreated()`.
const ANNOUNCED_REGISTRY: &str = "0x00000000000000000000000000000000000000a9";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const HOLDER: &str = "0x00000000000000000000000000000000000000d1";
const LABEL: &str = "reserved";
/// Beyond every block timestamp here, which is the block number in seconds.
const LIVE_EXPIRY: u64 = 1_000;

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

fn token_word() -> String {
    format!("0x{:064x}", versioned_token(LABEL, 0))
}

/// The registry announces itself, reserves the label, then registers it to the owner and mints
/// the token, with no `TokenResource`.
fn reserve_then_register(block: i64) -> Vec<RawLogInput> {
    let token = versioned_token(LABEL, 0);
    let label_hash = keccak256(LABEL.as_bytes());
    let owner: Address = OWNER.parse().unwrap();
    let logs = [
        RegistryCreated {}.encode_log_data(),
        v2_registry::LabelReserved {
            tokenId: token,
            labelHash: label_hash,
            label: LABEL.to_owned(),
            expiry: LIVE_EXPIRY,
            sender: owner,
        }
        .encode_log_data(),
        v2_registry::LabelRegistered {
            tokenId: token,
            labelHash: label_hash,
            label: LABEL.to_owned(),
            owner,
            expiry: LIVE_EXPIRY,
            sender: owner,
        }
        .encode_log_data(),
        v2_registry::TransferSingle {
            operator: owner,
            from: Address::ZERO,
            to: owner,
            id: token,
            value: U256::from(1),
        }
        .encode_log_data(),
    ];
    logs.into_iter()
        .enumerate()
        .map(|(index, log)| raw_at(log, block, index as i64, ANNOUNCED_REGISTRY))
        .collect()
}

fn transfer(block: i64) -> RawLogInput {
    let owner: Address = OWNER.parse().unwrap();
    raw_at(
        v2_registry::TransferSingle {
            operator: owner,
            from: owner,
            to: HOLDER.parse().unwrap(),
            id: versioned_token(LABEL, 0),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        0,
        ANNOUNCED_REGISTRY,
    )
}

fn event<'a>(output: &'a BatchOutput, event_kind: &str) -> &'a NormalizedEvent {
    output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == event_kind)
        .unwrap_or_else(|| panic!("the batch has no {event_kind} row"))
}

/// The resource and token lineage the first batch's reservation drafted.
fn reservation_identity(output: &BatchOutput) -> anyhow::Result<(Uuid, Uuid)> {
    let reservation = event(output, "RegistrationReserved");
    let resource_id = reservation
        .resource_id
        .expect("a reservation names its resource");
    let lineage = reservation
        .after_state
        .get(seam::TOKEN_LINEAGE_ID_KEY)
        .and_then(serde_json::Value::as_str)
        .expect("a reservation names its token lineage");
    Ok((resource_id, Uuid::parse_str(lineage)?))
}

/// Adapter state restored in a fresh session from `prior`, as Interpret restores it at a batch
/// boundary or a process start.
fn restored(
    output: &BatchOutput,
    prior: Vec<PriorEventInput>,
) -> anyhow::Result<crate::schema_v2::state::State> {
    let catalog = crate::schema_v2::catalog::Catalog::new(
        vec![sepolia_registry_manifest()],
        rules(),
        vec![declared_registry(), announced_admission(output)],
    )?;
    let restored = crate::schema_v2::state::State::with_cache_capacity_and_manifest_ids(
        prior,
        catalog.v2_suffix_anchors(),
        StateCacheCapacity::Unlimited,
        Some(catalog.provenance_ids()),
    );
    restored.ensure_restore_succeeded()?;
    Ok(restored)
}

#[test]
fn reserved_then_registered_token_keeps_its_lineage_after_a_restore() -> anyhow::Result<()> {
    let input = batch(
        vec![declared_registry()],
        Vec::new(),
        1,
        reserve_then_register(1),
    );
    let (first, _) = interpret_test_batch_incremental(input.clone(), None)?;
    assert!(first.decode_skips.is_empty(), "{:?}", first.decode_skips);
    let (resource_id, lineage) = reservation_identity(&first)?;
    let registration = event(&first, "RegistrationGranted");
    assert_eq!(registration.resource_id, Some(resource_id));
    assert_eq!(
        registration.after_state.get(seam::TOKEN_LINEAGE_ID_KEY),
        Some(&json!(lineage.to_string())),
        "a restore reattaches the resource a registration names with the lineage it names \
         beside it, so the registration must carry the reservation's lineage: {}",
        registration.after_state
    );

    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &input.blocks)?;
    let token = restored(&first, prior.clone())?
        .v2_token(ANNOUNCED_REGISTRY, &token_word())
        .expect("the restore installs the registered token");
    assert_eq!(token.resource_id, Some(resource_id));
    assert_eq!(
        token.token_lineage_id,
        Some(lineage),
        "the restored token holds the reservation's lineage"
    );

    let admissions = vec![declared_registry(), announced_admission(&first)];
    let (second, _) =
        interpret_test_batch_incremental(batch(admissions, prior, 2, vec![transfer(2)]), None)?;
    assert!(second.decode_skips.is_empty(), "{:?}", second.decode_skips);
    let drafted = second
        .resources
        .iter()
        .find(|resource| resource.resource_id == resource_id)
        .expect("the transfer drafts the token's resource");
    assert_eq!(
        drafted.token_lineage_id,
        Some(lineage),
        "a transfer after the restore drafts the resource with the reservation's lineage"
    );
    Ok(())
}

/// Another registry resource, for a registration row that names a resource the token does not
/// hold.
const OTHER_RESOURCE: &str = "0x000000000000000000000000000000000000000000000000000000000000000b";

/// The first batch's rows folded as a restore reads them, with the registration row rewritten by
/// `edit`.
fn prior_with_registration(
    edit: impl FnOnce(&mut PriorEventInput),
) -> anyhow::Result<(BatchOutput, Vec<PriorEventInput>)> {
    let input = batch(
        vec![declared_registry()],
        Vec::new(),
        1,
        reserve_then_register(1),
    );
    let (first, _) = interpret_test_batch_incremental(input.clone(), None)?;
    assert!(first.decode_skips.is_empty(), "{:?}", first.decode_skips);
    let mut prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &input.blocks)?;
    edit(
        prior
            .iter_mut()
            .find(|event| event.event_kind == "RegistrationGranted")
            .expect("the registration row is retained"),
    );
    Ok((first, prior))
}

/// The restored token's resource, upstream resource word and lineage.
fn restored_resource(
    output: &BatchOutput,
    prior: Vec<PriorEventInput>,
) -> anyhow::Result<(Option<Uuid>, Option<String>, Option<Uuid>)> {
    let token = restored(output, prior)?
        .v2_token(ANNOUNCED_REGISTRY, &token_word())
        .expect("the restore installs the registered token");
    Ok((
        token.resource_id,
        token.upstream_resource,
        token.token_lineage_id,
    ))
}

/// A registration row written before registrations carried the lineage names the reservation's
/// resource and no lineage. Restored after the reservation row, it keeps the lineage the
/// reservation row restored.
#[test]
fn restore_keeps_the_reserved_lineage_when_an_older_registration_row_names_none()
-> anyhow::Result<()> {
    let (first, prior) = prior_with_registration(|registration| {
        registration
            .after_state
            .as_object_mut()
            .expect("an after_state is an object")
            .remove(seam::TOKEN_LINEAGE_ID_KEY);
    })?;
    let (resource_id, lineage) = reservation_identity(&first)?;
    let (resource, _, restored_lineage) = restored_resource(&first, prior)?;
    assert_eq!(resource, Some(resource_id));
    assert_eq!(
        restored_lineage,
        Some(lineage),
        "a registration row with no lineage must not clear the reservation's lineage"
    );
    Ok(())
}

/// A null lineage reads as no lineage, as every other reader of the key reads it.
#[test]
fn restore_keeps_the_reserved_lineage_when_a_registration_row_names_a_null_lineage()
-> anyhow::Result<()> {
    let (first, prior) = prior_with_registration(|registration| {
        registration.after_state[seam::TOKEN_LINEAGE_ID_KEY] = serde_json::Value::Null;
    })?;
    let (resource_id, lineage) = reservation_identity(&first)?;
    let (resource, _, restored_lineage) = restored_resource(&first, prior)?;
    assert_eq!(resource, Some(resource_id));
    assert_eq!(
        restored_lineage,
        Some(lineage),
        "a null lineage must not clear the reservation's lineage"
    );
    Ok(())
}

/// A registration row with no lineage that names a resource the token does not hold attaches
/// that resource with no lineage. The token's earlier lineage belongs to another resource.
#[test]
fn restore_takes_no_lineage_from_a_registration_row_naming_another_resource() -> anyhow::Result<()>
{
    let other = Uuid::from_u128(0x0b);
    let (first, prior) = prior_with_registration(|registration| {
        registration.resource_id = Some(other);
        let after_state = registration
            .after_state
            .as_object_mut()
            .expect("an after_state is an object");
        after_state.remove(seam::TOKEN_LINEAGE_ID_KEY);
        after_state.insert("upstream_resource".to_owned(), json!(OTHER_RESOURCE));
        after_state.insert("resource_id".to_owned(), json!(other.to_string()));
    })?;
    assert_eq!(
        restored_resource(&first, prior)?,
        (Some(other), Some(OTHER_RESOURCE.to_owned()), None),
        "the token takes the named resource and no lineage"
    );
    Ok(())
}

/// `TokenResource` for a token that holds another resource, after the registration and its link.
fn link_to_another_resource(emitter: &str, announce: bool) -> Vec<RawLogInput> {
    let token = versioned_token(LABEL, 0);
    let owner: Address = OWNER.parse().unwrap();
    let mut logs = Vec::new();
    if announce {
        logs.push(RegistryCreated {}.encode_log_data());
    }
    logs.extend([
        v2_registry::LabelRegistered {
            tokenId: token,
            labelHash: keccak256(LABEL.as_bytes()),
            label: LABEL.to_owned(),
            owner,
            expiry: LIVE_EXPIRY,
            sender: owner,
        }
        .encode_log_data(),
        v2_registry::TokenResource {
            tokenId: token,
            resource: token,
        }
        .encode_log_data(),
        v2_registry::TokenResource {
            tokenId: token,
            resource: OTHER_RESOURCE.parse().unwrap(),
        }
        .encode_log_data(),
    ]);
    logs.into_iter()
        .enumerate()
        .map(|(index, log)| raw_at(log, 1, index as i64, emitter))
        .collect()
}

/// A token that holds a resource cannot be linked to another one. On a declared registry the
/// batch fails.
#[test]
fn a_link_to_another_resource_is_still_refused() {
    let input = batch(
        vec![declared_registry()],
        Vec::new(),
        1,
        link_to_another_resource(ETH_REGISTRY, false),
    );
    let error = interpret_test_batch_incremental(input, None)
        .expect_err("a declared registry's conflicting link fails the batch");
    assert!(
        format!("{error:#}").contains("does not confirm its retained resource"),
        "{error:#}"
    );
}

/// On a registry no manifest declares, the conflicting link is skipped and recorded.
#[test]
fn a_link_to_another_resource_is_skipped_on_an_announced_registry() -> anyhow::Result<()> {
    let input = batch(
        vec![declared_registry()],
        Vec::new(),
        1,
        link_to_another_resource(ANNOUNCED_REGISTRY, true),
    );
    let (output, _) = interpret_test_batch_incremental(input, None)?;
    assert_eq!(
        output
            .decode_skips
            .iter()
            .map(|skip| skip.inconsistency_site.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("token_resource_conflict")]
    );
    Ok(())
}
