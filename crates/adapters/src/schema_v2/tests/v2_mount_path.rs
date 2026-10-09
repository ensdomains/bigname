//! Lookahead loading for a registry named by its mount path, the parent token that points at
//! it, while its parent claim names a label the parent holds no token for.
use super::{super::v2_registry, *};

const MOUNTED: &str = "0x00000000000000000000000000000000000000a7";

struct Mounted {
    input: BatchInput,
    prior: Vec<PriorEventInput>,
    mount_key: String,
    unrelated_key: String,
}

/// `CONTRACT` is the anchored `eth` registry. It points `m` at `MOUNTED` and holds an
/// unrelated token. `MOUNTED` claims `unattached.eth` and holds `child`. The batch registers
/// `second` in `MOUNTED`.
fn mounted() -> anyhow::Result<Mounted> {
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let manifest = manifest_with_events(
        97,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut mounted_admission = admission(97, "registry");
    mounted_admission.address = MOUNTED.to_owned();
    mounted_admission.contract_instance_id =
        super::super::super::common::contract_id(CHAIN, MOUNTED);
    mounted_admission.discovery_edge_kind = Some("subregistry".to_owned());
    mounted_admission.discovery_from_contract_instance_id = Some(Uuid::from_u128(97));
    mounted_admission.discovery_observation_key = Some("subregistry:mounted".to_owned());
    let batch = |logs: Vec<RawLogInput>| {
        let mut batch = input(
            vec![manifest.clone()],
            vec![admission(97, "registry"), mounted_admission.clone()],
            logs,
        );
        batch.discovery_rules = vec![DiscoveryRuleInput {
            manifest_id: 97,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }];
        batch
    };
    let register = |label: &str, block, log, registry: &str| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: versioned_token(label, 1),
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry: 1_000,
                sender,
            }
            .encode_log_data(),
            block,
            log,
            registry,
        )
    };
    let link = |label: &str, resource: u64, block, log| {
        raw_at(
            v2_registry::TokenResource {
                tokenId: versioned_token(label, 1),
                resource: U256::from(resource),
            }
            .encode_log_data(),
            block,
            log,
            MOUNTED,
        )
    };
    let setup = batch(vec![
        register("m", 1, 0, CONTRACT),
        raw_at(
            v2_registry::SubregistryUpdated {
                tokenId: versioned_token("m", 1),
                subregistry: MOUNTED.parse()?,
                sender,
            }
            .encode_log_data(),
            1,
            1,
            CONTRACT,
        ),
        register("unrelated", 1, 2, CONTRACT),
        raw_at(
            v2_registry::ParentUpdated {
                parent: CONTRACT.parse()?,
                label: "unattached".to_owned(),
                sender,
            }
            .encode_log_data(),
            2,
            0,
            MOUNTED,
        ),
        register("child", 2, 1, MOUNTED),
        link("child", 71, 2, 2),
    ]);
    let output = interpret_test_batch(setup.clone())?;
    let prior = seam::fold_prior_events(Vec::new(), &output.normalized_events, &setup.blocks)?;
    let key = |label: &str| v2_key(CONTRACT, &format!("{:#066x}", versioned_token(label, 1)));
    Ok(Mounted {
        input: batch(vec![
            register("second", 3, 0, MOUNTED),
            link("second", 72, 3, 1),
        ]),
        prior,
        mount_key: key("m"),
        unrelated_key: key("unrelated"),
    })
}

#[test]
fn a_registry_event_loads_the_parent_token_that_mounts_its_registry() -> anyhow::Result<()> {
    let Mounted {
        mut input,
        prior,
        mount_key,
        unrelated_key,
    } = mounted()?;
    input.prior_events = prior;
    let (_, loaded) = scoped_match(input.clone())?;
    assert!(loaded.v2_keys.contains(&mount_key), "{:?}", loaded.v2_keys);
    assert!(!loaded.v2_keys.contains(&unrelated_key));
    assert!(!loaded.v2_keys.contains(&v2_registry_key(CONTRACT)));

    let output = interpret_schema_v2_batch(input)?;
    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.raw_name() == Some("second.m.eth")),
        "the registration is named by the mount path"
    );
    Ok(())
}

/// Loading the registry's own key loads the pointer event filed under it, and that event asks
/// for the parent token it sits on. The restore then reads no key outside the loaded set.
#[test]
fn a_mount_pointer_brings_its_parent_token_without_a_retry() -> anyhow::Result<()> {
    let Mounted {
        input,
        prior,
        mount_key,
        unrelated_key,
    } = mounted()?;
    let mut dependencies = collect_v1_batch_dependencies(&input, &input.manifests)?;
    dependencies.v2_due_window = Some((i64::MIN, i64::MAX));
    assert!(!dependencies.v2_keys.contains(&mount_key));
    let (scoped, rows) = scope(dependencies, &prior)?;
    assert!(scoped.v2_keys.contains(&mount_key), "{:?}", scoped.v2_keys);
    assert!(!scoped.v2_keys.contains(&unrelated_key));
    restore_schema_v2_lookahead_session(
        begin_schema_v2_adapter_restore(
            input.chain_id.clone(),
            input.manifests.clone(),
            input.discovery_rules.clone(),
            input.admissions.clone(),
            StateCacheCapacity::Unlimited,
        )?,
        rows,
        None,
        None,
        &scoped,
    )?;
    Ok(())
}

/// A path with a label that is not valid UTF-8 is kept as raw labels. A boundary move between
/// two such paths splits like any other: the old path ends as an expiry and the new path is
/// drafted as a shadow name.
#[test]
fn a_boundary_move_between_raw_label_paths_splits_into_a_release_and_a_shadow_grant() {
    use crate::schema_v2::{
        protocol,
        state::{V2NameTransition, V2RawNameState},
    };
    let raw_path = |mount: &[u8]| {
        let raw_labels = vec![vec![0xff], mount.to_vec(), b"eth".to_vec()];
        let namehash =
            super::super::super::common::namehash_raw(raw_labels.iter().map(Vec::as_slice));
        V2RawNameState {
            logical_name_id: format!("ens:{namehash}"),
            raw_labels,
            namehash,
        }
    };
    let transition = V2NameTransition {
        registry: MOUNTED.to_owned(),
        registry_contract_instance_id: None,
        token_id: "0x01".to_owned(),
        expiry: Some(1_000),
        previous: None,
        previous_shadow: Some(raw_path(b"a")),
        current: None,
        current_shadow: Some(raw_path(b"b")),
        resource_id: None,
        token_lineage_id: None,
        upstream_resource: None,
        registration: None,
        resolver: None,
        subregistry: None,
    };
    let moved =
        protocol::v2_registry::boundary::split_boundary_move(transition.clone(), &block(10));
    let released = moved.released.expect("the token was named before");
    assert_eq!(released.previous_shadow, transition.previous_shadow);
    assert_eq!(released.current_shadow, None);
    let released = protocol::v2_boundary_expiration(released, 10).expect("an expiry release");
    assert_eq!(released.events.len(), 1);
    assert_eq!(released.events[0].event_kind, "RegistrationReleased");
    let (granted, _) = moved.granted.expect("the token is named after");
    assert_eq!(granted.shadow_names.len(), 1);
    assert_eq!(
        Some(&granted.shadow_names[0].namehash),
        transition
            .current_shadow
            .as_ref()
            .map(|name| &name.namehash)
    );
    assert!(granted.names.is_empty());

    // The expiry writer takes only the released half. A transition that still names the
    // token is refused, so a caller that skips the split cannot drop the grant.
    let Err(refused) = protocol::v2_boundary_expiration(transition.clone(), 10) else {
        panic!("a transition with a current name is not an expiration");
    };
    assert_eq!(
        refused.to_string(),
        "block-boundary ENSv2 transition is not an expiration"
    );

    // A transition that leaves the token unnamed is released whole and grants nothing.
    let expired = V2NameTransition {
        current_shadow: None,
        ..transition
    };
    let moved = protocol::v2_registry::boundary::split_boundary_move(expired.clone(), &block(10));
    assert_eq!(moved.released, Some(expired));
    assert!(moved.granted.is_none());
}
