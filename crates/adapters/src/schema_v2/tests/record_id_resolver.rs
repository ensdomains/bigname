use super::*;

sol! {
    event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name);
    event AddressUpdated(uint256 indexed recordId, uint256 coinType, bytes addressBytes);
    event ContenthashUpdated(uint256 indexed recordId, bytes hash);
    event ABIUpdated(uint256 indexed recordId, uint256 indexed contentType);
    event InterfaceUpdated(uint256 indexed recordId, bytes4 indexed interfaceId, address implementer);
    event TextUpdated(uint256 indexed recordId, string indexed keyHash, string key, string value);
    event DataUpdated(uint256 indexed recordId, string indexed keyHash, string key, bytes value);
    event NameUpdated(uint256 indexed recordId, string primaryName);
    event ResourceArgument(uint256 indexed resource, bytes arg);
    event RawTextUpdated(uint256 indexed recordId, bytes32 indexed keyHash, bytes key, bytes value);
}

fn source() -> ManifestInput {
    manifest_with_events(
        921,
        "ens",
        "ens_v2_resolver_l1",
        &[
            (
                "Linked",
                "event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name)",
                &["resolver"],
                &["ResolverRecordLinked", "PreimageObserved"],
            ),
            (
                "AddressUpdated",
                "event AddressUpdated(uint256 indexed recordId, uint256 coinType, bytes addressBytes)",
                &["resolver"],
                &["RecordChanged"],
            ),
            (
                "ContenthashUpdated",
                "event ContenthashUpdated(uint256 indexed recordId, bytes hash)",
                &["resolver"],
                &["RecordChanged"],
            ),
            (
                "ABIUpdated",
                "event ABIUpdated(uint256 indexed recordId, uint256 indexed contentType)",
                &["resolver"],
                &["RecordChanged"],
            ),
            (
                "InterfaceUpdated",
                "event InterfaceUpdated(uint256 indexed recordId, bytes4 indexed interfaceId, address implementer)",
                &["resolver"],
                &["RecordChanged"],
            ),
            (
                "TextUpdated",
                "event TextUpdated(uint256 indexed recordId, string indexed keyHash, string key, string value)",
                &["resolver"],
                &["RecordChanged"],
            ),
            (
                "DataUpdated",
                "event DataUpdated(uint256 indexed recordId, string indexed keyHash, string key, bytes value)",
                &["resolver"],
                &["RecordChanged"],
            ),
            (
                "NameUpdated",
                "event NameUpdated(uint256 indexed recordId, string primaryName)",
                &["resolver"],
                &["RecordChanged"],
            ),
            (
                "ResourceArgument",
                "event ResourceArgument(uint256 indexed resource, bytes arg)",
                &["resolver"],
                &["ResolverPermissionArgument"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &["resolver"],
                &["PermissionChanged"],
            ),
        ],
    )
}

fn batch(
    logs: Vec<alloy_primitives::LogData>,
    prior_events: Vec<PriorEventInput>,
    block: i64,
) -> anyhow::Result<BatchOutput> {
    interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![source()],
        discovery_rules: vec![],
        admissions: vec![admission(921, "resolver")],
        prior_events,
        blocks: vec![],
        raw_logs: logs
            .into_iter()
            .enumerate()
            .map(|(i, log)| raw_at(log, block, i as i64, CONTRACT))
            .collect(),
    })
}

fn link(name: &[u8], record: u64) -> alloy_primitives::LogData {
    let labels = if name == b"\0" {
        Vec::new()
    } else {
        common::decode_dns_labels(name).unwrap()
    };
    Linked {
        recordId: U256::from(record),
        node: common::namehash_raw(labels.iter().map(Vec::as_slice))
            .parse()
            .unwrap(),
        name: name.to_vec().into(),
    }
    .encode_log_data()
}

#[test]
fn record_id_resolver_links_shared_records_root_and_unlinks_without_fabricated_versions()
-> anyhow::Result<()> {
    let output = batch(
        vec![
            link(b"\x05alice\x03eth\0", 7),
            link(b"\x03bob\x03eth\0", 7),
            link(b"\0", 9),
            TextUpdated {
                recordId: U256::from(7),
                keyHash: keccak256("url"),
                key: "url".into(),
                value: "shared".into(),
            }
            .encode_log_data(),
            link(b"\x05alice\x03eth\0", 0),
        ],
        vec![],
        1,
    )?;
    let links = output
        .normalized_events
        .iter()
        .filter(|e| e.event_kind == "ResolverRecordLinked")
        .collect::<Vec<_>>();
    assert_eq!(links.len(), 4);
    assert_eq!(links[0].after_state["resolver_record_id"], "7");
    assert_eq!(links[1].after_state["resolver_record_id"], "7");
    assert_ne!(links[0].after_state["node"], links[1].after_state["node"]);
    assert!(links.iter().all(|e| e.logical_name_id.is_none()));
    assert_eq!(links[2].after_state["node"], format!("{:#x}", B256::ZERO));
    assert_eq!(links[3].after_state["resolver_record_id"], "0");
    let record = output
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "RecordChanged")
        .unwrap();
    assert!(record.logical_name_id.is_none() && record.resource_id.is_none());
    assert_eq!(record.after_state["storage_model"], "resolver_record_id");
    assert_eq!(record.after_state["value"], "shared");
    assert!(
        !output
            .normalized_events
            .iter()
            .any(|e| e.event_kind == "RecordVersionChanged")
    );
    Ok(())
}

#[test]
fn record_id_resolver_retains_empty_updates_and_does_not_mix_record_before_states()
-> anyhow::Result<()> {
    let id = U256::from(7);
    let output = batch(
        vec![
            TextUpdated {
                recordId: id,
                keyHash: keccak256("url"),
                key: "url".into(),
                value: "old".into(),
            }
            .encode_log_data(),
            TextUpdated {
                recordId: U256::from(8),
                keyHash: keccak256("url"),
                key: "url".into(),
                value: "".into(),
            }
            .encode_log_data(),
            AddressUpdated {
                recordId: id,
                coinType: U256::from(60),
                addressBytes: vec![].into(),
            }
            .encode_log_data(),
            ContenthashUpdated {
                recordId: id,
                hash: vec![].into(),
            }
            .encode_log_data(),
            DataUpdated {
                recordId: id,
                keyHash: keccak256(""),
                key: "".into(),
                value: vec![].into(),
            }
            .encode_log_data(),
            NameUpdated {
                recordId: id,
                primaryName: "".into(),
            }
            .encode_log_data(),
            ABIUpdated {
                recordId: id,
                contentType: U256::from(1),
            }
            .encode_log_data(),
            InterfaceUpdated {
                recordId: id,
                interfaceId: [0_u8; 4].into(),
                implementer: Address::ZERO,
            }
            .encode_log_data(),
        ],
        vec![],
        1,
    )?;
    let events = &output.normalized_events;
    assert_eq!(events.len(), 8);
    assert_eq!(events[1].after_state["value"], "");
    assert_ne!(events[1].before_state.get("value"), Some(&json!("old")));
    assert_eq!(events[2].after_state["address_bytes_hex"], "0x");
    assert_eq!(events[3].after_state["contenthash_hex"], "0x");
    assert_eq!(events[4].after_state["value"], "0x");
    assert_eq!(events[5].after_state["raw_name"], "");
    assert_eq!(events[6].after_state["value_retained"], false);
    assert_eq!(events[7].after_state["implementer"], ZERO_ADDRESS);
    assert!(
        events
            .iter()
            .all(|e| e.logical_name_id.is_none() && e.resource_id.is_none())
    );
    Ok(())
}

#[test]
fn record_id_resolver_raw_strings_and_key_hash_validation() -> anyhow::Result<()> {
    let raw_key = vec![0xff];
    let raw_value = vec![0xfe];
    let output = batch(
        vec![
            with_topic0(
                RawTextUpdated {
                    recordId: U256::from(1),
                    keyHash: keccak256(&raw_key),
                    key: raw_key.clone().into(),
                    value: raw_value.into(),
                }
                .encode_log_data(),
                TextUpdated::SIGNATURE_HASH,
            ),
            with_topic0(
                RawTextUpdated {
                    recordId: U256::from(1),
                    keyHash: B256::ZERO,
                    key: raw_key.into(),
                    value: vec![].into(),
                }
                .encode_log_data(),
                TextUpdated::SIGNATURE_HASH,
            ),
        ],
        vec![],
        1,
    )?;
    assert_eq!(output.normalized_events.len(), 1);
    assert_eq!(
        output.normalized_events[0].after_state["record_family"],
        "text_opaque"
    );
    assert_eq!(
        output.normalized_events[0].after_state["value"]["bytes"],
        "0xfe"
    );
    Ok(())
}

#[test]
fn record_id_resolver_restores_arguments_without_assigning_name_identity() -> anyhow::Result<()> {
    let argument = U256::from(60).to_be_bytes::<32>();
    let resource = U256::from_be_bytes(*keccak256(argument));
    let roles = (U256::from(1) << 0) | (U256::from(1) << 12) | (U256::from(1) << 140);
    let grant = EACRolesChanged {
        resource,
        account: Address::ZERO,
        oldRoleBitmap: U256::ZERO,
        newRoleBitmap: roles,
    }
    .encode_log_data();
    let first = batch(
        vec![
            ResourceArgument {
                resource,
                arg: argument.to_vec().into(),
            }
            .encode_log_data(),
        ],
        vec![],
        1,
    )?;
    let restored = batch(
        vec![grant.clone()],
        first.normalized_events.iter().map(prior_event).collect(),
        2,
    )?;
    let live = batch(
        vec![
            ResourceArgument {
                resource,
                arg: argument.to_vec().into(),
            }
            .encode_log_data(),
            grant,
        ],
        vec![],
        1,
    )?;
    let permission = restored
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "PermissionChanged")
        .unwrap();
    let live_permission = live
        .normalized_events
        .iter()
        .find(|e| e.event_kind == "PermissionChanged")
        .unwrap();
    assert_eq!(permission.after_state, live_permission.after_state);
    assert!(permission.logical_name_id.is_none());
    assert_eq!(
        permission.resource_id,
        Some(common::ens_v2_resolver_resource_id(
            CHAIN,
            Uuid::from_u128(921),
            &format!("{resource:#066x}")
        ))
    );
    assert_eq!(
        permission.after_state["effective_powers"],
        json!(["set_addr", "set_abi", "admin_set_abi"])
    );
    assert_eq!(
        permission.after_state["selector"]["selectors"][0]["key"],
        "60"
    );
    assert_eq!(
        permission.after_state["selector"]["selectors"][1]["kind"],
        "abi"
    );
    let invalid = batch(
        vec![
            ResourceArgument {
                resource: U256::from(1),
                arg: argument.to_vec().into(),
            }
            .encode_log_data(),
        ],
        vec![],
        1,
    )?;
    assert!(invalid.normalized_events.is_empty());
    Ok(())
}

#[test]
fn record_id_resolver_root_roles_use_the_selected_generation() -> anyhow::Result<()> {
    let output = batch(
        vec![
            EACRolesChanged {
                resource: U256::ZERO,
                account: Address::ZERO,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: (U256::from(1) << 20)
                    | (U256::from(1) << 24)
                    | (U256::from(1) << 28)
                    | (U256::from(1) << 156),
            }
            .encode_log_data(),
        ],
        vec![],
        1,
    )?;
    let event = &output.normalized_events[0];
    assert_eq!(
        event.after_state["effective_powers"],
        json!(["set_name", "set_data", "link", "admin_link"])
    );
    assert_eq!(event.after_state["root_resource"], true);
    assert!(event.logical_name_id.is_none());
    Ok(())
}

#[test]
fn record_id_resolver_argument_selectors_are_role_specific_and_revocations_keep_them()
-> anyhow::Result<()> {
    for (arg, bit, kind, key) in [
        (b"avatar".to_vec(), 4, "text", "avatar"),
        (b"avatar".to_vec(), 24, "data", "avatar"),
        (vec![0x12, 0x34, 0x56, 0x78], 16, "interface", "0x12345678"),
    ] {
        let resource = U256::from_be_bytes(*keccak256(&arg));
        let bitmap = U256::from(1) << bit;
        let output = batch(
            vec![
                ResourceArgument {
                    resource,
                    arg: arg.into(),
                }
                .encode_log_data(),
                EACRolesChanged {
                    resource,
                    account: Address::ZERO,
                    oldRoleBitmap: bitmap,
                    newRoleBitmap: U256::ZERO,
                }
                .encode_log_data(),
            ],
            vec![],
            1,
        )?;
        let e = output
            .normalized_events
            .iter()
            .find(|e| e.event_kind == "PermissionChanged")
            .unwrap();
        assert_eq!(e.after_state["selector"]["kind"], kind);
        assert_eq!(e.after_state["selector"]["key"], key);
        assert_eq!(e.after_state["effective_powers"], json!([]));
        assert!(e.after_state["revocation_source"].is_object());
        assert!(e.logical_name_id.is_none());
    }
    Ok(())
}

#[test]
fn record_id_resolver_link_facts_do_not_require_a_name_surface() -> anyhow::Result<()> {
    let output = batch(
        vec![
            Linked {
                recordId: U256::from(1),
                node: B256::repeat_byte(1),
                name: vec![255].into(),
            }
            .encode_log_data(),
            Linked {
                recordId: U256::from(2),
                node: B256::repeat_byte(2),
                name: b"\x05alice\x03eth\0".to_vec().into(),
            }
            .encode_log_data(),
        ],
        vec![],
        1,
    )?;
    assert_eq!(output.normalized_events.len(), 2);
    assert!(output.name_surfaces.is_empty());
    assert!(
        output
            .normalized_events
            .iter()
            .all(|e| e.logical_name_id.is_none() && e.resource_id.is_none())
    );
    assert_ne!(
        output.normalized_events[0].raw_fact_ref["state_scope"],
        output.normalized_events[1].raw_fact_ref["state_scope"]
    );
    Ok(())
}

#[test]
fn record_id_resolver_official_manifest_routes_both_resolver_generations() -> anyhow::Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let repository = bigname_manifests::load_repository(root.join("manifests/sepolia"))?;
    assert_eq!(
        repository.summary().status,
        bigname_manifests::ManifestLoadStatus::Loaded
    );
    let manifest = &repository
        .manifests()
        .iter()
        .find(|m| m.manifest.source_family == "ens_v2_resolver_l1")
        .unwrap()
        .manifest;
    let public = manifest
        .contracts
        .iter()
        .find(|c| c.role == "public_resolver_v2")
        .unwrap();
    let input = ManifestInput {
        manifest_id: 921,
        manifest_version: 1,
        namespace: manifest.namespace.clone(),
        source_family: manifest.source_family.clone(),
        chain_id: manifest.chain.clone(),
        deployment_label: manifest.deployment_epoch.clone(),
        normalizer_version: manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(manifest)?,
    };
    let mut public_admission = admission(921, "public_resolver_v2");
    public_admission.address = public.address.to_ascii_lowercase();
    public_admission.contract_instance_id = Uuid::from_u128(922);
    let block = i64::try_from(public.start_block.unwrap())? + 1;
    let mut records = vec![
        raw_at(
            TextUpdated {
                recordId: U256::from(7),
                keyHash: keccak256("url"),
                key: "url".into(),
                value: "shared".into(),
            }
            .encode_log_data(),
            block,
            0,
            CONTRACT,
        ),
        raw_at(
            resolver_strings::TextChanged {
                node: common::namehash_raw([b"eth".as_slice()].into_iter()).parse()?,
                indexedKey: keccak256("url"),
                key: "url".into(),
                value: "direct".into(),
            }
            .encode_log_data(),
            block,
            1,
            &public_admission.address,
        ),
    ];
    for raw in &mut records {
        raw.chain_id = manifest.chain.clone();
    }
    let output = interpret_test_batch(BatchInput {
        chain_id: manifest.chain.clone(),
        manifests: vec![input],
        discovery_rules: vec![],
        admissions: vec![admission(921, "permissioned_resolver"), public_admission],
        prior_events: vec![],
        blocks: vec![],
        raw_logs: records,
    })?;
    let changes = output
        .normalized_events
        .iter()
        .filter(|e| e.event_kind == "RecordChanged")
        .collect::<Vec<_>>();
    assert_eq!(changes.len(), 2);
    assert_eq!(
        changes[0].after_state["storage_model"],
        "resolver_record_id"
    );
    assert_eq!(changes[0].after_state["resolver_record_id"], "7");
    assert_eq!(changes[1].after_state["value"], "direct");
    assert!(changes[1].after_state.get("storage_model").is_none());
    assert!(
        changes
            .iter()
            .all(|e| e.logical_name_id.is_none() && e.resource_id.is_none())
    );
    Ok(())
}
