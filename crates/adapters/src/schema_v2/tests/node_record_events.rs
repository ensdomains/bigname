//! `ABIChanged`, `NameChanged` and `ContenthashChanged` go through the same resolver selection
//! and decoder as the other ENSv1 record events: from the directly declared PublicResolverV2, from
//! the Basenames resolver family, and from an undeclared (custom) resolver that the family's
//! all-emitter record events already cover. Each case loads the checked-in manifests of its profile, so the declarations are the
//! ones production reads.
use serde_json::Value;

use super::*;

sol! {
    event ABIChanged(bytes32 indexed node, uint256 indexed contentType);
    event NameChanged(bytes32 indexed node, string name);
    event ContenthashChanged(bytes32 indexed node, bytes hash);
}

const CUSTOM_RESOLVER: &str = "0x00000000000000000000000000000000000c0570";

/// The checked-in manifests of `profile` for `families`, as interpretation inputs, and one
/// declared admission per `[[contracts]]` entry.
fn profile(
    profile: &str,
    families: &[&str],
) -> anyhow::Result<(String, Vec<ManifestInput>, Vec<AddressAdmissionInput>)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../manifests")
        .join(profile);
    let repository = bigname_manifests::load_repository(root)?;
    let mut chain = None;
    let mut manifests = Vec::new();
    let mut admissions = Vec::new();
    for (index, loaded) in repository
        .manifests()
        .iter()
        .filter(|loaded| families.contains(&loaded.manifest.source_family.as_str()))
        .enumerate()
    {
        let manifest = &loaded.manifest;
        let manifest_id = 7_000 + i64::try_from(index)?;
        chain.get_or_insert_with(|| manifest.chain.clone());
        manifests.push(ManifestInput {
            manifest_id,
            manifest_version: 1,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            chain_id: manifest.chain.clone(),
            deployment_label: manifest.deployment_epoch.clone(),
            normalizer_version: manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(manifest)?,
        });
        for (ordinal, contract) in manifest.contracts.iter().enumerate() {
            let mut declared = admission(manifest_id, &contract.role);
            declared.address = contract.address.to_ascii_lowercase();
            declared.contract_instance_id =
                Uuid::from_u128(u128::try_from(manifest_id)? * 100 + u128::try_from(ordinal)?);
            declared.active_from_block = Some(0);
            admissions.push(declared);
        }
    }
    assert_eq!(manifests.len(), families.len(), "{families:?} in {profile}");
    Ok((chain.expect("profile chain"), manifests, admissions))
}

fn declared_address(admissions: &[AddressAdmissionInput], role: &str) -> String {
    admissions
        .iter()
        .find(|admission| admission.role.as_deref() == Some(role))
        .map(|admission| admission.address.clone())
        .unwrap_or_else(|| panic!("no {role} declaration"))
}

/// Content types a standard setter can write, including ones beyond the four ENSIP-4 names.
fn content_types() -> Vec<U256> {
    vec![U256::from(1), U256::from(16), U256::from(1) << 255]
}

/// `ABIChanged` logs from `emitter` for `node`, one per content type, in block `block`.
fn abi_logs(chain: &str, emitter: &str, node: B256, block: i64) -> Vec<RawLogInput> {
    content_types()
        .into_iter()
        .enumerate()
        .map(|(index, content_type)| {
            let mut raw = raw_at(
                ABIChanged {
                    node,
                    contentType: content_type,
                }
                .encode_log_data(),
                block,
                i64::try_from(index).unwrap(),
                emitter,
            );
            raw.chain_id = chain.to_owned();
            raw
        })
        .collect()
}

/// The ABI record writes of `output` from `emitter`, as (source family, selector, value).
fn abi_writes(output: &BatchOutput, emitter: &str) -> Vec<(String, Value, Value)> {
    output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "RecordChanged"
                && event.after_state["record_family"] == "abi"
                && event.after_state["resolver"] == json!(emitter)
        })
        .map(|event| {
            assert_eq!(event.after_state["source_event"], "ABIChanged");
            assert_eq!(
                event.after_state["record_key"],
                json!(format!(
                    "abi:{}",
                    event.after_state["selector_key"].as_str().unwrap()
                ))
            );
            (
                event.source_family.clone(),
                event.after_state["selector_key"].clone(),
                event.after_state["value"].clone(),
            )
        })
        .collect()
}

fn expected(family: &str) -> Vec<(String, Value, Value)> {
    content_types()
        .into_iter()
        .map(|content_type| {
            let text = json!(content_type.to_string());
            (family.to_owned(), text.clone(), text)
        })
        .collect()
}

#[test]
fn public_resolver_v2_abi_changed_is_a_record_write_like_its_other_node_events()
-> anyhow::Result<()> {
    let (chain, manifests, admissions) =
        profile("sepolia", &["ens_v1_resolver_l1", "ens_v2_resolver_l1"])?;
    let public = declared_address(&admissions, "public_resolver_v2");
    let node: B256 = common::namehash(&["abi".to_owned(), "eth".to_owned()]).parse()?;
    let block = 11_709_100;
    let mut raw_logs = abi_logs(&chain, &public, node, block);
    raw_logs.extend(abi_logs(&chain, CUSTOM_RESOLVER, node, block + 1));
    let output = interpret_test_batch(BatchInput {
        chain_id: chain,
        manifests,
        discovery_rules: vec![],
        admissions,
        prior_events: vec![],
        blocks: vec![],
        raw_logs,
    })?;
    // The declared PublicResolverV2 writes under its own family, where the ENSv2 pointer's
    // guarded record partition reads them.
    assert_eq!(abi_writes(&output, &public), expected("ens_v2_resolver_l1"));
    // A custom resolver on the same chain is decoded by the ENSv1 all-emitter record events.
    assert_eq!(
        abi_writes(&output, CUSTOM_RESOLVER),
        expected("ens_v1_resolver_l1")
    );
    Ok(())
}

/// ENSv2 has no ABIChanged decoder of its own, only the PublicResolverV2 node path. A declaration
/// that could select an empty or legacy role would pass selection and then fail to decode, so the
/// manifest is refused when it is loaded; the shipped role-scoped declaration still interprets.
#[test]
fn ensv2_abi_changed_declared_for_a_non_public_role_is_refused() -> anyhow::Result<()> {
    let (chain, manifests, admissions) =
        profile("sepolia", &["ens_v1_resolver_l1", "ens_v2_resolver_l1"])?;
    let public = declared_address(&admissions, "public_resolver_v2");
    let mirror = declared_address(&admissions, "ensv1_mirror_resolver");
    let node: B256 = common::namehash(&["abi".to_owned(), "eth".to_owned()]).parse()?;
    let block = 11_709_100;
    let mut raw_logs = abi_logs(&chain, &public, node, block);
    raw_logs.extend(abi_logs(&chain, &mirror, node, block + 1));
    let batch = |manifests: Vec<ManifestInput>| BatchInput {
        chain_id: chain.clone(),
        manifests,
        discovery_rules: vec![],
        admissions: admissions.clone(),
        prior_events: vec![],
        blocks: vec![],
        raw_logs: raw_logs.clone(),
    };

    let output = interpret_test_batch(batch(manifests.clone()))?;
    assert_eq!(abi_writes(&output, &public), expected("ens_v2_resolver_l1"));

    for roles in [
        json!([]),
        json!(["ensv1_mirror_resolver"]),
        json!(["public_resolver_v2", "ensv1_mirror_resolver"]),
    ] {
        let mut manifests = manifests.clone();
        let v2 = manifests
            .iter_mut()
            .find(|manifest| manifest.source_family == "ens_v2_resolver_l1")
            .unwrap();
        let mut payload: Value = serde_json::from_str(&v2.payload_json)?;
        let abi_changed = payload["abi"]["events"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|event| event["name"] == "ABIChanged")
            .unwrap();
        abi_changed["emitter_roles"] = roles.clone();
        v2.payload_json = serde_json::to_string(&payload)?;
        let error = interpret_test_batch(batch(manifests))
            .expect_err("a non-public ENSv2 ABIChanged declaration must be refused");
        assert!(
            format!("{error:#}").contains(
                "declares ABIChanged for a role other than public_resolver_v2; only the PublicResolverV2 node path decodes it"
            ),
            "{roles}: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn basenames_abi_changed_is_a_record_write_for_declared_and_custom_resolvers() -> anyhow::Result<()>
{
    let (chain, manifests, admissions) = profile("mainnet", &["basenames_base_resolver"])?;
    let declared = declared_address(&admissions, "resolver");
    let node: B256 =
        common::namehash(&["abi".to_owned(), "base".to_owned(), "eth".to_owned()]).parse()?;
    let mut raw_logs = abi_logs(&chain, &declared, node, 20);
    raw_logs.extend(abi_logs(&chain, CUSTOM_RESOLVER, node, 21));
    let output = interpret_test_batch(BatchInput {
        chain_id: chain,
        manifests,
        discovery_rules: vec![],
        admissions,
        prior_events: vec![],
        blocks: vec![],
        raw_logs,
    })?;
    assert_eq!(
        abi_writes(&output, &declared),
        expected("basenames_base_resolver")
    );
    assert_eq!(
        abi_writes(&output, CUSTOM_RESOLVER),
        expected("basenames_base_resolver")
    );
    Ok(())
}

#[test]
fn ensv1_custom_resolver_abi_changed_uses_the_ordinary_all_emitter_path() -> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile("mainnet", &["ens_v1_resolver_l1"])?;
    let node: B256 = common::namehash(&["abi".to_owned(), "eth".to_owned()]).parse()?;
    let declared = declared_address(&admissions, "public_resolver");
    let mut raw_logs = abi_logs(&chain, &declared, node, 22_764_900);
    raw_logs.extend(abi_logs(&chain, CUSTOM_RESOLVER, node, 22_764_901));
    let output = interpret_test_batch(BatchInput {
        chain_id: chain,
        manifests,
        discovery_rules: vec![],
        admissions,
        prior_events: vec![],
        blocks: vec![],
        raw_logs,
    })?;
    assert_eq!(
        abi_writes(&output, &declared),
        expected("ens_v1_resolver_l1")
    );
    assert_eq!(
        abi_writes(&output, CUSTOM_RESOLVER),
        expected("ens_v1_resolver_l1")
    );
    Ok(())
}

/// One `NameChanged` (a set, then a clear) and one `ContenthashChanged` (a set, then a clear)
/// from `emitter` for `node` in block `block`.
fn name_and_contenthash_logs(
    chain: &str,
    emitter: &str,
    node: B256,
    block: i64,
) -> Vec<RawLogInput> {
    let logs = [
        NameChanged {
            node,
            name: "primary.eth".to_owned(),
        }
        .encode_log_data(),
        NameChanged {
            node,
            name: String::new(),
        }
        .encode_log_data(),
        ContenthashChanged {
            node,
            hash: vec![0xe3, 0x01, 0x01].into(),
        }
        .encode_log_data(),
        ContenthashChanged {
            node,
            hash: Vec::new().into(),
        }
        .encode_log_data(),
    ];
    logs.into_iter()
        .enumerate()
        .map(|(index, log)| {
            let mut raw = raw_at(log, block, i64::try_from(index).unwrap(), emitter);
            raw.chain_id = chain.to_owned();
            raw
        })
        .collect()
}

/// The name and contenthash record writes of `output` from `emitter`, as (source family, record
/// key, value field).
fn name_and_contenthash_writes(output: &BatchOutput, emitter: &str) -> Vec<(String, Value, Value)> {
    output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "RecordChanged"
                && event.after_state["resolver"] == json!(emitter)
                && matches!(
                    event.after_state["record_family"].as_str(),
                    Some("name" | "contenthash")
                )
        })
        .map(|event| {
            let after = &event.after_state;
            let value = if after["record_family"] == "name" {
                after["raw_name"].clone()
            } else {
                after["contenthash_hex"].clone()
            };
            (
                event.source_family.clone(),
                after["record_key"].clone(),
                value,
            )
        })
        .collect()
}

fn expected_name_and_contenthash(family: &str) -> Vec<(String, Value, Value)> {
    [
        ("name", json!("primary.eth")),
        ("name", json!("")),
        ("contenthash", json!("0xe30101")),
        ("contenthash", json!("0x")),
    ]
    .into_iter()
    .map(|(key, value)| (family.to_owned(), json!(key), value))
    .collect()
}

#[test]
fn public_resolver_v2_name_changed_is_a_record_write_like_its_other_node_events()
-> anyhow::Result<()> {
    let (chain, manifests, admissions) =
        profile("sepolia", &["ens_v1_resolver_l1", "ens_v2_resolver_l1"])?;
    let public = declared_address(&admissions, "public_resolver_v2");
    let node: B256 = common::namehash(&["name".to_owned(), "eth".to_owned()]).parse()?;
    let block = 11_709_100;
    let mut raw_logs = name_and_contenthash_logs(&chain, &public, node, block);
    raw_logs.extend(name_and_contenthash_logs(
        &chain,
        CUSTOM_RESOLVER,
        node,
        block + 1,
    ));
    let output = interpret_test_batch(BatchInput {
        chain_id: chain,
        manifests,
        discovery_rules: vec![],
        admissions,
        prior_events: vec![],
        blocks: vec![],
        raw_logs,
    })?;
    assert_eq!(
        name_and_contenthash_writes(&output, &public),
        expected_name_and_contenthash("ens_v2_resolver_l1")
    );
    assert_eq!(
        name_and_contenthash_writes(&output, CUSTOM_RESOLVER),
        expected_name_and_contenthash("ens_v1_resolver_l1")
    );
    Ok(())
}

#[test]
fn basenames_contenthash_changed_is_a_record_write_for_declared_and_custom_resolvers()
-> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile("mainnet", &["basenames_base_resolver"])?;
    let declared = declared_address(&admissions, "resolver");
    let node: B256 =
        common::namehash(&["name".to_owned(), "base".to_owned(), "eth".to_owned()]).parse()?;
    let mut raw_logs = name_and_contenthash_logs(&chain, &declared, node, 20);
    raw_logs.extend(name_and_contenthash_logs(&chain, CUSTOM_RESOLVER, node, 21));
    let output = interpret_test_batch(BatchInput {
        chain_id: chain,
        manifests,
        discovery_rules: vec![],
        admissions,
        prior_events: vec![],
        blocks: vec![],
        raw_logs,
    })?;
    assert_eq!(
        name_and_contenthash_writes(&output, &declared),
        expected_name_and_contenthash("basenames_base_resolver")
    );
    assert_eq!(
        name_and_contenthash_writes(&output, CUSTOM_RESOLVER),
        expected_name_and_contenthash("basenames_base_resolver")
    );
    Ok(())
}
