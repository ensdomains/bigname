use super::*;
sol! {
    event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name);
    event AddressUpdated(uint256 indexed recordId, uint256 coinType, bytes addressBytes);
    event VersionChanged(bytes32 indexed node, uint64 newVersion);
}

async fn resolver_database(role: &str, logs: Vec<RawLogInput>) -> Result<TestDatabase> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let mut source = repository
        .manifests()
        .iter()
        .find(|m| m.manifest.source_family == "ens_v2_resolver_l1")
        .context("resolver manifest")?
        .manifest
        .clone();
    source.chain = CHAIN.into();
    let manifest = ManifestInput {
        manifest_id: 997,
        manifest_version: source.manifest_version as i64,
        namespace: source.namespace.clone(),
        source_family: source.source_family.clone(),
        chain_id: CHAIN.into(),
        deployment_label: source.deployment_epoch.clone(),
        normalizer_version: source.normalizer_version.clone(),
        payload_json: serde_json::to_string(&source)?,
    };
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest.clone()],
            discovery_rules: vec![],
            prior_events: vec![],
            admissions: vec![AddressAdmissionInput {
                address: routes::RESOLVER.into(),
                contract_instance_id: Uuid::from_u128(997),
                source_manifest_id: Some(997),
                role: Some(role.into()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            }],
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.into(),
                block_hash: "0xhistory120".into(),
                block_number: 120,
                block_timestamp: timestamp(1_700_000_120),
                canonicality_state: "canonical".into(),
            }],
            raw_logs: logs,
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    assert!(!output.normalized_events.is_empty());
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=120).await?;
    v2_history_bounded_rebinding::persist_with_manifests(&database.pool, &[manifest], &output)
        .await?;
    routes::publish(&database, 120).await?;
    Ok(database)
}

#[tokio::test]
async fn history_actions_record_links_unlinks_and_record_ids_stay_distinct_from_writes()
-> Result<()> {
    let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let mut dns = vec![LABEL.len() as u8];
    dns.extend_from_slice(LABEL.as_bytes());
    dns.extend_from_slice(b"\x03eth\x00");
    let mut logs = Vec::new();
    for (index, id) in [U256::MAX, U256::ZERO, U256::from(42)]
        .into_iter()
        .enumerate()
    {
        logs.push(emitted(
            Linked {
                recordId: id,
                node,
                name: dns.clone().into(),
            }
            .encode_log_data(),
            routes::RESOLVER.parse()?,
            120,
            index as i64,
        ));
    }
    logs.push(emitted(
        Linked {
            recordId: U256::from(42),
            node: alloy_primitives::B256::ZERO,
            name: vec![0].into(),
        }
        .encode_log_data(),
        routes::RESOLVER.parse()?,
        120,
        3,
    ));
    logs.push(emitted(
        AddressUpdated {
            recordId: U256::from(42),
            coinType: U256::from(60),
            addressBytes: HOLDER.parse::<Address>()?.to_vec().into(),
        }
        .encode_log_data(),
        routes::RESOLVER.parse()?,
        120,
        4,
    ));
    let database = resolver_database("permissioned_resolver", logs).await?;
    for route in [
        format!("/v1/events?namespace=ens&resolver=1:{}", routes::RESOLVER),
        format!("/v1/events?contract_address={}", routes::RESOLVER),
    ] {
        let body = page(&database, &route).await?;
        let links = action_rows(&body, "resolver_record_linked");
        assert_eq!(links.len(), 4, "{body:#}");
        for (row, id) in links
            .iter()
            .zip([U256::MAX, U256::ZERO, U256::from(42), U256::from(42)])
        {
            assert_eq!(row["type"], "resolver");
            assert_eq!(row["data"]["record_id"], id.to_string());
            assert!(
                row["name"].is_null(),
                "DNS bytes alone are not association: {row}"
            );
        }
        assert_eq!(
            links[0]["data"]["dns_encoded_name"],
            format!("0x{}", hex::encode(&dns))
        );
        let writes = page(&database, &format!("{route}&record_key=addr:60")).await?;
        assert_eq!(writes["data"].as_array().unwrap().len(), 1, "{writes}");
        assert_eq!(writes["data"][0]["data"]["action"], "record_changed");
        assert_eq!(writes["data"][0]["data"]["record_id"], "42");
    }
    database.cleanup().await
}

#[tokio::test]
async fn history_actions_record_reset_keeps_uint64_version_exact() -> Result<()> {
    let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let database = resolver_database(
        "public_resolver_v2",
        vec![emitted(
            VersionChanged {
                node,
                newVersion: u64::MAX,
            }
            .encode_log_data(),
            routes::RESOLVER.parse()?,
            120,
            0,
        )],
    )
    .await?;
    let body = page(
        &database,
        &format!(
            "/v1/events?namespace=ens&resolver=1:{}&type=record&record_key=addr:60",
            routes::RESOLVER
        ),
    )
    .await?;
    let rows = body["data"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{body}");
    assert_eq!(rows[0]["data"]["action"], "record_version_changed");
    assert_eq!(rows[0]["data"]["record_version"], u64::MAX.to_string());
    assert_eq!(rows[0]["data"]["node"], format!("{node:#x}"));
    assert!(rows[0]["data"].get("key").is_none() && rows[0]["data"].get("value").is_none());
    database.cleanup().await
}
