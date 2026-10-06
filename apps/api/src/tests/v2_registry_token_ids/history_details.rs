//! Original ABI values survive normalization and the include=data HTTP boundary.
use super::*;

sol! {
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
}

const CALLER: &str = "0x00000000000000000000000000000000aabbccdd";

#[tokio::test]
async fn v2_history_details_preserve_registry_callers_and_original_role_words() -> Result<()> {
    let (manifest, _) = v2_history_bounded_regeneration::manifest_and_rules();
    let mut logs = registration(0, 0, 120);
    // The logged caller can differ from the transaction sender, owner and emitter.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/interfaces/IRegistryEvents.sol:L17-L24 @ ens_v2_sepolia_20261001@07e55a05)
    logs[0] = raw(
        LabelRegistered {
            tokenId: token(0),
            labelHash: keccak256(LABEL),
            label: LABEL.into(),
            owner: HOLDER.parse()?,
            expiry: 1_900_000_000,
            sender: CALLER.parse()?,
        }
        .encode_log_data(),
        120,
        0,
    );
    let high = (U256::from(1) << 255) | (U256::from(1) << 63);
    logs.extend([
        raw(
            ResolverUpdated {
                tokenId: token(0),
                resolver: routes::RESOLVER.parse()?,
                sender: CALLER.parse()?,
            }
            .encode_log_data(),
            121,
            0,
        ),
        raw(
            SubregistryUpdated {
                tokenId: token(0),
                subregistry: GRANTEE.parse()?,
                sender: CALLER.parse()?,
            }
            .encode_log_data(),
            121,
            1,
        ),
        raw(
            EACRolesChanged {
                resource: token(0),
                account: HOLDER.parse()?,
                oldRoleBitmap: U256::from(1),
                newRoleBitmap: high,
            }
            .encode_log_data(),
            121,
            2,
        ),
        raw(
            EACRolesChanged {
                resource: U256::ZERO,
                account: HOLDER.parse()?,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::MAX,
            }
            .encode_log_data(),
            121,
            3,
        ),
        raw(
            EACRolesChanged {
                resource: token(0),
                account: HOLDER.parse()?,
                oldRoleBitmap: high,
                newRoleBitmap: U256::ZERO,
            }
            .encode_log_data(),
            122,
            0,
        ),
        // ExpiryUpdated retains its own sender but is intentionally outside this field's scope.
        raw(
            ExpiryUpdated {
                tokenId: token(0),
                newExpiry: 1_900_000_001,
                sender: CALLER.parse()?,
            }
            .encode_log_data(),
            122,
            1,
        ),
    ]);
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=122).await?;
    seed_interpret(&database, &manifest, &logs).await?;
    bigname_interpret::Engine::new(database.pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: 120,
            to_block: 122,
            resume_current: None,
            mode: bigname_interpret::RunMode::Normal,
        })
        .await?;
    routes::publish(&database, 122).await?;
    let mut seen = std::collections::BTreeMap::new();
    for route in [
        format!("/v1/events?contract_address={REGISTRY}"),
        format!("/v1/events?name={NAME}"),
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let expanded = details_page(&database, &route, "data,raw,total_count").await?;
        let rows = expanded["data"].as_array().unwrap();
        for kind in [
            "RegistrationGranted",
            "ResolverChanged",
            "SubregistryChanged",
        ] {
            let row = rows
                .iter()
                .find(|r| r["kind"] == kind)
                .with_context(|| format!("{kind}: {expanded}"))?;
            assert_eq!(row["data"]["sender"], CALLER, "{row}");
            assert_eq!(row["contract_address"], REGISTRY, "{row}");
        }
        for row in rows {
            let expected = match (
                row["kind"].as_str().unwrap(),
                row["block_number"].as_i64().unwrap(),
            ) {
                ("PermissionChanged", 120) => Some((U256::ZERO, U256::from(1))),
                ("PermissionChanged", 121) => Some((U256::from(1), high)),
                ("PermissionChanged", 122) => Some((high, U256::ZERO)),
                ("RootPermissionChanged", 121) => Some((U256::ZERO, U256::MAX)),
                _ => None,
            };
            if let Some((old, new)) = expected {
                assert_words(row, old, new);
            } else {
                assert!(row["data"].get("old_role_bitmap").is_none(), "{row}");
                assert!(row["data"].get("new_role_bitmap").is_none(), "{row}");
            }
            if !matches!(
                row["kind"].as_str(),
                Some("RegistrationGranted" | "ResolverChanged" | "SubregistryChanged")
            ) {
                assert!(row["data"].get("sender").is_none(), "{row}");
            }
            if let Some(previous) = seen.insert(row["id"].to_string(), row["data"].clone()) {
                assert_eq!(previous, row["data"], "same event on {route}");
            }
        }
        let root_count = rows
            .iter()
            .filter(|row| row["kind"] == "RootPermissionChanged")
            .count();
        assert_eq!(
            root_count,
            usize::from(!route.contains("name=") && !route.contains("/names/"))
        );
        for includes in ["total_count", "raw,total_count"] {
            let lean = details_page(&database, &route, includes).await?;
            assert_eq!(lean["page"], expanded["page"]);
            assert_eq!(ids(&lean), ids(&expanded));
            assert!(
                lean["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|row| row.get("data").is_none())
            );
        }
        let mut first_pages = Vec::new();
        for includes in ["data,raw,total_count", "raw,total_count"] {
            let (status, page) = read_family_response(
                &database,
                &format!("{route}&include={includes}&order=asc&page_size=1"),
            )
            .await?;
            assert_eq!(status, StatusCode::OK, "{page}");
            assert!(page["page"]["next_cursor"].is_string(), "{page}");
            first_pages.push(page);
        }
        assert_eq!(first_pages[0]["page"], first_pages[1]["page"]);
        assert_eq!(ids(&first_pages[0]), ids(&first_pages[1]));
    }
    // Neither a later role change nor the current permission state overwrites the original words.
    let retained: Vec<Value> = sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE event_kind='PermissionChanged' ORDER BY block_number,log_index")
        .fetch_all(&database.pool).await?;
    assert_eq!(retained.len(), 3);
    assert_eq!(
        retained[1]["old_role_bitmap"],
        format!("{:#066x}", U256::from(1))
    );
    assert_eq!(retained[1]["role_bitmap"], format!("{high:#066x}"));
    database.cleanup().await
}

#[tokio::test]
async fn v2_history_details_preserve_resolver_role_words_without_display_metadata() -> Result<()> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let mut manifest = repository
        .manifests()
        .iter()
        .find(|m| m.manifest.source_family == "ens_v2_resolver_l1")
        .context("resolver manifest")?
        .manifest
        .clone();
    manifest.chain = CHAIN.into();
    let manifest = ManifestInput {
        manifest_id: 994,
        manifest_version: manifest.manifest_version as i64,
        namespace: manifest.namespace.clone(),
        source_family: manifest.source_family.clone(),
        chain_id: CHAIN.into(),
        deployment_label: manifest.deployment_epoch.clone(),
        normalizer_version: manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(&manifest)?,
    };
    let high = (U256::from(1) << 255) | (U256::from(1) << 63);
    let words = [(U256::ZERO, high), (high, U256::ZERO)];
    let logs = words
        .iter()
        .enumerate()
        .map(|(index, (old, new))| {
            let mut log = raw(
                EACRolesChanged {
                    resource: U256::from(42),
                    account: HOLDER.parse().unwrap(),
                    oldRoleBitmap: *old,
                    newRoleBitmap: *new,
                }
                .encode_log_data(),
                120,
                index as i64,
            );
            log.emitting_address = routes::RESOLVER.into();
            log
        })
        .collect();
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest.clone()],
            discovery_rules: vec![],
            prior_events: vec![],
            admissions: vec![AddressAdmissionInput {
                address: routes::RESOLVER.into(),
                contract_instance_id: Uuid::from_u128(994),
                source_manifest_id: Some(994),
                role: Some("permissioned_resolver".into()),
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
    assert_eq!(output.normalized_events.len(), 2);
    for (row, (old, new)) in output.normalized_events.iter().zip(words) {
        assert_eq!(row.after_state["old_role_bitmap"], format!("{old:#066x}"));
        assert_eq!(row.after_state["role_bitmap"], format!("{new:#066x}"));
        assert!(row.logical_name_id.is_none());
    }
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=120).await?;
    v2_history_bounded_rebinding::persist_with_manifests(&database.pool, &[manifest], &output)
        .await?;
    routes::publish(&database, 120).await?;
    for route in [
        format!("/v1/events?namespace=ens&resolver=1:{}", routes::RESOLVER),
        format!("/v1/events?contract_address={}", routes::RESOLVER),
    ] {
        let body = details_page(&database, &route, "data,raw,total_count").await?;
        let rows = body["data"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "{body}");
        for (row, (old, new)) in rows.iter().zip(words) {
            assert_words(row, old, new);
            assert!(row["data"].get("sender").is_none());
        }
        let lean = details_page(&database, &route, "raw,total_count").await?;
        assert_eq!(ids(&body), ids(&lean));
        assert_eq!(body["page"], lean["page"]);
        assert!(
            lean["data"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r.get("data").is_none())
        );
    }
    database.cleanup().await
}

fn assert_words(row: &Value, old: U256, new: U256) {
    assert_eq!(row["data"]["old_role_bitmap"], old.to_string(), "{row}");
    assert_eq!(row["data"]["new_role_bitmap"], new.to_string(), "{row}");
}

fn ids(body: &Value) -> Vec<&Value> {
    body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| &r["id"])
        .collect()
}

async fn details_page(database: &TestDatabase, route: &str, includes: &str) -> Result<Value> {
    let (status, body) = read_family_response(
        database,
        &format!("{route}&include={includes}&order=asc&page_size=200"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{route}: {body}");
    Ok(body)
}
