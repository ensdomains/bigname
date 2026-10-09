use super::*;
sol! {
    event RegistryCreated();
    event ParentUpdated(address indexed parent, string label, address indexed sender);
    event Upgraded(address indexed implementation);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
}

#[tokio::test]
async fn history_actions_detached_reservation_becomes_reachable_without_a_new_reserve() -> Result<()>
{
    let (manifest, _) = v2_history_bounded_regeneration::manifest_and_rules();
    let child: Address = "0x0000000000000000000000000000000000000c11".parse()?;
    let label = "reserved-child";
    let id = U256::from_be_bytes(*keccak256(label)) & !U256::from(u32::MAX);
    let mut logs = registration(0, 0, 120);
    logs.extend([
        emitted(RegistryCreated {}.encode_log_data(), child, 121, 0),
        emitted(
            LabelReserved {
                tokenId: id,
                labelHash: keccak256(label),
                label: label.into(),
                expiry: 1_900_000_000,
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
            child,
            122,
            0,
        ),
        raw(
            SubregistryUpdated {
                tokenId: token(0),
                subregistry: child,
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
            123,
            0,
        ),
        emitted(
            ParentUpdated {
                parent: REGISTRY.parse()?,
                label: LABEL.into(),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
            child,
            124,
            0,
        ),
    ]);
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=124).await?;
    seed_interpret(&database, &manifest, &logs).await?;
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    for block in 120..=124 {
        engine
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: None,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
    }
    routes::publish(&database, 124).await?;
    let body = page(&database, "/v1/events?namespace=ens&type=reservation").await?;
    let direct = action_rows(&body, "registration_reserved");
    let reachable = action_rows(&body, "reservation_became_reachable");
    assert_eq!(direct.len(), 1, "{body:#}");
    assert_eq!(reachable.len(), 1, "{body:#}");
    assert_eq!(direct[0]["block_number"], 122);
    assert!(direct[0]["name"].is_null(), "{body:#}");
    // The mount at block 123 already gives the child registry its name. The parent claim
    // one block later restates nothing.
    assert_eq!(reachable[0]["block_number"], 123);
    assert_eq!(reachable[0]["name"], format!("{label}.{NAME}"));
    assert_ne!(direct[0]["id"], reachable[0]["id"]);
    for row in direct.iter().chain(reachable.iter()) {
        assert_eq!(row["data"]["token_id"], id.to_string());
        assert!(row["data"].get("owner").is_none() && row["data"].get("registrant").is_none());
    }
    let name = page(
        &database,
        &format!("/v1/names/{label}.{NAME}/history?scope=both&type=reservation"),
    )
    .await?;
    assert_eq!(name["data"].as_array().unwrap().len(), 1, "{name:#}");
    assert_eq!(name["data"][0]["id"], reachable[0]["id"]);
    let (status,children)=read_family_response(&database,&format!("/v1/names/{NAME}/history?type=registration&include=data,child_registrations&page_size=200")).await?;
    assert_eq!(status, StatusCode::OK, "{children}");
    assert!(
        !children["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["name"] == format!("{label}.{NAME}")),
        "{children}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn history_actions_reservations_and_regeneration_preserve_exact_ids() -> Result<()> {
    let database = routes::database_at(134).await?;
    let all = page(
        &database,
        &format!("/v1/events?contract_address={REGISTRY}"),
    )
    .await?;
    let reserved = action_rows(&all, "registration_reserved");
    assert_eq!(reserved.len(), 2, "{all:#}");
    for (row, version) in reserved.iter().zip([0, 5]) {
        assert_eq!(row["type"], "reservation");
        assert_eq!(row["data"]["token_id"], token(version).to_string());
        assert!(row["data"].get("owner").is_none());
        assert!(row["data"].get("registrant").is_none());
    }
    let regen = action_rows(&all, "token_regenerated");
    assert_eq!(regen.len(), 2);
    for (row, version) in regen.iter().zip([0, 2]) {
        assert_eq!(row["type"], "token");
        assert_eq!(row["data"]["old_token_id"], token(version).to_string());
        assert_eq!(row["data"]["new_token_id"], token(version + 1).to_string());
        let grant = all["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| {
                g["kind"] == "RegistrationGranted"
                    && !g["registration_id"].is_null()
                    && g["data"]["token_id"] == token(version).to_string()
            })
            .context("prior registration")?;
        assert_eq!(row["registration_id"], grant["registration_id"]);
        assert!(row["data"].get("migration_path").is_none());
    }
    let filtered = page(&database, &format!("/v1/events?contract_address={REGISTRY}&type=reservation,token&exclude_type=reservation&kind=TokenRegenerated")).await?;
    assert_eq!(filtered["data"].as_array().unwrap().len(), 2);
    assert_eq!(filtered["page"]["total_count"], 2);
    let no_keys = page(
        &database,
        &format!(
            "/v1/events?contract_address={REGISTRY}&type=reservation,token&record_key=addr:60"
        ),
    )
    .await?;
    assert!(no_keys["data"].as_array().unwrap().is_empty());
    let (status, body) = read_family_response(
        &database,
        "/v1/events?namespace=ens&action=token_regenerated",
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    database.cleanup().await
}

#[tokio::test]
async fn history_actions_contract_lifecycle_is_global_without_name_or_account_fanout() -> Result<()>
{
    let (manifest, _) = v2_history_bounded_regeneration::manifest_and_rules();
    let mut logs = registration(0, 0, 120);
    logs.extend([
        raw(RegistryCreated {}.encode_log_data(), 121, 0),
        raw(
            ParentUpdated {
                parent: HOLDER.parse()?,
                label: "parent".into(),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
            121,
            1,
        ),
        raw(
            ParentUpdated {
                parent: Address::ZERO,
                label: "".into(),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
            121,
            2,
        ),
        raw(
            Upgraded {
                implementation: GRANTEE.parse()?,
            }
            .encode_log_data(),
            121,
            3,
        ),
    ]);
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=121).await?;
    seed_interpret(&database, &manifest, &logs).await?;
    bigname_interpret::Engine::new(database.pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: 120,
            to_block: 121,
            resume_current: None,
            mode: bigname_interpret::RunMode::Normal,
        })
        .await?;
    routes::publish(&database, 121).await?;
    let body = page(
        &database,
        &format!("/v1/events?contract_address={REGISTRY}&type=contract"),
    )
    .await?;
    let rows = body["data"].as_array().unwrap();
    assert_eq!(rows.len(), 4, "{body:#}");
    assert_eq!(body["page"]["total_count"], 4);
    assert!(
        rows.iter()
            .all(|r| r["name"].is_null() && r["registration_id"].is_null())
    );
    assert_eq!(rows[0]["data"]["action"], "registry_created");
    assert_eq!(
        rows[0]["data"]["registry"],
        json!({"chain_id":1,"address":REGISTRY})
    );
    assert_eq!(rows[1]["data"]["parent_cleared"], false);
    assert_eq!(rows[1]["data"]["parent"]["address"], HOLDER);
    assert_eq!(rows[1]["data"]["label"], "parent");
    assert_eq!(rows[2]["data"]["parent_cleared"], true);
    assert!(rows[2]["data"].get("parent").is_none());
    assert_eq!(rows[2]["data"]["label"], "");
    assert_eq!(rows[3]["data"]["implementation"]["address"], GRANTEE);
    assert_eq!(rows[3]["data"]["proxy"]["address"], REGISTRY);
    let overview = page(
        &database,
        &format!("/v1/events?contract_address={REGISTRY}"),
    )
    .await?;
    let (status, registry) = read_family_response(
        &database,
        &format!("/v1/registries/1/{REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{registry}");
    // Contract overview and event collection count exactly the same public rows.
    assert_eq!(
        registry["data"]["counts"]["events"], overview["page"]["total_count"],
        "{registry}"
    );
    for route in [
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/events?name={NAME}"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let body = page(&database, &format!("{route}&type=contract")).await?;
        assert!(body["data"].as_array().unwrap().is_empty(), "{body}");
    }
    database.cleanup().await
}
