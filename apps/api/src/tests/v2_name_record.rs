#[tokio::test]
async fn v2_get_name_returns_flat_name_record_envelope() -> Result<()> {
    let payload = v2_name_record_payload("/v1/names/Alice.eth").await?;

    assert!(payload.get("page").is_none());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("name"), Some(&json!("alice.eth")));
    assert_eq!(data.get("display_name"), Some(&json!("alice.eth")));
    assert_eq!(data.get("namespace"), Some(&json!("ens")));
    assert_eq!(
        data.get("namehash"),
        Some(&json!(
            "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec"
        ))
    );
    assert_eq!(data.get("registration_status"), Some(&json!("active")));
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert_eq!(data.get("chain_id"), Some(&json!(1)));
    assert_eq!(data.get("network"), Some(&json!("ethereum")));
    assert_eq!(
        data.get("resolver"),
        Some(&json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        }))
    );
    assert_eq!(
        data.get("registration_id"),
        Some(&json!(Uuid::from_u128(0x2200).to_string()))
    );
    assert_eq!(
        data.get("token_id"),
        Some(&json!(
            "70564938991660933374592024341600875602376452319261984317470407481576058979585"
        ))
    );
    assert_eq!(
        data.get("owner"),
        Some(&json!("0x00000000000000000000000000000000000000bb"))
    );
    assert!(data.get("manager").is_none());
    assert_eq!(
        data.get("registrant"),
        Some(&json!("0x00000000000000000000000000000000000000aa"))
    );
    assert_eq!(
        parse_rfc3339_utc_timestamp(data["registered_at"].as_str().context("registered_at")?).unwrap(),
        parse_rfc3339_utc_timestamp("2024-01-02T03:04:05Z").unwrap(),
    );
    assert_eq!(
        parse_rfc3339_utc_timestamp(data["created_at"].as_str().context("created_at")?).unwrap(),
        parse_rfc3339_utc_timestamp("2023-01-02T03:04:05Z").unwrap(),
    );
    assert_eq!(
        parse_rfc3339_utc_timestamp(data["expires_at"].as_str().context("expires_at")?).unwrap(),
        parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z").unwrap(),
    );
    assert_eq!(
        data.get("records"),
        Some(&json!({
            "address_keys": ["60"],
            "addresses": {"60": "0x0000000000000000000000000000000000000def"},
            "text_keys": ["avatar", "description"],
            "texts": {
                "avatar": "https://example.test/avatar.png",
                "description": "Alice profile"
            },
            "abi_keys": [],
            "abis": {},
            "contenthash": "ipfs://alice",
            "name": null
        })),
        "{payload}"
    );
    assert!(data.get("primary_name").is_none());
    assert_eq!(
        data.get("primary_address"),
        Some(&json!("0x0000000000000000000000000000000000000def"))
    );
    assert!(data.get("unsupported_fields").is_none());

    Ok(())
}

#[tokio::test]
async fn storage_name_surface_reads_preserve_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";
    const INPUT_LOGICAL_NAME_ID: &str = "ens:ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        INPUT_LOGICAL_NAME_ID,
        NORMALIZED_NAME,
        NORMALIZED_NAME,
        "node:ᏣᎳᎩ.eth",
        Uuid::from_u128(0x349_6001),
        Uuid::from_u128(0x349_6002),
        Uuid::from_u128(0x349_6003),
        "0x0000000000000000000000000000000000000349",
        bigname_storage::AddressNameRelation::TokenHolder,
        349,
    )
    .await?;

    let logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = $1",
    )
    .bind(NORMALIZED_NAME)
    .fetch_one(&database.pool)
    .await?;
    let one = bigname_storage::load_name_surface(&database.pool, &logical_name_id)
        .await?
        .expect("seeded surface");
    assert_eq!(one.normalized_name, NORMALIZED_NAME);

    let many = bigname_storage::load_name_surfaces_by_logical_name_ids(
        &database.pool,
        std::slice::from_ref(&logical_name_id),
    )
    .await?;
    assert_eq!(many[&logical_name_id].normalized_name, NORMALIZED_NAME);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_subnames_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.parent.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_subname_inputs(
        &database,
        NORMALIZED_NAME,
        85,
        Uuid::from_u128(0x349_2001),
        Uuid::from_u128(0x349_2002),
        Uuid::from_u128(0x349_2003),
        SubnameInput::RegistryOwner("0x0000000000000000000000000000000000034920"),
    )
    .await?;
    seed_subname_edge(
        &database,
        "parent.eth",
        "ᏣᎳᎩ".as_bytes(),
        "0x0000000000000000000000000000000000034920",
        85,
    )
    .await?;
    publish_subname_inputs(&database).await?;
    let stored_raw_name: String =
        sqlx::query_scalar("SELECT raw_name FROM bigname_phase.name_surfaces WHERE raw_name = $1")
            .bind(NORMALIZED_NAME)
            .fetch_one(&database.pool)
            .await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?page_size=20")
            .await?;
    let row = payload["data"]
        .as_array()
        .expect("subnames data must be an array")
        .iter()
        .find(|row| row["display_name"] == json!(NORMALIZED_NAME))
        .expect("Cherokee subname must be served");
    assert_eq!(row["name"], json!(stored_raw_name));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:ᏣᎳᎩ.eth",
        NORMALIZED_NAME,
        NORMALIZED_NAME,
        "namehash:ᏣᎳᎩ.eth",
        Uuid::from_u128(0x349_4001),
        Uuid::from_u128(0x349_4002),
        Uuid::from_u128(0x349_4003),
        "0x0000000000000000000000000000000000034940",
        bigname_storage::AddressNameRelation::TokenHolder,
        43,
    )
    .await?;
    let stored_raw_name: String = sqlx::query_scalar(
        "SELECT raw_name FROM bigname_phase.name_surfaces WHERE raw_name = $1",
    )
    .bind(NORMALIZED_NAME)
    .fetch_one(&database.pool)
    .await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        "/v1/names/%E1%8F%A3%E1%8E%B3%E1%8E%A9.eth",
    )
    .await?;
    assert_eq!(payload["data"]["name"], json!(stored_raw_name));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_does_not_serve_a_resolver_without_projected_authority() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Unbound).await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert!(data.get("resolver").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_serves_a_root_registry_pointer_without_projected_authority() -> Result<()> {
    let payload = v2_root_pointer_payload("/v1/names/eth", true).await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("ok")), "{payload}");
    assert_eq!(
        data.get("resolver"),
        Some(&json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        })),
        "{payload}"
    );
    assert_eq!(data.get("registration_status"), Some(&json!("unregistered")));
    assert!(data.get("registration_id").is_none(), "{payload}");
    assert!(data.get("owner").is_none_or(Value::is_null), "{payload}");
    assert!(data.get("authority").is_none_or(Value::is_null), "{payload}");
    assert_eq!(
        data["records"]["addresses"]["60"],
        json!("0x0000000000000000000000000000000000000def"),
        "records come from the serving resource's inventory: {payload}"
    );

    // The same pointer evidence without a serving resource stays withheld.
    let payload = v2_root_pointer_payload("/v1/names/eth", false).await?;
    assert_eq!(payload["data"]["status"], json!("ok"));
    assert!(payload["data"].get("resolver").is_none(), "{payload}");
    Ok(())
}

#[tokio::test]
async fn v2_get_name_exposes_projected_wrapper_state_and_fuses() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Wrapped).await?;

    assert_eq!(payload["data"]["wrapper_state"], json!("locked"));
    assert_eq!(payload["data"]["wrapper_fuses"]["fuses"], json!(196_609));
    assert_eq!(payload["data"]["wrapper_fuses"]["cannot_unwrap"], json!(true));
    assert_eq!(
        payload["data"]["wrapper_fuses"]["parent_cannot_control"],
        json!(true)
    );
    Ok(())
}

#[test]
fn name_record_decoder_rejects_unknown_wrapper_fuse_fields() {
    let mut row = exact_name_row("ens:alice.eth", Uuid::from_u128(0x3300), Uuid::from_u128(0x2200), Uuid::from_u128(0x1100));
    row.declared_summary["wrapper_state"] = json!("locked");
    row.declared_summary["wrapper_fuses"] = json!({
                "fuses": 65_537,
                "cannot_unwrap": true,
                "cannot_burn_fuses": false,
                "cannot_transfer": false,
                "cannot_set_resolver": false,
                "cannot_set_ttl": false,
                "cannot_create_subdomain": false,
                "cannot_approve": false,
                "parent_cannot_control": true,
                "is_dot_eth": false,
                "can_extend_expiry": false,
                "unknown_future_fuse": true
            });
    assert!(crate::v2::build_name_record(&row, None, Some(1), crate::v2::Status::Ok).is_err());
}

#[tokio::test]
async fn v2_get_name_response_omits_banned_v1_spellings() -> Result<()> {
    let payload = v2_name_record_payload("/v1/names/Alice.eth").await?;
    assert_no_banned_v1_spellings(&payload);
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_basenames_keeps_stale_inventory_before_lookup() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_basenames_record_lookup(
        &lookup_pool,
        21_000_003,
        "0xbase-binding",
        "0xbinding",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database.seed_snapshot_selector_chain_positions(&json!({
        "base":{"chain_id":"base-mainnet","block_number":21000003,"block_hash":"0xbase-binding","timestamp":"2026-04-17T00:00:03Z"},
        "ethereum":{"chain_id":"ethereum-mainnet","block_number":21000003,"block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03Z"}
    })).await?;
    let before =
        v2_name_record_payload_for_database(&database, "/v1/names/alice.base.eth?source=verified")
            .await?;
    let token = before["meta"]["as_of_token"]
        .as_str()
        .context("Base and Ethereum snapshot")?;
    database.seed_snapshot_selector_chain_positions(&json!({"base":{
        "chain_id":"base-mainnet","block_number":21000004,"block_hash":"0xbase-next","timestamp":"2026-04-17T00:00:04Z"
    }})).await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 21000004, "0xbase-next").await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/alice.base.eth?source=verified&at={token}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 stale basenames verified name profile request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;

    assert_eq!(status, StatusCode::CONFLICT, "{payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(
        payload["error"]["message"],
        json!("requested snapshot is not available for name")
    );

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_reports_stale_when_lookup_state_is_unavailable() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_alice_verified_inputs(
        &database,
        AliceInputState::Wrapped,
        &[family_fixture_record_write(
            "addr:60",
            Some(json!("0x0000000000000000000000000000000000000def")),
        )],
    )
    .await?;
    let payload =
        v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth?source=verified")
            .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("stale"));
    assert_eq!(
        payload["data"]["failure_reason"],
        json!("verified_answer_stale_for_snapshot")
    );
    assert_eq!(payload["data"]["unsupported_fields"], json!(["primary_address"]));
    // Every read key stays listed; a stale answer maps none of them, so the indexed value is
    // not substituted.
    let records = &payload["data"]["records"];
    assert!(records["address_keys"].as_array().is_some_and(|keys| keys.contains(&json!("60"))), "{payload}");
    assert_eq!(records["addresses"], json!({}), "{payload}");
    assert_eq!(records["texts"], json!({}), "{payload}");
    assert!(records.get("contenthash").is_none(), "{payload}");
    assert!(payload["data"].get("primary_address").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_verified_source_reports_unsupported_without_verified_boundary() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth?source=verified", AliceInputState::Unbound).await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("unsupported"));
    assert_eq!(
        payload["data"]["unsupported_reason"],
        json!("verified_records_not_supported")
    );
    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["primary_address"])
    );
    assert!(payload["data"].get("records").is_none());
    assert!(payload["data"].get("primary_address").is_none());

    Ok(())
}

/// A name the projection selected under the ENSv2 arm is refused by an execution manifest that
/// declares no `verified_authority_arms` (the Mainnet and Sepolia default, `ens_v1` only), with
/// its own public reason and without a provider call; the same declaration widened to `ens_v2`
/// executes the direct route through the declared Universal Resolver.
#[tokio::test]
async fn v2_verified_reads_follow_the_declared_authority_arms_for_an_ens_v2_name() -> Result<()> {
    for admits_ens_v2 in [false, true] {
        let database = TestDatabase::new_with_schemas(false, true).await?;
        seed_alice_verified_inputs(
            &database,
            AliceInputState::Registry,
            &[family_fixture_record_write(
                "addr:60",
                Some(json!("0x0000000000000000000000000000000000000def")),
            )],
        )
        .await?;
        if admits_ens_v2 {
            sqlx::query(
                "UPDATE bigname_phase.manifest_versions
                 SET manifest_payload = manifest_payload
                     || '{\"verified_authority_arms\": [\"ens_v1\", \"ens_v2\"]}'::jsonb
                 WHERE source_family = 'ens_execution'",
            )
            .execute(&database.pool)
            .await?;
        }
        let executed_address = "0x0000000000000000000000000000000000000e0e";
        let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(if admits_ens_v2 {
            vec![resolution_universal_resolver_addr60_response(
                executed_address,
            )]
        } else {
            Vec::new()
        })
        .await?;
        let chain_rpc_urls =
            bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
        let state = database
            .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
            .await?;

        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/names/Alice.eth/records?source=verified&keys=addr:60")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("ens_v2-arm verified records request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
        assert_eq!(payload["meta"]["source"], json!("verified"));
        if admits_ens_v2 {
            assert_eq!(
                payload["data"]["records"]["addr:60"],
                json!({"status": "ok", "value": executed_address}),
                "{payload}"
            );
            assert_eq!(
                join_primary_name_mock_rpc_requests(rpc_handle).await?.len(),
                1
            );
        } else {
            assert_eq!(
                payload["data"]["records"]["addr:60"],
                json!({
                    "status": "unsupported",
                    "unsupported_reason": "exact_name_authority_not_verifiable"
                }),
                "{payload}"
            );
            // Name detail shares the refusal and its reason.
            let detail = app_router(state)
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/Alice.eth?source=verified")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ens_v2-arm verified name detail request failed")?;
            let detail: Value = read_json(detail).await?;
            assert_eq!(detail["data"]["status"], json!("unsupported"), "{detail}");
            assert_eq!(
                detail["data"]["unsupported_reason"],
                json!("exact_name_authority_not_verifiable"),
                "{detail}"
            );
            assert_eq!(
                join_primary_name_mock_rpc_requests(rpc_handle).await?.len(),
                0
            );
        }

        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_accepts_event_linked_ownerless_registry_serving() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_verified_inputs(
        &database,
        AliceInputState::Ownerless,
        &[family_fixture_record_write(
            "addr:2147483648",
            Some(json!("0x0000000000000000000000000000000000000def")),
        )],
    )
    .await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    assert!(
        bigname_storage::load_name_current(&database.pool, &logical)
            .await?
            .is_some()
    );
    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_multicoin_response(executed_address),
        resolution_universal_resolver_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ownerless verified name profile request failed")?;

    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("ok"));
    assert_eq!(payload["data"]["records"]["addresses"]["60"], json!(executed_address));
    assert_eq!(
        join_primary_name_mock_rpc_requests(rpc_handle).await?.len(),
        2
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_executes_without_legacy_persistence_and_aborts_transport_failure()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;

    enable_alice_ensip19_inputs(&database).await?;
    insert_record_lookup_fixture_writes(
        &database,
        &[family_fixture_record_write(
            "addr:2147483648",
            Some(json!("0x0000000000000000000000000000000000000def")),
        )],
    )
    .await?;
    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_multicoin_response(executed_address),
        resolution_universal_resolver_addr60_response(executed_address),
        resolution_universal_resolver_multicoin_response(executed_address),
        resolution_universal_resolver_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 on-demand verified name profile request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_v2_name_snapshot_meta(&payload);
    assert_eq!(payload["data"]["status"], json!("ok"));
    assert_eq!(
        payload["data"]["records"]["addresses"],
        json!({
            "2147483648": executed_address,
            "60": executed_address
        })
    );
    assert_eq!(payload["data"]["records"]["address_keys"], json!(["60", "2147483648"]));
    assert_eq!(payload["data"]["records"]["text_keys"], json!([]));
    assert!(payload["data"]["records"].get("name").is_none());
    assert_eq!(payload["data"]["primary_address"], json!(executed_address));
    assert!(payload["data"].get("unsupported_fields").is_none(), "{payload}");
    assert_ne!(
        payload["data"]["records"]["addresses"]["60"],
        json!("0x0000000000000000000000000000000000000def")
    );

    let repeated_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 repeated verified name profile request failed")?;
    assert_eq!(repeated_response.status(), StatusCode::OK);
    let repeated_payload: Value = read_json(repeated_response).await?;
    assert_eq!(
        repeated_payload["data"]["records"]["addresses"]["60"],
        json!(executed_address)
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let mut transport_failure_state = state;
    transport_failure_state.lookup_chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!(
            "ethereum-mainnet={unavailable_rpc_url}"
        )])?;
    let transport_failure_response = app_router(transport_failure_state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 transport-failed verified name profile request failed")?;
    let transport_failure_status = transport_failure_response.status();
    let transport_failure_payload: Value = read_json(transport_failure_response).await?;
    assert_eq!(
        transport_failure_status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "unexpected response: {transport_failure_payload}"
    );
    assert_eq!(
        transport_failure_payload["error"]["code"],
        json!("internal_error")
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(
        rpc_requests.len(),
        4,
        "v2 must not reuse a verified cache outcome"
    );
    for request in &rpc_requests {
        assert_eq!(request["method"], json!("eth_call"));
        assert_eq!(
            request["params"][0]["to"],
            json!("0xeeeeeeee14d718c2b47d9923deab1335e144eeee")
        );
        assert_eq!(
            request["params"][1],
            json!({
                "blockHash": execution_block_hash,
                "requireCanonical": true
            })
        );
    }
    let ledger_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL")
            .fetch_one(&lookup_pool)
            .await?;
    assert_eq!(ledger_count, 2);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_verified_records_return_conflict_when_project_generation_changes_during_rpc()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, request_reached, release_response, rpc_handle) =
        spawn_primary_name_mock_rpc_with_last_response_gate(vec![
            resolution_universal_resolver_addr60_response(executed_address),
        ])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/names/Alice.eth/records?source=verified&keys=addr:60")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    request_reached
        .await
        .context("verified lookup did not reach its provider call")?;
    insert_record_lookup_fixture_writes(
        &database,
        &[family_fixture_record_write(
            "addr:60",
            Some(json!("0x0000000000000000000000000000000000000123")),
        )],
    )
    .await?;
    release_response
        .send(())
        .map_err(|()| anyhow::anyhow!("verified lookup dropped its provider response gate"))?;

    let response = request_task
        .await
        .context("v2 concurrent verified records task panicked")?
        .context("v2 concurrent verified records request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unexpected response: {payload}"
    );
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_null_exact_resolver_auto_and_verified_execute_universal_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    append_name_resolver_input(
        &database,
        "ens",
        "alice.eth",
        "0x0000000000000000000000000000000000000000",
    )
    .await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_text_response("https://alice.example"),
        resolution_universal_resolver_text_response(""),
        resolution_universal_resolver_addr60_response(executed_address),
        resolution_resolver_not_found_error(b"\x05alice\x03eth\0"),
    ])
    .await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;

    let auto_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=auto&keys=text:url,avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver auto records request failed")?;
    assert_eq!(auto_response.status(), StatusCode::OK);
    let auto_payload: Value = read_json(auto_response).await?;
    assert_eq!(auto_payload["meta"]["source"], json!("verified"));
    assert_eq!(auto_payload["data"]["resolver"], Value::Null);
    let mut mixed_statuses = [
        auto_payload["data"]["records"]["text:url"]["status"]
            .as_str()
            .expect("text:url status"),
        auto_payload["data"]["records"]["avatar"]["status"]
            .as_str()
            .expect("avatar status"),
    ];
    mixed_statuses.sort_unstable();
    assert_eq!(mixed_statuses, ["not_found", "ok"]);

    let verified_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver verified records request failed")?;
    assert_eq!(verified_response.status(), StatusCode::OK);
    let verified_payload: Value = read_json(verified_response).await?;
    assert_eq!(verified_payload["meta"]["source"], json!("verified"));
    assert_eq!(verified_payload["data"]["resolver"], Value::Null);
    assert_eq!(
        verified_payload["data"]["records"]["addr:60"],
        json!({"status": "ok", "value": executed_address})
    );
    let missing_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=verified&keys=text:url")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver missing-resolver request failed")?;
    assert_eq!(missing_response.status(), StatusCode::OK);
    let missing_payload: Value = read_json(missing_response).await?;
    assert_eq!(missing_payload["meta"]["source"], json!("verified"));
    assert_eq!(missing_payload["data"]["resolver"], Value::Null);
    assert_eq!(
        missing_payload["data"]["records"]["text:url"]["status"],
        json!("not_found")
    );
    assert_eq!(
        missing_payload["data"]["records"]["text:url"]["failure_reason"],
        json!("resolver_not_found")
    );
    let summary_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver summary request failed")?;
    assert_eq!(summary_response.status(), StatusCode::OK);
    let summary_payload: Value = read_json(summary_response).await?;
    // No inventory row: the default key set is empty, and unkeyed auto makes no provider call.
    assert_eq!(summary_payload["meta"]["source"], json!("indexed"));
    assert_eq!(summary_payload["data"]["records"], json!({}));

    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'deprecated'
         WHERE namespace = 'ens' AND source_family = 'ens_execution'",
    )
    .execute(&lookup_pool)
    .await?;
    let no_entrypoint_response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=auto&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver request without an admitted entrypoint failed")?;
    assert_eq!(no_entrypoint_response.status(), StatusCode::OK);
    let no_entrypoint_payload: Value = read_json(no_entrypoint_response).await?;
    assert_eq!(no_entrypoint_payload["meta"]["source"], json!("verified"));
    assert_eq!(
        no_entrypoint_payload["data"]["records"]["addr:60"]["status"],
        json!("unsupported")
    );
    assert_eq!(
        no_entrypoint_payload["data"]["records"]["addr:60"]["unsupported_reason"],
        json!("verified_records_not_supported")
    );
    assert_eq!(
        join_primary_name_mock_rpc_requests(rpc_handle).await?.len(),
        4
    );
    let ledger_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
        .fetch_one(&lookup_pool)
        .await?;
    assert_eq!(ledger_count, 0);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_null_resolver_auto_rejects_direct_route_selected_after_admission() -> Result<()> {
    assert_null_resolver_route_flip_is_stale("auto").await
}

#[tokio::test]
async fn v2_null_resolver_verified_rejects_direct_route_selected_after_admission() -> Result<()> {
    assert_null_resolver_route_flip_is_stale("verified").await
}

async fn assert_null_resolver_route_flip_is_stale(source: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    append_name_resolver_input(
        &database,
        "ens",
        "alice.eth",
        "0x0000000000000000000000000000000000000000",
    )
    .await?;

    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_basenames_l1_addr60_response(
            "0x0000000000000000000000000000000000000e0e",
        )])
        .await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;
    let (_guard, control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
    let uri = format!("/v1/names/Alice.eth/records?source={source}&keys=addr:60");
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    let direct_resolver = "0x1000000000000000000000000000000000000001";
    append_name_resolver_input(&database, "ens", "alice.eth", direct_resolver).await?;
    control.resume().await;

    let response = request_task
        .await
        .context("v2 discovery-route mismatch request task panicked")?
        .context("v2 discovery-route mismatch request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unexpected response: {payload}"
    );
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());
    assert_eq!(
        join_primary_name_mock_rpc_requests(rpc_handle).await?.len(),
        1
    );

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_default_source_matches_explicit_indexed() -> Result<()> {
    let default_payload = v2_name_record_payload("/v1/names/Alice.eth").await?;
    let indexed_payload = v2_name_record_payload("/v1/names/Alice.eth?source=indexed").await?;

    assert_eq!(default_payload, indexed_payload);

    Ok(())
}

#[tokio::test]
async fn v2_get_name_omits_record_maps_when_inventory_is_absent() -> Result<()> {
    let payload = v2_name_payload_without_inventory("/v1/names/Alice.eth").await?;

    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["primary_address"])
    );
    assert!(payload["data"].get("records").is_none());
    assert!(payload["data"].get("primary_address").is_none());

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_ens_v2_registry_as_registered() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Registry).await?;

    assert_eq!(payload["data"]["registration_status"], json!("registered"), "{payload:#}");

    Ok(())
}

/// An ENSv2 registration never transferred afterwards. The registry mints the token with a
/// TransferSingle from the zero address, which the adapter does not write; the owner is on the
/// AuthorityTransferred of the TokenResource log, so the name serves its registrant as owner and
/// reads registered.
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L466-L471 @ ens_v2@a971bd64)
#[tokio::test]
async fn v2_get_name_serves_an_untransferred_ens_v2_registration_with_its_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    sqlx::query(
        "UPDATE surface_bindings SET authority_arm = 'ens_v2' WHERE surface_binding_id = $1",
    )
    .bind(Uuid::from_u128(0x3300))
    .execute(&database.pool)
    .await?;
    let owner = "0x00000000000000000000000000000000000000cc";
    append_alice_name_input(
        &database,
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({"source_event":"LabelRegistered", "authority_kind":"ens_v2_registry",
            "registrant":owner, "expiry":4_000_000_000_u64, "status":"registered"}),
    )
    .await?;
    append_alice_name_input(
        &database,
        "AuthorityTransferred",
        "ens_v2_registry_l1",
        json!({"source_event":"LabelRegistered", "owner":owner}),
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let payload = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
    database.cleanup().await?;

    let data = &payload["data"];
    assert_eq!(
        (&data["registration_status"], &data["owner"], &data["registrant"]),
        (&json!("registered"), &json!(owner), &json!(owner)),
        "{payload:#}"
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_released_as_released() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Released).await?;

    assert_eq!(payload["data"]["registration_status"], json!("released"));

    Ok(())
}

#[tokio::test]
async fn storage_name_current_reads_serve_the_released_v1_tombstone_on_its_closed_binding()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    sqlx::query("UPDATE surface_bindings SET active_to=to_timestamp(1776384003) WHERE surface_binding_id=$1")
        .bind(Uuid::from_u128(0x3300)).execute(&database.pool).await?;
    append_alice_name_input(
        &database,
        "RegistrationReleased",
        "ens_v1_registrar_l1",
        json!({"authority_kind":"registrar","released_at":1776384003,"expiry":1700000000}),
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21000003, "0xbinding").await?;
    let released = bigname_storage::load_name_current(&database.pool, &logical)
        .await?
        .context("released name")?;
    let closed: bool = sqlx::query_scalar(
        "SELECT active_to IS NOT NULL FROM surface_bindings WHERE surface_binding_id=$1",
    )
    .bind(Uuid::from_u128(0x3300))
    .fetch_one(&database.pool)
    .await?;
    assert!(closed);
    assert_eq!(
        released.declared_summary["registration"]["status"],
        json!("released")
    );
    let listed = bigname_storage::families::name::load_family_search_page(
        &database.pool,
        &bigname_storage::NameCurrentListFilter {
            namespace: Some("ens".into()),
            name: Some("alice.eth".into()),
            ..Default::default()
        },
        None,
        1,
    )
    .await?;
    assert_eq!(listed.rows.len(), 1);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_withholds_retained_inventory_for_released_tombstone() -> Result<()> {
    // The fixture's inventory row and declared resolver stay attached: a
    // released tombstone must not serve them even if projection state loss
    // retains them.
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Released).await?;

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert_eq!(data.get("registration_status"), Some(&json!("released")));
    assert!(data.get("resolver").is_none());
    assert!(data.get("records").is_none());
    assert!(data.get("primary_address").is_none());
    assert_eq!(
        data.get("unsupported_fields"),
        Some(&json!(["primary_address"]))
    );
    Ok(())
}

// A released lease retains its historical expiry and lapsed holder, while current control and
// records disappear even when a distinct registry-only binding remains open.
#[tokio::test]
async fn v2_get_name_serves_a_lapsed_handed_off_lease_as_released() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_handed_off_lease_inputs(&database, "alice.eth", true).await?;
    let payload = v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth").await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert_eq!(data.get("registration_status"), Some(&json!("released")));
    for field in ["owner", "manager", "registrant"] {
        assert!(
            data.get(field).is_none_or(Value::is_null),
            "{field} must be absent for a released name: {payload}"
        );
    }
    assert_eq!(data.get("expires_at"), Some(&json!("2026-03-11T23:59:54Z")));
    assert_eq!(
        data.get("lapsed_registration"),
        Some(&json!({
            "registrant":V2_PERMISSIONS_OTHER_SUBJECT,"held_through":"registrar","released_at":"2026-06-09T23:59:55Z"
        }))
    );
    assert!(data.get("resolver").is_none(), "{payload}");
    assert!(data.get("records").is_none(), "{payload}");
    assert!(data.get("primary_address").is_none(), "{payload}");
    database.cleanup().await
}

/// A live `.eth` lease under the registry-only binding a transfer without `reclaim` opened,
/// after its token changed hands again without `reclaim`: Project serves the registry owner the
/// handoff left behind as the name's control and the token's latest holder as its registrant,
/// with the lease's own identity and dates. The API then serves owner and registrant as two
/// different addresses, with the registry-only shape's `registered` status.
#[tokio::test]
async fn v2_get_name_serves_a_transferred_lease_under_the_registry_only_binding() -> Result<()> {
    const REGISTRY_OWNER: &str = V2_PERMISSIONS_SUBJECT;
    const LATER_HOLDER: &str = "0x00000000000000000000000000000000000000cc";
    let database = TestDatabase::new_migrated().await?;
    let (lease, _) = seed_handed_off_lease_inputs(&database, "alice.eth", false).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    let transfer = permission_fixture_event(
        "name-second-handoff",
        Some(&logical),
        Some(lease),
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        120,
        0,
        json!({"source_event":"Transfer","namehash":bigname_lookup::ens_namehash_hex("alice.eth")?,
            "from":V2_PERMISSIONS_OTHER_SUBJECT,"to":LATER_HOLDER}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[transfer]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    let payload = v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth").await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("ok")), "{payload}");
    assert_eq!(
        data.get("registration_status"),
        Some(&json!("registered")),
        "{payload}"
    );
    assert_eq!(data.get("owner"), Some(&json!(REGISTRY_OWNER)), "{payload}");
    assert_eq!(
        data.get("registrant"),
        Some(&json!(LATER_HOLDER)),
        "{payload}"
    );
    assert_eq!(
        data.get("registered_at"),
        Some(&json!("2026-06-09T23:59:31+00:00")),
        "{payload}"
    );
    assert_eq!(
        data.get("expires_at"),
        Some(&json!("2030-03-17T17:46:40Z")),
        "{payload}"
    );
    assert!(data.get("manager").is_none(), "{payload}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_withholds_old_record_observations_after_release() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_state_inputs(&database, AliceInputState::Released).await?;
    let retained: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind='RecordChanged'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert!(
        retained > 0,
        "release does not remove the former resolver's observations"
    );
    let payload = v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth").await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("registration_status"), Some(&json!("released")));
    assert!(data.get("resolver").is_none());
    assert!(data.get("records").is_none());
    assert!(data.get("primary_address").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_withholds_expired_resource_identity_and_inventory_for_reservation() -> Result<()>
{
    // Model a reservation-selected row with inventory retained for the expired
    // resource. A reservation has no current registration.
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Reserved).await?;

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("registration_status"), Some(&json!("unregistered")));
    assert!(data.get("registration_id").is_none());
    assert!(data.get("resolver").is_none());
    assert!(data.get("records").is_none());
    assert!(data.get("primary_address").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_withholds_retained_inventory_for_reservation() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth/records?include=inventory", AliceInputState::Reserved).await?;

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("resolver"), Some(&Value::Null));
    assert_eq!(data.get("records"), Some(&json!({})));
    assert!(data.get("addresses").is_none());
    assert!(data.get("inventory").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_ignores_reservation_audit_selectors_on_every_source() -> Result<()> {
    for source in ["indexed", "auto", "verified"] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_state_inputs(&database, AliceInputState::Reserved).await?;
        let writes = (0..=200)
            .map(|index| {
                family_fixture_record_write(
                    &format!("text:audit-{index}"),
                    Some(json!("retained audit value")),
                )
            })
            .collect::<Vec<_>>();
        insert_family_fixture_record_writes(
            &database.pool,
            "ens",
            "ethereum-mainnet",
            "alice.eth",
            "0x0000000000000000000000000000000000000abc",
            21_000_003,
            "0xbinding",
            &writes,
        )
        .await?;
        rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding")
            .await?;
        let payload = v2_name_record_payload_for_database(
            &database,
            &format!("/v1/names/Alice.eth/records?source={source}&include=inventory"),
        )
        .await?;
        database.cleanup().await?;

        // Retained audit selectors are not served inventory: no default keys and no 422, even
        // above the 200-key limit.
        let data = payload["data"].as_object().expect("data must be an object");
        assert_eq!(data.get("resolver"), Some(&Value::Null), "{source}");
        assert_eq!(data.get("records"), Some(&json!({})), "{source}");
        assert!(data.get("addresses").is_none(), "{source}");
        assert!(data.get("inventory").is_none(), "{source}");
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_keeps_empty_records_for_active_name_on_every_source() -> Result<()> {
    for source in ["indexed", "auto", "verified"] {
        let payload = v2_name_records_payload_with_writes(
            &format!("/v1/names/Alice.eth/records?source={source}"), &[]).await?;

        assert_eq!(payload["data"]["records"], json!({}), "{source}");
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_withholds_retained_inventory_for_released_tombstone()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_verified_inputs(
        &database,
        AliceInputState::Released,
        &[family_fixture_record_write(
            "addr:60",
            Some(json!("0x0000000000000000000000000000000000000def")),
        )],
    )
    .await?;
    let retained: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind='RecordChanged'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert!(retained > 0);
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
            "0x0000000000000000000000000000000000000e0e",
        )])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 verified released-tombstone name profile request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let retained_payload: Value = read_json(response).await?;

    assert_eq!(retained_payload["meta"]["source"], json!("verified"));
    let data = retained_payload["data"]
        .as_object()
        .expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("unsupported")));
    assert_eq!(
        data.get("unsupported_reason"),
        Some(&json!("verified_records_not_supported"))
    );
    assert_eq!(data.get("registration_status"), Some(&json!("released")));
    assert_eq!(
        data.get("unsupported_fields"),
        Some(&json!(["primary_address"]))
    );
    assert!(data.get("resolver").is_none());
    assert!(data.get("records").is_none());
    assert!(data.get("primary_address").is_none());

    // The mock queue still holds its one response: any dispatch would have
    // consumed it and finished the task with a recorded request.
    rpc_handle.abort();
    let dispatched = match rpc_handle.await {
        Err(join_error) if join_error.is_cancelled() => Vec::new(),
        other => other.context("mock primary-name RPC task failed")??,
    };
    assert!(
        dispatched.is_empty(),
        "released tombstone must not dispatch a verified lookup: {dispatched:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_no_binding_as_unregistered() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Unbound).await?;

    assert_eq!(payload["data"]["registration_status"], json!("unregistered"));
    assert_eq!(payload["data"]["registration_id"], Value::Null);

    Ok(())
}

#[tokio::test]
async fn v2_get_name_rejects_source_auto() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/alice.eth?source=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 source=auto request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("source must be one of: indexed, verified")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_infers_exact_base_eth_as_ens() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unbound_name_inputs(&database, "base.eth", false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 base.eth name record request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["name"], json!("base.eth"));
    assert_eq!(payload["data"]["namespace"], json!("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_rejects_trailing_dot() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/alice.eth.")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 trailing-dot name record request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_uses_sepolia_positioned_at_token_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let at = v2_sepolia_snapshot_token();
    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={at}"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["11155111"],
        json!({
            "block_number": V2_SEPOLIA_SNAPSHOT_BLOCK,
            "block_hash": V2_SEPOLIA_SNAPSHOT_HASH,
            "timestamp": V2_SEPOLIA_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum-sepolia"));
    assert_eq!(payload["data"]["chain_id"], json!(11155111));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_at_tokens_round_trip_mainnet_and_sepolia_profiles() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let mainnet = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;
    let mainnet_at =
        v2_at_token_from_meta_as_of(&mainnet, "1", "ethereum", "ethereum-mainnet")?;
    let mainnet_replay = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_MAINNET_SNAPSHOT_NAME}?at={mainnet_at}"),
    )
    .await?;
    assert_eq!(mainnet_replay["meta"]["as_of"], mainnet["meta"]["as_of"]);
    assert_eq!(mainnet_replay["data"], mainnet["data"]);

    let sepolia_at = v2_sepolia_snapshot_token();
    let sepolia = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={sepolia_at}"),
    )
    .await?;
    let sepolia_replay_at = v2_at_token_from_meta_as_of(
        &sepolia,
        "11155111",
        "ethereum-sepolia",
        "ethereum-sepolia",
    )?;
    let sepolia_replay = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={sepolia_replay_at}"),
    )
    .await?;
    assert_eq!(sepolia_replay["meta"]["as_of"], sepolia["meta"]["as_of"]);
    assert_eq!(sepolia_replay["data"], sepolia["data"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_without_at_keeps_mainnet_preference_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": V2_MAINNET_SNAPSHOT_BLOCK,
            "block_hash": V2_MAINNET_SNAPSHOT_HASH,
            "timestamp": V2_MAINNET_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("11155111").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum"));
    assert_eq!(payload["data"]["chain_id"], json!(1));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_uses_phase_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;
    assert_eq!(payload["meta"]["as_of"]["1"]["block_hash"], V2_MAINNET_SNAPSHOT_HASH);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_timestamp_at_uses_sepolia_when_only_sepolia_phase_head_exists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_only_phase_head_name(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}?at=2026-04-17T00:10:30Z"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["11155111"],
        json!({
            "block_number": V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
            "block_hash": V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
            "timestamp": V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum-sepolia"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_returns_indexed_values_for_the_default_key_set() -> Result<()> {
    let payload = v2_name_records_payload("/v1/names/Alice.eth/records").await?;

    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(payload["data"]["namespace"], json!("ens"));
    assert_eq!(
        payload["data"]["resolver"],
        json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        })
    );
    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": {"status": "ok", "value": "0x0000000000000000000000000000000000000def"},
            "text:avatar": {"status": "ok", "value": "https://example.test/avatar.png"},
            "contenthash": {"status": "ok", "value": "ipfs://alice"},
            "text:description": {"status": "ok", "value": "Alice profile"}
        })
    );
    for field in ["addresses", "text_records", "content_hash"] {
        assert!(payload["data"].get(field).is_none(), "{field}: {payload}");
    }

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_keys_filter_values_and_per_key_answers() -> Result<()> {
    let payload =
        v2_name_records_payload("/v1/names/Alice.eth/records?keys=addr:60,text:description")
            .await?;

    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            "text:description": {
                "status": "ok",
                "value": "Alice profile"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_flattens_projected_byte_address_values() -> Result<()> {
    let payload = v2_name_records_payload_with_writes(
        "/v1/names/Alice.eth/records?keys=addr:0",
        &[family_fixture_record_write("addr:0", Some(json!({"encoding":"hex","bytes":"0x001122"})))],
    )
    .await?;

    assert_eq!(
        payload["data"]["records"]["addr:0"],
        json!({
            "status": "ok",
            "value": "0x001122"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_records_and_name_detail_derive_ensip19_default_addresses() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    replace_alice_record_inputs(&database, &[
        family_fixture_record_write("addr:2147483648", Some(json!("0x0000000000000000000000000000000000000DeF"))),
        family_fixture_record_write("addr:2147483649", Some(json!("0x"))),
    ]).await?;
    enable_alice_ensip19_inputs(&database).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unavailable_rpc_url}"),
        ])?)
        .await?;

    for uri in [
        "/v1/names/Alice.eth/records?source=indexed&keys=addr:2147483649",
        "/v1/names/Alice.eth/records?source=auto&keys=addr:2147483649",
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["meta"]["source"], "indexed");
        assert_eq!(
            payload["data"]["records"]["addr:2147483649"],
            json!({
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def",
                "meta": {
                    "basis": "derived",
                    "rule": "ensip19_default_address",
                    "source_record_key": "addr:2147483648"
                }
            })
        );
    }

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=indexed")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let payload: Value = read_json(response).await?;
    // The derived default answers `primary_address`; `records` holds the written keys only.
    assert_eq!(
        payload["data"]["records"]["addresses"],
        json!({"2147483648": "0x0000000000000000000000000000000000000DeF", "2147483649": null}),
        "{payload}"
    );
    assert_eq!(
        payload["data"]["primary_address"],
        "0x0000000000000000000000000000000000000def"
    );

    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "alice", "name": "Alice.eth"}]}),
    )
    .await?;
    assert_eq!(
        lookup["data"][0]["record"]["records"],
        payload["data"]["records"],
        "{lookup}"
    );
    assert_eq!(
        lookup["data"][0]["record"]["primary_address"],
        "0x0000000000000000000000000000000000000def"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_ensip19_zero_default_matches_each_requested_getter() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    replace_alice_record_inputs(&database, &[family_fixture_record_write("addr:2147483648", Some(json!("0x0000000000000000000000000000000000000000")))]).await?;
    enable_alice_ensip19_inputs(&database).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unavailable_rpc_url}"),
        ])?)
        .await?;

    let expected_records = json!({
        "addr:60": {
            "status": "not_found",
            "meta": {
                "basis": "derived",
                "rule": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }
        },
        "addr:2147483649": {
            "status": "ok",
            "value": "0x0000000000000000000000000000000000000000",
            "meta": {
                "basis": "derived",
                "rule": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }
        }
    });
    for source in ["indexed", "auto"] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/names/Alice.eth/records?source={source}&keys=addr:60,addr:2147483649"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["meta"]["source"], "indexed");
        assert_eq!(payload["data"]["records"], expected_records);
    }

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=indexed")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let payload: Value = read_json(response).await?;
    assert!(payload["data"]["records"]["addresses"].get("60").is_none());
    assert!(payload["data"].get("primary_address").is_none());

    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "alice", "name": "Alice.eth"}]}),
    )
    .await?;
    assert!(lookup["data"][0]["record"]["records"]["addresses"]
        .get("60")
        .is_none());
    assert!(lookup["data"][0]["record"]
        .get("primary_address")
        .is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_indexed_records_do_not_derive_for_unflagged_resolvers() -> Result<()> {
    let payload = v2_name_records_payload_with_writes(
        "/v1/names/Alice.eth/records?source=indexed&keys=addr:2147483649",
        &[family_fixture_record_write("addr:2147483648", Some(json!("0x0000000000000000000000000000000000000def")))],
    )
    .await?;
    assert_eq!(
        payload["data"]["records"]["addr:2147483649"],
        json!({"status":"not_found"})
    );
    Ok(())
}

#[tokio::test]
async fn v2_pre_surface_recovered_record_is_authoritative_for_profile_and_records() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs_with_writes(&database, &[]).await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xpre-surface",
            None,
            21_000_000,
            1_640_995_200,
        )],
    )
    .await?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "alice.eth",
        "0x0000000000000000000000000000000000000abc",
        21_000_000,
        "0xpre-surface",
        &[family_fixture_record_write(
            "text:pre-surface",
            Some(json!("recovered before the name surface")),
        )],
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;

    // A closed RPC endpoint makes any accidental verified fallback fail the request.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unavailable_rpc_url}"),
        ])?)
        .await?;

    let mut payloads = Vec::new();
    for uri in [
        "/v1/names/Alice.eth",
        "/v1/names/Alice.eth/records?source=indexed&keys=text:pre-surface",
        "/v1/names/Alice.eth/records?source=auto&keys=text:pre-surface&include=inventory",
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("pre-surface recovered record request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
        payloads.push(payload);
    }

    assert_eq!(payloads[0]["meta"]["source"], json!("indexed"));
    assert_eq!(
        payloads[0]["data"]["records"]["texts"]["pre-surface"],
        json!("recovered before the name surface")
    );
    assert!(payloads[0]["data"].get("unsupported_fields").is_none());
    for payload in &payloads[1..] {
        assert_eq!(payload["meta"]["source"], json!("indexed"));
        assert_eq!(
            payload["data"]["records"]["text:pre-surface"],
            json!({
                "status": "ok",
                "value": "recovered before the name surface"
            })
        );
    }
    assert_eq!(
        payloads[2]["data"]["inventory"],
        json!({
            "known_keys": ["text:pre-surface"],
            "unset_keys": [],
            "unsupported_keys": [],
            "abi_content_types": []
        })
    );
    let divergence_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
        .fetch_one(&database.lookup_pool)
        .await?;
    assert_eq!(divergence_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn v2_ownerless_event_linked_resolver_serves_indexed_records() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_state_inputs(&database, AliceInputState::Ownerless).await?;

    for uri in [
        "/v1/names/Alice.eth",
        "/v1/names/Alice.eth/records?source=indexed&keys=text:description",
        "/v1/names/Alice.eth/records?source=auto&keys=text:description&include=inventory",
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("ownerless resolver request failed")?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = read_json(response).await?;
        assert_eq!(
            payload["data"]["resolver"]["address"],
            json!("0x0000000000000000000000000000000000000abc")
        );
        assert_eq!(payload["data"]["registration_id"], Value::Null);
        if uri.contains("/records") {
            assert_eq!(payload["meta"]["source"], json!("indexed"));
            assert_eq!(
                payload["data"]["records"]["text:description"],
                json!({"status":"ok","value":"Alice profile"})
            );
            assert_ne!(
                payload["data"]["records"]["text:description"]["unsupported_reason"],
                json!("inventory_not_available")
            );
        } else {
            assert_eq!(
                payload["data"]["registration_status"],
                json!("unregistered")
            );
            assert!(
                payload["data"].get("token_id").is_none(),
                "ownerless exact-name payload must not imply token control: {payload}"
            );
            assert_eq!(
                payload["data"]["records"]["texts"]["description"],
                json!("Alice profile"),
                "ownerless exact-name payload: {payload}"
            );
        }
    }

    let lookup = v2_lookup_json(
        &database,
        json!({"profile":"detail","inputs":[{"id":"ownerless","name":"alice.eth"}]}),
    )
    .await?;
    let lookup_record = &lookup["data"][0]["record"];
    assert_eq!(lookup_record["registration_status"], json!("unregistered"));
    assert!(
        lookup_record.get("token_id").is_none(),
        "ownerless batch lookup must not imply token control: {lookup}"
    );
    assert_eq!(
        lookup_record["resolver"]["address"],
        json!("0x0000000000000000000000000000000000000abc")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_unclassified_serving_resource_does_not_expose_retained_records() -> Result<()> {
    let payload = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Unbound).await?;

    assert_eq!(
        payload["data"]["registration_status"],
        json!("unregistered")
    );
    assert!(payload["data"].get("resolver").is_none());
    assert!(payload["data"].get("records").is_none());

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_rejects_too_many_keys() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let keys = (0..=200)
        .map(|index| format!("text:key{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let uri = format!("/v1/names/alice.eth/records?keys={keys}");

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 oversized records keys request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("keys must contain at most 200 record keys")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_reports_unset_and_unsupported_per_key() -> Result<()> {
    let payload = v2_name_records_payload_with_writes(
        "/v1/names/Alice.eth/records?keys=contenthash,text:email",
        &[family_fixture_record_write("text:email", None)],
    )
    .await?;

    assert_eq!(
        payload["data"]["records"],
        json!({
            "contenthash": {
                "status": "not_found"
            },
            "text:email": {
                "status": "unsupported",
                "unsupported_reason": "value_not_retained"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_include_inventory_uses_product_key_lists() -> Result<()> {
    let payload = v2_name_records_payload_with_writes(
        "/v1/names/Alice.eth/records?keys=contenthash,text:email&include=inventory",
        &[family_fixture_record_write("addr:60", Some(json!("0x0000000000000000000000000000000000000def"))), family_fixture_record_write("avatar", Some(json!("https://example.test/avatar.png"))), family_fixture_record_write("text:email", None)],
    )
    .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": ["addr:60", "text:avatar"],
            "unset_keys": [],
            "unsupported_keys": ["text:email"],
            "abi_content_types": []
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_inventory_partitions_unsupported_entries() -> Result<()> {
    let payload = v2_name_records_payload_with_writes(
        "/v1/names/Alice.eth/records?keys=addr:60,avatar&include=inventory",
        &[family_fixture_record_write("addr:60", Some(json!("0x0000000000000000000000000000000000000def"))), family_fixture_record_write("avatar", None)],
    )
    .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": ["addr:60"],
            "unset_keys": [],
            "unsupported_keys": ["text:avatar"],
            "abi_content_types": []
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_inventory_absence_is_unknown_not_unsupported() -> Result<()> {
    let payload =
        v2_name_payload_without_inventory("/v1/names/Alice.eth/records?keys=addr:60&include=inventory")
            .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": [],
            "unset_keys": [],
            "unsupported_keys": [],
            "abi_content_types": null,
            "abi_unsupported_reason": "inventory_not_available"
        })
    );

    Ok(())
}

/// A row the projection left without a topology (no admitted absent-topology route either) is
/// outside every verified class and reports `verified_records_not_supported`; the arm refusal
/// `exact_name_authority_not_verifiable` is reserved for a topology-bearing row whose selected
/// arm the execution declaration does not admit.
#[tokio::test]
async fn v2_get_name_records_source_verified_reports_unsupported_without_lookup_topology()
-> Result<()> {
    let payload =
        v2_name_records_payload("/v1/names/Alice.eth/records?source=verified&keys=addr:60").await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "unsupported",
            "unsupported_reason": "verified_records_not_supported"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_withholds_unproven_authority_without_verified_lookup() -> Result<()> {
    for source in ["indexed", "verified", "auto"] {
        let payload = v2_alice_state_payload(
            &format!("/v1/names/Alice.eth/records?source={source}&keys=addr:60"),
            AliceInputState::Unbound,
        )
        .await?;
        assert_eq!(payload["data"]["resolver"], Value::Null);
        assert_eq!(
            payload["data"]["records"]["addr:60"],
            json!({
                "status":"unsupported", "unsupported_reason":"inventory_not_available"
            })
        );
        assert_eq!(
            payload["meta"]["source"],
            if source == "verified" {
                "verified"
            } else {
                "indexed"
            }
        );
    }

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_classifies_a_root_registry_pointer_through_its_inventory() -> Result<()> {
    for source in ["indexed", "auto"] {
        let payload = v2_root_pointer_payload(
            &format!("/v1/names/eth/records?source={source}&keys=addr:60,text:description"), true).await?;
        assert_eq!(
            payload["data"]["resolver"],
            json!({
                "chain_id": 1,
                "address": "0x0000000000000000000000000000000000000abc"
            }),
            "{source}: {payload}"
        );
        assert_eq!(
            payload["data"]["records"]["addr:60"],
            json!({
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            }),
            "{source}: {payload}"
        );
        assert_eq!(payload["meta"]["source"], json!("indexed"), "{source}");
    }
    Ok(())
}

#[tokio::test]
async fn v2_verified_name_reads_reject_oversized_inventory_derived_selector_sets() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    let writes = (0..=crate::v2::MAX_PAGE_SIZE).map(|index|
        family_fixture_record_write(&format!("text:key-{index}"), None)).collect::<Vec<_>>();
    replace_alice_record_inputs(&database, &writes).await?;
    let state = database.app_state();

    for uri in [
        "/v1/names/Alice.eth/records?source=verified",
        "/v1/names/Alice.eth?source=verified",
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("oversized inventory-derived verified request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{payload}");
        assert_eq!(payload["error"]["code"], json!("unsupported"));
        assert_eq!(
            payload["error"]["message"],
            json!("inventory-derived record key sets support at most 200 record keys")
        );
    }

    let narrowed = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=verified&keys=text:key-0")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("narrowed verified records request failed")?;
    assert_eq!(narrowed.status(), StatusCode::OK);

    database.cleanup().await?;
    Ok(())
}

async fn seed_base_indexed_name_inputs(
    database: &TestDatabase,
    resource: Uuid,
    address: &str,
) -> Result<()> {
    database.seed_snapshot_selector_chain_positions(&json!({"base":{
        "chain_id":"base-mainnet","block_number":21000003,"block_hash":"0xbase-binding","timestamp":"2026-04-17T00:00:03Z"
    }})).await?;
    seed_record_lookup_inputs(
        &database.pool,
        "base-mainnet",
        "basenames",
        "alice.base.eth",
        resource,
        Uuid::from_u128(resource.as_u128() + 1),
        21000003,
        "0xbase-binding",
        "2026-04-17T00:00:03Z",
        address,
    )
    .await?;
    Ok(())
}



#[tokio::test]
async fn v2_get_basenames_records_source_auto_stays_base_scoped_without_fallback() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let indexed_address = "0x0000000000000000000000000000000000000def";
    seed_base_indexed_name_inputs(&database, Uuid::from_u128(0x9250), indexed_address).await?;

    for uri in [
        "/v1/names/alice.base.eth/records?source=auto",
        "/v1/names/alice.base.eth/records?source=auto&keys=addr:60",
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 Base-only auto records request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected response for {uri}: {payload}"
        );
        assert_eq!(payload["meta"]["source"], json!("indexed"));
        assert!(payload["meta"]["as_of"].get("1").is_none());
        assert_eq!(
            payload["meta"]["as_of"]["8453"]["block_hash"],
            json!("0xbase-binding")
        );
        assert_eq!(
            payload["data"]["records"]["addr:60"],
            json!({"status": "ok", "value": indexed_address})
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_records_source_auto_retries_when_fallback_disappears_during_reselection()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let resource_id = Uuid::from_u128(0x9260);
    seed_v2_basenames_auto_transition_fixture(&database, resource_id).await?;
    let (_guard, control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/names/alice.base.eth/records?source=auto&keys=addr:60")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    append_name_resolver_input(
        &database,
        "basenames",
        "alice.base.eth",
        "0x1000000000000000000000000000000000000001",
    )
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("v2 auto fallback transition request task panicked")?
        .context("v2 auto fallback transition request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unexpected response: {payload}"
    );
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await?;
    Ok(())
}

async fn seed_v2_basenames_auto_transition_fixture(
    database: &TestDatabase,
    resource_id: Uuid,
) -> Result<()> {
    seed_base_indexed_name_inputs(
        database,
        resource_id,
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database.seed_snapshot_selector_chain_positions(&json!({
        "base":{"chain_id":"base-mainnet","block_number":21000003,"block_hash":"0xbase-binding","timestamp":"2026-04-17T00:00:03Z"},
        "ethereum":{"chain_id":"ethereum-mainnet","block_number":21000003,"block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03Z"}
    })).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21000003, "0xbinding").await?;
    seed_basenames_auto_fallback_requiring_inventory(database).await
}

#[tokio::test]
async fn v2_get_basenames_records_source_auto_retries_when_authority_reclassifies_during_reselection()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    let _namehash = seed_schema_v2_basenames_record_lookup(
        &lookup_pool,
        21_000_003,
        "0xbase-binding",
        "0xbinding",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }))
        .await?;
    seed_basenames_auto_fallback_requiring_inventory(&database).await?;

    let (_guard, control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_basenames_l1_addr60_response(
            "0x0000000000000000000000000000000000000e0e",
        )])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/names/alice.base.eth/records?source=auto&keys=addr:60")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        control.wait_until_reached(),
    )
    .await
    .context("auto fallback preparation must be reached")?;
    database.seed_snapshot_selector_chain_positions(&json!({"base":{
        "chain_id":"base-mainnet","block_number":21000004,"block_hash":"0xbase-release","timestamp":"2026-04-17T00:00:04Z"
    }})).await?;
    let logical = bigname_storage::logical_name_id_for_name("basenames", "alice.base.eth");
    let resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings WHERE logical_name_id=$1 AND active_to IS NULL",
    )
    .bind(&logical)
    .fetch_one(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to=to_timestamp(1776384004) WHERE resource_id=$1",
    )
    .bind(resource)
    .execute(&database.pool)
    .await?;
    let mut release = history_event(
        "base-release",
        Some(&logical),
        Some(resource),
        Some("base-mainnet"),
        Some(21000004),
        Some("0xbase-release"),
        Some("0xrelease"),
        Some(10000),
        CanonicalityState::Canonical,
    );
    release.namespace = "basenames".into();
    release.event_kind = "RegistrationReleased".into();
    release.source_family = "basenames_base_registrar".into();
    release.before_state = json!({});
    release.after_state = json!({"expiry":1700000000,"released_at":1776384004});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[release]).await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 21000004, "0xbase-release").await?;
    let released = bigname_storage::load_name_current(&database.pool, &logical)
        .await?
        .context("released Base name")?;
    assert!(released.surface_binding_id.is_none(), "{released:?}");
    control.resume().await;

    let response = request_task
        .await
        .context("v2 auto fallback authority transition request task panicked")?
        .context("v2 auto fallback authority transition request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unexpected response: {payload}"
    );
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(
        payload["error"]["message"],
        json!("name records changed while preparing verified fallback; retry the request")
    );

    // The mock queue still holds its one response: any dispatch would have
    // consumed it and finished the task with a recorded request.
    rpc_handle.abort();
    let dispatched = match rpc_handle.await {
        Err(join_error) if join_error.is_cancelled() => Vec::new(),
        other => other.context("mock primary-name RPC task failed")??,
    };
    assert!(
        dispatched.is_empty(),
        "authority reclassification during auto-fallback reselection must not dispatch a verified lookup: {dispatched:?}"
    );

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_records_source_auto_executes_verified_fallback_after_reselection()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    let _namehash = seed_schema_v2_basenames_record_lookup(
        &lookup_pool,
        21_000_003,
        "0xbase-binding",
        "0xbinding",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }))
        .await?;
    for source in ["indexed", "auto"] {
        let indexed = v2_name_record_payload_for_database(
            &database,
            &format!("/v1/names/alice.base.eth/records?source={source}&keys=addr:60"),
        )
        .await?;
        assert_eq!(indexed["meta"]["source"], json!("indexed"));
        assert!(indexed["meta"]["as_of"].get("1").is_none(), "{indexed}");
        assert_eq!(
            indexed["data"]["records"]["addr:60"]["value"],
            json!("0x0000000000000000000000000000000000000def")
        );
    }
    seed_basenames_auto_fallback_requiring_inventory(&database).await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_basenames_l1_addr60_response(
            executed_address,
        )])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/alice.base.eth/records?source=auto&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 auto fallback verified execution request failed")?;

    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

/// A registry pointer to an undeclared resolver makes explicit auto use the admitted L1 lookup.
async fn seed_basenames_auto_fallback_requiring_inventory(database: &TestDatabase) -> Result<()> {
    append_name_resolver_input(
        database,
        "basenames",
        "alice.base.eth",
        "0x000000000000000000000000000000000000fafa",
    )
    .await
}

#[tokio::test]
async fn v2_get_name_records_source_verified_executes_basenames_with_auxiliary_position()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-mainnet","block_number":21000002,
            "block_hash":"0xearlier-binding","timestamp":"2026-04-17T00:00:02Z"
        }}))
        .await?;
    let _namehash = seed_schema_v2_basenames_record_lookup(
        &lookup_pool,
        21_000_003,
        "0xbase-binding",
        "0xbinding",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xnewer-binding",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        }))
        .await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_basenames_l1_addr60_response(
            executed_address,
        )])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/alice.base.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 non-ENS on-demand verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["meta"]["as_of"]["1"]["block_hash"],
        json!("0xbinding"),
        "Basenames verified response metadata must expose the row's actual execution position"
    );
    assert_eq!(
        payload["meta"]["as_of"]["1"]["block_number"],
        json!(21_000_003)
    );
    assert_eq!(
        payload["meta"]["as_of"]["8453"]["block_hash"],
        json!("0xbase-binding")
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    assert_eq!(
        rpc_requests[0]["params"][1],
        json!({
            "blockHash": "0xbinding",
            "requireCanonical": true
        })
    );
    let ledger_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL")
            .fetch_one(&lookup_pool)
            .await?;
    assert_eq!(ledger_count, 1);

    // An explicitly selected older auxiliary block cannot use today's composed execution route.
    let old_auxiliary = bigname_storage::SelectedSnapshot {
        chain_positions: bigname_storage::ChainPositions::from_value(&json!({
            "base":{"chain_id":"base-mainnet","block_number":21000003,"block_hash":"0xbase-binding","timestamp":"2026-04-17T00:00:03Z"},
            "ethereum":{"chain_id":"ethereum-mainnet","block_number":21000002,"block_hash":"0xearlier-binding","timestamp":"2026-04-17T00:00:02Z"}
        }))?,
        consistency: bigname_storage::SnapshotConsistency::Head,
    };
    let token = crate::v2::encode_at_token(&old_auxiliary);
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/alice.base.eth/records?source=verified&keys=addr:60&at={token}"
                ))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reports_unsupported_without_verified_boundary()
-> Result<()> {
    let payload = v2_alice_state_payload(
        "/v1/names/Alice.eth/records?source=verified&keys=avatar",
        AliceInputState::Ownerless,
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["avatar"],
        json!({
            "status": "unsupported",
            "unsupported_reason": "verified_records_not_supported"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_auto_blends_indexed_and_verified_per_key() -> Result<()> {
    let payload = v2_name_records_payload_with_writes(
        "/v1/names/Alice.eth/records?source=auto&keys=addr:60,text:email",
        &[
            family_fixture_record_write(
                "addr:60",
                Some(json!("0x0000000000000000000000000000000000000def")),
            ),
            family_fixture_record_write("text:email", None),
        ],
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            "text:email": {
                "status": "unsupported",
                "unsupported_reason": "verified_records_not_supported"
            }
        })
    );

    Ok(())
}

async fn seed_unknown_resolver_inputs(database: &TestDatabase, writes: &[Value]) -> Result<()> {
    const RESOLVER: &str = "0x0000000000000000000000000000000000000abc";
    const CHAIN: &str = "ethereum-mainnet";
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;
    let resource = Uuid::from_u128(0x2200);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        "alice.eth",
        CHAIN,
        21_000_003,
        "0xbinding",
        resource,
        Uuid::from_u128(0x1100),
        Uuid::from_u128(0x3300),
        "ens_v1",
    )
    .await?;
    // A dynamic resolver family is known, but no retained upgrade identifies its implementation.
    let manifest_payload = json!({"contracts":[],"resolver_implementations":[]});
    let manifest:i64=sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id, deployment_label, rollout_status, normalizer_version, file_path, manifest_payload) VALUES (1,'ens','ens_v2_resolver_l1',$1,'api-test','active','test','test/ens/dynamic-resolver.toml',$2) RETURNING manifest_id")
        .bind(CHAIN).bind(&manifest_payload).fetch_one(&database.pool).await?;
    seed_fixture_manifest_update(
        &database.pool,
        manifest,
        CHAIN,
        "ens",
        "ens_v2_resolver_l1",
        &manifest_payload,
    )
    .await?;
    let origin = Uuid::from_u128(0xad01);
    let resolver_instance = Uuid::from_u128(0xad02);
    seed_schema_v2_ens_manifest(
        &database.pool,
        "ens_v2_registry_l1",
        "registry",
        "0x000000000000000000000000000000000000ad01",
        origin,
        false,
    )
    .await?;
    let origin_manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family='ens_v2_registry_l1'",
    )
    .fetch_one(&database.pool)
    .await?;
    sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1,$2,'contract')").bind(resolver_instance).bind(CHAIN).execute(&database.pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,source_manifest_id,active_from_block_number,active_from_block_hash) VALUES ($1,$2,$3,$4,21000003,'0xbinding')").bind(resolver_instance).bind(CHAIN).bind(RESOLVER).bind(origin_manifest).execute(&database.pool).await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state) VALUES ($1,'resolver',$2,$3,'ResolverChanged','registry_pointer',$4,21000003,'0xbinding','canonical')").bind(CHAIN).bind(origin).bind(resolver_instance).bind(origin_manifest).execute(&database.pool).await?;
    let node = bigname_lookup::ens_namehash_hex("alice.eth")?;
    let mut facts = vec![
        (
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(logical.as_str()),
            Some(resource),
            json!({"authority_kind":"registrar","registrant":V2_ADDRESS,"expiry":1900000000}),
        ),
        (
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            Some(logical.as_str()),
            Some(resource),
            json!({"source_event":"Transfer","node":node,"owner":V2_ADDRESS}),
        ),
        (
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(logical.as_str()),
            Some(resource),
            json!({"node":node,"resolver":RESOLVER}),
        ),
    ];
    facts.push(("ResolverRecordLinked","ens_v2_resolver_l1",None,None,json!({
        "source_event":"Linked","storage_model":"resolver_record_id","resolver":RESOLVER,
        "resolver_contract_instance_id":resolver_instance.to_string(),"resolver_record_id":"1","node":node
    })));
    for write in writes {
        let mut after = write.clone();
        after["resolver"] = json!(RESOLVER);
        after["storage_model"] = json!("resolver_record_id");
        after["resolver_record_id"] = json!("1");
        after["resolver_contract_instance_id"] = json!(resolver_instance.to_string());
        facts.push(("RecordChanged", "ens_v2_resolver_l1", None, None, after));
    }
    let mut events = Vec::new();
    for (index, (kind, family, name, resource, after)) in facts.into_iter().enumerate() {
        let mut event = history_event(
            &format!("unknown-resolver-{index}"),
            name,
            resource,
            Some(CHAIN),
            Some(21_000_003),
            Some("0xbinding"),
            Some("0xunknown-resolver"),
            Some(index as i64),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = family.into();
        event.manifest_version = 1;
        event.source_manifest_id = (family == "ens_v2_resolver_l1").then_some(manifest);
        event.raw_fact_ref = json!({"kind":"raw_log","emitting_address":RESOLVER});
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, CHAIN, 21_000_003, "0xbinding").await
}

fn unknown_resolver_record_writes() -> Vec<Value> {
    vec![
        family_fixture_record_write(
            "addr:60",
            Some(json!("0xfa75ed860000000000000000000000000000abcd")),
        ),
        family_fixture_record_write(
            "text:description",
            Some(json!(
                "retained value behind an unknown resolver implementation"
            )),
        ),
    ]
}

async fn unknown_resolver_payload(uri: &str) -> Result<Value> {
    let database = TestDatabase::new_migrated().await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let body = v2_name_record_payload_for_database(&database, uri).await?;
    database.cleanup().await?;
    Ok(body)
}



#[tokio::test]
async fn v2_get_name_records_withholds_values_from_unsupported_inventory() -> Result<()> {
    let payload = unknown_resolver_payload(
        "/v1/names/Alice.eth/records?keys=addr:60,text:description,avatar&include=inventory",
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("indexed"));
    // The declared registry pointer is registry evidence and stays; the values do not.
    assert_eq!(
        payload["data"]["resolver"],
        json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        })
    );
    let refused = json!({
        "status": "unsupported",
        "unsupported_reason": "resolver_implementation_unknown"
    });
    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": refused,
            "text:description": refused,
            "avatar": refused
        })
    );
    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": [],
            "unset_keys": [],
            "unsupported_keys": ["addr:60", "avatar", "text:description"],
            "abi_content_types": null,
            "abi_unsupported_reason": "inventory_not_authoritative"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_default_set_refuses_unsupported_inventory() -> Result<()> {
    let payload = unknown_resolver_payload("/v1/names/Alice.eth/records").await?;

    // Retained entries are not answers: each default key carries the row's reason, no value.
    let refused = json!({
        "status": "unsupported",
        "unsupported_reason": "resolver_implementation_unknown"
    });
    assert_eq!(
        payload["data"]["records"],
        json!({"addr:60": refused, "text:description": refused})
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_auto_does_not_satisfy_from_unsupported_inventory() -> Result<()>
{
    let payload =
        unknown_resolver_payload("/v1/names/Alice.eth/records?source=auto&keys=addr:60").await?;

    // The retained entry does not satisfy auto; the key goes to verified lookup, which this
    // fixture cannot execute, so no value is served either way.
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"]["status"],
        json!("unsupported")
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"]["unsupported_reason"],
        json!("verified_records_not_supported")
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_withholds_indexed_record_fields_from_unsupported_inventory() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;

    let payload = v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth").await?;

    assert_eq!(payload["data"]["status"], json!("ok"));
    assert_eq!(
        payload["data"]["resolver"],
        json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        })
    );
    // The keys the resolver was seen writing stay listed; none of their values is known.
    let records = &payload["data"]["records"];
    assert_eq!(records["address_keys"], json!(["60"]), "{payload}");
    assert_eq!(records["addresses"], json!({}), "{payload}");
    assert_eq!(records["text_keys"], json!(["description"]), "{payload}");
    assert_eq!(records["texts"], json!({}), "{payload}");
    assert!(payload["data"].get("primary_address").is_none());
    assert_eq!(payload["data"]["unsupported_fields"], json!(["primary_address"]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_missing_name_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/missing.eth/records")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 missing name records request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_response_omits_banned_v1_spellings() -> Result<()> {
    let payload =
        v2_name_records_payload("/v1/names/Alice.eth/records?keys=addr:60&include=inventory")
            .await?;
    assert_no_banned_v1_spellings(&payload);
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_uses_envelope_shape() -> Result<()> {
    let payload = v2_name_records_payload("/v1/names/Alice.eth/records?keys=addr:60").await?;

    assert!(payload.get("page").is_none());
    assert!(payload["data"].is_object());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_returns_record_shaped_rows_in_display_name_order() -> Result<()> {
    let (database, payload) =
        v2_subnames_payload("/v1/names/Parent.eth/subnames?page_size=3").await?;
    let page = bigname_storage::load_children_current_page(
        &database.pool,
        &bigname_storage::logical_name_id_for_name("ens", "parent.eth"),
        None,
        10,
    )
    .await?;
    assert_eq!(
        page.rows
            .iter()
            .find(|row| row.normalized_name == "gamma.parent.eth")
            .and_then(|row| row.owner.as_deref()),
        Some("0x00000000000000000000000000000000000000cc")
    );

    assert_eq!(payload["page"]["page_size"], json!(3));
    assert_eq!(payload["page"]["total_count"], json!(3));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert!(payload["meta"]["as_of"].is_object());

    let data = payload["data"]
        .as_array()
        .expect("subnames data must be an array");
    assert_eq!(data.len(), 3);
    assert_eq!(data[0]["name"], json!("alpha.parent.eth"));
    assert_eq!(data[1]["name"], json!("beta.parent.eth"));
    assert_eq!(data[2]["name"], json!("gamma.parent.eth"));
    assert_eq!(data[0]["display_name"], json!("alpha.parent.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(
        data[0]["namehash"],
        json!(bigname_lookup::ens_namehash_hex("alpha.parent.eth")?)
    );
    assert_eq!(
        data[0]["labelhash"],
        json!(labelhash_for_display_name("alpha.parent.eth"))
    );
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000aa")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000aa")
    );
    assert_eq!(data[0]["registration_status"], json!("registered"));
    assert_eq!(
        parse_rfc3339_utc_timestamp(
            data[0]["registered_at"]
                .as_str()
                .context("registration timestamp")?
        )?,
        parse_rfc3339_utc_timestamp("2024-01-02T03:04:05Z")?
    );
    assert_eq!(
        parse_rfc3339_utc_timestamp(
            data[0]["created_at"]
                .as_str()
                .context("creation timestamp")?
        )?,
        parse_rfc3339_utc_timestamp("2024-01-02T03:04:05Z")?
    );
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T03:04:05Z"));
    assert_eq!(data[1]["registration_status"], json!("unregistered"));
    assert_eq!(data[2]["registration_status"], json!("unregistered"));
    assert!(
        data[2].get("owner").is_none(),
        "a generic no-registration row must not inherit the children projection owner"
    );
    assert!(data[0].get("subname_count").is_none());
    assert!(data[0].get("resolver").is_none());
    assert!(data[0].get("records").is_none());
    assert_no_banned_v1_spellings(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_keeps_zero_owner_for_ownerless_resolver_child() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "gamma.parent.eth");
    let resource = Uuid::from_u128(0x4030);
    let hash = seed_subname_block(&database, 90).await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(resource)
        .execute(&database.pool)
        .await?;
    let mut events = Vec::new();
    for (log, kind, after) in [
        (
            0,
            "AuthorityTransferred",
            json!({"source_event":"Transfer", "node":bigname_lookup::ens_namehash_hex("gamma.parent.eth")?,
            "owner":"0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e", "owner_getter":"0x0000000000000000000000000000000000000000",
            "registry_owner":"0x0000000000000000000000000000000000000000", "owner_getter_reason":"registry_self"}),
        ),
        (
            1,
            "ResolverChanged",
            json!({"source_event":"NewResolver", "node":bigname_lookup::ens_namehash_hex("gamma.parent.eth")?,
            "resolver":"0x0000000000000000000000000000000000000abc"}),
        ),
    ] {
        let mut event = history_event(
            &format!("ownerless-child-{kind}"),
            Some(&logical),
            Some(resource),
            Some("ethereum-mainnet"),
            Some(90),
            Some(&hash),
            Some("0xownerless-child"),
            Some(log),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = "ens_v1_registry_l1".into();
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    declare_family_fixture_resolver(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        "0x0000000000000000000000000000000000000abc",
    )
    .await?;
    seed_subname_edge(
        &database,
        "parent.eth",
        b"gamma",
        "0x0000000000000000000000000000000000000000",
        90,
    )
    .await?;
    publish_subname_inputs(&database).await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?page_size=10")
            .await?;
    let child = payload["data"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["name"] == "gamma.parent.eth"))
        .expect("ownerless child must remain enumerable");
    assert_eq!(
        child["owner"],
        json!("0x0000000000000000000000000000000000000000")
    );
    assert_eq!(child["registrant"], Value::Null);
    assert_eq!(child["registration_status"], json!("unregistered"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_subnames_paginates_with_opaque_cursor_without_overlap() -> Result<()> {
    let (database, first_page) =
        v2_subnames_payload("/v1/names/parent.eth/subnames?page_size=2").await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();
    assert_eq!(first_page["page"]["has_more"], json!(true));

    let second_page = v2_subnames_payload_for_database(
        &database,
        &format!("/v1/names/parent.eth/subnames?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["next_cursor"], Value::Null);
    assert_eq!(second_page["page"]["has_more"], json!(false));
    assert_eq!(
        first_page["data"]
            .as_array()
            .expect("first page data")
            .iter()
            .map(|row| row["name"].as_str().expect("row name"))
            .collect::<Vec<_>>(),
        vec!["alpha.parent.eth", "beta.parent.eth"]
    );
    assert_eq!(
        second_page["data"]
            .as_array()
            .expect("second page data")
            .iter()
            .map(|row| row["name"].as_str().expect("row name"))
            .collect::<Vec<_>>(),
        vec!["gamma.parent.eth"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_uses_current_sepolia_anchor_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;
    let child_name = format!("child.{V2_SEPOLIA_SNAPSHOT_NAME}");
    seed_v2_snapshot_profile_name(
        &database,
        &child_name,
        "Child.Sepolia-Pin.eth",
        "namehash:child.sepolia-pin.eth",
        Uuid::from_u128(0x7e23),
        Uuid::from_u128(0x7e24),
        Uuid::from_u128(0x7e25),
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
        V2_SEPOLIA_SNAPSHOT_TIMESTAMP,
    )
    .await?;
    let label = insert_family_label_preimage(&database.pool, b"child").await?;
    insert_family_registry_child_edge(
        &database.pool,
        "ens",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_NAME,
        &label,
        "0x0000000000000000000000000000000000000001",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
    )
    .await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
    )
    .await?;
    database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-sepolia",
            "ens_v2_sepolia_20260915",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(format!("/v1/names/{V2_SEPOLIA_SNAPSHOT_NAME}/subnames"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload}");
    assert_eq!(
        payload["meta"]["as_of"]["11155111"]["block_number"],
        json!(V2_SEPOLIA_SNAPSHOT_BLOCK)
    );
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert_eq!(payload["data"][0]["name"], json!(child_name));
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_subnames_rejects_cursor_reused_for_different_parent() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    for (name, base, owner) in [
        (
            "other.eth",
            0x7010,
            "0x0000000000000000000000000000000000000002",
        ),
        (
            "one.other.eth",
            0x7020,
            "0x0000000000000000000000000000000000000003",
        ),
    ] {
        seed_subname_inputs(
            &database,
            name,
            85,
            Uuid::from_u128(base),
            Uuid::from_u128(base + 1),
            Uuid::from_u128(base + 2),
            SubnameInput::RegistryOwner(owner),
        )
        .await?;
    }
    seed_subname_edge(
        &database,
        "other.eth",
        b"one",
        "0x0000000000000000000000000000000000000003",
        85,
    )
    .await?;
    publish_subname_inputs(&database).await?;

    let first_page =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?page_size=2")
            .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor");

    let response = v2_subnames_response_for_database(
        &database,
        &format!("/v1/names/other.eth/subnames?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_include_counts_adds_child_subname_count_only_when_requested()
-> Result<()> {
    let (database, without_counts) =
        v2_subnames_payload("/v1/names/parent.eth/subnames?page_size=3").await?;
    assert!(
        without_counts["data"][0].get("subname_count").is_none(),
        "subname_count must be omitted by default"
    );

    let with_counts =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?include=counts")
            .await?;
    assert_eq!(with_counts["data"][0]["subname_count"], json!(1));
    assert_eq!(with_counts["data"][1]["subname_count"], json!(0));
    assert_eq!(with_counts["data"][2]["subname_count"], json!(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_subname_collections_filter_orphaned_observations_and_surfaces() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let parent_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = 'parent.eth'",
    )
    .fetch_one(&database.pool)
    .await?;

    // Rebuild after the beta observation loses canonical lineage. Gamma has a preimage and
    // canonical edge, but its existing orphaned surface must also be withheld.
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = 'ethereum-mainnet' AND block_number = 82")
        .execute(&database.pool).await?;
    sqlx::query(
        "UPDATE name_surfaces SET canonicality_state = 'orphaned' WHERE logical_name_id = $1",
    )
    .bind(bigname_storage::logical_name_id_for_name(
        "ens",
        "gamma.parent.eth",
    ))
    .execute(&database.pool)
    .await?;
    publish_subname_inputs(&database).await?;

    let page = bigname_storage::load_children_current_page(
        &database.pool,
        &parent_logical_name_id,
        None,
        10,
    )
    .await?;
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.normalized_name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha.parent.eth"]
    );
    assert_eq!(page.summary.child_count, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_paginates_across_a_child_with_no_observed_label() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    // A registry edge proves the child node and its labelhash but not the label. Project writes
    // that row with every name column null and no child name surface — the shape most historical
    // labels have — and the page must name it by the documented placeholder rather than decoding
    // a null into a mandatory field.
    seed_v2_subnames_topology_only_child(
        &database,
        "parent.eth",
        "0x00000000000000000000000000000000000000000000000000000000feed0001",
    )
    .await?;

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..5 {
        let uri = match cursor.as_deref() {
            Some(cursor) => format!("/v1/names/parent.eth/subnames?page_size=2&cursor={cursor}"),
            None => "/v1/names/parent.eth/subnames?page_size=2".to_owned(),
        };
        let payload = v2_subnames_payload_for_database(&database, &uri).await?;
        for row in payload["data"].as_array().expect("subnames data") {
            seen.push(
                row["name"]
                    .as_str()
                    .expect("row name must be a string")
                    .to_owned(),
            );
        }
        match payload["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }

    let mut deduped = seen.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        seen.len(),
        "no row may be served twice: {seen:?}"
    );
    assert!(
        seen.contains(
            &"[00000000000000000000000000000000000000000000000000000000feed0001].parent.eth"
                .to_owned()
        ),
        "the unobserved-label child must be named by its placeholder: {seen:?}"
    );
    assert_eq!(
        seen.len(),
        4,
        "every child must be paged exactly once: {seen:?}"
    );

    // A preimage whose label bytes do not decode is stored raw with no decoded form; the read
    // escape-encodes it. It is equally not an addressable name, and equally must not fail the page.
    seed_v2_subnames_undecodable_child(&database, "parent.eth", "0xfeed0002").await?;
    let with_undecodable =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?page_size=20")
            .await?;
    let names = with_undecodable["data"]
        .as_array()
        .expect("subnames data")
        .iter()
        .map(|row| row["name"].as_str().expect("row name").to_owned())
        .collect::<Vec<_>>();
    let escaped_row = with_undecodable["data"]
        .as_array()
        .expect("subnames data")
        .iter()
        .find(|row| row["labelhash"] == format!("{:#x}", alloy_primitives::keccak256(b"\xff\tBad")))
        .unwrap_or_else(|| panic!("an undecodable label must be served, not dropped: {names:?}"));
    assert_eq!(escaped_row["name"], "\\377\tBad.parent.eth");
    assert_eq!(escaped_row["display_name"], "\\377\tBad.parent.eth");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_gates_decoded_text_on_the_normalization_verdict() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    // Project composes the name columns only under a true normalization verdict; a
    // proof-checked label whose text fails keeps its raw bytes in the projection but is
    // written with both name columns null, whether the text normalizes to different bytes
    // ("Alice") or the normalizer errors outright (the ZWJ label).
    seed_v2_subnames_preimage_child(&database, "parent.eth", "alice", true).await?;
    seed_v2_subnames_preimage_child(&database, "parent.eth", "Alice", false).await?;
    seed_v2_subnames_preimage_child(&database, "parent.eth", "Ni\u{200d}ck", false).await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?page_size=20")
            .await?;
    let rows = payload["data"].as_array().expect("subnames data").clone();
    let row_for_label = |label: &str| {
        let labelhash = format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()));
        rows.iter()
            .find(|row| row["labelhash"] == labelhash)
            .unwrap_or_else(|| panic!("child for label {label:?} must be served: {rows:?}"))
    };

    // Verdict true: the decoded name serves, and the served name re-hashes to the served node.
    let decoded = row_for_label("alice");
    assert_eq!(decoded["name"], json!("alice.parent.eth"));
    assert_eq!(decoded["display_name"], json!("alice.parent.eth"));
    assert_eq!(decoded["namehash"], json!(namehash_of("alice.parent.eth")));

    // Verdict false with decodable text: the placeholder serves against the raw-byte node —
    // never the text, which would re-hash to a different node than the one proven on chain.
    let unnormalized = row_for_label("Alice");
    assert_eq!(
        unnormalized["name"],
        json!(format!(
            "[{}].parent.eth",
            &format!("{:#x}", alloy_primitives::keccak256(b"Alice"))[2..]
        ))
    );
    assert_eq!(unnormalized["name"], unnormalized["display_name"]);
    assert_eq!(unnormalized["namehash"], json!(namehash_of("Alice.parent.eth")));
    assert_ne!(unnormalized["namehash"], json!(namehash_of("alice.parent.eth")));

    // A normalizer error gates the same way.
    let errored = row_for_label("Ni\u{200d}ck");
    assert_eq!(
        errored["name"],
        json!(format!(
            "[{}].parent.eth",
            &format!("{:#x}", alloy_primitives::keccak256("Ni\u{200d}ck".as_bytes()))[2..]
        ))
    );
    assert_eq!(
        errored["namehash"],
        json!(namehash_of("Ni\u{200d}ck.parent.eth"))
    );

    database.cleanup().await
}

fn namehash_of(name: &str) -> String {
    let labels = name.split('.').map(str::as_bytes).collect::<Vec<_>>();
    format!("{:#x}", bigname_storage::ens_namehash_label_bytes(&labels))
}

/// Seeds the shape Project writes for a proof-checked preimage: the raw label bytes plus, only
/// when the label's text passes normalization, the composed name columns. A label whose decoded
/// text fails normalization keeps its raw bytes but is written with both name columns null, so
/// serving falls to the placeholder.
async fn seed_v2_subnames_preimage_child(
    database: &TestDatabase,
    parent_name: &str,
    label: &str,
    _verdict_true: bool,
) -> Result<()> {
    seed_subname_edge(
        database,
        parent_name,
        label.as_bytes(),
        "0x00000000000000000000000000000000000000cc",
        90,
    )
    .await?;
    publish_subname_inputs(database).await
}

#[tokio::test]
async fn v2_subname_counts_agree_with_the_page_after_a_child_observation_is_orphaned() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let counted =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?include=counts")
            .await?;
    assert_eq!(counted["data"][0]["name"], json!("alpha.parent.eth"));
    assert_eq!(counted["data"][0]["subname_count"], json!(1));

    // The grandchild's registry observation is removed by a reorg while its identity and
    // parent remain. Page rows and the composed parent's child count share the new publication.
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = 'ethereum-mainnet' AND block_number = 84")
        .execute(&database.pool).await?;
    publish_subname_inputs(&database).await?;

    let page = v2_subnames_payload_for_database(
        &database,
        "/v1/names/alpha.parent.eth/subnames?page_size=10",
    )
    .await?;
    assert_eq!(page["data"], json!([]));

    let recounted =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?include=counts")
            .await?;
    assert_eq!(recounted["data"][0]["name"], json!("alpha.parent.eth"));
    assert_eq!(recounted["data"][0]["subname_count"], json!(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_parent_with_zero_children_returns_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_parent(&database, "ens:empty.eth", "empty.eth", "node:empty.eth", 80).await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/empty.eth/subnames").await?;

    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_missing_parent_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish_subname_inputs(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/missing.eth/subnames")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 missing parent subnames request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_rejects_malformed_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/parent.eth/subnames?cursor=not-a-cursor")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 malformed subnames cursor request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_requires_restart_for_unbound_legacy_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let wrong_sort = crate::v2::encode(&crate::v2::CursorPayload::new(
        "wrong",
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("parent".to_owned(), "ens:parent.eth".to_owned()),
        ]),
        BTreeMap::from([
            ("display_name".to_owned(), "alpha.parent.eth".to_owned()),
            (
                "child_logical_name_id".to_owned(),
                "ens:alpha.parent.eth".to_owned(),
            ),
        ]),
        Some("wrong-snapshot".to_owned()),
    ));
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/parent.eth/subnames?cursor={wrong_sort}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 wrong-sort subnames cursor request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let legacy_snapshot = crate::v2::encode(&crate::v2::CursorPayload::new(
        "display_name_asc",
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            (
                "parent".to_owned(),
                bigname_storage::logical_name_id_for_name("ens", "parent.eth"),
            ),
        ]),
        BTreeMap::from([
            ("display_name".to_owned(), "alpha.parent.eth".to_owned()),
            (
                "child_logical_name_id".to_owned(),
                bigname_storage::logical_name_id_for_name("ens", "alpha.parent.eth"),
            ),
        ]),
        Some("legacy-snapshot".to_owned()),
    ));
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/parent.eth/subnames?cursor={legacy_snapshot}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 legacy-snapshot subnames cursor request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_sepolia_indexed_inventory_serves_name_and_records_at_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_indexed_inventory(&database).await?;
    let name = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}?source=indexed"),
    )
    .await?;
    assert_eq!(name["data"]["chain_id"], json!(11155111));
    assert_eq!(
        name["data"]["records"]["addresses"]["60"],
        json!("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(
        name["data"]["records"]["texts"]["com.twitter"],
        json!("sepolia-record")
    );
    let at = name["meta"]["as_of_token"]
        .as_str()
        .expect("snapshot token");
    for source in ["indexed", "auto"] {
        let records = v2_name_record_payload_for_database(&database, &format!(
            "/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}/records?source={source}&at={at}&keys=addr:60,text:com.twitter&include=inventory",
        )).await?;
        assert_eq!(records["meta"]["source"], json!("indexed"));
        assert_eq!(records["meta"]["as_of"], name["meta"]["as_of"]);
        assert_eq!(records["data"]["records"]["addr:60"]["status"], json!("ok"));
        assert_eq!(
            records["data"]["records"]["text:com.twitter"]["value"],
            json!("sepolia-record")
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_sepolia_verified_inventory_remains_unsupported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_indexed_inventory(&database).await?;
    let name = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}?source=verified"),
    )
    .await?;
    assert_eq!(name["data"]["status"], json!("unsupported"));
    assert_eq!(
        name["data"]["unsupported_reason"],
        json!("verified_records_not_supported")
    );
    let records = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}/records?source=verified&keys=addr:60",),
    )
    .await?;
    assert_eq!(
        records["data"]["records"]["addr:60"],
        json!({
            "status": "unsupported", "unsupported_reason": "verified_records_not_supported"
        })
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_sepolia_indexed_unclassified_or_historical_inventory_is_not_served() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_only_phase_head_name(&database).await?;
    let records_uri =
        format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}/records?source=indexed&keys=addr:60",);
    let missing = v2_name_record_payload_for_database(&database, &records_uri).await?;
    assert_eq!(
        missing["data"]["records"]["addr:60"],
        json!({
            "status": "unsupported", "unsupported_reason": "resolver_classification_missing"
        })
    );
    let missing_name = v2_name_record_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}?source=indexed"),
    )
    .await?;
    assert!(missing_name["data"]["records"]["addresses"].get("60").is_none(), "{missing_name}");
    assert_eq!(missing_name["data"]["unsupported_fields"], json!(["primary_address"]));
    insert_v2_sepolia_indexed_inventory(&database).await?;
    let current = v2_name_record_payload_for_database(&database, &records_uri).await?;
    let token = current["meta"]["as_of_token"]
        .as_str()
        .context("indexed snapshot token")?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-sepolia","block_number":V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK+1,
            "block_hash":"0xsepolia-next","timestamp":"2026-04-17T00:10:21Z"
        }}))
        .await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-sepolia",
        V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK + 1,
        "0xsepolia-next",
    )
    .await?;
    for uri in [
        format!("/v1/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}?source=indexed"),
        records_uri,
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!("{uri}&at={token}"))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
    database.cleanup().await
}

async fn seed_v2_sepolia_indexed_inventory(database: &TestDatabase) -> Result<()> {
    seed_v2_sepolia_only_phase_head_name(database).await?;
    insert_v2_sepolia_indexed_inventory(database).await
}

async fn insert_v2_sepolia_indexed_inventory(database: &TestDatabase) -> Result<()> {
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-sepolia",
        V2_SEPOLIA_ONLY_SNAPSHOT_NAME,
        "0x0000000000000000000000000000000000000abc",
        V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
        V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
        &[
            family_fixture_record_write("text:com.twitter", Some(json!("sepolia-record"))),
            family_fixture_record_write(
                "addr:60",
                Some(json!("0x0000000000000000000000000000000000000abc")),
            ),
            family_fixture_record_write("avatar", Some(json!("https://example.test/sepolia.png"))),
        ],
    )
    .await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-sepolia",
        V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
        V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
    )
    .await
}

const V2_MAINNET_SNAPSHOT_NAME: &str = "mainnet-pin.eth";
const V2_MAINNET_SNAPSHOT_HASH: &str = "0xv2-mainnet-pin";
const V2_MAINNET_SNAPSHOT_BLOCK: i64 = 21_000_011;
const V2_MAINNET_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:00:11Z";
const V2_SEPOLIA_SNAPSHOT_NAME: &str = "sepolia-pin.eth";
const V2_SEPOLIA_SNAPSHOT_HASH: &str = "0xv2-sepolia-pin";
const V2_SEPOLIA_SNAPSHOT_BLOCK: i64 = 111_551_110;
const V2_SEPOLIA_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:10:10Z";
const V2_SEPOLIA_ONLY_SNAPSHOT_NAME: &str = "sepolia-only.eth";
const V2_SEPOLIA_ONLY_SNAPSHOT_HASH: &str = "0xv2-sepolia-only";
const V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK: i64 = 111_551_120;
const V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:10:20Z";

async fn seed_v2_mixed_phase_head_names(database: &TestDatabase) -> Result<()> {
    seed_v2_snapshot_profile_name(
        database,
        V2_SEPOLIA_SNAPSHOT_NAME,
        "SepoliaPin.eth",
        "namehash:sepolia-pin.eth",
        Uuid::from_u128(0x7e20),
        Uuid::from_u128(0x7e21),
        Uuid::from_u128(0x7e22),
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
        V2_SEPOLIA_SNAPSHOT_TIMESTAMP,
    )
    .await?;
    seed_v2_snapshot_profile_name(
        database,
        V2_MAINNET_SNAPSHOT_NAME,
        "MainnetPin.eth",
        "namehash:mainnet-pin.eth",
        Uuid::from_u128(0x7e10),
        Uuid::from_u128(0x7e11),
        Uuid::from_u128(0x7e12),
        "ethereum",
        "ethereum-mainnet",
        V2_MAINNET_SNAPSHOT_BLOCK,
        V2_MAINNET_SNAPSHOT_HASH,
        V2_MAINNET_SNAPSHOT_TIMESTAMP,
    )
    .await
}

async fn seed_v2_sepolia_only_phase_head_name(database: &TestDatabase) -> Result<()> {
    seed_v2_snapshot_profile_name(
        database,
        V2_SEPOLIA_ONLY_SNAPSHOT_NAME,
        "SepoliaOnly.eth",
        "namehash:sepolia-only.eth",
        Uuid::from_u128(0x7e30),
        Uuid::from_u128(0x7e31),
        Uuid::from_u128(0x7e32),
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
        V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
        V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP,
    )
    .await?;
    sqlx::query("DELETE FROM chain_phase_state WHERE chain_id = 'ethereum-mainnet'")
        .execute(&database.lookup_pool)
        .await
        .context("failed to remove mainnet project state for sepolia-only v2 snapshot test")?;
    sqlx::query("DELETE FROM chain_heads WHERE chain_id = 'ethereum-mainnet'")
        .execute(&database.lookup_pool)
        .await
        .context("failed to remove mainnet phase head for sepolia-only v2 snapshot test")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_v2_snapshot_profile_name(
    database: &TestDatabase,
    normalized_name: &str,
    _display_name: &str,
    _namehash: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    database.seed_snapshot_selector_chain_positions(&v2_snapshot_chain_positions(
        slot, chain_id, block_number, block_hash, timestamp)).await?;
    let logical = seed_family_identity_inputs(&database.pool, "ens", normalized_name,
        chain_id, block_number, block_hash, resource_id, token_lineage_id, surface_binding_id,
        "ens_v1").await?;
    let mut resolver = history_event(&format!("snapshot-resolver-{resource_id}"), Some(&logical),
        Some(resource_id), Some(chain_id), Some(block_number), Some(block_hash),
        Some("0xsnapshot"), Some(0), CanonicalityState::Canonical);
    resolver.event_kind = "ResolverChanged".into();
    resolver.source_family = "ens_v1_registry_l1".into();
    resolver.after_state = json!({"node":bigname_lookup::ens_namehash_hex(normalized_name)?,
        "resolver":"0x0000000000000000000000000000000000000abc"});
    let mut grant = resolver.clone();
    grant.event_identity = format!("snapshot-grant-{resource_id}");
    grant.event_kind = "RegistrationGranted".into();
    grant.source_family = "ens_v1_registrar_l1".into();
    grant.log_index = Some(1);
    grant.after_state = json!({"authority_kind":"registrar", "registrant":"0x00000000000000000000000000000000000000aa", "expiry":4_000_000_000_u64});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[resolver, grant]).await?;
    rebuild_fixture_families(&database.pool, chain_id, block_number, block_hash).await
}

fn v2_snapshot_chain_positions(
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Value {
    json!({
        slot: {
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp
        }
    })
}

fn v2_sepolia_snapshot_token() -> String {
    v2_at_token(
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
        V2_SEPOLIA_SNAPSHOT_TIMESTAMP,
    )
    .expect("sepolia snapshot token fixture must encode")
}

fn v2_at_token_from_meta_as_of(
    payload: &Value,
    numeric_chain_id: &str,
    slot: &str,
    chain_id: &str,
) -> Result<String> {
    let as_of = payload
        .pointer(&format!("/meta/as_of/{numeric_chain_id}"))
        .with_context(|| format!("response must include meta.as_of[{numeric_chain_id}]"))?;
    let block_number = as_of
        .get("block_number")
        .and_then(Value::as_i64)
        .context("meta.as_of block_number must be an i64")?;
    let block_hash = as_of
        .get("block_hash")
        .and_then(Value::as_str)
        .context("meta.as_of block_hash must be a string")?;
    let timestamp = as_of
        .get("timestamp")
        .and_then(Value::as_str)
        .context("meta.as_of timestamp must be a string")?;

    v2_at_token(slot, chain_id, block_number, block_hash, timestamp)
}

fn v2_at_token(
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<String> {
    let position = bigname_storage::ChainPosition {
        slot: slot.to_owned(),
        chain_id: chain_id.to_owned(),
        block_number,
        block_hash: block_hash.to_owned(),
        timestamp: bigname_storage::parse_rfc3339_utc_timestamp(timestamp)
            .map_err(|error| anyhow::anyhow!("{error}"))?,
    };
    let selected = bigname_storage::SelectedSnapshot {
        chain_positions: bigname_storage::ChainPositions::new(std::collections::BTreeMap::from([
            (slot.to_owned(), position),
        ])),
        consistency: bigname_storage::SnapshotConsistency::Head,
    };
    Ok(crate::v2::encode_at_token(&selected))
}

async fn v2_root_pointer_payload(uri: &str, pointer: bool) -> Result<Value> {
    let database = TestDatabase::new_migrated().await?;
    seed_unbound_name_inputs(&database, "eth", pointer).await?;
    let body = v2_name_record_payload_for_database(&database, uri).await?;
    database.cleanup().await?;
    Ok(body)
}

async fn enable_alice_ensip19_inputs(database: &TestDatabase) -> Result<()> {
    let (manifest, mut payload): (i64, Value) = sqlx::query_as(
        "SELECT manifest_id, manifest_payload FROM manifest_versions WHERE source_family = 'ens_v1_resolver_l1'")
        .fetch_one(&database.pool).await?;
    payload["contracts"][0]["read_features"] = json!(["ensip19_default_address"]);
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
        .bind(manifest)
        .bind(&payload)
        .execute(&database.pool)
        .await?;
    seed_fixture_manifest_update(
        &database.pool,
        manifest,
        "ethereum-mainnet",
        "ens",
        "ens_v1_resolver_l1",
        &payload,
    )
    .await?;
    let (block, hash): (i64, String) = sqlx::query_as("SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = 'ethereum-mainnet'")
        .fetch_one(&database.pool).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, &hash).await
}

/// Explicit retained input scenarios used by the name routes. These construct events and
/// bindings; the response's authority, lifecycle and coverage are never supplied by the test.
#[derive(Clone, Copy)]
enum AliceInputState {
    Unbound,
    Registry,
    Released,
    Reserved,
    Wrapped,
    Ownerless,
}

async fn append_alice_name_input(
    database: &TestDatabase,
    kind: &str,
    family: &str,
    after: Value,
) -> Result<()> {
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    let mut event = history_event(
        &format!("alice-{kind}-{ordinal}"),
        Some(&logical),
        Some(Uuid::from_u128(0x2200)),
        Some("ethereum-mainnet"),
        Some(21_000_003),
        Some("0xbinding"),
        Some("0xalice-current"),
        Some(ordinal),
        CanonicalityState::Canonical,
    );
    event.event_kind = kind.into();
    event.source_family = family.into();
    event.before_state = json!({});
    event.after_state = after;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(())
}

async fn seed_unbound_name_inputs(
    database: &TestDatabase,
    name: &str,
    root_pointer: bool,
) -> Result<()> {
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;
    let resource = Uuid::from_u128(0x2200);
    let binding = Uuid::from_u128(0x3300);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        "ethereum-mainnet",
        21_000_003,
        "0xbinding",
        resource,
        Uuid::from_u128(0x1100),
        binding,
        "ens_v2",
    )
    .await?;
    // This fixture has observed a surface and token resource but no registration binding.
    sqlx::query("DELETE FROM surface_bindings WHERE surface_binding_id = $1")
        .bind(binding)
        .execute(&database.pool)
        .await?;
    if root_pointer {
        let mut pointer = history_event(
            "unregistered-root-pointer",
            Some(&logical),
            Some(resource),
            Some("ethereum-mainnet"),
            Some(21_000_003),
            Some("0xbinding"),
            Some("0xroot-pointer"),
            Some(0),
            CanonicalityState::Canonical,
        );
        pointer.event_kind = "ResolverChanged".into();
        pointer.source_family = "ens_v2_root_l1".into();
        pointer.after_state = json!({"node":bigname_lookup::ens_namehash_hex(name)?,
            "resolver":"0x0000000000000000000000000000000000000abc"});
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[pointer]).await?;
        insert_family_fixture_record_writes(
            &database.pool,
            "ens",
            "ethereum-mainnet",
            name,
            "0x0000000000000000000000000000000000000abc",
            21_000_003,
            "0xbinding",
            &[
                family_fixture_record_write(
                    "addr:60",
                    Some(json!("0x0000000000000000000000000000000000000def")),
                ),
                family_fixture_record_write("text:description", Some(json!("Root profile"))),
            ],
        )
        .await?;
        // Interpret admits the resolver reached from this root registry. The same-namespace
        // declaration then selects its concrete ENSv1 resolver implementation.
        let origin = Uuid::new_v4();
        seed_schema_v2_ens_manifest(
            &database.pool,
            "ens_v2_root_l1",
            "root_registry",
            "0x000000000000000000000000000000000000f001",
            origin,
            false,
        )
        .await?;
        let manifest: i64 = sqlx::query_scalar(
            "SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_v2_root_l1'",
        )
        .fetch_one(&database.pool)
        .await?;
        let resolver_instance: Uuid = sqlx::query_scalar("SELECT contract_instance_id FROM manifest_contract_instances WHERE declared_address = '0x0000000000000000000000000000000000000abc'").fetch_one(&database.pool).await?;
        sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, source_manifest_id) VALUES ($1, 'ethereum-mainnet', '0x0000000000000000000000000000000000000abc', $2)")
            .bind(resolver_instance).bind(manifest).execute(&database.pool).await?;
        sqlx::query("INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source, admission_basis, source_manifest_id, active_from_block_number, active_from_block_hash, canonicality_state) VALUES ('ethereum-mainnet', 'resolver', $1, $2, 'ResolverChanged', 'registry_pointer', $3, 21000003, '0xbinding', 'canonical')")
            .bind(origin).bind(resolver_instance).bind(manifest).execute(&database.pool).await?;
    }
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await
}

async fn seed_alice_state_inputs(database: &TestDatabase, state: AliceInputState) -> Result<()> {
    if matches!(state, AliceInputState::Unbound) {
        return seed_unbound_name_inputs(database, "alice.eth", false).await;
    }
    seed_alice_name_inputs(database).await?;
    match state {
        AliceInputState::Registry | AliceInputState::Reserved => {
            sqlx::query("UPDATE surface_bindings SET authority_arm = 'ens_v2' WHERE surface_binding_id = $1")
                .bind(Uuid::from_u128(0x3300)).execute(&database.pool).await?;
            append_alice_name_input(database, "RegistrationGranted", "ens_v2_registry_l1",
                json!({"source_event":"NameRegistered", "authority_kind":"ens_v2_registry",
                    "owner":"0x00000000000000000000000000000000000000bb", "expiry":4_000_000_000_u64})).await?;
            append_alice_name_input(database, "TokenControlTransferred", "ens_v2_registry_l1",
                json!({"source_event":"Transfer", "from":"0x0000000000000000000000000000000000000000",
                    "to":"0x00000000000000000000000000000000000000bb"})).await?;
            append_alice_name_input(
                database,
                "ResolverChanged",
                "ens_v2_registry_l1",
                json!({"node":bigname_lookup::ens_namehash_hex("alice.eth")?,
                    "resolver":"0x0000000000000000000000000000000000000abc"}),
            )
            .await?;
            if matches!(state, AliceInputState::Reserved) {
                // Retain the old resource's record observations after its binding and pointer end.
                sqlx::query("UPDATE surface_bindings SET active_to = to_timestamp(1776384003) WHERE surface_binding_id = $1")
                    .bind(Uuid::from_u128(0x3300)).execute(&database.pool).await?;
                append_alice_name_input(
                    database,
                    "SurfaceUnbound",
                    "ens_v2_registry_l1",
                    json!({"source_event":"LabelReserved", "token_id":"4294967297"}),
                )
                .await?;
                append_alice_name_input(database, "ResolverChanged", "ens_v2_registry_l1",
                    json!({"source_event":"LabelReserved", "token_id":"4294967297", "resolver":null})).await?;
                let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
                let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
                let mut reservation = history_event(
                    &format!("alice-reserved-{ordinal}"),
                    Some(&logical),
                    None,
                    Some("ethereum-mainnet"),
                    Some(21_000_003),
                    Some("0xbinding"),
                    Some("0xalice-current"),
                    Some(ordinal),
                    CanonicalityState::Canonical,
                );
                reservation.event_kind = "RegistrationReserved".into();
                reservation.source_family = "ens_v2_registry_l1".into();
                reservation.before_state = json!({});
                reservation.after_state = json!({"source_event":"LabelReserved", "status":"reserved",
                    "expiry":4_000_000_000_u64, "token_id":"4294967297", "current_token_id":"4294967297",
                    "registry_contract_instance_id":Uuid::from_u128(0x4400).to_string()});
                bigname_storage::insert_normalized_event_fixtures(&database.pool, &[reservation])
                    .await?;
            }
        }
        AliceInputState::Ownerless => {
            sqlx::query("UPDATE surface_bindings SET active_to = to_timestamp(1776384003) WHERE surface_binding_id = $1")
                .bind(Uuid::from_u128(0x3300)).execute(&database.pool).await?;
            sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
                .bind(Uuid::from_u128(0x2200))
                .execute(&database.pool)
                .await?;
            append_alice_name_input(database, "AuthorityTransferred", "ens_v1_registry_l1",
                json!({"source_event":"Transfer", "node":bigname_lookup::ens_namehash_hex("alice.eth")?,
                    "owner":"0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e", "owner_getter":"0x0000000000000000000000000000000000000000",
                    "registry_owner":"0x0000000000000000000000000000000000000000", "owner_getter_reason":"registry_self"})).await?;
        }
        AliceInputState::Released => {
            append_alice_name_input(
                database,
                "RegistrationReleased",
                "ens_v1_registrar_l1",
                json!({"authority_kind":"registrar", "released_at":1_776_384_003,
                    "expiry":1_700_000_000}),
            )
            .await?;
            sqlx::query("UPDATE surface_bindings SET active_to = to_timestamp(1776384003) WHERE surface_binding_id = $1")
                .bind(Uuid::from_u128(0x3300)).execute(&database.pool).await?;
        }
        AliceInputState::Wrapped => {
            append_alice_name_input(
                database,
                "AuthorityEpochChanged",
                "ens_v1_wrapper_l1",
                json!({"source_event":"NameWrapped", "authority_kind":"wrapper",
                    "owner":"0x00000000000000000000000000000000000000aa"}),
            )
            .await?;
            append_alice_name_input(database, "TokenControlTransferred", "ens_v1_wrapper_l1",
                json!({"source_event":"NameWrapped", "owner":"0x00000000000000000000000000000000000000aa"})).await?;
            append_alice_name_input(
                database,
                "PermissionScopeChanged",
                "ens_v1_wrapper_l1",
                json!({"source_event":"NameWrapped", "wrapper_state":"locked", "fuses":196609}),
            )
            .await?;
            append_alice_name_input(
                database,
                "ExpiryChanged",
                "ens_v1_wrapper_l1",
                json!({"source_event":"NameWrapped", "expiry":4_000_000_000_u64}),
            )
            .await?;
        }
        AliceInputState::Unbound => unreachable!(),
    }
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await
}

async fn seed_alice_verified_inputs(
    database: &TestDatabase,
    state: AliceInputState,
    writes: &[Value],
) -> Result<()> {
    seed_alice_state_inputs(database, state).await?;
    replace_alice_record_inputs(database, writes).await?;
    enable_alice_ensip19_inputs(database).await?;
    const HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    seed_schema_v2_ens_lookup_head(&database.pool, 21_000_004, HASH, "2026-04-17T00:00:04Z")
        .await?;
    seed_schema_v2_ens_manifest(
        &database.pool,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc301),
        true,
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_004, HASH).await
}

async fn v2_alice_state_payload(uri: &str, state: AliceInputState) -> Result<Value> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_state_inputs(&database, state).await?;
    let body = v2_name_record_payload_for_database(&database, uri).await?;
    database.cleanup().await?;
    Ok(body)
}

async fn v2_name_record_payload(uri: &str) -> Result<Value> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let payload = v2_name_record_payload_for_database(&database, uri).await?;
    database.cleanup().await?;
    Ok(payload)
}

async fn v2_name_record_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name record request failed")?;

    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    Ok(payload)
}

async fn v2_name_records_payload(uri: &str) -> Result<Value> {
    v2_name_record_payload(uri).await
}



async fn v2_name_payload_without_inventory(uri: &str) -> Result<Value> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    append_alice_name_input(&database, "ResolverChanged", "ens_v1_registry_l1", json!({
        "node":bigname_lookup::ens_namehash_hex("alice.eth")?,
        "resolver":"0x0000000000000000000000000000000000000000"
    })).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let body = v2_name_record_payload_for_database(&database, uri).await?;
    database.cleanup().await?;
    Ok(body)
}



// Sepolia's root registry registers `reverse` and points it at the ENSv1 mirror resolver
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ReverseMirror.ts:L25-L37 @ ens_v2_sepolia_20260916@366de741).
const REVERSE_MIRROR: &str = "0x0000000000000000000000000000000000000f10";

#[derive(Clone, Copy)]
enum MirrorFixtureSource {
    Exact,
    Absent,
    Ancestor,
}

// Retained input shape from Project's mirror fixture: an ENSv2 root pointer plus an independent
// ENSv1 node pointer and resolver writes. No serving rows are supplied by this constructor.
async fn v2_mirror_records_payload(
    uri: &str,
    name: &str,
    mirror: &str,
    source: MirrorFixtureSource,
) -> Result<Value> {
    const CHAIN: &str = "ethereum-sepolia";
    const HASH: &str = "0xmirror";
    const V1_REGISTRY: &str = "0x4444444444444444444444444444444444444401";
    const V1_RESOLVER: &str = "0x1111111111111111111111111111111111111111";
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({CHAIN:{
            "chain_id":CHAIN,"block_number":21000003,"block_hash":HASH,
            "timestamp":"2026-04-17T00:00:03Z"
        }}))
        .await?;
    let resource = Uuid::from_u128(0x6100);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        CHAIN,
        21000003,
        HASH,
        resource,
        Uuid::from_u128(0x6101),
        Uuid::from_u128(0x6102),
        "ens_v2",
    )
    .await?;
    let payload = json!({"deployment_epoch":"fixture",
        "correlation_addresses":{"ens_v1_registry":V1_REGISTRY},
        "contracts":[{"role":"ensv1_mirror_resolver","address":mirror,"proxy_kind":"none","start_block":0}]});
    let mut root_manifest = 0;
    for (family, payload) in [
        ("ens_v2_root_l1", json!({})),
        ("ens_v2_resolver_l1", payload),
    ] {
        let manifest:i64=sqlx::query_scalar("INSERT INTO manifest_versions
            (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload)
            VALUES (1,'ens',$1,$2,'fixture','active','fixture',$3,$4) RETURNING manifest_id")
            .bind(family).bind(CHAIN).bind(format!("fixture/{family}.toml")).bind(&payload)
            .fetch_one(&database.pool).await?;
        seed_fixture_manifest_update(&database.pool, manifest, CHAIN, "ens", family, &payload)
            .await?;
        if family == "ens_v2_root_l1" {
            root_manifest = manifest;
        }
    }
    let root_instance = Uuid::from_u128(0x6110);
    let mirror_instance = Uuid::from_u128(0x6111);
    for instance in [root_instance, mirror_instance] {
        sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1,$2,'contract')")
            .bind(instance).bind(CHAIN).execute(&database.pool).await?;
    }
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,source_manifest_id,active_from_block_number,active_from_block_hash)
        VALUES ($1,$2,$3,$4,21000003,$5)")
        .bind(mirror_instance).bind(CHAIN).bind(mirror).bind(root_manifest).bind(HASH).execute(&database.pool).await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state)
        VALUES ($1,'resolver',$2,$3,'ResolverChanged','registry_pointer',$4,21000003,$5,'canonical')")
        .bind(CHAIN).bind(root_instance).bind(mirror_instance).bind(root_manifest).bind(HASH).execute(&database.pool).await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let facts = [
        (
            "RegistrationGranted",
            json!({"source_event":"LabelRegistered","authority_kind":"ens_v2_registry","registrant":V2_ADDRESS,"expiry":u64::MAX}),
        ),
        (
            "TokenControlTransferred",
            json!({"source_event":"Transfer","from":"0x0000000000000000000000000000000000000000","to":V2_ADDRESS}),
        ),
        ("ResolverChanged", json!({"node":node,"resolver":mirror})),
    ];
    let mut events = Vec::new();
    for (index, (kind, after)) in facts.into_iter().enumerate() {
        let mut event = history_event(
            &format!("mirror-root-{kind}"),
            Some(&logical),
            Some(resource),
            Some(CHAIN),
            Some(21000003),
            Some(HASH),
            Some("0xmirror"),
            Some(index as i64),
            CanonicalityState::Canonical,
        );
        event.source_family = "ens_v2_root_l1".into();
        event.event_kind = kind.into();
        event.source_manifest_id = Some(root_manifest);
        event.manifest_version = 1;
        event.before_state = json!({});
        event.after_state = after;
        event.raw_fact_ref = json!({"emitting_address":V1_REGISTRY});
        events.push(event);
    }
    if !matches!(source, MirrorFixtureSource::Absent) {
        let pointer_name = if matches!(source, MirrorFixtureSource::Ancestor) {
            "eth"
        } else {
            name
        };
        let mut pointer = history_event(
            "mirror-v1-pointer",
            None,
            None,
            Some(CHAIN),
            Some(21000003),
            Some(HASH),
            Some("0xmirror"),
            Some(4),
            CanonicalityState::Canonical,
        );
        pointer.source_family = "ens_v1_registry_l1".into();
        pointer.event_kind = "ResolverChanged".into();
        pointer.before_state = json!({});
        pointer.after_state =
            json!({"node":bigname_lookup::ens_namehash_hex(pointer_name)?,"resolver":V1_RESOLVER});
        pointer.raw_fact_ref = json!({"emitting_address":V1_REGISTRY});
        events.push(pointer);
        insert_family_fixture_record_writes(
            &database.pool,
            "ens",
            CHAIN,
            name,
            V1_RESOLVER,
            21000003,
            HASH,
            &[
                family_fixture_record_write(
                    "addr:60",
                    Some(json!("0x0000000000000000000000000000000000000def")),
                ),
                family_fixture_record_write("text:description", Some(json!("Alice profile"))),
                family_fixture_record_write("text:url", Some(json!("https://reverse.example"))),
            ],
        )
        .await?;
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, CHAIN, 21000003, HASH).await?;
    let body = v2_name_record_payload_for_database(&database, uri).await?;
    database.cleanup().await?;
    Ok(body)
}

async fn v2_reverse_root_records_payload(uri: &str, mirror_projected: bool) -> Result<Value> {
    v2_mirror_records_payload(
        uri,
        "reverse",
        REVERSE_MIRROR,
        if mirror_projected {
            MirrorFixtureSource::Exact
        } else {
            MirrorFixtureSource::Absent
        },
    )
    .await
}

#[tokio::test]
async fn v2_get_name_records_serves_the_registered_reverse_root_through_its_mirror() -> Result<()> {
    for source in ["indexed", "auto"] {
        let payload = v2_reverse_root_records_payload(
            &format!("/v1/names/reverse/records?source={source}&keys=text:url&include=inventory"),
            true,
        )
        .await?;
        assert_eq!(
            payload["data"]["resolver"],
            json!({"chain_id": 11155111, "address": REVERSE_MIRROR}),
            "{source}: {payload}"
        );
        assert_eq!(
            payload["data"]["records"]["text:url"],
            json!({"status": "ok", "value": "https://reverse.example"}),
            "{source}: {payload}"
        );
        assert_eq!(payload["meta"]["source"], json!("indexed"), "{source}");
    }

    let indexed = v2_reverse_root_records_payload(
        "/v1/names/reverse/records?source=indexed&keys=text:url&include=inventory",
        false,
    )
    .await?;
    assert_eq!(
        indexed["data"]["resolver"],
        json!({"chain_id": 11155111, "address": REVERSE_MIRROR}),
        "{indexed}"
    );
    assert_eq!(
        indexed["data"]["records"]["text:url"],
        json!({"status": "unsupported", "unsupported_reason": "mirrored_resolver_not_projected"}),
        "{indexed}"
    );
    assert_eq!(
        indexed["data"]["inventory"]["unsupported_keys"],
        json!(["text:url"])
    );
    // An unsupported inventory does not satisfy `auto`; the key goes to verified lookup, which
    // this fixture declares no topology for, and the resolver stays the registration's own.
    let auto = v2_reverse_root_records_payload(
        "/v1/names/reverse/records?source=auto&keys=text:url",
        false,
    )
    .await?;
    assert_eq!(auto["meta"]["source"], json!("verified"), "{auto}");
    assert_eq!(
        auto["data"]["resolver"],
        json!({"chain_id": 11155111, "address": REVERSE_MIRROR}),
        "{auto}"
    );
    assert_eq!(
        auto["data"]["records"]["text:url"],
        json!({"status": "unsupported", "unsupported_reason": "verified_records_not_supported"}),
        "{auto}"
    );
    Ok(())
}

fn v2_subname_names(payload: &Value) -> Vec<String> {
    payload["data"]
        .as_array()
        .expect("subnames data must be an array")
        .iter()
        .map(|row| {
            row["name"]
                .as_str()
                .expect("subname name must be a string")
                .to_owned()
        })
        .collect()
}

#[tokio::test]
async fn v2_get_subnames_q_filters_by_normalized_label_prefix() -> Result<()> {
    let (database, payload) = v2_subnames_payload("/v1/names/Parent.eth/subnames?q=AL").await?;
    assert_eq!(v2_subname_names(&payload), vec!["alpha.parent.eth"]);
    assert_eq!(
        payload["page"]["total_count"],
        json!(1),
        "a filtered page reports its exact total"
    );
    assert_eq!(payload["page"]["has_more"], json!(false));

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/Parent.eth/subnames?q=alpha.")
            .await?;
    assert_eq!(v2_subname_names(&payload), vec!["alpha.parent.eth"]);

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/Parent.eth/subnames?q=alph.")
            .await?;
    assert!(v2_subname_names(&payload).is_empty());

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/Parent.eth/subnames?q=x").await?;
    assert!(v2_subname_names(&payload).is_empty());

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/Parent.eth/subnames?q=").await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["alpha.parent.eth", "beta.parent.eth", "gamma.parent.eth"]
    );
    assert_eq!(payload["page"]["total_count"], json!(3));

    let response =
        v2_subnames_response_for_database(&database, "/v1/names/Parent.eth/subnames?q=a..b")
            .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = read_json(response).await?;
    assert_eq!(body["error"]["code"], json!("invalid_input"));

    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_sorts_by_timestamps_and_binds_cursors_to_sort_and_order() -> Result<()> {
    let (database, payload) =
        v2_subnames_payload("/v1/names/Parent.eth/subnames?sort=expires_at&order=asc&page_size=1")
            .await?;
    assert_eq!(v2_subname_names(&payload), vec!["alpha.parent.eth"]);
    assert_eq!(payload["page"]["total_count"], json!(3));
    assert_eq!(payload["page"]["has_more"], json!(true));
    let first_cursor = payload["page"]["next_cursor"]
        .as_str()
        .expect("first page must carry a cursor")
        .to_owned();

    let payload = v2_subnames_payload_for_database(
        &database,
        &format!(
            "/v1/names/Parent.eth/subnames?sort=expires_at&order=asc&page_size=1&cursor={first_cursor}"
        ),
    )
    .await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["beta.parent.eth"],
        "rows without an expiry sort after every dated row, ties broken by name"
    );
    let second_cursor = payload["page"]["next_cursor"]
        .as_str()
        .expect("second page must carry a cursor")
        .to_owned();
    let payload = v2_subnames_payload_for_database(
        &database,
        &format!(
            "/v1/names/Parent.eth/subnames?sort=expires_at&order=asc&page_size=1&cursor={second_cursor}"
        ),
    )
    .await?;
    assert_eq!(v2_subname_names(&payload), vec!["gamma.parent.eth"]);
    assert_eq!(payload["page"]["has_more"], json!(false));

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?sort=expires_at&order=desc",
    )
    .await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["beta.parent.eth", "gamma.parent.eth", "alpha.parent.eth"],
        "descending timestamp order lists rows without an expiry first"
    );

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?sort=registered_at",
    )
    .await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["alpha.parent.eth", "beta.parent.eth", "gamma.parent.eth"]
    );
    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?sort=registered_at&order=desc",
    )
    .await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["beta.parent.eth", "gamma.parent.eth", "alpha.parent.eth"]
    );

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?sort=name&order=desc",
    )
    .await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["gamma.parent.eth", "beta.parent.eth", "alpha.parent.eth"]
    );

    let mut uri =
        "/v1/names/parent.eth/subnames?sort=registered_at&order=desc&page_size=1".to_owned();
    let mut registered_walk = Vec::new();
    loop {
        let page = v2_subnames_payload_for_database(&database, &uri).await?;
        registered_walk.extend(v2_subname_names(&page));
        let Some(cursor) = page["page"]["next_cursor"].as_str() else {
            break;
        };
        uri = format!(
            "/v1/names/parent.eth/subnames?sort=registered_at&order=desc&page_size=1&cursor={cursor}"
        );
    }
    assert_eq!(
        registered_walk,
        vec!["beta.parent.eth", "gamma.parent.eth", "alpha.parent.eth"]
    );

    for uri in [
        format!("/v1/names/Parent.eth/subnames?sort=name&page_size=1&cursor={first_cursor}"),
        format!(
            "/v1/names/Parent.eth/subnames?sort=expires_at&order=desc&page_size=1&cursor={first_cursor}"
        ),
        format!(
            "/v1/names/Parent.eth/subnames?sort=registered_at&page_size=1&cursor={first_cursor}"
        ),
        format!("/v1/names/Parent.eth/subnames?sort=expires_at&q=al&cursor={first_cursor}"),
        "/v1/names/Parent.eth/subnames?sort=expiry".to_owned(),
        "/v1/names/Parent.eth/subnames?order=sideways".to_owned(),
    ] {
        let response = v2_subnames_response_for_database(&database, &uri).await?;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{uri} must be rejected"
        );
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], json!("invalid_input"));
    }

    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_include_expired_false_omits_released_registrar_child() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_parent(&database, "ens:eth", "eth", "unused", 80).await?;
    seed_alice_state_inputs(&database, AliceInputState::Released).await?;
    let label = insert_family_label_preimage(&database.pool, b"alice").await?;
    insert_family_registry_child_edge(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "eth",
        &label,
        "0x00000000000000000000000000000000000000aa",
        21_000_003,
        "0xbinding",
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let all = v2_subnames_payload_for_database(&database, "/v1/names/eth/subnames").await?;
    assert_eq!(v2_subname_names(&all), vec!["alice.eth"]);
    assert_eq!(all["data"][0]["registration_status"], "released");
    let live =
        v2_subnames_payload_for_database(&database, "/v1/names/eth/subnames?include_expired=false")
            .await?;
    assert!(v2_subname_names(&live).is_empty());
    assert_eq!(live["page"]["total_count"], 0);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_subnames_include_expired_false_omits_past_expiry_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_subname_inputs(
        &database,
        "epsilon.parent.eth",
        85,
        Uuid::from_u128(0x7050),
        Uuid::from_u128(0x7051),
        Uuid::from_u128(0x7052),
        SubnameInput::Lease {
            owner: "0x00000000000000000000000000000000000000ea",
            registrant: "0x00000000000000000000000000000000000000eb",
            expiry: parse_rfc3339_utc_timestamp("2025-01-02T03:04:05Z")?.unix_timestamp(),
        },
    )
    .await?;
    seed_subname_edge(
        &database,
        "parent.eth",
        b"epsilon",
        "0x00000000000000000000000000000000000000ea",
        85,
    )
    .await?;
    publish_subname_inputs(&database).await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v1/names/Parent.eth/subnames").await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec![
            "alpha.parent.eth",
            "beta.parent.eth",
            "epsilon.parent.eth",
            "gamma.parent.eth"
        ],
        "the default keeps past-expiry rows"
    );
    assert_eq!(payload["page"]["total_count"], json!(4));
    assert_eq!(
        payload["data"][2]["registration_status"],
        json!("registered")
    );
    assert_eq!(
        payload["data"][2]["expires_at"],
        json!("2025-01-02T03:04:05Z")
    );

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?include_expired=true",
    )
    .await?;
    assert_eq!(v2_subname_names(&payload).len(), 4);
    assert_eq!(payload["page"]["total_count"], json!(4));

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?include_expired=false",
    )
    .await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["alpha.parent.eth", "beta.parent.eth", "gamma.parent.eth"],
        "rows whose expires_at has passed are omitted; unregistered rows stay"
    );
    assert_eq!(payload["page"]["total_count"], json!(3));

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?include_expired=false&sort=expires_at&order=desc",
    )
    .await?;
    assert_eq!(
        v2_subname_names(&payload),
        vec!["beta.parent.eth", "gamma.parent.eth", "alpha.parent.eth"]
    );

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v1/names/Parent.eth/subnames?include_expired=false&page_size=1",
    )
    .await?;
    assert_eq!(v2_subname_names(&payload), vec!["alpha.parent.eth"]);
    assert_eq!(payload["page"]["has_more"], json!(true));
    let cursor = payload["page"]["next_cursor"]
        .as_str()
        .expect("filtered page must carry a cursor")
        .to_owned();
    let payload = v2_subnames_payload_for_database(
        &database,
        &format!("/v1/names/Parent.eth/subnames?include_expired=false&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(v2_subname_names(&payload), vec!["beta.parent.eth"]);
    assert_eq!(payload["page"]["has_more"], json!(true));

    for uri in [
        format!("/v1/names/Parent.eth/subnames?page_size=1&cursor={cursor}"),
        format!("/v1/names/Parent.eth/subnames?include_expired=true&page_size=1&cursor={cursor}"),
        "/v1/names/Parent.eth/subnames?include_expired=maybe".to_owned(),
    ] {
        let response = v2_subnames_response_for_database(&database, &uri).await?;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{uri} must be rejected"
        );
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], json!("invalid_input"));
    }

    Ok(())
}

async fn v2_subnames_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    let payload = v2_subnames_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_subnames_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_subnames_response_for_database(database, uri).await?;

    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    Ok(payload)
}

async fn v2_subnames_response_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 subnames request failed")
}

#[derive(Clone, Copy)]
enum SubnameInput {
    RegistryOwner(&'static str),
    Lease {
        owner: &'static str,
        registrant: &'static str,
        expiry: i64,
    },
    Unbound,
}

async fn seed_subname_block(database: &TestDatabase, block: i64) -> Result<String> {
    let hash = format!("0xsubname{block}");
    let timestamp = if block == 10 {
        parse_rfc3339_utc_timestamp("2023-01-02T03:04:05Z")?
    } else if block >= 95 {
        parse_rfc3339_utc_timestamp("2026-02-03T04:05:06Z")? + time::Duration::seconds(block - 95)
    } else {
        parse_rfc3339_utc_timestamp("2024-01-02T03:04:04Z")? + time::Duration::seconds(block - 80)
    };
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            &hash,
            None,
            block,
            timestamp.unix_timestamp(),
        )],
    )
    .await?;
    Ok(hash)
}

async fn publish_subname_inputs(database: &TestDatabase) -> Result<()> {
    database.seed_snapshot_selector_chain_positions(&json!({"ethereum": {
        "chain_id":"ethereum-mainnet", "block_number":100, "block_hash":"0xsubnames-published", "timestamp":"2026-04-17T00:00:20Z"
    }})).await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-mainnet",
        100,
        "0xsubnames-published",
    )
    .await
}

async fn seed_subname_registry(database: &TestDatabase, parent: &str) -> Result<(Uuid, String)> {
    let digest = alloy_primitives::keccak256(parent.as_bytes());
    let instance = Uuid::from_slice(&digest[..16])?;
    let address = format!("0x{}", alloy_primitives::hex::encode(&digest[..20]));
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1, 'ethereum-mainnet', 'contract') ON CONFLICT DO NOTHING")
        .bind(instance).execute(&database.pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number)
        SELECT $1, 'ethereum-mainnet', $2, 80 WHERE NOT EXISTS (SELECT 1 FROM contract_instance_addresses WHERE contract_instance_id = $1)")
        .bind(instance).bind(&address).execute(&database.pool).await?;
    let hash = seed_subname_block(database, 80).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", parent);
    let mut events = Vec::new();
    for (log, kind, name, after) in [
        (
            8,
            "RegistryCreated",
            None,
            json!({"source_event":"RegistryCreated", "registry":address}),
        ),
        (
            9,
            "SubregistryChanged",
            Some(logical.as_str()),
            json!({"source_event":"SubregistryUpdated", "subregistry":address}),
        ),
    ] {
        let mut event = history_event(
            &format!("subname-registry-{instance}-{kind}"),
            name,
            None,
            Some("ethereum-mainnet"),
            Some(80),
            Some(&hash),
            Some("0xsubregistry"),
            Some(log),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = "ens_v2_registry_l1".into();
        event.raw_fact_ref = json!({"kind":"raw_log", "emitting_address":address});
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok((instance, address))
}

#[allow(clippy::too_many_arguments)]
async fn seed_subname_inputs(
    database: &TestDatabase,
    name: &str,
    block: i64,
    resource: Uuid,
    token: Uuid,
    binding: Uuid,
    state: SubnameInput,
) -> Result<()> {
    let name = bigname_domain::normalization::normalize_name(name)?.normalized_name;
    let created_hash = seed_subname_block(database, 10).await?;
    let hash = seed_subname_block(database, block).await?;
    let is_v2 = matches!(state, SubnameInput::Lease { .. });
    let registry = if is_v2 {
        Some(
            seed_subname_registry(database, name.split_once('.').context("subname parent")?.1)
                .await?,
        )
    } else {
        None
    };
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        &name,
        "ethereum-mainnet",
        10,
        &created_hash,
        resource,
        token,
        binding,
        if is_v2 { "ens_v2" } else { "ens_v1" },
    )
    .await?;
    if matches!(state, SubnameInput::Unbound) {
        sqlx::query("DELETE FROM surface_bindings WHERE surface_binding_id = $1")
            .bind(binding)
            .execute(&database.pool)
            .await?;
        return Ok(());
    }
    let mut facts = Vec::new();
    match state {
        SubnameInput::RegistryOwner(owner) => {
            facts.push(("AuthorityTransferred", "ens_v1_registry_l1", json!({
                "source_event":"Transfer", "node":bigname_lookup::ens_namehash_hex(&name)?, "owner":owner
            })));
            facts.push(("AuthorityEpochChanged", "ens_v1_registry_l1", json!({
                "authority_kind":"registry_only", "authority_key":format!("registry:{logical}"), "owner":owner
            })));
        }
        SubnameInput::Lease {
            owner,
            registrant,
            expiry,
            ..
        } => {
            facts.push(("RegistrationGranted", "ens_v2_registry_l1", json!({
                "source_event":"LabelRegistered", "authority_kind":"ens_v2_registry", "registrant":registrant,
                "owner":registrant, "expiry":expiry
            })));
            facts.push(("TokenControlTransferred", "ens_v2_registry_l1", json!({
                "source_event":"TransferSingle", "from":"0x0000000000000000000000000000000000000000", "to":owner
            })));
        }
        SubnameInput::Unbound => unreachable!(),
    }
    let mut events = Vec::new();
    for (log, (kind, family, mut after)) in facts.into_iter().enumerate() {
        let mut event = history_event(
            &format!("subname-{resource}-{kind}"),
            Some(&logical),
            Some(resource),
            Some("ethereum-mainnet"),
            Some(block),
            Some(&hash),
            Some("0xsubname"),
            Some(log as i64),
            CanonicalityState::Canonical,
        );
        if let Some((instance, address)) = &registry {
            after["registry_contract_instance_id"] = json!(instance.to_string());
            event.raw_fact_ref = json!({"kind":"raw_log", "emitting_address":address});
        }
        event.event_kind = kind.into();
        event.source_family = family.into();
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(())
}

async fn seed_subname_edge(
    database: &TestDatabase,
    parent: &str,
    label: &[u8],
    owner: &str,
    block: i64,
) -> Result<()> {
    let hash = seed_subname_block(database, block).await?;
    let labelhash = insert_family_label_preimage(&database.pool, label).await?;
    insert_family_registry_child_edge(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        parent,
        &labelhash,
        owner,
        block,
        &hash,
    )
    .await?;
    Ok(())
}

async fn seed_v2_subnames_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_subnames_parent(
        database,
        "ens:parent.eth",
        "parent.eth",
        "node:parent.eth",
        80,
    )
    .await?;
    seed_subname_inputs(
        database,
        "alpha.parent.eth",
        81,
        Uuid::from_u128(0x4010),
        Uuid::from_u128(0x5010),
        Uuid::from_u128(0x6010),
        SubnameInput::Lease {
            owner: "0x00000000000000000000000000000000000000aa",
            registrant: "0x00000000000000000000000000000000000000ab",
            expiry: 1798859045,
        },
    )
    .await?;
    seed_subname_inputs(
        database,
        "beta.parent.eth",
        82,
        Uuid::from_u128(0x4020),
        Uuid::from_u128(0x5020),
        Uuid::from_u128(0x6020),
        SubnameInput::Unbound,
    )
    .await?;
    seed_subname_inputs(
        database,
        "gamma.parent.eth",
        83,
        Uuid::from_u128(0x4030),
        Uuid::from_u128(0x5030),
        Uuid::from_u128(0x6030),
        SubnameInput::Unbound,
    )
    .await?;
    seed_subname_inputs(
        database,
        "delta.alpha.parent.eth",
        84,
        Uuid::from_u128(0x4040),
        Uuid::from_u128(0x5040),
        Uuid::from_u128(0x6040),
        SubnameInput::Unbound,
    )
    .await?;
    for (parent, label, block) in [
        ("parent.eth", "alpha", 81),
        ("parent.eth", "beta", 82),
        ("parent.eth", "gamma", 83),
        ("alpha.parent.eth", "delta", 84),
    ] {
        seed_subname_edge(
            database,
            parent,
            label.as_bytes(),
            "0x00000000000000000000000000000000000000cc",
            block,
        )
        .await?;
    }
    publish_subname_inputs(database).await
}

/// Seeds the shape Project writes for a preimage whose label bytes do not decode: raw bytes
/// stored with no decoded form.
async fn seed_v2_subnames_undecodable_child(
    database: &TestDatabase,
    parent_name: &str,
    _labelhash: &str,
) -> Result<()> {
    seed_subname_edge(
        database,
        parent_name,
        b"\xff\tBad",
        "0x00000000000000000000000000000000000000cc",
        90,
    )
    .await?;
    publish_subname_inputs(database).await
}

/// Seeds the shape Project writes for a registry edge whose label was never observed: every name
/// column null and no child `name_surfaces` row.
async fn seed_v2_subnames_topology_only_child(
    database: &TestDatabase,
    parent_name: &str,
    labelhash: &str,
) -> Result<()> {
    let hash = seed_subname_block(database, 90).await?;
    insert_family_registry_child_edge(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        parent_name,
        labelhash,
        "0x00000000000000000000000000000000000000cc",
        90,
        &hash,
    )
    .await?;
    publish_subname_inputs(database).await
}

async fn seed_v2_subnames_parent(
    database: &TestDatabase,
    _logical: &str,
    display_name: &str,
    _namehash: &str,
    block: i64,
) -> Result<()> {
    seed_subname_inputs(
        database,
        display_name,
        block,
        Uuid::from_u128(0x4000),
        Uuid::from_u128(0x5000),
        Uuid::from_u128(0x6000),
        SubnameInput::RegistryOwner("0x0000000000000000000000000000000000000001"),
    )
    .await?;
    publish_subname_inputs(database).await
}


fn resolution_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"),
        resolution_left_pad_hex("20", 64),
        resolution_padded_address_hex(address),
    ))
}

fn resolution_universal_resolver_text_response(text: &str) -> Value {
    let text_hex = hex::encode(text);
    let padded_text_len = text_hex.len().div_ceil(64) * 64;
    let inner_length = 64 + padded_text_len / 2;
    json!(format!(
        "0x{}{}{}{}{}{:0<padded_text_len$}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"),
        resolution_left_pad_hex(&format!("{inner_length:x}"), 64),
        resolution_left_pad_hex("20", 64),
        resolution_left_pad_hex(&format!("{:x}", text.len()), 64),
        text_hex,
    ))
}

fn resolution_resolver_not_found_error(name: &[u8]) -> Value {
    let selector = format!(
        "{:#x}",
        alloy_primitives::keccak256("ResolverNotFound(bytes)")
    );
    let name_hex = hex::encode(name);
    let padded_name_len = name_hex.len().div_ceil(64) * 64;
    json!({
        "__rpc_error": {
            "code": -32000,
            "message": "execution reverted",
            "data": {
                "originalError": {
                    "data": format!(
                        "0x{}{}{}{:0<padded_name_len$}",
                        &selector[2..10],
                        resolution_left_pad_hex("20", 64),
                        resolution_left_pad_hex(&format!("{:x}", name.len()), 64),
                        name_hex,
                    )
                }
            }
        }
    })
}

fn resolution_universal_resolver_multicoin_response(address: &str) -> Value {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    json!(format!(
        "0x{}{}{}{}{}{}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"),
        resolution_left_pad_hex("60", 64),
        resolution_left_pad_hex("20", 64),
        resolution_left_pad_hex("14", 64),
        format!("{stripped:0<64}"),
    ))
}

fn resolution_basenames_l1_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}",
        resolution_left_pad_hex("20", 64),
        resolution_left_pad_hex("20", 64),
        resolution_padded_address_hex(address),
    ))
}

fn resolution_padded_address_hex(address: &str) -> String {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    resolution_left_pad_hex(stripped, 64)
}

fn resolution_left_pad_hex(value: &str, width: usize) -> String {
    assert!(value.len() <= width, "test hex value must fit padded width");
    format!("{value:0>width$}")
}

fn assert_v2_name_snapshot_meta(payload: &Value) {
    assert!(
        payload["meta"]["as_of"].is_object(),
        "name response must include meta.as_of"
    );
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("name response must include meta.as_of_token");
    assert!(!token.is_empty(), "meta.as_of_token must not be empty");
    assert!(
        token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')),
        "meta.as_of_token must be URL-safe"
    );
}

fn assert_no_banned_v1_spellings(value: &Value) {
    const BANNED: &[&str] = &[
        "as_of_timestamp",
        "canonical_display_name",
        "chain_positions",
        "coin_addresses",
        "coin_type_addresses",
        "consistency",
        "coverage",
        "declared_state",
        "expiration",
        "expiry",
        "expiry_date",
        "last_updated",
        "logical_name_id",
        "manager_address",
        "normalized_name",
        "owner_address",
        "provenance",
        "resolver_address",
        "resource_id",
        "surface_binding_id",
        "token_lineage_id",
        "verified_state",
    ];

    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !BANNED.contains(&key.as_str()),
                    "v2 response leaked banned v1 field {key}"
                );
                assert_no_banned_v1_spellings(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_no_banned_v1_spellings(value);
            }
        }
        _ => {}
    }
}

/// Publishes the migration proof as a named retained event; composition supplies migrated_at.
async fn stamp_v2_alice_migration_transition(
    database: &TestDatabase,
    block_number: i64,
) -> Result<()> {
    let block_hash: String = sqlx::query_scalar("SELECT block_hash FROM chain_lineage WHERE chain_id = 'ethereum-mainnet' AND block_number = $1 AND canonicality_state = 'canonical'")
        .bind(block_number).fetch_one(&database.pool).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    let mut event = history_event(
        &format!("alice-migration-{block_number}"),
        Some(&logical),
        Some(Uuid::from_u128(0x2200)),
        Some("ethereum-mainnet"),
        Some(block_number),
        Some(&block_hash),
        Some("0xtxmigration"),
        Some(0),
        CanonicalityState::Canonical,
    );
    event.event_kind = "MigrationApplied".into();
    event.source_family = "ens_v2_migration_l1".into();
    event.derivation_kind = "ens_v2_migration".into();
    event.before_state = json!({});
    event.after_state = json!({"transition_id":"alice-migration"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await
}

#[tokio::test]
async fn v2_get_name_reports_authority_arm_without_migration_transition() -> Result<()> {
    let payload = v2_name_record_payload("/v1/names/Alice.eth").await?;
    assert_eq!(payload["data"]["authority"], "ens_v1");
    assert!(payload["data"].get("migrated_at").is_none());
    let unbound = v2_alice_state_payload("/v1/names/Alice.eth", AliceInputState::Unbound).await?;
    assert!(unbound["data"].get("authority").is_none());
    assert!(unbound["data"].get("migrated_at").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_and_lookup_report_migrated_at_from_the_migration_proof() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_state_inputs(&database, AliceInputState::Registry).await?;
    stamp_v2_alice_migration_transition(&database, 21_000_002).await?;

    let payload = v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth").await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("authority"), Some(&json!("ens_v2")));
    assert_eq!(data.get("migrated_at"), Some(&json!("2024-01-02T03:04:05Z")));

    let response = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"id": "alice", "name": "alice.eth"}]}),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let lookup: Value = read_json(response).await?;
    let record = &lookup["data"][0]["record"];
    assert_eq!(record["authority"], json!("ens_v2"));
    assert_eq!(record["migrated_at"], json!("2024-01-02T03:04:05Z"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_include_counts_reports_subname_and_record_counts() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;

    let plain = v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth").await?;
    assert!(plain["data"].get("subname_count").is_none());
    assert!(plain["data"].get("record_count").is_none());

    let counted =
        v2_name_record_payload_for_database(&database, "/v1/names/Alice.eth?include=counts")
            .await?;
    assert_eq!(counted["data"]["subname_count"], json!(0));
    assert_eq!(counted["data"]["record_count"], json!(4));
    assert!(counted["data"].get("event_count").is_none());

    let rejected = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?include=records")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_include_counts_counts_direct_subnames_of_the_parent() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let parent =
        v2_name_record_payload_for_database(&database, "/v1/names/parent.eth?include=counts")
            .await?;
    assert_eq!(parent["data"]["subname_count"], json!(3));

    let subnames =
        v2_subnames_payload_for_database(&database, "/v1/names/parent.eth/subnames?page_size=2")
            .await?;
    assert_eq!(subnames["page"]["total_count"], json!(3));
    assert_eq!(subnames["page"]["has_more"], json!(true));
    assert_eq!(subnames["data"].as_array().map(Vec::len), Some(2));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_serves_inventory_mirrored_from_ensv1() -> Result<()> {
    // Project publishes a name bound to a declared ENSv1 mirror resolver with the same name's
    // ENSv1 inventory and `provenance.mirror`; the route serves it like any supported inventory.
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    let payload = v2_mirror_records_payload(
        "/v1/names/alice.eth/records?keys=addr:60,text:description&include=inventory",
        "alice.eth",
        MIRROR,
        MirrorFixtureSource::Exact,
    )
    .await?;
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(payload["data"]["resolver"]["address"], json!(MIRROR));
    assert_eq!(payload["data"]["records"]["addr:60"]["status"], json!("ok"));
    assert_eq!(
        payload["data"]["records"]["addr:60"]["value"],
        json!("0x0000000000000000000000000000000000000def")
    );
    assert_eq!(
        payload["data"]["records"]["text:description"],
        json!({"status": "ok", "value": "Alice profile"})
    );
    let known = payload["data"]["inventory"]["known_keys"]
        .as_array()
        .expect("inventory known keys");
    assert!(known.contains(&json!("addr:60")) && known.contains(&json!("text:description")));
    assert!(payload["data"].get("mirror").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_refuses_a_mirror_whose_ancestor_resolver_is_rejected() -> Result<()> {
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    let payload_for = |uri: &'static str| {
        v2_mirror_records_payload(uri, "alice.eth", MIRROR, MirrorFixtureSource::Ancestor)
    };

    // Explicit indexed keys answer unsupported with the existing public mirror reason, and the
    // name keeps the mirror as its resolver.
    let payload = payload_for(
        "/v1/names/alice.eth/records?source=indexed&keys=addr:60,text:description&include=inventory",
    )
    .await?;
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(payload["data"]["resolver"]["address"], json!(MIRROR));
    let refused = json!({
        "status": "unsupported",
        "unsupported_reason": "mirrored_resolver_not_projected"
    });
    assert_eq!(
        payload["data"]["records"],
        json!({"addr:60": refused, "text:description": refused})
    );
    // `include=inventory` serializes key summaries only; the internal marker stays in the
    // persisted provenance.
    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": [],
            "unset_keys": [],
            "unsupported_keys": ["addr:60", "text:description"],
            "abi_content_types": null,
            "abi_unsupported_reason": "inventory_not_authoritative"
        })
    );
    let serialized = payload.to_string();
    for internal in [
        "ancestor_resolver_not_extended",
        "mirrored_unsupported_reason",
        "provenance",
    ] {
        assert!(
            !serialized.contains(internal),
            "{internal} leaked: {payload:#}"
        );
    }

    // Without keys the default set comes from the row's selectors, entries and gaps, all empty,
    // so no unsupported keys are manufactured.
    let payload = payload_for("/v1/names/alice.eth/records").await?;
    assert_eq!(payload["data"]["records"], json!({}), "{payload:#}");
    assert_eq!(payload["data"]["resolver"]["address"], json!(MIRROR));

    // `source=auto` with keys keeps the ordinary fallback to verified execution; this fixture
    // cannot execute it, so the answer is the existing verified contract, not an indexed value.
    let payload = payload_for("/v1/names/alice.eth/records?source=auto&keys=addr:60").await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"]["unsupported_reason"],
        json!("verified_records_not_supported")
    );
    Ok(())
}

#[tokio::test]
async fn v2_subname_filtered_totals_match_every_page_and_fixed_expiry_time() -> Result<()> {
    let (database, _) = v2_subnames_payload("/v1/names/parent.eth/subnames").await?;
    for filter in [
        "q=a",
        "q=missing",
        "include_expired=false",
        "q=b&include_expired=false",
    ] {
        let base = format!("/v1/names/parent.eth/subnames?{filter}");
        let all = v2_subnames_payload_for_database(&database, &base).await?;
        let expected = all["data"].as_array().unwrap().len();
        let mut seen = Vec::new();
        let mut uri = format!("{base}&page_size=1");
        loop {
            let page = v2_subnames_payload_for_database(&database, &uri).await?;
            assert_eq!(page["page"]["total_count"], json!(expected));
            seen.extend(v2_subname_names(&page));
            let Some(cursor) = page["page"]["next_cursor"].as_str() else {
                break;
            };
            uri = format!("{base}&page_size=1&cursor={cursor}");
        }
        assert_eq!(seen.len(), expected);
    }
    let at = bigname_storage::parse_rfc3339_utc_timestamp("2026-01-01T00:00:00Z")?;
    let filter = bigname_storage::ChildrenCurrentPageFilter {
        include_expired: false,
        evaluated_at: Some(at),
        ..Default::default()
    };
    let first = bigname_storage::load_children_current_page_filtered(
        &database.pool,
        &bigname_storage::logical_name_id_for_name("ens", "parent.eth"),
        &filter,
        None,
        1,
    )
    .await?;
    let next = bigname_storage::load_children_current_page_filtered(
        &database.pool,
        &bigname_storage::logical_name_id_for_name("ens", "parent.eth"),
        &filter,
        first.next_cursor.as_ref(),
        1,
    )
    .await?;
    assert_eq!(first.total_count, next.total_count);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_include_counts_rejects_same_height_republication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let baseline = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
    let position = &baseline["meta"]["as_of"]["1"];
    let block = position["block_number"].as_i64().expect("block number");
    let hash = position["block_hash"].as_str().expect("block hash");
    let time = position["timestamp"].as_str().expect("timestamp");
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state();
    let request = tokio::spawn(async move {
        app_router(state).oneshot(Request::builder()
            .uri("/v1/names/alice.eth?include=counts")
            .body(Body::empty()).expect("request must build")).await
    });
    control.wait_until_reached().await;
    seed_schema_v2_ens_lookup_head(&database.pool, block, hash, time).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, hash).await?;
    control.resume().await;
    let response = request.await??;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], "stale");
    assert!(payload.get("data").is_none());
    let fresh = v2_name_record_payload_for_database(&database,
        "/v1/names/alice.eth?include=counts").await?;
    assert_eq!(fresh["data"]["record_count"], json!(4));
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_include_counts_rejects_historical_count_mismatch() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let baseline = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
    let token = baseline["meta"]["as_of_token"].as_str().expect("snapshot token");
    let position = &baseline["meta"]["as_of"]["1"];
    let block = position["block_number"].as_i64().expect("block number");
    let time = bigname_storage::parse_rfc3339_utc_timestamp(
        position["timestamp"].as_str().expect("timestamp"))?;
    let later = crate::v2::format_timestamp(time + std::time::Duration::from_secs(1));
    seed_schema_v2_ens_lookup_head(&database.pool, block + 1, "0xcounts-new-publication", &later).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block + 1, "0xcounts-new-publication").await?;
    let response = app_router(database.app_state()).oneshot(Request::builder()
        .uri(format!("/v1/names/alice.eth?at={token}&include=counts"))
        .body(Body::empty()).expect("request must build")).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], "stale");
    assert_eq!(payload["error"]["code"], "stale");
    assert!(payload.get("data").is_none());
    database.cleanup().await
}

#[tokio::test]
async fn v2_subname_continuation_excludes_unpublished_subregistry_pointer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let first = v2_subnames_payload_for_database(&database, "/v1/names/alpha.eth/subnames?page_size=1").await?;
    let cursor = first["page"]["next_cursor"].as_str().unwrap();
    upsert_phase_raw_blocks(&database.pool, &[raw_block(REGISTRY_CHAIN_ID,
        "0xregistry84", None, 84, 1_700_000_084)]).await?;
    let event = registry_event("unpublished-child-pointer", Some(&registry_logical_name_id("two.alpha.eth")),
        "SubregistryChanged", 84, ALPHA_REGISTRY,
        json!({"source_event":"SubregistryUpdated", "subregistry":ONE_REGISTRY}));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    let next = v2_subnames_payload_for_database(&database,
        &format!("/v1/names/alpha.eth/subnames?page_size=1&cursor={cursor}")).await?;
    assert_eq!(next["data"][0]["name"], json!("two.alpha.eth"));
    assert!(next["data"][0].get("subregistry").is_none());
    assert_eq!(next["page"]["total_count"], first["page"]["total_count"]);
    seed_schema_v2_ens_lookup_head(&database.pool, 84, "0xregistry84", "2023-11-14T22:14:44Z").await?;
    rebuild_fixture_families(&database.pool, REGISTRY_CHAIN_ID, 84, "0xregistry84").await?;
    let published = v2_subnames_payload_for_database(&database,
        "/v1/names/alpha.eth/subnames?q=two").await?;
    assert_eq!(published["data"][0]["subregistry"]["address"], json!(ONE_REGISTRY));
    database.cleanup().await
}
