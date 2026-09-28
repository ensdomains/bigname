#[tokio::test]
async fn v2_lookup_rejects_invalid_request_shapes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for (uri, body) in [
        (
            "/v1/lookup",
            json!({"inputs": [{"id": "both", "name": "alice.eth", "address": "0x0000000000000000000000000000000000000abc"}]}),
        ),
        ("/v1/lookup", json!({"inputs": [{"id": "neither"}]})),
        ("/v1/lookup", json!({"inputs": [{"id": "", "name": "alice.eth"}]})),
        (
            "/v1/lookup",
            json!({"profile": "detail", "extra": true, "inputs": []}),
        ),
        (
            "/v1/lookup",
            json!({"namespace": "ens", "inputs": [{"id": "addr", "address": "0x0000000000000000000000000000000000000abc"}]}),
        ),
    ] {
        let response = v2_lookup_response_for_database(&database, uri, body).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"));
    }

    for uri in [
        "/v1/lookup?at=2026-04-17T00:00:00Z",
        "/v1/lookup?finality=safe",
    ] {
        let response = v2_lookup_response_for_database(
            &database,
            uri,
            json!({"inputs": [{"id": "name", "name": "alice.eth"}]}),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"));
    }

    let oversized_inputs = (0..=1000)
        .map(|index| json!({"id": format!("name-{index}"), "name": "alice.eth"}))
        .collect::<Vec<_>>();
    let response = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({"inputs": oversized_inputs}),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_validates_reverse_inputs_before_deployment_readiness() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let response = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": "not-an-address"}]}),
        &[],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    let response = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": "0x0000000000000000000000000000000000000abc"}]}),
        &[],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("conflict"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_empty_public_namespace_set_takes_precedence_over_bound_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let first = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": address, "page_size": 1}]}),
        &["ens", "basenames"],
    )
    .await?;
    assert_eq!(first.status(), StatusCode::OK);
    let first_payload: Value = read_json(first).await?;
    let cursor = first_payload["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("co-deployed reverse page must include a cursor");

    let response = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": address, "page_size": 1, "cursor": cursor}]}),
        &[],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("conflict")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_name_only_refuses_while_interpret_redo_is_in_progress()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_lookup_base_head(&database).await?;
    for (chain, deployment) in [
        ("ethereum-mainnet", "ens_v1"),
        ("ethereum-sepolia", "ens_v2_sepolia_20260915"),
    ] {
        database
            .insert_manifest(
                "ens",
                "ens_registry",
                chain,
                deployment,
                1,
                "active",
                "ensip15@ens-normalize-0.1.1",
            )
            .await?;
    }
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    assert!(crate::v2::support::derive_public_namespace_set(&state).await.is_err());
    database
        .simulate_interpret_redo_begin("base-mainnet", "recompute_flags")
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "namespace": "basenames",
                        "inputs": [{"name": "missing.base.eth"}]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("lookup request must build"),
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload:#}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_bare_reverse_discloses_a_redo_suppressed_request_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_public_authority(&database).await?;
    database
        .simulate_interpret_redo_begin("base-mainnet", "recompute_flags")
        .await?;

    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/lookup")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({"inputs": [{"address": address}]}))
                    .expect("body must serialize"),
            ))
            .expect("lookup request must build"),
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    assert!(payload["meta"]["as_of"]["1"].is_object());
    assert!(payload["meta"]["as_of"].get("8453").is_none());
    assert_eq!(
        payload["meta"]["as_of_completeness"]["8453"],
        json!({
            "completeness": "unsupported",
            "unsupported_reason": "temporarily_unavailable"
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_bare_reverse_returns_conflict_when_every_public_namespace_is_suppressed()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_public_authority(&database).await?;
    for chain_id in ["ethereum-mainnet", "base-mainnet"] {
        database
            .simulate_interpret_redo_begin(chain_id, "recompute_flags")
            .await?;
    }

    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/lookup")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({"inputs": [{"address": address}]}))
                    .expect("body must serialize"),
            ))
            .expect("lookup request must build"),
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload:#}");
    assert_eq!(payload["error"]["code"], json!("conflict"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_exact_scope_fallback_refuses_while_interpret_redo_is_in_progress() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_public_authority(&database).await?;
    database
        .simulate_interpret_redo_begin("base-mainnet", "recompute_flags")
        .await?;

    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/lookup")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "inputs": [
                        {"id": "reverse", "address": address},
                        {"id": "forward", "name": "missing.base.eth"}
                    ]
                }))
                .expect("body must serialize"),
            ))
            .expect("mixed lookup request must build"),
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload:#}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_forward_results_are_in_order_with_head_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:case.eth",
        "Case.eth",
        "case.eth",
        "namehash:case.eth",
        Uuid::from_u128(0x5a0101),
        Uuid::from_u128(0x5a0102),
        Uuid::from_u128(0x5a0103),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "namespace": "public",
            "inputs": [
                {"id": "hit", "name": "Case.eth"},
                {"id": "miss", "name": "missing.eth"},
                {"id": "bad", "name": "bad name.eth"}
            ]
        }),
    )
    .await?;

    assert!(payload.get("page").is_none());
    assert!(payload["data"].is_array());
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 38,
            "block_hash": "0xname26",
            "timestamp": "2026-04-17T00:00:38Z"
        })
    );
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("lookup response must include meta.as_of_token");
    let replay = v2_get_json(&database, &format!("/v1/names/case.eth?at={token}")).await?;
    assert_eq!(replay["meta"]["as_of"], payload["meta"]["as_of"]);
    assert_eq!(replay["meta"]["as_of_token"], payload["meta"]["as_of_token"]);

    assert_eq!(payload["data"][0]["input"], json!({"id": "hit", "name": "Case.eth"}));
    assert_eq!(payload["data"][0]["kind"], json!("name"));
    assert_eq!(payload["data"][0]["status"], json!("ok"));
    assert_eq!(
        payload["data"][0]["normalization"],
        json!({
            "changed": true,
            "input_name": "Case.eth",
            "reason": "case_normalized"
        })
    );
    assert_eq!(payload["data"][0]["record"]["name"], json!("case.eth"));
    assert_eq!(payload["data"][0]["record"]["display_name"], json!("case.eth"));
    assert_eq!(payload["data"][0]["record"]["namespace"], json!("ens"));
    assert_eq!(payload["data"][0]["record"]["status"], json!("ok"));
    assert_eq!(
        payload["data"][0]["record"]["addresses"]["60"],
        json!(address)
    );
    assert_eq!(payload["data"][0]["record"]["primary_address"], json!(address));
    assert!(payload["data"][0].get("records").is_none());

    let omitted_id = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "case.eth"}]}),
    )
    .await?;
    assert_eq!(omitted_id["data"][0]["input"], json!({"name": "case.eth"}));
    assert_eq!(omitted_id["data"][0]["status"], json!("ok"));

    assert_eq!(payload["data"][1]["input"]["id"], json!("miss"));
    assert_eq!(payload["data"][1]["status"], json!("not_found"));
    assert!(payload["data"][1].get("record").is_none());
    assert_eq!(payload["data"][2]["input"]["id"], json!("bad"));
    assert_eq!(payload["data"][2]["status"], json!("invalid_name"));
    assert_eq!(
        payload["data"][2]["normalization"]["reason"],
        json!("invalid_normalized_name")
    );

    let feed = v2_lookup_json(
        &database,
        json!({"profile": "feed", "inputs": [{"id": "feed", "name": "case.eth"}]}),
    )
    .await?;
    let feed_record = feed["data"][0]["record"]
        .as_object()
        .expect("feed record must be an object");
    assert_eq!(feed_record.get("name"), Some(&json!("case.eth")));
    assert!(feed_record.get("addresses").is_none());
    assert!(feed_record.get("owner").is_none());

    let detail = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "detail", "name": "case.eth"}]}),
    )
    .await?;
    let shadow = v2_lookup_json(
        &database,
        json!({"profile": "shadow", "inputs": [{"id": "detail", "name": "case.eth"}]}),
    )
    .await?;
    assert_eq!(shadow["data"][0]["record"], detail["data"][0]["record"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_withholds_resolver_without_projected_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    // alice.eth keeps its retained registration, registry and resolver inputs, but Interpret
    // observed no binding for its surface, so no authority is selected for it.
    sqlx::query("DELETE FROM surface_bindings WHERE surface_binding_id = $1")
        .bind(Uuid::from_u128(0x5a0203))
        .execute(&database.pool)
        .await?;
    republish_mainnet_fixture(&database).await?;

    let forward = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let record = &forward["data"][0]["record"];
    assert_eq!(record["status"], json!("unsupported"));
    assert_eq!(
        record["unsupported_reason"],
        json!("current_authority_not_projected")
    );
    // The record keeps the detail shape for this reason. With no selected binding there is no
    // served resolver, so neither the pointer nor the records written on it are served.
    assert_eq!(record["addresses"]["60"], Value::Null, "{record}");
    assert!(record.get("resolver").is_none());

    // Reverse detail shares build_detail_record with the forward path, so the
    // forward assertion above covers the builder for both. Reverse membership
    // additionally excludes unsupported name rows outright (readable_names
    // requires support_status = 'supported'), so the row cannot reach the
    // builder from a reverse input while its authority is not projected.
    let reverse = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"address": address}]}),
    )
    .await?;
    let records = reverse["data"][0]["records"]
        .as_array()
        .expect("reverse detail lookup must return records");
    assert!(
        records
            .iter()
            .all(|record| record["name"] != json!("alice.eth")),
        "unprojected-authority row must be absent from reverse membership"
    );
    let bob = records
        .iter()
        .find(|record| record["name"] == json!("bob.eth"))
        .expect("reverse records must include bob.eth");
    assert_eq!(
        bob["resolver"],
        json!({"chain_id": 1, "address": address})
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_serves_a_root_registry_pointer_without_projected_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // An ENSv2 TLD whose root-registry token has a resolver pointer but no observed registration.
    seed_unbound_name_inputs(&database, "eth", true).await?;

    let forward = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "eth"}]}),
    )
    .await?;
    let record = &forward["data"][0]["record"];
    assert_eq!(record["status"], json!("unsupported"), "{record}");
    assert_eq!(
        record["unsupported_reason"],
        json!("current_authority_not_projected")
    );
    assert_eq!(
        record["resolver"],
        json!({"chain_id": 1, "address": "0x0000000000000000000000000000000000000abc"}),
        "{record}"
    );
    assert_eq!(record["registration_status"], json!("unregistered"));
    assert!(record.get("registration_id").is_none(), "{record}");
    assert!(record.get("authority").is_none_or(Value::is_null), "{record}");
    assert_eq!(
        record["addresses"]["60"],
        json!("0x0000000000000000000000000000000000000def")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_withholds_retained_inventory_for_released_tombstone() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // The released lease keeps its resolver pointer and record writes as retained inputs: a
    // released tombstone must not serve them.
    seed_alice_state_inputs(&database, AliceInputState::Released).await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let record = &payload["data"][0]["record"];
    assert_eq!(record["status"], json!("ok"));
    assert_eq!(record["registration_status"], json!("released"));
    assert!(record.get("resolver").is_none());
    assert!(record.get("addresses").is_none());
    assert!(record.get("text_records").is_none());
    assert!(record.get("content_hash").is_none());
    assert!(record.get("primary_address").is_none());
    assert_eq!(
        record["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn wrapped_name_lookup_uses_the_registrar_lease_handle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // perms.eth was registered, then wrapped: its binding is the NameWrapper resource and the
    // wrap recorded the registrar lease.
    let (_wrapper_resource_id, registrar_resource_id) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "perms.eth"}]}),
    )
    .await?;
    assert_eq!(
        payload["data"][0]["record"]["registration_id"],
        json!(registrar_resource_id.to_string()),
        "batch lookup returned the NameWrapper resource instead of the registrar lease"
    );

    // A wrapped subname has no registrar lease, so Project selects no registration resource
    // and the bound NameWrapper resource stays the handle.
    let subname_wrapper = Uuid::from_u128(0x5a_0505);
    seed_wrapped_subname_inputs(&database, "sub.perms.eth", subname_wrapper).await?;
    let subname = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "sub.perms.eth"}]}),
    )
    .await?;
    assert_eq!(
        subname["data"][0]["record"]["registration_id"],
        json!(subname_wrapper.to_string())
    );

    database.cleanup().await
}

#[tokio::test]
async fn released_name_serves_its_lapsed_holder_only_in_the_lapsed_block() -> Result<()> {
    const HOLDER: &str = "0x0000000000000000000000000000000000000abc";
    let database = TestDatabase::new_migrated().await?;
    // Two registrar leases; the first lapses and is released, closing its binding.
    let lease = seed_names_registration(
        &database,
        "ens",
        "lapsed-lease.eth",
        91,
        "2023-02-02T00:00:00Z",
        1_700_000_000,
        HOLDER,
        HOLDER,
    )
    .await?;
    seed_names_registration(
        &database,
        "ens",
        "live-lease.eth",
        92,
        "2024-02-02T00:00:00Z",
        1_900_000_000,
        HOLDER,
        HOLDER,
    )
    .await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block("ethereum-mainnet", "0xlookup-released", None, 93, 1_707_776_000)],
    )
    .await?;
    sqlx::query("UPDATE surface_bindings SET active_to = to_timestamp(1707776000) WHERE resource_id = $1")
        .bind(lease)
        .execute(&database.pool)
        .await?;
    let mut release = history_event(
        "lookup-lapsed-release",
        Some(&bigname_storage::logical_name_id_for_name("ens", "lapsed-lease.eth")),
        Some(lease),
        Some("ethereum-mainnet"),
        Some(93),
        Some("0xlookup-released"),
        Some("0xrelease"),
        Some(0),
        CanonicalityState::Canonical,
    );
    release.event_kind = "RegistrationReleased".into();
    release.source_family = "ens_v1_registrar_l1".into();
    release.before_state =
        json!({"registrant":HOLDER, "authority_kind":"registrar", "authority_key":"registrar:lapsed"});
    release.after_state = json!({"expiry":1_700_000_000, "released_at":1_707_776_000});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[release]).await?;
    publish_v2_names_fixture(&database).await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [
            {"name": "lapsed-lease.eth"}, {"name": "live-lease.eth"}
        ]}),
    )
    .await?;
    let expected_lapsed = json!({
        "registrant": HOLDER,
        "held_through": "registrar",
        "released_at": "2024-02-12T22:13:20Z",
    });
    let lapsed = &payload["data"][0]["record"];
    assert_eq!(lapsed["registration_status"], json!("released"), "{lapsed:?}");
    assert_eq!(lapsed["registration_id"], json!(lease.to_string()));
    assert_eq!(lapsed["expires_at"], json!("2023-11-14T22:13:20Z"));
    assert!(lapsed.get("registrant").is_none(), "{lapsed:?}");
    assert!(lapsed.get("owner").is_none(), "{lapsed:?}");
    assert_eq!(lapsed["lapsed_registration"], expected_lapsed);
    let live = &payload["data"][1]["record"];
    assert!(live.get("lapsed_registration").is_none(), "{live:?}");

    let detail = v2_name_record_payload_for_database(&database, "/v1/names/lapsed-lease.eth")
        .await?;
    let record = &detail["data"];
    assert_eq!(record["registration_status"], json!("released"), "{record:?}");
    assert_eq!(record["registration_id"], json!(lease.to_string()));
    assert_eq!(record["expires_at"], json!("2023-11-14T22:13:20Z"));
    assert!(record.get("registrant").is_none(), "{record:?}");
    assert!(record.get("owner").is_none(), "{record:?}");
    assert_eq!(record["lapsed_registration"], expected_lapsed);
    let live = v2_name_record_payload_for_database(&database, "/v1/names/live-lease.eth").await?;
    assert!(live["data"].get("lapsed_registration").is_none(), "{live:?}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_ignores_stale_audit_inventory_for_reservation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // The ENSv2 label was reserved after its registration: the expired resource keeps its
    // resolver writes as retained audit inputs.
    seed_alice_state_inputs(&database, AliceInputState::Reserved).await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let record = &payload["data"][0]["record"];
    assert_eq!(record["registration_status"], json!("unregistered"));
    assert!(record.get("registration_id").is_none());
    assert!(record.get("resolver").is_none());
    assert!(record.get("addresses").is_none());
    assert!(record.get("text_records").is_none());
    assert!(record.get("content_hash").is_none());
    assert!(record.get("primary_address").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_flattens_phase_writer_byte_values() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:bytes.eth",
        "bytes.eth",
        "bytes.eth",
        "namehash:bytes.eth",
        Uuid::from_u128(0x5a0104),
        Uuid::from_u128(0x5a0105),
        Uuid::from_u128(0x5a0106),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    // A non-EVM coin write keeps its raw address bytes and a contenthash write its raw hash, as
    // the resolver adapter records AddressChanged and ContenthashChanged.
    let node = bigname_lookup::ens_namehash_hex("bytes.eth")?;
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "bytes.eth",
        "0x0000000000000000000000000000000000000abc",
        38,
        "0xname26",
        &[
            json!({"source_event":"AddressChanged", "node":node, "record_key":"addr:0",
                "record_family":"addr", "selector_key":"0", "coin_type":"0",
                "address_bytes_hex":"0x001122"}),
            json!({"source_event":"ContenthashChanged", "node":node, "record_key":"contenthash",
                "record_family":"contenthash", "selector_key":null,
                "contenthash_hex":"0xe3010170"}),
        ],
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 38, "0xname26").await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "bytes.eth"}]}),
    )
    .await?;

    assert_eq!(payload["data"][0]["record"]["addresses"]["0"], json!("0x001122"));
    assert_eq!(
        payload["data"][0]["record"]["content_hash"],
        json!("0xe3010170")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_marks_unsupported_phase_inventory_fields() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // The name's resolver is a dynamic ENSv2 resolver whose implementation is not identified,
    // so its retained record writes are refused.
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let record = &payload["data"][0]["record"];

    assert!(record.get("addresses").is_none());
    assert!(record.get("primary_address").is_none());
    assert!(record.get("text_records").is_none());
    assert!(record.get("content_hash").is_none());
    assert_eq!(
        record["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_include_inventory_serves_the_records_route_container() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:inventory-batch.eth",
        "inventory-batch.eth",
        "inventory-batch.eth",
        "namehash:inventory-batch.eth",
        Uuid::from_u128(0x5a0307),
        Uuid::from_u128(0x5a0308),
        Uuid::from_u128(0x5a0309),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    // One entry the row cannot serve: a text write whose value the event did not retain. It
    // partitions into unsupported_keys on both routes.
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "inventory-batch.eth",
        "0x0000000000000000000000000000000000000abc",
        38,
        "0xname26",
        &[family_fixture_record_write("text:url", None)],
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 38, "0xname26").await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "include": "inventory",
            "inputs": [
                {"name": "inventory-batch.eth"},
                {"name": "never-seeded.eth"},
                {"address": "0x0000000000000000000000000000000000000abc", "relation": "owner"}
            ]
        }),
    )
    .await?;
    let record = &payload["data"][0]["record"];
    let inventory = &record["inventory"];
    assert!(inventory["known_keys"].as_array().is_some_and(|keys| keys.contains(&json!("addr:60"))), "{record}");
    assert_eq!(inventory["unset_keys"], json!([]));
    assert_eq!(inventory["unsupported_keys"], json!(["text:url"]));
    assert!(record.get("addresses").is_some(), "{record}");

    // The container is the records route's, key for key.
    let response = v2_get_response(
        &database,
        "/v1/names/inventory-batch.eth/records?include=inventory",
    )
    .await?;
    let status = response.status();
    let records: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{records:#}");
    assert_eq!(records["data"]["inventory"], *inventory, "{records:#}");

    assert_eq!(payload["data"][1]["status"], json!("not_found"));
    assert!(payload["data"][1].get("record").is_none());
    let reverse_rows = payload["data"][2]["records"].as_array().expect("reverse rows");
    assert!(!reverse_rows.is_empty());
    assert!(reverse_rows.iter().all(|row| row.get("inventory").is_none()), "{reverse_rows:?}");

    // Without the include the container is absent, on the same rows.
    let plain = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "inventory-batch.eth"}]}),
    )
    .await?;
    assert!(plain["data"][0]["record"].get("inventory").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_include_inventory_lists_an_unsupported_row_under_unsupported_keys() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "include": "inventory", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let record = &payload["data"][0]["record"];
    assert_eq!(payload["data"][0]["status"], json!("ok"));
    assert!(record.get("addresses").is_none(), "{record}");
    let inventory = &record["inventory"];
    assert_eq!(inventory["known_keys"], json!([]));
    assert_eq!(inventory["unset_keys"], json!([]));
    assert!(inventory["unsupported_keys"].as_array().is_some_and(|keys| keys.contains(&json!("addr:60"))), "{record}");
    let records = v2_get_json(
        &database,
        "/v1/names/alice.eth/records?include=inventory",
    )
    .await?;
    assert_eq!(records["data"]["inventory"], *inventory, "{records:#}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_include_rejects_feed_and_unknown_expansions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    for body in [
        json!({"profile": "feed", "include": "inventory", "inputs": [{"name": "alice.eth"}]}),
        json!({"profile": "detail", "include": "lineage", "inputs": [{"name": "alice.eth"}]}),
        json!({"profile": "detail", "include": "inventory,counts", "inputs": [{"name": "alice.eth"}]}),
    ] {
        let response = v2_lookup_response_for_database(&database, "/v1/lookup", body).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"));
    }
    // An empty include is no include.
    let payload = v2_lookup_json(
        &database,
        json!({"profile": "feed", "include": "", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    assert_eq!(payload["data"][0]["status"], json!("not_found"));
    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_detail_withholds_record_values_from_unsupported_inventory() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // The retained inputs carry a successful addr:60 write behind an ENSv2 resolver whose
    // implementation is not an admitted profile.
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let record = &payload["data"][0]["record"];

    assert_eq!(payload["data"][0]["status"], json!("ok"));
    assert!(record.get("addresses").is_none(), "{record}");
    assert!(record.get("primary_address").is_none(), "{record}");
    assert!(record.get("text_records").is_none(), "{record}");
    assert!(record.get("content_hash").is_none(), "{record}");
    assert_eq!(
        record["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_serves_unchanged_phase_projection_after_head_advance() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:public-gap.eth",
        "public-gap.eth",
        "public-gap.eth",
        "namehash:public-gap.eth",
        Uuid::from_u128(0x5a0114),
        Uuid::from_u128(0x5a0115),
        Uuid::from_u128(0x5a0116),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    advance_v2_lookup_phase_only_ethereum_head(&database, 39, "0xlookup-phase-only").await?;
    // Project follows the new head; nothing about the name changed in that block.
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 39, "0xlookup-phase-only")
        .await?;

    let response = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"id": "public-gap", "name": "public-gap.eth"}]}),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"][0]["status"], json!("ok"));
    assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(39));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_ignores_invalid_phase_primary_claim() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::InvalidName,
        Some("bad name.eth"),
        false,
    )
    .await?;

    let payload = v2_lookup_json(&database, json!({"inputs": [{"address": address}]})).await?;

    assert_eq!(payload["data"][0]["status"], json!("ok"));
    for record in payload["data"][0]["records"]
        .as_array()
        .expect("reverse records must be an array")
    {
        assert_eq!(record["is_primary"], json!(false));
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_paginates_normalizable_phase_primary_claim() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    // A later reverse claim names the primary name in a spelling that normalizes to alice.eth.
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some("Alice.eth"),
        false,
    )
    .await?;

    let first = v2_lookup_json(
        &database,
        json!({"inputs": [{"address": address, "page_size": 1}]}),
    )
    .await?;
    assert_eq!(first["data"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(first["data"][0]["records"][0]["is_primary"], json!(true));
    let cursor = first["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor");

    let second = v2_lookup_json(
        &database,
        json!({"inputs": [{"address": address, "page_size": 1, "cursor": cursor}]}),
    )
    .await?;
    assert_eq!(second["data"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(second["data"][0]["page"]["has_more"], json!(false));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_rejects_head_reorg_before_project_republication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:reorg.eth",
        "reorg.eth",
        "reorg.eth",
        "namehash:reorg.eth",
        Uuid::from_u128(0x5a0121),
        Uuid::from_u128(0x5a0122),
        Uuid::from_u128(0x5a0123),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    // The served block 39 is only canonical (not yet safe), so a reorg can still replace it.
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES (
             'ethereum-mainnet', '0xlookup-before-reorg', 39,
             '2026-04-17T00:00:39Z'::timestamptz, 'canonical'
         )",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = '0xlookup-before-reorg',
             latest_block_number = 39,
             updated_at = now()
         WHERE chain_id = 'ethereum-mainnet'",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_number = 39, current_block_hash = '0xlookup-before-reorg'
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "inputs": [{"id": "reorg", "name": "reorg.eth"}]
                        }))
                        .expect("body must serialize"),
                    ))
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    // A reorg replaces the served block 39 and extends the new fork to 40. The head moves on
    // before Project republishes, so the served publication is no longer on the readable path.
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES (
             'ethereum-mainnet', '0xlookup-after-reorg', 40,
             '2026-04-17T00:00:40Z'::timestamptz, 'canonical'
         )",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = '0xlookup-after-reorg',
             latest_block_number = 40,
             updated_at = now()
         WHERE chain_id = 'ethereum-mainnet'",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xlookup-before-reorg'",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES (
             'ethereum-mainnet', '0xlookup-reorged-39', 39,
             '2026-04-17T00:00:39Z'::timestamptz, 'canonical'
         )",
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("lookup reorg request task panicked")?
        .context("lookup reorg request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_rejects_project_publication_between_selection_and_first_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:publication-race.eth",
        "publication-race.eth",
        "publication-race.eth",
        "namehash:publication-race.eth",
        Uuid::from_u128(0x5a0124),
        Uuid::from_u128(0x5a0125),
        Uuid::from_u128(0x5a0126),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    advance_v2_lookup_ethereum_head(&database, 39, "0xlookup-before-publication-race").await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_initial_validation_test_hooks::install(
            &database.lookup_pool,
        )
        .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "inputs": [{"name": "publication-race.eth"}]
                        }))
                        .expect("body must serialize"),
                    ))
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    // Project publishes the families at a new head while the request holds its selection.
    advance_mainnet_fixture_publication(&database, 40, "0xlookup-after-publication-race").await?;
    control.resume().await;

    let response = request_task
        .await
        .context("lookup publication-race request task panicked")?
        .context("lookup publication-race request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_internal_head_selection_error_is_sanitized() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = database.app_state();
    state.pool.close().await;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "namespace": "ens",
                        "inputs": [{"id": "name", "name": "alice.eth"}]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("v2 lookup closed-pool request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("internal_error"));
    assert_eq!(
        payload["error"]["message"],
        json!("failed to serve v2 request")
    );
    let error_body = payload["error"].to_string();
    for term in [
        "checkpoint",
        "chain_checkpoints",
        "chain_lineage",
        "stored",
        "lineage",
    ] {
        assert!(
            !error_body.contains(term),
            "lookup internal error leaked storage detail {term}: {error_body}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_tokens_remain_snapshot_capable_while_collections_reject_at() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 38,
                "block_hash": "0xname26",
                "timestamp": "2026-04-17T00:00:38Z"
            },
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 88,
                "block_hash": "0xlookup-base-head",
                "timestamp": "2026-04-17T00:01:28Z"
            }
        }))
        .await?;
    republish_mainnet_fixture(&database).await?;
    republish_fixture_chain(&database, "base-mainnet").await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "namespace": "ens",
            "inputs": [{"id": "miss", "name": "missing.eth"}]
        }),
    )
    .await?;

    assert_eq!(payload["data"][0]["status"], json!("not_found"));
    assert!(payload["meta"]["as_of"]["1"].is_object());
    assert!(payload["meta"]["as_of"].get("8453").is_none());
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("lookup response must include meta.as_of_token");

    let replay = v2_get_response(
        &database,
        &format!("/v1/names/missing.eth?at={token}"),
    )
    .await?;
    assert_eq!(replay.status(), StatusCode::NOT_FOUND);

    let collection = v2_get_response(
        &database,
        &format!("/v1/search?q=missing&namespace=ens&at={token}"),
    )
    .await?;
    assert_eq!(collection.status(), StatusCode::BAD_REQUEST);
    let collection_error: Value = read_json(collection).await?;
    assert_eq!(
        collection_error["error"]["message"],
        json!("at is not supported because collection routes read latest state")
    );

    let union_replay =
        v2_get_response(&database, &format!("/v1/search?q=missing&at={token}")).await?;
    assert_eq!(union_replay.status(), StatusCode::BAD_REQUEST);

    let public_payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "namespace": "public",
            "inputs": [
                {"id": "ens-miss", "name": "missing.eth"},
                {"id": "basenames-miss", "name": "missing.base.eth"}
            ]
        }),
    )
    .await?;
    assert!(public_payload["meta"]["as_of"]["1"].is_object());
    assert!(public_payload["meta"]["as_of"]["8453"].is_object());
    let public_token = public_payload["meta"]["as_of_token"]
        .as_str()
        .expect("public lookup response must include meta.as_of_token");
    let public_replay =
        v2_get_response(&database, &format!("/v1/search?q=missing&at={public_token}")).await?;
    assert_eq!(public_replay.status(), StatusCode::BAD_REQUEST);
    let public_replay_error: Value = read_json(public_replay).await?;
    assert_eq!(
        public_replay_error["error"]["message"],
        json!("at is not supported because collection routes read latest state")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_serves_reverse_pagination_after_unrelated_head_advance() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let first_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "id": "addr",
                "address": address,
                "page_size": 1
            }]
        }),
    )
    .await?;

    assert_eq!(first_page["data"][0]["kind"], json!("address"));
    assert_eq!(first_page["data"][0]["status"], json!("ok"));
    assert_eq!(
        first_page["data"][0]["input"],
        json!({
            "id": "addr",
            "address": address,
            "coin_type": 60,
            "page_size": 1
        })
    );
    assert_eq!(first_page["data"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(first_page["data"][0]["records"][0]["is_primary"], json!(true));
    // The holder of an unwrapped lease is also its registrant.
    assert_eq!(
        first_page["data"][0]["records"][0]["relations"],
        json!(["owner", "registrant"])
    );
    assert_eq!(first_page["data"][0]["page"]["cursor"], Value::Null);
    assert_eq!(first_page["data"][0]["page"]["page_size"], json!(1));
    assert_eq!(first_page["data"][0]["page"]["total_count"], json!(2));
    assert_eq!(first_page["data"][0]["page"]["has_more"], json!(true));
    let cursor = first_page["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must include next_cursor");

    advance_mainnet_fixture_publication(&database, 43, "0xlookup-advanced").await?;

    let second_page = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({
            "profile": "detail",
            "inputs": [{
                "id": "addr",
                "address": address,
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    assert_eq!(second_page.status(), StatusCode::OK);
    let second_page: Value = read_json(second_page).await?;
    assert_eq!(second_page["data"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(second_page["data"][0]["page"]["has_more"], json!(false));
    assert_eq!(second_page["meta"]["as_of"]["1"]["block_number"], json!(43));

    let mismatch = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({
            "profile": "detail",
            "inputs": [{
                "id": "wrong",
                "address": "0x0000000000000000000000000000000000000def",
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    assert_eq!(mismatch.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(mismatch).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_serves_the_batch_when_a_primary_claim_no_longer_normalizes()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    // A reverse record whose name does not normalize is one row's defect. The batched reverse
    // read must still answer, marking nothing primary, rather than failing every input.
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::InvalidName,
        Some("alice..eth"),
        false,
    )
    .await?;

    let payload = v2_lookup_json(&database, json!({"inputs": [{"address": address}]})).await?;

    assert_eq!(payload["data"][0]["status"], json!("ok"));
    let records = payload["data"][0]["records"]
        .as_array()
        .expect("reverse lookup records must be an array");
    assert!(!records.is_empty());
    assert!(records.iter().all(|record| record["is_primary"] == json!(false)));

    database.cleanup().await?;
    Ok(())
}

async fn paused_reverse_lookup(
    database: &TestDatabase,
    address: &str,
) -> Result<(
    tokio::task::JoinHandle<Result<Response>>,
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<tokio::sync::Notify>,
)> {
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let state = database.app_state();
    let address = address.to_owned();
    let (task_reached, task_resume) = (
        std::sync::Arc::clone(&reached),
        std::sync::Arc::clone(&resume),
    );
    let task =
        tokio::spawn(async move {
            bigname_storage::families::name::seams::with_pause_after_publication(
                task_reached,
                task_resume,
                async move {
                    Ok(app_router(state).oneshot(Request::builder().method("POST").uri("/v1/lookup")
                    .header("content-type", "application/json").body(Body::from(json!({
                        "profile":"detail", "inputs":[{"address":address,"page_size":1}]
                    }).to_string()))?).await?)
                },
            )
            .await
        });
    tokio::time::timeout(std::time::Duration::from_secs(10), reached.notified())
        .await
        .context("reverse lookup did not reach its family snapshot")?;
    Ok((task, reached, resume))
}

async fn finish_paused_reverse_lookup(
    mut task: tokio::task::JoinHandle<Result<Response>>,
    reached: std::sync::Arc<tokio::sync::Notify>,
    resume: std::sync::Arc<tokio::sync::Notify>,
) -> Result<Response> {
    resume.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            tokio::select! {
                result = &mut task => return result.context("reverse lookup task")?,
                () = reached.notified() => resume.notify_one(),
            }
        }
    })
    .await
    .context("reverse lookup did not finish")?
}

#[tokio::test]
async fn v2_lookup_reverse_keeps_primary_order_and_flag_coherent_across_projection_rewrite()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    let (task, reached, resume) = paused_reverse_lookup(&database, address).await?;
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some("bob.eth"),
        true,
    )
    .await?;
    let response = finish_paused_reverse_lookup(task, reached, resume).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refused: Value = read_json(response).await?;
    assert_eq!(refused["error"]["code"], "stale");
    assert!(refused.get("data").is_none());
    let fresh = v2_lookup_json(
        &database,
        json!({"profile":"detail", "inputs":[{"address":address,"page_size":1}]}),
    )
    .await?;
    assert_eq!(fresh["data"][0]["records"][0]["name"], "bob.eth");
    assert_eq!(fresh["data"][0]["records"][0]["is_primary"], true);
    let cursor = fresh["data"][0]["page"]["next_cursor"]
        .as_str()
        .context("fresh cursor")?;
    let next = v2_lookup_json(
        &database,
        json!({"profile":"detail", "inputs":[{"address":address,"page_size":1,"cursor":cursor}]}),
    )
    .await?;
    assert_eq!(lookup_record_names(&next), vec!["alice.eth"]);
    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_reverse_uses_candidate_name_for_order_flag_and_cursor_across_name_rewrite()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    let (task, reached, resume) = paused_reverse_lookup(&database, address).await?;
    let replacement_resource = Uuid::from_u128(0x5a0391);
    sqlx::query("UPDATE surface_bindings SET active_to = '2026-04-17T00:00:44Z' WHERE surface_binding_id = $1")
        .bind(Uuid::from_u128(0x5a0203)).execute(&database.pool).await?;

    // A new registration binding for the same normalized name replaces the selected identity.
    seed_identity_name(
        &database,
        "ens:alice.eth",
        "alice.eth",
        "alice.eth",
        "unused",
        replacement_resource,
        Uuid::from_u128(0x5a0392),
        Uuid::from_u128(0x5a0393),
        address,
        bigname_storage::AddressNameRelation::EffectiveController,
        44,
    )
    .await?;
    let response = finish_paused_reverse_lookup(task, reached, resume).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refused: Value = read_json(response).await?;
    assert_eq!(refused["error"]["code"], "stale");
    assert!(refused.get("data").is_none());
    let fresh = v2_lookup_json(
        &database,
        json!({"profile":"detail", "inputs":[{"address":address,"page_size":1}]}),
    )
    .await?;
    assert_eq!(fresh["data"][0]["records"][0]["name"], "alice.eth");
    assert_eq!(
        fresh["data"][0]["records"][0]["registration_id"],
        replacement_resource.to_string()
    );
    assert_eq!(fresh["data"][0]["records"][0]["is_primary"], true);
    let cursor = fresh["data"][0]["page"]["next_cursor"]
        .as_str()
        .context("fresh cursor")?;
    let next = v2_lookup_json(
        &database,
        json!({"profile":"detail", "inputs":[{"address":address,"page_size":1,"cursor":cursor}]}),
    )
    .await?;
    assert_eq!(lookup_record_names(&next), vec!["bob.eth"]);
    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_reverse_pages_a_case_unstable_primary_name_without_repeating_rows() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    // A single-script Cherokee name passes ENSIP-15 byte-identical, so the projection stores it
    // verbatim — but Postgres `lower()` maps it to the lowercase Cherokee block, i.e. different
    // bytes. Comparing or sorting through `lower()` would therefore put this row in the
    // non-primary block while the response reports it as primary, and the keyset built from the
    // reported flag then serves an earlier row a second time.
    let cherokee = "ᏣᎳᎩ.eth";
    seed_identity_name(
        &database,
        "ens:ᏣᎳᎩ.eth",
        cherokee,
        cherokee,
        "namehash:ᏣᎳᎩ.eth",
        Uuid::from_u128(0x5a0241),
        Uuid::from_u128(0x5a0242),
        Uuid::from_u128(0x5a0243),
        address,
        bigname_storage::AddressNameRelation::EffectiveController,
        43,
    )
    .await?;
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some(cherokee),
        true,
    )
    .await?;

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..5 {
        let mut input = json!({"id": "addr", "address": address, "page_size": 1});
        if let Some(cursor) = cursor.as_deref() {
            input["cursor"] = json!(cursor);
        }
        let payload =
            v2_lookup_json(&database, json!({"profile": "detail", "inputs": [input]})).await?;
        let records = payload["data"][0]["records"]
            .as_array()
            .expect("reverse lookup records must be an array");
        for record in records {
            let name = record["name"]
                .as_str()
                .expect("record name must be a string")
                .to_owned();
            assert_eq!(
                record["is_primary"],
                json!(name == cherokee),
                "only the claimed name is primary, got {record}"
            );
            seen.push(name);
        }
        match payload["data"][0]["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }

    assert_eq!(
        seen,
        vec![cherokee.to_owned(), "alice.eth".to_owned(), "bob.eth".to_owned()],
        "the primary row sorts first and no row is served twice"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_rejects_union_scope_with_missing_phase_head() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 77,
                "block_hash": "0xlookup-head",
                "timestamp": "2026-04-17T00:01:17Z"
            }
        }))
        .await?;
    republish_mainnet_fixture(&database).await?;
    let public_response = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({
            "inputs": [
                {"id": "ens-miss", "name": "missing.eth"},
                {"id": "basenames-miss", "name": "missing.base.eth"}
            ]
        }),
    )
    .await?;
    assert_eq!(public_response.status(), StatusCode::CONFLICT);
    let public_payload: Value = read_json(public_response).await?;
    assert_eq!(public_payload["error"]["code"], json!("conflict"));

    let invalid_only = v2_lookup_json(
        &database,
        json!({"inputs": [{"id": "bad", "name": "bad name.eth"}]}),
    )
    .await?;
    assert_eq!(invalid_only["data"][0]["status"], json!("invalid_name"));
    assert!(invalid_only["meta"].get("as_of").is_none());
    assert!(invalid_only["meta"].get("as_of_token").is_none());

    let payload = v2_lookup_json(
        &database,
        json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
    )
    .await?;

    assert_eq!(payload["data"][0]["status"], json!("not_found"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 77,
            "block_hash": "0xlookup-head",
            "timestamp": "2026-04-17T00:01:17Z"
        })
    );
    assert!(payload["meta"]["as_of"].get("8453").is_none());
    assert!(payload["meta"]["as_of_token"].is_string());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_explicit_namespace_invalid_name_keeps_the_selected_chain_in_meta() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_lookup_ethereum_head(&database, 77, "0xlookup-invalid-explicit").await?;
    republish_mainnet_fixture(&database).await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "namespace": "ens",
            "inputs": [{"name": "bad name.eth"}]
        }),
    )
    .await?;

    assert_eq!(payload["data"][0]["status"], json!("invalid_name"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 77,
            "block_hash": "0xlookup-invalid-explicit",
            "timestamp": "2026-04-17T00:00:17Z"
        })
    );
    assert!(payload["meta"].get("as_of_completeness").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_explicit_namespace_invalid_name_discloses_a_suppressed_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "namespace": "ens",
            "inputs": [{"name": "bad name.eth"}]
        }),
    )
    .await?;

    assert_eq!(payload["data"][0]["status"], json!("invalid_name"));
    assert!(payload["meta"].get("as_of").is_none());
    assert_eq!(
        payload["meta"]["as_of_completeness"]["1"],
        json!({
            "completeness": "unsupported",
            "unsupported_reason": "temporarily_unavailable"
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_inferred_name_scope_discloses_a_suppressed_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let payload = v2_lookup_json(
        &database,
        json!({"inputs": [{"name": "missing.eth"}]}),
    )
    .await?;

    assert_eq!(payload["data"][0]["status"], json!("not_found"));
    assert!(payload["meta"].get("as_of").is_none());
    assert_eq!(
        payload["meta"]["as_of_completeness"]["1"],
        json!({
            "completeness": "unsupported",
            "unsupported_reason": "temporarily_unavailable"
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_public_reverse_scope_uses_the_served_namespace_set() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_identity_name(
        &database,
        "basenames:stale.base.eth",
        "stale.base.eth",
        "stale.base.eth",
        "namehash:stale.base.eth",
        Uuid::from_u128(0x5a0221),
        Uuid::from_u128(0x5a0222),
        Uuid::from_u128(0x5a0223),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        43,
    )
    .await?;

    let response = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": address}]}),
        &["ens"],
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    assert!(payload["meta"]["as_of"].get("1").is_some());
    assert!(payload["meta"]["as_of"].get("8453").is_none());
    assert!(payload["meta"].get("completeness").is_none());
    assert_eq!(lookup_record_names(&payload), vec!["alice.eth", "bob.eth"]);
    assert_eq!(payload["data"][0]["page"]["total_count"], json!(2));
    assert_eq!(payload["data"][0]["page"]["has_more"], json!(false));

    let codeployed_page = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": address, "page_size": 1}]}),
        &["ens", "basenames"],
    )
    .await?;
    assert_eq!(codeployed_page.status(), StatusCode::OK);
    let codeployed_payload: Value = read_json(codeployed_page).await?;
    let cursor = codeployed_payload["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("co-deployed reverse page must include a cursor");
    let changed_set = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"address": address, "page_size": 1, "cursor": cursor}]}),
        &["ens"],
    )
    .await?;
    assert_eq!(changed_set.status(), StatusCode::BAD_REQUEST);
    let changed_payload: Value = read_json(changed_set).await?;
    assert_eq!(changed_payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_production_derivation_uses_the_sepolia_authority_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
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
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": 107,
                "block_hash": "0xlookup-sepolia",
                "timestamp": "2026-08-10T00:01:47Z"
            }
        }))
        .await?;
    republish_fixture_chain(&database, "ethereum-sepolia").await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "inputs": [{
                            "address": "0x0000000000000000000000000000000000000abc"
                        }]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("lookup request must build"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"][0]["records"], json!([]));
    assert!(payload["meta"]["as_of"].get("11155111").is_some());
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert!(payload["meta"]["as_of"].get("8453").is_none());
    assert!(payload["meta"].get("completeness").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_rejects_manifest_declaration_change_during_public_reverse_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "ens_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_manifest(
            "basenames",
            "basenames_base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"inputs": [{"address": address}]}))
                            .expect("body must serialize"),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        "UPDATE bigname_phase.manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload,
             '{roots}',
             '[{
                 \"name\": \"ChangedRoot\",
                 \"address\": \"0x0000000000000000000000000000000000000001\"
             }]'::jsonb
         )
         WHERE namespace = 'basenames' AND rollout_status = 'active'",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.normalized_events (
             event_identity,
             namespace,
             event_kind,
             source_family,
             manifest_version,
             chain_id,
             raw_fact_ref,
             derivation_kind,
             canonicality_state,
             before_state,
             after_state
         ) VALUES (
             'manifest_sync:test-roots-change',
             'basenames',
             'SourceManifestUpdated',
             'basenames_base_registry',
             1,
             'base-mainnet',
             '{\"deployment_epoch\": \"basenames_v1\"}'::jsonb,
             'manifest_sync',
             'finalized',
             '{}'::jsonb,
             '{\"manifest_payload\": {\"roots\": [{\"name\": \"ChangedRoot\"}]}}'::jsonb
         )",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = 'manifest-authority:test'
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("lookup manifest-change request task panicked")?
        .context("lookup manifest-change request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("conflict")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_rejects_interpret_redo_during_public_reverse_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_public_authority(&database).await?;
    let project_before = database
        .phase_state_fingerprint("ethereum-mainnet", "project")
        .await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_initial_validation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"inputs": [{"address": address}]}))
                            .expect("body must serialize"),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces
         SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum-mainnet'",
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(
        database
            .phase_state_fingerprint("ethereum-mainnet", "project")
            .await?,
        project_before,
        "the simulated Interpret redo must not update Project"
    );
    control.resume().await;

    let response = request_task
        .await
        .context("lookup Interpret-redo request task panicked")?
        .context("lookup Interpret-redo request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none(), "no partial page may be served");

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_allows_interpret_live_progress_during_public_reverse_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_public_authority(&database).await?;
    let interpret_before = database
        .phase_state_fingerprint("ethereum-mainnet", "interpret")
        .await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_initial_validation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"inputs": [{"address": address}]}))
                            .expect("body must serialize"),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    database
        .touch_interpret_phase_state("ethereum-mainnet")
        .await?;
    let interpret_after = database
        .phase_state_fingerprint("ethereum-mainnet", "interpret")
        .await?;
    assert_ne!(interpret_after.0, interpret_before.0);
    assert_ne!(interpret_after.4, interpret_before.4);
    assert_eq!(interpret_after.1, "completed");
    control.resume().await;

    let response = request_task
        .await
        .context("lookup Interpret-progress request task panicked")?
        .context("lookup Interpret-progress request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(lookup_record_names(&payload), vec!["alice.eth", "bob.eth"]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_rejects_public_namespace_becoming_ready_during_reverse_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "ens_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_manifest(
            "basenames",
            "basenames_base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    // Base has a selected head but no family publication yet, so basenames is not ready.
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"inputs": [{"address": address}]}))
                            .expect("body must serialize"),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    // Project publishes base's first families while the read is in flight.
    republish_fixture_chain(&database, "base-mainnet").await?;
    control.resume().await;

    let response = request_task
        .await
        .context("lookup readiness-change request task panicked")?
        .context("lookup readiness-change request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("stale")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_allows_manifest_freshness_change_without_authority_change() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "ens_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_manifest(
            "basenames",
            "basenames_base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"inputs": [{"address": address}]}))
                            .expect("body must serialize"),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        "UPDATE bigname_phase.manifest_versions
         SET loaded_at = loaded_at + INTERVAL '1 second'",
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("lookup manifest-refresh request task panicked")?
        .context("lookup manifest-refresh request failed")?;
    assert_eq!(response.status(), StatusCode::OK);

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_mixed_reverse_and_unserved_forward_namespace_fails_closed() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query("DELETE FROM bigname_phase.chain_heads WHERE chain_id = 'base-mainnet'")
        .execute(&database.lookup_pool)
        .await?;

    let response = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v1/lookup",
        json!({
            "inputs": [
                {"id": "reverse", "address": address},
                {"id": "basenames-miss", "name": "missing.base.eth"}
            ]
        }),
        &["ens"],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("conflict"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_rejects_single_scope_with_incompatible_project_generation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 78,
                "block_hash": "0xlookup-incompatible-project",
                "timestamp": "2026-04-17T00:01:18Z"
            }
        }))
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = 'incompatible-project-generation'
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;

    let response = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_serves_a_project_publication_a_few_blocks_behind_head() -> Result<()> {
    // Live-follow stores the head before Project publishes for it. A publication within the
    // lag tolerance is served as the snapshot and reported in `as_of`; one further behind is
    // still stale so a wedged Project cannot serve arbitrarily old data.
    for (publication_block, phase_status, expect_served) in [
        (78_i64, "completed", true),
        (78_i64, "running", true),
        (77_i64, "completed", false),
    ] {
        let database = TestDatabase::new_migrated().await?;
        database
            .seed_snapshot_selector_chain_positions(&json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 79,
                    "block_hash": "0xlookup-project-behind",
                    "timestamp": "2026-04-17T00:01:19Z"
                }
            }))
            .await?;
        sqlx::query(
            "INSERT INTO bigname_phase.chain_lineage
                 (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ('ethereum-mainnet', '0xlookup-previous', $1,
                     '2026-04-17T00:01:18Z'::timestamptz,
                     'finalized'::bigname_phase.canonicality_state)
             ON CONFLICT DO NOTHING",
        )
        .bind(publication_block)
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "UPDATE chain_phase_state
             SET current_block_number = $1, current_block_hash = '0xlookup-previous',
                 phase_status = $2,
                 finished_at = CASE WHEN $2 = 'running' THEN NULL ELSE finished_at END
             WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
        )
        .bind(publication_block)
        .bind(phase_status)
        .execute(&database.lookup_pool)
        .await?;
        rebuild_fixture_families(
            &database.pool,
            "ethereum-mainnet",
            publication_block,
            "0xlookup-previous",
        )
        .await?;

        let response = v2_lookup_response_for_database(
            &database,
            "/v1/lookup",
            json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
        )
        .await?;
        if expect_served {
            let status = response.status();
            let payload: Value = read_json(response).await?;
            assert_eq!(
                status,
                StatusCode::OK,
                "publication one block behind ({phase_status}) must be served: {payload}"
            );
            assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(78));
            assert_eq!(
                payload["meta"]["as_of"]["1"]["block_hash"],
                json!("0xlookup-previous")
            );
        } else {
            assert_eq!(
                response.status(),
                StatusCode::CONFLICT,
                "publication beyond the lag tolerance"
            );
            let payload: Value = read_json(response).await?;
            assert_eq!(payload["error"]["code"], json!("stale"));
        }

        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_feed_miss_and_all_miss_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [
                {"id": "hit", "address": address, "relation": "owner"},
                {"id": "miss", "address": "0x0000000000000000000000000000000000000def"}
            ]
        }),
    )
    .await?;
    assert_eq!(payload["data"][0]["status"], json!("ok"));
    assert_eq!(payload["data"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(payload["data"][0]["records"][0]["is_primary"], json!(true));
    assert_eq!(
        payload["data"][0]["records"][0]["relations"],
        json!(["owner"])
    );
    assert!(payload["data"][0]["records"][0].get("addresses").is_none());
    assert_eq!(payload["data"][0]["page"]["page_size"], json!(50));
    assert_eq!(payload["data"][0]["page"]["total_count"], Value::Null);
    assert_eq!(payload["data"][1]["status"], json!("ok"));
    assert_eq!(payload["data"][1]["records"], json!([]));
    assert_eq!(payload["data"][1]["page"]["total_count"], json!(0));

    let empty_database = TestDatabase::new_migrated().await?;
    empty_database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 77,
                "block_hash": "0xlookup-head",
                "timestamp": "2026-04-17T00:01:17Z"
            }
        }))
        .await?;
    republish_mainnet_fixture(&empty_database).await?;
    let miss_payload = v2_lookup_json(
        &empty_database,
        json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
    )
    .await?;
    assert_eq!(miss_payload["data"][0]["status"], json!("not_found"));
    assert_eq!(
        miss_payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 77,
            "block_hash": "0xlookup-head",
            "timestamp": "2026-04-17T00:01:17Z"
        })
    );

    database.cleanup().await?;
    empty_database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_relation_sets_and_any_match_any_listed_relation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "set", "address": address, "relation": "manager,owner"},
                {"id": "any", "address": address, "relation": "any"}
            ]
        }),
    )
    .await?;

    assert_eq!(
        payload["data"][0]["input"],
        json!({
            "id": "set",
            "address": address,
            "coin_type": 60,
            "relation": "owner,manager"
        })
    );
    let set_record_names = payload["data"][0]["records"]
        .as_array()
        .expect("set lookup records must be an array")
        .iter()
        .map(|record| record["name"].as_str().expect("record must include name"))
        .collect::<Vec<_>>();
    assert_eq!(set_record_names, vec!["alice.eth", "bob.eth"]);
    assert_eq!(
        payload["data"][0]["records"][0]["relations"],
        json!(["owner"])
    );
    assert_eq!(
        payload["data"][0]["records"][1]["relations"],
        json!(["manager"])
    );

    assert_eq!(
        payload["data"][1]["input"],
        json!({
            "id": "any",
            "address": address,
            "coin_type": 60,
            "relation": "owner,manager,registrant"
        })
    );
    let any_record_names = payload["data"][1]["records"]
        .as_array()
        .expect("any lookup records must be an array")
        .iter()
        .map(|record| record["name"].as_str().expect("record must include name"))
        .collect::<Vec<_>>();
    assert_eq!(any_record_names, vec!["alice.eth", "bob.eth"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_feed_uses_detail_pagination_semantics() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let first_page = v2_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [{
                "address": address,
                "page_size": 1
            }]
        }),
    )
    .await?;

    assert_eq!(first_page["data"][0]["input"], json!({
        "address": address,
        "coin_type": 60,
        "page_size": 1
    }));
    assert_eq!(first_page["data"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(first_page["data"][0]["page"]["has_more"], json!(true));
    assert_eq!(first_page["data"][0]["page"]["total_count"], json!(2));
    let cursor = first_page["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("feed first page must include next_cursor");

    let second_page = v2_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [{
                "address": address,
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;

    assert_eq!(second_page["data"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(second_page["data"][0]["page"]["has_more"], json!(false));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_relation_filters_owner_and_registrant_exactly() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (_count_guard, count_calls) =
        crate::v2::support::identity_facade_count_test_hooks::install(&database.pool).await?;
    let address = "0x0000000000000000000000000000000000000abc";
    // The address holds holder.eth's lease, so it is that name's owner and registrant: an
    // unwrapped lease's token holder is its registrant. It only controls managed.eth through the
    // registry, which neither filter may list.
    seed_relation_name(
        &database,
        "holder.eth",
        0x5a0301,
        44,
        RelationNameAccounts {
            registrant: address,
            controller: V2_LOOKUP_OTHER_ACCOUNT,
            resolver: address,
        },
    )
    .await?;
    seed_relation_name(
        &database,
        "managed.eth",
        0x5a0311,
        45,
        RelationNameAccounts {
            registrant: V2_LOOKUP_OTHER_ACCOUNT,
            controller: address,
            resolver: address,
        },
    )
    .await?;
    seed_v2_lookup_base_head(&database).await?;

    let owner = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "owner"
            }]
        }),
    )
    .await?;
    assert_eq!(lookup_record_names(&owner), vec!["holder.eth"]);
    assert_eq!(owner["data"][0]["records"][0]["relations"], json!(["owner"]));
    assert_eq!(owner["data"][0]["page"]["total_count"], Value::Null);

    let registrant = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "registrant"
            }]
        }),
    )
    .await?;
    assert_eq!(lookup_record_names(&registrant), vec!["holder.eth"]);
    assert_eq!(
        registrant["data"][0]["records"][0]["relations"],
        json!(["registrant"])
    );
    assert_eq!(registrant["data"][0]["page"]["total_count"], Value::Null);
    assert_eq!(
        count_calls.count(),
        0,
        "post-filtered reverse lookups must not execute a discarded live count"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_relation_filter_resumes_across_scan_boundaries() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_relation_scan_fixture(&database, address, 125, &[50, 101]).await?;

    let first_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "owner",
                "page_size": 1
            }]
        }),
    )
    .await?;
    assert_eq!(lookup_record_names(&first_page), vec!["scan050.eth"]);
    assert_eq!(first_page["data"][0]["page"]["has_more"], json!(true));
    let cursor = first_page["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("overflow page must include next_cursor");

    let second_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "owner",
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    assert_eq!(lookup_record_names(&second_page), vec!["scan101.eth"]);
    assert_eq!(second_page["data"][0]["page"]["has_more"], json!(false));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "no family producer yields an owned relation that is not `owner`, so the exact-relation scan never skips a row"]
async fn v2_lookup_reverse_relation_page_revalidates_generation_before_second_scan() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_relation_scan_fixture(&database, address, 125, &[101]).await?;
    let (_guard, control) = crate::v2::support::identity_facade_relation_page_test_hooks::install(
        &database.lookup_pool,
    )
    .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "profile": "detail",
                            "inputs": [{
                                "address": address,
                                "relation": "owner",
                                "page_size": 1
                            }]
                        })
                        .to_string(),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    advance_v2_lookup_ethereum_head(&database, 10_001, "0xlookup-scan-advanced").await?;
    control.resume().await;

    let response = request_task
        .await
        .context("reverse relation lookup request task panicked")?
        .context("reverse relation lookup request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(
        control.page_loader_call_count(),
        1,
        "a changed Project publication must be rejected before another broad page load"
    );

    database.cleanup().await
}

#[tokio::test]
#[ignore = "no family producer yields an owned relation that is not `owner`, so the exact-relation scan never skips a row"]
async fn v2_lookup_reverse_relation_filter_scan_cap_returns_resume_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_relation_scan_fixture(&database, address, 502, &[500]).await?;

    let capped_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "owner",
                "page_size": 1
            }]
        }),
    )
    .await?;
    assert_eq!(
        capped_page["data"][0]["records"]
            .as_array()
            .expect("records must be an array")
            .len(),
        0
    );
    assert_eq!(capped_page["data"][0]["page"]["has_more"], json!(true));
    let cursor = capped_page["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("scan-capped page must include next_cursor");

    let resumed_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "owner",
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    assert_eq!(lookup_record_names(&resumed_page), vec!["scan500.eth"]);
    assert_eq!(resumed_page["data"][0]["page"]["has_more"], json!(false));

    database.cleanup().await?;
    Ok(())
}

async fn advance_v2_lookup_ethereum_head(
    database: &TestDatabase,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60)
            }
        }))
        .await
}

async fn advance_v2_lookup_phase_only_ethereum_head(
    database: &TestDatabase,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
    let timestamp = format!("2026-04-17T00:00:{:02}Z", block_number % 60);
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('ethereum-mainnet', $1, $2, $3::timestamptz, 'finalized')",
    )
    .bind(block_hash)
    .bind(block_number)
    .bind(&timestamp)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = $1,
             latest_block_number = $2,
             safe_block_hash = $1,
             safe_block_number = $2,
             finalized_block_hash = $1,
             finalized_block_number = $2,
             updated_at = now()
         WHERE chain_id = 'ethereum-mainnet'",
    )
    .bind(block_hash)
    .bind(block_number)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $1,
             current_block_hash = $2,
             target_block_number = $1,
             target_block_hash = $2,
             input_content_hash = $3,
             finished_at = now(),
             updated_at = now()
         WHERE chain_id = 'ethereum-mainnet'
           AND phase_name = 'project'",
    )
    .bind(block_number)
    .bind(block_hash)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

async fn seed_v2_lookup_reverse_fixture(database: &TestDatabase, address: &str) -> Result<()> {
    // The address holds alice.eth's lease (owner and registrant) and controls bob.eth through
    // the registry (manager), with another account on the other side of each name.
    seed_relation_name(
        database,
        "alice.eth",
        0x5a0201,
        41,
        RelationNameAccounts {
            registrant: address,
            controller: V2_LOOKUP_OTHER_ACCOUNT,
            resolver: address,
        },
    )
    .await?;
    seed_relation_name(
        database,
        "bob.eth",
        0x5a0211,
        42,
        RelationNameAccounts {
            registrant: V2_LOOKUP_OTHER_ACCOUNT,
            controller: address,
            resolver: address,
        },
    )
    .await?;
    seed_phase_primary_name_snapshot(
        database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some("alice.eth"),
        true,
    )
    .await?;
    seed_v2_lookup_base_head(database).await?;
    Ok(())
}

const V2_LOOKUP_OTHER_ACCOUNT: &str = "0x0000000000000000000000000000000000000bbb";

async fn seed_v2_lookup_public_authority(database: &TestDatabase) -> Result<()> {
    database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "ens_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_manifest(
            "basenames",
            "basenames_base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    Ok(())
}

async fn seed_v2_lookup_base_head(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 88,
                "block_hash": "0xlookup-base-head",
                "timestamp": "2026-04-17T00:01:28Z"
            }
        }))
        .await
}

async fn seed_v2_lookup_ethereum_head(
    database: &TestDatabase,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60)
            }
        }))
        .await
}

async fn seed_v2_lookup_relation_scan_fixture(
    database: &TestDatabase,
    address: &str,
    row_count: usize,
    owner_match_indexes: &[usize],
) -> Result<()> {
    seed_v2_lookup_ethereum_head(database, 10_000, "0xlookup-scan-head").await?;
    seed_v2_lookup_base_head(database).await?;
    // The address holds the lease of each owner match; it controls every other name through the
    // registry only.
    for index in 0..row_count {
        let accounts = if owner_match_indexes.contains(&index) {
            RelationNameAccounts {
                registrant: address,
                controller: V2_LOOKUP_OTHER_ACCOUNT,
                resolver: address,
            }
        } else {
            RelationNameAccounts {
                registrant: V2_LOOKUP_OTHER_ACCOUNT,
                controller: address,
                resolver: address,
            }
        };
        seed_relation_name_inputs_at(
            database,
            &format!("scan{index:03}.eth"),
            0x7100_0000 + index as u128 * 3,
            10_000,
            "0xlookup-scan-head",
            accounts,
        )
        .await?;
    }
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 10_000, "0xlookup-scan-head").await
}

fn lookup_record_names(payload: &Value) -> Vec<&str> {
    payload["data"][0]["records"]
        .as_array()
        .expect("lookup records must be an array")
        .iter()
        .map(|record| record["name"].as_str().expect("record must include name"))
        .collect()
}

async fn v2_lookup_json(database: &TestDatabase, body: Value) -> Result<Value> {
    let response = v2_lookup_response_for_database(database, "/v1/lookup", body).await?;
    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    Ok(payload)
}

async fn v2_get_json(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_get_response(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_get_response(database: &TestDatabase, uri: &str) -> Result<Response<Body>> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 GET request failed")
}

async fn v2_lookup_response_for_database(
    database: &TestDatabase,
    uri: &str,
    body: Value,
) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&body).expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("v2 lookup request failed")
}

async fn v2_lookup_response_for_database_with_public_namespaces(
    database: &TestDatabase,
    uri: &str,
    body: Value,
    public_namespaces: &[&str],
) -> Result<Response> {
    app_router(database.app_state_with_public_namespaces(public_namespaces))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&body).expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("v2 lookup request failed")
}

/// A case name the address owns (holds the lease, so it is registrant and token holder), manages
/// (controls it through the registry), or both.
async fn seed_reverse_family_case_name(
    database: &TestDatabase,
    name: &str,
    address: &str,
    (owned, managed): (bool, bool),
    id: u128,
) -> Result<()> {
    let registrant = if owned { address } else { V2_LOOKUP_OTHER_ACCOUNT };
    let controller = if managed { address } else { V2_LOOKUP_OTHER_ACCOUNT };
    seed_relation_name(
        database,
        name,
        id,
        42,
        RelationNameAccounts {
            registrant,
            controller,
            resolver: address,
        },
    )
    .await
}

async fn assert_reverse_family_pages(
    database: &TestDatabase,
    input: &bigname_storage::ReverseIdentityStorageInput,
    namespaces: &[String],
    expected: &[&str],
) -> Result<bigname_storage::ReverseIdentityCursor> {
    use crate::v2::support::load_reverse_identity_records_live as load_reverse;
    use bigname_storage::{AddressNameRelation as Relation, ReverseIdentityCursor};
    let mut request = input.clone();
    let mut names = Vec::new();
    let mut first_cursor = None;
    loop {
        let groups = load_reverse(&database.lookup_pool, &[request.clone()], namespaces, None).await?;
        assert_eq!(groups.len(), 1);
        let group = &groups[0];
        assert_eq!(group.total_count, Some(expected.len() as u64));
        assert!(group.entries.len() <= input.page_size as usize);
        for entry in &group.entries {
            let row = &entry.name_record.row;
            assert!(namespaces.contains(&row.namespace));
            let facets = match row.normalized_name.as_str() {
                "birch.eth" => vec![
                    Relation::Registrant,
                    Relation::TokenHolder,
                    Relation::EffectiveController,
                ],
                "bob.eth" | "cedar.eth" | "elm.base.eth" => {
                    vec![Relation::EffectiveController]
                }
                _ => vec![Relation::Registrant, Relation::TokenHolder],
            };
            let mut facets = facets
                .into_iter()
                .filter(|r| input.roles.includes(*r))
                .collect::<Vec<_>>();
            facets.sort();
            assert_eq!(entry.relation_facets, facets);
            let is_primary = entry
                .primary_name
                .as_ref()
                .and_then(|p| p.normalized_claim_name.as_deref())
                == Some(row.normalized_name.as_str());
            assert_eq!(is_primary, row.normalized_name == "alice.eth");
            names.push(row.normalized_name.clone());
            assert_eq!(names.last().unwrap(), expected[names.len() - 1]);
        }
        let last = group
            .entries
            .last()
            .context("expected nonempty fixture page")?;
        let row = &last.name_record.row;
        let cursor = ReverseIdentityCursor {
            is_primary: row.normalized_name == "alice.eth",
            role_rank: if last
                .relation_facets
                .iter()
                .any(|r| matches!(r, Relation::Registrant | Relation::TokenHolder))
            {
                0
            } else {
                1
            },
            normalized_name: row.normalized_name.clone(),
            namespace: row.namespace.clone(),
            namehash: row.namehash.clone(),
        };
        first_cursor.get_or_insert_with(|| cursor.clone());
        if !group.has_more {
            break;
        }
        anyhow::ensure!(names.len() < expected.len(), "repeated or endless pages");
        request.cursor = Some(cursor);
    }
    assert_eq!(names, expected);
    first_cursor.context("missing first cursor")
}

#[tokio::test]
async fn reverse_identity_pages_preserve_roles_namespaces_and_long_names() -> Result<()> {
    use crate::v2::support::load_reverse_identity_records_live as load_reverse;
    use bigname_storage::{ReverseIdentityRoles as Roles, ReverseIdentityStorageInput};
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let other = "0x0000000000000000000000000000000000000def";
    let absent = "0x0000000000000000000000000000000000000123";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    let long_name = (0..64)
        .map(|i| format!("label{i:03}{}", "a".repeat(40)))
        .collect::<Vec<_>>()
        .join(".")
        + ".eth";
    assert_eq!(long_name.len(), 3139);
    assert_eq!(
        bigname_domain::normalization::normalize_name(&long_name)
            .map_err(|error| anyhow::anyhow!(error.message().to_owned()))?
            .normalized_name,
        long_name
    );
    const OWNED: (bool, bool) = (true, false);
    const MANAGED: (bool, bool) = (false, true);
    for (name, roles, id) in [
        ("amber.eth", OWNED, 0x842100),
        ("birch.eth", (true, true), 0x842110),
        ("cedar.eth", MANAGED, 0x842120),
        (long_name.as_str(), OWNED, 0x842130),
        ("dune.base.eth", OWNED, 0x842140),
        ("elm.base.eth", MANAGED, 0x842150),
    ] {
        seed_reverse_family_case_name(&database, name, address, roles, id).await?;
    }
    let namespaces = vec!["ens".to_owned(), "basenames".to_owned()];
    let expected = [
        vec![
            "alice.eth",
            "amber.eth",
            "birch.eth",
            "dune.base.eth",
            long_name.as_str(),
        ],
        vec!["birch.eth", "bob.eth", "cedar.eth", "elm.base.eth"],
        vec![
            "alice.eth",
            "amber.eth",
            "birch.eth",
            "dune.base.eth",
            long_name.as_str(),
            "bob.eth",
            "cedar.eth",
            "elm.base.eth",
        ],
    ];
    let inputs =
        [Roles::Owned, Roles::Managed, Roles::Both].map(|roles| ReverseIdentityStorageInput {
            address: address.to_owned(),
            coin_type: "60".to_owned(),
            roles,
            page_size: 2,
            cursor: None,
        });
    let empty = load_reverse(&database.lookup_pool, &[], &namespaces, None).await?;
    assert!(empty.is_empty());
    let missing = ReverseIdentityStorageInput {
        address: absent.to_owned(),
        ..inputs[2].clone()
    };
    seed_reverse_family_case_name(&database, "unrelated.eth", other, OWNED, 0x900000).await?;
    {
        let mut cases = Vec::new();
        for (index, input) in inputs.iter().enumerate() {
            let cursor =
                assert_reverse_family_pages(&database, input, &namespaces, &expected[index]).await?;
            let ens = expected[index]
                .iter()
                .copied()
                .filter(|n| !n.ends_with(".base.eth"))
                .collect::<Vec<_>>();
            assert_eq!(ens.len(), [4, 3, 6][index]);
            assert_reverse_family_pages(&database, input, &["ens".to_owned()], &ens).await?;
            cases.push(vec![input.clone()]);
            cases.push(vec![ReverseIdentityStorageInput {
                cursor: Some(cursor),
                ..input.clone()
            }]);
        }
        let mut batch = [0, 3, 4, 5].map(|case| cases[case][0].clone()).to_vec();
        batch.push(missing.clone());
        let groups = load_reverse(&database.lookup_pool, &batch, &namespaces, None).await?;
        let expected_batch = [
            &expected[0][..2],
            &expected[1][2..4],
            &expected[2][..2],
            &expected[2][2..4],
            &[],
        ];
        assert_eq!(groups.len(), 5);
        for (i, group) in groups.iter().enumerate() {
            assert_eq!(group.input, batch[i]);
            assert_eq!(group.total_count, Some([5, 4, 8, 8, 0][i]));
            assert_eq!(
                group
                    .entries
                    .iter()
                    .map(|e| e.name_record.row.normalized_name.as_str())
                    .collect::<Vec<_>>(),
                expected_batch[i]
            );
        }
    }
    database.cleanup().await
}
