const V2_SEARCH_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000d01";
const V2_SEARCH_REGISTRATION_REGISTRANT: &str = "0x0000000000000000000000000000000000000d03";

#[tokio::test]
async fn v2_search_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:ᏣᎳᎩ.eth",
        NORMALIZED_NAME,
        NORMALIZED_NAME,
        "namehash:ᏣᎳᎩ.eth",
        Uuid::from_u128(0x349_1001),
        Uuid::from_u128(0x349_1002),
        Uuid::from_u128(0x349_1003),
        "0x0000000000000000000000000000000000034910",
        bigname_storage::AddressNameRelation::TokenHolder,
        43,
    )
    .await?;
    let stored_raw_name: String =
        sqlx::query_scalar("SELECT raw_name FROM bigname_phase.name_surfaces WHERE raw_name = $1")
            .bind(NORMALIZED_NAME)
            .fetch_one(&database.pool)
            .await?;

    let prefix =
        v2_search_payload_for_database(&database, "/v1/search?q=%E1%8F%A3%E1%8E%B3&namespace=ens")
            .await?;
    assert_eq!(prefix["data"][0]["name"], json!(stored_raw_name));

    let contains = v2_search_payload_for_database(
        &database,
        "/v1/search?q=%E1%8F%A3%E1%8E%B3&match=contains&namespace=ens",
    )
    .await?;
    assert_eq!(contains["data"][0]["name"], json!(NORMALIZED_NAME));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_prefix_returns_record_rows() -> Result<()> {
    let (database, payload) = v2_search_payload("/v1/search?q=al&namespace=ens").await?;

    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_search_meta_chains(&payload, &["1"], &[]);

    let data = payload["data"]
        .as_array()
        .expect("search data must be an array");
    assert_eq!(v2_search_names(data), vec!["alpha.eth", "alpine.eth"]);
    assert_eq!(data[0]["display_name"], json!("alpha.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(
        data[0]["namehash"],
        json!(bigname_lookup::ens_namehash_hex("alpha.eth")?)
    );
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(
        data[0]["manager"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert!(data[0].get("registrant").is_none());
    assert_eq!(data[0]["status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("1704153600"));
    assert_eq!(data[0]["created_at"], json!("1672617600"));
    assert_eq!(data[0]["expires_at"], json!("1798848000"));
    assert!(data[0].get("relations").is_none());
    assert!(data[0].get("is_primary").is_none());
    assert!(data[0].get("role_summary").is_none());
    assert!(data[0].get("labelhash").is_none());
    assert!(data[0].get("subname_count").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_serves_the_registrant_as_owner_and_the_registry_owner_as_manager() -> Result<()> {
    let (database, payload) = v2_search_payload("/v1/search?q=precedence&namespace=ens").await?;

    let data = payload["data"]
        .as_array()
        .expect("search data must be an array");
    assert_eq!(v2_search_names(data), vec!["precedence.eth"]);
    assert_eq!(data[0]["owner"], json!(V2_SEARCH_REGISTRATION_REGISTRANT));
    assert_eq!(data[0]["manager"], json!(V2_SEARCH_REGISTRY_OWNER));
    assert_eq!(data[0]["status"], json!("active"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_match_modes_and_q_validation() -> Result<()> {
    let (database, prefix) = v2_search_payload("/v1/search?q=amm&namespace=ens").await?;
    assert_eq!(prefix["data"], json!([]));

    let contains = v2_search_payload_for_database(
        &database,
        "/v1/search?q=amm&match=contains&namespace=ens",
    )
    .await?;
    assert_eq!(
        v2_search_names(contains["data"].as_array().expect("contains data")),
        vec!["gamma.eth"]
    );

    for uri in [
        "/v1/search?namespace=ens",
        "/v1/search?q=&namespace=ens",
        "/v1/search?q=al&match=suffix&namespace=ens",
        "/v1/search?q=al%25&namespace=ens",
    ] {
        let response = v2_search_response_for_database(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let error: Value = read_json(response).await?;
        assert_eq!(error["error"]["code"], json!("invalid_input"), "{uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_contains_accepts_label_boundary_fragments() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_identity_name(
        &database,
        "ens:alice.eth.example",
        "alice.eth.example",
        "alice.eth.example",
        "namehash:alice.eth.example",
        Uuid::from_u128(0x584_1001),
        Uuid::from_u128(0x584_1002),
        Uuid::from_u128(0x584_1003),
        "0x0000000000000000000000000000000000058410",
        bigname_storage::AddressNameRelation::TokenHolder,
        220,
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:ethereal.xyz",
        "ethereal.xyz",
        "ethereal.xyz",
        "namehash:ethereal.xyz",
        Uuid::from_u128(0x584_2001),
        Uuid::from_u128(0x584_2002),
        Uuid::from_u128(0x584_2003),
        "0x0000000000000000000000000000000000058420",
        bigname_storage::AddressNameRelation::TokenHolder,
        221,
    )
    .await?;

    for (fragment, expected_names) in [
        (".eth", None),
        ("eth.", Some(vec!["alice.eth.example"])),
        (".eth.", Some(vec!["alice.eth.example"])),
        ("th.e", Some(vec!["alice.eth.example"])),
    ] {
        let uri = format!("/v1/search?q={fragment}&match=contains&namespace=ens");
        let response = v2_search_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::OK, "{fragment}");
        let payload: Value = read_json(response).await?;
        let names = v2_search_names(payload["data"].as_array().expect("contains data"));
        if let Some(expected_names) = expected_names {
            assert_eq!(names, expected_names, "{fragment}");
        } else {
            assert!(names.contains(&"alpha.eth"), "{fragment}: {names:?}");
            assert!(names.contains(&"gamma.eth"), "{fragment}: {names:?}");
            assert!(!names.contains(&"ethereal.xyz"), "{fragment}: {names:?}");
        }
    }

    for fragment in [".", ".."] {
        let uri = format!("/v1/search?q={fragment}&match=contains&namespace=ens");
        let response = v2_search_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{fragment}");
        assert_eq!(
            read_json::<Value>(response).await?["error"]["code"],
            json!("invalid_input"),
            "{fragment}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_normalizes_q_and_filters_namespace() -> Result<()> {
    let (database, uppercase) = v2_search_payload("/v1/search?q=AL&namespace=ens").await?;
    assert_eq!(
        v2_search_names(uppercase["data"].as_array().expect("uppercase data")),
        vec!["alpha.eth", "alpine.eth"]
    );

    let public = v2_search_payload_for_database(&database, "/v1/search?q=alpha").await?;
    assert_eq!(
        v2_search_names(public["data"].as_array().expect("public data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert_search_meta_chains(&public, &["1", "8453"], &[]);

    let label_boundary =
        v2_search_payload_for_database(&database, "/v1/search?q=ALPHA.&namespace=ens").await?;
    assert_eq!(
        v2_search_names(label_boundary["data"].as_array().expect("boundary data")),
        vec!["alpha.eth"]
    );

    let basenames =
        v2_search_payload_for_database(&database, "/v1/search?q=alpha&namespace=basenames").await?;
    assert_eq!(
        v2_search_names(basenames["data"].as_array().expect("basenames data")),
        vec!["alpha.base.eth"]
    );

    let unknown =
        v2_search_response_for_database(&database, "/v1/search?q=alpha&namespace=internal").await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_contains_normalizes_ascii_query_without_touching_stored_names(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    let page = v2_search_payload_for_database(&database,
        "/v1/search?q=AL&match=contains&namespace=ens").await?;
    assert_eq!(page["data"].as_array().map(Vec::len), Some(2));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_scope_matches_the_served_deployment_namespaces() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    let codeployed = v2_search_payload_for_database(&database, "/v1/search?q=alpha").await?;
    assert_eq!(
        v2_search_names(codeployed["data"].as_array().expect("codeployed data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert_search_meta_chains(&codeployed, &["1", "8453"], &[]);

    let ens_only = v2_search_payload_for_database_with_public_namespaces(
        &database,
        "/v1/search?q=alpha",
        &["ens"],
    )
    .await?;
    assert_eq!(
        v2_search_names(ens_only["data"].as_array().expect("ENS-only data")),
        vec!["alpha.eth"]
    );
    assert_search_meta_chains(&ens_only, &["1"], &[]);
    assert!(ens_only["meta"].get("completeness").is_none());

    let explicit_basenames = v2_search_payload_for_database_with_public_namespaces(
        &database,
        "/v1/search?q=alpha&namespace=basenames",
        &["ens"],
    )
    .await?;
    assert_eq!(
        v2_search_names(
            explicit_basenames["data"]
                .as_array()
                .expect("explicit Basenames data")
        ),
        vec!["alpha.base.eth"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_explicit_namespace_bypasses_broken_public_derivation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
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

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/search?q=alpha&namespace=basenames")
                .body(Body::empty())
                .expect("search request must build"),
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    assert_eq!(
        v2_search_names(payload["data"].as_array().expect("explicit search data")),
        vec!["alpha.base.eth"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_manifest_change_between_derivation_and_row_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    sqlx::query(
        "UPDATE bigname_phase.manifest_versions
         SET rollout_status = 'deprecated'
         WHERE namespace = 'basenames' AND rollout_status = 'active'",
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("search coherence request task panicked")?
        .context("search coherence request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("conflict")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_manifest_change_that_breaks_derivation_returns_conflict() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
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
    control.resume().await;

    let response = request_task
        .await
        .context("search derivation-breaking request task panicked")?
        .context("search derivation-breaking request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("conflict"));
    assert!(
        payload.get("data").is_none(),
        "no partial page may be served"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_cursor_fails_closed_when_the_served_namespace_set_changes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let first = v2_search_payload_for_database(&database, "/v1/search?q=al&page_size=1").await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("codeployed first page must include a cursor");

    let response = v2_search_response_for_database_with_public_namespaces(
        &database,
        &format!("/v1/search?q=al&page_size=1&cursor={cursor}"),
        &["ens"],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_validates_cursor_before_deployment_readiness() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    let malformed = v2_search_response_for_database_with_public_namespaces(
        &database,
        "/v1/search?q=alpha&cursor=not-a-cursor",
        &[],
    )
    .await?;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(malformed).await?["error"]["code"],
        json!("invalid_input")
    );

    let valid = v2_search_response_for_database_with_public_namespaces(
        &database,
        "/v1/search?q=alpha",
        &[],
    )
    .await?;
    assert_eq!(valid.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(valid).await?["error"]["code"],
        json!("conflict")
    );

    database.cleanup().await
}

#[tokio::test]
async fn public_namespace_derivation_tracks_manifest_authority_and_ready_checkpoints() -> Result<()>
{
    let sepolia = TestDatabase::new_migrated().await?;
    sepolia
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
    sepolia
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": 107,
                "block_hash": "0xnamespace-sepolia",
                "timestamp": "2026-08-10T00:01:47Z"
            }
        }))
        .await?;
    let resource = Uuid::from_u128(0x5e901);
    let logical = seed_family_identity_inputs(
        &sepolia.pool,
        "ens",
        "alpha.eth",
        "ethereum-sepolia",
        107,
        "0xnamespace-sepolia",
        resource,
        Uuid::from_u128(0x5e902),
        Uuid::from_u128(0x5e903),
        "ens_v2",
    )
    .await?;
    let mut events = Vec::new();
    for (index, (kind, after)) in [
        ("RegistrationGranted", json!({"source_event":"LabelRegistered", "authority_kind":"ens_v2_registry", "registrant":V2_SEARCH_REGISTRY_OWNER, "expiry":u64::MAX})),
        ("TokenControlTransferred", json!({"source_event":"Transfer", "to":V2_SEARCH_REGISTRY_OWNER})),
    ].into_iter().enumerate() {
        let mut event = history_event(&format!("search-sepolia-{kind}"), Some(&logical), Some(resource),
            Some("ethereum-sepolia"), Some(107), Some("0xnamespace-sepolia"), Some("0xsepolia"), Some(index as i64), CanonicalityState::Canonical);
        event.event_kind = kind.into(); event.source_family = "ens_v2_registry_l1".into();
        event.before_state = json!({}); event.after_state = after; events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&sepolia.pool, &events).await?;
    rebuild_fixture_families(
        &sepolia.pool,
        "ethereum-sepolia",
        107,
        "0xnamespace-sepolia",
    )
    .await?;
    let sepolia_state = AppState::new_with_rpc_urls(
        sepolia.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    assert_eq!(
        crate::v2::support::derive_public_namespace_set(&sepolia_state)
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?
            .names(),
        ["ens"]
    );

    let response = app_router(sepolia_state)
        .oneshot(
            Request::builder()
                .uri("/v1/search?q=alpha")
                .body(Body::empty())
                .expect("search request must build"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        v2_search_names(payload["data"].as_array().expect("search data")),
        vec!["alpha.eth"]
    );
    assert_search_meta_chains(&payload, &["11155111"], &[]);
    sepolia.cleanup().await?;

    let codeployed = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&codeployed).await?;
    seed_v2_search_public_authority(&codeployed).await?;
    let codeployed_state = AppState::new_with_rpc_urls(
        codeployed.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    assert_eq!(
        crate::v2::support::derive_public_namespace_set(&codeployed_state)
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?
            .names(),
        ["basenames", "ens"]
    );

    begin_search_family_rebuild(&codeployed, "base-mainnet", 211, "0xsearch-public-base").await?;
    assert_eq!(
        crate::v2::support::derive_public_namespace_set(&codeployed_state)
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?
            .names(),
        ["ens"]
    );

    codeployed.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_request_narrows_when_a_publication_is_not_ready() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    begin_search_family_rebuild(&database, "base-mainnet", 211, "0xsearch-public-base").await?;

    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(
        Request::builder()
            .uri("/v1/search?q=alpha")
            .body(Body::empty())
            .expect("search request must build"),
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    assert_eq!(
        v2_search_names(payload["data"].as_array().expect("search data")),
        vec!["alpha.eth"]
    );
    assert_search_meta_chains(&payload, &["1"], &["8453"]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_request_returns_conflict_when_every_public_namespace_is_suppressed()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
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
            .uri("/v1/search?q=alpha")
            .body(Body::empty())
            .expect("bare search request must build"),
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload:#}");
    assert_eq!(payload["error"]["code"], json!("conflict"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_test_override_withholds_redo_suppressed_namespace_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    database
        .simulate_interpret_redo_begin("base-mainnet", "recompute_flags")
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/search?q=alpha")
                .body(Body::empty())
                .expect("bare search request must build"),
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    assert_eq!(
        v2_search_names(payload["data"].as_array().expect("search data")),
        vec!["alpha.eth"]
    );
    assert_search_meta_chains(&payload, &["1"], &["8453"]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_request_recovers_when_publication_becomes_ready() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    begin_search_family_rebuild(&database, "base-mainnet", 211, "0xsearch-public-base").await?;

    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let narrowed = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/search?q=alpha")
                .body(Body::empty())
                .expect("search request must build"),
        )
        .await?;
    assert_eq!(narrowed.status(), StatusCode::OK);
    assert_eq!(
        v2_search_names(
            read_json::<Value>(narrowed).await?["data"]
                .as_array()
                .expect("narrowed search data")
        ),
        vec!["alpha.eth"]
    );

    rebuild_fixture_families(&database.pool, "base-mainnet", 211, "0xsearch-public-base").await?;

    let recovered = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/search?q=alpha")
                .body(Body::empty())
                .expect("search request must build"),
        )
        .await?;
    let status = recovered.status();
    let payload: Value = read_json(recovered).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    assert_eq!(
        v2_search_names(payload["data"].as_array().expect("search data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert_search_meta_chains(&payload, &["1", "8453"], &[]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_suppresses_bare_redo_scope_but_refuses_explicit_redo_scope()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    database
        .simulate_interpret_redo_begin("base-mainnet", "recompute_flags")
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state SET current_block_number = current_block_number - 1
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );

    assert_eq!(
        crate::v2::support::derive_public_namespace_set(&state)
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?
            .names(),
        ["ens"]
    );

    let bare_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v1/search?q=alpha")
                .body(Body::empty())
                .expect("bare search request must build"),
        )
        .await?;
    assert_eq!(bare_response.status(), StatusCode::OK);
    let bare_payload: Value = read_json(bare_response).await?;
    assert_eq!(
        v2_search_names(bare_payload["data"].as_array().expect("bare search data")),
        vec!["alpha.eth"]
    );
    assert!(bare_payload["meta"]["as_of"]["1"].is_object());
    assert!(bare_payload["meta"]["as_of"].get("8453").is_none());
    assert_eq!(
        bare_payload["meta"]["as_of_completeness"]["8453"],
        json!({
            "completeness": "unsupported",
            "unsupported_reason": "temporarily_unavailable"
        })
    );

    let explicit_response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/search?q=alpha&namespace=basenames")
                .body(Body::empty())
                .expect("explicit search request must build"),
        )
        .await?;
    assert_eq!(explicit_response.status(), StatusCode::CONFLICT);
    let explicit_payload: Value = read_json(explicit_response).await?;
    assert_eq!(explicit_payload["error"]["code"], json!("stale"));
    assert!(explicit_payload.get("data").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_escapes_like_metacharacters() -> Result<()> {
    // `_under.eth` and `bunder.eth` both match the unescaped LIKE pattern `_und%`.
    let (database, underscore) = v2_search_payload("/v1/search?q=_und&namespace=ens").await?;
    assert_eq!(
        v2_search_names(underscore["data"].as_array().expect("underscore data")),
        vec!["_under.eth"]
    );

    // `contains` builds its own pattern, so it needs the same escaping. `_und` unescaped would
    // also match `bunder.eth`.
    let contains = v2_search_payload_for_database(
        &database,
        "/v1/search?q=_und&namespace=ens&match=contains",
    )
    .await?;
    assert_eq!(
        v2_search_names(contains["data"].as_array().expect("contains data")),
        vec!["_under.eth"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_paginates_without_overlap_or_gap() -> Result<()> {
    let (database, first) = v2_search_payload("/v1/search?q=a&page_size=1").await?;
    assert_eq!(
        v2_search_names(first["data"].as_array().expect("first page data")),
        vec!["alpha.base.eth"]
    );
    assert_eq!(first["page"]["has_more"], json!(true));

    let mut page = first;
    let mut seen = vec!["alpha.base.eth".to_owned()];
    // Bounded so a cursor that stops advancing fails the assertion instead of looping forever.
    for _ in 0..8 {
        let Some(cursor) = page["page"]["next_cursor"].as_str().map(str::to_owned) else {
            break;
        };
        page = v2_search_payload_for_database(
            &database,
            &format!("/v1/search?q=a&page_size=1&cursor={cursor}"),
        )
        .await?;
        assert_eq!(page["page"]["cursor"], json!(cursor));
        let names = v2_search_names(page["data"].as_array().expect("page data"));
        assert_eq!(names.len(), 1);
        seen.push(names[0].to_owned());
    }

    assert_eq!(
        seen,
        vec![
            "alpha.base.eth",
            "alpha.eth",
            "alpine.base.eth",
            "alpine.eth"
        ]
    );
    assert_eq!(page["page"]["has_more"], json!(false));
    assert_eq!(page["page"]["next_cursor"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_project_republication_during_public_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 211, "0xsearch-public-base").await?;
    control.resume().await;

    let response = request_task
        .await
        .context("search generation-change request task panicked")?
        .context("search generation-change request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("conflict")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_explicit_namespace_rejects_a_position_change_after_the_page_read() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha&namespace=ens")
                    .body(Body::empty())
                    .expect("explicit search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 999,
                "block_hash": "0xsearch-explicit-later",
                "timestamp": "2026-08-26T00:16:39Z"
            }
        }))
        .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("explicit search position-change request task panicked")?
        .context("explicit search position-change request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("conflict")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_explicit_unpublished_scope_stays_unavailable_until_rebuilt() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    begin_search_family_rebuild(
        &database,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    let uri = "/v1/search?q=alpha&namespace=ens";
    let response = v2_search_response_for_database(&database, uri).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        "stale"
    );
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "redo")
        .await?;
    database
        .simulate_interpret_redo_finish("ethereum-mainnet")
        .await?;
    let response = v2_search_response_for_database(&database, uri).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        "stale"
    );
    rebuild_fixture_families(
        &database.pool,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    let body = v2_search_payload_for_database(&database, uri).await?;
    assert_eq!(
        v2_search_names(body["data"].as_array().context("search data")?),
        vec!["alpha.eth"]
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_explicit_namespace_refuses_an_unpublished_new_head() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    begin_search_family_rebuild(
        &database,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-mainnet", "block_number":999,
            "block_hash":"0xsearch-explicit-suppressed-later", "timestamp":"2026-08-26T00:16:39Z"
        }}))
        .await?;
    let response =
        v2_search_response_for_database(&database, "/v1/search?q=alpha&namespace=ens").await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = read_json::<Value>(response).await?;
    assert_eq!(body["error"]["code"], "stale");
    assert!(body.get("data").is_none());
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_explicit_namespace_reports_publication_readiness_change_as_conflict()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let (_selection_guard, selection_control) =
        crate::v2::search_public_namespace_read_test_hooks::install_at(
            &database.lookup_pool,
            crate::v2::search_public_namespace_read_test_hooks::ReadHookPoint::AfterExplicitSelection,
        )
        .await?;
    let (_fallback_guard, fallback_control) =
        crate::v2::search_public_namespace_read_test_hooks::install_at(
            &database.lookup_pool,
            crate::v2::search_public_namespace_read_test_hooks::ReadHookPoint::BeforeUnfencedGenerations,
        )
        .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha&namespace=ens")
                    .body(Body::empty())
                    .expect("explicit search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        selection_control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    begin_search_family_rebuild(
        &database,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    selection_control.resume().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        fallback_control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    fallback_control.resume().await;

    let response = request_task
        .await
        .context("explicit search readiness request task panicked")?
        .context("explicit search readiness request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "payload: {payload}");
    assert_eq!(payload["error"]["code"], json!("conflict"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_explicit_namespace_rejects_project_republication_after_the_page_read()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha&namespace=ens")
                    .body(Body::empty())
                    .expect("explicit search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("explicit search republication request task panicked")?
        .context("explicit search republication request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("conflict")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_namespace_returns_conflict_when_interpret_redo_begins_during_read()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    let project_before = database
        .phase_state_fingerprint("ethereum-mainnet", "project")
        .await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "redo")
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces
         SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum-mainnet' AND raw_name = 'alpha.eth'",
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
        .context("bare search Interpret-redo request task panicked")?
        .context("bare search Interpret-redo request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("conflict"));
    assert!(
        payload.get("data").is_none(),
        "no partial page may be served"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_explicit_namespace_returns_stale_when_interpret_redo_begins_during_read()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    let project_before = database
        .phase_state_fingerprint("ethereum-mainnet", "project")
        .await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha&namespace=ens")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "redo")
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces
         SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum-mainnet' AND raw_name = 'alpha.eth'",
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
        .context("search Interpret-redo request task panicked")?
        .context("search Interpret-redo request failed")?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(
        payload.get("data").is_none(),
        "no partial page may be served"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_allows_interpret_live_progress_during_public_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    let interpret_before = database
        .phase_state_fingerprint("ethereum-mainnet", "interpret")
        .await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
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
        .context("search Interpret-progress request task panicked")?
        .context("search Interpret-progress request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        v2_search_names(payload["data"].as_array().expect("search data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_allows_manifest_freshness_change_without_authority_change() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    let (_guard, control) =
        crate::v2::search_public_namespace_read_test_hooks::install(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        control.wait_until_reached(),
    )
    .await
    .context("search read hook was not reached")?;
    sqlx::query(
        "UPDATE bigname_phase.manifest_versions
         SET loaded_at = loaded_at + INTERVAL '1 second'",
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("search manifest-refresh request task panicked")?
        .context("search manifest-refresh request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        v2_search_names(payload["data"].as_array().expect("search data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_cursor_anchor_changes() -> Result<()> {
    let (database, first) = v2_search_payload("/v1/search?q=al&namespace=ens&page_size=1").await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor")
        .to_owned();

    for uri in [
        format!("/v1/search?q=ga&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v1/search?q=al&match=contains&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v1/search?q=al&namespace=basenames&page_size=1&cursor={cursor}"),
        format!("/v1/search?q=al&page_size=1&cursor={cursor}"),
    ] {
        let response = v2_search_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            read_json::<Value>(response).await?["error"]["code"],
            json!("invalid_input"),
            "{uri}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_snapshot_selectors_and_accepts_explicit_latest() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    for (uri, message) in [
        (
            "/v1/search?q=al&namespace=ens&at=2026-04-17T00:01:48Z",
            "at is not supported because collection routes read latest state",
        ),
        (
            "/v1/search?q=al&namespace=ens&finality=safe",
            "finality must be latest because collection routes read latest state",
        ),
        (
            "/v1/search?q=al&namespace=ens&finality=finalized",
            "finality must be latest because collection routes read latest state",
        ),
    ] {
        let response = v2_search_response_for_database(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let error: Value = read_json(response).await?;
        assert_eq!(error["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(error["error"]["message"], json!(message), "{uri}");
    }

    let latest =
        v2_search_payload_for_database(&database, "/v1/search?q=al&namespace=ens&finality=latest")
            .await?;
    assert_search_meta_chains(&latest, &["1"], &[]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_unknown_params_and_returns_empty_matches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    let unknown =
        v2_search_response_for_database(&database, "/v1/search?q=al&namespace=ens&sort=name")
            .await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    let empty = v2_search_payload_for_database(&database, "/v1/search?q=nomatch").await?;
    assert_eq!(empty["data"], json!([]));
    assert_eq!(empty["page"]["has_more"], json!(false));
    assert_eq!(empty["page"]["next_cursor"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_discloses_request_scope_without_snapshot_tokens() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    for (uri, chains) in [
        ("/v1/search?q=alpha", &["1", "8453"][..]),
        ("/v1/search?q=alpha&namespace=ens", &["1"][..]),
        (
            "/v1/search?q=alpha&namespace=basenames",
            &["8453"][..],
        ),
    ] {
        let payload = v2_search_payload_for_database(&database, uri).await?;
        assert_search_meta_chains(&payload, chains, &[]);
    }

    database.cleanup().await
}

async fn v2_search_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let payload = v2_search_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_search_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_search_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    read_json(response).await
}

async fn v2_search_response_for_database(database: &TestDatabase, uri: &str) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("search request must build"),
        )
        .await
        .context("v2 search request failed")
}

async fn v2_search_payload_for_database_with_public_namespaces(
    database: &TestDatabase,
    uri: &str,
    public_namespaces: &[&str],
) -> Result<Value> {
    let response = v2_search_response_for_database_with_public_namespaces(
        database,
        uri,
        public_namespaces,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    read_json(response).await
}

async fn v2_search_response_for_database_with_public_namespaces(
    database: &TestDatabase,
    uri: &str,
    public_namespaces: &[&str],
) -> Result<Response> {
    app_router(database.app_state_with_public_namespaces(public_namespaces))
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("search request must build"),
        )
        .await
        .context("v2 search request failed")
}

// Missing current authority without an admitted canonical allocation remains absent from
// discovery. Callers read name detail or batch lookup for the partial identity record.
#[tokio::test]
async fn v2_search_omits_a_name_without_current_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_unbound_name_inputs(&database, "almost.eth", false).await?;
    let name = v2_name_record_payload_for_database(&database, "/v1/names/almost.eth").await?;
    assert_eq!(name["data"]["read_status"], "ok", "{name}");
    assert_eq!(name["data"]["status"], "unregistered");
    assert!(name["data"].get("authority").is_none());
    let page = v2_search_payload_for_database(&database, "/v1/search?q=al&namespace=ens").await?;
    assert_eq!(
        v2_search_names(page["data"].as_array().context("search data")?),
        vec!["alpha.eth", "alpine.eth"]
    );
    database.cleanup().await
}

// Finite registrar expiries past year 9999 stay searchable and retain every digit.
// Full uint64 grant/renewal behavior is covered by the admitted ENSv2 fixture in
// v2_unix_timestamps.rs; this existing registrar case stays within its retained signed range.
#[tokio::test]
async fn v2_search_serves_a_name_whose_expiry_exceeds_the_timestamp_range() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture_with_extreme_expiry(&database, true).await?;

    let payload =
        v2_search_payload_for_database(&database, "/v1/search?q=al&namespace=ens").await?;
    let rows = payload["data"]
        .as_array()
        .expect("search data must be an array");
    assert_eq!(v2_search_names(rows), vec!["alpha.eth", "alpine.eth"]);
    for row in rows {
        assert_eq!(
            row.get("expires_at"),
            Some(&json!("253402300800")),
            "finite expiry is not clipped by the calendar range: {row}"
        );
    }
    database.cleanup().await?;
    Ok(())
}

fn v2_search_names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("search row must include name"))
        .collect()
}

fn assert_search_meta_chains(payload: &Value, as_of: &[&str], suppressed: &[&str]) {
    let mut actual_as_of = payload["meta"]["as_of"]
        .as_object()
        .map(|positions| positions.keys().map(String::as_str).collect::<Vec<_>>())
        .unwrap_or_default();
    actual_as_of.sort_unstable();
    let mut expected_as_of = as_of.to_vec();
    expected_as_of.sort_unstable();
    assert_eq!(actual_as_of, expected_as_of);

    let mut actual_suppressed = payload["meta"]["as_of_completeness"]
        .as_object()
        .map(|positions| positions.keys().map(String::as_str).collect::<Vec<_>>())
        .unwrap_or_default();
    actual_suppressed.sort_unstable();
    let mut expected_suppressed = suppressed.to_vec();
    expected_suppressed.sort_unstable();
    assert_eq!(actual_suppressed, expected_suppressed);
    if suppressed.is_empty() {
        assert!(
            payload["meta"].get("as_of_completeness").is_none(),
            "clean search metadata must omit as_of_completeness"
        );
    }
    for chain_id in suppressed {
        assert_eq!(
            payload["meta"]["as_of_completeness"][chain_id],
            json!({
                "completeness": "unsupported",
                "unsupported_reason": "temporarily_unavailable"
            })
        );
    }
    assert!(payload["meta"].get("as_of_token").is_none());
}

async fn seed_v2_search_public_authority(database: &TestDatabase) -> Result<()> {
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
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 210,
                "block_hash": "0xsearch-public-ethereum",
                "timestamp": "2026-08-10T00:03:30Z"
            },
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 211,
                "block_hash": "0xsearch-public-base",
                "timestamp": "2026-08-10T00:03:31Z"
            }
        }))
        .await
}

async fn seed_v2_search_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_search_fixture_with_extreme_expiry(database, false).await
}

async fn seed_v2_search_fixture_with_extreme_expiry(
    database: &TestDatabase,
    extreme: bool,
) -> Result<()> {
    for (index, spec) in v2_search_specs().into_iter().enumerate() {
        let created_block = 100 + index as i64;
        let grant_block = 150 + index as i64;
        let created_hash = format!("0xsearch-created-{index}");
        let grant_hash = format!("0xsearch-grant-{index}");
        let created = parse_rfc3339_utc_timestamp(spec.created_at)
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .unix_timestamp();
        let granted = parse_rfc3339_utc_timestamp(spec.registered_at)
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .unix_timestamp();
        upsert_phase_raw_blocks(
            &database.pool,
            &[
                raw_block(spec.chain_id(), &created_hash, None, created_block, created),
                raw_block(spec.chain_id(), &grant_hash, None, grant_block, granted),
            ],
        )
        .await?;
        let logical = seed_family_identity_inputs(
            &database.pool,
            spec.namespace,
            spec.name,
            spec.chain_id(),
            created_block,
            &created_hash,
            spec.resource_id(),
            spec.token_lineage_id(),
            spec.surface_binding_id(),
            if spec.namespace == "basenames" {
                "basenames"
            } else {
                "ens_v1"
            },
        )
        .await?;
        if spec.namespace == "internal" {
            continue;
        }
        let registry = if spec.namespace == "basenames" {
            "basenames_base_registry"
        } else {
            "ens_v1_registry_l1"
        };
        let registrar = if spec.namespace == "basenames" {
            "basenames_base_registrar"
        } else {
            "ens_v1_registrar_l1"
        };
        let expiry = if extreme && matches!(spec.name, "alpha.eth" | "alpine.eth") {
            253_402_300_800
        } else {
            parse_rfc3339_utc_timestamp(spec.expires_at)
                .map_err(|error| anyhow::anyhow!("{error}"))?
                .unix_timestamp() as u64
        };
        let mut events = Vec::new();
        for (kind, family, block, hash, after) in [
            (
                "AuthorityTransferred",
                registry,
                created_block,
                created_hash,
                json!({"source_event":"Transfer","node":bigname_lookup::ens_namehash_hex(spec.name)?,"owner":spec.owner}),
            ),
            (
                "RegistrationGranted",
                registrar,
                grant_block,
                grant_hash,
                json!({"source_event":"NameRegistered","authority_kind":"registrar","registrant":spec.registrant,"expiry":expiry}),
            ),
        ] {
            let mut event = history_event(
                &format!("search-{}-{kind}", spec.name),
                Some(&logical),
                Some(spec.resource_id()),
                Some(spec.chain_id()),
                Some(block),
                Some(&hash),
                Some("0xsearch"),
                Some(0),
                CanonicalityState::Canonical,
            );
            event.namespace = spec.namespace.into();
            event.event_kind = kind.into();
            event.source_family = family.into();
            event.before_state = json!({});
            event.after_state = after;
            events.push(event);
        }
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    }
    database.seed_snapshot_selector_chain_positions(&json!({
        "ethereum":{"chain_id":"ethereum-mainnet","block_number":210,"block_hash":"0xsearch-public-ethereum","timestamp":"2026-08-10T00:03:30Z"},
        "base":{"chain_id":"base-mainnet","block_number":211,"block_hash":"0xsearch-public-base","timestamp":"2026-08-10T00:03:31Z"}
    })).await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-mainnet",
        210,
        "0xsearch-public-ethereum",
    )
    .await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 211, "0xsearch-public-base").await
}

#[derive(Default)]
struct V2SearchSpec {
    namespace: &'static str,
    name: &'static str,
    id: u128,
    owner: &'static str,
    registrant: &'static str,
    registered_at: &'static str,
    created_at: &'static str,
    expires_at: &'static str,
}

impl V2SearchSpec {
    fn chain_id(&self) -> &'static str {
        chain_id_for_namespace(self.namespace)
    }

    fn resource_id(&self) -> Uuid {
        Uuid::from_u128(self.id)
    }

    fn token_lineage_id(&self) -> Uuid {
        Uuid::from_u128(self.id + 1)
    }

    fn surface_binding_id(&self) -> Uuid {
        Uuid::from_u128(self.id + 2)
    }


}

fn v2_search_specs() -> Vec<V2SearchSpec> {
    vec![
        V2SearchSpec {
            namespace: "ens",
            name: "alpha.eth",
            id: 0xa100,
            owner: "0x00000000000000000000000000000000000000a1",
            registrant: "0x00000000000000000000000000000000000000a2",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "ens",
            name: "alpine.eth",
            id: 0xa200,
            owner: "0x0000000000000000000000000000000000000a21",
            registrant: "0x0000000000000000000000000000000000000a22",
            registered_at: "2024-02-02T00:00:00Z",
            created_at: "2023-02-02T00:00:00Z",
            expires_at: "2027-02-02T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "ens",
            name: "gamma.eth",
            id: 0xa300,
            owner: "0x0000000000000000000000000000000000000a31",
            registrant: "0x0000000000000000000000000000000000000a32",
            registered_at: "2024-03-02T00:00:00Z",
            created_at: "2023-03-02T00:00:00Z",
            expires_at: "2027-03-02T00:00:00Z",
        },
        // `_under.eth` and `bunder.eth` both match the unescaped LIKE prefix `_und%`.
        V2SearchSpec {
            namespace: "ens",
            name: "_under.eth",
            id: 0xa400,
            owner: "0x0000000000000000000000000000000000000a41",
            registrant: "0x0000000000000000000000000000000000000a42",
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2023-04-02T00:00:00Z",
            expires_at: "2027-04-02T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "ens",
            name: "bunder.eth",
            id: 0xa500,
            owner: "0x0000000000000000000000000000000000000a51",
            registrant: "0x0000000000000000000000000000000000000a52",
            registered_at: "2024-05-02T00:00:00Z",
            created_at: "2023-05-02T00:00:00Z",
            expires_at: "2027-05-02T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "ens",
            name: "precedence.eth",
            id: 0xd100,
            owner: V2_SEARCH_REGISTRY_OWNER,
            registrant: V2_SEARCH_REGISTRATION_REGISTRANT,
            registered_at: "2024-07-03T00:00:00Z",
            created_at: "2023-07-03T00:00:00Z",
            expires_at: "2027-07-03T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "basenames",
            name: "alpha.base.eth",
            id: 0xb100,
            owner: "0x0000000000000000000000000000000000000b11",
            registrant: "0x0000000000000000000000000000000000000b12",
            registered_at: "2024-08-02T00:00:00Z",
            created_at: "2023-08-02T00:00:00Z",
            expires_at: "2027-08-02T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "basenames",
            name: "alpine.base.eth",
            id: 0xb200,
            owner: "0x0000000000000000000000000000000000000b21",
            registrant: "0x0000000000000000000000000000000000000b22",
            registered_at: "2024-08-03T00:00:00Z",
            created_at: "2023-08-03T00:00:00Z",
            expires_at: "2027-08-03T00:00:00Z",
        },
        // Not a public namespace: the default namespace set must exclude it.
        V2SearchSpec {
            namespace: "internal",
            name: "alpha.internal",
            id: 0xc100,
            owner: "0x0000000000000000000000000000000000000c11",
            registrant: "0x0000000000000000000000000000000000000c12",
            registered_at: "2024-09-02T00:00:00Z",
            created_at: "2023-09-02T00:00:00Z",
            expires_at: "2027-09-02T00:00:00Z",
        },
    ]
}

async fn begin_search_family_rebuild(
    database: &TestDatabase,
    chain: &str,
    block: i64,
    hash: &str,
) -> Result<()> {
    let token = bigname_project::families::input_token(&database.pool, chain).await?;
    let outcome = bigname_project::families::apply(
        &database.pool,
        chain,
        &bigname_project::Marker {
            number: block,
            hash: hash.into(),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        )
        .with_max_blocks_per_run(1),
    )
    .await?;
    assert!(outcome.reset);
    assert_ne!(
        outcome.marker.as_ref().map(|marker| marker.number),
        Some(block)
    );
    Ok(())
}
