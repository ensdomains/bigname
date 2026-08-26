const V2_SEARCH_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000d01";
const V2_SEARCH_CONTROL_OWNER: &str = "0x0000000000000000000000000000000000000d02";
const V2_SEARCH_REGISTRATION_REGISTRANT: &str = "0x0000000000000000000000000000000000000d03";
const V2_SEARCH_CONTROL_REGISTRANT: &str = "0x0000000000000000000000000000000000000d04";

#[tokio::test]
async fn v2_search_prefix_returns_record_rows() -> Result<()> {
    let (database, payload) = v2_search_payload("/v2/search?q=al&namespace=ens").await?;

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
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(data[0]["registration_status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("2024-01-02T00:00:00Z"));
    assert_eq!(data[0]["created_at"], json!("2023-01-02T00:00:00Z"));
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert!(data[0].get("relations").is_none());
    assert!(data[0].get("is_primary").is_none());
    assert!(data[0].get("role_summary").is_none());
    assert!(data[0].get("labelhash").is_none());
    assert!(data[0].get("subname_count").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_uses_dictionary_owner_and_registrant_precedence() -> Result<()> {
    let (database, payload) = v2_search_payload("/v2/search?q=precedence&namespace=ens").await?;

    let data = payload["data"]
        .as_array()
        .expect("search data must be an array");
    assert_eq!(v2_search_names(data), vec!["precedence.eth"]);
    assert_eq!(data[0]["owner"], json!(V2_SEARCH_CONTROL_OWNER));
    assert_eq!(
        data[0]["registrant"],
        json!(V2_SEARCH_REGISTRATION_REGISTRANT)
    );
    assert_eq!(data[0]["registration_status"], json!("active"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_match_modes_and_q_validation() -> Result<()> {
    let (database, prefix) = v2_search_payload("/v2/search?q=amm&namespace=ens").await?;
    assert_eq!(prefix["data"], json!([]));

    let contains = v2_search_payload_for_database(
        &database,
        "/v2/search?q=amm&match=contains&namespace=ens",
    )
    .await?;
    assert_eq!(
        v2_search_names(contains["data"].as_array().expect("contains data")),
        vec!["gamma.eth"]
    );

    for uri in [
        "/v2/search?namespace=ens",
        "/v2/search?q=&namespace=ens",
        "/v2/search?q=al&match=suffix&namespace=ens",
    ] {
        let response = v2_search_response_for_database(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let error: Value = read_json(response).await?;
        assert_eq!(error["error"]["code"], json!("invalid_input"), "{uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_lowercases_q_and_filters_namespace() -> Result<()> {
    let (database, uppercase) = v2_search_payload("/v2/search?q=AL&namespace=ens").await?;
    assert_eq!(
        v2_search_names(uppercase["data"].as_array().expect("uppercase data")),
        vec!["alpha.eth", "alpine.eth"]
    );

    let public = v2_search_payload_for_database(&database, "/v2/search?q=alpha").await?;
    assert_eq!(
        v2_search_names(public["data"].as_array().expect("public data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert_search_meta_chains(&public, &["1", "8453"], &[]);

    let basenames =
        v2_search_payload_for_database(&database, "/v2/search?q=alpha&namespace=basenames").await?;
    assert_eq!(
        v2_search_names(basenames["data"].as_array().expect("basenames data")),
        vec!["alpha.base.eth"]
    );

    let unknown =
        v2_search_response_for_database(&database, "/v2/search?q=alpha&namespace=internal").await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_scope_matches_the_served_deployment_namespaces() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    let codeployed = v2_search_payload_for_database(&database, "/v2/search?q=alpha").await?;
    assert_eq!(
        v2_search_names(codeployed["data"].as_array().expect("codeployed data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert_search_meta_chains(&codeployed, &["1", "8453"], &[]);

    let ens_only = v2_search_payload_for_database_with_public_namespaces(
        &database,
        "/v2/search?q=alpha",
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
        "/v2/search?q=alpha&namespace=basenames",
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
        ("ethereum-sepolia", "ens_v2_sepolia_post_audit"),
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
                .uri("/v2/search?q=alpha&namespace=basenames")
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
                "block_number": 208,
                "block_hash": "0xsearch-coherence-ethereum",
                "timestamp": "2026-08-10T00:03:28Z"
            },
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 209,
                "block_hash": "0xsearch-coherence-base",
                "timestamp": "2026-08-10T00:03:29Z"
            }
        }))
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
                    .uri("/v2/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
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
                    .uri("/v2/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-sepolia",
            "ens_v2_sepolia_post_audit",
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
    assert!(payload.get("data").is_none(), "no partial page may be served");

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_bare_cursor_fails_closed_when_the_served_namespace_set_changes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let first = v2_search_payload_for_database(&database, "/v2/search?q=al&page_size=1").await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("codeployed first page must include a cursor");

    let response = v2_search_response_for_database_with_public_namespaces(
        &database,
        &format!("/v2/search?q=al&page_size=1&cursor={cursor}"),
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
        "/v2/search?q=alpha&cursor=not-a-cursor",
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
        "/v2/search?q=alpha",
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
    seed_v2_search_fixture(&sepolia).await?;
    sepolia
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-sepolia",
            "ens_v2_sepolia_post_audit",
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
                .uri("/v2/search?q=alpha")
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
    codeployed
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
    codeployed
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
    codeployed
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 108,
                "block_hash": "0xnamespace-mainnet",
                "timestamp": "2026-08-10T00:01:48Z"
            },
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 109,
                "block_hash": "0xnamespace-base",
                "timestamp": "2026-08-10T00:01:49Z"
            }
        }))
        .await?;
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

    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = 'manifest-authority:test'
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .execute(&codeployed.lookup_pool)
    .await?;
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
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = 'manifest-authority:test'
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;

    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(
        Request::builder()
            .uri("/v2/search?q=alpha")
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
async fn v2_search_bare_request_recovers_when_publication_becomes_ready() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = 'manifest-authority:test'
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;

    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let narrowed = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/search?q=alpha")
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

    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = $1
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;

    let recovered = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/search?q=alpha")
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
async fn v2_search_discloses_interpret_redo_for_bare_and_explicit_namespace_requests()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    seed_v2_search_public_authority(&database).await?;
    database
        .simulate_interpret_redo_begin("base-mainnet", "recompute_flags")
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
                .uri("/v2/search?q=alpha")
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
                .uri("/v2/search?q=alpha&namespace=basenames")
                .body(Body::empty())
                .expect("explicit search request must build"),
        )
        .await?;
    assert_eq!(explicit_response.status(), StatusCode::OK);
    let explicit_payload: Value = read_json(explicit_response).await?;
    assert_eq!(
        v2_search_names(
            explicit_payload["data"]
                .as_array()
                .expect("explicit search data")
        ),
        vec!["alpha.base.eth"]
    );
    assert!(explicit_payload["meta"].get("as_of").is_none());
    assert_eq!(
        explicit_payload["meta"]["as_of_completeness"]["8453"],
        json!({
            "completeness": "unsupported",
            "unsupported_reason": "temporarily_unavailable"
        })
    );
    assert!(
        explicit_payload["meta"]["as_of_completeness"]
            .get("1")
            .is_none(),
        "an explicit Basenames request must not disclose out-of-scope Ethereum"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_escapes_like_metacharacters() -> Result<()> {
    // `_under.eth` and `bunder.eth` both match the unescaped LIKE pattern `_und%`.
    let (database, underscore) = v2_search_payload("/v2/search?q=_und&namespace=ens").await?;
    assert_eq!(
        v2_search_names(underscore["data"].as_array().expect("underscore data")),
        vec!["_under.eth"]
    );

    // `al%` matches every `al`-prefixed name only if the percent stays a wildcard. The percent is
    // exercised query-side only: the phase fixtures recompute identity through ENSIP-15, which
    // rejects a literal `%` label, so a stored `percent%name.eth` cannot be seeded here.
    let percent =
        v2_search_payload_for_database(&database, "/v2/search?q=al%25&namespace=ens").await?;
    assert_eq!(percent["data"], json!([]));

    // `contains` builds its own pattern, so it needs the same escaping. `_und` unescaped would
    // also match `bunder.eth`.
    let contains = v2_search_payload_for_database(
        &database,
        "/v2/search?q=_und&namespace=ens&match=contains",
    )
    .await?;
    assert_eq!(
        v2_search_names(contains["data"].as_array().expect("contains data")),
        vec!["_under.eth"]
    );

    let contains_percent = v2_search_payload_for_database(
        &database,
        "/v2/search?q=al%25&namespace=ens&match=contains",
    )
    .await?;
    assert_eq!(contains_percent["data"], json!([]));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_paginates_without_overlap_or_gap() -> Result<()> {
    let (database, first) = v2_search_payload("/v2/search?q=a&page_size=1").await?;
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
            &format!("/v2/search?q=a&page_size=1&cursor={cursor}"),
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
                "block_number": 208,
                "block_hash": "0xsearch-generation-ethereum",
                "timestamp": "2026-08-10T00:03:28Z"
            },
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 209,
                "block_hash": "0xsearch-generation-base",
                "timestamp": "2026-08-10T00:03:29Z"
            }
        }))
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
                    .uri("/v2/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET updated_at = updated_at
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
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
                    .uri("/v2/search?q=alpha&namespace=ens")
                    .body(Body::empty())
                    .expect("explicit search request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
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
                    .uri("/v2/search?q=alpha&namespace=ens")
                    .body(Body::empty())
                    .expect("explicit search request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET updated_at = updated_at
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
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
async fn v2_search_rejects_interpret_redo_during_public_read() -> Result<()> {
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
                    .uri("/v2/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
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
    assert_eq!(payload["error"]["code"], json!("conflict"));
    assert!(payload.get("data").is_none(), "no partial page may be served");

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
                    .uri("/v2/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
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
                "block_number": 208,
                "block_hash": "0xsearch-refresh-ethereum",
                "timestamp": "2026-08-10T00:03:28Z"
            },
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 209,
                "block_hash": "0xsearch-refresh-base",
                "timestamp": "2026-08-10T00:03:29Z"
            }
        }))
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
                    .uri("/v2/search?q=alpha")
                    .body(Body::empty())
                    .expect("search request must build"),
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
    let (database, first) = v2_search_payload("/v2/search?q=al&namespace=ens&page_size=1").await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor")
        .to_owned();

    for uri in [
        format!("/v2/search?q=ga&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&match=contains&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&namespace=basenames&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&page_size=1&cursor={cursor}"),
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
            "/v2/search?q=al&namespace=ens&at=2026-04-17T00:01:48Z",
            "at is not supported because collection routes read latest state",
        ),
        (
            "/v2/search?q=al&namespace=ens&finality=safe",
            "finality must be latest because collection routes read latest state",
        ),
        (
            "/v2/search?q=al&namespace=ens&finality=finalized",
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
        v2_search_payload_for_database(&database, "/v2/search?q=al&namespace=ens&finality=latest")
            .await?;
    assert_search_meta_chains(&latest, &["1"], &[]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_unknown_params_and_returns_empty_matches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    let unknown =
        v2_search_response_for_database(&database, "/v2/search?q=al&namespace=ens&sort=name")
            .await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    let empty = v2_search_payload_for_database(&database, "/v2/search?q=nomatch").await?;
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
        ("/v2/search?q=alpha", &["1", "8453"][..]),
        ("/v2/search?q=alpha&namespace=ens", &["1"][..]),
        (
            "/v2/search?q=alpha&namespace=basenames",
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

// Search carries no row-local status or unsupported-reason field, so a name whose exact-name
// authority is unsupported is omitted whatever the reason rather than served from a registration
// no selected authority backs. Callers read name detail or batch lookup for the reason.
#[tokio::test]
async fn v2_search_omits_every_unsupported_exact_name() -> Result<()> {
    for reason in [
        "conflicting_current_ens_authority",
        "independent_ens_deployments_overlap",
        "ensv2_exact_name_profile_shadow",
        "current_authority_not_projected",
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_search_fixture(&database).await?;

        // Anti-vacuity: both names are served while the projection still supports them.
        let before =
            v2_search_payload_for_database(&database, "/v2/search?q=al&namespace=ens").await?;
        assert_eq!(
            v2_search_names(before["data"].as_array().expect("search data must be an array")),
            vec!["alpha.eth", "alpine.eth"]
        );

        sqlx::query(
            "UPDATE bigname_phase.name_current
             SET support_status = 'unsupported', unsupported_reason = $1
             WHERE raw_name = 'alpha.eth'",
        )
        .bind(reason)
        .execute(&database.pool)
        .await?;

        let payload =
            v2_search_payload_for_database(&database, "/v2/search?q=al&namespace=ens").await?;
        assert_eq!(
            v2_search_names(payload["data"].as_array().expect("search data must be an array")),
            vec!["alpine.eth"],
            "{reason} was not omitted from search"
        );
        database.cleanup().await?;
    }
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
    let specs = v2_search_specs();

    let raw_blocks = specs
        .iter()
        .map(|spec| {
            raw_block(
                spec.chain_id(),
                &spec.block_hash(),
                None,
                spec.block_number,
                1_717_190_000 + spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &raw_blocks).await?;

    let surfaces = specs
        .iter()
        .map(|spec| {
            collection_name_surface(
                &spec.logical_name_id(),
                spec.name,
                &spec.namehash(),
                spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    upsert_test_name_surfaces(&database.pool, &surfaces).await?;

    let token_lineages = specs
        .iter()
        .map(|spec| TokenLineage {
            token_lineage_id: spec.token_lineage_id(),
            chain_id: spec.chain_id().to_owned(),
            block_hash: spec.block_hash(),
            block_number: spec.block_number,
            provenance: json!({"seed": "v2_search"}),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_test_token_lineages(&database.pool, &token_lineages).await?;

    let resources = specs
        .iter()
        .map(|spec| Resource {
            resource_id: spec.resource_id(),
            token_lineage_id: Some(spec.token_lineage_id()),
            chain_id: spec.chain_id().to_owned(),
            block_hash: spec.block_hash(),
            block_number: spec.block_number,
            provenance: json!({"seed": "v2_search"}),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_test_resources(&database.pool, &resources).await?;

    let bindings = specs
        .iter()
        .map(|spec| SurfaceBinding {
            surface_binding_id: spec.surface_binding_id(),
            logical_name_id: spec.logical_name_id(),
            resource_id: spec.resource_id(),
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            authority_arm: if spec.namespace == "basenames" {
                "basenames"
            } else {
                "ens_v1"
            }
            .to_owned(),
            active_from: timestamp(1_717_190_000 + spec.block_number),
            active_to: None,
            chain_id: spec.chain_id().to_owned(),
            block_hash: spec.block_hash(),
            block_number: spec.block_number,
            provenance: json!({"seed": "v2_search"}),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_test_surface_bindings(&database.pool, &bindings).await?;

    for spec in &specs {
        database
            .insert_name_current_row(address_name_name_current_row(
                &spec.logical_name_id(),
                spec.name,
                spec.name,
                &spec.namehash(),
                spec.surface_binding_id(),
                spec.resource_id(),
                Some(spec.token_lineage_id()),
                spec.block_number,
                spec.declared_summary(),
            ))
            .await?;
    }

    Ok(())
}

#[derive(Default)]
struct V2SearchSpec {
    namespace: &'static str,
    name: &'static str,
    id: u128,
    block_number: i64,
    owner: &'static str,
    control_owner: Option<&'static str>,
    registrant: &'static str,
    control_registrant: Option<&'static str>,
    registered_at: &'static str,
    created_at: &'static str,
    expires_at: &'static str,
}

impl V2SearchSpec {
    fn logical_name_id(&self) -> String {
        format!("{}:{}", self.namespace, self.name)
    }

    fn namehash(&self) -> String {
        format!("node:{}", self.name)
    }

    fn chain_id(&self) -> &'static str {
        chain_id_for_namespace(self.namespace)
    }

    fn block_hash(&self) -> String {
        format!("0xsearch{:04x}", self.block_number)
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

    fn declared_summary(&self) -> Value {
        let mut control = json!({ "registry_owner": self.owner, "expiry": self.expires_at });
        if let Some(control_owner) = self.control_owner {
            control["owner"] = json!(control_owner);
        }
        if let Some(control_registrant) = self.control_registrant {
            control["registrant"] = json!(control_registrant);
        }
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar",
                "registrant": self.registrant,
                "registered_at": self.registered_at,
                "created_at": self.created_at,
                "expiry": self.expires_at
            },
            "control": control
        })
    }
}

fn v2_search_specs() -> Vec<V2SearchSpec> {
    vec![
        V2SearchSpec {
            namespace: "ens",
            name: "alpha.eth",
            id: 0xa100,
            block_number: 201,
            owner: "0x00000000000000000000000000000000000000a1",
            registrant: "0x00000000000000000000000000000000000000a2",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "alpine.eth",
            id: 0xa200,
            block_number: 202,
            owner: "0x0000000000000000000000000000000000000a21",
            registrant: "0x0000000000000000000000000000000000000a22",
            registered_at: "2024-02-02T00:00:00Z",
            created_at: "2023-02-02T00:00:00Z",
            expires_at: "2027-02-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "gamma.eth",
            id: 0xa300,
            block_number: 203,
            owner: "0x0000000000000000000000000000000000000a31",
            registrant: "0x0000000000000000000000000000000000000a32",
            registered_at: "2024-03-02T00:00:00Z",
            created_at: "2023-03-02T00:00:00Z",
            expires_at: "2027-03-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        // `_under.eth` and `bunder.eth` both match the unescaped LIKE prefix `_und%`.
        V2SearchSpec {
            namespace: "ens",
            name: "_under.eth",
            id: 0xa400,
            block_number: 204,
            owner: "0x0000000000000000000000000000000000000a41",
            registrant: "0x0000000000000000000000000000000000000a42",
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2023-04-02T00:00:00Z",
            expires_at: "2027-04-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "bunder.eth",
            id: 0xa500,
            block_number: 205,
            owner: "0x0000000000000000000000000000000000000a51",
            registrant: "0x0000000000000000000000000000000000000a52",
            registered_at: "2024-05-02T00:00:00Z",
            created_at: "2023-05-02T00:00:00Z",
            expires_at: "2027-05-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "precedence.eth",
            id: 0xd100,
            block_number: 206,
            owner: V2_SEARCH_REGISTRY_OWNER,
            control_owner: Some(V2_SEARCH_CONTROL_OWNER),
            registrant: V2_SEARCH_REGISTRATION_REGISTRANT,
            control_registrant: Some(V2_SEARCH_CONTROL_REGISTRANT),
            registered_at: "2024-07-03T00:00:00Z",
            created_at: "2023-07-03T00:00:00Z",
            expires_at: "2027-07-03T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "basenames",
            name: "alpha.base.eth",
            id: 0xb100,
            block_number: 207,
            owner: "0x0000000000000000000000000000000000000b11",
            registrant: "0x0000000000000000000000000000000000000b12",
            registered_at: "2024-08-02T00:00:00Z",
            created_at: "2023-08-02T00:00:00Z",
            expires_at: "2027-08-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "basenames",
            name: "alpine.base.eth",
            id: 0xb200,
            block_number: 208,
            owner: "0x0000000000000000000000000000000000000b21",
            registrant: "0x0000000000000000000000000000000000000b22",
            registered_at: "2024-08-03T00:00:00Z",
            created_at: "2023-08-03T00:00:00Z",
            expires_at: "2027-08-03T00:00:00Z",
            ..V2SearchSpec::default()
        },
        // Not a public namespace: the default namespace set must exclude it.
        V2SearchSpec {
            namespace: "internal",
            name: "alpha.internal",
            id: 0xc100,
            block_number: 209,
            owner: "0x0000000000000000000000000000000000000c11",
            registrant: "0x0000000000000000000000000000000000000c12",
            registered_at: "2024-09-02T00:00:00Z",
            created_at: "2023-09-02T00:00:00Z",
            expires_at: "2027-09-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
    ]
}
