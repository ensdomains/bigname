#[tokio::test]
async fn v2_lookup_rejects_invalid_request_shapes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for (uri, body) in [
        (
            "/v2/lookup",
            json!({"inputs": [{"id": "both", "name": "alice.eth", "address": "0x0000000000000000000000000000000000000abc"}]}),
        ),
        ("/v2/lookup", json!({"inputs": [{"id": "neither"}]})),
        ("/v2/lookup", json!({"inputs": [{"id": "", "name": "alice.eth"}]})),
        (
            "/v2/lookup",
            json!({"profile": "detail", "extra": true, "inputs": []}),
        ),
        (
            "/v2/lookup",
            json!({"namespace": "ens", "inputs": [{"id": "addr", "address": "0x0000000000000000000000000000000000000abc"}]}),
        ),
    ] {
        let response = v2_lookup_response_for_database(&database, uri, body).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"));
    }

    for uri in [
        "/v2/lookup?at=2026-04-17T00:00:00Z",
        "/v2/lookup?finality=safe",
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
        "/v2/lookup",
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
        "/v2/lookup",
        json!({"inputs": [{"address": "not-an-address"}]}),
        &[],
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    let response = v2_lookup_response_for_database_with_public_namespaces(
        &database,
        "/v2/lookup",
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
        "/v2/lookup",
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
        "/v2/lookup",
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
    database
        .simulate_interpret_redo_begin("base-mainnet", "recompute_flags")
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/lookup")
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
            .uri("/v2/lookup")
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
            .uri("/v2/lookup")
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
            .uri("/v2/lookup")
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
    let replay = v2_get_json(&database, &format!("/v2/names/case.eth?at={token}")).await?;
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
async fn v2_lookup_withholds_fields_for_unsupported_name_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:authority-gap.eth",
        "authority-gap.eth",
        "authority-gap.eth",
        "namehash:authority-gap.eth",
        Uuid::from_u128(0x5a0191),
        Uuid::from_u128(0x5a0192),
        Uuid::from_u128(0x5a0193),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;

    // Keyed on the unsupported status, not on a list of reasons: a projection reason with no
    // partial-serve contract withholds the same fields, under its public name.
    for (reason, expected) in [
        (
            "conflicting_current_ens_authority",
            "conflicting_current_ens_authority",
        ),
        (
            "independent_ens_deployments_overlap",
            "independent_ens_deployments_overlap",
        ),
        (
            "ensv2_exact_name_profile_shadow",
            "exact_name_profile_not_supported",
        ),
        (
            "a_reason_this_build_has_never_seen",
            "a_reason_this_build_has_never_seen",
        ),
        (
            "future_projection_gap",
            "unsupported_reason_unrecognized",
        ),
    ] {
        sqlx::query(
            "UPDATE name_current
             SET support_status = 'unsupported', unsupported_reason = $1
             WHERE raw_name = 'authority-gap.eth'",
        )
        .bind(reason)
        .execute(&database.lookup_pool)
        .await?;

        for profile in ["detail", "feed"] {
            let payload = v2_lookup_json(
                &database,
                json!({
                    "profile": profile,
                    "inputs": [{"name": "authority-gap.eth"}]
                }),
            )
            .await?;
            assert_eq!(payload["data"][0]["status"], "unsupported", "{reason}");
            assert_eq!(payload["data"][0]["unsupported_reason"], expected);
            assert_eq!(
                payload["data"][0]["record"],
                json!({
                    "name":"authority-gap.eth",
                    "display_name":"authority-gap.eth",
                    "namespace":"ens",
                    "namehash":bigname_lookup::ens_namehash_hex("authority-gap.eth")?,
                    "status":"unsupported",
                    "unsupported_reason":expected
                }),
                "{reason} served fields beyond the identity-only record"
            );
        }
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_withholds_resolver_without_projected_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query(
        "UPDATE name_current
         SET support_status = 'unsupported',
             unsupported_reason = 'current_authority_not_projected'
         WHERE raw_name = 'alice.eth'",
    )
    .execute(&database.lookup_pool)
    .await?;

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
    // The record keeps the full detail shape for this reason; only the
    // retained internal resolver pointer is withheld.
    assert_eq!(record["addresses"]["60"], json!(address));
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
async fn v2_lookup_withholds_retained_inventory_for_released_tombstone() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:released.eth",
        "released.eth",
        "released.eth",
        "namehash:released.eth",
        Uuid::from_u128(0x5a0401),
        Uuid::from_u128(0x5a0402),
        Uuid::from_u128(0x5a0403),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    // The seeded inventory row and declared resolver stay attached: a released
    // tombstone must not serve them even if projection state loss retains them.
    sqlx::query(
        "UPDATE name_current
         SET declared_summary =
             jsonb_set(declared_summary, '{registration,status}', '\"released\"')
         WHERE raw_name = 'released.eth'",
    )
    .execute(&database.lookup_pool)
    .await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "released.eth"}]}),
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
async fn v2_lookup_ignores_stale_audit_inventory_for_reservation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:reserved.eth",
        "reserved.eth",
        "reserved.eth",
        "namehash:reserved.eth",
        Uuid::from_u128(0x5a0411),
        Uuid::from_u128(0x5a0412),
        Uuid::from_u128(0x5a0413),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 declared_summary
                     #- '{control,owner}'
                     #- '{control,registry_owner}'
                     #- '{control,registrant}'
                     #- '{registration,registrant}'
                     #- '{registration,registered_at}',
                 '{registration,status}',
                 '\"reserved\"'
             ),
             '{registration,authority_kind}',
             '\"ens_v2_registry\"'
         )
         WHERE raw_name = 'reserved.eth'",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES
             ('ethereum-mainnet', '0xorphaned-audit-inventory', 39,
              '2026-04-17T00:00:39Z', 'canonical')",
    )
    .execute(&database.lookup_pool)
    .await?;
    let updated = sqlx::query(
        "UPDATE record_inventory_current inventory
         SET chain_positions = inventory.chain_positions || jsonb_build_object(
             'target_block_number', 39,
             'target_block_hash', '0xorphaned-audit-inventory'
         ),
         canonicality_summary = inventory.canonicality_summary || jsonb_build_object(
             'target_block_number', 39,
             'target_block_hash', '0xorphaned-audit-inventory'
         )
         FROM name_current name
         WHERE name.resource_id = inventory.resource_id
           AND name.raw_name = 'reserved.eth'",
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "reserved.eth"}]}),
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
    sqlx::query(
        r#"
        UPDATE record_inventory_current inventory
        SET entries = $1
        FROM name_current name
        WHERE name.resource_id = inventory.resource_id
          AND name.raw_name = 'bytes.eth'
        "#,
    )
    .bind(json!([
        {
            "record_key": "addr:0",
            "record_family": "addr",
            "selector_key": "0",
            "status": "success",
            "value": {"encoding": "hex", "bytes": "0x001122"}
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": {"encoding": "hex", "bytes": "0xe3010170"}
        }
    ]))
    .execute(&database.lookup_pool)
    .await?;

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
    seed_identity_name(
        &database,
        "ens:unsupported-inventory.eth",
        "unsupported-inventory.eth",
        "unsupported-inventory.eth",
        "namehash:unsupported-inventory.eth",
        Uuid::from_u128(0x5a0107),
        Uuid::from_u128(0x5a0108),
        Uuid::from_u128(0x5a0109),
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE record_inventory_current inventory
        SET support_status = 'unsupported',
            unsupported_reason = 'resolver_classification_missing',
            unsupported_families = '[{"record_family":"resolver_classification","unsupported_reason":"resolver_classification_missing"}]'::jsonb
        FROM name_current name
        WHERE name.resource_id = inventory.resource_id
          AND name.raw_name = 'unsupported-inventory.eth'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;

    let payload = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "unsupported-inventory.eth"}]}),
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

    let response = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
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
async fn v2_lookup_rejects_address_relation_from_another_phase_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage ( \
             chain_id, block_hash, block_number, block_timestamp, canonicality_state \
         ) VALUES ( \
             'ethereum-mainnet', '0xfuture-publication', 43, \
             '2026-04-17T00:00:43Z', 'canonical' \
         )",
    )
    .execute(&database.lookup_pool)
    .await?;
    let updated = sqlx::query(
        r#"
        UPDATE bigname_phase.address_names_current
        SET chain_positions = jsonb_build_object(
                'block_number', 43,
                'block_hash', '0xfuture-publication',
                'target_block_number', 43,
                'target_block_hash', '0xfuture-publication'
            ),
            canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 43,
                'target_block_hash', '0xfuture-publication'
            )
        WHERE lower(raw_name) = 'alice.eth'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);

    let response = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
        json!({"inputs": [{"address": address}]}),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_excludes_lower_height_orphaned_name_and_relation_targets() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES
            ('ethereum-mainnet', '0xorphaned-lookup-name', 39,
             '2026-04-17T00:00:39Z', 'orphaned'),
            ('ethereum-mainnet', '0xorphaned-lookup-relation', 40,
             '2026-04-17T00:00:40Z', 'orphaned');
        UPDATE bigname_phase.name_current
        SET canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 39,
                'target_block_hash', '0xorphaned-lookup-name'
            )
        WHERE lower(raw_name) = 'alice.eth';
        UPDATE bigname_phase.address_names_current
        SET chain_positions = jsonb_build_object(
                'block_number', 40,
                'block_hash', '0xorphaned-lookup-relation',
                'target_block_number', 40,
                'target_block_hash', '0xorphaned-lookup-relation'
            ),
            canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 40,
                'target_block_hash', '0xorphaned-lookup-relation'
            )
        WHERE lower(raw_name) = 'bob.eth';
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;

    let direct = v2_lookup_json(
        &database,
        json!({"inputs": [{"id": "orphaned-name", "name": "alice.eth"}]}),
    )
    .await?;
    assert_eq!(direct["data"][0]["status"], json!("not_found"));

    let reverse = v2_lookup_json(
        &database,
        json!({"inputs": [{"id": "orphaned-reverse", "address": address}]}),
    )
    .await?;
    assert_eq!(reverse["data"][0]["status"], json!("ok"));
    assert_eq!(reverse["data"][0]["records"], json!([]));
    assert_eq!(reverse["data"][0]["page"]["total_count"], json!(0));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_excludes_unsupported_phase_relations() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query(
        "UPDATE address_names_current
         SET support_status = 'unsupported', unsupported_reason = 'relation_pending'
         WHERE lower(raw_name) = 'alice.eth'",
    )
    .execute(&database.lookup_pool)
    .await?;

    let payload = v2_lookup_json(&database, json!({"inputs": [{"address": address}]})).await?;

    assert_eq!(payload["data"][0]["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(payload["data"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(payload["data"][0]["page"]["total_count"], json!(1));

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
async fn v2_lookup_rejects_primary_claim_from_future_phase_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage ( \
             chain_id, block_hash, block_number, block_timestamp, canonicality_state \
         ) VALUES ( \
             'ethereum-mainnet', '0xfuture-primary', 43, \
             '2026-04-17T00:00:43Z', 'canonical' \
         )",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE primary_names_current
        SET claim_provenance = claim_provenance
            || '{"target_block_number":43,"target_block_hash":"0xfuture-primary"}'::jsonb
        WHERE address = lower($1) AND namespace = 'ens' AND coin_type = '60'
        "#,
    )
    .bind(address)
    .execute(&database.lookup_pool)
    .await?;

    let response = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
        json!({"inputs": [{"address": address}]}),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_paginates_normalizable_phase_primary_claim() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query(
        "UPDATE primary_names_current
         SET raw_claim_name = 'Alice.eth', claim_name_is_normalized = false
         WHERE address = lower($1) AND namespace = 'ens' AND coin_type = '60'",
    )
    .bind(address)
    .execute(&database.lookup_pool)
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
async fn v2_lookup_uses_the_event_baseline_for_an_orphaned_hydration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES (
             'ethereum-mainnet', '0xorphaned-hydration', 42,
             '2026-04-17T00:00:42Z', 'orphaned'
         )",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE primary_names_current
        SET claim_status = 'success', raw_claim_name = 'alice.eth',
            claim_name_is_normalized = true,
            claim_provenance = claim_provenance || jsonb_build_object(
                'canonical_head_multicall_hydration', jsonb_build_object(
                    'chain_id', 'ethereum-mainnet',
                    'block_number', 42,
                    'block_hash', '0xorphaned-hydration',
                    'baseline', jsonb_build_object(
                        'claim_status', 'unsupported',
                        'raw_claim_name', NULL,
                        'claim_name_is_normalized', false,
                        'unsupported_reason', 'legacy_resolver_does_not_emit_name'
                    )
                )
            )
        WHERE address = lower($1) AND namespace = 'ens' AND coin_type = '60'
        "#,
    )
    .bind(address)
    .execute(&database.lookup_pool)
    .await?;

    let payload = v2_lookup_json(&database, json!({"inputs": [{"address": address}]})).await?;
    let alice = payload["data"][0]["records"]
        .as_array()
        .expect("reverse records must be an array")
        .iter()
        .find(|record| record["name"] == json!("alice.eth"))
        .expect("alice relation must remain readable");
    assert_eq!(alice["is_primary"], json!(false));

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
    advance_v2_lookup_ethereum_head(&database, 39, "0xlookup-before-reorg").await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/lookup")
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
                    .uri("/v2/lookup")
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
    advance_v2_lookup_ethereum_head(&database, 40, "0xlookup-after-publication-race").await?;
    sqlx::query("DELETE FROM name_current WHERE raw_name = 'publication-race.eth'")
        .execute(&database.lookup_pool)
        .await?;
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
                .uri("/v2/lookup")
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
        &format!("/v2/names/missing.eth?at={token}"),
    )
    .await?;
    assert_eq!(replay.status(), StatusCode::NOT_FOUND);

    let collection = v2_get_response(
        &database,
        &format!("/v2/search?q=missing&namespace=ens&at={token}"),
    )
    .await?;
    assert_eq!(collection.status(), StatusCode::BAD_REQUEST);
    let collection_error: Value = read_json(collection).await?;
    assert_eq!(
        collection_error["error"]["message"],
        json!("at is not supported because collection routes read latest state")
    );

    let union_replay =
        v2_get_response(&database, &format!("/v2/search?q=missing&at={token}")).await?;
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
        v2_get_response(&database, &format!("/v2/search?q=missing&at={public_token}")).await?;
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
    assert_eq!(
        first_page["data"][0]["records"][0]["relations"],
        json!(["owner"])
    );
    assert_eq!(first_page["data"][0]["page"]["cursor"], Value::Null);
    assert_eq!(first_page["data"][0]["page"]["page_size"], json!(1));
    assert_eq!(first_page["data"][0]["page"]["total_count"], json!(2));
    assert_eq!(first_page["data"][0]["page"]["has_more"], json!(true));
    let cursor = first_page["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must include next_cursor");

    advance_v2_lookup_ethereum_head(&database, 43, "0xlookup-advanced").await?;

    let second_page = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
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
        "/v2/lookup",
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
    // A successful claim whose stored spelling does not normalize is one row's defect. The batched
    // reverse read must still answer, marking nothing primary, rather than failing every input.
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
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

#[tokio::test]
async fn v2_lookup_reverse_orders_pages_by_the_is_primary_it_returns() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    // The marker says these stored bytes are the published normalized form, so they are served
    // unchanged and no longer equal the current name row. Paging orders by `is_primary`, so the
    // ordering predicate and the emitted flag have to be derived the same way. The claim names the
    // second row in page order, so a re-normalizing ordering would sort it first and the keyset
    // predicate — built from the emitted flag — would then skip the first row entirely.
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some("Bob.eth"),
        true,
    )
    .await?;

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..3 {
        let mut input = json!({"id": "addr", "address": address, "page_size": 1});
        if let Some(cursor) = cursor.as_deref() {
            input["cursor"] = json!(cursor);
        }
        let payload = v2_lookup_json(
            &database,
            json!({"profile": "detail", "inputs": [input]}),
        )
        .await?;
        let records = payload["data"][0]["records"]
            .as_array()
            .expect("reverse lookup records must be an array");
        for record in records {
            assert_eq!(
                record["is_primary"],
                json!(false),
                "a claim served in its stored spelling must not mark the normalized name primary"
            );
            seen.push(
                record["name"]
                    .as_str()
                    .expect("record name must be a string")
                    .to_owned(),
            );
        }
        match payload["data"][0]["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }

    assert_eq!(seen, vec!["alice.eth".to_owned(), "bob.eth".to_owned()]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_keeps_primary_order_and_flag_coherent_across_projection_rewrite()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    let (_guard, control) =
        crate::v2::support::identity_facade_primary_coherence_test_hooks::install(
            &database.lookup_pool,
        )
        .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "profile": "detail",
                            "inputs": [{"address": address, "page_size": 1}]
                        })
                        .to_string(),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
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
    control.resume().await;

    let response = request_task
        .await
        .context("reverse lookup request task panicked")?
        .context("reverse lookup request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    let records = payload["data"][0]["records"]
        .as_array()
        .expect("reverse lookup records must be an array");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("alice.eth"));
    assert_eq!(
        records[0]["is_primary"],
        json!(true),
        "the page must emit the same primary-name generation that ordered it"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_reverse_uses_candidate_name_for_order_flag_and_cursor_across_name_rewrite()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    let (guard, control) =
        crate::v2::support::identity_facade_primary_coherence_test_hooks::install(
            &database.lookup_pool,
        )
        .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "profile": "detail",
                            "inputs": [{"address": address, "page_size": 1}]
                        })
                        .to_string(),
                    ))
                    .expect("lookup request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    let rewrite = sqlx::query(
        "UPDATE bigname_phase.name_current
         SET raw_name = 'rewritten-alice.eth'
         WHERE raw_name = 'alice.eth'",
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(rewrite.rows_affected(), 1);
    control.resume().await;

    let response = request_task
        .await
        .context("reverse lookup request task panicked")?
        .context("reverse lookup request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET raw_name = 'alice.eth'
         WHERE raw_name = 'rewritten-alice.eth'",
    )
    .execute(&database.lookup_pool)
    .await?;
    crate::v2::support::identity_facade_primary_coherence_test_hooks::uninstall(
        &database.lookup_pool,
    )
    .await?;
    drop(guard);

    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");
    assert_eq!(
        json!({
            "name": payload["data"][0]["records"][0]["name"],
            "display_name": payload["data"][0]["records"][0]["display_name"],
            "token_id": payload["data"][0]["records"][0]["token_id"],
            "is_primary": payload["data"][0]["records"][0]["is_primary"],
        }),
        json!({
            "name": "alice.eth",
            "display_name": "alice.eth",
            "token_id": "70564938991660933374592024341600875602376452319261984317470407481576058979585",
            "is_primary": true
        })
    );
    assert_eq!(payload["data"][0]["page"]["has_more"], json!(true));
    let cursor = payload["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must include next_cursor");

    let next_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    assert_eq!(lookup_record_names(&next_page), vec!["bob.eth"]);

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
async fn v2_lookup_reverse_page_and_count_include_primary_when_matching_relation_is_unreadable()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let logical_name_id = "ens:membership-edge.eth";
    let normalized_name = "membership-edge.eth";
    let readable_resource_id = Uuid::from_u128(0x5a0221);
    let readable_token_lineage_id = Uuid::from_u128(0x5a0222);
    let readable_surface_binding_id = Uuid::from_u128(0x5a0223);

    seed_identity_name(
        &database,
        logical_name_id,
        "membership-edge.eth",
        normalized_name,
        "namehash:membership-edge.eth",
        readable_resource_id,
        readable_token_lineage_id,
        readable_surface_binding_id,
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        46,
    )
    .await?;

    // The primary-matching manager relation is unreadable, while the owner relation for the same
    // current name remains readable and therefore owns page membership.
    let unreadable_resource_id = Uuid::from_u128(0x5a0231);
    let unreadable_token_lineage_id = Uuid::from_u128(0x5a0232);
    let unreadable_surface_binding_id = Uuid::from_u128(0x5a0233);
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            unreadable_token_lineage_id,
            "0xresource",
            99,
        )],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            unreadable_resource_id,
            Some(unreadable_token_lineage_id),
            "0xresource",
            99,
        )],
    )
    .await?;
    let mut unreadable_binding = surface_binding(
        unreadable_surface_binding_id,
        logical_name_id,
        unreadable_resource_id,
        timestamp(1_717_171_700),
    );
    unreadable_binding.canonicality_state = CanonicalityState::Orphaned;
    upsert_test_surface_bindings(&database.pool, &[unreadable_binding]).await?;
    // The phase projection omits the relation because its binding is orphaned.

    upsert_primary_name_current_snapshots(
        &database.pool,
        &[bigname_storage::PrimaryNameCurrentSnapshot {
            row: bigname_storage::PrimaryNameCurrentRow {
                address: address.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: bigname_storage::PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({"source": "v2_lookup_membership_edge_test"}),
            },
            normalized_claim_name: Some(normalized_name.to_owned()),
            claim_name_is_normalized: true,
        }],
    )
    .await?;
    seed_phase_primary_name_snapshot(
        &database,
        address,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some(normalized_name),
        true,
    )
    .await?;
    seed_v2_lookup_base_head(&database).await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "id": "membership-edge",
                "address": address
            }]
        }),
    )
    .await?;

    let records = payload["data"][0]["records"]
        .as_array()
        .expect("reverse lookup records must be an array");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!(normalized_name));
    assert_eq!(records[0]["is_primary"], json!(true));
    assert_eq!(records[0]["relations"], json!(["owner"]));
    assert_eq!(payload["data"][0]["page"]["total_count"], json!(1));
    assert_eq!(
        payload["data"][0]["page"]["total_count"].as_u64(),
        Some(records.len() as u64),
        "page membership and live count must agree"
    );
    assert_eq!(payload["data"][0]["page"]["has_more"], json!(false));

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
    let public_response = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
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
        "/v2/lookup",
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
        "/v2/lookup",
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
        "/v2/lookup",
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
            "ens_v2_sepolia_post_audit",
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
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/lookup")
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
                    .uri("/v2/lookup")
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
                    .uri("/v2/lookup")
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
                    .uri("/v2/lookup")
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
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = 'public-namespace:test-unready'
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
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
                    .uri("/v2/lookup")
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
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = $1
         WHERE chain_id = 'base-mainnet' AND phase_name = 'project'",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;
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
                    .uri("/v2/lookup")
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
        "/v2/lookup",
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
        "/v2/lookup",
        json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_reports_stale_when_project_phase_is_behind_head() -> Result<()> {
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
        "UPDATE chain_phase_state
         SET current_block_number = 78, current_block_hash = '0xlookup-previous'
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;

    let response = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
        json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await
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
async fn v2_lookup_excludes_unsupported_rows_without_leaking_pipeline_reasons() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    for failure_reason in ["raw_log_decoder_failed", "identity_sidecar_missing"] {
        sqlx::query(
            r#"
            UPDATE name_current
            SET support_status = 'unsupported', unsupported_reason = $1
            WHERE lower(raw_name) = 'alice.eth'
            "#,
        )
        .bind(failure_reason)
        .execute(&database.lookup_pool)
        .await?;

        let response = v2_lookup_response_for_database(
            &database,
            "/v2/lookup",
            json!({
                "profile": "detail",
                "inputs": [{
                    "address": address
                }]
            }),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK, "{failure_reason}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["data"][0]["records"][0]["name"], json!("bob.eth"));
        assert!(!payload.to_string().contains(failure_reason));
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_relation_filters_owner_and_registrant_exactly() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (_count_guard, count_calls) =
        crate::v2::support::identity_facade_count_test_hooks::install(&database.pool).await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:holder.eth",
        "holder.eth",
        "holder.eth",
        "namehash:holder.eth",
        Uuid::from_u128(0x5a0301),
        Uuid::from_u128(0x5a0302),
        Uuid::from_u128(0x5a0303),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        44,
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:registrant.eth",
        "registrant.eth",
        "registrant.eth",
        "namehash:registrant.eth",
        Uuid::from_u128(0x5a0311),
        Uuid::from_u128(0x5a0312),
        Uuid::from_u128(0x5a0313),
        address,
        bigname_storage::AddressNameRelation::Registrant,
        45,
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
    assert_eq!(owner["data"][0]["records"][0]["name"], json!("holder.eth"));
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
    assert_eq!(
        registrant["data"][0]["records"][0]["name"],
        json!("registrant.eth")
    );
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
                    .uri("/v2/lookup")
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
    // The interpret gate admits a name surface as active only when its bytes are already the
    // ENSIP-15 normalized form, so a supported fixture row carries the normalized spelling.
    seed_identity_name(
        database,
        "ens:alice.eth",
        "alice.eth",
        "alice.eth",
        "namehash:alice.eth",
        Uuid::from_u128(0x5a0201),
        Uuid::from_u128(0x5a0202),
        Uuid::from_u128(0x5a0203),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        41,
    )
    .await?;
    seed_identity_name(
        database,
        "ens:bob.eth",
        "bob.eth",
        "bob.eth",
        "namehash:bob.eth",
        Uuid::from_u128(0x5a0211),
        Uuid::from_u128(0x5a0212),
        Uuid::from_u128(0x5a0213),
        address,
        bigname_storage::AddressNameRelation::EffectiveController,
        42,
    )
    .await?;
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[bigname_storage::PrimaryNameCurrentSnapshot {
            row: bigname_storage::PrimaryNameCurrentRow {
                address: address.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: bigname_storage::PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({"source": "v2_lookup_test"}),
            },
            normalized_claim_name: Some("alice.eth".to_owned()),
            claim_name_is_normalized: true,
        }],
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
    let publication_positions = json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 10_000,
            "block_hash": "0xlookup-scan-head",
            "timestamp": "2026-04-17T00:00:40Z"
        }
    });

    let owner_match_indexes = owner_match_indexes
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut phase_logical_name_ids = Vec::new();
    let mut phase_names = Vec::new();
    let mut phase_namehashes = Vec::new();
    let mut phase_resource_ids = Vec::new();
    let mut phase_token_lineage_ids = Vec::new();
    let mut phase_surface_binding_ids = Vec::new();
    let mut phase_relations = Vec::new();

    for index in 0..row_count {
        let name = format!("scan{index:03}.eth");
        let resource_id = Uuid::from_u128(0x7100_0000 + index as u128 * 3);
        let token_lineage_id = Uuid::from_u128(0x7100_0001 + index as u128 * 3);
        let surface_binding_id = Uuid::from_u128(0x7100_0002 + index as u128 * 3);
        let relation = if owner_match_indexes.contains(&index) {
            bigname_storage::AddressNameRelation::TokenHolder
        } else {
            bigname_storage::AddressNameRelation::Registrant
        };
        let phase_namehash = bigname_lookup::ens_namehash_hex(&name)?;
        phase_logical_name_ids.push(format!("ens:{phase_namehash}"));
        phase_names.push(name.clone());
        phase_namehashes.push(phase_namehash);
        phase_resource_ids.push(resource_id);
        phase_token_lineage_ids.push(token_lineage_id);
        phase_surface_binding_ids.push(surface_binding_id);
        phase_relations.push(relation.as_str().to_owned());
    }
    seed_phase_lookup_scan_rows(
        database,
        address,
        &phase_logical_name_ids,
        &phase_names,
        &phase_namehashes,
        &phase_resource_ids,
        &phase_token_lineage_ids,
        &phase_surface_binding_ids,
        &phase_relations,
        &publication_positions,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_phase_lookup_scan_rows(
    database: &TestDatabase,
    address: &str,
    logical_name_ids: &[String],
    names: &[String],
    namehashes: &[String],
    resource_ids: &[Uuid],
    token_lineage_ids: &[Uuid],
    surface_binding_ids: &[Uuid],
    relations: &[String],
    publication_positions: &Value,
) -> Result<()> {
    let target_positions = json!({
        "block_number": 10_000,
        "block_hash": "0xlookup-scan-head",
        "target_block_number": 10_000,
        "target_block_hash": "0xlookup-scan-head",
    });
    let projection_provenance = json!({ "chain_id": "ethereum-mainnet" });
    let canonicality_summary = json!({
        "state": "canonical_lineage",
        "target_block_number": 10_000,
        "target_block_hash": "0xlookup-scan-head",
    });
    let mut transaction = database.lookup_pool.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO token_lineages (
            token_lineage_id, chain_id, block_hash, block_number,
            provenance, canonicality_state
        )
        SELECT token_lineage_id, 'ethereum-mainnet', '0xlookup-scan-head', 10000,
               '{}'::jsonb, 'finalized'::bigname_phase.canonicality_state
        FROM UNNEST($1::UUID[]) AS seeded(token_lineage_id)
        ON CONFLICT (token_lineage_id) DO NOTHING
        "#,
    )
    .bind(token_lineage_ids)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id, token_lineage_id, chain_id, block_hash, block_number,
            provenance, canonicality_state
        )
        SELECT resource_id, token_lineage_id, 'ethereum-mainnet',
               '0xlookup-scan-head', 10000, '{}'::jsonb,
               'finalized'::bigname_phase.canonicality_state
        FROM UNNEST($1::UUID[], $2::UUID[])
            AS seeded(resource_id, token_lineage_id)
        ON CONFLICT (resource_id) DO NOTHING
        "#,
    )
    .bind(resource_ids)
    .bind(token_lineage_ids)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
            namehash, labelhashes, normalizer_version, visibility_state,
            normalization_errors, chain_id, block_hash, block_number,
            provenance, canonicality_state
        )
        SELECT logical_name_id, 'ens', raw_name, string_to_array(raw_name, '.'),
               convert_to(raw_name, 'UTF8'), namehash,
               ARRAY(
                   SELECT 'labelhash:' || label
                   FROM UNNEST(string_to_array(raw_name, '.')) AS label
               ),
               $4, 'active', '[]'::jsonb, 'ethereum-mainnet',
               '0xlookup-scan-head', 10000, '{}'::jsonb,
               'finalized'::bigname_phase.canonicality_state
        FROM UNNEST($1::TEXT[], $2::TEXT[], $3::TEXT[])
            AS seeded(logical_name_id, raw_name, namehash)
        ON CONFLICT (logical_name_id) DO NOTHING
        "#,
    )
    .bind(logical_name_ids)
    .bind(names)
    .bind(namehashes)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO surface_bindings (
            surface_binding_id, logical_name_id, resource_id, binding_kind,
            authority_arm, active_from, chain_id, block_hash, block_number, provenance,
            canonicality_state
        )
        SELECT surface_binding_id, logical_name_id, resource_id,
               'declared_registry_path', 'ens_v1', now(), 'ethereum-mainnet',
               '0xlookup-scan-head', 10000, '{}'::jsonb,
               'finalized'::bigname_phase.canonicality_state
        FROM UNNEST($1::UUID[], $2::TEXT[], $3::UUID[])
            AS seeded(surface_binding_id, logical_name_id, resource_id)
        ON CONFLICT (surface_binding_id) DO NOTHING
        "#,
    )
    .bind(surface_binding_ids)
    .bind(logical_name_ids)
    .bind(resource_ids)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id, namespace, raw_name, namehash, surface_binding_id,
            resource_id, token_lineage_id, binding_kind, declared_summary,
            support_status, provenance, chain_positions, canonicality_summary,
            manifest_version
        )
        SELECT logical_name_id, 'ens', raw_name, namehash, surface_binding_id,
               resource_id, token_lineage_id, 'declared_registry_path',
               '{}'::jsonb, 'supported', $7, $8, $9, 1
        FROM UNNEST(
            $1::TEXT[], $2::TEXT[], $3::TEXT[], $4::UUID[], $5::UUID[], $6::UUID[]
        ) AS seeded(
            logical_name_id, raw_name, namehash, surface_binding_id,
            resource_id, token_lineage_id
        )
        ON CONFLICT (logical_name_id) DO NOTHING
        "#,
    )
    .bind(logical_name_ids)
    .bind(names)
    .bind(namehashes)
    .bind(surface_binding_ids)
    .bind(resource_ids)
    .bind(token_lineage_ids)
    .bind(&projection_provenance)
    .bind(publication_positions)
    .bind(&canonicality_summary)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address, logical_name_id, relation, namespace, raw_name, namehash,
            surface_binding_id, resource_id, token_lineage_id, binding_kind,
            support_status, provenance, chain_positions, canonicality_summary,
            manifest_version
        )
        SELECT lower($1), logical_name_id, relation, 'ens', raw_name, namehash,
               surface_binding_id, resource_id, token_lineage_id,
               'declared_registry_path', 'supported', $9, $10, $11, 1
        FROM UNNEST(
            $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::UUID[], $6::UUID[],
            $7::UUID[], $8::TEXT[]
        ) AS seeded(
            logical_name_id, raw_name, namehash, surface_binding_id,
            resource_id, token_lineage_id, relation
        )
        ON CONFLICT (address, logical_name_id, relation) DO NOTHING
        "#,
    )
    .bind(address)
    .bind(logical_name_ids)
    .bind(names)
    .bind(namehashes)
    .bind(surface_binding_ids)
    .bind(resource_ids)
    .bind(token_lineage_ids)
    .bind(relations)
    .bind(&projection_provenance)
    .bind(&target_positions)
    .bind(&canonicality_summary)
    .execute(&mut *transaction)
    .await?;

    transaction.commit().await?;
    Ok(())
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
    let response = v2_lookup_response_for_database(database, "/v2/lookup", body).await?;
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

type ReversePlanWork = std::collections::BTreeMap<String, [f64; 5]>;

#[derive(Clone, Copy)]
struct ReversePlanContext {
    page: bool,
    unrelated: i64,
    relation_rows: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReverseRelationAccess {
    Seed,
    Recheck,
}

impl ReverseRelationAccess {
    fn subtotal(self) -> &'static str {
        match self {
            Self::Seed => "address_seed",
            Self::Recheck => "address_recheck",
        }
    }
}

// Only split unconditional conjunctions. Preserve parentheses around disjunctions,
// quoted literals and function arguments; unfamiliar expressions fail closed.
fn reverse_plan_parts<'a>(expression: &'a str, separator: &str) -> Result<Vec<&'a str>> {
    let expression = expression.trim();
    let bytes = expression.as_bytes();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut disjunction = false;
    let mut parts = Vec::new();
    let mut start = 0;
    let mut outer_end = None;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            if quoted && bytes.get(index + 1) == Some(&b'\'') {
                index += 2;
                continue;
            }
            quoted = !quoted;
        } else if !quoted {
            match bytes[index] {
                b'(' => depth += 1,
                b')' => {
                    depth = depth.checked_sub(1).context("unbalanced plan predicate")?;
                    if depth == 0 && outer_end.is_none() {
                        outer_end = Some(index);
                    }
                }
                _ => {}
            }
            disjunction |= depth == 0 && bytes[index..].starts_with(b" OR ");
            if depth == 0 && bytes[index..].starts_with(separator.as_bytes()) {
                parts.extend(reverse_plan_parts(&expression[start..index], separator)?);
                index += separator.len();
                start = index;
                continue;
            }
        }
        index += 1;
    }
    anyhow::ensure!(!quoted && depth == 0, "unbalanced plan predicate");
    if disjunction {
        return Ok(vec![expression]);
    }
    if start != 0 {
        parts.extend(reverse_plan_parts(&expression[start..], separator)?);
        return Ok(parts);
    }
    if bytes.first() == Some(&b'(') && outer_end == Some(bytes.len() - 1) {
        return reverse_plan_parts(&expression[1..expression.len() - 1], separator);
    }
    Ok(vec![expression])
}

fn reverse_plan_conjuncts(expression: &str) -> Result<Vec<&str>> {
    reverse_plan_parts(expression, " AND ")
}

fn reverse_plan_has(expression: &str, expected: &str) -> bool {
    reverse_plan_conjuncts(expression).is_ok_and(|terms| terms.contains(&expected))
}

fn reverse_plan_seed_filter(node: &Value, page: bool) -> bool {
    let Some(filter) = node["Filter"].as_str() else {
        return false;
    };
    let address = if page {
        "lower(address) = lower(requested.address)"
    } else {
        "address = requested.address"
    };
    let roles = "(requested.roles = 'both'::text) OR ((requested.roles = 'owned'::text) AND (relation = ANY ('{registrant,token_holder}'::text[]))) OR ((requested.roles = 'managed'::text) AND (relation = 'effective_controller'::text))";
    reverse_plan_has(filter, address)
        && reverse_plan_has(filter, "namespace = ANY ('{ens,basenames}'::text[])")
        && reverse_plan_has(filter, roles)
}

fn reverse_plan_relation_bound(
    node: &Value,
    role: ReverseRelationAccess,
    context: ReversePlanContext,
    scan_filter: &str,
    condition: &str,
    batch_address: Option<&str>,
) -> bool {
    let kind = node["Node Type"].as_str().unwrap_or("");
    let index = node["Index Name"].as_str().unwrap_or("");
    if let (ReverseRelationAccess::Seed, Some(address_array)) = (role, batch_address) {
        if kind == "Seq Scan" {
            return context.unrelated == 256 && context.relation_rows == 265;
        }
        let (expected_index, key) = if context.page {
            ("address_names_current_address_idx", "lower(address)")
        } else {
            ("address_names_current_pkey", "address")
        };
        return matches!(kind, "Index Scan" | "Index Only Scan" | "Bitmap Index Scan")
            && index == expected_index
            && reverse_plan_has(condition, &format!("{key} = ANY ({address_array})"));
    }
    if role == ReverseRelationAccess::Seed {
        if kind == "Seq Scan" {
            return context.unrelated == 256
                && context.relation_rows == 265
                && reverse_plan_seed_filter(node, context.page);
        }
        if !matches!(kind, "Index Scan" | "Index Only Scan" | "Bitmap Index Scan") {
            return false;
        }
        let (expected_index, equality) = if context.page {
            (
                "address_names_current_address_idx",
                "lower(address) = lower(requested.address)",
            )
        } else {
            ("address_names_current_pkey", "address = requested.address")
        };
        return index == expected_index && reverse_plan_has(condition, equality);
    }
    if !matches!(kind, "Index Scan" | "Index Only Scan" | "Bitmap Index Scan") {
        return false;
    }
    let identity = reverse_plan_has(condition, "logical_name_id = seed.logical_name_id");
    let relation = reverse_plan_has(condition, "relation = seed.relation");
    let address = "address = seed.address";
    identity
        && relation
        && match index {
            "address_names_current_pkey" => reverse_plan_has(condition, address),
            "address_names_current_name_idx" => reverse_plan_has(scan_filter, address),
            _ => false,
        }
}

// Batch role assignment requires real root producers and a complete, planner-visible
// address set from the verified input Function Scan. Unknown plan syntax fails closed.
fn reverse_plan_batch_address(plan: &Value, context: ReversePlanContext) -> Result<Option<String>> {
    fn nodes<'a>(node: &'a Value, output: &mut Vec<&'a Value>) -> Result<()> {
        output.push(node);
        if let Some(children) = node.get("Plans") {
            for child in children.as_array().context("invalid child plans")? {
                nodes(child, output)?;
            }
        }
        Ok(())
    }
    let mut all = Vec::new();
    nodes(plan, &mut all)?;
    let producers = all
        .iter()
        .filter(|node| {
            node["Subplan Name"]
                .as_str()
                .is_some_and(|s| s.starts_with("CTE "))
        })
        .copied()
        .collect::<Vec<_>>();
    if producers.is_empty() {
        anyhow::ensure!(
            all.iter().all(|n| n.get("CTE Name").is_none()),
            "orphan CTE scan"
        );
        return Ok(None);
    }
    let children = plan["Plans"].as_array().context("missing root producers")?;
    anyhow::ensure!(producers.len() == 2, "unexpected CTE producer count");
    for name in ["requested", "readable_candidates"] {
        let label = format!("CTE {name}");
        anyhow::ensure!(
            producers
                .iter()
                .filter(|n| n["Subplan Name"] == label)
                .count()
                == 1
                && children
                    .iter()
                    .any(|n| n["Subplan Name"] == label && n["Parent Relationship"] == "InitPlan"),
            "missing, duplicate or non-root producer {name}"
        );
    }
    for node in &all {
        if let Some(name) = node.get("CTE Name") {
            anyhow::ensure!(
                node["Node Type"] == "CTE Scan"
                    && matches!(name.as_str(), Some("requested" | "readable_candidates")),
                "unverified CTE consumer"
            );
        }
    }
    let producer = |name: &str| -> &Value {
        producers
            .iter()
            .find(|n| n["Subplan Name"] == format!("CTE {name}"))
            .unwrap()
    };
    let requested = producer("requested");
    anyhow::ensure!(
        requested["Node Type"] == "Function Scan"
            && requested["Alias"] == "request_input"
            && requested["Output"]
                .as_array()
                .is_some_and(|v| v.contains(&json!("request_input.address"))
                    && v.contains(&json!("request_input.roles")))
            && requested.get("Plans").is_none(),
        "unverified input producer"
    );
    let call = requested["Function Call"]
        .as_str()
        .context("missing input function")?;
    let arguments = reverse_plan_parts(call, ", ")?;
    anyhow::ensure!(
        arguments.len() == if context.page { 12 } else { 2 },
        "unexpected input arity"
    );
    anyhow::ensure!(
        arguments
            .iter()
            .all(|s| s.starts_with("unnest(") && s.ends_with(')')),
        "unverified input function"
    );
    let literal = arguments[usize::from(context.page)]
        .strip_prefix("unnest('")
        .and_then(|s| s.strip_suffix("'::text[])"))
        .context("nonliteral requested addresses")?;
    // The fixed plan fixture uses hexadecimal addresses; do not guess at SQL escapes,
    // arbitrary collations, nulls or unknown parameter values when proving the array.
    let addresses = literal
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .context("invalid address array")?;
    anyhow::ensure!(
        !addresses.is_empty()
            && addresses.split(',').all(|s| !s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_hexdigit() || matches!(b, b'x' | b'X'))),
        "unverified address literal"
    );
    let literal = if context.page {
        literal.to_ascii_lowercase()
    } else {
        literal.to_owned()
    };
    let address_array = format!("'{literal}'::text[]");
    let readable = producer("readable_candidates");
    let operations = readable["Plans"]
        .as_array()
        .context("missing readability inputs")?
        .iter()
        .filter(|n| matches!(n["Parent Relationship"].as_str(), Some("Outer" | "Inner")))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        readable["Node Type"] == "Nested Loop"
            && operations.len() == 2
            && operations[0]["Parent Relationship"] == "Outer"
            && operations[1]["Parent Relationship"] == "Inner",
        "readability is not correlated directly to a seed"
    );
    let seed = operations[0];
    anyhow::ensure!(
        seed["Relation Name"] == "address_names_current"
            && seed["Schema"] == "bigname_phase"
            && seed["Alias"] == "seed"
            && seed["Output"].as_array().is_some_and(|v| [
                "seed.address",
                "seed.logical_name_id",
                "seed.relation"
            ]
            .iter()
            .all(|field| v.contains(&json!(field))))
            && matches!(
                seed["Node Type"].as_str(),
                Some("Seq Scan" | "Index Scan" | "Bitmap Heap Scan")
            ),
        "batch seed is not the stored physical identity"
    );
    for producer in [requested, readable, seed] {
        let loops = producer["Actual Loops"]
            .as_f64()
            .context("missing producer loops")?;
        anyhow::ensure!(
            loops.is_finite() && (0.0..=1.0).contains(&loops),
            "repeated batch producer"
        );
    }
    let subplans = seed["Plans"]
        .as_array()
        .context("missing seed input proof")?;
    let matches = subplans
        .iter()
        .filter(|n| n["Parent Relationship"] == "SubPlan")
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matches.len() == 1
            && subplans.len() == 1 + usize::from(seed["Node Type"] == "Bitmap Heap Scan"),
        "unexpected raw seed child access"
    );
    let matcher = matches[0];
    let match_name = matcher["Subplan Name"]
        .as_str()
        .context("missing request SubPlan")?;
    anyhow::ensure!(
        match_name.starts_with("SubPlan ") && match_name[8..].parse::<usize>().is_ok(),
        "invalid match SubPlan"
    );
    let address = if context.page {
        "lower(request_match.address) = lower(seed.address)"
    } else {
        "request_match.address = seed.address"
    };
    let roles = "(request_match.roles = 'both'::text) OR ((request_match.roles = 'owned'::text) AND (seed.relation = ANY ('{registrant,token_holder}'::text[]))) OR ((request_match.roles = 'managed'::text) AND (seed.relation = 'effective_controller'::text))";
    let filter = matcher["Filter"]
        .as_str()
        .context("missing same-request filter")?;
    anyhow::ensure!(
        matcher["Node Type"] == "CTE Scan"
            && matcher["CTE Name"] == "requested"
            && matcher["Alias"] == "request_match"
            && matcher.get("Plans").is_none()
            && reverse_plan_has(filter, address)
            && reverse_plan_has(filter, roles),
        "address and roles do not belong to the same request"
    );
    let filter = seed["Filter"]
        .as_str()
        .context("missing batch seed filter")?;
    anyhow::ensure!(
        reverse_plan_has(filter, "seed.namespace = ANY ('{ens,basenames}'::text[])")
            && reverse_plan_has(filter, match_name),
        "seed input restrictions are conditional or missing"
    );
    let address_key = if context.page {
        "lower(seed.address)"
    } else {
        "seed.address"
    };
    let condition = format!("{address_key} = ANY ({address_array})");
    anyhow::ensure!(
        reverse_plan_has(filter, &condition)
            || reverse_plan_has(seed["Index Cond"].as_str().unwrap_or(""), &condition)
            || seed["Node Type"] == "Bitmap Heap Scan",
        "missing complete batch address restriction"
    );
    Ok(Some(address_array))
}

fn reverse_plan_unqualify(expression: &str, alias: &str) -> String {
    if alias.is_empty() {
        expression.to_owned()
    } else {
        expression.replace(&format!("{alias}."), "")
    }
}

fn reverse_plan_growth(smaller: &ReversePlanWork, larger: &ReversePlanWork) -> Result<()> {
    for (table, values) in larger {
        if table != "chain_lineage" {
            let baseline = smaller.get(table).map_or(0.0, |v| v[0]);
            anyhow::ensure!(
                values[0] <= 2.0 * baseline + 32.0,
                "population-driven {table}: {larger:?}"
            );
        }
    }
    Ok(())
}

fn reverse_plan_scan_work(plan: &Value, context: ReversePlanContext) -> Result<ReversePlanWork> {
    anyhow::ensure!(
        matches!(
            (context.unrelated, context.relation_rows),
            (256, 265) | (2048, 2057)
        ),
        "unverified fixture population"
    );
    fn visit(
        node: &Value,
        bitmap_table: Option<&str>,
        context: ReversePlanContext,
        role: Option<ReverseRelationAccess>,
        inherited_scan: (&str, &str),
        batch_address: Option<&str>,
        totals: &mut ReversePlanWork,
    ) -> Result<bool> {
        let kind = node["Node Type"].as_str().context("missing node type")?;
        let alias = node["Alias"].as_str().unwrap_or(inherited_scan.1);
        let filter =
            reverse_plan_unqualify(node["Filter"].as_str().unwrap_or(inherited_scan.0), alias);
        let condition = reverse_plan_unqualify(node["Index Cond"].as_str().unwrap_or(""), alias);
        let children = node
            .get("Plans")
            .map(|v| v.as_array().context("invalid child plans"))
            .transpose()?
            .into_iter()
            .flatten()
            .map(|child| {
                (
                    child,
                    matches!(
                        child["Parent Relationship"].as_str(),
                        Some("InitPlan" | "SubPlan")
                    ),
                )
            })
            .collect::<Vec<_>>();
        let operations = children
            .iter()
            .filter(|(_, side)| !side)
            .map(|(child, _)| *child)
            .collect::<Vec<_>>();
        let bitmap = matches!(kind, "BitmapAnd" | "BitmapOr" | "Bitmap Index Scan");
        anyhow::ensure!(bitmap == bitmap_table.is_some(), "invalid bitmap context");
        let declared = node
            .get("Relation Name")
            .map(|v| v.as_str().context("invalid relation"))
            .transpose()?;
        anyhow::ensure!(!bitmap || declared.is_none(), "ambiguous bitmap relation");
        let table = declared.or(bitmap_table);
        anyhow::ensure!(
            batch_address.is_none() || declared.is_none() || node["Schema"] == "bigname_phase",
            "unverified physical schema"
        );
        if matches!(
            kind,
            "Seq Scan" | "Index Scan" | "Index Only Scan" | "Bitmap Heap Scan"
        ) {
            anyhow::ensure!(declared.is_some(), "missing scan relation");
        }
        let key = match table {
            Some("address_names_current") => Some("address"),
            Some("name_current" | "name_surfaces") => Some("logical_name_id"),
            Some("resources") => Some("resource_id"),
            Some("surface_bindings") => Some("surface_binding_id"),
            Some("token_lineages") => Some("token_lineage_id"),
            Some("chain_lineage") => Some("block_hash"),
            Some(_) => anyhow::bail!("unexpected physical relation"),
            None => None,
        };
        if role == Some(ReverseRelationAccess::Seed)
            && batch_address.is_none()
            && kind != "Bitmap Heap Scan"
            && !bitmap
        {
            anyhow::ensure!(children.is_empty(), "raw seed contains a child access");
        }
        let cte = node["CTE Name"].as_str();
        anyhow::ensure!(
            cte.is_none() || batch_address.is_some(),
            "unverified CTE scan"
        );
        let input = (node["Subplan Name"] == "CTE requested").then_some("request_input");
        let work_table = table
            .map(str::to_owned)
            .or_else(|| cte.map(|name| format!("cte:{name}")))
            .or_else(|| input.map(str::to_owned));
        let mut bounded = false;
        if let Some(work_table) = work_table {
            anyhow::ensure!(kind.contains("Scan") || bitmap, "malformed monitored node");
            if kind.contains("Scan") {
                let counter = |field: &str, optional: bool| -> Result<f64> {
                    if optional && node.get(field).is_none() {
                        return Ok(0.0);
                    }
                    let value = node[field].as_f64().context(format!("missing {field}"))?;
                    anyhow::ensure!(value.is_finite() && value >= 0.0, "invalid {field}");
                    Ok(value)
                };
                let loops = counter("Actual Loops", false)?;
                anyhow::ensure!(
                    batch_address.is_none()
                        || role != Some(ReverseRelationAccess::Seed)
                        || loops <= 1.0,
                    "repeated batch seed"
                );
                let rows = counter("Actual Rows", false)?;
                let filtered = counter("Rows Removed by Filter", true)?;
                let rechecked = counter("Rows Removed by Index Recheck", true)?;
                let counters = [
                    loops * (rows + filtered + rechecked).max(1.0),
                    loops * rows,
                    loops * filtered,
                    loops * rechecked,
                    loops,
                ];
                for (total, value) in totals
                    .entry(work_table)
                    .or_default()
                    .iter_mut()
                    .zip(counters)
                {
                    *total += value;
                }
                if table == Some("address_names_current") {
                    let role = role.context("unclassified relation access")?;
                    for (total, value) in totals
                        .entry(role.subtotal().to_owned())
                        .or_default()
                        .iter_mut()
                        .zip(counters)
                    {
                        *total += value;
                    }
                    eprintln!("reverse relation role={role:?} counters={counters:?} node={node}");
                    bounded = reverse_plan_relation_bound(
                        node,
                        role,
                        context,
                        &filter,
                        &condition,
                        batch_address,
                    );
                } else if let Some(key) = key {
                    bounded =
                        matches!(kind, "Index Scan" | "Index Only Scan" | "Bitmap Index Scan")
                            && !condition.contains(" OR ")
                            && condition.split(" AND ").any(|term| {
                                term.trim_start_matches('(')
                                    .starts_with(&format!("{key} = "))
                            });
                }
            }
        }
        if kind == "Bitmap Index Scan" {
            anyhow::ensure!(children.is_empty(), "bitmap index has children");
        }
        let inherits = bitmap || kind == "Bitmap Heap Scan";
        // Only the verified global producer may assign batch roles. Historical scalar
        // controls use the same direct outer physical seed / inner recheck relationship.
        let seed_join = role.is_none()
            && kind == "Nested Loop"
            && operations.len() == 2
            && operations[0]["Relation Name"] == "address_names_current"
            && operations[0]["Alias"] == "seed"
            && operations[0]["Parent Relationship"] == "Outer"
            && operations[1]["Parent Relationship"] == "Inner"
            && (batch_address.is_none() || node["Subplan Name"] == "CTE readable_candidates");
        let mut bounds = Vec::new();
        for (child, side) in children {
            let child_role = if side {
                None
            } else if seed_join {
                Some(if child["Parent Relationship"] == "Outer" {
                    ReverseRelationAccess::Seed
                } else {
                    ReverseRelationAccess::Recheck
                })
            } else {
                role
            };
            let bound = visit(
                child,
                if inherits && !side { table } else { None },
                context,
                child_role,
                if inherits && !side {
                    (&filter, alias)
                } else {
                    ("", "")
                },
                batch_address,
                totals,
            )?;
            if !side {
                bounds.push(bound);
            }
        }
        if inherits && kind != "Bitmap Index Scan" {
            anyhow::ensure!(
                !bounds.is_empty()
                    && table.is_some()
                    && (kind != "Bitmap Heap Scan" || bounds.len() == 1),
                "empty or orphan bitmap tree"
            );
            bounded = if kind == "BitmapOr" || table == Some("address_names_current") {
                bounds.iter().all(|v| *v)
            } else {
                bounds.iter().any(|v| *v)
            };
        }
        if key.is_some() && table != Some("chain_lineage") && !bitmap {
            anyhow::ensure!(bounded, "broad or unbounded monitored access: {node}");
        }
        Ok(bounded)
    }
    let roots = plan.as_array().context("invalid plan root")?;
    anyhow::ensure!(roots.len() == 1, "expected one plan root");
    let mut totals = std::collections::BTreeMap::new();
    let batch_address = reverse_plan_batch_address(&roots[0]["Plan"], context)?;
    visit(
        &roots[0]["Plan"],
        None,
        context,
        None,
        ("", ""),
        batch_address.as_deref(),
        &mut totals,
    )?;
    let seed = totals.get("address_seed").map_or(0.0, |v| v[0]);
    let recheck = totals.get("address_recheck").map_or(0.0, |v| v[0]);
    anyhow::ensure!(
        totals.get("address_names_current").map_or(0.0, |v| v[0]) == seed + recheck,
        "relation role accounting mismatch"
    );
    if totals.contains_key("address_names_current") {
        anyhow::ensure!(
            totals.contains_key("address_seed") && totals.contains_key("address_recheck"),
            "missing correlated relation role"
        );
    }
    for (table, values) in &totals {
        anyhow::ensure!(
            matches!(
                table.as_str(),
                "chain_lineage" | "address_seed" | "address_recheck"
            ) || values[0] <= 512.0,
            "excess scan/probe work: {totals:?}"
        );
    }
    Ok(totals)
}

#[test]
fn reverse_plan_validator_rejects_hidden_work() {
    let context = ReversePlanContext {
        page: true,
        unrelated: 256,
        relation_rows: 265,
    };
    let scan = json!({"Node Type":"Index Scan", "Relation Name":"name_current",
        "Index Cond":"(logical_name_id = anc.logical_name_id)", "Actual Rows":1, "Actual Loops":1});
    let plan = json!([{"Plan":{"Node Type":"Nested Loop","Plans":[scan.clone()]}}]);
    assert!(reverse_plan_scan_work(&plan, context).is_ok());
    let broad = json!({"Node Type":"Index Scan", "Relation Name":"name_current",
        "Index Cond":"((logical_name_id > ''::text) AND (namespace = 'ens'::text))",
        "Actual Rows":264, "Actual Loops":1});
    let mut probes = scan.clone();
    probes["Actual Rows"] = json!(0);
    probes["Actual Loops"] = json!(513);
    let mut missing = scan.clone();
    missing.as_object_mut().unwrap().remove("Actual Rows");
    for bad in [broad, probes, missing] {
        let plan = json!([{"Plan":{"Node Type":"Nested Loop","Plans":[scan.clone(), bad]}}]);
        assert!(reverse_plan_scan_work(&plan, context).is_err());
    }
}

fn reverse_plan_relation_example(page: bool) -> Value {
    let address = if page {
        "lower(address) = lower(requested.address)"
    } else {
        "address = requested.address"
    };
    let roles = "(requested.roles = 'both'::text) OR ((requested.roles = 'owned'::text) AND (relation = ANY ('{registrant,token_holder}'::text[]))) OR ((requested.roles = 'managed'::text) AND (relation = 'effective_controller'::text))";
    let seed = json!({
        "Node Type": "Seq Scan", "Relation Name": "address_names_current",
        "Alias": "seed", "Parent Relationship": "Outer",
        "Filter": format!("(({address}) AND (namespace = ANY ('{{ens,basenames}}'::text[])) AND ({roles}))"),
        "Actual Rows": 5, "Rows Removed by Filter": 260, "Actual Loops": 1
    });
    let recheck = json!({
        "Node Type": "Index Scan", "Relation Name": "address_names_current",
        "Alias": "anc", "Parent Relationship": "Inner",
        "Index Name": "address_names_current_name_idx",
        "Index Cond": "((logical_name_id = seed.logical_name_id) AND (relation = seed.relation))",
        "Filter": "((address = seed.address) AND (support_status = 'supported'::text))",
        "Actual Rows": 1, "Actual Loops": 5
    });
    json!([{"Plan": {"Node Type": "Nested Loop", "Plans": [seed, recheck]}}])
}

fn reverse_plan_batch_example(page: bool, addresses: &str) -> Value {
    let address = if page {
        "lower(seed.address)"
    } else {
        "seed.address"
    };
    let request_address = if page {
        "lower(request_match.address)"
    } else {
        "request_match.address"
    };
    let arguments = if page {
        vec![
            "'{0}'::integer[]",
            "'{A}'::text[]",
            "'{60}'::text[]",
            "'{both}'::text[]",
            "'{\"{}\"}'::jsonb[]",
            "'{2}'::bigint[]",
            "'{f}'::boolean[]",
            "'{NULL}'::boolean[]",
            "'{NULL}'::smallint[]",
            "'{NULL}'::text[]",
            "'{NULL}'::text[]",
            "'{NULL}'::text[]",
        ]
    } else {
        vec!["'{a}'::text[]", "'{both}'::text[]"]
    };
    let calls = arguments
        .iter()
        .map(|value| format!("unnest({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let calls = calls.replace(if page { "{A}" } else { "{a}" }, addresses);
    let address_array = if page {
        addresses.to_ascii_lowercase()
    } else {
        addresses.to_owned()
    };
    let roles = "(request_match.roles = 'both'::text) OR ((request_match.roles = 'owned'::text) AND (seed.relation = ANY ('{registrant,token_holder}'::text[]))) OR ((request_match.roles = 'managed'::text) AND (seed.relation = 'effective_controller'::text))";
    let mut recheck = reverse_plan_relation_example(page)[0]["Plan"]["Plans"][1].clone();
    recheck["Alias"] = json!("relation_check");
    recheck["Schema"] = json!("bigname_phase");
    recheck["Index Cond"] = json!(
        "((relation_check.logical_name_id = seed.logical_name_id) AND (relation_check.relation = seed.relation))"
    );
    recheck["Filter"] = json!("relation_check.address = seed.address");
    json!([{"Plan":{"Node Type":"Nested Loop", "Plans":[
        {"Node Type":"Function Scan", "Alias":"request_input", "Parent Relationship":"InitPlan",
         "Subplan Name":"CTE requested", "Function Call":calls,
         "Output":["request_input.address", "request_input.roles"], "Actual Rows":addresses.split(',').count(), "Actual Loops":1},
        {"Node Type":"Nested Loop", "Parent Relationship":"InitPlan", "Subplan Name":"CTE readable_candidates",
         "Actual Loops":1, "Plans":[
            {"Node Type":"Seq Scan", "Alias":"seed", "Relation Name":"address_names_current",
             "Schema":"bigname_phase", "Output":["seed.address", "seed.logical_name_id", "seed.relation"],
             "Parent Relationship":"Outer", "Filter":format!("(({address} = ANY ('{address_array}'::text[])) AND (seed.namespace = ANY ('{{ens,basenames}}'::text[])) AND (SubPlan 2))"),
             "Actual Rows":5, "Rows Removed by Filter":260, "Actual Loops":1, "Plans":[
                {"Node Type":"CTE Scan", "CTE Name":"requested", "Alias":"request_match", "Parent Relationship":"SubPlan",
                 "Subplan Name":"SubPlan 2", "Filter":format!("(({request_address} = {address}) AND ({roles}))"),
                 "Actual Rows":1, "Actual Loops":5}]}, recheck]},
        {"Node Type":"CTE Scan", "CTE Name":"readable_candidates", "Alias":"candidate",
         "Actual Rows":5, "Actual Loops":5}
    ]}}])
}

#[test]
fn reverse_plan_validator_checks_batch_producers() {
    for page in [true, false] {
        let context = ReversePlanContext {
            page,
            unrelated: 256,
            relation_rows: 265,
        };
        let plan = reverse_plan_batch_example(page, if page { "{A}" } else { "{a}" });
        let work = reverse_plan_scan_work(&plan, context).unwrap();
        assert_eq!(
            (work["address_seed"][0], work["address_recheck"][0]),
            (265.0, 5.0)
        );
        assert_eq!(
            (
                work["request_input"][0],
                work["cte:requested"][0],
                work["cte:readable_candidates"][0]
            ),
            (1.0, 5.0, 25.0)
        );
        let large = ReversePlanContext {
            unrelated: 2048,
            relation_rows: 2057,
            ..context
        };
        assert!(reverse_plan_scan_work(&plan, large).is_err());
        let seed_path = "/0/Plan/Plans/1/Plans/0";
        let mut indexed = plan.clone();
        let seed = indexed.pointer_mut(seed_path).unwrap();
        seed["Node Type"] = json!("Index Scan");
        seed["Index Name"] = json!(if page {
            "address_names_current_address_idx"
        } else {
            "address_names_current_pkey"
        });
        seed["Index Cond"] = json!(if page {
            "lower(seed.address) = ANY ('{a}'::text[])"
        } else {
            "seed.address = ANY ('{a}'::text[])"
        });
        seed["Rows Removed by Filter"] = json!(0);
        assert!(reverse_plan_scan_work(&indexed, large).is_ok());
        for (pointer, replacement) in [
            ("/0/Plan/Plans/0/Actual Loops", json!(2)),
            ("/0/Plan/Plans/1/Actual Loops", json!(2)),
            ("/0/Plan/Plans/1/Plans/0/Actual Loops", json!(2)),
            ("/0/Plan/Plans/2/Actual Loops", json!(513)),
            ("/0/Plan/Plans/1/Plans/0/Plans/0/Actual Loops", json!(513)),
            ("/0/Plan/Plans/0/Alias", json!("requested")),
            ("/0/Plan/Plans/1/Plans/0/Plans/0/CTE Name", json!("other")),
            (
                "/0/Plan/Plans/1/Plans/1/Filter",
                json!("relation_check.address = other.address"),
            ),
            ("/0/Plan/Plans/1/Parent Relationship", json!("SubPlan")),
            ("/0/Plan/Plans/2/CTE Name", json!("readable_relations")),
            ("/0/Plan/Plans/1/Plans/0/Schema", json!("other")),
            ("/0/Plan/Plans/1/Plans/1/Schema", json!("other")),
            (
                "/0/Plan/Plans/1/Plans/0/Index Cond",
                json!("seed.address = ANY ($99)"),
            ),
        ] {
            let mut bad = indexed.clone();
            *bad.pointer_mut(pointer).unwrap() = replacement;
            assert!(
                reverse_plan_scan_work(&bad, context).is_err(),
                "accepted {pointer}"
            );
        }
        for pointer in [
            "/0/Plan/Plans/1/Plans/0/Filter",
            "/0/Plan/Plans/1/Plans/0/Plans/0/Filter",
        ] {
            let original = plan.pointer(pointer).unwrap().as_str().unwrap();
            for replacement in [
                format!("({original}) OR true"),
                original.replace("seed.", "other."),
                original.replace("request_match.roles", "other.roles"),
            ] {
                if replacement == original {
                    continue;
                }
                let mut bad = plan.clone();
                *bad.pointer_mut(pointer).unwrap() = json!(replacement);
                assert!(reverse_plan_scan_work(&bad, context).is_err());
            }
        }
        let multiple = reverse_plan_batch_example(page, if page { "{A,B}" } else { "{a,b}" });
        assert!(reverse_plan_scan_work(&multiple, context).is_ok());
        let mut parameter = indexed.clone();
        let calls = parameter[0]["Plan"]["Plans"][0]["Function Call"]
            .as_str()
            .unwrap();
        parameter[0]["Plan"]["Plans"][0]["Function Call"] = json!(calls.replace(
            if page {
                "unnest('{A}'::text[])"
            } else {
                "unnest('{a}'::text[])"
            },
            "unnest($99::text[])"
        ));
        assert!(reverse_plan_scan_work(&parameter, context).is_err());
        for literal in ["{b}", "{a,b}", "{NULL}", "{a\\b}"] {
            let mut bad = indexed.clone();
            let calls = bad[0]["Plan"]["Plans"][0]["Function Call"]
                .as_str()
                .unwrap()
                .replace(if page { "{A}" } else { "{a}" }, literal);
            bad[0]["Plan"]["Plans"][0]["Function Call"] = json!(calls);
            assert!(reverse_plan_scan_work(&bad, context).is_err());
        }
        let mut duplicate = plan.clone();
        duplicate[0]["Plan"]["Plans"]
            .as_array_mut()
            .unwrap()
            .push(plan[0]["Plan"]["Plans"][1].clone());
        assert!(reverse_plan_scan_work(&duplicate, context).is_err());
        for relationship in ["InitPlan", "SubPlan"] {
            let extra = json!({"Node Type":"Index Scan", "Schema":"bigname_phase", "Relation Name":"name_current",
                "Alias":"nc", "Index Cond":"nc.logical_name_id = seed.logical_name_id",
                "Parent Relationship":relationship, "Actual Rows":1, "Actual Loops":1});
            let mut nested = indexed.clone();
            let siblings = nested[0]["Plan"]["Plans"][1]["Plans"]
                .as_array_mut()
                .unwrap();
            siblings.push(extra);
            assert_eq!(
                reverse_plan_scan_work(&nested, context).unwrap()["name_current"][0],
                1.0
            );
            nested[0]["Plan"]["Plans"][1]["Plans"]
                .as_array_mut()
                .unwrap()
                .last_mut()
                .unwrap()["Actual Loops"] = json!(513);
            assert!(reverse_plan_scan_work(&nested, context).is_err());
        }
        let mut bitmap = indexed.clone();
        let seed = bitmap.pointer_mut(seed_path).unwrap();
        seed["Node Type"] = json!("Bitmap Heap Scan");
        let leaf = json!({"Node Type":"Bitmap Index Scan", "Index Name":seed["Index Name"], "Index Cond":seed["Index Cond"], "Actual Rows":5, "Actual Loops":1});
        seed.as_object_mut().unwrap().remove("Index Name");
        seed.as_object_mut().unwrap().remove("Index Cond");
        seed["Plans"]
            .as_array_mut()
            .unwrap()
            .push(json!({"Node Type":"BitmapAnd", "Plans":[leaf.clone(), leaf]}));
        assert!(reverse_plan_scan_work(&bitmap, large).is_ok());
        for relationship in ["InitPlan", "SubPlan"] {
            bitmap[0]["Plan"]["Plans"][1]["Plans"].as_array_mut().unwrap().push(json!({
                "Node Type":"Result", "Parent Relationship":relationship, "Actual Rows":1, "Actual Loops":1}));
        }
        assert!(reverse_plan_scan_work(&bitmap, large).is_ok());
        bitmap[0]["Plan"]["Plans"][1]["Plans"][0]["Plans"][1]["Plans"][1]["Index Cond"] =
            json!("logical_name_id = seed.logical_name_id");
        assert!(reverse_plan_scan_work(&bitmap, large).is_err());
        let mut missing = plan.clone();
        missing[0]["Plan"]["Plans"][1]["Plans"][0]["Plans"][0]
            .as_object_mut()
            .unwrap()
            .remove("Actual Loops");
        assert!(reverse_plan_scan_work(&missing, context).is_err());
        let mut growth = work.clone();
        growth.get_mut("cte:readable_candidates").unwrap()[0] = 83.0;
        assert!(reverse_plan_growth(&work, &growth).is_err());
    }
}

#[test]
fn reverse_plan_validator_distinguishes_relation_access() {
    for page in [true, false] {
        let context = ReversePlanContext {
            page,
            unrelated: 256,
            relation_rows: 265,
        };
        let plan = reverse_plan_relation_example(page);
        let work = reverse_plan_scan_work(&plan, context).unwrap();
        assert_eq!(work["address_names_current"][0], 270.0);
        assert_eq!(work["address_seed"][0], 265.0);
        assert_eq!(work["address_recheck"][0], 5.0);
        assert!(
            reverse_plan_scan_work(
                &plan,
                ReversePlanContext {
                    unrelated: 2048,
                    relation_rows: 2057,
                    ..context
                }
            )
            .is_err()
        );
        assert!(
            reverse_plan_scan_work(
                &plan,
                ReversePlanContext {
                    relation_rows: 266,
                    ..context
                }
            )
            .is_err()
        );
        let original = plan[0]["Plan"]["Plans"][0]["Filter"].as_str().unwrap();
        for filter in [
            original.replace("requested.address", "other.address"),
            original.replace("namespace = ANY", "unrelated_namespace = ANY"),
            original.replace("'owned'::text", "'managed'::text"),
            original.replace("'effective_controller'::text", "'token_holder'::text"),
            format!("({original}) OR true"),
            original.replace("AND (namespace", "OR (namespace"),
        ] {
            let mut bad = plan.clone();
            bad[0]["Plan"]["Plans"][0]["Filter"] = json!(filter);
            assert!(reverse_plan_scan_work(&bad, context).is_err());
        }
        let mut alias_only = plan.clone();
        alias_only[0]["Plan"]["Node Type"] = json!("Hash Join");
        assert!(reverse_plan_scan_work(&alias_only, context).is_err());
        let mut inner_scan = plan.clone();
        inner_scan[0]["Plan"]["Plans"][1] = plan[0]["Plan"]["Plans"][0].clone();
        inner_scan[0]["Plan"]["Plans"][1]["Parent Relationship"] = json!("Inner");
        assert!(reverse_plan_scan_work(&inner_scan, context).is_err());
        for condition in [
            "(logical_name_id = seed.logical_name_id)",
            "(relation = seed.relation)",
            "((logical_name_id > ''::text) AND (relation = seed.relation))",
            "((logical_name_id = other.logical_name_id) AND (relation = seed.relation))",
            "logical_name_id = seed.logical_name_id AND relation = seed.relation OR true",
        ] {
            let mut bad = plan.clone();
            bad[0]["Plan"]["Plans"][1]["Index Cond"] = json!(condition);
            assert!(reverse_plan_scan_work(&bad, context).is_err());
        }
        for filter in [
            "support_status = 'supported'::text",
            "lower(address) = lower(seed.address)",
            "address = requested.address",
            "address = other.address",
            "address = seed.address OR true",
            "address = seed.address AND support_status = 'supported'::text OR true",
        ] {
            let mut bad = plan.clone();
            bad[0]["Plan"]["Plans"][1]["Filter"] = json!(filter);
            assert!(reverse_plan_scan_work(&bad, context).is_err());
        }
        let mut complete_key = plan.clone();
        complete_key[0]["Plan"]["Plans"][1]["Index Name"] = json!("address_names_current_pkey");
        complete_key[0]["Plan"]["Plans"][1]["Index Cond"] = json!(
            "((address = seed.address) AND (logical_name_id = seed.logical_name_id) AND (relation = seed.relation))"
        );
        complete_key[0]["Plan"]["Plans"][1]["Filter"] = json!("support_status = 'supported'::text");
        assert!(reverse_plan_scan_work(&complete_key, context).is_ok());
        let mut combined = plan.clone();
        combined[0]["Plan"]["Plans"][1]["Actual Loops"] = json!(248);
        assert!(reverse_plan_scan_work(&combined, context).is_err());
        let mut filtered = plan.clone();
        filtered[0]["Plan"]["Plans"][1]["Rows Removed by Filter"] = json!(100);
        assert!(reverse_plan_scan_work(&filtered, context).is_err());

        let mut indexed = plan.clone();
        let seed = &mut indexed[0]["Plan"]["Plans"][0];
        seed["Node Type"] = json!("Index Scan");
        seed["Rows Removed by Filter"] = json!(0);
        seed["Index Name"] = json!(if page {
            "address_names_current_address_idx"
        } else {
            "address_names_current_pkey"
        });
        seed["Index Cond"] = json!(if page {
            "lower(address) = lower(requested.address)"
        } else {
            "address = requested.address"
        });
        let large = ReversePlanContext {
            unrelated: 2048,
            relation_rows: 2057,
            ..context
        };
        assert!(reverse_plan_scan_work(&indexed, large).is_ok());
        let mut wrong_index = indexed.clone();
        wrong_index[0]["Plan"]["Plans"][0]["Index Name"] = json!("address_names_current_name_idx");
        assert!(reverse_plan_scan_work(&wrong_index, large).is_err());
        let mut bitmap = indexed.clone();
        let mut leaf = bitmap[0]["Plan"]["Plans"][0].clone();
        leaf["Node Type"] = json!("Bitmap Index Scan");
        for field in ["Relation Name", "Alias", "Filter", "Parent Relationship"] {
            leaf.as_object_mut().unwrap().remove(field);
        }
        let heap = &mut bitmap[0]["Plan"]["Plans"][0];
        heap["Node Type"] = json!("Bitmap Heap Scan");
        heap.as_object_mut().unwrap().remove("Index Name");
        heap.as_object_mut().unwrap().remove("Index Cond");
        heap["Plans"] = json!([{"Node Type": "BitmapOr", "Plans": [leaf.clone(), leaf.clone()]}]);
        assert!(reverse_plan_scan_work(&bitmap, large).is_ok());
        let mut valid_and = bitmap.clone();
        valid_and[0]["Plan"]["Plans"][0]["Plans"][0]["Node Type"] = json!("BitmapAnd");
        assert!(reverse_plan_scan_work(&valid_and, large).is_ok());
        let mut hidden = bitmap.clone();
        hidden[0]["Plan"]["Plans"][0]["Plans"][0]["Plans"][1]["Index Cond"] =
            json!("logical_name_id > ''::text");
        assert!(reverse_plan_scan_work(&hidden, large).is_err());
        hidden[0]["Plan"]["Plans"][0]["Plans"][0]["Node Type"] = json!("BitmapAnd");
        assert!(reverse_plan_scan_work(&hidden, large).is_err());
        let mut repeated = bitmap.clone();
        repeated[0]["Plan"]["Plans"][0]["Plans"][0]["Plans"][1]["Actual Rows"] = json!(0);
        repeated[0]["Plan"]["Plans"][0]["Plans"][0]["Plans"][1]["Actual Loops"] = json!(513);
        assert!(reverse_plan_scan_work(&repeated, large).is_err());
        let orphan = json!([{"Plan": leaf}]);
        assert!(reverse_plan_scan_work(&orphan, large).is_err());
    }
}

#[test]
fn reverse_plan_validator_rejects_masked_relation_growth() {
    let small = ReversePlanWork::from([
        (
            "address_names_current".to_owned(),
            [270.0, 0.0, 0.0, 0.0, 0.0],
        ),
        ("address_seed".to_owned(), [265.0, 0.0, 0.0, 0.0, 0.0]),
        ("address_recheck".to_owned(), [5.0, 0.0, 0.0, 0.0, 0.0]),
    ]);
    let mut large = small.clone();
    large.get_mut("address_names_current").unwrap()[0] = 400.0;
    large.get_mut("address_seed").unwrap()[0] = 10.0;
    large.get_mut("address_recheck").unwrap()[0] = 390.0;
    assert!(reverse_plan_growth(&small, &large).is_err());
    large.get_mut("address_names_current").unwrap()[0] = 52.0;
    large.get_mut("address_recheck").unwrap()[0] = 42.0;
    assert!(reverse_plan_growth(&small, &large).is_ok());
    large.get_mut("address_names_current").unwrap()[0] = 53.0;
    large.get_mut("address_recheck").unwrap()[0] = 43.0;
    assert!(reverse_plan_growth(&small, &large).is_err());
}

#[tokio::test]
async fn reverse_readability_rechecks_the_same_stored_address() -> Result<()> {
    use crate::v2::support::load_reverse_identity_records_live as load_reverse;
    use bigname_storage::{AddressNameRelation, ReverseIdentityRoles, ReverseIdentityStorageInput};
    let database = TestDatabase::new_migrated().await?;
    let addresses = [
        "0x0000000000000000000000000000000000000abc",
        "0x0000000000000000000000000000000000000def",
    ];
    for address in addresses {
        seed_reverse_plan_name(
            &database,
            "shared-recheck.eth",
            address,
            AddressNameRelation::TokenHolder,
            0x842200,
        )
        .await?;
    }
    let requests = addresses.map(|address| ReverseIdentityStorageInput {
        address: address.to_owned(),
        coin_type: "60".to_owned(),
        roles: ReverseIdentityRoles::Owned,
        page_size: 2,
        cursor: None,
    });
    let namespaces = vec!["ens".to_owned()];
    let before = load_reverse(&database.lookup_pool, &requests, &namespaces).await?;
    assert_eq!(before.len(), 2);
    for group in before {
        assert_eq!(group.total_count, Some(1));
        assert_eq!(group.entries.len(), 1);
        assert_eq!(
            group.entries[0].name_record.row.normalized_name,
            "shared-recheck.eth"
        );
        assert!(group.entries[0].primary_name.is_none());
    }
    let changed = sqlx::query(
        "UPDATE bigname_phase.address_names_current
         SET support_status = 'unsupported', unsupported_reason = 'relation_control'
         WHERE address = $1 AND logical_name_id = $2 AND relation = 'token_holder'",
    )
    .bind(addresses[0])
    .bind(format!(
        "ens:{}",
        bigname_lookup::ens_namehash_hex("shared-recheck.eth")?
    ))
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(changed.rows_affected(), 1);
    let after = load_reverse(&database.lookup_pool, &requests, &namespaces).await?;
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].input, requests[0]);
    assert_eq!(after[0].total_count, Some(0));
    assert!(after[0].entries.is_empty());
    assert!(!after[0].has_more);
    assert_eq!(after[1].input, requests[1]);
    assert_eq!(after[1].total_count, Some(1));
    assert_eq!(after[1].entries.len(), 1);
    assert_eq!(
        after[1].entries[0].name_record.row.normalized_name,
        "shared-recheck.eth"
    );
    assert_eq!(
        after[1].entries[0].relation_facets,
        vec![AddressNameRelation::TokenHolder]
    );
    assert!(!after[1].has_more);
    database.cleanup().await
}

async fn seed_reverse_plan_name(
    database: &TestDatabase,
    name: &str,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    id: u128,
) -> Result<()> {
    let namespace = if name.ends_with(".base.eth") {
        "basenames"
    } else {
        "ens"
    };
    let hash = bigname_lookup::ens_namehash_hex(name)?;
    seed_identity_name(
        database,
        &format!("{namespace}:{name}"),
        name,
        name,
        &hash,
        Uuid::from_u128(id),
        Uuid::from_u128(id + 1),
        Uuid::from_u128(id + 2),
        address,
        relation,
        42,
    )
    .await
}

async fn reverse_plan_stable_state(database: &TestDatabase, address: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
            'heads', (SELECT jsonb_agg(to_jsonb(h) - 'updated_at' ORDER BY chain_id) FROM chain_heads h),
            'lineage', (SELECT count(*) FROM chain_lineage),
            'primary', (SELECT jsonb_agg(to_jsonb(p) ORDER BY namespace, coin_type)
                        FROM primary_names_current p WHERE address = $1))"
    ).bind(address).fetch_one(&database.lookup_pool).await?)
}

async fn reverse_plan_pages(
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
        let groups = load_reverse(&database.lookup_pool, &[request.clone()], namespaces).await?;
        assert_eq!(groups.len(), 1);
        let group = &groups[0];
        assert_eq!(group.total_count, Some(expected.len() as u64));
        assert!(group.entries.len() <= input.page_size as usize);
        for entry in &group.entries {
            let row = &entry.name_record.row;
            assert!(namespaces.contains(&row.namespace));
            let facets = match row.normalized_name.as_str() {
                "amber.eth" | "dune.base.eth" => vec![Relation::Registrant],
                "birch.eth" => vec![Relation::TokenHolder, Relation::EffectiveController],
                "bob.eth" | "cedar.eth" | "elm.base.eth" => {
                    vec![Relation::EffectiveController]
                }
                _ => vec![Relation::TokenHolder],
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
async fn reverse_readability_plans_follow_address_candidates() -> Result<()> {
    use crate::v2::support::{
        explain_reverse_identity_count as explain_count,
        explain_reverse_identity_page as explain_page,
        load_reverse_identity_records_live as load_reverse,
    };
    use bigname_storage::{
        AddressNameRelation as Relation, ReverseIdentityRoles as Roles, ReverseIdentityStorageInput,
    };
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
    for (name, relation, id) in [
        ("amber.eth", Relation::Registrant, 0x842100),
        ("birch.eth", Relation::TokenHolder, 0x842110),
        ("birch.eth", Relation::EffectiveController, 0x842110),
        ("cedar.eth", Relation::EffectiveController, 0x842120),
        (long_name.as_str(), Relation::TokenHolder, 0x842130),
        ("dune.base.eth", Relation::Registrant, 0x842140),
        ("elm.base.eth", Relation::EffectiveController, 0x842150),
    ] {
        seed_reverse_plan_name(&database, name, address, relation, id).await?;
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
    let empty = load_reverse(&database.lookup_pool, &[], &namespaces).await?;
    assert!(empty.is_empty());
    assert!(
        explain_page(&database.lookup_pool, &[], &namespaces)
            .await?
            .is_none()
    );
    let missing = ReverseIdentityStorageInput {
        address: absent.to_owned(),
        ..inputs[2].clone()
    };
    let stable = reverse_plan_stable_state(&database, address).await?;
    let indexes: Vec<(String, bool, String)> = sqlx::query_as(
        "SELECT c.relname::text, i.indisvalid AND i.indisready, pg_get_indexdef(i.indexrelid)
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
         WHERE i.indrelid = 'bigname_phase.address_names_current'::regclass",
    )
    .fetch_all(&database.lookup_pool)
    .await?;
    for (name, keys) in [
        (
            "address_names_current_address_idx",
            "(lower(address), relation, namespace, namehash, logical_name_id)",
        ),
        (
            "address_names_current_pkey",
            "(address, logical_name_id, relation)",
        ),
        (
            "address_names_current_name_idx",
            "(logical_name_id, relation, lower(address))",
        ),
    ] {
        assert!(
            indexes
                .iter()
                .any(|(index, valid, definition)| index == name
                    && *valid
                    && definition.ends_with(&format!("USING btree {keys}")))
        );
    }
    eprintln!("reverse verified relation indexes={indexes:?}");
    let settings_query = "SELECT jsonb_build_object('version', version(), 'settings',
         (SELECT jsonb_object_agg(name, setting) FROM pg_settings
          WHERE name IN ('random_page_cost', 'seq_page_cost', 'cpu_tuple_cost',
                        'cpu_index_tuple_cost', 'cpu_operator_cost', 'enable_seqscan',
                        'enable_indexscan', 'enable_bitmapscan', 'default_statistics_target')))";
    let settings: Value = sqlx::query_scalar(settings_query)
        .fetch_one(&database.lookup_pool)
        .await?;
    eprintln!("reverse database and planner settings={settings}");
    let mut previous = Vec::<ReversePlanWork>::new();
    let mut plans = 0;
    for (phase, range) in [(0, 0..256), (1, 256..2048)] {
        for index in range {
            seed_reverse_plan_name(
                &database,
                &format!("unrelated{index:04}.eth"),
                other,
                Relation::TokenHolder,
                0x900000 + index as u128 * 4,
            )
            .await?;
        }
        assert_eq!(reverse_plan_stable_state(&database, address).await?, stable);
        let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT count(DISTINCT logical_name_id) FILTER (WHERE address = $1),
                    count(*) FILTER (WHERE address = $1),
                    count(DISTINCT logical_name_id) FILTER (WHERE address = $2),
                    count(*) FILTER (WHERE address = $2), count(*) FROM address_names_current",
        )
        .bind(address)
        .bind(other)
        .fetch_one(&database.lookup_pool)
        .await?;
        let population = if phase == 0 { 256 } else { 2048 };
        assert_eq!(counts, (8, 9, population, population, population + 9));
        for table in [
            "address_names_current",
            "name_current",
            "name_surfaces",
            "resources",
            "surface_bindings",
            "token_lineages",
            "chain_lineage",
            "primary_names_current",
        ] {
            sqlx::query(&format!("ANALYZE bigname_phase.{table}"))
                .execute(&database.lookup_pool)
                .await?;
        }
        let mut cases = Vec::new();
        for (index, input) in inputs.iter().enumerate() {
            let cursor =
                reverse_plan_pages(&database, input, &namespaces, &expected[index]).await?;
            let ens = expected[index]
                .iter()
                .copied()
                .filter(|n| !n.ends_with(".base.eth"))
                .collect::<Vec<_>>();
            assert_eq!(ens.len(), [4, 3, 6][index]);
            reverse_plan_pages(&database, input, &["ens".to_owned()], &ens).await?;
            cases.push(vec![input.clone()]);
            cases.push(vec![ReverseIdentityStorageInput {
                cursor: Some(cursor),
                ..input.clone()
            }]);
        }
        let mut batch = [0, 3, 4, 5].map(|case| cases[case][0].clone()).to_vec();
        batch.push(missing.clone());
        let groups = load_reverse(&database.lookup_pool, &batch, &namespaces).await?;
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
        cases.push(batch.clone());
        for (case, requests) in cases.iter().chain(std::iter::once(&batch)).enumerate() {
            let page = case != 7;
            let plan = if page {
                explain_page(&database.lookup_pool, requests, &namespaces)
                    .await?
                    .context("missing nonempty plan")?
            } else {
                explain_count(&database.lookup_pool, requests, &namespaces).await?
            };
            plans += 1;
            eprintln!("reverse population={population} case={case} page={page} plan={plan}");
            let work = reverse_plan_scan_work(
                &plan,
                ReversePlanContext {
                    page,
                    unrelated: population,
                    relation_rows: counts.4,
                },
            )?;
            anyhow::ensure!(
                work.contains_key("address_names_current") && work.contains_key("name_current"),
                "missing monitored access"
            );
            eprintln!("reverse population={population} case={case} page={page} work={work:?}");
            if phase == 0 {
                previous.push(work);
            } else {
                reverse_plan_growth(&previous[case], &work)?;
            }
        }
    }
    assert_eq!(plans, 16);
    let final_settings: Value = sqlx::query_scalar(settings_query)
        .fetch_one(&database.lookup_pool)
        .await?;
    assert_eq!(
        final_settings, settings,
        "planner settings changed during fixture"
    );
    database.cleanup().await
}
