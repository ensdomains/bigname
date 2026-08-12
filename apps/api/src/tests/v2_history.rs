#[tokio::test]
async fn v2_get_history_returns_lean_product_rows_newest_first() -> Result<()> {
    let (database, payload) = v2_history_payload("/v2/names/History.eth/history?page_size=20").await?;

    assert_eq!(payload["page"]["page_size"], json!(20));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["meta"], json!({}));

    let data = payload["data"]
        .as_array()
        .expect("history data must be an array");
    assert_eq!(
        history_types(data),
        vec![
            "renewal",
            "expiry",
            "release",
            "permission",
            "record",
            "authority",
            "resolver",
            "transfer",
            "registration",
            "authority",
        ]
    );
    assert_eq!(
        data.iter()
            .map(|row| row["block_number"].as_i64().expect("block number"))
            .collect::<Vec<_>>(),
        vec![110, 109, 108, 107, 106, 105, 104, 103, 102, 101]
    );
    assert_eq!(data[0]["name"], json!("history.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(data[0]["timestamp"], json!("2023-11-14T22:15:10Z"));
    assert_eq!(data[0]["transaction_hash"], json!("0xtx110"));
    assert_eq!(data[0]["log_index"], json!(0));
    assert_eq!(
        data[0]["registration_id"],
        json!(Uuid::from_u128(0x7100).to_string())
    );
    assert!(
        data.iter()
            .any(|row| row["type"] == json!("record") && row.get("registration_id").is_none()),
        "surface-only rows must omit registration_id"
    );
    assert!(
        data.iter().any(|row| {
            row["block_number"] == json!(105)
                && row["transaction_hash"] == json!("0xtx105")
                && row["type"] == json!("authority")
                && row.get("registration_id").is_none()
        }),
        "AuthorityEpochChanged must surface as an authority history row"
    );
    for row in data {
        assert!(row.get("data").is_none());
        assert!(row.get("before").is_none());
        assert!(row.get("after").is_none());
        assert!(row.get("event_kind").is_none());
        assert!(row.get("normalized_event_id").is_none());
        assert!(row.get("resource_id").is_none());
    }
    assert_no_banned_v1_spellings(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_filters_non_product_rows_and_advances_cursor() -> Result<()> {
    let (database, first_page) =
        v2_history_payload("/v2/names/History.eth/history?page_size=1").await?;

    assert_eq!(first_page["data"], json!([]));
    assert_eq!(first_page["page"]["has_more"], json!(true));
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("filtered first page must still expose next cursor");

    let second_page = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/History.eth/history?page_size=1&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(history_types(second_page["data"].as_array().expect("data")), vec!["renewal"]);
    assert_ne!(second_page["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_paginates_with_anchor_bound_cursor() -> Result<()> {
    let (database, first_page) =
        v2_history_payload("/v2/names/history.eth/history?page_size=3").await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();
    assert_eq!(first_page["page"]["has_more"], json!(true));

    let second_page = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/history.eth/history?page_size=3&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["has_more"], json!(true));
    let first_hashes = history_transaction_hashes(&first_page);
    let second_hashes = history_transaction_hashes(&second_page);
    assert!(
        first_hashes
            .iter()
            .all(|hash| !second_hashes.contains(hash)),
        "history pages must not overlap"
    );
    assert_eq!(first_hashes, vec!["0xtx110", "0xtx109"]);
    assert_eq!(second_hashes, vec!["0xtx108", "0xtx107", "0xtx106"]);

    let replay = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/history.eth/history?page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(replay["data"], second_page["data"]);
    assert_eq!(replay["page"], second_page["page"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_event_cursors_resume_after_rewalk_ids_rotate() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let routes = [
        (
            "/v2/names/history.eth/history?page_size=1".to_owned(),
            false,
        ),
        ("/v2/events?name=history.eth&page_size=1".to_owned(), false),
        (
            format!("/v2/addresses/{ADDRESS}/history?page_size=1"),
            false,
        ),
        (
            "/v2/diagnostics/events?name=history.eth&page_size=1".to_owned(),
            true,
        ),
    ];

    let mut before = Vec::new();
    for (route, diagnostic) in &routes {
        let first = v2_history_payload_for_database(&database, route).await?;
        if route.starts_with("/v2/names/") || route.starts_with("/v2/events?") {
            assert_eq!(
                first["data"],
                json!([]),
                "the saved product cursor must anchor the unmapped SurfaceBound row: {route}"
            );
        }
        let cursor = first["page"]["next_cursor"]
            .as_str()
            .with_context(|| format!("{route} must produce a saved cursor"))?
            .to_owned();
        before.push((
            cursor_surface_page(&first, *diagnostic),
            cursor.clone(),
            collect_remaining_cursor_pages(&database, route, cursor, *diagnostic).await?,
        ));
    }

    sqlx::query(
        "UPDATE normalized_events
         SET normalized_event_id = DEFAULT",
    )
    .execute(&database.pool)
    .await?;

    for ((route, diagnostic), (first_before, saved_cursor, remaining_before)) in
        routes.iter().zip(before)
    {
        let remaining_after = collect_remaining_cursor_pages(
            &database,
            route,
            saved_cursor,
            *diagnostic,
        )
        .await?;
        assert_eq!(remaining_after, remaining_before, "saved cursor: {route}");

        let fresh = v2_history_payload_for_database(&database, route).await?;
        assert_eq!(
            cursor_surface_page(&fresh, *diagnostic),
            first_before,
            "fresh page: {route}"
        );
        let fresh_cursor = fresh["page"]["next_cursor"]
            .as_str()
            .with_context(|| format!("{route} must produce a fresh cursor"))?
            .to_owned();
        assert_eq!(
            collect_remaining_cursor_pages(&database, route, fresh_cursor, *diagnostic).await?,
            remaining_before,
            "fresh cursor: {route}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn candidate_migration_rows_are_diagnostic_only() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    seed_v2_history_name(
        &database,
        "ens:candidate-only.eth",
        "Candidate-Only.eth",
        "node:candidate-only.eth",
        118,
        Uuid::from_u128(0x7500),
        Uuid::from_u128(0x8500),
        Uuid::from_u128(0x9500),
    )
    .await?;
    seed_v2_history_blocks(&database, 112..=113).await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[v2_history_event(
            "candidate-only-ordinary-renewal",
            None,
            Some(Uuid::from_u128(0x7500)),
            "RegistrationRenewed",
            112,
        )],
    )
    .await?;

    let product_routes = [
        "/v2/names/history.eth/history?page_size=20".to_owned(),
        "/v2/events?name=history.eth&page_size=20".to_owned(),
        format!("/v2/addresses/{ADDRESS}/history?page_size=20"),
    ];
    let mut product_before = Vec::new();
    for route in &product_routes {
        product_before.push(v2_history_payload_for_database(&database, route).await?);
    }

    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state,
             migration_correlation_ids, consumer_visibility
         ) VALUES (
             'candidate-migration-renewal', 'ens', NULL,
             '00000000-0000-0000-0000-000000007100'::uuid,
             'RegistrationRenewed', 'ens_v2_migration_l1', 1,
             'ethereum-mainnet', 111, '0xhistory111', '0xcandidate-migration',
             1, 9, '{}'::jsonb, 'ens_v2_migration', 'canonical', '{}'::jsonb,
             '{\"expiry\": 1999999999}'::jsonb,
             ARRAY['candidate-correlation'], 'candidate'
         ), (
             'candidate-address-registration', 'ens', NULL,
             '00000000-0000-0000-0000-000000007500'::uuid,
             'RegistrationGranted', 'ens_v1_registrar_l1', 1,
             'ethereum-mainnet', 113, '0xhistory113', '0xcandidate-address',
             1, 0, '{}'::jsonb, 'ens_v1_unwrapped_authority', 'canonical', '{}'::jsonb,
             '{\"registrant\": \"0x00000000000000000000000000000000000000cc\"}'::jsonb,
             ARRAY['candidate-address-correlation'], 'candidate'
         )",
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO migration_event_associations (
             event_identity, migration_correlation_id, correlation_kind, evidence_refs,
             chain_id, block_number, block_hash, transaction_hash, transaction_index,
             log_index, canonicality_state, consumer_visibility, interpreter_content_hash
         ) VALUES (
             'history-renewal', 'attached-candidate-correlation', 'synchronized_renewal',
             '[{\"event_identity\": \"candidate-migration-renewal\"}]'::jsonb,
             'ethereum-mainnet', 110, '0xhistory110', '0xtx110', 0, 0,
             'canonical', 'candidate', 'keccak256:test'
         )",
    )
    .execute(&database.pool)
    .await?;

    for (route, before) in product_routes.iter().zip(product_before) {
        let after = v2_history_payload_for_database(&database, route).await?;
        assert_eq!(after, before, "candidate storage changed product route {route}");
    }

    let diagnostics = v2_history_payload_for_database(
        &database,
        "/v2/diagnostics/events?name=history.eth&page_size=20",
    )
    .await?;
    let diagnostic_rows = diagnostics["data"].as_array().expect("diagnostic rows");
    let candidate = diagnostic_rows
        .iter()
        .find(|row| row["event_identity"] == "candidate-migration-renewal")
        .expect("candidate migration row");
    assert_eq!(candidate["consumer_visibility"], json!("candidate"));
    assert_eq!(candidate["migration_correlation_ids"], json!(["candidate-correlation"]));
    assert!(candidate.get("migration_associations").is_none());

    let ordinary = diagnostic_rows
        .iter()
        .find(|row| row["event_identity"] == "history-renewal")
        .expect("ordinary renewal row");
    assert_eq!(ordinary["consumer_visibility"], json!("activated"));
    assert_eq!(ordinary["migration_correlation_ids"], json!([]));
    assert_eq!(ordinary["migration_associations"], json!([{
        "migration_correlation_ids": ["attached-candidate-correlation"],
        "correlation_kind": "synchronized_renewal",
        "consumer_visibility": "candidate",
    }]));

    let candidate_only_diagnostics = v2_history_payload_for_database(
        &database,
        &format!(
            "/v2/diagnostics/events?registration_id={}&page_size=20",
            Uuid::from_u128(0x7500)
        ),
    )
    .await?;
    assert!(candidate_only_diagnostics["data"].as_array().is_some_and(|rows| {
        rows.iter().any(|row| row["event_identity"] == "candidate-address-registration")
    }));

    database.cleanup().await
}

async fn collect_remaining_cursor_pages(
    database: &TestDatabase,
    route: &str,
    mut cursor: String,
    diagnostic: bool,
) -> Result<Vec<Value>> {
    let mut pages = Vec::new();
    loop {
        let payload = v2_history_payload_for_database(
            database,
            &format!("{route}&cursor={cursor}"),
        )
        .await?;
        pages.push(cursor_surface_page(&payload, diagnostic));
        let Some(next) = payload["page"]["next_cursor"].as_str() else {
            return Ok(pages);
        };
        cursor = next.to_owned();
    }
}

fn product_page(payload: &Value) -> Value {
    json!({
        "data": payload["data"],
        "page_size": payload["page"]["page_size"],
        "total_count": payload["page"]["total_count"],
        "has_more": payload["page"]["has_more"],
        "meta": payload["meta"],
    })
}

fn cursor_surface_page(payload: &Value, diagnostic: bool) -> Value {
    let mut page = product_page(payload);
    if diagnostic
        && let Some(rows) = page["data"].as_array_mut()
    {
        for row in rows {
            if let Some(row) = row.as_object_mut() {
                row.remove("normalized_event_id");
            }
        }
    }
    page
}

#[tokio::test]
async fn v2_get_history_rejects_cross_name_and_cross_scope_cursor_reuse() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    seed_v2_history_name(
        &database,
        "ens:other.eth",
        "Other.eth",
        "node:other.eth",
        81,
        Uuid::from_u128(0x7200),
        Uuid::from_u128(0x8200),
        Uuid::from_u128(0x9200),
    )
    .await?;

    let first_page =
        v2_history_payload_for_database(&database, "/v2/names/history.eth/history?page_size=3")
            .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor");

    let cross_name = v2_history_response_for_database(
        &database,
        &format!("/v2/names/other.eth/history?page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_name.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(cross_name).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    let cross_scope = v2_history_response_for_database(
        &database,
        &format!("/v2/names/history.eth/history?scope=name&page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_scope.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(cross_scope).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_scope_filters_name_registration_and_both() -> Result<()> {
    let (database, name_scope) =
        v2_history_payload("/v2/names/history.eth/history?scope=name&page_size=20").await?;
    let registration_scope = v2_history_payload_for_database(
        &database,
        "/v2/names/history.eth/history?scope=registration&page_size=20",
    )
    .await?;
    let both_scope = v2_history_payload_for_database(
        &database,
        "/v2/names/history.eth/history?scope=both&page_size=20",
    )
    .await?;

    assert_eq!(history_types(name_scope["data"].as_array().expect("data")), vec![
        "record",
        "authority",
        "resolver",
        "authority",
    ]);
    assert_eq!(
        history_types(registration_scope["data"].as_array().expect("data")),
        vec![
            "renewal",
            "expiry",
            "release",
            "permission",
            "transfer",
            "registration",
        ]
    );
    assert_eq!(both_scope["data"].as_array().expect("data").len(), 10);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_history_ignores_masked_owner_tail_but_keeps_valid_authority_history(
) -> Result<()> {
    const MATCHED_ADDRESS: &str = "0x00000000000000000000000000000000000000cc";

    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_name(
        &database,
        "ens:masked-tail.eth",
        "Masked-Tail.eth",
        "node:masked-tail.eth",
        80,
        Uuid::from_u128(0x7400),
        Uuid::from_u128(0x8400),
        Uuid::from_u128(0x9400),
    )
    .await?;
    seed_v2_history_name(
        &database,
        "ens:valid-owner.eth",
        "Valid-Owner.eth",
        "node:valid-owner.eth",
        81,
        Uuid::from_u128(0x7401),
        Uuid::from_u128(0x8401),
        Uuid::from_u128(0x9401),
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=123).await?;

    let mut masked = v2_history_event(
        "masked-owner-authority",
        Some("ens:masked-tail.eth"),
        None,
        "AuthorityTransferred",
        121,
    );
    masked.after_state = json!({
        "owner": MATCHED_ADDRESS,
        "owner_word_unmasked": true,
        "owner_word_raw":
            "0x0102030405060708090a0b0c00000000000000000000000000000000000000cc",
    });
    let valid = v2_history_event(
        "valid-owner-authority",
        Some("ens:valid-owner.eth"),
        None,
        "AuthorityTransferred",
        122,
    );
    let masked_name_event = v2_history_event(
        "masked-owner-name-event",
        Some("ens:masked-tail.eth"),
        None,
        "RecordChanged",
        123,
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[masked, valid, masked_name_event],
    )
    .await?;

    let address_payload = v2_history_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{MATCHED_ADDRESS}/history?relation=manager&page_size=20"
        ),
    )
    .await?;
    let address_rows = address_payload["data"]
        .as_array()
        .expect("address history data");
    assert_eq!(address_rows.len(), 1);
    assert_eq!(address_rows[0]["name"], json!("valid-owner.eth"));
    assert_eq!(address_rows[0]["transaction_hash"], json!("0xtx122"));
    assert!(
        address_rows
            .iter()
            .all(|row| row["name"] != json!("masked-tail.eth")),
        "the masked owner tail must not add its logical name to the address anchor set"
    );

    let name_payload = v2_history_payload_for_database(
        &database,
        "/v2/names/masked-tail.eth/history?scope=name&page_size=20",
    )
    .await?;
    let name_rows = name_payload["data"].as_array().expect("name history data");
    assert_eq!(name_rows.len(), 2);
    assert!(name_rows.iter().any(|row| {
        row["name"] == json!("masked-tail.eth")
            && row["transaction_hash"] == json!("0xtx121")
            && row["type"] == json!("authority")
    }));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_history_keeps_prior_registration_resources_after_rebinding() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let logical_name_id = "ens:history.eth";
    let prior_resource_id = Uuid::from_u128(0x7101);
    let prior_token_lineage_id = Uuid::from_u128(0x8101);
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            prior_token_lineage_id,
            "0xprior-token",
            75,
        )],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            prior_resource_id,
            Some(prior_token_lineage_id),
            "0xprior-resource",
            76,
        )],
    )
    .await?;
    let mut prior_binding = address_name_surface_binding(
        Uuid::from_u128(0x9101),
        logical_name_id,
        prior_resource_id,
        "0xprior-binding",
        77,
        1_717_176_077,
    );
    prior_binding.active_to = Some(timestamp(1_717_176_079));
    upsert_test_surface_bindings(&database.pool, &[prior_binding]).await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[v2_history_event(
            "history-prior-registration",
            None,
            Some(prior_resource_id),
            "RegistrationGranted",
            101,
        )],
    )
    .await?;

    let payload = v2_history_payload_for_database(
        &database,
        "/v2/names/history.eth/history?scope=registration&page_size=20",
    )
    .await?;
    assert!(payload["data"].as_array().expect("history data").iter().any(
        |row| row["registration_id"] == json!(prior_resource_id.to_string())
    ));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_history_empty_and_missing_name_semantics() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_name(
        &database,
        "ens:quiet.eth",
        "Quiet.eth",
        "node:quiet.eth",
        80,
        Uuid::from_u128(0x7300),
        Uuid::from_u128(0x8300),
        Uuid::from_u128(0x9300),
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=120).await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[v2_history_event(
            "quiet-surface-bound",
            Some("ens:quiet.eth"),
            None,
            "SurfaceBound",
            120,
        )],
    )
    .await?;

    let payload =
        v2_history_payload_for_database(&database, "/v2/names/quiet.eth/history").await?;
    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    let response = v2_history_response_for_database(&database, "/v2/names/missing.eth/history")
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_uses_current_sepolia_anchor_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;
    seed_v2_mixed_phase_head_history(&database).await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}/history"),
    )
    .await?;
    assert_eq!(payload["meta"], json!({}));
    assert_eq!(
        history_types(payload["data"].as_array().expect("history data")),
        vec!["registration"]
    );

    database.cleanup().await
}

async fn v2_history_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let payload = v2_history_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_history_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_history_response_for_database(database, uri).await?;
    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    Ok(payload)
}

async fn v2_history_response_for_database(
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
        .context("v2 history request failed")
}

async fn seed_v2_history_fixture(database: &TestDatabase) -> Result<()> {
    let logical_name_id = "ens:history.eth";
    let resource_id = Uuid::from_u128(0x7100);
    seed_v2_history_name(
        database,
        logical_name_id,
        "History.eth",
        "node:history.eth",
        80,
        resource_id,
        Uuid::from_u128(0x8100),
        Uuid::from_u128(0x9100),
    )
    .await?;
    seed_v2_history_blocks(database, 101..=111).await?;

    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            v2_history_event(
                "history-surface-bound",
                Some(logical_name_id),
                None,
                "SurfaceBound",
                111,
            ),
            v2_history_event(
                "history-renewal",
                None,
                Some(resource_id),
                "RegistrationRenewed",
                110,
            ),
            v2_history_event(
                "history-expiry",
                None,
                Some(resource_id),
                "ExpiryChanged",
                109,
            ),
            v2_history_event(
                "history-release",
                None,
                Some(resource_id),
                "RegistrationReleased",
                108,
            ),
            v2_history_event(
                "history-permission",
                None,
                Some(resource_id),
                "PermissionChanged",
                107,
            ),
            v2_history_event(
                "history-record",
                Some(logical_name_id),
                None,
                "RecordChanged",
                106,
            ),
            v2_history_event(
                "history-authority-epoch",
                Some(logical_name_id),
                None,
                "AuthorityEpochChanged",
                105,
            ),
            v2_history_event(
                "history-resolver",
                Some(logical_name_id),
                None,
                "ResolverChanged",
                104,
            ),
            v2_history_event(
                "history-transfer",
                None,
                Some(resource_id),
                "TokenControlTransferred",
                103,
            ),
            v2_history_event(
                "history-registration",
                None,
                Some(resource_id),
                "RegistrationGranted",
                102,
            ),
            v2_history_event(
                "history-authority",
                Some(logical_name_id),
                None,
                "AuthorityTransferred",
                101,
            ),
        ],
    )
    .await
    .context("failed to upsert v2 history fixture events")?;

    Ok(())
}

async fn seed_v2_mixed_phase_head_history(database: &TestDatabase) -> Result<()> {
    let logical_name_id = format!("ens:{V2_SEPOLIA_SNAPSHOT_NAME}");
    let resource_id = Uuid::from_u128(0x7e20);
    let block_number = V2_SEPOLIA_SNAPSHOT_BLOCK + 1;
    let block_hash = "0xv2-sepolia-history-event";

    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-sepolia",
            block_hash,
            Some(V2_SEPOLIA_SNAPSHOT_HASH),
            block_number,
            1_776_384_711,
        )],
    )
    .await?;

    let mut event = history_event(
        "v2-sepolia-current-history-registration",
        None,
        Some(resource_id),
        Some("ethereum-sepolia"),
        Some(block_number),
        Some(block_hash),
        Some("0xv2sepoliahistorytx"),
        Some(0),
        CanonicalityState::Canonical,
    );
    event.namespace = "ens".to_owned();
    event.logical_name_id = Some(logical_name_id);
    event.event_kind = "RegistrationGranted".to_owned();
    event.source_family = "ens_v2_registry_l1".to_owned();
    event.derivation_kind = "ens_v2_exact_name_profile".to_owned();
    event.after_state = json!({
        "authority_kind": "ens_v2_registry",
        "authority_key": "registry:ethereum-sepolia:sepolia-pin",
        "registrant": "0x00000000000000000000000000000000000000aa",
    });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;

    Ok(())
}

// Each independent identity and lineage value stays explicit in this fixture helper.
#[expect(clippy::too_many_arguments)]
async fn seed_v2_history_name(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    seed_v2_subnames_bound_child(
        database,
        logical_name_id,
        display_name,
        namehash,
        block_number,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000001"
            }
        }),
    )
    .await
}

async fn seed_v2_history_blocks(
    database: &TestDatabase,
    range: std::ops::RangeInclusive<i64>,
) -> Result<()> {
    let blocks = range
        .map(|block_number| {
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{block_number}"),
                None,
                block_number,
                1_700_000_000 + block_number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    Ok(())
}

fn v2_history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    block_number: i64,
) -> NormalizedEvent {
    let mut event = history_event(
        event_identity,
        logical_name_id,
        resource_id,
        Some("ethereum-mainnet"),
        Some(block_number),
        Some(&format!("0xhistory{block_number}")),
        Some(&format!("0xtx{block_number}")),
        Some(0),
        CanonicalityState::Canonical,
    );
    event.event_kind = event_kind.to_owned();
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.before_state = json!({});
    event.after_state = v2_history_after_state(event_kind);
    event
}

fn v2_history_after_state(event_kind: &str) -> Value {
    match event_kind {
        "RegistrationGranted" => json!({
            "authority_kind": "registrar",
            "authority_key": "registrar:ethereum-mainnet:history",
            "registrant": "0x00000000000000000000000000000000000000aa",
            "expiry": 1_900_000_000_i64,
        }),
        "RegistrationRenewed" | "ExpiryChanged" => json!({
            "expiry": 1_950_000_000_i64,
        }),
        "RegistrationReleased" => json!({
            "released_at": 1_960_000_000_i64,
        }),
        "TokenControlTransferred" => json!({
            "to": "0x00000000000000000000000000000000000000bb",
        }),
        "AuthorityTransferred" => json!({
            "owner": "0x00000000000000000000000000000000000000cc",
        }),
        "AuthorityEpochChanged" => json!({
            "authority_kind": "registrar",
            "authority_key": "registrar:ethereum-mainnet:history",
            "registry_owner": "0x00000000000000000000000000000000000000cc",
        }),
        "ResolverChanged" => json!({
            "resolver": "0x0000000000000000000000000000000000000abc",
            "namehash": "node:history.eth",
        }),
        "RecordChanged" => json!({
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "value": "0x0000000000000000000000000000000000000def",
        }),
        "PermissionChanged" => json!({
            "subject": "0x00000000000000000000000000000000000000dd",
            "scope": {
                "kind": "resource"
            },
            "powers": ["resource_control"],
        }),
        "SurfaceBound" => json!({
            "binding_kind": "declared_registry_path",
        }),
        _ => json!({}),
    }
}

fn history_types(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["type"].as_str().expect("history row type"))
        .collect()
}

fn history_transaction_hashes(payload: &Value) -> Vec<&str> {
    payload["data"]
        .as_array()
        .expect("history data")
        .iter()
        .map(|row| {
            row["transaction_hash"]
                .as_str()
                .expect("history row transaction_hash")
        })
        .collect()
}
