// `GET /v1/names`: the namespace-wide expiry window listing.
//
// The fixture writes `registration.expiry` the way the projection does — a JSON number of unix
// seconds — plus one string-expiry row and one no-expiry row that the index-backed listing must
// not serve, and one basenames row the namespace filter must exclude. Rows are seeded without a
// surface binding, so their `registration_status` classifies as unregistered; the listing's
// window, order, and cursor behaviour are what is under test here.

const V2_NAMES_BETA_EXPIRY: i64 = 1_790_812_800; // 2026-10-01T00:00:00Z
const V2_NAMES_GAMMA_EXPIRY: i64 = 1_764_547_200; // 2025-12-01T00:00:00Z
const V2_NAMES_ALPHA_EXPIRY: i64 = 1_798_761_600; // 2027-01-01T00:00:00Z
const V2_NAMES_BASE_EXPIRY: i64 = 1_793_491_200; // 2026-11-01T00:00:00Z

async fn seed_v2_names_fixture(database: &TestDatabase) -> Result<()> {
    let rows: [(&str, &str, &str, i64, Value); 6] = [
        (
            "ens:alpha.eth",
            "alpha.eth",
            "node:alpha.eth",
            91,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                    "registrant": "0x00000000000000000000000000000000000000a2",
                    "registered_at": "2024-01-02T00:00:00Z",
                    "expiry": V2_NAMES_ALPHA_EXPIRY
                },
                "control": { "registry_owner": "0x00000000000000000000000000000000000000a1" }
            }),
        ),
        (
            "ens:beta.eth",
            "beta.eth",
            "node:beta.eth",
            92,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                    "registrant": "0x00000000000000000000000000000000000000b2",
                    "registered_at": "2024-02-02T00:00:00Z",
                    "expiry": V2_NAMES_BETA_EXPIRY
                },
                "control": { "registry_owner": "0x00000000000000000000000000000000000000b1" }
            }),
        ),
        (
            "ens:gamma.eth",
            "gamma.eth",
            "node:gamma.eth",
            93,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                    "registrant": "0x00000000000000000000000000000000000000c2",
                    "registered_at": "2024-03-02T00:00:00Z",
                    "expiry": V2_NAMES_GAMMA_EXPIRY
                },
                "control": { "registry_owner": "0x00000000000000000000000000000000000000c1" }
            }),
        ),
        (
            "ens:delta.eth",
            "delta.eth",
            "node:delta.eth",
            94,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                    "registrant": "0x00000000000000000000000000000000000000d2",
                    "expiry": "2026-10-15T00:00:00Z"
                },
                "control": { "registry_owner": "0x00000000000000000000000000000000000000d1" }
            }),
        ),
        (
            "ens:epsilon.eth",
            "epsilon.eth",
            "node:epsilon.eth",
            95,
            json!({}),
        ),
        (
            "basenames:alpha.base.eth",
            "alpha.base.eth",
            "node:alpha.base.eth",
            96,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                    "registrant": "0x00000000000000000000000000000000000000e2",
                    "expiry": V2_NAMES_BASE_EXPIRY
                },
                "control": { "registry_owner": "0x00000000000000000000000000000000000000e1" }
            }),
        ),
    ];

    for (logical_name_id, name, namehash, block_number, declared_summary) in rows {
        upsert_test_name_surfaces(
            &database.pool,
            &[collection_name_surface(
                logical_name_id,
                name,
                namehash,
                block_number,
            )],
        )
        .await?;
        database
            .insert_name_current_row(v2_subnames_name_current_row(
                logical_name_id,
                name,
                namehash,
                block_number,
                None,
                None,
                None,
                declared_summary,
            ))
            .await?;
    }
    Ok(())
}

async fn v2_names_response(database: &TestDatabase, uri: &str) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 names request failed")
}

async fn v2_names_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_names_response(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    read_json(response).await
}

fn v2_names_listed(payload: &Value) -> Vec<String> {
    payload["data"]
        .as_array()
        .expect("names data must be an array")
        .iter()
        .map(|row| {
            row["name"]
                .as_str()
                .expect("name must be a string")
                .to_owned()
        })
        .collect()
}

#[tokio::test]
async fn v2_get_names_lists_a_namespace_expiry_window_in_expiry_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&expires_before=2027-06-01T00:00:00Z&sort=expires_at&order=asc",
    )
    .await?;
    assert_eq!(
        v2_names_listed(&payload),
        vec!["gamma.eth", "beta.eth", "alpha.eth"],
        "numeric registration expiries in the window, ascending; string and missing expiries and other namespaces are absent"
    );
    assert_eq!(
        payload["data"][0]["expires_at"],
        json!("2025-12-01T00:00:00Z")
    );
    assert_eq!(payload["data"][0]["namespace"], json!("ens"));
    assert_eq!(payload["data"][0]["display_name"], json!("gamma.eth"));
    assert_eq!(
        payload["data"][0]["registrant"],
        json!("0x00000000000000000000000000000000000000c2")
    );
    assert_eq!(
        payload["data"][0]["owner"],
        json!("0x00000000000000000000000000000000000000c1")
    );
    assert_eq!(
        payload["data"][0]["registered_at"],
        json!("2024-03-02T00:00:00Z")
    );
    assert!(payload["data"][0].get("relations").is_none());
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert!(payload["meta"].get("as_of_token").is_none());

    // `sort` and `order` default to expires_at ascending.
    let defaulted = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&defaulted), v2_names_listed(&payload));

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_before=2027-06-01T00:00:00Z&order=desc",
    )
    .await?;
    assert_eq!(
        v2_names_listed(&payload),
        vec!["alpha.eth", "beta.eth", "gamma.eth"]
    );

    // `expires_after` is inclusive and `expires_before` exclusive, so consecutive windows tile.
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-12-01T00:00:00Z&expires_before=2026-10-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["gamma.eth"]);
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2026-10-01T00:00:00Z&expires_before=2027-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["beta.eth"]);

    // Fractional bounds still include whole-second expiries before the exclusive upper bound.
    for before in ["2026-10-01T00:00:00.5Z", "2026-10-01T00:00:00.000001Z"] {
        let payload = v2_names_payload(
            &database,
            &format!(
                "/v1/names?namespace=ens&expires_after=2026-10-01T00:00:00Z&expires_before={before}"
            ),
        )
        .await?;
        assert_eq!(v2_names_listed(&payload), vec!["beta.eth"], "{before}");
    }
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2026-10-01T00:00:00.5Z&expires_before=2027-01-01T00:00:00.5Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["alpha.eth"]);

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=basenames&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["alpha.base.eth"]);

    // At large Unix timestamps, f64 rounds a fractional bound back onto the whole-second
    // expiry. The index prefilter must still leave the exact timestamp comparison to decide.
    sqlx::query(
        "UPDATE name_current SET declared_summary = jsonb_set(         declared_summary, '{registration,expiry}', '253402214400'::jsonb)          WHERE raw_name = 'beta.eth'",
    )
    .execute(&database.pool)
    .await?;
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=9999-12-31T00:00:00Z&expires_before=9999-12-31T00:00:00.00001Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["beta.eth"]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_cursor_requires_same_publication_and_bound_evaluation_time() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        100,
        "0xcollection-head",
        "2026-06-10T00:00:00Z",
    )
    .await?;
    let base = "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1";
    let first = v2_names_payload(&database, base).await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .context("first page has continuation")?;
    let payload = crate::v2::decode(cursor).expect("issued cursor decodes");
    assert!(
        payload
            .snapshot
            .as_deref()
            .is_some_and(|token| token.starts_with("publication-"))
    );
    assert!(payload.evaluated_at.is_some());
    let second = v2_names_payload(&database, &format!("{base}&cursor={cursor}")).await?;
    let second_cursor = crate::v2::decode(
        second["page"]["next_cursor"]
            .as_str()
            .context("second continuation")?,
    )
    .expect("issued continuation decodes");
    assert_eq!(second_cursor.evaluated_at, payload.evaluated_at);
    assert_eq!(second["meta"]["as_of"], first["meta"]["as_of"]);

    let mut legacy = payload;
    legacy.snapshot = None;
    legacy.evaluated_at = None;
    let response = v2_names_response(
        &database,
        &format!("{base}&cursor={}", crate::v2::encode(&legacy)),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        "stale"
    );

    // Same block and hash, but a new Project publication transaction.
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        100,
        "0xcollection-head",
        "2026-06-10T00:00:00Z",
    )
    .await?;
    let response = v2_names_response(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let stale: Value = read_json(response).await?;
    assert_eq!(stale["error"]["code"], "stale");
    assert!(
        stale["error"]["message"]
            .as_str()
            .unwrap()
            .contains("restart")
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_revalidates_publication_after_reads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        100,
        "0xcollection-head",
        "2026-06-10T00:00:00Z",
    )
    .await?;
    let state = database.app_state();
    let snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("capture ready publication");
    assert!(snapshot.finish(&state).await.is_ok());
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        100,
        "0xcollection-head",
        "2026-06-10T00:00:00Z",
    )
    .await?;
    let error = snapshot
        .finish(&state)
        .await
        .expect_err("republished state cannot finish prior read");
    assert_eq!(error.code(), crate::v2::ErrorCode::Stale);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_names_paginates_by_keyset_and_binds_cursors_to_window_and_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;

    let first = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1",
    )
    .await?;
    assert_eq!(v2_names_listed(&first), vec!["gamma.eth"]);
    assert_eq!(first["page"]["has_more"], json!(true));
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must carry a cursor")
        .to_owned();

    let second = v2_names_payload(
        &database,
        &format!(
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(v2_names_listed(&second), vec!["beta.eth"]);
    assert_eq!(second["page"]["cursor"], json!(cursor));
    let second_cursor = second["page"]["next_cursor"]
        .as_str()
        .expect("second page must carry a cursor")
        .to_owned();
    let third = v2_names_payload(
        &database,
        &format!("/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={second_cursor}"),
    )
    .await?;
    assert_eq!(v2_names_listed(&third), vec!["alpha.eth"]);
    assert_eq!(third["page"]["has_more"], json!(false));
    assert_eq!(third["page"]["next_cursor"], Value::Null);

    for uri in [
        format!(
            "/v1/names?namespace=ens&expires_after=2025-01-02T00:00:00Z&page_size=1&cursor={cursor}"
        ),
        format!(
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&expires_before=2027-06-01T00:00:00Z&page_size=1&cursor={cursor}"
        ),
        format!(
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&order=desc&page_size=1&cursor={cursor}"
        ),
        format!(
            "/v1/names?namespace=basenames&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={cursor}"
        ),
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&cursor=not-a-cursor".to_owned(),
    ] {
        let response = v2_names_response(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], json!("invalid_input"));
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_names_rejects_unbounded_or_malformed_requests() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;

    for (uri, message_fragment) in [
        (
            "/v1/names?expires_after=2025-01-01T00:00:00Z",
            "namespace is required",
        ),
        ("/v1/names?namespace=ens", "expires_after or expires_before"),
        (
            "/v1/names?namespace=ens&sort=expires_at",
            "expires_after or expires_before",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2027-01-01T00:00:00Z&expires_before=2026-01-01T00:00:00Z",
            "earlier than",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2026-01-01T00:00:00Z&expires_before=2026-01-01T00:00:00Z",
            "earlier than",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&sort=name",
            "sort must be expires_at",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&sort=registered_at",
            "sort must be expires_at",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&order=sideways",
            "order is invalid",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01",
            "RFC 3339",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=201",
            "page_size",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&q=al",
            "q",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&at=2026-01-01T00:00:00Z",
            "at is not supported",
        ),
    ] {
        let response = v2_names_response(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], json!("invalid_input"), "{uri}");
        let message = body["error"]["message"]
            .as_str()
            .expect("error message must be a string");
        assert!(
            message.contains(message_fragment),
            "{uri}: {message} must mention {message_fragment}"
        );
    }

    let response = v2_names_response(
        &database,
        "/v1/names?namespace=nope&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value = read_json(response).await?;
    assert_eq!(body["error"]["code"], json!("not_found"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_indexed_name_read_carries_weak_etag_and_honours_if_none_match() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_alice_name_record_fixture(&database, |_| {}, |_, _, _| {}).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("indexed name request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response
        .headers()
        .get(axum::http::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("indexed name read must carry an ETag")
        .to_owned();
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("public, max-age=12, stale-while-revalidate=48")
    );
    let payload: Value = read_json(response).await?;
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("indexed name read must carry meta.as_of_token");
    assert!(etag.starts_with("W/\""));
    assert_ne!(etag, format!("W/\"{token}\""));

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth")
                .header(axum::http::header::IF_NONE_MATCH, etag.as_str())
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("conditional indexed name request failed")?;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(etag.as_str())
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .context("304 body must read")?;
    assert!(body.is_empty(), "304 must carry no body");

    // The same snapshot pinned explicitly yields the same validator; a verified read never does.
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/names/Alice.eth?at={token}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("pinned indexed name request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(etag.as_str())
    );

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=verified")
                .header(axum::http::header::IF_NONE_MATCH, "*")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified name request failed")?;
    assert_ne!(response.status(), StatusCode::NOT_MODIFIED);
    assert!(
        !response.headers().contains_key(axum::http::header::ETAG),
        "verified reads must not carry an ETag"
    );
    assert!(
        !response
            .headers()
            .contains_key(axum::http::header::CACHE_CONTROL),
        "verified reads must not carry Cache-Control"
    );

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/subnames")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("subnames request failed")?;
    assert!(
        !response.headers().contains_key(axum::http::header::ETAG),
        "collections must not carry an ETag"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_explicit_namespace_ignores_unavailable_other_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": { "chain_id": "ethereum-mainnet", "block_number": 100,
                "block_hash": "0xcollection-head", "timestamp": "2026-06-10T00:00:00Z" }
        }))
        .await?;
    let state = database.app_state();
    let ens = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("ENS publication is ready independently");
    ens.finish(&state).await.expect("ENS still ready");
    assert!(
        crate::v2::collection_snapshot::CollectionSnapshot::capture(&state, None)
            .await
            .is_err(),
        "aggregate must not silently omit an unavailable namespace"
    );
    database.cleanup().await
}
