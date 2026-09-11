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
    assert_eq!(payload["data"][0]["expires_at"], json!("2025-12-01T00:00:00Z"));
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
    assert_eq!(payload["meta"], json!({}));

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

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=basenames&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["alpha.base.eth"]);

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
        &format!("/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={cursor}"),
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
        format!("/v1/names?namespace=ens&expires_after=2025-01-02T00:00:00Z&page_size=1&cursor={cursor}"),
        format!("/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&expires_before=2027-06-01T00:00:00Z&page_size=1&cursor={cursor}"),
        format!("/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&order=desc&page_size=1&cursor={cursor}"),
        format!("/v1/names?namespace=basenames&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={cursor}"),
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
