// `GET /v1/names/{name}/records?values=false`: the name's served record inventory with
// `records: {}`, without the 200-key limit of the value answers (docs/api-v1-routes.md
// § `GET /v1/names/{name}/records`). Each case compares the mode with the `include=inventory`
// read of the same name, which is the inventory it must return.

async fn records_route_status(state: &AppState, uri: &str) -> Result<(StatusCode, Value)> {
    let (status, body) = records_route_get(state, uri).await?;
    Ok((status, serde_json::from_str(&body)?))
}

/// `values=false` returns the `include=inventory` read's data with an empty `records` map.
fn inventory_only_data(full: &Value) -> Value {
    let mut expected = full["data"].clone();
    expected["records"] = json!({});
    expected
}

#[tokio::test]
async fn v2_get_name_records_values_false_serves_the_inventory_without_answers() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs_with_writes(&database, &[
        family_fixture_record_write("addr:60", Some(json!("0x0000000000000000000000000000000000000def"))),
        family_fixture_record_write("avatar", None),
        family_fixture_record_write("text:description", Some(json!("Alice profile"))),
        json!({"source_event":"PubkeyChanged","record_key":"pubkey","record_family":"pubkey","selector_key":null,"x":"0x00","y":"0x00"})
    ]).await?;
    let state = database.app_state();
    let full = records_route_json(&state, "/v1/names/Alice.eth/records?include=inventory").await?;
    assert_eq!(
        full["data"]["inventory"],
        json!({
            "known_keys": ["addr:60", "text:description"],
            "unset_keys": [], "unsupported_keys": ["text:avatar"], "abi_content_types": []
        }),
        "{full:#}"
    );
    assert_eq!(
        full["data"]["records"]
            .as_object()
            .map(|records| records.len()),
        Some(3)
    );
    for query in [
        "values=false",
        "values=false&include=inventory",
        "values=false&keys=",
        "values=%20false%20&keys=%20%20",
        "values=false&source=indexed",
        "values=false&source=auto",
    ] {
        let uri = format!("/v1/names/Alice.eth/records?{query}");
        let body = records_route_json(&state, &uri).await?;
        assert_eq!(body["data"], inventory_only_data(&full), "{uri}: {body:#}");
        assert_eq!(body["meta"], full["meta"], "{uri}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_values_false_serves_inventories_above_the_value_limit() -> Result<()> {
    for count in [200, 201] {
        let database = TestDatabase::new_with_schemas(false, true).await?;
        let writes = (0..count)
            .map(|index| family_fixture_record_write(&format!("text:key-{index}"), Some(json!(""))))
            .collect::<Vec<_>>();
        seed_alice_name_inputs_with_writes(&database, &writes).await?;
        // A provider call would fail the request with a transport error, and reaching verified
        // execution would block on the installed hook until the bounded request timeout.
        let state = unavailable_rpc_state(&database).await?;
        let (_hook_guard, _hook_control) =
            crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
        let mut known_keys = (0..count)
            .map(|index| format!("text:key-{index}"))
            .collect::<Vec<_>>();
        known_keys.sort();
        for query in ["values=false", "values=false&source=auto"] {
            let uri = format!("/v1/names/Alice.eth/records?{query}");
            let body = records_route_json(&state, &uri).await?;
            assert_eq!(body["data"]["records"], json!({}), "{uri}");
            assert_eq!(
                body["data"]["inventory"],
                json!({"known_keys": known_keys, "unset_keys": [], "unsupported_keys": [],
                       "abi_content_types": []}),
                "{uri}"
            );
            assert_eq!(body["meta"]["source"], json!("indexed"), "{uri}");
        }
        // The value answers keep their limit.
        for query in [
            "",
            "values=true",
            "values=",
            "include=inventory&values=true",
        ] {
            let uri = format!("/v1/names/Alice.eth/records?{query}");
            let (status, body) = records_route_status(&state, &uri).await?;
            if count == 200 {
                assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
                assert_eq!(
                    body["data"]["records"]
                        .as_object()
                        .map(|records| records.len()),
                    Some(200)
                );
            } else {
                assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}: {body:#}");
                assert_eq!(
                    body["error"]["message"],
                    json!("inventory-derived record key sets support at most 200 record keys"),
                    "{uri}"
                );
            }
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_values_false_without_a_resolver_reports_no_inventory_row() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs_with_writes(&database, &[]).await?;
    append_alice_name_input(
        &database,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({
            "node": bigname_lookup::ens_namehash_hex("alice.eth")?,
            "resolver": "0x0000000000000000000000000000000000000000"
        }),
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let state = database.app_state();
    let full = records_route_json(&state, "/v1/names/Alice.eth/records?include=inventory").await?;
    let body = records_route_json(&state, "/v1/names/Alice.eth/records?values=false").await?;
    assert_eq!(body["data"], inventory_only_data(&full), "{body:#}");
    assert!(body["data"]["resolver"].is_null(), "{body:#}");
    assert_eq!(
        body["data"]["inventory"],
        json!({"known_keys": [], "unset_keys": [], "unsupported_keys": [],
               "abi_content_types": null, "abi_unsupported_reason": "inventory_not_available"}),
        "{body:#}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_values_false_keeps_unsupported_inventory_meanings() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let state = database.app_state();
    let full = records_route_json(&state, "/v1/names/Alice.eth/records?include=inventory").await?;
    let body = records_route_json(&state, "/v1/names/Alice.eth/records?values=false").await?;
    assert_eq!(body["data"], inventory_only_data(&full), "{body:#}");
    assert_eq!(
        body["data"]["inventory"],
        json!({"known_keys": [], "unset_keys": [], "unsupported_keys": ["addr:60", "text:description"],
               "abi_content_types": null, "abi_unsupported_reason": "inventory_not_authoritative"}),
        "{body:#}"
    );
    database.cleanup().await
}

/// Registration states: an ownerless registry name and a wrapped name keep the inventory, while a
/// released name, a reservation and an unbound authority row withhold it, as `include=inventory`
/// does.
#[tokio::test]
async fn v2_get_name_records_values_false_follows_the_include_inventory_withholding() -> Result<()>
{
    for (label, input, has_inventory) in [
        ("ownerless serving", AliceInputState::Ownerless, true),
        ("wrapped", AliceInputState::Wrapped, true),
        ("released", AliceInputState::Released, false),
        ("reservation", AliceInputState::Reserved, false),
        ("unbound authority", AliceInputState::Unbound, false),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_state_inputs(&database, input).await?;
        let state = database.app_state();
        let full =
            records_route_json(&state, "/v1/names/Alice.eth/records?include=inventory").await?;
        let body = records_route_json(&state, "/v1/names/Alice.eth/records?values=false").await?;
        assert_eq!(
            body["data"],
            inventory_only_data(&full),
            "{label}: {body:#}"
        );
        assert_eq!(
            body["data"].get("inventory").is_some(),
            has_inventory,
            "{label}: {body:#}"
        );
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_values_false_answers_404_for_an_unindexed_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let state = database.app_state();
    for uri in [
        "/v1/names/nobody.eth/records?values=false",
        "/v1/names/nobody.eth/records",
    ] {
        let (status, body) = records_route_status(&state, uri).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {body:#}");
        assert_eq!(body["error"]["code"], json!("not_found"), "{uri}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_values_false_rejects_keys_verified_and_bad_flags() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let state = database.app_state();
    let oversized = (0..201)
        .map(|index| format!("text:key-{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let keys_conflict = "keys must be omitted when values=false";
    let verified_conflict = "source must be indexed or auto when values=false";
    let mut cases = vec![
        (
            "values=false&keys=text:description".to_owned(),
            keys_conflict,
        ),
        ("values=false&keys=abi:1".to_owned(), keys_conflict),
        (format!("values=false&keys={oversized}"), keys_conflict),
        (
            "values=false&source=auto&keys=text:description".to_owned(),
            keys_conflict,
        ),
        ("values=false&source=verified".to_owned(), verified_conflict),
        (
            "values=false&source=verified&keys=abi:1".to_owned(),
            verified_conflict,
        ),
        (
            "values=false&include=counts".to_owned(),
            "include must contain only inventory",
        ),
        (
            "values=false&values=false".to_owned(),
            "query parameters are invalid",
        ),
    ];
    for invalid in ["0", "FALSE", "none", "true,false"] {
        cases.push((format!("values={invalid}"), "values is invalid"));
    }
    for (query, message) in cases {
        let uri = format!("/v1/names/alice.eth/records?{query}");
        let (status, body) = records_route_status(&state, &uri).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body:#}");
        assert_eq!(
            body["error"],
            json!({"code": "invalid_input", "details": {}, "message": message}),
            "{uri}"
        );
    }
    // The route that owns the flag accepts it. Name detail and the record diagnostic do not.
    for uri in [
        "/v1/names/alice.eth?values=false",
        "/v1/diagnostics/names/alice.eth/records?values=false",
    ] {
        let (status, body) = records_route_status(&state, uri).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body:#}");
        assert_eq!(
            body["error"]["message"],
            json!("unknown query parameter: values"),
            "{uri}"
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_values_true_keeps_the_value_answers() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let state = database.app_state();
    for base in [
        "include=inventory",
        "keys=addr:60,text:missing&include=inventory",
        "keys=contenthash",
    ] {
        let expected =
            records_route_json(&state, &format!("/v1/names/alice.eth/records?{base}")).await?;
        assert_ne!(
            expected["data"]["records"],
            json!({}),
            "{base}: {expected:#}"
        );
        for flag in ["true", "", "%20", "%20true%20"] {
            let uri = format!("/v1/names/alice.eth/records?{base}&values={flag}");
            let body = records_route_json(&state, &uri).await?;
            assert_eq!(body, expected, "{uri}");
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_records_values_false_carries_its_own_cache_validator() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let state = database.app_state();
    let get_with = async |uri: &str, validator: Option<&str>| {
        let mut request = Request::builder().uri(uri);
        if let Some(validator) = validator {
            request = request.header(axum::http::header::IF_NONE_MATCH, validator);
        }
        let response = app_router(state.clone())
            .oneshot(request.body(Body::empty())?)
            .await?;
        anyhow::Ok((response.status(), response.headers().clone()))
    };
    let etag_of = |headers: &axum::http::HeaderMap| {
        headers
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let inventory_only = "/v1/names/alice.eth/records?values=false";
    let (status, headers) = get_with(inventory_only, None).await?;
    assert_eq!(status, StatusCode::OK);
    let etag = etag_of(&headers).context("values=false ETag")?;
    assert_eq!(
        headers
            .get(axum::http::header::CACHE_CONTROL)
            .map(|value| value.as_bytes()),
        Some("public, max-age=12, stale-while-revalidate=48".as_bytes())
    );
    let (status, _) = get_with(inventory_only, Some(&etag)).await?;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    let (_, value_headers) =
        get_with("/v1/names/alice.eth/records?include=inventory", None).await?;
    let value_etag = etag_of(&value_headers).context("value ETag")?;
    assert_ne!(value_etag, etag);
    let (status, headers) = get_with(inventory_only, Some(&value_etag)).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(etag_of(&headers), Some(etag));
    database.cleanup().await
}

/// A name whose ENSv2 resolver is a declared ENSv1 mirror: the mode returns the inventory the
/// mirror's ENSv1 registry walk selects, and the refusal when the walk reaches an ancestor.
#[tokio::test]
async fn v2_get_name_records_values_false_follows_the_mirror_walk() -> Result<()> {
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    for (source, known_keys) in [
        (MirrorFixtureSource::Exact, true),
        (MirrorFixtureSource::Ancestor, false),
    ] {
        let database = v2_mirror_records_database("alice.eth", MIRROR, source, "resolver").await?;
        let full = v2_name_record_payload_for_database(
            &database,
            "/v1/names/alice.eth/records?include=inventory",
        )
        .await?;
        let body = v2_name_record_payload_for_database(
            &database,
            "/v1/names/alice.eth/records?values=false",
        )
        .await?;
        assert_eq!(body["data"], inventory_only_data(&full), "{body:#}");
        assert_eq!(
            body["data"]["resolver"]["address"],
            json!(MIRROR),
            "{body:#}"
        );
        let inventory = &body["data"]["inventory"];
        if known_keys {
            assert_ne!(inventory["known_keys"], json!([]), "{body:#}");
        } else {
            assert_eq!(inventory["known_keys"], json!([]), "{body:#}");
            assert_eq!(
                inventory["abi_unsupported_reason"],
                json!("inventory_not_authoritative"),
                "{body:#}"
            );
        }
        database.cleanup().await?;
    }
    Ok(())
}
