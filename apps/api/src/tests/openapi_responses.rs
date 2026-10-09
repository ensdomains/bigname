use super::*;

// The existing family conformance fixture captures fourteen product operations
// and six diagnostics. These six complete the selected OpenAPI surface.
const ADDITIONAL_OPENAPI_OPERATIONS: &[&str] = &[
    "GET /v1/names",
    "GET /v1/registries/{chain_id}/{address}",
    "GET /v1/registries/{chain_id}/{address}/labels",
    "GET /v1/resolvers/{chain_id}/{address}/links",
    "GET /v1/resolvers/{chain_id}/{address}/roles",
    "GET /v1/resolvers/{chain_id}/{address}/records",
];

#[tokio::test]
async fn openapi_page_size_boundaries_match_real_query_and_lookup_requests() -> Result<()> {
    let names_database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&names_database).await?;
    let lookup_database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&lookup_database, address).await?;
    for (page_size, status) in [
        (0, StatusCode::BAD_REQUEST),
        (1, StatusCode::OK),
        (200, StatusCode::OK),
        (201, StatusCode::BAD_REQUEST),
    ] {
        let names = v2_names_response(
            &names_database,
            &format!(
                "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size={page_size}"
            ),
        )
        .await?;
        let lookup = v2_lookup_response_for_database(
            &lookup_database,
            "/v1/lookup",
            json!({"inputs":[{"address":address,"relation":"owner","page_size":page_size}]}),
        )
        .await?;
        for (route, response, page_path) in [
            ("GET /v1/names", names, "/page/page_size"),
            ("POST /v1/lookup", lookup, "/data/0/page/page_size"),
        ] {
            assert_eq!(response.status(), status, "{route} page_size={page_size}");
            let body: Value = read_json(response).await?;
            if status == StatusCode::OK {
                assert_eq!(body.pointer(page_path), Some(&json!(page_size)), "{route}");
            } else {
                assert_eq!(body["error"]["code"], "invalid_input", "{route}");
                assert!(
                    body["error"]["message"]
                        .as_str()
                        .unwrap()
                        .contains("page_size")
                );
            }
        }
    }
    names_database.cleanup().await?;
    lookup_database.cleanup().await
}

#[test]
fn openapi_nonempty_fixture_inventory_covers_every_operation() {
    let captured = V2_CONFORMANCE_ROUTES
        .iter()
        .filter(|route| route.tier == V2RouteTier::Product)
        .map(|route| route.label)
        .chain(ADDITIONAL_OPENAPI_OPERATIONS.iter().copied())
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
    let documented = openapi_contract::document()["paths"]
        .as_object()
        .unwrap()
        .iter()
        .flat_map(|(path, methods)| {
            methods
                .as_object()
                .unwrap()
                .keys()
                .map(move |method| format!("{} {path}", method.to_uppercase()))
        })
        .collect();
    assert_eq!(captured, documented);
}

#[tokio::test]
async fn openapi_record_keys_boundaries_match_real_requests() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    for count in [0, 1, 200, 201] {
        let keys = (0..count)
            .map(|i| format!("text:key{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/names/alice.eth/records?keys={keys}"))
                    .body(Body::empty())?,
            )
            .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        if count <= 200 {
            assert_eq!(status, StatusCode::OK, "keys count={count}: {body}");
            let records = body["data"]["records"].as_object().unwrap();
            if count == 0 {
                assert!(
                    !records.is_empty(),
                    "blank keys must retain inventory defaults"
                );
            } else {
                assert_eq!(
                    records.len(),
                    count,
                    "the explicit key set must not truncate"
                );
            }
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["code"], "invalid_input");
            assert_eq!(
                body["error"]["message"],
                "keys must contain at most 200 record keys"
            );
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn openapi_additional_operations_have_nonempty_response_captures() -> Result<()> {
    let mut captured = Vec::new();
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;
    let names = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_non_empty_json(&names["data"], ADDITIONAL_OPENAPI_OPERATIONS[0], "data");
    captured.push(ADDITIONAL_OPENAPI_OPERATIONS[0]);
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    for (index, suffix) in [(1, ""), (2, "/labels")] {
        let payload = registry_payload(
            &database,
            &format!("/v1/registries/1/{ALPHA_REGISTRY}{suffix}?include=counts"),
        )
        .await?;
        assert_non_empty_json(
            &payload["data"],
            ADDITIONAL_OPENAPI_OPERATIONS[index],
            "data",
        );
        captured.push(ADDITIONAL_OPENAPI_OPERATIONS[index]);
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    let manifest = seed_permissioned_collection_inputs(&database).await?;
    let resource = Uuid::from_u128(0x5500);
    insert_collection_permission_resource(&database.pool, resource).await?;
    let mut events = vec![collection_role_event(
        resource,
        V2_ADDRESS,
        V2_RESOLVER_ADDRESS,
        160,
        0,
        json!(["set_addr", "set_text"]),
        manifest,
    )];
    let mut event = history_event(
        "openapi-ResolverRecordLinked",
        None,
        None,
        Some("ethereum-mainnet"),
        Some(150),
        Some("0xcollection150"),
        Some("0xopenapi"),
        Some(2),
        CanonicalityState::Canonical,
    );
    event.event_kind = "ResolverRecordLinked".into();
    event.source_family = "ens_v2_resolver_l1".into();
    event.before_state = json!({});
    event.after_state = json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":V2_RESOLVER_ADDRESS,"node":format!("0x{:064x}",1),"resolver_record_id":"10"});
    event.raw_fact_ref["emitting_address"] = json!(V2_RESOLVER_ADDRESS);
    events.push(event);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_resolver_collection_inputs(&database).await?;
    for (index, section) in [(3, "links"), (4, "roles")] {
        let payload = v2_resolver_payload_for_database(
            &database,
            &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/{section}"),
        )
        .await?;
        assert_non_empty_json(
            &payload["data"],
            ADDITIONAL_OPENAPI_OPERATIONS[index],
            "data",
        );
        captured.push(ADDITIONAL_OPENAPI_OPERATIONS[index]);
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolvers/1/0x0000000000000000000000000000000000000abc/records?name=alice.eth")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_non_empty_json(
        &payload["data"]["records"],
        ADDITIONAL_OPENAPI_OPERATIONS[5],
        "data.records",
    );
    captured.push(ADDITIONAL_OPENAPI_OPERATIONS[5]);
    database.cleanup().await?;
    assert_eq!(captured, ADDITIONAL_OPENAPI_OPERATIONS);
    Ok(())
}
