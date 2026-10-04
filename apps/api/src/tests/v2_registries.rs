const REGISTRY_CHAIN_ID: &str = "ethereum-mainnet";
const ROOT_REGISTRY: &str = "0x00000000000000000000000000000000000000a0";
const ALPHA_REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
const ONE_REGISTRY: &str = "0x00000000000000000000000000000000000000a2";
const OTHER_EMITTER: &str = "0x00000000000000000000000000000000000000a3";
const DECLARED_REGISTRY: &str = "0x00000000000000000000000000000000000000a9";
const UNKNOWN_REGISTRY: &str = "0x00000000000000000000000000000000000000ff";

fn registry_logical_name_id(name: &str) -> String {
    bigname_storage::logical_name_id_for_name("ens", name)
}

fn registry_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    event_kind: &str,
    block_number: i64,
    emitting_address: &str,
    after_state: Value,
) -> NormalizedEvent {
    let mut event = history_event(
        event_identity,
        logical_name_id,
        None,
        Some(REGISTRY_CHAIN_ID),
        Some(block_number),
        Some(&format!("0xregistry{block_number}")),
        Some(&format!("0xtx{block_number}")),
        Some(0),
        CanonicalityState::Canonical,
    );
    event.event_kind = event_kind.to_owned();
    event.source_family = "ens_v2_registry_l1".to_owned();
    event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "event_identity": event_identity,
        "emitting_address": emitting_address,
    });
    event.before_state = json!({});
    event.after_state = after_state;
    event
}

/// Real identity inputs, registry announcements, child grants and a registry edge. The
/// current subregistry holds one and two; zed is a direct registry edge outside that instance.
async fn seed_registry_fixture(database: &TestDatabase) -> Result<()> {
    let blocks = (55..=83)
        .map(|block| {
            raw_block(
                REGISTRY_CHAIN_ID,
                &format!("0xregistry{block}"),
                None,
                block,
                1_700_000_000 + block,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    for (name, base, arm) in [
        ("alpha.eth", 0xA000, "ens_v1"),
        ("one.alpha.eth", 0xA100, "ens_v2"),
        ("two.alpha.eth", 0xA110, "ens_v2"),
        ("zed.alpha.eth", 0xA120, "ens_v1"),
    ] {
        seed_family_identity_inputs(
            &database.pool,
            "ens",
            name,
            REGISTRY_CHAIN_ID,
            59,
            "0xregistry59",
            Uuid::from_u128(base),
            Uuid::from_u128(base + 1),
            Uuid::from_u128(base + 2),
            arm,
        )
        .await?;
    }
    let alpha_instance = Uuid::from_u128(0xA190);
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, $2, 'contract')",
    )
    .bind(alpha_instance)
    .bind(REGISTRY_CHAIN_ID)
    .execute(&database.pool)
    .await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number)
        VALUES ($1, $2, $3, 61)").bind(alpha_instance).bind(REGISTRY_CHAIN_ID).bind(ALPHA_REGISTRY).execute(&database.pool).await?;
    {
        let grant = family_event(
            "registry-alpha-grant",
            Some(&registry_logical_name_id("alpha.eth")),
            Some(Uuid::from_u128(0xA000)),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            59,
            0,
            json!({"authority_kind":"registrar", "registrant":V2_ADDRESS, "expiry":1_900_000_000_i64}),
        );
        let mut grant = grant;
        grant.block_hash = Some("0xregistry59".into());
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    }
    for label in ["one", "two"] {
        insert_family_label_preimage(&database.pool, label.as_bytes()).await?;
    }
    let zed = insert_family_label_preimage(&database.pool, b"zed").await?;
    insert_family_registry_child_edge(
        &database.pool,
        "ens",
        REGISTRY_CHAIN_ID,
        "alpha.eth",
        &zed,
        V2_ADDRESS,
        65,
        "0xregistry65",
    )
    .await?;

    let alpha = registry_logical_name_id("alpha.eth");
    let one = registry_logical_name_id("one.alpha.eth");
    let two = registry_logical_name_id("two.alpha.eth");
    let mut events = vec![
        registry_event(
            "registry-root-created",
            None,
            "RegistryCreated",
            60,
            ROOT_REGISTRY,
            json!({ "source_event": "RegistryCreated", "registry": ROOT_REGISTRY }),
        ),
        registry_event(
            "registry-alpha-pointer",
            Some(&alpha),
            "SubregistryChanged",
            61,
            ROOT_REGISTRY,
            json!({ "source_event": "SubregistryUpdated", "subregistry": ALPHA_REGISTRY }),
        ),
        registry_event(
            "registry-alpha-created",
            None,
            "RegistryCreated",
            61,
            ALPHA_REGISTRY,
            json!({ "source_event": "RegistryCreated", "registry": ALPHA_REGISTRY }),
        ),
        registry_event(
            "registry-one-registered",
            Some(&one),
            "RegistrationGranted",
            62,
            ALPHA_REGISTRY,
            json!({ "source_event": "LabelRegistered", "registry_contract_instance_id":alpha_instance,
                    "status":"registered", "registrant":V2_ADDRESS, "expiry":1_900_000_000_i64 }),
        ),
        registry_event(
            "registry-one-pointer",
            Some(&one),
            "SubregistryChanged",
            63,
            ALPHA_REGISTRY,
            json!({ "source_event": "SubregistryUpdated", "subregistry": ONE_REGISTRY }),
        ),
        registry_event(
            "registry-two-registered",
            Some(&two),
            "RegistrationGranted",
            64,
            ALPHA_REGISTRY,
            json!({ "source_event": "LabelRegistered", "registry_contract_instance_id":alpha_instance,
                    "status":"registered", "registrant":V2_ADDRESS, "expiry":1_900_000_000_i64 }),
        ),
    ];
    let mut transfers = Vec::new();
    for event in &mut events {
        if event.event_kind == "RegistrationGranted" {
            event.resource_id = Some(if event.logical_name_id.as_deref() == Some(one.as_str()) {
                Uuid::from_u128(0xA100)
            } else {
                Uuid::from_u128(0xA110)
            });
            event.after_state["authority_kind"] = json!("ens_v2_registry");
            event.log_index = Some(1);
            let mut transfer = event.clone();
            transfer.event_identity.push_str("-transfer");
            transfer.event_kind = "TokenControlTransferred".into();
            transfer.log_index = Some(0);
            transfer.after_state = json!({"source_event":"Transfer", "to":V2_ADDRESS});
            transfers.push(transfer);
        }
    }
    events.extend(transfers);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum": {
        "chain_id":REGISTRY_CHAIN_ID,"block_number":83,"block_hash":"0xregistry83",
        "timestamp":"2023-11-14T22:14:43Z"}}))
        .await?;
    publish_test_families_on(&database.pool, REGISTRY_CHAIN_ID, 83).await
}

async fn seed_declared_registry(database: &TestDatabase) -> Result<()> {
    let contract_instance_id = Uuid::from_u128(0xA900);
    seed_schema_v2_ens_manifest(
        &database.pool,
        "ens_v2_root_l1",
        "root_registry",
        DECLARED_REGISTRY,
        contract_instance_id,
        false,
    )
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instance_addresses
             (contract_instance_id, chain_id, address, active_from_block_number)
         VALUES ($1, $2, $3, 55)",
    )
    .bind(contract_instance_id)
    .bind(REGISTRY_CHAIN_ID)
    .bind(DECLARED_REGISTRY)
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn registry_response(database: &TestDatabase, uri: &str) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("registry request failed")
}

async fn registry_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = registry_response(database, uri).await?;
    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected response for {uri}: {payload}"
    );
    Ok(payload)
}

async fn assert_registry_error(
    database: &TestDatabase,
    uri: &str,
    status: StatusCode,
    code: &str,
) -> Result<()> {
    let response = registry_response(database, uri).await?;
    assert_eq!(response.status(), status, "unexpected status for {uri}");
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!(code), "{uri}: {payload}");
    Ok(())
}

#[tokio::test]
async fn v2_get_registry_serves_name_parent_creation_counts_and_references() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;

    let payload = registry_payload(
        &database,
        &format!(
            "/v1/registries/1/{}",
            ALPHA_REGISTRY.to_uppercase().replace("0X", "0x")
        ),
    )
    .await?;
    let data = &payload["data"];
    assert_eq!(data["chain_id"], json!(1));
    assert_eq!(data["address"], json!(ALPHA_REGISTRY));
    assert_eq!(
        data["name"],
        json!({
            "name": "alpha.eth",
            "display_name": "alpha.eth",
            "namespace": "ens",
            "namehash": bigname_lookup::ens_namehash_hex("alpha.eth")?,
        })
    );
    assert_eq!(
        data["parent_registry"],
        json!({ "chain_id": 1, "address": ROOT_REGISTRY })
    );
    assert_eq!(data["created_block_number"], json!(61));
    assert_eq!(data["created_transaction_hash"], json!("0xtx61"));
    assert_eq!(data["created_basis"], json!("registry_created"));
    assert_eq!(data["created_at"], json!("1700000061"));
    assert_eq!(data["counts"], json!({ "labels": 2 }));
    assert_eq!(
        data["referenced_by"]["data"],
        json!([{
            "name": "alpha.eth",
            "display_name": "alpha.eth",
            "namespace": "ens",
            "namehash": bigname_lookup::ens_namehash_hex("alpha.eth")?,
        }])
    );
    assert_eq!(data["referenced_by"]["page"]["has_more"], json!(false));
    // Without the count opt-in the reference total is not counted.
    assert_eq!(data["referenced_by"]["page"]["total_count"], Value::Null);
    assert!(payload["meta"]["as_of"].is_object(), "{payload}");
    assert!(payload["meta"]["as_of_token"].is_string(), "{payload}");
    assert!(payload.get("page").is_none_or(Value::is_null));
    assert_no_banned_v1_spellings(&payload);

    let counted = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(
        counted["data"]["counts"],
        json!({ "labels": 2, "events": 5, "roles": 0 })
    );
    assert_eq!(
        counted["data"]["referenced_by"]["page"]["total_count"],
        json!(1)
    );

    let child = registry_payload(&database, &format!("/v1/registries/1/{ONE_REGISTRY}")).await?;
    assert_eq!(child["data"]["name"]["name"], json!("one.alpha.eth"));
    assert_eq!(
        child["data"]["parent_registry"],
        json!({ "chain_id": 1, "address": ALPHA_REGISTRY })
    );
    assert_eq!(child["data"]["created_basis"], json!("subregistry_pointer"));
    assert_eq!(child["data"]["created_block_number"], json!(63));
    assert_eq!(child["data"]["created_transaction_hash"], json!("0xtx63"));
    assert_eq!(child["data"]["counts"], json!({ "labels": 0 }));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_registry_root_has_no_name_or_parent_and_declared_registries_resolve() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    seed_declared_registry(&database).await?;

    let root = registry_payload(&database, &format!("/v1/registries/1/{ROOT_REGISTRY}")).await?;
    assert_eq!(root["data"]["name"], Value::Null);
    assert_eq!(root["data"]["parent_registry"], Value::Null);
    assert_eq!(root["data"]["created_basis"], json!("registry_created"));
    assert_eq!(root["data"]["created_block_number"], json!(60));
    assert_eq!(root["data"]["counts"], json!({ "labels": 0 }));
    assert_eq!(root["data"]["referenced_by"]["data"], json!([]));
    assert_eq!(
        root["data"]["referenced_by"]["page"]["has_more"],
        json!(false)
    );
    assert_eq!(
        root["data"]["referenced_by"]["page"]["total_count"],
        Value::Null
    );

    let declared =
        registry_payload(&database, &format!("/v1/registries/1/{DECLARED_REGISTRY}")).await?;
    assert_eq!(declared["data"]["created_basis"], json!("declared"));
    assert_eq!(declared["data"]["created_block_number"], json!(55));
    assert_eq!(declared["data"]["created_transaction_hash"], Value::Null);
    assert_eq!(declared["data"]["name"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_registry_counts_every_name_referencing_it_on_each_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let two = registry_logical_name_id("two.alpha.eth");
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[registry_event(
            "registry-two-pointer",
            Some(&two),
            "SubregistryChanged",
            66,
            ALPHA_REGISTRY,
            json!({ "source_event": "SubregistryUpdated", "subregistry": ONE_REGISTRY }),
        )],
    )
    .await?;
    publish_test_families_on(&database.pool, REGISTRY_CHAIN_ID, 83).await?;

    let uncounted = registry_payload(
        &database,
        &format!("/v1/registries/1/{ONE_REGISTRY}?page_size=1"),
    )
    .await?;
    let references = &uncounted["data"]["referenced_by"];
    assert_eq!(references["data"][0]["name"], json!("one.alpha.eth"));
    assert_eq!(references["page"]["total_count"], Value::Null);
    assert_eq!(references["page"]["has_more"], json!(true));

    let first = registry_payload(
        &database,
        &format!("/v1/registries/1/{ONE_REGISTRY}?page_size=1&include=counts"),
    )
    .await?;
    let references = &first["data"]["referenced_by"];
    assert_eq!(references["data"][0]["name"], json!("one.alpha.eth"));
    assert_eq!(references["page"]["total_count"], json!(2));
    assert_eq!(references["page"]["has_more"], json!(true));
    let next_cursor = references["page"]["next_cursor"]
        .as_str()
        .expect("referenced_by next cursor");
    let second = registry_payload(
        &database,
        &format!(
            "/v1/registries/1/{ONE_REGISTRY}?page_size=1&include=counts&cursor={next_cursor}"
        ),
    )
    .await?;
    let references = &second["data"]["referenced_by"];
    assert_eq!(references["data"][0]["name"], json!("two.alpha.eth"));
    assert_eq!(references["page"]["total_count"], json!(2));
    assert_eq!(references["page"]["has_more"], json!(false));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_registry_rejects_unknown_registry_and_malformed_path() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;

    assert_registry_error(
        &database,
        &format!("/v1/registries/1/{UNKNOWN_REGISTRY}"),
        StatusCode::NOT_FOUND,
        "not_found",
    )
    .await?;
    assert_registry_error(
        &database,
        &format!("/v1/registries/1/{UNKNOWN_REGISTRY}/labels"),
        StatusCode::NOT_FOUND,
        "not_found",
    )
    .await?;
    for uri in [
        "/v1/registries/1/0x1234".to_owned(),
        "/v1/registries/1/not-an-address".to_owned(),
        format!("/v1/registries/abc/{ALPHA_REGISTRY}"),
        format!("/v1/registries/1/{ALPHA_REGISTRY}?include=roles"),
        format!("/v1/registries/1/{ALPHA_REGISTRY}?owner=x"),
        format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?at=2026-04-17T00:00:00Z"),
        "/v1/registries/1/0x1234/labels".to_owned(),
    ] {
        assert_registry_error(&database, &uri, StatusCode::BAD_REQUEST, "invalid_input").await?;
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_registry_labels_pages_held_labels_with_bound_cursor_and_counts() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;

    let first = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?page_size=1"),
    )
    .await?;
    let rows = first["data"].as_array().expect("labels data");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], json!("one.alpha.eth"));
    assert_eq!(rows[0]["display_name"], json!("one.alpha.eth"));
    assert_eq!(rows[0]["namespace"], json!("ens"));
    assert_eq!(
        rows[0]["namehash"],
        json!(bigname_lookup::ens_namehash_hex("one.alpha.eth")?)
    );
    assert_eq!(rows[0]["registration_status"], json!("registered"));
    assert_eq!(
        rows[0]["subregistry"],
        json!({ "chain_id": 1, "address": ONE_REGISTRY })
    );
    assert!(rows[0].get("subname_count").is_none());
    assert_eq!(first["page"]["page_size"], json!(1));
    assert_eq!(first["page"]["total_count"], json!(2));
    assert_eq!(first["page"]["has_more"], json!(true));
    assert!(first["meta"]["as_of"].is_object());
    assert_no_banned_v1_spellings(&first);
    let next_cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must carry a cursor")
        .to_owned();

    rebuild_fixture_families(&database.pool, REGISTRY_CHAIN_ID, 83, "0xregistry83").await?;

    let second = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?page_size=1&cursor={next_cursor}"),
    )
    .await?;
    let rows = second["data"].as_array().expect("labels data");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], json!("two.alpha.eth"));
    assert!(rows[0].get("subregistry").is_none());
    assert_eq!(second["page"]["has_more"], json!(false));
    assert_eq!(second["page"]["next_cursor"], Value::Null);

    assert_registry_error(
        &database,
        &format!("/v1/registries/1/{ROOT_REGISTRY}/labels?page_size=1&cursor={next_cursor}"),
        StatusCode::BAD_REQUEST,
        "invalid_input",
    )
    .await?;

    let counted = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?include=counts"),
    )
    .await?;
    let rows = counted["data"].as_array().expect("labels data");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["subname_count"], json!(0));
    assert_eq!(rows[1]["name"], json!("two.alpha.eth"));

    let root = registry_payload(
        &database,
        &format!("/v1/registries/1/{ROOT_REGISTRY}/labels"),
    )
    .await?;
    assert_eq!(root["data"], json!([]));
    assert_eq!(root["page"]["total_count"], json!(0));

    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_subregistry_stays_at_the_served_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    seed_identity_name(
        &database,
        "basenames:mixed.base.eth",
        "mixed.base.eth",
        "mixed.base.eth",
        "namehash:mixed.base.eth",
        Uuid::from_u128(0xff201),
        Uuid::from_u128(0xff202),
        Uuid::from_u128(0xff203),
        V2_ADDRESS,
        bigname_storage::AddressNameRelation::TokenHolder,
        200,
    )
    .await?;
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            REGISTRY_CHAIN_ID,
            "0xregistry84",
            None,
            84,
            1_776_384_024,
        )],
    )
    .await?;
    sqlx::query(
        "UPDATE chain_heads SET latest_block_number = 84, latest_block_hash = '0xregistry84' \
         WHERE chain_id = $1",
    )
    .bind(REGISTRY_CHAIN_ID)
    .execute(&database.pool)
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[registry_event(
            "unpublished-lookup-subregistry",
            Some(&registry_logical_name_id("alpha.eth")),
            "SubregistryChanged",
            84,
            ROOT_REGISTRY,
            json!({"source_event": "SubregistryUpdated", "subregistry": ONE_REGISTRY}),
        )],
    )
    .await?;

    for profile in ["feed", "detail"] {
        let lookup = v2_lookup_json(
            &database,
            json!({"profile": profile, "inputs": [{"name": "alpha.eth"}, {"name": "mixed.base.eth"}]}),
        )
        .await?;
        assert_eq!(lookup["meta"]["as_of"]["1"]["block_number"], json!(83));
        assert_eq!(lookup["meta"]["as_of"]["8453"]["block_number"], json!(200));
        assert_eq!(lookup["data"][1]["status"], json!("ok"));
        assert!(lookup["data"][1]["record"].get("subregistry").is_none());
        assert_eq!(
            lookup["data"][0]["record"]["subregistry"]["address"],
            json!(ALPHA_REGISTRY)
        );
    }

    seed_schema_v2_ens_lookup_head(&database.pool, 84, "0xregistry84", "2026-04-17T00:00:24Z")
        .await?;
    publish_test_families_on(&database.pool, REGISTRY_CHAIN_ID, 84).await?;
    let published = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alpha.eth"}]}),
    )
    .await?;
    assert_eq!(
        published["data"][0]["record"]["subregistry"]["address"],
        json!(ONE_REGISTRY)
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_routes_carry_the_current_subregistry_pointer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;

    let name = registry_payload(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(
        name["data"]["subregistry"],
        json!({ "chain_id": 1, "address": ALPHA_REGISTRY })
    );
    let two = registry_payload(&database, "/v1/names/two.alpha.eth").await?;
    assert!(two["data"].get("subregistry").is_none(), "{two}");

    let subnames = registry_payload(&database, "/v1/names/alpha.eth/subnames").await?;
    let rows = subnames["data"].as_array().expect("subnames data");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["name"], json!("one.alpha.eth"));
    assert_eq!(
        rows[0]["subregistry"],
        json!({ "chain_id": 1, "address": ONE_REGISTRY })
    );
    assert!(rows[1].get("subregistry").is_none());
    assert!(rows[2].get("subregistry").is_none());

    let response = v2_lookup_response_for_database(
        &database,
        "/v1/lookup",
        json!({ "profile": "detail", "inputs": [{ "id": "alpha", "name": "alpha.eth" }] }),
    )
    .await?;
    let status = response.status();
    let lookup: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{lookup}");
    assert_eq!(
        lookup["data"][0]["record"]["subregistry"],
        json!({ "chain_id": 1, "address": ALPHA_REGISTRY }),
        "{lookup}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_events_filters_by_contract_address_and_binds_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;

    let first = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ALPHA_REGISTRY}&page_size=2"),
    )
    .await?;
    let rows = first["data"].as_array().expect("events data");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["type"], json!("registration"));
    assert_eq!(rows[0]["block_number"], json!(64));
    assert_eq!(rows[0]["name"], json!("two.alpha.eth"));
    assert_eq!(rows[1]["type"], json!("transfer"));
    assert_eq!(rows[1]["block_number"], json!(64));
    assert_eq!(first["page"]["has_more"], json!(true));
    let next_cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must carry a cursor")
        .to_owned();

    let second = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ALPHA_REGISTRY}&page_size=2&cursor={next_cursor}"),
    )
    .await?;
    let rows = second["data"].as_array().expect("events data");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["type"], json!("subregistry"));
    assert_eq!(rows[0]["block_number"], json!(63));
    assert_eq!(rows[1]["type"], json!("registration"));
    assert_eq!(rows[1]["block_number"], json!(62));
    assert_eq!(second["page"]["has_more"], json!(true));
    let cursor = second["page"]["next_cursor"]
        .as_str()
        .context("third event page")?;
    let last = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ALPHA_REGISTRY}&page_size=2&cursor={cursor}"),
    )
    .await?;
    assert_eq!(last["data"].as_array().unwrap().len(), 1);
    assert_eq!(last["data"][0]["type"], json!("transfer"));
    assert_eq!(last["data"][0]["block_number"], json!(62));
    assert_eq!(last["page"]["has_more"], json!(false));

    assert_registry_error(
        &database,
        &format!("/v1/events?contract_address={ROOT_REGISTRY}&page_size=2&cursor={next_cursor}"),
        StatusCode::BAD_REQUEST,
        "invalid_input",
    )
    .await?;
    assert_registry_error(
        &database,
        &format!("/v1/events?page_size=2&cursor={next_cursor}"),
        StatusCode::BAD_REQUEST,
        "invalid_input",
    )
    .await?;
    assert_registry_error(
        &database,
        "/v1/events?contract_address=0x1234",
        StatusCode::BAD_REQUEST,
        "invalid_input",
    )
    .await?;

    let root = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ROOT_REGISTRY}"),
    )
    .await?;
    let rows = root["data"].as_array().expect("events data");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["type"], json!("subregistry"));
    assert_eq!(rows[0]["block_number"], json!(61));
    assert_eq!(rows[0]["name"], json!("alpha.eth"));

    let mixed = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ALPHA_REGISTRY}&type=subregistry"),
    )
    .await?;
    assert_eq!(mixed["data"].as_array().expect("events data").len(), 1);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_registry_role_counts_fold_assignments_and_current_label_holders() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let blocks = (65..=70)
        .chain([90])
        .map(|block| {
            raw_block(
                REGISTRY_CHAIN_ID,
                &format!("0xregistry{block}"),
                None,
                block,
                1_700_000_000 + block,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    let current = Uuid::from_u128(0xA100);
    let previous = Uuid::from_u128(0xA200);
    let root = Uuid::from_u128(0xA300);
    let future = Uuid::from_u128(0xA400);
    for id in [previous, root, future] {
        upsert_test_resources(
            &database.pool,
            &[address_name_resource(id, None, "0xregistry65", 65)],
        )
        .await?;
    }
    let role_event =
        |id: &str, resource: Uuid, upstream: u64, account: u8, bitmap: u64, block: i64| {
            let mut event = registry_event(
                id,
                None,
                if upstream == 0 {
                    "RootPermissionChanged"
                } else {
                    "PermissionChanged"
                },
                block,
                ALPHA_REGISTRY,
                json!({
                    "source_event": "EACRolesChanged",
                    "subject": format!("0x{account:040x}"),
                    "upstream_resource": format!("0x{upstream:064x}"),
                    "role_bitmap": format!("0x{bitmap:064x}"),
                }),
            );
            event.resource_id = Some(resource);
            event
        };
    let mut other_registry = role_event("other-registry-holder", current, 0x100000001, 4, 1, 69);
    other_registry.raw_fact_ref["emitting_address"] = json!(OTHER_EMITTER);
    let mut orphan = role_event("orphan-holder", current, 0x100000001, 5, 1, 69);
    orphan.canonicality_state = CanonicalityState::Orphaned;
    let events = vec![
        role_event(
            "previous-generation-holder",
            previous,
            0x100000000,
            9,
            1,
            65,
        ),
        role_event("registry-root-holder", root, 0, 1, 1, 65),
        role_event("current-holder-grant", current, 0x100000001, 1, 0x11, 66),
        role_event(
            "current-holder-more-bits",
            current,
            0x100000001,
            1,
            0x101,
            67,
        ),
        role_event("second-holder-grant", current, 0x100000001, 2, 1, 67),
        role_event("second-holder-revoke", current, 0x100000001, 2, 0, 68),
        role_event("orphan-lineage-holder", current, 0x100000001, 7, 1, 70),
        role_event(
            "unpublished-future-generation",
            future,
            0x100000002,
            6,
            1,
            90,
        ),
        other_registry,
        orphan,
    ];
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query("UPDATE bigname_phase.chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = $1 AND block_hash = '0xregistry70'")
        .bind(REGISTRY_CHAIN_ID).execute(&database.pool).await?;

    let overview = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(overview["data"]["counts"]["roles"], json!(2), "{overview}");
    let labels = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?include=counts"),
    )
    .await?;
    assert_eq!(labels["data"][0]["role_holder_count"], json!(1), "{labels}");
    assert_eq!(labels["data"][1]["role_holder_count"], json!(0), "{labels}");
    let without_counts = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels"),
    )
    .await?;
    assert!(without_counts["data"][0].get("role_holder_count").is_none());

    let historical = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts&at=2023-11-14T22:14:27Z"),
    )
    .await?;
    assert_eq!(
        historical["data"]["counts"]["roles"],
        json!(3),
        "{historical}"
    );
    assert_eq!(historical["data"]["counts"]["labels"], Value::Null);

    // A losing-fork revocation must not erase the surviving canonical grant.
    sqlx::query("UPDATE bigname_phase.normalized_events SET canonicality_state = 'orphaned' WHERE event_identity = 'second-holder-revoke'")
        .execute(&database.pool).await?;
    let replayed = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(replayed["data"]["counts"]["roles"], json!(3));
    let labels = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?include=counts"),
    )
    .await?;
    assert_eq!(labels["data"][0]["role_holder_count"], json!(2));

    sqlx::query("UPDATE bigname_phase.normalized_events SET canonicality_state = 'canonical' WHERE event_identity = 'second-holder-revoke'")
        .execute(&database.pool).await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[role_event(
            "last-label-holder-revoke",
            current,
            0x100000001,
            1,
            0,
            69,
        )],
    )
    .await?;
    let empty_label = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(
        empty_label["data"]["counts"]["roles"],
        json!(1),
        "revoking the new generation must not restore the older generation"
    );
    let labels = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels?include=counts"),
    )
    .await?;
    assert_eq!(labels["data"][0]["role_holder_count"], json!(0));
    database.cleanup().await
}

#[tokio::test]
async fn declared_registry_reads_honor_start_retirement_and_retraction() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_declared_registry(&database).await?;
    for (block, expected) in [(54, false), (55, true), (60, true)] {
        let row = bigname_storage::load_registry_contract(
            &database.pool,
            REGISTRY_CHAIN_ID,
            DECLARED_REGISTRY,
            Some(block),
        )
        .await?;
        assert_eq!(row.is_some(), expected, "open declaration at block {block}");
    }
    sqlx::query("UPDATE bigname_phase.contract_instance_addresses SET active_to_block_number = 60, deactivated_at = now() WHERE address = $1")
        .bind(DECLARED_REGISTRY).execute(&database.pool).await?;
    for (block, expected) in [(54, false), (55, true), (60, true), (61, false)] {
        let row = bigname_storage::load_registry_contract(
            &database.pool,
            REGISTRY_CHAIN_ID,
            DECLARED_REGISTRY,
            Some(block),
        )
        .await?;
        assert_eq!(
            row.is_some(),
            expected,
            "retired declaration at block {block}"
        );
    }
    sqlx::query("UPDATE bigname_phase.contract_instance_addresses SET active_to_block_number = NULL WHERE address = $1")
        .bind(DECLARED_REGISTRY).execute(&database.pool).await?;
    assert!(
        bigname_storage::load_registry_contract(
            &database.pool,
            REGISTRY_CHAIN_ID,
            DECLARED_REGISTRY,
            Some(55),
        )
        .await?
        .is_none(),
        "retracted declaration must not become historical evidence"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_registry_event_count_matches_the_events_feed_total() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    // A registrar-surface snapshot replays retained state; the feed does not serve it.
    let mut snapshot = registry_event(
        "registry-two-snapshot-expiry",
        Some(&registry_logical_name_id("two.alpha.eth")),
        "ExpiryChanged",
        64,
        ALPHA_REGISTRY,
        json!({ "source_event": "ReadableNameObserved", "state_derived": true,
                "registrar_surface_snapshot": true, "expiry": 1_900_000_000_i64 }),
    );
    snapshot.log_index = Some(2);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[snapshot]).await?;

    let overview = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    let feed = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ALPHA_REGISTRY}&include=total_count&page_size=1"),
    )
    .await?;
    assert_eq!(feed["page"]["total_count"], json!(5), "{feed}");
    assert_eq!(
        overview["data"]["counts"]["events"], feed["page"]["total_count"],
        "{overview}"
    );
    database.cleanup().await
}

fn spawn_registry_request(
    database: &TestDatabase,
    uri: String,
) -> tokio::task::JoinHandle<Result<Response, std::convert::Infallible>> {
    let state = database.app_state();
    tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    })
}

async fn reach(control: &crate::v2::collection_snapshot::finish_test_hooks::FinishControl) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(30), control.wait_until_reached())
        .await
        .context("registry request never reached the hook")
}

/// Runs the overview, pausing once its snapshot has passed the publication check and before it
/// reads anything on it, runs `late` (a change published during the read), and returns the
/// response.
async fn registry_with_late_change<F, Fut>(
    database: &TestDatabase,
    uri: &str,
    late: F,
) -> Result<(StatusCode, Value)>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    use crate::v2::collection_snapshot::finish_test_hooks::{Stage, install_at};
    let (_guard, read) = install_at(&database.pool, Stage::Pinned).await?;
    let request = spawn_registry_request(database, uri.to_owned());
    reach(&read).await?;
    late().await?;
    read.resume().await;
    let response = request.await.context("registry request task panicked")??;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

// The block-bounded evidence and the current labels count are read on one snapshot of the
// captured publication: a publication during the read neither refuses the overview nor adds its
// new label to the count.
/// Registry root role changes are `permission` events of the registry: the overview's event
/// count includes them and still equals the feed total, while the history of a name the
/// registry holds does not gain them.
#[tokio::test]
async fn v2_get_registry_event_count_includes_root_role_changes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let name_history = "/v1/names/one.alpha.eth/history?scope=both&include=total_count";
    let before = registry_payload(&database, name_history).await?;
    let root = Uuid::from_u128(0xA300);
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(root, None, "0xregistry66", 66)],
    )
    .await?;
    let root_change = |id: &str, account: u8, log_index: i64| {
        let mut event = registry_event(
            id,
            None,
            "RootPermissionChanged",
            66,
            ALPHA_REGISTRY,
            json!({
                "source_event": "EACRolesChanged",
                "subject": format!("0x{account:040x}"),
                "scope": {"kind": "registry_root", "chain_id": REGISTRY_CHAIN_ID,
                    "registry_address": ALPHA_REGISTRY},
                "upstream_resource": format!("0x{:064x}", 0),
                "role_bitmap": "0x1",
                "effective_powers": ["registrar"],
                "root_resource": true,
            }),
        );
        event.resource_id = Some(root);
        event.log_index = Some(log_index);
        event.derivation_kind = "ens_v2_permissions".to_owned();
        event
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            root_change("alpha-root-grant-1", 1, 3),
            root_change("alpha-root-grant-2", 2, 4),
        ],
    )
    .await?;

    let overview = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    let feed = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ALPHA_REGISTRY}&include=total_count&page_size=1"),
    )
    .await?;
    assert_eq!(feed["page"]["total_count"], json!(7), "{feed}");
    assert_eq!(
        overview["data"]["counts"]["events"], feed["page"]["total_count"],
        "{overview}"
    );
    let root_feed = registry_payload(
        &database,
        &format!(
            "/v1/events?contract_address={ALPHA_REGISTRY}&kind=RootPermissionChanged&include=total_count"
        ),
    )
    .await?;
    assert_eq!(root_feed["page"]["total_count"], json!(2), "{root_feed}");
    assert!(
        root_feed["data"]
            .as_array()
            .is_some_and(|rows| rows.iter().all(|row| row["type"] == "permission")),
        "{root_feed}"
    );

    let after = registry_payload(&database, name_history).await?;
    assert_eq!(after["data"], before["data"], "{after}");
    assert_eq!(after["page"]["total_count"], before["page"]["total_count"]);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_registry_counts_labels_at_the_captured_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let uri = format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts");
    let selected = registry_payload(&database, &uri).await?;

    let pool = database.pool.clone();
    let (status, payload) =
        registry_with_late_change(&database, &uri, || commit_family_block(&pool)).await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(payload, selected);
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    seed_unpublished_alpha_label(&database).await?;
    let pool = database.pool.clone();
    let (status, payload) = registry_with_late_change(&database, &uri, || async move {
        publish_test_families_on(&pool, REGISTRY_CHAIN_ID, 84).await
    })
    .await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(payload["data"]["counts"], selected["data"]["counts"]);
    assert_eq!(payload["meta"], selected["meta"]);
    assert_eq!(payload["data"]["name"], selected["data"]["name"]);
    assert_eq!(payload["data"]["referenced_by"], selected["data"]["referenced_by"]);
    database.cleanup().await
}

// A later publication rebinds the alpha registry from alpha.eth to beta.eth, keeping its two
// labels. A rebind during the read leaves the count on alpha.eth; the next read follows it.
#[tokio::test]
async fn v2_get_registry_labels_count_follows_a_rebind_on_the_next_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let uri = format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts");
    let selected = registry_payload(&database, &uri).await?;
    seed_unpublished_alpha_rebind(&database, true).await?;
    let pool = database.pool.clone();
    let (status, payload) = registry_with_late_change(&database, &uri, || async move {
        publish_test_families_on(&pool, REGISTRY_CHAIN_ID, 84).await
    })
    .await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(payload["data"]["counts"]["labels"], json!(2), "{payload:#}");
    assert_eq!(payload["meta"], selected["meta"]);
    assert_eq!(payload["data"]["name"]["name"], json!("alpha.eth"));

    seed_schema_v2_ens_lookup_head(&database.pool, 84, "0xregistry84", "2023-11-14T22:14:44Z")
        .await?;
    let rebound = registry_payload(&database, &uri).await?;
    assert_eq!(rebound["data"]["name"]["name"], json!("beta.eth"), "{rebound:#}");
    assert_eq!(rebound["data"]["counts"]["labels"], json!(2), "{rebound:#}");
    database.cleanup().await
}

// A rebind that leaves alpha.eth's pointer in place: alpha.eth stays the registry's
// representative pointing name (the earliest current pointer), so `name` and `counts.labels`
// both describe alpha.eth, whose labels were released, while `referenced_by` lists both names.
#[tokio::test]
async fn v2_get_registry_counts_labels_under_its_representative_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    seed_unpublished_alpha_rebind(&database, false).await?;
    publish_test_families_on(&database.pool, REGISTRY_CHAIN_ID, 84).await?;
    seed_schema_v2_ens_lookup_head(&database.pool, 84, "0xregistry84", "2023-11-14T22:14:44Z")
        .await?;
    let overview = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(overview["data"]["name"]["name"], json!("alpha.eth"), "{overview:#}");
    let referenced: Vec<&Value> = overview["data"]["referenced_by"]["data"]
        .as_array()
        .expect("referenced_by data")
        .iter()
        .map(|name| &name["name"])
        .collect();
    assert_eq!(referenced, [&json!("alpha.eth"), &json!("beta.eth")]);
    assert_eq!(overview["data"]["counts"]["labels"], json!(0), "{overview:#}");
    let labels = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}/labels"),
    )
    .await?;
    assert_eq!(labels["page"]["total_count"], overview["data"]["counts"]["labels"]);
    database.cleanup().await
}

// The count has no namespace filter: an event the same address emitted into another namespace
// on this chain counts, while the bare feed reads only `ens`.
#[tokio::test]
async fn v2_get_registry_event_count_includes_every_namespace_the_feed_default_does_not() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let mut other = registry_event(
        "registry-alpha-other-namespace",
        None,
        "ResolverChanged",
        64,
        ALPHA_REGISTRY,
        json!({ "node": "node-other", "resolver": V2_ADDRESS }),
    );
    other.namespace = "basenames".to_owned();
    other.log_index = Some(3);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[other]).await?;
    let overview = registry_payload(
        &database,
        &format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    let feed = registry_payload(
        &database,
        &format!("/v1/events?contract_address={ALPHA_REGISTRY}&include=total_count&page_size=1"),
    )
    .await?;
    assert_eq!(feed["page"]["total_count"], json!(5), "{feed}");
    assert_eq!(overview["data"]["counts"]["events"], json!(6), "{overview}");
    database.cleanup().await
}

// A marker that becomes unservable during the read does not reach the count, which reads the
// captured publication on the overview's snapshot.
#[tokio::test]
async fn v2_get_registry_labels_count_ignores_a_marker_change_during_the_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_fixture(&database).await?;
    let uri = format!("/v1/registries/1/{ALPHA_REGISTRY}?include=counts");
    let selected = registry_payload(&database, &uri).await?;
    let pool = database.pool.clone();
    let (status, payload) = registry_with_late_change(&database, &uri, || async move {
        sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
            .execute(&pool)
            .await?;
        Ok(())
    })
    .await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(payload, selected);
    database.cleanup().await
}

/// At block 84, which no publication covers: the root registry points beta.eth at the alpha
/// registry, which releases one.alpha.eth and two.alpha.eth and registers one.beta.eth and
/// two.beta.eth. With `clear_alpha` the root also clears alpha.eth's pointer; without it
/// alpha.eth keeps pointing at the registry.
async fn seed_unpublished_alpha_rebind(database: &TestDatabase, clear_alpha: bool) -> Result<()> {
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(REGISTRY_CHAIN_ID, "0xregistry84", None, 84, 1_700_000_084)],
    )
    .await?;
    for (name, base, arm) in [
        ("beta.eth", 0xB000, "ens_v1"),
        ("one.beta.eth", 0xB100, "ens_v2"),
        ("two.beta.eth", 0xB110, "ens_v2"),
    ] {
        seed_family_identity_inputs(
            &database.pool,
            "ens",
            name,
            REGISTRY_CHAIN_ID,
            84,
            "0xregistry84",
            Uuid::from_u128(base),
            Uuid::from_u128(base + 1),
            Uuid::from_u128(base + 2),
            arm,
        )
        .await?;
    }
    let beta = registry_logical_name_id("beta.eth");
    let mut beta_grant = family_event(
        "registry-beta-grant",
        Some(&beta),
        Some(Uuid::from_u128(0xB000)),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        84,
        0,
        json!({"authority_kind":"registrar", "registrant":V2_ADDRESS, "expiry":1_900_000_000_i64}),
    );
    beta_grant.block_hash = Some("0xregistry84".into());
    let mut events = vec![beta_grant];
    let mut alpha_cleared = registry_event(
        "registry-alpha-pointer-cleared",
        Some(&registry_logical_name_id("alpha.eth")),
        "SubregistryChanged",
        84,
        ROOT_REGISTRY,
        json!({ "source_event": "SubregistryUpdated",
                "subregistry": "0x0000000000000000000000000000000000000000" }),
    );
    alpha_cleared.log_index = Some(1);
    let mut beta_pointer = registry_event(
        "registry-beta-pointer",
        Some(&beta),
        "SubregistryChanged",
        84,
        ROOT_REGISTRY,
        json!({ "source_event": "SubregistryUpdated", "subregistry": ALPHA_REGISTRY }),
    );
    beta_pointer.log_index = Some(2);
    if clear_alpha {
        events.push(alpha_cleared);
    }
    events.push(beta_pointer);
    for (index, (name, resource)) in [("one.alpha.eth", 0xA100), ("two.alpha.eth", 0xA110)]
        .into_iter()
        .enumerate()
    {
        let mut release = registry_event(
            &format!("registry-{name}-released"),
            Some(&registry_logical_name_id(name)),
            "RegistrationReleased",
            84,
            ALPHA_REGISTRY,
            json!({ "source_event": "ParentUpdated",
                    "terminal_reason": "registry_name_binding_changed", "status": "released",
                    "registry_contract_instance_id": Uuid::from_u128(0xA190) }),
        );
        release.resource_id = Some(Uuid::from_u128(resource));
        release.log_index = Some(20 + i64::try_from(index)?);
        events.push(release);
    }
    for (index, (name, resource)) in [("one.beta.eth", 0xB100), ("two.beta.eth", 0xB110)]
        .into_iter()
        .enumerate()
    {
        let log = 3 + 2 * i64::try_from(index)?;
        let mut grant = registry_event(
            &format!("registry-{name}-registered"),
            Some(&registry_logical_name_id(name)),
            "RegistrationGranted",
            84,
            ALPHA_REGISTRY,
            json!({ "source_event": "LabelRegistered",
                    "registry_contract_instance_id": Uuid::from_u128(0xA190),
                    "status": "registered", "registrant": V2_ADDRESS,
                    "expiry": 1_900_000_000_i64, "authority_kind": "ens_v2_registry" }),
        );
        grant.resource_id = Some(Uuid::from_u128(resource));
        grant.log_index = Some(log + 1);
        let mut transfer = grant.clone();
        transfer.event_identity.push_str("-transfer");
        transfer.event_kind = "TokenControlTransferred".into();
        transfer.log_index = Some(log);
        transfer.after_state = json!({"source_event": "Transfer", "to": V2_ADDRESS});
        events.extend([grant, transfer]);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(())
}

/// `three.alpha.eth`, registered by the alpha registry at block 84, which no publication covers.
async fn seed_unpublished_alpha_label(database: &TestDatabase) -> Result<()> {
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(REGISTRY_CHAIN_ID, "0xregistry84", None, 84, 1_700_000_084)],
    )
    .await?;
    seed_family_identity_inputs(
        &database.pool,
        "ens",
        "three.alpha.eth",
        REGISTRY_CHAIN_ID,
        84,
        "0xregistry84",
        Uuid::from_u128(0xA130),
        Uuid::from_u128(0xA131),
        Uuid::from_u128(0xA132),
        "ens_v2",
    )
    .await?;
    insert_family_label_preimage(&database.pool, b"three").await?;
    let three = registry_logical_name_id("three.alpha.eth");
    let mut grant = registry_event(
        "registry-three-registered",
        Some(&three),
        "RegistrationGranted",
        84,
        ALPHA_REGISTRY,
        json!({ "source_event": "LabelRegistered", "registry_contract_instance_id": Uuid::from_u128(0xA190),
                "status": "registered", "registrant": V2_ADDRESS, "expiry": 1_900_000_000_i64,
                "authority_kind": "ens_v2_registry" }),
    );
    grant.resource_id = Some(Uuid::from_u128(0xA130));
    grant.log_index = Some(1);
    let mut transfer = grant.clone();
    transfer.event_identity.push_str("-transfer");
    transfer.event_kind = "TokenControlTransferred".into();
    transfer.log_index = Some(0);
    transfer.after_state = json!({"source_event": "Transfer", "to": V2_ADDRESS});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, transfer]).await?;
    Ok(())
}
