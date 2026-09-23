#[tokio::test]
async fn v2_address_history_rejects_resolves_to_relation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let response = v2_history_response_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?relation=resolves_to&page_size=20",
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert!(
        payload["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("resolves_to")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_history_returns_lean_product_rows_newest_first() -> Result<()> {
    let (database, payload) = v2_history_payload("/v1/names/History.eth/history?page_size=20").await?;

    assert_eq!(payload["page"]["page_size"], json!(20));
    assert_eq!(payload["page"]["total_count"], json!(10));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert!(payload["meta"]["as_of"].is_object());

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
            .any(|row| row["type"] == json!("record")
                && row.get("registration_id") == Some(&Value::Null)),
        "surface-only rows must return a null registration_id"
    );
    assert!(
        data.iter().any(|row| {
            row["block_number"] == json!(105)
                && row["transaction_hash"] == json!("0xtx105")
                && row["type"] == json!("authority")
                && row.get("registration_id") == Some(&Value::Null)
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

// `id` is the one handle a consumer merging feeds can de-duplicate on: opaque, unique per
// row, and the same for the same event on every history route and page.
#[tokio::test]
async fn v2_history_rows_carry_one_opaque_identity_across_routes_and_pages() -> Result<()> {
    let (database, history) =
        v2_history_payload("/v1/names/History.eth/history?page_size=20").await?;
    let history_rows = history["data"].as_array().expect("history rows");
    let ids = history_rows
        .iter()
        .map(|row| row["id"].as_str().expect("id is a string").to_owned())
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 10);
    assert!(ids.iter().all(|id| id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit())));
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        ids.len(),
        "ids must be distinct per row"
    );
    assert!(ids.iter().all(|id| !id.contains(':')), "ids must not leak the storage identity");

    let events =
        v2_history_payload_for_database(&database, "/v1/events?name=history.eth&page_size=20")
            .await?;
    let event_ids = events["data"]
        .as_array()
        .expect("event rows")
        .iter()
        .map(|row| row["id"].as_str().expect("id is a string").to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    for (row, id) in history_rows.iter().zip(&ids) {
        assert!(
            event_ids.contains(id),
            "history row {} at block {} must carry the same id on /v1/events",
            row["type"],
            row["block_number"]
        );
    }

    let first = v2_history_payload_for_database(
        &database,
        "/v1/names/History.eth/history?page_size=4",
    )
    .await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("second page");
    let second = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/History.eth/history?page_size=4&cursor={cursor}"),
    )
    .await?;
    let paged = first["data"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["data"].as_array().unwrap())
        .map(|row| row["id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(paged, ids[..8], "ids are stable across pages");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_history_lists_pointer_attributed_record_writes_for_the_registration() -> Result<()> {
    const ADDRESS: &str = "0x0000000000000000000000000000000000007130";
    const RESOLVER: &str = "0x00000000000000000000000000000000000000c2";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:attributed-record.eth";
    let resource_id = Uuid::from_u128(0x7130);
    seed_identity_name(
        &database,
        logical_name_id,
        "attributed-record.eth",
        "attributed-record.eth",
        "node:attributed-record.eth",
        resource_id,
        Uuid::from_u128(0x8130),
        Uuid::from_u128(0x9130),
        ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 130..=132).await?;

    // Resolver writes stay node-keyed at interpretation: no logical name, no resource. The
    // registration's resolver pointer attributes the first one to it; the second write targets
    // another node and stays unattributed.
    let node_write = |event_identity: &str, block_number: i64, name: &str| -> Result<_> {
        let mut event =
            v2_history_event(event_identity, None, None, "RecordChanged", block_number);
        event.source_family = "ens_v1_resolver_l1".to_owned();
        event.raw_fact_ref["emitting_address"] = json!(RESOLVER);
        event.after_state = json!({
            "source_event": "TextChanged",
            "resolver": RESOLVER,
            "node": bigname_lookup::ens_namehash_hex(name)?,
            "record_key": "text:post-migration",
            "record_family": "text",
            "selector_key": "post-migration",
            "value_retained": true,
            "value": "current",
        });
        Ok(event)
    };
    let real_logical_name_id =
        bigname_storage::logical_name_id_for_name("ens", "attributed-record.eth");
    let mut pointer = v2_history_event(
        "attributed-record-pointer",
        Some(&real_logical_name_id),
        Some(resource_id),
        "ResolverChanged",
        130,
    );
    pointer.source_family = "ens_v1_registry_l1".to_owned();
    pointer.after_state = json!({
        "node": bigname_lookup::ens_namehash_hex("attributed-record.eth")?,
        "resolver": RESOLVER,
    });
    pointer.log_index = Some(1);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            // The grant makes the resource a registration, which the events feed filter needs.
            v2_history_event(
                "attributed-record-grant",
                Some(logical_name_id),
                Some(resource_id),
                "RegistrationGranted",
                130,
            ),
            pointer,
            node_write("attributed-record-write", 131, "attributed-record.eth")?,
            node_write("unattributed-record-write", 132, "other.eth")?,
        ],
    )
    .await?;

    for (scope, listed) in [("both", true), ("registration", true), ("name", false)] {
        let payload = v2_history_payload_for_database(
            &database,
            &format!("/v1/names/attributed-record.eth/history?scope={scope}&page_size=20"),
        )
        .await?;
        let rows = payload["data"].as_array().expect("history data");
        let attributed = rows.iter().find(|row| row["transaction_hash"] == json!("0xtx131"));
        assert_eq!(attributed.is_some(), listed, "scope={scope}: {rows:?}");
        if let Some(row) = attributed {
            assert_eq!(row["type"], json!("record"), "scope={scope}");
            assert_eq!(row["block_number"], json!(131), "scope={scope}");
            assert_eq!(row["registration_id"], Value::Null, "scope={scope}");
        }
        assert!(
            !rows.iter().any(|row| row["transaction_hash"] == json!("0xtx132")),
            "scope={scope}: an unattributed node write must not appear: {rows:?}"
        );
    }

    // The registration filter on the events feed lists the same attributed write.
    let by_registration = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={resource_id}&page_size=20"),
    )
    .await?;
    let rows = by_registration["data"].as_array().expect("event data");
    assert!(
        rows.iter().any(|row| row["transaction_hash"] == json!("0xtx131")),
        "registration-filtered events dropped the attributed write: {rows:?}"
    );
    assert!(
        !rows.iter().any(|row| row["transaction_hash"] == json!("0xtx132")),
        "registration-filtered events listed an unattributed node write: {rows:?}"
    );

    // A keyset cursor issued on the attributed row must validate and continue.
    let first = v2_history_payload_for_database(
        &database,
        "/v1/names/attributed-record.eth/history?scope=registration&page_size=1",
    )
    .await?;
    assert_eq!(first["data"][0]["transaction_hash"], json!("0xtx131"));
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .map(str::to_owned);
    if let Some(cursor) = cursor {
        let next = v2_history_payload_for_database(
            &database,
            &format!(
                "/v1/names/attributed-record.eth/history?scope=registration&page_size=1&cursor={cursor}"
            ),
        )
        .await?;
        assert!(
            !next["data"]
                .as_array()
                .expect("history data")
                .iter()
                .any(|row| row["transaction_hash"] == json!("0xtx131")),
            "the attributed row must not repeat after its cursor: {next}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_product_history_deduplicates_resolver_control_resource_linkage() -> Result<()> {
    const ADDRESS: &str = "0x0000000000000000000000000000000000007120";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:resolver-history.eth";
    seed_identity_name(
        &database,
        logical_name_id,
        "resolver-history.eth",
        "resolver-history.eth",
        "node:resolver-history.eth",
        Uuid::from_u128(0x7120),
        Uuid::from_u128(0x8120),
        Uuid::from_u128(0x9120),
        ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=121).await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            Uuid::from_u128(0x7121),
            None,
            "0xresolver-history-resource",
            81,
        )],
    )
    .await?;

    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            v2_history_event(
                "ens_v1_unwrapped_authority:2:ethereum-mainnet:block:tx:0:ResolverChanged:0",
                Some(logical_name_id),
                Some(Uuid::from_u128(0x7120)),
                "ResolverChanged",
                121,
            ),
            v2_history_event(
                "ens_v1_unwrapped_authority:2:ethereum-mainnet:block:tx:0:ResolverChanged:registry-read:0x00000000000000000000000000000000000000aa",
                Some(logical_name_id),
                Some(Uuid::from_u128(0x7121)),
                "ResolverChanged",
                121,
            ),
        ],
    )
    .await?;

    for route in [
        "/v1/names/resolver-history.eth/history?scope=both&page_size=20",
        "/v1/names/resolver-history.eth/history?scope=registration&page_size=20",
        "/v1/events?name=resolver-history.eth&page_size=20",
        "/v1/events?registration_id=00000000-0000-0000-0000-000000007120&page_size=20",
        "/v1/addresses/0x0000000000000000000000000000000000007120/history?relation=manager&page_size=20",
    ] {
        let payload = v2_history_payload_for_database(&database, route).await?;
        let rows = payload["data"].as_array().expect("history data");
        assert_eq!(rows.len(), 1, "{route}: {rows:?}");
        assert_eq!(rows[0]["type"], json!("resolver"), "{route}");
        assert_eq!(
            rows[0]["registration_id"],
            json!(Uuid::from_u128(0x7120).to_string()),
            "{route}"
        );
    }

    let diagnostics = v2_history_payload_for_database(
        &database,
        "/v1/diagnostics/events?name=resolver-history.eth&page_size=20",
    )
    .await?;
    let diagnostic_rows = diagnostics["data"].as_array().expect("diagnostic events");
    assert_eq!(diagnostic_rows.len(), 2, "{diagnostic_rows:?}");
    assert!(diagnostic_rows.iter().any(|row| {
        row["event_identity"]
            .as_str()
            .is_some_and(|identity| identity.contains(":ResolverChanged:registry-read:"))
    }));

    let diagnostic_first = v2_history_payload_for_database(
        &database,
        "/v1/diagnostics/events?name=resolver-history.eth&page_size=1",
    )
    .await?;
    let diagnostic_cursor = diagnostic_first["page"]["next_cursor"]
        .as_str()
        .expect("diagnostic cursor after the control-resource row");
    assert!(diagnostic_first["data"][0]["event_identity"]
        .as_str()
        .is_some_and(|identity| identity.contains(":ResolverChanged:registry-read:")));
    let diagnostic_second = v2_history_payload_for_database(
        &database,
        &format!(
            "/v1/diagnostics/events?name=resolver-history.eth&page_size=1&cursor={diagnostic_cursor}"
        ),
    )
    .await?;
    assert_eq!(diagnostic_second["data"].as_array().map(Vec::len), Some(1));
    assert!(!diagnostic_second["data"][0]["event_identity"]
        .as_str()
        .is_some_and(|identity| identity.contains(":ResolverChanged:registry-read:")));

    let product_event_kinds = crate::v2::product_history_event_kinds();
    let product_page = bigname_storage::load_name_history_page(
        &database.pool,
        logical_name_id,
        &[Uuid::from_u128(0x7120), Uuid::from_u128(0x7121)],
        bigname_storage::HistoryScope::Both,
        true,
        None,
        20,
        bigname_storage::HistorySummaryMode::Count,
        &bigname_storage::HistoryPageOptions {
            event_kinds: product_event_kinds,
            ..bigname_storage::HistoryPageOptions::default()
        },
        None,
    )
    .await?;
    assert_eq!(product_page.rows.len(), 1);
    assert_eq!(
        product_page.summary.map(|summary| summary.total_count),
        Some(1)
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_tokenized_registry_history_keeps_identity_outside_the_binding_window() -> Result<()> {
    const ADDRESS: &str = "0x0000000000000000000000000000000000007122";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:event-position-history.eth";
    let resource_id = Uuid::from_u128(0x7122);
    seed_identity_name(
        &database,
        logical_name_id,
        "event-position-history.eth",
        "event-position-history.eth",
        "node:event-position-history.eth",
        resource_id,
        Uuid::from_u128(0x8122),
        Uuid::from_u128(0x9122),
        ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=121).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000121) + interval '1 microsecond',
             active_to = to_timestamp(1700000121) + interval '3 microseconds'
         WHERE surface_binding_id = $1",
    )
    .bind(Uuid::from_u128(0x9122))
    .execute(&database.pool)
    .await?;

    let mut before = v2_history_event(
        "registry-resolver-before-binding",
        Some(logical_name_id),
        Some(resource_id),
        "ResolverChanged",
        121,
    );
    before.source_family = "ens_v1_registry_l1".to_owned();
    before.log_index = Some(0);
    let mut during = v2_history_event(
        "registry-resolver-during-binding",
        Some(logical_name_id),
        Some(resource_id),
        "ResolverChanged",
        121,
    );
    during.source_family = "ens_v1_registry_l1".to_owned();
    during.log_index = Some(2);
    let mut after = v2_history_event(
        "registry-resolver-after-binding",
        Some(logical_name_id),
        Some(resource_id),
        "ResolverChanged",
        121,
    );
    after.source_family = "ens_v1_registry_l1".to_owned();
    after.log_index = Some(4);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[before, during, after],
    )
    .await?;

    let payload = v2_history_payload_for_database(
        &database,
        "/v1/events?name=event-position-history.eth&page_size=20",
    )
    .await?;
    let rows = payload["data"].as_array().expect("product history rows");
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(rows[0]["log_index"], json!(4));
    assert_eq!(rows[0]["registration_id"], json!(resource_id.to_string()));
    assert_eq!(rows[1]["log_index"], json!(2));
    assert_eq!(rows[1]["registration_id"], json!(resource_id.to_string()));
    assert_eq!(rows[2]["log_index"], json!(0));
    assert_eq!(rows[2]["registration_id"], json!(resource_id.to_string()));

    database.cleanup().await
}

#[tokio::test]
async fn v2_ownerless_registry_history_omits_registration_identity() -> Result<()> {
    const ADDRESS: &str = "0x0000000000000000000000000000000000007130";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:ownerless-history.eth";
    let control_resource_id = Uuid::from_u128(0x7130);
    let read_resource_id = Uuid::from_u128(0x7131);
    seed_identity_name(
        &database,
        logical_name_id,
        "ownerless-history.eth",
        "ownerless-history.eth",
        "node:ownerless-history.eth",
        control_resource_id,
        Uuid::from_u128(0x8130),
        Uuid::from_u128(0x9130),
        ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            read_resource_id,
            None,
            "0xownerless-history-resource",
            81,
        )],
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET serving_resource_id = $2,
             surface_binding_id = NULL,
             resource_id = NULL,
             token_lineage_id = NULL,
             binding_kind = NULL,
             declared_summary = jsonb_build_object(
                 'registration', jsonb_build_object('status', 'unregistered'),
                 'control', jsonb_build_object('status', 'unregistered')
             )
         WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .bind(read_resource_id)
    .execute(&database.pool)
    .await?;
    seed_v2_history_blocks(&database, 121..=122).await?;
    let mut authority = v2_history_event(
        "ownerless-registry-authority",
        Some(logical_name_id),
        Some(read_resource_id),
        "AuthorityTransferred",
        121,
    );
    authority.source_family = "ens_v1_registry_l1".to_owned();
    authority.after_state = json!({
        "node": "node:ownerless-history.eth",
        "owner": "0x0000000000000000000000000000000000000000",
        "owner_getter": "0x0000000000000000000000000000000000000000",
        "owner_getter_reason": "literal_zero",
        "authority_kind": null
    });
    let mut epoch = v2_history_event(
        "ownerless-registry-authority-epoch",
        Some(logical_name_id),
        Some(read_resource_id),
        "AuthorityEpochChanged",
        121,
    );
    epoch.source_family = "ens_v1_registry_l1".to_owned();
    epoch.after_state = json!({
        "node": "node:ownerless-history.eth",
        "owner": "0x0000000000000000000000000000000000000000",
        "owner_getter": "0x0000000000000000000000000000000000000000",
        "owner_getter_reason": "literal_zero",
        "authority_kind": null
    });
    let mut resolver = v2_history_event(
        "ownerless-registry-resolver",
        Some(logical_name_id),
        Some(read_resource_id),
        "ResolverChanged",
        122,
    );
    resolver.source_family = "ens_v1_registry_l1".to_owned();
    resolver.after_state = json!({
        "node": "node:ownerless-history.eth",
        "resolver": "0x00000000000000000000000000000000000000aa"
    });
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[authority, epoch, resolver],
    )
    .await?;

    for route in [
        "/v1/names/ownerless-history.eth/history?scope=both&page_size=20",
        "/v1/events?name=ownerless-history.eth&page_size=20",
    ] {
        let payload = v2_history_payload_for_database(&database, route).await?;
        let rows = payload["data"].as_array().expect("product history rows");
        assert_eq!(rows.len(), 3, "{route}: {rows:?}");
        assert_eq!(
            rows.iter()
                .filter(|row| row["type"] == json!("authority"))
                .count(),
            2,
            "{route}: {rows:?}"
        );
        assert!(
            rows.iter()
                .all(|row| row.get("registration_id") == Some(&Value::Null)),
            "{route} exposed the read-only registry resource as a registration: {rows:?}"
        );
    }

    let filtered = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={read_resource_id}&page_size=20"),
    )
    .await?;
    assert_eq!(filtered["data"], json!([]));

    let diagnostics = v2_history_payload_for_database(
        &database,
        "/v1/diagnostics/events?name=ownerless-history.eth&page_size=20",
    )
    .await?;
    let diagnostic_rows = diagnostics["data"].as_array().expect("diagnostic rows");
    assert_eq!(diagnostic_rows.len(), 3);
    assert!(diagnostic_rows.iter().all(|row| {
        row["registration_id"] == json!(read_resource_id.to_string())
    }));

    database.cleanup().await
}

#[tokio::test]
async fn v2_registration_filter_keeps_bound_name_surface_history() -> Result<()> {
    const ADDRESS: &str = "0x0000000000000000000000000000000000007140";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:registration-filter-history.eth";
    let resource_id = Uuid::from_u128(0x7140);
    let later_resource_id = Uuid::from_u128(0x7141);
    seed_identity_name(
        &database,
        logical_name_id,
        "registration-filter-history.eth",
        "registration-filter-history.eth",
        "node:registration-filter-history.eth",
        resource_id,
        Uuid::from_u128(0x8140),
        Uuid::from_u128(0x9140),
        ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            later_resource_id,
            None,
            "0xregistration-filter-later-resource",
            81,
        )],
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=125).await?;
    // The old registration ends exactly where the next binding begins.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000121), active_to = to_timestamp(1700000123)
         WHERE resource_id = $1",
    )
    .bind(resource_id)
    .execute(&database.pool)
    .await?;
    let later_binding = address_name_surface_binding(
        Uuid::from_u128(0x9141),
        logical_name_id,
        later_resource_id,
        "0xhistory123",
        123,
        1_700_000_123,
    );
    upsert_test_surface_bindings(&database.pool, &[later_binding]).await?;
    let mut unmapped_surface_event = v2_history_event(
        "registration-filter-unmapped-surface",
        Some(logical_name_id),
        None,
        "RegistrarNameRegistered",
        124,
    );
    unmapped_surface_event.source_family = "ens_v2_registrar_l1".to_owned();
    let mut events = vec![
        v2_history_event(
            "registration-filter-grant",
            Some(logical_name_id),
            Some(resource_id),
            "RegistrationGranted",
            121,
        ),
        v2_history_event(
            "registration-filter-record",
            Some(logical_name_id),
            None,
            "RecordChanged",
            122,
        ),
        v2_history_event(
            "registration-filter-later-grant",
            Some(logical_name_id),
            Some(later_resource_id),
            "RegistrationGranted",
            123,
        ),
        v2_history_event(
            "registration-filter-before-record",
            Some(logical_name_id),
            None,
            "RecordChanged",
            120,
        ),
        v2_history_event(
            "registration-filter-next-record",
            Some(logical_name_id),
            None,
            "RecordChanged",
            123,
        ),
        v2_history_event(
            "registration-filter-latest-record",
            Some(logical_name_id),
            None,
            "RecordChanged",
            125,
        ),
        unmapped_surface_event,
    ];
    for event in &mut events {
        if event.event_kind == "RecordChanged" {
            event.source_family = "ens_v1_resolver_l1".to_owned();
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={resource_id}&page_size=20"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("product event rows");
    assert_eq!(history_types(rows), vec!["record", "registration"]);
    assert_eq!(rows[0]["registration_id"], Value::Null);
    assert_eq!(rows[1]["registration_id"], json!(resource_id.to_string()));

    let first_page = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={resource_id}&page_size=1"),
    )
    .await?;
    assert_eq!(
        history_types(first_page["data"].as_array().unwrap()),
        vec!["record"]
    );
    assert_eq!(first_page["page"]["has_more"], json!(true));

    let cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("old registration cursor");
    let second_page = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={resource_id}&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(
        history_types(second_page["data"].as_array().unwrap()),
        vec!["registration"]
    );
    assert_eq!(second_page["page"]["has_more"], json!(false));
    assert_eq!(second_page["page"]["next_cursor"], Value::Null);

    let newer_page = bigname_storage::load_event_history_page(
        &database.pool,
        bigname_storage::EventHistoryFilter {
            resource_id: Some(later_resource_id),
            ..bigname_storage::EventHistoryFilter::default()
        },
        true,
        None,
        1,
        bigname_storage::HistorySummaryMode::Full,
        false,
    )
    .await?;
    assert_eq!(newer_page.rows.len(), 1);
    assert_eq!(
        newer_page.rows[0].event_identity,
        "registration-filter-latest-record"
    );
    assert_eq!(newer_page.summary.as_ref().unwrap().total_count, 3);
    let foreign_anchor = newer_page
        .next_cursor
        .as_ref()
        .expect("newer registration cursor");
    let invalid_cursor = bigname_storage::load_event_history_page(
        &database.pool,
        bigname_storage::EventHistoryFilter {
            resource_id: Some(resource_id),
            ..bigname_storage::EventHistoryFilter::default()
        },
        true,
        Some(foreign_anchor),
        1,
        bigname_storage::HistorySummaryMode::None,
        false,
    )
    .await
    .expect_err("a newer name event cannot anchor the old registration");
    assert!(
        invalid_cursor
            .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
            .is_some()
    );

    let storage_page = bigname_storage::load_event_history_page(
        &database.pool,
        bigname_storage::EventHistoryFilter {
            resource_id: Some(resource_id),
            ..bigname_storage::EventHistoryFilter::default()
        },
        true,
        None,
        20,
        bigname_storage::HistorySummaryMode::Count,
        false,
    )
    .await?;
    assert_eq!(storage_page.rows.len(), 2);
    assert_eq!(
        storage_page.summary.map(|summary| summary.total_count),
        Some(2)
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_product_event_routes_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";
    const ADDRESS: &str = "0x0000000000000000000000000000000000034930";

    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:ᏣᎳᎩ.eth",
        NORMALIZED_NAME,
        NORMALIZED_NAME,
        "namehash:ᏣᎳᎩ.eth",
        Uuid::from_u128(0x349_3001),
        Uuid::from_u128(0x349_3002),
        Uuid::from_u128(0x349_3003),
        ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        43,
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=121).await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[v2_history_event(
            "cherokee-record",
            Some("ens:ᏣᎳᎩ.eth"),
            None,
            "RecordChanged",
            121,
        )],
    )
    .await?;
    let stored_raw_name: String = sqlx::query_scalar(
        "SELECT raw_name FROM bigname_phase.name_current WHERE raw_name = $1",
    )
    .bind(NORMALIZED_NAME)
    .fetch_one(&database.pool)
    .await?;

    for uri in [
        "/v1/events?name=%E1%8F%A3%E1%8E%B3%E1%8E%A9.eth&page_size=20".to_owned(),
        format!("/v1/addresses/{ADDRESS}/history?page_size=20"),
    ] {
        let payload = v2_history_payload_for_database(&database, &uri).await?;
        assert_eq!(payload["data"][0]["name"], json!(stored_raw_name), "{uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_history_paginates_with_anchor_bound_cursor() -> Result<()> {
    let (database, first_page) =
        v2_history_payload("/v1/names/history.eth/history?page_size=3").await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();
    assert_eq!(first_page["page"]["has_more"], json!(true));

    let second_page = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/history.eth/history?page_size=3&cursor={next_cursor}"),
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
    assert_eq!(first_hashes, vec!["0xtx110", "0xtx109", "0xtx108"]);
    assert_eq!(second_hashes, vec!["0xtx107", "0xtx106", "0xtx105"]);

    let replay = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/history.eth/history?page_size=3&cursor={next_cursor}"),
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
            "/v1/names/history.eth/history?page_size=1".to_owned(),
            false,
        ),
        ("/v1/events?name=history.eth&page_size=1".to_owned(), false),
        (
            format!("/v1/addresses/{ADDRESS}/history?page_size=1"),
            false,
        ),
        (
            "/v1/diagnostics/events?name=history.eth&page_size=1".to_owned(),
            true,
        ),
    ];

    let mut before = Vec::new();
    for (route, diagnostic) in &routes {
        let first = v2_history_payload_for_database(&database, route).await?;
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
        "/v1/names/history.eth/history?page_size=20".to_owned(),
        "/v1/events?name=history.eth&page_size=20".to_owned(),
        format!("/v1/addresses/{ADDRESS}/history?page_size=20"),
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
        "/v1/diagnostics/events?name=history.eth&page_size=20",
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
            "/v1/diagnostics/events?registration_id={}&page_size=20",
            Uuid::from_u128(0x7500)
        ),
    )
    .await?;
    assert!(candidate_only_diagnostics["data"].as_array().is_some_and(|rows| {
        rows.iter().any(|row| row["event_identity"] == "candidate-address-registration")
    }));

    let candidate_address_diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v1/diagnostics/events?address={ADDRESS}&page_size=20"),
    )
    .await?;
    let candidate_address_rows = candidate_address_diagnostics["data"]
        .as_array()
        .expect("candidate address diagnostic rows");
    assert!(candidate_address_rows.iter().any(|row| {
        row["event_identity"] == "candidate-address-registration"
            && row["consumer_visibility"] == "candidate"
    }));
    assert!(candidate_address_rows.iter().any(|row| {
        row["event_identity"] == "candidate-only-ordinary-renewal"
            && row["consumer_visibility"] == "activated"
    }));

    database.cleanup().await
}

#[tokio::test]
async fn diagnostics_hide_an_event_on_an_orphaned_lineage_with_its_still_canonical_association()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    sqlx::query(
        "INSERT INTO migration_event_associations (
             event_identity, migration_correlation_id, correlation_kind, evidence_refs,
             chain_id, block_number, block_hash, transaction_hash, transaction_index,
             log_index, canonicality_state, consumer_visibility, interpreter_content_hash
         ) VALUES (
             'history-renewal', 'orphaned-lineage-correlation', 'synchronized_renewal',
             '[]'::jsonb, 'ethereum-mainnet', 110, '0xhistory110', '0xtx110', 0, 0,
             'canonical', 'candidate', 'keccak256:test'
         )",
    )
    .execute(&database.pool)
    .await?;
    let route = "/v1/diagnostics/events?name=history.eth&page_size=20";

    let before = v2_history_payload_for_database(&database, route).await?;
    let renewal = before["data"]
        .as_array()
        .expect("diagnostic rows")
        .iter()
        .find(|row| row["event_identity"] == "history-renewal")
        .expect("the renewal is served while its lineage is canonical");
    assert_eq!(renewal["migration_associations"], json!([{
        "migration_correlation_ids": ["orphaned-lineage-correlation"],
        "correlation_kind": "synchronized_renewal",
        "consumer_visibility": "candidate",
    }]));

    // Head publication orphans chain_lineage only; the event row and its association keep
    // their `canonical` stamps until Interpret's redo deletes the event.
    sqlx::query(
        "UPDATE bigname_phase.chain_lineage
         SET canonicality_state = 'orphaned'::bigname_phase.canonicality_state
         WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xhistory110'",
    )
    .execute(&database.pool)
    .await?;

    let after = v2_history_payload_for_database(&database, route).await?;
    let rows = after["data"].as_array().expect("diagnostic rows");
    assert!(
        rows.iter().all(|row| row["event_identity"] != "history-renewal"),
        "an event on an orphaned lineage must leave diagnostics before Interpret clears it"
    );
    assert!(
        rows.iter().any(|row| row["event_identity"] == "history-expiry"),
        "sibling events on readable blocks stay served"
    );
    let stamps: Vec<(String, String)> = sqlx::query_as(
        "SELECT ne.canonicality_state::text, association.canonicality_state::text
         FROM normalized_events ne
         JOIN migration_event_associations association
           ON association.event_identity = ne.event_identity
         WHERE ne.event_identity = 'history-renewal'",
    )
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(stamps, vec![("canonical".to_owned(), "canonical".to_owned())]);

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
        v2_history_payload_for_database(&database, "/v1/names/history.eth/history?page_size=3")
            .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor");

    let cross_name = v2_history_response_for_database(
        &database,
        &format!("/v1/names/other.eth/history?page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_name.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(cross_name).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    let cross_scope = v2_history_response_for_database(
        &database,
        &format!("/v1/names/history.eth/history?scope=name&page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_scope.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(cross_scope).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_serves_subregistry_changes_as_registration_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    seed_v2_history_blocks(&database, 112..=112).await?;
    let mut event = v2_history_event(
        "history-subregistry",
        None,
        Some(Uuid::from_u128(0x7100)),
        "SubregistryChanged",
        112,
    );
    event.after_state = json!({
        "subregistry": "0x0000000000000000000000000000000000000abd",
    });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event])
        .await
        .context("failed to upsert subregistry history event")?;

    let unfiltered = v2_history_payload_for_database(
        &database,
        "/v1/names/history.eth/history?page_size=20",
    )
    .await?;
    let rows = unfiltered["data"].as_array().expect("data");
    assert_eq!(rows.len(), 11);
    assert_eq!(rows[0]["type"], json!("subregistry"));
    assert_eq!(rows[0]["block_number"], json!(112));
    assert_eq!(
        rows[0]["registration_id"],
        json!(Uuid::from_u128(0x7100).to_string())
    );

    let filtered = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&type=subregistry&page_size=20",
    )
    .await?;
    assert_eq!(
        history_types(filtered["data"].as_array().expect("data")),
        vec!["subregistry"]
    );
    let registration_scope = v2_history_payload_for_database(
        &database,
        "/v1/names/history.eth/history?scope=registration&page_size=20",
    )
    .await?;
    assert_eq!(
        history_types(registration_scope["data"].as_array().expect("data"))[0],
        "subregistry"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_scope_filters_name_registration_and_both() -> Result<()> {
    let (database, name_scope) =
        v2_history_payload("/v1/names/history.eth/history?scope=name&page_size=20").await?;
    let registration_scope = v2_history_payload_for_database(
        &database,
        "/v1/names/history.eth/history?scope=registration&page_size=20",
    )
    .await?;
    let both_scope = v2_history_payload_for_database(
        &database,
        "/v1/names/history.eth/history?scope=both&page_size=20",
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
            "/v1/addresses/{MATCHED_ADDRESS}/history?relation=manager&page_size=20"
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
        "/v1/names/masked-tail.eth/history?scope=name&page_size=20",
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
        "/v1/names/history.eth/history?scope=registration&page_size=20",
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
        v2_history_payload_for_database(&database, "/v1/names/quiet.eth/history").await?;
    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    let response = v2_history_response_for_database(&database, "/v1/names/missing.eth/history")
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_excludes_unpublished_sepolia_events_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;
    seed_v2_mixed_phase_head_history(&database).await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{V2_SEPOLIA_SNAPSHOT_NAME}/history"),
    )
    .await?;
    assert!(payload["meta"]["as_of"].is_object());
    assert_eq!(
        history_types(payload["data"].as_array().expect("history data")),
        Vec::<String>::new()
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
    app_router(database.app_state_with_public_namespaces(&["ens"]))
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

    database.seed_default_ens_primary_name_fallback_context().await?;
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
    .await?;
    database.seed_default_ens_snapshot_selector_position().await
}

async fn seed_v2_history_blocks(
    database: &TestDatabase,
    range: std::ops::RangeInclusive<i64>,
) -> Result<()> {
    let end = *range.end();
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
    let current: Option<i64> = sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = 'ethereum-mainnet'")
        .fetch_optional(&database.pool).await?;
    if !blocks.is_empty() && current.is_none_or(|head| head < end) {
        let timestamp = sqlx::types::time::OffsetDateTime::from_unix_timestamp(1_700_000_000 + end)?;
        seed_schema_v2_ens_lookup_head(&database.pool, end, &format!("0xhistory{end}"),
            &crate::v2::format_timestamp(timestamp)).await?;
    }
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

#[tokio::test]
async fn v2_history_order_asc_returns_oldest_first_with_order_bound_cursor() -> Result<()> {
    let (database, first_page) =
        v2_history_payload("/v1/names/History.eth/history?order=asc&page_size=4").await?;

    assert_eq!(history_blocks(&first_page), vec![101, 102, 103, 104]);
    assert_eq!(first_page["page"]["has_more"], json!(true));
    let cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("nonterminal asc page must provide a cursor")
        .to_owned();

    let second_page = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/history.eth/history?order=asc&page_size=4&cursor={cursor}"),
    )
    .await?;
    assert_eq!(history_blocks(&second_page), vec![105, 106, 107, 108]);
    let cursor = second_page["page"]["next_cursor"]
        .as_str()
        .expect("second asc page must provide a cursor")
        .to_owned();
    let third_page = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/history.eth/history?order=asc&page_size=4&cursor={cursor}"),
    )
    .await?;
    assert_eq!(history_blocks(&third_page), vec![109, 110]);
    assert_eq!(third_page["page"]["has_more"], json!(false));
    assert_eq!(third_page["page"]["next_cursor"], Value::Null);

    // An asc cursor cannot continue a desc (default) request.
    let response = v2_history_response_for_database(
        &database,
        &format!("/v1/names/history.eth/history?page_size=4&cursor={cursor}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let default_page =
        v2_history_payload_for_database(&database, "/v1/names/history.eth/history?page_size=3")
            .await?;
    assert_eq!(history_blocks(&default_page), vec![110, 109, 108]);
    let desc_page = v2_history_payload_for_database(
        &database,
        "/v1/names/history.eth/history?order=desc&page_size=3",
    )
    .await?;
    assert_eq!(history_blocks(&desc_page), vec![110, 109, 108]);

    for route in [
        "/v1/events?name=history.eth&order=asc&page_size=3",
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?order=asc&page_size=3",
    ] {
        let payload = v2_history_payload_for_database(&database, route).await?;
        let blocks = history_blocks(&payload);
        assert!(
            blocks.windows(2).all(|pair| pair[0] < pair[1]),
            "{route} must return oldest-first rows: {blocks:?}"
        );
        assert_eq!(blocks[0], 101, "{route} must start at the oldest row");
    }

    let response =
        v2_history_response_for_database(&database, "/v1/events?name=history.eth&order=sideways")
            .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await
}

#[tokio::test]
async fn v2_history_type_sets_filter_rows_and_bind_cursors() -> Result<()> {
    let (database, payload) = v2_history_payload(
        "/v1/names/history.eth/history?type=registration,renewal,%20renewal&page_size=20",
    )
    .await?;
    let rows = payload["data"].as_array().expect("history data");
    assert_eq!(history_types(rows), vec!["renewal", "registration"]);
    assert_eq!(payload["page"]["total_count"], json!(2));

    let events = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&type=record,resolver,transfer&order=asc&page_size=2",
    )
    .await?;
    assert_eq!(
        history_types(events["data"].as_array().expect("events data")),
        vec!["transfer", "resolver"]
    );
    assert_eq!(events["page"]["has_more"], json!(true));
    let cursor = events["page"]["next_cursor"]
        .as_str()
        .expect("type-set page must provide a cursor")
        .to_owned();
    let continued = v2_history_payload_for_database(
        &database,
        &format!(
            "/v1/events?name=history.eth&type=transfer,resolver,record&order=asc&page_size=2&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(
        history_types(continued["data"].as_array().expect("events data")),
        vec!["record"]
    );
    // The same cursor cannot continue a request with a different type set.
    let response = v2_history_response_for_database(
        &database,
        &format!("/v1/events?name=history.eth&type=record&order=asc&page_size=2&cursor={cursor}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let address = v2_history_payload_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?type=authority,record&page_size=20",
    )
    .await?;
    let types = history_types(address["data"].as_array().expect("address history data"));
    assert!(!types.is_empty());
    assert!(types.iter().all(|kind| *kind == "authority" || *kind == "record"), "{types:?}");

    for route in [
        "/v1/names/history.eth/history?type=registration,bogus",
        "/v1/names/history.eth/history?type=,",
        "/v1/events?name=history.eth&type=registered",
    ] {
        let response = v2_history_response_for_database(&database, route).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{route}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_history_timestamp_window_resolves_blocks_through_lineage() -> Result<()> {
    // Fixture block N has timestamp 1_700_000_000 + N; block 104 is 2023-11-14T22:15:04Z.
    let (database, payload) = v2_history_payload(
        "/v1/names/history.eth/history?from_timestamp=2023-11-14T22:15:04Z&to_timestamp=2023-11-14T22:15:07Z&page_size=20",
    )
    .await?;
    assert_eq!(history_blocks(&payload), vec![107, 106, 105, 104]);
    assert_eq!(payload["page"]["total_count"], json!(4));

    // A bound between two blocks snaps inward: 22:15:04.5 -> block 105, 22:15:06.5 -> block 106.
    let payload = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&from_timestamp=2023-11-14T22:15:04.500Z&to_timestamp=2023-11-14T22:15:06.500Z&order=asc",
    )
    .await?;
    assert_eq!(history_blocks(&payload), vec![105, 106]);

    // Open-ended bounds and numeric-offset timestamps work; block bounds intersect.
    let payload = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&from_timestamp=2023-11-14T23:15:08%2B01:00&to_block=109",
    )
    .await?;
    assert_eq!(history_blocks(&payload), vec![109, 108]);

    // A window after the last known block matches nothing.
    let payload = v2_history_payload_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?from_timestamp=2030-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["total_count"], json!(0));

    // Cursors bind the timestamp window.
    let first = v2_history_payload_for_database(
        &database,
        "/v1/names/history.eth/history?from_timestamp=2023-11-14T22:15:04Z&page_size=2",
    )
    .await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("windowed page must provide a cursor")
        .to_owned();
    let response = v2_history_response_for_database(
        &database,
        &format!("/v1/names/history.eth/history?page_size=2&cursor={cursor}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let continued = v2_history_payload_for_database(
        &database,
        &format!(
            "/v1/names/history.eth/history?from_timestamp=2023-11-14T22:15:04Z&page_size=2&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(history_blocks(&continued), vec![108, 107]);

    for route in [
        "/v1/names/history.eth/history?from_timestamp=yesterday",
        "/v1/names/history.eth/history?from_timestamp=2023-11-14T22:15:07Z&to_timestamp=2023-11-14T22:15:04Z",
        "/v1/events?name=history.eth&to_timestamp=1700000000",
    ] {
        let response = v2_history_response_for_database(&database, route).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{route}");
        assert_eq!(
            read_json::<Value>(response).await?["error"]["code"],
            json!("invalid_input"),
            "{route}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_history_total_count_is_populated_only_for_anchored_requests() -> Result<()> {
    let (database, payload) = v2_history_payload("/v1/names/history.eth/history?page_size=3").await?;
    assert_eq!(payload["page"]["total_count"], json!(10));
    assert_eq!(payload["page"]["has_more"], json!(true));

    let payload =
        v2_history_payload_for_database(&database, "/v1/events?name=history.eth&page_size=3")
            .await?;
    assert_eq!(payload["page"]["total_count"], json!(10));

    let payload = v2_history_payload_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?page_size=3",
    )
    .await?;
    let total = payload["page"]["total_count"]
        .as_u64()
        .expect("address history must count anchored rows");
    assert!(total >= 1);

    let payload =
        v2_history_payload_for_database(&database, "/v1/events?namespace=ens&page_size=3").await?;
    assert_eq!(payload["page"]["total_count"], Value::Null);
    let payload = v2_history_payload_for_database(
        &database,
        "/v1/events?namespace=ens&type=registration&from_block=100&to_block=200",
    )
    .await?;
    assert_eq!(payload["page"]["total_count"], Value::Null);

    database.cleanup().await
}

fn history_blocks(payload: &Value) -> Vec<i64> {
    payload["data"]
        .as_array()
        .expect("history data")
        .iter()
        .map(|row| row["block_number"].as_i64().expect("history row block_number"))
        .collect()
}

#[tokio::test]
async fn v2_history_include_data_adds_friendly_payloads_and_keeps_lean_rows_otherwise(
) -> Result<()> {
    const RESOLVER: &str = "0x0000000000000000000000000000000000000abc";
    let (database, lean) = v2_history_payload("/v1/names/history.eth/history?page_size=20").await?;
    for row in lean["data"].as_array().expect("history data") {
        assert!(row.get("data").is_none());
        assert!(row.get("kind").is_none());
        assert!(row.get("contract_address").is_none());
    }
    // The record write was emitted by the resolver contract; the fixture stores
    // the emitter in mixed case on the raw fact reference.
    sqlx::query(
        "UPDATE bigname_phase.normalized_events \
         SET raw_fact_ref = raw_fact_ref || '{\"emitting_address\":\"0x0000000000000000000000000000000000000ABC\"}'::jsonb \
         WHERE event_identity = 'history-record'",
    )
    .execute(&database.pool)
    .await?;

    let payload = v2_history_payload_for_database(
        &database,
        "/v1/names/history.eth/history?include=data&page_size=20",
    )
    .await?;
    let rows = payload["data"].as_array().expect("history data");
    assert_eq!(rows.len(), 10);
    let row_at = |block: i64| {
        rows.iter()
            .find(|row| row["block_number"] == json!(block))
            .unwrap_or_else(|| panic!("row at block {block}"))
    };
    for row in rows {
        assert!(row.get("kind").is_none(), "{row}");
        assert!(row.get("contract_address").is_some(), "{row}");
        assert!(row["data"].is_object(), "{row}");
        for key in ["type", "name", "namespace", "registration_id", "block_number", "timestamp", "transaction_hash", "log_index"] {
            assert!(row.get(key).is_some(), "{key} missing from {row}");
        }
        assert!(row.get("before_state").is_none());
        assert!(row.get("after_state").is_none());
        assert!(row.get("event_kind").is_none());
    }
    assert_no_banned_v1_spellings(&payload);

    let registration = row_at(102);
    assert!(registration.get("kind").is_none());
    assert_eq!(registration["contract_address"], Value::Null);
    assert_eq!(
        registration["data"],
        json!({
            "registrant": "0x00000000000000000000000000000000000000aa",
            "expires_at": "2030-03-17T17:46:40Z",
        })
    );
    assert_eq!(
        row_at(110)["data"],
        json!({ "expires_at": "2031-10-17T10:40:00Z" })
    );
    assert_eq!(
        row_at(109)["data"],
        json!({ "expires_at": "2031-10-17T10:40:00Z" })
    );
    assert_eq!(row_at(108)["data"], json!({}));
    assert_eq!(
        row_at(103)["data"],
        json!({ "to": "0x00000000000000000000000000000000000000bb" })
    );
    assert_eq!(
        row_at(101)["data"],
        json!({ "owner": "0x00000000000000000000000000000000000000cc" })
    );
    assert_eq!(
        row_at(105)["data"],
        json!({ "owner": "0x00000000000000000000000000000000000000cc" })
    );
    assert_eq!(
        row_at(104)["data"],
        json!({ "resolver": { "chain_id": 1, "address": RESOLVER } })
    );
    let record = row_at(106);
    assert!(record.get("kind").is_none());
    assert_eq!(record["contract_address"], json!(RESOLVER));
    assert_eq!(
        record["data"],
        json!({
            "key": "addr:60",
            "coin_type": 60,
            "value": "0x0000000000000000000000000000000000000def",
        })
    );
    assert_eq!(
        row_at(107)["data"],
        json!({
            "address": "0x00000000000000000000000000000000000000dd",
            "powers": ["registration_control"],
        })
    );

    let events = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&type=record&include=data",
    )
    .await?;
    let event_rows = events["data"].as_array().expect("events data");
    assert_eq!(event_rows.len(), 1);
    assert!(event_rows[0].get("kind").is_none());
    assert_eq!(event_rows[0]["contract_address"], json!(RESOLVER));
    assert_eq!(event_rows[0]["data"]["key"], json!("addr:60"));
    let lean_events =
        v2_history_payload_for_database(&database, "/v1/events?name=history.eth&type=record")
            .await?;
    assert!(lean_events["data"][0].get("data").is_none());
    assert!(lean_events["data"][0].get("kind").is_none());

    let address = v2_history_payload_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?include=data&page_size=20",
    )
    .await?;
    let address_rows = address["data"].as_array().expect("address history data");
    assert!(!address_rows.is_empty());
    assert!(address_rows.iter().all(|row| row.get("kind").is_none() && row["data"].is_object()));

    for route in [
        "/v1/names/history.eth/history?include=bogus",
        "/v1/names/history.eth/history?include=data,bogus",
        "/v1/events?name=history.eth&include=events",
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?include=payload",
    ] {
        let response = v2_history_response_for_database(&database, route).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{route}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_history_include_raw_exposes_storage_kind_only_behind_the_opt_in() -> Result<()> {
    let (database, raw) =
        v2_history_payload("/v1/names/history.eth/history?include=raw&page_size=20").await?;
    let rows = raw["data"].as_array().expect("history data");
    assert_eq!(rows.len(), 10);
    for row in rows {
        assert!(row["kind"].is_string(), "{row}");
        assert!(row.get("data").is_none(), "{row}");
        assert!(row.get("contract_address").is_none(), "{row}");
        assert!(row.get("event_kind").is_none(), "{row}");
    }
    let row_at = |block: i64| {
        rows.iter()
            .find(|row| row["block_number"] == json!(block))
            .unwrap_or_else(|| panic!("row at block {block}"))
    };
    assert_eq!(row_at(101)["kind"], json!("AuthorityTransferred"));
    assert_eq!(row_at(102)["kind"], json!("RegistrationGranted"));
    assert_eq!(row_at(103)["kind"], json!("TokenControlTransferred"));
    assert_eq!(row_at(104)["kind"], json!("ResolverChanged"));
    assert_eq!(row_at(105)["kind"], json!("AuthorityEpochChanged"));
    assert_eq!(row_at(106)["kind"], json!("RecordChanged"));
    assert_eq!(row_at(107)["kind"], json!("PermissionChanged"));
    assert_eq!(row_at(108)["kind"], json!("RegistrationReleased"));
    assert_eq!(row_at(109)["kind"], json!("ExpiryChanged"));
    assert_eq!(row_at(110)["kind"], json!("RegistrationRenewed"));

    // The two flags compose in either order.
    for route in [
        "/v1/names/history.eth/history?include=data,raw&page_size=20",
        "/v1/names/history.eth/history?include=raw,%20data&page_size=20",
    ] {
        let both = v2_history_payload_for_database(&database, route).await?;
        let rows = both["data"].as_array().expect("history data");
        assert_eq!(rows.len(), 10, "{route}");
        for row in rows {
            assert!(row["kind"].is_string(), "{route}: {row}");
            assert!(row["data"].is_object(), "{route}: {row}");
            assert!(row.get("contract_address").is_some(), "{route}: {row}");
        }
    }

    let events = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&type=record&include=raw",
    )
    .await?;
    assert_eq!(events["data"][0]["kind"], json!("RecordChanged"));
    assert!(events["data"][0].get("data").is_none());
    let events = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&type=record&include=raw,data",
    )
    .await?;
    assert_eq!(events["data"][0]["kind"], json!("RecordChanged"));
    assert_eq!(events["data"][0]["data"]["key"], json!("addr:60"));

    let address = v2_history_payload_for_database(
        &database,
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?include=raw&page_size=20",
    )
    .await?;
    let address_rows = address["data"].as_array().expect("address history data");
    assert!(!address_rows.is_empty());
    assert!(
        address_rows
            .iter()
            .all(|row| row["kind"].is_string() && row.get("data").is_none())
    );

    for route in [
        "/v1/names/history.eth/history?include=raw,bogus",
        "/v1/events?name=history.eth&include=kind",
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?include=raw_kind",
    ] {
        let response = v2_history_response_for_database(&database, route).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{route}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_events_resolver_filter_lists_rows_for_one_resolver_contract() -> Result<()> {
    const RESOLVER: &str = "0x0000000000000000000000000000000000000abc";
    let (database, _) = v2_history_payload("/v1/names/history.eth/history?page_size=1").await?;
    sqlx::query(
        "UPDATE bigname_phase.normalized_events \
         SET raw_fact_ref = raw_fact_ref || '{\"emitting_address\":\"0x0000000000000000000000000000000000000ABC\"}'::jsonb \
         WHERE event_identity = 'history-record'",
    )
    .execute(&database.pool)
    .await?;

    // The record write was emitted by the resolver and the pointer change names it.
    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?resolver=1:{RESOLVER}"),
    )
    .await?;
    assert_eq!(history_blocks(&payload), vec![106, 104]);
    assert_eq!(
        history_types(payload["data"].as_array().expect("events data")),
        vec!["record", "resolver"]
    );
    assert_eq!(payload["page"]["total_count"], json!(2));
    assert_eq!(payload["data"][0]["name"], json!("history.eth"));

    let mixed_case = v2_history_payload_for_database(
        &database,
        "/v1/events?resolver=1:0x0000000000000000000000000000000000000ABC&order=asc",
    )
    .await?;
    assert_eq!(history_blocks(&mixed_case), vec![104, 106]);

    let detailed = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?resolver=1:{RESOLVER}&type=record&include=data"),
    )
    .await?;
    assert_eq!(history_blocks(&detailed), vec![106]);
    assert_eq!(detailed["data"][0]["contract_address"], json!(RESOLVER));

    // Cursors bind the resolver filter.
    let first = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?resolver=1:{RESOLVER}&page_size=1"),
    )
    .await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("resolver page must provide a cursor")
        .to_owned();
    let continued = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?resolver=1:{RESOLVER}&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(history_blocks(&continued), vec![104]);
    let response = v2_history_response_for_database(
        &database,
        &format!(
            "/v1/events?resolver=1:0x0000000000000000000000000000000000000bbb&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Another chain or another resolver matches nothing; the chain scopes the read.
    let empty = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?resolver=8453:{RESOLVER}"),
    )
    .await?;
    assert_eq!(empty["data"], json!([]));
    assert_eq!(empty["page"]["total_count"], json!(0));

    for route in [
        format!("/v1/events?resolver={RESOLVER}"),
        format!("/v1/events?resolver=99:{RESOLVER}"),
        "/v1/events?resolver=1:0x12".to_owned(),
        format!("/v1/events?resolver=one:{RESOLVER}"),
        format!("/v1/diagnostics/events?resolver=1:{RESOLVER}"),
    ] {
        let response = v2_history_response_for_database(&database, &route).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{route}");
        assert_eq!(
            read_json::<Value>(response).await?["error"]["code"],
            json!("invalid_input"),
            "{route}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_history_requested_exact_count_exceeds_default_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let extra = (0..10_001)
        .map(|n| {
            v2_history_event(
                &format!("exact-count-{n}"),
                Some("ens:history.eth"),
                None,
                "RecordChanged",
                101,
            )
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &extra).await?;
    let base = "/v1/names/history.eth/history?scope=name&type=record&page_size=1";
    let capped = v2_history_payload_for_database(&database, base).await?;
    assert_eq!(capped["page"]["total_count"], Value::Null);
    let exact =
        v2_history_payload_for_database(&database, &format!("{base}&include=total_count")).await?;
    assert!(exact["page"]["total_count"].as_u64().unwrap() > 10_000);
    let cursor = exact["page"]["next_cursor"].as_str().unwrap();
    let next = v2_history_payload_for_database(
        &database,
        &format!("{base}&include=total_count&cursor={cursor}"),
    )
    .await?;
    assert_eq!(next["page"]["total_count"], exact["page"]["total_count"]);
    let events = v2_history_payload_for_database(
        &database,
        "/v1/events?name=history.eth&type=record&page_size=1&include=total_count",
    )
    .await?;
    assert_eq!(events["page"]["total_count"], exact["page"]["total_count"]);
    let unanchored = v2_history_payload_for_database(
        &database,
        "/v1/events?namespace=ens&page_size=1&include=total_count",
    )
    .await?;
    assert_eq!(unanchored["page"]["total_count"], Value::Null);
    database.cleanup().await
}

#[tokio::test]
async fn v2_history_continuation_excludes_unpublished_interpret_events() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let base = "/v1/names/history.eth/history?page_size=1&include=total_count";
    let first = v2_history_payload_for_database(&database, base).await?;
    let cursor = first["page"]["next_cursor"].as_str().unwrap();
    // Interpret can append this block while Project still publishes the previous block.
    upsert_phase_raw_blocks(&database.pool, &[raw_block("ethereum-mainnet",
        "0xhistory21000004", None, 21_000_004, 1_776_384_004)]).await?;
    let next_event = v2_history_event("unpublished-history", Some("ens:history.eth"), None,
        "RecordChanged", 21_000_004);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[next_event]).await?;
    let next = v2_history_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(next["page"]["total_count"], first["page"]["total_count"]);
    assert!(history_blocks(&next).iter().all(|block| *block <= 21_000_003));
    database.seed_snapshot_selector_chain_positions(&json!({"ethereum": {
        "chain_id": "ethereum-mainnet", "block_number": 21_000_004,
        "block_hash": "0xhistory21000004", "timestamp": "2026-04-17T00:00:04Z"
    }})).await?;
    let response = v2_history_response_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let failure: Value = read_json(response).await?;
    assert_eq!(failure["error"]["code"], json!("stale"));
    database.cleanup().await
}

#[tokio::test]
async fn v2_history_ignores_unpublished_binding_and_address_anchor_expansion() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let resource = Uuid::from_u128(0xf7100);
    let token_lineage = Uuid::from_u128(0xf8100);
    upsert_test_token_lineages(&database.pool,
        &[address_name_token_lineage(token_lineage, "0xhistory101", 101)]).await?;
    upsert_test_resources(&database.pool, &[address_name_resource(resource, Some(token_lineage), "0xhistory101", 101)]).await?;
    let old_record = v2_history_event("old-unlinked-resource", None, Some(resource), "RecordChanged", 106);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[old_record]).await?;
    let first = v2_history_payload_for_database(&database,
        "/v1/names/history.eth/history?page_size=1&include=total_count").await?;
    upsert_phase_raw_blocks(&database.pool, &[raw_block("ethereum-mainnet", "0xfuture-binding",
        None, 21_000_004, 1_776_384_004)]).await?;
    let mut future_binding = address_name_surface_binding(Uuid::from_u128(0xf9100),
        "ens:history.eth", resource, "0xfuture-binding", 21_000_004, 1_776_384_004);
    future_binding.authority_arm = "ens_v2".to_owned();
    future_binding.active_to = Some(bigname_storage::parse_rfc3339_utc_timestamp("2027-01-01T00:00:00Z")?);
    upsert_test_surface_bindings(&database.pool, &[future_binding]).await?;
    let mut future_owner = v2_history_event("future-owner-anchor", None, Some(resource), "RegistrationGranted", 21_000_004);
    future_owner.block_hash = Some("0xfuture-binding".to_owned());
    future_owner.after_state = json!({"registrant":"0x0000000000000000000000000000000000000f99"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[future_owner]).await?;
    for route in ["/v1/names/history.eth/history?page_size=1&include=total_count",
        "/v1/events?name=history.eth&page_size=1&include=total_count"] {
        let page = v2_history_payload_for_database(&database, route).await?;
        assert_eq!(page["page"]["total_count"], first["page"]["total_count"]);
    }
    for route in ["/v1/addresses/0x0000000000000000000000000000000000000f99/history?include=total_count",
        "/v1/events?address=0x0000000000000000000000000000000000000f99&include=total_count"] {
        let page = v2_history_payload_for_database(&database, route).await?;
        assert_eq!(page["page"]["total_count"], json!(0));
    }
    let mut prior_owner = v2_history_event("prior-owner-anchor", None, Some(resource), "RegistrationGranted", 101);
    prior_owner.after_state = json!({"registrant":"0x0000000000000000000000000000000000000f98"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[prior_owner]).await?;
    let narrowed = v2_history_payload_for_database(&database,
        "/v1/addresses/0x0000000000000000000000000000000000000f98/history?type=record&from_timestamp=2023-11-14T22%3A15%3A06Z&include=total_count").await?;
    assert_eq!(narrowed["page"]["total_count"], json!(1), "an ownership anchor before the requested event window still applies");
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_routes_reject_unknown_namespace_before_publication_capture() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    for uri in [
        "/v1/events?namespace=en",
        "/v1/events?namespace=en&name=history.eth",
        "/v1/names/history.eth/history?namespace=en",
        "/v1/names/history.eth/subnames?namespace=en",
        "/v1/names/history.eth?namespace=en&include=counts",
    ] {
        let response = v2_history_response_for_database(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("not_found"));
        assert_eq!(payload["error"]["message"], json!("namespace en is not supported"));
    }
    // A recognized namespace without a publication is a temporary availability failure.
    let response = v2_history_response_for_database(&database, "/v1/events?namespace=ens").await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    database.cleanup().await
}
