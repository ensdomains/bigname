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
    seed_v2_history_blocks(&database, 121..=122).await?;
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
        "/v2/names/resolver-history.eth/history?scope=both&page_size=20",
        "/v2/names/resolver-history.eth/history?scope=registration&page_size=20",
        "/v2/events?name=resolver-history.eth&page_size=20",
        "/v2/events?registration_id=00000000-0000-0000-0000-000000007120&page_size=20",
        "/v2/addresses/0x0000000000000000000000000000000000007120/history?relation=manager&page_size=20",
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
        "/v2/diagnostics/events?name=resolver-history.eth&page_size=20",
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
        "/v2/diagnostics/events?name=resolver-history.eth&page_size=1",
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
            "/v2/diagnostics/events?name=resolver-history.eth&page_size=1&cursor={diagnostic_cursor}"
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
        &product_event_kinds,
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
        "/v2/events?name=event-position-history.eth&page_size=20",
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
async fn pre_enrichment_registrar_resolver_keeps_the_registration_handle() -> Result<()> {
    const ADDRESS: &str = "0x0000000000000000000000000000000000007123";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:pre-enrichment-resolver.eth";
    let registrar_resource_id = Uuid::from_u128(0x7123);
    let surface_binding_id = Uuid::from_u128(0x9123);
    seed_identity_name(
        &database,
        logical_name_id,
        "pre-enrichment-resolver.eth",
        "pre-enrichment-resolver.eth",
        "node:pre-enrichment-resolver.eth",
        registrar_resource_id,
        Uuid::from_u128(0x8123),
        surface_binding_id,
        ADDRESS,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=122).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000122)
         WHERE surface_binding_id = $1",
    )
    .bind(surface_binding_id)
    .execute(&database.pool)
    .await?;

    let mut resolver = v2_history_event(
        "registrar-resolver-before-enrichment",
        None,
        Some(registrar_resource_id),
        "ResolverChanged",
        121,
    );
    resolver.source_family = "ens_v1_registry_l1".to_owned();
    resolver.after_state = json!({
        "source_event": "NewResolver",
        "node": "node:pre-enrichment-resolver.eth",
        "authority_kind": "registrar",
        "resolver": "0x00000000000000000000000000000000000000aa"
    });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[resolver]).await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v2/events?registration_id={registrar_resource_id}&page_size=20"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("registrar history rows");
    assert_eq!(rows.len(), 1, "pre-enrichment resolver was omitted: {rows:?}");
    assert_eq!(rows[0]["type"], json!("resolver"));
    assert_eq!(
        rows[0]["registration_id"],
        json!(registrar_resource_id.to_string())
    );

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
        "/v2/names/ownerless-history.eth/history?scope=both&page_size=20",
        "/v2/events?name=ownerless-history.eth&page_size=20",
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
        &format!("/v2/events?registration_id={read_resource_id}&page_size=20"),
    )
    .await?;
    assert_eq!(filtered["data"], json!([]));

    let diagnostics = v2_history_payload_for_database(
        &database,
        "/v2/diagnostics/events?name=ownerless-history.eth&page_size=20",
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
async fn v2_registry_only_binding_does_not_admit_registration_history() -> Result<()> {
    assert_registry_binding_does_not_witness_registration(false).await
}

#[tokio::test]
async fn v2_retained_registry_after_release_does_not_admit_registration_history() -> Result<()> {
    assert_registry_binding_does_not_witness_registration(true).await
}

async fn assert_registry_binding_does_not_witness_registration(released: bool) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let name = "registry-witness.eth";
    let logical_name_id = "ens:registry-witness.eth";
    let registry = Uuid::from_u128(0x7132);
    let registrar = Uuid::from_u128(0x7133);
    seed_identity_name(
        &database,
        logical_name_id,
        name,
        name,
        "node:registry-witness.eth",
        registry,
        Uuid::from_u128(0x8132),
        Uuid::from_u128(0x9132),
        "0x0000000000000000000000000000000000007132",
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=125).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp($2)
         WHERE resource_id = $1",
    )
    .bind(registry)
    .bind(if released {
        1_700_000_123_f64
    } else {
        1_700_000_120_f64
    })
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET serving_resource_id = $1, resource_id = NULL, token_lineage_id = NULL,
             surface_binding_id = NULL, binding_kind = NULL,
             declared_summary = jsonb_build_object(
                 'registration', jsonb_build_object('status', 'unregistered'),
                 'control', jsonb_build_object('status', $2::text)
             )
         WHERE resource_id = $1",
    )
    .bind(registry)
    .bind(if released { "unregistered" } else { "active" })
    .execute(&database.pool)
    .await?;
    for query in [
        "UPDATE bigname_phase.address_names_current SET token_lineage_id = NULL WHERE resource_id = $1",
        "UPDATE bigname_phase.resources SET token_lineage_id = NULL WHERE resource_id = $1",
    ] {
        sqlx::query(query)
            .bind(registry)
            .execute(&database.pool)
            .await?;
    }
    let mut binding = v2_history_event(
        "registry-witness-binding",
        Some(logical_name_id),
        Some(registry),
        "SurfaceBound",
        if released { 123 } else { 120 },
    );
    binding.source_family = "ens_v1_registry_l1".to_owned();
    let mut record = v2_history_event(
        "registry-witness-record",
        Some(logical_name_id),
        None,
        "RecordChanged",
        124,
    );
    record.source_family = "ens_v1_resolver_l1".to_owned();
    let mut events = vec![binding, record];
    for kind in ["AuthorityTransferred", "AuthorityEpochChanged"] {
        let mut authority = v2_history_event(
            &format!("registry-witness-{kind}"),
            Some(logical_name_id),
            Some(registry),
            kind,
            123,
        );
        authority.source_family = "ens_v1_registry_l1".to_owned();
        authority.after_state = json!({
            "authority_kind": "registry_only",
            "owner": "0x0000000000000000000000000000000000007132",
            "owner_getter": "0x0000000000000000000000000000000000007132",
        });
        events.push(authority);
    }
    if released {
        upsert_test_resources(
            &database.pool,
            &[address_name_resource(
                registrar,
                None,
                "0xregistry-witness-registrar",
                81,
            )],
        )
        .await?;
        let mut old_binding = address_name_surface_binding(
            Uuid::from_u128(0x9133),
            logical_name_id,
            registrar,
            "0xhistory120",
            120,
            1_700_000_120,
        );
        old_binding.active_to = Some(timestamp(1_700_000_123));
        upsert_test_surface_bindings(&database.pool, &[old_binding]).await?;
        for (kind, block, resource) in [
            ("RegistrationGranted", 120, Some(registrar)),
            ("RecordChanged", 121, None),
            ("RegistrationReleased", 122, Some(registrar)),
        ] {
            events.push(v2_history_event(
                &format!("registry-witness-{kind}"),
                Some(logical_name_id),
                resource,
                kind,
                block,
            ));
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let filtered = v2_history_payload_for_database(
        &database,
        &format!("/v2/events?registration_id={registry}&page_size=20"),
    )
    .await?;
    assert!(
        filtered["data"].as_array().is_some_and(Vec::is_empty),
        "registry resource admitted product registration history: {filtered:?}"
    );
    assert_eq!(filtered["page"]["has_more"], json!(false));
    for route in [
        format!("/v2/names/{name}/history?scope=both&page_size=20"),
        format!("/v2/events?name={name}&page_size=20"),
    ] {
        let payload = v2_history_payload_for_database(&database, &route).await?;
        let rows = payload["data"].as_array().expect("name history rows");
        assert_eq!(
            rows.len(),
            if released { 6 } else { 3 },
            "{route}: {rows:?}"
        );
        assert_eq!(rows[0]["type"], json!("record"));
        assert_eq!(rows[0]["block_number"], json!(124));
        assert_eq!(rows[0]["registration_id"], Value::Null);
        assert_eq!(
            rows.iter().filter(|row| row["type"] == "authority").count(),
            2
        );
    }
    let diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v2/diagnostics/events?name={name}&page_size=20"),
    )
    .await?;
    assert_eq!(
        diagnostics["data"].as_array().map(Vec::len),
        Some(if released { 7 } else { 4 })
    );
    for (resource_id, include_candidates, expected) in [
        (Some(registry), false, 0),
        (Some(registry), true, if released { 7 } else { 4 }),
        (None, false, if released { 6 } else { 3 }),
    ] {
        let page = bigname_storage::load_event_history_page(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id,
                logical_name_id: resource_id
                    .is_none()
                    .then(|| bigname_storage::logical_name_id_for_name("ens", name)),
                event_kinds: if include_candidates {
                    Vec::new()
                } else {
                    crate::v2::product_history_event_kinds()
                },
                ..bigname_storage::EventHistoryFilter::default()
            },
            true,
            None,
            1,
            bigname_storage::HistorySummaryMode::Full,
            include_candidates,
        )
        .await?;
        assert_eq!(page.rows.len(), usize::from(expected != 0));
        assert_eq!(page.next_cursor.is_some(), expected > 1);
        assert_eq!(page.summary.unwrap().total_count, expected);
    }
    if released {
        let previous = v2_history_payload_for_database(
            &database,
            &format!("/v2/events?registration_id={registrar}&page_size=20"),
        )
        .await?;
        let rows = previous["data"]
            .as_array()
            .expect("released registration history");
        assert_eq!(
            history_types(rows),
            vec!["release", "record", "registration"]
        );
        assert_eq!(rows[1]["block_number"], json!(121));
        assert_eq!(rows[1]["registration_id"], Value::Null);
    }
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
        &format!("/v2/events?registration_id={resource_id}&page_size=20"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("product event rows");
    assert_eq!(history_types(rows), vec!["record", "registration"]);
    assert_eq!(rows[0]["registration_id"], Value::Null);
    assert_eq!(rows[1]["registration_id"], json!(resource_id.to_string()));

    let first_page = v2_history_payload_for_database(
        &database,
        &format!("/v2/events?registration_id={resource_id}&page_size=1"),
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
        &format!("/v2/events?registration_id={resource_id}&page_size=1&cursor={cursor}"),
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
        "/v2/events?name=%E1%8F%A3%E1%8E%B3%E1%8E%A9.eth&page_size=20".to_owned(),
        format!("/v2/addresses/{ADDRESS}/history?page_size=20"),
    ] {
        let payload = v2_history_payload_for_database(&database, &uri).await?;
        assert_eq!(payload["data"][0]["name"], json!(stored_raw_name), "{uri}");
    }

    database.cleanup().await
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
    assert_eq!(first_hashes, vec!["0xtx110", "0xtx109", "0xtx108"]);
    assert_eq!(second_hashes, vec!["0xtx107", "0xtx106", "0xtx105"]);

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

    let candidate_address_diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v2/diagnostics/events?address={ADDRESS}&page_size=20"),
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
    let route = "/v2/diagnostics/events?name=history.eth&page_size=20";

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
async fn noncanonical_born_wrapped_history_keeps_one_registration_handle() -> Result<()> {
    const NAME: &str = "noncanonical-born-wrapped.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let wrapper = Uuid::from_u128(0x7160);
    let registrar = Uuid::from_u128(0x7161);
    seed_identity_name(
        &database,
        "ens:noncanonical-born-wrapped.eth",
        NAME,
        NAME,
        &namehash,
        wrapper,
        Uuid::from_u128(0x8160),
        Uuid::from_u128(0x9160),
        "0x0000000000000000000000000000000000007160",
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            registrar,
            None,
            "0xnoncanonical-registrar",
            79,
        )],
    )
    .await?;
    let blocks = (130..=133)
        .map(|block_number| {
            let parent = (block_number > 130).then(|| format!("0xhistory{}", block_number - 1));
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{block_number}"),
                parent.as_deref(),
                block_number,
                1_700_000_000 + block_number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000130), canonicality_state = 'orphaned',
             block_hash = '0xhistory130', block_number = 130
         WHERE resource_id = $1",
    )
    .bind(wrapper)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces
         SET canonicality_state = 'orphaned', block_hash = '0xhistory130', block_number = 130
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .execute(&database.pool)
    .await?;
    let mut grant = v2_history_event(
        "noncanonical-grant",
        None,
        Some(registrar),
        "RegistrationGranted",
        130,
    );
    grant.after_state["namehash"] = json!(namehash);
    let mut binding = v2_history_event(
        "noncanonical-binding",
        Some(&logical_name_id),
        Some(wrapper),
        "SurfaceBound",
        130,
    );
    binding.source_family = "ens_v1_wrapper_l1".to_owned();
    binding.after_state = json!({"wrapped_registrar_resource_id": registrar});
    let mut transfer = v2_history_event(
        "noncanonical-transfer",
        Some(&logical_name_id),
        Some(wrapper),
        "TokenControlTransferred",
        131,
    );
    transfer.source_family = "ens_v1_wrapper_l1".to_owned();
    let permission = v2_history_event(
        "noncanonical-permission",
        None,
        Some(registrar),
        "PermissionChanged",
        132,
    );
    let mut record = v2_history_event(
        "noncanonical-record",
        Some(&logical_name_id),
        None,
        "RecordChanged",
        133,
    );
    record.source_family = "ens_v1_resolver_l1".to_owned();
    let mut events = vec![grant, binding, transfer, permission, record];
    for event in &mut events {
        event.canonicality_state = CanonicalityState::Orphaned;
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum-mainnet' AND block_number BETWEEN 130 AND 133",
    )
    .execute(&database.pool)
    .await?;
    for canonical_only in [true, false] {
        let name_rows = bigname_storage::load_name_history(
            &database.pool,
            &logical_name_id,
            &[wrapper, registrar],
            bigname_storage::HistoryScope::Both,
            canonical_only,
        )
        .await?;
        let registration_rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id: Some(wrapper),
                ..bigname_storage::EventHistoryFilter::default()
            },
            canonical_only,
        )
        .await?;
        for rows in [name_rows, registration_rows] {
            assert_eq!(rows.len(), if canonical_only { 0 } else { 5 }, "{rows:?}");
            if canonical_only {
                continue;
            }
            assert_eq!(
                rows.iter()
                    .map(|row| row.event_identity.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    "noncanonical-record",
                    "noncanonical-permission",
                    "noncanonical-transfer",
                    "noncanonical-grant",
                    "noncanonical-binding"
                ]
            );
            assert!(
                rows.iter()
                    .filter(|row| row.resource_id.is_some())
                    .all(|row| row.registration_id == Some(wrapper)),
                "{rows:?}"
            );
            assert_eq!(rows[0].resource_id, None);
            assert_eq!(rows[0].registration_id, None);
        }
    }
    let registrar_rows = bigname_storage::load_event_history(
        &database.pool,
        bigname_storage::EventHistoryFilter {
            resource_id: Some(registrar),
            ..bigname_storage::EventHistoryFilter::default()
        },
        false,
    )
    .await?;
    assert!(
        registrar_rows.is_empty(),
        "second lifecycle handle: {registrar_rows:?}"
    );
    database.cleanup().await
}

#[tokio::test]
#[rustfmt::skip]
async fn born_wrapped_detail_and_history_keep_the_wrapper_registration_handle() -> Result<()> {
    const NAME: &str = "born-wrapped-history.eth"; const LOGICAL: &str = "ens:born-wrapped-history.eth";
    let database = TestDatabase::new_migrated().await?; let wrapper = Uuid::from_u128(0x7140); let registrar = Uuid::from_u128(0x7141); let rewrapper = Uuid::from_u128(0x7142); let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    seed_identity_name(&database, LOGICAL, NAME, NAME, &namehash, wrapper, Uuid::from_u128(0x8140), Uuid::from_u128(0x9140), "0x0000000000000000000000000000000000007140", bigname_storage::AddressNameRelation::Registrant, 80).await?;
    let orphan_wrapper = Uuid::from_u128(0x7143); upsert_test_resources(&database.pool, &[address_name_resource(registrar, None, "0xborn-wrap-registrar", 79), address_name_resource(rewrapper, None, "0xborn-wrap-rewrapper", 79), address_name_resource(orphan_wrapper, None, "0xborn-wrap-orphan", 79)]).await?; seed_v2_history_blocks(&database, 130..=136).await?;
    // Bind the fixture at its normalized history time, rather than the seed's default date.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000130) WHERE resource_id = $1",
    )
    .bind(wrapper)
    .execute(&database.pool)
    .await?;
    let mut grant = v2_history_event("born-wrap-grant", None, Some(registrar), "RegistrationGranted", 130); grant.after_state["namehash"] = json!(namehash); let mut setup = v2_history_event("born-wrap-setup", None, Some(registrar), "AuthorityTransferred", 130); setup.source_family = "ens_v1_registry_l1".to_owned(); setup.after_state = json!({"source_event":"NewOwner","child_node":namehash,"owner":"0x0000000000000000000000000000000000007140"}); let mut binding = v2_history_event("born-wrap-binding", Some(LOGICAL), Some(wrapper), "SurfaceBound", 130); binding.source_family = "ens_v1_wrapper_l1".to_owned(); binding.after_state = json!({"source_event":"NameWrapped","node":namehash,"wrapped_registrar_resource_id":registrar});
    let mut transfer = v2_history_event("born-wrap-transfer", Some(LOGICAL), Some(wrapper), "TokenControlTransferred", 131); transfer.source_family = "ens_v1_wrapper_l1".to_owned();
    let mut unbound = v2_history_event("born-wrap-unbound", Some(LOGICAL), Some(wrapper), "SurfaceUnbound", 132); unbound.source_family = "ens_v1_wrapper_l1".to_owned();
    let unwrapped_transfer = v2_history_event("born-wrap-unwrapped-transfer", Some(LOGICAL), Some(registrar), "TokenControlTransferred", 132);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, setup, binding, transfer, unbound, unwrapped_transfer]).await?;
    let unwrapped = v2_history_payload_for_database(&database, &format!("/v2/events?registration_id={wrapper}&page_size=20")).await?; assert_eq!(history_types(unwrapped["data"].as_array().expect("unwrapped history data")), vec!["transfer", "transfer", "authority", "registration"]);
    let mut rewrap_binding = v2_history_event("born-wrap-rewrap-binding", Some(LOGICAL), Some(rewrapper), "SurfaceBound", 133); rewrap_binding.source_family = "ens_v1_wrapper_l1".to_owned(); rewrap_binding.after_state = json!({"source_event":"NameWrapped","node":namehash,"wrapped_registrar_resource_id":registrar});
    let mut rewrap_transfer = v2_history_event("born-wrap-rewrap-transfer", Some(LOGICAL), Some(rewrapper), "TokenControlTransferred", 134); rewrap_transfer.source_family = "ens_v1_wrapper_l1".to_owned();
    let permission = v2_history_event("born-wrap-registrar-permission", None, Some(registrar), "PermissionChanged", 135);
    let mut record = v2_history_event("born-wrap-resource-less-record", Some(LOGICAL), None, "RecordChanged", 135); record.source_family = "ens_v1_resolver_l1".to_owned();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[rewrap_binding, rewrap_transfer, permission, record]).await?; let detail = v2_history_payload_for_database(&database, &format!("/v2/names/{NAME}")).await?; assert_eq!(detail["data"]["registration_id"], json!(wrapper.to_string()));
    let history = v2_history_payload_for_database(&database, &format!("/v2/names/{NAME}/history?scope=registration&page_size=20")).await?; let rows = history["data"].as_array().expect("history data"); let direct = v2_history_payload_for_database(&database, &format!("/v2/events?registration_id={wrapper}&page_size=20")).await?; let direct_rows = direct["data"].as_array().expect("direct history data");
    assert_eq!(history_types(direct_rows), vec!["record", "permission", "transfer", "transfer", "transfer", "authority", "registration"], "wrapper registration_id lost registrar, resource-less, or later-wrapper history: {direct_rows:?}"); assert!(rows.iter().filter(|row| matches!(row["type"].as_str(), Some("registration" | "transfer" | "authority" | "permission" | "record"))).all(|row| row["registration_id"] == json!(wrapper.to_string())), "born-wrapped history split its first wrapper handle after re-wrap: {rows:?}"); assert!(direct_rows.iter().any(|row| row["block_number"] == json!(134) && row["registration_id"] == json!(wrapper.to_string())), "the first wrapper handle did not follow the later wrapper row: {direct_rows:?}");
    let registrar_history = v2_history_payload_for_database(&database, &format!("/v2/events?registration_id={registrar}&page_size=20")).await?; assert!(registrar_history["data"].as_array().is_some_and(Vec::is_empty), "registrar-scoped rows leaked under a second registration handle: {registrar_history:?}");
    let rewrapper_history = v2_history_payload_for_database(&database, &format!("/v2/events?registration_id={rewrapper}&page_size=20")).await?; assert!(rewrapper_history["data"].as_array().is_some_and(Vec::is_empty), "later-wrapper rows leaked under a second registration handle: {rewrapper_history:?}");
    let plan = bigname_storage::explain_registration_history_filter_for_test(&database.pool, wrapper, LOGICAL, "ethereum-mainnet", "ens", &namehash).await?;
    assert_registration_history_plan_is_page_keyed(&plan);
    let orphan_grant = v2_history_event("born-wrap-orphan-grant", None, Some(registrar), "RegistrationGranted", 136); let mut orphan_binding = v2_history_event("born-wrap-orphan-binding", Some(LOGICAL), Some(orphan_wrapper), "SurfaceBound", 136); orphan_binding.source_family = "ens_v1_wrapper_l1".to_owned(); orphan_binding.after_state = json!({"source_event":"NameWrapped","node":namehash,"wrapped_registrar_resource_id":registrar});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[orphan_grant, orphan_binding]).await?; sqlx::query("UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xhistory136'").execute(&database.pool).await?;
    let canonical = v2_history_payload_for_database(&database, &format!("/v2/events?registration_id={wrapper}&page_size=20")).await?; let canonical_rows = canonical["data"].as_array().expect("canonical history data"); assert_eq!(history_types(canonical_rows), vec!["record", "permission", "transfer", "transfer", "transfer", "authority", "registration"], "orphaned born-wrap evidence hid canonical registration history: {canonical_rows:?}"); assert!(canonical_rows.iter().filter(|row| matches!(row["type"].as_str(), Some("registration" | "transfer" | "authority" | "permission"))).all(|row| row["registration_id"] == json!(wrapper.to_string())), "orphaned born-wrap evidence changed canonical registration identity: {canonical_rows:?}");
    database.cleanup().await
}

#[tokio::test]
async fn later_wrapped_name_keeps_one_followable_registrar_lifecycle_handle() -> Result<()> {
    const NAME: &str = "later-wrapped-history.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:later-wrapped-history.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007150";
    let database = TestDatabase::new_migrated().await?;
    let wrapper_resource_id = Uuid::from_u128(0x7150);
    let registrar_resource_id = Uuid::from_u128(0x7151);
    let older_registrar_resource_id = Uuid::from_u128(0x7152);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);

    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        "node:later-wrapped-history.eth",
        wrapper_resource_id,
        Uuid::from_u128(0x8150),
        Uuid::from_u128(0x9150),
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(
                registrar_resource_id,
                None,
                "0xregistrar-resource",
                79,
            ),
            address_name_resource(
                older_registrar_resource_id,
                None,
                "0xolder-registrar-resource",
                78,
            ),
        ],
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
                 jsonb_set(
                     declared_summary,
                     '{registration,authority_kind}',
                     '\"wrapper\"'::jsonb,
                     true
                 ),
                 '{registration,resource_id}',
                 to_jsonb($2::text),
                 true
             )
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .bind(registrar_resource_id)
    .execute(&database.pool)
    .await?;
    let projected_registration_id: Option<String> = sqlx::query_scalar(
        "SELECT declared_summary #>> '{registration,resource_id}'
         FROM bigname_phase.name_current
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(projected_registration_id, Some(registrar_resource_id.to_string()));
    seed_v2_history_blocks(&database, 120..=125).await?;

    // Bind the fixture at its normalized history time, rather than the seed's default date.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000121) WHERE resource_id = $1",
    )
    .bind(wrapper_resource_id)
    .execute(&database.pool)
    .await?;
    let mut older_registration = v2_history_event(
        "later-wrap-older-unbound-registration",
        None,
        Some(older_registrar_resource_id),
        "RegistrationGranted",
        120,
    );
    older_registration.after_state["namehash"] = json!(&namehash);
    let mut registration = v2_history_event(
        "later-wrap-controller-free-registration",
        None,
        Some(registrar_resource_id),
        "RegistrationGranted",
        121,
    );
    registration.after_state["namehash"] = json!(&namehash);
    let mut wrapper_binding = v2_history_event(
        "later-wrap-binding",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "SurfaceBound",
        122,
    );
    wrapper_binding.source_family = "ens_v1_wrapper_l1".to_owned();
    wrapper_binding.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": registrar_resource_id,
    });
    let mut wrapper_transfer = v2_history_event(
        "later-wrap-holder-transfer",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "TokenControlTransferred",
        123,
    );
    wrapper_transfer.source_family = "ens_v1_wrapper_l1".to_owned();
    let mut wrapped_controller_renewal = v2_history_event(
        "later-wrap-wrapped-controller-renewal",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "ExpiryChanged",
        124,
    );
    wrapped_controller_renewal.source_family = "ens_v1_registrar_l1".to_owned();
    wrapped_controller_renewal.after_state["source_event"] = json!("NameRenewed");
    let mut wrapper_registry_resolver = v2_history_event(
        "later-wrap-registry-resolver",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "ResolverChanged",
        125,
    );
    wrapper_registry_resolver.source_family = "ens_v1_registry_l1".to_owned();
    wrapper_registry_resolver.after_state["source_event"] = json!("NewResolver");
    let surface_record = v2_history_event(
        "later-wrap-surface-record",
        Some(&logical_name_id),
        None,
        "RecordChanged",
        125,
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            older_registration,
            registration,
            wrapper_binding,
            wrapper_transfer,
            wrapped_controller_renewal,
            wrapper_registry_resolver,
            surface_record,
        ],
    )
    .await?;

    let exact_name = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/{NAME}"),
    )
    .await?;
    assert_eq!(
        exact_name["data"]["registration_id"],
        json!(registrar_resource_id.to_string()),
        "the exact-name response returned the wrapper resource instead of the registrar lifecycle handle"
    );

    let name_history_route =
        format!("/v2/names/{NAME}/history?scope=registration&page_size=20");
    {
        let payload = v2_history_payload_for_database(&database, &name_history_route).await?;
        let rows = payload["data"].as_array().expect("history data");
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("registration")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not return the registrar registration: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("transfer")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not keep the wrapper holder change on the registrar lifecycle handle: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("expiry")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not keep the wrapped-controller renewal on the registrar lifecycle handle: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("resolver")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not keep the wrapper-active registry resolver on the registrar lifecycle handle: {rows:?}"
        );
    }

    let registration_route =
        format!("/v2/events?registration_id={registrar_resource_id}&page_size=20");
    let registration_payload =
        v2_history_payload_for_database(&database, &registration_route).await?;
    let registration_rows = registration_payload["data"]
        .as_array()
        .expect("history data");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("registration")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }));
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("transfer")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }), "the registrar lifecycle filter omitted the later wrapper holder change: {registration_rows:?}");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("expiry")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }), "the registrar lifecycle filter omitted the wrapped-controller renewal: {registration_rows:?}");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("resolver")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }), "the registrar lifecycle filter omitted the wrapper-active registry resolver: {registration_rows:?}");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("record") && row["registration_id"] == Value::Null
    }), "the registrar lifecycle filter omitted resource-less exact-name history: {registration_rows:?}");
    let plan = bigname_storage::explain_registration_history_filter_for_test(
        &database.pool,
        registrar_resource_id,
        &logical_name_id,
        "ethereum-mainnet",
        "ens",
        &namehash,
    )
    .await?;
    assert!(
        plan.contains("normalized_events_resource_projection_replay_idx")
            || plan.contains("normalized_events_history_resource_lookup_idx")
            || plan.contains("normalized_events_resource_history_idx"),
        "registration history must retain an indexed registrar-resource anchor:\n{plan}"
    );
    assert!(
        plan.contains("normalized_events_name_projection_replay_idx")
            || plan.contains("normalized_events_name_history_idx"),
        "registration history must use the exact-name history index for associated rows:\n{plan}"
    );
    assert!(
        plan.contains("name_surfaces_exact_namehash_projection_idx")
            || plan.contains("name_surfaces_hash_idx")
            || plan.contains("name_surfaces_visibility_idx"),
        "wrapper association must use the exact-namehash surface index:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on normalized_events"),
        "registration history and association discovery must not scan the complete normalized-event table:\n{plan}"
    );
    assert_registration_history_plan_is_page_keyed(&plan);
    let older_registration_payload = v2_history_payload_for_database(
        &database,
        &format!(
            "/v2/events?registration_id={older_registrar_resource_id}&page_size=20"
        ),
    )
    .await?;
    let older_registration_rows = older_registration_payload["data"]
        .as_array()
        .expect("older registration history");
    assert_eq!(history_types(older_registration_rows), vec!["registration"]);
    assert_eq!(
        older_registration_rows[0]["registration_id"],
        json!(older_registrar_resource_id.to_string())
    );

    let by_name = v2_history_payload_for_database(
        &database,
        &format!("/v2/events?name={NAME}&page_size=20"),
    )
    .await?;
    let by_name_rows = by_name["data"].as_array().expect("history data");
    assert!(by_name_rows.iter().any(|row| {
        row["type"] == json!("registration")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }));

    sqlx::query(
        "UPDATE bigname_phase.normalized_events
         SET canonicality_state = 'orphaned'
         WHERE event_identity = 'later-wrap-binding'",
    )
    .execute(&database.pool)
    .await?;
    let after_binding_retraction =
        v2_history_payload_for_database(&database, &registration_route).await?;
    assert_eq!(
        history_types(
            after_binding_retraction["data"]
                .as_array()
                .expect("history after wrapper-binding retraction")
        ),
        vec!["registration"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_history_keeps_a_superseded_controller_free_wrapped_registration() -> Result<()> {
    const NAME: &str = "superseded-later-wrap.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:superseded-later-wrap.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007160";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let prior_wrapper_resource_id = Uuid::from_u128(0x7160);
    let prior_registrar_resource_id = Uuid::from_u128(0x7161);
    let current_registrar_resource_id = Uuid::from_u128(0x7162);

    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        &namehash,
        current_registrar_resource_id,
        Uuid::from_u128(0x8162),
        Uuid::from_u128(0x9162),
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        84,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(
                prior_wrapper_resource_id,
                None,
                "0xprior-wrapper-resource",
                79,
            ),
            address_name_resource(
                prior_registrar_resource_id,
                None,
                "0xprior-registrar-resource",
                78,
            ),
        ],
    )
    .await?;
    let mut prior_wrapper_binding = address_name_surface_binding(
        Uuid::from_u128(0x9160),
        SEED_LOGICAL_NAME_ID,
        prior_wrapper_resource_id,
        "0xprior-wrapper-binding",
        80,
        1_717_171_600,
    );
    prior_wrapper_binding.active_to = Some(timestamp(1_717_171_699));
    upsert_test_surface_bindings(&database.pool, &[prior_wrapper_binding]).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary,
             '{registration,resource_id}',
             to_jsonb($2::text),
             true
         )
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .bind(current_registrar_resource_id)
    .execute(&database.pool)
    .await?;
    seed_v2_history_blocks(&database, 121..=124).await?;

    let prior_registration = v2_history_event(
        "superseded-later-wrap-registration",
        None,
        Some(prior_registrar_resource_id),
        "RegistrationGranted",
        121,
    );
    let mut prior_wrapper_binding_event = v2_history_event(
        "superseded-later-wrap-binding",
        Some(&logical_name_id),
        Some(prior_wrapper_resource_id),
        "SurfaceBound",
        122,
    );
    prior_wrapper_binding_event.source_family = "ens_v1_wrapper_l1".to_owned();
    prior_wrapper_binding_event.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": prior_registrar_resource_id,
    });
    let prior_release = v2_history_event(
        "superseded-later-wrap-release",
        None,
        Some(prior_registrar_resource_id),
        "RegistrationReleased",
        123,
    );
    let current_registration = v2_history_event(
        "superseded-current-registration",
        Some(&logical_name_id),
        Some(current_registrar_resource_id),
        "RegistrationGranted",
        124,
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            prior_registration,
            prior_wrapper_binding_event,
            prior_release,
            current_registration,
        ],
    )
    .await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("history data");
    assert!(
        rows.iter().any(|row| {
            row["type"] == json!("registration")
                && row["registration_id"] == json!(prior_registrar_resource_id.to_string())
        }),
        "name-scoped history forgot the superseded controller-free registrar lifecycle: {rows:?}"
    );
    assert!(rows.iter().any(|row| {
        row["type"] == json!("registration")
            && row["registration_id"] == json!(current_registrar_resource_id.to_string())
    }));

    let event_payload = v2_history_payload_for_database(
        &database,
        &format!("/v2/events?name={NAME}&page_size=20"),
    )
    .await?;
    let event_rows = event_payload["data"].as_array().expect("event data");
    assert!(
        event_rows.iter().any(|row| {
            row["type"] == json!("registration")
                && row["registration_id"] == json!(prior_registrar_resource_id.to_string())
        }),
        "name-filtered events forgot the superseded controller-free registrar lifecycle: {event_rows:?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn wrapper_without_prior_registrar_keeps_name_history_readable() -> Result<()> {
    const NAME: &str = "wrapper-without-registrar.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:wrapper-without-registrar.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007170";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let wrapper_resource_id = Uuid::from_u128(0x7170);

    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        &namehash,
        wrapper_resource_id,
        Uuid::from_u128(0x8170),
        Uuid::from_u128(0x9170),
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=121).await?;
    let mut wrapper_binding_event = v2_history_event(
        "wrapper-without-prior-registrar-binding",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "SurfaceBound",
        121,
    );
    wrapper_binding_event.source_family = "ens_v1_wrapper_l1".to_owned();
    wrapper_binding_event.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": null,
    });
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[wrapper_binding_event],
    )
    .await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    assert_eq!(payload["data"], json!([]));

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

fn assert_registration_history_plan_is_page_keyed(plan: &str) {
    let history_plan = plan
        .split_once("history page:\n")
        .map(|(_, history_plan)| history_plan)
        .expect("combined plan must contain the history page section");
    let lines = history_plan.lines().collect::<Vec<_>>();
    let scans = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.contains(" on normalized_events "))
        .filter_map(|(index, node)| {
            lines[index + 1..]
                .iter()
                .take_while(|line| !line.trim_start().starts_with("->"))
                .find(|line| line.trim_start().starts_with("Index Cond:"))
                .map(|condition| (*node, *condition))
        })
        .collect::<Vec<_>>();

    assert!(
        !scans.iter().any(|(_, condition)| {
            condition.contains("chain_id =") && !condition.contains(" AND ")
        }),
        "no normalized-events history scan may be keyed only by chain_id:\n{history_plan}"
    );
    assert!(
        !scans.iter().any(|(_, condition)| {
            condition.contains("event_kind =") && !condition.contains(" AND ")
        }),
        "no normalized-events history scan may be keyed only by event_kind:\n{history_plan}"
    );

    let born_wrapper_scans = scans
        .iter()
        .filter(|(node, _)| node.contains(" born_wrapper_candidate"))
        .collect::<Vec<_>>();
    assert!(
        !born_wrapper_scans.is_empty()
            && born_wrapper_scans
                .iter()
                .all(|(_, condition)| condition.contains("logical_name_id =")),
        "every born-wrapper candidate scan must be keyed by logical_name_id:\n{history_plan}"
    );

    for alias in [" registrar_grant", " wrapper_binding"] {
        let resource_scans = scans
            .iter()
            .filter(|(node, _)| node.contains(alias))
            .collect::<Vec<_>>();
        assert!(
            !resource_scans.is_empty()
                && resource_scans
                    .iter()
                    .all(|(_, condition)| condition.contains("resource_id =")),
            "every {alias} scan must be keyed by resource_id:\n{history_plan}"
        );
    }
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
async fn noncanonical_registration_history_keeps_resource_less_events_on_their_fork() -> Result<()>
{
    const NAME: &str = "fork-registration-history.eth";
    const SEED: &str = "ens:fork-registration-history.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let registration_a = Uuid::from_u128(0x7170);
    let registration_b = Uuid::from_u128(0x7171);
    seed_identity_name(
        &database,
        SEED,
        NAME,
        NAME,
        "node:fork-registration-history.eth",
        registration_a,
        Uuid::from_u128(0x8170),
        Uuid::from_u128(0x9170),
        "0x0000000000000000000000000000000000007170",
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            registration_b,
            None,
            "0xfork-resource-b",
            79,
        )],
    )
    .await?;
    let mut blocks = vec![raw_block(
        "ethereum-mainnet",
        "0xfork-common",
        None,
        129,
        1_700_000_129,
    )];
    for branch in ["a", "b"] {
        for number in 130..=131 {
            let parent = if number == 130 {
                "0xfork-common".to_owned()
            } else {
                format!("0xfork-{branch}-130")
            };
            let mut block = raw_block(
                "ethereum-mainnet",
                &format!("0xfork-{branch}-{number}"),
                Some(&parent),
                number,
                1_700_000_000 + number,
            );
            if branch == "b" {
                block.canonicality_state = CanonicalityState::Orphaned;
            }
            blocks.push(block);
        }
    }
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    // Both active intervals cover exactly the same wall-clock times. Fork hashes
    // are the only valid discriminator for the resource-less records below.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
        SET block_hash = '0xfork-a-130', block_number = 130,
            active_from = to_timestamp(1700000130), active_to = NULL
        WHERE resource_id = $1",
    )
    .bind(registration_a)
    .execute(&database.pool)
    .await?;
    let mut binding_b = address_name_surface_binding(
        Uuid::from_u128(0x9171),
        SEED,
        registration_b,
        "0xfork-b-130",
        130,
        1_700_000_130,
    );
    binding_b.canonicality_state = CanonicalityState::Orphaned;
    upsert_test_surface_bindings(&database.pool, &[binding_b]).await?;
    let mut events = Vec::new();
    for (branch, resource, state) in [
        ("a", registration_a, CanonicalityState::Canonical),
        ("b", registration_b, CanonicalityState::Orphaned),
    ] {
        for (suffix, kind, number, resource_id) in [
            ("grant", "RegistrationGranted", 130, Some(resource)),
            ("record", "RecordChanged", 131, None),
        ] {
            let mut event = v2_history_event(
                &format!("fork-{branch}-{suffix}"),
                Some(&logical_name_id),
                resource_id,
                kind,
                number,
            );
            event.block_hash = Some(format!("0xfork-{branch}-{number}"));
            event.transaction_hash = Some(format!("0xfork-{branch}-tx-{number}"));
            event.canonicality_state = state;
            if resource_id.is_none() {
                event.source_family = "ens_v1_resolver_l1".to_owned();
            }
            events.push(event);
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // Repeat with both branches orphaned, so a canonicality-state equality
    // substitute cannot satisfy the regression.
    for both_orphaned in [false, true] {
        if both_orphaned {
            for table in ["normalized_events", "surface_bindings", "chain_lineage"] {
                sqlx::query(&format!("UPDATE bigname_phase.{table}
                    SET canonicality_state = 'orphaned'
                    WHERE chain_id = 'ethereum-mainnet' AND block_hash IN ('0xfork-a-130', '0xfork-a-131')"))
                    .execute(&database.pool).await?;
            }
        }
        let name_rows = bigname_storage::load_name_history(
            &database.pool,
            &logical_name_id,
            &[registration_a, registration_b],
            bigname_storage::HistoryScope::Both,
            false,
        )
        .await?;
        assert_eq!(name_rows.len(), 4, "{name_rows:?}");
        for (branch, resource, other_resource) in [
            ("a", registration_a, registration_b),
            ("b", registration_b, registration_a),
        ] {
            let filter = bigname_storage::EventHistoryFilter {
                resource_id: Some(resource),
                ..Default::default()
            };
            let rows =
                bigname_storage::load_event_history(&database.pool, filter.clone(), false).await?;
            assert_eq!(
                rows.iter()
                    .map(|row| row.event_identity.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    format!("fork-{branch}-record"),
                    format!("fork-{branch}-grant")
                ]
            );
            assert_eq!(rows[0].registration_id, None);
            assert_eq!(rows[1].registration_id, Some(resource));
            let canonical =
                bigname_storage::load_event_history(&database.pool, filter.clone(), true).await?;
            assert_eq!(
                canonical.len(),
                if branch == "a" && !both_orphaned {
                    2
                } else {
                    0
                }
            );
            for summary_mode in [
                bigname_storage::HistorySummaryMode::Count,
                bigname_storage::HistorySummaryMode::Full,
            ] {
                let page = bigname_storage::load_event_history_page(
                    &database.pool,
                    filter.clone(),
                    false,
                    None,
                    1,
                    summary_mode,
                    false,
                )
                .await?;
                assert_eq!(page.rows.len(), 1);
                assert_eq!(page.rows[0].event_identity, format!("fork-{branch}-record"));
                assert_eq!(page.summary.as_ref().unwrap().total_count, 2);
                let cursor = page.next_cursor.as_ref().expect("same-fork grant remains");
                let next = bigname_storage::load_event_history_page(
                    &database.pool,
                    filter.clone(),
                    false,
                    Some(cursor),
                    1,
                    summary_mode,
                    false,
                )
                .await?;
                assert_eq!(next.rows.len(), 1);
                assert_eq!(next.rows[0].event_identity, format!("fork-{branch}-grant"));
                assert!(next.next_cursor.is_none());
                let error = bigname_storage::load_event_history_page(
                    &database.pool,
                    bigname_storage::EventHistoryFilter {
                        resource_id: Some(other_resource),
                        ..Default::default()
                    },
                    false,
                    Some(cursor),
                    1,
                    summary_mode,
                    false,
                )
                .await
                .expect_err("another fork's record must not anchor this registration");
                assert!(
                    error
                        .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
                        .is_some()
                );
            }
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn pre_enrichment_registration_handle_requires_token_lineage() -> Result<()> {
    const NAME: &str = "pre-enrichment-token-history.eth";
    const SEED: &str = "ens:pre-enrichment-token-history.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let registrar = Uuid::from_u128(0x7180);
    let registry = Uuid::from_u128(0x7181);
    seed_identity_name(
        &database,
        SEED,
        NAME,
        NAME,
        "node:pre-enrichment-token-history.eth",
        registrar,
        Uuid::from_u128(0x8180),
        Uuid::from_u128(0x9180),
        "0x0000000000000000000000000000000000007180",
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(registry, None, "0xresource", 99)],
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=121).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
        SET block_hash = '0xhistory120', block_number = 120,
            active_from = to_timestamp(1700000120), active_to = to_timestamp(1700000121)
        WHERE resource_id = $1",
    )
    .bind(registrar)
    .execute(&database.pool)
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            Uuid::from_u128(0x9181),
            SEED,
            registry,
            "0xhistory121",
            121,
            1_700_000_121,
        )],
    )
    .await?;
    let mut events = Vec::new();
    for (suffix, resource, block_number) in
        [("registrar", registrar, 120), ("registry", registry, 121)]
    {
        let mut event = v2_history_event(
            &format!("pre-enrichment-token-{suffix}"),
            None,
            Some(resource),
            "ResolverChanged",
            block_number,
        );
        event.source_family = "ens_v1_registry_l1".to_owned();
        event.after_state = json!({"source_event": "NewResolver",
            "node": "node:pre-enrichment-token-history.eth",
            "authority_kind": if resource == registrar { "registrar" } else { "registry_only" },
            "resolver": "0x00000000000000000000000000000000000000aa"});
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // Each event lies inside its resource's non-overlapping canonical binding
    // epoch, so both rows retain identity. The public-handle check distinguishes token lineage.
    let name_rows = bigname_storage::load_name_history(
        &database.pool,
        &logical_name_id,
        &[registrar, registry],
        bigname_storage::HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(name_rows.len(), 2, "{name_rows:?}");
    for resource in [registrar, registry] {
        assert!(
            name_rows
                .iter()
                .any(|row| row.registration_id == Some(resource)),
            "the identity mapper must not hide the handle eligibility regression: {name_rows:?}"
        );
    }
    // There is no grant, wrapper event or event-level name attribution for either
    // resource. Accepting arbitrary bound resources fails the negative assertion.
    for (resource, count) in [(registrar, 1), (registry, 0)] {
        let payload = v2_history_payload_for_database(
            &database,
            &format!("/v2/events?registration_id={resource}&page_size=20"),
        )
        .await?;
        let rows = payload["data"]
            .as_array()
            .expect("registration history rows");
        assert_eq!(rows.len(), count, "{resource}: {payload:?}");
        assert_eq!(payload["page"]["has_more"], json!(false));
        if resource == registrar {
            assert_eq!(rows[0]["type"], json!("resolver"));
            assert_eq!(rows[0]["registration_id"], json!(registrar.to_string()));
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn reserved_token_resource_is_not_a_public_registration_handle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = Uuid::from_u128(0x7190);
    let token = Uuid::from_u128(0x8190);
    seed_v2_history_blocks(&database, 120..=122).await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(token, "0xhistory120", 120)],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            resource,
            Some(token),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    // Match the reservation producer: token identity, no binding or grant, then
    // resource-bearing expiry and resolver updates while still reserved.
    let mut events = Vec::new();
    for (kind, source_event, number) in [
        ("RegistrationReserved", "LabelReserved", 120),
        ("ExpiryChanged", "ExpiryUpdated", 121),
        ("ResolverChanged", "ResolverUpdated", 122),
    ] {
        let mut event = v2_history_event(
            &format!("reserved-handle-{number}"),
            None,
            Some(resource),
            kind,
            number,
        );
        event.source_family = "ens_v2_registry_l1".to_owned();
        event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        event.after_state["source_event"] = json!(source_event);
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    for canonical_only in [true, false] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id: Some(resource),
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert!(rows.is_empty(), "reservation acquired a handle: {rows:?}");
        let payload = v2_history_payload_for_database(
            &database,
            &format!("/v2/events?registration_id={resource}&page_size=1"),
        )
        .await?;
        assert_eq!(payload["data"], json!([]), "{payload:?}");
        assert_eq!(payload["page"]["has_more"], json!(false));
    }
    let diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v2/diagnostics/events?registration_id={resource}&page_size=20"),
    )
    .await?;
    assert_eq!(
        diagnostics["data"]
            .as_array()
            .expect("diagnostic rows")
            .len(),
        3
    );
    database.cleanup().await
}

#[tokio::test]
async fn reservation_product_rows_omit_registration_identity() -> Result<()> {
    const NAME: &str = "reserved-product-history.eth";
    const SEED: &str = "ens:reserved-product-history.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let reservation = Uuid::from_u128(0x71a0);
    let registration = reservation;
    let blocks = (120..=125)
        .map(|number| {
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{number}"),
                (number > 120)
                    .then(|| format!("0xhistory{}", number - 1))
                    .as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_identity_name(
        &database,
        SEED,
        NAME,
        NAME,
        "node:reserved-product-history.eth",
        registration,
        Uuid::from_u128(0x81a0),
        Uuid::from_u128(0x91a1),
        "0x00000000000000000000000000000000000071a1",
        bigname_storage::AddressNameRelation::EffectiveController,
        120,
    )
    .await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            Uuid::from_u128(0x81a0),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            reservation,
            Some(Uuid::from_u128(0x81a0)),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    let mut events = Vec::new();
    // A claim can retain the reservation resource; earlier rows must stay unregistered.
    for (resource, kind, source_event, number) in [
        (reservation, "RegistrationReserved", "LabelReserved", 120),
        (reservation, "ExpiryChanged", "ExpiryUpdated", 121),
        (reservation, "ResolverChanged", "ResolverUpdated", 122),
        (registration, "RegistrationGranted", "LabelRegistered", 123),
        (registration, "ExpiryChanged", "ExpiryUpdated", 124),
        (registration, "ResolverChanged", "ResolverUpdated", 125),
    ] {
        let mut event = v2_history_event(
            &format!("reservation-product-{number}"),
            Some(&logical_name_id),
            Some(resource),
            kind,
            number,
        );
        event.source_family = "ens_v2_registry_l1".to_owned();
        event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        event.after_state["source_event"] = json!(source_event);
        if kind == "RegistrationGranted" {
            event.after_state["authority_kind"] = json!("ens_v2_registry");
        }
        events.push(event);
    }
    for number in [122, 125] {
        let mut bridge = v2_history_event(
            &format!("reservation-product-bridge-{number}"),
            Some(&logical_name_id),
            Some(registration),
            "RegistrationRenewed",
            number,
        );
        bridge.source_family = "ens_v2_migration_l1".to_owned();
        bridge.derivation_kind = "ens_v2_migration".to_owned();
        bridge.log_index = Some(1);
        bridge.after_state = json!({"source_event":"NameRenewed","duration":100});
        events.push(bridge);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET derivation_kind = 'ens_v2_migration' WHERE event_identity LIKE 'reservation-product-bridge-%'")
        .execute(&database.pool).await?;
    for canonical_only in [true, false] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                logical_name_id: Some(logical_name_id.clone()),
                event_kinds: vec![
                    "RegistrationGranted".to_owned(),
                    "RegistrationRenewed".to_owned(),
                    "ExpiryChanged".to_owned(),
                    "ResolverChanged".to_owned(),
                ],
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert_eq!(rows.len(), 7, "canonical_only={canonical_only}: {rows:?}");
        for row in rows {
            let expected = (row.block_number.expect("block number") >= 123).then_some(registration);
            assert_eq!(row.registration_id, expected, "{row:?}");
        }
    }
    for route in [
        format!("/v2/names/{NAME}/history?scope=name&page_size=20"),
        format!("/v2/names/{NAME}/history?scope=both&page_size=20"),
        format!("/v2/events?name={NAME}&page_size=20"),
        "/v2/events?page_size=20".to_owned(),
    ] {
        let payload = v2_history_payload_for_database(&database, &route).await?;
        let rows = payload["data"].as_array().expect("product history rows");
        assert_eq!(rows.len(), 7, "{route}: {payload:?}");
        for row in rows {
            let number = row["block_number"].as_i64().expect("block number");
            let expected = if number < 123 {
                Value::Null
            } else {
                json!(registration)
            };
            assert_eq!(row["registration_id"], expected, "{route}: {row:?}");
        }
    }
    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v2/events?registration_id={registration}&page_size=20"),
    )
    .await?;
    assert_eq!(
        payload["data"]
            .as_array()
            .expect("registration history")
            .len(),
        4
    );
    let diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v2/diagnostics/events?registration_id={reservation}&page_size=20"),
    )
    .await?;
    let rows = diagnostics["data"].as_array().expect("diagnostic rows");
    assert_eq!(rows.len(), 8);
    assert!(
        rows.iter()
            .all(|row| row["registration_id"] == json!(reservation))
    );
    database.cleanup().await
}

#[tokio::test]
async fn retained_registrar_resolver_survives_resource_reanchoring() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = Uuid::from_u128(0x7191);
    let token = Uuid::from_u128(0x8191);
    let mut blocks = vec![raw_block(
        "ethereum-mainnet",
        "0xreanchor-common",
        None,
        129,
        1_700_000_129,
    )];
    for branch in ["a", "b"] {
        for number in 130..=131 {
            let parent = if number == 130 {
                "0xreanchor-common".to_owned()
            } else {
                format!("0xreanchor-{branch}-130")
            };
            let mut block = raw_block(
                "ethereum-mainnet",
                &format!("0xreanchor-{branch}-{number}"),
                Some(&parent),
                number,
                1_700_000_000 + number,
            );
            if branch == "a" {
                block.canonicality_state = CanonicalityState::Orphaned;
            }
            blocks.push(block);
        }
    }
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(token, "0xreanchor-a-130", 130)],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            resource,
            Some(token),
            "0xreanchor-a-130",
            130,
        )],
    )
    .await?;
    let mut events = Vec::new();
    for branch in ["a", "b"] {
        let mut event = v2_history_event(
            &format!("reanchor-{branch}"),
            None,
            Some(resource),
            "ResolverChanged",
            131,
        );
        event.source_family = "ens_v1_registry_l1".to_owned();
        event.block_hash = Some(format!("0xreanchor-{branch}-131"));
        event.transaction_hash = Some(format!("0xreanchor-{branch}-tx"));
        event.after_state["source_event"] = json!("NewResolver");
        event.after_state["authority_kind"] = json!("registrar");
        if branch == "a" {
            event.canonicality_state = CanonicalityState::Orphaned;
        }
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // Reproduce the identity writer's post-reorg result. The normalized event
    // on A remains, while both singleton identity anchors now refer to B.
    sqlx::query(
        "UPDATE bigname_phase.resources SET block_hash = '0xreanchor-b-130',
         block_number = 130, canonicality_state = 'canonical' WHERE resource_id = $1",
    )
    .bind(resource)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.token_lineages SET block_hash = '0xreanchor-b-130',
         block_number = 130, canonicality_state = 'canonical' WHERE token_lineage_id = $1",
    )
    .bind(token)
    .execute(&database.pool)
    .await?;
    for (canonical_only, count) in [(true, 1), (false, 2)] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id: Some(resource),
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert_eq!(rows.len(), count, "retained branch history: {rows:?}");
        for summary_mode in [
            bigname_storage::HistorySummaryMode::Count,
            bigname_storage::HistorySummaryMode::Full,
        ] {
            let page = bigname_storage::load_event_history_page(
                &database.pool,
                bigname_storage::EventHistoryFilter {
                    resource_id: Some(resource),
                    ..Default::default()
                },
                canonical_only,
                None,
                1,
                summary_mode,
                false,
            )
            .await?;
            assert_eq!(
                page.summary.as_ref().expect("summary").total_count,
                count as u64
            );
            assert_eq!(page.rows.len(), 1);
            if canonical_only {
                assert_eq!(page.rows[0].event_identity, "reanchor-b");
                assert!(page.next_cursor.is_none());
            } else {
                let next = bigname_storage::load_event_history_page(
                    &database.pool,
                    bigname_storage::EventHistoryFilter {
                        resource_id: Some(resource),
                        ..Default::default()
                    },
                    false,
                    page.next_cursor.as_ref(),
                    1,
                    summary_mode,
                    false,
                )
                .await?;
                assert!(page.next_cursor.is_some());
                assert_eq!(next.rows.len(), 1);
                let mut identities = vec![
                    page.rows[0].event_identity.as_str(),
                    next.rows[0].event_identity.as_str(),
                ];
                identities.sort();
                assert_eq!(identities, vec!["reanchor-a", "reanchor-b"]);
                assert!(next.next_cursor.is_none());
            }
        }
        if canonical_only {
            let payload = v2_history_payload_for_database(
                &database,
                &format!("/v2/events?registration_id={resource}&page_size=1"),
            )
            .await?;
            assert_eq!(
                history_transaction_hashes(&payload),
                vec!["0xreanchor-b-tx"]
            );
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn block_only_reservation_release_omits_registration_identity() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let reserved = Uuid::from_u128(0x71b0);
    let registered = Uuid::from_u128(0x71b1);
    let current = raw_block(
        "ethereum-mainnet",
        "0xhistory120",
        Some("0xhistory119"),
        120,
        1_700_000_120,
    );
    upsert_phase_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xhistory119", None, 119, 1_700_000_119),
            current,
        ],
    )
    .await?;
    for (resource, token, number) in [
        (reserved, Uuid::from_u128(0x81b0), 120),
        (registered, Uuid::from_u128(0x81b1), 119),
    ] {
        upsert_test_token_lineages(
            &database.pool,
            &[address_name_token_lineage(
                token,
                &format!("0xhistory{number}"),
                number,
            )],
        )
        .await?;
        upsert_test_resources(
            &database.pool,
            &[address_name_resource(
                resource,
                Some(token),
                &format!("0xhistory{number}"),
                number,
            )],
        )
        .await?;
    }
    let mut events = Vec::new();
    for (resource, kind, source_event, number) in [
        (reserved, "RegistrationReserved", "LabelReserved", 120),
        (registered, "RegistrationGranted", "LabelRegistered", 119),
    ] {
        let mut event = v2_history_event(
            &format!("block-only-origin-{resource}"),
            None,
            Some(resource),
            kind,
            number,
        );
        event.source_family = "ens_v2_registry_l1".to_owned();
        event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        event.log_index = Some(7);
        event.after_state["source_event"] = json!(source_event);
        events.push(event);
    }
    for (resource, status) in [(reserved, "reserved"), (registered, "registered")] {
        let mut release = v2_history_event(
            &format!("block-only-release-{resource}"),
            None,
            Some(resource),
            "RegistrationReleased",
            120,
        );
        release.source_family = "ens_v2_registry_l1".to_owned();
        release.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        release.log_index = None;
        release.transaction_hash = None;
        release.raw_fact_ref = json!({"kind":"raw_block","chain_id":"ethereum-mainnet","block_hash":"0xhistory120","block_number":120});
        release.before_state = json!({"status":status,"expiry":1_700_000_120});
        release.after_state = json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released","expiry":1_700_000_120});
        events.push(release);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    for canonical_only in [true, false] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                event_kinds: vec!["RegistrationReleased".to_owned()],
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert_eq!(rows.len(), 2, "{rows:?}");
        for row in rows {
            let expected = row
                .event_identity
                .ends_with(&registered.to_string())
                .then_some(registered);
            assert_eq!(row.registration_id, expected, "{row:?}");
            assert!(row.log_index.is_none());
        }
    }
    let payload = v2_history_payload_for_database(&database, "/v2/events?page_size=20").await?;
    let rows = payload["data"].as_array().expect("product rows");
    let releases = rows
        .iter()
        .filter(|row| row["type"] == "release")
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 2, "{payload:?}");
    assert_eq!(
        releases
            .iter()
            .filter(|row| row["registration_id"].is_null())
            .count(),
        1,
        "{payload:?}"
    );
    assert_eq!(
        releases
            .iter()
            .filter(|row| row["registration_id"] == json!(registered))
            .count(),
        1,
        "{payload:?}"
    );
    database.cleanup().await
}
