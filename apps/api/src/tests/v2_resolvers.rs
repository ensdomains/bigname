const V2_RESOLVER_ADDRESS: &str = "0x0000000000000000000000000000000000000aaa";
const DIVERGENT_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000d01";
const DIVERGENT_CONTROL_OWNER: &str = "0x0000000000000000000000000000000000000d02";
const DIVERGENT_REGISTRATION_REGISTRANT: &str = "0x0000000000000000000000000000000000000d03";
const DIVERGENT_CONTROL_REGISTRANT: &str = "0x0000000000000000000000000000000000000d04";

#[test]
fn v2_bound_names_cursor_payload_round_trips_storage_cursor() {
    let cursor = v2_bound_names_cursor();
    let binding = v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, "snapshot-1");
    let next = crate::v2::bound_names_next_cursor(&cursor, &binding);
    let payload = crate::v2::decode(&next).expect("issued cursor decodes");

    assert_eq!(payload.sort, "name_asc");
    assert_eq!(
        payload.filters,
        std::collections::BTreeMap::from([
            ("chain_id".to_owned(), "1".to_owned()),
            ("resolver".to_owned(), V2_RESOLVER_ADDRESS.to_owned()),
            ("namespace".to_owned(), "ens".to_owned()),
        ])
    );
    assert_eq!(
        crate::v2::bound_names_storage_cursor(&next, &binding).expect("cursor must decode"),
        cursor
    );
}

#[test]
fn v2_bound_names_cursor_rejects_wrong_chain_resolver_sort_or_snapshot() {
    let cursor = v2_bound_names_cursor();
    let binding = v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, "snapshot-1");
    let issued = || {
        crate::v2::decode(&crate::v2::bound_names_next_cursor(&cursor, &binding))
            .expect("issued cursor decodes")
    };
    let refused = |payload: &crate::v2::CursorPayload| {
        crate::v2::bound_names_storage_cursor(&crate::v2::encode(payload), &binding).is_err()
    };

    let mut payload = issued();
    payload.sort = "wrong".to_owned();
    assert!(refused(&payload));

    let mut payload = issued();
    payload
        .filters
        .insert("chain_id".to_owned(), "8453".to_owned());
    assert!(refused(&payload));

    let mut payload = issued();
    payload.filters.insert(
        "resolver".to_owned(),
        "0x0000000000000000000000000000000000000bbb".to_owned(),
    );
    assert!(refused(&payload));

    let mut payload = issued();
    payload.snapshot = Some("snapshot-2".to_owned());
    assert!(refused(&payload));
}

#[test]
fn v2_resolver_overview_serves_no_counts_or_sections() {
    // The stored summaries still carry every section and count; the overview serves none.
    let overview = crate::v2::build_resolver_overview(
        resolver_current_row_with_writer_alias("ethereum-mainnet", V2_RESOLVER_ADDRESS),
        1,
        empty_bound_names(),
    );
    let value = serde_json::to_value(overview).expect("overview must serialize");
    let keys = value
        .as_object()
        .expect("overview must be an object")
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        std::collections::BTreeSet::from(["address", "bound_names", "chain_id"])
    );
}

#[tokio::test]
async fn v2_get_resolver_returns_overview_with_nested_bound_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;

    let first_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1"),
    )
    .await?;

    assert!(first_page.get("page").is_none());
    assert_eq!(first_page["meta"]["as_of"]["1"]["block_number"], json!(203));
    assert_eq!(first_page["data"]["chain_id"], json!(1));
    assert_eq!(first_page["data"]["address"], json!(V2_RESOLVER_ADDRESS));
    for removed in ["counts", "nodes", "aliases", "links", "roles", "events"] {
        assert!(first_page["data"].get(removed).is_none(), "{removed}");
    }
    assert!(first_page["meta"].get("unsupported_fields").is_none());

    let bound_names = &first_page["data"]["bound_names"];
    assert_eq!(bound_names["page"]["cursor"], Value::Null);
    assert_eq!(bound_names["page"]["page_size"], json!(1));
    assert_eq!(bound_names["page"]["total_count"], Value::Null);
    assert_eq!(bound_names["page"]["has_more"], json!(true));
    let next_cursor = bound_names["page"]["next_cursor"]
        .as_str()
        .expect("first page must provide a nested cursor");
    assert_eq!(bound_names["data"][0]["name"], json!("alpha.eth"));
    assert_eq!(bound_names["data"][0]["display_name"], json!("alpha.eth"));
    assert_eq!(bound_names["data"][0]["namespace"], json!("ens"));
    assert_eq!(
        bound_names["data"][0]["namehash"],
        json!(bigname_lookup::ens_namehash_hex("alpha.eth")?)
    );
    assert_eq!(
        bound_names["data"][0]["owner"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(
        bound_names["data"][0]["registrant"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(
        bound_names["data"][0]["registered_at"],
        json!("2024-01-02T00:00:00+00:00")
    );
    assert_eq!(
        bound_names["data"][0]["created_at"],
        json!("2023-01-02T00:00:00+00:00")
    );
    assert_eq!(
        bound_names["data"][0]["expires_at"],
        json!("2027-01-02T00:00:00Z")
    );
    assert_eq!(
        bound_names["data"][0]["resolver"],
        json!({
            "chain_id": 1,
            "address": V2_RESOLVER_ADDRESS,
        })
    );

    let second_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(
        second_page["data"]["bound_names"]["data"][0]["name"],
        json!("beta.eth")
    );
    assert_eq!(
        second_page["data"]["bound_names"]["page"]["has_more"],
        json!(false)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_rejects_include_and_serves_no_sampled_sections() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    for removed in ["counts", "nodes", "aliases", "links", "roles", "events"] {
        assert!(payload["data"].get(removed).is_none(), "{removed}");
    }

    for include in ["nodes", "nodes,aliases,roles", "links", "events"] {
        let response = v2_resolver_response_for_database(
            &database,
            &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?include={include}"),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{include}");
        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "invalid_input");
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_rejects_pre_unification_cursor_snapshot_binding() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let old_resolver_snapshot_token = v2_at_token(
        "ethereum-mainnet",
        "ethereum-mainnet",
        102,
        "0xname66",
        "2026-04-17T00:00:02Z",
    )?;
    let old_cursor = crate::v2::bound_names_next_cursor(
        &v2_bound_names_cursor(),
        &v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, &old_resolver_snapshot_token),
    );

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1&cursor={old_cursor}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "cursor must be a valid pagination cursor"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_returns_empty_bound_names_when_overview_exists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;

    assert_eq!(payload["data"]["bound_names"]["data"], json!([]));
    assert_eq!(
        payload["data"]["bound_names"]["page"]["has_more"],
        json!(false)
    );
    assert!(payload.get("page").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_omits_names_without_projected_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET support_status = 'unsupported',
             unsupported_reason = 'current_authority_not_projected'
         WHERE raw_name = 'alpha.eth'",
    )
    .execute(&database.pool)
    .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound names must be an array")
        .iter()
        .map(|row| row["name"].as_str().expect("bound name must be text"))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["beta.eth"]);

    database.cleanup().await?;
    Ok(())
}

/// Bound-name membership follows the name's selected resolver in `name_current`, never the
/// record inventory. A mirror row that Project refused because the nearest ENSv1 resolver is a
/// non-extended ancestor leaves the listing unchanged.
#[tokio::test]
async fn v2_get_resolver_bound_names_ignore_an_ancestor_rejected_mirror_inventory() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let uri = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}");
    let before = v2_resolver_payload_for_database(&database, &uri).await?;

    let spec = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == "alpha.eth")
        .expect("fixture seeds alpha.eth");
    let mut inventory = address_name_record_inventory_current_row(&spec);
    ancestor_rejected_mirror_inventory(V2_RESOLVER_ADDRESS, &mut inventory);
    database
        .insert_record_inventory_current_row(inventory)
        .await?;
    let after = v2_resolver_payload_for_database(&database, &uri).await?;

    assert!(
        after["data"]["bound_names"]["data"]
            .as_array()
            .expect("bound names must be an array")
            .iter()
            .any(|row| row["name"] == "alpha.eth"),
        "{after}"
    );
    assert_eq!(after["data"]["bound_names"], before["data"]["bound_names"]);

    database.cleanup().await?;
    Ok(())
}

/// A `.eth` lease that lapsed under a registry-only binding is released like any other lapse, so
/// the resolver its registry owner set no longer lists the name among its bound names.
#[tokio::test]
async fn v2_get_resolver_omits_a_lapsed_handed_off_name_from_bound_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 declared_summary,
                 '{registration}',
                 (declared_summary -> 'registration') || jsonb_build_object(
                     'status', 'released',
                     'authority_kind', 'registry_only',
                     'released_at', '2026-06-14T00:00:00Z',
                     'registrant', NULL,
                     'expiry', NULL
                 )
             ),
             '{control}',
             '{\"status\": \"unregistered\"}'::jsonb
         )
         WHERE raw_name = 'alpha.eth'",
    )
    .execute(&database.pool)
    .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound names must be an array")
        .iter()
        .map(|row| row["name"].as_str().expect("bound name must be text"))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["beta.eth"], "{payload}");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_lists_a_root_registry_pointer_without_projected_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    // An ENSv2 TLD whose root-registry token has a resolver pointer but no observed registration.
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET support_status = 'unsupported',
             unsupported_reason = 'current_authority_not_projected',
             serving_resource_id = resource_id,
             resource_id = NULL,
             surface_binding_id = NULL,
             token_lineage_id = NULL,
             binding_kind = NULL,
             provenance = provenance || jsonb_build_object(
                 'read_reachability', jsonb_build_object(
                     'basis', 'root_registry_resolver_pointer'))
         WHERE raw_name = 'alpha.eth'",
    )
    .execute(&database.pool)
    .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound names must be an array")
        .iter()
        .map(|row| row["name"].as_str().expect("bound name must be text"))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["alpha.eth", "beta.eth"], "{payload}");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_omits_ownerless_reservations_from_bound_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET surface_binding_id = NULL,
             resource_id = NULL,
             token_lineage_id = NULL,
             binding_kind = NULL,
             declared_summary =
                 jsonb_set(declared_summary, '{registration,status}', '\"active\"')
         WHERE raw_name = 'alpha.eth'",
    )
    .execute(&database.pool)
    .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound names must be an array")
        .iter()
        .map(|row| row["name"].as_str().expect("bound name must be text"))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["beta.eth"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_serves_phase_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 204,
                "block_hash": "0xresolvercc",
                "timestamp": "2026-04-17T00:00:24Z",
            }
        }))
        .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 204, "0xresolvercc").await?;
    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;

    assert_eq!(payload["data"]["address"], json!(V2_RESOLVER_ADDRESS));
    assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(204));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_rejects_bound_name_from_another_phase_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    sqlx::query(
        r#"
        UPDATE name_current
        SET chain_positions = jsonb_set(
            jsonb_set(
                chain_positions,
                '{ethereum,block_number}',
                '204'::jsonb
            ),
            '{ethereum,block_hash}',
            '"0xresolvercc"'::jsonb
        )
        WHERE raw_name = 'alpha.eth'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=50"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "stale");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_excludes_ownerless_name_when_bindings_are_unsupported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    sqlx::query(
        r#"UPDATE bigname_phase.resolver_current
         SET declared_summary = jsonb_set(
             declared_summary, '{bindings,status}', '"unsupported"'::jsonb)
         WHERE chain_id = 'ethereum-mainnet' AND resolver_address = lower($1)"#,
    )
    .bind(V2_RESOLVER_ADDRESS)
    .execute(&database.pool)
    .await?;
    let updated = sqlx::query(
        r#"UPDATE bigname_phase.name_current
         SET serving_resource_id = resource_id, surface_binding_id = NULL,
             resource_id = NULL, token_lineage_id = NULL, binding_kind = NULL,
             declared_summary = jsonb_set(
                 jsonb_set(declared_summary, '{registration,status}', '"unregistered"'::jsonb),
                 '{control,status}', '"unregistered"'::jsonb),
             provenance = provenance || jsonb_build_object(
                 'read_reachability', jsonb_build_object(
                     'basis', 'retained_registry_resolver_pointer'))
         WHERE raw_name = 'alpha.eth'"#,
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);
    let ownerless_shape: bool = sqlx::query_scalar(
        r#"SELECT surface_binding_id IS NULL
               AND resource_id IS NULL
               AND serving_resource_id IS NOT NULL
               AND provenance #>> '{read_reachability,basis}' =
                   'retained_registry_resolver_pointer'
           FROM bigname_phase.name_current
           WHERE raw_name = 'alpha.eth'"#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert!(
        ownerless_shape,
        "fixture must model event-linked ownerless serving"
    );

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    assert!(names.iter().all(|row| row["name"] != "alpha.eth"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_resolver_excludes_unclassified_serving_resource_row() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let updated = sqlx::query(
        r#"UPDATE bigname_phase.name_current
         SET serving_resource_id = resource_id, surface_binding_id = NULL,
             resource_id = NULL, token_lineage_id = NULL, binding_kind = NULL,
             declared_summary = jsonb_set(
                 jsonb_set(declared_summary, '{registration,status}', '"reserved"'::jsonb),
                 '{registration,authority_kind}', '"ens_v2_registry"'::jsonb)
         WHERE raw_name = 'alpha.eth'"#,
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    assert!(names.iter().all(|row| row["name"] != "alpha.eth"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_resolver_includes_ownerless_name_when_bindings_are_supported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let updated = sqlx::query(
        r#"UPDATE bigname_phase.name_current
         SET serving_resource_id = resource_id, surface_binding_id = NULL,
             resource_id = NULL, token_lineage_id = NULL, binding_kind = NULL,
             declared_summary = jsonb_set(
                 jsonb_set(declared_summary, '{registration,status}', '"unregistered"'::jsonb),
                 '{control,status}', '"unregistered"'::jsonb),
             provenance = provenance || jsonb_build_object(
                 'read_reachability', jsonb_build_object(
                     'basis', 'retained_registry_resolver_pointer'))
         WHERE raw_name = 'alpha.eth'"#,
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);
    let ownerless_shape: bool = sqlx::query_scalar(
        r#"SELECT surface_binding_id IS NULL
               AND resource_id IS NULL
               AND serving_resource_id IS NOT NULL
               AND provenance #>> '{read_reachability,basis}' =
                   'retained_registry_resolver_pointer'
           FROM bigname_phase.name_current
           WHERE raw_name = 'alpha.eth'"#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert!(
        ownerless_shape,
        "fixture must model event-linked ownerless serving"
    );

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    assert!(
        names.iter().any(|row| row["name"] == "alpha.eth"),
        "supported ownerless row missing: {payload}"
    );

    database.cleanup().await
}

#[test]
fn v2_bound_name_presentation_preserves_dictionary_precedence_and_wrapper_flags() -> Result<()> {
    let flags = json!({
        "fuses":65537, "cannot_unwrap":true, "cannot_burn_fuses":false,
        "cannot_transfer":false, "cannot_set_resolver":false, "cannot_set_ttl":false,
        "cannot_create_subdomain":false, "cannot_approve":false, "parent_cannot_control":true,
        "is_dot_eth":false, "can_extend_expiry":false
    });
    // Deliberately divergent dictionary fields test the renderer's precedence, without claiming
    // that a single protocol event produces all four independent owner/registrant values.
    let row = address_name_name_current_row(
        "ens:precedence.eth",
        "precedence.eth",
        "precedence.eth",
        "node:precedence.eth",
        Uuid::from_u128(0xe102),
        Uuid::from_u128(0xe100),
        Some(Uuid::from_u128(0xe101)),
        203,
        json!({"registration":{"status":"active", "authority_kind":"registrar", "registrant":DIVERGENT_REGISTRATION_REGISTRANT},
            "control":{"registry_owner":DIVERGENT_REGISTRY_OWNER,"owner":DIVERGENT_CONTROL_OWNER,"registrant":DIVERGENT_CONTROL_REGISTRANT},
            "wrapper_state":"locked", "wrapper_fuses":flags}),
    );
    // The bound-name adapter delegates to this same name-record renderer.
    let record = serde_json::to_value(
        crate::v2::build_name_record(&row, None, Some(1), crate::v2::Status::Ok)
            .map_err(|error| anyhow::anyhow!("{error:?}"))?,
    )?;
    assert_eq!(record["owner"], json!(DIVERGENT_CONTROL_OWNER));
    assert_eq!(
        record["registrant"],
        json!(DIVERGENT_REGISTRATION_REGISTRANT)
    );
    assert_eq!(record["registration_status"], json!("active"));
    assert_eq!(record["wrapper_state"], json!("locked"));
    assert_eq!(record["wrapper_fuses"], flags);
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_serves_an_unsupported_resolver_without_section_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = unsupported_resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    seed_v2_resolver_overview(&database, false).await?;

    let produced = bigname_storage::families::topology::load_family_resolver_current(
        &database.pool,
        "ethereum-mainnet",
        V2_RESOLVER_ADDRESS,
    )
    .await?
    .context("observed resolver classification")?;
    assert_eq!(produced.coverage["status"], json!("unsupported"));

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;

    assert!(
        payload["meta"]["as_of"]["1"].is_object(),
        "resolver meta must preserve as_of"
    );
    assert!(
        payload["meta"]["as_of_token"].is_string(),
        "resolver meta must preserve the snapshot token"
    );
    // The overview no longer describes the stored sections, so their support and reasons are
    // not reported: the collections carry their own completeness.
    for field in ["unsupported_fields", "completeness", "unsupported_reason"] {
        assert!(payload["meta"].get(field).is_none(), "{field}");
    }
    assert!(!payload.to_string().contains("resolver_family_pending"));
    assert_eq!(payload["data"]["bound_names"]["data"], json!([]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_filters_bound_names_by_declared_resolver_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &["ethereum-mainnet", "base-mainnet"],
    )
    .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let rows = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");

    assert_eq!(names(rows), vec!["alpha.eth"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_excludes_lower_height_orphaned_project_targets() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &["ethereum-mainnet", "base-mainnet"],
    )
    .await?;
    seed_v2_resolver_overview(&database, true).await?;
    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES
            ('ethereum-mainnet', '0xorphaned-bound-name-target', 201,
             '2026-04-17T00:00:21Z', 'orphaned'),
            ('ethereum-mainnet', '0xorphaned-resolver-target', 202,
             '2026-04-17T00:00:22Z', 'orphaned');
        UPDATE bigname_phase.name_current
        SET canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 201,
                'target_block_hash', '0xorphaned-bound-name-target'
            )
        WHERE lower(raw_name) = 'alpha.eth';
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;

    let names_payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    assert_eq!(names_payload["data"]["bound_names"]["data"], json!([]));

    sqlx::query(
        r#"
        UPDATE bigname_phase.resolver_current
        SET chain_positions = jsonb_build_object(
                'target_block_number', 202,
                'target_block_hash', '0xorphaned-resolver-target'
            ),
            canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 202,
                'target_block_hash', '0xorphaned-resolver-target'
            )
        WHERE chain_id = 'ethereum-mainnet'
          AND lower(resolver_address) = lower($1)
        "#,
    )
    .bind(V2_RESOLVER_ADDRESS)
    .execute(&database.lookup_pool)
    .await?;

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_resolver_paginates_route_chain_rows_across_interleaved_chains() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &["ethereum-mainnet", "base-mainnet", "ethereum-mainnet"],
    )
    .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let first_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1"),
    )
    .await?;
    let first_rows = first_page["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    assert_eq!(names(first_rows), vec!["alpha.eth"]);
    let next_cursor = first_page["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .expect("first page must provide a nested cursor");

    let second_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1&cursor={next_cursor}"),
    )
    .await?;
    let second_rows = second_page["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");

    assert_eq!(names(second_rows), vec!["gamma.eth"]);
    assert_eq!(
        first_rows
            .iter()
            .chain(second_rows.iter())
            .map(|row| row["name"].as_str().expect("row must include name"))
            .collect::<Vec<_>>(),
        vec!["alpha.eth", "gamma.eth"]
    );
    assert_eq!(
        second_page["data"]["bound_names"]["page"]["next_cursor"],
        Value::Null
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_does_not_advertise_wrong_chain_lookahead_as_more() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &["ethereum-mainnet", "base-mainnet"],
    )
    .await?;
    seed_v2_resolver_overview(&database, true).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1"),
    )
    .await?;

    assert_eq!(
        names(
            payload["data"]["bound_names"]["data"]
                .as_array()
                .expect("bound_names data must be an array")
        ),
        vec!["alpha.eth"]
    );
    assert_eq!(
        payload["data"]["bound_names"]["page"]["has_more"],
        json!(false)
    );
    assert_eq!(
        payload["data"]["bound_names"]["page"]["next_cursor"],
        Value::Null
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_missing_overview_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(
            &resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS).chain_positions,
        )
        .await?;

    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 202, "0xresolverc8").await?;
    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_missing_historical_projection_returns_stale() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    seed_v2_resolver_overview(&database, true).await?;
    let initial = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let token = initial["meta"]["as_of_token"]
        .as_str()
        .expect("resolver response must include a snapshot token");
    let token_input =
        bigname_project::families::input_token(&database.pool, "ethereum-mainnet").await?;
    let mut options = bigname_project::families::FamilyOptions::new(
        bigname_content_hash::INTERPRETER_CONTENT_HASH,
    );
    options.max_blocks_per_run = 0;
    bigname_project::families::apply(
        &database.pool,
        "ethereum-mainnet",
        &bigname_project::Marker {
            number: 203,
            hash: "0xresolvercb".into(),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token_input,
        &options,
    )
    .await?;

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?at={token}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "stale");

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_resolver_rejects_malformed_input() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for uri in [
        format!("/v1/resolvers/ethereum-mainnet/{V2_RESOLVER_ADDRESS}"),
        format!("/v1/resolvers/99999999/{V2_RESOLVER_ADDRESS}"),
        "/v1/resolvers/1/not-an-address".to_owned(),
        format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?include=records"),
        format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    ] {
        let response = v2_resolver_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "invalid_input");
    }

    database.cleanup().await?;
    Ok(())
}

async fn v2_resolver_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_resolver_response_for_database(database, uri).await?;
    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    Ok(payload)
}

async fn v2_resolver_response_for_database(database: &TestDatabase, uri: &str) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 resolver request failed")
}

async fn upsert_test_resolver_current_rows(
    database: &TestDatabase,
    rows: &[ResolverCurrentRow],
) -> Result<()> {
    upsert_phase_resolver_current_rows(&database.pool, rows).await?;
    for row in rows {
        let mut declared_summary = row.declared_summary.clone();
        if let Some(items) = declared_summary
            .pointer_mut("/bindings/items")
            .and_then(Value::as_array_mut)
        {
            for item in items {
                let Some(namespace) = item.get("namespace").and_then(Value::as_str) else {
                    continue;
                };
                let Some(name) = item
                    .get("normalized_name")
                    .or_else(|| item.get("raw_name"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let namehash = bigname_lookup::ens_namehash_hex(name)?;
                item["logical_name_id"] = json!(format!("{namespace}:{namehash}"));
                item["namehash"] = json!(namehash);
            }
        }
        let (target_number, target_hash): (i64, String) = sqlx::query_as(
            r#"
            SELECT target_block_number, target_block_hash
            FROM chain_phase_state
            WHERE chain_id = $1
              AND phase_name = 'project'
              AND phase_status = 'completed'
            "#,
        )
        .bind(&row.chain_id)
        .fetch_one(&database.lookup_pool)
        .await?;
        let support_status = if row.coverage["status"] == json!("unsupported") {
            "unsupported"
        } else {
            "supported"
        };
        let unsupported_reason = (support_status == "unsupported")
            .then(|| {
                row.coverage["unsupported_reason"]
                    .as_str()
                    .map(str::to_owned)
            })
            .flatten()
            .or_else(|| {
                (support_status == "unsupported")
                    .then(|| "resolver_overview_not_supported".to_owned())
            });
        sqlx::query(
            r#"
            INSERT INTO resolver_current (
                chain_id, resolver_address, declared_summary, support_status,
                unsupported_reason, provenance, chain_positions,
                canonicality_summary, manifest_version
            ) VALUES (
                $1, lower($2), $3, $4, $5, $6,
                jsonb_build_object(
                    'target_block_number', $7::BIGINT,
                    'target_block_hash', $8::TEXT
                ),
                jsonb_build_object(
                    'state', 'canonical_lineage',
                    'target_block_number', $7::BIGINT,
                    'target_block_hash', $8::TEXT
                ),
                $9
            )
            ON CONFLICT (chain_id, resolver_address) DO UPDATE SET
                declared_summary = EXCLUDED.declared_summary,
                support_status = EXCLUDED.support_status,
                unsupported_reason = EXCLUDED.unsupported_reason,
                provenance = EXCLUDED.provenance,
                chain_positions = EXCLUDED.chain_positions,
                canonicality_summary = EXCLUDED.canonicality_summary,
                manifest_version = EXCLUDED.manifest_version,
                last_recomputed_at = now()
            "#,
        )
        .bind(&row.chain_id)
        .bind(&row.resolver_address)
        .bind(&declared_summary)
        .bind(support_status)
        .bind(unsupported_reason)
        .bind(&row.provenance)
        .bind(target_number)
        .bind(&target_hash)
        .bind(row.manifest_version)
        .execute(&database.lookup_pool)
        .await?;
    }
    Ok(())
}

async fn seed_v2_resolver_bound_names_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_resolver_bound_names_fixture_with_chains(
        database,
        &["ethereum-mainnet", "ethereum-mainnet"],
    )
    .await
}

/// Names on each requested chain have actual identity, registry, registrar and resolver inputs.
/// Base uses its Basenames namespace; chain filtering is never fabricated in a name summary.
async fn seed_v2_resolver_bound_names_fixture_with_chains(
    database: &TestDatabase,
    resolver_chains: &[&str],
) -> Result<()> {
    let specs = v2_address_name_specs();
    anyhow::ensure!(
        resolver_chains.len() <= 3,
        "use the explicit precedence unit fixture"
    );
    for chain in ["ethereum-mainnet", "base-mainnet"] {
        let selected: Vec<_> = specs
            .iter()
            .zip(resolver_chains)
            .filter(|(_, selected)| **selected == chain)
            .collect();
        if selected.is_empty() {
            continue;
        }
        let (namespace, arm, registry, registrar, resolver_family) = if chain == "base-mainnet" {
            (
                "basenames",
                "basenames",
                "basenames_base_registry",
                "basenames_base_registrar",
                "basenames_base_resolver",
            )
        } else {
            (
                "ens",
                "ens_v1",
                "ens_v1_registry_l1",
                "ens_v1_registrar_l1",
                "ens_v1_resolver_l1",
            )
        };
        let mut times = selected
            .iter()
            .flat_map(|(spec, _)| [spec.created_at, spec.registered_at])
            .collect::<Vec<_>>();
        times.sort_unstable();
        times.dedup();
        for (index, time) in times.iter().enumerate() {
            seed_schema_v2_lookup_head(
                &database.pool,
                chain,
                100 + index as i64,
                &format!("0xresolver-input-{index}"),
                time,
            )
            .await?;
        }
        database.seed_snapshot_selector_chain_positions(&json!({"head":{
            "chain_id":chain, "block_number":203, "block_hash":"0xresolvercb", "timestamp":"2026-04-17T00:00:23Z"
        }})).await?;
        declare_family_fixture_resolver(
            &database.pool,
            namespace,
            chain,
            resolver_family,
            V2_RESOLVER_ADDRESS,
        )
        .await?;
        let mut events = Vec::new();
        for (index, (spec, _)) in selected.iter().enumerate() {
            let name = if namespace == "basenames" {
                spec.name.replace(".eth", ".base.eth")
            } else {
                spec.name.to_owned()
            };
            let created = times
                .iter()
                .position(|time| *time == spec.created_at)
                .unwrap();
            let granted = times
                .iter()
                .position(|time| *time == spec.registered_at)
                .unwrap();
            let logical = seed_family_identity_inputs(
                &database.pool,
                namespace,
                &name,
                chain,
                100 + created as i64,
                &format!("0xresolver-input-{created}"),
                spec.resource_id,
                spec.token_lineage_id,
                spec.surface_binding_id,
                arm,
            )
            .await?;
            let node = bigname_lookup::ens_namehash_hex(&name)?;
            // All three names are actively registered at this fixture's April 2026 publication.
            let expiry = if spec.name == "beta.eth" {
                "2027-01-02T00:00:00Z"
            } else {
                spec.expires_at
            };
            for (block, hash, kind, family, after) in [
                (
                    100 + created as i64,
                    format!("0xresolver-input-{created}"),
                    "AuthorityTransferred",
                    registry,
                    json!({"source_event":"Transfer", "node":node, "owner":spec.owner}),
                ),
                (
                    100 + granted as i64,
                    format!("0xresolver-input-{granted}"),
                    "RegistrationGranted",
                    registrar,
                    json!({"authority_kind":"registrar", "registrant":spec.registrant,
                        "expiry":parse_rfc3339_utc_timestamp(expiry).map_err(|error| anyhow::anyhow!("{error}"))?.unix_timestamp()}),
                ),
                (
                    203,
                    "0xresolvercb".to_owned(),
                    "ResolverChanged",
                    registry,
                    json!({"node":node, "resolver":V2_RESOLVER_ADDRESS}),
                ),
            ] {
                let mut event = history_event(
                    &format!("resolver-{chain}-{index}-{kind}"),
                    Some(&logical),
                    Some(spec.resource_id),
                    Some(chain),
                    Some(block),
                    Some(&hash),
                    Some("0xresolver-input"),
                    Some(index as i64),
                    CanonicalityState::Canonical,
                );
                event.namespace = namespace.into();
                event.source_family = family.into();
                event.event_kind = kind.into();
                event.before_state = json!({});
                event.after_state = after;
                events.push(event);
            }
        }
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
        rebuild_fixture_families(&database.pool, chain, 203, "0xresolvercb").await?;
    }
    Ok(())
}

/// Declare a resolver (or observe an undeclared one) and publish its actual classification.
/// The registry root pointer provides a candidate even when there are no bound name surfaces.
async fn seed_v2_resolver_overview(database: &TestDatabase, declared: bool) -> Result<()> {
    let chain = "ethereum-mainnet";
    let position: Option<(i64, String)> = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'"
    ).bind(chain).fetch_optional(&database.pool).await?;
    let (block, hash) = position.unwrap_or((202, "0xresolverc8".to_owned()));
    let at: Option<OffsetDateTime> = sqlx::query_scalar(
        "SELECT block_timestamp FROM chain_lineage WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(chain)
    .bind(&hash)
    .fetch_optional(&database.pool)
    .await?;
    let at = at
        .map(|at| at.format(&time::format_description::well_known::Rfc3339))
        .transpose()?
        .unwrap_or_else(|| "2026-04-17T00:00:22Z".to_owned());
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":chain, "block_number":block, "block_hash":hash, "timestamp":at
        }}))
        .await?;
    // An undeclared address under the same active family is classified as unsupported.
    declare_family_fixture_resolver(
        &database.pool,
        "ens",
        chain,
        "ens_v1_resolver_l1",
        if declared {
            V2_RESOLVER_ADDRESS
        } else {
            "0x0000000000000000000000000000000000000bbb"
        },
    )
    .await?;
    let mut event = history_event(
        "resolver-overview-root",
        None,
        None,
        Some(chain),
        Some(block),
        Some(&hash),
        Some("0xresolver-root"),
        Some(999),
        CanonicalityState::Canonical,
    );
    event.source_family = "ens_v1_registry_l1".into();
    event.event_kind = "ResolverChanged".into();
    event.before_state = json!({});
    event.after_state =
        json!({"node":format!("0x{}", "00".repeat(32)), "resolver":V2_RESOLVER_ADDRESS});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_fixture_families(&database.pool, chain, block, &hash).await
}

fn v2_bound_names_cursor() -> bigname_storage::NameCurrentListCursor {
    bigname_storage::NameCurrentListCursor {
        sort_value: bigname_storage::NameCurrentListCursorValue::Name("alice.eth".to_owned()),
        namespace: "ens".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "node:alice.eth".to_owned(),
    }
}

/// A binding pinned to the `at` token `at`.
fn v2_bound_names_cursor_binding<'a>(
    resolver_address: &'a str,
    at: &'a str,
) -> crate::v2::BoundNamesCursorBinding<'a> {
    crate::v2::BoundNamesCursorBinding {
        chain_id: 1,
        resolver_address,
        namespace: Some("ens"),
        sort: "name_asc",
        at: Some(at),
    }
}

fn empty_bound_names() -> crate::v2::BoundNames {
    crate::v2::BoundNames {
        data: Vec::new(),
        page: crate::v2::Page {
            cursor: None,
            next_cursor: None,
            page_size: 50,
            total_count: None,
            has_more: false,
        },
    }
}

fn unsupported_resolver_current_row(chain_id: &str, resolver_address: &str) -> ResolverCurrentRow {
    let mut row = resolver_current_row(chain_id, resolver_address);
    row.declared_summary = json!({
        "bindings": {
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending",
        },
        "aliases": {
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending",
        },
        "role_holders": {
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending",
        },
        "event_summary": {
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending",
        },
    });
    row
}

#[test]
fn v2_resolver_overview_reports_a_declared_ensv1_mirror() {
    let plain = crate::v2::build_resolver_overview(
        resolver_current_row("ethereum-sepolia", V2_RESOLVER_ADDRESS),
        11_155_111,
        empty_bound_names(),
    );
    assert!(
        serde_json::to_value(plain)
            .expect("overview must serialize")
            .get("mirror")
            .is_none()
    );

    let mut row = resolver_current_row("ethereum-sepolia", V2_RESOLVER_ADDRESS);
    row.declared_summary["classification"] = json!({
        "source_family": "ens_v2_resolver_l1",
        "role": "ensv1_mirror_resolver",
        "basis": "manifest_declared_address",
        "read_features": [],
        "mirror": {
            "mirrored_source_family": "ens_v1_resolver_l1",
            "mirrored_registry_source_family": "ens_v1_registry_l1",
            "mirrored_registry_address": "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
        }
    });
    let overview = crate::v2::build_resolver_overview(row, 11_155_111, empty_bound_names());
    let value = serde_json::to_value(overview).expect("overview must serialize");
    assert_eq!(
        value["mirror"],
        json!({
            "kind": "ensv1_registry",
            "registry": {
                "chain_id": 11_155_111,
                "address": "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
            }
        })
    );
}
