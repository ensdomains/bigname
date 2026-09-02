const V2_RESOLVER_ADDRESS: &str = "0x0000000000000000000000000000000000000aaa";
const DIVERGENT_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000d01";
const DIVERGENT_CONTROL_OWNER: &str = "0x0000000000000000000000000000000000000d02";
const DIVERGENT_REGISTRATION_REGISTRANT: &str = "0x0000000000000000000000000000000000000d03";
const DIVERGENT_CONTROL_REGISTRANT: &str = "0x0000000000000000000000000000000000000d04";

#[test]
fn v2_bound_names_cursor_payload_round_trips_storage_cursor() {
    let cursor = v2_bound_names_cursor();
    let binding = v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, "snapshot-1");
    let payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);

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
        crate::v2::bound_names_storage_cursor(&payload, &binding).expect("cursor must decode"),
        cursor
    );
}

#[test]
fn v2_bound_names_cursor_rejects_wrong_chain_resolver_sort_or_snapshot() {
    let cursor = v2_bound_names_cursor();
    let binding = v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, "snapshot-1");

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload.sort = "wrong".to_owned();
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload
        .filters
        .insert("chain_id".to_owned(), "8453".to_owned());
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload.filters.insert(
        "resolver".to_owned(),
        "0x0000000000000000000000000000000000000bbb".to_owned(),
    );
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload.snapshot = Some("snapshot-2".to_owned());
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());
}

#[test]
fn v2_resolver_include_controls_overview_sections_and_rejects_unknown() {
    let include = crate::v2::resolver_overview_include(&["nodes".to_owned()])
        .expect("valid include must parse");
    let overview = crate::v2::build_resolver_overview(
        resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS),
        1,
        include,
        empty_bound_names(),
    )
    .expect("resolver overview must build");
    let value = serde_json::to_value(overview).expect("overview must serialize");

    assert!(value["nodes"].is_array());
    assert!(value.get("aliases").is_none());
    assert!(value.get("roles").is_none());
    assert!(value.get("events").is_none());

    let include = crate::v2::resolver_overview_include(&[]).expect("empty include defaults to all");
    let mut resolver_row =
        resolver_current_row_with_writer_alias("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    resolver_row.declared_summary["role_holders"]["items"][0]["effective_powers"] =
        json!(["resource_control", "set_resolver"]);
    resolver_row.declared_summary["role_holders"]["items"][0]["permission_row_count"] = json!(2);
    let overview = crate::v2::build_resolver_overview(
        resolver_row,
        1,
        include,
        empty_bound_names(),
    )
    .expect("resolver overview must build");
    let value = serde_json::to_value(overview).expect("overview must serialize");
    assert!(value["nodes"].is_array());
    assert_eq!(
        value["aliases"],
        json!([
            {
                "namespace": "ens",
                "name": "beta.eth",
                "display_name": "beta.eth",
                "namehash": "namehash:beta.eth"
            },
            {
                "namespace": "ens",
                "from_name": "alias.eth",
                "to_name": "beta.eth",
                "state": "active",
                "resolver": {
                    "chain_id": 1,
                    "address": "0x0000000000000000000000000000000000000aaa"
                },
                "to_registration_id": "00000000-0000-0000-0000-00000000b102"
            }
        ])
    );
    // One resolver-scoped permission row grants both powers, so its row count is one.
    assert_eq!(
        value["roles"],
        json!([
            {
                "address": "0x0000000000000000000000000000000000000abc",
                "registration_count": 1,
                "permission_count": 1,
                "powers": ["registration_control", "set_resolver"]
            }
        ])
    );
    assert!(value["roles"][0].get("subject").is_none());
    assert!(value["roles"][0].get("resource_count").is_none());
    assert!(value["roles"][0].get("permission_row_count").is_none());
    assert!(value["roles"][0].get("effective_powers").is_none());
    assert!(value["roles"][0].get("resource_ids").is_none());
    assert!(value["roles"][0].get("registration_ids").is_none());
    assert_eq!(
        value["events"],
        json!({
            "count": 4,
            "by_type": {
                "permission": 1,
                "resolver": 2,
            }
        })
    );
    assert!(value["events"].get("by_kind").is_none());

    let error = crate::v2::resolver_overview_include(&["records".to_owned()])
        .expect_err("unknown include must fail");
    assert_eq!(error.code(), crate::v2::ErrorCode::InvalidInput);

    let include = crate::v2::resolver_overview_include(&["aliases".to_owned()])
        .expect("valid include must parse");
    let mut resolver_row =
        resolver_current_row_with_writer_alias("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    resolver_row.declared_summary["aliases"]["items"][1]["unmapped_resource_id"] =
        json!("00000000-0000-0000-0000-00000000ffff");
    let error =
        crate::v2::build_resolver_overview(resolver_row, 1, include, empty_bound_names())
            .expect_err("unmapped banned alias keys must fail loudly");
    assert_eq!(error.code(), crate::v2::ErrorCode::InternalError);
}

#[test]
fn v2_resolver_events_summary_maps_writer_by_kind_to_product_types() {
    let include =
        crate::v2::resolver_overview_include(&["events".to_owned()]).expect("events include parses");
    let mut resolver_row = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    resolver_row.declared_summary["event_summary"] = json!({
        "status": "supported",
        "count": 10,
        "by_kind": {
            "RecordChanged": 2,
            "RecordVersionChanged": 3,
            "SurfaceBound": 4,
            "PermissionChanged": 1,
        },
    });

    let overview = crate::v2::build_resolver_overview(resolver_row, 1, include, empty_bound_names())
        .expect("resolver overview must build");
    let value = serde_json::to_value(overview).expect("overview must serialize");

    assert_eq!(
        value["events"],
        json!({
            "count": 10,
            "by_type": {
                "permission": 1,
                "record": 5,
            }
        })
    );
    assert!(value["events"].get("by_kind").is_none());
    assert!(value["events"]["by_type"].get("SurfaceBound").is_none());
}

#[test]
fn v2_resolver_alias_summary_preserves_null_targets_for_removed_and_unknown() {
    let include =
        crate::v2::resolver_overview_include(&["aliases".to_owned()]).expect("aliases include parses");
    let mut resolver_row =
        resolver_current_row_with_writer_alias("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    let aliases = resolver_row.declared_summary["aliases"]["items"]
        .as_array_mut()
        .expect("aliases fixture items must be an array");
    aliases[1]["alias_state"] = json!("removed");
    aliases[1]["to_name"] = Value::Null;
    aliases[1]["to_logical_name_id"] = Value::Null;
    aliases[1]["to_resource_id"] = Value::Null;
    let mut unknown = aliases[1].clone();
    unknown["logical_name_id"] = json!("ens:unknown-alias.eth");
    unknown["resource_id"] = json!("00000000-0000-0000-0000-00000000b105");
    unknown["alias_state"] = json!("unknown");
    unknown["from_name"] = json!("unknown-alias.eth");
    unknown["from_dns_encoded_name"] = json!("0x0d756e6b6e6f776e2d616c6961730365746800");
    aliases.push(unknown);
    resolver_row.declared_summary["aliases"]["count"] = json!(3);

    let overview = crate::v2::build_resolver_overview(resolver_row, 1, include, empty_bound_names())
        .expect("resolver overview must build");
    let value = serde_json::to_value(overview).expect("overview must serialize");

    assert_eq!(
        value["aliases"][1],
        json!({
            "namespace": "ens",
            "from_name": "alias.eth",
            "to_name": null,
            "state": "removed",
            "resolver": {
                "chain_id": 1,
                "address": "0x0000000000000000000000000000000000000aaa"
            }
        })
    );
    assert_eq!(
        value["aliases"][2],
        json!({
            "namespace": "ens",
            "from_name": "unknown-alias.eth",
            "to_name": null,
            "state": "unknown",
            "resolver": {
                "chain_id": 1,
                "address": "0x0000000000000000000000000000000000000aaa"
            }
        })
    );
    assert!(value["aliases"][1].get("to_registration_id").is_none());
    assert!(value["aliases"][2].get("to_registration_id").is_none());
}

#[tokio::test]
async fn v2_get_resolver_returns_overview_with_nested_bound_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    let updated = sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = declared_summary || $1::jsonb
         WHERE raw_name = 'alpha.eth'",
    )
    .bind(json!({
        "wrapper_state": "locked",
        "wrapper_fuses": {
            "fuses": 65_537,
            "cannot_unwrap": true,
            "cannot_burn_fuses": false,
            "cannot_transfer": false,
            "cannot_set_resolver": false,
            "cannot_set_ttl": false,
            "cannot_create_subdomain": false,
            "cannot_approve": false,
            "parent_cannot_control": true,
            "is_dot_eth": false,
            "can_extend_expiry": false
        }
    }))
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row_with_writer_alias(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;

    let first_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1"),
    )
    .await?;

    assert!(first_page.get("page").is_none());
    assert_eq!(first_page["meta"]["as_of"]["1"]["block_number"], json!(203));
    assert_eq!(first_page["data"]["chain_id"], json!(1));
    assert_eq!(first_page["data"]["address"], json!(V2_RESOLVER_ADDRESS));
    assert_eq!(
        first_page["data"]["counts"],
        json!({
            "nodes": 2,
            "aliases": 2,
            "role_holders": 1,
            "events": 4,
        })
    );
    assert!(first_page["data"].get("aliases").is_none());
    assert!(first_page["data"].get("roles").is_none());
    assert!(first_page["data"].get("events").is_none());
    assert_eq!(first_page["data"]["nodes"][0]["namespace"], json!("ens"));
    assert_eq!(first_page["data"]["nodes"][0]["name"], json!("alice.eth"));
    assert_eq!(
        first_page["data"]["nodes"][0]["display_name"],
        json!("alice.eth")
    );
    assert!(first_page["data"]["nodes"][0].get("normalized_name").is_none());

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
    assert_eq!(bound_names["data"][0]["registered_at"], json!("2024-01-02T00:00:00Z"));
    assert_eq!(bound_names["data"][0]["created_at"], json!("2023-01-02T00:00:00Z"));
    assert_eq!(bound_names["data"][0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert_eq!(bound_names["data"][0]["wrapper_state"], json!("locked"));
    assert_eq!(
        bound_names["data"][0]["wrapper_fuses"],
        json!({
            "fuses": 65_537,
            "cannot_unwrap": true,
            "cannot_burn_fuses": false,
            "cannot_transfer": false,
            "cannot_set_resolver": false,
            "cannot_set_ttl": false,
            "cannot_create_subdomain": false,
            "cannot_approve": false,
            "parent_cannot_control": true,
            "is_dot_eth": false,
            "can_extend_expiry": false
        })
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
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(second_page["data"]["bound_names"]["data"][0]["name"], json!("beta.eth"));
    assert_eq!(second_page["data"]["bound_names"]["page"]["has_more"], json!(false));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_serves_total_counts_with_bounded_samples() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    let nodes = (0..100)
        .map(|ordinal| {
            json!({
                "logical_name_id": format!("ens:sample-{ordinal:03}.eth"),
                "normalized_name": format!("sample-{ordinal:03}.eth"),
                "namehash": format!("namehash:sample-{ordinal:03}.eth"),
            })
        })
        .collect::<Vec<_>>();
    let aliases = nodes.clone();
    let roles = (0..100)
        .map(|ordinal| {
            json!({
                "subject": format!("0x{ordinal:040x}"),
                "resource_count": 1,
                "permission_row_count": 1,
                "effective_powers": ["set_resolver"],
                "resource_ids": [format!(
                    "00000000-0000-0000-0000-{ordinal:012}"
                )],
            })
        })
        .collect::<Vec<_>>();
    resolver.declared_summary["bindings"] = json!({
        "status": "supported",
        "count": 101,
        "total_count": 101,
        "sample_limit": 100,
        "sample_count": 100,
        "truncated": true,
        "items": nodes,
    });
    resolver.declared_summary["aliases"] = json!({
        "status": "supported",
        "count": 101,
        "total_count": 101,
        "sample_limit": 100,
        "sample_count": 100,
        "truncated": true,
        "items": aliases,
    });
    resolver.declared_summary["role_holders"] = json!({
        "status": "supported",
        "count": 101,
        "total_count": 101,
        "sample_limit": 100,
        "sample_count": 100,
        "truncated": true,
        "items": roles,
    });
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(&database, &[resolver]).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!(
            "/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes,aliases,roles"
        ),
    )
    .await?;

    assert_eq!(payload["data"]["counts"]["nodes"], 101);
    assert_eq!(payload["data"]["counts"]["aliases"], 101);
    assert_eq!(payload["data"]["counts"]["role_holders"], 101);
    for section in ["nodes", "aliases", "roles"] {
        assert_eq!(
            payload["data"][section].as_array().map(Vec::len),
            Some(100),
            "{section}"
        );
    }
    assert!(payload["data"]["roles"].as_array().is_some_and(|items| {
        items.iter().all(|item| {
            item.get("resource_ids").is_none() && item.get("registration_ids").is_none()
        })
    }));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_rejects_pre_unification_cursor_snapshot_binding() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;
    let old_resolver_snapshot_token = v2_at_token(
        "ethereum-mainnet",
        "ethereum-mainnet",
        102,
        "0xname66",
        "2026-04-17T00:00:02Z",
    )?;
    let old_cursor_payload = crate::v2::bound_names_cursor_payload(
        &v2_bound_names_cursor(),
        &v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, &old_resolver_snapshot_token),
    );
    let old_cursor = crate::v2::encode(&old_cursor_payload);

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1&cursor={old_cursor}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(payload.error.message, "cursor must be a valid pagination cursor");

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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;

    assert_eq!(payload["data"]["bound_names"]["data"], json!([]));
    assert_eq!(payload["data"]["bound_names"]["page"]["has_more"], json!(false));
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;
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
    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;
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
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=50"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;
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
                 '{control,status}', '"unregistered"'::jsonb)
         WHERE raw_name = 'alpha.eth'"#,
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let names = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    assert!(names.iter().all(|row| row["name"] != "alpha.eth"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_resolver_uses_dictionary_owner_and_registrant_precedence_for_bound_names(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &[
            "base-mainnet",
            "base-mainnet",
            "base-mainnet",
            "base-mainnet",
            "base-mainnet",
            "ethereum-mainnet",
        ],
    )
    .await?;
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=50"),
    )
    .await?;
    let rows = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    let row = rows
        .iter()
        .find(|row| row["name"] == json!("precedence.eth"))
        .expect("precedence row must be present");

    assert_eq!(row["owner"], json!(DIVERGENT_CONTROL_OWNER));
    assert_eq!(row["registrant"], json!(DIVERGENT_REGISTRATION_REGISTRANT));
    assert_eq!(row["registration_status"], json!("active"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_reports_unsupported_requested_sections_in_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = unsupported_resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(
        &database,
        &[resolver],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!(
            "/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes,aliases,roles,events"
        ),
    )
    .await?;

    assert_eq!(payload["data"]["nodes"], Value::Null);
    assert!(
        payload["meta"]["as_of"]["1"].is_object(),
        "resolver unsupported meta must preserve as_of"
    );
    assert!(
        payload["meta"]["as_of_token"].is_string(),
        "resolver unsupported meta must preserve the snapshot token"
    );
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["nodes", "aliases", "roles", "events"])
    );
    assert_eq!(payload["meta"]["completeness"], json!("unsupported"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("resolver_family_pending")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_reports_narrowed_unsupported_sections_as_unsupported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = unsupported_resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(
        &database,
        &[resolver],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;

    assert_eq!(payload["data"]["nodes"], Value::Null);
    assert_eq!(payload["meta"]["unsupported_fields"], json!(["nodes"]));
    assert_eq!(payload["meta"]["completeness"], json!("unsupported"));

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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;
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
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
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
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let first_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1"),
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
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1&cursor={next_cursor}"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1"),
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
    assert_eq!(payload["data"]["bound_names"]["page"]["has_more"], json!(false));
    assert_eq!(payload["data"]["bound_names"]["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_maps_unsupported_reason_to_product_vocabulary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    let mut resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    resolver.declared_summary["bindings"] = json!({
        "status": "unsupported",
        "unsupported_reason": "resolver_binding_enumeration_not_projected"
    });
    upsert_test_resolver_current_rows(
        &database, &[resolver]).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;

    assert_eq!(payload["meta"]["completeness"], json!("unsupported"));
    assert_eq!(payload["meta"]["unsupported_fields"], json!(["nodes"]));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("binding_enumeration_not_supported")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_rejects_pipeline_unsupported_reason() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    let mut resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    resolver.declared_summary["bindings"] = json!({
        "status": "unsupported",
        "unsupported_reason": "resolver_sidecar_missing"
    });
    upsert_test_resolver_current_rows(
        &database, &[resolver]).await?;

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to map resolver reason vocabulary"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_supplies_reason_when_unsupported_summary_omits_one() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    let mut resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    resolver.declared_summary["bindings"] = json!({
        "status": "unsupported"
    });
    upsert_test_resolver_current_rows(
        &database, &[resolver]).await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;

    assert_eq!(payload["meta"]["completeness"], json!("unsupported"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("resolver_overview_not_supported")
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

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
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
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;
    let initial = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;
    let token = initial["meta"]["as_of_token"]
        .as_str()
        .expect("resolver response must include a snapshot token");
    sqlx::query("DELETE FROM resolver_current WHERE chain_id = 'ethereum-mainnet'")
        .execute(&database.lookup_pool)
        .await?;

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?at={token}"),
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
        format!("/v2/resolvers/ethereum-mainnet/{V2_RESOLVER_ADDRESS}"),
        format!("/v2/resolvers/99999999/{V2_RESOLVER_ADDRESS}"),
        "/v2/resolvers/1/not-an-address".to_owned(),
        format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=records"),
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

async fn v2_resolver_response_for_database(
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
            .then(|| row.coverage["unsupported_reason"].as_str().map(str::to_owned))
            .flatten()
            .or_else(|| (support_status == "unsupported").then(|| "resolver_overview_not_supported".to_owned()));
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

async fn seed_v2_resolver_bound_names_fixture_with_chains(
    database: &TestDatabase,
    resolver_chains: &[&str],
) -> Result<()> {
    let resolver_snapshot = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)
        .chain_positions;
    database
        .seed_snapshot_selector_chain_positions(&resolver_snapshot)
        .await?;
    let mut specs = v2_address_name_specs();
    specs.push(V2AddressNameSpec {
        logical_name_id: "ens:precedence.eth",
        name: "precedence.eth",
        namehash: "node:precedence.eth",
        resource_id: Uuid::from_u128(0xe100),
        token_lineage_id: Uuid::from_u128(0xe101),
        surface_binding_id: Uuid::from_u128(0xe102),
        block_hash: "0xname70",
        block_number: 106,
        owner: DIVERGENT_REGISTRY_OWNER,
        registrant: DIVERGENT_REGISTRATION_REGISTRANT,
        registered_at: "2024-07-03T00:00:00Z",
        created_at: "2023-07-03T00:00:00Z",
        expires_at: "2027-07-03T00:00:00Z",
        relations: &[],
    });
    seed_v2_address_name_storage(database, &specs).await?;

    for (spec, resolver_chain) in specs.iter().zip(resolver_chains.iter().copied()) {
        let control_owner = (spec.logical_name_id == "ens:precedence.eth")
            .then_some(DIVERGENT_CONTROL_OWNER);
        let control_registrant = if spec.logical_name_id == "ens:precedence.eth" {
            DIVERGENT_CONTROL_REGISTRANT
        } else {
            spec.registrant
        };

        let row = address_name_name_current_row(
            spec.logical_name_id,
            spec.name,
            spec.name,
            spec.namehash,
            spec.surface_binding_id,
            spec.resource_id,
            Some(spec.token_lineage_id),
            spec.block_number,
            json!({
                    "registration": {
                        "status": "active",
                        "authority_kind": "registrar",
                        "registrant": spec.registrant,
                        "registered_at": spec.registered_at,
                        "created_at": spec.created_at,
                        "expiry": spec.expires_at
                    },
                    "control": {
                        "registry_owner": spec.owner,
                        "owner": control_owner,
                        "registrant": control_registrant,
                        "expiry": spec.expires_at
                    },
                    "resolver": {
                        "chain_id": resolver_chain,
                        "address": V2_RESOLVER_ADDRESS,
                        "latest_event_kind": "ResolverChanged"
                    }
            }),
        );
        database.insert_name_current_row(row.clone()).await?;
        seed_phase_identity_name(
            database,
            spec.name,
            spec.name,
            spec.resource_id,
            spec.token_lineage_id,
            spec.surface_binding_id,
            spec.owner,
            bigname_storage::AddressNameRelation::TokenHolder,
            &row.declared_summary,
        )
        .await?;
    }

    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 203,
                "block_hash": "0xresolvercb",
                "timestamp": "2026-04-17T00:00:23Z",
            }
        }))
        .await?;

    Ok(())
}

fn v2_bound_names_cursor() -> bigname_storage::NameCurrentListCursor {
    bigname_storage::NameCurrentListCursor {
        sort_value: bigname_storage::NameCurrentListCursorValue::Name("alice.eth".to_owned()),
        namespace: "ens".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "node:alice.eth".to_owned(),
    }
}

fn v2_bound_names_cursor_binding<'a>(
    resolver_address: &'a str,
    snapshot_token: &'a str,
) -> crate::v2::BoundNamesCursorBinding<'a> {
    crate::v2::BoundNamesCursorBinding {
        chain_id: 1,
        resolver_address,
        namespace: Some("ens"),
        sort: "name_asc",
        snapshot_token,
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
