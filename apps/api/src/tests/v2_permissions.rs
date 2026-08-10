#[tokio::test]
async fn v2_get_permissions_requires_at_least_one_filter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = v2_permissions_response_for_database(&database, "/v2/permissions").await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("at least one of name, registration_id, or address is required")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_rejects_conflicting_name_and_registration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let stale_resource_id = v2_permissions_stale_resource_id();

    let response = v2_permissions_response_for_database(
        &database,
        &format!("/v2/permissions?name=perms.eth&registration_id={stale_resource_id}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("unsupported"));
    assert_eq!(
        payload["error"]["message"],
        json!("conflicting registration filters")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_maps_rows_and_lineage() -> Result<()> {
    let (database, payload) = v2_permissions_payload(&format!(
        "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=10"
    ))
    .await?;
    let current_resource_id = v2_permissions_current_resource_id();
    let stale_resource_id = v2_permissions_stale_resource_id();

    assert_eq!(payload["page"]["page_size"], json!(10));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert!(payload["meta"].get("as_of").is_none());
    assert!(payload["meta"].get("as_of_token").is_none());
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("wrapper_holder_permissions_not_supported")
    );
    assert!(payload["meta"].get("unsupported_fields").is_none());

    let rows = payload["data"]
        .as_array()
        .expect("permissions data must be an array");
    assert_eq!(rows.len(), 4);
    let resolver = permission_row_by_scope_kind(rows, "resolver");
    let record_manager = permission_row_by_scope_kind(rows, "record_manager");
    let migration_derived = permission_row_by_scope_kind(rows, "migration_derived");
    let stale = permission_row_by_registration(rows, stale_resource_id);

    assert_eq!(resolver["address"], json!(V2_PERMISSIONS_SUBJECT));
    assert_eq!(
        resolver["registration_id"],
        json!(current_resource_id.to_string())
    );
    assert_eq!(resolver["name"], json!("perms.eth"));
    assert_eq!(
        resolver["grant_scope"],
        json!({
            "kind": "resolver",
            "detail": {
                "resolver": {
                    "chain_id": 1,
                    "address": "0x0000000000000000000000000000000000000abc"
                }
            }
        })
    );
    assert_eq!(
        resolver["powers"],
        json!(["set_resolver", "create_subnames"])
    );
    assert_eq!(
        resolver["lineage"],
        json!({
            "grant": {
                "kind": "event"
            },
            "revocation": {
                "kind": "event"
            },
            "inheritance_path": [
                {
                    "kind": "resolver_root_fallback",
                    "resolver": {
                        "chain_id": 1,
                        "address": "0x0000000000000000000000000000000000000abc"
                    }
                },
                {
                    "kind": "registry_root_fallback"
                }
            ]
        })
    );

    assert_eq!(
        record_manager["grant_scope"],
        json!({
            "kind": "record_manager",
            "detail": {
                "chain_id": 1,
                "manager": "0x0000000000000000000000000000000000000cc3"
            }
        })
    );
    assert_eq!(
        migration_derived["grant_scope"],
        json!({
            "kind": "migration_derived",
            "detail": {
                "predecessor_registration_id": v2_permissions_predecessor_resource_id().to_string()
            }
        })
    );

    assert_eq!(
        stale["registration_id"],
        json!(stale_resource_id.to_string())
    );
    assert!(stale.get("name").is_none());
    assert_eq!(
        stale["grant_scope"],
        json!({
            "kind": "registration",
            "detail": {}
        })
    );
    assert_eq!(
        stale["powers"],
        json!(["registration_control", "resolver_control"])
    );
    assert_eq!(
        stale["lineage"],
        json!({
            "grant": {
                "kind": "ens_v1_authority"
            }
        })
    );
    assert!(stale["lineage"].get("revocation").is_none());
    assert!(stale["lineage"].get("inheritance_path").is_none());
    assert!(stale["lineage"].get("transfer_behavior").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_exposes_atomic_wrapper_state_and_fuses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = declared_summary || $2::jsonb
         WHERE resource_id = $1",
    )
    .bind(resource_id)
    .bind(json!({
        "wrapper_state": "locked",
        "wrapper_fuses": {
            "fuses": 196_609,
            "cannot_unwrap": true,
            "cannot_burn_fuses": false,
            "cannot_transfer": false,
            "cannot_set_resolver": false,
            "cannot_set_ttl": false,
            "cannot_create_subdomain": false,
            "cannot_approve": false,
            "parent_cannot_control": true,
            "is_dot_eth": true,
            "can_extend_expiry": false
        }
    }))
    .execute(&database.pool)
    .await?;

    let payload = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?registration_id={resource_id}"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("permissions rows");
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| row["wrapper_state"] == "locked"));
    assert!(
        rows.iter()
            .all(|row| row["wrapper_fuses"]["fuses"] == 196_609)
    );
    assert!(
        rows.iter()
            .all(|row| row["wrapper_fuses"]["cannot_unwrap"].as_bool() == Some(true))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_filters_by_name_registration_and_address() -> Result<()> {
    let (database, by_name) = v2_permissions_payload("/v2/permissions?name=Perms.eth").await?;
    let current_resource_id = v2_permissions_current_resource_id();

    let name_rows = by_name["data"]
        .as_array()
        .expect("name-filtered permissions data");
    assert_eq!(name_rows.len(), 4);
    assert!(
        name_rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );
    assert!(by_name["meta"].get("completeness").is_none());

    let by_registration = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?registration_id={current_resource_id}"),
    )
    .await?;
    let registration_rows = by_registration["data"]
        .as_array()
        .expect("registration-filtered permissions data");
    assert_eq!(registration_rows.len(), 4);
    assert!(
        registration_rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );
    assert!(by_registration["meta"].get("completeness").is_none());

    let by_address_and_registration = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v2/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}&registration_id={current_resource_id}"
        ),
    )
    .await?;
    assert_eq!(
        by_address_and_registration["data"][0]["address"],
        json!(V2_PERMISSIONS_OTHER_SUBJECT)
    );
    assert_eq!(
        by_address_and_registration["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        by_address_and_registration["meta"]
            .get("completeness")
            .is_none()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_non_name_filters_do_not_require_snapshot_metadata() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;

    for uri in [
        format!("/v2/permissions?address={V2_PERMISSIONS_SUBJECT}"),
        format!(
            "/v2/permissions?registration_id={}",
            v2_permissions_current_resource_id()
        ),
    ] {
        let response = v2_permissions_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let payload: Value = read_json(response).await?;
        assert!(!payload["data"].as_array().unwrap().is_empty(), "{uri}");
        assert!(payload["meta"].get("as_of").is_none(), "{uri}");
        assert!(payload["meta"].get("as_of_token").is_none(), "{uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_name_filter_uses_current_registration_without_snapshot_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let current_resource_id = v2_permissions_current_resource_id();

    let payload =
        v2_permissions_payload_for_database(&database, "/v2/permissions?name=Perms.eth").await?;
    let rows = payload["data"]
        .as_array()
        .expect("name-filtered permissions data");
    assert_eq!(rows.len(), 4);
    assert!(
        rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );
    assert!(payload["meta"].get("as_of").is_none());
    assert!(payload["meta"].get("as_of_token").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_name_filter_uses_current_sepolia_anchor_on_mixed_phase_heads()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;
    let resource_id = Uuid::from_u128(0x7e20);
    upsert_phase_permissions_current_rows(
        &database.pool,
        &[permission_current_row(
            resource_id,
            V2_PERMISSIONS_SUBJECT,
            PermissionScope::Resource,
            1,
            V2_SEPOLIA_SNAPSHOT_BLOCK,
        )],
    )
    .await?;
    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("registrar")),
    )
    .await?;

    let payload = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?name={V2_SEPOLIA_SNAPSHOT_NAME}"),
    )
    .await?;
    assert_eq!(payload["data"][0]["registration_id"], json!(resource_id));
    assert!(payload["meta"].get("as_of").is_none());
    assert!(payload["meta"].get("as_of_token").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_paginates_and_rejects_mismatched_cursor() -> Result<()> {
    let (database, first_page) = v2_permissions_payload(&format!(
        "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1"
    ))
    .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();

    let second_page = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["has_more"], json!(true));
    assert_ne!(first_page["data"], second_page["data"]);

    let cross_address = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v2/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(cross_address).await?["error"]["code"],
        json!("invalid_input")
    );

    let cross_include = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_include.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(cross_include).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_empty_results_return_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    let by_address = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?address={V2_PERMISSIONS_SUBJECT}"),
    )
    .await?;
    assert_eq!(by_address["data"], json!([]));
    assert_eq!(by_address["page"]["has_more"], json!(false));
    assert_eq!(by_address["page"]["next_cursor"], Value::Null);
    assert_eq!(by_address["meta"]["completeness"], json!("partial"));
    assert_eq!(
        by_address["meta"]["unsupported_reason"],
        json!("wrapper_holder_permissions_not_supported")
    );

    let by_missing_name =
        v2_permissions_payload_for_database(&database, "/v2/permissions?name=missing.eth").await?;
    assert_eq!(by_missing_name["data"], json!([]));
    assert_eq!(by_missing_name["page"]["has_more"], json!(false));
    assert_eq!(by_missing_name["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_permissions_empty_resource_fails_closed_from_typed_support_summary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;
    let resource_id = Uuid::from_u128(0xe400);
    upsert_test_resources(&database.pool, &[resource(resource_id)]).await?;
    let uri = format!("/v2/permissions?registration_id={resource_id}");

    let missing = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(missing["data"], json!([]));
    assert_eq!(missing["meta"]["completeness"], json!("partial"));
    assert_eq!(
        missing["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("registrar")),
    )
    .await?;
    let full = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(full["data"], json!([]));
    assert!(full["meta"].get("completeness").is_none());
    assert!(full["meta"].get("unsupported_reason").is_none());

    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("wrapper")),
    )
    .await?;
    let wrapper = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(wrapper["data"], json!([]));
    assert_eq!(wrapper["meta"]["completeness"], json!("unsupported"));
    assert_eq!(
        wrapper["meta"]["unsupported_reason"],
        json!("wrapper_holder_permissions_not_supported")
    );

    database.cleanup().await
}

/// `transport_derived` is a reserved permission scope: no adapter emits it
/// (guarded in `crates/adapters`), so a projection rebuild never produces this
/// row. The schema value and the typed reader are retained rather than deleted —
/// `docs/glossary.md` § Reserved surface and `docs/architecture.md` § Permissions
/// carry the reasoning.
///
/// This pins that the retained read path still works: a stored row carrying the
/// reserved kind decodes and serves instead of failing the page. That makes
/// removing the `transport_derived` arm of `PermissionScope::parse`, or the
/// schema's CHECK value, visible as the breaking change it would be for any row
/// carrying the kind.
///
/// It asserts neither that the scope is supported output nor that bigname has
/// ever written such a row — no adapter has ever emitted one. Malformed rows are
/// out of scope: a row whose detail does not match its kind fails the page for
/// every scope kind, reserved or not.
#[tokio::test]
async fn v2_get_permissions_decodes_and_serves_a_stored_reserved_scope() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;

    let mut residual_row = permission_current_row(
        v2_permissions_current_resource_id(),
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::TransportDerived {
            transport: "retired".to_owned(),
        },
        12,
        113,
    );
    apply_raw_log_permission_lineage(&mut residual_row, "set_resolver", 113);
    upsert_phase_permissions_current_rows(&database.pool, &[residual_row]).await?;

    let response = v2_permissions_response_for_database(
        &database,
        &format!("/v2/permissions?address={V2_PERMISSIONS_SUBJECT}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    let rows = payload["data"]
        .as_array()
        .expect("permissions data must be an array");
    let residual = permission_row_by_scope_kind(rows, "transport_derived");
    assert_eq!(residual["grant_scope"]["detail"]["transport"], json!("retired"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_serve_unprojected_authority_resources_as_partial() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    let uri = format!("/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=10");

    // The projection records an unprojected authority for every kind it cannot enumerate,
    // including a NULL kind; those rows must degrade the page rather than fail its read.
    for authority_kind in [None, Some("subregistry")] {
        upsert_phase_permissions_current_resource_summary(
            &database.pool,
            &permission_current_resource_summary(resource_id, authority_kind),
        )
        .await?;

        let response = v2_permissions_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = read_json(response).await?;
        assert_eq!(
            payload["data"]
                .as_array()
                .expect("permissions data must be an array")
                .len(),
            4
        );
        assert_eq!(payload["meta"]["completeness"], json!("partial"));
        assert_eq!(
            payload["meta"]["unsupported_reason"],
            json!("permission_support_unknown")
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_admit_project_vocabulary_and_exclude_orphaned_projection_targets()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    let uri = format!("/v2/permissions?registration_id={resource_id}");

    let readable = v2_permissions_payload_for_database(&database, &uri).await?;
    assert!(!readable["data"].as_array().is_none_or(Vec::is_empty));
    assert!(readable["meta"].get("completeness").is_none());

    sqlx::query(
        r#"
        UPDATE bigname_phase.chain_lineage lineage
        SET canonicality_state = 'orphaned'::bigname_phase.canonicality_state
        WHERE (lineage.chain_id, lineage.block_hash) IN (
            SELECT pc.provenance ->> 'chain_id',
                   pc.chain_positions ->> 'target_block_hash'
            FROM bigname_phase.permissions_current pc
            WHERE pc.resource_id = $1
            UNION
            SELECT summary.provenance ->> 'chain_id',
                   summary.chain_positions ->> 'target_block_hash'
            FROM bigname_phase.permissions_current_resource_summary summary
            WHERE summary.resource_id = $1
        )
        "#,
    )
    .bind(resource_id)
    .execute(&database.pool)
    .await?;

    let orphaned = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(orphaned["data"], json!([]));
    assert_eq!(orphaned["meta"]["completeness"], json!("partial"));
    assert_eq!(
        orphaned["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

    database.cleanup().await
}

const V2_PERMISSIONS_SUBJECT: &str = "0x0000000000000000000000000000000000000cc1";
const V2_PERMISSIONS_OTHER_SUBJECT: &str = "0x0000000000000000000000000000000000000cc2";

async fn v2_permissions_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let payload = v2_permissions_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_permissions_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_permissions_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_permissions_response_for_database(
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
        .context("v2 permissions request failed")
}

async fn seed_v2_permissions_fixture(database: &TestDatabase) -> Result<()> {
    let current_resource_id = v2_permissions_current_resource_id();
    let stale_resource_id = v2_permissions_stale_resource_id();
    let token_lineage_id = Uuid::from_u128(0xe102);
    let surface_binding_id = Uuid::from_u128(0xe103);

    database
        .seed_name_current_binding_migrated(
            "ens:perms.eth",
            current_resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:perms.eth",
            "Perms.eth",
            "perms.eth",
            "node:perms.eth",
            surface_binding_id,
            current_resource_id,
            Some(token_lineage_id),
            130,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar"
                },
                "control": {
                    "registry_owner": V2_PERMISSIONS_SUBJECT
                }
            }),
        ))
        .await?;
    upsert_test_resources(&database.pool, &[resource(stale_resource_id)]).await?;

    let mut current_row = permission_current_row(
        current_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000ABC".to_owned(),
        },
        8,
        108,
    );
    current_row.grant_source = json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": "root",
        "root_resource": true,
        "changed_powers": ["set_resolver"],
        "resolver_contract_instance_id": "00000000-0000-0000-0000-00000000c108"
    });
    current_row.revocation_source = Some(json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": "root",
        "root_resource": true,
        "changed_powers": ["set_resolver"],
        "resolver_contract_instance_id": "00000000-0000-0000-0000-00000000c109"
    }));
    current_row.inheritance_path = json!([
        {
            "kind": "resolver_root_fallback",
            "chain_id": "ethereum-mainnet",
            "resolver_address": "0x0000000000000000000000000000000000000ABC",
            "upstream_resource": "root"
        },
        {
            "kind": "registry_root_fallback",
            "chain_id": "ethereum-mainnet",
            "registry_address": "0x0000000000000000000000000000000000000DEF",
            "upstream_resource": "root"
        }
    ]);
    current_row.transfer_behavior = json!({});

    let mut stale_row = permission_current_row(
        stale_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::Resource,
        7,
        109,
    );
    stale_row.effective_powers = json!(["resource_control", "resolver_control"]);
    stale_row.grant_source = json!({
        "kind": "ens_v1_authority",
        "authority_kind": "registry_owner",
        "authority_key": "registry:ethereum-mainnet:perms",
        "source_event_kind": "Transfer"
    });
    stale_row.inheritance_path = json!([]);
    stale_row.transfer_behavior = Value::Null;

    let mut record_manager_row = permission_current_row(
        current_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::RecordManager {
            chain_id: "ethereum-mainnet".to_owned(),
            manager_address: "0x0000000000000000000000000000000000000cC3".to_owned(),
        },
        10,
        111,
    );
    apply_raw_log_permission_lineage(&mut record_manager_row, "set_records", 111);
    let mut migration_derived_row = permission_current_row(
        current_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::MigrationDerived {
            predecessor_resource_id: v2_permissions_predecessor_resource_id(),
        },
        11,
        112,
    );
    apply_raw_log_permission_lineage(&mut migration_derived_row, "set_records", 112);

    upsert_phase_permissions_current_rows(
        &database.pool,
        &[
            current_row,
            record_manager_row,
            migration_derived_row,
            stale_row,
            permission_current_row(
                current_resource_id,
                V2_PERMISSIONS_OTHER_SUBJECT,
                PermissionScope::Registry,
                9,
                110,
            ),
        ],
    )
    .await?;
    for resource_id in [current_resource_id, stale_resource_id] {
        upsert_phase_permissions_current_resource_summary(
            &database.pool,
            &permission_current_resource_summary(resource_id, Some("registrar")),
        )
        .await?;
    }

    Ok(())
}

fn apply_raw_log_permission_lineage(
    row: &mut bigname_storage::PermissionsCurrentRow,
    power: &str,
    suffix: i64,
) {
    row.grant_source = json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": "root",
        "root_resource": true,
        "changed_powers": [power],
        "resolver_contract_instance_id": format!("00000000-0000-0000-0000-00000000c{suffix:03}")
    });
    row.revocation_source = None;
    row.inheritance_path = json!([]);
    row.transfer_behavior = Value::Null;
}

fn permission_row_by_registration(rows: &[Value], resource_id: Uuid) -> &Value {
    let registration_id = resource_id.to_string();
    rows.iter()
        .find(|row| row["registration_id"] == json!(registration_id))
        .expect("permission row must exist")
}

fn permission_row_by_scope_kind<'a>(rows: &'a [Value], kind: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["grant_scope"]["kind"] == json!(kind))
        .unwrap_or_else(|| panic!("permission row with scope kind {kind} must exist"))
}

fn v2_permissions_current_resource_id() -> Uuid {
    Uuid::from_u128(0xe100)
}

fn v2_permissions_stale_resource_id() -> Uuid {
    Uuid::from_u128(0xe200)
}

fn v2_permissions_predecessor_resource_id() -> Uuid {
    Uuid::from_u128(0xe300)
}
