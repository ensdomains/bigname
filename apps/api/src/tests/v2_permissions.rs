#[tokio::test]
async fn v2_get_permissions_requires_at_least_one_filter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = v2_permissions_response_for_database(&database, "/v1/permissions").await?;

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
async fn v2_get_permissions_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current SET raw_name = $1
         WHERE resource_id = $2",
    )
    .bind(NORMALIZED_NAME)
    .bind(v2_permissions_current_resource_id())
    .execute(&database.pool)
    .await?;
    let stored_raw_name: String = sqlx::query_scalar(
        "SELECT raw_name FROM bigname_phase.name_current WHERE resource_id = $1",
    )
    .bind(v2_permissions_current_resource_id())
    .fetch_one(&database.pool)
    .await?;

    let payload = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?registration_id={}",
            v2_permissions_current_resource_id()
        ),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("permissions data must be an array");
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .all(|row| row["name"] == json!(stored_raw_name))
    );

    database.cleanup().await
}

// A registration the name filter did not select is not a rejected filter combination: it is a
// registration that no longer holds the name. It stays queryable on its own as an audit read, and
// pairing it with the name it lost returns an empty collection.
#[tokio::test]
async fn v2_get_permissions_empties_a_superseded_name_and_registration_pair() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let stale_resource_id = v2_permissions_stale_resource_id();

    let paired = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=perms.eth&registration_id={stale_resource_id}"),
    )
    .await?;
    assert_eq!(paired["data"], json!([]));
    assert!(paired["meta"].get("completeness").is_none());
    assert!(paired["meta"].get("unsupported_reason").is_none());

    // Anti-vacuity: the same superseded registration is still readable as a resource audit.
    let audited = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={stale_resource_id}"),
    )
    .await?;
    assert!(
        !audited["data"]
            .as_array()
            .expect("audit read must return an array")
            .is_empty(),
        "the superseded registration lost its own audit read"
    );
    assert!(
        audited["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["authority_context"] == json!("resource_audit"))
    );

    database.cleanup().await?;
    Ok(())
}

// A `.eth` lease that lapsed under a registry-only binding (a registrar token transferred
// without `reclaim`) is released like any other lapse: a name-filtered request selects nothing,
// as for every released name, while the resource audit keeps its rows.
#[tokio::test]
async fn v2_get_permissions_empties_a_lapsed_handed_off_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let released_resource_id = v2_permissions_current_resource_id();

    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 jsonb_set(
                     declared_summary,
                     '{registration,status}',
                     '\"released\"'::jsonb
                 ),
                 '{registration,authority_kind}',
                 '\"registry_only\"'::jsonb
             ),
             '{control}',
             '{\"status\": \"unregistered\"}'::jsonb
         )
         WHERE resource_id = $1",
    )
    .bind(released_resource_id)
    .execute(&database.pool)
    .await?;

    let by_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    assert_eq!(by_name["data"], json!([]), "{by_name}");

    let audited = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={released_resource_id}"),
    )
    .await?;
    let rows = audited["data"]
        .as_array()
        .expect("resource audit must return an array");
    assert!(!rows.is_empty(), "the released resource lost its audit read");

    database.cleanup().await?;
    Ok(())
}

// An explicit ENSv2 release leaves retained permission rows available to a resource audit, but
// the released name no longer has a current registration and cannot select those rows.
#[tokio::test]
async fn v2_get_permissions_empties_a_released_name_but_keeps_its_resource_audit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let released_resource_id = v2_permissions_current_resource_id();

    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary,
             '{registration,status}',
             '\"released\"'::jsonb
         )
         WHERE resource_id = $1",
    )
    .bind(released_resource_id)
    .execute(&database.pool)
    .await?;

    let by_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    assert_eq!(by_name["data"], json!([]));

    let audited = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={released_resource_id}"),
    )
    .await?;
    let rows = audited["data"]
        .as_array()
        .expect("resource audit must return an array");
    assert!(!rows.is_empty(), "the released resource lost its audit read");
    assert!(
        rows.iter()
            .all(|row| row["authority_context"] == json!("resource_audit"))
    );

    database.cleanup().await?;
    Ok(())
}

// This deliberately retains resource-keyed audit evidence while changing the name summary to the
// reservation shape and removing current-owner evidence. The API therefore classifies the name as
// unregistered, so the retained evidence remains audit-only and cannot become `current_for_name`.
#[tokio::test]
async fn v2_get_permissions_keeps_retained_resource_audit_out_of_reserved_name_scope() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let reserved_resource_id = v2_permissions_current_resource_id();

    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
                 jsonb_set(
                     declared_summary #- '{control,owner}' #- '{control,registry_owner}',
                     '{registration,status}', '\"reserved\"'::jsonb
                 ),
                 '{registration,authority_kind}', '\"ens_v2_registry\"'::jsonb
             )
         WHERE resource_id = $1",
    )
    .bind(reserved_resource_id)
    .execute(&database.pool)
    .await?;

    let by_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    assert_eq!(by_name["data"], json!([]));

    let audited = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={reserved_resource_id}"),
    )
    .await?;
    let rows = audited["data"]
        .as_array()
        .expect("resource audit must return an array");
    assert!(!rows.is_empty(), "the reserved resource lost its audit read");
    assert!(
        rows.iter()
            .all(|row| row["authority_context"] == json!("resource_audit"))
    );

    database.cleanup().await?;
    Ok(())
}

// Every permission row says how it may be read. Only a `name` filter that selected the row's
// current registration claims `current_for_name`; a resource-keyed read never does, even when the
// row carries an optional display name.
#[tokio::test]
async fn v2_get_permissions_marks_the_authority_context_of_every_row() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let current_resource_id = v2_permissions_current_resource_id();

    for (uri, expected) in [
        ("/v1/permissions?name=Perms.eth".to_owned(), "current_for_name"),
        (
            format!("/v1/permissions?registration_id={current_resource_id}"),
            "resource_audit",
        ),
        (
            format!("/v1/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}"),
            "resource_audit",
        ),
    ] {
        let payload = v2_permissions_payload_for_database(&database, &uri).await?;
        let rows = payload["data"].as_array().expect("permissions data");
        assert!(!rows.is_empty(), "{uri} returned no rows to classify");
        assert!(
            rows.iter()
                .all(|row| row["authority_context"] == json!(expected)),
            "{uri} did not mark every row {expected}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

// A name filter selects only the exact-name authority's current registration. When the projection
// does not support the name, there is no such registration and the collection is empty rather than
// falling back to whatever resource the row still carries.
#[tokio::test]
async fn v2_get_permissions_empties_a_name_filter_the_projection_does_not_support() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;

    // Anti-vacuity: the name filter returns rows while the projection supports the name.
    let supported =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    assert!(
        !supported["data"]
            .as_array()
            .expect("permissions data")
            .is_empty()
    );

    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET support_status = 'unsupported',
             unsupported_reason = 'conflicting_current_ens_authority'
         WHERE lower(raw_name) = 'perms.eth'",
    )
    .execute(&database.pool)
    .await?;

    let payload =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

    sqlx::query("UPDATE bigname_phase.name_current SET surface_binding_id = NULL, resource_id = NULL, token_lineage_id = NULL, binding_kind = NULL, support_status = 'supported', unsupported_reason = NULL WHERE raw_name = 'perms.eth'")
        .execute(&database.pool)
        .await?;
    let unbound =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    assert_eq!(unbound["data"], json!([]));
    assert_eq!(unbound["meta"]["completeness"], json!("partial"));
    assert_eq!(
        unbound["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_maps_rows_and_lineage() -> Result<()> {
    let (database, payload) = v2_permissions_payload(&format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=10"
    ))
    .await?;
    let current_resource_id = v2_permissions_current_resource_id();
    let stale_resource_id = v2_permissions_stale_resource_id();

    assert_eq!(payload["page"]["page_size"], json!(10));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert!(payload["meta"].get("as_of").is_some());
    assert!(payload["meta"].get("as_of_token").is_none());
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );
    assert!(payload["meta"].get("unsupported_fields").is_none());

    let rows = payload["data"]
        .as_array()
        .expect("permissions data must be an array");
    assert_eq!(rows.len(), 3);
    let resolver = permission_row_by_scope_kind(rows, "resolver");
    let record_manager = permission_row_by_scope_kind(rows, "record_manager");
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
        &format!("/v1/permissions?registration_id={resource_id}"),
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
    let (database, by_name) = v2_permissions_payload("/v1/permissions?name=Perms.eth").await?;
    let current_resource_id = v2_permissions_current_resource_id();

    let name_rows = by_name["data"]
        .as_array()
        .expect("name-filtered permissions data");
    assert_eq!(name_rows.len(), 3);
    assert!(
        name_rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );
    assert_eq!(by_name["meta"]["completeness"], json!("partial"));
    assert_eq!(
        by_name["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );

    let by_registration = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={current_resource_id}"),
    )
    .await?;
    let registration_rows = by_registration["data"]
        .as_array()
        .expect("registration-filtered permissions data");
    assert_eq!(registration_rows.len(), 3);
    assert!(
        registration_rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );
    assert_eq!(by_registration["meta"]["completeness"], json!("partial"));
    assert_eq!(
        by_registration["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );

    let by_address_and_registration = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}&registration_id={current_resource_id}"
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
    assert_eq!(
        by_address_and_registration["meta"]["completeness"],
        json!("partial")
    );
    assert_eq!(
        by_address_and_registration["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_name_and_name_filtered_permissions_select_the_same_live_registration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let expected = v2_permissions_current_resource_id().to_string();

    let name = v2_name_record_payload_for_database(&database, "/v1/names/Perms.eth").await?;
    assert_eq!(name["data"]["registration_status"], json!("active"));
    assert_eq!(name["data"]["registration_id"], json!(expected));

    let permissions =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    let rows = permissions["data"].as_array().expect("permissions data");
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| {
        row["registration_id"] == name["data"]["registration_id"]
            && row["authority_context"] == json!("current_for_name")
    }));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_non_name_filters_carry_publication_metadata() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;

    for uri in [
        format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"),
        format!(
            "/v1/permissions?registration_id={}",
            v2_permissions_current_resource_id()
        ),
    ] {
        let response = v2_permissions_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let payload: Value = read_json(response).await?;
        assert!(!payload["data"].as_array().unwrap().is_empty(), "{uri}");
        assert!(payload["meta"].get("as_of").is_some(), "{uri}");
        assert!(payload["meta"].get("as_of_token").is_none(), "{uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_name_filter_uses_current_registration_with_publication_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let current_resource_id = v2_permissions_current_resource_id();

    let payload =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    let rows = payload["data"]
        .as_array()
        .expect("name-filtered permissions data");
    assert_eq!(rows.len(), 3);
    assert!(
        rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );
    assert!(payload["meta"].get("as_of").is_some());
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
        &format!("/v1/permissions?name={V2_SEPOLIA_SNAPSHOT_NAME}"),
    )
    .await?;
    assert_eq!(payload["data"][0]["registration_id"], json!(resource_id));
    assert!(payload["meta"].get("as_of").is_some());
    assert!(payload["meta"].get("as_of_token").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_paginates_and_rejects_mismatched_cursor() -> Result<()> {
    let (database, first_page) = v2_permissions_payload(&format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1"
    ))
    .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();

    let second_page = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["has_more"], json!(true));
    assert_ne!(first_page["data"], second_page["data"]);

    let cross_address = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v1/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}&page_size=1&cursor={next_cursor}"
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
            "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=1&cursor={next_cursor}"
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
        &format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"),
    )
    .await?;
    assert_eq!(by_address["data"], json!([]));
    assert_eq!(by_address["page"]["has_more"], json!(false));
    assert_eq!(by_address["page"]["next_cursor"], Value::Null);
    assert_eq!(by_address["meta"]["completeness"], json!("partial"));
    assert_eq!(
        by_address["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );

    let by_missing_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=missing.eth").await?;
    assert_eq!(by_missing_name["data"], json!([]));
    assert_eq!(by_missing_name["page"]["has_more"], json!(false));
    assert_eq!(by_missing_name["page"]["next_cursor"], Value::Null);
    assert_eq!(
        by_missing_name["meta"]["completeness"],
        json!("partial")
    );
    assert_eq!(
        by_missing_name["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

    let by_missing_name_and_registration = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?name=missing.eth&registration_id={}",
            v2_permissions_current_resource_id()
        ),
    )
    .await?;
    assert_eq!(by_missing_name_and_registration["data"], json!([]));
    assert_eq!(
        by_missing_name_and_registration["meta"]["completeness"],
        json!("partial")
    );
    assert_eq!(
        by_missing_name_and_registration["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

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
    let uri = format!("/v1/permissions?registration_id={resource_id}");

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
    let partial = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(partial["data"], json!([]));
    assert_eq!(partial["meta"]["completeness"], json!("partial"));
    assert_eq!(
        partial["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );

    sqlx::query(
        "UPDATE bigname_phase.permissions_current_resource_summary
         SET support_status = 'supported', unsupported_reason = NULL
         WHERE resource_id = $1",
    )
    .bind(resource_id)
    .execute(&database.pool)
    .await?;
    let synthetic_full = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(synthetic_full["data"], json!([]));
    assert!(synthetic_full["meta"].get("completeness").is_none());
    assert!(synthetic_full["meta"].get("unsupported_reason").is_none());

    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("wrapper")),
    )
    .await?;
    let wrapper = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(wrapper["data"], json!([]));
    assert_eq!(wrapper["meta"]["completeness"], json!("partial"));
    assert_eq!(
        wrapper["meta"]["unsupported_reason"],
        json!("parent_and_resolver_delegation_permissions_not_supported")
    );
    assert!(wrapper.get("restrictions").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_resource_bound_read_serves_wrapper_restrictions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    let mut summary = permission_current_resource_summary(resource_id, Some("wrapper"));
    summary.resource_restrictions = Some(json!({
        "kind": "ens_v1_wrapper",
        "wrapper_state": "locked",
        "fuses": 196_609,
        "expiry_seconds": 1_800_000_000,
    }));
    upsert_phase_permissions_current_resource_summary(&database.pool, &summary).await?;

    let registration = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={resource_id}"),
    )
    .await?;
    assert_eq!(
        registration["restrictions"],
        json!({
            "kind": "ens_v1_wrapper",
            "registration_id": resource_id.to_string(),
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
                "can_extend_expiry": false,
            },
            "wrapper_expires_at": "2027-01-15T08:00:00Z",
        })
    );
    assert_eq!(registration["meta"]["completeness"], json!("partial"));
    assert_eq!(
        registration["meta"]["unsupported_reason"],
        json!("parent_and_resolver_delegation_permissions_not_supported")
    );

    let by_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=perms.eth").await?;
    assert_eq!(by_name["restrictions"]["kind"], json!("ens_v1_wrapper"));

    let address_only = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=10"),
    )
    .await?;
    assert!(address_only.get("restrictions").is_none());
    assert!(!address_only["data"].as_array().expect("rows").is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_resource_bound_read_serves_registry_locked_roles() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    let mut summary = permission_current_resource_summary(resource_id, Some("ens_v2_registry"));
    summary.resource_restrictions = Some(json!({
        "kind": "ens_v2_registry",
        "locked_roles": ["renew", "transfer"],
    }));
    upsert_phase_permissions_current_resource_summary(&database.pool, &summary).await?;

    let payload = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={resource_id}"),
    )
    .await?;
    assert_eq!(
        payload["restrictions"],
        json!({
            "kind": "ens_v2_registry",
            "registration_id": resource_id.to_string(),
            "locked_roles": ["renew", "transfer"],
        })
    );
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );

    summary.resource_restrictions = None;
    upsert_phase_permissions_current_resource_summary(&database.pool, &summary).await?;
    let unrestricted = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={resource_id}"),
    )
    .await?;
    assert!(unrestricted.get("restrictions").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_serve_unprojected_authority_resources_as_partial() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    let uri = format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=10");

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
            3
        );
        assert_eq!(payload["meta"]["completeness"], json!("partial"));
        assert_eq!(
            payload["meta"]["unsupported_reason"],
            json!("permission_support_unknown")
        );
    }

    sqlx::query(
        "UPDATE bigname_phase.permissions_current_resource_summary
         SET support_status = 'unsupported', unsupported_reason = 'future_permission_reason'
         WHERE resource_id = $1",
    )
    .bind(resource_id)
    .execute(&database.pool)
    .await?;
    let response = v2_permissions_response_for_database(&database, &uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_admit_project_vocabulary_and_exclude_orphaned_projection_targets()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    let uri = format!("/v1/permissions?registration_id={resource_id}");

    let readable = v2_permissions_payload_for_database(&database, &uri).await?;
    assert!(!readable["data"].as_array().is_none_or(Vec::is_empty));
    assert_eq!(readable["meta"]["completeness"], json!("partial"));
    assert_eq!(
        readable["meta"]["unsupported_reason"],
        json!("approval_and_delegation_permissions_not_supported")
    );

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
    app_router(database.app_state_with_public_namespaces(&["ens"]))
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
    upsert_phase_permissions_current_rows(
        &database.pool,
        &[
            current_row,
            record_manager_row,
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

    let block_hash: String = sqlx::query_scalar("SELECT block_hash FROM bigname_phase.chain_lineage WHERE chain_id = 'ethereum-mainnet' AND block_number = 130 AND canonicality_state IN ('canonical', 'safe', 'finalized')")
        .fetch_one(&database.pool).await?;
    seed_schema_v2_ens_lookup_head(&database.pool, 130, &block_hash, "2026-06-10T00:00:00Z").await?;
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

#[tokio::test]
async fn v2_permissions_namespace_filters_audit_rows_before_paging_and_counting() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    // The lower-sorting registration belongs to another namespace. The matching ENS
    // registration has no current name and must remain readable through its audit identity.
    let mut events = Vec::new();
    for (namespace, resource_id, canonicality) in [
        ("basenames", v2_permissions_current_resource_id(), CanonicalityState::Canonical),
        ("ens", v2_permissions_stale_resource_id(), CanonicalityState::Canonical),
        ("ens", v2_permissions_current_resource_id(), CanonicalityState::Orphaned),
    ] {
        let mut event = history_event(
            &format!("permission-namespace-{namespace}-{resource_id}"), None, Some(resource_id),
            Some("ethereum-mainnet"), Some(99), Some("0xresource"), None, None, canonicality,
        );
        event.namespace = namespace.to_owned();
        event.event_kind = "PermissionChanged".to_owned();
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let unfiltered = v2_permissions_payload_for_database(
        &database, &format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"),
    ).await?;
    assert_eq!(unfiltered["data"].as_array().unwrap().len(), 3);
    let filtered = v2_permissions_payload_for_database(
        &database, &format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&namespace=ens&page_size=1"),
    ).await?;
    assert_eq!(filtered["data"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["data"][0]["registration_id"], json!(v2_permissions_stale_resource_id()));
    let storage_page = bigname_storage::load_permissions_current_account_resource_page(
        &database.pool, Some(V2_PERMISSIONS_SUBJECT), None, Some("ens"), None, 1,
    ).await?;
    assert_eq!(storage_page.summary.row_count, 1);
    assert_eq!(filtered["page"]["has_more"], json!(false));
    assert_eq!(filtered["page"]["next_cursor"], Value::Null);
    let mut summary = permission_current_resource_summary(v2_permissions_current_resource_id(), Some("ens_v2_registry"));
    summary.resource_restrictions = Some(json!({"kind": "ens_v2_registry", "locked_roles": ["renew"]}));
    upsert_phase_permissions_current_resource_summary(&database.pool, &summary).await?;
    let matching = v2_permissions_payload_for_database(
        &database, &format!("/v1/permissions?registration_id={}&namespace=ens", v2_permissions_stale_resource_id()),
    ).await?;
    assert_eq!(matching["data"].as_array().unwrap().len(), 1);
    let unscoped = v2_permissions_payload_for_database(
        &database, &format!("/v1/permissions?registration_id={}", v2_permissions_current_resource_id()),
    ).await?;
    assert!(unscoped.get("restrictions").is_some());
    let excluded = v2_permissions_payload_for_database(
        &database, &format!("/v1/permissions?registration_id={}&namespace=ens", v2_permissions_current_resource_id()),
    ).await?;
    assert_eq!(excluded["data"], json!([]));
    assert!(excluded.get("restrictions").is_none());
    assert!(excluded["meta"].get("completeness").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_rejects_unknown_namespace_before_snapshot_capture() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // No publication is needed to reject a namespace that this API does not recognize.
    for selector in [
        format!("address={V2_PERMISSIONS_SUBJECT}"),
        format!("registration_id={}", v2_permissions_current_resource_id()),
        "name=perms.eth".to_owned(),
    ] {
        let response = v2_permissions_response_for_database(
            &database, &format!("/v1/permissions?{selector}&namespace=unknown"),
        ).await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{selector}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("not_found"));
        assert_eq!(payload["error"]["message"], json!("namespace unknown is not supported"));
    }
    database.cleanup().await
}
