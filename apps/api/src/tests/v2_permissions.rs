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
async fn v2_permissions_rejects_non_public_namespace_before_cursor_decoding() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let path = "/v1/permissions?address=0x0000000000000000000000000000000000000eee&namespace=bogus";
    let response = v2_permissions_response_for_database(&database, path).await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = v2_permissions_response_for_database(&database, &format!("{path}&cursor=bogus")).await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture_named(&database, NORMALIZED_NAME).await?;
    let stored_raw_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_surfaces WHERE logical_name_id = $1")
            .bind(bigname_storage::logical_name_id_for_name(
                "ens",
                NORMALIZED_NAME,
            ))
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
    assert!(rows.iter().all(|row| row["name"] == json!(stored_raw_name)));

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
    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(stale_resource_id, Some("wrapper")),
    )
    .await?;

    let paired = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=perms.eth&registration_id={stale_resource_id}"),
    )
    .await?;
    assert_eq!(paired["data"], json!([]));
    assert_eq!(paired["meta"]["completeness"], json!("partial"));
    assert_unlisted_permission_surfaces(&paired, V2_WRAPPER_UNLISTED_SURFACES);

    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(stale_resource_id, Some("registrar")),
    ).await?;
    let paired = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=perms.eth&registration_id={stale_resource_id}"),
    ).await?;
    assert_unlisted_permission_surfaces(&paired, V2_UNWRAPPED_UNLISTED_SURFACES);

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

// A name paired with another current name's registration is a superseded pair too. Its empty
// page is classified like a standalone read of that registration: for a wrapped `.eth` lease, from
// the NameWrapper resource the lease resolves to, not from the lease's own summary. The NameWrapper
// resource itself is not a registration, so pairing the name with it keeps the raw resource
// classification, and reading it alone still answers the empty not-a-registration page.
#[tokio::test]
async fn v2_get_permissions_classifies_a_paired_wrapped_lease_like_its_standalone_read()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // alpha.eth serves lease L; its permission rows and wrapper summary live on NameWrapper
    // resource W. beta.eth is a second supported current name serving its own registration.
    let wrapper_resource_id = Uuid::from_u128(0xa100);
    let lease_resource_id = Uuid::from_u128(0xe400);
    seed_alpha_registrar_lease(&database, lease_resource_id).await?;
    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(wrapper_resource_id, Some("wrapper")),
    )
    .await?;
    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(lease_resource_id, Some("registrar")),
    )
    .await?;

    let standalone = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={lease_resource_id}"),
    )
    .await?;
    assert!(!standalone["data"].as_array().expect("lease rows").is_empty());
    assert_unlisted_permission_surfaces(&standalone, V2_WRAPPER_UNLISTED_SURFACES);

    let paired = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=beta.eth&registration_id={lease_resource_id}"),
    )
    .await?;
    assert_eq!(paired["data"], json!([]));
    assert!(paired.get("restrictions").is_none(), "{paired}");
    assert_eq!(
        paired["meta"], standalone["meta"],
        "the paired read must classify support like the standalone read of the lease"
    );

    // Control: the NameWrapper resource is not a registration. Paired with the name it keeps its
    // raw resource classification; alone it selects nothing and claims no completeness.
    let paired_wrapper = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=beta.eth&registration_id={wrapper_resource_id}"),
    )
    .await?;
    assert_eq!(paired_wrapper["data"], json!([]));
    assert!(paired_wrapper.get("restrictions").is_none(), "{paired_wrapper}");
    assert_unlisted_permission_surfaces(&paired_wrapper, V2_WRAPPER_UNLISTED_SURFACES);
    let alone_wrapper = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={wrapper_resource_id}"),
    )
    .await?;
    assert_eq!(alone_wrapper["data"], json!([]));
    assert!(alone_wrapper.get("restrictions").is_none(), "{alone_wrapper}");
    assert!(
        alone_wrapper["meta"].get("completeness").is_none(),
        "{}",
        alone_wrapper["meta"]
    );

    database.cleanup().await
}

// A cursor is bound to the registration handle the request named, not to the resource that
// handle reads. A page of `?registration_id=<lease>` continues under the lease; under the
// NameWrapper resource the lease reads, which is a different request, the cursor is rejected.
#[tokio::test]
async fn v2_get_permissions_cursor_binds_the_requested_registration_id() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let wrapper_resource_id = Uuid::from_u128(0xa100);
    let lease_resource_id = Uuid::from_u128(0xe400);
    seed_alpha_registrar_lease(&database, lease_resource_id).await?;

    let first = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={lease_resource_id}&page_size=1"),
    )
    .await?;
    assert_eq!(first["data"].as_array().expect("first page").len(), 1);
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("the lease has more than one permission row")
        .to_owned();

    let continued = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={lease_resource_id}&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(continued["data"].as_array().expect("second page").len(), 1);
    assert_ne!(continued["data"][0], first["data"][0]);

    let response = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v1/permissions?registration_id={wrapper_resource_id}&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    // A name-only page binds the name's public registration, the lease, not the NameWrapper
    // resource that holds its rows. The same name continues it, the same name with the lease
    // continues it (one collection), and the same name with the NameWrapper resource, the
    // proven-empty pair, is a different request and is rejected.
    let first = v2_permissions_payload_for_database(
        &database,
        "/v1/permissions?name=alpha.eth&page_size=1",
    )
    .await?;
    assert_eq!(first["data"].as_array().expect("first name page").len(), 1);
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("the name has more than one permission row")
        .to_owned();
    let continued = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=alpha.eth&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(continued["data"].as_array().expect("second name page").len(), 1);
    assert_ne!(continued["data"][0], first["data"][0]);
    let with_lease = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?name=alpha.eth&registration_id={lease_resource_id}&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(with_lease["data"], continued["data"], "{with_lease}");
    let response = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v1/permissions?name=alpha.eth&registration_id={wrapper_resource_id}&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"), "{payload}");

    database.cleanup().await
}

// The NameWrapper resource that wrapped a lease stays outside the public handle space after the
// name leaves it. Once the name's current row no longer names the wrapper (unwrapped, released,
// migrated, registered again, or, as here, unsupported), only the recorded wrap link can reject
// the resource, as history does.
#[tokio::test]
async fn v2_get_permissions_rejects_a_historical_name_wrapper_resource() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (wrapper_resource_id, lease_resource_id) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET support_status = 'unsupported',
             unsupported_reason = 'conflicting_current_ens_authority'
         WHERE raw_name = 'perms.eth'",
    )
    .execute(&database.pool)
    .await?;

    for uri in [
        format!("/v1/permissions?registration_id={wrapper_resource_id}"),
        format!(
            "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&registration_id={wrapper_resource_id}"
        ),
    ] {
        let response = v2_permissions_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["data"], json!([]), "{uri}: {payload}");
        assert!(payload.get("restrictions").is_none(), "{uri}");
        assert!(
            payload["meta"].get("completeness").is_none(),
            "{uri}: {}",
            payload["meta"]
        );
    }

    // Control: the lease itself stays a registration handle.
    let response = v2_permissions_response_for_database(
        &database,
        &format!("/v1/permissions?registration_id={lease_resource_id}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);

    database.cleanup().await
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
    assert_unlisted_permission_surfaces(&payload, V2_ALL_UNLISTED_SURFACES);
    assert!(payload["meta"].get("unsupported_fields").is_none());

    let rows = payload["data"]
        .as_array()
        .expect("permissions data must be an array");
    assert_eq!(rows.len(), 3);
    let resolver = permission_row_by_scope_kind(rows, "resolver");
    let current_grant = rows.iter().find(|row| row["registration_id"] == json!(current_resource_id)
        && row["grant_scope"]["kind"] == "registration").expect("current registrar grant");
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
        json!(["set_text"])
    );
    assert_eq!(resolver["lineage"], json!({"grant":{"kind":"event"}}));
    assert_eq!(current_grant["powers"], json!(["registration_control"]));
    assert_eq!(current_grant["lineage"], json!({"grant":{"kind":"ens_v1_authority"}}));

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
        json!(["registration_control"])
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
async fn v2_permissions_serves_registry_operator_for_address_name_and_registration() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    let resource_id = v2_permissions_current_resource_id();
    for uri in [
        format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"),
        "/v1/permissions?name=perms.eth".to_owned(),
        format!("/v1/permissions?registration_id={resource_id}"),
    ] {
        let payload = v2_permissions_payload_for_database(&database, &uri).await?;
        assert!(operator_row(&payload).is_some(), "operator row missing for {uri}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_combined_filters_intersect_operator_rows() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    let resource_id = v2_permissions_current_resource_id();
    let matched = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=perms.eth&registration_id={resource_id}&address={V2_PERMISSIONS_SUBJECT}"),
    ).await?;
    assert!(operator_row(&matched).is_some());
    let missed = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=perms.eth&registration_id={resource_id}&address={V2_PERMISSIONS_OTHER_SUBJECT}"),
    ).await?;
    assert!(operator_row(&missed).is_none());
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_omits_direct_grant_relation() -> Result<()> {
    let (database, payload) = v2_permissions_payload(&format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"
    )).await?;
    assert!(payload["data"].as_array().unwrap().iter().all(|row| row.get("grant_relation").is_none()));
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_serves_revocation_as_absence() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    insert_permission_registry_approval(&database, false, 122).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    let payload = v2_permissions_payload_for_database(&database, &format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"
    )).await?;
    assert!(operator_row(&payload).is_none());
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_does_not_carry_operator_across_registry_generations() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    insert_permission_registry_owner(
        &database, v2_permissions_current_resource_id(), "0x0000000000000000000000000000000000000d44", 122,
    ).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    let payload = v2_permissions_payload_for_database(&database, &format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"
    )).await?;
    assert!(operator_row(&payload).is_none());
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_emits_account_scope_and_registry_control() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    let payload = v2_permissions_payload_for_database(&database, &format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"
    )).await?;
    let row = operator_row(&payload).expect("operator row");
    assert_eq!(row["grant_scope"], json!({"kind":"account","detail":{
        "chain_id":1,"authority_kind":"registry","authority_contract":V2_OPERATOR_REGISTRY,
        "owner":V2_OPERATOR_OWNER}}));
    assert_eq!(row["powers"], json!(["registry_control"]));
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_operator_lineage_uses_approval_grant() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    let payload = v2_permissions_payload_for_database(&database, &format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage"
    )).await?;
    let row = operator_row(&payload).expect("operator row");
    assert_eq!(row["lineage"]["grant"], json!({"kind":"event"}));
    assert!(row["lineage"].get("registry_binding").is_none());
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_cursor_crosses_direct_operator_boundary() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    let base = format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1");
    let first = v2_permissions_payload_for_database(&database, &base).await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("second row");
    let second = v2_permissions_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    let rows = [first["data"][0].clone(), second["data"][0].clone()];
    assert_eq!(rows.iter().filter(|row| row.get("grant_relation").is_some()).count(), 1);
    assert_ne!(rows[0]["grant_scope"], rows[1]["grant_scope"]);
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_cursor_binds_account_collection_anchor() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    let first = v2_permissions_payload_for_database(&database, &format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1"
    )).await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("cursor");
    let response = v2_permissions_response_for_database(&database, &format!(
        "/v1/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}&page_size=1&cursor={cursor}"
    )).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_namespace_filters_before_operator_paging() -> Result<()> {
    let database = seed_v2_registry_operator_fixture().await?;
    let resource_id = Uuid::from_u128(0xf100);
    database.seed_name_current_binding(
        "basenames:base-perms.base.eth", "basenames", "base-perms.base.eth", "base-perms.base.eth",
        "base-perms", resource_id, Uuid::from_u128(0xf101), Uuid::from_u128(0xf102),
    ).await?;
    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("registrar")),
    ).await?;
    upsert_phase_permissions_current_rows(&database.pool, &[permission_current_row(
        resource_id, V2_PERMISSIONS_SUBJECT, PermissionScope::Resource, 1, 112,
    )]).await?;
    seed_registry_operator(&database, resource_id).await?;
    seed_permission_namespace_event(&database, "basenames", resource_id).await?;

    // The collection publication for a Basenames read spans Base and Ethereum.
    database.seed_snapshot_selector_chain_positions(&json!({"base": {
        "chain_id": "base-mainnet", "block_number": 84_530_001,
        "block_hash": "0xpermissions-base", "timestamp": "2026-04-17T00:10:01Z",
    }})).await?;
    let get = async |uri: String| -> Result<Value> {
        let response = app_router(database.app_state())
            .oneshot(Request::builder().uri(uri).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        read_json(response).await
    };
    let base = format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&namespace=basenames&page_size=1");
    let first = get(base.clone()).await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("second Basenames row");
    let second = get(format!("{base}&cursor={cursor}")).await?;
    for row in [&first["data"][0], &second["data"][0]] {
        assert_eq!(row["registration_id"], json!(resource_id.to_string()));
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_resource_reason_names_registrar_and_resolver_gaps() -> Result<()> {
    let (database, payload) = v2_permissions_payload("/v1/permissions?name=perms.eth").await?;
    assert_unlisted_permission_surfaces(&payload, V2_UNWRAPPED_UNLISTED_SURFACES);
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_ens_v2_registry_resource_names_registry_operator_gap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("ens_v2_registry")),
    )
    .await?;

    for selector in [format!("registration_id={resource_id}"), "name=perms.eth".to_owned()] {
        let payload =
            v2_permissions_payload_for_database(&database, &format!("/v1/permissions?{selector}"))
                .await?;
        assert_unlisted_permission_surfaces(&payload, V2_ENS_V2_REGISTRY_UNLISTED_SURFACES);
        // An ENSv2 registration has no BaseRegistrar token, so that code must not appear.
        let surfaces = payload["meta"]["unlisted_permission_surfaces"]
            .as_array()
            .expect("surfaces");
        assert!(!surfaces.contains(&json!("registrar_approvals")), "{selector}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_account_reason_names_all_four_gaps() -> Result<()> {
    let (database, payload) = v2_permissions_payload(&format!(
        "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"
    )).await?;
    assert_unlisted_permission_surfaces(&payload, V2_ALL_UNLISTED_SURFACES);
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_empty_account_result_remains_request_relative_partial() -> Result<()> {
    let (database, payload) = v2_permissions_payload(
        "/v1/permissions?address=0x0000000000000000000000000000000000000eee",
    )
    .await?;
    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_unlisted_permission_surfaces(&payload, V2_ALL_UNLISTED_SURFACES);
    database.cleanup().await
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
    assert_unlisted_permission_surfaces(&by_name, V2_UNWRAPPED_UNLISTED_SURFACES);

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
    assert_unlisted_permission_surfaces(&by_registration, V2_UNWRAPPED_UNLISTED_SURFACES);

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
    assert_unlisted_permission_surfaces(&by_address_and_registration, V2_UNWRAPPED_UNLISTED_SURFACES);

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

/// How a wrapped `.eth` name's NameWrapper resource came to hold its BaseRegistrar lease.
#[derive(Clone, Copy, Debug)]
enum WrappedLeaseShape {
    /// The name was wrapped after its registration, so `NameWrapped` recorded the lease.
    LinkRecorded,
    /// The name was registered through the NameWrapper: `NameWrapped` recorded no lease and
    /// the registrar controller's later grant names the lease. The lease has no binding of its
    /// own; Project selects it as the name's registration.
    ControllerGranted,
}

#[tokio::test]
async fn wrapped_name_permissions_carry_the_registrar_lease_handle() -> Result<()> {
    assert_wrapped_name_permissions_carry_the_registrar_lease_handle(
        WrappedLeaseShape::LinkRecorded,
    )
    .await
}

#[tokio::test]
async fn controller_granted_wrapped_name_permissions_carry_the_registrar_lease_handle()
-> Result<()> {
    assert_wrapped_name_permissions_carry_the_registrar_lease_handle(
        WrappedLeaseShape::ControllerGranted,
    )
    .await
}

/// Give perms.eth the shape of a wrapped `.eth` name: the fixture's bound resource plays the
/// NameWrapper resource and a new BaseRegistrar lease, which Project serves as the registration,
/// is linked to it by the shape's rule. Returns `(wrapper_resource_id, lease_resource_id)`.
async fn seed_perms_wrapped_lease(
    database: &TestDatabase,
    shape: WrappedLeaseShape,
) -> Result<(Uuid, Uuid)> {
    seed_v2_permissions_fixture(database).await?;
    // The fixture's bound resource plays the NameWrapper resource of a wrapped `.eth` name;
    // Project serves the BaseRegistrar lease it wrapped as the registration resource.
    let wrapper_resource_id = v2_permissions_current_resource_id();
    let lease_resource_id = Uuid::from_u128(0xe300);
    upsert_test_resources(&database.pool, &[resource(lease_resource_id)]).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary,
             '{registration,resource_id}',
             to_jsonb($1::text),
             true
         )
         WHERE raw_name = 'perms.eth'",
    )
    .bind(lease_resource_id)
    .execute(&database.pool)
    .await?;
    let logical_name_id: String =
        sqlx::query_scalar("SELECT logical_name_id FROM bigname_phase.name_current WHERE raw_name = 'perms.eth'")
            .fetch_one(&database.pool)
            .await?;
    let namehash = bigname_lookup::ens_namehash_hex("perms.eth")?;
    // The wrapper's constraint model must be served under the lease's handle on every page.
    let mut summary = permission_current_resource_summary(wrapper_resource_id, Some("wrapper"));
    summary.resource_restrictions = Some(json!({
        "kind": "ens_v1_wrapper",
        "wrapper_state": "wrapped",
        "fuses": 0,
        "expiry_seconds": 1_800_000_000,
    }));
    upsert_phase_permissions_current_resource_summary(&database.pool, &summary).await?;
    // The lease's own rows carry the node. With a recorded link the NameWrapped binding names
    // the lease; without one the later controller grant names the name.
    let (grant_block, grant_logical_name_id, binding_block, link) = match shape {
        WrappedLeaseShape::LinkRecorded => (120, None, 121, json!(lease_resource_id)),
        WrappedLeaseShape::ControllerGranted => {
            (120, Some(logical_name_id.as_str()), 120, Value::Null)
        }
    };
    let mut grant = v2_history_event(
        "perms-lease-grant",
        grant_logical_name_id,
        Some(lease_resource_id),
        "RegistrationGranted",
        grant_block,
    );
    grant.after_state["namehash"] = json!(namehash);
    if matches!(shape, WrappedLeaseShape::ControllerGranted) {
        grant.log_index = Some(2);
    }
    let mut binding = v2_history_event(
        "perms-wrapper-binding",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "SurfaceBound",
        binding_block,
    );
    binding.source_family = "ens_v1_wrapper_l1".to_owned();
    binding.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": link,
    });
    seed_v2_history_blocks(database, 120..=121).await?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, binding]).await?;
    Ok((wrapper_resource_id, lease_resource_id))
}

async fn assert_wrapped_name_permissions_carry_the_registrar_lease_handle(
    shape: WrappedLeaseShape,
) -> Result<()> {
    const UNGRANTED_ADDRESS: &str = "0x00000000000000000000000000000000000000ee";
    let database = TestDatabase::new_migrated().await?;
    let (wrapper_resource_id, lease_resource_id) =
        seed_perms_wrapped_lease(&database, shape).await?;

    let name = v2_name_record_payload_for_database(&database, "/v1/names/Perms.eth").await?;
    assert_eq!(
        name["data"]["registration_id"],
        json!(lease_resource_id.to_string())
    );
    let by_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=Perms.eth").await?;
    let rows = by_name["data"].as_array().expect("permissions data");
    assert!(!rows.is_empty());
    assert!(
        rows.iter().all(|row| {
            row["registration_id"] == name["data"]["registration_id"]
                && row["authority_context"] == json!("current_for_name")
        }),
        "{shape:?}: permission rows must carry the registration_id the name serves: {rows:?}"
    );
    assert_eq!(
        by_name["restrictions"]["registration_id"],
        json!(lease_resource_id.to_string()),
        "{shape:?}: {}",
        by_name["restrictions"]
    );

    let paired = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=Perms.eth&registration_id={lease_resource_id}"),
    )
    .await?;
    assert_eq!(paired["data"], by_name["data"], "{shape:?}");
    assert_eq!(paired["restrictions"], by_name["restrictions"], "{shape:?}");

    // The registration_id read from the name selects the same permissions on its own.
    let by_lease = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={lease_resource_id}"),
    )
    .await?;
    let lease_rows = by_lease["data"].as_array().expect("lease permissions");
    assert_eq!(lease_rows.len(), rows.len(), "{shape:?}: {lease_rows:?}");
    assert!(lease_rows.iter().all(|row| {
        row["registration_id"] == json!(lease_resource_id.to_string())
            && row["authority_context"] == json!("resource_audit")
    }), "{shape:?}: {lease_rows:?}");
    assert_eq!(
        by_lease["restrictions"]["registration_id"],
        json!(lease_resource_id.to_string()),
        "{shape:?}: {}",
        by_lease["restrictions"]
    );

    // An address with no grant leaves the page empty, not the registration unidentified.
    for uri in [
        format!("/v1/permissions?name=Perms.eth&address={UNGRANTED_ADDRESS}"),
        format!("/v1/permissions?registration_id={lease_resource_id}&address={UNGRANTED_ADDRESS}"),
    ] {
        let payload = v2_permissions_payload_for_database(&database, &uri).await?;
        assert_eq!(payload["data"], json!([]), "{shape:?}: {uri}");
        assert_eq!(
            payload["restrictions"]["registration_id"],
            json!(lease_resource_id.to_string()),
            "{shape:?}: {uri}: {}",
            payload["restrictions"]
        );
        assert_eq!(payload["restrictions"]["kind"], json!("ens_v1_wrapper"), "{shape:?}: {uri}");
    }

    // The NameWrapper resource is not the name's registration.
    let wrapper_pair = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=Perms.eth&registration_id={wrapper_resource_id}"),
    )
    .await?;
    assert_eq!(wrapper_pair["data"], json!([]), "{shape:?}");

    // Nor does it select anything on its own: history rejects the same value, so permissions
    // must not serve the wrapper's rows under it.
    for uri in [
        format!("/v1/permissions?registration_id={wrapper_resource_id}"),
        format!(
            "/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&registration_id={wrapper_resource_id}"
        ),
    ] {
        let response = v2_permissions_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::OK, "{shape:?}: {uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["data"], json!([]), "{shape:?}: {uri}");
        assert_eq!(payload["page"]["has_more"], json!(false), "{shape:?}: {uri}");
        assert_eq!(payload["page"]["next_cursor"], Value::Null, "{shape:?}: {uri}");
        assert_eq!(payload["meta"]["as_of"], by_lease["meta"]["as_of"], "{shape:?}: {uri}");
        assert!(payload.get("restrictions").is_none(), "{shape:?}: {uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn wrapped_subname_permissions_read_by_the_name_wrapper_resource() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    // A wrapped subname has no BaseRegistrar lease: Project records no registration resource,
    // so its NameWrapper resource is its registration_id.
    let wrapper_resource_id = v2_permissions_current_resource_id();
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary,
             '{registration}',
             '{\"status\": \"wrapped\", \"authority_kind\": \"wrapper\"}'::jsonb,
             true
         )
         WHERE raw_name = 'perms.eth'",
    )
    .execute(&database.pool)
    .await?;

    let name = v2_name_record_payload_for_database(&database, "/v1/names/Perms.eth").await?;
    assert_eq!(
        name["data"]["registration_id"],
        json!(wrapper_resource_id.to_string())
    );
    let by_registration = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={wrapper_resource_id}"),
    )
    .await?;
    let rows = by_registration["data"].as_array().expect("permissions data");
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(rows.iter().all(|row| {
        row["registration_id"] == json!(wrapper_resource_id.to_string())
            && row["authority_context"] == json!("resource_audit")
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
async fn v2_permissions_cursor_binds_name_selection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let other_resource_id = Uuid::from_u128(0xe300);
    let logical = seed_family_identity_inputs(&database.pool, "ens", "other.eth", "ethereum-mainnet",
        100, "0xperms100", other_resource_id, Uuid::from_u128(0xe301), Uuid::from_u128(0xe302), "ens_v1").await?;
    let grant = permission_fixture_event("permissions-other-grant", Some(&logical), Some(other_resource_id),
        "RegistrationGranted", "ens_v1_registrar_l1", 101, 2,
        json!({"authority_kind":"registrar", "registrant":V2_PERMISSIONS_SUBJECT, "expiry":1_900_000_000_i64}));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;

    let first = v2_permissions_payload_for_database(
        &database,
        "/v1/permissions?name=Perms.eth&page_size=1",
    )
    .await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("name page must have a cursor");
    let replay = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v1/permissions?name=other.eth&registration_id={}&page_size=1&cursor={cursor}",
            v2_permissions_current_resource_id()
        ),
    )
    .await?;

    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(replay).await?["error"]["code"],
        json!("invalid_input")
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_permissions_empty_results_return_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    publish_test_families_on(&database.pool, "ethereum-mainnet", 21_000_003).await?;

    let by_address = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"),
    )
    .await?;
    assert_eq!(by_address["data"], json!([]));
    assert_eq!(by_address["page"]["has_more"], json!(false));
    assert_eq!(by_address["page"]["next_cursor"], Value::Null);
    assert_eq!(by_address["meta"]["completeness"], json!("partial"));
    assert_unlisted_permission_surfaces(&by_address, V2_ALL_UNLISTED_SURFACES);

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
    assert_unlisted_permission_surfaces(&partial, V2_UNWRAPPED_UNLISTED_SURFACES);

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
    // Independently proven full support lists every surface, so nothing is reported.
    for key in ["completeness", "unsupported_reason", "unlisted_permission_surfaces"] {
        assert!(synthetic_full["meta"].get(key).is_none(), "{key}");
    }

    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("wrapper")),
    )
    .await?;
    let wrapper = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(wrapper["data"], json!([]));
    assert_eq!(wrapper["meta"]["completeness"], json!("partial"));
    assert_unlisted_permission_surfaces(&wrapper, V2_WRAPPER_UNLISTED_SURFACES);
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
    assert_unlisted_permission_surfaces(&registration, V2_WRAPPER_UNLISTED_SURFACES);

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
    assert_unlisted_permission_surfaces(&payload, V2_ENS_V2_REGISTRY_UNLISTED_SURFACES);

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
    assert_unlisted_permission_surfaces(&readable, V2_UNWRAPPED_UNLISTED_SURFACES);

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
const V2_OPERATOR_OWNER: &str = "0x0000000000000000000000000000000000000a11";
const V2_OPERATOR_REGISTRY: &str = "0x0000000000000000000000000000000000000c33";
const V2_UNWRAPPED_UNLISTED_SURFACES: &[&str] = &["registrar_approvals", "resolver_approvals"];
const V2_WRAPPER_UNLISTED_SURFACES: &[&str] = &["resolver_approvals", "wrapper_parent_control"];
const V2_ENS_V2_REGISTRY_UNLISTED_SURFACES: &[&str] =
    &["ens_v2_registry_operators", "resolver_approvals"];
const V2_ALL_UNLISTED_SURFACES: &[&str] = &[
    "ens_v2_registry_operators",
    "registrar_approvals",
    "resolver_approvals",
    "wrapper_parent_control",
];

// Known partial permission coverage: one generic reason plus the sorted unlisted surfaces.
fn assert_unlisted_permission_surfaces(payload: &Value, surfaces: &[&str]) {
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("permissions_partially_listed")
    );
    assert_eq!(payload["meta"]["unlisted_permission_surfaces"], json!(surfaces));
}

fn operator_row(payload: &Value) -> Option<&Value> {
    payload["data"].as_array()?.iter().find(|row| {
        row.get("grant_relation").and_then(Value::as_str) == Some("operator")
    })
}

async fn seed_v2_registry_operator_fixture() -> Result<TestDatabase> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    seed_registry_operator(&database, v2_permissions_current_resource_id()).await?;
    Ok(database)
}

// Namespace membership comes from retained canonical interpreted events, so namespace-filtered
// reads need one for every registration they expect to see.
async fn seed_permission_namespace_event(
    database: &TestDatabase,
    namespace: &str,
    resource_id: Uuid,
) -> Result<()> {
    let (chain_id, block_hash, block_number): (String, String, i64) = sqlx::query_as(
        "SELECT chain_id, block_hash, block_number FROM bigname_phase.resources WHERE resource_id = $1",
    )
    .bind(resource_id)
    .fetch_one(&database.pool)
    .await?;
    let mut event = history_event(
        &format!("permission-namespace-{namespace}-{resource_id}"), None, Some(resource_id),
        Some(&chain_id), Some(block_number), Some(&block_hash), None, None,
        CanonicalityState::Canonical,
    );
    event.namespace = namespace.to_owned();
    event.event_kind = "PermissionChanged".to_owned();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(())
}

async fn seed_registry_operator(database: &TestDatabase, resource_id: Uuid) -> Result<()> {
    insert_permission_registry_owner(database, resource_id, V2_OPERATOR_REGISTRY, 120).await?;
    insert_permission_registry_approval(database, true, 121).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await
}

async fn insert_permission_registry_owner(
    database: &TestDatabase,
    resource_id: Uuid,
    registry: &str,
    block: i64,
) -> Result<()> {
    let (name, node): (String, String) = sqlx::query_as(
        "SELECT surface.logical_name_id, surface.namehash FROM name_surfaces surface
         JOIN surface_bindings binding USING (logical_name_id) WHERE binding.resource_id = $1",
    ).bind(resource_id).fetch_one(&database.pool).await?;
    let event = permission_fixture_event(
        &format!("permissions-operator-owner-{resource_id}-{block}"), Some(&name), Some(resource_id),
        "AuthorityTransferred", "ens_v1_registry_l1", block, 0,
        json!({"source_event":"Transfer", "node":node, "owner":V2_OPERATOR_OWNER,
            "owner_getter":V2_OPERATOR_OWNER, "registry_contract":registry, "emitter_role":"registry"}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(())
}

async fn insert_permission_registry_approval(
    database: &TestDatabase,
    approved: bool,
    block: i64,
) -> Result<()> {
    let source = json!({"kind":"raw_log", "source_event":"ApprovalForAll"});
    let event = permission_fixture_event(
        &format!("permissions-operator-approval-{block}"), None, None,
        "AccountPermissionChanged", "ens_v1_registry_l1", block, 0,
        json!({"subject":V2_PERMISSIONS_SUBJECT, "relation_kind":"operator", "approved":approved,
            "scope":{"kind":"account", "chain_id":"ethereum-mainnet", "authority_kind":"registry",
                "authority_contract":V2_OPERATOR_REGISTRY,
                "authority_contract_instance_id":Uuid::from_u128(0x605), "owner":V2_OPERATOR_OWNER},
            "effective_powers":if approved { json!(["registry_control"]) } else { json!([]) },
            "grant_source":if approved { source.clone() } else { json!({}) },
            "revocation_source":if approved { Value::Null } else { source }, "inheritance_path":[],
            "transfer_behavior":{"mode":"owner_scoped", "on_registry_owner_change":"ceases_to_apply"},
            "source_event":"ApprovalForAll"}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(())
}

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
    seed_v2_permissions_fixture_named(database, "perms.eth").await
}

/// A current registrar name, its resource grant, two resolver role holders and a nameless
/// retained registrar grant. Each permission is a normalized event reduced by Project.
async fn seed_v2_permissions_fixture_named(database: &TestDatabase, name: &str) -> Result<()> {
    let chain = "ethereum-mainnet";
    let target = parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .unix_timestamp();
    let blocks = (99..=130)
        .map(|block| {
            raw_block(
                chain,
                &format!("0xperms{block}"),
                None,
                block,
                target - 130 + block,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    database.seed_snapshot_selector_chain_positions(&json!({"ethereum":{
        "chain_id":chain,"block_number":130,"block_hash":"0xperms130","timestamp":"2026-06-10T00:00:00Z"}})).await?;
    let current = v2_permissions_current_resource_id();
    let stale = v2_permissions_stale_resource_id();
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        chain,
        100,
        "0xperms100",
        current,
        Uuid::from_u128(0xe102),
        Uuid::from_u128(0xe103),
        "ens_v1",
    )
    .await?;
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
        VALUES ($1, $2, 100, '0xperms100', 'canonical')").bind(stale).bind(chain).execute(&database.pool).await?;
    let resolver = "0x0000000000000000000000000000000000000abc";
    let implementation = "0x0000000000000000000000000000000000000fed";
    let payload = json!({"contracts":[], "resolver_implementations":[{"role":"permissioned_resolver","address":implementation}]});
    let manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_resolver_l1',$1,'fixture','active','fixture','fixture/permissions-resolver.toml',$2) RETURNING manifest_id")
        .bind(chain).bind(&payload).fetch_one(&database.pool).await?;
    seed_fixture_manifest_update(
        &database.pool,
        manifest,
        chain,
        "ens",
        "ens_v2_resolver_l1",
        &payload,
    )
    .await?;
    let mut upgrade = permission_fixture_event(
        "permissions-upgrade",
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        104,
        0,
        json!({"source_event":"Upgraded", "proxy_address":resolver,"implementation":implementation}),
    );
    upgrade.source_manifest_id = Some(manifest);
    upgrade.manifest_version = 1;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let mut events = vec![
        upgrade,
        permission_fixture_event(
            "permissions-current-grant",
            Some(&logical),
            Some(current),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            101,
            0,
            json!({"authority_kind":"registrar", "registrant":V2_PERMISSIONS_SUBJECT, "expiry":1_900_000_000_i64}),
        ),
        permission_fixture_event(
            "permissions-stale-grant",
            None,
            Some(stale),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            101,
            1,
            json!({"authority_kind":"registrar", "registrant":V2_PERMISSIONS_SUBJECT, "expiry":1_900_000_000_i64}),
        ),
        permission_fixture_event(
            "permissions-owner",
            Some(&logical),
            Some(current),
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            102,
            0,
            json!({"source_event":"Transfer", "node":node,"owner":V2_PERMISSIONS_SUBJECT,"owner_getter":V2_PERMISSIONS_SUBJECT,"emitter_role":"registry"}),
        ),
    ];
    for (resource, name) in [(current, Some(logical.as_str())), (stale, None)] {
        let source = json!({"kind":"ens_v1_authority", "authority_kind":"registrar",
            "authority_key":format!("registrar:{chain}:{resource}"), "source_event_kind":"Transfer"});
        events.push(permission_fixture_event(&format!("permissions-authority-{resource}"), name, Some(resource),
            "PermissionChanged", "ens_v1_registrar_l1", 105, 0,
            json!({"subject":V2_PERMISSIONS_SUBJECT,"scope":{"kind":"resource"},
                "effective_powers":["resource_control"], "grant_source":source,"revocation_source":null,
                "inheritance_path":[],"transfer_behavior":"replace_on_authority_change"})));
    }
    for (index, subject, powers) in [
        (0, V2_PERMISSIONS_SUBJECT, json!(["set_text"])),
        (1, V2_PERMISSIONS_OTHER_SUBJECT, json!(["set_addr"])),
    ] {
        let mut event =
            collection_role_event(current, subject, resolver, 110, index, powers, manifest);
        event.block_hash = Some("0xperms110".into());
        event.after_state["resource"] = json!(node);
        event.after_state["upstream_resource"] = json!(node);
        event.after_state["grant_source"]["upstream_resource"] = json!(node);
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, chain, 130, "0xperms130").await
}

#[allow(clippy::too_many_arguments)]
fn permission_fixture_event(
    identity: &str,
    name: Option<&str>,
    resource: Option<Uuid>,
    kind: &str,
    family: &str,
    block: i64,
    log: i64,
    after: Value,
) -> NormalizedEvent {
    let mut event = v2_history_event(identity, name, resource, kind, block);
    event.block_hash = Some(format!("0xperms{block}"));
    event.source_family = family.into();
    event.log_index = Some(log);
    event.after_state = after;
    event
}

async fn replace_permission_resolver_roles(
    database: &TestDatabase,
    block: i64,
    powers: Value,
    selector: Option<Value>,
) -> Result<()> {
    let manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_v2_resolver_l1'",
    ).fetch_one(&database.pool).await?;
    let resource = v2_permissions_current_resource_id();
    let mut events = Vec::new();
    for (index, subject) in [V2_PERMISSIONS_SUBJECT, V2_PERMISSIONS_OTHER_SUBJECT].into_iter().enumerate() {
        let mut event = collection_role_event(resource, subject,
            "0x0000000000000000000000000000000000000abc", block, index as i64, powers.clone(), manifest);
        event.block_hash = Some(format!("0xperms{block}"));
        if let Some(selector) = &selector {
            event.after_state["upstream_resource"] = selector["hash"].clone();
            event.after_state["resource"] = selector["hash"].clone();
            event.after_state["selector"] = selector.clone();
            event.after_state["grant_source"]["upstream_resource"] = selector["hash"].clone();
        }
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await
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

#[tokio::test]
async fn historical_controller_wrap_is_never_a_registration_handle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (wrapper, _) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::ControllerGranted).await?;
    sqlx::query("DELETE FROM bigname_phase.name_current WHERE raw_name = 'perms.eth'")
        .execute(&database.pool)
        .await?;
    let payload = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={wrapper}"),
    )
    .await?;
    assert_eq!(payload["data"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn historical_registry_control_is_never_a_registration_handle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let id = v2_permissions_current_resource_id();
    let (logical, node): (String, String) = sqlx::query_as(
        "SELECT logical_name_id, namehash FROM bigname_phase.name_surfaces WHERE raw_name = 'perms.eth'"
    ).fetch_one(&database.pool).await?;
    let mut grant = v2_history_event("perms-retained-lease", None,
        Some(v2_permissions_stale_resource_id()), "RegistrationGranted", 119);
    grant.source_family = "ens_v1_registrar_l1".to_owned();
    grant.after_state = json!({"namehash": node});
    let mut epoch = v2_history_event(
        "perms-historical-registry-epoch",
        Some(&logical),
        Some(id),
        "AuthorityEpochChanged",
        120,
    );
    epoch.source_family = "ens_v1_registrar_l1".to_owned();
    epoch.after_state = json!({"authority_kind": "registry_only"});
    seed_v2_history_blocks(&database, 119..=120).await?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, epoch]).await?;
    sqlx::query("DELETE FROM bigname_phase.name_current WHERE raw_name = 'perms.eth'")
        .execute(&database.pool)
        .await?;
    let payload = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={id}"),
    )
    .await?;
    assert_eq!(payload["data"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn resolver_roles_use_the_wrapped_registration_lease_handle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (wrapper, lease) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    let resolver = resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS);
    database
        .seed_snapshot_selector_chain_positions(&resolver.chain_positions)
        .await?;
    upsert_test_resolver_current_rows(&database, &[resolver]).await?;
    let mut permission = permission_current_row(
        wrapper,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: V2_RESOLVER_ADDRESS.to_owned(),
        },
        7,
        120,
    );
    permission.provenance["normalized_event_ids"] = json!([]);
    upsert_phase_permissions_current_rows(&database.pool, &[permission]).await?;
    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles"),
    )
    .await?;
    assert_eq!(payload["data"].as_array().unwrap().len(), 1, "{payload}");
    assert_eq!(
        payload["data"][0]["registration_id"],
        lease.to_string(),
        "{payload}"
    );
    let followed = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={lease}"),
    )
    .await?;
    assert!(
        followed["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["grant_scope"]["kind"] == "resolver"),
        "{followed}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn nameless_registry_epoch_rejects_raw_permission_handle_with_distinct_lease() -> Result<()> {
    assert_nameless_registry_permission_handle("AuthorityEpochChanged").await
}

#[tokio::test]
async fn nameless_registry_transfer_rejects_raw_permission_handle_with_distinct_lease() -> Result<()>
{
    assert_nameless_registry_permission_handle("AuthorityTransferred").await
}

async fn assert_nameless_registry_permission_handle(kind: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let registry = v2_permissions_stale_resource_id();
    let lease = v2_permissions_current_resource_id();
    let node = bigname_lookup::ens_namehash_hex("presurface-permissions.eth")?;
    let other_node = bigname_lookup::ens_namehash_hex("unrelated-presurface.eth")?;
    sqlx::query("DELETE FROM bigname_phase.name_current")
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.resources SET token_lineage_id = NULL WHERE resource_id = $1",
    )
    .bind(registry)
    .execute(&database.pool)
    .await?;
    seed_v2_history_blocks(&database, 118..=119).await?;
    let mut permission = permission_current_row(
        registry,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::Resource,
        1,
        119,
    );
    permission.chain_positions = json!({"block_number": 119, "block_hash": "0xhistory119"});
    permission.effective_powers = json!(["resource_control"]);
    permission.grant_source = json!({"kind": "ens_v1_authority", "authority_kind": "registry_only",
        "authority_key": format!("registry-only:ethereum-mainnet:{node}"),
        "source_event_kind": "AuthorityTransferred"});
    permission.transfer_behavior = json!("replace_on_authority_change");
    upsert_phase_permissions_current_rows(&database.pool, &[permission]).await?;
    let surface_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bigname_phase.name_surfaces WHERE namehash = $1")
            .bind(&node)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(
        surface_count, 0,
        "there is no materialized name for this node"
    );
    let route = format!("/v1/permissions?registration_id={registry}");
    let no_lease = v2_permissions_payload_for_database(&database, &route).await?;
    assert!(
        !no_lease["data"].as_array().unwrap().is_empty(),
        "{no_lease}"
    );

    // Numeric registration and registry authority producers retain the node and resource
    // before the label is known. The authority need not have a SurfaceBound observation.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142-L152 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
    let mut grant = v2_history_event(
        "presurface-lease",
        None,
        Some(lease),
        "RegistrationGranted",
        118,
    );
    grant.after_state["namehash"] = json!(node);
    let mut authority = v2_history_event("presurface-authority", None, Some(registry), kind, 119);
    authority.source_family = "ens_v1_registry_l1".into();
    authority.after_state = json!({"source_event": "Transfer", "node": node,
        "owner": V2_PERMISSIONS_SUBJECT, "owner_getter": V2_PERMISSIONS_SUBJECT,
        "authority_kind": "registry_only", "authority_key": format!("registry-only:ethereum-mainnet:{node}")});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, authority]).await?;
    let rejected = v2_permissions_payload_for_database(&database, &route).await?;
    assert_eq!(rejected["data"], json!([]), "{kind}: {rejected}");

    assert_nameless_permission_roundtrip(&database, registry, lease).await?;

    // NewOwner evidence carries both a parent node and a child node. Prefer the child;
    // legacy namehash evidence also takes precedence over the generic node field.
    for field in ["namehash", "child_node"] {
        sqlx::query("UPDATE bigname_phase.normalized_events SET after_state = jsonb_set(after_state, ARRAY[$1], $2) WHERE event_identity = 'presurface-authority'")
            .bind(field).bind(json!(other_node)).execute(&database.pool).await?;
        assert!(
            !bigname_storage::resource_is_registry_control_for_registrar_lease(
                &database.pool,
                registry
            )
            .await?,
            "{field} must take precedence"
        );
        assert_no_registry_permission_mapping(&database, registry, lease).await?;
        sqlx::query("UPDATE bigname_phase.normalized_events SET after_state = jsonb_set(after_state, ARRAY[$1], $2) WHERE event_identity = 'presurface-authority'")
            .bind(field).bind(json!(node.to_uppercase())).execute(&database.pool).await?;
        assert!(
            bigname_storage::resource_is_registry_control_for_registrar_lease(
                &database.pool,
                registry
            )
            .await?,
            "node case is not identity"
        );
        assert_nameless_permission_roundtrip(&database, registry, lease).await?;
        sqlx::query("UPDATE bigname_phase.normalized_events SET after_state = after_state - $1 WHERE event_identity = 'presurface-authority'")
            .bind(field).execute(&database.pool).await?;
    }
    // Each independent evidence failure must keep the ordinary resource audit available.
    // In particular, two absent logical names never prove that two nodes are the same.
    for (field, value) in [("namehash", json!(other_node)), ("namehash", Value::Null)] {
        sqlx::query("UPDATE bigname_phase.normalized_events SET after_state = jsonb_set(after_state, ARRAY[$1], $2) WHERE event_identity = 'presurface-lease'")
            .bind(field).bind(value).execute(&database.pool).await?;
        assert_no_registry_permission_mapping(&database, registry, lease).await?;
        let audit = v2_permissions_payload_for_database(&database, &route).await?;
        assert_eq!(
            audit["data"], no_lease["data"],
            "unrelated or unknown node: {audit}"
        );
    }
    sqlx::query("UPDATE bigname_phase.normalized_events SET after_state = jsonb_set(after_state, '{namehash}', $1) WHERE event_identity = 'presurface-lease'")
        .bind(json!(node)).execute(&database.pool).await?;
    for identity in ["presurface-lease", "presurface-authority"] {
        sqlx::query("UPDATE bigname_phase.normalized_events SET consumer_visibility = 'candidate', migration_correlation_ids = ARRAY['presurface-permission-test'] WHERE event_identity = $1")
            .bind(identity).execute(&database.pool).await?;
        assert!(
            !bigname_storage::resource_is_registry_control_for_registrar_lease(
                &database.pool,
                registry
            )
            .await?,
            "candidate {identity}"
        );
        assert_no_registry_permission_mapping(&database, registry, lease).await?;
        sqlx::query("UPDATE bigname_phase.normalized_events SET consumer_visibility = 'activated', canonicality_state = 'orphaned' WHERE event_identity = $1")
            .bind(identity).execute(&database.pool).await?;
        assert!(
            !bigname_storage::resource_is_registry_control_for_registrar_lease(
                &database.pool,
                registry
            )
            .await?,
            "noncanonical {identity}"
        );
        assert_no_registry_permission_mapping(&database, registry, lease).await?;
        sqlx::query("UPDATE bigname_phase.normalized_events SET canonicality_state = 'canonical' WHERE event_identity = $1")
            .bind(identity).execute(&database.pool).await?;
    }
    for block in [118_i64, 119] {
        sqlx::query("UPDATE bigname_phase.chain_lineage SET canonicality_state = 'orphaned' WHERE block_hash = $1 AND chain_id = 'ethereum-mainnet'")
            .bind(format!("0xhistory{block}")).execute(&database.pool).await?;
        assert!(
            !bigname_storage::resource_is_registry_control_for_registrar_lease(
                &database.pool,
                registry
            )
            .await?,
            "noncanonical lineage {block}"
        );
        assert_no_registry_permission_mapping(&database, registry, lease).await?;
        sqlx::query("UPDATE bigname_phase.chain_lineage SET canonicality_state = 'canonical' WHERE block_hash = $1 AND chain_id = 'ethereum-mainnet'")
            .bind(format!("0xhistory{block}")).execute(&database.pool).await?;
    }
    assert!(
        bigname_storage::resource_is_registry_control_for_registrar_lease(&database.pool, registry)
            .await?
    );
    assert_registry_permission_namespace_chains(&database, registry, lease).await?;
    assert_nameless_permission_publication_lifecycle(&database, registry, lease, &node).await?;
    database.cleanup().await
}

async fn assert_nameless_permission_roundtrip(
    database: &TestDatabase,
    registry: Uuid,
    lease: Uuid,
) -> Result<()> {
    let by_address = v2_permissions_payload_for_database(
        database, &format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}"),
    ).await?;
    let row = by_address["data"].as_array().unwrap().iter()
        .find(|row| row["powers"] == json!(["registration_control"]))
        .expect("retained registry grant");
    assert_eq!(row["registration_id"], lease.to_string(), "{by_address}");
    assert_eq!(row["authority_context"], "resource_audit");
    assert_eq!(row["grant_scope"]["kind"], "registration");
    if registry != lease {
        assert!(!by_address["data"].as_array().unwrap().iter()
            .any(|row| row["registration_id"] == registry.to_string()), "{by_address}");
    }
    for route in [format!("/v1/permissions?registration_id={lease}"),
        format!("/v1/permissions?registration_id={lease}&address={V2_PERMISSIONS_SUBJECT}")] {
        let selected = v2_permissions_payload_for_database(database, &route).await?;
        assert!(selected["data"].as_array().unwrap().contains(row), "{route}: {selected}");
    }
    let raw = v2_permissions_payload_for_database(database,
        &format!("/v1/permissions?registration_id={registry}")).await?;
    if registry != lease {
        assert_eq!(raw["data"], json!([]), "{raw}");
    } else {
        assert!(raw["data"].as_array().unwrap().contains(row), "{raw}");
    }
    Ok(())
}

async fn assert_no_registry_permission_mapping(
    database: &TestDatabase, registry: Uuid, lease: Uuid,
) -> Result<()> {
    let bounds = std::collections::BTreeMap::from([("ethereum-mainnet".to_owned(), 130)]);
    for (resources, requested) in [(vec![registry], None), (vec![], Some(lease))] {
        let mapping = bigname_storage::load_registry_permission_registration_map(
            &database.pool, &resources, requested, &bounds).await?;
        assert!(mapping.is_empty(), "unexpected registry mapping {mapping:?}");
    }
    Ok(())
}

async fn assert_nameless_permission_publication_lifecycle(
    database: &TestDatabase, registry: Uuid, lease: Uuid, node: &str,
) -> Result<()> {
    // Boundary-derived evidence can lack log positions. Match the existing history order
    // (-1 for absent positions), including a release at the same boundary as its grant.
    let mut boundary_release = v2_history_event("permission-boundary-release", None, Some(lease), "RegistrationReleased", 118);
    boundary_release.source_family = "ens_v2_migration_l1".into();
    boundary_release.log_index = None;
    boundary_release.transaction_hash = None;
    sqlx::query("UPDATE bigname_phase.normalized_events SET transaction_index=NULL, log_index=NULL WHERE event_identity='presurface-lease'")
        .execute(&database.pool).await?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[boundary_release]).await?;
    assert_nameless_permission_roundtrip(database, registry, registry).await?;
    sqlx::query("DELETE FROM bigname_phase.normalized_events WHERE event_identity='permission-boundary-release'")
        .execute(&database.pool).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET transaction_index=0, log_index=0 WHERE event_identity='presurface-lease'")
        .execute(&database.pool).await?;

    // Interpret can be ahead of Project. Seed retained future facts without advancing the
    // published head, then explicitly publish each point of this registry/lease lifecycle.
    let blocks = (140..=160).map(|number| raw_block("ethereum-mainnet",
        &format!("0xhistory{number}"), None, number, 1_700_000_000 + number)).collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    let publication_hash: String = sqlx::query_scalar("SELECT current_block_hash FROM chain_phase_state WHERE chain_id='ethereum-mainnet' AND phase_name='project'")
        .fetch_one(&database.pool).await?;

    sqlx::query("UPDATE bigname_phase.normalized_events SET block_number=140, block_hash='0xhistory140' WHERE event_identity='presurface-lease'")
        .execute(&database.pool).await?;
    assert_nameless_permission_roundtrip(database, registry, registry).await?;
    assert_no_registry_permission_mapping(database, registry, lease).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET block_number=118, block_hash='0xhistory118' WHERE event_identity='presurface-lease'")
        .execute(&database.pool).await?;

    let successor = Uuid::from_u128(0xe300);
    upsert_test_resources(&database.pool, &[resource(successor)]).await?;
    let mut release = v2_history_event("presurface-release", None, Some(lease), "RegistrationReleased", 140);
    release.source_family = "ens_v2_migration_l1".into();
    release.log_index = None;
    release.transaction_hash = None;
    release.after_state["namehash"] = json!(node);
    let mut grant = v2_history_event("presurface-successor", None, Some(successor), "RegistrationGranted", 150);
    grant.after_state["namehash"] = json!(node);
    let mut successor_release = v2_history_event("presurface-successor-release", None, Some(successor), "RegistrationReleased", 155);
    successor_release.source_family = "ens_v2_migration_l1".into();
    successor_release.log_index = None;
    successor_release.transaction_hash = None;
    successor_release.after_state["namehash"] = json!(node);
    let mut handoff = v2_history_event("presurface-handoff", None, Some(successor), "AuthorityEpochChanged", 160);
    handoff.after_state["namehash"] = json!(node);
    handoff.after_state["authority_kind"] = json!("registrar");
    handoff.after_state["authority_key"] = json!("registrar:successor");
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[release, grant, successor_release, handoff]).await?;
    assert_nameless_permission_roundtrip(database, registry, lease).await?;

    for (block, handle) in [(140, registry), (150, successor), (155, registry)] {
        seed_schema_v2_ens_lookup_head(&database.pool, block, &format!("0xhistory{block}"), "2026-06-10T00:00:00Z").await?;
        assert_nameless_permission_roundtrip(database, registry, handle).await?;
        let old_lease = v2_permissions_payload_for_database(database,
            &format!("/v1/permissions?registration_id={lease}&address={V2_PERMISSIONS_SUBJECT}")).await?;
        assert!(!old_lease["data"].as_array().unwrap().iter()
            .any(|row| row["powers"] == json!(["registration_control"])), "old lease selected registry grant: {old_lease}");
    }
    // A released latest grant must not revive an older grant even if its release is absent.
    sqlx::query("DELETE FROM bigname_phase.normalized_events WHERE event_identity='presurface-release'")
        .execute(&database.pool).await?;
    assert_nameless_permission_roundtrip(database, registry, registry).await?;
    // Now remove successor release, proving the *different-resource* handoff itself closes R.
    sqlx::query("DELETE FROM bigname_phase.normalized_events WHERE event_identity='presurface-successor-release'")
        .execute(&database.pool).await?;
    seed_schema_v2_ens_lookup_head(&database.pool, 160, "0xhistory160", "2026-06-10T00:00:00Z").await?;
    assert_nameless_permission_roundtrip(database, registry, registry).await?;
    let bounds = std::collections::BTreeMap::from([("ethereum-mainnet".to_owned(), 160)]);
    for (resources, requested) in [(vec![registry], None), (vec![], Some(successor))] {
        assert!(bigname_storage::load_registry_permission_registration_map(
            &database.pool, &resources, requested, &bounds).await?.is_empty());
    }
    assert!(bigname_storage::load_registry_permission_registration_map(
        &database.pool, &[registry], None, &std::collections::BTreeMap::new()).await?.is_empty());
    seed_schema_v2_ens_lookup_head(&database.pool, 130, &publication_hash, "2026-06-10T00:00:00Z").await?;
    Ok(())
}

async fn assert_registry_permission_namespace_chains(
    database: &TestDatabase, registry: Uuid, lease: Uuid,
) -> Result<()> {
    sqlx::query("UPDATE bigname_phase.normalized_events SET namespace='basenames' WHERE event_identity='presurface-authority'")
        .execute(&database.pool).await?;
    assert_no_registry_permission_mapping(database, registry, lease).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET namespace='ens' WHERE event_identity='presurface-authority'")
        .execute(&database.pool).await?;
    let blocks = (118..=119).map(|n| raw_block("base-mainnet", &format!("0xhistory{n}"), None, n, 1_700_000_000+n)).collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    let base_registry = Uuid::from_u128(0xf001);
    let base_lease = Uuid::from_u128(0xf002);
    let resources = [base_registry, base_lease].map(|id| {
        let mut row = resource(id);
        row.chain_id = "base-mainnet".into();
        row.block_number = 118;
        row.block_hash = "0xhistory118".into();
        row
    });
    upsert_test_resources(&database.pool, &resources).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET chain_id='base-mainnet', resource_id=$1 WHERE event_identity='presurface-authority'")
        .bind(base_registry).execute(&database.pool).await?;
    let bounds = std::collections::BTreeMap::from([("ethereum-mainnet".to_owned(),130),("base-mainnet".to_owned(),130)]);
    assert!(bigname_storage::load_registry_permission_registration_map(&database.pool, &[base_registry], None, &bounds).await?.is_empty());
    assert!(bigname_storage::load_registry_permission_registration_map(&database.pool, &[], Some(lease), &bounds).await?.is_empty());
    sqlx::query("UPDATE bigname_phase.normalized_events SET chain_id='base-mainnet', namespace='basenames', resource_id=CASE WHEN event_identity='presurface-authority' THEN $1 ELSE $2 END, source_family=CASE WHEN event_identity='presurface-authority' THEN 'basenames_base_registry' ELSE 'basenames_base_registrar' END WHERE event_identity IN ('presurface-authority','presurface-lease')")
        .bind(base_registry).bind(base_lease).execute(&database.pool).await?;
    let expected = std::collections::BTreeMap::from([(base_registry,base_lease)]);
    assert_eq!(bigname_storage::load_registry_permission_registration_map(&database.pool, &[base_registry], None, &bounds).await?, expected);
    assert_eq!(bigname_storage::load_registry_permission_registration_map(&database.pool, &[], Some(base_lease), &bounds).await?, expected);
    sqlx::query("UPDATE bigname_phase.normalized_events SET chain_id='ethereum-mainnet', namespace='ens', resource_id=CASE WHEN event_identity='presurface-authority' THEN $1 ELSE $2 END, source_family=CASE WHEN event_identity='presurface-authority' THEN 'ens_v1_registry_l1' ELSE 'ens_v1_registrar_l1' END WHERE event_identity IN ('presurface-authority','presurface-lease')")
        .bind(registry).bind(lease).execute(&database.pool).await?;
    Ok(())
}

#[path = "v2_resolver_registry_roles.rs"]
mod resolver_registry_roles;

#[path = "v2_registry_token_handoff.rs"]
mod registry_token_handoff;

#[path = "v2_registry_released_audit.rs"]
mod registry_released_audit;
