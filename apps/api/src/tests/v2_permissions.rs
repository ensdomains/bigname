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
    let paired = v2_permissions_payload_for_database(
        &database, &format!("/v1/permissions?name=perms.eth&registration_id={stale_resource_id}"),
    ).await?;
    assert_eq!(paired["data"], json!([]));
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
    let (wrapper_resource_id, lease_resource_id) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    let other = Uuid::from_u128(0xe400);
    let other_name = seed_family_identity_inputs(&database.pool, "ens", "beta.eth", "ethereum-mainnet",
        100, "0xperms100", other, Uuid::from_u128(0xe401), Uuid::from_u128(0xe402), "ens_v1").await?;
    let grant = permission_fixture_event("permissions-beta-grant", Some(&other_name), Some(other),
        "RegistrationGranted", "ens_v1_registrar_l1", 101, 3,
        json!({"authority_kind":"registrar", "registrant":V2_PERMISSIONS_OTHER_SUBJECT, "expiry":1_900_000_000_i64}));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;

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
    let (wrapper_resource_id, lease_resource_id) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;

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
        "/v1/permissions?name=perms.eth&page_size=1",
    )
    .await?;
    assert_eq!(first["data"].as_array().expect("first name page").len(), 1);
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("the name has more than one permission row")
        .to_owned();
    let continued = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?name=perms.eth&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(continued["data"].as_array().expect("second name page").len(), 1);
    assert_ne!(continued["data"][0], first["data"][0]);
    let with_lease = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?name=perms.eth&registration_id={lease_resource_id}&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(with_lease["data"], continued["data"], "{with_lease}");
    let response = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v1/permissions?name=perms.eth&registration_id={wrapper_resource_id}&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"), "{payload}");

    database.cleanup().await
}

// A closed wrapper binding remains outside the public registration handle space. The
// recorded wrap link must reject it after the registrar binding becomes current again.
#[tokio::test]
async fn v2_get_permissions_rejects_a_historical_name_wrapper_resource() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (wrapper_resource_id, lease_resource_id) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    unwrap_permission_fixture(&database, wrapper_resource_id, lease_resource_id).await?;

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

// Exercise a retained lease release while a distinct registry binding remains open.
#[tokio::test]
async fn v2_get_permissions_empties_a_lapsed_handed_off_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (released_resource_id, _) = seed_handed_off_lease_inputs(&database, "perms.eth", true).await?;
    let name = v2_permissions_payload_for_database(&database, "/v1/names/perms.eth").await?;
    assert_eq!(name["data"]["registration_status"], "released", "{name}");

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
    let released_resource_id = seed_registry_permission_inputs(&database).await?;
    close_registry_permission_name(&database, released_resource_id, false).await?;

    let by_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=one.alpha.eth").await?;
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

// A released registration can be reserved again while its resource grants remain available
// for audit. The name cannot select those retained grants as current authority.
#[tokio::test]
async fn v2_get_permissions_keeps_retained_resource_audit_out_of_reserved_name_scope() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let reserved_resource_id = seed_registry_permission_inputs(&database).await?;
    close_registry_permission_name(&database, reserved_resource_id, true).await?;

    let by_name =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=one.alpha.eth").await?;
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

// A serving resource does not establish registration authority for a name. A root pointer
// can serve records while root grants remain resource-audit permissions only.
#[tokio::test]
async fn v2_get_permissions_empties_a_name_filter_without_registration_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_unbound_name_inputs(&database, "eth", true).await?;
    let resource = Uuid::from_u128(0x2200);
    let mut grant = permission_fixture_event("permissions-unbound-root-grant", None, Some(resource),
        "RootPermissionChanged", "ens_v2_root_l1", 21_000_003, 10,
        json!({"subject":V2_PERMISSIONS_SUBJECT,
            "scope":{"kind":"registry_root","chain_id":"ethereum-mainnet","registry_address":"0x000000000000000000000000000000000000f001"},
            "effective_powers":["renew"],"grant_source":{"kind":"raw_log","source_event":"EACRolesChanged","root_resource":true},
            "revocation_source":null,"inheritance_path":[],"transfer_behavior":{}}));
    grant.block_hash = Some("0xbinding".into());
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let audit = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registration_id={resource}")).await?;
    assert!(!audit["data"].as_array().unwrap().is_empty(), "{audit}");
    let by_name = v2_permissions_payload_for_database(&database, "/v1/permissions?name=eth").await?;
    assert_eq!(by_name["data"], json!([]));
    assert_eq!(by_name["meta"]["completeness"], "partial");
    assert_eq!(by_name["meta"]["unsupported_reason"], "permission_support_unknown");
    database.cleanup().await
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
    let resource_id = seed_base_permission_inputs(&database, true).await?;
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
    let resource_id = seed_registry_permission_inputs(&database).await?;

    for selector in [format!("registration_id={resource_id}"), "name=one.alpha.eth".to_owned()] {
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
    let (wrapper, resource_id) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    insert_permission_wrapper_state(&database, wrapper, "locked", 196_609, 1_800_000_000, 124).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;

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

/// Publish a distinct wrapper resource and registrar lease, with either a recorded lease link
/// or the controller's later grant. The current binding points at the wrapper.
async fn seed_perms_wrapped_lease(
    database: &TestDatabase,
    shape: WrappedLeaseShape,
) -> Result<(Uuid, Uuid)> {
    seed_v2_permissions_fixture(database).await?;
    let lease = v2_permissions_current_resource_id();
    let wrapper = Uuid::from_u128(0xe300);
    sqlx::query("UPDATE surface_bindings SET active_to = (SELECT block_timestamp FROM chain_lineage
        WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xperms120') WHERE surface_binding_id = $1")
        .bind(Uuid::from_u128(0xe103)).execute(&database.pool).await?;
    let logical = seed_family_identity_inputs(&database.pool, "ens", "perms.eth", "ethereum-mainnet",
        120, "0xperms120", wrapper, Uuid::from_u128(0xe301), Uuid::from_u128(0xe302), "ens_v1").await?;
    let node = bigname_lookup::ens_namehash_hex("perms.eth")?;
    let (grant_block, grant_name, link) = match shape {
        WrappedLeaseShape::LinkRecorded => (119, None, json!(lease)),
        WrappedLeaseShape::ControllerGranted => (120, Some(logical.as_str()), Value::Null),
    };
    let mut events = vec![
        permission_fixture_event("permissions-wrapper-lease-grant", grant_name, Some(lease),
            "RegistrationGranted", "ens_v1_registrar_l1", grant_block, 2,
            json!({"authority_kind":"registrar", "namehash":node,
                "registrant":V2_PERMISSIONS_SUBJECT, "expiry":1_800_000_000_i64})),
        permission_fixture_event("permissions-wrapper-binding", Some(&logical), Some(wrapper),
            "SurfaceBound", "ens_v1_wrapper_l1", 120, 0,
            json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper",
                "wrapped_registrar_resource_id":link})),
        permission_fixture_event("permissions-wrapper-epoch", Some(&logical), Some(wrapper),
            "AuthorityEpochChanged", "ens_v1_wrapper_l1", 120, 0,
            json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper",
                "owner":V2_PERMISSIONS_SUBJECT})),
        permission_fixture_event("permissions-wrapper-owner", Some(&logical), Some(wrapper),
            "TokenControlTransferred", "ens_v1_wrapper_l1", 120, 0,
            json!({"source_event":"NameWrapped", "node":node, "owner":V2_PERMISSIONS_SUBJECT,
                "to_address":V2_PERMISSIONS_SUBJECT})),
        permission_fixture_event("permissions-wrapper-grant", Some(&logical), Some(wrapper),
            "PermissionChanged", "ens_v1_wrapper_l1", 120, 0,
            json!({"subject":V2_PERMISSIONS_SUBJECT, "scope":{"kind":"resource"},
                "effective_powers":["resource_control"], "grant_source":{"kind":"raw_log",
                    "source_event":"NameWrapped", "authority_kind":"wrapper", "relation_kind":"holder"},
                "revocation_source":null, "inheritance_path":[], "transfer_behavior":{}})),
    ];
    let manifest: i64 = sqlx::query_scalar("SELECT manifest_id FROM manifest_versions
        WHERE source_family = 'ens_v2_resolver_l1'").fetch_one(&database.pool).await?;
    for (log, subject, powers) in [(0, V2_PERMISSIONS_SUBJECT, json!(["set_text"])),
        (1, V2_PERMISSIONS_OTHER_SUBJECT, json!(["set_addr"]))] {
        let mut role = collection_role_event(wrapper, subject,
            "0x0000000000000000000000000000000000000abc", 122, log, powers, manifest);
        role.block_hash = Some("0xperms122".into());
        events.push(role);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    insert_permission_wrapper_state(database, wrapper, "emancipated", 196_608, 1_807_776_000, 123).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    Ok((wrapper, lease))
}

async fn unwrap_permission_fixture(database: &TestDatabase, wrapper: Uuid, lease: Uuid) -> Result<()> {
    let logical = bigname_storage::logical_name_id_for_name("ens", "perms.eth");
    let node = bigname_lookup::ens_namehash_hex("perms.eth")?;
    sqlx::query("UPDATE surface_bindings SET active_to = (SELECT block_timestamp FROM chain_lineage
        WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xperms125')
        WHERE logical_name_id = $1 AND active_to IS NULL")
        .bind(&logical).execute(&database.pool).await?;
    seed_family_identity_inputs(&database.pool, "ens", "perms.eth", "ethereum-mainnet", 125,
        "0xperms125", lease, Uuid::from_u128(0xe102), Uuid::from_u128(0xe104), "ens_v1").await?;
    let after = json!({"source_event":"NameUnwrapped", "node":node, "owner":V2_PERMISSIONS_SUBJECT,
        "reactivated_resource_id":lease, "reactivated_token_lineage_id":Uuid::from_u128(0xe102)});
    let events = [
        permission_fixture_event("permissions-unwrapped", Some(&logical), Some(wrapper),
            "SurfaceUnbound", "ens_v1_wrapper_l1", 125, 0, after.clone()),
        permission_fixture_event("permissions-reactivated-lease", Some(&logical), Some(lease),
            "SurfaceBound", "ens_v1_wrapper_l1", 125, 0, after.clone()),
        permission_fixture_event("permissions-unwrapped-epoch", Some(&logical), Some(lease),
            "AuthorityEpochChanged", "ens_v1_wrapper_l1", 125, 0, after),
        permission_fixture_event("permissions-unwrapped-holder-revoked", Some(&logical), Some(wrapper),
            "PermissionChanged", "ens_v1_wrapper_l1", 125, 0,
            json!({"subject":V2_PERMISSIONS_SUBJECT,"scope":{"kind":"resource"},"effective_powers":[],
                "grant_source":null,"revocation_source":{"kind":"raw_log", "source_event":"NameUnwrapped",
                    "relation_kind":"holder","authority_kind":"wrapper"}, "inheritance_path":[], "transfer_behavior":{}})),
    ];
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await
}

async fn insert_permission_wrapper_state(
    database: &TestDatabase,
    resource: Uuid,
    state: &str,
    fuses: i64,
    expiry: i64,
    block: i64,
) -> Result<()> {
    let events = [
        permission_fixture_event(&format!("permissions-wrapper-scope-{resource}-{block}"), None, Some(resource),
            "PermissionScopeChanged", "ens_v1_wrapper_l1", block, 0,
            json!({"source_event":"NameWrapped", "wrapper_state":state, "fuses":fuses})),
        permission_fixture_event(&format!("permissions-wrapper-expiry-{resource}-{block}"), None, Some(resource),
            "ExpiryChanged", "ens_v1_wrapper_l1", block, 0,
            json!({"source_event":"NameWrapped", "expiry":expiry})),
    ];
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(())
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
    seed_v2_permissions_fixture_named(&database, "parent.eth").await?;
    let wrapper_resource_id = Uuid::from_u128(0xe400);
    let logical = seed_family_identity_inputs(&database.pool, "ens", "child.parent.eth", "ethereum-mainnet",
        120, "0xperms120", wrapper_resource_id, Uuid::from_u128(0xe401), Uuid::from_u128(0xe402), "ens_v1").await?;
    let node = bigname_lookup::ens_namehash_hex("child.parent.eth")?;
    let mut events = vec![
        permission_fixture_event("permissions-subname-binding", Some(&logical), Some(wrapper_resource_id),
            "SurfaceBound", "ens_v1_wrapper_l1", 120, 0,
            json!({"source_event":"NameWrapped", "node":node,"authority_kind":"wrapper", "wrapped_registrar_resource_id":null})),
        permission_fixture_event("permissions-subname-epoch", Some(&logical), Some(wrapper_resource_id),
            "AuthorityEpochChanged", "ens_v1_wrapper_l1", 120, 0,
            json!({"source_event":"NameWrapped", "node":node,"authority_kind":"wrapper", "owner":V2_PERMISSIONS_SUBJECT})),
        permission_fixture_event("permissions-subname-owner", Some(&logical), Some(wrapper_resource_id),
            "TokenControlTransferred", "ens_v1_wrapper_l1", 120, 0,
            json!({"source_event":"NameWrapped", "node":node,"owner":V2_PERMISSIONS_SUBJECT,"to_address":V2_PERMISSIONS_SUBJECT})),
        permission_fixture_event("permissions-subname-holder", Some(&logical), Some(wrapper_resource_id),
            "PermissionChanged", "ens_v1_wrapper_l1", 120, 0,
            json!({"subject":V2_PERMISSIONS_SUBJECT,"scope":{"kind":"resource"}, "effective_powers":["resource_control"],
                "grant_source":{"kind":"raw_log","source_event":"NameWrapped", "authority_kind":"wrapper","relation_kind":"holder"},
                "revocation_source":null,"inheritance_path":[],"transfer_behavior":{}})),
    ];
    let manifest: i64 = sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_v2_resolver_l1'")
        .fetch_one(&database.pool).await?;
    for (log, subject, powers) in [(0, V2_PERMISSIONS_SUBJECT, json!(["set_text"])),
        (1, V2_PERMISSIONS_OTHER_SUBJECT, json!(["set_addr"]))] {
        let mut role = collection_role_event(wrapper_resource_id, subject,
            "0x0000000000000000000000000000000000000abc", 122, log, powers, manifest);
        role.block_hash = Some("0xperms122".into());
        events.push(role);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    insert_permission_wrapper_state(&database, wrapper_resource_id, "wrapped", 0, 1_800_000_000, 123).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;

    let name = v2_name_record_payload_for_database(&database, "/v1/names/child.parent.eth").await?;
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
    let name = bigname_storage::logical_name_id_for_name("ens", V2_SEPOLIA_SNAPSHOT_NAME);
    let mut grant = permission_fixture_event("permissions-sepolia-grant", Some(&name), Some(resource_id),
        "PermissionChanged", "ens_v1_registrar_l1", V2_SEPOLIA_SNAPSHOT_BLOCK, 4,
        json!({"subject":V2_PERMISSIONS_SUBJECT,"scope":{"kind":"resource"},"effective_powers":["resource_control"],
            "grant_source":{"kind":"ens_v1_authority","authority_kind":"registrar"},
            "revocation_source":null,"inheritance_path":[],"transfer_behavior":"replace_on_authority_change"}));
    grant.chain_id = Some("ethereum-sepolia".into());
    grant.block_hash = Some(V2_SEPOLIA_SNAPSHOT_HASH.into());
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-sepolia", V2_SEPOLIA_SNAPSHOT_BLOCK, V2_SEPOLIA_SNAPSHOT_HASH).await?;

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
async fn v2_permissions_empty_resource_support_comes_from_retained_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = Uuid::from_u128(0xe400);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
        VALUES ($1, 'ethereum-mainnet', 100, '0xperms100', 'canonical')")
        .bind(resource_id).execute(&database.pool).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    let uri = format!("/v1/permissions?registration_id={resource_id}");
    let missing = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(missing["data"], json!([]));
    assert_eq!(missing["meta"]["completeness"], json!("partial"));
    assert_eq!(missing["meta"]["unsupported_reason"], json!("permission_support_unknown"));

    let grant = permission_fixture_event("permissions-empty-registrar", None, Some(resource_id),
        "RegistrationGranted", "ens_v1_registrar_l1", 120, 0,
        json!({"authority_kind":"registrar", "registrant":V2_PERMISSIONS_SUBJECT, "expiry":1_900_000_000_i64}));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    let partial = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(partial["data"], json!([]));
    assert_unlisted_permission_surfaces(&partial, V2_UNWRAPPED_UNLISTED_SURFACES);
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_resource_bound_read_serves_wrapper_restrictions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (wrapper, resource_id) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    insert_permission_wrapper_state(&database, wrapper, "locked", 196_609, 1_800_000_000, 124).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;

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
    let resource_id = seed_registry_permission_inputs(&database).await?;

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

    insert_registry_permission_roles(&database, resource_id, false, 75, json!([])).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 83, "0xregistry83").await?;
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
    let resource = Uuid::from_u128(0xe400);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
        VALUES ($1, 'ethereum-mainnet', 100, '0xperms100', 'canonical')")
        .bind(resource).execute(&database.pool).await?;
    let manifest: i64 = sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_v2_resolver_l1'")
        .fetch_one(&database.pool).await?;
    let mut grant = collection_role_event(resource, V2_PERMISSIONS_SUBJECT,
        "0x0000000000000000000000000000000000000abc", 120, 0, json!(["set_text"]), manifest);
    grant.block_hash = Some("0xperms120".into());
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    let payload = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=10")).await?;
    assert_eq!(payload["data"].as_array().unwrap().len(), 4);
    assert!(payload["data"].as_array().unwrap().iter().any(|row| row["registration_id"] == resource.to_string()));
    assert_eq!(payload["meta"]["completeness"], "partial");
    assert_eq!(payload["meta"]["unsupported_reason"], "permission_support_unknown");
    database.cleanup().await
}

#[tokio::test]
async fn v2_permissions_refuse_an_incomplete_family_rebuild()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let resource_id = v2_permissions_current_resource_id();
    let uri = format!("/v1/permissions?registration_id={resource_id}");

    let readable = v2_permissions_payload_for_database(&database, &uri).await?;
    assert!(!readable["data"].as_array().is_none_or(Vec::is_empty));
    assert_eq!(readable["meta"]["completeness"], json!("partial"));
    assert_unlisted_permission_surfaces(&readable, V2_UNWRAPPED_UNLISTED_SURFACES);

    let chain = "ethereum-mainnet";
    let token = bigname_project::families::input_token(&database.pool, chain).await?;
    let outcome = bigname_project::families::apply(&database.pool, chain,
        &bigname_project::Marker { number:130, hash:"0xperms130".into() },
        bigname_project::families::FamilyMode::Rebuild, &token,
        &bigname_project::families::FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .with_max_blocks_per_run(1)).await?;
    assert!(outcome.reset);
    assert_ne!(outcome.marker.as_ref().map(|marker| marker.number), Some(130));
    let response = v2_permissions_response_for_database(&database, &uri).await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(read_json::<Value>(response).await?["error"]["code"], "stale");
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

/// A separate Base publication with a registrar grant; the named variant also observes its
/// registry owner and an account-wide operator approval.
async fn seed_base_permission_inputs(database: &TestDatabase, with_operator: bool) -> Result<Uuid> {
    let chain = "base-mainnet";
    let resource = Uuid::from_u128(0xf100);
    let at = parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?.unix_timestamp();
    let blocks = (100..=130).map(|block| raw_block(chain, &format!("0xperms-base-{block}"),
        None, block, at - 130 + block)).collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    database.seed_snapshot_selector_chain_positions(&json!({"base":{"chain_id":chain,
        "block_number":130,"block_hash":"0xperms-base-130","timestamp":"2026-06-10T00:00:00Z"}})).await?;
    let name = if with_operator {
        Some(seed_family_identity_inputs(&database.pool, "basenames", "base-perms.base.eth", chain,
            100, "0xperms-base-100", resource, Uuid::from_u128(0xf101), Uuid::from_u128(0xf102), "basenames").await?)
    } else {
        sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
            VALUES ($1, $2, 100, '0xperms-base-100', 'canonical')")
            .bind(resource).bind(chain).execute(&database.pool).await?;
        None
    };
    let mut events = vec![
        permission_fixture_event("permissions-base-registration", name.as_deref(), Some(resource),
            "RegistrationGranted", "basenames_base_registrar", 101, 0,
            json!({"authority_kind":"registrar","registrant":V2_PERMISSIONS_SUBJECT,"expiry":1_900_000_000_i64})),
        permission_fixture_event("permissions-base-authority-grant", name.as_deref(), Some(resource),
            "PermissionChanged", "basenames_base_registrar", 105, 0,
            json!({"subject":V2_PERMISSIONS_SUBJECT,"scope":{"kind":"resource"},"effective_powers":["resource_control"],
                "grant_source":{"kind":"ens_v1_authority","authority_kind":"registrar"},
                "revocation_source":null,"inheritance_path":[],"transfer_behavior":"replace_on_authority_change"})),
    ];
    if with_operator {
        events.push(permission_fixture_event("permissions-base-owner", name.as_deref(), Some(resource),
            "AuthorityTransferred", "basenames_base_registry", 120, 0,
            json!({"source_event":"Transfer","node":bigname_lookup::ens_namehash_hex("base-perms.base.eth")?,
                "owner":V2_OPERATOR_OWNER,"owner_getter":V2_OPERATOR_OWNER,"registry_contract":V2_OPERATOR_REGISTRY})));
        events.push(permission_fixture_event("permissions-base-operator", None, None,
            "AccountPermissionChanged", "basenames_base_registry", 121, 0,
            json!({"subject":V2_PERMISSIONS_SUBJECT,"relation_kind":"operator","approved":true,
                "scope":{"kind":"account","chain_id":chain,"authority_kind":"registry",
                    "authority_contract":V2_OPERATOR_REGISTRY,"owner":V2_OPERATOR_OWNER},
                "effective_powers":["registry_control"],"grant_source":{"kind":"raw_log","source_event":"ApprovalForAll"},
                "revocation_source":null,"inheritance_path":[],
                "transfer_behavior":{"mode":"owner_scoped","on_registry_owner_change":"ceases_to_apply"}})));
    }
    for event in &mut events {
        event.namespace = "basenames".into();
        event.chain_id = Some(chain.into());
        event.block_hash = event.block_number.map(|block| format!("0xperms-base-{block}"));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, chain, 130, "0xperms-base-130").await?;
    Ok(resource)
}

async fn seed_registry_permission_inputs(database: &TestDatabase) -> Result<Uuid> {
    seed_registry_fixture(database).await?;
    let registration = Uuid::from_u128(0xA100);
    let root = Uuid::from_u128(0xA300);
    let provenance = json!({"source_family":"ens_v2_registry_l1",
        "registry_contract_instance_id":Uuid::from_u128(0xA190),
        "upstream_resource":format!("0x{:064x}", 0)});
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, provenance, canonicality_state)
        VALUES ($1, 'ethereum-mainnet', 59, '0xregistry59', $2, 'canonical')")
        .bind(root).bind(&provenance).execute(&database.pool).await?;
    sqlx::query("UPDATE resources SET provenance = provenance || $2 WHERE resource_id = $1")
        .bind(registration).bind(json!({"source_family":"ens_v2_registry_l1",
            "registry_contract_instance_id":Uuid::from_u128(0xA190),
            "upstream_resource":format!("0x{:064x}", 0x100000001_u64)})).execute(&database.pool).await?;
    insert_registry_permission_roles(database, registration, false, 70, json!(["renew"])).await?;
    insert_registry_permission_roles(database, root, true, 70,
        json!(["admin_unregister", "admin_set_subregistry", "admin_set_resolver"])).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 83, "0xregistry83").await?;
    Ok(registration)
}

async fn close_registry_permission_name(database: &TestDatabase, resource: Uuid, reserved: bool) -> Result<()> {
    let name = registry_logical_name_id("one.alpha.eth");
    let mut release = registry_event("permission-registry-release", Some(&name),
        "RegistrationReleased", 75, ALPHA_REGISTRY,
        json!({"source_event":"LabelUnregistered", "sender":V2_PERMISSIONS_SUBJECT,
            "registry_contract_instance_id":Uuid::from_u128(0xA190), "authority_kind":"ens_v2_registry"}));
    release.resource_id = Some(resource);
    let mut transfer = registry_event("permission-registry-burn", Some(&name),
        "TokenControlTransferred", 75, ALPHA_REGISTRY,
        json!({"source_event":"Transfer", "to":"0x0000000000000000000000000000000000000000"}));
    transfer.resource_id = Some(resource);
    transfer.log_index = Some(1);
    let mut events = vec![release, transfer];
    if reserved {
        let mut reservation = registry_event("permission-registry-reserved", Some(&name),
            "RegistrationReserved", 76, ALPHA_REGISTRY,
            json!({"source_event":"LabelReserved", "status":"reserved", "expiry":1_900_000_000_i64,
                "reservation_resource":true, "registry_contract_instance_id":Uuid::from_u128(0xA190),
                "authority_kind":"ens_v2_registry"}));
        reservation.resource_id = Some(resource);
        events.push(reservation);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query("UPDATE surface_bindings SET active_to = (SELECT block_timestamp FROM chain_lineage
        WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xregistry75') WHERE resource_id = $1")
        .bind(resource).execute(&database.pool).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 83, "0xregistry83").await
}

async fn insert_registry_permission_roles(
    database: &TestDatabase, resource: Uuid, root: bool, block: i64, powers: Value,
) -> Result<()> {
    let upstream = format!("0x{:064x}", if root { 0 } else { 0x100000001_u64 });
    let source = json!({"kind":"raw_log", "source_event":"EACRolesChanged", "upstream_resource":upstream,
        "registry_contract_instance_id":Uuid::from_u128(0xA190), "root_resource":root, "changed_powers":powers});
    let revoked = powers.as_array().is_some_and(Vec::is_empty);
    let mut event = registry_event(&format!("permission-registry-role-{resource}-{block}"), None,
        if root { "RootPermissionChanged" } else { "PermissionChanged" }, block, ALPHA_REGISTRY,
        json!({"subject":V2_PERMISSIONS_SUBJECT,"scope":{"kind":if root { "registry_root" } else { "registry" },
            "chain_id":"ethereum-mainnet","registry_address":ALPHA_REGISTRY}, "effective_powers":powers,
            "source_event":"EACRolesChanged", "upstream_resource":upstream, "resource":upstream,
            "registry_contract_instance_id":Uuid::from_u128(0xA190), "root_resource":root,
            "grant_source":if revoked { json!({}) } else { source.clone() },
            "revocation_source":if revoked { source } else { Value::Null },
            "inheritance_path":[], "transfer_behavior":{}}));
    event.resource_id = Some(resource);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(())
}

async fn seed_v2_registry_operator_fixture() -> Result<TestDatabase> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    seed_registry_operator(&database, v2_permissions_current_resource_id()).await?;
    Ok(database)
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
    let status = response.status();
    let body: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(body)
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
    seed_permission_lease_inputs(database, name, false).await
}

async fn seed_permission_lease_inputs(database: &TestDatabase, name: &str, lapsed: bool) -> Result<()> {
    let chain = "ethereum-mainnet";
    let target = parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .unix_timestamp();
    let expiry = if lapsed { target - 5 - 90 * 24 * 60 * 60 - 1 } else { 1_900_000_000 };
    let blocks = (99..=130)
        .map(|block| {
            raw_block(
                chain,
                &format!("0xperms{block}"),
                None,
                block,
                if lapsed && block < 125 { expiry - 125 + block } else { target - 130 + block },
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
            json!({"authority_kind":"registrar", "registrant":V2_PERMISSIONS_SUBJECT, "expiry":expiry}),
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

/// Retained inputs for a lease whose token holder changes while registry control stays put.
/// The optional boundary release uses the same event shape as schema_v2::settle_block_boundary.
async fn seed_handed_off_lease_inputs(database: &TestDatabase, name: &str, lapsed: bool) -> Result<(Uuid, Uuid)> {
    seed_permission_lease_inputs(database, name, lapsed).await?;
    let lease = v2_permissions_current_resource_id();
    let registry = Uuid::from_u128(0xe400);
    let logical = bigname_storage::logical_name_id_for_name("ens", name);
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let at: OffsetDateTime = sqlx::query_scalar("SELECT block_timestamp FROM chain_lineage
        WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xperms119'")
        .fetch_one(&database.pool).await?;
    sqlx::query("UPDATE surface_bindings SET active_to = $1 WHERE surface_binding_id = $2")
        .bind(at).bind(Uuid::from_u128(0xe103)).execute(&database.pool).await?;
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
        VALUES ($1, 'ethereum-mainnet', 119, '0xperms119', 'canonical')")
        .bind(registry).execute(&database.pool).await?;
    upsert_test_surface_bindings(&database.pool, &[SurfaceBinding {
        surface_binding_id: Uuid::from_u128(0xe403), logical_name_id: format!("ens:{name}"), resource_id: registry,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath, authority_arm: "ens_v1".into(),
        active_from: at, active_to: None, chain_id: "ethereum-mainnet".into(), block_number: 119,
        block_hash: "0xperms119".into(), provenance: json!({}), canonicality_state: CanonicalityState::Canonical,
    }]).await?;
    let authority = json!({"source_event":"Transfer", "node":node,"authority_kind":"registry_only",
        "authority_key":format!("registry:ethereum-mainnet:{node}"), "owner":V2_PERMISSIONS_SUBJECT,
        "owner_getter":V2_PERMISSIONS_SUBJECT, "registry_contract":"0x0000000000000000000000000000000000000a01"});
    let mut events = vec![
        permission_fixture_event("permissions-handoff-token", Some(&logical), Some(lease),
            "TokenControlTransferred", "ens_v1_registrar_l1", 119, 0,
            json!({"source_event":"Transfer", "namehash":node, "from":V2_PERMISSIONS_SUBJECT,
                "to":V2_PERMISSIONS_OTHER_SUBJECT})),
        permission_fixture_event("permissions-handoff-unbound", Some(&logical), Some(lease),
            "SurfaceUnbound", "ens_v1_registrar_l1", 119, 0, authority.clone()),
        permission_fixture_event("permissions-handoff-bound", Some(&logical), Some(registry),
            "SurfaceBound", "ens_v1_registry_l1", 119, 0, authority.clone()),
        permission_fixture_event("permissions-handoff-epoch", Some(&logical), Some(registry),
            "AuthorityEpochChanged", "ens_v1_registry_l1", 119, 0, authority),
    ];
    for (subject, granted) in [(V2_PERMISSIONS_SUBJECT, false), (V2_PERMISSIONS_OTHER_SUBJECT, true)] {
        let source = json!({"kind":"ens_v1_authority", "authority_kind":"registrar",
            "authority_key":format!("registrar:ethereum-mainnet:{lease}"),"source_event_kind":"Transfer"});
        events.push(permission_fixture_event(&format!("permissions-handoff-holder-{subject}"), Some(&logical), Some(lease),
            "PermissionChanged", "ens_v1_registrar_l1", 119, 0,
            json!({"subject":subject,"scope":{"kind":"resource"},
                "effective_powers":if granted { json!(["resource_control"]) } else { json!([]) },
                "grant_source":if granted { source.clone() } else { Value::Null },
                "revocation_source":if granted { Value::Null } else { source },
                "inheritance_path":[],"transfer_behavior":"replace_on_authority_change"})));
    }
    if lapsed {
        let released_at: OffsetDateTime = sqlx::query_scalar("SELECT block_timestamp FROM chain_lineage
            WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xperms125'")
            .fetch_one(&database.pool).await?;
        let expiry = released_at.unix_timestamp() - 90 * 24 * 60 * 60 - 1;
        let mut release = permission_fixture_event("permissions-handoff-release", Some(&logical), Some(lease),
            "RegistrationReleased", "ens_v1_registrar_l1", 125, 0,
            json!({"source_event":"RegistrationReleased", "namehash":node,
                "expiry":expiry,"released_at":released_at.unix_timestamp()}));
        release.before_state = json!({"registrant":V2_PERMISSIONS_OTHER_SUBJECT,"expiry":expiry});
        release.transaction_hash = None;
        release.log_index = None;
        events.push(release);
        events.push(permission_fixture_event("permissions-handoff-release-holder", Some(&logical), Some(lease),
            "PermissionChanged", "ens_v1_registrar_l1", 125, 0,
            json!({"subject":V2_PERMISSIONS_OTHER_SUBJECT,"scope":{"kind":"resource"},"effective_powers":[],
                "grant_source":null,"revocation_source":{"kind":"ens_v1_authority","authority_kind":"registrar",
                    "authority_key":format!("registrar:ethereum-mainnet:{lease}"),"source_event_kind":"RegistrationReleased"},
                "inheritance_path":[],"transfer_behavior":"replace_on_authority_change"})));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
    Ok((lease, registry))
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
    let ens = seed_registry_permission_inputs(&database).await?;
    let base = seed_base_permission_inputs(&database, false).await?;
    let get = async |uri: String| -> Result<Value> {
        let response = app_router(database.app_state())
            .oneshot(Request::builder().uri(uri).body(Body::empty())?).await?;
        assert_eq!(response.status(), StatusCode::OK);
        read_json(response).await
    };
    let unfiltered = get(format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}")).await?;
    let rows = unfiltered["data"].as_array().unwrap();
    assert!(rows.iter().any(|row| row["registration_id"] == ens.to_string()));
    assert!(rows.iter().any(|row| row["registration_id"] == base.to_string()));
    let filtered = get(format!("/v1/permissions?address={V2_PERMISSIONS_SUBJECT}&namespace=basenames&page_size=1")).await?;
    assert_eq!(filtered["data"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["data"][0]["registration_id"], json!(base));
    assert!(filtered["data"][0].get("name").is_none());
    assert_eq!(filtered["page"]["has_more"], false);
    assert_eq!(filtered["page"]["next_cursor"], Value::Null);
    let matching = get(format!("/v1/permissions?registration_id={base}&namespace=basenames")).await?;
    assert_eq!(matching["data"].as_array().unwrap().len(), 1);
    let unscoped = get(format!("/v1/permissions?registration_id={ens}")).await?;
    assert!(unscoped.get("restrictions").is_some());
    let excluded = get(format!("/v1/permissions?registration_id={ens}&namespace=basenames")).await?;
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
    let (wrapper, lease) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::ControllerGranted).await?;
    unwrap_permission_fixture(&database, wrapper, lease).await?;

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
    let registry = v2_permissions_stale_resource_id();
    let lease = v2_permissions_current_resource_id();
    seed_nameless_registry_permission_inputs(&database, registry, lease).await?;
    let node = bigname_lookup::ens_namehash_hex("historical-registry.eth")?;
    let mut grant = v2_history_event("permissions-retained-lease", None, Some(lease), "RegistrationGranted", 119);
    grant.after_state["namehash"] = json!(node);
    let mut epoch = v2_history_event("permissions-historical-registry-epoch", None, Some(registry), "AuthorityEpochChanged", 120);
    epoch.source_family = "ens_v1_registry_l1".into();
    epoch.after_state = json!({"source_event":"Transfer","node":node,"authority_kind":"registry_only",
        "owner":V2_PERMISSIONS_SUBJECT,"owner_getter":V2_PERMISSIONS_SUBJECT});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant, epoch]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xhistory130").await?;
    let payload = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registration_id={registry}")).await?;
    assert_eq!(payload["data"], json!([]));
    assert_nameless_permission_roundtrip(&database, registry, lease).await?;
    database.cleanup().await
}

#[tokio::test]
async fn resolver_roles_use_the_wrapped_registration_lease_handle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (_, lease) =
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    let payload = v2_resolver_payload_for_database(
        &database,
        "/v1/resolvers/1/0x0000000000000000000000000000000000000abc/roles",
    )
    .await?;
    let wrapper_roles = payload["data"].as_array().unwrap().iter()
        .filter(|row| row["grant_event"]["block_number"] == 122).collect::<Vec<_>>();
    assert_eq!(wrapper_roles.len(), 2, "{payload}");
    assert!(wrapper_roles.iter().all(|row| row["registration_id"] == lease.to_string()), "{payload}");

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

async fn seed_nameless_registry_permission_inputs(database: &TestDatabase, registry: Uuid, lease: Uuid) -> Result<()> {
    let at = parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?.unix_timestamp();
    let blocks = (100..=130).map(|number| {
        let mut block = raw_block("ethereum-mainnet", &format!("0xhistory{number}"), None, number, at - 130 + number);
        block.canonicality_state = CanonicalityState::Canonical;
        block
    }).collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    database.seed_snapshot_selector_chain_positions(&json!({"ethereum":{"chain_id":"ethereum-mainnet",
        "block_number":130,"block_hash":"0xhistory130","timestamp":"2026-06-10T00:00:00Z"}})).await?;
    for resource in [registry, lease] {
        sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
            VALUES ($1, 'ethereum-mainnet', 100, '0xhistory100', 'canonical')")
            .bind(resource).execute(&database.pool).await?;
    }
    let mut permission = v2_history_event("permissions-nameless-registry-grant", None, Some(registry), "PermissionChanged", 117);
    permission.source_family = "ens_v1_registry_l1".into();
    permission.after_state = json!({"subject":V2_PERMISSIONS_SUBJECT,"scope":{"kind":"resource"},
        "effective_powers":["resource_control"], "grant_source":{"kind":"ens_v1_authority",
            "authority_kind":"registry_only","source_event_kind":"AuthorityTransferred"},
        "revocation_source":null,"inheritance_path":[],"transfer_behavior":"replace_on_authority_change"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[permission]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xhistory130").await
}

async fn assert_nameless_registry_permission_handle(kind: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let registry = v2_permissions_stale_resource_id();
    let lease = v2_permissions_current_resource_id();
    seed_nameless_registry_permission_inputs(&database, registry, lease).await?;
    let node = bigname_lookup::ens_namehash_hex("presurface-permissions.eth")?;
    let other_node = bigname_lookup::ens_namehash_hex("unrelated-presurface.eth")?;
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
    let at = parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?.unix_timestamp();
    let blocks = (131..=160).map(|number| raw_block("ethereum-mainnet",
        &format!("0xhistory{number}"), None, number, at + number - 130)).collect::<Vec<_>>();
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
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
        VALUES ($1, 'ethereum-mainnet', 100, '0xhistory100', 'canonical')")
        .bind(successor).execute(&database.pool).await?;
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
        publish_test_families_on(&database.pool, "ethereum-mainnet", block).await?;
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
    publish_test_families_on(&database.pool, "ethereum-mainnet", 160).await?;
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
