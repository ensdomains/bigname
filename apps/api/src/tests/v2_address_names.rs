use std::collections::BTreeSet;

#[tokio::test]
async fn v2_get_address_names_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    let specs = [V2AddressNameSpec {
        logical_name_id: "ens:ᏣᎳᎩ.eth",
        name: NORMALIZED_NAME,
        resource_id: Uuid::from_u128(0x34900),
        token_lineage_id: Uuid::from_u128(0x34901),
        surface_binding_id: Uuid::from_u128(0x34902),
        block_hash: "0xname349",
        block_number: 349,
        owner: "0x0000000000000000000000000000000000000349",
        registrant: V2_ADDRESS,
        registered_at: "2024-01-02T00:00:00Z",
        created_at: "2023-01-02T00:00:00Z",
        expires_at: "2027-01-02T00:00:00Z",
        relations: &[bigname_storage::AddressNameRelation::TokenHolder],
    }];
    seed_v2_address_name_identities(&database, &specs).await?;
    publish_v2_address_name_inputs(&database, &specs).await?;
    assert_v2_address_name_relations(&database, &specs).await?;
    let stored_raw_name: String = sqlx::query_scalar(
        "SELECT raw_name FROM bigname_phase.name_surfaces
         WHERE raw_name = $1 AND visibility_state = 'active'
           AND normalization_errors = '[]'::jsonb",
    )
    .bind(NORMALIZED_NAME)
    .fetch_one(&database.pool)
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"].as_str(), Some(stored_raw_name.as_str()));

    let prefix_payload = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?q=%E1%8F%A3%E1%8E%B3"
        ),
    )
    .await?;
    assert_eq!(
        prefix_payload["data"][0]["name"],
        json!(NORMALIZED_NAME)
    );
    let boundary_payload = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?q=%E1%8F%A3%E1%8E%B3%E1%8E%A9."
        ),
    )
    .await?;
    assert_eq!(
        boundary_payload["data"][0]["name"],
        json!(NORMALIZED_NAME)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_returns_record_rows_with_relations_and_primary_flag() -> Result<()> {
    let (database, payload) =
        v2_address_names_payload(&format!("/v1/addresses/{V2_ADDRESS}/names")).await?;

    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["total_count"], json!(5));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(105));

    let data = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert_eq!(
        names(data),
        vec![
            "alpha.eth",
            "beta.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    assert_eq!(data[0]["display_name"], json!("alpha.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(
        data[0]["namehash"],
        json!(bigname_lookup::ens_namehash_hex("alpha.eth")?)
    );
    assert_eq!(data[0]["owner"], json!(V2_ADDRESS));
    assert_eq!(data[0]["manager"], json!(V2_PERMISSION_SUBJECT));
    assert!(data[0].get("registrant").is_none());
    assert_eq!(data[0]["status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("1704153600"));
    assert_eq!(data[0]["created_at"], json!("1672617600"));
    assert_eq!(data[0]["expires_at"], json!("1798848000"));
    assert_eq!(data[0]["relations"], json!(["owner"]));
    assert_eq!(data[0]["is_primary"], json!(true));
    assert_eq!(data[1]["relations"], json!(["manager"]));
    assert_eq!(data[1]["is_primary"], json!(false));
    assert!(data[0].get("resolver").is_none());
    assert!(data[0].get("addresses").is_none());
    assert!(data[0].get("text_records").is_none());
    assert!(data[0].get("content_hash").is_none());
    assert_no_banned_v1_spellings(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_filters_owner_relation_and_q_prefix() -> Result<()> {
    let (database, owner_payload) =
        v2_address_names_payload(&format!("/v1/addresses/{V2_ADDRESS}/names?relation=owner"))
            .await?;

    let owner_rows = owner_payload["data"]
        .as_array()
        .expect("owner data must be an array");
    assert_eq!(
        names(owner_rows),
        vec!["alpha.eth", "gamma.eth", "shared-one.eth", "shared-two.eth"]
    );
    assert_eq!(owner_rows[0]["relations"], json!(["owner"]));
    assert_eq!(owner_rows[1]["relations"], json!(["owner"]));

    let q_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=ga"),
    )
    .await?;
    let q_rows = q_payload["data"]
        .as_array()
        .expect("q data must be an array");
    assert_eq!(names(q_rows), vec!["gamma.eth"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_normalizes_ascii_mixed_case_q_prefix() -> Result<()> {
    let (database, lowercase_payload) = v2_address_names_payload(&format!(
        "/v1/addresses/{V2_ADDRESS}/names?q=al"
    ))
    .await?;
    let mixed_case_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=AL"),
    )
    .await?;

    let lowercase_rows = lowercase_payload["data"]
        .as_array()
        .expect("lowercase q data must be an array");
    let mixed_case_rows = mixed_case_payload["data"]
        .as_array()
        .expect("mixed-case q data must be an array");
    assert_eq!(names(lowercase_rows), vec!["alpha.eth"]);
    assert_eq!(names(mixed_case_rows), names(lowercase_rows));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_treats_empty_q_as_absent() -> Result<()> {
    let (database, unfiltered_payload) =
        v2_address_names_payload(&format!("/v1/addresses/{V2_ADDRESS}/names")).await?;
    let empty_q_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q="),
    )
    .await?;

    assert_eq!(empty_q_payload, unfiltered_payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_trailing_dot_q_matches_label_boundary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let specs = v2_address_name_boundary_specs();
    seed_v2_address_name_identities(&database, &specs).await?;
    publish_v2_address_name_inputs(&database, &specs).await?;
    assert_v2_address_name_relations(&database, &specs).await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=alice."),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert_eq!(names(rows), vec!["alice.eth"]);

    let mixed_case_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=ALICE."),
    )
    .await?;
    assert_eq!(mixed_case_payload, payload);

    let interior_dot_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=alice.e"),
    )
    .await?;
    assert_eq!(interior_dot_payload, payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_rejects_invalid_q_dot_shapes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    for q in ["alice..", ".", "alice..x"] {
        let response = v2_address_names_response_for_database(
            &database,
            &format!("/v1/addresses/{V2_ADDRESS}/names?q={q}"),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "q={q}");
        let payload = read_json::<Value>(response).await?;
        assert_eq!(
            payload["error"]["code"],
            json!("invalid_input"),
            "q={q}"
        );
        assert!(
            payload["error"]["message"]
                .as_str()
                .is_some_and(|message| message
                    .starts_with("q must be a valid ENSIP-15 name prefix:")),
            "q={q}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_filters_relation_sets_and_any() -> Result<()> {
    let (database, set_payload) = v2_address_names_payload(&format!(
        "/v1/addresses/{V2_ADDRESS}/names?relation=owner,manager"
    ))
    .await?;
    let any_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=any"),
    )
    .await?;

    let set_rows = set_payload["data"]
        .as_array()
        .expect("relation set data must be an array");
    assert_eq!(names(set_rows), vec!["alpha.eth", "beta.eth", "gamma.eth", "shared-one.eth", "shared-two.eth"]);
    assert_eq!(set_rows[0]["relations"], json!(["owner"]));
    assert_eq!(set_rows[1]["relations"], json!(["manager"]));

    let any_rows = any_payload["data"]
        .as_array()
        .expect("relation any data must be an array");
    assert_eq!(
        names(any_rows),
        vec![
            "alpha.eth",
            "beta.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    assert_eq!(any_rows[0]["relations"], json!(["owner"]));

    // `owner` is the token holder, so the separate `registrant` relation is gone.
    for relation in ["registrant", "former_registrant"] {
        let response = v2_address_names_response_for_database(
            &database,
            &format!("/v1/addresses/{V2_ADDRESS}/names?relation={relation}"),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{relation}");
        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "invalid_input", "{relation}");
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_marks_primary_for_a_successful_non_normalized_claim() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // The address names itself in a spelling that is valid but not normalized. The claim reducer
    // keeps the raw spelling; it is still a successful claim for alpha.eth.
    publish_primary_claim(&database.pool, "ens", V2_ADDRESS, b"Alpha.eth").await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    let alpha = rows
        .iter()
        .find(|row| row["name"] == json!("alpha.eth"))
        .expect("alpha.eth row must be present");
    assert_eq!(alpha["is_primary"], json!(true));
    assert!(
        rows.iter()
            .filter(|row| row["name"] != json!("alpha.eth"))
            .all(|row| row["is_primary"] == json!(false))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_serves_the_page_when_a_primary_claim_no_longer_normalizes()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // The address names itself with bytes that do not normalize.
    publish_primary_claim(&database.pool, "ens", V2_ADDRESS, b"alpha..eth").await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| row["is_primary"] == json!(false)));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_non_success_primary_claim_does_not_mark_primary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // Setting an empty name replaces the reverse node's name record, leaving no claimed name.
    publish_primary_claim(&database.pool, "ens", V2_ADDRESS, b"").await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| row["is_primary"] == json!(false)));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_scopes_primary_claim_by_row_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_identity_name(
        &database,
        "basenames:alpha.eth",
        "alpha.eth",
        "alpha.eth",
        "node:basenames-alpha.eth",
        Uuid::from_u128(0xe100),
        Uuid::from_u128(0xe101),
        Uuid::from_u128(0xe102),
        V2_ADDRESS,
        bigname_storage::AddressNameRelation::TokenHolder,
        106,
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=alpha"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    let ens_alpha = rows
        .iter()
        .find(|row| row["namespace"] == json!("ens") && row["name"] == json!("alpha.eth"))
        .expect("ens alpha row must be present");
    let basenames_alpha = rows
        .iter()
        .find(|row| row["namespace"] == json!("basenames") && row["name"] == json!("alpha.eth"))
        .expect("basenames alpha row must be present");

    assert_eq!(ens_alpha["is_primary"], json!(true));
    assert_eq!(basenames_alpha["is_primary"], json!(false));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_dedupe_name_vs_registration() -> Result<()> {
    let (database, dedupe_name) =
        v2_address_names_payload(&format!("/v1/addresses/{V2_ADDRESS}/names?dedupe=name")).await?;
    let dedupe_registration = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?dedupe=registration"),
    )
    .await?;

    let name_rows = dedupe_name["data"]
        .as_array()
        .expect("dedupe=name data must be an array");
    let registration_rows = dedupe_registration["data"]
        .as_array()
        .expect("dedupe=registration data must be an array");

    assert_eq!(name_rows.len(), 5);
    assert_eq!(registration_rows.len(), 5);
    assert_eq!(
        name_rows
            .iter()
            .filter(|row| {
                row["name"] == json!("shared-one.eth") || row["name"] == json!("shared-two.eth")
            })
            .count(),
        2
    );
    assert_eq!(
        registration_rows
            .iter()
            .filter(|row| {
                row["name"] == json!("shared-one.eth") || row["name"] == json!("shared-two.eth")
            })
            .count(),
        2
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_names_registration_dedupe_preserves_role_summary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // shared-one.eth and shared-two.eth are distinct registrations with the same registry owner.
    let shared = v2_address_name_specs().remove(3);
    let (block, hash) = address_fixture_head(&database).await?;
    let grant = address_owner_grant(&shared, json!({"kind":"resource"}), "resource_control", block, &hash)?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    seed_address_registry_operator(&database, shared.owner, V2_PERMISSION_OTHER_SUBJECT).await?;
    let payload = v2_address_names_payload_for_database(&database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?dedupe=registration&include=role_summary")).await?;
    let rows = payload["data"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    let shared_rows = rows.iter().filter(|row| row["name"].as_str().unwrap().starts_with("shared-")).collect::<Vec<_>>();
    assert_eq!(shared_rows.len(), 2);
    assert_ne!(
        shared_rows[0]["permission_resource_id"],
        shared_rows[1]["permission_resource_id"]
    );
    for row in &shared_rows {
        let grants = address_name_inline_grants(row);
        assert!(!grants.is_empty());
        assert_eq!(grants, address_name_permission_grants(&database,
            row["permission_resource_id"].as_str().unwrap(), "").await?);
    }
    assert_eq!(address_name_inline_grants(shared_rows[0]).len(), 2);
    assert_eq!(address_name_inline_grants(shared_rows[1]).len(), 1);
    database.cleanup().await
}

// Finite expiries remain ordered and visible when they cross the calendar formatting limit.
// The real ENSv2 grant/renewal test covers the wider unsigned domain.
#[tokio::test]
async fn v2_get_address_names_retains_finite_expiry_beyond_the_calendar_range() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    for expiry in [0_i64, 253_402_300_799, 253_402_300_800] {
        set_address_name_expiry(&database, "alpha.eth", &json!(expiry)).await?;
        for order in ["asc", "desc"] {
            let payload = v2_address_names_payload_for_database(
                &database,
                &format!("/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&order={order}"),
            ).await?;
            let rows = payload["data"].as_array().expect("expiry rows");
            let alpha = rows.iter().find(|row| row["name"] == "alpha.eth").unwrap();
            assert_eq!(alpha["expires_at"], json!(expiry.to_string()), "{payload}");
            assert!(alpha.get("expires_at_reason").is_none(), "{payload}");
            let first = (expiry == 0) == (order == "asc");
            let position = if first { 0 } else { rows.len() - 1 };
            assert_eq!(rows[position]["name"], json!("alpha.eth"), "{payload}");
        }
    }
    database.cleanup().await
}

// Equal observed integer expiries use the collection identity as their stable tie-break.
#[tokio::test]
async fn v2_get_address_names_breaks_equal_expiry_ties_by_identity() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let formatted = json!("1735689600");
    set_address_name_expiry(&database, "alpha.eth", &json!(1_735_689_600_i64)).await?;
    set_address_name_expiry(&database, "beta.eth", &json!(1_735_689_600_i64)).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&order=asc"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("expires asc data");
    assert_eq!(&names(rows)[..2], ["beta.eth", "alpha.eth"], "{payload}");
    for row in &rows[..2] {
        assert_eq!(row.get("expires_at"), Some(&formatted), "{row}");
    }
    database.cleanup().await
}

// Paging keeps exact finite expiries and equal-value identity ties across the calendar limit.
#[tokio::test]
async fn v2_get_address_names_pages_through_large_finite_expiries() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    for (name, registration) in [
        ("alpha.eth", json!(253_402_300_801_i64)),
        ("beta.eth", json!(253_402_300_801_i64)),
        ("gamma.eth", json!(253_402_300_800_i64)),
    ] {
        set_address_name_expiry(&database, name, &registration).await?;
    }
    for (order, expected_names, expected_expiries) in [
        ("asc", ["shared-one.eth", "shared-two.eth", "gamma.eth", "beta.eth", "alpha.eth"],
            [1_862_006_400_i64, 1_862_006_400, 253_402_300_800, 253_402_300_801, 253_402_300_801]),
        ("desc", ["beta.eth", "alpha.eth", "gamma.eth", "shared-one.eth", "shared-two.eth"],
            [253_402_300_801_i64, 253_402_300_801, 253_402_300_800, 1_862_006_400, 1_862_006_400]),
    ] {
        let mut listed = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let uri = match &cursor {
                Some(cursor) => format!(
                    "/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&order={order}&page_size=1&cursor={cursor}"
                ),
                None => format!(
                    "/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&order={order}&page_size=1"
                ),
            };
            let page = v2_address_names_payload_for_database(&database, &uri).await?;
            for row in page["data"].as_array().expect("page data") {
                listed.push((
                    row["name"].as_str().expect("row name").to_owned(),
                    row.get("expires_at").cloned(),
                ));
            }
            match page["page"]["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }
        assert_eq!(listed.len(), 5, "{order}: {listed:?}");
        assert_eq!(listed.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(),
            expected_names, "{order}: {listed:?}");
        for ((_, expiry), expected) in listed.iter().zip(expected_expiries) {
            assert_eq!(expiry.as_ref(), Some(&json!(expected.to_string())), "{order}: {listed:?}");
        }
    }
    database.cleanup().await
}

async fn set_address_name_expiry(database: &TestDatabase, name: &str, expiry: &Value) -> Result<()> {
    sqlx::query("UPDATE normalized_events SET after_state = jsonb_set(after_state, '{expiry}', $2)
        WHERE logical_name_id = $1 AND event_kind = 'RegistrationGranted'")
        .bind(bigname_storage::logical_name_id_for_name("ens", name)).bind(expiry)
        .execute(&database.pool).await?;
    rebuild_address_fixture(database).await
}

#[tokio::test]
async fn v2_get_address_names_sorts_by_expiry_and_registered_at() -> Result<()> {
    let (database, expires_asc) = v2_address_names_payload(&format!(
        "/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&order=asc"
    ))
    .await?;
    let expires_desc = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&order=desc"),
    )
    .await?;
    let registered = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?sort=registered_at"),
    )
    .await?;

    assert_eq!(
        names(expires_asc["data"].as_array().expect("expires asc data")),
        vec![
            "beta.eth",
            "alpha.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    assert_eq!(
        names(expires_desc["data"].as_array().expect("expires desc data")),
        vec![
            "shared-one.eth",
            "shared-two.eth",
            "gamma.eth",
            "alpha.eth",
            "beta.eth"
        ]
    );
    assert_eq!(
        names(registered["data"].as_array().expect("registered data")),
        vec![
            "gamma.eth",
            "alpha.eth",
            "beta.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_paginates_and_rejects_bound_cursor_reuse() -> Result<()> {
    let (database, first_page) =
        v2_address_names_payload(&format!("/v1/addresses/{V2_ADDRESS}/names?page_size=2")).await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor")
        .to_owned();
    let second_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    let first_names = names(first_page["data"].as_array().expect("first page data"));
    let second_names = names(second_page["data"].as_array().expect("second page data"));
    assert_eq!(first_names, vec!["alpha.eth", "beta.eth"]);
    assert_eq!(second_names, vec!["gamma.eth", "shared-one.eth"]);
    assert!(first_names.iter().all(|name| !second_names.contains(name)));
    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));

    let cross_address = v2_address_names_response_for_database(
        &database,
        &format!("/v1/addresses/{V2_OTHER_ADDRESS}/names?page_size=2&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(cross_address).await?["error"]["code"],
        json!("invalid_input")
    );

    let cross_sort = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&page_size=2&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_sort.status(), StatusCode::BAD_REQUEST);

    let expires_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?sort=expires_at&page_size=1"),
    )
    .await?;
    let expires_cursor = expires_page["page"]["next_cursor"]
        .as_str()
        .expect("expires page must include a cursor");
    let cross_timestamp_sort = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?sort=registered_at&page_size=1&cursor={expires_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_timestamp_sort.status(), StatusCode::BAD_REQUEST);

    let relation_set_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=manager,owner&page_size=1"),
    )
    .await?;
    let relation_set_cursor = relation_set_page["page"]["next_cursor"]
        .as_str()
        .expect("relation set page must include a cursor");
    let reordered_relation_set = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=owner,manager&page_size=1&cursor={relation_set_cursor}"
        ),
    )
    .await?;
    assert_eq!(reordered_relation_set.status(), StatusCode::OK);
    let changed_relation_set = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=owner&page_size=1&cursor={relation_set_cursor}"
        ),
    )
    .await?;
    assert_eq!(changed_relation_set.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_role_summary_marks_wrapper_page_as_non_authoritative() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    wrap_address_name(&database, "beta.eth", 0xb300, None).await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta&include=role_summary"),
    )
    .await?;

    assert_eq!(payload["data"][0]["name"], json!("beta.eth"));
    // The wrapper token holder's grant is the only listed one.
    assert_eq!(
        payload["data"][0]["role_summary"],
        json!([{"address":V2_ADDRESS, "grants":[{
            "grant_scope":{"kind":"registration", "detail":{}},
            "powers":["registration_control"]
        }]}])
    );
    assert!(payload["data"][0].get("restrictions").is_none());
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["role_summary"])
    );
    assert_unlisted_permission_surfaces(&payload, V2_WRAPPER_UNLISTED_SURFACES);

    database.cleanup().await
}

#[tokio::test]
async fn v2_address_role_summary_serves_restrictions_per_row() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let resource_id = Uuid::from_u128(0xb100);
    wrap_address_name(
        &database,
        "beta.eth",
        0xb300,
        Some(("emancipated", 65_536, 1_900_000_000)),
    )
    .await?;

    let with_summary = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta&include=role_summary"),
    )
    .await?;
    assert_eq!(with_summary["data"][0]["name"], json!("beta.eth"));
    assert_eq!(
        with_summary["data"][0]["restrictions"]["kind"],
        json!("ens_v1_wrapper")
    );
    assert_eq!(
        with_summary["data"][0]["restrictions"]["registration_id"],
        json!(resource_id.to_string())
    );
    assert_eq!(
        with_summary["data"][0]["restrictions"]["wrapper_state"],
        json!("emancipated")
    );
    assert_eq!(
        with_summary["data"][0]["restrictions"]["wrapper_fuses"]["parent_cannot_control"],
        json!(true)
    );
    assert_eq!(
        with_summary["data"][0]["restrictions"]["wrapper_expires_at"],
        json!("1900000000")
    );

    let without_summary = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta"),
    )
    .await?;
    assert!(without_summary["data"][0].get("restrictions").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_address_role_summary_marks_uningested_approvals_non_authoritative() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary"),
    )
    .await?;

    assert!(
        payload["data"][0]["role_summary"]
            .as_array()
            .is_some_and(|summary| !summary.is_empty())
    );
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["role_summary"])
    );
    assert_unlisted_permission_surfaces(&payload, V2_UNWRAPPED_UNLISTED_SURFACES);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_address_names_include_role_summary_groups_permissions_by_address() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_address_alpha_record_inputs(&database, true).await?;
    seed_v2_address_registry_operator(&database).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?include=role_summary&page_size=1"),
    )
    .await?;

    let row = &payload["data"]
        .as_array()
        .expect("role-summary data must be an array")[0];
    assert_eq!(row["name"], json!("alpha.eth"));
    assert_eq!(row["record_count"], json!(3));
    assert_eq!(
        row["role_summary"],
        json!([
            {"address":V2_PERMISSION_SUBJECT, "grants":[
                {"grant_scope":{"kind":"resolver", "detail":{"resolver":{
                    "chain_id":1, "address":"0x0000000000000000000000000000000000000aaa"}}},
                 "powers":["resolver_control"]},
                {"grant_scope":{"kind":"registration","detail":{}},
                 "powers":["registration_control"]}
            ]},
            {"address":V2_PERMISSION_OTHER_SUBJECT, "grants":[{
                "grant_relation":"operator",
                "grant_scope":{"kind":"account", "detail":{
                    "chain_id":1,"authority_kind":"registry", "authority_contract":V2_ADDRESS_REGISTRY,
                    "owner":V2_PERMISSION_SUBJECT}},
                "powers":["registry_control"]
            }]}
        ])
    );
    assert!(row["role_summary"][0].get("subject").is_none());
    assert!(
        row["role_summary"][0]["grants"][0]
            .get("effective_powers")
            .is_none()
    );
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["role_summary"])
    );
    assert_unlisted_permission_surfaces(&payload, V2_UNWRAPPED_UNLISTED_SURFACES);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_role_summary_includes_registry_operator_grant() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_address_registry_operator(&database).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary"),
    ).await?;
    let grants = payload["data"][0]["role_summary"].as_array().unwrap();
    assert!(grants.iter().flat_map(|role| role["grants"].as_array().unwrap()).any(|grant| {
        grant.get("grant_relation") == Some(&json!("operator"))
            && grant["powers"] == json!(["registry_control"])
    }));
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_role_summary_does_not_change_address_membership() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let uri = format!("/v1/addresses/{V2_ADDRESS}/names");
    let before = v2_address_names_payload_for_database(&database, &uri).await?;
    seed_v2_address_registry_operator(&database).await?;
    let after = v2_address_names_payload_for_database(&database, &uri).await?;
    assert_eq!(before["data"], after["data"]);
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_role_summary_omits_relation_for_direct_grants() -> Result<()> {
    let (database, payload) = v2_address_names_payload(&format!(
        "/v1/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary"
    )).await?;
    assert!(payload["data"][0]["role_summary"].as_array().unwrap().iter()
        .flat_map(|role| role["grants"].as_array().unwrap())
        .all(|grant| grant.get("grant_relation").is_none()));
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_role_summary_uses_wrapper_reason_for_wrapper_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    wrap_address_name(&database, "beta.eth", 0xb300, None).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta&include=role_summary"),
    ).await?;
    assert_unlisted_permission_surfaces(&payload, V2_WRAPPER_UNLISTED_SURFACES);
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_role_summary_reports_sorted_union_for_ens_v1_and_ens_v2_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // alpha.eth keeps its ENSv1 registrar authority; beta.eth moves to an ENSv2 registration.
    bind_address_name_ens_v2(&database, "beta.eth", 0xb200, false).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?include=role_summary"),
    ).await?;
    let names = payload["data"].as_array().unwrap().iter()
        .map(|row| row["name"].as_str().unwrap()).collect::<Vec<_>>();
    assert!(names.contains(&"alpha.eth") && names.contains(&"beta.eth"), "{names:?}");
    assert_eq!(payload["meta"]["unsupported_fields"], json!(["role_summary"]));
    assert_unlisted_permission_surfaces(
        &payload,
        &["ens_v2_registry_operators", "registrar_approvals", "resolver_approvals"],
    );

    let ens_v2_only = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta&include=role_summary"),
    ).await?;
    assert_unlisted_permission_surfaces(&ens_v2_only, V2_ENS_V2_REGISTRY_UNLISTED_SURFACES);
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_address_names_rejects_bad_address_and_unknown_include() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    let bad_address =
        v2_address_names_response_for_database(&database, "/v1/addresses/not-an-address/names")
            .await?;
    assert_eq!(bad_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(bad_address).await?["error"]["code"],
        json!("invalid_input")
    );

    let bad_include = v2_address_names_response_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?include=events"),
    )
    .await?;
    assert_eq!(bad_include.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(bad_include).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_empty_returns_200_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    seed_v2_address_name_identities(&database, &[]).await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;

    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["total_count"], json!(0));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_name_collections_exclude_orphaned_phase_lineage_before_project_redo()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;

    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-mainnet', '0xreorg-beta', 1002, '2026-04-17T01:00:02Z',
            'canonical'::bigname_phase.canonicality_state
        );
        UPDATE bigname_phase.name_surfaces
        SET block_hash = '0xreorg-beta', block_number = 1002,
            canonicality_state = 'canonical'::bigname_phase.canonicality_state
        WHERE raw_name = 'beta.eth';
        UPDATE bigname_phase.token_lineages
        SET block_hash = '0xreorg-beta', block_number = 1002,
            canonicality_state = 'canonical'::bigname_phase.canonicality_state
        WHERE token_lineage_id = '00000000-0000-0000-0000-00000000b101'::uuid;
        UPDATE bigname_phase.resources
        SET block_hash = '0xreorg-beta', block_number = 1002,
            canonicality_state = 'canonical'::bigname_phase.canonicality_state
        WHERE resource_id = '00000000-0000-0000-0000-00000000b100'::uuid;
        UPDATE bigname_phase.surface_bindings
        SET block_hash = '0xreorg-beta', block_number = 1002,
            canonicality_state = 'canonical'::bigname_phase.canonicality_state
        WHERE surface_binding_id = '00000000-0000-0000-0000-00000000b102'::uuid;
        UPDATE bigname_phase.chain_lineage
        SET canonicality_state = 'orphaned'::bigname_phase.canonicality_state
        WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xreorg-beta'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert_eq!(
        names(rows),
        vec![
            "alpha.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_name_reads_require_readable_phase_identity_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let cases = [
        "UPDATE bigname_phase.name_surfaces SET canonicality_state = 'orphaned' WHERE raw_name = 'beta.eth'",
        "UPDATE bigname_phase.resources SET canonicality_state = 'orphaned' WHERE resource_id = '00000000-0000-0000-0000-00000000b100'::uuid",
        "UPDATE bigname_phase.surface_bindings SET canonicality_state = 'orphaned' WHERE surface_binding_id = '00000000-0000-0000-0000-00000000b102'::uuid",
        "UPDATE bigname_phase.token_lineages SET canonicality_state = 'orphaned' WHERE token_lineage_id = '00000000-0000-0000-0000-00000000b101'::uuid",
    ];
    let resets = [
        "UPDATE bigname_phase.name_surfaces SET canonicality_state = 'finalized' WHERE raw_name = 'beta.eth'",
        "UPDATE bigname_phase.resources SET canonicality_state = 'finalized' WHERE resource_id = '00000000-0000-0000-0000-00000000b100'::uuid",
        "UPDATE bigname_phase.surface_bindings SET canonicality_state = 'finalized' WHERE surface_binding_id = '00000000-0000-0000-0000-00000000b102'::uuid",
        "UPDATE bigname_phase.token_lineages SET canonicality_state = 'finalized' WHERE token_lineage_id = '00000000-0000-0000-0000-00000000b101'::uuid",
    ];

    for (orphan, reset) in cases.into_iter().zip(resets) {
        sqlx::query(orphan).execute(&database.pool).await?;
        let rows = bigname_storage::load_address_names_current(
            &database.pool,
            V2_ADDRESS,
            Some("ens"),
            None,
        )
        .await?;
        assert!(
            rows
                .iter()
                .all(|row| row.canonical_display_name != "beta.eth"),
            "canonical address read admitted beta after {orphan}"
        );
        sqlx::query(reset).execute(&database.pool).await?;
    }

    database.cleanup().await?;
    Ok(())
}

const V2_ADDRESS: &str = "0x0000000000000000000000000000000000000abc";
const V2_OTHER_ADDRESS: &str = "0x0000000000000000000000000000000000000def";
const V2_PERMISSION_SUBJECT: &str = "0x0000000000000000000000000000000000000c01";
const V2_PERMISSION_OTHER_SUBJECT: &str = "0x0000000000000000000000000000000000000c02";

async fn v2_address_names_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let payload = v2_address_names_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_address_names_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = v2_address_names_response_for_database(database, uri).await?;
    let status = response.status();
    let payload = read_json::<Value>(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload}");
    Ok(payload)
}

async fn v2_address_names_response_for_database(
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
        .context("v2 address names request failed")
}

fn names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("row must include name"))
        .collect()
}

async fn seed_v2_address_names_fixture(database: &TestDatabase) -> Result<()> {
    let specs = v2_address_name_specs();
    seed_v2_address_name_identities(database, &specs).await?;
    publish_v2_address_name_inputs(database, &specs).await?;
    assert_v2_address_name_relations(database, &specs).await?;
    seed_v2_address_name_permissions(database, &specs).await?;
    publish_primary_claim(&database.pool, "ens", V2_ADDRESS, b"alpha.eth").await?;
    Ok(())
}

async fn seed_v2_address_name_identities(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"base":{
            "chain_id":"base-mainnet", "block_number":1, "block_hash":"0xcount-base-empty",
            "timestamp":"2024-01-01T00:00:00Z"
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 1, "0xcount-base-empty").await?;
    for spec in specs {
        for at in [spec.created_at, spec.registered_at] {
            let (block, hash) = address_fixture_time_block(at)?;
            sqlx::query(
                "INSERT INTO chain_lineage
                    (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
                 VALUES ('ethereum-mainnet', $1, $2, $3::timestamptz, 'canonical')
                 ON CONFLICT (chain_id, block_hash) DO NOTHING",
            )
            .bind(&hash)
            .bind(block)
            .bind(at)
            .execute(&database.pool)
            .await?;
        }
    }
    for spec in specs {
        let (block, hash) = address_fixture_time_block(spec.created_at)?;
        seed_family_identity_inputs(
            &database.pool,
            "ens",
            spec.name,
            "ethereum-mainnet",
            block,
            &hash,
            spec.resource_id,
            spec.token_lineage_id,
            spec.surface_binding_id,
            "ens_v1",
        )
        .await?;
        database
            .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
                "chain_id":"ethereum-mainnet", "block_number":spec.block_number,
                "block_hash":spec.block_hash, "timestamp":"2024-05-31T18:26:47Z"
            }}))
            .await?;
    }
    if specs.is_empty() {
        rebuild_address_fixture(database).await?;
    }
    Ok(())
}

// The fixture's creation and registration times, each at its own block below the head blocks.
const ADDRESS_FIXTURE_TIMES: [&str; 8] = [
    "2023-01-02T00:00:00Z",
    "2023-03-02T00:00:00Z",
    "2023-12-01T00:00:00Z",
    "2023-12-02T00:00:00Z",
    "2024-01-02T00:00:00Z",
    "2024-03-02T00:00:00Z",
    "2024-04-01T00:00:00Z",
    "2024-04-02T00:00:00Z",
];

fn address_fixture_time_block(at: &str) -> Result<(i64, String)> {
    let index = ADDRESS_FIXTURE_TIMES
        .iter()
        .position(|time| *time == at)
        .with_context(|| format!("{at} is not an address fixture time"))?;
    Ok((10 + index as i64, format!("0xaddress-time-{index}")))
}

// These are retained registry and registrar inputs. Relations are derived by the family reader.
async fn publish_v2_address_name_inputs(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let mut events = Vec::new();
    for spec in specs {
        let logical = bigname_storage::logical_name_id_for_name("ens", spec.name);
        let node = bigname_lookup::ens_namehash_hex(spec.name)?;
        for (at, kind, family, after) in [
            (
                spec.created_at,
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                json!({"source_event":"Transfer", "node":node, "owner":spec.owner,
                    "owner_getter":spec.owner, "registry_contract":V2_ADDRESS_REGISTRY,
                    "emitter_role":"registry"}),
            ),
            (
                spec.registered_at,
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                json!({"authority_kind":"registrar", "registrant":spec.registrant,
                    "expiry":parse_rfc3339_utc_timestamp(spec.expires_at).map_err(|e| anyhow::anyhow!("{e}"))?.unix_timestamp()}),
            ),
        ] {
            let (block, hash) = address_fixture_time_block(at)?;
            events.push(address_fixture_event(
                &format!("address-{}-{kind}", spec.resource_id),
                Some(&logical),
                Some(spec.resource_id),
                kind,
                family,
                block,
                &hash,
                NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
                after,
            ));
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(database).await
}

async fn assert_v2_address_name_relations(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let rows =
        bigname_storage::load_address_names_current(&database.pool, V2_ADDRESS, Some("ens"), None)
            .await?;
    for spec in specs {
        for relation in spec.relations {
            anyhow::ensure!(
                rows.iter().any(|row| row.logical_name_id
                    == bigname_storage::logical_name_id_for_name("ens", spec.name)
                    && row.relation == *relation),
                "actual ownership inputs did not derive {relation:?} for {}",
                spec.name
            );
        }
    }
    Ok(())
}

const V2_ADDRESS_REGISTRY: &str = "0x0000000000000000000000000000000000000b22";

#[allow(clippy::too_many_arguments)]
fn address_fixture_event(
    identity: &str,
    name: Option<&str>,
    resource: Option<Uuid>,
    kind: &str,
    family: &str,
    block: i64,
    hash: &str,
    log: i64,
    after: Value,
) -> NormalizedEvent {
    let mut event = history_event(
        identity,
        name,
        resource,
        Some("ethereum-mainnet"),
        Some(block),
        Some(hash),
        Some("0xaddress-input"),
        Some(log),
        CanonicalityState::Canonical,
    );
    event.event_kind = kind.into();
    event.source_family = family.into();
    event.before_state = json!({});
    event.after_state = after;
    event
}

async fn address_fixture_head(database: &TestDatabase) -> Result<(i64, String)> {
    Ok(sqlx::query_as("SELECT block_number, block_hash FROM chain_lineage
        WHERE chain_id = 'ethereum-mainnet' AND canonicality_state IN ('canonical','safe','finalized')
        ORDER BY block_number DESC LIMIT 1").fetch_one(&database.pool).await?)
}

async fn rebuild_address_fixture(database: &TestDatabase) -> Result<()> {
    let (block, hash) = address_fixture_head(database).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, &hash).await
}

// The registry owner's grant as the ENSv1 adapter writes it for a registrar-backed name.
async fn seed_v2_address_name_permissions(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let Some(alpha) = specs.iter().find(|spec| spec.name == "alpha.eth") else {
        return Ok(());
    };
    let (block, hash) = address_fixture_head(database).await?;
    let event = address_owner_grant(alpha, json!({"kind":"resource"}), "resource_control", block, &hash)?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_address_fixture(database).await
}

fn address_owner_grant(
    spec: &V2AddressNameSpec,
    scope: Value,
    power: &str,
    block: i64,
    hash: &str,
) -> Result<NormalizedEvent> {
    let logical = bigname_storage::logical_name_id_for_name("ens", spec.name);
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    Ok(address_fixture_event(
        &format!("address-owner-grant-{}-{ordinal}", spec.resource_id),
        Some(&logical),
        Some(spec.resource_id),
        "PermissionChanged",
        "ens_v1_registry_l1",
        block,
        hash,
        ordinal,
        json!({"subject":spec.owner, "scope":scope, "effective_powers":[power],
            "grant_source":{"kind":"ens_v1_authority", "authority_kind":"registrar",
                "authority_key":format!("registrar:ethereum-mainnet:{}", spec.resource_id),
                "source_event_kind":"AuthorityTransferred"},
            "revocation_source":null, "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"}),
    ))
}

async fn seed_address_alpha_record_inputs(
    database: &TestDatabase,
    old_version: bool,
) -> Result<()> {
    let resolver = "0x0000000000000000000000000000000000000aaa";
    let (block, hash) = address_fixture_head(database).await?;
    append_name_resolver_input(database, "ens", "alpha.eth", resolver).await?;
    // Setting a resolver gives the registry owner resolver control over it.
    let alpha = v2_address_name_specs().remove(0);
    let grant = address_owner_grant(
        &alpha,
        json!({"kind":"resolver", "chain_id":"ethereum-mainnet", "resolver_address":resolver}),
        "resolver_control",
        block,
        &hash,
    )?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    if old_version {
        insert_family_fixture_record_writes(
            &database.pool,
            "ens",
            "ethereum-mainnet",
            "alpha.eth",
            resolver,
            block,
            &hash,
            &[family_fixture_record_write(
                "text:obsolete",
                Some(json!("old")),
            )],
        )
        .await?;
        let mut version = address_fixture_event(
            "address-alpha-version",
            None,
            None,
            "RecordVersionChanged",
            "ens_v1_resolver_l1",
            block,
            &hash,
            NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
            json!({"resolver":resolver,"node":bigname_lookup::ens_namehash_hex("alpha.eth")?,
                "source_event":"VersionChanged","record_version":1}),
        );
        version.raw_fact_ref["emitting_address"] = json!(resolver);
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[version]).await?;
    }
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "alpha.eth",
        resolver,
        block,
        &hash,
        &[
            family_fixture_record_write("addr:60", Some(json!(V2_ADDRESS))),
            family_fixture_record_write("text:url", Some(json!("https://example.test"))),
            family_fixture_record_write("contenthash", Some(json!("0x1234"))),
        ],
    )
    .await?;
    rebuild_address_fixture(database).await
}

fn v2_address_name_specs() -> Vec<V2AddressNameSpec> {
    vec![
        V2AddressNameSpec {
            logical_name_id: "ens:alpha.eth",
            name: "alpha.eth",
            resource_id: Uuid::from_u128(0xa100),
            token_lineage_id: Uuid::from_u128(0xa101),
            surface_binding_id: Uuid::from_u128(0xa102),
            block_hash: "0xname65",
            block_number: 101,
            owner: V2_PERMISSION_SUBJECT,
            registrant: V2_ADDRESS,
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:beta.eth",
            name: "beta.eth",
            resource_id: Uuid::from_u128(0xb100),
            token_lineage_id: Uuid::from_u128(0xb101),
            surface_binding_id: Uuid::from_u128(0xb102),
            block_hash: "0xname66",
            block_number: 102,
            owner: V2_ADDRESS,
            registrant: "0x00000000000000000000000000000000000000b2",
            registered_at: "2024-03-02T00:00:00Z",
            created_at: "2023-03-02T00:00:00Z",
            expires_at: "2026-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::EffectiveController],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:gamma.eth",
            name: "gamma.eth",
            resource_id: Uuid::from_u128(0xc100),
            token_lineage_id: Uuid::from_u128(0xc101),
            surface_binding_id: Uuid::from_u128(0xc102),
            block_hash: "0xname67",
            block_number: 103,
            owner: "0x00000000000000000000000000000000000000c1",
            registrant: V2_ADDRESS,
            registered_at: "2023-12-02T00:00:00Z",
            created_at: "2023-12-01T00:00:00Z",
            expires_at: "2028-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:shared-one.eth",
            name: "shared-one.eth",
            resource_id: Uuid::from_u128(0xd100),
            token_lineage_id: Uuid::from_u128(0xd101),
            surface_binding_id: Uuid::from_u128(0xd102),
            block_hash: "0xname68",
            block_number: 104,
            owner: "0x00000000000000000000000000000000000000d1",
            registrant: V2_ADDRESS,
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2024-04-01T00:00:00Z",
            expires_at: "2029-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:shared-two.eth",
            name: "shared-two.eth",
            resource_id: Uuid::from_u128(0xd200),
            token_lineage_id: Uuid::from_u128(0xd201),
            surface_binding_id: Uuid::from_u128(0xd202),
            block_hash: "0xname69",
            block_number: 105,
            owner: "0x00000000000000000000000000000000000000d1",
            registrant: V2_ADDRESS,
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2024-04-01T00:00:00Z",
            expires_at: "2029-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
    ]
}

fn v2_address_name_boundary_specs() -> Vec<V2AddressNameSpec> {
    vec![
        V2AddressNameSpec {
            logical_name_id: "ens:alice.eth",
            name: "alice.eth",
            resource_id: Uuid::from_u128(0x34a00),
            token_lineage_id: Uuid::from_u128(0x34a01),
            surface_binding_id: Uuid::from_u128(0x34a02),
            block_hash: "0xname34a",
            block_number: 350,
            owner: "0x000000000000000000000000000000000000034a",
            registrant: V2_ADDRESS,
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:alicex.eth",
            name: "alicex.eth",
            resource_id: Uuid::from_u128(0x34b00),
            token_lineage_id: Uuid::from_u128(0x34b01),
            surface_binding_id: Uuid::from_u128(0x34b02),
            block_hash: "0xname34b",
            block_number: 351,
            owner: "0x000000000000000000000000000000000000034b",
            registrant: V2_ADDRESS,
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
    ]
}

async fn seed_v2_address_registry_operator(database: &TestDatabase) -> Result<()> {
    seed_address_registry_operator(database, V2_PERMISSION_SUBJECT, V2_PERMISSION_OTHER_SUBJECT)
        .await
}

async fn seed_address_registry_operator(
    database: &TestDatabase,
    owner: &str,
    subject: &str,
) -> Result<()> {
    let (block, hash) = address_fixture_head(database).await?;
    let event = address_operator_approval(subject, owner, true, block, &hash);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_address_fixture(database).await
}

/// An ENSv1 registry `ApprovalForAll` of `owner` to `subject`, as the registry adapter writes it.
fn address_operator_approval(
    subject: &str,
    owner: &str,
    approved: bool,
    block: i64,
    hash: &str,
) -> NormalizedEvent {
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let source = json!({"kind":"raw_log", "source_event":"ApprovalForAll"});
    address_fixture_event(
        &format!("address-registry-operator-{owner}-{subject}-{ordinal}"),
        None,
        None,
        "AccountPermissionChanged",
        "ens_v1_registry_l1",
        block,
        hash,
        ordinal,
        json!({"subject":subject, "relation_kind":"operator", "approved":approved,
            "scope":{"kind":"account", "chain_id":"ethereum-mainnet", "authority_kind":"registry",
                "authority_contract":V2_ADDRESS_REGISTRY,
                "authority_contract_instance_id":Uuid::from_u128(0x605), "owner":owner},
            "effective_powers":if approved { json!(["registry_control"]) } else { json!([]) },
            "grant_source":if approved { source.clone() } else { json!({}) },
            "revocation_source":if approved { Value::Null } else { source },
            "inheritance_path":[],
            "transfer_behavior":{"mode":"owner_scoped", "on_registry_owner_change":"ceases_to_apply"},
            "source_event":"ApprovalForAll"}),
    )
}

/// Bind `name` to a new ENSv2 registration at the head block, registered to `V2_ADDRESS`, with a
/// migration proof when `migrated`.
async fn bind_address_name_ens_v2(
    database: &TestDatabase,
    name: &str,
    seed: u128,
    migrated: bool,
) -> Result<Uuid> {
    let (block, hash) = address_fixture_head(database).await?;
    let resource = Uuid::from_u128(seed);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        "ethereum-mainnet",
        block,
        &hash,
        resource,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v2",
    )
    .await?;
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let mut events = vec![address_fixture_event(
        &format!("address-{name}-ens-v2-grant"),
        Some(&logical),
        Some(resource),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        block,
        &hash,
        ordinal,
        json!({"authority_kind":"ens_v2_registry", "status":"registered",
            "registrant":V2_ADDRESS, "expiry":1_900_000_000_i64}),
    )];
    if migrated {
        let mut proof = address_fixture_event(
            &format!("address-{name}-migration"),
            Some(&logical),
            Some(resource),
            "MigrationApplied",
            "ens_v2_migration_l1",
            block,
            &hash,
            ordinal + 1,
            json!({"transition_id":format!("{name}-migration")}),
        );
        proof.derivation_kind = "ens_v2_migration".into();
        events.push(proof);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(database).await?;
    Ok(resource)
}

/// Wrap `name` in the NameWrapper at the head block: its registrar lease stays the linked
/// registration, the wrapper resource `seed` holds control, and the wrapper token goes to
/// `V2_ADDRESS`. `state` adds the wrapper state, fuses and expiry.
async fn wrap_address_name(
    database: &TestDatabase,
    name: &str,
    seed: u128,
    state: Option<(&str, i64, i64)>,
) -> Result<Uuid> {
    let spec = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == name)
        .context("wrapped name must be an address-name fixture")?;
    let holder = V2_ADDRESS;
    let (block, hash) = address_fixture_head(database).await?;
    let wrapper = Uuid::from_u128(seed);
    sqlx::query(
        "UPDATE surface_bindings SET active_to = (SELECT block_timestamp FROM chain_lineage
         WHERE chain_id = 'ethereum-mainnet' AND block_hash = $2) WHERE surface_binding_id = $1",
    )
    .bind(spec.surface_binding_id)
    .bind(&hash)
    .execute(&database.pool)
    .await?;
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        "ethereum-mainnet",
        block,
        &hash,
        wrapper,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v1",
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    // The binding records the log of the `NameWrapped` that opened it.
    sqlx::query(
        "UPDATE surface_bindings SET provenance = jsonb_build_object('transaction_index', 0,
         'log_index', $2::bigint) WHERE surface_binding_id = $1",
    )
    .bind(Uuid::from_u128(seed + 2))
    .bind(ordinal)
    .execute(&database.pool)
    .await?;
    let wrapped = |suffix: &str, kind: &str, log: i64, after: Value| {
        address_fixture_event(
            &format!("address-{name}-wrapper-{suffix}"),
            Some(&logical),
            Some(wrapper),
            kind,
            "ens_v1_wrapper_l1",
            block,
            &hash,
            ordinal + log,
            after,
        )
    };
    let mut events = vec![
        wrapped("binding", "SurfaceBound", 0,
            json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper",
                "wrapped_registrar_resource_id":spec.resource_id})),
        wrapped("epoch", "AuthorityEpochChanged", 1,
            json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper",
                "owner":holder})),
        wrapped("holder", "TokenControlTransferred", 2,
            json!({"source_event":"NameWrapped", "node":node, "owner":holder, "to":holder})),
        wrapped("grant", "PermissionChanged", 3,
            json!({"subject":holder, "scope":{"kind":"resource"},
                "effective_powers":["resource_control"], "grant_source":{"kind":"raw_log",
                    "source_event":"NameWrapped", "authority_kind":"wrapper", "relation_kind":"holder"},
                "revocation_source":null, "inheritance_path":[], "transfer_behavior":{}})),
    ];
    if let Some((wrapper_state, fuses, expiry)) = state {
        let mut scope = wrapped("scope", "PermissionScopeChanged", 4,
            json!({"source_event":"NameWrapped", "wrapper_state":wrapper_state, "fuses":fuses}));
        let mut expiry = wrapped("expiry", "ExpiryChanged", 5,
            json!({"source_event":"NameWrapped", "expiry":expiry}));
        scope.logical_name_id = None;
        expiry.logical_name_id = None;
        events.extend([scope, expiry]);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(database).await?;
    Ok(wrapper)
}

struct V2AddressNameSpec {
    logical_name_id: &'static str,
    name: &'static str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    block_hash: &'static str,
    block_number: i64,
    owner: &'static str,
    registrant: &'static str,
    registered_at: &'static str,
    created_at: &'static str,
    expires_at: &'static str,
    relations: &'static [bigname_storage::AddressNameRelation],
}

#[tokio::test]
async fn v2_get_address_names_filters_by_authority_and_reports_migration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // alpha.eth migrates to an ENSv2 registration at the head block.
    bind_address_name_ens_v2(&database, "alpha.eth", 0xa200, true).await?;

    let all = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = all["data"].as_array().expect("data must be an array");
    assert_eq!(rows[0]["name"], json!("alpha.eth"));
    assert_eq!(rows[0]["authority"], json!("ens_v2"));
    assert_eq!(rows[0]["migrated_at"], json!("1717180007"));
    // The migration keeps the ENSv1 lease's registration time.
    assert_eq!(rows[0]["registered_at"], json!("1704153600"));
    assert_eq!(rows[1]["name"], json!("beta.eth"));
    assert_eq!(rows[1]["authority"], json!("ens_v1"));
    assert!(rows[1].get("migrated_at").is_none());
    assert_eq!(rows[2]["authority"], json!("ens_v1"));

    let v2_only = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?authority=ens_v2"),
    )
    .await?;
    assert_eq!(
        names(v2_only["data"].as_array().expect("v2 data")),
        vec!["alpha.eth"]
    );
    let v1_only = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?authority=ens_v1&page_size=1"),
    )
    .await?;
    assert_eq!(
        names(v1_only["data"].as_array().expect("v1 data")),
        vec!["beta.eth"]
    );

    // Native ENSv2 authority alone does not prove migration.
    bind_address_name_ens_v2(&database, "beta.eth", 0xb200, false).await?;
    for (filter, expected) in [
        ("is_migrated=true", vec!["alpha.eth"]),
        (
            "is_migrated=false",
            vec!["beta.eth", "gamma.eth", "shared-one.eth", "shared-two.eth"],
        ),
        ("authority=ens_v2", vec!["alpha.eth", "beta.eth"]),
        ("is_migrated=true&q=beta", vec![]),
    ] {
        let payload = v2_address_names_payload_for_database(
            &database,
            &format!("/v1/addresses/{V2_ADDRESS}/names?{filter}"),
        )
        .await?;
        assert_eq!(names(payload["data"].as_array().unwrap()), expected);
        assert_eq!(payload["page"]["total_count"], json!(expected.len()));
    }

    let invalid = v2_address_names_response_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?authority=basenames"),
    )
    .await?;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let first_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1"),
    )
    .await?;
    let cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor");
    let rebound = v2_address_names_response_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&authority=ens_v1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(rebound.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_include_counts_adds_subname_and_record_counts() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_address_alpha_record_inputs(&database, false).await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?include=counts"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("data must be an array");
    assert_eq!(rows[0]["name"], json!("alpha.eth"));
    assert_eq!(rows[0]["subname_count"], json!(0));
    assert_eq!(rows[0]["record_count"], json!(3));
    assert!(rows[0].get("role_summary").is_none());
    assert!(rows[0].get("event_count").is_none());
    assert_eq!(rows[1]["name"], json!("beta.eth"));
    assert_eq!(rows[1]["subname_count"], json!(0));
    assert!(rows[1].get("record_count").is_none());

    let plain = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    assert!(plain["data"][0].get("subname_count").is_none());
    assert!(plain["data"][0].get("record_count").is_none());

    let both = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?include=counts,role_summary"),
    )
    .await?;
    assert_eq!(both["data"][0]["record_count"], json!(3));
    assert!(both["data"][0].get("role_summary").is_some());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_name_totals_match_filtered_deduplicated_pages() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    for filter in [
        "relation=owner",
        "relation=owner&dedupe=registration",
        "relation=owner,manager",
        "dedupe=registration",
        "q=shared",
        "q=missing",
    ] {
        let base = format!("/v1/addresses/{V2_ADDRESS}/names?{filter}");
        let all = v2_address_names_payload_for_database(&database, &base).await?;
        let expected = all["data"].as_array().unwrap().len();
        assert_eq!(all["page"]["total_count"], json!(expected), "{filter}");
        let mut seen = Vec::new();
        let mut uri = format!("{base}&page_size=1");
        loop {
            let page = v2_address_names_payload_for_database(&database, &uri).await?;
            assert_eq!(page["page"]["total_count"], json!(expected), "{filter}");
            seen.extend(
                page["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["name"].clone()),
            );
            let Some(cursor) = page["page"]["next_cursor"].as_str() else {
                break;
            };
            uri = format!("{base}&page_size=1&cursor={cursor}");
        }
        assert_eq!(seen.len(), expected);
        assert_eq!(
            seen,
            all["data"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["name"].clone())
                .collect::<Vec<_>>()
        );
    }
    database.cleanup().await
}

/// `owner` is the token holder and `manager` the registry owner of an unwrapped lease; a wrapped
/// one is owned and managed by its token holder in every wrapper state, and has no `manager` in
/// registrar grace, on every row that serves the name and in the address relations.
#[tokio::test]
async fn v2_owner_and_manager_follow_the_wrapper_state_on_every_name_row() -> Result<()> {
    const HOLDER: &str = "0x00000000000000000000000000000000000000b2";
    const DOT_ETH: i64 = 65_536 | 131_072;
    for (wrap, owner, manager) in [
        (None, HOLDER, Some(V2_ADDRESS)),
        (Some(("emancipated", DOT_ETH, None)), V2_ADDRESS, Some(V2_ADDRESS)),
        (Some(("locked", DOT_ETH | 1, None)), V2_ADDRESS, Some(V2_ADDRESS)),
        (Some(("emancipated", DOT_ETH, Some(30))), V2_ADDRESS, None),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_address_names_fixture(&database).await?;
        if let Some((state, fuses, grace_days)) = wrap {
            let clock: i64 = sqlx::query_scalar(
                "SELECT extract(epoch FROM block_timestamp)::bigint FROM chain_lineage
                 WHERE chain_id = 'ethereum-mainnet' ORDER BY block_number DESC LIMIT 1",
            )
            .fetch_one(&database.pool)
            .await?;
            let expiry = grace_days.map_or(1_900_000_000, |days| clock + days * 86_400);
            wrap_address_name(&database, "beta.eth", 0xb300, Some((state, fuses, expiry))).await?;
        }
        let detail = assert_lookup_detail_matches_name_detail(&database, "beta.eth").await?;
        assert_eq!(detail["owner"], json!(owner), "{wrap:?}: {detail}");
        assert_eq!(detail.get("manager"), manager.map(|manager| json!(manager)).as_ref(), "{wrap:?}: {detail}");
        assert!(detail.get("registrant").is_none(), "{wrap:?}: {detail}");

        let rows = |payload: &Value| {
            payload["data"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|row| row["name"] == json!("beta.eth"))
                .cloned()
        };
        for uri in [
            format!("/v1/addresses/{owner}/names?q=beta"),
            "/v1/names?namespace=ens&expires_after=0".to_owned(),
            "/v1/search?q=beta".to_owned(),
        ] {
            let (status, payload) = read_family_response(&database, &uri).await?;
            assert_eq!(status, StatusCode::OK, "{uri}: {payload}");
            let row = rows(&payload).with_context(|| format!("{wrap:?} {uri}: no beta.eth row {payload}"))?;
            assert_eq!(row.get("manager"), detail.get("manager"), "{wrap:?} {uri}: {row}");
            assert_eq!(row.get("owner"), detail.get("owner"), "{wrap:?} {uri}: {row}");
            assert!(row.get("registrant").is_none(), "{wrap:?} {uri}: {row}");
        }
        for (relation, address, listed) in [
            ("owner", owner, true),
            ("manager", manager.unwrap_or(V2_ADDRESS), manager.is_some()),
        ] {
            let uri = format!("/v1/addresses/{address}/names?relation={relation}");
            let (status, payload) = read_family_response(&database, &uri).await?;
            assert_eq!(status, StatusCode::OK, "{uri}: {payload}");
            assert_eq!(rows(&payload).is_some(), listed, "{wrap:?} {uri}: {payload}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

/// Subnames reads wrapper expiries once per chain while composing its parent and page.
/// Search serves the same fields from the published summary without a wrapper-state read.
#[tokio::test]
async fn v2_wrapper_expiries_are_read_once_per_chain_per_request() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
    insert_family_registry_child_edge(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "perms.eth",
        &format!("{:#x}", alloy_primitives::keccak256(b"sub")),
        "0x00000000000000000000000000000000000000e7",
        120,
        "0xperms120",
    )
    .await?;
    seed_wrapped_subname_inputs(&database, "sub.perms.eth", Uuid::from_u128(0x5a_0505)).await?;
    for (uri, batch, served, expected_reads) in [
        ("/v1/names/perms.eth/subnames", 200, 1, 1),
        ("/v1/search?q=perms&match=contains&namespace=ens", 1, 2, 0),
    ] {
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (status, body) =
            bigname_storage::wrapper_expiry::seams::with_wrapper_expiry_read_counter(
                reads.clone(),
                bigname_storage::families::name::seams::with_batch_size(
                    batch,
                    read_family_response(&database, uri),
                ),
            )
            .await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        let with_expiry = body["data"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|row| row["ens_v1"]["wrapper_expires_at"].is_string())
            .count();
        assert_eq!(with_expiry, served, "{uri}: {body}");
        assert_eq!(
            reads.load(std::sync::atomic::Ordering::Relaxed),
            expected_reads,
            "{uri}: wrapper-state reads"
        );
    }
    database.cleanup().await
}

/// Search reads its page and wrapper expiries on one snapshot and releases it before the namespace
/// revalidation, which reads through the pool, so a one-connection pool serves both a bare and
/// a namespace-scoped search.
#[tokio::test]
async fn v2_search_serves_wrapper_expiries_on_a_one_connection_pool() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_wrapped_reserved_after_cutover(&database).await?;
    let config = database.database_config(1)?;
    let options = PgConnectOptions::from_str(config.database_url.as_deref().context("test URL")?)?
        .options([("search_path", "bigname_phase".to_owned())]);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    for uri in [
        "/v1/search?q=alice",
        "/v1/search?q=alice&namespace=ens",
    ] {
        let state =
            AppState::new_with_rpc_urls(pool.clone(), bigname_lookup::ChainRpcUrls::default())
                .with_public_namespaces_for_test(["ens"]);
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            app_router(state).oneshot(Request::builder().uri(uri).body(Body::empty())?),
        )
        .await
        .with_context(|| format!("{uri} stalled on a one-connection pool"))??;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert!(
            body["data"][0]["ens_v1"]["wrapper_expires_at"].is_string(),
            "{uri}: {body}"
        );
    }
    pool.close().await;
    database.cleanup().await
}

/// TYR-134: a wrapped subname serves its token holder as `manager` on name detail, lookup detail
/// and its parent's subnames row, before and after it is emancipated, and the `manager` relation
/// lists the subname for that holder.
#[tokio::test]
async fn v2_wrapped_subname_manager_is_the_token_holder_in_every_state() -> Result<()> {
    for (state, fuses) in [("wrapped", 0), ("emancipated", 65_536)] {
        let manager = Some(json!(V2_PERMISSIONS_SUBJECT));
        let database = TestDatabase::new_migrated().await?;
        seed_perms_wrapped_lease(&database, WrappedLeaseShape::LinkRecorded).await?;
        // perms.eth created the child in the registry and the NameWrapper took its node.
        insert_family_registry_child_edge(
            &database.pool,
            "ens",
            "ethereum-mainnet",
            "perms.eth",
            &format!("{:#x}", alloy_primitives::keccak256(b"sub")),
            "0x00000000000000000000000000000000000000e7",
            120,
            "0xperms120",
        )
        .await?;
        let wrapper = Uuid::from_u128(0x5a_0505);
        seed_wrapped_subname_inputs(&database, "sub.perms.eth", wrapper).await?;
        if state != "wrapped" {
            insert_permission_wrapper_state(&database, wrapper, state, fuses, 1_800_000_000, 125)
                .await?;
            rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130")
                .await?;
        }
        let detail = assert_lookup_detail_matches_name_detail(&database, "sub.perms.eth").await?;
        assert_eq!(detail["ens_v1"]["wrapper_state"], json!(state), "{detail}");
        // A wrapped subname row serves its NameWrapper expiry as the top-level expiry too.
        assert_eq!(detail["ens_v1"]["wrapper_expires_at"], json!("1800000000"), "{detail}");
        assert_eq!(detail["expires_at"], json!("1800000000"), "{detail}");
        assert_eq!(detail.get("manager"), manager.as_ref(), "{state}: {detail}");
        let (status, subnames) =
            read_family_response(&database, "/v1/names/perms.eth/subnames").await?;
        assert_eq!(status, StatusCode::OK, "{subnames}");
        let row = subnames["data"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|row| row["namehash"] == detail["namehash"])
            .with_context(|| format!("no sub.perms.eth row: {subnames}"))?;
        assert_eq!(row.get("manager"), manager.as_ref(), "{state}: {row}");
        assert_eq!(row["ens_v1"], detail["ens_v1"], "{state}: {row}");
        let (status, managed) = read_family_response(
            &database,
            &format!("/v1/addresses/{V2_PERMISSIONS_SUBJECT}/names?relation=manager&namespace=ens"),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{managed}");
        assert!(
            managed["data"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|row| row["namehash"] == detail["namehash"]),
            "{state}: {managed}"
        );
        database.cleanup().await?;
    }
    Ok(())
}
