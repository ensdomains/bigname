use std::collections::BTreeSet;

#[tokio::test]
async fn v2_get_address_names_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    let specs = [V2AddressNameSpec {
        logical_name_id: "ens:ᏣᎳᎩ.eth",
        name: NORMALIZED_NAME,
        namehash: "node:ᏣᎳᎩ.eth",
        resource_id: Uuid::from_u128(0x34900),
        token_lineage_id: Uuid::from_u128(0x34901),
        surface_binding_id: Uuid::from_u128(0x34902),
        block_hash: "0xname349",
        block_number: 349,
        owner: "0x0000000000000000000000000000000000000349",
        registrant: "0x0000000000000000000000000000000000000349",
        registered_at: "2024-01-02T00:00:00Z",
        created_at: "2023-01-02T00:00:00Z",
        expires_at: "2027-01-02T00:00:00Z",
        relations: &[bigname_storage::AddressNameRelation::TokenHolder],
    }];
    seed_v2_address_name_storage(&database, &specs).await?;
    seed_v2_address_name_current_rows(&database, &specs).await?;
    seed_v2_address_name_relations(&database, &specs).await?;
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
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
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
            "/v2/addresses/{V2_ADDRESS}/names?q=%E1%8F%A3%E1%8E%B3"
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
            "/v2/addresses/{V2_ADDRESS}/names?q=%E1%8F%A3%E1%8E%B3%E1%8E%A9."
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
        v2_address_names_payload(&format!("/v2/addresses/{V2_ADDRESS}/names")).await?;

    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["meta"], json!({}));

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
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(data[0]["registration_status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("2024-01-02T00:00:00Z"));
    assert_eq!(data[0]["created_at"], json!("2023-01-02T00:00:00Z"));
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert_eq!(data[0]["relations"], json!(["registrant", "owner"]));
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
        v2_address_names_payload(&format!("/v2/addresses/{V2_ADDRESS}/names?relation=owner"))
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
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=ga"),
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
        "/v2/addresses/{V2_ADDRESS}/names?q=al"
    ))
    .await?;
    let mixed_case_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=AL"),
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
        v2_address_names_payload(&format!("/v2/addresses/{V2_ADDRESS}/names")).await?;
    let empty_q_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q="),
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
    seed_v2_address_name_storage(&database, &specs).await?;
    seed_v2_address_name_current_rows(&database, &specs).await?;
    seed_v2_address_name_relations(&database, &specs).await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=alice."),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert_eq!(names(rows), vec!["alice.eth"]);

    let mixed_case_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=ALICE."),
    )
    .await?;
    assert_eq!(mixed_case_payload, payload);

    let interior_dot_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=alice.e"),
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
            &format!("/v2/addresses/{V2_ADDRESS}/names?q={q}"),
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
        "/v2/addresses/{V2_ADDRESS}/names?relation=registrant,manager"
    ))
    .await?;
    let any_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?relation=any"),
    )
    .await?;

    let set_rows = set_payload["data"]
        .as_array()
        .expect("relation set data must be an array");
    assert_eq!(names(set_rows), vec!["alpha.eth", "beta.eth"]);
    assert_eq!(set_rows[0]["relations"], json!(["registrant"]));
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
    assert_eq!(any_rows[0]["relations"], json!(["registrant", "owner"]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_marks_primary_for_a_successful_non_normalized_claim() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // The projection stores the raw claim spelling and a false is-normalized marker for a valid
    // claim whose bytes were not already normalized. It is still a successful claim for alpha.eth.
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: V2_ADDRESS.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: Some("Alpha.eth".to_owned()),
                claim_provenance: json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            normalized_claim_name: None,
            claim_name_is_normalized: false,
        }],
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
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
    // A successful claim whose stored spelling does not normalize is only reachable while a
    // normalizer revision is mid-re-derivation. It is one row's defect: the page still serves and
    // no row claims to be primary.
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: V2_ADDRESS.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: Some("alpha..eth".to_owned()),
                claim_provenance: json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            normalized_claim_name: None,
            claim_name_is_normalized: false,
        }],
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
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
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: V2_ADDRESS.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::NotFound,
                raw_claim_name: None,
                claim_provenance: json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            normalized_claim_name: None,
            claim_name_is_normalized: false,
        }],
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
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
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=alpha"),
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
        v2_address_names_payload(&format!("/v2/addresses/{V2_ADDRESS}/names?dedupe=name")).await?;
    let dedupe_registration = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?dedupe=registration"),
    )
    .await?;

    let name_rows = dedupe_name["data"]
        .as_array()
        .expect("dedupe=name data must be an array");
    let registration_rows = dedupe_registration["data"]
        .as_array()
        .expect("dedupe=registration data must be an array");

    assert_eq!(name_rows.len(), 5);
    assert_eq!(registration_rows.len(), 4);
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
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_names_registration_dedupe_preserves_role_summary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let shared_resource_id = Uuid::from_u128(0xd100);
    upsert_phase_permissions_current_rows(
        &database.pool,
        &[permission_current_row(
            shared_resource_id,
            V2_PERMISSION_SUBJECT,
            PermissionScope::Registry,
            12,
            111,
        )],
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ADDRESS}/names?dedupe=registration&include=role_summary"
        ),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("combined address-name data must be an array");
    let shared_rows = rows
        .iter()
        .filter(|row| {
            row["name"] == json!("shared-one.eth") || row["name"] == json!("shared-two.eth")
        })
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 4);
    assert_eq!(shared_rows.len(), 1);
    assert_eq!(
        shared_rows[0]["role_summary"],
        json!([{
            "address": V2_PERMISSION_SUBJECT,
            "grants": [{
                "grant_scope": {"kind": "registry", "detail": {}},
                "powers": ["set_resolver", "create_subnames"]
            }]
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_address_names_sorts_by_expiry_and_registered_at() -> Result<()> {
    let (database, expires_asc) = v2_address_names_payload(&format!(
        "/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&order=asc"
    ))
    .await?;
    let expires_desc = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&order=desc"),
    )
    .await?;
    let registered = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?sort=registered_at"),
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
        v2_address_names_payload(&format!("/v2/addresses/{V2_ADDRESS}/names?page_size=2")).await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor")
        .to_owned();
    let second_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?page_size=2&cursor={next_cursor}"),
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
        &format!("/v2/addresses/{V2_OTHER_ADDRESS}/names?page_size=2&cursor={next_cursor}"),
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
            "/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&page_size=2&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_sort.status(), StatusCode::BAD_REQUEST);

    let expires_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&page_size=1"),
    )
    .await?;
    let expires_cursor = expires_page["page"]["next_cursor"]
        .as_str()
        .expect("expires page must include a cursor");
    let cross_timestamp_sort = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ADDRESS}/names?sort=registered_at&page_size=1&cursor={expires_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_timestamp_sort.status(), StatusCode::BAD_REQUEST);

    let relation_set_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?relation=manager,owner&page_size=1"),
    )
    .await?;
    let relation_set_cursor = relation_set_page["page"]["next_cursor"]
        .as_str()
        .expect("relation set page must include a cursor");
    let reordered_relation_set = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ADDRESS}/names?relation=owner,manager&page_size=1&cursor={relation_set_cursor}"
        ),
    )
    .await?;
    assert_eq!(reordered_relation_set.status(), StatusCode::OK);
    let changed_relation_set = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ADDRESS}/names?relation=owner&page_size=1&cursor={relation_set_cursor}"
        ),
    )
    .await?;
    assert_eq!(changed_relation_set.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_role_summary_missing_support_is_partial() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let resource_id = Uuid::from_u128(0xa100);
    sqlx::query("DELETE FROM permissions_current_resource_summary WHERE resource_id = $1")
        .bind(resource_id)
        .execute(&database.pool)
        .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary"),
    )
    .await?;

    assert_eq!(payload["data"][0]["name"], json!("alpha.eth"));
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
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("permission_support_unknown")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_address_role_summary_marks_wrapper_empty_as_non_authoritative() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let resource_id = Uuid::from_u128(0xb100);
    upsert_phase_permissions_current_resource_summary(
        &database.pool,
        &permission_current_resource_summary(resource_id, Some("wrapper")),
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=beta&include=role_summary"),
    )
    .await?;

    assert_eq!(payload["data"][0]["name"], json!("beta.eth"));
    assert_eq!(payload["data"][0]["role_summary"], json!([]));
    assert_eq!(payload["meta"]["completeness"], json!("partial"));
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["role_summary"])
    );
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("wrapper_holder_permissions_not_supported")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_address_names_include_role_summary_groups_permissions_by_address() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let alpha = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == "alpha.eth")
        .expect("alpha address-name fixture must exist");
    let current_inventory = address_name_record_inventory_current_row(&alpha);
    let mut stale_inventory = current_inventory.clone();
    stale_inventory.record_version_boundary = address_name_record_inventory_boundary_with_pointer(
        &alpha,
        Some(9_999),
        Some("TextChanged"),
    );
    stale_inventory.selectors = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true
        }
    ]);
    stale_inventory.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc"
            }
        }
    ]);
    database
        .insert_record_inventory_current_row(current_inventory)
        .await?;
    database
        .insert_record_inventory_current_row(stale_inventory)
        .await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?include=role_summary&page_size=1"),
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
            {
                "address": V2_PERMISSION_SUBJECT,
                "grants": [
                    {
                        "grant_scope": {
                            "kind": "registry",
                            "detail": {}
                        },
                        "powers": ["set_resolver", "create_subnames"]
                    },
                    {
                        "grant_scope": {
                            "kind": "registration",
                            "detail": {}
                        },
                        "powers": ["registration_control", "resolver_control"]
                    }
                ]
            },
            {
                "address": V2_PERMISSION_OTHER_SUBJECT,
                "grants": [
                    {
                        "grant_scope": {
                            "kind": "record_manager",
                            "detail": {
                                "chain_id": 1,
                                "manager": "0x0000000000000000000000000000000000000bb1"
                            }
                        },
                        "powers": ["set_resolver", "create_subnames"]
                    },
                    {
                        "grant_scope": {
                            "kind": "resolver",
                            "detail": {
                                "resolver": {
                                    "chain_id": 1,
                                    "address": "0x0000000000000000000000000000000000000aaa"
                                }
                            }
                        },
                        "powers": ["set_resolver", "set_records"]
                    }
                ]
            }
        ])
    );
    assert!(row["role_summary"][0].get("subject").is_none());
    assert!(
        row["role_summary"][0]["grants"][0]
            .get("effective_powers")
            .is_none()
    );
    assert!(payload["meta"].get("completeness").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_rejects_bad_address_and_unknown_include() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    let bad_address =
        v2_address_names_response_for_database(&database, "/v2/addresses/not-an-address/names")
            .await?;
    assert_eq!(bad_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(bad_address).await?["error"]["code"],
        json!("invalid_input")
    );

    let bad_include = v2_address_names_response_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?include=counts"),
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

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
    )
    .await?;

    assert_eq!(payload["data"], json!([]));
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
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
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

    let audit_rows = bigname_storage::load_address_names_current_including_noncanonical(
        &database.pool,
        V2_ADDRESS,
        Some("ens"),
        None,
    )
    .await?;
    assert!(
        audit_rows
            .iter()
            .any(|row| row.normalized_name == "beta.eth")
    );

    let compact_page = bigname_storage::load_name_current_list_page(
        &database.pool,
        &bigname_storage::NameCurrentListFilter {
            address: Some(bigname_storage::NameCurrentAddressFilter {
                address: V2_ADDRESS.to_owned(),
                relation: bigname_storage::NameCurrentAddressRelationFilter::Any,
                addresses: None,
            }),
            ..Default::default()
        },
        bigname_storage::NameCurrentListSort::Name,
        bigname_storage::NameCurrentListOrder::Asc,
        None,
        50,
        true,
    )
    .await?;
    assert!(
        compact_page
            .rows
            .iter()
            .all(|row| row.row.normalized_name != "beta.eth")
    );
    assert_eq!(compact_page.total_count, Some(4));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_current_name_reads_exclude_orphaned_project_targets_before_redo() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let beta_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = 'beta.eth'",
    )
    .fetch_one(&database.pool)
    .await?;

    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES
            ('ethereum-mainnet', '0xproject-address-target', 2001,
             '2026-04-17T02:00:01Z', 'canonical'),
            ('ethereum-mainnet', '0xproject-name-target', 2002,
             '2026-04-17T02:00:02Z', 'canonical'),
            ('ethereum-mainnet', '0xproject-primary-target', 2003,
             '2026-04-17T02:00:03Z', 'canonical');
        UPDATE bigname_phase.address_names_current
        SET chain_positions = jsonb_build_object(
                'block_number', 2001,
                'block_hash', '0xproject-address-target',
                'target_block_number', 2001,
                'target_block_hash', '0xproject-address-target'
            ),
            canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 2001,
                'target_block_hash', '0xproject-address-target'
            )
        WHERE lower(raw_name) = 'beta.eth';
        UPDATE bigname_phase.name_current
        SET chain_positions = jsonb_build_object(
                'ethereum', jsonb_build_object(
                    'chain_id', 'ethereum-mainnet',
                    'block_number', 2002,
                    'block_hash', '0xproject-name-target'
                )
            ),
            canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 2002,
                'target_block_hash', '0xproject-name-target'
            )
        WHERE lower(raw_name) = 'beta.eth';
        UPDATE bigname_phase.primary_names_current
        SET claim_provenance = claim_provenance || jsonb_build_object(
                'chain_id', 'ethereum-mainnet',
                'target_block_number', 2003,
                'target_block_hash', '0xproject-primary-target'
            )
        WHERE address = '0x0000000000000000000000000000000000000abc';
        "#,
    )
    .execute(&database.pool)
    .await?;

    let project_targets_back_no_identity_rows: bool = sqlx::query_scalar(
        r#"
        SELECT NOT EXISTS (
            SELECT 1 FROM bigname_phase.name_surfaces
            WHERE block_hash IN (
                '0xproject-address-target', '0xproject-name-target',
                '0xproject-primary-target'
            )
            UNION ALL
            SELECT 1 FROM bigname_phase.resources
            WHERE block_hash IN (
                '0xproject-address-target', '0xproject-name-target',
                '0xproject-primary-target'
            )
            UNION ALL
            SELECT 1 FROM bigname_phase.surface_bindings
            WHERE block_hash IN (
                '0xproject-address-target', '0xproject-name-target',
                '0xproject-primary-target'
            )
            UNION ALL
            SELECT 1 FROM bigname_phase.token_lineages
            WHERE block_hash IN (
                '0xproject-address-target', '0xproject-name-target',
                '0xproject-primary-target'
            )
        )
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert!(project_targets_back_no_identity_rows);
    assert!(
        bigname_storage::load_name_current(&database.pool, &beta_logical_name_id)
            .await?
            .is_some()
    );
    assert!(
        bigname_storage::load_primary_name_current(&database.pool, V2_ADDRESS, "ens", "60")
            .await?
            .is_some()
    );
    assert!(
        bigname_storage::load_address_names_current(
            &database.pool,
            V2_ADDRESS,
            Some("ens"),
            None,
        )
        .await?
        .iter()
        .any(|row| row.normalized_name == "beta.eth")
    );

    sqlx::query(
        "UPDATE bigname_phase.chain_lineage \
         SET canonicality_state = 'orphaned' \
         WHERE block_hash IN ( \
             '0xproject-address-target', '0xproject-name-target', \
             '0xproject-primary-target' \
         )",
    )
    .execute(&database.pool)
    .await?;

    assert!(
        bigname_storage::load_name_current(&database.pool, &beta_logical_name_id)
            .await?
            .is_none()
    );
    assert!(
        bigname_storage::load_primary_name_current(&database.pool, V2_ADDRESS, "ens", "60")
            .await?
            .is_none()
    );
    let default_rows = bigname_storage::load_address_names_current(
        &database.pool,
        V2_ADDRESS,
        Some("ens"),
        None,
    )
    .await?;
    assert!(
        default_rows
            .iter()
            .all(|row| row.normalized_name != "beta.eth")
    );
    let audit_rows = bigname_storage::load_address_names_current_including_noncanonical(
        &database.pool,
        V2_ADDRESS,
        Some("ens"),
        None,
    )
    .await?;
    assert!(
        audit_rows
            .iter()
            .any(|row| row.normalized_name == "beta.eth")
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
            rows.iter().all(|row| row.normalized_name != "beta.eth"),
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
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
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
    seed_v2_address_name_storage(database, &specs).await?;
    seed_v2_address_name_current_rows(database, &specs).await?;
    seed_v2_address_name_relations(database, &specs).await?;
    seed_v2_address_name_permissions(database, &specs).await?;
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: V2_ADDRESS.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            normalized_claim_name: Some("alpha.eth".to_owned()),
            claim_name_is_normalized: true,
        }],
    )
    .await?;
    Ok(())
}

async fn seed_v2_address_name_storage(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let surfaces = specs
        .iter()
        .map(|spec| {
            collection_name_surface(
                spec.logical_name_id,
                spec.name,
                spec.namehash,
                spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    let mut seen_resources = BTreeSet::new();
    let resources = specs
        .iter()
        .filter(|spec| seen_resources.insert(spec.resource_id))
        .map(|spec| {
            address_name_resource(
                spec.resource_id,
                Some(spec.token_lineage_id),
                spec.block_hash,
                spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    let mut seen_token_lineages = BTreeSet::new();
    let token_lineages = specs
        .iter()
        .filter(|spec| seen_token_lineages.insert(spec.token_lineage_id))
        .map(|spec| {
            address_name_token_lineage(spec.token_lineage_id, spec.block_hash, spec.block_number)
        })
        .collect::<Vec<_>>();
    let bindings = specs
        .iter()
        .map(|spec| {
            address_name_surface_binding(
                spec.surface_binding_id,
                spec.logical_name_id,
                spec.resource_id,
                spec.block_hash,
                spec.block_number,
                1_717_180_000 + spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    let mut seen_raw_blocks = BTreeSet::new();
    let raw_blocks = specs
        .iter()
        .filter(|spec| seen_raw_blocks.insert((spec.block_hash, spec.block_number)))
        .map(|spec| {
            raw_block(
                "ethereum-mainnet",
                spec.block_hash,
                None,
                spec.block_number,
                1_717_180_000 + spec.block_number,
            )
        })
        .collect::<Vec<_>>();

    upsert_phase_raw_blocks(&database.pool, &raw_blocks).await?;
    upsert_test_name_surfaces(&database.pool, &surfaces).await?;
    upsert_test_token_lineages(&database.pool, &token_lineages).await?;
    upsert_test_resources(&database.pool, &resources).await?;
    upsert_test_surface_bindings(&database.pool, &bindings).await?;
    Ok(())
}

async fn seed_v2_address_name_current_rows(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let mut inserted = BTreeSet::new();
    for spec in specs {
        if !inserted.insert(spec.logical_name_id) {
            continue;
        }
        database
            .insert_name_current_row(address_name_name_current_row(
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
                        "registrant": spec.registrant,
                        "expiry": spec.expires_at
                    }
                }),
            ))
            .await?;
    }
    Ok(())
}

async fn seed_v2_address_name_relations(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let mut rows = Vec::new();
    for spec in specs {
        for relation in spec.relations {
            rows.push(address_name_current_row(
                V2_ADDRESS,
                spec.logical_name_id,
                *relation,
                spec.name,
                spec.name,
                spec.namehash,
                spec.surface_binding_id,
                spec.resource_id,
                Some(spec.token_lineage_id),
                spec.block_number,
            ));
        }
    }

    upsert_phase_address_names_current_rows(&database.pool, &rows).await?;
    Ok(())
}

async fn seed_v2_address_name_permissions(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let alpha_resource_id = Uuid::from_u128(0xa100);
    let mut resource_row = permission_current_row(
        alpha_resource_id,
        V2_PERMISSION_SUBJECT,
        PermissionScope::Resource,
        7,
        107,
    );
    resource_row.effective_powers = json!(["resource_control", "resolver_control"]);
    resource_row.grant_source = json!({
        "kind": "ens_v1_authority",
        "authority_kind": "registry_owner",
        "authority_key": "registry:ethereum-mainnet:alpha",
        "source_event_kind": "Transfer"
    });

    upsert_phase_permissions_current_rows(
        &database.pool,
        &[
            resource_row,
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_SUBJECT,
                PermissionScope::Registry,
                8,
                108,
            ),
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_OTHER_SUBJECT,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                },
                9,
                109,
            ),
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_OTHER_SUBJECT,
                PermissionScope::RecordManager {
                    chain_id: "ethereum-mainnet".to_owned(),
                    manager_address: "0x0000000000000000000000000000000000000BB1".to_owned(),
                },
                10,
                110,
            ),
        ],
    )
    .await?;
    for resource_id in specs
        .iter()
        .map(|spec| spec.resource_id)
        .collect::<BTreeSet<_>>()
    {
        upsert_phase_permissions_current_resource_summary(
            &database.pool,
            &permission_current_resource_summary(resource_id, Some("registrar")),
        )
        .await?;
    }
    Ok(())
}

fn v2_address_name_specs() -> Vec<V2AddressNameSpec> {
    vec![
        V2AddressNameSpec {
            logical_name_id: "ens:alpha.eth",
            name: "alpha.eth",
            namehash: "node:alpha.eth",
            resource_id: Uuid::from_u128(0xa100),
            token_lineage_id: Uuid::from_u128(0xa101),
            surface_binding_id: Uuid::from_u128(0xa102),
            block_hash: "0xname65",
            block_number: 101,
            owner: "0x00000000000000000000000000000000000000a1",
            registrant: "0x00000000000000000000000000000000000000a2",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[
                bigname_storage::AddressNameRelation::TokenHolder,
                bigname_storage::AddressNameRelation::Registrant,
            ],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:beta.eth",
            name: "beta.eth",
            namehash: "node:beta.eth",
            resource_id: Uuid::from_u128(0xb100),
            token_lineage_id: Uuid::from_u128(0xb101),
            surface_binding_id: Uuid::from_u128(0xb102),
            block_hash: "0xname66",
            block_number: 102,
            owner: "0x00000000000000000000000000000000000000b1",
            registrant: "0x00000000000000000000000000000000000000b2",
            registered_at: "2024-03-02T00:00:00Z",
            created_at: "2023-03-02T00:00:00Z",
            expires_at: "2026-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::EffectiveController],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:gamma.eth",
            name: "gamma.eth",
            namehash: "node:gamma.eth",
            resource_id: Uuid::from_u128(0xc100),
            token_lineage_id: Uuid::from_u128(0xc101),
            surface_binding_id: Uuid::from_u128(0xc102),
            block_hash: "0xname67",
            block_number: 103,
            owner: "0x00000000000000000000000000000000000000c1",
            registrant: "0x00000000000000000000000000000000000000c2",
            registered_at: "2023-12-02T00:00:00Z",
            created_at: "2023-12-01T00:00:00Z",
            expires_at: "2028-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:shared-one.eth",
            name: "shared-one.eth",
            namehash: "node:shared-one.eth",
            resource_id: Uuid::from_u128(0xd100),
            token_lineage_id: Uuid::from_u128(0xd101),
            surface_binding_id: Uuid::from_u128(0xd102),
            block_hash: "0xname68",
            block_number: 104,
            owner: "0x00000000000000000000000000000000000000d1",
            registrant: "0x00000000000000000000000000000000000000d2",
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2024-04-01T00:00:00Z",
            expires_at: "2029-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:shared-two.eth",
            name: "shared-two.eth",
            namehash: "node:shared-two.eth",
            resource_id: Uuid::from_u128(0xd100),
            token_lineage_id: Uuid::from_u128(0xd101),
            surface_binding_id: Uuid::from_u128(0xd202),
            block_hash: "0xname69",
            block_number: 105,
            owner: "0x00000000000000000000000000000000000000d1",
            registrant: "0x00000000000000000000000000000000000000d2",
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
            namehash: "node:alice.eth",
            resource_id: Uuid::from_u128(0x34a00),
            token_lineage_id: Uuid::from_u128(0x34a01),
            surface_binding_id: Uuid::from_u128(0x34a02),
            block_hash: "0xname34a",
            block_number: 350,
            owner: "0x000000000000000000000000000000000000034a",
            registrant: "0x000000000000000000000000000000000000034a",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:alicex.eth",
            name: "alicex.eth",
            namehash: "node:alicex.eth",
            resource_id: Uuid::from_u128(0x34b00),
            token_lineage_id: Uuid::from_u128(0x34b01),
            surface_binding_id: Uuid::from_u128(0x34b02),
            block_hash: "0xname34b",
            block_number: 351,
            owner: "0x000000000000000000000000000000000000034b",
            registrant: "0x000000000000000000000000000000000000034b",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
    ]
}

struct V2AddressNameSpec {
    logical_name_id: &'static str,
    name: &'static str,
    namehash: &'static str,
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

fn address_name_record_inventory_current_row(
    spec: &V2AddressNameSpec,
) -> bigname_storage::RecordInventoryCurrentRow {
    let mut row = record_inventory_current_row(spec.logical_name_id, spec.resource_id);
    row.record_version_boundary =
        address_name_record_inventory_boundary_with_pointer(spec, None, None);
    row.chain_positions = json!({
        "ethereum-mainnet": address_name_record_inventory_chain_position(spec)
    });
    row
}

fn address_name_record_inventory_boundary_with_pointer(
    spec: &V2AddressNameSpec,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
) -> Value {
    json!({
        "logical_name_id": spec.logical_name_id,
        "resource_id": spec.resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": address_name_record_inventory_chain_position(spec)
    })
}

fn address_name_record_inventory_chain_position(spec: &V2AddressNameSpec) -> Value {
    json!({
        "chain_id": "ethereum-mainnet",
        "block_number": spec.block_number,
        "block_hash": format!("0xname{:02x}", spec.block_number),
        "timestamp": format!("2026-04-17T00:00:{:02}Z", spec.block_number % 60)
    })
}
