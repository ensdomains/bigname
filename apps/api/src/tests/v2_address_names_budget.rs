// Exercise the real router and follow the public cursor contract.
async fn address_name_permission_grants(
    database: &TestDatabase,
    id: &str,
    namespace: &str,
) -> Result<Vec<String>> {
    let mut uri = format!("/v2/permissions?registration_id={id}{namespace}&page_size=37");
    let mut grants = Vec::new();
    loop {
        let payload = v2_permissions_payload_for_database(database, &uri).await?;
        for row in payload["data"].as_array().unwrap() {
            grants.push(
                json!([
                    row["address"],
                    row["grant_relation"],
                    row["grant_scope"],
                    row["powers"]
                ])
                .to_string(),
            );
        }
        let Some(cursor) = payload["page"]["next_cursor"].as_str() else {
            break;
        };
        uri =
            format!("/v2/permissions?registration_id={id}{namespace}&page_size=37&cursor={cursor}");
    }
    grants.sort();
    let unique = grants.iter().collect::<BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        grants.len(),
        "cursor traversal must not duplicate grants"
    );
    Ok(grants)
}

fn address_name_inline_grants(row: &Value) -> Vec<String> {
    let mut grants = Vec::new();
    for subject in row["role_summary"].as_array().unwrap() {
        for grant in subject["grants"].as_array().unwrap() {
            grants.push(
                json!([
                    subject["address"],
                    grant["grant_relation"],
                    grant["grant_scope"],
                    grant["powers"]
                ])
                .to_string(),
            );
        }
    }
    grants.sort();
    grants
}

async fn seed_address_name_budget_grants(
    database: &TestDatabase,
    resource: Uuid,
    count: usize,
) -> Result<()> {
    sqlx::query("DELETE FROM bigname_phase.permissions_current")
        .execute(&database.pool)
        .await?;
    let rows = (0..count)
        .map(|index| {
            permission_current_row(
                resource,
                &format!("0x{:040x}", index + 1),
                PermissionScope::Registry,
                8,
                108,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_permissions_current_rows(&database.pool, &rows).await?;
    Ok(())
}

#[tokio::test]
async fn v2_address_names_grant_budget_boundaries_and_single_resource_recovery() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let id = Uuid::from_u128(0xa100);
    for count in [999, 1000, 1001] {
        seed_address_name_budget_grants(&database, id, count).await?;
        let uri = format!("/v2/addresses/{V2_ADDRESS}/names?q=alpha&page_size=200");
        let plain = v2_address_names_payload_for_database(&database, &uri).await?;
        assert_eq!(
            plain["data"][0]["permission_resource_id"],
            json!(id.to_string())
        );
        let response = v2_address_names_response_for_database(
            &database,
            &format!("{uri}&include=role_summary"),
        )
        .await?;
        if count <= 1000 {
            assert_eq!(response.status(), StatusCode::OK);
            let included: Value = read_json(response).await?;
            let grants = address_name_inline_grants(&included["data"][0]);
            assert_eq!(grants.len(), count);
            let oracle = bigname_storage::load_effective_permissions_by_resource_ids(
                &database.pool,
                &[id],
                None,
            )
            .await?;
            let mut oracle_grants = oracle
                .iter()
                .map(|row| {
                    json!([
                        row.subject,
                        row.grant_relation.map(|_| "operator"),
                        crate::v2::effective_permission_scope_value(&row.scope).unwrap(),
                        crate::v2::permission_powers_value(&row.effective_powers).unwrap()
                    ])
                    .to_string()
                })
                .collect::<Vec<_>>();
            oracle_grants.sort();
            assert_eq!(grants, oracle_grants);
            assert_eq!(
                grants,
                address_name_permission_grants(&database, &id.to_string(), "").await?
            );
            assert_eq!(included["page"], plain["page"]);
        } else {
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let error: Value = read_json(response).await?;
            assert_eq!(error["error"]["code"], json!("unsupported"));
            assert!(error.get("data").is_none());
            let mut violations = Vec::new();
            collect_pipeline_vocabulary_in_error_body(
                "role-summary overflow",
                &error,
                &mut violations,
            );
            assert!(violations.is_empty(), "{violations:?}");
            assert_eq!(
                address_name_permission_grants(
                    &database,
                    plain["data"][0]["permission_resource_id"].as_str().unwrap(),
                    ""
                )
                .await?
                .len(),
                count
            );
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_grant_budget_counts_repeated_resource_summaries() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let id = Uuid::from_u128(0xd100);
    for count in [500, 501] {
        seed_address_name_budget_grants(&database, id, count).await?;
        let uri = format!("/v2/addresses/{V2_ADDRESS}/names?q=shared&include=role_summary");
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(
            response.status(),
            if count == 500 {
                StatusCode::OK
            } else {
                StatusCode::UNPROCESSABLE_ENTITY
            }
        );
        if count == 500 {
            let payload: Value = read_json(response).await?;
            assert_eq!(
                payload["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| address_name_inline_grants(row).len())
                    .sum::<usize>(),
                1000
            );
        }
        let deduped =
            v2_address_names_payload_for_database(&database, &format!("{uri}&dedupe=registration"))
                .await?;
        assert_eq!(deduped["data"].as_array().unwrap().len(), 1);
        assert_eq!(address_name_inline_grants(&deduped["data"][0]).len(), count);
        assert_eq!(
            deduped["data"][0]["permission_resource_id"],
            json!(id.to_string())
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_permission_id_recovery_ignores_name_anchor_and_product_registration()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_address_registry_operator(&database).await?;
    let id = Uuid::from_u128(0xa100);
    // Prepare distinct and matching declared product IDs for the #816 name-detail
    // preference. This baseline does not yet expose that preference through name detail;
    // the test checks that this metadata cannot redirect permission-resource reads.
    for product_id in [Uuid::from_u128(0xb100), id] {
        for kind in ["registry_only", "registrar", "wrapper"] {
            for unsupported in [false, true] {
                sqlx::query(
                    "UPDATE bigname_phase.name_current
                     SET declared_summary=jsonb_set(declared_summary, '{registration,resource_id}', to_jsonb($1::text)),
                         support_status=$2, unsupported_reason=$3
                     WHERE raw_name='alpha.eth'",
                )
                .bind(product_id.to_string())
                .bind(if unsupported { "unsupported" } else { "supported" })
                .bind(unsupported.then_some("conflicting_current_ens_authority"))
                .execute(&database.pool)
                .await?;
                // Preserve the registry binding that supplies the operator grant.
                sqlx::query("UPDATE bigname_phase.permissions_current_resource_summary SET authority_kind=$1 WHERE resource_id=$2")
                    .bind(kind).bind(id).execute(&database.pool).await?;
                for namespace in ["", "&namespace=ens"] {
                    for dedupe in ["name", "registration"] {
                        let uri = format!(
                            "/v2/addresses/{V2_ADDRESS}/names?q=alpha&dedupe={dedupe}{namespace}"
                        );
                        let plain = v2_address_names_payload_for_database(&database, &uri).await?;
                        let included = v2_address_names_payload_for_database(
                            &database,
                            &format!("{uri}&include=role_summary"),
                        )
                        .await?;
                        assert_eq!(
                            plain["data"][0]["permission_resource_id"],
                            json!(id.to_string())
                        );
                        let expected = address_name_inline_grants(&included["data"][0]);
                        assert!(!expected.is_empty());
                        assert_eq!(
                            expected,
                            address_name_permission_grants(&database, &id.to_string(), namespace)
                                .await?
                        );
                    }
                }
            }
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_permission_id_does_not_resolve_name_again() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let selected = Uuid::from_u128(0xa100);
    let other = Uuid::from_u128(0xb100);
    // The address-name representative and a subsequent current-name lookup can differ.
    sqlx::query("UPDATE bigname_phase.name_current SET resource_id=$1 WHERE raw_name='alpha.eth'")
        .bind(other)
        .execute(&database.pool)
        .await?;
    let named =
        v2_permissions_payload_for_database(&database, "/v2/permissions?name=alpha.eth").await?;
    assert_eq!(named["data"], json!([]));
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary"),
    )
    .await?;
    assert_eq!(
        payload["data"][0]["permission_resource_id"],
        json!(selected.to_string())
    );
    let grants = address_name_inline_grants(&payload["data"][0]);
    assert!(!grants.is_empty());
    assert_eq!(
        grants,
        address_name_permission_grants(&database, &selected.to_string(), "").await?
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_grant_budget_maximum_page_operators_and_default_plan() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut specs = v2_address_name_specs();
    specs.truncate(1);
    for index in 1..200_u128 {
        specs.push(V2AddressNameSpec {
            logical_name_id: Box::leak(format!("ens:budget{index}.eth").into_boxed_str()),
            name: Box::leak(format!("budget{index}.eth").into_boxed_str()),
            namehash: Box::leak(format!("node:budget{index}.eth").into_boxed_str()),
            resource_id: Uuid::from_u128(0x900000 + index * 3),
            token_lineage_id: Uuid::from_u128(0x900001 + index * 3),
            surface_binding_id: Uuid::from_u128(0x900002 + index * 3),
            block_hash: Box::leak(format!("0xname{:x}", 1000 + index).into_boxed_str()),
            block_number: (1000 + index) as i64,
            owner: "0x00000000000000000000000000000000000000a1",
            registrant: "0x00000000000000000000000000000000000000a2",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        });
    }
    seed_v2_address_name_storage(&database, &specs).await?;
    seed_v2_address_name_current_rows(&database, &specs).await?;
    seed_v2_address_name_relations(&database, &specs).await?;
    seed_v2_address_name_permissions(&database, &specs).await?;
    sqlx::query("DELETE FROM bigname_phase.permissions_current")
        .execute(&database.pool)
        .await?;
    seed_v2_address_registry_operator(&database).await?;
    sqlx::query("UPDATE bigname_phase.permissions_current_resource_summary SET registry_owner='0x0000000000000000000000000000000000000a11', registry_contract='0x0000000000000000000000000000000000000b22', registry_binding_provenance=jsonb_build_object('chain_id', provenance->>'chain_id'), registry_binding_chain_positions=jsonb_build_object('block_hash', chain_positions->>'target_block_hash')")
        .execute(&database.pool).await?;
    // Four more approved operators plus 1,100 revoked candidates. Eligibility must
    // precede both branch limits, and each approved operator applies to all 200 names.
    sqlx::query(
        r#"INSERT INTO bigname_phase.account_permission_state_current (
        chain_id, authority_kind, authority_contract, authority_contract_instance_id,
        owner, subject, relation_kind, approved, effective_powers, grant_source,
        inheritance_path, transfer_behavior, provenance, chain_positions,
        canonicality_summary, manifest_version)
        SELECT chain_id, authority_kind, authority_contract, authority_contract_instance_id,
        owner, '0x'||lpad(to_hex(i), 40, '0'), relation_kind, i <= 4,
        effective_powers, grant_source, inheritance_path, transfer_behavior,
        provenance, chain_positions, canonicality_summary, manifest_version
        FROM bigname_phase.account_permission_state_current CROSS JOIN generate_series(1,1104) i
        WHERE subject=$1"#,
    )
    .bind(V2_PERMISSION_SUBJECT)
    .execute(&database.pool)
    .await?;
    let ids = specs
        .iter()
        .map(|spec| spec.resource_id)
        .collect::<Vec<_>>();
    let uri = format!("/v2/addresses/{V2_ADDRESS}/names?page_size=200");
    let plain = v2_address_names_payload_for_database(&database, &uri).await?;
    let at_budget =
        v2_address_names_payload_for_database(&database, &format!("{uri}&include=role_summary"))
            .await?;
    assert_eq!(at_budget["data"].as_array().unwrap().len(), 200);
    assert_eq!(at_budget["page"], plain["page"]);
    assert_eq!(
        at_budget["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| address_name_inline_grants(row).len())
            .sum::<usize>(),
        1000
    );
    let oracle =
        bigname_storage::load_effective_permissions_by_resource_ids(&database.pool, &ids, None)
            .await?;
    assert_eq!(oracle.len(), 1000);
    let bounded = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
        &database.pool,
        &ids,
        None,
        1000,
    )
    .await?;
    let keys = |rows: &[bigname_storage::EffectivePermissionRow]| {
        rows.iter()
            .map(|row| {
                (
                    row.resource_id,
                    row.subject.clone(),
                    row.scope.storage_key(),
                    row.effective_powers.to_string(),
                )
            })
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(keys(&bounded), keys(&oracle));
    for row in at_budget["data"].as_array().unwrap() {
        assert_eq!(
            address_name_inline_grants(row),
            address_name_permission_grants(
                &database,
                row["permission_resource_id"].as_str().unwrap(),
                ""
            )
            .await?
        );
    }
    // A single direct grant over the same page makes the mixed total 1,001.
    upsert_phase_permissions_current_rows(
        &database.pool,
        &[permission_current_row(
            Uuid::from_u128(0xa100),
            V2_PERMISSION_SUBJECT,
            PermissionScope::Registry,
            8,
            108,
        )],
    )
    .await?;
    let over =
        v2_address_names_response_for_database(&database, &format!("{uri}&include=role_summary"))
            .await?;
    assert_eq!(over.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let plan = bigname_storage::explain_bounded_effective_permissions_by_resource_ids(
        &database.pool,
        &ids,
        None,
        1000,
    )
    .await?;
    println!(
        "bounded role-summary default plan: {}",
        serde_json::to_string(&plan)?
    );
    assert_eq!(plan[0]["Plan"]["Node Type"], json!("Limit"));
    assert_eq!(plan[0]["Plan"]["Actual Rows"], json!(1001));
    // Pure operator overflow is independently rejected.
    sqlx::query("DELETE FROM bigname_phase.permissions_current")
        .execute(&database.pool)
        .await?;
    sqlx::query("UPDATE bigname_phase.account_permission_state_current SET approved=true WHERE subject='0x0000000000000000000000000000000000000005'").execute(&database.pool).await?;
    let over =
        v2_address_names_response_for_database(&database, &format!("{uri}&include=role_summary"))
            .await?;
    assert_eq!(over.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        v2_address_names_payload_for_database(&database, &uri).await?,
        plain
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_grant_budget_filters_ineligible_direct_rows_before_limit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let id = Uuid::from_u128(0xa100);
    seed_address_name_budget_grants(&database, id, 2100).await?;
    sqlx::query("UPDATE bigname_phase.permissions_current SET canonicality_summary=jsonb_build_object('state', 'orphaned') WHERE subject <= $1")
        .bind(format!("0x{:040x}", 1100)).execute(&database.pool).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary&namespace=ens"),
    )
    .await?;
    let grants = address_name_inline_grants(&payload["data"][0]);
    assert_eq!(grants.len(), 1000);
    assert_eq!(
        grants,
        address_name_permission_grants(&database, &id.to_string(), "&namespace=ens").await?
    );
    database.cleanup().await
}
