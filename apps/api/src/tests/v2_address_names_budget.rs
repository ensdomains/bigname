// Exercise the real router and follow the public cursor contract.
async fn address_name_permission_grants(
    database: &TestDatabase,
    id: &str,
    namespace: &str,
) -> Result<Vec<String>> {
    let mut uri = format!("/v1/permissions?registration_id={id}{namespace}&page_size=37");
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
            format!("/v1/permissions?registration_id={id}{namespace}&page_size=37&cursor={cursor}");
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

const ADDRESS_BUDGET_RESOLVER: &str = "0x0000000000000000000000000000000000000fab";
const ADDRESS_BUDGET_MANIFEST: &str = "fixture/address-budget-resolver.toml";

fn address_budget_subject(index: usize) -> String {
    format!("0x{:040x}", 0xb000_0000_usize + index)
}

/// A permissioned resolver behind a proxy: its per-subject roles give one registration as many
/// grants as a test needs. Declared once per database, at the head block.
async fn address_budget_resolver_manifest(database: &TestDatabase) -> Result<i64> {
    if let Some(manifest) =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE file_path = $1")
            .bind(ADDRESS_BUDGET_MANIFEST)
            .fetch_optional(&database.pool)
            .await?
    {
        return Ok(manifest);
    }
    let chain = "ethereum-mainnet";
    let implementation = "0x0000000000000000000000000000000000000fac";
    let payload = json!({"contracts":[], "resolver_implementations":[
        {"role":"permissioned_resolver", "address":implementation}]});
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_resolver_l1', $1, 'fixture', 'active', 'fixture', $2, $3)
         RETURNING manifest_id",
    )
    .bind(chain)
    .bind(ADDRESS_BUDGET_MANIFEST)
    .bind(&payload)
    .fetch_one(&database.pool)
    .await?;
    seed_fixture_manifest_update(&database.pool, manifest, chain, "ens", "ens_v2_resolver_l1", &payload)
        .await?;
    let (block, hash) = address_fixture_head(database).await?;
    let mut upgrade = address_fixture_event(
        "address-budget-resolver-upgrade",
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        block,
        &hash,
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
        json!({"source_event":"Upgraded", "proxy_address":ADDRESS_BUDGET_RESOLVER,
            "implementation":implementation}),
    );
    upgrade.source_manifest_id = Some(manifest);
    upgrade.manifest_version = 1;
    upgrade.raw_fact_ref["emitting_address"] = json!(ADDRESS_BUDGET_RESOLVER);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[upgrade]).await?;
    Ok(manifest)
}

/// One subject's roles on the budget resolver for `resource`, granted or revoked.
fn address_budget_role_event(
    resource: Uuid,
    subject: &str,
    granted: bool,
    manifest: i64,
    block: i64,
    hash: &str,
) -> NormalizedEvent {
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let node = format!("0x{:064x}", resource.as_u128());
    let powers = if granted { json!(["set_addr"]) } else { json!([]) };
    let source = json!({"kind":"raw_log", "source_event":"EACRolesChanged",
        "upstream_resource":node, "root_resource":false, "changed_powers":["set_addr"]});
    let mut event = address_fixture_event(
        &format!("address-budget-role-{resource}-{ordinal}"),
        None,
        Some(resource),
        "PermissionChanged",
        "ens_v2_resolver_l1",
        block,
        hash,
        ordinal,
        json!({"subject":subject, "scope":{"kind":"resolver", "chain_id":"ethereum-mainnet",
                "resolver_address":ADDRESS_BUDGET_RESOLVER},
            "effective_powers":powers, "source_event":"EACRolesChanged", "upstream_resource":node,
            "resource":node, "root_resource":false, "storage_model":"resolver_record_id",
            "resolver":ADDRESS_BUDGET_RESOLVER, "resolver_record_id":"0", "record_key":"permission",
            "grant_source":if granted { source.clone() } else { Value::Null },
            "revocation_source":if granted { Value::Null } else { source },
            "inheritance_path":[], "transfer_behavior":{}}),
    );
    event.source_manifest_id = Some(manifest);
    event.manifest_version = 1;
    event.raw_fact_ref["emitting_address"] = json!(ADDRESS_BUDGET_RESOLVER);
    event
}

/// Grant or revoke resolver roles so `resource` has exactly `count` grants in total, then
/// publish them.
async fn seed_address_name_budget_grants(
    database: &TestDatabase,
    resource: Uuid,
    count: usize,
) -> Result<()> {
    let manifest = address_budget_resolver_manifest(database).await?;
    let rows = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
        &database.pool,
        &[resource],
        None,
        100_000,
    )
    .await?;
    let active = rows
        .iter()
        .filter(|row| row.subject.starts_with(&address_budget_subject(0)[..35]))
        .count();
    let others = rows.len() - active;
    anyhow::ensure!(count >= others, "{resource} already has {others} other grants");
    let wanted = count - others;
    let (block, hash) = address_fixture_head(database).await?;
    let events = (wanted.min(active)..wanted.max(active))
        .map(|index| {
            let subject = address_budget_subject(index + 1);
            address_budget_role_event(resource, &subject, wanted > active, manifest, block, &hash)
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(database).await
}

#[tokio::test]
async fn v2_address_names_grant_budget_boundaries_and_single_resource_recovery() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let id = Uuid::from_u128(0xa100);
    for count in [999, 1000, 1001] {
        seed_address_name_budget_grants(&database, id, count).await?;
        let uri = format!("/v1/addresses/{V2_ADDRESS}/names?q=alpha&page_size=200");
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

// The inline budget sums grants over every returned row.
#[tokio::test]
async fn v2_address_names_grant_budget_counts_grants_across_returned_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let (first, second) = (Uuid::from_u128(0xd100), Uuid::from_u128(0xd200));
    seed_address_name_budget_grants(&database, first, 500).await?;
    for count in [500, 501] {
        seed_address_name_budget_grants(&database, second, count).await?;
        let uri = format!("/v1/addresses/{V2_ADDRESS}/names?q=shared&include=role_summary");
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
        let one = v2_address_names_payload_for_database(
            &database,
            &format!("/v1/addresses/{V2_ADDRESS}/names?q=shared-two&include=role_summary"),
        )
        .await?;
        assert_eq!(one["data"].as_array().unwrap().len(), 1);
        assert_eq!(address_name_inline_grants(&one["data"][0]).len(), count);
        assert_eq!(one["data"][0]["permission_resource_id"], json!(second.to_string()));
    }
    database.cleanup().await
}

// A wrapped `.eth` name serves its BaseRegistrar lease as `permission_resource_id`, the same
// handle `registration_id` serves everywhere else, and the permissions route resolves that handle
// to the NameWrapper resource's rows, with and without `address`. The NameWrapper resource itself
// is not a public registration handle.
#[tokio::test]
async fn v2_address_names_wrapped_name_permission_resource_id_is_the_registrar_lease()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_address_registry_operator(&database).await?;
    let lease_resource_id = Uuid::from_u128(0xa100);
    let wrapper_resource_id = wrap_address_name(&database, "alpha.eth", 0xa300, None).await?;
    let lease = lease_resource_id.to_string();

    for namespace in ["", "&namespace=ens"] {
        let uri = format!("/v1/addresses/{V2_ADDRESS}/names?q=alpha{namespace}");
        let plain = v2_address_names_payload_for_database(&database, &uri).await?;
        let included = v2_address_names_payload_for_database(
            &database,
            &format!("{uri}&include=role_summary"),
        )
        .await?;
        for payload in [&plain, &included] {
            assert_eq!(payload["data"][0]["name"], json!("alpha.eth"));
            assert_eq!(
                payload["data"][0]["permission_resource_id"],
                json!(lease),
                "{namespace:?}: {}",
                payload["data"][0]
            );
        }
        let expected = address_name_inline_grants(&included["data"][0]);
        assert!(!expected.is_empty());
        assert_eq!(
            expected,
            address_name_permission_grants(&database, &lease, namespace).await?,
            "{namespace:?}"
        );
        assert!(
            address_name_permission_grants(&database, &wrapper_resource_id.to_string(), namespace)
                .await?
                .is_empty(),
            "{namespace:?}: the NameWrapper resource must not be a public registration handle"
        );

        let summaries = included["data"][0]["role_summary"]
            .as_array()
            .expect("role summary");
        for summary in summaries {
            let subject = summary["address"].as_str().expect("subject");
            let payload = v2_permissions_payload_for_database(
                &database,
                &format!("/v1/permissions?registration_id={lease}&address={subject}{namespace}"),
            )
            .await?;
            let rows = payload["data"].as_array().expect("permissions data");
            assert_eq!(
                rows.len(),
                summary["grants"].as_array().expect("grants").len(),
                "{namespace:?} {subject}: {rows:?}"
            );
            assert!(
                rows.iter().all(|row| {
                    row["address"] == json!(subject) && row["registration_id"] == json!(lease)
                }),
                "{namespace:?} {subject}: {rows:?}"
            );
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_permission_id_does_not_resolve_name_again() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let selected = Uuid::from_u128(0xa100);
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary"),
    )
    .await?;
    assert_eq!(
        payload["data"][0]["permission_resource_id"],
        json!(selected.to_string())
    );
    let grants = address_name_inline_grants(&payload["data"][0]);
    assert!(!grants.is_empty());

    // Advance the name to a new registration after the caller captured its permission ID.
    // The old binding ends where the new one starts; its grants remain an ID-only audit.
    let replacement = (Uuid::from_u128(0xe100), Uuid::from_u128(0xe101), Uuid::from_u128(0xe102));
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-mainnet", "block_number":200, "block_hash":"0xnamec8",
            "timestamp":"2024-06-01T00:00:00Z"
        }}))
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings SET active_to=$1 WHERE surface_binding_id=$2",
    )
    .bind(parse_rfc3339_utc_timestamp("2024-06-01T00:00:00Z").map_err(|e| anyhow::anyhow!("{e}"))?)
    .bind(Uuid::from_u128(0xa102))
    .execute(&database.pool)
    .await?;
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        "alpha.eth",
        "ethereum-mainnet",
        200,
        "0xnamec8",
        replacement.0,
        replacement.1,
        replacement.2,
        "ens_v1",
    )
    .await?;
    let registration = address_fixture_event(
        "address-alpha-replacement-grant",
        Some(&logical),
        Some(replacement.0),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        200,
        "0xnamec8",
        0,
        json!({"authority_kind":"registrar", "registrant":V2_ADDRESS, "expiry":1_900_000_000_i64}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[registration]).await?;
    rebuild_address_fixture(&database).await?;
    let (logical_name_id, _) = phase_logical_identity("ens", "alpha.eth")?;
    let current = bigname_storage::load_name_current(&database.pool, &logical_name_id)
        .await?
        .expect("the replacement name must be readable");
    assert_eq!(current.resource_id, Some(replacement.0));
    assert_eq!(current.surface_binding_id, Some(replacement.2));
    assert_eq!(current.token_lineage_id, Some(replacement.1));
    let named =
        v2_permissions_payload_for_database(&database, "/v1/permissions?name=alpha.eth").await?;
    assert_eq!(named["data"], json!([]));
    assert_eq!(
        grants,
        address_name_permission_grants(&database, &selected.to_string(), "").await?
    );
    database.cleanup().await
}

// Every name on a 200-row page has the same registry owner, so each approved registry operator
// of that owner is a grant on every row.
#[tokio::test]
async fn v2_address_names_grant_budget_maximum_page_operators() -> Result<()> {
    const OPERATOR_OWNER: &str = "0x0000000000000000000000000000000000000a11";
    let database = TestDatabase::new_migrated().await?;
    let mut specs = v2_address_name_specs();
    specs.truncate(1);
    specs[0].owner = OPERATOR_OWNER;
    for index in 1..200_u128 {
        specs.push(V2AddressNameSpec {
            logical_name_id: Box::leak(format!("ens:budget{index}.eth").into_boxed_str()),
            name: Box::leak(format!("budget{index}.eth").into_boxed_str()),
            resource_id: Uuid::from_u128(0x900000 + index * 3),
            token_lineage_id: Uuid::from_u128(0x900001 + index * 3),
            surface_binding_id: Uuid::from_u128(0x900002 + index * 3),
            block_hash: Box::leak(format!("0xname{:x}", 1000 + index).into_boxed_str()),
            block_number: (1000 + index) as i64,
            owner: OPERATOR_OWNER,
            registrant: V2_ADDRESS,
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        });
    }
    seed_v2_address_name_identities(&database, &specs).await?;
    publish_v2_address_name_inputs(&database, &specs).await?;
    assert_v2_address_name_relations(&database, &specs).await?;
    // Five approved operators of the owner plus 1,100 revoked ones. Eligibility must precede
    // both branch limits, and each approved operator applies to all 200 names.
    let (block, hash) = address_fixture_head(&database).await?;
    let approvals = (1..=1105_usize)
        .map(|index| address_operator_approval(&address_budget_subject(index), OPERATOR_OWNER, index <= 5, block, &hash))
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &approvals).await?;
    rebuild_address_fixture(&database).await?;
    let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size=200");
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
    let alpha = &specs[0];
    let grant = address_owner_grant(alpha, json!({"kind":"resource"}), "resource_control", block, &hash)?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_address_fixture(&database).await?;
    let over =
        v2_address_names_response_for_database(&database, &format!("{uri}&include=role_summary"))
            .await?;
    assert_eq!(over.status(), StatusCode::UNPROCESSABLE_ENTITY);
    // Pure operator overflow is independently rejected: the direct grant is revoked and a sixth
    // operator approved.
    let mut revoke = address_owner_grant(alpha, json!({"kind":"resource"}), "resource_control", block, &hash)?;
    revoke.after_state["revocation_source"] = revoke.after_state["grant_source"].take();
    revoke.after_state["effective_powers"] = json!([]);
    let sixth = address_operator_approval(&address_budget_subject(6), OPERATOR_OWNER, true, block, &hash);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[revoke, sixth]).await?;
    rebuild_address_fixture(&database).await?;
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

// Revoked grants stay behind as rows with no powers; the inline limit counts only live ones.
#[tokio::test]
async fn v2_address_names_grant_budget_filters_ineligible_direct_rows_before_limit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let id = Uuid::from_u128(0xa100);
    seed_address_name_budget_grants(&database, id, 2100).await?;
    seed_address_name_budget_grants(&database, id, 1000).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=alpha&include=role_summary&namespace=ens"),
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
