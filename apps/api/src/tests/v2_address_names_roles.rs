// The `role_holder` address-name relation: names on which an address holds an ENSv2 registry
// role on the name's own registration (TYR-70). The fixture's beta.eth moves to an ENSv2 registration registered to V2_ADDRESS;
// ROLE_HOLDER holds roles on that registration only.

const ROLE_HOLDER: &str = "0x3ed205a5ad7cc1545aea8fae0113df3026d9a861";
const ROLE_REGISTRY: &str = "0x0000000000000000000000000000000000000b2e";
const ROLE_RESOURCE: u128 = 0xb200;

/// An ENSv2 registry `EACRolesChanged` as the registry adapter stores it: `subject` holds
/// exactly `powers` on the registration `resource` (or on the registry root with `root`).
fn address_role_event(
    name: Option<&str>,
    resource: Uuid,
    subject: &str,
    root: bool,
    powers: Value,
    block: i64,
    hash: &str,
) -> NormalizedEvent {
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let upstream = format!("0x{:064x}", if root { 0 } else { 0x100000001_u64 });
    let source = json!({"kind":"raw_log", "source_event":"EACRolesChanged",
        "upstream_resource":upstream, "root_resource":root, "changed_powers":powers});
    let revoked = powers.as_array().is_some_and(Vec::is_empty);
    let mut event = address_fixture_event(
        &format!("address-role-{resource}-{subject}-{ordinal}"),
        name,
        Some(resource),
        if root { "RootPermissionChanged" } else { "PermissionChanged" },
        "ens_v2_registry_l1",
        block,
        hash,
        ordinal,
        json!({"subject":subject,
            "scope":{"kind":if root { "registry_root" } else { "registry" },
                "chain_id":"ethereum-mainnet", "registry_address":ROLE_REGISTRY},
            "effective_powers":powers, "source_event":"EACRolesChanged",
            "upstream_resource":upstream, "resource":upstream, "root_resource":root,
            "grant_source":if revoked { json!({}) } else { source.clone() },
            "revocation_source":if revoked { source } else { Value::Null },
            "inheritance_path":[], "transfer_behavior":{}}),
    );
    event.raw_fact_ref["emitting_address"] = json!(ROLE_REGISTRY);
    event
}

/// beta.eth on an ENSv2 registration, with ROLE_HOLDER's roles on it set to `powers`.
async fn seed_role_holder(database: &TestDatabase, powers: Value) -> Result<Uuid> {
    seed_v2_address_names_fixture(database).await?;
    let resource = bind_address_name_ens_v2(database, "beta.eth", ROLE_RESOURCE, false).await?;
    set_role_holder_powers(database, resource, powers).await?;
    Ok(resource)
}

async fn set_role_holder_powers(database: &TestDatabase, resource: Uuid, powers: Value) -> Result<()> {
    let (block, hash) = address_fixture_head(database).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let event = address_role_event(Some(&name), resource, ROLE_HOLDER, false, powers, block, &hash);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_address_fixture(database).await
}

async fn role_holder_names(database: &TestDatabase, relation: &str) -> Result<Value> {
    v2_address_names_payload_for_database(
        database,
        &format!("/v1/addresses/{ROLE_HOLDER}/names?relation={relation}"),
    )
    .await
}

fn row_names_and_relations(payload: &Value) -> Vec<(String, Value)> {
    payload["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (row["name"].as_str().unwrap().to_owned(), row["relations"].clone()))
        .collect()
}

#[tokio::test]
async fn v2_address_names_list_a_name_whose_registry_role_the_address_holds() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["set_resolver", "set_subregistry"])).await?;

    // The permissions route serves the grant.
    let permissions = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v1/permissions?registration_id={}&address={ROLE_HOLDER}",
            Uuid::from_u128(ROLE_RESOURCE)
        ),
    )
    .await?;
    assert_eq!(
        permissions["data"][0]["powers"],
        json!(["set_resolver", "set_subregistry"]),
        "{permissions}"
    );

    for relation in ["role_holder", "any", "manager,role_holder"] {
        let payload = role_holder_names(&database, relation).await?;
        assert_eq!(
            row_names_and_relations(&payload),
            vec![("beta.eth".to_owned(), json!(["role_holder"]))],
            "relation={relation}: {payload}"
        );
    }
    // Holding a role does not make the address the manager, owner or registrant.
    for relation in ["manager", "owner", "registrant", "owner,manager,registrant"] {
        let payload = role_holder_names(&database, relation).await?;
        assert_eq!(payload["data"], json!([]), "relation={relation}: {payload}");
    }
    let unfiltered = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{ROLE_HOLDER}/names"),
    )
    .await?;
    assert_eq!(
        row_names_and_relations(&unfiltered),
        vec![("beta.eth".to_owned(), json!(["role_holder"]))]
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_list_any_registry_role_but_not_the_reservation_marker() -> Result<()> {
    for powers in [
        json!(["renew"]),
        json!(["unregister"]),
        json!(["admin_set_resolver"]),
        json!(["can_transfer_admin"]),
        json!(["was_reserved", "renew"]),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_role_holder(&database, powers.clone()).await?;
        assert_eq!(
            row_names_and_relations(&role_holder_names(&database, "role_holder").await?),
            vec![("beta.eth".to_owned(), json!(["role_holder"]))],
            "{powers}"
        );
        database.cleanup().await?;
    }
    // `was_reserved` marks a registration made from a reservation; it is not a role.
    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["was_reserved"])).await?;
    assert_eq!(role_holder_names(&database, "any").await?["data"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_drop_a_role_holder_once_every_role_is_revoked() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = seed_role_holder(&database, json!(["renew"])).await?;
    set_role_holder_powers(&database, resource, json!(["renew", "set_resolver"])).await?;
    assert_eq!(
        row_names_and_relations(&role_holder_names(&database, "role_holder").await?),
        vec![("beta.eth".to_owned(), json!(["role_holder"]))]
    );
    set_role_holder_powers(&database, resource, json!([])).await?;
    assert_eq!(role_holder_names(&database, "any").await?["data"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_omit_registry_root_role_holders() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!([])).await?;
    // A registry-root role reaches every name of the registry. It is served on each name's
    // permission rows but does not add the registry's names to the holder's list.
    let (block, hash) = address_fixture_head(&database).await?;
    let root_resource = Uuid::from_u128(0xb2f0);
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
         VALUES ($1, 'ethereum-mainnet', $2, $3, 'canonical')",
    )
    .bind(root_resource)
    .bind(block)
    .bind(&hash)
    .execute(&database.pool)
    .await?;
    let root = address_role_event(
        None,
        root_resource,
        ROLE_HOLDER,
        true,
        json!(["set_resolver", "set_subregistry"]),
        block,
        &hash,
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[root]).await?;
    rebuild_address_fixture(&database).await?;
    assert_eq!(role_holder_names(&database, "any").await?["data"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_add_role_holder_to_an_owner_that_holds_a_role() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!([])).await?;
    // The registrant also holds a role: one row, with every relation it matched.
    let (block, hash) = address_fixture_head(&database).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let event = address_role_event(
        Some(&name),
        Uuid::from_u128(ROLE_RESOURCE),
        V2_ADDRESS,
        false,
        json!(["set_resolver"]),
        block,
        &hash,
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_address_fixture(&database).await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta"),
    )
    .await?;
    assert_eq!(
        row_names_and_relations(&payload),
        vec![(
            "beta.eth".to_owned(),
            json!(["registrant", "owner", "manager", "role_holder"])
        )],
        "{payload}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_reverse_keeps_its_three_relations() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["set_resolver"])).await?;
    let payload = v2_lookup_json(
        &database,
        json!({"inputs": [
            {"id": "manager", "address": ROLE_HOLDER, "relation": "manager"},
            {"id": "any", "address": ROLE_HOLDER, "relation": "any"}
        ]}),
    )
    .await?;
    for index in [0, 1] {
        assert_eq!(payload["data"][index]["records"], json!([]), "{payload}");
    }
    assert_eq!(
        payload["data"][1]["input"]["relation"],
        json!("owner,manager,registrant")
    );
    let rejected = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"inputs": [
                        {"address": ROLE_HOLDER, "relation": "role_holder"}
                    ]})
                    .to_string(),
                ))
                .expect("request must build"),
        )
        .await?;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_history_follows_a_role_held_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["set_resolver"])).await?;
    let history = |relation: &'static str| {
        let database = &database;
        async move {
            v2_address_names_payload_for_database(
                database,
                &format!("/v1/addresses/{ROLE_HOLDER}/history?relation={relation}"),
            )
            .await
        }
    };
    let payload = history("role_holder").await?;
    let rows = payload["data"].as_array().unwrap();
    assert!(rows.iter().all(|row| row["name"] == json!("beta.eth")), "{payload}");
    assert!(
        rows.iter().any(|row| row["type"] == json!("permission")),
        "the role grant itself is in the holder's history: {payload}"
    );
    assert_eq!(history("any").await?["data"], payload["data"]);
    assert_eq!(history("manager").await?["data"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_list_one_holder_among_many_on_a_registration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = seed_role_holder(&database, json!(["renew"])).await?;
    // Sixty other accounts hold roles on the same registration.
    let (block, hash) = address_fixture_head(&database).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let others: Vec<String> = (1..=60).map(|n| format!("0x{n:040x}")).collect();
    let events: Vec<_> = others
        .iter()
        .map(|other| {
            address_role_event(Some(&name), resource, other, false, json!(["set_resolver"]), block, &hash)
        })
        .collect();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(&database).await?;
    for holder in [ROLE_HOLDER, others[0].as_str(), others[59].as_str()] {
        let payload = v2_address_names_payload_for_database(
            &database,
            &format!("/v1/addresses/{holder}/names?relation=any"),
        )
        .await?;
        assert_eq!(
            row_names_and_relations(&payload),
            vec![("beta.eth".to_owned(), json!(["role_holder"]))],
            "{holder}: {payload}"
        );
    }
    // The owner's row is unchanged by the other holders.
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?q=beta"),
    )
    .await?;
    assert_eq!(
        row_names_and_relations(&payload),
        vec![("beta.eth".to_owned(), json!(["registrant", "owner", "manager"]))],
        "{payload}"
    );
    database.cleanup().await
}
