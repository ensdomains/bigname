fn generated_type_ref(value: &Value) -> String {
    match value["kind"].as_str().expect("introspection type kind") {
        "NON_NULL" => format!("{}!", generated_type_ref(&value["ofType"])),
        "LIST" => format!("[{}]", generated_type_ref(&value["ofType"])),
        _ => value["name"]
            .as_str()
            .expect("introspection named type")
            .to_owned(),
    }
}

fn generated_field<'a>(type_value: &'a Value, name: &str) -> &'a Value {
    type_value["fields"]
        .as_array()
        .expect("introspection fields")
        .iter()
        .find(|field| field["name"] == name)
        .unwrap_or_else(|| panic!("missing GraphQL field {name}"))
}

#[tokio::test]
async fn graphql_generated_domain_root_signature_matches_pinned_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let payload = post_graphql(
        database.app_state(),
        r#"query {
          queryType: __type(name: "Query") { fields { name args { name defaultValue type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
          domainType: __type(name: "Domain") { fields { name type { kind name ofType { kind name } } } }
        }"#,
        json!({}),
    )
    .await?;
    let query = &payload["data"]["queryType"];
    for (name, expected_args, expected_return) in [
        (
            "domain",
            vec![
                ("id", "ID!", None),
                ("block", "Block_height", None),
                ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny")),
            ],
            "Domain",
        ),
        (
            "domains",
            vec![
                ("skip", "Int", Some("0")),
                ("first", "Int", Some("100")),
                ("orderBy", "Domain_orderBy", None),
                ("orderDirection", "OrderDirection", None),
                ("where", "Domain_filter", None),
                ("block", "Block_height", None),
                ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny")),
            ],
            "[Domain!]!",
        ),
    ] {
        let field = generated_field(query, name);
        let actual_args = field["args"]
            .as_array()
            .expect("field args")
            .iter()
            .map(|arg| {
                (
                    arg["name"].as_str().unwrap(),
                    generated_type_ref(&arg["type"]),
                    arg["defaultValue"].as_str(),
                )
            })
            .collect::<Vec<_>>();
        let expected_args = expected_args
            .into_iter()
            .map(|(arg, ty, default)| (arg, ty.to_owned(), default))
            .collect::<Vec<_>>();
        assert_eq!(actual_args, expected_args, "{name} argument signature");
        assert_eq!(generated_type_ref(&field["type"]), expected_return);
    }
    let id = generated_field(&payload["data"]["domainType"], "id");
    assert_eq!(generated_type_ref(&id["type"]), "ID!");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_filter_has_only_the_t2_members() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let payload = post_graphql(
        database.app_state(),
        r#"query { __type(name: "Domain_filter") { inputFields { name } } }"#,
        json!({}),
    )
    .await?;
    let mut actual = payload["data"]["__type"]["inputFields"]
        .as_array()
        .context("Domain_filter input fields")?
        .iter()
        .filter_map(|field| field["name"].as_str())
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(
        actual,
        ["id", "id_in", "name", "name_contains", "owner", "owner_in"]
    );
    database.cleanup().await
}

async fn generated_domain_names(database: &TestDatabase, where_value: Value) -> Result<Vec<String>> {
    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) { domains(where: $where, orderBy: name) { name } }"#,
        json!({"where": where_value}),
    )
    .await?;
    Ok(payload["data"]["domains"]
        .as_array()
        .context("domains array")?
        .iter()
        .filter_map(|row| row["name"].as_str().map(str::to_owned))
        .collect())
}

async fn generated_domain_owner_rows(
    database: &TestDatabase,
    where_value: Value,
) -> Result<Vec<(String, String)>> {
    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where, orderBy: name) { name owner { id } }
        }"#,
        json!({"where": where_value}),
    )
    .await?;
    payload["data"]["domains"]
        .as_array()
        .context("domains array")?
        .iter()
        .map(|row| {
            Ok((
                row["name"].as_str().context("domain name")?.to_owned(),
                row["owner"]["id"]
                    .as_str()
                    .context("domain owner id")?
                    .to_owned(),
            ))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn seed_generated_owner_shape(
    database: &TestDatabase,
    name: &str,
    registry_owner: &str,
    token_holder: &str,
    tokenized: bool,
    wrapped: bool,
    id_base: u128,
    block_number: i64,
) -> Result<()> {
    let namehash = bigname_lookup::ens_namehash_hex(name)?;
    let resource_id = Uuid::from_u128(id_base);
    let token_lineage_id = Uuid::from_u128(id_base + 1);
    let surface_binding_id = Uuid::from_u128(id_base + 2);
    seed_identity_name(
        database,
        &format!("ens:{name}"),
        name,
        name,
        &namehash,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        token_holder,
        bigname_storage::AddressNameRelation::TokenHolder,
        block_number,
    )
    .await?;

    let mut summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar",
            "registrant": token_holder,
            "expiry": 1_900_000_000_i64,
            "created_at": 1_700_000_000_i64,
        },
        "control": {
            "registry_owner": registry_owner,
            "registrant": token_holder,
            "expiry": 1_900_000_000_i64,
        }
    });
    if wrapped {
        summary["wrapper_state"] = json!("wrapped");
    }
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = $1,
             token_lineage_id = CASE WHEN $2 THEN token_lineage_id ELSE NULL END
         WHERE raw_name = $3",
    )
    .bind(summary)
    .bind(tokenized)
    .bind(name)
    .execute(&database.lookup_pool)
    .await?;
    if !tokenized {
        sqlx::query("DELETE FROM bigname_phase.address_names_current WHERE raw_name = $1")
            .bind(name)
            .execute(&database.lookup_pool)
            .await?;
    }
    upsert_phase_address_names_current_rows(
        &database.lookup_pool,
        &[address_name_current_row(
            registry_owner,
            &format!("ens:{name}"),
            bigname_storage::AddressNameRelation::EffectiveController,
            name,
            name,
            &namehash,
            surface_binding_id,
            resource_id,
            tokenized.then_some(token_lineage_id),
            block_number,
        )],
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn graphql_generated_domains_default_to_first_100_ids_from_zero() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let mut expected = vec![
        GRAPHQL_ALICE_NAMEHASH.to_owned(),
        GRAPHQL_BOB_NAMEHASH.to_owned(),
        GRAPHQL_CAROL_NAMEHASH.to_owned(),
        GRAPHQL_DAVE_NAMEHASH.to_owned(),
    ];
    for index in 0..99_u128 {
        let name = format!("generated-{index:03}.eth");
        let namehash = bigname_lookup::ens_namehash_hex(&name)?;
        expected.push(namehash.clone());
        seed_identity_name(
            &database,
            &format!("ens:{name}"),
            &name,
            &name,
            &namehash,
            Uuid::from_u128(0x670_1000 + index * 3),
            Uuid::from_u128(0x670_1001 + index * 3),
            Uuid::from_u128(0x670_1002 + index * 3),
            GRAPHQL_OWNER,
            bigname_storage::AddressNameRelation::TokenHolder,
            500 + index as i64,
        )
        .await?;
    }
    expected.sort();
    expected.truncate(100);
    let payload = post_graphql(database.app_state(), "query { domains { id } }", json!({})).await?;
    let actual = payload["data"]["domains"]
        .as_array()
        .context("domains array")?
        .iter()
        .filter_map(|row| row["id"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domains_apply_id_and_current_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    for (filter, expected) in [
        (json!({"id": GRAPHQL_ALICE_NAMEHASH}), vec!["alice.eth"]),
        (json!({"id_in": [GRAPHQL_BOB_NAMEHASH, GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH]}), vec!["alice.eth", "bob.eth"]),
        (json!({"id": GRAPHQL_ALICE_NAMEHASH, "id_in": [GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH]}), vec!["alice.eth"]),
        (json!({"id": GRAPHQL_ALICE_NAMEHASH, "id_in": [GRAPHQL_BOB_NAMEHASH]}), vec![]),
        (json!({"id_in": []}), vec![]),
        (json!({"name": "alice.eth"}), vec!["alice.eth"]),
        (json!({"name_contains": "lic"}), vec!["alice.eth"]),
        (json!({"owner": GRAPHQL_OWNER}), vec!["alice.eth", "bob.eth"]),
        (json!({"owner_in": [GRAPHQL_OWNER]}), vec!["alice.eth", "bob.eth"]),
        (json!({"owner_in": []}), vec![]),
        (json!({"owner": GRAPHQL_OWNER, "owner_in": [GRAPHQL_FALLBACK_HOLDER]}), vec![]),
        (json!({"owner": GRAPHQL_OWNER, "name": "alice.eth"}), vec!["alice.eth"]),
    ] {
        assert_eq!(generated_domain_names(&database, filter).await?, expected);
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_owner_filters_match_the_served_owner() -> Result<()> {
    const SUBNAME_OWNER: &str = "0x0000000000000000000000000000000000000671";
    const SECOND_LEVEL_OWNER: &str = "0x0000000000000000000000000000000000000672";
    const SECOND_LEVEL_HOLDER: &str = "0x0000000000000000000000000000000000000673";
    const NAME_WRAPPER: &str = "0x0000000000000000000000000000000000000674";
    const WRAPPED_HOLDER: &str = "0x0000000000000000000000000000000000000675";
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    for (name, owner, holder, tokenized, wrapped, id_base, block_number) in [
        (
            "owner-sub.parent.eth",
            SUBNAME_OWNER,
            SUBNAME_OWNER,
            false,
            false,
            0x670_3001,
            710,
        ),
        (
            "owner-second-level.eth",
            SECOND_LEVEL_OWNER,
            SECOND_LEVEL_HOLDER,
            true,
            false,
            0x670_3011,
            711,
        ),
        (
            "owner-wrapped.eth",
            NAME_WRAPPER,
            WRAPPED_HOLDER,
            true,
            true,
            0x670_3021,
            712,
        ),
    ] {
        seed_generated_owner_shape(
            &database,
            name,
            owner,
            holder,
            tokenized,
            wrapped,
            id_base,
            block_number,
        )
        .await?;
    }

    let served_owner_matches = [
        (SUBNAME_OWNER, "owner-sub.parent.eth"),
        (SECOND_LEVEL_OWNER, "owner-second-level.eth"),
        (NAME_WRAPPER, "owner-wrapped.eth"),
    ];
    let mut actual = Vec::new();
    for (owner, _) in served_owner_matches {
        actual.push((
            owner,
            generated_domain_owner_rows(&database, json!({"owner": owner})).await?,
        ));
    }
    let mut actual_holders = Vec::new();
    for holder in [SECOND_LEVEL_HOLDER, WRAPPED_HOLDER] {
        actual_holders.push((
            holder,
            generated_domain_owner_rows(&database, json!({"owner": holder})).await?,
        ));
    }
    let owner_in = generated_domain_owner_rows(
        &database,
        json!({"owner_in": [SUBNAME_OWNER, SECOND_LEVEL_OWNER, NAME_WRAPPER]}),
    )
    .await?;

    assert_eq!(
        (actual, actual_holders, owner_in),
        (
            vec![
                (SUBNAME_OWNER, vec![("owner-sub.parent.eth".into(), SUBNAME_OWNER.into())]),
                (SECOND_LEVEL_OWNER, vec![("owner-second-level.eth".into(), SECOND_LEVEL_OWNER.into())]),
                (NAME_WRAPPER, vec![("owner-wrapped.eth".into(), NAME_WRAPPER.into())]),
            ],
            vec![(SECOND_LEVEL_HOLDER, vec![]), (WRAPPED_HOLDER, vec![])],
            vec![
                ("owner-second-level.eth".into(), SECOND_LEVEL_OWNER.into()),
                ("owner-sub.parent.eth".into(), SUBNAME_OWNER.into()),
                ("owner-wrapped.eth".into(), NAME_WRAPPER.into()),
            ],
        )
    );
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domains_apply_skip_first_and_direction() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let mut ids = [GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH];
    ids.sort_unstable();
    for (direction, skip, expected) in [
        ("asc", 1, ids[1]),
        ("desc", 0, ids[1]),
        ("desc", 1, ids[0]),
    ] {
        let payload = post_graphql(
            database.app_state(),
            "query Page($skip: Int!, $direction: OrderDirection!, $where: Domain_filter!) { domains(first: 1, skip: $skip, orderBy: id, orderDirection: $direction, where: $where) { id } }",
            json!({"skip": skip, "direction": direction, "where": {"owner": GRAPHQL_OWNER}}),
        )
        .await?;
        assert_eq!(payload["data"]["domains"][0]["id"], json!(expected));
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_name_fallback_cannot_shadow_namehash() -> Result<()> {
    const TARGET_NAME: &str = "namehash-target.eth";
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let target_namehash = bigname_lookup::ens_namehash_hex(TARGET_NAME)?;
    let normalized = bigname_domain::normalization::normalize_name(&target_namehash)?;
    assert_eq!(normalized.normalized_name, target_namehash);
    let hash_shaped_namehash = bigname_lookup::ens_namehash_hex(&target_namehash)?;
    assert_ne!(hash_shaped_namehash, target_namehash);
    seed_identity_name(
        &database,
        &format!("ens:{target_namehash}"),
        &target_namehash,
        &target_namehash,
        &hash_shaped_namehash,
        Uuid::from_u128(0x670_2001),
        Uuid::from_u128(0x670_2002),
        Uuid::from_u128(0x670_2003),
        GRAPHQL_OWNER,
        bigname_storage::AddressNameRelation::TokenHolder,
        700,
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:namehash-target.eth",
        TARGET_NAME,
        TARGET_NAME,
        &target_namehash,
        Uuid::from_u128(0x670_2011),
        Uuid::from_u128(0x670_2012),
        Uuid::from_u128(0x670_2013),
        GRAPHQL_OWNER,
        bigname_storage::AddressNameRelation::TokenHolder,
        701,
    )
    .await?;
    let payload = post_graphql(
        database.app_state(),
        "query Domain($id: ID!) { domain(id: $id) { name } }",
        json!({"id": target_namehash}),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["name"], json!(TARGET_NAME));
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domains_reject_t3_filter_members() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    for (query, variables, member) in [
        ("query { domains(where: { id_not: \"0x00\" }) { id } }", json!({}), "id_not"),
        ("query Domains($where: Domain_filter!) { domains(where: $where) { id } }", json!({"where": {"owner_contains": "0x"}}), "owner_contains"),
    ] {
        let payload = post_graphql_allow_errors(database.app_state(), query, variables).await?;
        let error = payload["errors"][0]["message"].as_str().context("validation error")?;
        assert!(error.contains("Domain_filter") && error.contains(member), "{error}");
        assert!(payload.get("data").is_none() || payload["data"].is_null());
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_roots_enforce_current_snapshot_blocks() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let (old_number, old_hash): (i64, String) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM bigname_phase.chain_phase_state WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .fetch_one(&database.lookup_pool)
    .await?;
    let number = old_number + 1;
    let hash = format!("0x{}", "22".repeat(32));
    sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state) VALUES ('ethereum-mainnet', $1, $2, $3, now(), 'finalized')")
        .bind(&hash)
        .bind(&old_hash)
        .bind(number)
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query("UPDATE bigname_phase.chain_heads SET latest_block_hash = $1, latest_block_number = $2, safe_block_hash = $1, safe_block_number = $2, finalized_block_hash = $1, finalized_block_number = $2 WHERE chain_id = 'ethereum-mainnet'")
        .bind(&hash)
        .bind(number)
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query("UPDATE bigname_phase.chain_phase_state SET current_block_hash = $1, current_block_number = $2, target_block_hash = $1, target_block_number = $2 WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'")
        .bind(&hash)
        .bind(number)
        .execute(&database.lookup_pool)
        .await?;
    let wrong_hash = format!("0x{}", "ff".repeat(32));
    for block in [
        format!("{{ number: {number} }}"),
        format!("{{ hash: \"{hash}\" }}"),
        format!("{{ number_gte: {} }}", number - 1),
    ] {
        for root in ["domain(id: \"alice.eth\"", "domains("] {
            let payload = post_graphql_allow_errors(
                database.app_state(),
                &format!("query {{ {root} block: {block}) {{ id }} }}"),
                json!({}),
            )
            .await?;
            assert!(payload.get("errors").is_none(), "{payload}");
        }
    }
    for (block, message) in [
        (format!("{{ number: {} }}", number - 1), "the requested block number is not the served head"),
        (format!("{{ hash: \"{wrong_hash}\" }}"), "the requested block hash is not the served head"),
        (format!("{{ number_gte: {} }}", number + 1), "the served head has not reached block.number_gte"),
        ("{}".into(), "block must contain hash, number, or number_gte"),
        ("{ hash: null }".into(), "block.hash must not be null"),
        ("{ number: null }".into(), "block.number must not be null"),
        ("{ number_gte: null }".into(), "block.number_gte must not be null"),
        ("{ number: -1 }".into(), "block number constraints must be non-negative"),
        ("{ number_gte: -1 }".into(), "block number constraints must be non-negative"),
    ] {
        for (field, root) in [("domain", "domain(id: \"alice.eth\""), ("domains", "domains(")] {
            let payload = post_graphql_allow_errors(
                database.app_state(),
                &format!("query {{ {root} block: {block}) {{ id }} }}"),
                json!({}),
            )
            .await?;
            assert_eq!(payload["errors"][0]["message"], json!(message));
            assert_eq!(payload["errors"][0]["path"], json!([field]));
            if message == "the requested block number is not the served head" {
                println!("{field} refusal: {payload}");
            }
            if field == "domain" {
                assert_eq!(payload["data"], json!({"domain": null}));
            } else {
                assert_eq!(payload["data"], Value::Null);
            }
        }
    }
    database.cleanup().await
}
