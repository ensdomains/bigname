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

#[tokio::test]
async fn graphql_generated_domains_default_to_first_100_ids_from_zero() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let mut expected = vec![GRAPHQL_ALICE_NAMEHASH.to_owned(), GRAPHQL_BOB_NAMEHASH.to_owned()];
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
            "query Page($skip: Int!, $direction: OrderDirection!) { domains(first: 1, skip: $skip, orderBy: id, orderDirection: $direction) { id } }",
            json!({"skip": skip, "direction": direction}),
        )
        .await?;
        assert_eq!(payload["data"]["domains"][0]["id"], json!(expected));
    }
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
    let (number, hash): (i64, String) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM bigname_phase.chain_phase_state WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .fetch_one(&database.lookup_pool)
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
            if field == "domain" {
                assert_eq!(payload["data"][field], Value::Null);
            } else {
                assert_eq!(payload["data"], Value::Null);
            }
        }
    }
    database.cleanup().await
}
