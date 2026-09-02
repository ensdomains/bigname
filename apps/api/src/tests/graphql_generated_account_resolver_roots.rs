fn generated_entity_type_ref(value: &Value) -> String {
    match value["kind"].as_str().expect("introspection type kind") {
        "NON_NULL" => format!("{}!", generated_entity_type_ref(&value["ofType"])),
        "LIST" => format!("[{}]", generated_entity_type_ref(&value["ofType"])),
        _ => value["name"].as_str().expect("named type").to_owned(),
    }
}

fn generated_entity_field<'a>(ty: &'a Value, name: &str) -> &'a Value {
    ty["fields"].as_array().expect("fields").iter()
        .find(|field| field["name"] == name)
        .unwrap_or_else(|| panic!("missing GraphQL field {name}"))
}

#[tokio::test]
async fn graphql_generated_account_resolver_root_signatures_match_pinned_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let payload = post_graphql(database.app_state(), r#"query {
      query: __type(name: "Query") { fields { name args { name defaultValue type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } type { kind name ofType { kind name ofType { kind name } } } } }
      account: __type(name: "Account") { fields { name type { kind name ofType { kind name } } } }
      resolver: __type(name: "Resolver") { fields { name type { kind name ofType { kind name } } } }
      accountFilter: __type(name: "Account_filter") { inputFields { name } }
      resolverFilter: __type(name: "Resolver_filter") { inputFields { name } }
      accountOrder: __type(name: "Account_orderBy") { enumValues { name } }
      resolverOrder: __type(name: "Resolver_orderBy") { enumValues { name } }
    }"#, json!({})).await?;
    for (name, args, result) in [
        ("account", vec![("id", "ID!", None), ("block", "Block_height", None), ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny"))], "Account"),
        ("accounts", vec![("skip", "Int", Some("0")), ("first", "Int", Some("100")), ("orderBy", "Account_orderBy", None), ("orderDirection", "OrderDirection", None), ("where", "Account_filter", None), ("block", "Block_height", None), ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny"))], "[Account!]!"),
        ("resolver", vec![("id", "ID!", None), ("block", "Block_height", None), ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny"))], "Resolver"),
        ("resolvers", vec![("skip", "Int", Some("0")), ("first", "Int", Some("100")), ("orderBy", "Resolver_orderBy", None), ("orderDirection", "OrderDirection", None), ("where", "Resolver_filter", None), ("block", "Block_height", None), ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny"))], "[Resolver!]!"),
    ] {
        let field = generated_entity_field(&payload["data"]["query"], name);
        let actual = field["args"].as_array().expect("args").iter().map(|arg| (
            arg["name"].as_str().unwrap(), generated_entity_type_ref(&arg["type"]), arg["defaultValue"].as_str()
        )).collect::<Vec<_>>();
        let expected = args.into_iter().map(|(name, ty, default)| (name, ty.to_owned(), default)).collect::<Vec<_>>();
        assert_eq!(actual, expected, "{name} argument signature");
        assert_eq!(generated_entity_type_ref(&field["type"]), result);
    }
    assert_eq!(generated_entity_type_ref(&generated_entity_field(&payload["data"]["account"], "id")["type"]), "ID!");
    assert_eq!(generated_entity_type_ref(&generated_entity_field(&payload["data"]["resolver"], "id")["type"]), "ID!");
    assert_eq!(generated_entity_type_ref(&generated_entity_field(&payload["data"]["resolver"], "address")["type"]), "Bytes!");
    for (key, expected) in [("accountFilter", vec!["id", "id_in"]), ("resolverFilter", vec!["address", "domain", "id"])] {
        let mut actual = payload["data"][key]["inputFields"].as_array().expect("input fields").iter().filter_map(|field| field["name"].as_str()).collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(actual, expected);
    }
    assert_eq!(payload["data"]["accountOrder"]["enumValues"], json!([{"name":"id"}]));
    assert_eq!(payload["data"]["resolverOrder"]["enumValues"], json!([{"name":"id"}]));
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_account_resolver_reject_unimplemented_inputs_and_order_values() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    for (query, needle) in [
        ("query { accounts(where: { id_not: \"x\" }) { id } }", "id_not"),
        ("query { accounts(where: { domains_: {} }) { id } }", "domains_"),
        ("query { resolvers(where: { address_in: [] }) { id } }", "address_in"),
        ("query { resolvers(where: { domain_: {} }) { id } }", "domain_"),
        ("query { accounts(orderBy: domains) { id } }", "domains"),
        ("query { resolvers(orderBy: address) { id } }", "address"),
    ] {
        let payload = post_graphql_allow_errors(database.app_state(), query, json!({})).await?;
        assert!(payload["errors"][0]["message"].as_str().is_some_and(|error| error.contains(needle)), "{payload}");
    }
    database.cleanup().await
}

async fn generated_account_ids(database: &TestDatabase, where_value: Value) -> Result<Vec<String>> {
    let payload = post_graphql(database.app_state(), "query Accounts($where: Account_filter!) { accounts(where: $where) { id } }", json!({"where": where_value})).await?;
    Ok(payload["data"]["accounts"].as_array().context("accounts")?.iter().filter_map(|row| row["id"].as_str().map(str::to_owned)).collect())
}

#[tokio::test]
async fn graphql_generated_account_filters_match_served_id() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let mixed = GRAPHQL_OWNER.to_ascii_uppercase().replacen("0X", "0x", 1);
    for filter in [json!({"id": mixed}), json!({"id_in": [mixed, GRAPHQL_OWNER]}), json!({"id": mixed, "id_in": [GRAPHQL_OWNER, GRAPHQL_REGISTRANT]})] {
        assert_eq!(generated_account_ids(&database, filter).await?, vec![GRAPHQL_OWNER]);
    }
    assert!(generated_account_ids(&database, json!({"id": GRAPHQL_OWNER, "id_in": [GRAPHQL_REGISTRANT]})).await?.is_empty());
    assert!(generated_account_ids(&database, json!({"id_in": []})).await?.is_empty());
    let point = post_graphql(database.app_state(), "query Account($id: ID!) { account(id: $id) { id } }", json!({"id": mixed})).await?;
    assert_eq!(point["data"]["account"]["id"], json!(GRAPHQL_OWNER));
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_accounts_are_distinct_current_addresses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    assert_eq!(generated_account_ids(&database, json!({"id": GRAPHQL_OWNER})).await?, vec![GRAPHQL_OWNER]);
    sqlx::query("DELETE FROM bigname_phase.address_names_current WHERE LOWER(address) = $1").bind(GRAPHQL_OWNER).execute(&database.lookup_pool).await?;
    let point = post_graphql(database.app_state(), "query { account(id: \"0x000000000000000000000000000000000000000a\") { id } }", json!({})).await?;
    assert!(point["data"]["account"].is_null());
    database.cleanup().await
}

async fn generated_resolver_rows(database: &TestDatabase, where_value: Value) -> Result<Vec<Value>> {
    let payload = post_graphql(database.app_state(), "query Resolvers($where: Resolver_filter!) { resolvers(where: $where) { id address } }", json!({"where": where_value})).await?;
    Ok(payload["data"]["resolvers"].as_array().context("resolvers")?.clone())
}

#[tokio::test]
async fn graphql_generated_resolver_filters_match_served_values() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let id = format!("{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}");
    let nested = post_graphql(database.app_state(), "query { domain(id: \"alice.eth\") { id resolver { id address } } }", json!({})).await?;
    assert_eq!(nested["data"]["domain"]["resolver"], json!({"id": id, "address": GRAPHQL_RESOLVER}));
    for filter in [json!({"id": id}), json!({"address": GRAPHQL_RESOLVER}), json!({"domain": GRAPHQL_ALICE_NAMEHASH}), json!({"id": id, "address": GRAPHQL_RESOLVER, "domain": GRAPHQL_ALICE_NAMEHASH})] {
        let rows = generated_resolver_rows(&database, filter).await?;
        assert!(rows.iter().any(|row| row["id"] == id));
    }
    let mixed_id = id.to_ascii_uppercase().replace("0X", "0x");
    let mixed_address = GRAPHQL_RESOLVER.to_ascii_uppercase().replacen("0X", "0x", 1);
    let mixed_domain = GRAPHQL_ALICE_NAMEHASH.to_ascii_uppercase().replacen("0X", "0x", 1);
    for filter in [
        json!({"id": mixed_id}),
        json!({"address": mixed_address}),
        json!({"domain": mixed_domain}),
        json!({"id": mixed_id, "address": mixed_address, "domain": mixed_domain}),
    ] {
        let rows = generated_resolver_rows(&database, filter).await?;
        assert!(rows.iter().any(|row| row["id"] == id));
    }
    assert!(generated_resolver_rows(&database, json!({"id": id, "domain": GRAPHQL_BOB_NAMEHASH})).await?.is_empty());
    let point = post_graphql(database.app_state(), "query Resolver($id: ID!) { resolver(id: $id) { id address } }", json!({"id": mixed_id})).await?;
    assert_eq!(point["data"]["resolver"]["id"], json!(id));
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_resolver_id_is_per_domain_and_current_binding_only() -> Result<()> {
    const NEXT: &str = "0x000000000000000000000000000000000000def1";
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let rows = generated_resolver_rows(&database, json!({"address": GRAPHQL_RESOLVER})).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["address"], rows[1]["address"]);
    assert_ne!(rows[0]["id"], rows[1]["id"]);
    let old_id = format!("{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}");
    let new_id = format!("{NEXT}-{GRAPHQL_ALICE_NAMEHASH}");
    sqlx::query("UPDATE bigname_phase.name_current SET declared_summary = jsonb_set(declared_summary, '{resolver,address}', to_jsonb($1::text)) WHERE namehash = $2").bind(NEXT).bind(GRAPHQL_ALICE_NAMEHASH).execute(&database.lookup_pool).await?;
    for (id, present) in [(old_id, false), (new_id, true)] {
        let payload = post_graphql(database.app_state(), "query Resolver($id: ID!) { resolver(id: $id) { id } }", json!({"id": id})).await?;
        assert_eq!(!payload["data"]["resolver"].is_null(), present);
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_zero_resolver_binding_is_not_an_entity() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.name_current SET declared_summary = jsonb_set(declared_summary, '{resolver,address}', to_jsonb($1::text)) WHERE namehash = $2").bind(ZERO_ADDRESS).bind(GRAPHQL_ALICE_NAMEHASH).execute(&database.lookup_pool).await?;
    assert!(generated_resolver_rows(&database, json!({"address": ZERO_ADDRESS})).await?.is_empty());
    let id = format!("{ZERO_ADDRESS}-{GRAPHQL_ALICE_NAMEHASH}");
    let payload = post_graphql(database.app_state(), "query Resolver($id: ID!) { resolver(id: $id) { id } }", json!({"id": id})).await?;
    assert!(payload["data"]["resolver"].is_null());
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_account_resolver_block_contract_matches_domain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let (number, hash): (i64, String) = sqlx::query_as("SELECT current_block_number, current_block_hash FROM bigname_phase.chain_phase_state WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'").fetch_one(&database.lookup_pool).await?;
    let roots = vec![
        ("account(id: \"0x000000000000000000000000000000000000000a\"".to_owned(), true),
        ("accounts(".to_owned(), false),
        (format!("resolver(id: \"{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}\""), true),
        ("resolvers(".to_owned(), false),
    ];
    for (root, singular) in roots {
        for block in [format!("{{ number: {number} }}"), format!("{{ hash: \\\"{hash}\\\" }}"), format!("{{ number_gte: {number} }}")] {
            let payload = post_graphql_allow_errors(database.app_state(), &format!("query {{ {root} block: {block}) {{ id }} }}"), json!({})).await?;
            assert!(payload.get("errors").is_none(), "{payload}");
        }
        let payload = post_graphql_allow_errors(database.app_state(), &format!("query {{ {root} block: {{ number: {} }}) {{ id }} }}", number - 1), json!({})).await?;
        assert_eq!(payload["errors"][0]["message"], json!("the requested block number is not the served head"));
        assert_eq!(payload["data"].is_null(), !singular);
    }
    database.cleanup().await
}
