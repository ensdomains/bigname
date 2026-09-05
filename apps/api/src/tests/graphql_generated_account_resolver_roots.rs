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
      query: __type(name: "Query") { fields { name args { name defaultValue type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
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
    assert!(point["data"]["account"].is_null());
    let uppercase_prefix = GRAPHQL_OWNER.replacen("0x", "0X", 1);
    let point = post_graphql(database.app_state(), "query Account($id: ID!) { account(id: $id) { id } }", json!({"id": uppercase_prefix})).await?;
    assert!(point["data"]["account"].is_null());
    let point = post_graphql(database.app_state(), "query Account($id: ID!) { account(id: $id) { id } }", json!({"id": GRAPHQL_OWNER})).await?;
    assert_eq!(point["data"]["account"]["id"], json!(GRAPHQL_OWNER));
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_accounts_are_distinct_current_addresses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    assert_eq!(generated_account_ids(&database, json!({"id": GRAPHQL_OWNER})).await?, vec![GRAPHQL_OWNER]);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_accounts_are_current_relation_only() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query("DELETE FROM bigname_phase.address_names_current WHERE LOWER(address) = $1").bind(GRAPHQL_OWNER).execute(&database.lookup_pool).await?;
    let point = post_graphql(database.app_state(), "query { account(id: \"0x000000000000000000000000000000000000000a\") { id } }", json!({})).await?;
    assert!(point["data"]["account"].is_null());
    assert_eq!(generated_account_ids(&database, json!({"id": GRAPHQL_REGISTRANT})).await?, vec![GRAPHQL_REGISTRANT]);
    database.cleanup().await
}

async fn seed_generated_account_page(database: &TestDatabase, end: i32) -> Result<()> {
    sqlx::query(r#"WITH source AS (
        SELECT * FROM bigname_phase.address_names_current
        WHERE LOWER(address) = $1 LIMIT 1
    ) INSERT INTO bigname_phase.address_names_current (
        address, logical_name_id, relation, namespace, raw_name, namehash,
        surface_binding_id, resource_id, token_lineage_id, binding_kind,
        support_status, unsupported_reason, provenance, chain_positions,
        canonicality_summary, manifest_version
    ) SELECT '0x' || LPAD(TO_HEX(n), 40, '0'), logical_name_id, relation, namespace,
        raw_name, namehash, surface_binding_id, resource_id, token_lineage_id,
        binding_kind, support_status, unsupported_reason, provenance, chain_positions,
        canonicality_summary, manifest_version
      FROM source CROSS JOIN GENERATE_SERIES(256, $2) n"#)
        .bind(GRAPHQL_REGISTRANT)
        .bind(end)
        .execute(&database.lookup_pool).await?;
    sqlx::query("ANALYZE bigname_phase.address_names_current")
        .execute(&database.lookup_pool).await?;
    Ok(())
}

#[tokio::test]
async fn graphql_generated_accounts_page_and_order_in_postgres() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_generated_account_page(&database, 476).await?;
    for (args, expected) in [("", 100), ("first: 500", 200), ("first: 0", 0), ("first: -1", 0), ("skip: 1000001", 0)] {
        let args = if args.is_empty() { String::new() } else { format!("({args})") };
        let payload = post_graphql(database.app_state(), &format!("query {{ accounts{args} {{ id }} }}"), json!({})).await?;
        assert_eq!(payload["data"]["accounts"].as_array().context("accounts")?.len(), expected, "{args}");
    }
    let payload = post_graphql(database.app_state(), "query { asc: accounts(first: 2, skip: -1) { id } desc: accounts(first: 2, orderDirection: desc) { id } }", json!({})).await?;
    let asc = payload["data"]["asc"].as_array().context("asc")?;
    let desc = payload["data"]["desc"].as_array().context("desc")?;
    assert!(asc[0]["id"].as_str() < asc[1]["id"].as_str());
    assert!(desc[0]["id"].as_str() > desc[1]["id"].as_str());
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
        json!({"address": mixed_address}),
        json!({"domain": mixed_domain}),
        json!({"id": mixed_id.clone()}),
    ] {
        let rows = generated_resolver_rows(&database, filter).await?;
        assert!(rows.iter().any(|row| row["id"] == id));
    }
    assert!(generated_resolver_rows(&database, json!({"id": id, "domain": GRAPHQL_BOB_NAMEHASH})).await?.is_empty());
    let uppercase_prefix = id.replacen("0x", "0X", 1);
    assert!(generated_resolver_rows(&database, json!({"id": uppercase_prefix})).await?.is_empty());
    for noncanonical in [mixed_id, uppercase_prefix] {
        let point = post_graphql(database.app_state(), "query Resolver($id: ID!) { resolver(id: $id) { id address } }", json!({"id": noncanonical})).await?;
        assert!(point["data"]["resolver"].is_null());
    }
    let point = post_graphql(database.app_state(), "query Resolver($id: ID!) { resolver(id: $id) { id address } }", json!({"id": id})).await?;
    assert_eq!(point["data"]["resolver"]["id"], json!(id));

    let bad_bytes = post_graphql_allow_errors(database.app_state(), "query { resolvers(where: { address: \"0X000000000000000000000000000000000000000A\" }) { id } }", json!({})).await?;
    assert!(bad_bytes["errors"][0]["message"].as_str().is_some_and(|message| message.contains("Bytes")), "{bad_bytes}");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_resolver_id_is_per_domain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let rows = generated_resolver_rows(&database, json!({"address": GRAPHQL_RESOLVER})).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["address"], rows[1]["address"]);
    assert_ne!(rows[0]["id"], rows[1]["id"]);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_resolver_roots_validate_current_resource() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let update = sqlx::query(
        r#"UPDATE bigname_phase.name_current alice
           SET serving_resource_id = bob.resource_id
          FROM bigname_phase.name_current bob
         WHERE alice.namehash = $1
           AND bob.namehash = $2"#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .bind(GRAPHQL_BOB_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(update.rows_affected(), 1);
    let orphaned = sqlx::query(
        r#"UPDATE bigname_phase.resources resource
              SET canonicality_state = 'orphaned'
             FROM bigname_phase.name_current alice
            WHERE alice.namehash = $1
              AND resource.resource_id = alice.resource_id"#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(orphaned.rows_affected(), 1);

    let id = format!("{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}");
    let payload = post_graphql(
        database.app_state(),
        r#"query ResolverCanonicality($id: ID!, $domainId: ID!, $domainFilter: String!) {
            domain(id: $domainId) { id }
            resolver(id: $id) { id }
            resolvers(where: { domain: $domainFilter }) { id }
        }"#,
        json!({
            "id": id,
            "domainId": GRAPHQL_ALICE_NAMEHASH,
            "domainFilter": GRAPHQL_ALICE_NAMEHASH,
        }),
    )
    .await?;
    assert!(payload.get("errors").is_none(), "{payload}");
    assert!(payload["data"]["domain"].is_null());
    assert!(payload["data"]["resolver"].is_null());
    assert_eq!(payload["data"]["resolvers"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_resolver_id_uses_current_binding_only() -> Result<()> {
    const NEXT: &str = "0x000000000000000000000000000000000000def1";
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
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
async fn graphql_generated_resolvers_page_and_order_in_postgres() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let invalid_identity_rows: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
             FROM bigname_phase.name_current
            WHERE logical_name_id <> namespace || ':' || namehash
               OR namehash !~ '^0x[0-9a-f]{64}$'"#,
    )
    .fetch_one(&database.lookup_pool)
    .await?;
    assert_eq!(
        invalid_identity_rows, 0,
        "every fixture name must use the minted namespace:namehash identity"
    );
    for (args, expected) in [("", 2), ("first: 500", 2), ("first: 0", 0), ("first: -1", 0), ("skip: 1000001", 0)] {
        let args = if args.is_empty() { String::new() } else { format!("({args})") };
        let payload = post_graphql(database.app_state(), &format!("query {{ resolvers{args} {{ id }} }}"), json!({})).await?;
        assert_eq!(payload["data"]["resolvers"].as_array().context("resolvers")?.len(), expected, "{args}");
    }
    let payload = post_graphql(database.app_state(), "query { asc: resolvers(first: 2, skip: -1) { id address } desc: resolvers(first: 2, orderDirection: desc) { id address } }", json!({})).await?;
    let asc = payload["data"]["asc"].as_array().context("asc")?;
    let desc = payload["data"]["desc"].as_array().context("desc")?;
    let asc_ids = asc.iter().map(|row| row["id"].as_str().unwrap()).collect::<Vec<_>>();
    let mut lexical_ids = asc_ids.clone();
    lexical_ids.sort_unstable();
    assert_eq!(asc_ids, lexical_ids, "(address, namehash) order equals composite ID order");
    assert_eq!(desc.iter().map(|row| row["id"].as_str().unwrap()).rev().collect::<Vec<_>>(), lexical_ids);
    assert_eq!(asc[0]["address"], asc[1]["address"]);
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
    let (old_number, old_hash): (i64, String) = sqlx::query_as("SELECT current_block_number, current_block_hash FROM bigname_phase.chain_phase_state WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'").fetch_one(&database.lookup_pool).await?;
    let number = old_number + 1;
    let hash = format!("0x{}", "22".repeat(32));
    sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state) VALUES ('ethereum-mainnet', $1, $2, $3, now(), 'finalized')").bind(&hash).bind(&old_hash).bind(number).execute(&database.lookup_pool).await?;
    sqlx::query("UPDATE bigname_phase.chain_heads SET latest_block_hash = $1, latest_block_number = $2, safe_block_hash = $1, safe_block_number = $2, finalized_block_hash = $1, finalized_block_number = $2 WHERE chain_id = 'ethereum-mainnet'").bind(&hash).bind(number).execute(&database.lookup_pool).await?;
    sqlx::query("UPDATE bigname_phase.chain_phase_state SET current_block_hash = $1, current_block_number = $2, target_block_hash = $1, target_block_number = $2 WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'").bind(&hash).bind(number).execute(&database.lookup_pool).await?;
    let roots = vec![
        ("account(id: \"0x000000000000000000000000000000000000000a\"".to_owned(), true),
        ("accounts(".to_owned(), false),
        (format!("resolver(id: \"{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}\""), true),
        ("resolvers(".to_owned(), false),
    ];
    for (root, singular) in roots {
        let no_block = if root.ends_with('(') { root.trim_end_matches('(').to_owned() } else { format!("{root})") };
        let payload = post_graphql(database.app_state(), &format!("query {{ {no_block} {{ id }} }}"), json!({})).await?;
        assert!(!payload["data"].is_null());
        for block in [format!("{{ number: {number} }}"), format!("{{ hash: \"{hash}\" }}"), format!("{{ number_gte: {number} }}")] {
            let payload = post_graphql_allow_errors(database.app_state(), &format!("query {{ {root} block: {block}) {{ id }} }}"), json!({})).await?;
            assert!(payload.get("errors").is_none(), "{payload}");
        }
        let field = root.split(['(', ' ']).next().unwrap();
        for (block, message) in [
            (format!("{{ number: {} }}", number - 1), "the requested block number is not the served head"),
            (format!("{{ hash: \"0x{}\" }}", "ff".repeat(32)), "the requested block hash is not the served head"),
            (format!("{{ number_gte: {} }}", number + 1), "the served head has not reached block.number_gte"),
            ("{}".to_owned(), "block must contain hash, number, or number_gte"),
            ("{ hash: null }".to_owned(), "block.hash must not be null"),
            ("{ number: null }".to_owned(), "block.number must not be null"),
            ("{ number_gte: null }".to_owned(), "block.number_gte must not be null"),
            ("{ number: -1 }".to_owned(), "block number constraints must be non-negative"),
            ("{ number_gte: -1 }".to_owned(), "block number constraints must be non-negative"),
        ] {
            let payload = post_graphql_allow_errors(database.app_state(), &format!("query {{ {root} block: {block}) {{ id }} }}"), json!({})).await?;
            assert_eq!(payload["errors"][0]["message"], json!(message));
            assert_eq!(payload["errors"][0]["path"], json!([field]));
            assert_eq!(payload["data"].is_null(), !singular);
        }

        let (_guard, control) = crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool).await?;
        let state = database.app_state();
        let query = format!("query {{ {no_block} {{ id }} }}");
        let request = tokio::spawn(async move { post_graphql_allow_errors(state, &query, json!({})).await });
        control.wait_until_reached().await;
        sqlx::query("UPDATE bigname_phase.chain_phase_state SET updated_at = clock_timestamp() WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'").execute(&database.lookup_pool).await?;
        control.resume().await;
        let payload = request.await.context("GraphQL root request panicked")??;
        assert_eq!(payload["data"].is_null(), !singular);
        assert_eq!(payload["errors"][0]["path"], json!([field]));
    }
    sqlx::query("DELETE FROM bigname_phase.chain_heads WHERE chain_id = 'ethereum-mainnet'").execute(&database.lookup_pool).await?;
    for (root, singular) in [("account(id: \"0x000000000000000000000000000000000000000a\"", true), ("accounts(", false), (&format!("resolver(id: \"{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}\""), true), ("resolvers(", false)] {
        let payload = post_graphql_allow_errors(database.app_state(), &format!("query {{ {root} block: {{ number: {number} }}) {{ id }} }}"), json!({})).await?;
        assert_eq!(payload["errors"][0]["message"], json!("served head is unavailable for the requested block"));
        assert_eq!(payload["data"].is_null(), !singular);
    }
    database.cleanup().await
}

fn collect_plan_nodes<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
    nodes.push(node);
    if let Some(children) = node["Plans"].as_array() {
        for child in children { collect_plan_nodes(child, nodes); }
    }
}

fn plan_nodes(plan: &Value) -> Vec<&Value> {
    let mut nodes = Vec::new();
    collect_plan_nodes(&plan[0]["Plan"], &mut nodes);
    nodes
}

fn assert_no_full_sort(nodes: &[&Value]) {
    assert!(!nodes.iter().any(|node| node["Node Type"] == "Sort"), "full Sort in plan");
}

fn outer_plan(node: &Value) -> Result<&Value> {
    node["Plans"]
        .as_array()
        .and_then(|plans| {
            plans
                .iter()
                .find(|plan| plan["Parent Relationship"] == "Outer")
        })
        .context("plan node has no outer child")
}

fn outer_chain_index<'a>(mut node: &'a Value, index_name: &str) -> Result<&'a Value> {
    loop {
        if node["Index Name"] == index_name {
            return Ok(node);
        }
        assert_eq!(
            node["Node Type"], "Nested Loop",
            "only nested-loop joins may sit above ordered index {index_name}"
        );
        node = outer_plan(node)?;
    }
}

// NULL relationship IDs make these rows prove the bounded outer scan, not lineage joins or serving-resource EXISTS.
async fn pad_resolver_planner_statistics(database: &TestDatabase) -> Result<()> {
    let alice_id = format!("ens:{GRAPHQL_ALICE_NAMEHASH}");
    let surfaces = sqlx::query(r#"WITH source AS (
        SELECT * FROM bigname_phase.name_surfaces WHERE logical_name_id = $1
    ), generated AS (
        SELECT n, '0x' || LPAD(TO_HEX(n), 64, '0') AS hash FROM GENERATE_SERIES(1000, 5999) n
    ) INSERT INTO bigname_phase.name_surfaces (
        logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
        labelhashes, normalizer_version, visibility_state, normalization_errors,
        chain_id, block_hash, block_number, provenance, canonicality_state
    ) SELECT 'ens:' || hash, source.namespace, source.raw_name || n, source.raw_labels,
        source.dns_encoded_name, hash, source.labelhashes, source.normalizer_version,
        source.visibility_state, source.normalization_errors, source.chain_id,
        source.block_hash, source.block_number, source.provenance, source.canonicality_state
      FROM source CROSS JOIN generated"#)
        .bind(&alice_id)
        .execute(&database.lookup_pool)
        .await?;
    assert_eq!(surfaces.rows_affected(), 5_000);
    let names = sqlx::query(r#"WITH source AS (
        SELECT * FROM bigname_phase.name_current WHERE logical_name_id = $1
    ), generated AS (
        SELECT n, '0x' || LPAD(TO_HEX(n), 64, '0') AS hash
          FROM GENERATE_SERIES(1000, 5999) n
    ) INSERT INTO bigname_phase.name_current (
        logical_name_id, namespace, raw_name, namehash, declared_summary,
        support_status, unsupported_reason, provenance, chain_positions,
        canonicality_summary, manifest_version
    ) SELECT 'ens:' || hash, source.namespace, source.raw_name || n, hash,
        JSONB_SET(source.declared_summary, '{resolver,address}', TO_JSONB($2::TEXT)),
        source.support_status, source.unsupported_reason, source.provenance,
        source.chain_positions, source.canonicality_summary, source.manifest_version
      FROM source CROSS JOIN generated"#)
        .bind(&alice_id)
        .bind(GRAPHQL_RESOLVER)
        .execute(&database.lookup_pool)
        .await?;
    assert_eq!(names.rows_affected(), 5_000);
    sqlx::query("ANALYZE bigname_phase.name_current, bigname_phase.name_surfaces")
        .execute(&database.lookup_pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn graphql_generated_accounts_plan_is_distinct_index_bounded() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_generated_account_page(&database, 10_255).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    let account = crate::graphql::explain_phase_graphql_account_page(
        &database.lookup_pool,
        "ens",
        &chains,
        &crate::graphql::GeneratedAccountFilter::default(),
        100,
        false,
    )
    .await?;
    println!("ACCOUNT PLAN {}", serde_json::to_string_pretty(&account)?);
    let plan = &account[0]["Plan"];
    assert_eq!(plan["Node Type"], "Limit");
    let unique = outer_plan(plan)?;
    assert_eq!(unique["Node Type"], "Unique");
    let joins = outer_plan(unique)?;
    assert_eq!(joins["Node Type"], "Nested Loop");
    let scan = outer_chain_index(joins, "address_names_current_address_idx")?;
    let nodes = plan_nodes(&account);
    assert_no_full_sort(&nodes);
    assert!(!nodes.iter().any(|node| node["Node Type"] == "HashAggregate"));
    assert_eq!(scan["Node Type"], "Index Scan");
    assert!(scan["Actual Rows"].as_u64().context("index actual rows")? >= 100);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_resolvers_plan_pages_in_postgres() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_resolver_planner_statistics(&database).await?;
    let invalid_identity_rows: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
             FROM bigname_phase.name_current
            WHERE logical_name_id <> namespace || ':' || namehash
               OR namehash !~ '^0x[0-9a-f]{64}$'"#,
    )
    .fetch_one(&database.lookup_pool)
    .await?;
    assert_eq!(
        invalid_identity_rows, 0,
        "every padded name must use the minted namespace:namehash identity"
    );
    let chains = vec!["ethereum-mainnet".to_owned()];
    let rows = crate::graphql::load_phase_graphql_resolver_page_offset(
        &database.lookup_pool,
        "ens",
        &chains,
        &crate::graphql::GeneratedResolverFilter::default(),
        bigname_storage::NameCurrentListOrder::Asc,
        100,
        0,
    )
    .await?;
    let ids = rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>();
    let mut composite_id_order = ids.clone();
    composite_id_order.sort_unstable();
    assert_eq!(
        ids, composite_id_order,
        "minted logical-name order must equal served composite Resolver-ID order"
    );
    let enabled: String = sqlx::query_scalar("SHOW enable_incremental_sort").fetch_one(&database.lookup_pool).await?;
    assert_eq!(enabled, "on");
    for (label, filter) in [
        ("UNFILTERED", crate::graphql::GeneratedResolverFilter::default()),
        ("ADDRESS", crate::graphql::GeneratedResolverFilter { address: Some(GRAPHQL_RESOLVER.to_owned()), ..Default::default() }),
        ("DOMAIN", crate::graphql::GeneratedResolverFilter { domain: Some(GRAPHQL_ALICE_NAMEHASH.to_owned()), ..Default::default() }),
        ("ID", crate::graphql::GeneratedResolverFilter { id: crate::graphql::parse_resolver_id(&format!("{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}")), ..Default::default() }),
    ] {
        let resolver = crate::graphql::explain_phase_graphql_resolver_page(
            &database.lookup_pool, "ens", &chains, &filter, 100, false,
        ).await?;
        println!("RESOLVER {label} PLAN {}", serde_json::to_string_pretty(&resolver)?);
        let plan = &resolver[0]["Plan"];
        assert_eq!(plan["Node Type"], "Limit");
        if matches!(label, "UNFILTERED" | "ADDRESS") {
            assert_eq!(plan["Actual Rows"], 100);
        }
        let joins = outer_plan(plan)?;
        if matches!(label, "UNFILTERED" | "ADDRESS") {
            assert_eq!(joins["Node Type"], "Nested Loop");
        }
        let expected_index = match label {
            "DOMAIN" => "name_current_lookup_idx",
            "ID" => "name_current_pkey",
            _ => "name_current_resolver_idx",
        };
        let scan = outer_chain_index(joins, expected_index)?;
        assert_eq!(scan["Node Type"], "Index Scan");
        if matches!(label, "UNFILTERED" | "ADDRESS") {
            assert!(
                scan["Actual Rows"]
                    .as_u64()
                    .context("resolver index actual rows")?
                    <= 105,
                "ordered resolver index scan must stop within a small constant of the 100-row page: {scan}"
            );
        }
        if label == "ID" {
            let condition = scan["Index Cond"].as_str().context("Resolver ID index condition")?;
            assert!(condition.contains("logical_name_id"), "{condition}");
            let rows_removed = scan["Rows Removed by Filter"].as_u64().unwrap_or(0);
            assert!(
                rows_removed <= 1,
                "Resolver point lookup must not scan the shared-address group: {scan}"
            );
            assert!(
                scan["Shared Hit Blocks"].as_u64().context("point index shared hit blocks")? <= 16,
                "Resolver point lookup must touch a bounded number of index/heap blocks: {scan}"
            );
        }
        if label == "ADDRESS" {
            let condition = scan["Index Cond"].as_str().context("address index condition")?;
            assert!(condition.contains(">="));
            assert!(condition.contains("<="));
        }
        let nodes = plan_nodes(&resolver);
        if matches!(label, "UNFILTERED" | "ADDRESS") { assert_no_full_sort(&nodes); }
    }
    database.cleanup().await
}
