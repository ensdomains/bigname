const GRAPHQL_RESPONSE_EXPECTATIONS: &str = include_str!("fixtures/graphql-response-contract.json");

#[derive(Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct GraphqlResponseContract {
    cases: Vec<GraphqlResponseCase>,
}

#[derive(Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct GraphqlResponseCase {
    id: String,
    response: Value,
}

struct GraphqlRequestCase {
    id: &'static str,
    query: String,
    variables: Value,
}

impl GraphqlRequestCase {
    fn new(id: &'static str, query: impl Into<String>, variables: Value) -> Self {
        Self {
            id,
            query: query.into(),
            variables,
        }
    }
}

#[tokio::test]
async fn graphql_responses_match_committed_contract() -> Result<()> {
    let expected: GraphqlResponseContract = serde_json::from_str(GRAPHQL_RESPONSE_EXPECTATIONS)
        .context("GraphQL response fixture is invalid")?;
    let mut cases = Vec::new();

    let base = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&base).await?;
    let base_output = execute_graphql_requests(&base, base_graphql_requests()).await;
    let base_cleanup = base.cleanup().await;
    base_cleanup?;
    cases.extend(base_output?);

    let alice_records = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&alice_records).await?;
    seed_alice_record_inventory(&alice_records).await?;
    let alice_output =
        execute_graphql_requests(&alice_records, alice_record_graphql_requests()).await;
    let alice_cleanup = alice_records.cleanup().await;
    alice_cleanup?;
    cases.extend(alice_output?);

    let all_records = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&all_records).await?;
    seed_alice_record_inventory(&all_records).await?;
    seed_bob_record_inventory(&all_records).await?;
    let all_output = execute_graphql_requests(
        &all_records,
        vec![GraphqlRequestCase::new(
            "existing_domains_records_for_each_name",
            r#"query Domains($where: Domain_filter!) {
                domains(where: $where, orderBy: name, orderDirection: asc) {
                    name
                    resolver { contentHash addresses { coinType address } }
                }
            }"#,
            json!({ "where": { "owner": GRAPHQL_OWNER } }),
        )],
    )
    .await;
    let all_cleanup = all_records.cleanup().await;
    all_cleanup?;
    cases.extend(all_output?);

    let sepolia = TestDatabase::new_migrated().await?;
    seed_erin_sepolia_record_fixture(&sepolia).await?;
    let sepolia_output = execute_graphql_requests(
        &sepolia,
        vec![GraphqlRequestCase::new(
            "existing_sepolia_record_anchor_fallback",
            r#"query Domain($id: ID!) {
                domain(id: $id) {
                    resolver { texts addresses { coinType address } contentHash }
                }
            }"#,
            json!({ "id": "erin.eth" }),
        )],
    )
    .await;
    let sepolia_cleanup = sepolia.cleanup().await;
    sepolia_cleanup?;
    cases.extend(sepolia_output?);

    let actual = GraphqlResponseContract { cases };
    if actual != expected {
        anyhow::bail!(
            "GraphQL response contract changed; update the committed fixture with the reviewed API change\n\
             expected:\n{}\nactual:\n{}",
            serde_json::to_string_pretty(&expected)?,
            serde_json::to_string_pretty(&actual)?,
        );
    }
    Ok(())
}

async fn execute_graphql_requests(
    database: &TestDatabase,
    requests: Vec<GraphqlRequestCase>,
) -> Result<Vec<GraphqlResponseCase>> {
    let mut responses = Vec::with_capacity(requests.len());
    for request in requests {
        responses.push(GraphqlResponseCase {
            id: request.id.to_owned(),
            response: post_graphql(database.app_state(), &request.query, request.variables).await?,
        });
    }
    Ok(responses)
}

fn base_graphql_requests() -> Vec<GraphqlRequestCase> {
    vec![
        GraphqlRequestCase::new(
            "generated_account_by_id",
            "query { account(id: \"0x000000000000000000000000000000000000000a\") { id } }",
            json!({}),
        ),
        GraphqlRequestCase::new(
            "generated_accounts_page",
            "query { accounts(where: { id_in: [\"0x000000000000000000000000000000000000000a\", \"0x000000000000000000000000000000000000000b\"] }) { id } }",
            json!({}),
        ),
        GraphqlRequestCase::new(
            "generated_resolver_by_id",
            format!("query {{ resolver(id: \"{GRAPHQL_RESOLVER}-{GRAPHQL_ALICE_NAMEHASH}\") {{ id address }} }}"),
            json!({}),
        ),
        GraphqlRequestCase::new(
            "generated_resolvers_page",
            format!("query {{ resolvers(where: {{ address: \"{GRAPHQL_RESOLVER}\" }}) {{ id address }} }}"),
            json!({}),
        ),
        GraphqlRequestCase::new(
            "existing_domain_shape",
            r#"query Domain($id: ID!) {
                domain(id: $id) {
                    id name normalizedName tokenId createdAt expiryDate
                    owner { id }
                    resolver { id address texts contentHash addresses { coinType address } }
                }
            }"#,
            json!({ "id": GRAPHQL_ALICE_NAMEHASH }),
        ),
        GraphqlRequestCase::new(
            "existing_domain_by_name",
            r#"query Domain($id: ID!) {
                domain(id: $id) { id name normalizedName owner { id } }
            }"#,
            json!({ "id": "alice.eth" }),
        ),
        GraphqlRequestCase::new(
            "existing_domain_missing",
            r#"query Domain($id: ID!) { domain(id: $id) { id } }"#,
            json!({ "id": "0xdeadbeef" }),
        ),
        GraphqlRequestCase::new(
            "existing_domains_first_page",
            r#"query Domains(
                $where: Domain_filter!
                $first: Int
                $skip: Int
                $orderBy: Domain_orderBy
                $orderDirection: OrderDirection
            ) {
                domains(
                    where: $where
                    first: $first
                    skip: $skip
                    orderBy: $orderBy
                    orderDirection: $orderDirection
                ) { id name owner { id } }
            }"#,
            json!({
                "where": { "owner_in": [GRAPHQL_OWNER] },
                "first": 200,
                "skip": 0,
                "orderBy": "name",
                "orderDirection": "asc",
            }),
        ),
        GraphqlRequestCase::new(
            "existing_domains_second_page",
            r#"query Domains(
                $where: Domain_filter!
                $first: Int
                $skip: Int
                $orderBy: Domain_orderBy
                $orderDirection: OrderDirection
            ) {
                domains(
                    where: $where
                    first: $first
                    skip: $skip
                    orderBy: $orderBy
                    orderDirection: $orderDirection
                ) { name }
            }"#,
            json!({
                "where": { "owner_in": [GRAPHQL_OWNER] },
                "first": 1,
                "skip": 1,
                "orderBy": "name",
                "orderDirection": "asc",
            }),
        ),
        GraphqlRequestCase::new(
            "existing_owner_filter",
            r#"query Domains($where: Domain_filter!) {
                domains(where: $where) { id }
            }"#,
            json!({ "where": { "owner_in": [GRAPHQL_OWNER] } }),
        ),
        GraphqlRequestCase::new(
            "existing_empty_owner_filter",
            r#"query Domains($where: Domain_filter!) {
                domains(where: $where) { id }
            }"#,
            json!({ "where": { "owner_in": [] } }),
        ),
        GraphqlRequestCase::new(
            "existing_empty_owner_count",
            r#"query DomainConnection($where: DomainFilter!) {
                domainConnection(first: 0, where: $where) { totalCount }
            }"#,
            json!({ "where": { "owner_in": [] } }),
        ),
        GraphqlRequestCase::new(
            "existing_owned_names_count",
            r#"query OwnedNamesCount($where: RegistrationFilter!) {
                registrationConnection(first: 0, where: $where) { totalCount }
            }"#,
            json!({ "where": { "registrant": GRAPHQL_REGISTRANT } }),
        ),
        GraphqlRequestCase::new(
            "existing_migrated_names_count",
            r#"query MigratedNamesCount($where: DomainFilter!) {
                domainConnection(first: 0, where: $where) { totalCount }
            }"#,
            json!({ "where": { "owner": GRAPHQL_OWNER, "isMigrated": true } }),
        ),
        GraphqlRequestCase::new(
            "existing_all_names_count",
            r#"query MigratedNamesCount($where: DomainFilter!) {
                domainConnection(first: 0, where: $where) { totalCount }
            }"#,
            json!({ "where": { "owner": GRAPHQL_OWNER } }),
        ),
        GraphqlRequestCase::new(
            "existing_owner_registrant_fallback",
            r#"query Domain($id: ID!) {
                domain(id: $id) { owner { id } expiryDate }
            }"#,
            json!({ "id": "carol.eth" }),
        ),
        GraphqlRequestCase::new(
            "existing_owner_zero_address_fallback",
            r#"query Domain($id: ID!) {
                domain(id: $id) { owner { id } expiryDate createdAt }
            }"#,
            json!({ "id": "dave.eth" }),
        ),
        ordered_names_request("existing_name_descending", "name", "desc"),
        ordered_names_request("existing_expiry_ascending", "expiryDate", "asc"),
        ordered_names_request("existing_expiry_descending", "expiryDate", "desc"),
        GraphqlRequestCase::new(
            "existing_registrant_list_filter",
            r#"query OwnedNamesCount($where: RegistrationFilter!) {
                registrationConnection(first: 0, where: $where) { totalCount }
            }"#,
            json!({
                "where": { "registrant_in": [GRAPHQL_REGISTRANT, GRAPHQL_REGISTRANT_C] }
            }),
        ),
        GraphqlRequestCase::new(
            "existing_name_contains_filter",
            r#"query Domains($where: Domain_filter!) {
                domains(where: $where) { name }
            }"#,
            json!({
                "where": {
                    "name_contains": "aro"
                }
            }),
        ),
    ]
}

fn ordered_names_request(
    id: &'static str,
    order_by: &'static str,
    direction: &'static str,
) -> GraphqlRequestCase {
    GraphqlRequestCase::new(
        id,
        r#"query Domains(
            $where: Domain_filter!
            $orderBy: Domain_orderBy
            $orderDirection: OrderDirection
        ) {
            domains(
                where: $where
                orderBy: $orderBy
                orderDirection: $orderDirection
            ) { name }
        }"#,
        json!({
            "where": { "id_in": [GRAPHQL_CAROL_NAMEHASH, GRAPHQL_DAVE_NAMEHASH] },
            "orderBy": order_by,
            "orderDirection": direction,
        }),
    )
}

fn alice_record_graphql_requests() -> Vec<GraphqlRequestCase> {
    vec![
        GraphqlRequestCase::new(
            "existing_domains_record_hit_and_miss",
            r#"query Domains($where: Domain_filter!) {
                domains(where: $where, orderBy: name, orderDirection: asc) {
                    name
                    resolver { address contentHash texts addresses { coinType address } }
                }
            }"#,
            json!({ "where": { "owner": GRAPHQL_OWNER } }),
        ),
        GraphqlRequestCase::new(
            "existing_domain_records",
            r#"query Domain($id: ID!) {
                domain(id: $id) {
                    resolver { id address texts contentHash addresses { coinType address } }
                }
            }"#,
            json!({ "id": "alice.eth" }),
        ),
        GraphqlRequestCase::new(
            "existing_domain_without_records",
            r#"query Domain($id: ID!) {
                domain(id: $id) {
                    resolver { texts contentHash addresses { coinType address } }
                }
            }"#,
            json!({ "id": "bob.eth" }),
        ),
        GraphqlRequestCase::new(
            "manager_domain",
            manager_domain_document(),
            json!({ "id": GRAPHQL_ALICE_NAMEHASH }),
        ),
        GraphqlRequestCase::new(
            "manager_domains",
            manager_domains_document(),
            json!({
                "where": { "owner_in": [GRAPHQL_OWNER] },
                "first": 200,
                "skip": 0,
                "orderBy": "name",
                "orderDirection": "asc",
            }),
        ),
        GraphqlRequestCase::new(
            "manager_owned_names_count",
            include_str!("fixtures/manager-graphql/queries/OwnedNamesCount.graphql"),
            json!({ "where": { "registrant": GRAPHQL_REGISTRANT } }),
        ),
        GraphqlRequestCase::new(
            "manager_migrated_names_count",
            include_str!("fixtures/manager-graphql/queries/MigratedNamesCount.graphql"),
            json!({ "where": { "owner": GRAPHQL_OWNER, "isMigrated": true } }),
        ),
    ]
}

fn manager_domain_document() -> String {
    [
        include_str!("fixtures/manager-graphql/queries/Domain.graphql"),
        include_str!("fixtures/manager-graphql/fragments/Domain.graphql"),
        include_str!("fixtures/manager-graphql/fragments/Resolver.graphql"),
        include_str!("fixtures/manager-graphql/fragments/Account.graphql"),
    ]
    .join("\n")
    .replace("$id: String!", "$id: ID!")
}

fn manager_domains_document() -> String {
    [
        include_str!("fixtures/manager-graphql/queries/Domains.graphql"),
        include_str!("fixtures/manager-graphql/fragments/Domain.graphql"),
        include_str!("fixtures/manager-graphql/fragments/Resolver.graphql"),
        include_str!("fixtures/manager-graphql/fragments/Account.graphql"),
    ]
    .join("\n")
    .replace("$where: DomainFilter!", "$where: Domain_filter!")
}
