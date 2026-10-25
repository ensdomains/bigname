// The candidate lookup of `relation=resolves_to` (`families/records/candidates.rs`) finds the
// candidates the statement it replaced found, for every input: that statement joined every index
// row of the address to every pointer of its chain and tested each pair with an OR filter. The
// fixtures are the resolves-to, mirror, record-id and bulk tests'.
//
// Each way the forward read admits a value (`rows.rs`, `admitted_partitions`, and the link
// selection in `links.rs`) has a candidate piece per pointer source. The resource pointer is the
// resource's own pointer. The registry pointer is the node's ENSv1 registry pointer, which the
// forward read serves an ENSv1 pointer through when the two name different resolvers
// (`serving.rs`, `serving_pointers`). The bounded walk and lookup read the same candidates.
//
// | Admission (index row)  | Resource pointer piece    | Registry pointer piece             |
// | ---------------------- | ------------------------- | ---------------------------------- |
// | native: the row's node | node, row's resolver      | node, primary key (rows 15, 16)    |
// | named: the row's name  | namehash, row's resolver  | namehash, primary key (16, 18)     |
// | linked record (by id)  | all at row's resolver     | row's resolver, resolver index (17)|
// | mirror                 | node, mirror resolver     | none                               |
// | guarded, Basenames     | the node pieces           | none                               |
//
// The named registry piece reads only rows whose name's namehash is not their node. The node
// pieces cover the rest. Mirror admission is the `ens_v2` arm at a mirror resolver, not a V1
// pointer family, so the registry pieces don't apply to it. Guarded admission is for ENSv2
// families alone, and Basenames keeps no registry pointer per node.

/// The candidate statement before TYR-244 split it into equality pieces.
const PREVIOUS_CANDIDATES: &str = "WITH keys AS (
             SELECT chain_id, resolver_address, node, NULL::text AS record_id,
                    NULL::uuid AS named_resource,
                    CASE WHEN logical_name_id <> ''
                         THEN lower(split_part(logical_name_id, ':', 2)) END AS named_namehash,
                    coin_type, true AS indexed
             FROM bigname_phase.project_address_record_node_index
             WHERE address = $1 AND coin_type = ANY($2::text[])
             UNION ALL
             SELECT chain_id, resolver_address, NULL, record_id, NULL, NULL, coin_type, true
             FROM bigname_phase.project_address_record_id_index
             WHERE address = $1 AND coin_type = ANY($2::text[])
         ),
         mirrors AS (
             SELECT chain_id, resolver_address
             FROM bigname_phase.project_resolver_classification
             WHERE classification ->> 'role' = 'ensv1_mirror_resolver'
         )
         SELECT pointer.chain_id, pointer.resource_id, keys.coin_type, bool_or(keys.indexed)
                    AS indexed
         FROM keys
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = keys.chain_id
          AND (
              ((pointer.namehash = keys.node OR pointer.resource_id = keys.named_resource
                OR pointer.namehash = keys.named_namehash)
               AND (pointer.resolver_address = keys.resolver_address
                    OR pointer.resolver_address IN (
                        SELECT mirror.resolver_address FROM mirrors mirror
                        WHERE mirror.chain_id = keys.chain_id)))
              OR (keys.record_id IS NOT NULL
                  AND pointer.resolver_address = keys.resolver_address)
          )
         GROUP BY pointer.chain_id, pointer.resource_id, keys.coin_type";

type CandidateRow = (String, Uuid, String, bool);

fn current_candidates() -> &'static str {
    bigname_storage::families::records::seams::candidate_statement()
}

async fn candidate_rows(
    pool: &PgPool,
    statement: &str,
    address: &str,
    coin_types: &[String],
) -> Result<Vec<CandidateRow>> {
    let query = sqlx::query_as(statement).bind(address).bind(coin_types);
    // The current statement also binds the requested namespaces, for rows written under no name.
    let query = if statement.contains("$3") {
        query.bind(["ens", "basenames"].map(str::to_owned).to_vec())
    } else {
        query
    };
    let mut rows: Vec<CandidateRow> = query.fetch_all(pool).await?;
    rows.sort();
    Ok(rows)
}

/// Every address the inverse index lists finds the same rows through both statements, for each
/// of its coin types alone, for all of them, and for a coin type it has no row for. Returns the
/// rows found.
async fn assert_candidates_unchanged(database: &TestDatabase) -> Result<Vec<CandidateRow>> {
    let addresses: Vec<(String, Vec<String>)> = sqlx::query_as(
        "SELECT address, array_agg(DISTINCT coin_type ORDER BY coin_type)
         FROM (SELECT address, coin_type FROM bigname_phase.project_address_record_node_index
               UNION ALL
               SELECT address, coin_type FROM bigname_phase.project_address_record_id_index) key
         GROUP BY address ORDER BY address",
    )
    .fetch_all(&database.pool)
    .await?;
    assert!(!addresses.is_empty(), "the fixture indexes no address");
    let mut found = Vec::new();
    for (address, coin_types) in addresses {
        let mut sets: Vec<Vec<String>> = coin_types.iter().map(|coin| vec![coin.clone()]).collect();
        sets.push(coin_types.clone());
        sets.push(vec!["999999".to_owned()]);
        for set in sets {
            let previous =
                candidate_rows(&database.pool, PREVIOUS_CANDIDATES, &address, &set).await?;
            let current =
                candidate_rows(&database.pool, current_candidates(), &address, &set).await?;
            assert_eq!(current, previous, "{address} {set:?}");
            found.extend(current);
        }
    }
    Ok(found)
}

fn found_resources(rows: &[CandidateRow]) -> std::collections::BTreeSet<Uuid> {
    rows.iter().map(|row| row.1).collect()
}

/// Row 1: the resolves-to fixture, with names on coin 60, other EVM coin types and Base.
#[tokio::test]
async fn v2_resolves_to_candidates_match_on_the_resolves_to_fixture() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_evm_records(&database).await?;
    let found = assert_candidates_unchanged(&database).await?;
    for coin in ["60", "61", V2_RESOLVES_TO_BASE_COIN] {
        assert!(found.iter().any(|row| row.2 == coin), "{coin}: {found:?}");
    }
    database.cleanup().await
}

/// Row 2: a name found through its ENSIP-19 default EVM address.
#[tokio::test]
async fn v2_resolves_to_candidates_match_through_the_default_address() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    let found = assert_candidates_unchanged(&database).await?;
    assert!(
        found.iter().any(|row| row.2 == V2_ENSIP19_DEFAULT_COIN),
        "{found:?}"
    );
    database.cleanup().await
}

/// Row 3: an ENSv2 root-registry TLD served through its root resolver pointer alone.
#[tokio::test]
async fn v2_resolves_to_candidates_match_on_an_ens_v2_root_pointer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    seed_v2_lookup_resolves_to_records(&database, address).await?;
    seed_resolves_to_root_tld(&database, "eth", address).await?;
    let found = assert_candidates_unchanged(&database).await?;
    assert!(
        found_resources(&found).contains(&Uuid::from_u128(0x7d0100)),
        "{found:?}"
    );
    database.cleanup().await
}

/// Row 4: an ENSv2 name whose pointer names a declared ENSv1 mirror resolver, found through the
/// mirrored resolver's records for its node.
#[tokio::test]
async fn v2_resolves_to_candidates_match_through_a_mirror_resolver() -> Result<()> {
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    let database =
        v2_mirror_records_database("alice.eth", MIRROR, MirrorFixtureSource::Exact, "resolver")
            .await?;
    let found = assert_candidates_unchanged(&database).await?;
    assert!(
        found_resources(&found).contains(&Uuid::from_u128(0x6100)),
        "{found:?}"
    );
    database.cleanup().await
}

/// Row 5: a record-id resolver's values, which every pointer at the resolver can serve.
#[tokio::test]
async fn v2_resolves_to_candidates_match_on_a_record_id_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_record_id_resolver_inputs(
        &database,
        &[family_fixture_record_write(
            "addr:60",
            Some(json!("0x3333333333333333333333333333333333333333")),
        )],
        false,
    )
    .await?;
    let records: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bigname_phase.project_address_record_id_index")
            .fetch_one(&database.pool)
            .await?;
    assert!(records > 0, "the fixture writes no record-id value");
    assert!(!assert_candidates_unchanged(&database).await?.is_empty());
    database.cleanup().await
}

/// Row 6: the bulk fixture, whose names resolve on coin 60, the default address and coin 61, with
/// aliases.
#[tokio::test]
async fn v2_resolves_to_candidates_match_on_the_bulk_fixture() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 300, &[(10, 200), (20, 21)], bulk_resolving).await?;
    let found = assert_candidates_unchanged(&database).await?;
    // Two of the 300 names are bound to another name's registration.
    let resources = found_resources(&found).len();
    assert!(resources > 250, "{resources}");
    database.cleanup().await
}

const CANDIDATE_ADDRESS: &str = "0x00000000000000000000000000000000000c0de1";
const CANDIDATE_RESOLVER: &str = "0x00000000000000000000000000000000000c0de2";
const CANDIDATE_MIRROR: &str = "0x00000000000000000000000000000000000c0de3";
const CANDIDATE_OTHER_RESOLVER: &str = "0x00000000000000000000000000000000000c0de4";
const CANDIDATE_CHAIN: &str = "ethereum-mainnet";
const CANDIDATE_OTHER_CHAIN: &str = "ethereum-sepolia";
const CANDIDATE_NODE: &str = "0x00000000000000000000000000000000000000000000000000000000000c0de5";
const CANDIDATE_NAMED: &str = "0x00000000000000000000000000000000000000000000000000000000000c0de6";

async fn insert_candidate_node_key(
    database: &TestDatabase,
    chain: &str,
    coin: &str,
    resolver: &str,
    node: &str,
    logical_name_id: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_record_node_index
            (address, coin_type, chain_id, resolver_address, node, logical_name_id)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(CANDIDATE_ADDRESS)
    .bind(coin)
    .bind(chain)
    .bind(resolver)
    .bind(node)
    .bind(logical_name_id)
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn insert_candidate_record_key(
    database: &TestDatabase,
    chain: &str,
    coin: &str,
    resolver: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_record_id_index
            (address, coin_type, chain_id, resolver_address, record_id)
         VALUES ($1, $2, $3, $4, '0x01')",
    )
    .bind(CANDIDATE_ADDRESS)
    .bind(coin)
    .bind(chain)
    .bind(resolver)
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn insert_candidate_pointer(
    database: &TestDatabase,
    chain: &str,
    resource: u128,
    resolver: &str,
    namehash: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_resource_pointer
            (chain_id, resource_id, block_number, event_identity, resolver_address, namehash)
         VALUES ($1, $2, 1, $3, $4, $5)",
    )
    .bind(chain)
    .bind(Uuid::from_u128(resource))
    .bind(format!("candidate-pointer-{resource:x}"))
    .bind(resolver)
    .bind(namehash)
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn insert_candidate_resolver_role(
    database: &TestDatabase,
    chain: &str,
    resolver: &str,
    role: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_resolver_classification
            (chain_id, resolver_address, block_number, event_identity, classification,
             support_status)
         VALUES ($1, $2, 1, $3, $4, 'supported')",
    )
    .bind(chain)
    .bind(resolver)
    .bind(format!("candidate-role-{chain}-{resolver}"))
    .bind(json!({ "role": role }))
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// The current statement's candidates of [`CANDIDATE_ADDRESS`] for `coin_types`, as chain,
/// resource and coin type, after checking both statements agree on every input.
async fn synthetic_candidates(
    database: &TestDatabase,
    coin_types: &[&str],
) -> Result<Vec<(String, u128, String)>> {
    assert_candidates_unchanged(database).await?;
    let coin_types: Vec<String> = coin_types.iter().map(|coin| (*coin).to_owned()).collect();
    let rows = candidate_rows(
        &database.pool,
        current_candidates(),
        CANDIDATE_ADDRESS,
        &coin_types,
    )
    .await?;
    assert!(rows.iter().all(|row| row.3), "{rows:?}");
    Ok(rows
        .into_iter()
        .map(|(chain, resource, coin, _)| (chain, resource.as_u128(), coin))
        .collect())
}

fn candidate(chain: &str, resource: u128, coin: &str) -> (String, u128, String) {
    (chain.to_owned(), resource, coin.to_owned())
}

/// Row 7: a node-keyed row reaches the pointers at its node and resolver, and no pointer at its
/// node on another resolver or at its resolver on another node.
#[tokio::test]
async fn v2_resolves_to_candidates_node_piece_contributes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (chain, node) = (CANDIDATE_CHAIN, CANDIDATE_NODE);
    insert_candidate_node_key(&database, chain, "60", CANDIDATE_RESOLVER, node, "").await?;
    insert_candidate_pointer(&database, chain, 0xc1, CANDIDATE_RESOLVER, Some(node)).await?;
    insert_candidate_pointer(&database, chain, 0xc2, CANDIDATE_OTHER_RESOLVER, Some(node)).await?;
    let named = Some(CANDIDATE_NAMED);
    insert_candidate_pointer(&database, chain, 0xc3, CANDIDATE_RESOLVER, named).await?;
    insert_candidate_pointer(&database, chain, 0xc4, CANDIDATE_RESOLVER, None).await?;
    assert_eq!(
        synthetic_candidates(&database, &["60"]).await?,
        [candidate(chain, 0xc1, "60")]
    );
    database.cleanup().await
}

/// Row 8: a row written under a name reaches the pointers at that name's namehash, compared in
/// lower case, though its node differs.
#[tokio::test]
async fn v2_resolves_to_candidates_named_piece_contributes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let chain = CANDIDATE_CHAIN;
    let logical = format!(
        "ens:{}",
        CANDIDATE_NAMED.to_ascii_uppercase().replace("0X", "0x")
    );
    insert_candidate_node_key(
        &database,
        chain,
        "60",
        CANDIDATE_RESOLVER,
        CANDIDATE_NODE,
        &logical,
    )
    .await?;
    let named = Some(CANDIDATE_NAMED);
    insert_candidate_pointer(&database, chain, 0xc1, CANDIDATE_RESOLVER, named).await?;
    insert_candidate_pointer(&database, chain, 0xc2, CANDIDATE_OTHER_RESOLVER, named).await?;
    assert_eq!(
        synthetic_candidates(&database, &["60"]).await?,
        [candidate(chain, 0xc1, "60")]
    );
    database.cleanup().await
}

/// Row 9: a node-keyed row reaches the pointers at a mirror resolver of its chain, at its node
/// and at its name's namehash, and none at a resolver of another role.
#[tokio::test]
async fn v2_resolves_to_candidates_mirror_piece_contributes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let chain = CANDIDATE_CHAIN;
    let logical = format!("ens:{CANDIDATE_NAMED}");
    insert_candidate_node_key(
        &database,
        chain,
        "60",
        CANDIDATE_RESOLVER,
        CANDIDATE_NODE,
        &logical,
    )
    .await?;
    insert_candidate_resolver_role(&database, chain, CANDIDATE_MIRROR, "ensv1_mirror_resolver")
        .await?;
    insert_candidate_resolver_role(&database, chain, CANDIDATE_OTHER_RESOLVER, "resolver").await?;
    let (node, named) = (Some(CANDIDATE_NODE), Some(CANDIDATE_NAMED));
    insert_candidate_pointer(&database, chain, 0xc1, CANDIDATE_MIRROR, node).await?;
    insert_candidate_pointer(&database, chain, 0xc2, CANDIDATE_MIRROR, named).await?;
    insert_candidate_pointer(&database, chain, 0xc3, CANDIDATE_OTHER_RESOLVER, node).await?;
    assert_eq!(
        synthetic_candidates(&database, &["60"]).await?,
        [candidate(chain, 0xc1, "60"), candidate(chain, 0xc2, "60")]
    );
    database.cleanup().await
}

/// Row 10: a record-id row reaches every pointer at its resolver, whatever its node, and none at
/// a mirror resolver.
#[tokio::test]
async fn v2_resolves_to_candidates_record_id_piece_contributes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let chain = CANDIDATE_CHAIN;
    insert_candidate_record_key(&database, chain, "60", CANDIDATE_RESOLVER).await?;
    insert_candidate_resolver_role(&database, chain, CANDIDATE_MIRROR, "ensv1_mirror_resolver")
        .await?;
    let node = Some(CANDIDATE_NODE);
    insert_candidate_pointer(&database, chain, 0xc1, CANDIDATE_RESOLVER, node).await?;
    insert_candidate_pointer(&database, chain, 0xc2, CANDIDATE_RESOLVER, None).await?;
    insert_candidate_pointer(&database, chain, 0xc3, CANDIDATE_MIRROR, node).await?;
    insert_candidate_pointer(&database, chain, 0xc4, CANDIDATE_OTHER_RESOLVER, node).await?;
    assert_eq!(
        synthetic_candidates(&database, &["60"]).await?,
        [candidate(chain, 0xc1, "60"), candidate(chain, 0xc2, "60")]
    );
    database.cleanup().await
}

/// Row 11: a pointer every piece reaches, through a resolver that is itself declared a mirror,
/// is one candidate per coin type.
#[tokio::test]
async fn v2_resolves_to_candidates_dedupe_a_pointer_two_pieces_reach() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (chain, node) = (CANDIDATE_CHAIN, CANDIDATE_NODE);
    let logical = format!("ens:{node}");
    for coin in ["60", "2147483648"] {
        insert_candidate_node_key(&database, chain, coin, CANDIDATE_RESOLVER, node, &logical)
            .await?;
        insert_candidate_record_key(&database, chain, coin, CANDIDATE_RESOLVER).await?;
    }
    insert_candidate_resolver_role(
        &database,
        chain,
        CANDIDATE_RESOLVER,
        "ensv1_mirror_resolver",
    )
    .await?;
    insert_candidate_pointer(&database, chain, 0xc1, CANDIDATE_RESOLVER, Some(node)).await?;
    assert_eq!(
        synthetic_candidates(&database, &["60", "2147483648"]).await?,
        [
            candidate(chain, 0xc1, "2147483648"),
            candidate(chain, 0xc1, "60")
        ]
    );
    database.cleanup().await
}

/// Row 12: a row reaches only pointers of its own chain, through its own chain's mirrors.
#[tokio::test]
async fn v2_resolves_to_candidates_never_pair_across_chains() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (chain, other, node) = (CANDIDATE_CHAIN, CANDIDATE_OTHER_CHAIN, CANDIDATE_NODE);
    let logical = format!("ens:{CANDIDATE_NAMED}");
    insert_candidate_node_key(&database, chain, "60", CANDIDATE_RESOLVER, node, &logical).await?;
    insert_candidate_record_key(&database, other, "60", CANDIDATE_OTHER_RESOLVER).await?;
    insert_candidate_resolver_role(&database, other, CANDIDATE_MIRROR, "ensv1_mirror_resolver")
        .await?;
    let named = Some(CANDIDATE_NAMED);
    insert_candidate_pointer(&database, chain, 0xc1, CANDIDATE_RESOLVER, Some(node)).await?;
    insert_candidate_pointer(&database, other, 0xc2, CANDIDATE_RESOLVER, Some(node)).await?;
    insert_candidate_pointer(&database, other, 0xc3, CANDIDATE_RESOLVER, named).await?;
    insert_candidate_pointer(&database, chain, 0xc4, CANDIDATE_MIRROR, Some(node)).await?;
    insert_candidate_pointer(&database, chain, 0xc5, CANDIDATE_OTHER_RESOLVER, None).await?;
    insert_candidate_pointer(&database, other, 0xc6, CANDIDATE_OTHER_RESOLVER, None).await?;
    assert_eq!(
        synthetic_candidates(&database, &["60"]).await?,
        [candidate(chain, 0xc1, "60"), candidate(other, 0xc6, "60")]
    );
    database.cleanup().await
}

/// Row 13: with no mirror resolver declared, a pointer at the mirror fixture's mirror is no
/// candidate, and the pointers at a row's own resolver still are.
#[tokio::test]
async fn v2_resolves_to_candidates_match_with_an_empty_mirror_set() -> Result<()> {
    const MIRROR: &str = "0x1010101010101010101010101010101010101010";
    let database =
        v2_mirror_records_database("alice.eth", MIRROR, MirrorFixtureSource::Exact, "resolver")
            .await?;
    let removed = sqlx::query(
        "DELETE FROM bigname_phase.project_resolver_classification
         WHERE classification ->> 'role' = 'ensv1_mirror_resolver'",
    )
    .execute(&database.pool)
    .await?
    .rows_affected();
    assert!(removed > 0);
    let found = assert_candidates_unchanged(&database).await?;
    assert!(
        !found_resources(&found).contains(&Uuid::from_u128(0x6100)),
        "{found:?}"
    );
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    let (chain, node) = (CANDIDATE_CHAIN, CANDIDATE_NODE);
    insert_candidate_node_key(&database, chain, "60", CANDIDATE_RESOLVER, node, "").await?;
    insert_candidate_pointer(&database, chain, 0xc1, CANDIDATE_RESOLVER, Some(node)).await?;
    insert_candidate_pointer(&database, chain, 0xc2, CANDIDATE_MIRROR, Some(node)).await?;
    assert_eq!(
        synthetic_candidates(&database, &["60"]).await?,
        [candidate(chain, 0xc1, "60")]
    );
    database.cleanup().await
}

/// Row 14: the statement's joins are equality joins, with no OR filter and no per-row subplan,
/// on the bulk fixture.
#[tokio::test]
async fn v2_resolves_to_candidates_plan_joins_on_equalities() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 40, &[], bulk_resolving).await?;
    let coin_types = vec!["60".to_owned(), V2_ENSIP19_DEFAULT_COIN.to_owned()];
    let plan: Value =
        explain_candidates(&mut *database.pool.acquire().await?, &coin_types, &["ens"]).await?;
    let nodes = plan_nodes(&plan);
    assert!(
        nodes.iter().any(|node| node.get("Join Type").is_some()),
        "{plan:#}"
    );
    for node in nodes {
        for filter in [
            "Join Filter",
            "Filter",
            "Hash Cond",
            "Merge Cond",
            "Index Cond",
        ] {
            let text = node[filter].as_str().unwrap_or_default();
            assert!(!text.contains(" OR "), "{filter}: {text}\n{plan:#}");
        }
        // A CTE is an init plan run once. A subplan runs per outer row.
        assert_ne!(node["Parent Relationship"], json!("SubPlan"), "{plan:#}");
    }
    database.cleanup().await
}

/// The current statement's plan for [`BULK_ADDRESS`], binding `namespaces` as the namespaces
/// the request spans when the statement takes them.
async fn explain_candidates(
    conn: &mut sqlx::PgConnection,
    coin_types: &[String],
    namespaces: &[&str],
) -> Result<Value> {
    explain_candidates_of(conn, BULK_ADDRESS, coin_types, namespaces).await
}

/// [`explain_candidates`] for `address`.
async fn explain_candidates_of(
    conn: &mut sqlx::PgConnection,
    address: &str,
    coin_types: &[String],
    namespaces: &[&str],
) -> Result<Value> {
    let statement = format!("EXPLAIN (FORMAT JSON, VERBOSE) {}", current_candidates());
    let query = sqlx::query_scalar(&statement)
        .bind(address)
        .bind(coin_types);
    let query = if statement.contains("$3") {
        query.bind(
            namespaces
                .iter()
                .map(|namespace| (*namespace).to_owned())
                .collect::<Vec<_>>(),
        )
    } else {
        query
    };
    Ok(query.fetch_one(conn).await?)
}

async fn insert_candidate_registry_pointer(
    database: &TestDatabase,
    chain: &str,
    node: &str,
    resolver: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_registry_pointer
            (chain_id, namespace, node, block_number, event_identity, resolver_address,
             source_family)
         VALUES ($1, 'ens', $2, 2, $3, $4, 'ens_v1_registry_l1')",
    )
    .bind(chain)
    .bind(node)
    .bind(format!("candidate-registry-pointer-{node}"))
    .bind(resolver)
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn insert_candidate_family_pointer(
    database: &TestDatabase,
    chain: &str,
    resource: u128,
    resolver: &str,
    namehash: &str,
    source_family: &str,
) -> Result<()> {
    insert_candidate_pointer(database, chain, resource, resolver, Some(namehash)).await?;
    sqlx::query(
        "UPDATE bigname_phase.project_resource_pointer
         SET namespace = 'ens', source_family = $3
         WHERE chain_id = $1 AND resource_id = $2",
    )
    .bind(chain)
    .bind(Uuid::from_u128(resource))
    .bind(source_family)
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// Row 15: a node-keyed row reaches the ENSv1 pointers at its node through the node's registry
/// pointer at its resolver, though their own pointer names another resolver, as the forward read
/// serves them through it. A row written under a name takes the name's namespace, a row written
/// under none each requested namespace. An ENSv2 pointer at the node, an ENSv1 pointer at another
/// node and a node whose registry pointer names another resolver are not reached.
#[tokio::test]
async fn v2_resolves_to_candidates_registry_piece_contributes() -> Result<()> {
    let (chain, node, other) = (CANDIDATE_CHAIN, CANDIDATE_NODE, CANDIDATE_NAMED);
    for named in [false, true] {
        let logical = |node: &str| {
            if named {
                format!("ens:{node}")
            } else {
                String::new()
            }
        };
        let database = TestDatabase::new_migrated().await?;
        insert_candidate_node_key(
            &database,
            chain,
            "60",
            CANDIDATE_RESOLVER,
            node,
            &logical(node),
        )
        .await?;
        insert_candidate_node_key(
            &database,
            chain,
            "60",
            CANDIDATE_RESOLVER,
            other,
            &logical(other),
        )
        .await?;
        insert_candidate_registry_pointer(&database, chain, node, CANDIDATE_RESOLVER).await?;
        insert_candidate_registry_pointer(&database, chain, other, CANDIDATE_OTHER_RESOLVER)
            .await?;
        let stale = CANDIDATE_OTHER_RESOLVER;
        insert_candidate_family_pointer(&database, chain, 0xd1, stale, node, "ens_v1_registrar_l1")
            .await?;
        insert_candidate_family_pointer(&database, chain, 0xd2, stale, node, "ens_v2_registry_l1")
            .await?;
        insert_candidate_family_pointer(
            &database,
            chain,
            0xd3,
            stale,
            other,
            "ens_v1_registrar_l1",
        )
        .await?;
        let rows = candidate_rows(
            &database.pool,
            current_candidates(),
            CANDIDATE_ADDRESS,
            &["60".to_owned()],
        )
        .await?;
        let found: Vec<(String, u128, String)> = rows
            .into_iter()
            .map(|(chain, resource, coin, _)| (chain, resource.as_u128(), coin))
            .collect();
        assert_eq!(found, [candidate(chain, 0xd1, "60")], "named {named}");
        database.cleanup().await?;
    }
    Ok(())
}

/// Row 16: the node and named registry pieces read the registry pointer through its primary key,
/// with the chain, the namespace and the node all bound, the namespace from the row's name or
/// from the requested set, and never walk the resolver index, which for a shared resolver holds
/// every node it serves. It holds for a request in one namespace and for one spanning every
/// servable namespace, as a request naming none does, with rows written under no name among the
/// keys. Sequential scans and hash and merge joins are off, so the plan shows the index path the
/// statement can bind.
#[tokio::test]
async fn v2_resolves_to_candidates_registry_piece_binds_the_primary_key() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_resolves_to_names(&database, 40, &[], bulk_resolving).await?;
    sqlx::query(
        "UPDATE bigname_phase.project_address_record_node_index SET logical_name_id = ''
         WHERE address = $1 AND node IN (
             SELECT node FROM bigname_phase.project_address_record_node_index
             WHERE address = $1 ORDER BY node LIMIT 5)",
    )
    .bind(BULK_ADDRESS)
    .execute(&database.pool)
    .await?;
    let coin_types = vec!["60".to_owned(), V2_ENSIP19_DEFAULT_COIN.to_owned()];
    for namespaces in [&["ens"][..], &["basenames", "ens"][..]] {
        let mut transaction = database.pool.begin().await?;
        for setting in ["enable_seqscan", "enable_hashjoin", "enable_mergejoin"] {
            sqlx::query(&format!("SET LOCAL {setting} = off"))
                .execute(&mut *transaction)
                .await?;
        }
        let plan = explain_candidates(&mut transaction, &coin_types, namespaces).await?;
        transaction.rollback().await?;
        // The node and named pieces' registry reads, `registry` to `registry_2` in statement
        // order. Row 17 covers the record-id piece's `registry_3`.
        let registry: Vec<&Value> = plan_nodes(&plan)
            .into_iter()
            .filter(|node| {
                node["Index Name"]
                    .as_str()
                    .is_some_and(|index| index.starts_with("project_registry_pointer"))
                    && ["registry", "registry_1", "registry_2"]
                        .contains(&node["Alias"].as_str().unwrap_or(""))
            })
            .collect();
        // One read per piece: rows written under a name and under none at their node, and rows
        // written under a name at another node.
        assert_eq!(registry.len(), 3, "{namespaces:?}: {plan:#}");
        for node in registry {
            assert_eq!(
                node["Index Name"], "project_registry_pointer_pkey",
                "{namespaces:?}: {plan:#}"
            );
            let alias = node["Alias"].as_str().unwrap_or_default();
            let condition = node["Index Cond"].as_str().unwrap_or_default();
            for column in ["chain_id", "namespace", "node"] {
                let part = format!("({alias}.{column} = ");
                assert!(
                    condition.contains(&part),
                    "{namespaces:?} {part}: {condition}\n{plan:#}"
                );
            }
        }
    }
    database.cleanup().await
}

/// Row 17: a record-id row, a linked record, reaches the ENSv1 pointers at every node whose
/// registry pointer names its resolver, though their own pointer names another resolver. An
/// ENSv2 pointer at such a node and an ENSv1 pointer at a node whose registry pointer names
/// another resolver are not reached. On a chain with many other names, and with sequential
/// scans and hash and merge joins off, the piece reads the registry pointers at the resolver
/// through the resolver index and the resource pointers by node, and no table of the statement
/// is read by a sequential scan.
#[tokio::test]
async fn v2_resolves_to_candidates_record_id_registry_piece() -> Result<()> {
    let (chain, node, other) = (CANDIDATE_CHAIN, CANDIDATE_NODE, CANDIDATE_NAMED);
    let stale = CANDIDATE_OTHER_RESOLVER;
    let database = TestDatabase::new_migrated().await?;
    insert_candidate_record_key(&database, chain, "60", CANDIDATE_RESOLVER).await?;
    insert_candidate_registry_pointer(&database, chain, node, CANDIDATE_RESOLVER).await?;
    insert_candidate_registry_pointer(&database, chain, other, stale).await?;
    insert_candidate_family_pointer(&database, chain, 0xe1, stale, node, "ens_v1_registrar_l1")
        .await?;
    insert_candidate_family_pointer(&database, chain, 0xe2, stale, node, "ens_v2_registry_l1")
        .await?;
    insert_candidate_family_pointer(&database, chain, 0xe3, stale, other, "ens_v1_registrar_l1")
        .await?;
    let rows = candidate_rows(
        &database.pool,
        current_candidates(),
        CANDIDATE_ADDRESS,
        &["60".to_owned()],
    )
    .await?;
    let found: Vec<(String, u128, String)> = rows
        .into_iter()
        .map(|(chain, resource, coin, _)| (chain, resource.as_u128(), coin))
        .collect();
    assert_eq!(found, [candidate(chain, 0xe1, "60")]);
    // Other names on the chain, each with an ENSv1 pointer and a registry pointer at another
    // resolver, so the planner sizes a chain-wide read of either table by its statistics.
    sqlx::query(
        "INSERT INTO bigname_phase.project_resource_pointer
            (chain_id, resource_id, block_number, event_identity, resolver_address, namehash,
             namespace, source_family)
         SELECT $1, md5('filler-' || i)::uuid, 1, 'filler-pointer-' || i, $2,
                '0x' || lpad(to_hex(i), 64, '0'), 'ens', 'ens_v1_registrar_l1'
         FROM generate_series(1, 2000) i",
    )
    .bind(chain)
    .bind(CANDIDATE_MIRROR)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_registry_pointer
            (chain_id, namespace, node, block_number, event_identity, resolver_address,
             source_family)
         SELECT $1, 'ens', '0x' || lpad(to_hex(i), 64, '0'), 2, 'filler-registry-' || i, $2,
                'ens_v1_registry_l1'
         FROM generate_series(1, 2000) i",
    )
    .bind(chain)
    .bind(CANDIDATE_MIRROR)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "ANALYZE bigname_phase.project_resource_pointer, bigname_phase.project_registry_pointer,
             bigname_phase.project_address_record_id_index",
    )
    .execute(&database.pool)
    .await?;
    let mut transaction = database.pool.begin().await?;
    for setting in ["enable_seqscan", "enable_hashjoin", "enable_mergejoin"] {
        sqlx::query(&format!("SET LOCAL {setting} = off"))
            .execute(&mut *transaction)
            .await?;
    }
    let plan = explain_candidates_of(
        &mut transaction,
        CANDIDATE_ADDRESS,
        &["60".to_owned()],
        &["ens"],
    )
    .await?;
    transaction.rollback().await?;
    let nodes = plan_nodes(&plan);
    assert!(
        nodes.iter().all(|node| node["Node Type"] != "Seq Scan"),
        "{plan:#}"
    );
    // The record-id piece's reads, `registry_3` and `pointer_5` in statement order.
    // A bitmap heap scan reads through the bitmap index scan below it.
    let read = |alias: &str| {
        let node = nodes
            .iter()
            .find(|node| node["Alias"] == alias)
            .copied()
            .unwrap_or_else(|| panic!("no {alias} read: {plan:#}"));
        if node["Node Type"] == "Bitmap Heap Scan" {
            &node["Plans"][0]
        } else {
            node
        }
    };
    for (alias, index, columns) in [
        (
            "registry_3",
            "project_registry_pointer_resolver_idx",
            &["chain_id", "resolver_address"][..],
        ),
        (
            "pointer_5",
            "project_resource_pointer_hydration_node_idx",
            &["chain_id", "namespace", "namehash"][..],
        ),
    ] {
        let node = read(alias);
        assert_eq!(node["Index Name"], index, "{alias}: {plan:#}");
        let condition = node["Index Cond"].as_str().unwrap_or_default();
        for column in columns {
            let part = format!("({alias}.{column} = ");
            assert!(condition.contains(&part), "{alias} {part}: {condition}\n{plan:#}");
        }
    }
    database.cleanup().await
}

/// Row 18: a row written under a name at another node, which the forward read's named arm admits
/// by the logical name alone, reaches the ENSv1 pointers of the name's namehash through that
/// namehash's registry pointer at the row's resolver, though their own pointer names another
/// resolver. An ENSv2 pointer there, and the name's pointers when its registry pointer names
/// another resolver, are not reached.
#[tokio::test]
async fn v2_resolves_to_candidates_named_registry_piece_contributes() -> Result<()> {
    let (chain, node, named) = (CANDIDATE_CHAIN, CANDIDATE_NODE, CANDIDATE_NAMED);
    let stale = CANDIDATE_OTHER_RESOLVER;
    for registry_resolver in [CANDIDATE_RESOLVER, stale] {
        let database = TestDatabase::new_migrated().await?;
        insert_candidate_node_key(
            &database,
            chain,
            "60",
            CANDIDATE_RESOLVER,
            node,
            &format!("ens:{named}"),
        )
        .await?;
        insert_candidate_registry_pointer(&database, chain, named, registry_resolver).await?;
        insert_candidate_family_pointer(&database, chain, 0xf1, stale, named, "ens_v1_registrar_l1")
            .await?;
        insert_candidate_family_pointer(&database, chain, 0xf2, stale, named, "ens_v2_registry_l1")
            .await?;
        let rows = candidate_rows(
            &database.pool,
            current_candidates(),
            CANDIDATE_ADDRESS,
            &["60".to_owned()],
        )
        .await?;
        let found: Vec<(String, u128, String)> = rows
            .into_iter()
            .map(|(chain, resource, coin, _)| (chain, resource.as_u128(), coin))
            .collect();
        let expected = if registry_resolver == CANDIDATE_RESOLVER {
            vec![candidate(chain, 0xf1, "60")]
        } else {
            Vec::new()
        };
        assert_eq!(found, expected, "registry at {registry_resolver}");
        database.cleanup().await?;
    }
    Ok(())
}
