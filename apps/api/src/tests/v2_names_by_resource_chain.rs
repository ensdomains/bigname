// The current name of each resource on a permission page or a resolver's role list
// (`load_current_names_by_resource_ids`, which serves the family load's
// `load_family_names_by_resource_ids`), read only on the chains of the caller's publication.
//
// The rows are compared with the lookup as it was before it took the chains
// ([`names_by_resource_unkeyed`]) on the same fixture, and the plan of the lookup is read from
// the statement the function actually sends.

const BY_RESOURCE_BASE: &str = "base-mainnet";
/// Candidate rows of the plan fixture on the main chain. Base gets a tenth as many.
const BY_RESOURCE_PLAN_ROWS: i64 = 200_000;
const BY_RESOURCE_PLAN_OFFSET: u128 = 0x7b00_0000_0000;

/// The served names of the shared resource's names, in name id order. They differ only by case,
/// and two pairs tie. Bytewise `PICK.eth` sorts first (the 2nd and 4th ids). Under a
/// lowercase-first collation such as ICU's, `pick.eth` does (the 3rd and 5th). The 1st id
/// carries `Pick.eth`, which sorts second under both. So the name id order, the bytewise order
/// and the ICU order each pick a different name. Within a tie the lower id wins.
const BY_RESOURCE_SHARED_NAMES: [&str; 5] =
    ["Pick.eth", "PICK.eth", "pick.eth", "PICK.eth", "pick.eth"];

fn by_resource_chains() -> Vec<String> {
    vec![FAMILY_CHAIN.to_owned(), BY_RESOURCE_BASE.to_owned()]
}

/// The resources of the composed fixture.
struct ByResourceFixture {
    /// alpha.eth's resource: one name.
    alpha: Uuid,
    /// beta.eth's resource: one name.
    beta: Uuid,
    /// One resource bound to five names.
    shared: Uuid,
    /// The shared resource's names in name id order, each with its served name
    /// ([`BY_RESOURCE_SHARED_NAMES`]).
    shared_names: Vec<(String, &'static str)>,
    /// pick.base.eth's resource, on Base.
    base: Uuid,
    /// A resource no name is bound to.
    nameless: Uuid,
}

/// alpha.eth and beta.eth with their registrations, five names bound to one resource, a
/// Basenames name on Base and a nameless resource, published at 240 on both chains.
async fn seed_by_resource_fixture(database: &TestDatabase) -> Result<ByResourceFixture> {
    let (_, _, alpha) = seed_family_names_events(database).await?;
    let beta: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM bigname_phase.surface_bindings WHERE logical_name_id = $1",
    )
    .bind(bigname_storage::logical_name_id_for_name("ens", "beta.eth"))
    .fetch_one(&database.pool)
    .await?;

    let mut rows = [
        "pick-one.eth",
        "pick-two.eth",
        "pick-three.eth",
        "pick-four.eth",
        "pick-five.eth",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, name)| {
        family_name_rows(
            name,
            0x7a1_0000 + 0x1_0000 * index as u128,
            "ens_v1",
            "ens",
            FAMILY_CHAIN,
            FAMILY_FIRST_BLOCK,
        )
    })
    .collect::<Result<Vec<_>>>()?;
    let shared = rows[0].resource.resource_id;
    for row in &mut rows[1..] {
        row.binding.resource_id = shared;
    }
    let mut ids = rows
        .iter()
        .map(|row| row.surface.logical_name_id.clone())
        .collect::<Vec<_>>();
    ids.sort();
    let shared_names = ids
        .iter()
        .cloned()
        .zip(BY_RESOURCE_SHARED_NAMES)
        .collect::<Vec<_>>();
    upsert_family_name_rows(database, rows).await?;
    // As Interpret's identity write does, the surfaces and their search documents change
    // together.
    let paths: Vec<Vec<String>> = sqlx::query_scalar(
        "SELECT labelhashes FROM bigname_phase.name_surfaces
         WHERE logical_name_id = ANY($1) ORDER BY logical_name_id",
    )
    .bind(&ids)
    .fetch_all(&database.pool)
    .await?;
    let mut transaction = database.pool.begin().await?;
    bigname_storage::identity_search::prepare(
        &mut transaction,
        bigname_storage::identity_search::LabelWrite::NONE,
        &paths,
        &ids,
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces surface SET raw_name = served.raw_name
         FROM unnest($1::text[], $2::text[]) AS served(logical_name_id, raw_name)
         WHERE surface.logical_name_id = served.logical_name_id",
    )
    .bind(&ids)
    .bind(&BY_RESOURCE_SHARED_NAMES[..])
    .execute(&mut *transaction)
    .await?;
    bigname_storage::identity_search::refresh(&mut transaction, &ids, &[]).await?;
    transaction.commit().await?;

    let blocks = (200..=241)
        .map(|number| {
            raw_block(
                BY_RESOURCE_BASE,
                &format!("0xhistory{number}"),
                (number > 200)
                    .then(|| format!("0xhistory{}", number - 1))
                    .as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_schema_v2_lookup_head(
        &database.pool,
        BY_RESOURCE_BASE,
        240,
        "0xhistory240",
        &bigname_storage::UnixSeconds::from(OffsetDateTime::from_unix_timestamp(1_700_000_240)?)
            .internal_string(),
    )
    .await?;
    let (_, base) = seed_family_name_on(
        database,
        "pick.base.eth",
        0x7a8_0000,
        "basenames",
        "basenames",
        BY_RESOURCE_BASE,
    )
    .await?;

    let nameless = Uuid::from_u128(0x7a9_0000);
    sqlx::query(
        "INSERT INTO bigname_phase.resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, $2, '0xhistory212', 212, 'canonical')",
    )
    .bind(nameless)
    .bind(FAMILY_CHAIN)
    .execute(&database.pool)
    .await?;

    publish_test_families(database, 240).await?;
    sqlx::query(
        "INSERT INTO chain_phase_state (chain_id, phase_name, phase_status, current_block_number,
             current_block_hash, target_block_number, target_block_hash, input_content_hash,
             started_at, finished_at)
         VALUES ($1, 'interpret', 'completed', 240, '0xhistory240', 240, '0xhistory240', $2,
                 now(), now())
         ON CONFLICT (chain_id, phase_name) DO NOTHING",
    )
    .bind(BY_RESOURCE_BASE)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.pool)
    .await?;
    publish_test_families_on(&database.pool, BY_RESOURCE_BASE, 240).await?;

    // Each class is reached: the five names compose onto the shared resource.
    let composed =
        bigname_storage::load_name_current_by_logical_name_ids(&database.pool, &ids).await?;
    let on_shared = composed
        .values()
        .filter(|row| row.resource_id == Some(shared))
        .count();
    assert_eq!(on_shared, 5, "the shared resource's names: {composed:#?}");
    Ok(ByResourceFixture {
        alpha,
        beta,
        shared,
        shared_names,
        base,
        nameless,
    })
}

impl ByResourceFixture {
    fn all(&self) -> Vec<Uuid> {
        vec![self.alpha, self.beta, self.shared, self.base, self.nameless]
    }
}

/// The current names of `ids` as the lookup read them before it took the chains: the names bound
/// to the resources on any chain, then the family load's order, composition and pick, as
/// `load_family_names_by_resource_ids` had them.
async fn names_by_resource_unkeyed(
    pool: &PgPool,
    ids: &[Uuid],
) -> Result<std::collections::BTreeMap<Uuid, bigname_storage::NameCurrentRow>> {
    let mut out = std::collections::BTreeMap::new();
    if ids.is_empty() {
        return Ok(out);
    }
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT candidate.logical_name_id
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.resource_id = ANY($1::uuid[])",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;
    let ordered: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT surface.logical_name_id FROM bigname_phase.name_surfaces surface
         WHERE surface.logical_name_id = ANY($1)
         ORDER BY {name} ASC, surface.logical_name_id ASC",
        name = bigname_storage::rendered_name::rendered_name_sql("surface")
    ))
    .bind(&names)
    .fetch_all(pool)
    .await?;
    let mut rows =
        bigname_storage::families::name::load_family_names_by_logical_name_ids(pool, &names)
            .await?;
    for name in ordered {
        let Some(row) = rows.remove(&name) else {
            continue;
        };
        if let Some(resource) = row.resource_id.filter(|id| ids.contains(id)) {
            out.entry(resource).or_insert(row);
        }
    }
    Ok(out)
}

// Parity: for each input class the lookup, given the publication's chains, serves exactly the
// rows it served before it took them. It runs on the server's default collation and on an ICU
// one, so the shared resource's pick is checked under two orders of its served names.
#[tokio::test]
async fn current_names_by_resource_match_the_family_load() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_current_names_by_resource_parity(&database).await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn current_names_by_resource_match_the_family_load_under_icu() -> Result<()> {
    let database = TestDatabase::new_migrated_icu().await?;
    let result = check_current_names_by_resource_parity(&database).await;
    database.cleanup().await?;
    result
}

async fn check_current_names_by_resource_parity(database: &TestDatabase) -> Result<()> {
    let fixture = seed_by_resource_fixture(database).await?;
    let chains = by_resource_chains();
    let cases = [
        ("a resource with one name", vec![fixture.alpha]),
        ("a resource with five names", vec![fixture.shared]),
        ("resources on two chains", vec![fixture.alpha, fixture.base]),
        ("a resource with no name", vec![fixture.nameless]),
        ("no resources", Vec::new()),
        ("every class on one page", fixture.all()),
    ];
    for (label, ids) in cases {
        let keyed =
            bigname_storage::load_current_names_by_resource_ids(&database.pool, &ids, &chains)
                .await?;
        let unkeyed = names_by_resource_unkeyed(&database.pool, &ids).await?;
        assert_eq!(keyed, unkeyed, "{label}");
    }
    let served = bigname_storage::load_current_names_by_resource_ids(
        &database.pool,
        &fixture.all(),
        &chains,
    )
    .await?;
    assert_eq!(
        served
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            fixture.alpha,
            fixture.beta,
            fixture.shared,
            fixture.base
        ]),
        "{served:#?}"
    );
    // The shared resource is served under the first of its served names in the database's own
    // order, the lower id of that name's tie. Neither the first name id nor the higher id of
    // the tie is that name, so a pick that ignored the served name or the tie-break would
    // differ here, under either collation.
    let first: String = sqlx::query_scalar(
        "SELECT name FROM unnest($1::text[]) AS served(name) ORDER BY name LIMIT 1",
    )
    .bind(&BY_RESOURCE_SHARED_NAMES[..])
    .fetch_one(&database.pool)
    .await?;
    let tie = fixture
        .shared_names
        .iter()
        .filter(|(_, name)| *name == first)
        .map(|(id, _)| id)
        .collect::<Vec<_>>();
    assert_eq!(
        tie.len(),
        2,
        "{first} ties two names: {:?}",
        fixture.shared_names
    );
    assert_ne!(tie[0], &fixture.shared_names[0].0);
    assert_eq!(
        &served[&fixture.shared].logical_name_id, tie[0],
        "served under {first}: {:?}",
        fixture.shared_names
    );
    Ok(())
}

// A second chain: a list of the main chain alone serves no name of the resource on Base, and a
// list of both chains serves it.
#[tokio::test]
async fn current_names_by_resource_read_only_the_listed_chains() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_current_names_by_resource_chains(&database).await;
    database.cleanup().await?;
    result
}

async fn check_current_names_by_resource_chains(database: &TestDatabase) -> Result<()> {
    let fixture = seed_by_resource_fixture(database).await?;
    let ids = fixture.all();
    let unkeyed = names_by_resource_unkeyed(&database.pool, &ids).await?;
    assert!(unkeyed.contains_key(&fixture.base), "{unkeyed:#?}");
    let both = bigname_storage::load_current_names_by_resource_ids(
        &database.pool,
        &ids,
        &by_resource_chains(),
    )
    .await?;
    assert_eq!(both, unkeyed);
    let main = bigname_storage::load_current_names_by_resource_ids(
        &database.pool,
        &ids,
        &[FAMILY_CHAIN.to_owned()],
    )
    .await?;
    let mut expected = unkeyed;
    expected.remove(&fixture.base);
    assert_eq!(main, expected);
    Ok(())
}

// An empty chain list serves no names, never every chain's, and sends no statement.
#[tokio::test]
async fn current_names_by_resource_on_no_chain_are_none() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_current_names_by_resource_no_chain(&database).await;
    database.cleanup().await?;
    result
}

async fn check_current_names_by_resource_no_chain(database: &TestDatabase) -> Result<()> {
    let fixture = seed_by_resource_fixture(database).await?;
    let counter = bigname_test_support::StatementCounter::connect(&database.pool).await?;
    let (none, sent) = counter
        .count(bigname_storage::load_current_names_by_resource_ids(
            counter.pool(),
            &fixture.all(),
            &[],
        ))
        .await;
    counter.close().await;
    let none = none?;
    assert!(none.is_empty(), "an empty chain list served {none:#?}");
    assert_eq!(sent.total(), 0, "{:#?}", sent.statements());
    Ok(())
}

// An empty id list serves no names and sends no statement.
#[tokio::test]
async fn current_names_of_no_resources_are_none() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_current_names_of_no_resources(&database).await;
    database.cleanup().await?;
    result
}

async fn check_current_names_of_no_resources(database: &TestDatabase) -> Result<()> {
    seed_by_resource_fixture(database).await?;
    let counter = bigname_test_support::StatementCounter::connect(&database.pool).await?;
    let (none, sent) = counter
        .count(bigname_storage::load_current_names_by_resource_ids(
            counter.pool(),
            &[],
            &by_resource_chains(),
        ))
        .await;
    counter.close().await;
    let none = none?;
    assert!(none.is_empty(), "no resources served {none:#?}");
    assert_eq!(sent.total(), 0, "{:#?}", sent.statements());
    Ok(())
}

// Plan: the lookup the function sends seeks project_binding_candidate_resource_idx on the chain
// and the resource, in the custom and the generic plan. There are two page shapes. The
// page_size 5 shape has 4 resources, 2 of them named through 4 candidate rows. The page_size 50
// shape has 41 resources, 40 of them named through 44 candidate rows. Sequential scans stay
// enabled.
#[tokio::test]
async fn current_names_by_resource_seek_the_chain_and_the_resource() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_current_names_by_resource_plans(&database).await;
    database.cleanup().await?;
    result
}

fn by_resource_plan_resource(n: i64) -> Uuid {
    Uuid::from_u128(BY_RESOURCE_PLAN_OFFSET + n as u128)
}

async fn check_current_names_by_resource_plans(database: &TestDatabase) -> Result<()> {
    // Resource n has one candidate of name n; resources 1, 2 and 101..=104 have a second one of
    // the same name. Resources above the main chain's rows are on Base.
    raw_sql(&format!(
        "INSERT INTO bigname_phase.project_binding_candidate (surface_binding_id,
             logical_name_id, namespace, chain_id, authority_arm, resource_id, binding_kind,
             canonicality_state, block_number, event_identity)
         SELECT lpad(to_hex(n), 32, '0')::uuid, 'ens:plan-' || n, 'ens',
                CASE WHEN n > {main} THEN '{base}' ELSE '{chain}' END, 'ens_v1',
                lpad(to_hex({offset} + n), 32, '0')::uuid, 'declared_registry_path',
                'canonical', 200, 'plan-' || n
         FROM generate_series(1::bigint, {all}::bigint) n;
         INSERT INTO bigname_phase.project_binding_candidate (surface_binding_id,
             logical_name_id, namespace, chain_id, authority_arm, resource_id, binding_kind,
             canonicality_state, block_number, event_identity)
         SELECT lpad(to_hex({all} + n), 32, '0')::uuid, 'ens:plan-' || n, 'ens', '{chain}',
                'ens_v1', lpad(to_hex({offset} + n), 32, '0')::uuid,
                'declared_registry_path', 'canonical', 201, 'plan-second-' || n
         FROM unnest(ARRAY[1, 2, 101, 102, 103, 104]::bigint[]) n;
         ANALYZE bigname_phase.project_binding_candidate;",
        main = BY_RESOURCE_PLAN_ROWS,
        all = BY_RESOURCE_PLAN_ROWS + BY_RESOURCE_PLAN_ROWS / 10,
        offset = BY_RESOURCE_PLAN_OFFSET,
        base = BY_RESOURCE_BASE,
        chain = FAMILY_CHAIN,
    ))
    .execute(&database.pool)
    .await?;
    let nameless = |n: i64| by_resource_plan_resource(BY_RESOURCE_PLAN_ROWS * 10 + n);
    let shapes = [
        (
            "page_size 5",
            vec![
                by_resource_plan_resource(1),
                by_resource_plan_resource(2),
                nameless(1),
                nameless(2),
            ],
        ),
        (
            "page_size 50",
            (101..=140)
                .map(by_resource_plan_resource)
                .chain([nameless(3)])
                .collect(),
        ),
    ];
    let chains = by_resource_chains();
    let counter = bigname_test_support::StatementCounter::connect(&database.pool).await?;
    let mut failures = Vec::new();
    for (shape, ids) in shapes {
        // The plan fixture has candidates only, so the composition after the lookup has no
        // surface to compose. Its result is not what this test reads.
        let (_, sent) = counter
            .count(bigname_storage::load_current_names_by_resource_ids(
                counter.pool(),
                &ids,
                &chains,
            ))
            .await;
        let lookups = sent
            .statements()
            .iter()
            .filter(|statement| {
                statement.contains("project_binding_candidate")
                    && statement.contains("DISTINCT candidate.logical_name_id")
            })
            .collect::<Vec<_>>();
        assert_eq!(lookups.len(), 1, "{shape}: {:#?}", sent.statements());
        let mut connection = database.pool.acquire().await?;
        raw_sql(&format!(
            "DEALLOCATE ALL; PREPARE by_resource_lookup (uuid[], text[]) AS {}",
            lookups[0]
        ))
        .execute(&mut *connection)
        .await?;
        let ids = ids
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let chains = chains.join(",");
        for mode in ["force_custom_plan", "force_generic_plan"] {
            let plan = raw_sql(&format!(
                "SET plan_cache_mode = {mode};
                 EXPLAIN (COSTS OFF) EXECUTE by_resource_lookup
                     ('{{{ids}}}'::uuid[], '{{{chains}}}'::text[])"
            ))
            .fetch_all(&mut *connection)
            .await?
            .iter()
            .map(|row| row.try_get::<String, _>(0))
            .collect::<Result<Vec<_>, _>>()?;
            if !by_resource_lookup_is_keyed(&plan) {
                failures.push(format!("{shape} {mode}:\n{}", plan.join("\n")));
            }
        }
        raw_sql("DEALLOCATE ALL; RESET plan_cache_mode")
            .execute(&mut *connection)
            .await?;
    }
    counter.close().await;
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    Ok(())
}

/// The plan reads project_binding_candidate only through project_binding_candidate_resource_idx,
/// with both the chain and the resource in the index condition. In the text plan the condition
/// is the line right after the scan node's own line.
fn by_resource_lookup_is_keyed(plan: &[String]) -> bool {
    let keyed = plan.windows(2).any(|lines| {
        lines[0].contains("project_binding_candidate_resource_idx")
            && lines[1].contains("Index Cond:")
            && lines[1].contains("chain_id = ")
            && lines[1].contains("resource_id = ")
    });
    let walked = plan.iter().any(|line| {
        line.contains("Seq Scan on project_binding_candidate")
            || (line.contains("Index Cond:")
                && line.contains("resource_id = ")
                && !line.contains("chain_id = "))
    });
    keyed && !walked
}

// Route: GET /v1/permissions?address= serves each row's current name, which the route reads on
// its publication's chains, and no name for a nameless resource.
#[tokio::test]
async fn address_permission_rows_carry_the_current_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_permissions_fixture(&database).await?;
    let (status, bob) = read_family_response(
        &database,
        &format!("/v1/permissions?address={FAMILY_BOB}&namespace=ens"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{bob:#}");
    let names = bob["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| &row["name"])
        .collect::<Vec<_>>();
    assert!(
        !names.is_empty() && names.iter().all(|name| **name == json!("alpha.eth")),
        "{bob:#}"
    );
    let (status, carol) = read_family_response(
        &database,
        &format!("/v1/permissions?address={FAMILY_CAROL}&namespace=ens"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{carol:#}");
    let names = carol["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| &row["name"])
        .collect::<Vec<_>>();
    assert!(
        !names.is_empty() && names.iter().all(|name| name.is_null()),
        "{carol:#}"
    );
    database.cleanup().await
}

// Route: a resolver's role list serves each holder's current name the same way.
#[tokio::test]
async fn resolver_role_rows_carry_the_current_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_permissions_fixture(&database).await?;
    let (status, roles) = read_family_response(
        &database,
        &format!("/v1/resolvers/1/{FAMILY_V2_RESOLVER}/roles"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{roles:#}");
    let listed = roles["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| (&row["address"], &row["name"]))
        .collect::<Vec<_>>();
    assert_eq!(
        listed,
        [
            (&json!(FAMILY_BOB), &json!("alpha.eth")),
            (&json!(FAMILY_ALICE), &json!("beta.eth")),
            (&json!(FAMILY_CAROL), &Value::Null),
        ],
        "{roles:#}"
    );
    database.cleanup().await
}

// Project: the family step never sends the lookup, so the chain key changes no statement Project
// sends and no row it writes. A full rebuild of the fixture's two chains on a counted pool sends
// neither of the lookup's statements. The run log carries a digest of the statements the
// rebuild sent and one of the family rows it wrote, which the base and head runs must print
// alike. The digests leave out the content hashes and `updated_at`, which a build or a clock
// moves.
#[tokio::test]
async fn project_rebuild_sends_no_resource_name_lookup() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_project_rebuild_without_the_lookup(&database).await;
    database.cleanup().await?;
    result
}

async fn check_project_rebuild_without_the_lookup(database: &TestDatabase) -> Result<()> {
    use std::io::Write as _;

    seed_by_resource_fixture(database).await?;
    let mut statements = std::collections::BTreeSet::new();
    for chain in by_resource_chains() {
        let hash: String = sqlx::query_scalar(
            "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = 240
             AND canonicality_state IN ('canonical', 'safe', 'finalized')",
        )
        .bind(&chain)
        .fetch_one(&database.pool)
        .await?;
        let counter = bigname_test_support::StatementCounter::connect(&database.pool).await?;
        let (rebuilt, sent) = counter
            .count(rebuild_fixture_families(counter.pool(), &chain, 240, &hash))
            .await;
        counter.close().await;
        rebuilt?;
        let lookups = sent.tagged("families.name.by_resource")
            + sent.tagged("families.name.by_resource_order");
        assert_eq!(lookups, 0, "{chain}: {:#?}", sent.statements());
        assert!(sent.total() > 0, "{chain}: the rebuild sent nothing");
        statements.extend(sent.statements().iter().cloned());
    }
    let statements = statements.into_iter().collect::<Vec<_>>();
    let statements_digest: String = sqlx::query_scalar("SELECT md5($1)")
        .bind(statements.join("\n"))
        .fetch_one(&database.pool)
        .await?;

    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.tables
         WHERE table_schema = 'bigname_phase' AND table_type = 'BASE TABLE'
           AND table_name LIKE 'project\\_%'
           AND table_name NOT IN ('project_family_marker', 'project_repair_record')
         ORDER BY table_name",
    )
    .fetch_all(&database.pool)
    .await?;
    let mut digests = Vec::with_capacity(tables.len());
    let mut rows = 0_i64;
    for table in &tables {
        let (digest, count): (String, i64) = sqlx::query_as(&format!(
            "SELECT md5(COALESCE(string_agg(row_text, E'\\n' ORDER BY row_text), '')), count(*)
             FROM (SELECT (SELECT jsonb_object_agg(field.key, field.value)
                           FROM jsonb_each(to_jsonb(stored)) field
                           WHERE field.key NOT LIKE '%content_hash'
                             AND field.key <> 'updated_at')::text AS row_text
                   FROM bigname_phase.{table} stored) written"
        ))
        .fetch_one(&database.pool)
        .await?;
        rows += count;
        digests.push(format!("{table} {digest} {count}"));
    }
    let rows_digest: String = sqlx::query_scalar("SELECT md5($1)")
        .bind(digests.join("\n"))
        .fetch_one(&database.pool)
        .await?;
    // Written past the test harness's capture, so the run's log keeps the record.
    let mut log = std::io::stderr();
    writeln!(
        log,
        "PROJECT_STATEMENTS_MD5 {statements_digest} {}",
        statements.len()
    )?;
    writeln!(log, "PROJECT_FAMILY_ROWS_MD5 {rows_digest} {rows}")?;
    for line in &digests {
        writeln!(log, "PROJECT_FAMILY_TABLE {line}")?;
    }
    assert!(rows > 0, "the rebuild wrote no family row");
    Ok(())
}
