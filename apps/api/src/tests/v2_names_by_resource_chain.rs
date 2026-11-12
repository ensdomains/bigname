// The current name of each resource on a permission page or a resolver's role list
// (`load_current_names_by_resource_ids`), read only on the chains of the caller's publication.
//
// On the release line the wrapper sends its own copy of the family load's statements, with the
// chain key added. So its rows are compared with the family load's own
// (`load_family_names_by_resource_ids`) on the same fixture, and the plan of the lookup it
// sends is read from the statement the wrapper actually sends.

const BY_RESOURCE_BASE: &str = "base-mainnet";
/// Candidate rows of the plan fixture on the main chain. Base gets a tenth as many.
const BY_RESOURCE_PLAN_ROWS: i64 = 200_000;
const BY_RESOURCE_PLAN_OFFSET: u128 = 0x7b00_0000_0000;

fn by_resource_chains() -> Vec<String> {
    vec![FAMILY_CHAIN.to_owned(), BY_RESOURCE_BASE.to_owned()]
}

/// The resources of the composed fixture.
struct ByResourceFixture {
    /// alpha.eth's resource: one name.
    alpha: Uuid,
    /// beta.eth's resource: one name.
    beta: Uuid,
    /// One resource bound to three names, served as `Pick.eth`, `pick.eth` and `pick.eth`.
    shared: Uuid,
    shared_names: Vec<String>,
    /// pick.base.eth's resource, on Base.
    base: Uuid,
    /// A resource no name is bound to.
    nameless: Uuid,
}

/// alpha.eth and beta.eth with their registrations, three names bound to one resource, a
/// Basenames name on Base and a nameless resource, published at 240 on both chains.
async fn seed_by_resource_fixture(database: &TestDatabase) -> Result<ByResourceFixture> {
    let (_, _, alpha) = seed_family_names_events(database).await?;
    let beta: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM bigname_phase.surface_bindings WHERE logical_name_id = $1",
    )
    .bind(bigname_storage::logical_name_id_for_name("ens", "beta.eth"))
    .fetch_one(&database.pool)
    .await?;

    let mut rows = ["pick-one.eth", "pick-two.eth", "pick-three.eth"]
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
    let shared_names = rows
        .iter()
        .map(|row| row.surface.logical_name_id.clone())
        .collect::<Vec<_>>();
    upsert_family_name_rows(database, rows).await?;
    // Served names that differ only by case, and two that tie, so the pick takes its order from
    // the database's collation and then from the name id. As Interpret's identity write does,
    // the surfaces and their search documents change together.
    let paths: Vec<Vec<String>> = sqlx::query_scalar(
        "SELECT labelhashes FROM bigname_phase.name_surfaces
         WHERE logical_name_id = ANY($1) ORDER BY logical_name_id",
    )
    .bind(&shared_names)
    .fetch_all(&database.pool)
    .await?;
    let mut transaction = database.pool.begin().await?;
    bigname_storage::identity_search::prepare(
        &mut transaction,
        bigname_storage::identity_search::LabelWrite::NONE,
        &paths,
        &shared_names,
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces
         SET raw_name = CASE WHEN logical_name_id = $1 THEN 'Pick.eth' ELSE 'pick.eth' END
         WHERE logical_name_id = ANY($2)",
    )
    .bind(&shared_names[0])
    .bind(&shared_names)
    .execute(&mut *transaction)
    .await?;
    bigname_storage::identity_search::refresh(&mut transaction, &shared_names, &[]).await?;
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
        0x7a5_0000,
        "basenames",
        "basenames",
        BY_RESOURCE_BASE,
    )
    .await?;

    let nameless = Uuid::from_u128(0x7a6_0000);
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

    // Each class is reached: the three names compose onto the shared resource, and every named
    // resource is served.
    let composed =
        bigname_storage::load_name_current_by_logical_name_ids(&database.pool, &shared_names)
            .await?;
    let on_shared = composed
        .values()
        .filter(|row| row.resource_id == Some(shared))
        .count();
    assert_eq!(on_shared, 3, "the shared resource's names: {composed:#?}");
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

// Parity: for each input class the wrapper, given the publication's chains, serves exactly the
// rows the family load serves.
#[tokio::test]
async fn current_names_by_resource_match_the_family_load() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_current_names_by_resource_parity(&database).await;
    database.cleanup().await?;
    result
}

async fn check_current_names_by_resource_parity(database: &TestDatabase) -> Result<()> {
    let fixture = seed_by_resource_fixture(database).await?;
    let chains = by_resource_chains();
    let cases = [
        ("a resource with one name", vec![fixture.alpha]),
        ("a resource with three names", vec![fixture.shared]),
        ("resources on two chains", vec![fixture.alpha, fixture.base]),
        ("a resource with no name", vec![fixture.nameless]),
        ("no resources", Vec::new()),
        ("every class on one page", fixture.all()),
    ];
    for (label, ids) in cases {
        let keyed =
            bigname_storage::load_current_names_by_resource_ids(&database.pool, &ids, &chains)
                .await?;
        let family = bigname_storage::families::name::load_family_names_by_resource_ids(
            &database.pool,
            &ids,
        )
        .await?;
        assert_eq!(keyed, family, "{label}");
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
    let shared = &served[&fixture.shared].logical_name_id;
    assert!(
        fixture.shared_names.contains(shared),
        "the shared resource is served under one of its own names: {shared}"
    );
    Ok(())
}

// The chain list bounds the lookup: an empty list serves no names, never every chain's, and a
// list of one chain serves only that chain's resources.
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
    let family =
        bigname_storage::families::name::load_family_names_by_resource_ids(&database.pool, &ids)
            .await?;
    assert!(family.contains_key(&fixture.base), "{family:#?}");
    let none =
        bigname_storage::load_current_names_by_resource_ids(&database.pool, &ids, &[]).await?;
    assert!(none.is_empty(), "an empty chain list served {none:#?}");
    let main = bigname_storage::load_current_names_by_resource_ids(
        &database.pool,
        &ids,
        &[FAMILY_CHAIN.to_owned()],
    )
    .await?;
    let mut expected = family;
    expected.remove(&fixture.base);
    assert_eq!(main, expected);
    Ok(())
}

// Plan: the lookup the wrapper sends seeks project_binding_candidate_resource_idx on the chain
// and the resource, in the custom and the generic plan. The two shapes are the pages of the
// route read on the staging copy: page_size 5 for one address had 4 resources, 2 of them named
// through 4 candidate rows; page_size 50 for another had 41 resources, 40 of them named through
// 44 candidate rows. Sequential scans stay enabled.
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
