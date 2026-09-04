fn plan_domain_filter(member: &str, value: &str) -> crate::graphql::GeneratedDomainFilter {
    let mut filter = crate::graphql::GeneratedDomainFilter::default();
    let list = || Some(vec![value.to_owned()]);
    match member {
        "id" => filter.id.eq = Some(Some(value.into())),
        "id_not" => filter.id.not = Some(Some(value.into())),
        "id_gt" => filter.id.gt = Some(value.into()),
        "id_gte" => filter.id.gte = Some(value.into()),
        "id_lt" => filter.id.lt = Some(value.into()),
        "id_lte" => filter.id.lte = Some(value.into()),
        "id_in" => filter.id.in_values = list(),
        "id_not_in" => filter.id.not_in_values = list(),
        "name" => filter.name.eq = Some(Some(value.into())),
        "name_not" => filter.name.not = Some(Some(value.into())),
        "name_gt" => filter.name.gt = Some(value.into()),
        "name_gte" => filter.name.gte = Some(value.into()),
        "name_lt" => filter.name.lt = Some(value.into()),
        "name_lte" => filter.name.lte = Some(value.into()),
        "name_in" => filter.name.in_values = list(),
        "name_not_in" => filter.name.not_in_values = list(),
        "name_contains" => filter.name.contains = Some(value.into()),
        "name_contains_nocase" => filter.name.contains_nocase = Some(value.into()),
        "name_not_contains" => filter.name.not_contains = Some(value.into()),
        "name_not_contains_nocase" => filter.name.not_contains_nocase = Some(value.into()),
        "name_starts_with" => filter.name.starts_with = Some(value.into()),
        "name_starts_with_nocase" => filter.name.starts_with_nocase = Some(value.into()),
        "name_not_starts_with" => filter.name.not_starts_with = Some(value.into()),
        "name_not_starts_with_nocase" => filter.name.not_starts_with_nocase = Some(value.into()),
        "name_ends_with" => filter.name.ends_with = Some(value.into()),
        "name_ends_with_nocase" => filter.name.ends_with_nocase = Some(value.into()),
        "name_not_ends_with" => filter.name.not_ends_with = Some(value.into()),
        "name_not_ends_with_nocase" => filter.name.not_ends_with_nocase = Some(value.into()),
        _ => panic!("unknown generated Domain member {member}"),
    }
    filter
}

#[test]
fn noncanonical_id_ranges_pin_c_collation() {
    let mut sql = sqlx::QueryBuilder::<sqlx::Postgres>::new("");
    let filter = plan_domain_filter("id_gt", "0xA");
    crate::graphql::push_generated_domain_filters(&mut sql, &filter);
    assert!(sql.sql().contains("(nc.namehash COLLATE \"C\") >"), "{}", sql.sql());
}

async fn pad_generated_domain_plans(database: &TestDatabase) -> Result<()> {
    pad_resolver_planner_statistics(database).await?;
    let alternate_owner = "0x0000000000000000000000000000000000000672";
    let alternate_resolver = "0x0000000000000000000000000000000000000673";
    let changed = sqlx::query(
        r#"UPDATE bigname_phase.name_current
              SET declared_summary = JSONB_SET(
                    JSONB_SET(declared_summary, '{control,registry_owner}', TO_JSONB($1::TEXT)),
                    '{resolver,address}', TO_JSONB($2::TEXT))
            WHERE namehash >= '0x0000000000000000000000000000000000000000000000000000000000000dac'"#,
    )
    .bind(alternate_owner)
    .bind(alternate_resolver)
    .execute(&database.lookup_pool)
    .await?;
    assert!(changed.rows_affected() >= 2_500);
    sqlx::query("ANALYZE bigname_phase.name_current")
        .execute(&database.lookup_pool)
        .await?;
    Ok(())
}

fn assert_domain_plan_shape(explain: &Value, label: &str) -> Result<()> {
    let plan = &explain[0]["Plan"];
    assert_eq!(plan["Node Type"], "Limit", "{label}");
    let text = serde_json::to_string(plan)?;
    assert!(text.contains("name_current"), "predicate must execute below Limit: {label}");
    assert!(text.contains("chain_positions") && text.contains("supported"), "snapshot eligibility: {label}");
    assert!(text.contains("chain_lineage_readable_height_idx"), "lineage index: {label}");
    for node in plan_nodes(explain) {
        if node["Node Type"] == "Sort" {
            assert!(matches!(node["Sort Method"].as_str(), Some("top-N heapsort" | "quicksort")), "sort method: {label}: {node}");
            assert!(node["Sort Space Used"].as_u64().unwrap_or(0) <= 2_048, "sort memory: {label}");
        }
    }
    Ok(())
}

fn assert_id_index_bounded(explain: &Value, label: &str) -> Result<()> {
    assert_domain_plan_shape(explain, label)?;
    let nodes = plan_nodes(explain);
    let scan = nodes.iter().find(|node| node["Node Type"] == "Index Scan" && node["Index Name"] == "name_current_lookup_idx")
        .with_context(|| format!("name_current_lookup_idx Index Scan: {label}"))?;
    assert!(scan["Actual Rows"].as_u64().unwrap_or(0) <= 204, "index rows: {label}: {scan}");
    assert_eq!(scan["Actual Loops"], 1, "index loops: {label}: {scan}");
    assert!(scan["Rows Removed by Filter"].as_u64().unwrap_or(0) <= 4, "index removals: {label}: {scan}");
    Ok(())
}

fn assert_flat_eligibility(explain: &Value, label: &str) -> Result<()> {
    let surface = plan_nodes(explain)
        .into_iter()
        .find(|node| node["Relation Name"] == "name_surfaces" && node["Alias"] == "surface")
        .with_context(|| format!("name_surfaces plan: {label}"))?;
    assert_eq!(surface["Actual Loops"], 1, "flat eligibility join: {label}: {surface}");
    assert!(explain[0].get("JIT").is_none(), "linear page must not trigger JIT: {label}: {explain}");
    assert!(explain[0]["Plan"]["Total Cost"].as_f64().unwrap_or(f64::MAX) < 100_000.0, "linear page cost must stay below jit_above_cost: {label}: {explain}");
    Ok(())
}

#[tokio::test]
async fn graphql_generated_domain_operator_plans_are_index_bounded_or_linear() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    let late_id = format!("0x{:064x}", 5_999);
    let locale = sqlx::query("SELECT VERSION() AS version, datlocprovider::TEXT AS provider, datcollate AS locale FROM pg_database WHERE datname = CURRENT_DATABASE()")
        .fetch_one(&database.lookup_pool).await?;
    let version: String = locale.try_get("version")?;
    let provider: String = locale.try_get("provider")?;
    let locale: String = locale.try_get("locale")?;
    if !version.starts_with("PostgreSQL 16.") {
        eprintln!("skipping PostgreSQL 16 image identity check on {version}; byte-order assertions remain applicable");
    }
    assert!(!provider.is_empty() && !locale.is_empty(), "collation authority: {provider}/{locale}");
    let database_range = sqlx::query_scalar::<_, String>("SELECT namehash FROM bigname_phase.name_current WHERE namespace = 'ens' AND namehash >= $1 ORDER BY namehash").bind("0x").fetch_all(&database.lookup_pool).await?;
    let byte_range = sqlx::query_scalar::<_, String>("SELECT namehash FROM bigname_phase.name_current WHERE namespace = 'ens' AND convert_to(namehash, 'UTF8') >= convert_to($1, 'UTF8') ORDER BY convert_to(namehash, 'UTF8')").bind("0x").fetch_all(&database.lookup_pool).await?;
    assert_eq!(database_range, byte_range, "fixed-width hexadecimal order must match UTF-8 byte order under {provider}/{locale} on {version}");
    let pair = vec![format!("0x2a{}", "0".repeat(62)), format!("0x10a{}", "0".repeat(61))];
    let database_pair = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY value").bind(&pair).fetch_all(&database.lookup_pool).await?;
    let byte_pair = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY convert_to(value, 'UTF8')").bind(&pair).fetch_all(&database.lookup_pool).await?;
    assert_eq!(database_pair, byte_pair, "deployed collation must order hexadecimal text bytewise under {provider}/{locale} on {version}");
    let adversarial = vec!["B", "a", "0xA", "0xa"];
    let byte_adversarial = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY convert_to(value, 'UTF8')").bind(&adversarial).fetch_all(&database.lookup_pool).await?;
    let has_icu = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'en-US-x-icu')",
    )
    .fetch_one(&database.lookup_pool)
    .await?;
    if has_icu {
        let icu_adversarial = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY value COLLATE \"en-US-x-icu\"").bind(&adversarial).fetch_all(&database.lookup_pool).await?;
        assert_ne!(byte_adversarial, icu_adversarial, "negative control must distinguish locale ordering from byte ordering");
    } else {
        eprintln!("skipping ICU negative control: server does not provide en-US-x-icu");
    }
    crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &Default::default(),
        crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Desc,
        200, 0,
    ).await?;
    let members = [
        "id", "id_not", "id_gt", "id_gte", "id_lt", "id_lte", "id_in", "id_not_in",
        "name", "name_not", "name_gt", "name_gte", "name_lt", "name_lte", "name_in", "name_not_in",
        "name_contains", "name_contains_nocase", "name_not_contains", "name_not_contains_nocase",
        "name_starts_with", "name_starts_with_nocase", "name_not_starts_with", "name_not_starts_with_nocase",
        "name_ends_with", "name_ends_with_nocase", "name_not_ends_with", "name_not_ends_with_nocase",
    ];
    for member in members {
        let value = match member {
            "id_not" | "id_not_in" => "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "id_gt" | "id_gte" => "0x000000000000000000000000000000000000000000000000000000000000176e",
            "id_lt" | "id_lte" => "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            value if value.starts_with("id") => late_id.as_str(),
            value if value.starts_with("name_not") => "never-present",
            "name_gt" | "name_gte" => "alice.eth5998",
            "name_lt" | "name_lte" => "zzzz",
            value if value.contains("starts") => "alice",
            value if value.contains("ends") || value.contains("contains") => "5999",
            _ => "alice.eth5999",
        };
        let filter = plan_domain_filter(member, value);
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &filter,
            crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Desc,
            200, 0,
        ).await?;
        println!("DOMAIN OPERATOR {member} PLAN {}", serde_json::to_string_pretty(&explain)?);
        if matches!(member, "id" | "id_gt" | "id_gte" | "id_lt" | "id_lte" | "id_in") {
            assert_id_index_bounded(&explain, member)?;
        } else {
            assert_domain_plan_shape(&explain, member)?;
        }
        assert!(
            serde_json::to_string(&explain[0]["Plan"])?.contains(value),
            "bound predicate value must occur below Limit: {member}"
        );
        let returned = crate::graphql::load_phase_graphql_name_list_page_offset(
            &database.lookup_pool,
            &bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() },
            &chains, &filter, crate::graphql::GeneratedDomainSort::Id,
            bigname_storage::NameCurrentListOrder::Desc, 200, 0,
        ).await?;
        assert!(returned.iter().any(|row| row.row.row.namehash == late_id), "late target: {member}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_order_plans_are_index_bounded_or_linear() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &Default::default(),
        crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Asc,
        200, 0,
    ).await?;
    let sorts = [
        crate::graphql::GeneratedDomainSort::Id,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::CreatedAt),
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::ExpiryDate),
        crate::graphql::GeneratedDomainSort::Owner,
        crate::graphql::GeneratedDomainSort::OwnerId,
        crate::graphql::GeneratedDomainSort::Resolver,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::RegistrationDate),
    ];
    for sort in sorts {
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &Default::default(), sort,
            bigname_storage::NameCurrentListOrder::Asc, 200, 0,
        ).await?;
        println!("DOMAIN ORDER {sort:?} PLAN {}", serde_json::to_string_pretty(&explain)?);
        if sort == crate::graphql::GeneratedDomainSort::Id {
            assert_id_index_bounded(&explain, "id order")?;
            assert!(!plan_nodes(&explain).iter().any(|node| node["Node Type"] == "Sort"), "id order must not sort");
        } else {
            assert_domain_plan_shape(&explain, &format!("{sort:?}"))?;
            assert_flat_eligibility(&explain, &format!("{sort:?}"))?;
            assert!(plan_nodes(&explain).iter().any(|node| node["Node Type"] == "Sort"), "sort must occur below Limit: {sort:?}");
        }
    }
    let id_lt = plan_domain_filter("id_lt", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    let unselective_range = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &id_lt,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        bigname_storage::NameCurrentListOrder::Asc, 200, 0,
    ).await?;
    assert_flat_eligibility(&unselective_range, "unselective id_lt with name order")?;

    let id_gt = plan_domain_filter("id_gt", "0x000000000000000000000000000000000000000000000000000000000000176e");
    let selective_range = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &id_gt,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        bigname_storage::NameCurrentListOrder::Asc, 200, 0,
    ).await?;
    assert_flat_eligibility(&selective_range, "selective id_gt with name order")?;

    let id = plan_domain_filter("id", "0x000000000000000000000000000000000000000000000000000000000000176f");
    let bounded_equality = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &id,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        bigname_storage::NameCurrentListOrder::Asc, 200, 0,
    ).await?;
    assert_id_index_bounded(&bounded_equality, "id equality with name order")?;
    database.cleanup().await
}

#[tokio::test]
#[ignore = "run on glibc: docker rm -f bigname-test-postgres-glibc; BIGNAME_TEST_POSTGRES_IMAGE=postgres:16-bookworm BIGNAME_TEST_POSTGRES_CONTAINER=bigname-test-postgres-glibc BIGNAME_TEST_POSTGRES_PORT=59555 ./scripts/test-db -- cargo test -p bigname-api tests::graphql_glibc_en_us_hexadecimal_order_matches_bytes -- --ignored --exact"]
async fn graphql_glibc_en_us_hexadecimal_order_matches_bytes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let locale = sqlx::query("SELECT VERSION() AS version, datlocprovider::TEXT AS provider, datcollate AS locale FROM pg_database WHERE datname = CURRENT_DATABASE()")
        .fetch_one(&database.lookup_pool).await?;
    let version: String = locale.try_get("version")?;
    let provider: String = locale.try_get("provider")?;
    let locale: String = locale.try_get("locale")?;
    assert!(
        !version.contains("musl"),
        "glibc probe cannot run on {version}; rerun with: BIGNAME_TEST_POSTGRES_IMAGE=postgres:16-bookworm BIGNAME_TEST_POSTGRES_CONTAINER=bigname-test-postgres-glibc BIGNAME_TEST_POSTGRES_PORT=59555 ./scripts/test-db -- cargo test -p bigname-api tests::graphql_glibc_en_us_hexadecimal_order_matches_bytes -- --ignored --exact"
    );
    assert_eq!(provider, "c", "glibc probe requires libc provider: {provider}");
    assert!(locale.to_ascii_lowercase().starts_with("en_us"), "glibc probe requires en_US locale: {locale}");
    let database_order = sqlx::query_scalar::<_, String>("SELECT '0x' || LPAD(TO_HEX(value), 64, '0') AS namehash FROM GENERATE_SERIES(0, 24999) value ORDER BY namehash")
        .fetch_all(&database.lookup_pool).await?;
    let byte_order = sqlx::query_scalar::<_, String>("SELECT '0x' || LPAD(TO_HEX(value), 64, '0') AS namehash FROM GENERATE_SERIES(0, 24999) value ORDER BY convert_to('0x' || LPAD(TO_HEX(value), 64, '0'), 'UTF8')")
        .fetch_all(&database.lookup_pool).await?;
    assert_eq!(database_order, byte_order, "25,000 lowercase hexadecimal strings under {locale}");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_legacy_count_plan_keeps_flat_eligibility_join() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let filter = bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() };
    let chains = vec!["ethereum-mainnet".into()];
    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ");
    crate::graphql::push_filtered_names(&mut builder, &filter, None, None, Some(&chains));
    builder.push(" SELECT COUNT(*) FROM filtered_names");
    let explain: Value = builder.build().fetch_one(&database.lookup_pool).await?.try_get(0)?;
    let surface = plan_nodes(&explain).into_iter().find(|node| node["Relation Name"] == "name_surfaces").context("name_surfaces plan")?;
    assert_eq!(surface["Actual Loops"], 1, "count eligibility must remain flat: {surface}");
    assert!(explain[0].get("JIT").is_none(), "count plan must not trigger JIT: {explain}");
    database.cleanup().await
}
