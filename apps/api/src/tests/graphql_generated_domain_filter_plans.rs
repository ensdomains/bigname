fn plan_domain_filter(member: &str, value: &str) -> crate::graphql::GeneratedDomainFilter {
    let mut filter = crate::graphql::GeneratedDomainFilter::default();
    let list = || Some(vec![value.to_owned()]);
    match member {
        "id" => filter.id.eq = Some(value.into()),
        "id_not" => filter.id.not = Some(value.into()),
        "id_gt" => filter.id.gt = Some(value.into()),
        "id_gte" => filter.id.gte = Some(value.into()),
        "id_lt" => filter.id.lt = Some(value.into()),
        "id_lte" => filter.id.lte = Some(value.into()),
        "id_in" => filter.id.in_values = list(),
        "id_not_in" => filter.id.not_in_values = list(),
        "name" => filter.name.eq = Some(value.into()),
        "name_not" => filter.name.not = Some(value.into()),
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

fn assert_domain_plan_bounds(explain: &Value, label: &str) -> Result<()> {
    let plan = &explain[0]["Plan"];
    assert_eq!(plan["Node Type"], "Limit", "{label}");
    let text = serde_json::to_string(plan)?;
    assert!(text.contains("name_current"), "predicate must execute below Limit: {label}");
    for node in plan_nodes(explain) {
        assert!(node["Actual Loops"].as_u64().unwrap_or(0) <= 5_100, "loops: {label}: {node}");
        assert!(node["Shared Hit Blocks"].as_u64().unwrap_or(0) <= 10_000, "hits: {label}: {node}");
        assert!(node["Shared Read Blocks"].as_u64().unwrap_or(0) <= 128, "reads: {label}: {node}");
        assert!(node["Rows Removed by Filter"].as_u64().unwrap_or(0) <= 5_100, "removed: {label}: {node}");
        if node["Node Type"] == "Sort" {
            assert!(node["Actual Rows"].as_u64().unwrap_or(0) <= 5_100, "sort rows: {label}");
            assert!(node["Sort Space Used"].as_u64().unwrap_or(0) <= 2_048, "sort memory: {label}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn graphql_generated_domain_operator_plans_are_bounded_below_limit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    let late_id = format!("0x{:064x}", 5_999);
    let members = [
        "id", "id_not", "id_gt", "id_gte", "id_lt", "id_lte", "id_in", "id_not_in",
        "name", "name_not", "name_gt", "name_gte", "name_lt", "name_lte", "name_in", "name_not_in",
        "name_contains", "name_contains_nocase", "name_not_contains", "name_not_contains_nocase",
        "name_starts_with", "name_starts_with_nocase", "name_not_starts_with", "name_not_starts_with_nocase",
        "name_ends_with", "name_ends_with_nocase", "name_not_ends_with", "name_not_ends_with_nocase",
    ];
    for member in members {
        let value = if member.starts_with("id") { late_id.as_str() } else if member.contains("starts") { "alice" } else if member.contains("ends") { "5999" } else if member.contains("contains") { "5999" } else { "alice.eth5999" };
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &plan_domain_filter(member, value),
            crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Desc,
            200, 0,
        ).await?;
        println!("DOMAIN OPERATOR {member} PLAN {}", serde_json::to_string_pretty(&explain)?);
        assert_domain_plan_bounds(&explain, member)?;
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_order_plans_sort_before_limit_with_fixed_bounds() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    for sort in crate::graphql::GeneratedDomainSort::ALL {
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &Default::default(), sort,
            bigname_storage::NameCurrentListOrder::Asc, 200, 0,
        ).await?;
        println!("DOMAIN ORDER {sort:?} PLAN {}", serde_json::to_string_pretty(&explain)?);
        assert_domain_plan_bounds(&explain, &format!("{sort:?}"))?;
    }
    database.cleanup().await
}
