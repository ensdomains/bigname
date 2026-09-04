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

#[tokio::test]
async fn graphql_generated_domain_operator_plans_are_index_bounded_or_linear() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    let late_id = format!("0x{:064x}", 5_999);
    let database_range = sqlx::query_scalar::<_, String>("SELECT namehash FROM bigname_phase.name_current WHERE namespace = 'ens' AND namehash >= $1 ORDER BY namehash").bind("0x").fetch_all(&database.lookup_pool).await?;
    let c_range = sqlx::query_scalar::<_, String>("SELECT namehash FROM bigname_phase.name_current WHERE namespace = 'ens' AND (namehash COLLATE \"C\") >= ($1 COLLATE \"C\") ORDER BY namehash COLLATE \"C\"").bind("0x").fetch_all(&database.lookup_pool).await?;
    assert_eq!(database_range, c_range, "fixed-width hexadecimal range order");
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
            assert!(plan_nodes(&explain).iter().any(|node| node["Node Type"] == "Sort"), "sort must occur below Limit: {sort:?}");
        }
    }
    database.cleanup().await
}
