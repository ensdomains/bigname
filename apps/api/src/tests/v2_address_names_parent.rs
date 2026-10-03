// `parent` on `GET /v1/addresses/{address}/names` (TYR-199): only names exactly one label below
// the given name, before grouping, paging and `page.total_count`, on every relation.

async fn address_names_parent_status(database: &TestDatabase, uri: &str) -> Result<StatusCode> {
    Ok(v2_address_names_response_for_database(database, uri)
        .await?
        .status())
}

#[tokio::test]
async fn v2_address_names_parent_selects_names_one_label_below() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    let base = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens");
    let read = |query: &str| {
        let uri = format!("{base}&page_size=1&{query}");
        let database = &database;
        async move { read_family_pages(database, &uri).await }
    };

    // The registrations held: the two leases, not the named subname or the two registry
    // children, with the count every page reports.
    let all = read("relation=owner&dedupe=registration").await?;
    assert_eq!(all[0]["total_count"], json!(5), "{all:#?}");
    for query in [
        "relation=owner&dedupe=registration&parent=eth",
        "relation=owner&parent=eth",
        "parent=ETH",
    ] {
        let pages = read(query).await?;
        assert_eq!(names_of(&rows_of(&pages)), ["alpha.eth", "zeta.eth"], "{query}");
        assert!(
            pages.iter().all(|page| page["total_count"] == json!(2)),
            "{query}: {pages:#?}"
        );
    }
    // Below a second-level name: its named subname and its registry children with no name row.
    let mut below_alpha = vec![
        "gains.alpha.eth".to_owned(),
        "known.alpha.eth".to_owned(),
        placeholder("unknown", "alpha.eth"),
    ];
    below_alpha.sort();
    for query in ["parent=alpha.eth", "relation=manager&parent=alpha.eth"] {
        let pages = read(query).await?;
        assert_eq!(names_of(&rows_of(&pages)), below_alpha, "{query}");
        assert_eq!(pages[0]["total_count"], json!(3), "{query}");
    }
    for query in ["parent=known.alpha.eth", "parent=zeta.eth", "parent=base.eth"] {
        let pages = read(query).await?;
        assert!(rows_of(&pages).is_empty(), "{query}: {pages:#?}");
        assert_eq!(pages[0]["total_count"], json!(0), "{query}");
    }

    for query in ["parent=", "parent=a..eth", "parent=%25", "parent=eth&parent=eth"] {
        assert_eq!(
            address_names_parent_status(&database, &format!("{base}&{query}")).await?,
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }

    // A cursor binds the normalized parent, and an unfiltered cursor binds none.
    let first =
        v2_address_names_payload_for_database(&database, &format!("{base}&parent=eth&page_size=1"))
            .await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("cursor");
    let unfiltered =
        v2_address_names_payload_for_database(&database, &format!("{base}&page_size=1")).await?;
    let unfiltered_cursor = unfiltered["page"]["next_cursor"].as_str().expect("cursor");
    let next = v2_address_names_payload_for_database(
        &database,
        &format!("{base}&parent=ETH&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(names(next["data"].as_array().unwrap()), ["zeta.eth"]);
    for uri in [
        format!("{base}&page_size=1&cursor={cursor}"),
        format!("{base}&parent=alpha.eth&page_size=1&cursor={cursor}"),
        format!("{base}&parent=eth&page_size=1&cursor={unfiltered_cursor}"),
    ] {
        assert_eq!(
            address_names_parent_status(&database, &uri).await?,
            StatusCode::BAD_REQUEST,
            "{uri}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_parent_filters_resolves_to() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    let base = format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to");

    for (query, expected) in [
        ("parent=eth", vec!["alpha.eth", "gamma.eth"]),
        ("parent=alpha.eth", vec![]),
    ] {
        let payload =
            v2_address_names_payload_for_database(&database, &format!("{base}&{query}")).await?;
        assert_eq!(names(payload["data"].as_array().unwrap()), expected, "{query}");
    }
    let evm = v2_address_names_payload_for_database(&database, &format!("{base}&coin_type=evm"))
        .await?;
    assert!(!evm["data"].as_array().unwrap().is_empty(), "{evm}");
    assert_eq!(
        v2_address_names_payload_for_database(
            &database,
            &format!("{base}&coin_type=evm&parent=eth")
        )
        .await?["data"],
        evm["data"]
    );
    let below = v2_address_names_payload_for_database(
        &database,
        &format!("{base}&coin_type=evm&parent=alpha.eth"),
    )
    .await?;
    assert_eq!(below["data"], json!([]), "{below}");

    let first = v2_address_names_payload_for_database(
        &database,
        &format!("{base}&parent=eth&page_size=1"),
    )
    .await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("cursor");
    assert_eq!(
        address_names_parent_status(&database, &format!("{base}&page_size=1&cursor={cursor}"))
            .await?,
        StatusCode::BAD_REQUEST
    );

    database.cleanup().await?;
    Ok(())
}

/// A former name matches by its normalized spelling, as on the other relations, even when its
/// stored spelling is not normalized.
#[tokio::test]
async fn v2_address_names_parent_filters_former_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_former_cursor_names_spelled(
        &database,
        true,
        [
            "dated-a.eth",
            "kid.former.eth",
            "dated-c.eth",
            "unregistered-a.eth",
            "unregistered-b.eth",
        ],
    )
    .await?;
    // The surface keeps the verbatim spelling; the composed name normalizes it.
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces SET raw_name = 'kid.Former.eth'
         WHERE raw_name = 'kid.former.eth'",
    )
    .execute(&database.pool)
    .await?;
    publish_v2_names_fixture(&database).await?;
    let base = former_cursor_route();

    let all = rows_of(&read_family_pages(&database, &format!("{base}&page_size=2")).await?);
    assert_eq!(all.len(), 5, "{all:#?}");
    let eth =
        rows_of(&read_family_pages(&database, &format!("{base}&parent=eth&page_size=2")).await?);
    let mut expected = all.clone();
    expected.retain(|row| row["name"] != json!("kid.former.eth"));
    assert_eq!(eth, expected);
    for (parent, expected) in [
        ("former.eth", vec!["kid.former.eth"]),
        ("Former.eth", vec!["kid.former.eth"]),
        ("dated-a.eth", vec![]),
    ] {
        let payload =
            v2_address_names_payload_for_database(&database, &format!("{base}&parent={parent}"))
                .await?;
        assert_eq!(names(payload["data"].as_array().unwrap()), expected, "{parent}");
    }

    let first =
        v2_address_names_payload_for_database(&database, &format!("{base}&parent=eth&page_size=1"))
            .await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("cursor");
    assert_eq!(
        address_names_parent_status(&database, &format!("{base}&page_size=1&cursor={cursor}"))
            .await?,
        StatusCode::BAD_REQUEST
    );

    database.cleanup().await
}
