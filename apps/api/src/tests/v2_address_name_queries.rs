//! Name-list query controls (TYR-69, TYR-74, TYR-75): `match=contains` on the address-names and
//! subnames `q`, `sort=created_at` on address names, and `authority` sets on address names.
use super::*;

/// Every row of `uri`, following `next_cursor`; each page must report `total` when given.
async fn walk_address_names(
    database: &TestDatabase,
    uri: &str,
    total: Option<usize>,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut next = uri.to_owned();
    loop {
        let page = v2_address_names_payload_for_database(database, &next).await?;
        if let Some(total) = total {
            assert_eq!(page["page"]["total_count"], json!(total), "{next}: {page}");
        }
        rows.extend(
            page["data"]
                .as_array()
                .expect("data must be an array")
                .clone(),
        );
        match page["page"]["next_cursor"].as_str() {
            Some(cursor) => next = format!("{uri}&cursor={cursor}"),
            None => return Ok(rows),
        }
    }
}

async fn address_names_status(database: &TestDatabase, uri: &str) -> Result<StatusCode> {
    Ok(v2_address_names_response_for_database(database, uri)
        .await?
        .status())
}

fn row_names(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("row name").to_owned())
        .collect()
}

#[tokio::test]
async fn v2_address_names_match_contains_filters_before_paging_and_counting() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let base = format!("/v1/addresses/{V2_ADDRESS}/names");

    for (query, expected) in [
        // `ha` is inside alpha and shared-*, and starts no name.
        (
            "q=ha&match=contains",
            vec!["alpha.eth", "shared-one.eth", "shared-two.eth"],
        ),
        ("q=ha", vec![]),
        (
            "q=HA&match=contains",
            vec!["alpha.eth", "shared-one.eth", "shared-two.eth"],
        ),
        // A leading dot is a label boundary kept for matching; a trailing one too.
        (
            "q=.eth&match=contains",
            vec![
                "alpha.eth",
                "beta.eth",
                "gamma.eth",
                "shared-one.eth",
                "shared-two.eth",
            ],
        ),
        (
            "q=a.&match=contains",
            vec!["alpha.eth", "beta.eth", "gamma.eth"],
        ),
        ("q=a.&match=prefix", vec![]),
        (
            "q=-&match=contains",
            vec!["shared-one.eth", "shared-two.eth"],
        ),
        // `_` is a LIKE wildcard; it must match only a literal underscore, which no name has.
        ("q=_&match=contains", vec![]),
        // Contains combines with the other filters before paging.
        (
            "q=ha&match=contains&relation=owner",
            vec!["alpha.eth", "shared-one.eth", "shared-two.eth"],
        ),
        (
            "q=ha&match=contains&dedupe=registration",
            vec!["alpha.eth", "shared-one.eth", "shared-two.eth"],
        ),
        (
            "q=a.&match=contains&sort=expires_at&order=desc",
            vec!["gamma.eth", "alpha.eth", "beta.eth"],
        ),
    ] {
        let rows = walk_address_names(
            &database,
            &format!("{base}?{query}&page_size=1"),
            Some(expected.len()),
        )
        .await?;
        assert_eq!(row_names(&rows), expected, "{query}");
        let whole =
            v2_address_names_payload_for_database(&database, &format!("{base}?{query}")).await?;
        assert_eq!(whole["data"].as_array().unwrap().clone(), rows, "{query}");
    }

    // `match` without `q` selects nothing; an empty `q` is still absent.
    let unfiltered = v2_address_names_payload_for_database(&database, &base).await?;
    for query in ["match=contains", "q=&match=contains", "match=prefix"] {
        let payload =
            v2_address_names_payload_for_database(&database, &format!("{base}?{query}")).await?;
        assert_eq!(payload, unfiltered, "{query}");
    }

    for query in [
        "q=ha&match=fuzzy",
        "q=ha&match=CONTAINS",
        // `%` and `\` are not name characters: the normalizer refuses them.
        "q=%25&match=contains",
        "q=%5C&match=contains",
        "q=.&match=contains",
        "q=..&match=contains",
        "q=a&match=contains&match=prefix",
    ] {
        assert_eq!(
            address_names_status(&database, &format!("{base}?{query}")).await?,
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }

    // A cursor binds the match mode: a contains cursor does not resume a prefix page and back.
    let contains_page = v2_address_names_payload_for_database(
        &database,
        &format!("{base}?q=a&match=contains&page_size=1"),
    )
    .await?;
    let contains_cursor = contains_page["page"]["next_cursor"]
        .as_str()
        .expect("cursor");
    let prefix_page =
        v2_address_names_payload_for_database(&database, &format!("{base}?q=a&page_size=1"))
            .await?;
    let prefix_cursor = prefix_page["page"]["next_cursor"].as_str();
    for uri in [
        format!("{base}?q=a&page_size=1&cursor={contains_cursor}"),
        format!("{base}?q=a&match=prefix&page_size=1&cursor={contains_cursor}"),
    ] {
        assert_eq!(
            address_names_status(&database, &uri).await?,
            StatusCode::BAD_REQUEST,
            "{uri}"
        );
    }
    assert!(prefix_cursor.is_none(), "{prefix_page}");
    let prefix_cursor =
        v2_address_names_payload_for_database(&database, &format!("{base}?q=shared&page_size=1"))
            .await?["page"]["next_cursor"]
            .as_str()
            .expect("prefix cursor")
            .to_owned();
    assert_eq!(
        address_names_status(
            &database,
            &format!("{base}?q=shared&match=contains&page_size=1&cursor={prefix_cursor}")
        )
        .await?,
        StatusCode::BAD_REQUEST
    );
    // An explicit `match=prefix` is the default and resumes a prefix cursor.
    assert_eq!(
        address_names_status(
            &database,
            &format!("{base}?q=shared&match=prefix&page_size=1&cursor={prefix_cursor}")
        )
        .await?,
        StatusCode::OK
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_sort_by_created_at_is_first_observation_with_identity_ties() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    // alpha.eth migrates to ENSv2 at the head; its first observation does not move.
    bind_address_name_ens_v2(&database, "alpha.eth", 0xa200, true).await?;
    let base = format!("/v1/addresses/{V2_ADDRESS}/names");

    let created = |rows: &[Value]| -> Vec<(String, Option<String>)> {
        rows.iter()
            .map(|row| {
                (
                    row["name"].as_str().expect("row name").to_owned(),
                    row.get("created_at")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                )
            })
            .collect()
    };
    let asc = walk_address_names(
        &database,
        &format!("{base}?sort=created_at&page_size=1"),
        Some(5),
    )
    .await?;
    // shared-one and shared-two were first observed at the same block: identity breaks the tie
    // the same way in both directions.
    assert_eq!(
        row_names(&asc),
        [
            "alpha.eth",
            "beta.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ],
        "{:?}",
        created(&asc)
    );
    let desc = walk_address_names(
        &database,
        &format!("{base}?sort=created_at&order=desc&page_size=1"),
        Some(5),
    )
    .await?;
    assert_eq!(
        row_names(&desc),
        [
            "shared-one.eth",
            "shared-two.eth",
            "gamma.eth",
            "beta.eth",
            "alpha.eth"
        ],
        "{:?}",
        created(&desc)
    );
    // The order is the served `created_at`, not registration time, and every row has one.
    let served = created(&asc);
    let times = served
        .iter()
        .map(|(name, at)| {
            let at = at
                .as_deref()
                .unwrap_or_else(|| panic!("{name} has no created_at"));
            bigname_storage::parse_rfc3339_utc_timestamp(at)
                .map_err(|error| anyhow::anyhow!("{name}: {error}"))
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(
        times.windows(2).all(|pair| pair[0] <= pair[1]),
        "{served:?}"
    );
    assert_eq!(times[3], times[4], "{served:?}");
    let registered =
        v2_address_names_payload_for_database(&database, &format!("{base}?sort=registered_at"))
            .await?;
    assert_ne!(
        row_names(registered["data"].as_array().unwrap()),
        row_names(&asc),
        "registered_at and created_at differ on this fixture"
    );

    // Filters narrow before the sort pages.
    let owned = walk_address_names(
        &database,
        &format!("{base}?sort=created_at&order=desc&relation=registrant&page_size=1"),
        Some(4),
    )
    .await?;
    assert_eq!(
        row_names(&owned),
        ["shared-one.eth", "shared-two.eth", "gamma.eth", "alpha.eth"]
    );

    // A created_at cursor resumes only a created_at page of the same order.
    let first = v2_address_names_payload_for_database(
        &database,
        &format!("{base}?sort=created_at&page_size=2"),
    )
    .await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("cursor");
    for uri in [
        format!("{base}?sort=registered_at&page_size=2&cursor={cursor}"),
        format!("{base}?sort=created_at&order=desc&page_size=2&cursor={cursor}"),
        format!("{base}?page_size=2&cursor={cursor}"),
    ] {
        assert_eq!(
            address_names_status(&database, &uri).await?,
            StatusCode::BAD_REQUEST,
            "{uri}"
        );
    }
    database.cleanup().await
}

/// Every non-empty set of the three authority values.
const AUTHORITY_SETS: [&[&str]; 7] = [
    &["ens_v0"],
    &["ens_v1"],
    &["ens_v2"],
    &["ens_v0", "ens_v1"],
    &["ens_v0", "ens_v2"],
    &["ens_v1", "ens_v2"],
    &["ens_v0", "ens_v1", "ens_v2"],
];

#[tokio::test]
async fn v2_address_names_authority_sets_match_any_listed_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"base": {
            "chain_id": "base-mainnet", "block_number": 1, "block_hash": "0xcount-base-empty",
            "timestamp": "2024-01-01T00:00:00Z"
        }}))
        .await?;
    republish_fixture_chain(&database, "base-mainnet").await?;
    // alpha ens_v0, beta and gamma ens_v1, shared-two ens_v2; shared-one is ownerless and serves
    // no authority, so no set matches it where it is listed.
    seed_authority_shape_names(&database, V2_ADDRESS).await?;

    for (base, counted) in [
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1"),
            true,
        ),
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?sort=created_at&order=desc&page_size=1"),
            true,
        ),
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?dedupe=registration&page_size=1"),
            true,
        ),
        (
            format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1"),
            false,
        ),
        (
            format!(
                "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm&page_size=1"
            ),
            false,
        ),
    ] {
        let unfiltered = walk_address_names(&database, &base, None).await?;
        let served: BTreeSet<&str> = unfiltered
            .iter()
            .filter_map(|row| row.get("authority").and_then(Value::as_str))
            .collect();
        if counted {
            assert_eq!(
                served,
                BTreeSet::from(["ens_v0", "ens_v1", "ens_v2"]),
                "{base}"
            );
        } else {
            assert!(!served.is_empty(), "{base}");
        }
        for set in AUTHORITY_SETS {
            let expected = unfiltered
                .iter()
                .filter(|row| {
                    row.get("authority")
                        .and_then(Value::as_str)
                        .is_some_and(|authority| set.contains(&authority))
                })
                .cloned()
                .collect::<Vec<_>>();
            // Listing order, repeats and blank segments do not change the set.
            let mut reversed = set.to_vec();
            reversed.reverse();
            for spelling in [
                set.join(","),
                reversed.join(","),
                format!("{},{}", set.join(","), set[0]),
                format!(" {} ,", reversed.join(" , ")),
            ] {
                let uri = format!("{base}&authority={}", spelling.replace(' ', "%20"));
                let rows =
                    walk_address_names(&database, &uri, counted.then_some(expected.len())).await?;
                assert_eq!(rows, expected, "{uri}");
            }
        }
        // Rows keep their own authority: a combined set is the union, not a relabelling.
        let all = walk_address_names(
            &database,
            &format!("{base}&authority=ens_v0,ens_v1,ens_v2"),
            None,
        )
        .await?;
        assert!(
            all.iter().all(|row| row.get("authority").is_some()),
            "{base}: {all:?}"
        );
    }

    let names_uri = format!("/v1/addresses/{V2_ADDRESS}/names");
    let both = walk_address_names(
        &database,
        &format!("{names_uri}?authority=ens_v0,ens_v1"),
        Some(3),
    )
    .await?;
    assert_eq!(row_names(&both), ["alpha.eth", "beta.eth", "gamma.eth"]);
    let authorities: Vec<&str> = both
        .iter()
        .map(|row| row["authority"].as_str().expect("authority"))
        .collect();
    assert_eq!(authorities, ["ens_v0", "ens_v1", "ens_v1"]);

    // The cursor binds the set, spelled canonically: a reordered set resumes, another does not,
    // and a one-value set is the single value it always was.
    let first = v2_address_names_payload_for_database(
        &database,
        &format!("{names_uri}?authority=ens_v1,ens_v0&page_size=1"),
    )
    .await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("cursor");
    assert_eq!(
        address_names_status(
            &database,
            &format!("{names_uri}?authority=ens_v0,ens_v1&page_size=1&cursor={cursor}")
        )
        .await?,
        StatusCode::OK
    );
    for uri in [
        format!("{names_uri}?authority=ens_v0&page_size=1&cursor={cursor}"),
        format!("{names_uri}?authority=ens_v0,ens_v1,ens_v2&page_size=1&cursor={cursor}"),
        format!("{names_uri}?page_size=1&cursor={cursor}"),
    ] {
        assert_eq!(
            address_names_status(&database, &uri).await?,
            StatusCode::BAD_REQUEST,
            "{uri}"
        );
    }
    let single = v2_address_names_payload_for_database(
        &database,
        &format!("{names_uri}?authority=ens_v1&page_size=1"),
    )
    .await?;
    let single_cursor = single["page"]["next_cursor"]
        .as_str()
        .expect("single cursor");
    assert_eq!(
        address_names_status(
            &database,
            &format!("{names_uri}?authority=ens_v1,ens_v1&page_size=1&cursor={single_cursor}")
        )
        .await?,
        StatusCode::OK
    );

    // Whitespace-only is absent; a list with no value, an unknown value or a repeated parameter
    // is invalid.
    let unfiltered = v2_address_names_payload_for_database(&database, &names_uri).await?;
    assert_eq!(
        v2_address_names_payload_for_database(&database, &format!("{names_uri}?authority=%20"))
            .await?,
        unfiltered
    );
    for query in [
        "authority=,",
        "authority=ens_v1,basenames",
        "authority=ens_v1,,ens_v3",
        "authority=ENS_V1",
        "authority=ens_v1&authority=ens_v2",
    ] {
        assert_eq!(
            address_names_status(&database, &format!("{names_uri}?{query}")).await?,
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_subnames_match_contains_filters_before_paging_and_counting() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;

    let served = |pages: &[Value]| -> Vec<String> {
        pages
            .iter()
            .flat_map(|page| page["data"].as_array().into_iter().flatten())
            .map(|row| row["name"].as_str().expect("name").to_owned())
            .collect()
    };
    for (query, expected) in [
        (
            "q=o&match=contains",
            vec!["carol.alpha.eth", "one.alpha.eth", "two.alpha.eth"],
        ),
        ("q=o", vec!["one.alpha.eth"]),
        ("q=ar&match=contains", vec!["carol.alpha.eth"]),
        (
            "q=e.alpha&match=contains",
            vec!["dave.alpha.eth", "one.alpha.eth"],
        ),
        (
            "q=.alpha.eth&match=contains",
            vec![
                "carol.alpha.eth",
                "dave.alpha.eth",
                "one.alpha.eth",
                "two.alpha.eth",
            ],
        ),
        ("q=_&match=contains", vec![]),
        // Undated carol sorts first descending, as without `match`.
        (
            "q=o&match=contains&sort=expires_at&order=desc",
            vec!["carol.alpha.eth", "one.alpha.eth", "two.alpha.eth"],
        ),
    ] {
        let pages = read_family_pages(
            &database,
            &format!("/v1/names/alpha.eth/subnames?{query}&page_size=1"),
        )
        .await?;
        let mut names = served(&pages);
        if !query.contains("sort=") {
            names.sort();
        }
        assert_eq!(names, expected, "{query}: {pages:#?}");
        for page in &pages {
            assert_eq!(
                page["total_count"],
                json!(expected.len()),
                "{query}: {pages:#?}"
            );
        }
    }

    let first = read_family_response(
        &database,
        "/v1/names/alpha.eth/subnames?q=o&match=contains&page_size=1",
    )
    .await?
    .1;
    let cursor = first["page"]["next_cursor"].as_str().expect("cursor");
    for uri in [
        format!("/v1/names/alpha.eth/subnames?q=o&page_size=1&cursor={cursor}"),
        "/v1/names/alpha.eth/subnames?sort=created_at".to_owned(),
        "/v1/names/alpha.eth/subnames?q=o&match=substring".to_owned(),
        "/v1/names/alpha.eth/subnames?q=%25&match=contains".to_owned(),
    ] {
        let (status, body) = read_family_response(&database, &uri).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body:#}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_routes_without_name_list_controls_reject_them() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;
    for uri in [
        "/v1/registries/1/0x00000000000000000000000000000000000000b1/labels?match=contains",
        "/v1/names?namespace=ens&expires_after=2024-01-01T00:00:00Z&match=contains",
    ] {
        let (status, body) = read_family_response(&database, uri).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body:#}");
    }
    database.cleanup().await
}
