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
            at.parse::<bigname_storage::UnixSeconds>()
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
        // dave's registry record was moved to the zero owner, so it has no owner and is not
        // listed (`seed_family_children_fixture`).
        ("q=e.alpha&match=contains", vec!["one.alpha.eth"]),
        (
            "q=.alpha.eth&match=contains",
            vec!["carol.alpha.eth", "one.alpha.eth", "two.alpha.eth"],
        ),
        ("q=_&match=contains", vec![]),
        // Undated carol sorts last descending, as without `match`.
        (
            "q=o&match=contains&sort=expires_at&order=desc",
            vec!["one.alpha.eth", "two.alpha.eth", "carol.alpha.eth"],
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

/// Percent-encodes every byte, so a query value reaches the route exactly as written.
fn encode_query_value(value: &str) -> String {
    value.bytes().map(|byte| format!("%{byte:02X}")).collect()
}

fn unicode_name_spec(name: &'static str, logical: &'static str, seed: u128) -> V2AddressNameSpec {
    V2AddressNameSpec {
        logical_name_id: logical,
        name,
        resource_id: Uuid::from_u128(seed),
        token_lineage_id: Uuid::from_u128(seed + 1),
        surface_binding_id: Uuid::from_u128(seed + 2),
        block_hash: "0xname349",
        block_number: 349,
        owner: "0x0000000000000000000000000000000000000349",
        registrant: V2_ADDRESS,
        registered_at: "2024-01-02T00:00:00Z",
        created_at: "2023-01-02T00:00:00Z",
        expires_at: "2027-01-02T00:00:00Z",
        relations: &[bigname_storage::AddressNameRelation::TokenHolder],
    }
}

// `match=contains` normalizes its fragment as a name. A fragment that is a valid name on its
// own matches wherever its bytes occur, even inside a longer emoji sequence; a substring that is
// not a valid name alone (a leading combining mark, a joiner, a lone skin-tone modifier) is not
// an admissible query and returns 400, although valid indexed names contain it.
#[tokio::test]
async fn v2_address_names_contains_admits_only_fragments_that_are_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let specs = [
        unicode_name_spec("नमस्ते.eth", "ens:नमस्ते.eth", 0x3_4c00),
        unicode_name_spec("👨\u{200d}💻.eth", "ens:👨\u{200d}💻.eth", 0x3_4d00),
        unicode_name_spec("👍🏽.eth", "ens:👍🏽.eth", 0x3_4e00),
    ];
    seed_v2_address_name_identities(&database, &specs).await?;
    publish_v2_address_name_inputs(&database, &specs).await?;
    assert_v2_address_name_relations(&database, &specs).await?;
    let base = format!("/v1/addresses/{V2_ADDRESS}/names");

    for (fragment, expected) in [
        ("स\u{94d}", vec!["नमस्ते.eth"]),
        ("ते", vec!["नमस्ते.eth"]),
        ("👨", vec!["👨\u{200d}💻.eth"]),
        ("💻", vec!["👨\u{200d}💻.eth"]),
        ("👍", vec!["👍🏽.eth"]),
    ] {
        let uri = format!("{base}?q={}&match=contains", encode_query_value(fragment));
        let rows = walk_address_names(&database, &uri, Some(expected.len())).await?;
        assert_eq!(row_names(&rows), expected, "{fragment:?}");
    }
    for fragment in [
        "\u{94d}ते",
        "\u{94d}",
        "\u{200d}",
        "👨\u{200d}",
        "\u{200d}💻",
        "🏽",
    ] {
        let uri = format!("{base}?q={}&match=contains", encode_query_value(fragment));
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{fragment:?}");
        let payload = read_json::<Value>(response).await?;
        assert!(
            payload["error"]["message"].as_str().is_some_and(
                |message| message.starts_with("q must be a valid ENSIP-15 name substring:")
            ),
            "{fragment:?}: {payload}"
        );
    }
    database.cleanup().await
}

/// Every listed row's served `created_at` must come from the name's recorded first observation
/// (`registration.created_at`), never from the publication-time fallback the name renderer
/// keeps, so the sort key and the served value are the same instant on every row. Covers
/// registrar names, a migrated ENSv2 name, and record-serving-only names (a cleared registry
/// owner with a retained resolver pointer) listed through `relation=resolves_to`.
#[tokio::test]
async fn v2_address_names_created_at_sort_matches_the_served_first_observation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    bind_address_name_ens_v2(&database, "alpha.eth", 0xa200, true).await?;
    for (name, seed) in [("serving-one.eth", 0xe500), ("serving-two.eth", 0xf500)] {
        seed_resolves_to_ownerless_name(&database, name, seed).await?;
    }
    let base = format!("/v1/addresses/{V2_ADDRESS}/names");
    let mut serving_only_seen = BTreeSet::new();
    for relation in [
        "",
        "&relation=resolves_to",
        "&relation=resolves_to&coin_type=evm",
    ] {
        for order in ["asc", "desc"] {
            let uri = format!("{base}?sort=created_at&order={order}&page_size=2{relation}");
            let rows = walk_address_names(&database, &uri, None).await?;
            assert!(!rows.is_empty(), "{uri}");
            let ids = rows
                .iter()
                .map(|row| {
                    bigname_storage::logical_name_id_for_name(
                        "ens",
                        row["name"].as_str().expect("row name"),
                    )
                })
                .collect::<Vec<_>>();
            let composed =
                bigname_storage::load_name_current_by_logical_name_ids(&database.pool, &ids)
                    .await?;
            let mut served = Vec::new();
            for (row, id) in rows.iter().zip(&ids) {
                let name = row["name"].as_str().expect("row name");
                if name.starts_with("serving-") {
                    serving_only_seen.insert(name.to_owned());
                }
                let at = row
                    .get("created_at")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| panic!("{uri}: {name} has no created_at: {row}"));
                let recorded = composed
                    .get(id)
                    .and_then(|name_row| {
                        name_row
                            .declared_summary
                            .pointer("/registration/created_at")
                    })
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| panic!("{uri}: {name} has no recorded first observation"));
                let at = at.parse::<bigname_storage::UnixSeconds>()?;
                let recorded = time::OffsetDateTime::parse(
                    recorded,
                    &time::format_description::well_known::Rfc3339,
                )
                .map_err(|e| anyhow::anyhow!("{name}: {recorded}: {e}"))?;
                assert_eq!(at, recorded.into(), "{uri}: {name}");
                served.push(at);
            }
            let ordered = served.windows(2).all(|pair| match order {
                "asc" => pair[0] <= pair[1],
                _ => pair[0] >= pair[1],
            });
            assert!(ordered, "{uri}: {:?}", row_names(&rows));
        }
    }
    assert_eq!(
        serving_only_seen,
        BTreeSet::from(["serving-one.eth".to_owned(), "serving-two.eth".to_owned()]),
        "the record-serving-only names are listed"
    );
    database.cleanup().await
}
