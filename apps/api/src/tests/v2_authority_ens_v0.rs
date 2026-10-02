use super::*;

/// The public `authority` each name of `seed_authority_shape_names` serves. `shared-one.eth`
/// is the ownerless registry profile: it keeps its ENSv1 arm (and its 2017-registry generation)
/// but serves no `authority`.
fn expected_authority(name: &str) -> Option<&'static str> {
    match name {
        "alpha.eth" => Some("ens_v0"),
        "beta.eth" | "gamma.eth" => Some("ens_v1"),
        "shared-two.eth" => Some("ens_v2"),
        _ => None,
    }
}

/// Every page of `uri`, following `next_cursor`.
async fn all_address_name_rows(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut next = uri.to_owned();
    loop {
        let page = v2_address_names_payload_for_database(database, &next).await?;
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

#[tokio::test]
async fn v2_address_names_split_the_ens_v1_arm_by_registry_generation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // The routes read both public namespaces; the Base index is published and empty.
    database
        .seed_snapshot_selector_chain_positions(&json!({"base": {
            "chain_id": "base-mainnet", "block_number": 1, "block_hash": "0xcount-base-empty",
            "timestamp": "2024-01-01T00:00:00Z"
        }}))
        .await?;
    republish_fixture_chain(&database, "base-mainnet").await?;
    seed_authority_shape_names(&database, V2_ADDRESS).await?;

    for base in [
        format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1"),
        format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1"),
        format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&coin_type=evm&page_size=1"),
    ] {
        let unfiltered = all_address_name_rows(&database, &base).await?;
        assert!(!unfiltered.is_empty(), "{base}");
        for row in &unfiltered {
            let name = row["name"].as_str().expect("row name");
            assert_eq!(
                row.get("authority").and_then(Value::as_str),
                expected_authority(name),
                "{base}: {row}"
            );
            if row.get("authority") != Some(&json!("ens_v2")) {
                assert!(row.get("migrated_at").is_none(), "{base}: {row}");
            }
        }
        for value in ["ens_v0", "ens_v1", "ens_v2"] {
            let filtered =
                all_address_name_rows(&database, &format!("{base}&authority={value}")).await?;
            let expected = unfiltered
                .iter()
                .filter(|row| row.get("authority") == Some(&json!(value)))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(filtered, expected, "{base}&authority={value}");
        }
    }
    let v0 = all_address_name_rows(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?authority=ens_v0"),
    )
    .await?;
    assert_eq!(names(&v0), vec!["alpha.eth"]);
    let v1 = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?authority=ens_v1&page_size=1"),
    )
    .await?;
    assert_eq!(names(v1["data"].as_array().unwrap()), vec!["beta.eth"]);
    assert_eq!(v1["page"]["total_count"], json!(2));
    // The filter applies before grouping.
    let grouped = all_address_name_rows(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?dedupe=registration&authority=ens_v2"),
    )
    .await?;
    assert_eq!(names(&grouped), vec!["shared-two.eth"]);
    let resolved_v0 = all_address_name_rows(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&authority=ens_v0"),
    )
    .await?;
    assert_eq!(names(&resolved_v0), vec!["alpha.eth"]);

    // `ens_v0` has nothing to do with the ENSv1→ENSv2 migration.
    let migrated = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?is_migrated=true&authority=ens_v0"),
    )
    .await?;
    assert_eq!(migrated["data"], json!([]));

    let cursor = v1["page"]["next_cursor"].as_str().expect("ens_v1 cursor");
    for uri in [
        format!("/v1/addresses/{V2_ADDRESS}/names?authority=ens_v0&page_size=1&cursor={cursor}"),
        format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&cursor={cursor}"),
        format!("/v1/addresses/{V2_ADDRESS}/names?authority=ens_v3"),
        format!("/v1/addresses/{V2_ADDRESS}/names?authority=basenames"),
    ] {
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
    let resolved = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&page_size=1"),
    )
    .await?;
    let cursor = resolved["page"]["next_cursor"]
        .as_str()
        .expect("resolves_to cursor");
    let response = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v1/addresses/{V2_ADDRESS}/names?relation=resolves_to&authority=ens_v0&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    database.cleanup().await
}

#[tokio::test]
async fn v2_name_detail_and_lookup_serve_ens_v0_and_omit_ownerless_authority() -> Result<()> {
    const HOLDER: &str = "0x0000000000000000000000000000000000000abc";
    let database = TestDatabase::new_migrated().await?;
    seed_authority_shape_names(&database, HOLDER).await?;

    for name in AUTHORITY_SHAPE_NAMES {
        let expected = expected_authority(name);
        let detail = v2_get_json(&database, &format!("/v1/names/{name}")).await?;
        assert_eq!(
            detail["data"].get("authority").and_then(Value::as_str),
            expected,
            "{name}: {detail}"
        );
    }
    let lookup = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": AUTHORITY_SHAPE_NAMES
                .iter()
                .map(|name| json!({"id": name, "name": name}))
                .chain([json!({"id": "reverse", "address": HOLDER})])
                .collect::<Vec<_>>(),
        }),
    )
    .await?;
    let results = lookup["data"].as_array().expect("lookup results");
    for (index, name) in AUTHORITY_SHAPE_NAMES.into_iter().enumerate() {
        let expected = expected_authority(name);
        let record = &results[index]["record"];
        assert_eq!(
            record.get("authority").and_then(Value::as_str),
            expected,
            "{name}: {record}"
        );
        assert!(record.get("migrated_at").is_none(), "{name}: {record}");
    }
    let reverse = results[AUTHORITY_SHAPE_NAMES.len()]["records"]
        .as_array()
        .expect("reverse detail records");
    // The ownerless name has no selected binding, so the holder has no relation to it.
    let mut listed = reverse
        .iter()
        .map(|record| record["name"].as_str().expect("reverse record name"))
        .collect::<Vec<_>>();
    listed.sort_unstable();
    assert_eq!(
        listed,
        ["alpha.eth", "beta.eth", "gamma.eth", "shared-two.eth"],
        "{lookup}"
    );
    for record in reverse {
        let name = record["name"].as_str().expect("reverse record name");
        assert_eq!(
            record.get("authority").and_then(Value::as_str),
            expected_authority(name),
            "{name}: {record}"
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rows_serve_the_authority_of_name_detail() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_authority_shape_names(&database, V2_ADDRESS).await?;

    for name in AUTHORITY_SHAPE_NAMES {
        let search = v2_get_json(&database, &format!("/v1/search?q={name}&namespace=ens")).await?;
        let row = search["data"]
            .as_array()
            .expect("search data")
            .iter()
            .find(|row| row["name"] == name)
            .unwrap_or_else(|| panic!("search must list {name}: {search}"));
        match expected_authority(name) {
            Some(authority) => assert_eq!(row["authority"], authority, "{name}: {row}"),
            None => assert!(row.get("authority").is_none(), "{name}: {row}"),
        }
    }
    database.cleanup().await
}
