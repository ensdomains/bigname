use super::*;

/// The public `authority` value for each projected selection shape Project writes, keyed by the
/// address-names fixture's names. `shared-one.eth` is the ownerless registry profile: it keeps
/// its ENSv1 arm (and here its 2017-registry generation) but serves no `authority`.
fn ens_v0_selections() -> [(&'static str, Value, Option<&'static str>); 5] {
    [
        (
            "alpha.eth",
            json!({"authority_arm": "ens_v1", "registry_generation": "old"}),
            Some("ens_v0"),
        ),
        (
            "beta.eth",
            json!({"authority_arm": "ens_v1", "registry_generation": "current",
                   "registry_handoff_block_number": 90}),
            Some("ens_v1"),
        ),
        // A row projected before registry generation existed reads as `ens_v1`.
        (
            "gamma.eth",
            json!({"authority_arm": "ens_v1"}),
            Some("ens_v1"),
        ),
        (
            "shared-one.eth",
            json!({"authority_arm": "ens_v1", "registry_generation": "old",
                   "ownerless_registry": true}),
            None,
        ),
        (
            "shared-two.eth",
            json!({"authority_arm": "ens_v2"}),
            Some("ens_v2"),
        ),
    ]
}

async fn stamp_ens_v0_selections(database: &TestDatabase) -> Result<()> {
    for (name, selection, _) in ens_v0_selections() {
        sqlx::query(
            "UPDATE bigname_phase.name_current
             SET provenance = provenance || jsonb_build_object('authority_selection', $2::jsonb)
             WHERE namespace = 'ens' AND raw_name = $1",
        )
        .bind(name)
        .bind(selection)
        .execute(&database.pool)
        .await?;
    }
    Ok(())
}

fn expected_authority(name: &str) -> Option<&'static str> {
    ens_v0_selections()
        .into_iter()
        .find(|(candidate, _, _)| *candidate == name)
        .and_then(|(_, _, authority)| authority)
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
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    stamp_ens_v0_selections(&database).await?;

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
    // The filter applies before grouping: a registration shared by an ownerless name and an
    // ENSv2 name is represented by the ENSv2 member under `authority=ens_v2`.
    let grouped = all_address_name_rows(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?dedupe=registration&authority=ens_v2"),
    )
    .await?;
    assert_eq!(names(&grouped), vec!["shared-two.eth"]);
    let grouped_all = all_address_name_rows(
        &database,
        &format!("/v1/addresses/{V2_ADDRESS}/names?dedupe=registration"),
    )
    .await?;
    assert!(
        grouped_all
            .iter()
            .any(|row| row["name"] == "shared-one.eth" && row.get("authority").is_none())
    );
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
    for (index, (name, _, _)) in ens_v0_selections().into_iter().enumerate() {
        let index = index as u128;
        seed_identity_name(
            &database,
            &format!("ens:{name}"),
            name,
            name,
            &format!("namehash:{name}"),
            Uuid::from_u128(0x7170_0100 + index),
            Uuid::from_u128(0x7170_0200 + index),
            Uuid::from_u128(0x7170_0300 + index),
            HOLDER,
            bigname_storage::AddressNameRelation::TokenHolder,
            38,
        )
        .await?;
    }
    stamp_ens_v0_selections(&database).await?;

    for (name, _, expected) in ens_v0_selections() {
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
            "inputs": ens_v0_selections()
                .iter()
                .map(|(name, _, _)| json!({"id": name, "name": name}))
                .chain([json!({"id": "reverse", "address": HOLDER})])
                .collect::<Vec<_>>(),
        }),
    )
    .await?;
    let results = lookup["data"].as_array().expect("lookup results");
    for (index, (name, _, expected)) in ens_v0_selections().into_iter().enumerate() {
        let record = &results[index]["record"];
        assert_eq!(
            record.get("authority").and_then(Value::as_str),
            expected,
            "{name}: {record}"
        );
        assert!(record.get("migrated_at").is_none(), "{name}: {record}");
    }
    let reverse = results[ens_v0_selections().len()]["records"]
        .as_array()
        .expect("reverse detail records");
    assert_eq!(reverse.len(), ens_v0_selections().len(), "{lookup}");
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
