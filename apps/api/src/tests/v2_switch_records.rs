
const SWITCH_REVERSE_NODE: &str =
    "0x00000000000000000000000000000000000000000000000000000000000abcde";

/// alice's direct primary claim of alpha.eth: a ReverseChanged at 210 and the reverse resolver's
/// name record naming the tuple at 211.
fn switch_primary_claim_events() -> Vec<NormalizedEvent> {
    let reverse = switch_event(
        "switch-alice-reverse",
        None,
        None,
        "ReverseChanged",
        "ens_v1_reverse_registrar_l1",
        210,
        0,
        json!({"address": SWITCH_ALICE, "coin_type": "60", "namespace": "ens",
               "reverse_node": SWITCH_REVERSE_NODE, "source_event": "NameForAddrChanged",
               "claim_provenance": {"source": "reverse_registrar"}}),
    );
    let claim = switch_event(
        "switch-alice-claim",
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        211,
        0,
        json!({"node": SWITCH_REVERSE_NODE, "record_key": "name",
               "source_event": "NameForAddrChanged", "raw_name": "alpha.eth",
               "primary_claim_source": {"address": SWITCH_ALICE, "coin_type": "60",
                                        "namespace": "ens",
                                        "reverse_node": SWITCH_REVERSE_NODE}}),
    );
    vec![reverse, claim]
}

async fn seed_switch_records_fixture(database: &TestDatabase) -> Result<()> {
    seed_switch_routes_fixture_with(database, switch_primary_claim_events()).await
}

#[tokio::test]
async fn v2_address_names_from_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let base = format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens");
    // Every page of the listing, one name a page, in each served sort and order.
    for query in [
        "page_size=1",
        "page_size=1&order=desc",
        "page_size=1&sort=expires_at",
        "page_size=1&sort=registered_at&order=desc",
        "page_size=1&dedupe=registration",
        "page_size=1&relation=registrant",
        "page_size=1&relation=owner",
        "page_size=1&relation=manager",
        "page_size=1&relation=owner,manager",
        "page_size=1&authority=ens_v1",
        "page_size=1&q=al",
        "page_size=1&include=counts",
        "page_size=1&include=role_summary",
    ] {
        let pages = read_family_pages(&database, &format!("{base}&{query}")).await?;
        let listed: usize = pages
            .iter()
            .map(|page| page["data"].as_array().map_or(0, Vec::len))
            .sum();
        assert!(listed > 0, "{query}: {pages:#?}");
    }
    let (status, body) =
        read_family_response(&database, &format!("{base}&include=counts"))
            .await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let names: Vec<&Value> = body["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| &row["name"])
        .collect();
    assert_eq!(names, [&json!("alpha.eth"), &json!("beta.eth")], "{body:#}");
    assert_eq!(body["page"]["total_count"], json!(2), "{body:#}");
    // alpha.eth is also alice's primary name.
    assert_eq!(body["data"][0]["is_primary"], json!(true), "{body:#}");
    // bob holds two.alpha.eth under ENSv2; an address with no names lists none.
    for (address, expected) in [(SWITCH_BOB, json!(["two.alpha.eth"])), ("0x0000000000000000000000000000000000000fff", json!([]))] {
        let (status, body) = read_family_response(&database, &format!("/v1/addresses/{address}/names?namespace=ens"))
            .await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        let names: Vec<_> = body["data"].as_array().unwrap().iter().map(|row| row["name"].clone()).collect();
        assert_eq!(json!(names), expected, "{body:#}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_resolves_to_from_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let base = format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&relation=resolves_to");
    for coin in ["60", "evm"] {
        for query in [
            "page_size=1",
            "page_size=1&order=desc",
            "page_size=1&sort=expires_at",
            "page_size=1&dedupe=registration",
            "page_size=1&authority=ens_v1",
            "page_size=1&include=counts",
        "page_size=1&include=role_summary",
        ] {
            let pages = read_family_pages(
                &database,
                &format!("{base}&coin_type={coin}&{query}"),
            )
            .await?;
            let names: Vec<&Value> = pages
                .iter()
                .flat_map(|page| page["data"].as_array().into_iter().flatten())
                .map(|row| &row["name"])
                .collect();
            assert_eq!(names, [&json!("alpha.eth")], "{coin} {query}: {pages:#?}");
        }
    }
    // A coin type nothing answers, and an address nothing resolves to.
    for uri in [format!("{base}&coin_type=0"), "/v1/addresses/0x0000000000000000000000000000000000000fff/names?namespace=ens&relation=resolves_to&coin_type=60".to_owned()] {
        let (status, body) = read_family_response(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        assert_eq!(body["data"], json!([]), "{body:#}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_address_inline_roles_from_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let uris: Vec<String> = ["", "&relation=resolves_to&coin_type=60"].into_iter()
        .map(|relation| format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&include=role_summary{relation}"))
        .collect();
    for uri in &uris {
        let (status, body) = read_family_response(&database, uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_records_from_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    for uri in [
        "/v1/names/alpha.eth/records",
        "/v1/names/alpha.eth/records?include=inventory",
        "/v1/names/alpha.eth/records?keys=addr:60",
        "/v1/names/beta.eth/records",
    ] {
        let (status, body) = read_family_response(&database, uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    }
    let (_, body) =
        read_family_response(&database, "/v1/names/alpha.eth/records?keys=addr:60").await?;
    assert!(
        body.to_string().contains(SWITCH_ALICE),
        "alpha.eth's addr:60 answers alice: {body:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_primary_names_from_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let (status, body) = read_family_response(
        &database,
        &format!("/v1/addresses/{SWITCH_ALICE}/primary-name?source=indexed"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert!(
        body.to_string().contains("alpha.eth"),
        "alice's claim names alpha.eth: {body:#}"
    );
    for uri in [
        format!("/v1/addresses/{SWITCH_BOB}/primary-name?source=indexed"),
        format!("/v1/addresses/{SWITCH_ALICE}/primary-name?source=indexed&coin_type=2147483658"),
    ] {
        let (status, body) = read_family_response(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    }

    database.cleanup().await
}
#[tokio::test]
async fn v2_family_record_reads_answer_409_while_the_families_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.pool)
        .await?;
    for uri in [
        "/v1/names/alpha.eth/records".to_owned(),
        format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens"),
        format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&relation=resolves_to&coin_type=60"),
        format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&relation=resolves_to&coin_type=evm"),
        format!("/v1/addresses/{SWITCH_ALICE}/primary-name?source=indexed"),
    ] {
        let response = v2_get_response(&database, &uri)
        .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(
            (status, &body["error"]["code"]),
            (StatusCode::CONFLICT, &json!("stale")),
            "{uri}: {body:#}"
        );
    }
    database.cleanup().await
}
#[tokio::test]
async fn v2_family_record_inventory_refuses_an_at_below_the_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let at = crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(1_700_000_239)?);
    let response = v2_get_response(&database, &format!("/v1/names/alpha.eth/records?at={at}"))
    .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
    assert_eq!(body["error"]["code"], json!("stale"), "{body:#}");
    database.cleanup().await
}
#[tokio::test]
async fn v2_name_detail_and_records_diagnostic_inventories_from_families()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let uris = [
        "/v1/names/alpha.eth",
        "/v1/names/alpha.eth?source=verified",
        "/v1/names/beta.eth",
        "/v1/diagnostics/names/alpha.eth/records",
        "/v1/diagnostics/names/alpha.eth/records?keys=addr:60",
        "/v1/diagnostics/names/beta.eth/records",
    ];
    for uri in uris {
        let (status, body) = read_family_response(&database, uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    }
    let (_, body) = read_family_response(
        &database,
        "/v1/diagnostics/names/alpha.eth/records?keys=addr:60",
    )
    .await?;
    assert!(
        body["data"]["record_inventory"].to_string().contains("addr"),
        "the diagnostic reads alpha.eth's inventory: {body:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_family_abi_inventory_uses_resolver_classification() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_routes_events(&database).await?;
    let manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM bigname_phase.manifest_versions WHERE file_path = 'fixture/switch-resolver.toml'",
    ).fetch_one(&database.pool).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", "alpha.eth");
    let mut event = switch_event("switch-alpha-abi", None, None, "RecordChanged", "ens_v1_resolver_l1", 212, 0,
        json!({"source_event": "ABIChanged", "node": name.strip_prefix("ens:").unwrap(),
            "resolver": SWITCH_RESOLVER, "record_key": "abi:4", "record_family": "abi",
            "selector_key": "4", "value_retained": true, "value": "4"}));
    event.raw_fact_ref["emitting_address"] = json!(SWITCH_RESOLVER);
    event.source_manifest_id = Some(manifest);
    event.manifest_version = 1;
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    publish_test_families(&database, 240).await?;
    let uri = "/v1/names/alpha.eth/records?include=inventory";
    let (status, body) = read_family_response(&database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"]["inventory"]["abi_content_types"], json!(["4"]), "{body:#}");

    database.cleanup().await
}
