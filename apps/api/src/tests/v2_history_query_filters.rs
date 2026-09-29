// TYR-71/72: public route filters select one collection before paging and counting.
#[tokio::test]
async fn history_query_filters_apply_before_pages_counts_and_child_union() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    hkw_seed(&database).await?;
    for route in hk_routes() {
        for order in ["asc", "desc"] {
            let query = format!("{route}&exclude_type=record,permission&order={order}");
            let (expected, total) = hkw_baseline(&database, &query).await?;
            assert!(!expected.is_empty());
            assert_eq!(total, json!(expected.len()), "{query}");
            assert_eq!(
                hkw_rest(&database, &query, None, Some(&total)).await?,
                expected
            );
            let empty = hk_ok(
                &database,
                &format!("{route}&type=record&exclude_type=record"),
            )
            .await?;
            assert_eq!(empty["data"], json!([]), "{route}");
            assert_eq!(empty["page"]["total_count"], json!(0), "{route}");
        }
    }
    database.cleanup().await
}

const G_NAME: &str = "history-key.eth";
const G_RESOLVER: &str = "0x00000000000000000000000000000000000000c2";
const G_OTHER_RESOLVER: &str = "0x00000000000000000000000000000000000000c3";
const G_ADDRESS: &str = "0x0000000000000000000000000000000000007130";

async fn g_seed_records(database: &TestDatabase) -> Result<()> {
    let logical = bigname_storage::logical_name_id_for_name("ens", G_NAME);
    let resource = Uuid::from_u128(0x7172);
    seed_identity_name(
        database,
        &logical,
        G_NAME,
        G_NAME,
        "node:history-key.eth",
        resource,
        Uuid::from_u128(0x8172),
        Uuid::from_u128(0x9172),
        G_ADDRESS,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    seed_v2_history_blocks(database, 130..=145).await?;
    let node = bigname_lookup::ens_namehash_hex(G_NAME)?;
    let mut pointer = v2_history_event(
        "g-pointer",
        Some(&logical),
        Some(resource),
        "ResolverChanged",
        130,
    );
    pointer.source_family = "ens_v1_registry_l1".to_owned();
    pointer.after_state = json!({"node":node, "resolver":G_RESOLVER});
    pointer.log_index = Some(1);
    let record = |identity: &str, block: i64, resolver: &str, after: Value| {
        let kind = if after["source_event"] == "VersionChanged" {
            "RecordVersionChanged"
        } else {
            "RecordChanged"
        };
        event_data_event(
            identity,
            None,
            None,
            kind,
            "ens_v1_resolver_l1",
            block,
            &format!("0xtx{block}"),
            0,
            resolver,
            after,
        )
    };
    let addr = |bytes: &str| {
        json!({"source_event":"AddressChanged", "resolver":G_RESOLVER,
        "node":node, "record_key":"addr:60", "record_family":"addr", "selector_key":"60",
        "coin_type":"60", "value_retained":false, "address_bytes_hex":bytes})
    };
    let text = |key: &str| {
        json!({"source_event":"TextChanged", "resolver":G_RESOLVER,
        "node":node, "record_key":format!("text:{key}"), "record_family":"text",
        "selector_key":key, "value_retained":true, "value":"value"})
    };
    let mut events = vec![
        v2_history_event(
            "g-grant",
            Some(&logical),
            Some(resource),
            "RegistrationGranted",
            130,
        ),
        pointer,
        record("g-set", 131, G_RESOLVER, addr(G_ADDRESS)),
        record("g-avatar", 132, G_RESOLVER, text("avatar")),
        record("g-clear", 133, G_RESOLVER, addr("0x")),
        record(
            "g-reset",
            134,
            G_RESOLVER,
            version_after(&node, G_RESOLVER, 1),
        ),
        record("g-set-again", 135, G_RESOLVER, addr(G_ADDRESS)),
        record(
            "g-other-node-reset",
            136,
            G_RESOLVER,
            version_after(
                &bigname_lookup::ens_namehash_hex("other-key.eth")?,
                G_RESOLVER,
                1,
            ),
        ),
        record(
            "g-other-resolver-reset",
            137,
            G_OTHER_RESOLVER,
            version_after(&node, G_OTHER_RESOLVER, 1),
        ),
        record("g-text-literal", 138, G_RESOLVER, text("Name, with space ")),
    ];
    let mut clear_pointer = v2_history_event(
        "g-pointer-clear",
        Some(&logical),
        Some(resource),
        "ResolverChanged",
        139,
    );
    clear_pointer.source_family = "ens_v1_registry_l1".to_owned();
    clear_pointer.after_state =
        json!({"node":node, "resolver":"0x0000000000000000000000000000000000000000"});
    events.push(clear_pointer);
    events.push(record(
        "g-ended-pointer-reset",
        140,
        G_RESOLVER,
        version_after(&node, G_RESOLVER, 2),
    ));
    publish_event_data(database, &events, 145).await
}

#[tokio::test]
async fn history_query_record_key_keeps_writes_clears_and_only_scoped_resets() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    let routes = [
        format!("/v1/names/{G_NAME}/history?scope=both"),
        format!("/v1/names/{G_NAME}/history?scope=both&include=child_registrations,data,raw"),
        format!("/v1/events?name={G_NAME}"),
        format!("/v1/events?registration_id={}", Uuid::from_u128(0x7172)),
        format!("/v1/addresses/{G_ADDRESS}/history?scope=both"),
    ];
    for route in routes {
        for order in ["asc", "desc"] {
            let base = format!(
                "{route}&record_key=addr:60&from_timestamp=2023-11-14T22:15:30Z&order={order}"
            );
            let (ids, total) = hkw_baseline(&database, &base).await?;
            let mut expected = ["g-set", "g-clear", "g-reset", "g-set-again"]
                .map(hkw_id)
                .to_vec();
            if order == "desc" {
                expected.reverse();
            }
            assert_eq!(ids, expected, "{base}");
            assert_eq!(total, json!(4), "{base}");
            assert_eq!(
                hkw_rest(&database, &base, None, Some(&total)).await?,
                expected
            );
        }
    }
    let base = format!(
        "/v1/names/{G_NAME}/history?record_key=addr:60&from_timestamp=2023-11-14T22:15:30Z&include=data,raw"
    );
    let payload = hk_ok(&database, &base).await?;
    let reset = event_data_rows(&payload)
        .into_iter()
        .find(|row| row["kind"] == "RecordVersionChanged")
        .context("reset row")?;
    assert_eq!(
        reset["data"]["node"],
        json!(bigname_lookup::ens_namehash_hex(G_NAME)?)
    );
    assert!(reset["data"].get("key").is_none());
    assert!(reset["data"].get("value").is_none());
    for (suffix, expected) in [
        ("&kind=RecordChanged", 3),
        ("&kind=RecordVersionChanged", 1),
        ("&exclude_type=record", 0),
        ("&type=registration", 0),
    ] {
        let page = hk_ok(&database, &format!("{base}{suffix}")).await?;
        assert_eq!(page["page"]["total_count"], json!(expected), "{suffix}");
        assert_eq!(page["data"].as_array().unwrap().len(), expected);
    }
    let literal = hk_ok(&database, &format!("/v1/names/{G_NAME}/history?record_key=text:Name%2C%20with%20space%20&kind=RecordChanged")).await?;
    assert_eq!(hk_ids(&literal), vec![hkw_id("g-text-literal")]);
    let wrong_case = hk_ok(
        &database,
        &format!("/v1/names/{G_NAME}/history?record_key=Addr:60&kind=RecordChanged"),
    )
    .await?;
    assert_eq!(wrong_case["data"], json!([]));
    database.cleanup().await
}

#[tokio::test]
async fn history_query_filter_cursors_bind_canonical_sets_and_exact_key() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    let base = format!(
        "/v1/names/{G_NAME}/history?type=record&exclude_type=permission,renewal&kind=RecordVersionChanged,RecordChanged&record_key=addr:60&from_timestamp=2023-11-14T22:15:30Z"
    );
    let first = hk_ok(&database, &format!("{base}&page_size=1")).await?;
    let cursor = hk_next_cursor(&first)?;
    let equivalent = base
        .replace("permission,renewal", "renewal,permission,permission")
        .replace(
            "RecordVersionChanged,RecordChanged",
            "RecordChanged,,RecordVersionChanged,RecordChanged",
        );
    hk_ok(
        &database,
        &format!("{equivalent}&include=data,raw,total_count&cursor={cursor}"),
    )
    .await?;
    for changed in [
        base.replace("permission,renewal", "permission"),
        base.replace("RecordVersionChanged,RecordChanged", "RecordChanged"),
        base.replace("addr:60", "addr:61"),
        format!("{base}&order=asc"),
    ] {
        let (status, payload) = hk_get(&database, &format!("{changed}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{changed}: {payload}");
    }
    // A modern position cursor keeps working when its original event disappears.
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = $1")
        .bind(hk_anchor(&cursor)?)
        .execute(&database.pool)
        .await?;
    assert_eq!(
        hk_ids(&hk_ok(&database, &format!("{base}&cursor={cursor}")).await?).len(),
        3
    );
    database.cleanup().await
}

#[tokio::test]
async fn history_query_filters_reject_invalid_values_and_repeated_parameters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    for query in [
        "exclude_type=bogus",
        "kind=TextChanged",
        "kind=SourceManifestUpdated",
        "kind=recordchanged",
        "record_key=",
        "exclude_type=,,",
        "kind=,,",
        "kind=RecordChanged&kind=RecordVersionChanged",
        "record_key=addr:60&record_key=addr:61",
        "exclude_type=record&exclude_type=permission",
    ] {
        let (status, payload) =
            hk_get(&database, &format!("/v1/names/{G_NAME}/history?{query}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {payload}");
    }
    let (_, baseline) =
        hkw_baseline(&database, &format!("/v1/names/{G_NAME}/history?scope=both")).await?;
    let blank = hk_ok(
        &database,
        &format!("/v1/names/{G_NAME}/history?type=&exclude_type=%20&kind="),
    )
    .await?;
    assert_eq!(blank["page"]["total_count"], baseline);
    let (status, _) = hk_get(&database, "/v1/diagnostics/events?record_key=addr:60").await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    database.cleanup().await
}

#[tokio::test]
async fn history_query_record_key_rejects_nul_before_storage() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    g_seed_records(&database).await?;
    let mut responses = Vec::new();
    for route in [
        format!("/v1/names/{G_NAME}/history?scope=both"),
        format!("/v1/addresses/{G_ADDRESS}/history?scope=both"),
        format!("/v1/events?contract_address={G_RESOLVER}"),
    ] {
        for key in ["%00", "addr:60%00"] {
            let uri = format!("{route}&record_key={key}");
            let (status, payload) = hk_get(&database, &uri).await?;
            responses.push((uri, status, payload));
        }
    }
    assert!(
        responses
            .iter()
            .all(|(_, status, payload)| *status == StatusCode::BAD_REQUEST
                && payload["error"]["code"] == "invalid_input"),
        "invalid record keys must be client errors on every history route: {responses:?}"
    );
    database.cleanup().await
}
