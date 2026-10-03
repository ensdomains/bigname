// Exercise the public timestamp contract through normalized registry events and the real family
// reducers. Grant and renewal payloads follow protocol/v2_registry/transfer.rs and v2_registry.rs.
// Each name has its own binding, resource and token lineage; no served summary is seeded.

const UNIX_EXPIRY_REGISTRY: &str = "0x0000000000000000000000000000000000000667";
const UNIX_EXPIRY_HOLDER: &str = "0x0000000000000000000000000000000000000067";
const UNIX_EXPIRY_CASES: [(&str, u64, u64); 6] = [
    ("ordinary.numbers.eth", 1_900_000_000, 1_950_000_000),
    ("calendar.numbers.eth", 253_402_300_799, 253_402_300_800),
    (
        "safe.numbers.eth",
        9_007_199_254_740_992,
        9_007_199_254_740_993,
    ),
    (
        "signed.numbers.eth",
        9_223_372_036_854_775_807,
        9_223_372_036_854_775_808,
    ),
    ("nearmax.numbers.eth", u64::MAX - 2, u64::MAX - 1),
    // Only the admitted root eth/reverse entries classify this maximum as a sentinel. An
    // ordinary user-registry registration keeps it as an exact finite second.
    ("maximum.numbers.eth", u64::MAX, u64::MAX),
];

fn unix_expiry_event(
    logical: &str,
    resource: Uuid,
    kind: &str,
    block: i64,
    log: i64,
    manifest: i64,
    after: Value,
) -> NormalizedEvent {
    let mut event = family_event(
        &format!("unix-expiry-{logical}-{kind}-{block}"),
        Some(logical),
        Some(resource),
        kind,
        "ens_v2_registry_l1",
        block,
        log,
        after,
    );
    event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    event.source_manifest_id = Some(manifest);
    event.manifest_version = 1;
    event.raw_fact_ref["emitting_address"] = json!(UNIX_EXPIRY_REGISTRY);
    event
}

async fn unix_expiry_response(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = read_family_response(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

fn assert_unix_expiry_record(record: &Value, name: &str, expiry: u64) {
    assert_eq!(record["name"], json!(name), "{record:#}");
    assert_eq!(
        record["expires_at"],
        json!(expiry.to_string()),
        "{record:#}"
    );
    // These are user-registry subnames with no registrar grace.
    assert_eq!(
        record["grace_ends_at"],
        json!(expiry.to_string()),
        "{record:#}"
    );
    assert!(record.get("expires_at_reason").is_none(), "{record:#}");
    assert_eq!(record["registered_at"], json!("1700000201"), "{record:#}");
    assert_eq!(record["created_at"], json!("1700000201"), "{record:#}");
    assert_eq!(
        record["registration_status"],
        json!("registered"),
        "{record:#}"
    );
}

#[tokio::test]
async fn v2_unix_timestamps_preserve_large_user_registry_grants_and_renewals() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 201).await?;
    let (manifest, registry) = declare_family_fixture_contract(
        &database.pool,
        "ens",
        FAMILY_CHAIN,
        "ens_v2_registry_l1",
        "registry",
        UNIX_EXPIRY_REGISTRY,
    )
    .await?;
    let mut registrations = Vec::new();
    let mut grants = Vec::new();
    for (index, (name, expiry, _)) in UNIX_EXPIRY_CASES.iter().enumerate() {
        let seed = 0x6670_0000 + index as u128 * 16;
        let resource = Uuid::from_u128(seed);
        let token_lineage = Uuid::from_u128(seed + 1);
        let logical = seed_family_identity_inputs(
            &database.pool,
            "ens",
            name,
            FAMILY_CHAIN,
            201,
            "0xhistory201",
            resource,
            token_lineage,
            Uuid::from_u128(seed + 2),
            "ens_v2",
        )
        .await?;
        let label = name.split('.').next().expect("label");
        let labelhash = format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()));
        // Version-zero token/resource word: LibLabel replaces the lower 32 bits.
        // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/utils/LibLabel.sol:L11-L16 @ ens_v2_sepolia_20260916@366de741)
        let token = format!("{}00000000", &labelhash[..labelhash.len() - 8]);
        let grant = json!({
            "source_event": "LabelRegistered", "status": "registered",
            "authority_kind": "ens_v2_registry",
            "authority_key": format!("ens-v2-registry:{FAMILY_CHAIN}:{registry}:{token}"),
            "registry_contract_instance_id": registry.to_string(),
            "token_id": token, "current_token_id": token, "upstream_resource": token,
            "token_lineage_id": token_lineage.to_string(), "resource_pending": false,
            "label": label, "labelhash": labelhash, "namehash": logical.trim_start_matches("ens:"),
            "registrant": UNIX_EXPIRY_HOLDER, "sender": UNIX_EXPIRY_HOLDER, "expiry": expiry,
        });
        grants.push(unix_expiry_event(
            &logical,
            resource,
            "RegistrationGranted",
            201,
            index as i64,
            manifest,
            grant,
        ));
        grants.push(unix_expiry_event(
            &logical,
            resource,
            "ExpiryChanged",
            201,
            index as i64,
            manifest,
            json!({"source_event": "LabelRegistered", "token_id": token,
                   "current_token_id": token, "upstream_resource": token, "expiry": expiry}),
        ));
        grants.push(unix_expiry_event(
            &logical,
            resource,
            "AuthorityTransferred",
            201,
            index as i64,
            manifest,
            json!({"source_event": "LabelRegistered", "token_id": token,
                   "current_token_id": token, "upstream_resource": token,
                   "owner": UNIX_EXPIRY_HOLDER}),
        ));
        registrations.push((logical, resource, token, labelhash));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &grants).await?;
    publish_test_families(&database, 201).await?;
    for (name, granted, _) in UNIX_EXPIRY_CASES {
        let body = unix_expiry_response(&database, &format!("/v1/names/{name}")).await?;
        assert_unix_expiry_record(&body["data"], name, granted);
        assert_eq!(body["data"]["authority"], json!("ens_v2"), "{body:#}");
        assert_eq!(
            body["meta"]["as_of"]["1"]["timestamp"],
            json!("1700000201"),
            "{body:#}"
        );
    }

    // ExpiryUpdated emits both normalized rows on the same registration. Renew beyond the
    // previous calendar/safe-integer/signed-integer bounds, leaving the maximum case unchanged.
    // register/renew use uint64 and renewal must increase expiry.
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L206-L219 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L240-L255 @ ens_v2_sepolia_20260916@366de741)
    let mut renewals = Vec::new();
    for (index, ((_, granted, renewed), (logical, resource, token, labelhash))) in
        UNIX_EXPIRY_CASES.iter().zip(&registrations).enumerate()
    {
        if granted == renewed {
            continue;
        }
        for kind in ["ExpiryChanged", "RegistrationRenewed"] {
            let mut event = unix_expiry_event(
                logical,
                *resource,
                kind,
                210,
                index as i64,
                manifest,
                json!({"source_event": "ExpiryUpdated", "token_id": token,
                       "registry_contract_instance_id": registry.to_string(),
                       "labelhash": labelhash, "expiry": renewed,
                       "sender": UNIX_EXPIRY_HOLDER, "revived_from_expiry": false}),
            );
            event.before_state = json!({"expiry": granted});
            renewals.push(event);
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &renewals).await?;
    publish_test_families(&database, 210).await?;
    for (name, granted, renewed) in UNIX_EXPIRY_CASES {
        for at in ["1700000210", "2023-11-14T23:16:50%2B01:00"] {
            let body =
                unix_expiry_response(&database, &format!("/v1/names/{name}?at={at}")).await?;
            assert_unix_expiry_record(&body["data"], name, renewed);
            assert_eq!(
                body["meta"]["as_of"]["1"]["timestamp"],
                json!("1700000210"),
                "{body:#}"
            );
        }
        let upper = u128::from(renewed) + 1;
        let uri = format!("/v1/names?namespace=ens&expires_after={renewed}&expires_before={upper}");
        let window = unix_expiry_response(&database, &uri).await?;
        let rows = window["data"].as_array().expect("window rows");
        assert_eq!(rows.len(), 1, "{uri}: {window:#}");
        assert_unix_expiry_record(&rows[0], name, renewed);
        if granted != renewed {
            let upper = u128::from(granted) + 1;
            let old_window = unix_expiry_response(
                &database,
                &format!("/v1/names?namespace=ens&expires_after={granted}&expires_before={upper}"),
            )
            .await?;
            assert_eq!(
                old_window["data"],
                json!([]),
                "renewal must remove the old expiry: {old_window:#}"
            );
        }
    }

    assert_history_submicrosecond_bounds(&database).await?;
    assert_history_database_minimum_bounds(&database).await?;

    // The same finite word survives each shared public name-record builder and event detail.
    let name = "maximum.numbers.eth";
    let search =
        unix_expiry_response(&database, &format!("/v1/search?q={name}&namespace=ens")).await?;
    assert_unix_expiry_record(&search["data"][0], name, u64::MAX);
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": name}]}),
    )
    .await?;
    assert_unix_expiry_record(&lookup["data"][0]["record"], name, u64::MAX);
    let address = unix_expiry_response(&database,
        &format!("/v1/addresses/{UNIX_EXPIRY_HOLDER}/names?namespace=ens&relation=owner&sort=expires_at&order=desc&page_size=1")).await?;
    assert_unix_expiry_record(&address["data"][0], name, u64::MAX);
    let history =
        unix_expiry_response(&database, &format!("/v1/names/{name}/history?include=data")).await?;
    let registration = history["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["data"].get("expires_at").is_some())
        .expect("registration history");
    assert_eq!(
        registration["data"]["expires_at"],
        json!(u64::MAX.to_string()),
        "{history:#}"
    );
    assert_eq!(
        registration["timestamp"],
        json!("1700000201"),
        "{history:#}"
    );

    // These keys deliberately span different decimal widths. Each continuation changes the
    // lower bound's spelling while preserving its instant, proving equivalent cursor filters.
    for (order, expected) in [
        ("asc", UNIX_EXPIRY_CASES.to_vec()),
        ("desc", UNIX_EXPIRY_CASES.into_iter().rev().collect()),
    ] {
        let mut cursor: Option<String> = None;
        for (index, (name, _, expiry)) in expected.iter().enumerate() {
            let after = if index % 2 == 0 {
                "1577836800"
            } else {
                "2020-01-01T01:00:00%2B01:00"
            };
            let mut uri =
                format!("/v1/names?namespace=ens&expires_after={after}&order={order}&page_size=1");
            if let Some(cursor) = &cursor {
                uri.push_str(&format!("&cursor={cursor}"));
            }
            let page = unix_expiry_response(&database, &uri).await?;
            let rows = page["data"].as_array().expect("page rows");
            assert_eq!(rows.len(), 1, "{uri}: {page:#}");
            assert_unix_expiry_record(&rows[0], name, *expiry);
            assert_eq!(
                page["meta"]["as_of"]["1"]["timestamp"],
                json!("1700000210"),
                "{page:#}"
            );
            let more = index + 1 < expected.len();
            assert_eq!(page["page"]["has_more"], json!(more), "{page:#}");
            cursor = page["page"]["next_cursor"].as_str().map(str::to_owned);
            assert_eq!(cursor.is_some(), more, "{page:#}");
        }
    }
    database.cleanup().await
}

// Whole-second chain events must not leak into a window one nanosecond after them.
async fn assert_history_submicrosecond_bounds(database: &TestDatabase) -> Result<()> {
    let route = "/v1/names/ordinary.numbers.eth/history";
    let bounds = ["1700000201.000000001", "2023-11-14T22:16:41.000000001Z"];
    let mut point_windows = Vec::new();
    for bound in bounds {
        let body = unix_expiry_response(
            database,
            &format!("{route}?from_timestamp={bound}&to_timestamp={bound}&include=data"),
        )
        .await?;
        point_windows.push(body["data"].clone());
    }
    assert_eq!(
        point_windows,
        [json!([]), json!([])],
        "neither spelling may include the earlier registration"
    );
    for bound in bounds {
        let body = unix_expiry_response(
            database,
            &format!("{route}?from_timestamp={bound}&include=data"),
        )
        .await?;
        let events = body["data"].as_array().expect("history events");
        assert!(
            !events.is_empty(),
            "later renewal remains eligible: {body:#}"
        );
        assert!(
            events
                .iter()
                .all(|event| event["timestamp"] == "1700000210"),
            "{body:#}"
        );
    }
    let precise = bounds[0]
        .parse::<bigname_storage::UnixSeconds>()
        .expect("precise bound")
        .to_datetime()
        .expect("clock bound");
    let point = bigname_storage::resolve_chain_block_ranges(
        &database.pool,
        &[FAMILY_CHAIN],
        Some(precise),
        Some(precise),
    )
    .await?;
    assert!(
        point.is_empty(),
        "no lineage block exists at this instant: {point:?}"
    );
    let lower = bigname_storage::resolve_chain_block_ranges(
        &database.pool,
        &[FAMILY_CHAIN],
        Some(precise),
        None,
    )
    .await?;
    assert_eq!(lower.len(), 1);
    assert_eq!(lower[0].from_block, Some(202));
    Ok(())
}

// Accepted ancient input bounds still select the ordinary modern chain correctly.
async fn assert_history_database_minimum_bounds(database: &TestDatabase) -> Result<()> {
    let route = "/v1/names/ordinary.numbers.eth/history";
    let expected = unix_expiry_response(database, &format!("{route}?include=data")).await?;
    let events = expected["data"].as_array().expect("ordinary history");
    for timestamp in ["1700000201", "1700000210"] {
        assert!(events.iter().any(|event| event["timestamp"] == timestamp));
    }
    let epoch = OffsetDateTime::from_unix_timestamp(0)?;
    let expected_ranges = bigname_storage::resolve_chain_block_ranges(
        &database.pool,
        &[FAMILY_CHAIN],
        Some(epoch),
        None,
    )
    .await?;
    assert_eq!(expected_ranges.len(), 1);
    // Below PostgreSQL's finite minimum, exactly on it, and one nanosecond above it.
    for bound in [
        "-210866803200.000000001",
        "-300000000000",
        "-210866803200",
        "-210866803199.999999999",
    ] {
        let at = bound
            .parse::<bigname_storage::UnixSeconds>()?
            .to_datetime()
            .unwrap();
        let upper = unix_expiry_response(
            database,
            &format!("{route}?to_timestamp={bound}&include=data"),
        )
        .await?;
        assert_eq!(upper["data"], json!([]), "upper {bound}");
        assert!(
            bigname_storage::resolve_chain_block_ranges(
                &database.pool,
                &[FAMILY_CHAIN],
                None,
                Some(at),
            )
            .await?
            .is_empty(),
            "upper {bound}"
        );
        let lower = unix_expiry_response(
            database,
            &format!("{route}?from_timestamp={bound}&include=data"),
        )
        .await?;
        assert_eq!(lower["data"], expected["data"], "lower {bound}");
        let ranges = bigname_storage::resolve_chain_block_ranges(
            &database.pool,
            &[FAMILY_CHAIN],
            Some(at),
            None,
        )
        .await?;
        assert_eq!(ranges, expected_ranges, "lower {bound}");
    }
    Ok(())
}

#[tokio::test]
async fn v2_unix_timestamps_wrapper_keeps_finite_values_and_classifies_sentinels() -> Result<()> {
    for (expiry, reason) in [
        (9_007_199_254_740_993_u64, None),
        (u64::MAX - 1, None),
        (u64::MAX, Some("no_expiry")),
        (0, Some("not_set")),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_permissions_fixture(&database).await?;
        let wrapper = Uuid::from_u128(0x6670);
        let name = "child.perms.eth";
        seed_wrapped_subname_inputs(&database, name, wrapper).await?;
        let event = permission_fixture_event(
            &format!("exact-wrapper-{expiry}"),
            None,
            Some(wrapper),
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            124,
            0,
            json!({"source_event": "ExpiryExtended", "expiry": expiry}),
        );
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
        rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await?;
        let body = unix_expiry_response(&database, &format!("/v1/names/{name}")).await?;
        let record = &body["data"];
        let expected = reason.map_or_else(|| json!(expiry.to_string()), |_| Value::Null);
        assert_eq!(record["expires_at"], expected, "{body:#}");
        assert_eq!(record["expires_at_reason"], json!(reason), "{body:#}");
        // The wrapper expiry is the subname's only expiry; it has no lease date.
        assert_eq!(record["ens_v1"].get("expires_at"), Some(&Value::Null), "{body:#}");
        let feed = v2_lookup_json(
            &database,
            json!({"profile": "feed", "inputs": [{"name": name}]}),
        )
        .await?;
        assert_feed_expiry_fields(&feed["data"][0]["record"], record, "wrapper lookup feed");
        let permissions = v2_permissions_payload_for_database(
            &database,
            &format!("/v1/permissions?registration_id={wrapper}"),
        )
        .await?;
        assert_eq!(
            permissions["restrictions"]["wrapper_expires_at"], expected,
            "{permissions:#}"
        );
        assert_eq!(
            permissions["restrictions"]["wrapper_expires_at_reason"],
            json!(reason),
            "{permissions:#}"
        );
        if reason.is_some() {
            assert!(
                record.get("expires_at").is_some(),
                "classified expiry must be explicit null"
            );
            assert_eq!(record["grace_ends_at"], Value::Null);
        }
        database.cleanup().await?;
    }
    Ok(())
}
