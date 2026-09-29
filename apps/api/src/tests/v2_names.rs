// `GET /v1/names`: the namespace-wide expiry window listing.
//
// Retained registrar grants produce expiry values; an unbound surface has no expiry, and a
// Basenames grant verifies namespace filtering. Dates and owners are read from actual family data.

const V2_NAMES_BETA_EXPIRY: i64 = 1_790_812_800; // 2026-10-01T00:00:00Z
const V2_NAMES_GAMMA_EXPIRY: i64 = 1_764_547_200; // 2025-12-01T00:00:00Z
const V2_NAMES_ALPHA_EXPIRY: i64 = 1_798_761_600; // 2027-01-01T00:00:00Z
const V2_NAMES_BASE_EXPIRY: i64 = 1_793_491_200; // 2026-11-01T00:00:00Z

async fn publish_v2_names_fixture(database: &TestDatabase) -> Result<()> {
    database.seed_snapshot_selector_chain_positions(&json!({
        "ethereum":{"chain_id":"ethereum-mainnet","block_number":100,"block_hash":"0xcollection-head","timestamp":"2026-06-10T00:00:00Z"},
        "base":{"chain_id":"base-mainnet","block_number":100,"block_hash":"0xbase-collection-head","timestamp":"2026-06-10T00:00:00Z"}
    })).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 100, "0xcollection-head").await?;
    rebuild_fixture_families(
        &database.pool,
        "base-mainnet",
        100,
        "0xbase-collection-head",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_names_registration(
    database: &TestDatabase,
    namespace: &str,
    name: &str,
    block: i64,
    registered_at: &str,
    expiry: u64,
    owner: &str,
    registrant: &str,
) -> Result<Uuid> {
    let chain = chain_id_for_namespace(namespace);
    let hash = format!("0xnames{block}");
    upsert_phase_raw_blocks(
        &database.pool,
        &[
            raw_block(
                chain,
                "0xnames-created",
                None,
                80,
                parse_rfc3339_utc_timestamp("2023-01-02T00:00:00Z")?.unix_timestamp(),
            ),
            raw_block(
                chain,
                &hash,
                None,
                block,
                parse_rfc3339_utc_timestamp(registered_at)?.unix_timestamp(),
            ),
        ],
    )
    .await?;
    let resource = Uuid::from_u128(0x85000 + block as u128 * 10);
    let token = Uuid::from_u128(resource.as_u128() + 1);
    let binding = Uuid::from_u128(resource.as_u128() + 2);
    let logical = seed_family_identity_inputs(
        &database.pool,
        namespace,
        name,
        chain,
        80,
        "0xnames-created",
        resource,
        token,
        binding,
        if namespace == "ens" {
            "ens_v1"
        } else {
            "basenames"
        },
    )
    .await?;
    let (registrar, registry) = if namespace == "ens" {
        ("ens_v1_registrar_l1", "ens_v1_registry_l1")
    } else {
        ("basenames_base_registrar", "basenames_base_registry")
    };
    let mut events = Vec::new();
    for (log, kind, family, after) in [
        (
            0,
            "RegistrationGranted",
            registrar,
            json!({"authority_kind":"registrar","registrant":registrant,"expiry":expiry}),
        ),
        (
            1,
            "AuthorityTransferred",
            registry,
            json!({"source_event":"Transfer","node":bigname_lookup::ens_namehash_hex(name)?,"owner":owner}),
        ),
    ] {
        let mut event = history_event(
            &format!("names-{name}-{kind}"),
            Some(&logical),
            Some(resource),
            Some(chain),
            Some(block),
            Some(&hash),
            Some("0xnames"),
            Some(log),
            CanonicalityState::Canonical,
        );
        event.namespace = namespace.into();
        event.event_kind = kind.into();
        event.source_family = family.into();
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(resource)
}

async fn seed_v2_names_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_names_with_alpha_expiry(database, V2_NAMES_ALPHA_EXPIRY as u64).await
}

async fn seed_v2_names_with_alpha_expiry(database: &TestDatabase, alpha_expiry: u64) -> Result<()> {
    for (namespace, name, block, registered, expiry, owner, registrant) in [
        (
            "ens",
            "alpha.eth",
            91,
            "2024-01-02T00:00:00Z",
            alpha_expiry,
            "0x00000000000000000000000000000000000000a1",
            "0x00000000000000000000000000000000000000a2",
        ),
        (
            "ens",
            "beta.eth",
            92,
            "2024-02-02T00:00:00Z",
            V2_NAMES_BETA_EXPIRY as u64,
            "0x00000000000000000000000000000000000000b1",
            "0x00000000000000000000000000000000000000b2",
        ),
        (
            "ens",
            "gamma.eth",
            93,
            "2024-03-02T00:00:00Z",
            V2_NAMES_GAMMA_EXPIRY as u64,
            "0x00000000000000000000000000000000000000c1",
            "0x00000000000000000000000000000000000000c2",
        ),
        (
            "basenames",
            "alpha.base.eth",
            96,
            "2024-03-02T00:00:00Z",
            V2_NAMES_BASE_EXPIRY as u64,
            "0x00000000000000000000000000000000000000e1",
            "0x00000000000000000000000000000000000000e2",
        ),
    ] {
        seed_names_registration(
            database, namespace, name, block, registered, expiry, owner, registrant,
        )
        .await?;
    }
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        "epsilon.eth",
        "ethereum-mainnet",
        80,
        "0xnames-created",
        Uuid::from_u128(0x86000),
        Uuid::from_u128(0x86001),
        Uuid::from_u128(0x86002),
        "ens_v1",
    )
    .await?;
    sqlx::query("DELETE FROM surface_bindings WHERE logical_name_id=$1")
        .bind(logical)
        .execute(&database.pool)
        .await?;
    publish_v2_names_fixture(database).await
}

async fn v2_names_response(database: &TestDatabase, uri: &str) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 names request failed")
}

async fn v2_names_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_names_response(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    read_json(response).await
}

fn v2_names_listed(payload: &Value) -> Vec<String> {
    payload["data"]
        .as_array()
        .expect("names data must be an array")
        .iter()
        .map(|row| {
            row["name"]
                .as_str()
                .expect("name must be a string")
                .to_owned()
        })
        .collect()
}

// The ENSv2 root registry registers `eth` and `reverse` with the largest uint64 expiry, which no
// timestamp can hold. The listing treats that expiry as unknown and leaves the name out instead
// of failing.
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/script/deploy-constants.ts:L1 @ ens_v2_sepolia_20260916@366de741)
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ETHRegistry.ts:L36-L48 @ ens_v2_sepolia_20260916@366de741)
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ReverseMirror.ts:L25-L37 @ ens_v2_sepolia_20260916@366de741)
#[tokio::test]
async fn v2_get_names_skips_an_expiry_beyond_the_timestamp_range() -> Result<()> {
    for (expiry, listed, alpha_expires_at) in [
        (u64::MAX, vec!["gamma.eth", "beta.eth"], None),
        (253_402_300_800_u64, vec!["gamma.eth", "beta.eth"], None),
        (
            253_402_300_799_u64,
            vec!["gamma.eth", "beta.eth", "alpha.eth"],
            Some("9999-12-31T23:59:59Z"),
        ),
        (
            0_u64,
            vec!["alpha.eth", "gamma.eth", "beta.eth"],
            Some("1970-01-01T00:00:00Z"),
        ),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_v2_names_with_alpha_expiry(&database, expiry).await?;
        let payload = v2_names_payload(
            &database,
            "/v1/names?namespace=ens&expires_after=1969-01-01T00:00:00Z&sort=expires_at&order=asc",
        )
        .await?;
        assert_eq!(v2_names_listed(&payload), listed, "{expiry}");
        let alpha = payload["data"]
            .as_array()
            .expect("names data")
            .iter()
            .find(|row| row["name"] == "alpha.eth");
        assert_eq!(
            alpha.and_then(|row| row.get("expires_at")),
            alpha_expires_at.map(Value::from).as_ref(),
            "{expiry}"
        );
        if expiry == 253_402_300_799 {
            assert!(alpha.expect("representable expiry remains listed").get("grace_ends_at").is_none(),
                "the grace deadline lies outside RFC3339, while expiry is representable: {payload}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_names_lists_a_namespace_expiry_window_in_expiry_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&expires_before=2027-06-01T00:00:00Z&sort=expires_at&order=asc",
    )
    .await?;
    assert_eq!(
        v2_names_listed(&payload),
        vec!["gamma.eth", "beta.eth", "alpha.eth"],
        "registration expiries in the window, ascending; missing expiries and other namespaces are absent"
    );
    assert_eq!(
        payload["data"][0]["expires_at"],
        json!("2025-12-01T00:00:00Z")
    );
    assert_eq!(payload["data"][0]["namespace"], json!("ens"));
    assert_eq!(payload["data"][0]["display_name"], json!("gamma.eth"));
    assert_eq!(
        payload["data"][0]["registrant"],
        json!("0x00000000000000000000000000000000000000c2")
    );
    assert_eq!(
        payload["data"][0]["owner"],
        json!("0x00000000000000000000000000000000000000c1")
    );
    assert_eq!(
        payload["data"][0]["registered_at"],
        json!("2024-03-02T00:00:00+00:00")
    );
    assert!(payload["data"][0].get("relations").is_none());
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert!(payload["meta"].get("as_of_token").is_none());

    // `sort` and `order` default to expires_at ascending.
    let defaulted = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&defaulted), v2_names_listed(&payload));

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_before=2027-06-01T00:00:00Z&order=desc",
    )
    .await?;
    assert_eq!(
        v2_names_listed(&payload),
        vec!["alpha.eth", "beta.eth", "gamma.eth"]
    );

    // `expires_after` is inclusive and `expires_before` exclusive, so consecutive windows tile.
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-12-01T00:00:00Z&expires_before=2026-10-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["gamma.eth"]);
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2026-10-01T00:00:00Z&expires_before=2027-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["beta.eth"]);

    // Fractional bounds still include whole-second expiries before the exclusive upper bound.
    for before in ["2026-10-01T00:00:00.5Z", "2026-10-01T00:00:00.000001Z"] {
        let payload = v2_names_payload(
            &database,
            &format!(
                "/v1/names?namespace=ens&expires_after=2026-10-01T00:00:00Z&expires_before={before}"
            ),
        )
        .await?;
        assert_eq!(v2_names_listed(&payload), vec!["beta.eth"], "{before}");
    }
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2026-10-01T00:00:00.5Z&expires_before=2027-01-01T00:00:00.5Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["alpha.eth"]);

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=basenames&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["alpha.base.eth"]);

    // At large Unix timestamps, f64 rounds a fractional bound back onto the whole-second
    // expiry. The index prefilter must still leave the exact timestamp comparison to decide.
    let logical = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let resource = Uuid::from_u128(0x85000 + 92 * 10);
    let mut renewal = history_event(
        "names-beta-renewed",
        Some(&logical),
        Some(resource),
        Some("ethereum-mainnet"),
        Some(100),
        Some("0xcollection-head"),
        Some("0xrenew"),
        Some(0),
        CanonicalityState::Canonical,
    );
    renewal.event_kind = "RegistrationRenewed".into();
    renewal.source_family = "ens_v1_registrar_l1".into();
    renewal.before_state = json!({});
    renewal.after_state = json!({"expiry":253402214400_u64});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[renewal]).await?;
    publish_v2_names_fixture(&database).await?;
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=9999-12-31T00:00:00Z&expires_before=9999-12-31T00:00:00.00001Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["beta.eth"]);

    database.cleanup().await
}

// A released `.eth` name stays in the listing: the listing means "registrations whose expiry falls
// in this window", whether the registration is live, in grace or released. The released row
// carries its status and its old expiry and no current registrant or owner.
#[tokio::test]
async fn v2_get_names_lists_a_released_name_inside_the_window_next_to_a_live_one() -> Result<()> {
    const HOLDER: &str = "0x0000000000000000000000000000000000000abc";
    let database = TestDatabase::new_migrated().await?;
    let released = seed_names_registration(
        &database,
        "ens",
        "lapsed-listed.eth",
        91,
        "2023-02-02T00:00:00Z",
        1700000000,
        HOLDER,
        HOLDER,
    )
    .await?;
    seed_names_registration(
        &database,
        "ens",
        "live-listed.eth",
        92,
        "2024-02-02T00:00:00Z",
        1900000000,
        HOLDER,
        HOLDER,
    )
    .await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "lapsed-listed.eth");
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xnames-released",
            None,
            93,
            1707776000,
        )],
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to=to_timestamp(1707776000) WHERE resource_id=$1",
    )
    .bind(released)
    .execute(&database.pool)
    .await?;
    let mut release = history_event(
        "names-lapsed-release",
        Some(&logical),
        Some(released),
        Some("ethereum-mainnet"),
        Some(93),
        Some("0xnames-released"),
        Some("0xrelease"),
        Some(0),
        CanonicalityState::Canonical,
    );
    release.event_kind = "RegistrationReleased".into();
    release.source_family = "ens_v1_registrar_l1".into();
    release.before_state = json!({"registrant":HOLDER,"authority_kind":"registrar","authority_key":"registrar:lapsed"});
    release.after_state = json!({"expiry":1700000000,"released_at":1707776000});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[release]).await?;
    publish_v2_names_fixture(&database).await?;

    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2023-01-01T00:00:00Z&expires_before=2031-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(
        payload["data"],
        json!([
            {
                "name": "lapsed-listed.eth",
                "display_name": "lapsed-listed.eth",
                "namespace": "ens",
                "namehash": "0x648f55586c02f13100041eeef8fa1550dc7dc35f0e5871dc7bea445cfbccce32",
                "registration_status": "released",
                "registered_at": "2023-02-02T00:00:00+00:00",
                "created_at": "2023-02-02T00:00:00+00:00",
                "expires_at": "2023-11-14T22:13:20Z",
                "grace_ends_at": "2024-02-12T22:13:20Z"
            },
            {
                "name": "live-listed.eth",
                "display_name": "live-listed.eth",
                "namespace": "ens",
                "namehash": "0xf5d57b8ccea9df92d48c79fec7260d742812b8f135ef7bf64d5b7b87ac11ea29",
                "owner": HOLDER,
                "registrant": HOLDER,
                "registration_status": "active",
                "registered_at": "2024-02-02T00:00:00+00:00",
                "created_at": "2024-02-02T00:00:00+00:00",
                "expires_at": "2030-03-17T17:46:40Z",
                "grace_ends_at": "2030-06-15T17:46:40Z"
            }
        ]),
        "the released name keeps its place at its old expiry, with no registrant or owner"
    );

    // A window that covers only the old expiry still serves the released name.
    let payload = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2023-11-14T22:13:20Z&expires_before=2023-11-14T22:13:21Z",
    )
    .await?;
    assert_eq!(v2_names_listed(&payload), vec!["lapsed-listed.eth"]);

    // The lapsed holder is on the name's own record, not on the listing row.
    let detail =
        v2_name_record_payload_for_database(&database, "/v1/names/lapsed-listed.eth").await?;
    assert_eq!(
        detail["data"]["lapsed_registration"],
        json!({
            "registrant": HOLDER,
            "held_through": "registrar",
            "released_at": "2024-02-12T22:13:20Z",
        })
    );

    database.cleanup().await
}

// A names cursor holds the window, order and last row's position only. A continuation
// reads the publication current when it runs, so a new Project publication between pages does
// not refuse it; a cursor that still carries the publication binding answers 400.
#[tokio::test]
async fn v2_names_cursor_holds_no_publication_and_continues_across_one() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;
    publish_v2_names_fixture(&database).await?;
    let base = "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1";
    let first = v2_names_payload(&database, base).await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .context("first page has continuation")?;
    let payload = crate::v2::decode(cursor).expect("issued cursor decodes");
    assert_eq!(payload.snapshot, None);
    assert_eq!(payload.evaluated_at, None);
    assert_eq!(payload.last_item.len(), 4);
    let second = v2_names_payload(&database, &format!("{base}&cursor={cursor}")).await?;

    // Same block and hash, but a new Project publication transaction.
    publish_v2_names_fixture(&database).await?;
    let again = v2_names_payload(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(again["data"], second["data"]);
    assert_eq!(again["page"], second["page"]);

    let mut bound = payload;
    bound.snapshot = Some(format!("publication-0x{}", "ab".repeat(32)));
    bound.evaluated_at = Some("2026-06-10T00:00:00Z".to_owned());
    let response = v2_names_response(
        &database,
        &format!("{base}&cursor={}", crate::v2::encode(&bound)),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let refused: Value = read_json(response).await?;
    assert_eq!(refused["error"]["code"], "invalid_input");
    assert_eq!(
        refused["error"]["message"],
        "cursor must be a valid pagination cursor"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_revalidates_publication_after_reads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;
    publish_v2_names_fixture(&database).await?;
    let state = database.app_state();
    let snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("capture ready publication");
    assert!(snapshot.finish(&state).await.is_ok());
    publish_v2_names_fixture(&database).await?;
    let error = snapshot
        .finish(&state)
        .await
        .expect_err("republished state cannot finish prior read");
    assert_eq!(error.code(), crate::v2::ErrorCode::Stale);
    // A first page carried no cursor, so there is none to drop: the request is retried.
    assert_eq!(
        error.envelope().error.message,
        "collection publication changed during the read; retry the request"
    );

    // A continued page must restart without its cursor.
    let first = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("capture republished publication");
    let cursor = crate::v2::encode(&first.bind_cursor(crate::v2::CursorPayload::new(
        "test",
        Default::default(),
        Default::default(),
        None,
    )));
    let continued = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        Some(&cursor),
        Some("ens"),
    )
    .await
    .expect("cursor bound to the current publication");
    publish_v2_names_fixture(&database).await?;
    let error = continued
        .finish(&state)
        .await
        .expect_err("republished state cannot finish a continued read");
    assert_eq!(error.code(), crate::v2::ErrorCode::Stale);
    assert_eq!(
        error.envelope().error.message,
        "collection publication is no longer available; restart pagination without a cursor"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_names_paginates_by_keyset_and_binds_cursors_to_window_and_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;

    let first = v2_names_payload(
        &database,
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1",
    )
    .await?;
    assert_eq!(v2_names_listed(&first), vec!["gamma.eth"]);
    assert_eq!(first["page"]["has_more"], json!(true));
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must carry a cursor")
        .to_owned();

    let second = v2_names_payload(
        &database,
        &format!(
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={cursor}"
        ),
    )
    .await?;
    assert_eq!(v2_names_listed(&second), vec!["beta.eth"]);
    assert_eq!(second["page"]["cursor"], json!(cursor));
    let second_cursor = second["page"]["next_cursor"]
        .as_str()
        .expect("second page must carry a cursor")
        .to_owned();
    let third = v2_names_payload(
        &database,
        &format!("/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={second_cursor}"),
    )
    .await?;
    assert_eq!(v2_names_listed(&third), vec!["alpha.eth"]);
    assert_eq!(third["page"]["has_more"], json!(false));
    assert_eq!(third["page"]["next_cursor"], Value::Null);

    for uri in [
        format!(
            "/v1/names?namespace=ens&expires_after=2025-01-02T00:00:00Z&page_size=1&cursor={cursor}"
        ),
        format!(
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&expires_before=2027-06-01T00:00:00Z&page_size=1&cursor={cursor}"
        ),
        format!(
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&order=desc&page_size=1&cursor={cursor}"
        ),
        format!(
            "/v1/names?namespace=basenames&expires_after=2025-01-01T00:00:00Z&page_size=1&cursor={cursor}"
        ),
        "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&cursor=not-a-cursor".to_owned(),
    ] {
        let response = v2_names_response(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], json!("invalid_input"));
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_names_rejects_unbounded_or_malformed_requests() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_fixture(&database).await?;

    for (uri, message_fragment) in [
        (
            "/v1/names?expires_after=2025-01-01T00:00:00Z",
            "namespace is required",
        ),
        ("/v1/names?namespace=ens", "expires_after or expires_before"),
        (
            "/v1/names?namespace=ens&sort=expires_at",
            "expires_after or expires_before",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2027-01-01T00:00:00Z&expires_before=2026-01-01T00:00:00Z",
            "earlier than",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2026-01-01T00:00:00Z&expires_before=2026-01-01T00:00:00Z",
            "earlier than",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&sort=name",
            "sort must be expires_at",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&sort=registered_at",
            "sort must be expires_at",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&order=sideways",
            "order is invalid",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01",
            "RFC 3339",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&page_size=201",
            "page_size",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&q=al",
            "q",
        ),
        (
            "/v1/names?namespace=ens&expires_after=2025-01-01T00:00:00Z&at=2026-01-01T00:00:00Z",
            "at is not supported",
        ),
    ] {
        let response = v2_names_response(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], json!("invalid_input"), "{uri}");
        let message = body["error"]["message"]
            .as_str()
            .expect("error message must be a string");
        assert!(
            message.contains(message_fragment),
            "{uri}: {message} must mention {message_fragment}"
        );
    }

    let response = v2_names_response(
        &database,
        "/v1/names?namespace=nope&expires_after=2025-01-01T00:00:00Z",
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value = read_json(response).await?;
    assert_eq!(body["error"]["code"], json!("not_found"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_indexed_name_read_carries_weak_etag_and_honours_if_none_match() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("indexed name request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response
        .headers()
        .get(axum::http::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("indexed name read must carry an ETag")
        .to_owned();
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("public, max-age=12, stale-while-revalidate=48")
    );
    let payload: Value = read_json(response).await?;
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("indexed name read must carry meta.as_of_token");
    assert!(etag.starts_with("W/\""));
    assert_ne!(etag, format!("W/\"{token}\""));

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth")
                .header(axum::http::header::IF_NONE_MATCH, etag.as_str())
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("conditional indexed name request failed")?;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(etag.as_str())
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .context("304 body must read")?;
    assert!(body.is_empty(), "304 must carry no body");

    // The same snapshot pinned explicitly yields the same validator; a verified read never does.
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/names/Alice.eth?at={token}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("pinned indexed name request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(etag.as_str())
    );

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth?source=verified")
                .header(axum::http::header::IF_NONE_MATCH, "*")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified name request failed")?;
    assert_ne!(response.status(), StatusCode::NOT_MODIFIED);
    assert!(
        !response.headers().contains_key(axum::http::header::ETAG),
        "verified reads must not carry an ETag"
    );
    assert!(
        !response
            .headers()
            .contains_key(axum::http::header::CACHE_CONTROL),
        "verified reads must not carry Cache-Control"
    );

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/subnames")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("subnames request failed")?;
    assert!(
        !response.headers().contains_key(axum::http::header::ETAG),
        "collections must not carry an ETag"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_explicit_namespace_ignores_unavailable_other_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": { "chain_id": "ethereum-mainnet", "block_number": 100,
                "block_hash": "0xcollection-head", "timestamp": "2026-06-10T00:00:00Z" }
        }))
        .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 100, "0xcollection-head").await?;
    let state = database.app_state();
    let ens = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("ENS publication is ready independently");
    ens.finish(&state).await.expect("ENS still ready");
    assert!(
        crate::v2::collection_snapshot::CollectionSnapshot::capture(&state, None)
            .await
            .is_err(),
        "aggregate must not silently omit an unavailable namespace"
    );
    database.cleanup().await
}
