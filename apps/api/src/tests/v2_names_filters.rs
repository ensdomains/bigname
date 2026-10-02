// `GET /v1/names` `authority` and `parent`: one window holding an ENSv1 lease, an ENSv2 `.eth`
// registration, a wrapped subname of the lease and an ENSv2 subname of the registration.

const V2_NAMES_FILTER_HOLDER: &str = "0x00000000000000000000000000000000000000f7";
const V2_NAMES_FILTER_WINDOW: &str = "expires_after=1800000000&expires_before=1900000000";

/// One name of the filter window at `block` on the `ens_v1` or `ens_v2` arm, with `events`
/// (kind, family, after state, whether it names the name) on its resource.
async fn seed_names_filter_name(
    database: &TestDatabase,
    name: &str,
    block: i64,
    arm: &str,
    events: Vec<(&str, &str, Value, bool)>,
) -> Result<(String, Uuid)> {
    let hash = format!("0xnames{block}");
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            &hash,
            None,
            block,
            parse_rfc3339_utc_timestamp("2024-05-02T00:00:00Z")?.unix_timestamp(),
        )],
    )
    .await?;
    let resource = Uuid::from_u128(0x87000 + block as u128 * 10);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        "ethereum-mainnet",
        80,
        "0xnames-created",
        resource,
        Uuid::from_u128(resource.as_u128() + 1),
        Uuid::from_u128(resource.as_u128() + 2),
        arm,
    )
    .await?;
    let mut rows = Vec::new();
    for (log, (kind, family, after, named)) in events.into_iter().enumerate() {
        let mut event = history_event(
            &format!("names-filter-{name}-{log}"),
            named.then_some(logical.as_str()),
            Some(resource),
            Some("ethereum-mainnet"),
            Some(block),
            Some(&hash),
            Some("0xnames-filter"),
            Some(log as i64),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = family.into();
        event.before_state = json!({});
        event.after_state = after;
        rows.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &rows).await?;
    Ok((logical, resource))
}

async fn seed_v2_names_filter_fixture(database: &TestDatabase) -> Result<()> {
    seed_names_registration(
        database,
        "ens",
        "lease.eth",
        91,
        "2024-01-02T00:00:00Z",
        1_810_000_000,
        V2_NAMES_FILTER_HOLDER,
        V2_NAMES_FILTER_HOLDER,
    )
    .await?;
    let ens_v2 = |expiry: u64| {
        vec![
            (
                "RegistrationGranted",
                "ens_v2_registry_l1",
                json!({"source_event":"NameRegistered", "authority_kind":"ens_v2_registry",
                    "owner":V2_NAMES_FILTER_HOLDER, "registrant":V2_NAMES_FILTER_HOLDER,
                    "expiry":expiry}),
                true,
            ),
            (
                "TokenControlTransferred",
                "ens_v2_registry_l1",
                json!({"source_event":"Transfer",
                    "from":"0x0000000000000000000000000000000000000000",
                    "to":V2_NAMES_FILTER_HOLDER}),
                true,
            ),
        ]
    };
    seed_names_filter_name(database, "fresh.eth", 92, "ens_v2", ens_v2(1_820_000_000)).await?;
    seed_names_filter_name(database, "kid.fresh.eth", 94, "ens_v2", ens_v2(1_840_000_000))
        .await?;
    let node = bigname_lookup::ens_namehash_hex("wrapped.lease.eth")?;
    let wrapped = |after: Value| ("", "ens_v1_wrapper_l1", after, true);
    let mut events = vec![
        wrapped(json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper"})),
        wrapped(json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper",
            "owner":V2_NAMES_FILTER_HOLDER})),
        wrapped(json!({"source_event":"NameWrapped", "node":node,
            "owner":V2_NAMES_FILTER_HOLDER, "to_address":V2_NAMES_FILTER_HOLDER})),
        wrapped(json!({"source_event":"NameWrapped", "wrapper_state":"wrapped", "fuses":0})),
        wrapped(json!({"source_event":"NameWrapped", "expiry":1_830_000_000_u64})),
    ];
    for (event, (kind, named)) in events.iter_mut().zip([
        ("SurfaceBound", true),
        ("AuthorityEpochChanged", true),
        ("TokenControlTransferred", true),
        ("PermissionScopeChanged", false),
        ("ExpiryChanged", false),
    ]) {
        event.0 = kind;
        event.3 = named;
    }
    let (_, wrapper) = seed_names_filter_name(database, "wrapped.lease.eth", 93, "ens_v1", events)
        .await?;
    // The binding records the log of the `NameWrapped` that opened it.
    sqlx::query(
        "UPDATE surface_bindings SET provenance = jsonb_build_object('transaction_index', 0,
         'log_index', 0) WHERE surface_binding_id = $1",
    )
    .bind(Uuid::from_u128(wrapper.as_u128() + 2))
    .execute(&database.pool)
    .await?;
    publish_v2_names_fixture(database).await
}

fn v2_names_filter_uri(filters: &str) -> String {
    format!("/v1/names?namespace=ens&{V2_NAMES_FILTER_WINDOW}{filters}")
}

#[tokio::test]
async fn v2_get_names_serves_authority_and_filters_by_authority_and_parent() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_filter_fixture(&database).await?;

    let all = v2_names_payload(&database, &v2_names_filter_uri("")).await?;
    assert_eq!(
        v2_names_listed(&all),
        ["lease.eth", "fresh.eth", "wrapped.lease.eth", "kid.fresh.eth"],
        "{all:#}"
    );
    assert_eq!(all["page"]["total_count"], Value::Null);
    for row in all["data"].as_array().expect("names data") {
        let name = row["name"].as_str().expect("row name");
        let detail = v2_get_json(&database, &format!("/v1/names/{name}")).await?;
        assert_eq!(row.get("authority"), detail["data"].get("authority"), "{name}");
        assert_eq!(
            row["authority"],
            if name.contains("fresh") { "ens_v2" } else { "ens_v1" },
            "{name}"
        );
        assert_eq!(row.get("ens_v1").is_some(), row["authority"] == "ens_v1", "{row}");
    }

    for (filters, expected) in [
        ("&authority=ens_v1", vec!["lease.eth", "wrapped.lease.eth"]),
        ("&authority=ens_v2", vec!["fresh.eth", "kid.fresh.eth"]),
        ("&authority=ens_v1,ens_v0", vec!["lease.eth", "wrapped.lease.eth"]),
        ("&authority=ens_v0", vec![]),
        ("&parent=eth", vec!["lease.eth", "fresh.eth"]),
        ("&parent=ETH", vec!["lease.eth", "fresh.eth"]),
        ("&parent=eth&authority=ens_v2", vec!["fresh.eth"]),
        ("&parent=eth&authority=ens_v0,ens_v1", vec!["lease.eth"]),
        ("&parent=lease.eth", vec!["wrapped.lease.eth"]),
        ("&parent=fresh.eth&authority=ens_v2", vec!["kid.fresh.eth"]),
        ("&parent=base.eth", vec![]),
        ("&order=desc&parent=eth", vec!["fresh.eth", "lease.eth"]),
    ] {
        let uri = v2_names_filter_uri(filters);
        let payload = v2_names_payload(&database, &uri).await?;
        assert_eq!(v2_names_listed(&payload), expected, "{uri}");
        assert_eq!(payload["page"]["total_count"], Value::Null, "{uri}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_names_cursor_binds_authority_and_parent() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_filter_fixture(&database).await?;

    let filters = "&authority=ens_v2,ens_v1&parent=eth&page_size=1";
    let first = v2_names_payload(&database, &v2_names_filter_uri(filters)).await?;
    assert_eq!(v2_names_listed(&first), ["lease.eth"]);
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .context("a filtered first page continues")?;
    // The same set in another order and spelling binds the same cursor.
    let next = v2_names_payload(
        &database,
        &v2_names_filter_uri(&format!(
            "&authority=ens_v1,,ens_v2&parent=Eth&page_size=1&cursor={cursor}"
        )),
    )
    .await?;
    assert_eq!(v2_names_listed(&next), ["fresh.eth"]);
    assert_eq!(next["page"]["has_more"], json!(false));

    for other in [
        "&page_size=1",
        "&authority=ens_v2,ens_v1&page_size=1",
        "&parent=eth&page_size=1",
        "&authority=ens_v2&parent=eth&page_size=1",
        "&authority=ens_v2,ens_v1&parent=lease.eth&page_size=1",
    ] {
        let uri = v2_names_filter_uri(&format!("{other}&cursor={cursor}"));
        let response = v2_names_response(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_get_names_rejects_invalid_authority_and_parent() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_names_filter_fixture(&database).await?;

    for filters in [
        "&authority=,",
        "&authority=basenames",
        "&authority=ens_v1&authority=ens_v2",
        "&parent=",
        "&parent=%20",
        "&parent=a..eth",
        "&parent=eth&parent=base.eth",
    ] {
        let uri = v2_names_filter_uri(filters);
        let response = v2_names_response(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], "invalid_input", "{uri}: {payload}");
    }
    // A whitespace-only `authority` is absent, as on address names.
    let blank = v2_names_payload(&database, &v2_names_filter_uri("&authority=%20")).await?;
    assert_eq!(v2_names_listed(&blank).len(), 4, "{blank:#}");
    database.cleanup().await
}
