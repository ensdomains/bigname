//! `GET /v1/events` that `address` alone anchors returns, page for page, what the SQL page that
//! served it before the address-history walk returns: the same rows, in the same order, with the
//! same cursors and totals. Cursors issued by either path continue on the other.
use super::*;

/// An address no fixture name, account or role belongs to.
const UNRELATED: &str = "0x000000000000000000000000000000000000e0e0";
/// `seed_names`' first registration, `walk-0000.eth`.
const FIRST_REGISTRATION: &str = "00000000-0000-0000-0000-000000235000";

/// One page through the address-history walk, which must have served it.
async fn walked_page(database: &TestDatabase, uri: &str) -> Result<Value> {
    let stats = Arc::new(Mutex::new(
        bigname_storage::AddressHistoryWorkingSet::default(),
    ));
    let body = bigname_storage::with_address_history_working_set(
        stats.clone(),
        v2_history_payload_for_database(database, uri),
    )
    .await?;
    let stats = stats.lock().unwrap().clone();
    assert_eq!(
        stats.counters.get("catalogue"),
        Some(&1),
        "{uri}: the read did not take the catalogue walk: {stats:?}"
    );
    Ok(body)
}

/// The same page on the SQL page.
async fn sql_page(database: &TestDatabase, uri: &str) -> Result<Value> {
    crate::v2::with_address_sql_page(v2_history_payload_for_database(database, uri)).await
}

/// Walks `/v1/events?{query}` to its end in both orders at each page size, comparing every page
/// of the two paths; the walk's own cursor continues each page. Returns the rows of the walk.
async fn assert_walk_matches_sql_page(
    database: &TestDatabase,
    query: &str,
    sizes: &[usize],
) -> Result<usize> {
    let mut rows = 0;
    for order in ["desc", "asc"] {
        for &size in sizes {
            let mut cursor: Option<String> = None;
            let mut seen = 0;
            loop {
                let mut uri = format!("/v1/events?{query}&order={order}&page_size={size}");
                if let Some(cursor) = &cursor {
                    uri.push_str(&format!("&cursor={cursor}"));
                }
                let walked = walked_page(database, &uri).await?;
                let sql = sql_page(database, &uri).await?;
                assert_eq!(walked["data"], sql["data"], "{uri}");
                assert_eq!(walked["page"], sql["page"], "{uri}");
                seen += walked["data"].as_array().map_or(0, Vec::len);
                cursor = walked["page"]["next_cursor"].as_str().map(str::to_owned);
                if cursor.is_none() {
                    break;
                }
            }
            rows = seen;
        }
    }
    Ok(rows)
}

/// Names held as registrant and as controller, their records, every event-type and block
/// filter, `contract_address`, exact totals, an address with nothing, and page sizes down to
/// one row, where the only key between rows of one block and log position is their identity.
#[tokio::test]
async fn events_address_walk_equals_the_sql_page_for_held_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let events = seed_names(&database, 11).await?;
    let all = Some(events.len());
    for (query, sizes, rows) in [
        (format!("address={ADDRESS}"), &[1, 7, 200][..], all),
        (
            format!("address={ADDRESS}&include=total_count"),
            &[7][..],
            all,
        ),
        (format!("address={ADDRESS}&type=record"), &[3][..], Some(11)),
        (
            format!("address={ADDRESS}&kind=RegistrationGranted,ResolverChanged"),
            &[5][..],
            Some(22),
        ),
        (
            format!("address={ADDRESS}&exclude_type=record"),
            &[200][..],
            Some(33),
        ),
        (
            format!("address={ADDRESS}&record_key=addr:60"),
            &[4][..],
            Some(11),
        ),
        (
            format!("address={ADDRESS}&contract_address={RESOLVER}&include=total_count"),
            &[7][..],
            all,
        ),
        (
            format!("address={ADDRESS}&from_block=240&to_block=240"),
            &[7][..],
            all,
        ),
        (format!("address={ADDRESS}&to_block=239"), &[7][..], Some(0)),
        (
            format!("address={ADDRESS}&from_timestamp=1776384000"),
            &[7][..],
            all,
        ),
        (format!("address={OTHER}"), &[1, 200][..], None),
        (
            format!("address={OTHER}&include=total_count"),
            &[200][..],
            None,
        ),
        (
            format!("address={UNRELATED}&include=total_count"),
            &[200][..],
            Some(0),
        ),
    ] {
        let walked = assert_walk_matches_sql_page(&database, &query, sizes).await?;
        if let Some(rows) = rows {
            assert_eq!(walked, rows, "{query}");
        }
    }
    database.cleanup().await
}

/// Registry handoff copies of one resolver change, of which the walk keeps one winner.
#[tokio::test]
async fn events_address_walk_equals_the_sql_page_across_handoff_duplicates() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 8).await?;
    let mut events = Vec::new();
    for (origin, log) in [("walk-handoff-a", 100), ("walk-handoff-b", 101)] {
        events.push(address_fixture_event(
            &format!("{origin}:ResolverChanged:registry-fallback-handoff:0000-outside"),
            None,
            None,
            "ResolverChanged",
            "ens_v1_registry_l1",
            BLOCK,
            HASH,
            log,
            json!({"node":"same-node","resolver":RESOLVER}),
        ));
        for n in 0..8 {
            let logical =
                bigname_storage::logical_name_id_for_name("ens", &format!("walk-{n:04}.eth"));
            events.push(address_fixture_event(
                &format!(
                    "{origin}:ResolverChanged:registry-fallback-handoff:{:04}",
                    n + 1
                ),
                Some(&logical),
                Some(Uuid::from_u128(0x235000 + n as u128 * 4)),
                "ResolverChanged",
                "ens_v1_registry_l1",
                BLOCK,
                HASH,
                log,
                json!({"node":"same-node","resolver":RESOLVER}),
            ));
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", BLOCK, HASH).await?;
    for (query, sizes) in [
        (
            format!("address={ADDRESS}&kind=ResolverChanged"),
            &[1, 200][..],
        ),
        (
            format!("address={ADDRESS}&kind=ResolverChanged&include=total_count"),
            &[3][..],
        ),
        (format!("address={ADDRESS}"), &[200][..]),
    ] {
        assert_walk_matches_sql_page(&database, &query, sizes).await?;
    }
    database.cleanup().await
}

/// A current role holder whose name was once held through another resource, with a resolver
/// record attributed to the current resource only.
#[tokio::test]
async fn events_address_walk_equals_the_sql_page_for_a_role_holder() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let current = seed_role_holder(&database, json!(["set_resolver"])).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let node = bigname_lookup::ens_namehash_hex("beta.eth")?;
    let (block, hash) = address_fixture_head(&database).await?;
    admit_fixture_resolver(
        &database.pool,
        "ens_v2_registry_l1",
        ROLE_REGISTRY,
        RESOLVER,
    )
    .await?;
    let (manifest, _) = declare_family_fixture_contract(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "ens_v2_resolver_l1",
        "public_resolver_v2",
        RESOLVER,
    )
    .await?;
    let pointer = address_fixture_event(
        "events-walk-role-pointer",
        Some(&logical),
        Some(current),
        "ResolverChanged",
        "ens_v2_registry_l1",
        block,
        &hash,
        200_000,
        json!({"node":node,"resolver":RESOLVER}),
    );
    let renewal = address_fixture_event(
        "events-walk-role-renewal",
        None,
        Some(current),
        "RegistrationRenewed",
        "ens_v2_registry_l1",
        block,
        &hash,
        200_001,
        json!({"expiry":1_900_000_000}),
    );
    let mut record = address_fixture_event(
        "events-walk-role-record",
        None,
        None,
        "RecordChanged",
        "ens_v2_resolver_l1",
        block,
        &hash,
        200_002,
        json!({"node":node,"resolver":RESOLVER,"record_key":"text:description",
            "record_family":"text","selector_key":"description","value":"current"}),
    );
    record.source_manifest_id = Some(manifest);
    record.manifest_version = 1;
    let historical = Uuid::from_u128(0xb236);
    sqlx::query("INSERT INTO resources(resource_id,chain_id,block_number,block_hash,canonicality_state) VALUES($1,'ethereum-mainnet',$2,$3,'canonical')")
        .bind(historical).bind(block).bind(&hash).execute(&database.pool).await?;
    let mut grant = address_fixture_event(
        "events-walk-role-prior-grant",
        Some(&logical),
        Some(historical),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        block,
        &hash,
        199_999,
        json!({"registrant":ROLE_HOLDER,"expiry":1_900_000_000}),
    );
    grant.derivation_kind = "ens_v2_registry_resource_surface".into();
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[pointer, renewal, record, grant],
    )
    .await?;
    rebuild_address_fixture(&database).await?;
    for (query, sizes) in [
        (format!("address={ROLE_HOLDER}"), &[1, 200][..]),
        (
            format!("address={ROLE_HOLDER}&include=total_count"),
            &[200][..],
        ),
        (
            format!("address={ROLE_HOLDER}&kind=RegistrationRenewed,RecordChanged"),
            &[1][..],
        ),
    ] {
        let rows = assert_walk_matches_sql_page(&database, &query, sizes).await?;
        assert!(rows > 0, "{query}");
    }
    database.cleanup().await
}

/// The shared history fixture's registrant, whose name has registration and name events.
#[tokio::test]
async fn events_address_walk_equals_the_sql_page_for_the_history_fixture() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    const REGISTRANT: &str = "0x00000000000000000000000000000000000000cc";
    for (query, sizes) in [
        (format!("address={REGISTRANT}"), &[1, 200][..]),
        (
            format!("address={REGISTRANT}&include=total_count"),
            &[2][..],
        ),
        (
            format!("address={REGISTRANT}&type=renewal,expiry"),
            &[1][..],
        ),
    ] {
        let rows = assert_walk_matches_sql_page(&database, &query, sizes).await?;
        assert!(rows > 0 || query.contains("type="), "{query}");
    }
    database.cleanup().await
}

/// Two events either side of a 256-block catalogue bucket boundary: a one-row walk crosses it
/// without losing or repeating a row, in both orders.
#[tokio::test]
async fn events_address_cursor_crosses_a_bucket_boundary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let events = seed_names(&database, 1).await?;
    publish_bounded_membership_at(&database, 255).await?;
    publish_bounded_membership_at(&database, 256).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "walk-0000.eth");
    let renewal = |identity: &str, block: i64| {
        address_fixture_event(
            identity,
            Some(&logical),
            Some(Uuid::from_u128(0x235000)),
            "RegistrationRenewed",
            "ens_v1_registrar_l1",
            block,
            &format!("0xhistory{block}"),
            0,
            json!({"expiry":1_900_000_000}),
        )
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            renewal("events-walk-bucket-0", 255),
            renewal("events-walk-bucket-1", 256),
        ],
    )
    .await?;
    publish_test_families(&database, 256).await?;
    let first = walked_page(
        &database,
        &format!("/v1/events?address={ADDRESS}&page_size=2"),
    )
    .await?;
    assert_eq!(
        hk_ids(&first),
        vec![
            hkw_id("events-walk-bucket-1"),
            hkw_id("events-walk-bucket-0")
        ],
        "{first}"
    );
    let rows =
        assert_walk_matches_sql_page(&database, &format!("address={ADDRESS}"), &[1][..]).await?;
    assert_eq!(rows, events.len() + 2);
    database.cleanup().await
}

/// A cursor issued by the SQL page has the history cursor layout, and it continues on the walk
/// at the same row, as do the same position written out by hand and the anchor-only form.
#[tokio::test]
async fn events_address_sql_page_cursor_resumes_on_the_walk() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 2).await?;
    for (order, sort) in [
        ("desc", "chain_position_desc"),
        ("asc", "chain_position_asc"),
    ] {
        let uri = format!("/v1/events?address={ADDRESS}&order={order}&page_size=1");
        let first = sql_page(&database, &uri).await?;
        let issued = first["page"]["next_cursor"]
            .as_str()
            .context("the address has more than one page")?
            .to_owned();
        let decoded: Value = serde_json::from_slice(&hex::decode(&issued)?)?;
        let anchor = decoded["last_item"]["event_identity"]
            .as_str()
            .context("cursor anchor")?
            .to_owned();
        assert_eq!(hk_ids(&first), vec![hkw_id(&anchor)], "{uri}");
        let (chain_id, block_number, block_hash, transaction_index, log_index): (
            String,
            Option<i64>,
            Option<String>,
            Option<i64>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT chain_id, block_number, block_hash, transaction_index, log_index
             FROM bigname_phase.normalized_events WHERE event_identity = $1",
        )
        .bind(&anchor)
        .fetch_one(&database.pool)
        .await?;
        let mut last_item = serde_json::Map::new();
        last_item.insert("event_identity".to_owned(), json!(anchor));
        last_item.insert("chain_id".to_owned(), json!(chain_id));
        for (key, value) in [
            ("block_number", block_number),
            ("transaction_index", transaction_index),
            ("log_index", log_index),
        ] {
            if let Some(value) = value {
                last_item.insert(key.to_owned(), json!(value.to_string()));
            }
        }
        if let Some(block_hash) = block_hash {
            last_item.insert("block_hash".to_owned(), json!(block_hash));
        }
        let layout = json!({
            "version": 1,
            "sort": sort,
            "filters": {"address": ADDRESS, "history_contract": "2", "namespace": "ens"},
            "last_item": last_item,
            "snapshot": null,
        });
        assert_eq!(decoded, layout, "{uri}: the issued cursor's layout");

        let continued = format!("{uri}&cursor={issued}");
        let walked = walked_page(&database, &continued).await?;
        let sql = sql_page(&database, &continued).await?;
        assert_eq!(walked["data"], sql["data"], "{continued}");
        assert_eq!(walked["page"], sql["page"], "{continued}");
        assert!(!hk_ids(&walked).contains(&hkw_id(&anchor)), "{continued}");

        let written = hex::encode(serde_json::to_vec(&layout)?);
        let legacy = hk_legacy_cursor(&database, &issued).await?;
        for cursor in [written, legacy] {
            let resumed = walked_page(&database, &format!("{uri}&cursor={cursor}")).await?;
            assert_eq!(resumed["data"], walked["data"], "{uri}&cursor={cursor}");
            assert_eq!(
                resumed["page"]["next_cursor"], walked["page"]["next_cursor"],
                "{uri}&cursor={cursor}"
            );
        }
    }
    database.cleanup().await
}

/// A name, registration or resolver anchor beside the address keeps the SQL page, and so does
/// the diagnostics route. A default address read counts nothing; an empty address reads one
/// bucket seek and no bucket.
#[tokio::test]
async fn events_address_other_anchors_keep_the_sql_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 2).await?;
    for uri in [
        format!("/v1/events?address={ADDRESS}&name=walk-0000.eth&page_size=200"),
        format!("/v1/events?address={ADDRESS}&registration_id={FIRST_REGISTRATION}&page_size=200"),
        format!("/v1/events?address={ADDRESS}&resolver=1:{RESOLVER}&page_size=200"),
        format!("/v1/diagnostics/events?address={ADDRESS}&page_size=20"),
    ] {
        let stats = Arc::new(Mutex::new(
            bigname_storage::AddressHistoryWorkingSet::default(),
        ));
        let body = bigname_storage::with_address_history_working_set(
            stats.clone(),
            v2_history_payload_for_database(&database, &uri),
        )
        .await?;
        let stats = stats.lock().unwrap().clone();
        assert!(
            !stats.counters.contains_key("catalogue"),
            "{uri}: took the walk: {stats:?}"
        );
        if uri.starts_with("/v1/events") {
            let sql = sql_page(&database, &uri).await?;
            assert_eq!(body["data"], sql["data"], "{uri}");
            assert_eq!(body["page"], sql["page"], "{uri}");
        }
    }

    for (address, rows) in [(ADDRESS, 8), (UNRELATED, 0)] {
        let uri = format!("/v1/events?address={address}&page_size=200");
        let (body, stats) = measured(&database, &uri, 200).await?;
        assert_eq!(hk_ids(&body).len(), rows, "{uri}: {body}");
        assert_eq!(body["page"]["total_count"], Value::Null, "{uri}");
        for key in [
            "catalogue_direct_count",
            "catalogue_proof_batches",
            "address_history_exact_cursor_micros",
            "address_history_count_walk_micros",
        ] {
            assert!(!stats.counters.contains_key(key), "{uri}: {key}: {stats:?}");
        }
        if rows == 0 {
            assert_eq!(
                stats.counters.get("catalogue_bucket_seeks"),
                Some(&1),
                "{uri}: {stats:?}"
            );
            assert!(
                !stats.counters.contains_key("catalogue_bucket_batches"),
                "{uri}: {stats:?}"
            );
        }
        let counted = format!("{uri}&include=total_count");
        let (body, _) = measured(&database, &counted, 200).await?;
        assert_eq!(body["page"]["total_count"], json!(rows), "{counted}");
    }
    database.cleanup().await
}
