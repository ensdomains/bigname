//! `GET /v1/events?address=&contract_address=` visits only the catalogue buckets where both the
//! address and the contract can have a row. Its rows, order, cursors and totals are those of the
//! walk that visits every bucket of the address, and of the SQL page that served the read before.
use super::*;

/// The newest fixture block. Every block from `BLOCK` to it has lineage.
const HEAD: i64 = 16 * 256;
/// Contracts whose events the fixtures place by catalogue bucket.
const NEWEST: &str = "0x000000000000000000000000000000000000c909";
const MIDDLE: &str = "0x000000000000000000000000000000000000c905";
const OLDEST: &str = "0x000000000000000000000000000000000000c900";
const WINDOWED: &str = "0x000000000000000000000000000000000000c950";
const PAGED: &str = "0x000000000000000000000000000000000000c990";
const TIED: &str = "0x000000000000000000000000000000000000c9e0";
/// A contract that emitted events in buckets 2 and 7, none of them the address's.
const ELSEWHERE: &str = "0x000000000000000000000000000000000000c927";
/// A contract that emitted no event at all.
const SILENT: &str = "0x000000000000000000000000000000000000c9ff";
/// A contract with events in every bucket, like a registry.
const DENSE: &str = "0x000000000000000000000000000000000000c9dd";
/// A registry with two events in a long history of the address, neither of them the address's.
const REGISTRY: &str = "0x000000000000000000000000000000000000c9aa";
/// An address holding one name, all of whose events are in bucket 3.
const SPARSE: &str = "0x000000000000000000000000000000000000a236";
const SPARSE_CONTROLLER: &str = "0x000000000000000000000000000000000000b236";

/// A block in `bucket`. Bucket 0's is above `BLOCK`, so it has lineage too.
fn block(bucket: i64) -> i64 {
    bucket * 256 + 250
}

fn hash(block: i64) -> String {
    format!("0xhistory{block}")
}

/// Lineage for every block above `BLOCK` up to `HEAD`, with later blocks later in time.
async fn extend_lineage(database: &TestDatabase) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT 'ethereum-mainnet', '0xhistory' || n, n, to_timestamp(1776384000 + (n - 240) * 12),
                'canonical'::bigname_phase.canonicality_state
         FROM generate_series($1::bigint, $2::bigint) n
         ON CONFLICT (chain_id, block_hash) DO NOTHING",
    )
    .bind(BLOCK + 1)
    .bind(HEAD)
    .execute(&database.pool)
    .await?;
    publish_bounded_membership_at(database, HEAD).await
}

/// A renewal of `seed_names`' name `name`, which the fixture address registered.
fn renewal(identity: &str, name: usize, block: i64, log: i64) -> NormalizedEvent {
    let logical = bigname_storage::logical_name_id_for_name("ens", &format!("walk-{name:04}.eth"));
    address_fixture_event(
        identity,
        Some(&logical),
        Some(Uuid::from_u128(0x235000 + name as u128 * 4)),
        "RegistrationRenewed",
        "ens_v1_registrar_l1",
        block,
        &hash(block),
        log,
        json!({"expiry":1_900_000_000_i64}),
    )
}

/// An event that names no name or resource, so no address's catalogue holds it.
fn unnamed(identity: &str, block: i64, log: i64) -> NormalizedEvent {
    address_fixture_event(
        identity,
        None,
        None,
        "RegistrationRenewed",
        "ens_v1_registrar_l1",
        block,
        &hash(block),
        log,
        json!({"expiry":1_900_000_000_i64}),
    )
}

fn emitted_by(mut event: NormalizedEvent, contract: &str) -> NormalizedEvent {
    event.raw_fact_ref = json!({"kind":"raw_log", "event_identity":event.event_identity,
        "emitting_address":contract});
    event
}

/// Four names of the fixture address with events in buckets 0, 1, 5 and 9: each name's events
/// start in bucket 0, and names 1, 2 and 3 are renewed in buckets 1, 5 and 9. Each contract above
/// emitted the events its name says, the others nothing on the address.
async fn seed_buckets(database: &TestDatabase) -> Result<()> {
    seed_names(database, 4).await?;
    extend_lineage(database).await?;
    let mut events = vec![
        renewal("leapfrog-address-1", 1, block(1), 1_000),
        renewal("leapfrog-address-5", 2, block(5), 1_000),
        renewal("leapfrog-address-9", 3, block(9), 1_000),
        emitted_by(renewal("leapfrog-newest", 3, block(9) + 1, 1_001), NEWEST),
        emitted_by(renewal("leapfrog-middle", 2, block(5) + 1, 1_001), MIDDLE),
        emitted_by(renewal("leapfrog-oldest", 0, block(0) + 1, 1_001), OLDEST),
        emitted_by(
            renewal("leapfrog-window-5", 2, block(5) + 2, 1_002),
            WINDOWED,
        ),
        emitted_by(
            renewal("leapfrog-window-0", 0, block(0) + 2, 1_002),
            WINDOWED,
        ),
        emitted_by(renewal("leapfrog-page-9", 3, block(9) + 2, 1_002), PAGED),
        emitted_by(renewal("leapfrog-page-0", 1, block(0) + 3, 1_003), PAGED),
        emitted_by(unnamed("leapfrog-elsewhere-2", block(2), 1_000), ELSEWHERE),
        emitted_by(unnamed("leapfrog-elsewhere-7", block(7), 1_000), ELSEWHERE),
    ];
    // Two rows of one block and transaction apart only by log index, and two block-level rows
    // apart only by identity.
    let tied = block(5) + 3;
    for (identity, log) in [("leapfrog-tie-log-a", 1_010), ("leapfrog-tie-log-b", 1_011)] {
        events.push(emitted_by(renewal(identity, 2, tied, log), TIED));
    }
    for identity in ["leapfrog-tie-block-a", "leapfrog-tie-block-b"] {
        let mut event = emitted_by(renewal(identity, 2, tied, 0), TIED);
        event.transaction_hash = None;
        event.log_index = None;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", HEAD, &hash(HEAD)).await
}

/// The plan modes each namespace row runs in.
const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];

/// The app state the rows serve `ens` from.
fn ens_state(database: &TestDatabase) -> AppState {
    database.app_state_with_public_namespaces(&["ens"])
}

/// An app state serving `ens` and `basenames` on a pool of its own, whose connections run with
/// `plan_mode` as their `plan_cache_mode`.
async fn state_in(database: &TestDatabase, plan_mode: &str) -> Result<AppState> {
    let setup = format!("SET plan_cache_mode = {plan_mode}");
    let pool = PgPoolOptions::new()
        .max_connections(database.lookup_pool.options().get_max_connections())
        .after_connect(move |connection, _| {
            let setup = setup.clone();
            Box::pin(async move {
                sqlx::query(&setup).execute(&mut *connection).await?;
                Ok(())
            })
        })
        .connect_with(database.lookup_pool.connect_options().as_ref().clone())
        .await
        .context("failed to connect the plan-mode pool")?;
    let state = AppState::new_with_rpc_urls(pool, bigname_lookup::ChainRpcUrls::default());
    Ok(state.with_public_namespaces_for_test(["ens", "basenames"]))
}

/// The payload of `uri` on `state`, which must answer 200.
async fn payload_on(state: &AppState, uri: &str) -> Result<Value> {
    let request = Request::builder().uri(uri).body(Body::empty())?;
    let response = app_router(state.clone())
        .oneshot(request)
        .await
        .context("events request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {payload}");
    Ok(payload)
}

/// One page through the catalogue walk, with the leapfrog on or off, and its measurements.
async fn walked(
    database: &TestDatabase,
    uri: &str,
    leapfrog: bool,
) -> Result<(Value, bigname_storage::AddressHistoryWorkingSet)> {
    walked_on(&ens_state(database), uri, leapfrog).await
}

/// [`walked`] on `state`.
async fn walked_on(
    state: &AppState,
    uri: &str,
    leapfrog: bool,
) -> Result<(Value, bigname_storage::AddressHistoryWorkingSet)> {
    let stats = Arc::new(Mutex::new(bigname_storage::AddressHistoryWorkingSet {
        contract_leapfrog: leapfrog,
        ..Default::default()
    }));
    let body =
        bigname_storage::with_address_history_working_set(stats.clone(), payload_on(state, uri))
            .await?;
    let stats = stats.lock().unwrap().clone();
    assert_eq!(
        stats.counters.get("catalogue"),
        Some(&1),
        "{uri}: the read did not take the catalogue walk: {stats:?}"
    );
    Ok((body, stats))
}

fn counted(stats: &bigname_storage::AddressHistoryWorkingSet, key: &str) -> usize {
    stats.counters.get(key).copied().unwrap_or_default()
}

/// The same page on the SQL page.
async fn sql_page_on(state: &AppState, uri: &str) -> Result<Value> {
    crate::v2::with_address_sql_page(payload_on(state, uri)).await
}

/// Walks `/v1/events?{query}` to its end at `page_size`, and checks every page against the walk
/// without the leapfrog and the SQL page: rows, cursors and totals. Returns the row identities
/// and, per page, the bucket batches and seeks with and without the leapfrog.
async fn walk_three_ways(
    database: &TestDatabase,
    query: &str,
    page_size: usize,
) -> Result<(Vec<String>, Vec<[usize; 4]>)> {
    walk_three_ways_on(&ens_state(database), query, page_size).await
}

/// [`walk_three_ways`] on `state`.
async fn walk_three_ways_on(
    state: &AppState,
    query: &str,
    page_size: usize,
) -> Result<(Vec<String>, Vec<[usize; 4]>)> {
    let mut cursor: Option<String> = None;
    let mut rows = Vec::new();
    let mut pages = Vec::new();
    loop {
        let mut uri = format!("/v1/events?{query}&page_size={page_size}");
        if let Some(cursor) = &cursor {
            uri.push_str(&format!("&cursor={cursor}"));
        }
        let (body, stats) = walked_on(state, &uri, true).await?;
        let (without, before) = walked_on(state, &uri, false).await?;
        let sql = sql_page_on(state, &uri).await?;
        assert_eq!(body["data"], without["data"], "{uri}");
        assert_eq!(body["page"], without["page"], "{uri}");
        assert_eq!(body["data"], sql["data"], "{uri}");
        assert_eq!(body["page"], sql["page"], "{uri}");
        rows.extend(hk_ids(&body));
        let page = [
            counted(&stats, "catalogue_bucket_batches"),
            counted(&stats, "catalogue_bucket_seeks"),
            counted(&before, "catalogue_bucket_batches"),
            counted(&before, "catalogue_bucket_seeks"),
        ];
        eprintln!(
            "{uri}: catalogue_bucket_batches={} catalogue_bucket_seeks={} without the leapfrog: \
             catalogue_bucket_batches={} catalogue_bucket_seeks={}",
            page[0], page[1], page[2], page[3]
        );
        pages.push(page);
        cursor = body["page"]["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    Ok((rows, pages))
}

/// One full page of `contract` on the fixture address in `order`: the rows are `expected`, the
/// walk probes `batches` buckets and seeks at most `seeks` times, and the exact total is the
/// number of rows.
async fn assert_one_page(
    database: &TestDatabase,
    contract: &str,
    order: &str,
    window: &str,
    expected: &[&str],
    batches: usize,
    seeks: usize,
) -> Result<()> {
    let query = format!("address={ADDRESS}&contract_address={contract}&order={order}{window}");
    let (rows, pages) = walk_three_ways(database, &query, 200).await?;
    let expected = expected.iter().map(|id| hkw_id(id)).collect::<Vec<_>>();
    assert_eq!(rows, expected, "{query}");
    let [walked_batches, walked_seeks, ..] = pages[0];
    assert_eq!(
        walked_batches, batches,
        "{query}: buckets probed: {pages:?}"
    );
    assert!(
        walked_seeks <= seeks,
        "{query}: {walked_seeks} seeks: {pages:?}"
    );
    let total = format!("/v1/events?{query}&page_size=200&include=total_count");
    let (body, _) = walked(database, &total, true).await?;
    assert_eq!(
        body["page"]["total_count"],
        json!(expected.len()),
        "{total}"
    );
    Ok(())
}

/// A contract with events in buckets 2 and 7, none of them the address's, which holds names in
/// buckets 0, 1, 5 and 9. No bucket can hold a row, so none is probed. Each seek moves one side
/// past the other: address 9, contract 7, address 5, contract 2, address 1, contract none.
#[tokio::test]
async fn events_address_unrelated_contract_probes_no_bucket() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    assert_one_page(&database, ELSEWHERE, "desc", "", &[], 0, 6).await?;
    database.cleanup().await
}

/// The contract's only row is in the address's newest bucket.
#[tokio::test]
async fn events_address_contract_rows_in_newest_bucket() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    assert_one_page(&database, NEWEST, "desc", "", &["leapfrog-newest"], 1, 4).await?;
    database.cleanup().await
}

/// The contract's only row is in a middle bucket of the address, so the newer one is skipped.
#[tokio::test]
async fn events_address_contract_rows_in_middle_bucket() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    assert_one_page(&database, MIDDLE, "desc", "", &["leapfrog-middle"], 1, 4).await?;
    database.cleanup().await
}

/// The contract's only row is in the address's oldest bucket, below three it skips.
#[tokio::test]
async fn events_address_contract_rows_in_oldest_bucket() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    assert_one_page(&database, OLDEST, "desc", "", &["leapfrog-oldest"], 1, 4).await?;
    database.cleanup().await
}

/// Blocks 256 to 1535 cover buckets 1 to 5. The contract's bucket-0 row is outside them, so the
/// contract's side stops at bucket 5's row and the walk ends without probing bucket 1.
#[tokio::test]
async fn events_address_block_window_bounds_the_emitter_seek() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    let window = "&from_block=256&to_block=1535";
    assert_one_page(
        &database,
        WINDOWED,
        "desc",
        window,
        &["leapfrog-window-5"],
        1,
        4,
    )
    .await?;
    database.cleanup().await
}

/// The contract's rows are in buckets 9 and 0, one per page. The second page resumes in the
/// cursor's bucket and reaches bucket 0 over the buckets between without losing or repeating a
/// row, and every page equals the walk without the leapfrog, cursors included.
#[tokio::test]
async fn events_address_cursor_resumes_across_skipped_buckets() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    for (order, expected) in [
        ("desc", ["leapfrog-page-9", "leapfrog-page-0"]),
        ("asc", ["leapfrog-page-0", "leapfrog-page-9"]),
    ] {
        let query = format!("address={ADDRESS}&contract_address={PAGED}&order={order}");
        let (rows, pages) = walk_three_ways(&database, &query, 1).await?;
        assert_eq!(rows, expected.map(hkw_id).to_vec(), "{query}");
        assert_eq!(pages.len(), 2, "{query}: {pages:?}");
        for [batches, ..] in &pages {
            assert!(*batches <= 2, "{query}: buckets probed per page: {pages:?}");
        }
    }
    database.cleanup().await
}

/// Rows of one block and transaction apart only by log index, and block-level rows apart only
/// by identity, page one at a time in the order and with the cursors of the walk without the
/// leapfrog and of the SQL page.
#[tokio::test]
async fn events_address_contract_ties_unchanged() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    for order in ["desc", "asc"] {
        let query = format!("address={ADDRESS}&contract_address={TIED}&order={order}");
        let (rows, _) = walk_three_ways(&database, &query, 1).await?;
        assert_eq!(rows.len(), 4, "{query}");
    }
    database.cleanup().await
}

/// Two ENSv1 names of the address, whose events in bucket 0 its resolver emitted. A basenames
/// manifest also admits that resolver on the same chain, and under it the resolver wrote a
/// record for the first name's node in bucket 2. Returns the ids of the address's ens events.
async fn seed_cross_namespace(database: &TestDatabase) -> Result<Vec<String>> {
    let seeded = seed_names(database, 2).await?;
    extend_lineage(database).await?;
    let manifest = declare_family_fixture_resolver(
        &database.pool,
        "basenames",
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        RESOLVER,
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex("walk-0000.eth")?;
    let mut write = emitted_by(
        address_fixture_event(
            "leapfrog-basenames-write",
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            block(2),
            &hash(block(2)),
            1_000,
            json!({"source_event":"TextChanged", "node":node, "resolver":RESOLVER,
                "record_key":"text:description", "record_family":"text",
                "selector_key":"description", "value":"basenames"}),
        ),
        RESOLVER,
    );
    write.namespace = "basenames".into();
    write.manifest_version = 1;
    write.source_manifest_id = Some(manifest);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[write]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", HEAD, &hash(HEAD)).await?;
    let ids = seeded.iter().map(|(_, identity)| hkw_id(identity));
    Ok(ids.collect())
}

/// An ENSv1 name of the address points to a resolver that a basenames manifest also admits on
/// the same chain, and that resolver wrote a basenames record for the name's node. With that
/// resolver as `contract_address` and `namespace=ens`, the page and its total hold the name's ens
/// events and not the basenames write, and the walk probes no more buckets than the resolver
/// emitted in.
#[tokio::test]
async fn events_address_contract_keeps_to_the_requested_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let seeded = seed_cross_namespace(&database).await?;
    for order in ["desc", "asc"] {
        let query =
            format!("address={ADDRESS}&contract_address={RESOLVER}&namespace=ens&order={order}");
        let (rows, pages) = walk_three_ways(&database, &query, 200).await?;
        assert_eq!(rows.len(), seeded.len(), "{query}: {rows:?}");
        // The resolver emitted in buckets 0 and 2 only.
        assert!(pages[0][0] <= 2, "{query}: buckets probed: {pages:?}");
        assert!(
            !rows.contains(&hkw_id("leapfrog-basenames-write")),
            "{query}: {rows:?}"
        );
        let total = format!("/v1/events?{query}&page_size=200&include=total_count");
        let (body, _) = walked(&database, &total, true).await?;
        assert_eq!(body["page"]["total_count"], json!(seeded.len()), "{total}");
    }
    database.cleanup().await
}

/// Publishes base-mainnet, with no events, so that a basenames read has a collection publication
/// to read at, as `publish_v2_names_fixture` does for name reads.
async fn publish_base(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"base":{
            "chain_id":"base-mainnet", "block_number":100, "block_hash":"0xbase-history-head",
            "timestamp":"2026-04-17T00:00:00Z"
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 100, "0xbase-history-head").await
}

/// The `contract_address` reads of the namespace rows, each with the resolver of
/// [`seed_cross_namespace`].
#[derive(Clone, Copy)]
enum ContractRead {
    /// One page. The resolver emitted the address's ens events and the basenames write.
    WithEvents,
    /// One page from block 512, where the resolver emitted only the basenames write.
    WithoutTheAddresssEvents,
    /// One row per page, every cursor followed.
    Cursor,
    /// One row per page with `include=total_count`, every cursor followed.
    Count,
}

/// One namespace row: the resolver as `contract_address` in `namespace`, or in the route's
/// default namespace when it is `None`, in both orders and both plan modes. Every page equals
/// the walk without the leapfrog and the SQL page, cursors and totals included. The rows are
/// the address's events in the requested namespace that `read` admits, the exact total is their
/// number, and no page probes more buckets than the resolver emitted in.
async fn assert_namespace_row(namespace: Option<&str>, read: ContractRead) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let ens_events = seed_cross_namespace(&database).await?;
    if namespace == Some("basenames") {
        publish_base(&database).await?;
    }
    // The address holds no basenames name, so a basenames read admits none of its events.
    let mut expected = match (namespace, read) {
        (Some("basenames"), _) | (_, ContractRead::WithoutTheAddresssEvents) => Vec::new(),
        _ => ens_events,
    };
    expected.sort();
    // The resolver emitted in buckets 0 and 2, and from block 512 only in bucket 2.
    let (window, buckets) = match read {
        ContractRead::WithoutTheAddresssEvents => ("&from_block=512", 1),
        _ => ("", 2),
    };
    let (page_size, include) = match read {
        ContractRead::Cursor => (1, ""),
        ContractRead::Count => (1, "&include=total_count"),
        _ => (200, ""),
    };
    let namespace = match namespace {
        Some(namespace) => format!("&namespace={namespace}"),
        None => String::new(),
    };
    for mode in PLAN_MODES {
        let state = state_in(&database, mode).await?;
        for order in ["desc", "asc"] {
            let query = format!(
                "address={ADDRESS}&contract_address={RESOLVER}&order={order}{namespace}{window}"
            );
            let paged = format!("{query}{include}");
            let (mut rows, pages) = walk_three_ways_on(&state, &paged, page_size).await?;
            rows.sort();
            assert_eq!(rows, expected, "{paged} {mode}");
            for [batches, ..] in &pages {
                assert!(
                    *batches <= buckets,
                    "{paged} {mode}: buckets probed: {pages:?}"
                );
            }
            let total = format!("/v1/events?{query}&page_size=200&include=total_count");
            let (body, _) = walked_on(&state, &total, true).await?;
            assert_eq!(
                body["page"]["total_count"],
                json!(expected.len()),
                "{total} {mode}"
            );
        }
        state.pool.close().await;
    }
    database.cleanup().await
}

// The namespace rows: `namespace=ens`, no namespace (the route reads `ens`) and
// `namespace=basenames`, each with the four reads of `ContractRead`.

#[tokio::test]
async fn events_address_ens_contract_with_the_addresss_events() -> Result<()> {
    assert_namespace_row(Some("ens"), ContractRead::WithEvents).await
}

#[tokio::test]
async fn events_address_ens_contract_without_the_addresss_events() -> Result<()> {
    assert_namespace_row(Some("ens"), ContractRead::WithoutTheAddresssEvents).await
}

#[tokio::test]
async fn events_address_ens_contract_cursor() -> Result<()> {
    assert_namespace_row(Some("ens"), ContractRead::Cursor).await
}

#[tokio::test]
async fn events_address_ens_contract_count() -> Result<()> {
    assert_namespace_row(Some("ens"), ContractRead::Count).await
}

#[tokio::test]
async fn events_address_default_namespace_contract_with_the_addresss_events() -> Result<()> {
    assert_namespace_row(None, ContractRead::WithEvents).await
}

#[tokio::test]
async fn events_address_default_namespace_contract_without_the_addresss_events() -> Result<()> {
    assert_namespace_row(None, ContractRead::WithoutTheAddresssEvents).await
}

#[tokio::test]
async fn events_address_default_namespace_contract_cursor() -> Result<()> {
    assert_namespace_row(None, ContractRead::Cursor).await
}

#[tokio::test]
async fn events_address_default_namespace_contract_count() -> Result<()> {
    assert_namespace_row(None, ContractRead::Count).await
}

#[tokio::test]
async fn events_address_basenames_contract_with_the_addresss_events() -> Result<()> {
    assert_namespace_row(Some("basenames"), ContractRead::WithEvents).await
}

#[tokio::test]
async fn events_address_basenames_contract_without_the_addresss_events() -> Result<()> {
    assert_namespace_row(Some("basenames"), ContractRead::WithoutTheAddresssEvents).await
}

#[tokio::test]
async fn events_address_basenames_contract_cursor() -> Result<()> {
    assert_namespace_row(Some("basenames"), ContractRead::Cursor).await
}

#[tokio::test]
async fn events_address_basenames_contract_count() -> Result<()> {
    assert_namespace_row(Some("basenames"), ContractRead::Count).await
}

/// A contract with events in every bucket and an address whose one name has events only in
/// bucket 3: the walk probes that bucket alone, as it does without the leapfrog.
#[tokio::test]
async fn events_address_dense_contract_sparse_address() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 1).await?;
    extend_lineage(&database).await?;
    seed_relation_name_inputs_at(
        &database,
        "sparse-0000.eth",
        0x236000,
        block(3),
        &hash(block(3)),
        RelationNameAccounts {
            registrant: SPARSE,
            controller: SPARSE_CONTROLLER,
            resolver: RESOLVER,
        },
    )
    .await?;
    let dense = (0..=9)
        .map(|bucket| {
            emitted_by(
                unnamed(
                    &format!("leapfrog-dense-{bucket}"),
                    block(bucket) + 5,
                    1_000,
                ),
                DENSE,
            )
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &dense).await?;
    sqlx::query(
        "UPDATE bigname_phase.normalized_events
         SET raw_fact_ref = raw_fact_ref || jsonb_build_object('emitting_address', $1::text)
         WHERE event_identity LIKE 'relation-236000-%'",
    )
    .bind(DENSE)
    .execute(&database.pool)
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", HEAD, &hash(HEAD)).await?;
    for order in ["desc", "asc"] {
        let query = format!("address={SPARSE}&contract_address={DENSE}&order={order}");
        let (rows, pages) = walk_three_ways(&database, &query, 200).await?;
        assert_eq!(rows.len(), 4, "{query}");
        let [batches, seeks, ..] = pages[0];
        assert_eq!(batches, 1, "{query}: {pages:?}");
        assert!(seeks <= 4, "{query}: {pages:?}");
    }
    database.cleanup().await
}

/// In ascending order the walk mirrors the descending one: the unrelated contract probes no
/// bucket, and the middle, oldest and windowed rows each take one probe.
#[tokio::test]
async fn events_address_contract_leapfrog_ascending_mirrors_descending() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_buckets(&database).await?;
    assert_one_page(&database, ELSEWHERE, "asc", "", &[], 0, 6).await?;
    assert_one_page(&database, NEWEST, "asc", "", &["leapfrog-newest"], 1, 4).await?;
    assert_one_page(&database, MIDDLE, "asc", "", &["leapfrog-middle"], 1, 4).await?;
    assert_one_page(&database, OLDEST, "asc", "", &["leapfrog-oldest"], 1, 4).await?;
    let window = "&from_block=256&to_block=1535";
    assert_one_page(
        &database,
        WINDOWED,
        "asc",
        window,
        &["leapfrog-window-5"],
        1,
        4,
    )
    .await?;
    database.cleanup().await
}

/// The measured shape: an address whose sixteen names each start in bucket 0 and end in a
/// different bucket from 0 to 15, and a registry with two events in buckets 4 and 11 that
/// involve none of them, with no block window. Without the leapfrog the walk probes a bucket
/// per distinct end of the address's names. With it, it probes only the registry's two buckets
/// and seeks at most twice per side of the registry's buckets, plus the final pair. The page is
/// empty either way and the exact total is 0.
#[tokio::test]
async fn events_address_unrelated_contract_leapfrogs_the_address_buckets() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 16).await?;
    extend_lineage(&database).await?;
    let mut events = (1..16)
        .map(|name| {
            renewal(
                &format!("leapfrog-wide-{name}"),
                name,
                block(name as i64),
                1_000,
            )
        })
        .collect::<Vec<_>>();
    for bucket in [4, 11] {
        events.push(emitted_by(
            unnamed(
                &format!("leapfrog-registry-{bucket}"),
                block(bucket) + 7,
                1_001,
            ),
            REGISTRY,
        ));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", HEAD, &hash(HEAD)).await?;
    let query = format!("address={ADDRESS}&contract_address={REGISTRY}");
    let (rows, pages) = walk_three_ways(&database, &query, 200).await?;
    assert!(rows.is_empty(), "{query}: {rows:?}");
    let [batches, seeks, before_batches, _] = pages[0];
    let registry_buckets = 2;
    assert!(
        before_batches > 2 * registry_buckets,
        "{query}: the fixture is not the measured shape: {pages:?}"
    );
    assert!(batches <= registry_buckets, "{query}: {pages:?}");
    assert!(seeks <= 2 * (registry_buckets + 1), "{query}: {pages:?}");
    let total = format!("/v1/events?{query}&page_size=200&include=total_count");
    let (body, _) = walked(&database, &total, true).await?;
    assert_eq!(body["page"]["total_count"], json!(0), "{total}");
    assert_eq!(body["page"]["next_cursor"], Value::Null, "{total}");
    database.cleanup().await
}

/// A cursor without a position names its row only by identity. On a contract that emitted no
/// event no row can be it, so the read refuses the cursor before reading the address's history.
/// The route cannot send such a cursor: `history_keyset::resolve_on` gives every legacy cursor
/// its row's position first. So this test calls the storage read directly.
#[tokio::test]
async fn events_address_identity_cursor_on_a_silent_contract_reads_no_history() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 2).await?;
    let options = bigname_storage::HistoryPageOptions {
        contract_address: Some(SILENT.to_owned()),
        publication_block_bounds: Some(std::collections::BTreeMap::from([(
            "ethereum-mainnet".to_owned(),
            BLOCK,
        )])),
        ..Default::default()
    };
    let cursor = bigname_storage::HistoryCursor {
        normalized_event_id: None,
        event_identity: "relation-235000-RegistrationGranted".to_owned(),
        position: None,
    };
    let stats = Arc::new(Mutex::new(
        bigname_storage::AddressHistoryWorkingSet::default(),
    ));
    let error = bigname_storage::with_address_history_working_set(
        stats.clone(),
        bigname_storage::load_address_history_page_for_relations(
            &database.pool,
            ADDRESS,
            Some("ens"),
            None,
            bigname_storage::HistoryScope::Both,
            true,
            Some(&cursor),
            20,
            bigname_storage::HistorySummaryMode::None,
            &options,
            false,
        ),
    )
    .await
    .expect_err("no row of a silent contract can be the cursor's");
    assert!(
        error
            .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
            .is_some(),
        "{error:?}"
    );
    let stats = stats.lock().unwrap().clone();
    assert_eq!(counted(&stats, "contract_without_events"), 1, "{stats:?}");
    for key in [
        "witness_batches",
        "catalogue_bucket_batches",
        "catalogue_bucket_seeks",
    ] {
        assert!(
            !stats.counters.contains_key(key),
            "read the address's history ({key}): {stats:?}"
        );
    }
    database.cleanup().await
}
