//! `GET /v1/events` that `address` alone anchors pages through the address-history catalogue, as
//! address history does. A page reads a bounded prefix of the address's history in every plan
//! mode; it never sends the statement that ORs every name and resource of the address.
use super::*;

/// The registry the first fixture name's events are reattributed to.
const CONTRACT: &str = "0x000000000000000000000000000000000000e235";
/// A contract that emitted no event at all.
const SILENT_CONTRACT: &str = "0x000000000000000000000000000000000000dead";
/// The membership the SQL page ORs together: every logical name of the address in one array.
const NAME_ARRAY_MEMBERSHIP: &str = "ne.logical_name_id = ANY(";

/// A counted pool whose connections run with `plan_mode` as their `plan_cache_mode`.
async fn counted_pool(
    database: &TestDatabase,
    plan_mode: &str,
    connections: u32,
) -> Result<bigname_test_support::StatementCounter> {
    let setup = format!("SET plan_cache_mode = {plan_mode}");
    bigname_test_support::StatementCounter::connect_with_pool_options(
        &database.lookup_pool,
        PgPoolOptions::new()
            .max_connections(connections)
            .after_connect(move |connection, _| {
                let setup = setup.clone();
                Box::pin(async move {
                    sqlx::query(&setup).execute(&mut *connection).await?;
                    Ok(())
                })
            }),
    )
    .await
}

type Walked = (
    StatusCode,
    Value,
    bigname_storage::AddressHistoryWorkingSet,
    Vec<String>,
);

/// One request on `counter`: its status, payload, walk measurements and statements.
async fn walked_on(counter: &bigname_test_support::StatementCounter, uri: &str) -> Result<Walked> {
    let state = AppState::new_with_rpc_urls(
        counter.pool().clone(),
        bigname_lookup::ChainRpcUrls::default(),
    )
    .with_public_namespaces_for_test(["ens"]);
    let stats = Arc::new(Mutex::new(bigname_storage::AddressHistoryWorkingSet {
        batch_size: 7,
        ..Default::default()
    }));
    let request = Request::builder().uri(uri).body(Body::empty())?;
    let (response, sent) = counter
        .count(bigname_storage::with_address_history_working_set(
            stats.clone(),
            app_router(state).oneshot(request),
        ))
        .await;
    let response = response.context("events request failed")?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    let stats = stats.lock().unwrap().clone();
    Ok((status, body, stats, sent.statements().to_vec()))
}

/// The page took the catalogue walk and read a few pages of candidates at most, however long
/// the address's history is.
fn assert_bounded_walk(
    uri: &str,
    stats: &bigname_storage::AddressHistoryWorkingSet,
    statements: &[String],
    page_size: usize,
) {
    assert_eq!(
        stats.counters.get("catalogue"),
        Some(&1),
        "{uri}: the read did not take the catalogue walk: {stats:?}"
    );
    let candidates = stats
        .counters
        .get("catalogue_candidate_events")
        .copied()
        .unwrap_or_default();
    assert!(
        candidates <= 3 * (page_size + 1),
        "{uri}: {candidates} candidate events for one page: {stats:?}"
    );
    assert!(
        stats
            .peak
            .get("retained_page_ids")
            .copied()
            .unwrap_or_default()
            <= page_size + 1,
        "{uri}: {stats:?}"
    );
    let unbounded = statements
        .iter()
        .filter(|statement| statement.contains(NAME_ARRAY_MEMBERSHIP))
        .collect::<Vec<_>>();
    assert!(
        unbounded.is_empty(),
        "{uri}: the read sent the page over every name of the address: {unbounded:?}"
    );
}

/// `uri` returns the rows of `oracle` (address history, which reads the same events) and stays
/// bounded under a forced custom plan, a forced generic plan, and six default-mode runs on one
/// connection, each run reading the same candidates.
async fn assert_bounded_pages(
    database: &TestDatabase,
    uri: &str,
    oracle: &str,
    page_size: usize,
) -> Result<()> {
    let expected = hk_ids(&v2_history_payload_for_database(database, oracle).await?);
    assert_eq!(expected.len(), page_size, "{oracle}");
    let connections = database.lookup_pool.options().get_max_connections();
    for (mode, connections, runs) in [
        ("force_custom_plan", connections, 1),
        ("force_generic_plan", connections, 1),
        ("auto", 1, 6),
    ] {
        let counter = counted_pool(database, mode, connections).await?;
        let mut first = None;
        for run in 0..runs {
            let (status, body, stats, statements) = walked_on(&counter, uri).await?;
            assert_eq!(status, StatusCode::OK, "{uri} {mode}: {body}");
            assert_eq!(
                hk_ids(&body),
                expected,
                "{uri} {mode} run {run}: the rows differ from {oracle}"
            );
            assert_bounded_walk(uri, &stats, &statements, page_size);
            let shape = (
                stats.counters.get("catalogue_candidate_events").copied(),
                stats.counters.get("catalogue_bucket_batches").copied(),
            );
            let first_shape = *first.get_or_insert(shape);
            assert_eq!(shape, first_shape, "{uri} {mode} run {run}: {stats:?}");
        }
        counter.close().await;
    }
    Ok(())
}

/// One name with more events than a page. Today's page reads all of them, then sorts.
#[tokio::test]
async fn events_address_one_name_over_the_page_limit_reads_a_bounded_prefix() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 1).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "walk-0000.eth");
    let renewals = (0..1_201)
        .map(|n| {
            address_fixture_event(
                &format!("events-walk-renewal-{n:04}"),
                Some(&logical),
                Some(Uuid::from_u128(0x235000)),
                "RegistrationRenewed",
                "ens_v1_registrar_l1",
                BLOCK,
                HASH,
                100 + n,
                json!({"expiry": 1_900_000_000_i64}),
            )
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &renewals).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", BLOCK, HASH).await?;
    assert_bounded_pages(
        &database,
        &format!("/v1/events?address={ADDRESS}&page_size=20"),
        &format!("/v1/addresses/{ADDRESS}/history?page_size=20"),
        20,
    )
    .await?;
    database.cleanup().await
}

/// An address holding more names than a page. Today's page ORs all 257 names and their
/// resources into one statement.
#[tokio::test]
async fn events_address_heavy_holder_page_is_bounded() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 257).await?;
    assert_bounded_pages(
        &database,
        &format!("/v1/events?address={ADDRESS}&page_size=50"),
        &format!("/v1/addresses/{ADDRESS}/history?page_size=50"),
        50,
    )
    .await?;
    database.cleanup().await
}

/// `contract_address` filters inside the walk, and a contract that emitted nothing
/// answers the empty page without walking the address's history.
#[tokio::test]
async fn events_address_contract_address_filters_inside_the_walk() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let events = seed_names(&database, 3).await?;
    sqlx::query(
        "UPDATE bigname_phase.normalized_events
         SET raw_fact_ref = raw_fact_ref || jsonb_build_object('emitting_address', $1::text)
         WHERE event_identity LIKE 'relation-235000-%'",
    )
    .bind(CONTRACT)
    .execute(&database.pool)
    .await?;
    let newest_first = |first_name: bool| {
        events
            .iter()
            .rev()
            .filter(|(_, id)| id.starts_with("relation-235000-") == first_name)
            .map(|(_, id)| hkw_id(id))
            .collect::<Vec<_>>()
    };
    let counter = counted_pool(
        &database,
        "auto",
        database.lookup_pool.options().get_max_connections(),
    )
    .await?;
    for (contract, expected) in [
        (CONTRACT, newest_first(true)),
        (RESOLVER, newest_first(false)),
    ] {
        let uri = format!(
            "/v1/events?address={ADDRESS}&contract_address={contract}&page_size=200&include=total_count"
        );
        let (status, body, _, _) = walked_on(&counter, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert_eq!(hk_ids(&body), expected, "{uri}: {body}");
        assert_eq!(body["page"]["total_count"], json!(expected.len()), "{uri}");
    }
    let uri = format!(
        "/v1/events?address={ADDRESS}&contract_address={SILENT_CONTRACT}&page_size=200&include=total_count"
    );
    let (status, body, stats, statements) = walked_on(&counter, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body}");
    assert_eq!(body["data"], json!([]), "{uri}");
    assert_eq!(body["page"]["next_cursor"], Value::Null, "{uri}");
    assert_eq!(body["page"]["has_more"], json!(false), "{uri}");
    assert_eq!(body["page"]["total_count"], json!(0), "{uri}");
    assert_eq!(
        stats.counters.get("catalogue"),
        Some(&1),
        "{uri}: the read did not take the catalogue walk: {stats:?}"
    );
    assert_eq!(
        stats.counters.get("contract_without_events"),
        Some(&1),
        "{uri}: {stats:?}"
    );
    for key in [
        "catalogue_bucket_seeks",
        "catalogue_bucket_batches",
        "witness_batches",
        "catalogue_direct_count",
        "address_history_exact_cursor_micros",
        "address_history_count_walk_micros",
    ] {
        assert!(
            !stats.counters.contains_key(key),
            "{uri}: walked the address's history ({key}): {stats:?}"
        );
    }
    assert!(
        !statements
            .iter()
            .any(|statement| statement.contains(NAME_ARRAY_MEMBERSHIP)),
        "{uri}: {statements:?}"
    );
    counter.close().await;
    database.cleanup().await
}

/// With the catalogue unstamped for the captured publication the read is the retryable
/// stale answer, as address history's is, never the unbounded page. Other anchors still read.
#[tokio::test]
async fn events_address_unstamped_catalogue_returns_stale() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 2).await?;
    let before = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?address={ADDRESS}&page_size=200"),
    )
    .await?;
    assert_eq!(hk_ids(&before).len(), 8, "{before}");
    sqlx::query("DELETE FROM bigname_phase.project_history_catalogue_marker")
        .execute(&database.pool)
        .await?;
    for uri in [
        format!("/v1/events?address={ADDRESS}&page_size=200"),
        format!("/v1/events?address={ADDRESS}&page_size=200&include=total_count"),
        format!("/v1/events?address={ADDRESS}&contract_address={RESOLVER}"),
        format!("/v1/addresses/{ADDRESS}/history?page_size=200"),
    ] {
        let response = v2_history_response_for_database(&database, &uri).await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{uri}: {body}");
        assert_eq!(body["error"]["code"], "stale", "{uri}: {body}");
    }
    for uri in [
        "/v1/events?name=walk-0000.eth&page_size=200".to_owned(),
        format!("/v1/events?address={ADDRESS}&name=walk-0000.eth&page_size=200"),
    ] {
        let page = v2_history_payload_for_database(&database, &uri).await?;
        assert!(!hk_ids(&page).is_empty(), "{uri}: {page}");
    }
    database.cleanup().await
}
