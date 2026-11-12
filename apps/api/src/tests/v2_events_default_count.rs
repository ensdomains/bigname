// A `/v1/events` read counts only when it sends `include=total_count`. Without the flag
// `page.total_count` is null whatever anchors the read, and the request sends no count
// statement, so the history read runs once per page.

/// The history count statement's select list (`crates/storage/src/history/summary.rs`).
const EVENTS_COUNT_STATEMENT: &str = "COUNT(*)::BIGINT AS total_count";
/// The registration of `history.eth` in `seed_v2_history_fixture`.
const EVENTS_REGISTRATION: &str = "00000000-0000-0000-0000-000000007100";
const EVENTS_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";
const EVENTS_ADDRESS: &str = "0x00000000000000000000000000000000000000cc";

/// The payload of `uri` and the count statements it sent, on connections of its own.
async fn counted_events(database: &TestDatabase, uri: &str) -> Result<(Value, usize)> {
    let counter = bigname_test_support::StatementCounter::connect(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        counter.pool().clone(),
        bigname_lookup::ChainRpcUrls::default(),
    )
    .with_public_namespaces_for_test(["ens"]);
    let request = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request must build");
    let (response, sent) = counter.count(app_router(state).oneshot(request)).await;
    let response = response.context("counted events request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    counter.close().await;
    assert_eq!(status, StatusCode::OK, "{uri}: {payload}");
    let counts = sent
        .statements()
        .iter()
        .filter(|statement| statement.contains(EVENTS_COUNT_STATEMENT))
        .count();
    Ok((payload, counts))
}

/// The default read of `uri` reports no total and sends no count statement. The same read with
/// `include=total_count` sends one, returns the same page, and counts every row when the page
/// holds them all.
async fn assert_events_count_only_on_opt_in(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (default, default_counts) = counted_events(database, uri).await?;
    assert_eq!(
        default["page"]["total_count"],
        Value::Null,
        "{uri}: {default}"
    );
    assert_eq!(
        default_counts, 0,
        "{uri}: the default read sent a count statement"
    );
    // `include` is one parameter, so the flag joins a list the read already sends.
    let counted_uri = if uri.contains("include=") {
        uri.replacen("include=", "include=total_count,", 1)
    } else {
        format!("{uri}&include=total_count")
    };
    let (counted, counted_counts) = counted_events(database, &counted_uri).await?;
    assert_eq!(
        counted_counts, 1,
        "{counted_uri}: the opt-in sends one count statement"
    );
    assert_eq!(counted["data"], default["data"], "{counted_uri}");
    assert_eq!(
        counted["page"]["next_cursor"], default["page"]["next_cursor"],
        "{counted_uri}"
    );
    assert_eq!(
        counted["page"]["has_more"], default["page"]["has_more"],
        "{counted_uri}"
    );
    if default["page"]["has_more"] == json!(false) && !uri.contains("cursor=") {
        let rows = default["data"].as_array().context("events data")?.len();
        assert_eq!(
            counted["page"]["total_count"],
            json!(rows),
            "{counted_uri}: {counted}"
        );
    }
    Ok(default)
}

#[tokio::test]
async fn events_default_read_sends_no_count_statement_on_any_anchor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    // The record write is emitted by the resolver, which the resolver anchor then reads.
    sqlx::query(
        "UPDATE bigname_phase.normalized_events \
         SET raw_fact_ref = raw_fact_ref || jsonb_build_object('emitting_address', $1::text) \
         WHERE event_identity = 'history-record'",
    )
    .bind(EVENTS_RESOLVER)
    .execute(&database.pool)
    .await?;

    for route in [
        format!("/v1/events?registration_id={EVENTS_REGISTRATION}"),
        "/v1/events?name=history.eth".to_owned(),
        // An address alone reads the address-history walk, whose count is no single statement
        // (`v2_events_address_walk.rs`). With a name it keeps the SQL page.
        format!("/v1/events?address={EVENTS_ADDRESS}&name=history.eth"),
        format!("/v1/events?resolver=1:{EVENTS_RESOLVER}"),
        format!("/v1/events?resolver=1:{EVENTS_RESOLVER}&name=history.eth"),
        format!("/v1/events?registration_id={EVENTS_REGISTRATION}&finality=latest"),
        format!("/v1/events?registration_id={EVENTS_REGISTRATION}&from_block=102&to_block=110"),
        format!(
            "/v1/events?registration_id={EVENTS_REGISTRATION}&from_timestamp=1700000102&to_timestamp=1700000110"
        ),
    ] {
        for page_size in [1, 20, 200] {
            assert_events_count_only_on_opt_in(
                &database,
                &format!("{route}&page_size={page_size}"),
            )
            .await?;
        }
    }
    let rows = |payload: &Value| payload["data"].as_array().map_or(0, Vec::len);
    let resolver = assert_events_count_only_on_opt_in(
        &database,
        &format!("/v1/events?resolver=1:{EVENTS_RESOLVER}&page_size=200"),
    )
    .await?;
    assert_eq!(rows(&resolver), 2, "{resolver}");
    let registration = assert_events_count_only_on_opt_in(
        &database,
        &format!("/v1/events?registration_id={EVENTS_REGISTRATION}&page_size=200"),
    )
    .await?;
    assert!(rows(&registration) > 1, "{registration}");

    // A history read accepts only the latest selectors; anything else is refused before any read.
    for selector in ["finality=safe", "finality=finalized", "at=1"] {
        let uri = format!("/v1/events?registration_id={EVENTS_REGISTRATION}&{selector}");
        let response = v2_history_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }

    database.cleanup().await
}

/// The roles read a client makes for one registration's permission changes: no count on the
/// first page or on a page reached by cursor.
#[tokio::test]
async fn events_registration_permission_read_sends_no_count_statement() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;

    let roles = assert_events_count_only_on_opt_in(
        &database,
        &format!(
            "/v1/events?registration_id={EVENTS_REGISTRATION}&type=permission&include=data,raw&order=asc&page_size=200"
        ),
    )
    .await?;
    let rows = roles["data"].as_array().context("roles data")?;
    assert!(!rows.is_empty(), "{roles}");
    for row in rows {
        assert_eq!(row["type"], json!("permission"), "{roles}");
        assert!(
            row["kind"].is_string() && row.get("data").is_some(),
            "{roles}"
        );
    }

    let base = format!(
        "/v1/events?registration_id={EVENTS_REGISTRATION}&include=data,raw&order=asc&page_size=2"
    );
    let first = assert_events_count_only_on_opt_in(&database, &base).await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .context("the registration has more than one page")?;
    let second =
        assert_events_count_only_on_opt_in(&database, &format!("{base}&cursor={cursor}")).await?;
    assert!(
        second["data"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty()),
        "{second}"
    );

    database.cleanup().await
}
