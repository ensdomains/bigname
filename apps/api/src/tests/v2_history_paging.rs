#[tokio::test]
async fn v2_history_routes_page_over_product_visible_rows() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    seed_v2_history_blocks(&database, 112..=114).await?;
    let internal = (112..=114)
        .map(|block| {
            v2_history_event(
                &format!("history-surface-bound-{block}"),
                Some("ens:history.eth"),
                None,
                "SurfaceBound",
                block,
            )
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &internal).await?;

    let routes = [
        "/v2/events?name=history.eth".to_owned(),
        "/v2/names/history.eth/history".to_owned(),
        format!("/v2/addresses/{ADDRESS}/history"),
    ];
    for route in &routes {
        assert_product_history_pages(&database, route).await?;
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_history_routes_treat_internal_only_matches_as_no_product_matches() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_name(
        &database,
        "ens:quiet.eth",
        "Quiet.eth",
        "node:quiet.eth",
        80,
        Uuid::from_u128(0x7300),
        Uuid::from_u128(0x8300),
        Uuid::from_u128(0x9300),
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=122).await?;
    let internal = (120..=122)
        .map(|block| {
            v2_history_event(
                &format!("quiet-surface-bound-{block}"),
                Some("ens:quiet.eth"),
                None,
                "SurfaceBound",
                block,
            )
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &internal).await?;

    let routes = [
        "/v2/events?name=quiet.eth&page_size=2".to_owned(),
        "/v2/names/quiet.eth/history?page_size=2".to_owned(),
        format!("/v2/addresses/{ADDRESS}/history?page_size=2"),
    ];
    for route in &routes {
        let payload = v2_history_payload_for_database(&database, route).await?;
        assert_eq!(payload["data"], json!([]), "route: {route}");
        assert_eq!(payload["page"]["has_more"], json!(false), "route: {route}");
        assert_eq!(payload["page"]["next_cursor"], Value::Null, "route: {route}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_events_rejects_foreign_kind_anchor_for_explicit_type() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let anchor = bigname_storage::HistoryCursor {
        normalized_event_id: sqlx::query_scalar(
            "SELECT normalized_event_id FROM bigname_phase.normalized_events WHERE event_identity = 'history-renewal'",
        )
        .fetch_one(&database.pool)
        .await?,
        event_identity: "history-renewal".to_owned(),
    };
    let cursor = crate::v2::encode(&crate::v2::events_cursor_payload(
        &anchor,
        &std::collections::BTreeMap::from([
            ("name".to_owned(), bigname_storage::logical_name_id_for_name("ens", "history.eth")),
            ("namespace".to_owned(), "ens".to_owned()),
            ("type".to_owned(), "registration".to_owned()),
        ]),
    ));
    let response = v2_history_response_for_database(
        &database,
        &format!("/v2/events?name=history.eth&type=registration&cursor={cursor}"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(read_json::<Value>(response).await?["error"]["code"], json!("invalid_input"));
    database.cleanup().await
}

#[tokio::test]
async fn v2_history_routes_continue_from_legacy_non_product_cursor() -> Result<()> {
    const ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
    const EVENT: &str = "history-surface-bound";
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", "history.eth");
    let anchor = bigname_storage::HistoryCursor {
        normalized_event_id: sqlx::query_scalar(
            "SELECT normalized_event_id FROM bigname_phase.normalized_events WHERE event_identity = $1",
        )
        .bind(EVENT)
        .fetch_one(&database.pool)
        .await?,
        event_identity: EVENT.to_owned(),
    };
    let address_binding = crate::v2::AddressHistoryCursorBinding {
        address: ADDRESS,
        namespace: "ens",
        relation: None,
        scope: crate::v2::HistoryScope::Both,
    };
    let routes = [
        (
            "/v2/events?name=history.eth&page_size=2",
            crate::v2::encode(&crate::v2::events_cursor_payload(
                &anchor,
                &std::collections::BTreeMap::from([
                    ("name".to_owned(), logical_name_id.clone()),
                    ("namespace".to_owned(), "ens".to_owned()),
                ]),
            )),
            vec!["renewal", "expiry"],
        ),
        (
            "/v2/names/history.eth/history?page_size=2",
            crate::v2::encode(&crate::v2::history_cursor_payload(
                &anchor,
                "ens",
                &logical_name_id,
                crate::v2::HistoryScope::Both,
            )),
            vec!["renewal", "expiry"],
        ),
        (
            "/v2/addresses/0x00000000000000000000000000000000000000cc/history?page_size=2",
            crate::v2::encode(&crate::v2::address_history_cursor_payload(
                &anchor,
                &address_binding,
            )),
            vec!["record", "authority"],
        ),
    ];
    sqlx::query("UPDATE bigname_phase.normalized_events SET normalized_event_id = DEFAULT")
        .execute(&database.pool)
        .await?;
    let mut continued = Vec::new();
    for (route, cursor, expected) in &routes {
        let response =
            v2_history_response_for_database(&database, &format!("{route}&cursor={cursor}"))
                .await?;
        let status = response.status();
        continued.push((*route, status, expected, read_json::<Value>(response).await?));
    }
    assert!(
        continued
            .iter()
            .all(|(_, status, _, _)| *status == StatusCode::OK),
        "legacy cursor continuation: {continued:#?}"
    );
    for (_, _, expected, payload) in &continued {
        assert_eq!(
            history_types(payload["data"].as_array().context("history data")?),
            expected.as_slice()
        );
    }

    sqlx::query("DELETE FROM bigname_phase.normalized_events WHERE event_identity = $1")
        .bind(EVENT)
        .execute(&database.pool)
        .await?;
    let response = v2_history_response_for_database(
        &database,
        &format!("{}&cursor={}", routes[0].0, routes[0].1),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await
}

async fn assert_product_history_pages(database: &TestDatabase, route: &str) -> Result<()> {
    let separator = if route.contains('?') { '&' } else { '?' };
    let baseline = v2_history_payload_for_database(
        database,
        &format!("{route}{separator}page_size=200"),
    )
    .await?;
    let expected = baseline["data"].as_array().expect("baseline data").clone();
    let mut actual = Vec::new();
    let mut cursor = None;
    for _ in 0..16 {
        let cursor_query = cursor
            .as_ref()
            .map(|cursor| format!("&cursor={cursor}"))
            .unwrap_or_default();
        let payload = v2_history_payload_for_database(
            database,
            &format!("{route}{separator}page_size=2{cursor_query}"),
        )
        .await?;
        let rows = payload["data"].as_array().expect("page data");
        let has_more = payload["page"]["has_more"].as_bool().expect("has_more");
        if has_more {
            assert_eq!(rows.len(), 2, "short nonterminal page: {route}");
        } else {
            assert_eq!(payload["page"]["next_cursor"], Value::Null, "route: {route}");
        }
        for row in rows {
            assert!(!actual.contains(row), "overlapping page row: {route}");
            actual.push(row.clone());
        }
        if !has_more {
            assert_eq!(actual, expected, "paged rows: {route}");
            return Ok(());
        }
        cursor = Some(
            payload["page"]["next_cursor"]
                .as_str()
                .context("nonterminal page cursor")?
                .to_owned(),
        );
    }
    anyhow::bail!("history pagination did not terminate: {route}")
}
