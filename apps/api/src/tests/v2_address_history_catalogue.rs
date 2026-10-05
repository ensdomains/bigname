//! The real route admits and reads one publication on the same page transaction.
use super::*;
use bigname_storage::history_anchor_read_test_hooks::{self, HistoryReadHookPoint};

fn assert_no_count(stats: &bigname_storage::AddressHistoryWorkingSet) {
    for key in [
        "catalogue_direct_count",
        "catalogue_proof_batches",
        "address_history_exact_cursor_micros",
        "address_history_count_walk_micros",
    ] {
        assert!(!stats.counters.contains_key(key), "{key}: {stats:?}");
    }
}

#[tokio::test]
async fn address_history_catalogue_count_paths_keep_exact_results_and_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let events = seed_names(&database, 11).await?;
    let expected_grants: Vec<_> = events
        .iter()
        .filter(|(_, id)| id.ends_with("RegistrationGranted"))
        .map(|(_, id)| hkw_id(id))
        .collect();
    let base = format!(
        "/v1/addresses/{ADDRESS}/history?relation=owner&kind=RegistrationGranted&order=asc&page_size=1"
    );
    let (uncounted, stats) = measured(&database, &base, 1).await?;
    assert_no_count(&stats);
    assert_eq!(uncounted["page"]["total_count"], Value::Null);
    let (counted, stats) = measured(&database, &format!("{base}&include=total_count"), 1).await?;
    assert_eq!(stats.counters.get("catalogue_direct_count"), Some(&1));
    assert_eq!(counted["page"]["total_count"], json!(11));
    assert_eq!(uncounted["data"], counted["data"]);
    assert_eq!(
        uncounted["page"]["next_cursor"],
        counted["page"]["next_cursor"]
    );
    assert_eq!(uncounted["page"]["has_more"], counted["page"]["has_more"]);
    assert_eq!(hk_ids(&uncounted), expected_grants[..1]);
    // The same public cursor can continue with or without an explicit total.
    let cursor = uncounted["page"]["next_cursor"].as_str().unwrap();
    for include in ["", "&include=total_count"] {
        let (next, stats) =
            measured(&database, &format!("{base}&cursor={cursor}{include}"), 1).await?;
        assert_eq!(
            next["page"]["total_count"],
            if include.is_empty() {
                Value::Null
            } else {
                json!(11)
            }
        );
        assert_eq!(hk_ids(&next), expected_grants[1..2]);
        if include.is_empty() {
            assert_no_count(&stats);
        }
    }
    for include in ["", "&include=total_count"] {
        let (empty, stats) = measured(
            &database,
            &format!("/v1/addresses/{ADDRESS}/history?relation=owner&record_key=missing{include}"),
            50,
        )
        .await?;
        assert_eq!(empty["data"], json!([]));
        assert_eq!(empty["page"]["has_more"], json!(false));
        assert_eq!(empty["page"]["next_cursor"], Value::Null);
        assert_eq!(
            empty["page"]["total_count"],
            if include.is_empty() {
                Value::Null
            } else {
                json!(0)
            }
        );
        if include.is_empty() {
            assert_no_count(&stats);
        }
    }
    let uri = format!(
        "/v1/addresses/{ADDRESS}/history?relation=owner&record_key=addr:60&order=desc&page_size=2"
    );
    let (records, stats) = measured(&database, &uri, 2).await?;
    let expected_records: Vec<_> = events
        .iter()
        .rev()
        .filter(|(_, id)| id.ends_with("RecordChanged"))
        .take(2)
        .map(|(_, id)| hkw_id(id))
        .collect();
    assert_eq!(hk_ids(&records), expected_records);
    assert_eq!(records["page"]["total_count"], Value::Null);
    assert_no_count(&stats);
    assert!(!stats.counters.contains_key("catalogue_direct_count"));
    let uri = format!(
        "/v1/addresses/{ADDRESS}/history?relation=owner&order=asc&page_size=200&include=total_count"
    );
    let (mixed, stats) = measured(&database, &uri, 200).await?;
    assert_eq!(mixed["page"]["total_count"], json!(44));
    assert_eq!(
        hk_ids(&mixed),
        events.iter().map(|(_, id)| hkw_id(id)).collect::<Vec<_>>()
    );
    assert!(
        stats
            .counters
            .contains_key("address_history_exact_cursor_micros")
    );
    database.cleanup().await
}

#[tokio::test]
async fn address_history_new_owner_reads_old_shared_history_without_backfill() -> Result<()> {
    const NEW_OWNER: &str = "0x000000000000000000000000000000000000d235";
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 2).await?;
    let uri = format!(
        "/v1/addresses/{NEW_OWNER}/history?relation=owner&order=asc&page_size=200&include=total_count"
    );
    let (before, _) = measured(&database, &uri, 200).await?;
    assert!(hk_ids(&before).is_empty());
    let shared_counts = |pool: PgPool| async move {
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM project_history_source),
                    (SELECT count(*) FROM project_history_source_edge)",
        )
        .fetch_one(&pool)
        .await
    };
    let old_shared = shared_counts(database.pool.clone()).await?;
    publish_bounded_membership_at(&database, 241).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "walk-0000.eth");
    let event = address_fixture_event(
        "catalogue-new-owner",
        Some(&logical),
        Some(Uuid::from_u128(0x235000)),
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        241,
        "0xhistory241",
        0,
        json!({"from":ADDRESS,"to":NEW_OWNER}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    let retained = |pool: PgPool| async move {
        sqlx::query_scalar::<_, String>(
            "SELECT md5(string_agg(to_jsonb(event)::text, E'\\n' ORDER BY event_identity))
             FROM normalized_events event",
        )
        .fetch_one(&pool)
        .await
    };
    let before_project = retained(database.pool.clone()).await?;
    publish_test_families(&database, 241).await?;
    assert_eq!(retained(database.pool.clone()).await?, before_project);
    assert_eq!(shared_counts(database.pool.clone()).await?, old_shared);
    let new_anchors: i64 =
        sqlx::query_scalar("SELECT count(*) FROM project_address_history_anchor WHERE address=$1")
            .bind(NEW_OWNER)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(
        new_anchors, 2,
        "one name and one resource membership share old sources"
    );
    let (after, stats) = measured(&database, &uri, 200).await?;
    assert_eq!(stats.counters.get("catalogue"), Some(&1));
    assert_eq!(after["page"]["total_count"], json!(5));
    let mut expected = [
        "RegistrationGranted",
        "AuthorityTransferred",
        "ResolverChanged",
        "RecordChanged",
    ]
    .map(|kind| hkw_id(&format!("relation-235000-{kind}")))
    .to_vec();
    expected.push(hkw_id("catalogue-new-owner"));
    assert_eq!(hk_ids(&after), expected);
    database.cleanup().await
}

#[tokio::test]
async fn address_history_catalogue_publication_change_uses_original_bounds() -> Result<()> {
    check_publication_change("&include=total_count").await
}

#[tokio::test]
async fn address_history_catalogue_publication_change_without_count_uses_original_bounds()
-> Result<()> {
    check_publication_change("").await
}

async fn check_publication_change(include: &str) -> Result<()> {
    for (point, change, path) in [
        (HistoryReadHookPoint::AfterAnchors, "advance", "catalogue"),
        (
            HistoryReadHookPoint::AfterPublicationCheck,
            "advance",
            "catalogue",
        ),
        (
            HistoryReadHookPoint::AfterAnchors,
            "same-block-rebuild",
            "catalogue",
        ),
        (HistoryReadHookPoint::AfterAnchors, "reset", "catalogue"),
        (
            HistoryReadHookPoint::AfterAnchors,
            "missing-marker",
            "catalogue",
        ),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_names(&database, 2).await?;
        let uri = format!("/v1/addresses/{ADDRESS}/history?relation=owner&page_size=200{include}");
        let (before, before_stats) = measured(&database, &uri, 200).await?;
        assert_eq!(before_stats.counters.get("catalogue"), Some(&1));
        if include.is_empty() {
            assert_no_count(&before_stats);
        }
        let (_guard, control) =
            history_anchor_read_test_hooks::install(&database.lookup_pool, point).await?;
        let state = database.app_state_with_public_namespaces(&["ens"]);
        let stats = Arc::new(Mutex::new(
            bigname_storage::AddressHistoryWorkingSet::default(),
        ));
        let request_stats = stats.clone();
        let request_uri = uri.clone();
        let request = tokio::spawn(async move {
            bigname_storage::with_address_history_working_set(
                request_stats,
                app_router(state).oneshot(
                    Request::builder()
                        .uri(request_uri)
                        .body(Body::empty())
                        .unwrap(),
                ),
            )
            .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            control.wait_until_reached(),
        )
        .await
        .context("address history did not reach the publication seam")?;
        match change {
            "advance" => {
                // This extra row belongs to the same address, but is above admission's bound.
                publish_bounded_membership_at(&database, 241).await?;
                let logical = bigname_storage::logical_name_id_for_name("ens", "walk-0000.eth");
                let event = address_fixture_event(
                    "catalogue-next-publication",
                    Some(&logical),
                    Some(Uuid::from_u128(0x235000)),
                    "RegistrationRenewed",
                    "ens_v1_registrar_l1",
                    241,
                    "0xhistory241",
                    0,
                    json!({"expiry":1_900_000_000}),
                );
                bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
                publish_test_families(&database, 241).await?;
            }
            "same-block-rebuild" => {
                rebuild_fixture_families(&database.pool, "ethereum-mainnet", BLOCK, HASH).await?
            }
            "reset" => reset_family_families(&database).await?,
            "missing-marker" => {
                sqlx::query("DELETE FROM bigname_phase.project_history_catalogue_marker")
                    .execute(&database.pool)
                    .await?;
            }
            _ => unreachable!(),
        }
        control.resume().await;
        let response = request.await??;
        let status = response.status();
        let body: Value = read_json(response).await?;
        let stats = stats.lock().unwrap().clone();
        assert_eq!(
            stats.counters.get(path),
            Some(&1),
            "{change}/{point:?}: {stats:?}"
        );
        let receipt = stats
            .catalogue_receipt
            .as_ref()
            .expect("source selection receipt");
        assert_eq!(receipt["path"], path);
        assert!(receipt["admission_to_snapshot_ms"].as_f64().is_some());
        assert_eq!(status, StatusCode::OK, "{body}");
        if include.is_empty() {
            assert_no_count(&stats);
        }
        assert_eq!(
            body["data"], before["data"],
            "the pinned publication changed the page"
        );
        assert_eq!(body["page"], before["page"]);
        assert_eq!(body["meta"], before["meta"]);
        assert_eq!(receipt["captured"], receipt["observed"]);
        if matches!(change, "reset" | "missing-marker") {
            // The in-flight snapshot was valid. A subsequent request sees the missing state.
            let response = app_router(database.app_state_with_public_namespaces(&["ens"]))
                .oneshot(Request::builder().uri(&uri).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body: Value = read_json(response).await?;
            assert_eq!(body["error"]["code"], "stale");
        } else {
            let (fresh, fresh_stats) = measured(&database, &uri, 200).await?;
            assert_eq!(fresh_stats.counters.get("catalogue"), Some(&1));
            if change == "advance" {
                assert!(hk_ids(&fresh).contains(&hkw_id("catalogue-next-publication")));
                assert_eq!(
                    fresh["page"]["total_count"],
                    if include.is_empty() {
                        Value::Null
                    } else {
                        json!(9)
                    }
                );
            }
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn address_history_refuses_missing_publication_before_snapshot_is_pinned() -> Result<()> {
    use crate::v2::collection_snapshot::finish_test_hooks::{Stage, install_at};
    for reset in [false, true] {
        let database = TestDatabase::new_migrated().await?;
        seed_names(&database, 2).await?;
        let (_guard, control) = install_at(&database.lookup_pool, Stage::BeforeRead).await?;
        let state = database.app_state_with_public_namespaces(&["ens"]);
        let request = tokio::spawn(async move {
            app_router(state)
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{ADDRESS}/history?relation=owner&page_size=1"
                        ))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            control.wait_until_reached(),
        )
        .await?;
        if reset {
            reset_family_families(&database).await?;
        } else {
            sqlx::query("DELETE FROM bigname_phase.project_history_catalogue_marker")
                .execute(&database.pool)
                .await?;
        }
        control.resume().await;
        let response = request.await??;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], "stale");
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn address_history_snapshot_reuses_one_pool_connection() -> Result<()> {
    use std::str::FromStr;
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, 2).await?;
    let config = database.database_config(1)?;
    let options =
        sqlx::postgres::PgConnectOptions::from_str(config.database_url.as_deref().unwrap())?
            .options([("search_path", "bigname_phase".to_owned())]);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let state = AppState::new_with_rpc_urls(pool.clone(), bigname_lookup::ChainRpcUrls::default())
        .with_public_namespaces_for_test(["ens"]);
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        app_router(state).oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{ADDRESS}/history?relation=owner&page_size=1&include=data"
                ))
                .body(Body::empty())?,
        ),
    )
    .await??;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = read_json(response).await?;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["page"]["total_count"], Value::Null);
    pool.close().await;
    database.cleanup().await
}
