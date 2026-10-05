//! The real route chooses its source at the request snapshot, using admission's old token.
use super::*;
use bigname_storage::history_anchor_read_test_hooks::{self, HistoryReadHookPoint};

#[tokio::test]
async fn address_history_catalogue_count_paths_keep_exact_results_and_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let events = seed_names(&database, 11).await?;
    let expected_grants: Vec<_> = events
        .iter()
        .filter(|(_, id)| id.ends_with("RegistrationGranted"))
        .map(|(_, id)| hkw_id(id))
        .collect();
    for include in ["", "&include=total_count"] {
        let uri = format!(
            "/v1/addresses/{ADDRESS}/history?relation=owner&kind=RegistrationGranted&order=asc&page_size=1{include}"
        );
        let (first, stats) = measured(&database, &uri, 1).await?;
        assert_eq!(stats.counters.get("catalogue_direct_count"), Some(&1));
        assert_eq!(first["page"]["total_count"], json!(11));
        assert_eq!(hk_ids(&first), expected_grants[..1]);
        let cursor = first["page"]["next_cursor"].as_str().unwrap();
        let (next, stats) = measured(&database, &format!("{uri}&cursor={cursor}"), 1).await?;
        assert_eq!(stats.counters.get("catalogue_direct_count"), Some(&1));
        assert_eq!(next["page"]["total_count"], json!(11));
        assert_eq!(hk_ids(&next), expected_grants[1..2]);
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
    assert_eq!(records["page"]["total_count"], json!(11));
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
    for (point, change, path) in [
        (
            HistoryReadHookPoint::AfterAnchors,
            "advance",
            "captured_publication_advanced",
        ),
        (
            HistoryReadHookPoint::AfterPublicationCheck,
            "advance",
            "catalogue",
        ),
        (
            HistoryReadHookPoint::AfterAnchors,
            "same-block-rebuild",
            "captured_publication_advanced",
        ),
        (
            HistoryReadHookPoint::AfterAnchors,
            "reset",
            "catalogue_unavailable",
        ),
        (
            HistoryReadHookPoint::AfterAnchors,
            "missing-marker",
            "catalogue_unavailable",
        ),
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_names(&database, 2).await?;
        let uri = format!(
            "/v1/addresses/{ADDRESS}/history?relation=owner&page_size=200&include=total_count"
        );
        let (before, before_stats) = measured(&database, &uri, 200).await?;
        assert_eq!(before_stats.counters.get("catalogue"), Some(&1));
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
        if matches!(change, "reset" | "missing-marker") {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["code"], "stale");
        } else {
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(
                body["data"], before["data"],
                "the publication gap changed the bounded page"
            );
            assert_eq!(body["page"]["total_count"], before["page"]["total_count"]);
            if path == "catalogue" {
                assert_eq!(receipt["captured"], receipt["observed"]);
            } else {
                assert_ne!(receipt["captured"], receipt["observed"]);
            }
            let (fresh, fresh_stats) = measured(&database, &uri, 200).await?;
            assert_eq!(fresh_stats.counters.get("catalogue"), Some(&1));
            if change == "advance" {
                assert!(hk_ids(&fresh).contains(&hkw_id("catalogue-next-publication")));
                assert_eq!(fresh["page"]["total_count"], json!(9));
            }
        }
        database.cleanup().await?;
    }
    Ok(())
}
