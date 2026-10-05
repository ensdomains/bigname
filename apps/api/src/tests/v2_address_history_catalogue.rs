//! The real route chooses its source at the request snapshot, using admission's old token.
use super::*;
use bigname_storage::history_anchor_read_test_hooks::{self, HistoryReadHookPoint};

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
