// History keeps its bounded walk across completed publications, but classification rows
// from a partial family rebuild must never be mistaken for definitive absence.
#[tokio::test]
async fn v2_history_classification_refuses_reset_after_anchor_capture() -> Result<()> {
    for republished in [false, true] {
        let database = TestDatabase::new_migrated().await?;
        seed_switch_routes_fixture(&database).await?;
        let uri = "/v1/names/alpha.eth/history";
        let (status, before) = with_serve_on(&database, uri).await?;
        assert_eq!(status, StatusCode::OK, "{before:#}");
        assert!(
            before["data"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
        );
        let (_guard, control) = bigname_storage::history_anchor_read_test_hooks::install(
            &database.lookup_pool,
            bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterAnchors,
        )
        .await?;
        let state = database.app_state_with_public_namespaces(&["ens"]);
        let request = tokio::spawn(async move {
            bigname_storage::publication_source::with_serve_from_families(true, async move {
                app_router(state)
                    .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                    .await
            })
            .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            control.wait_until_reached(),
        )
        .await
        .context("history did not capture anchors")?;
        reset_switch_families(&database).await?;
        if republished {
            publish_project_and_families(&database, 240).await?;
        }
        control.resume().await;
        let response = request.await??;
        let status = response.status();
        let body: Value = read_json(response).await?;
        if republished {
            assert_eq!(status, StatusCode::OK, "{body:#}");
            assert_eq!(
                body["data"], before["data"],
                "bounded history survives a completed replacement"
            );
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
            assert_eq!(body["error"]["code"], "stale", "{body:#}");
        }
        database.cleanup().await?;
    }
    Ok(())
}
