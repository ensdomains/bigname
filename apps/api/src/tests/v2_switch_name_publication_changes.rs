// Exercise the publication changes that can land after the route captures its fence.

async fn get_with_family_change_after_fence<F, Fut>(
    database: &TestDatabase,
    uri: &str,
    change: F,
) -> Result<(StatusCode, Value)>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let request = bigname_storage::families::name::seams::with_pause_before_snapshot(
        std::sync::Arc::clone(&reached),
        std::sync::Arc::clone(&resume),
        with_serve_on(database, uri),
    );
    tokio::pin!(request);
    let mut changed = false;
    loop {
        tokio::select! {
            response = &mut request => {
                anyhow::ensure!(changed, "{uri}: no composed read ran after the fence");
                return response;
            }
            () = reached.notified() => {
                if !changed {
                    change().await?;
                    changed = true;
                }
                resume.notify_one();
            }
        }
    }
}

/// Run the production reset transaction and stop before its first replay block. This clears
/// the marker's block/hash and every family table, unlike changing only its state.
async fn reset_switch_families(database: &TestDatabase) -> Result<()> {
    let token = bigname_project::families::input_token(&database.pool, SWITCH_CHAIN).await?;
    let mut options = bigname_project::families::FamilyOptions::new(
        bigname_content_hash::INTERPRETER_CONTENT_HASH,
    );
    options.max_blocks_per_run = 0;
    let outcome = bigname_project::families::apply(
        &database.pool,
        SWITCH_CHAIN,
        &bigname_project::Marker {
            number: 240,
            hash: "0xhistory240".to_owned(),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &options,
    )
    .await?;
    anyhow::ensure!(
        outcome.reset
            && outcome.budget_exhausted
            && outcome.marker.is_none()
           ,
        "reset before replay: {outcome:?}"
    );
    for table in bigname_project::families::family_tables() {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM bigname_phase.{table} WHERE chain_id = $1"
        ))
        .bind(SWITCH_CHAIN)
        .fetch_one(&database.pool)
        .await?;
        anyhow::ensure!(count == 0, "reset left {count} rows in {table}");
    }
    Ok(())
}

#[tokio::test]
async fn v2_search_refuses_a_real_family_reset_after_its_fence() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let (status, before) = with_serve_on(&database, SWITCH_SEARCH).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert_eq!(before["data"].as_array().map(Vec::len), Some(2));
    let (status, body) = get_with_family_change_after_fence(&database, SWITCH_SEARCH, || {
        reset_switch_families(&database)
    })
    .await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_resource_permissions_refuse_a_real_family_reset_after_their_fence() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_routes_fixture(&database).await?;
    let uri = format!("/v1/permissions?address={SWITCH_BOB}&namespace=ens");
    let (status, before) = with_serve_on(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert!(
        before["data"]
            .as_array()
            .is_some_and(|rows| { rows.iter().any(|row| row["name"] == json!("alpha.eth")) }),
        "{before:#}"
    );
    let (status, body) =
        get_with_family_change_after_fence(&database, &uri, || reset_switch_families(&database))
            .await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_missing_subnames_parent_refuses_a_replacement_live_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let uri = "/v1/names/alpha.eth/subnames";
    let (status, before) = with_serve_on(&database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    let (status, body) = get_with_family_change_after_fence(&database, uri, || async {
        // The old-fork parent is no longer readable when the replacement publication is live.
        sqlx::query(
            "UPDATE bigname_phase.name_surfaces SET canonicality_state = 'orphaned'
                     WHERE raw_name = 'alpha.eth'",
        )
        .execute(&database.pool)
        .await?;
        publish_project_and_families(&database, 241).await
    })
    .await?;
    assert_eq!(
        (status, &body["error"]["code"]),
        (StatusCode::CONFLICT, &json!("stale")),
        "{body:#}"
    );
    database.cleanup().await
}

async fn assert_mixed_width_expiry_pages(order: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_events(&database).await?;
    let (later, later_resource) =
        seed_switch_name(&database, "later.eth", 0x5f1_0000, "ens_v1").await?;
    let (unbounded, unbounded_resource) =
        seed_switch_name(&database, "unbounded.eth", 0x5f2_0000, "ens_v1").await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            switch_event(
                "switch-later-grant",
                Some(&later),
                Some(later_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                206,
                0,
                json!({"authority_kind": "registrar", "registrant": SWITCH_ALICE,
                   "expiry": 10_000_000_000i64}),
            ),
            switch_event(
                "switch-unbounded-wrapper",
                Some(&unbounded),
                Some(unbounded_resource),
                "ExpiryChanged",
                "ens_v1_wrapper_l1",
                207,
                0,
                json!({"expiry": u64::MAX}),
            ),
        ],
    )
    .await?;
    publish_project_and_families(&database, 240).await?;
    let wrapper_expiry: String = sqlx::query_scalar(
        "SELECT expiry_seconds::text FROM bigname_phase.project_wrapper_state
         WHERE resource_id = $1",
    )
    .bind(unbounded_resource)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(wrapper_expiry, u64::MAX.to_string());
    let uri = format!(
        "/v1/names?namespace=ens&expires_after=2020-01-01T00:00:00Z\
                       &order={order}&page_size=1"
    );
    let pages = bigname_storage::families::name::seams::with_batch_size(
        1,
        assert_switch_differential_pages(&database, &uri),
    )
    .await?;
    let names: Vec<_> = pages
        .iter()
        .flat_map(|page| page["data"].as_array().into_iter().flatten())
        .map(|row| row["name"].clone())
        .collect();
    let mut expected = vec![json!("beta.eth"), json!("alpha.eth"), json!("later.eth")];
    if order == "desc" {
        expected.reverse();
    }
    assert_eq!(names, expected);
    database.cleanup().await
}

#[tokio::test]
async fn v2_expiry_numeric_order_crosses_mixed_width_batches_ascending() -> Result<()> {
    assert_mixed_width_expiry_pages("asc").await
}

#[tokio::test]
async fn v2_expiry_numeric_order_crosses_mixed_width_batches_descending() -> Result<()> {
    assert_mixed_width_expiry_pages("desc").await
}
