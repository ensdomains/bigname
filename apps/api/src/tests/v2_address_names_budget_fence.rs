// The role-summary grant read is not pinned to the captured publication. These tests hold a
// request between membership selection and that read, so a publication change lands mid-request.

const ADDRESS_NAME_FENCE_ROUTES: [&str; 3] = [
    "q=alpha",
    "relation=resolves_to&q=alpha",
    "relation=resolves_to&coin_type=evm&q=alpha",
];

/// Runs one `include=role_summary` request paused before its grant read. `republish_with`
/// replaces the resource's grants and advances the Project publication during the pause.
async fn address_name_role_summary_response_across_pause(
    database: &TestDatabase,
    uri: &str,
    resource: Uuid,
    republish_with: Option<usize>,
) -> Result<Response> {
    let (_guard, control) =
        crate::v2::address_names_grant_read_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state();
    let uri = uri.to_owned();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    if let Some(count) = republish_with {
        seed_address_name_budget_grants(database, resource, count).await?;
        let advanced = sqlx::query(
            "UPDATE chain_phase_state
             SET updated_at = clock_timestamp()
             WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
        )
        .execute(&database.lookup_pool)
        .await?;
        assert_eq!(advanced.rows_affected(), 1, "fixture must publish Project");
    }
    control.resume().await;

    request_task
        .await
        .context("address-name role-summary request task panicked")?
        .context("address-name role-summary request failed")
}

async fn address_name_fence_resource(database: &TestDatabase, uri: &str) -> Result<Uuid> {
    let plain = v2_address_names_payload_for_database(database, uri).await?;
    let id = plain["data"][0]["permission_resource_id"]
        .as_str()
        .with_context(|| format!("first row must carry a permission handle: {plain}"))?;
    Ok(Uuid::parse_str(id)?)
}

async fn assert_overflow_after_publication_change_is_stale(route: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    let uri = format!("/v1/addresses/{V2_ADDRESS}/names?{route}&page_size=200");
    let resource = address_name_fence_resource(&database, &uri).await?;
    // The captured publication is within budget; the next one takes the page over it.
    seed_address_name_budget_grants(&database, resource, 1000).await?;
    let response = address_name_role_summary_response_across_pause(
        &database,
        &format!("{uri}&include=role_summary"),
        resource,
        Some(1001),
    )
    .await?;
    let status = response.status();
    let error: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{route}: {error}");
    assert_eq!(error["error"]["code"], json!("stale"), "{route}");
    assert!(error.get("data").is_none(), "{route}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_grant_budget_overflow_after_publication_change_is_stale() -> Result<()> {
    assert_overflow_after_publication_change_is_stale(ADDRESS_NAME_FENCE_ROUTES[0]).await
}

#[tokio::test]
async fn v2_resolves_to_grant_budget_overflow_after_publication_change_is_stale() -> Result<()> {
    assert_overflow_after_publication_change_is_stale(ADDRESS_NAME_FENCE_ROUTES[1]).await
}

#[tokio::test]
async fn v2_resolves_to_evm_grant_budget_overflow_after_publication_change_is_stale() -> Result<()>
{
    assert_overflow_after_publication_change_is_stale(ADDRESS_NAME_FENCE_ROUTES[2]).await
}

#[tokio::test]
async fn v2_address_names_grant_budget_boundaries_hold_on_a_stable_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_v2_resolves_to_records(&database).await?;
    for route in ADDRESS_NAME_FENCE_ROUTES {
        let uri = format!("/v1/addresses/{V2_ADDRESS}/names?{route}&page_size=200");
        let resource = address_name_fence_resource(&database, &uri).await?;
        for count in [1000, 1001] {
            seed_address_name_budget_grants(&database, resource, count).await?;
            let response = address_name_role_summary_response_across_pause(
                &database,
                &format!("{uri}&include=role_summary"),
                resource,
                None,
            )
            .await?;
            let status = response.status();
            let payload: Value = read_json(response).await?;
            if count == 1000 {
                assert_eq!(status, StatusCode::OK, "{route}: {payload}");
                assert_eq!(
                    address_name_inline_grants(&payload["data"][0]).len(),
                    count,
                    "{route}"
                );
            } else {
                assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{route}: {payload}");
                assert_eq!(payload["error"]["code"], json!("unsupported"), "{route}");
            }
        }
    }
    database.cleanup().await
}
