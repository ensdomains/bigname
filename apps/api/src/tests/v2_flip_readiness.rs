async fn seed_family_status_fixture(database: &TestDatabase) -> Result<AppState> {
    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES
            ('ethereum-mainnet', '0xflip-119', 119, '2026-08-06T00:00:08Z', 'canonical'),
            ('ethereum-mainnet', '0xflip-120', 120, '2026-08-06T00:00:20Z', 'canonical');
        INSERT INTO bigname_phase.chain_heads (chain_id, latest_block_hash, latest_block_number)
        VALUES ('ethereum-mainnet', '0xflip-120', 120);
        INSERT INTO bigname_phase.service_heartbeats (
            service_name, instance_id, chain_id, phase_name, started_at, heartbeat_at
        ) VALUES (
            'phase-runner', 'flip-status-test', 'ethereum-mainnet', 'live', now(), now()
        );
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_phase_state (
             chain_id, phase_name, phase_status, current_block_number, current_block_hash,
             target_block_number, target_block_hash, input_content_hash, started_at, finished_at
         ) VALUES (
             'ethereum-mainnet', 'project', 'completed', 120, '0xflip-120', 120, '0xflip-120',
             $1, now(), now()
         )",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;
    publish_test_families_on(&database.lookup_pool, "ethereum-mainnet", 119).await?;
    let chain_rpc_urls = bigname_lookup::ChainRpcUrls::from_entries(&[
        "ethereum-mainnet=http://rpc.test".to_owned(),
    ])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    state
        .status_freshness
        .seed_success(
            "ethereum-mainnet",
            120,
            sqlx::types::time::OffsetDateTime::now_utc(),
        )
        .await;
    Ok(state)
}

async fn family_status_chain(state: &AppState) -> Result<Value> {
    let payload = status_payload(state.clone()).await?;
    Ok(payload["data"]["chains"]["1"].clone())
}
#[tokio::test]
async fn v2_status_reads_the_family_marker_as_the_indexed_block() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = seed_family_status_fixture(&database).await?;

    let families = family_status_chain(&state).await?;
    assert_eq!(families["indexed_block"], json!(119));
    assert_eq!(families["lag_blocks"], json!(1));
    assert_eq!(families["lag_seconds"], json!(12));
    assert_eq!(families["status"], json!("ready"));

    database.cleanup().await
}
#[tokio::test]
async fn v2_status_degrades_during_a_family_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = seed_family_status_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.lookup_pool)
        .await?;

    let families = family_status_chain(&state).await?;
    assert_eq!(families["status"], json!("degraded"));
    assert_eq!(families["indexed_block"], json!(119));

    database.cleanup().await
}
#[tokio::test]
async fn v2_status_has_no_indexed_block_without_a_family_marker() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = seed_family_status_fixture(&database).await?;
    sqlx::query("DELETE FROM bigname_phase.project_family_marker")
        .execute(&database.lookup_pool)
        .await?;

    let families = family_status_chain(&state).await?;
    assert_eq!(families["indexed_block"], Value::Null);
    assert_ne!(families["status"], json!("ready"));

    database.cleanup().await
}

/// The verified-records fixture of
/// `v2_verified_records_return_conflict_when_project_generation_changes_during_rpc`, with a family
/// marker in `state` on the Project row's publication.
async fn seed_family_verified_records_fixture(database: &TestDatabase, state: &str) -> Result<()> {
    database.initialize_lookup_schema().await?;
    let execution_block_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = $1")
        .bind(state)
        .execute(&lookup_pool)
        .await?;
    Ok(())
}

async fn family_verified_records(database: &TestDatabase) -> Result<(StatusCode, Value, usize)> {
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
            "0x0000000000000000000000000000000000000e0e",
        )])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    rpc_handle.abort();
    let requests = match rpc_handle.await {
        Ok(requests) => requests?.len(),
        Err(error) if error.is_cancelled() => 0,
        Err(error) => return Err(error.into()),
    };
    Ok((status, payload, requests))
}
#[tokio::test]
async fn v2_verified_records_answer_stale_during_a_family_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_verified_records_fixture(&database, "bootstrap_pending").await?;

    let (status, payload, requests) = family_verified_records(&database).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(requests, 0, "a refused read calls no provider");

    database.cleanup().await
}
#[tokio::test]
async fn v2_status_lag_is_unknown_during_an_interpret_redo() -> Result<()> {
    assert_lag_unknown_during_redo("interpret").await
}

#[tokio::test]
async fn v2_status_lag_is_unknown_during_a_project_redo() -> Result<()> {
    assert_lag_unknown_during_redo("project").await
}

async fn assert_lag_unknown_during_redo(phase: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = seed_family_status_fixture(&database).await?;
    publish_test_families_on(&database.lookup_pool, "ethereum-mainnet", 120).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_phase_state (
             chain_id, phase_name, phase_status, current_block_number, current_block_hash,
             target_block_number, target_block_hash, input_content_hash, started_at, finished_at
         ) VALUES (
             'ethereum-mainnet', 'interpret', 'completed', 120, '0xflip-120', 120, '0xflip-120',
             $1, now(), now()
         )",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;
    {
        let caught_up = family_status_chain(&state).await?;
        assert_eq!(caught_up["lag_blocks"], json!(0), "family publication");
        assert_eq!(caught_up["status"], json!("ready"), "family publication");
    }

    // The redo marker as the runner's redo begin writes it (support.rs,
    // `simulate_interpret_redo_begin`), on the named phase's row. Only Interpret may
    // recompute flags; a Project redo is a plain redo.
    let mode = if phase == "interpret" {
        "recompute_flags"
    } else {
        "redo"
    };
    let started = sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET phase_status = 'running',
             redo_in_progress = true,
             redo_attempt_generation = redo_attempt_generation + 1,
             redo_mode = $2,
             redo_previous_phase_status = phase_status,
             redo_previous_last_error = last_error,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 0,
             redo_to_block_number = current_block_number,
             started_at = now(),
             finished_at = NULL,
             updated_at = now()
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = $1",
    )
    .bind(phase)
    .bind(mode)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(started.rows_affected(), 1);
    {
        let chain = family_status_chain(&state).await?;
        assert_eq!(chain["indexed_block"], json!(120), "{phase}");
        assert_eq!(chain["lag_blocks"], Value::Null, "{phase}");
        assert_eq!(chain["lag_seconds"], Value::Null, "{phase}");
        assert_eq!(chain["status"], json!("degraded"), "{phase}");
    }

    database.cleanup().await
}
