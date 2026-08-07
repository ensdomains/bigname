#[tokio::test]
async fn v2_status_and_startup_chain_discovery_read_phase_state() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp,
            canonicality_state
        ) VALUES
            ('ethereum-mainnet', '0xphase-finalized', 100,
             '2026-08-06T00:00:00Z', 'finalized'),
            ('ethereum-mainnet', '0xphase-safe', 110,
             '2026-08-06T00:00:10Z', 'safe'),
            ('ethereum-mainnet', '0xphase-projected', 115,
             '2026-08-06T00:00:15Z', 'canonical'),
            ('ethereum-mainnet', '0xphase-latest', 120,
             '2026-08-06T00:00:20Z', 'canonical')
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_heads (
            chain_id, latest_block_hash, latest_block_number,
            safe_block_hash, safe_block_number,
            finalized_block_hash, finalized_block_number
        ) VALUES (
            'ethereum-mainnet', '0xphase-latest', 120,
            '0xphase-safe', 110, '0xphase-finalized', 100
        )
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_phase_state (
            chain_id, phase_name, phase_status, current_block_number,
            current_block_hash, target_block_number, target_block_hash,
            input_content_hash, started_at
        ) VALUES (
            'ethereum-mainnet', 'project', 'running', 115,
            '0xphase-projected', 120, '0xphase-latest', $1, now()
        )
        "#,
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;

    assert_eq!(
        bigname_storage::load_phase_expected_status_chain_ids(&database.lookup_pool).await?,
        vec!["ethereum-mainnet"]
    );

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/status")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["pending_invalidation_count"], json!(0));
    assert_eq!(
        payload["data"]["pending_invalidation_count_capped"],
        json!(false)
    );
    assert_eq!(payload["data"]["dead_letter_count"], json!(0));
    assert_eq!(payload["data"]["chains"].as_object().unwrap().len(), 1);
    assert_eq!(payload["data"]["chains"]["1"]["latest_block"], json!(120));
    assert_eq!(payload["data"]["chains"]["1"]["indexed_block"], json!(115));
    assert_eq!(payload["data"]["chains"]["1"]["safe_block"], json!(110));
    assert_eq!(payload["data"]["chains"]["1"]["finalized_block"], json!(100));
    assert_eq!(payload["data"]["chains"]["1"]["lag_blocks"], json!(5));
    assert_eq!(payload["data"]["chains"]["1"]["lag_seconds"], json!(5));

    database.cleanup().await
}

#[tokio::test]
async fn v2_status_maps_phase_lifecycle_and_heartbeat_to_readiness() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp,
            canonicality_state
        ) VALUES
        (
            'ethereum-mainnet', '0xphase-projected', 115,
            '2026-08-06T00:00:15Z', 'finalized'
        ),
        (
            'ethereum-mainnet', '0xphase-head', 120,
            '2026-08-06T00:00:20Z', 'canonical'
        );
        INSERT INTO chain_heads (
            chain_id, latest_block_hash, latest_block_number,
            safe_block_hash, safe_block_number,
            finalized_block_hash, finalized_block_number
        ) VALUES (
            'ethereum-mainnet', '0xphase-head', 120,
            NULL, NULL, NULL, NULL
        );
        INSERT INTO chain_phase_state (
            chain_id, phase_name, phase_status, current_block_number,
            current_block_hash, target_block_number, target_block_hash,
            last_error, started_at, finished_at
        ) VALUES (
            'ethereum-mainnet', 'project', 'failed', 120,
            '0xphase-head', 120, '0xphase-head', 'project failed',
            now() - interval '1 second', now()
        );
        INSERT INTO service_heartbeats (
            service_name, instance_id, chain_id, phase_name,
            started_at, heartbeat_at
        ) VALUES (
            'phase-runner', 'status-test', 'ethereum-mainnet', 'live',
            now() - interval '1 second', now()
        )
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = $1 \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;

    let chain_rpc_urls = bigname_lookup::ChainRpcUrls::from_entries(&[
        "ethereum-mainnet=http://rpc.test".to_owned(),
    ])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?
        .with_phase_heartbeat_max_age_secs(1);
    state
        .status_freshness
        .seed_success(
            "ethereum-mainnet",
            120,
            sqlx::types::time::OffsetDateTime::now_utc(),
        )
        .await;

    assert_eq!(status_value(state.clone()).await?, json!("stale"));

    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'paused', last_error = NULL,
            started_at = now(), finished_at = NULL
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("degraded"));

    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'completed', finished_at = now()
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("ready"));

    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = 'old-interpreter' \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("degraded"));
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = $1 \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("ready"));

    sqlx::query(
        "UPDATE chain_phase_state SET current_block_hash = '0xold-same-height-head' \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let read = bigname_storage::load_phase_indexing_status(&database.lookup_pool).await?;
    assert!(!read.chains[0].project_generation_current);
    assert_eq!(status_value(state.clone()).await?, json!("degraded"));
    sqlx::query(
        "UPDATE chain_phase_state SET current_block_hash = '0xphase-head' \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("ready"));

    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'running', current_block_number = 115,
            current_block_hash = '0xphase-projected', finished_at = NULL
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("ready"));

    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET current_block_number = NULL, current_block_hash = NULL
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("degraded"));

    sqlx::raw_sql(
        r#"
        UPDATE chain_phase_state
        SET current_block_number = 120, current_block_hash = '0xphase-head'
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project';
        UPDATE service_heartbeats
        SET started_at = now() - interval '1 minute',
            heartbeat_at = now()
        WHERE service_name = 'phase-runner' AND chain_id = 'ethereum-mainnet'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("ready"));

    sqlx::query(
        "DELETE FROM service_heartbeats
         WHERE service_name = 'phase-runner' AND chain_id = 'ethereum-mainnet'",
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("degraded"));

    sqlx::query(
        r#"
        INSERT INTO service_heartbeats (
            service_name, instance_id, chain_id, phase_name,
            started_at, heartbeat_at
        ) VALUES (
            'phase-runner', 'status-test', 'ethereum-mainnet', 'live',
            now() - interval '3 minutes', now() - interval '2 minutes'
        )
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state.clone()).await?, json!("stale"));

    sqlx::raw_sql(
        r#"
        UPDATE service_heartbeats
        SET started_at = now(), heartbeat_at = now()
        WHERE service_name = 'phase-runner' AND chain_id = 'ethereum-mainnet';
        UPDATE chain_phase_state
        SET phase_status = 'running', finished_at = NULL,
            redo_in_progress = true, redo_mode = 'redo',
            redo_previous_phase_status = 'completed',
            redo_previous_started_at = now() - interval '2 minutes',
            redo_previous_finished_at = now() - interval '1 minute',
            redo_from_block_number = 100, redo_to_block_number = 120,
            redo_current_block_number = 120,
            redo_current_block_hash = '0xphase-head',
            redo_target_block_number = 120,
            redo_target_block_hash = '0xphase-head'
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(status_value(state).await?, json!("degraded"));

    database.cleanup().await
}

#[tokio::test]
async fn startup_and_v2_status_tolerate_an_absent_phase_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query("DROP SCHEMA bigname_phase CASCADE")
        .execute(&database.pool)
        .await?;

    assert!(
        load_expected_status_chain_ids_at_startup(&database.lookup_pool)
            .await?
            .is_empty()
    );
    let response = app_router(database.app_state())
        .oneshot(Request::builder().uri("/v2/status").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("degraded"));
    assert_eq!(payload["data"]["chains"], json!({}));

    database.cleanup().await
}

#[tokio::test]
async fn startup_and_v2_status_reject_a_partially_missing_phase_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query("DROP TABLE bigname_phase.chain_phase_state CASCADE")
        .execute(&database.pool)
        .await?;

    assert!(
        load_expected_status_chain_ids_at_startup(&database.lookup_pool)
            .await
            .is_err()
    );
    let response = app_router(database.app_state())
        .oneshot(Request::builder().uri("/v2/status").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    database.cleanup().await
}

async fn status_value(state: AppState) -> Result<Value> {
    let response = app_router(state)
        .oneshot(Request::builder().uri("/v2/status").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    Ok(payload["data"]["chains"]["1"]["status"].clone())
}
