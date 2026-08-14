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
async fn v2_status_keeps_sepolia_unready_until_provider_trusted_verify_completes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp,
            canonicality_state
        ) VALUES (
            'ethereum-sepolia', '0xsepolia-published', 120,
            '2026-08-06T00:00:20Z', 'finalized'
        );
        INSERT INTO chain_heads (
            chain_id, latest_block_hash, latest_block_number,
            safe_block_hash, safe_block_number,
            finalized_block_hash, finalized_block_number
        ) VALUES (
            'ethereum-sepolia', '0xsepolia-published', 120,
            '0xsepolia-published', 120, '0xsepolia-published', 120
        );
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_phase_state (
            chain_id, phase_name, phase_status, current_block_number,
            current_block_hash, target_block_number, target_block_hash,
            input_content_hash, started_at, finished_at
        ) VALUES (
            'ethereum-sepolia', 'project', 'completed', 120,
            '0xsepolia-published', 120, '0xsepolia-published', $1,
            now() - interval '2 seconds', now() - interval '1 second'
        )
        "#,
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::raw_sql(
        r#"
        INSERT INTO chain_phase_state (
            chain_id, phase_name, phase_status, current_block_number,
            current_block_hash, target_block_number, target_block_hash,
            live_handoff_block_number, live_handoff_block_hash,
            started_at, finished_at
        ) VALUES (
            'ethereum-sepolia', 'ingest', 'completed', 120,
            '0xsepolia-published', 120, '0xsepolia-published',
            120, '0xsepolia-published',
            now() - interval '4 seconds', now() - interval '3 seconds'
        );
        INSERT INTO chain_phase_state (
            chain_id, phase_name, phase_status, verification_level, started_at
        ) VALUES (
            'ethereum-sepolia', 'verify', 'running', 'quick_synced',
            now() - interval '1 second'
        );
        INSERT INTO service_heartbeats (
            service_name, instance_id, chain_id, phase_name,
            started_at, heartbeat_at
        ) VALUES (
            'phase-runner', 'sepolia-status-test', 'ethereum-sepolia', 'verify',
            now() - interval '1 second', now()
        )
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let chain_rpc_urls = bigname_lookup::ChainRpcUrls::from_entries(&[
        "ethereum-sepolia=http://rpc.test".to_owned(),
    ])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    state
        .status_freshness
        .seed_success(
            "ethereum-sepolia",
            120,
            sqlx::types::time::OffsetDateTime::now_utc(),
        )
        .await;

    let while_verify_runs = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            current_block_number = 120, current_block_hash = '0xsepolia-published',
            target_block_number = 120, target_block_hash = '0xsepolia-published',
            last_error = NULL, finished_at = now()
        WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let after_initial_verify_completes = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             settled_while_unconfigured = TRUE,
             live_handoff_block_number = NULL,
             live_handoff_block_hash = NULL,
             finished_at = now()
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'ingest'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let while_ingest_completion_is_settled = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'failed', last_error = 'verification failed', finished_at = now()
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let failed_verify_while_ingest_is_settled = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', last_error = NULL, finished_at = now()
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE service_heartbeats
         SET started_at = now() - interval '2 hours 1 second',
             heartbeat_at = now() - interval '2 hours'
         WHERE service_name = 'phase-runner' AND chain_id = 'ethereum-sepolia'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let stale_heartbeat_while_ingest_is_settled = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE service_heartbeats SET heartbeat_at = now()
         WHERE service_name = 'phase-runner' AND chain_id = 'ethereum-sepolia'",
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET settled_while_unconfigured = NULL,
             live_handoff_block_number = target_block_number,
             live_handoff_block_hash = target_block_hash,
             finished_at = now()
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'ingest'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let after_ingest_completion_is_genuine = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET settled_while_unconfigured = TRUE,
             verification_level = NULL,
             current_block_number = 119, current_block_hash = '0xsepolia-before-published'
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let while_verify_completion_is_settled_without_level =
        sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET verification_level = 'cross_checked'
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let while_verify_completion_is_settled_cross_checked =
        sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET verification_level = 'node_checked'
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let while_verify_completion_is_settled_node_checked =
        sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET verification_level = 'quick_synced'
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let while_verify_completion_is_settled_quick_synced =
        sepolia_status_value(state.clone()).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET settled_while_unconfigured = NULL,
             current_block_number = target_block_number,
             current_block_hash = target_block_hash
         WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let after_verify_redo_clears_settlement = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'failed',
            last_error = 'completed phase validation failed: source identity changed',
            finished_at = now()
        WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'ingest'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let after_completed_ingest_validation_fails = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'completed', last_error = NULL, finished_at = now()
        WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'ingest'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let after_ingest_validation_recovers = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'failed', last_error = 'verification failed',
            finished_at = now()
        WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let after_completed_revalidation_fails = sepolia_status_value(state.clone()).await?;
    sqlx::query(
        r#"
        UPDATE chain_phase_state
        SET phase_status = 'completed', verification_level = 'quick_synced',
            current_block_number = 120, current_block_hash = '0xsepolia-published',
            target_block_number = 120, target_block_hash = '0xsepolia-published',
            last_error = NULL, finished_at = now()
        WHERE chain_id = 'ethereum-sepolia' AND phase_name = 'verify'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let after_verify_completes = sepolia_status_value(state).await?;

    database.cleanup().await?;
    assert_eq!(while_verify_runs, json!("degraded"));
    assert_eq!(after_initial_verify_completes, json!("ready"));
    assert_eq!(while_ingest_completion_is_settled, json!("degraded"));
    assert_eq!(failed_verify_while_ingest_is_settled, json!("stale"));
    assert_eq!(stale_heartbeat_while_ingest_is_settled, json!("stale"));
    assert_eq!(after_ingest_completion_is_genuine, json!("ready"));
    assert_eq!(
        while_verify_completion_is_settled_without_level,
        json!("degraded")
    );
    assert_eq!(
        while_verify_completion_is_settled_cross_checked,
        json!("degraded")
    );
    assert_eq!(
        while_verify_completion_is_settled_node_checked,
        json!("degraded")
    );
    assert_eq!(
        while_verify_completion_is_settled_quick_synced,
        json!("degraded")
    );
    assert_eq!(after_verify_redo_clears_settlement, json!("ready"));
    assert_eq!(after_completed_ingest_validation_fails, json!("stale"));
    assert_eq!(after_ingest_validation_recovers, json!("ready"));
    assert_eq!(after_completed_revalidation_fails, json!("stale"));
    assert_eq!(after_verify_completes, json!("ready"));
    Ok(())
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

async fn sepolia_status_value(state: AppState) -> Result<Value> {
    let response = app_router(state)
        .oneshot(Request::builder().uri("/v2/status").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    Ok(payload["data"]["chains"]["11155111"]["status"].clone())
}
