// What the flip (TYR-36 step 7b-6) changes outside the moved routes, under both states of the
// publication switch: `/v1/status` reads the family marker's block as the indexed block with the
// switch on, and reports no lag during a redo in either state.

/// A head at 120 with the Project row published at the head and a live family marker one block
/// behind at 119: the two publications differ, so the tests see which one status reads.
async fn seed_flip_status_fixture(database: &TestDatabase) -> Result<AppState> {
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
    sqlx::query(
        "INSERT INTO bigname_phase.project_family_marker (
             chain_id, current_block_number, current_block_hash, block_timestamp,
             input_content_hash, sequence, state
         ) VALUES (
             'ethereum-mainnet', 119, '0xflip-119', '2026-08-06T00:00:08Z', $1, 3, 'live'
         )",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;
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

async fn flip_status_chain(state: &AppState, on: bool) -> Result<Value> {
    let payload = bigname_storage::publication_source::with_serve_from_families(
        on,
        status_payload(state.clone()),
    )
    .await?;
    Ok(payload["data"]["chains"]["1"].clone())
}

/// Ruling J12: with the switch on, the indexed block, block lag and time lag are the family
/// marker's; with it off they stay the Project row's.
#[tokio::test]
async fn v2_status_reads_the_family_marker_as_the_indexed_block_with_the_switch_on() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let state = seed_flip_status_fixture(&database).await?;

    let served = flip_status_chain(&state, false).await?;
    assert_eq!(served["indexed_block"], json!(120));
    assert_eq!(served["lag_blocks"], json!(0));
    assert_eq!(served["lag_seconds"], json!(0));
    assert_eq!(served["status"], json!("ready"));

    let families = flip_status_chain(&state, true).await?;
    assert_eq!(families["indexed_block"], json!(119));
    assert_eq!(families["lag_blocks"], json!(1));
    assert_eq!(families["lag_seconds"], json!(12));
    assert_eq!(families["status"], json!("ready"));

    database.cleanup().await
}

/// Ruling J14: a family rebuild (`bootstrap_pending`) serves nothing with the switch on, so
/// status degrades; with the switch off the marker is not read and status is unchanged.
#[tokio::test]
async fn v2_status_degrades_during_a_family_rebuild_only_with_the_switch_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = seed_flip_status_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.lookup_pool)
        .await?;

    let families = flip_status_chain(&state, true).await?;
    assert_eq!(families["status"], json!("degraded"));
    assert_eq!(families["indexed_block"], json!(119));

    let served = flip_status_chain(&state, false).await?;
    assert_eq!(served["status"], json!("ready"));
    assert_eq!(served["indexed_block"], json!(120));

    database.cleanup().await
}

/// With the switch on and no family marker at all, nothing is indexed from the families yet.
#[tokio::test]
async fn v2_status_has_no_indexed_block_without_a_family_marker_with_the_switch_on() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let state = seed_flip_status_fixture(&database).await?;
    sqlx::query("DELETE FROM bigname_phase.project_family_marker")
        .execute(&database.lookup_pool)
        .await?;

    let families = flip_status_chain(&state, true).await?;
    assert_eq!(families["indexed_block"], Value::Null);
    assert_ne!(families["status"], json!("ready"));
    assert_eq!(flip_status_chain(&state, false).await?["status"], json!("ready"));

    database.cleanup().await
}

/// A live family marker on the Project row's publication, so the switch-on fence admits the read.
async fn seed_live_marker_on_the_project_publication(database: &TestDatabase) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_family_marker
             (chain_id, current_block_number, current_block_hash, block_timestamp,
              input_content_hash, sequence, state)
         SELECT project.chain_id, project.current_block_number, project.current_block_hash,
                lineage.block_timestamp, project.input_content_hash, 1, 'live'
         FROM bigname_phase.chain_phase_state project
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = project.chain_id
          AND lineage.block_number = project.current_block_number
          AND lineage.block_hash = project.current_block_hash
         WHERE project.phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

/// The verified-records fixture of
/// `v2_verified_records_return_conflict_when_project_generation_changes_during_rpc`, with a family
/// marker in `state` on the Project row's publication.
async fn seed_flip_verified_records_fixture(database: &TestDatabase, state: &str) -> Result<()> {
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    let position = json!({
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_003,
        "block_hash": execution_block_hash,
        "timestamp": "2026-04-17T00:00:03Z"
    });
    seed_v2_alice_name_record_fixture_migrated(
        database,
        |row| {
            row.namehash = namehash;
            row.chain_positions = json!({ "ethereum": position.clone() });
        },
        |_, _, inventory| {
            inventory.selectors = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            }]);
            inventory.entries = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000def"
                }
            }]);
            inventory.record_version_boundary["chain_position"]["block_hash"] =
                json!(execution_block_hash);
            inventory.chain_positions = json!({ "ethereum-mainnet": position.clone() });
        },
    )
    .await?;
    seed_live_marker_on_the_project_publication(database).await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = $1")
        .bind(state)
        .execute(&lookup_pool)
        .await?;
    Ok(())
}

async fn flip_verified_records(database: &TestDatabase, on: bool) -> Result<(StatusCode, Value, usize)> {
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_addr60_response(
            "0x0000000000000000000000000000000000000e0e",
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let response = bigname_storage::publication_source::with_serve_from_families(
        on,
        app_router(state).oneshot(
            Request::builder()
                .uri("/v1/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())?,
        ),
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

/// Ruling J14 at the route: while the families rebuild (`bootstrap_pending`) a verified read
/// answers the stale 409 with the switch on, before any provider call; with the switch off the
/// marker is not read and the lookup executes as before.
#[tokio::test]
async fn v2_verified_records_answer_stale_during_a_family_rebuild_only_with_the_switch_on()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_flip_verified_records_fixture(&database, "bootstrap_pending").await?;

    let (status, payload, requests) = flip_verified_records(&database, true).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(requests, 0, "a refused read calls no provider");

    let (status, payload, _) = flip_verified_records(&database, false).await?;
    assert_eq!(status, StatusCode::OK, "{payload}");

    database.cleanup().await
}

/// Flip prerequisite 6: during an Interpret redo the stored head and the indexed position both
/// stall, so their difference reads 0 while the chain moves on. Status reports the lags as
/// unknown (null) for the redo's duration instead, with the switch on and off.
#[tokio::test]
async fn v2_status_lag_is_unknown_during_an_interpret_redo_in_both_switch_states() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let state = seed_flip_status_fixture(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.project_family_marker
         SET current_block_number = 120, current_block_hash = '0xflip-120',
             block_timestamp = '2026-08-06T00:00:20Z'",
    )
    .execute(&database.lookup_pool)
    .await?;
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
    let caught_up = flip_status_chain(&state, true).await?;
    assert_eq!(caught_up["lag_blocks"], json!(0));
    assert_eq!(caught_up["status"], json!("ready"));

    database
        .simulate_interpret_redo_begin("ethereum-mainnet", "recompute_flags")
        .await?;
    let families = flip_status_chain(&state, true).await?;
    assert_eq!(families["indexed_block"], json!(120));
    assert_eq!(families["lag_blocks"], Value::Null);
    assert_eq!(families["lag_seconds"], Value::Null);
    assert_eq!(families["status"], json!("degraded"));

    let served = flip_status_chain(&state, false).await?;
    assert_eq!(served["indexed_block"], json!(120));
    assert_eq!(served["lag_blocks"], Value::Null);
    assert_eq!(served["lag_seconds"], Value::Null);
    assert_eq!(served["status"], json!("degraded"));

    database.cleanup().await
}
