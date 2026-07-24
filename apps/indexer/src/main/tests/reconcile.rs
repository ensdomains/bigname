#[tokio::test]
async fn reconcile_fetched_heads_initializes_chain_from_provider_heads() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(31);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for cold start reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let canonical_parent = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
        41,
    );
    let safe_head = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
        40,
    );
    let safe_parent = provider_block(
        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        39,
    );
    let finalized_head = provider_block(
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        38,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&canonical_head)],
            block: canonical_head.clone(),
        },
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&canonical_parent)],
            block: canonical_parent,
        },
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&safe_head)],
            block: safe_head.clone(),
        },
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&safe_parent)],
            block: safe_parent,
        },
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&finalized_head)],
            block: finalized_head.clone(),
        },
    ])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: Some(safe_head),
            finalized: Some(finalized_head),
        },
    )
    .await?
    .expect("cold start reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert!(outcome.canonical_head_changed);
    assert!(outcome.safe_head_changed);
    assert!(outcome.finalized_head_changed);
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(42));
    assert_eq!(next_task.checkpoint.safe_block_number, Some(40));
    assert_eq!(next_task.checkpoint.finalized_block_number, Some(38));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_transactions")
            .fetch_one(database.pool())
            .await?,
        5,
        "cold start must retain every canonical payload from finalized through latest"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_receipts")
            .fetch_one(database.pool())
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_payload_cache_metadata")
            .fetch_one(database.pool())
            .await?,
        15
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        5,
        "cold-start normalization must include events in the unfinalized tail, not only checkpoint heads"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 38"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 38"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 38"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_number = 38"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 38"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events WHERE event_kind = 'PreimageObserved' AND block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "wrapped.eth".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn live_tail_records_exact_ens_v2_coverage_above_latched_finalized_head() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-sepolia-live-tail-coverage";
    let selected_address = "0x00000000000000000000000000000000000000a1";
    let later_unselected_address = "0x00000000000000000000000000000000000000b2";
    let manifest_id = 77_001;
    let selected_contract_instance_id = Uuid::from_u128(77_001);
    let later_contract_instance_id = Uuid::from_u128(77_002);

    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_id,
            namespace,
            source_family,
            chain,
            rollout_status,
            manifest_payload
        )
        VALUES (
            $1,
            'ens',
            'ens_v2_registry_l1',
            $2,
            'active',
            $3::JSONB
        )
        "#,
    )
    .bind(manifest_id)
    .bind(chain)
    .bind(
        json!({
            "contracts": [
                {
                    "role": "registry",
                    "address": selected_address,
                    "start_block": 3
                },
                {
                    "role": "later_registry",
                    "address": later_unselected_address,
                    "start_block": 5
                }
            ]
        })
        .to_string(),
    )
    .execute(database.pool())
    .await?;
    insert_contract_instance(
        database.pool(),
        selected_contract_instance_id,
        chain,
        "registry",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        selected_contract_instance_id,
        chain,
        selected_address,
        Some(manifest_id),
    )
    .await?;
    sqlx::query(
        "UPDATE contract_instance_addresses SET active_from_block_number = 3 WHERE contract_instance_id = $1",
    )
    .bind(selected_contract_instance_id)
    .execute(database.pool())
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        manifest_id,
        "registry",
        selected_contract_instance_id,
        selected_address,
        "none",
        None,
        None,
    )
    .await?;

    let block_0 = provider_block(&format!("0x{:064x}", 100), None, 0);
    let block_1 = provider_block(&format!("0x{:064x}", 101), Some(&block_0.block_hash), 1);
    let block_2 = provider_block(&format!("0x{:064x}", 102), Some(&block_1.block_hash), 2);
    let block_3 = provider_block(&format!("0x{:064x}", 103), Some(&block_2.block_hash), 3);
    let block_4 = provider_block(&format!("0x{:064x}", 104), Some(&block_3.block_hash), 4);
    let (provider, server) = bundle_provider_with_fixtures(
        [&block_0, &block_1, &block_2, &block_3, &block_4]
            .into_iter()
            .map(|block| ProviderBlockFixture {
                block: block.clone(),
                logs: Vec::new(),
            })
            .collect(),
    )
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: None,
            canonical_block_number: None,
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: block_4.clone(),
            safe: Some(block_0.clone()),
            finalized: Some(block_0.clone()),
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .context("cold live-tail reconciliation must advance above finalized block zero")?;
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(task.checkpoint.canonical_block_number, Some(4));
    assert_eq!(
        sqlx::query_as::<_, (bool, Option<i64>, Option<i64>)>(
            r#"
            SELECT
                retained_history_complete,
                proven_retention_generation,
                proven_through_block
            FROM raw_log_staging_input_revisions
            WHERE chain_id = $1
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (true, Some(0), Some(4)),
        "the exact live selection must close the manifest target interval 3..=4 even while adapter sync is disabled"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM backfill_coverage_facts WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        0,
        "live-tail proof must not invent a backfill job or coverage fact"
    );

    insert_contract_instance(
        database.pool(),
        later_contract_instance_id,
        chain,
        "registry",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        later_contract_instance_id,
        chain,
        later_unselected_address,
        Some(manifest_id),
    )
    .await?;
    sqlx::query(
        "UPDATE contract_instance_addresses SET active_from_block_number = 5 WHERE contract_instance_id = $1",
    )
    .bind(later_contract_instance_id)
    .execute(database.pool())
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        manifest_id,
        "later_registry",
        later_contract_instance_id,
        later_unselected_address,
        "none",
        None,
        None,
    )
    .await?;

    let block_5 = provider_block(&format!("0x{:064x}", 105), Some(&block_4.block_hash), 5);
    let (provider_5, server_5) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: block_0.clone(),
            logs: Vec::new(),
        },
        ProviderBlockFixture {
            block: block_5.clone(),
            logs: Vec::new(),
        },
    ])
    .await?;
    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider_5,
        &ProviderHeadSnapshot {
            canonical: block_5.clone(),
            safe: Some(block_0.clone()),
            finalized: Some(block_0),
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("an address omitted from the provider selection must not inherit live coverage");
    let requirement = error
        .downcast_ref::<bigname_adapters::EnsV2MissingCoverage>()
        .cloned()
        .with_context(|| format!("unexpected unselected-target refusal: {error:#}"))?;
    assert_eq!(
        requirement,
        bigname_adapters::EnsV2MissingCoverage {
            chain: chain.to_owned(),
            retention_generation: 0,
            source_family: "ens_v2_registry_l1".to_owned(),
            address: later_unselected_address.to_owned(),
            required_from_block: 5,
            required_to_block: 5,
        },
        "the recovery request must name only the provider-unselected tuple"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT proven_through_block FROM raw_log_staging_input_revisions WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        Some(4),
        "a failed unselected interval must not widen the durable proof"
    );

    let mut stale_requirement = requirement.clone();
    stale_requirement.retention_generation += 1;
    assert_eq!(
        recover_ens_v2_live_coverage_requirement(
            database.pool(),
            "test",
            &provider_5,
            HeaderAuditMode::Minimal,
            &stale_requirement,
        )
        .await?,
        EnsV2LiveCoverageRecoveryStatus::AuthorityChanged,
        "a stale typed requirement must replan before provider I/O"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM backfill_coverage_facts WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        0
    );

    assert_eq!(
        recover_ens_v2_live_coverage_requirement(
            database.pool(),
            "test",
            &provider_5,
            HeaderAuditMode::Minimal,
            &requirement,
        )
        .await?,
        EnsV2LiveCoverageRecoveryStatus::Recovered
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64, i64, Option<i64>)>(
            r#"
            SELECT
                fact.source_family,
                fact.address,
                fact.covered_from_block,
                fact.covered_to_block,
                job.raw_log_retention_generation
            FROM backfill_coverage_facts fact
            JOIN backfill_jobs job
              ON job.backfill_job_id = fact.backfill_job_id
            WHERE fact.chain_id = $1
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (
            "ens_v2_registry_l1".to_owned(),
            later_unselected_address.to_owned(),
            5,
            5,
            Some(0),
        ),
        "automatic recovery must persist only the exact provider-fetched tuple in the current generation"
    );

    let (recovered_task, _) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider_5,
        &ProviderHeadSnapshot {
            canonical: block_5,
            safe: None,
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .context("the unchanged live poll must advance after exact provider-backed recovery")?;
    assert_eq!(recovered_task.checkpoint.canonical_block_number, Some(5));
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT proven_through_block FROM raw_log_staging_input_revisions WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        Some(5)
    );

    server.abort();
    server_5.abort();
    database.cleanup().await
}

#[tokio::test]
async fn reconcile_fetched_heads_refuses_checkpoint_for_deferred_resolver_profile_chain()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver = "0x00000000000000000000000000000000000000ff";
    let current = provider_block(
        "0x0101010101010101010101010101010101010101010101010101010101010101",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let latest = provider_block(
        "0x0202020202020202020202020202020202020202020202020202020202020202",
        Some(&current.block_hash),
        42,
    );
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since
        ) VALUES ($1, 0, 1, FALSE, now())
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    bigname_storage::enqueue_resolver_profile_reconciliations(
        database.pool(),
        &[bigname_storage::ResolverProfileReconciliationTarget {
            chain_id: chain.to_owned(),
            contract_address: resolver.to_owned(),
        }],
    )
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: Vec::new(),
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: latest.clone(),
        logs: Vec::new(),
    }])
    .await?;

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: None,
            finalized: None,
        },
        true,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("deferred resolver-profile work must fence checkpoint publication");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("resolver-profile reconciliation")
            && rendered.contains("refusing chain checkpoint advancement"),
        "checkpoint refusal must name the deferred resolver-profile boundary: {rendered}"
    );
    let checkpoint = bigname_storage::load_chain_checkpoint(database.pool(), chain)
        .await?
        .expect("original checkpoint must remain stored");
    assert_eq!(checkpoint.canonical_block_hash, Some(current.block_hash));
    assert_eq!(
        checkpoint.canonical_block_number,
        Some(current.block_number)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM resolver_profile_input_changes WHERE processed_generation < generation"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "deferred profile generation must remain durable and unacknowledged"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_live_tip_event_silent_retains_full_payload_and_call_observation()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let event_silent_address = "0x0000000000000000000000000000000000000002";
    let current = provider_block(
        "0x0101010101010101010101010101010101010101010101010101010101010101",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let latest = provider_block(
        "0x0202020202020202020202020202020202020202020202020202020202020202",
        Some(&current.block_hash),
        42,
    );
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: latest.clone(),
        logs: vec![],
    }])
    .await?;

    let (_task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest.clone(),
            safe: None,
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[event_silent_address.to_owned()],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("live append must advance with event-silent enabled");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Appended
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM raw_payload_cache_metadata WHERE payload_kind = $1"
        )
        .bind(provider::RAW_PAYLOAD_KIND_FULL_BLOCK)
        .fetch_one(database.pool())
        .await?,
        1,
        "live event-silent reconciliation must retain full-block payload metadata at the tip"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        0,
        "event-silent direct-call capture must not depend on selected logs"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_transactions")
            .fetch_one(database.pool())
            .await?,
        1,
        "event-silent live-tip capture must retain the direct-call transaction"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_receipts")
            .fetch_one(database.pool())
            .await?,
        1,
        "event-silent live-tip capture must retain the direct-call receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT resolver_address FROM event_silent_resolver_call_observations"
        )
        .fetch_one(database.pool())
        .await?,
        event_silent_address.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_live_tip_retains_unlisted_generic_ensv1_resolver_log() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let selected_address = "0x00000000000000000000000000000000000000a1";
    let generic_resolver_address = "0x00000000000000000000000000000000000000b1";
    let sibling_only_address = "0x00000000000000000000000000000000000000c1";
    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_id,
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES (1, 1, 'ens', 'ens_v1_resolver_l1', $1, 'ens_v1', 'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_resolver_l1/v1.toml', $2)
        "#,
    )
    .bind(chain)
    .bind(json!({
        "abi": {
            "events": [{
                "name": "TextChanged",
                "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key)",
                "normalized_events": ["RecordChanged"]
            }]
        }
    }))
    .execute(database.pool())
    .await?;

    let current = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let latest = provider_block(
        "0x2222222222222222222222222222222222222222222222222222222222222222",
        Some(&current.block_hash),
        42,
    );
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let generic_log = rpc_resolver_text_changed_log_payload(
        &latest,
        generic_resolver_address,
        "generic",
        "description",
        0,
    );
    let mut sibling_only_log = rpc_log_payload(&latest);
    sibling_only_log["address"] = json!(sibling_only_address);
    sibling_only_log["logIndex"] = json!("0x1");
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: latest.clone(),
        logs: vec![generic_log, sibling_only_log],
    }])
    .await?;

    let (_task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: None,
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("live append must retain the generic resolver topic");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Appended
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(emitting_address ORDER BY log_index) FROM raw_logs"
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            generic_resolver_address.to_owned(),
            sibling_only_address.to_owned(),
        ],
        "generic resolver selection must retain same-transaction sibling logs as transaction context"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_transactions")
            .fetch_one(database.pool())
            .await?,
        1,
        "the generic resolver log transaction must be retained"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_receipts")
            .fetch_one(database.pool())
            .await?,
        1,
        "the generic resolver log receipt must be retained"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(contract_address ORDER BY contract_address) FROM raw_code_hashes"
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            selected_address.to_owned(),
            generic_resolver_address.to_owned(),
        ],
        "live code observation must include the watched baseline and unlisted generic resolver emitter, but not its sibling-only emitter"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn reconcile_fetched_heads_fetches_missing_emitter_code_despite_unrelated_block_code_row()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let emitter_address = "0x0000000000000000000000000000000000000001";
    let quiet_address = "0x0000000000000000000000000000000000000002";
    let canonical_head = provider_block(
        "0xabababababababababababababababababababababababababababababababab",
        Some("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
        42,
    );
    let safe_head = provider_block(
        "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
        None,
        41,
    );
    let task = crate::runtime::IntakeChainTask {
        chain: "ethereum-mainnet".to_owned(),
        addresses: vec![emitter_address.to_owned(), quiet_address.to_owned()],
        manifest_root_entry_count: 1,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: bigname_storage::ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: Some(canonical_head.block_hash.clone()),
            canonical_block_number: Some(canonical_head.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &canonical_head,
                CanonicalityState::Canonical,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &safe_head,
                CanonicalityState::Safe,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: canonical_head.block_hash.clone(),
                block_number: canonical_head.block_number,
                transaction_hash: transaction_hash_for_block(&canonical_head),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitter_address.to_owned(),
                topics: vec![name_wrapped_topic0()],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: safe_head.block_hash.clone(),
                block_number: safe_head.block_number,
                transaction_hash: transaction_hash_for_block(&safe_head),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitter_address.to_owned(),
                topics: vec![name_wrapped_topic0()],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Safe,
            },
        ],
    )
    .await?;
    upsert_raw_code_hashes(
        database.pool(),
        &[RawCodeHash {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: canonical_head.block_hash.clone(),
            block_number: canonical_head.block_number,
            contract_address: quiet_address.to_owned(),
            code_hash: "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                .to_owned(),
            code_byte_length: 0,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let code_requests =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String, u64, u64)>::new()));
    let request_log = std::sync::Arc::clone(&code_requests);
    let (url, server) = spawn_json_rpc_server(std::sync::Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let first_param = params
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        let result = match method {
            "eth_getCode" => {
                request_log
                    .lock()
                    .expect("code request log must not be poisoned")
                    .push((
                        first_param.clone(),
                        body.get("_test_http_request_id")
                            .and_then(Value::as_u64)
                            .expect("test server must annotate the HTTP request id"),
                        body.get("_test_batch_size")
                            .and_then(Value::as_u64)
                            .expect("test server must annotate the batch size"),
                    ));
                Value::String("0x6001600155".to_owned())
            }
            _ => panic!("unexpected reconciliation RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;
    let provider = provider::JsonRpcProvider::new(&url)?;

    persist_reconciled_raw_code_hashes(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: Some(safe_head),
            finalized: None,
        },
        &CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Unchanged,
            canonical: Some(CheckpointBlockRef {
                block_hash: canonical_head.block_hash.clone(),
                block_number: canonical_head.block_number,
            }),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: Vec::new(),
            raw_orphan_stop_before_hash: None,
        },
        HeadChangeSet {
            canonical_head_changed: false,
            safe_head_changed: false,
            finalized_head_changed: false,
        },
        0,
        &ChainCoverageFrontiers::default(),
    )
    .await?;

    assert_eq!(
        code_requests
            .lock()
            .expect("code request log must not be poisoned")
            .as_slice(),
        &[
            (emitter_address.to_owned(), 0, 2),
            (emitter_address.to_owned(), 0, 2),
        ],
        "all candidate blocks must share one batched provider call"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        3
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn raw_code_baseline_sweep_persists_progress_and_resweeps_on_admission_epoch_move()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address_a = "0x00000000000000000000000000000000000000aa";
    let address_b = "0x00000000000000000000000000000000000000bb";
    let address_c = "0x00000000000000000000000000000000000000cc";
    let canonical_head = provider_block(
        "0xfefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe",
        Some("0xadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad"),
        61,
    );
    let task_for = |addresses: Vec<String>| crate::runtime::IntakeChainTask {
        chain: "ethereum-mainnet".to_owned(),
        addresses,
        manifest_root_entry_count: 1,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: bigname_storage::ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: Some(canonical_head.block_hash.clone()),
            canonical_block_number: Some(canonical_head.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let heads = ProviderHeadSnapshot {
        canonical: canonical_head.clone(),
        safe: None,
        finalized: None,
    };
    let canonical_reconciliation = CanonicalReconciliation {
        status: CanonicalReconciliationStatus::Unchanged,
        canonical: Some(CheckpointBlockRef {
            block_hash: canonical_head.block_hash.clone(),
            block_number: canonical_head.block_number,
        }),
        fetched_parent_count: 0,
        orphaned_block_count: 0,
        reconciled_blocks: Vec::new(),
        raw_orphan_stop_before_hash: None,
    };
    let unchanged_heads = HeadChangeSet {
        canonical_head_changed: false,
        safe_head_changed: false,
        finalized_head_changed: false,
    };
    upsert_raw_blocks(
        database.pool(),
        &[provider_block_to_raw_block(
            "ethereum-mainnet",
            &canonical_head,
            CanonicalityState::Canonical,
        )],
    )
    .await?;

    let code_requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let request_log = std::sync::Arc::clone(&code_requests);
    let (url, server) = spawn_json_rpc_server(std::sync::Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let first_param = params
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        let result = match method {
            "eth_getCode" => {
                request_log
                    .lock()
                    .expect("code request log must not be poisoned")
                    .push(first_param.clone());
                Value::String("0x6001600155".to_owned())
            }
            _ => panic!("unexpected reconciliation RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;
    let provider = provider::JsonRpcProvider::new(&url)?;
    let coverage_frontiers = ChainCoverageFrontiers::default();

    // First tick: every watched address is missing a baseline observation, so
    // the sweep fetches them (in sorted order) and upserts the observations.
    persist_reconciled_raw_code_hashes(
        database.pool(),
        &task_for(vec![address_a.to_owned(), address_b.to_owned()]),
        &provider,
        &heads,
        &canonical_reconciliation,
        unchanged_heads,
        0,
        &coverage_frontiers,
    )
    .await?;
    assert_eq!(
        *code_requests
            .lock()
            .expect("code request log must not be poisoned"),
        vec![address_a.to_owned(), address_b.to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        2
    );

    // Second tick under the same admission epoch: the finished sweep is
    // remembered, so no address is re-fetched and no anti-join re-runs over
    // the watch surface.
    persist_reconciled_raw_code_hashes(
        database.pool(),
        &task_for(vec![address_a.to_owned(), address_b.to_owned()]),
        &provider,
        &heads,
        &canonical_reconciliation,
        unchanged_heads,
        0,
        &coverage_frontiers,
    )
    .await?;
    assert_eq!(
        code_requests
            .lock()
            .expect("code request log must not be poisoned")
            .len(),
        2
    );

    // A watched-surface mutation bumps the chain's admission epoch (the
    // ratified invariant). Once the matching plan is loaded, the sweep
    // restarts — probing observed addresses and fetching only the new one.
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ('ethereum-mainnet', 1)
        ON CONFLICT (chain_id) DO UPDATE SET epoch = discovery_admission_epochs.epoch + 1
        "#,
    )
    .execute(database.pool())
    .await?;

    // The DB epoch can move before the poll loop applies the corresponding
    // plan. A sweep over the still-loaded old plan must remain pinned to its
    // old epoch rather than recording completion for the unseen address.
    persist_reconciled_raw_code_hashes(
        database.pool(),
        &task_for(vec![address_a.to_owned(), address_b.to_owned()]),
        &provider,
        &heads,
        &canonical_reconciliation,
        unchanged_heads,
        0,
        &coverage_frontiers,
    )
    .await?;
    assert_eq!(
        code_requests
            .lock()
            .expect("code request log must not be poisoned")
            .len(),
        2
    );

    persist_reconciled_raw_code_hashes(
        database.pool(),
        &task_for(vec![
            address_a.to_owned(),
            address_b.to_owned(),
            address_c.to_owned(),
        ]),
        &provider,
        &heads,
        &canonical_reconciliation,
        unchanged_heads,
        1,
        &coverage_frontiers,
    )
    .await?;
    assert_eq!(
        *code_requests
            .lock()
            .expect("code request log must not be poisoned"),
        vec![
            address_a.to_owned(),
            address_b.to_owned(),
            address_c.to_owned(),
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        3
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_skips_stale_finalized_checkpoint_tag() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let block_102 = provider_block(
        "0x1020000000000000000000000000000000000000000000000000000000000000",
        Some("0x1010000000000000000000000000000000000000000000000000000000000000"),
        102,
    );
    let block_103 = provider_block(
        "0x1030000000000000000000000000000000000000000000000000000000000000",
        Some(&block_102.block_hash),
        103,
    );
    let block_104 = provider_block(
        "0x1040000000000000000000000000000000000000000000000000000000000000",
        Some(&block_103.block_hash),
        104,
    );
    let block_105 = provider_block(
        "0x1050000000000000000000000000000000000000000000000000000000000000",
        Some(&block_104.block_hash),
        105,
    );
    let block_106 = provider_block(
        "0x1060000000000000000000000000000000000000000000000000000000000000",
        Some(&block_105.block_hash),
        106,
    );
    let block_107 = provider_block(
        "0x1070000000000000000000000000000000000000000000000000000000000000",
        Some(&block_106.block_hash),
        107,
    );
    let (provider, server) = bundle_provider(vec![
        block_102.clone(),
        block_103.clone(),
        block_104.clone(),
        block_105.clone(),
        block_106.clone(),
        block_107.clone(),
    ])
    .await?;

    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: Vec::new(),
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: None,
            canonical_block_number: None,
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let (task, _) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: block_105.clone(),
            safe: Some(block_104.clone()),
            finalized: Some(block_103.clone()),
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("initial reconciliation must seed checkpoints");

    let (task, _) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: block_107,
            safe: Some(block_106),
            finalized: Some(block_102),
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("stale finalized provider tag must not fail canonical advancement");

    assert_eq!(task.checkpoint.canonical_block_number, Some(107));
    assert_eq!(task.checkpoint.safe_block_number, Some(106));
    assert_eq!(task.checkpoint.finalized_block_number, Some(103));
    assert_eq!(
        task.checkpoint.finalized_block_hash.as_deref(),
        Some(block_103.block_hash.as_str())
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_does_not_revive_off_branch_safe_head_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let block_100 = provider_block(
        "0x1000000000000000000000000000000000000000000000000000000000000000",
        Some("0x0990000000000000000000000000000000000000000000000000000000000000"),
        100,
    );
    let block_101 = provider_block(
        "0x1010000000000000000000000000000000000000000000000000000000000000",
        Some(&block_100.block_hash),
        101,
    );
    let block_102 = provider_block(
        "0x1020000000000000000000000000000000000000000000000000000000000000",
        Some(&block_101.block_hash),
        102,
    );
    let off_branch_safe = provider_block(
        "0x2afe000000000000000000000000000000000000000000000000000000000000",
        Some("0x2afa000000000000000000000000000000000000000000000000000000000000"),
        100,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block_100,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block_101,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &off_branch_safe,
        CanonicalityState::Canonical,
    )
    .await?;
    bigname_storage::mark_chain_lineage_range_orphaned(
        database.pool(),
        chain,
        &off_branch_safe.block_hash,
        None,
    )
    .await?;
    let (provider, server) = bundle_provider(vec![
        block_100.clone(),
        block_101.clone(),
        block_102.clone(),
        off_branch_safe.clone(),
    ])
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: Vec::new(),
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(block_101.block_hash.clone()),
            canonical_block_number: Some(block_101.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, _outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: block_102,
            safe: Some(off_branch_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("canonical append must still advance");

    assert_eq!(task.checkpoint.canonical_block_number, Some(102));
    assert_eq!(task.checkpoint.safe_block_number, None);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = $1"
        )
        .bind(&off_branch_safe.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_canonical_head_rejects_gap_larger_than_bounded_backfill_chunk() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let gap_end_block = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 2;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=gap_end_block {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &current,
        CanonicalityState::Canonical,
    )
    .await?;
    for block in blocks.iter().skip(1) {
        if block.block_number == current.block_number + 10 {
            continue;
        }
        insert_chain_lineage_for_block(database.pool(), chain, block, CanonicalityState::Canonical)
            .await?;
    }
    let (provider, server) = bundle_provider(blocks).await?;
    let checkpoint = ChainCheckpoint {
        chain_id: chain.to_owned(),
        canonical_block_hash: Some(current.block_hash.clone()),
        canonical_block_number: Some(current.block_number),
        safe_block_hash: None,
        safe_block_number: None,
        finalized_block_hash: None,
        finalized_block_number: None,
    };

    let error = reconcile_canonical_head(
        database.pool(),
        &provider,
        chain,
        &checkpoint,
        &latest,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("live reconciliation must reject unbounded contiguous gaps");

    assert!(
        error.to_string().contains("exceeds live gap fill limit"),
        "unexpected unbounded gap error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_promotes_large_gap_from_stored_safe_lineage_with_completed_backfill_coverage()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number =
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS * 2 + 7;
    let live_latest_block_number = stored_safe_block_number + 25;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_054,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_054),
        selected_address,
    )
    .await?;
    let selected_log_blocks = blocks
        .iter()
        .filter(|block| {
            matches!(
                block.block_number - current.block_number,
                10 | 512 | crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    insert_selected_raw_log_inputs(
        database.pool(),
        chain,
        &selected_log_blocks,
        selected_address,
        false,
    )
    .await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest.clone(),
            safe: Some(stored_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("dense stored lineage must promote a checkpoint batch")
    .expect("stored lineage promotion must advance the checkpoint");

    let promoted_block_number =
        current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let promoted_block = blocks
        .iter()
        .find(|block| block.block_number == promoted_block_number)
        .expect("promoted batch target block must exist");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(outcome.fetched_parent_count, 0);
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(promoted_block_number)
    );
    assert_eq!(
        task.checkpoint.canonical_block_hash.as_deref(),
        Some(promoted_block.block_hash.as_str())
    );
    assert!(promoted_block_number < latest.block_number);
    assert!(promoted_block_number < stored_safe.block_number);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_payload_cache_metadata")
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_hash = $2"
        )
        .bind(chain)
        .bind(&latest.block_hash)
        .fetch_optional(database.pool())
        .await?,
        None
    );

    let persisted_checkpoint = bigname_storage::load_chain_checkpoint(database.pool(), chain)
        .await?
        .expect("promoted checkpoint row must exist");
    assert_eq!(persisted_checkpoint, task.checkpoint);

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Scaffolds a large-gap stored-lineage promotion over base-mainnet with one
/// watched contract and a single seeded raw log (block 11, inside the first
/// promoted batch), then runs one reconciliation cycle. Transactions and
/// receipts are always seeded for the log; `seed_code_rows` controls its code
/// companion, `log_topic0` its selectedness, and
/// `watched_active_from_block_number` narrows the watched entry's active
/// window. Returns the reconciliation result, the checkpoint block number,
/// and the handles the caller must clean up.
async fn companion_scope_promotion_scenario(
    manifest_id: i64,
    log_topic0: &str,
    seed_code_rows: bool,
    watched_active_from_block_number: Option<i64>,
) -> Result<(
    Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>>,
    i64,
    TestDatabase,
    JoinHandle<()>,
)> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number =
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS * 2 + 7;
    let live_latest_block_number = stored_safe_block_number + 25;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        manifest_id,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(u128::try_from(manifest_id).expect("non-negative test manifest id")),
        selected_address,
    )
    .await?;
    if let Some(active_from_block_number) = watched_active_from_block_number {
        sqlx::query(
            r#"
            UPDATE contract_instance_addresses
            SET active_from_block_number = $1
            WHERE chain_id = $2
              AND LOWER(address) = LOWER($3)
            "#,
        )
        .bind(active_from_block_number)
        .bind(chain)
        .bind(selected_address)
        .execute(database.pool())
        .await
        .context("failed to narrow the watched active window")?;
    }
    insert_raw_log_inputs_with_topic0(
        database.pool(),
        chain,
        &[blocks[10].clone()],
        selected_address,
        log_topic0,
        seed_code_rows,
        false,
    )
    .await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let outcome = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await;

    Ok((outcome, current.block_number, database, server))
}

/// A sibling-retained log from a watched address whose topic0 is not in the
/// watched family's manifest ABI topic0 set never receives a write-side code
/// observation, so promotion must not demand one.
#[tokio::test]
async fn reconcile_fetched_heads_promotes_despite_foreign_topic_sibling_log_without_code_row()
-> Result<()> {
    let (outcome, current_block_number, database, server) = companion_scope_promotion_scenario(
        10_060,
        &keccak256_hex(b"BaseReverseClaimed(address,bytes32)"),
        false,
        None,
    )
    .await?;

    let (task, outcome) = outcome
        .expect("foreign-topic sibling log must not block stored lineage promotion")
        .expect("stored lineage promotion must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current_block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A family-selected log (watched emitter, in-window, family topic0) missing
/// its raw code companion must still refuse promotion with the actionable
/// per-kind counts.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_family_selected_log_missing_code_companion() -> Result<()>
{
    let (outcome, current_block_number, database, server) =
        companion_scope_promotion_scenario(10_061, &family_selected_test_topic0(), false, None)
            .await?;

    let error = outcome
        .expect_err("a family-selected log without its code companion must refuse promotion");
    let refusal = format!("{error:#}");
    assert!(
        refusal.contains(&format!(
            "stored lineage selected logs over {}..={} are missing raw code/transaction/receipt companion rows (missing code: 1, transactions: 0, receipts: 0)",
            current_block_number + 1,
            current_block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
        )),
        "companion refusal must report the range and per-kind counts: {refusal}"
    );
    assert!(
        refusal.contains("rerun hash-pinned backfill for the selected range before retrying"),
        "companion refusal must include an actionable remedy: {refusal}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A family-topic0 log emitted before the address's watched active window
/// opened was never fetch-selected, so its missing code companion must not
/// block promotion.
#[tokio::test]
async fn reconcile_fetched_heads_promotes_when_selected_topic_log_predates_watched_window()
-> Result<()> {
    let log_block_number = 11;
    let (outcome, current_block_number, database, server) = companion_scope_promotion_scenario(
        10_062,
        &family_selected_test_topic0(),
        false,
        Some(log_block_number + 1),
    )
    .await?;

    let (task, outcome) = outcome
        .expect("an out-of-window log must not block stored lineage promotion")
        .expect("stored lineage promotion must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current_block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Companion validation follows the watched tuple's historical block
/// interval: missing companions for an in-interval log refuse promotion even
/// after the tuple is retired, while unrelated logs after that interval do
/// not widen the retained-fact requirement.
#[tokio::test]
async fn reconcile_fetched_heads_checks_historical_selected_log_companions_within_active_interval()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let stored_safe_block_number = chunk + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    let promoted_target = blocks
        .iter()
        .find(|block| block.block_number == current.block_number + chunk)
        .expect("test chain must include the first promoted target")
        .clone();
    let selected_log_block = blocks[9].clone();
    let post_interval_log_block = blocks[10].clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }

    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_079,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_079),
        selected_address,
    )
    .await?;
    insert_selected_raw_log_inputs(
        database.pool(),
        chain,
        &[selected_log_block.clone(), post_interval_log_block.clone()],
        selected_address,
        false,
    )
    .await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = $1,
            deactivated_at = now()
        WHERE chain_id = $2
          AND LOWER(address) = $3
        "#,
    )
    .bind(selected_log_block.block_number)
    .bind(chain)
    .bind(selected_address)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        DELETE FROM raw_receipts
        WHERE chain_id = $1
          AND block_hash IN ($2, $3)
        "#,
    )
    .bind(chain)
    .bind(&selected_log_block.block_hash)
    .bind(&post_interval_log_block.block_hash)
    .execute(database.pool())
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![
        latest.clone(),
        stored_safe.clone(),
        promoted_target.clone(),
    ])
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let coverage_frontiers = ChainCoverageFrontiers::default();

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest.clone(),
            safe: Some(stored_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("a retired tuple's in-interval selected log must retain its receipt companion");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("missing raw code/transaction/receipt companion rows")
            && rendered.contains("receipts: 1"),
        "unexpected historical missing-companion refusal: {rendered}"
    );
    assert_eq!(
        bigname_storage::load_chain_checkpoint(database.pool(), chain)
            .await?
            .expect("checkpoint row must remain after companion refusal"),
        task.checkpoint
    );

    insert_selected_raw_log_inputs(
        database.pool(),
        chain,
        &[selected_log_block],
        selected_address,
        false,
    )
    .await?;
    let (promoted_task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .context("post-interval logs without companions must not refuse promotion")?
    .expect("the repaired historical interval must promote");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        promoted_task.checkpoint.canonical_block_hash,
        Some(promoted_target.block_hash)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A family-topic scan selects B's manifest topic and retains watched A's
/// different-topic log only because both logs share a transaction. The
/// transaction and receipt are shared companions, but only selected emitter B
/// is supposed to carry a code observation.
#[tokio::test]
async fn reconcile_fetched_heads_ignores_unselected_watched_same_transaction_sibling_for_code()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let stored_safe_block_number = chunk + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks[0].clone();
    let selected_log_block = blocks[9].clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in blocks.iter().take(stored_safe_block_number as usize) {
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }

    let selected_address = "0x00000000000000000000000000000000000000b2";
    let sibling_address = "0x00000000000000000000000000000000000000a1";
    let manifest_id = 10_080;
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        manifest_id,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_080),
        selected_address,
    )
    .await?;
    insert_contract_instance(database.pool(), Uuid::from_u128(10_081), chain, "contract").await?;
    insert_active_contract_instance_address(
        database.pool(),
        Uuid::from_u128(10_081),
        chain,
        sibling_address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        manifest_id,
        "SiblingContract",
        Uuid::from_u128(10_081),
        sibling_address,
        "none",
        None,
        None,
    )
    .await?;

    insert_selected_raw_log_inputs(
        database.pool(),
        chain,
        std::slice::from_ref(&selected_log_block),
        selected_address,
        false,
    )
    .await?;
    let selected_topic = format!("{:#x}", keccak256(b"NewResolver(bytes32,address)"));
    sqlx::query(
        r#"
        UPDATE raw_logs
        SET topics = ARRAY[$1]::TEXT[]
        WHERE chain_id = $2
          AND block_hash = $3
          AND LOWER(emitting_address) = $4
        "#,
    )
    .bind(&selected_topic)
    .bind(chain)
    .bind(&selected_log_block.block_hash)
    .bind(selected_address)
    .execute(database.pool())
    .await?;
    let transaction_hash = format!("0x{:064x}", selected_log_block.block_number + 10_000);
    bigname_storage::upsert_raw_logs(
        database.pool(),
        &[bigname_storage::RawLog {
            chain_id: chain.to_owned(),
            block_hash: selected_log_block.block_hash.clone(),
            block_number: selected_log_block.block_number,
            transaction_hash,
            transaction_index: 0,
            log_index: 1,
            emitting_address: sibling_address.to_owned(),
            topics: vec![
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned(),
            ],
            data: vec![2],
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let family_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        json!({"source_identity_hash": "test:family-topic-scan-sibling"}),
        "family-topic-scan-sibling",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        family_job_id,
        &[family_coverage_fact(
            "test_source_family",
            current.block_number + 1,
            stored_safe_block_number,
        )],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 2,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (_, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("an unselected same-transaction sibling must not require a code observation")
    .expect("selected B and its shared transaction companions must promote");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_event_silent_catchup_promotes_then_live_tip_observes_current_call()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let event_silent_address = "0x0000000000000000000000000000000000000002";
    let no_topic_address = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31";
    let no_topic_manifest_id = 12_510;
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        no_topic_manifest_id,
        "basenames",
        chain,
        "basenames_execution",
        Uuid::from_u128(12_510),
        no_topic_address,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE manifest_versions
        SET manifest_payload = jsonb_set(manifest_payload, '{abi,events}', '[]'::jsonb)
        WHERE manifest_id = $1
        "#,
    )
    .bind(no_topic_manifest_id)
    .execute(database.pool())
    .await?;
    let current_block_number = 1;
    let stored_safe_block_number =
        current_block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let live_latest_block_number = stored_safe_block_number + 1;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .iter()
        .find(|block| block.block_number == current_block_number)
        .expect("test chain must include the current checkpoint block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    let latest = blocks
        .iter()
        .find(|block| block.block_number == live_latest_block_number)
        .expect("test chain must include the live latest block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![no_topic_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, promotion_outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest.clone(),
            safe: Some(stored_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[event_silent_address.to_owned()],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("historic promotion must advance with event-silent enabled");

    assert_eq!(
        promotion_outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(stored_safe_block_number)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_payload_cache_metadata")
            .fetch_one(database.pool())
            .await?,
        0,
        "historic promotion must not fetch full payloads for latest-only event-silent state"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM event_silent_resolver_call_observations"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "historic promotion must not synthesize event-silent direct-call observations"
    );

    let (_task, live_outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[event_silent_address.to_owned()],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("ordinary live reconciliation must resume after historic promotion");

    assert_eq!(
        live_outcome.canonical_status,
        CanonicalReconciliationStatus::Appended
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM raw_payload_cache_metadata WHERE payload_kind = $1"
        )
        .bind(provider::RAW_PAYLOAD_KIND_FULL_BLOCK)
        .fetch_one(database.pool())
        .await?,
        1,
        "live-tip reconciliation must still retain the current full payload"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM event_silent_resolver_call_observations"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "live-tip reconciliation must still record current event-silent direct-call observations"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_promotes_stored_anchor_at_parent_fetch_depth_limit() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let current_block_number = 1;
    let stored_anchor_block_number = current_block_number + 1;
    let parent_fetch_depth = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS * 4;
    let safe_block_number = stored_anchor_block_number + parent_fetch_depth;
    let blocks = linear_provider_blocks(safe_block_number);
    let current = blocks
        .iter()
        .find(|block| block.block_number == current_block_number)
        .expect("test chain must include current block")
        .clone();
    let stored_anchor = blocks
        .iter()
        .find(|block| block.block_number == stored_anchor_block_number)
        .expect("test chain must include stored anchor")
        .clone();
    let safe = blocks
        .last()
        .expect("test chain must include safe block")
        .clone();

    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &current,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &stored_anchor,
        CanonicalityState::Safe,
    )
    .await?;
    // A stored canonical row above the provider safe candidate keeps the
    // primary stored-frontier anchor strategy out of play (its height exceeds
    // every candidate), so this test still exercises the strategy-2
    // parent-hash walk at its depth limit.
    let stray_stored_frontier = provider_block(
        &format!("0x{:064x}", 0xf00d_f00d_u64),
        None,
        safe_block_number + 5,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &stray_stored_frontier,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        stored_anchor_block_number,
        stored_anchor_block_number,
        &[],
    )
    .await?;

    let provider_blocks = blocks
        .iter()
        .filter(|block| block.block_number >= stored_anchor_block_number)
        .cloned()
        .collect::<Vec<_>>();
    let (provider, server) = bundle_provider(provider_blocks).await?;
    let checkpoint = ChainCheckpoint {
        chain_id: chain.to_owned(),
        canonical_block_hash: Some(current.block_hash.clone()),
        canonical_block_number: Some(current.block_number),
        safe_block_hash: None,
        safe_block_number: None,
        finalized_block_hash: None,
        finalized_block_number: None,
    };

    let heartbeat_instance_id = "canonical-reconciliation-in-flight-progress-test";
    install_stale_indexer_heartbeat(database.pool(), heartbeat_instance_id).await?;
    let (mut progress, progress_handle) = BlockingHeartbeatProgress::new(
        heartbeat_instance_id,
        vec![chain.to_owned()],
        2,
    );
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let mut operation = Box::pin(reconcile_canonical_head_with_adapter_progress(
        database.pool(),
        &provider,
        chain,
        &checkpoint,
        &safe,
        HeaderAuditMode::Minimal,
        std::slice::from_ref(&safe),
        &coverage_frontiers,
        &mut progress,
    ));
    tokio::time::timeout(tokio::time::Duration::from_secs(10), async {
        tokio::select! {
            () = progress_handle.wait_until_blocked() => Ok(()),
            result = operation.as_mut() => Err(anyhow::anyhow!(
                "canonical reconciliation completed before its later progress boundary blocked: {result:?}"
            )),
        }
    })
    .await
    .context("canonical reconciliation did not reach its later progress boundary")??;
    let heartbeat = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        heartbeat_instance_id,
    )
    .await?
    .context("canonical reconciliation progress heartbeat must remain registered")?;
    assert!(
        heartbeat.age_seconds <= 1,
        "an earlier fetched ancestor must beat before the later parent walk finishes"
    );

    assert!(
        progress_handle.record_count() >= 2,
        "the depth-limit parent walk must expose a later in-flight progress boundary"
    );
    progress_handle.resume();
    let outcome = tokio::time::timeout(tokio::time::Duration::from_secs(30), operation.as_mut())
        .await
        .context("canonical reconciliation did not finish after progress resumed")??;
    drop(operation);
    assert!(
        progress_handle.record_count() >= usize::try_from(parent_fetch_depth)?,
        "the depth-limit parent walk must beat after each fetched ancestor"
    );

    assert_eq!(
        outcome.status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        outcome
            .canonical
            .expect("promotion must return a checkpoint")
            .block_number,
        stored_anchor_block_number
    );
    assert_eq!(outcome.fetched_parent_count, 0);

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_refuses_without_fetching_past_parent_depth_limit() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let current_block_number = 1;
    let boundary_block_number = current_block_number + 1;
    let parent_fetch_depth = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS * 4;
    let safe_block_number = boundary_block_number + parent_fetch_depth;
    let blocks = linear_provider_blocks(safe_block_number);
    let current = blocks
        .iter()
        .find(|block| block.block_number == current_block_number)
        .expect("test chain must include current block")
        .clone();
    let safe = blocks
        .last()
        .expect("test chain must include safe block")
        .clone();

    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &current,
        CanonicalityState::Canonical,
    )
    .await?;

    let provider_blocks = blocks
        .iter()
        .filter(|block| block.block_number >= boundary_block_number)
        .cloned()
        .collect::<Vec<_>>();
    let (provider, server) = bundle_provider(provider_blocks).await?;
    let checkpoint = ChainCheckpoint {
        chain_id: chain.to_owned(),
        canonical_block_hash: Some(current.block_hash.clone()),
        canonical_block_number: Some(current.block_number),
        safe_block_hash: None,
        safe_block_number: None,
        finalized_block_hash: None,
        finalized_block_number: None,
    };

    let error = reconcile_canonical_head(
        database.pool(),
        &provider,
        chain,
        &checkpoint,
        &safe,
        HeaderAuditMode::Minimal,
        std::slice::from_ref(&safe),
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("missing stored anchor inside the parent-fetch bound must refuse");
    assert!(
        format!("{error:#}").contains("within 4096 parent fetches"),
        "unexpected anchor-depth refusal error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Provider RPC failures during the strategy-2 parent-hash walk now propagate
/// as reconcile errors instead of being swallowed into a "no stored anchor"
/// refusal (which used to silently fall through to the finalized candidate).
#[tokio::test]
async fn reconcile_fetched_heads_propagates_stored_anchor_walk_rpc_errors() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_003,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_003),
        selected_address,
    )
    .await?;
    let stored_finalized_block_number =
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 7;
    let provider_safe_block_number = stored_finalized_block_number + 50;
    let live_latest_block_number = provider_safe_block_number + 25;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let provider_safe = blocks
        .iter()
        .find(|block| block.block_number == provider_safe_block_number)
        .expect("test chain must include the provider safe block")
        .clone();
    let stored_finalized = blocks
        .iter()
        .find(|block| block.block_number == stored_finalized_block_number)
        .expect("test chain must include the stored finalized block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_finalized_block_number {
            continue;
        }
        let state = if block.block_number == stored_finalized_block_number {
            CanonicalityState::Finalized
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    // Keep the stored frontier above every provider candidate so the primary
    // stored-frontier anchor strategy is skipped and the strategy-2 parent
    // walk (whose RPC failure this test exercises) runs.
    let stray_stored_frontier = provider_block(
        &format!("0x{:064x}", 0xf00d_f00e_u64),
        None,
        provider_safe_block_number + 10,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &stray_stored_frontier,
        CanonicalityState::Canonical,
    )
    .await?;

    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_finalized_block_number,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let safe_parent_hash = provider_safe
        .parent_hash
        .clone()
        .expect("test provider safe block must have a parent");
    let (provider, server) = bundle_provider(vec![
        latest.clone(),
        provider_safe.clone(),
        stored_finalized.clone(),
    ])
    .await?;
    let provider = HashFailingProvider {
        inner: &provider,
        failing_hash: safe_parent_hash,
    };
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(provider_safe),
            finalized: Some(stored_finalized),
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("a provider failure during the stored-anchor parent walk must surface as an error");
    assert!(
        format!("{error:#}").contains("test provider intentionally cannot serve block hash"),
        "unexpected walk RPC failure error: {error:#}"
    );
    let persisted_checkpoint = bigname_storage::load_chain_checkpoint(database.pool(), chain)
        .await?
        .expect("checkpoint row must survive the failed poll");
    assert_eq!(
        persisted_checkpoint.canonical_block_number,
        Some(current.block_number),
        "a propagated walk error must not advance the checkpoint"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Coverage rows attached to a pending job or extending outside a completed
/// job's declared range must not authorize stored-lineage promotion.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_watched_tuple_with_only_untrusted_coverage_facts()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_055,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_055),
        selected_address,
    )
    .await?;
    insert_selected_raw_log_inputs(
        database.pool(),
        chain,
        &[blocks[10].clone()],
        selected_address,
        false,
    )
    .await?;
    let required_from_block = current.block_number + 1;
    let required_to_block =
        current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let pending_job = bigname_storage::create_backfill_job(
        database.pool(),
        &backfill_job_create(
            chain,
            required_from_block,
            required_to_block,
            &[selected_address],
            "untrusted-pending-coverage",
        ),
    )
    .await?;
    insert_untrusted_backfill_coverage_fact_row(
        database.pool(),
        pending_job.job.backfill_job_id,
        chain,
        "test_source_family",
        selected_address,
        required_from_block,
        required_to_block,
    )
    .await?;
    let outside_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        required_from_block + 1,
        required_to_block - 1,
        serde_json::json!({"fixture": "untrusted-outside-job-coverage"}),
        "untrusted-outside-job-coverage",
    )
    .await?;
    insert_untrusted_backfill_coverage_fact_row(
        database.pool(),
        outside_job_id,
        chain,
        "test_source_family",
        selected_address,
        required_from_block,
        required_to_block,
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("stored lineage promotion must refuse untrusted coverage facts");
    assert!(
        format!("{error:#}").contains("do not form gap-free coverage"),
        "unexpected uncovered-tuple refusal error: {error:#}"
    );
    assert!(
        format!("{error:#}").contains(&format!(
            "(source_family test_source_family, address {selected_address}, blocks {}..={})",
            current.block_number + 1,
            current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
        )),
        "uncovered-tuple refusal should name the violating tuple: {error:#}"
    );
    assert!(
        format!("{error:#}").contains("run hash-pinned or Coinbase SQL backfill"),
        "uncovered-tuple refusal should include an actionable remedy: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_promotes_completed_coverage_with_orphaned_same_height_repair_row()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }

    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_026,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_026),
        selected_address,
    )
    .await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;

    let ambiguous_block_number = current.block_number + 2;
    let ambiguous_parent = blocks
        .iter()
        .find(|block| block.block_number == ambiguous_block_number - 1)
        .expect("test chain must include ambiguous parent");
    let ambiguous_fork = provider_block(
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff02",
        Some(&ambiguous_parent.block_hash),
        ambiguous_block_number,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &ambiguous_fork,
        CanonicalityState::Orphaned,
    )
    .await?;

    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("orphaned repaired fork rows must not make completed coverage ambiguous")
    .expect("stored lineage promotion must advance despite orphaned same-height rows");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_refuses_non_orphan_same_height_fork_before_coverage() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }

    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;

    let ambiguous_block_number = current.block_number + 2;
    let ambiguous_parent = blocks
        .iter()
        .find(|block| block.block_number == ambiguous_block_number - 1)
        .expect("test chain must include ambiguous parent");
    let ambiguous_fork = provider_block(
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff02",
        Some(&ambiguous_parent.block_hash),
        ambiguous_block_number,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &ambiguous_fork,
        CanonicalityState::Canonical,
    )
    .await?;

    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("non-orphan same-height fork coverage must remain ambiguous");
    assert!(
        format!("{error:#}").contains("incomplete or has duplicate canonical children"),
        "unexpected ambiguous-fork refusal error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_refuses_observed_same_height_fork_before_coverage() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }

    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_051,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_051),
        selected_address,
    )
    .await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;

    let ambiguous_block_number = current.block_number + 2;
    let ambiguous_parent = blocks
        .iter()
        .find(|block| block.block_number == ambiguous_block_number - 1)
        .expect("test chain must include ambiguous parent");
    let ambiguous_fork = provider_block(
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff03",
        Some(&ambiguous_parent.block_hash),
        ambiguous_block_number,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &ambiguous_fork,
        CanonicalityState::Observed,
    )
    .await?;

    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("observed same-height fork coverage must remain ambiguous");
    assert!(
        format!("{error:#}").contains("non-orphaned same-height fork"),
        "unexpected observed-fork refusal error: {error:#}"
    );
    assert!(
        format!("{error:#}").contains(&format!("at block {ambiguous_block_number}")),
        "observed-fork refusal should name the forked height: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A stored safe anchor whose parents are unknown must not let a uniquely
/// canonical-marked stale-fork path promote. The provider's block at the
/// promoted height disagrees with the stored target here, even though both the
/// target and the disconnected safe anchor retain canonical markings.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_unlinked_stale_fork_target() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_anchor_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 7;
    let live_latest_block_number = stored_anchor_block_number + 25;
    let canonical_blocks =
        linear_provider_blocks(crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 1);
    let current = canonical_blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    for block in &canonical_blocks {
        insert_chain_lineage_for_block(database.pool(), chain, block, CanonicalityState::Canonical)
            .await?;
    }
    let stored_anchor = provider_block(
        "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffaaa",
        Some("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffaa9"),
        stored_anchor_block_number,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &stored_anchor,
        CanonicalityState::Safe,
    )
    .await?;
    let latest = provider_block(
        "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffbbb",
        Some(&stored_anchor.block_hash),
        live_latest_block_number,
    );
    let stored_target = canonical_blocks
        .last()
        .expect("test chain must include a promotion target");
    let provider_target = provider_block(
        "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffccc",
        stored_target.parent_hash.as_deref(),
        stored_target.block_number,
    );
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_anchor_block_number,
        &[],
    )
    .await?;
    let checkpoint = ChainCheckpoint {
        chain_id: chain.to_owned(),
        canonical_block_hash: Some(current.block_hash.clone()),
        canonical_block_number: Some(current.block_number),
        safe_block_hash: None,
        safe_block_number: None,
        finalized_block_hash: None,
        finalized_block_number: None,
    };
    let (provider, server) =
        bundle_provider(vec![latest.clone(), stored_anchor.clone(), provider_target]).await?;

    let error = reconcile_canonical_head(
        database.pool(),
        &provider,
        chain,
        &checkpoint,
        &latest,
        HeaderAuditMode::Minimal,
        std::slice::from_ref(&stored_anchor),
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("a provider-disputed stored target must refuse checkpoint promotion");
    assert!(
        format!("{error:#}").contains("provider block at the promoted height"),
        "unexpected stale-fork refusal error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_promotes_when_current_target_is_inactive_for_early_covered_blocks()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let selected_address = "0x0000000000000000000000000000000000000001";
    let target_start_block = 50;
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_030,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_030),
        selected_address,
    )
    .await?;
    sqlx::query(
        "UPDATE contract_instance_addresses SET active_from_block_number = $1 WHERE contract_instance_id = $2",
    )
    .bind(target_start_block)
    .bind(Uuid::from_u128(10_030))
    .execute(database.pool())
    .await?;

    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let source_identity = source_identity_with_selected_targets(vec![json!({
        "source_family": "test_source_family",
        "contract_instance_id": Uuid::from_u128(10_030),
        "address": selected_address,
        "effective_from_block": target_start_block,
        "effective_to_block": stored_safe_block_number
    })]);
    let backfill_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        source_identity,
        "inactive-early-blocks",
    )
    .await?;
    // The fact starts at the contract's activation block: the requirement side
    // must clip the tuple's interval to its active window, so path blocks
    // before `target_start_block` need no coverage row at all.
    insert_backfill_coverage_fact_rows(
        database.pool(),
        backfill_job_id,
        &[address_coverage_fact(
            "test_source_family",
            selected_address,
            target_start_block,
            stored_safe_block_number,
        )],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("completed coverage must ignore selected targets before their active interval")
    .expect("stored lineage promotion must advance with interval-aware coverage");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Coverage facts are keyed by (source_family, address): the same watched
/// address under two families needs a fact for each family. A fact for
/// family A must not credit family B's tuple; adding family B's own fact
/// unblocks promotion.
#[tokio::test]
async fn reconcile_fetched_heads_multi_family_address_requires_its_own_family_coverage()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let shared_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_040,
        "test",
        chain,
        "test_source_family_a",
        Uuid::from_u128(10_040),
        shared_address,
    )
    .await?;
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_041,
        "test",
        chain,
        "test_source_family_b",
        Uuid::from_u128(10_041),
        shared_address,
    )
    .await?;

    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        "test_source_family_a",
        &[shared_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![shared_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 2,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest.clone(),
            safe: Some(stored_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("family A's coverage fact must not credit family B's tuple for the same address");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("do not form gap-free coverage"),
        "unexpected cross-family refusal error: {rendered}"
    );
    assert!(
        rendered.contains(&format!(
            "(source_family test_source_family_b, address {shared_address}, blocks {}..={})",
            current.block_number + 1,
            current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
        )),
        "cross-family refusal must name family B's uncovered tuple: {rendered}"
    );
    assert!(
        !rendered.contains("(source_family test_source_family_a"),
        "family A's covered tuple must not be reported as violating: {rendered}"
    );

    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        "test_source_family_b",
        &[shared_address],
    )
    .await?;
    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("per-family facts for both tuples must prove the stored lineage path")
    .expect("stored lineage promotion must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A coverage fact that starts after the tuple's required interval must not
/// credit the tuple: containment is against the full required interval, so a
/// fact missing the early path blocks refuses promotion.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_coverage_fact_interval_missing_early_path_blocks()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_056,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_056),
        selected_address,
    )
    .await?;
    let source_identity = source_identity_with_selected_targets(vec![json!({
        "source_family": "test_source_family",
        "contract_instance_id": Uuid::from_u128(10_056),
        "address": selected_address,
        "effective_from_block": current.block_number + 50,
        "effective_to_block": stored_safe_block_number
    })]);
    let backfill_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        source_identity,
        "interval-miss",
    )
    .await?;
    // The fact starts 50 blocks in, but the watched tuple is active from the
    // path start: its required interval is not contained in this fact.
    insert_backfill_coverage_fact_rows(
        database.pool(),
        backfill_job_id,
        &[address_coverage_fact(
            "test_source_family",
            selected_address,
            current.block_number + 50,
            stored_safe_block_number,
        )],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("coverage must not pass before the fact interval starts");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("do not form gap-free coverage"),
        "unexpected interval-miss refusal error: {rendered}"
    );
    assert!(
        rendered.contains(&format!(
            "(source_family test_source_family, address {selected_address}, blocks {}..={})",
            current.block_number + 1,
            current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
        )),
        "interval-miss refusal must name the uncovered tuple: {rendered}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_refuses_stored_safe_lineage_hole_despite_completed_backfill_coverage()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let missing_block_number = 10;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number
            || block.block_number == missing_block_number
        {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("stored lineage promotion must refuse a canonical child-path hole");
    assert!(
        format!("{error:#}").contains("stored lineage path from checkpoint"),
        "unexpected lineage-hole refusal error: {error:#}"
    );
    assert!(
        format!("{error:#}").contains("incomplete or has duplicate canonical children"),
        "lineage-hole refusal should name the path problem: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_refuses_incomplete_backfill_crash_residue_without_completed_range()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=live_latest_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_057,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_057),
        selected_address,
    )
    .await?;
    insert_incomplete_backfill_range_residue(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("stored lineage promotion must refuse incomplete crash-residue ranges");
    assert!(
        format!("{error:#}").contains("do not form gap-free coverage"),
        "unexpected incomplete-range refusal error: {error:#}"
    );
    assert!(
        error.to_string().contains("exceeds live gap fill limit"),
        "incomplete-range refusal should still be tied to the live gap gate: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A family-scoped fact covers every watched address of that family over the
/// range — and only that family: tuples of other families still need their
/// own facts.
#[tokio::test]
async fn reconcile_fetched_heads_family_scope_fact_credits_all_addresses_of_that_family_only()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let family_a_address_x = "0x00000000000000000000000000000000000000a1";
    let family_a_address_y = "0x00000000000000000000000000000000000000a2";
    let family_b_address_z = "0x00000000000000000000000000000000000000b1";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_070,
        "test",
        chain,
        "test_source_family_a",
        Uuid::from_u128(10_070),
        family_a_address_x,
    )
    .await?;
    // Second family-A contract lives in another namespace: manifest_versions
    // is unique per (namespace, source_family, chain, epoch, version).
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_071,
        "test_second",
        chain,
        "test_source_family_a",
        Uuid::from_u128(10_071),
        family_a_address_y,
    )
    .await?;
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_072,
        "test",
        chain,
        "test_source_family_b",
        Uuid::from_u128(10_072),
        family_b_address_z,
    )
    .await?;

    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let family_scan_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        source_identity_with_selected_targets(Vec::new()),
        "family-scope-scan",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        family_scan_job_id,
        &[family_coverage_fact(
            "test_source_family_a",
            current.block_number + 1,
            stored_safe_block_number,
        )],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![
            family_a_address_x.to_owned(),
            family_a_address_y.to_owned(),
            family_b_address_z.to_owned(),
        ],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 3,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest.clone(),
            safe: Some(stored_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("family A's family-scoped fact must not credit family B's tuple");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains(&format!(
            "(source_family test_source_family_b, address {family_b_address_z}, blocks {}..={})",
            current.block_number + 1,
            current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
        )),
        "family-scope refusal must name family B's uncovered tuple: {rendered}"
    );
    assert!(
        !rendered.contains(family_a_address_x) && !rendered.contains(family_a_address_y),
        "family A's addresses are family-fact covered and must not be reported: {rendered}"
    );

    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        "test_source_family_b",
        &[family_b_address_z],
    )
    .await?;
    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("family fact plus family B's address fact must prove the stored lineage path")
    .expect("stored lineage promotion must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// The default ops-catchup shape completes one 32-block job at a time. Its
/// independently persisted facts must form one gap-free coverage interval
/// across a full 1,024-block stored-lineage promotion slice.
#[tokio::test]
async fn reconcile_fetched_heads_promotes_coverage_spanning_default_ops_catchup_facts() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_073,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_073),
        selected_address,
    )
    .await?;

    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let promotion_through =
        current.block_number + crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let ops_chunk = crate::ops_catchup::DEFAULT_OPS_CATCHUP_CHUNK_BLOCKS;
    assert_eq!(
        ops_chunk, 32,
        "regression must model the shipped ops-catchup default"
    );
    assert_eq!(
        (promotion_through - current.block_number) / ops_chunk,
        32,
        "one promotion slice must span 32 default ops-catchup jobs"
    );
    let omitted_fact_from = current.block_number + ops_chunk * 10 + 1;
    let omitted_fact_through = omitted_fact_from + ops_chunk - 1;
    let mut fact_from = current.block_number + 1;
    while fact_from <= promotion_through {
        let fact_through = (fact_from + ops_chunk - 1).min(promotion_through);
        if fact_from != omitted_fact_from {
            insert_completed_backfill_range_coverage(
                database.pool(),
                chain,
                fact_from,
                fact_through,
                &[selected_address],
            )
            .await?;
        }
        fact_from = fact_through + 1;
    }
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest.clone(),
            safe: Some(stored_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("one missing ops-catchup fact must leave a promotion coverage gap");
    assert!(
        format!("{error:#}").contains("do not form gap-free coverage"),
        "unexpected fragmented-coverage refusal: {error:#}"
    );

    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        omitted_fact_from,
        omitted_fact_through,
        &[selected_address],
    )
    .await?;

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("contiguous default ops-catchup facts must prove the stored lineage path")
    .expect("stored lineage promotion must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(promotion_through)
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// One shared `ChainCoverageFrontiers` across successive poll cycles: two
/// promotions extend the verified frontier incrementally, and a third slice
/// with a coverage hole refuses naming the concrete uncovered tuple.
#[tokio::test]
async fn reconcile_fetched_heads_verified_frontier_extends_incrementally_across_polls() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_074,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_074),
        selected_address,
    )
    .await?;

    let stored_safe_block_number = chunk * 3 + 7;
    let covered_through_block = chunk * 2 + 951;
    let live_latest_block_number = stored_safe_block_number + 25;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    // Facts stop short of the stored safe head: the third promotion slice has
    // a coverage hole.
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        covered_through_block,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let heads = ProviderHeadSnapshot {
        canonical: latest,
        safe: Some(stored_safe),
        finalized: None,
    };
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, first_outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("first covered slice must promote")
    .expect("first promotion must advance the checkpoint");
    assert_eq!(
        first_outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + chunk)
    );

    let (task, second_outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("second covered slice must promote against the shared frontier")
    .expect("second promotion must advance the checkpoint");
    assert_eq!(
        second_outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + chunk * 2)
    );

    let third_slice_from = current.block_number + chunk * 2 + 1;
    let third_slice_through = current.block_number + chunk * 3;
    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("the slice with a coverage hole must refuse promotion");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("do not form gap-free coverage"),
        "unexpected frontier-hole refusal error: {rendered}"
    );
    assert!(
        rendered.contains(&format!(
            "(source_family test_source_family, address {selected_address}, blocks {third_slice_from}..={third_slice_through})"
        )),
        "frontier-hole refusal must name the concrete uncovered tuple: {rendered}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A fresh process-local coordinator reuses the durable proof after all
/// completed-job facts are removed as a test-only detector. Reuse does not
/// bypass the live same-height-fork check on the next promotion attempt.
#[tokio::test]
async fn reconcile_fetched_heads_reuses_persisted_coverage_after_restart() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let first_process = ChainCoverageFrontiers::default();
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &first_process).await?;
    let persisted_before_restart =
        bigname_storage::load_stored_lineage_coverage_frontier_header(database.pool(), chain)
            .await?
            .expect("the first promotion must publish durable coverage");
    drop(first_process);
    sqlx::query("DELETE FROM backfill_coverage_facts WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await
        .context("failed to remove facts after durable frontier publication")?;

    let restarted_process = ChainCoverageFrontiers::default();
    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &restarted_process,
    )
    .await
    .expect("a restarted indexer must reuse an unchanged durable frontier")
    .expect("durable frontier reuse must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        bigname_storage::load_stored_lineage_coverage_frontier_header(database.pool(), chain)
            .await?
            .expect("the frontier remains durable")
            .snapshot_revision,
        persisted_before_restart.snapshot_revision,
        "unchanged restart reuse must not republish the requirement snapshot"
    );
    assert!(
        restarted_process
            .take_required_tuple_range_scans_for_tests(chain)
            .is_empty(),
        "restart reuse validates saved row shape without re-verifying historical fact intervals"
    );

    let fork_block_number = task
        .checkpoint
        .canonical_block_number
        .expect("restart promotion must persist its checkpoint")
        + 1;
    let fork = provider_block(
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff41",
        Some(&format!("0x{:064x}", fork_block_number - 1)),
        fork_block_number,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &fork,
        CanonicalityState::Observed,
    )
    .await?;
    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("durable coverage must not suppress the live fork refusal");
    assert!(format!("{error:#}").contains("non-orphaned same-height fork"));

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A saved proof whose lower bound is above the requested stored path is not
/// eligible. The indexer must reprove the full current candidate before CAS
/// replacement, leaving the narrowed revision unchanged when facts are absent.
#[tokio::test]
async fn reconcile_fetched_heads_deep_regression_cold_reproves_before_replacement() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let selected_address = "0x0000000000000000000000000000000000000001";
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    let original =
        bigname_storage::load_stored_lineage_coverage_frontier_header(database.pool(), chain)
            .await?
            .expect("the first promotion must publish durable coverage");
    let requested_from = task
        .checkpoint
        .canonical_block_number
        .expect("the first promotion must advance its checkpoint")
        + 1;
    let narrowed_from = requested_from + 100;
    let original_fact_from = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT MIN(covered_from_block)::BIGINT
        FROM backfill_coverage_facts
        WHERE chain_id = $1
          AND address = $2
        "#,
    )
    .bind(chain)
    .bind(selected_address)
    .fetch_one(database.pool())
    .await?;
    assert!(narrowed_from < original.verified_through_block);
    sqlx::query(
        r#"
        UPDATE stored_lineage_coverage_frontiers
        SET verified_from_block = $2
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .bind(narrowed_from)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE stored_lineage_coverage_frontier_requirements
        SET required_intervals = required_intervals
            * int8multirange(int8range($2, $3 + 1, '[)'))
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .bind(narrowed_from)
    .bind(original.verified_through_block)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        WITH row_hashes AS (
            SELECT md5(
                jsonb_build_array(source_family, address, required_intervals::TEXT)::TEXT
            ) AS row_hash
            FROM stored_lineage_coverage_frontier_requirements
            WHERE chain_id = $1
        ), integrity AS (
            SELECT
                COUNT(*)::BIGINT AS row_count,
                LPAD(to_hex(COALESCE(bit_xor(('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT), 0)), 16, '0')
                || LPAD(to_hex(COALESCE(bit_xor(('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT), 0)), 16, '0')
                    AS digest
            FROM row_hashes
        )
        UPDATE stored_lineage_coverage_frontiers header
        SET requirement_row_count = integrity.row_count,
            requirement_digest = integrity.digest
        FROM integrity
        WHERE header.chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE backfill_coverage_facts
        SET covered_from_block = $2
        WHERE chain_id = $1
          AND address = $3
        "#,
    )
    .bind(chain)
    .bind(narrowed_from)
    .bind(selected_address)
    .execute(database.pool())
    .await?;

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("deep regression must not reuse a proof whose lower bound is too high");
    assert!(format!("{error:#}").contains("do not form gap-free coverage"));
    let refused =
        bigname_storage::load_stored_lineage_coverage_frontier_header(database.pool(), chain)
            .await?
            .expect("the narrowed proof remains after refusal");
    assert_eq!(refused.snapshot_revision, original.snapshot_revision);
    assert_eq!(refused.verified_from_block, narrowed_from);

    sqlx::query(
        r#"
        UPDATE backfill_coverage_facts
        SET covered_from_block = $3
        WHERE chain_id = $1
          AND address = $2
        "#,
    )
    .bind(chain)
    .bind(selected_address)
    .bind(original_fact_from)
    .execute(database.pool())
    .await?;
    let (_, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("restored full facts must permit cold reproving")
    .expect("cold reproving must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    let repaired =
        bigname_storage::load_stored_lineage_coverage_frontier_header(database.pool(), chain)
            .await?
            .expect("cold reproving must replace the saved frontier");
    assert_eq!(repaired.snapshot_revision, original.snapshot_revision + 1);
    assert!(repaired.verified_from_block < narrowed_from);

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Strategy-1 anchoring: when the provider's block at the stored canonical
/// frontier height matches the stored hash, promotion anchors there directly
/// with no parent-hash walk — the mock provider has NO walkable parents, so
/// any walk attempt would fail the test.
#[tokio::test]
async fn reconcile_fetched_heads_promotes_from_stored_frontier_anchor_without_parent_walk()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_075,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_075),
        selected_address,
    )
    .await?;

    let stored_frontier_block_number = chunk + 7;
    // The provider safe head sits deeper than the 4096-block parent walk could
    // ever bridge; only the stored-frontier strategy can anchor.
    let provider_safe_block_number = stored_frontier_block_number + chunk * 4 + 800;
    let live_latest_block_number = provider_safe_block_number + 25;
    let blocks = linear_provider_blocks(stored_frontier_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let stored_frontier = blocks
        .last()
        .expect("test chain must include the stored frontier")
        .clone();
    for block in &blocks {
        insert_chain_lineage_for_block(database.pool(), chain, block, CanonicalityState::Canonical)
            .await?;
    }
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_frontier_block_number,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let latest = provider_block(
        &format!("0x{:064x}", 0xbeef_0001_u64),
        Some(&format!("0x{:064x}", 0xbeef_0000_u64)),
        live_latest_block_number,
    );
    let provider_safe = provider_block(
        &format!("0x{:064x}", 0x5afe_0001_u64),
        Some(&format!("0x{:064x}", 0x5afe_0000_u64)),
        provider_safe_block_number,
    );
    // Only the head and the frontier block exist on the mock: a parent walk
    // from the safe candidate would panic the fixture server.
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_frontier.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(provider_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("the stored frontier must anchor promotion without any parent walk")
    .expect("stored lineage promotion must advance the checkpoint");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + chunk)
    );
    assert_eq!(outcome.fetched_parent_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM chain_lineage WHERE chain_id = $1 AND block_number > $2"
        )
        .bind(chain)
        .bind(stored_frontier_block_number)
        .fetch_one(database.pool())
        .await?,
        0,
        "frontier anchoring must not pre-store lineage above the stored frontier"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_payload_cache_metadata")
            .fetch_one(database.pool())
            .await?,
        0,
        "historic frontier promotion must not fetch or retain full-block payloads"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Strategy-1 hash mismatch (stale fork tip) falls back to the parent walk;
/// with no walkable parents the promotion refuses rather than erroring.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_stored_frontier_hash_mismatch_without_walkable_parents()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let stored_frontier_block_number = chunk + 7;
    let provider_safe_block_number = stored_frontier_block_number + chunk * 4 + 800;
    let live_latest_block_number = provider_safe_block_number + 25;
    let blocks = linear_provider_blocks(stored_frontier_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let stored_frontier = blocks
        .last()
        .expect("test chain must include the stored frontier")
        .clone();
    for block in &blocks {
        insert_chain_lineage_for_block(database.pool(), chain, block, CanonicalityState::Canonical)
            .await?;
    }
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    // The provider's block at the stored frontier height carries a DIFFERENT
    // hash: the stored tip is a stale fork.
    let forked_frontier = provider_block(
        &format!("0x{:064x}", 0xf0f0_0001_u64),
        stored_frontier.parent_hash.as_deref(),
        stored_frontier_block_number,
    );
    let latest = provider_block(
        &format!("0x{:064x}", 0xbeef_0003_u64),
        Some(&format!("0x{:064x}", 0xbeef_0002_u64)),
        live_latest_block_number,
    );
    // The safe candidate has no parent hash, so the fallback walk terminates
    // immediately without finding a stored anchor.
    let provider_safe = provider_block(
        &format!("0x{:064x}", 0x5afe_0002_u64),
        None,
        provider_safe_block_number,
    );
    let (provider, server) = bundle_provider(vec![latest.clone(), forked_frontier]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: Vec::new(),
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(provider_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("a mismatching frontier hash without walkable parents must refuse promotion");
    assert!(
        format!("{error:#}").contains("stored-lineage checkpoint promotion requires"),
        "unexpected frontier-mismatch refusal error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Provider errors while resolving the stored frontier height surface as
/// reconcile errors, not as a "no stored anchor" refusal.
#[tokio::test]
async fn reconcile_canonical_head_propagates_stored_frontier_number_fetch_rpc_errors() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let stored_frontier_block_number = chunk + 7;
    let provider_safe_block_number = stored_frontier_block_number + 50;
    let live_latest_block_number = provider_safe_block_number + 25;
    let blocks = linear_provider_blocks(stored_frontier_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    for block in &blocks {
        insert_chain_lineage_for_block(database.pool(), chain, block, CanonicalityState::Canonical)
            .await?;
    }
    let latest = provider_block(
        &format!("0x{:064x}", 0xbeef_0005_u64),
        Some(&format!("0x{:064x}", 0xbeef_0004_u64)),
        live_latest_block_number,
    );
    let provider_safe = provider_block(
        &format!("0x{:064x}", 0x5afe_0003_u64),
        Some(&format!("0x{:064x}", 0x5afe_0004_u64)),
        provider_safe_block_number,
    );
    let (provider, server) = bundle_provider(vec![latest.clone()]).await?;
    let provider = NumberResolutionFailingProvider { inner: &provider };
    let checkpoint = ChainCheckpoint {
        chain_id: chain.to_owned(),
        canonical_block_hash: Some(current.block_hash.clone()),
        canonical_block_number: Some(current.block_number),
        safe_block_hash: None,
        safe_block_number: None,
        finalized_block_hash: None,
        finalized_block_number: None,
    };

    let error = reconcile_canonical_head(
        database.pool(),
        &provider,
        chain,
        &checkpoint,
        &latest,
        HeaderAuditMode::Minimal,
        std::slice::from_ref(&provider_safe),
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("a number-resolution RPC failure must surface as an error, not a refusal");
    assert!(
        format!("{error:#}").contains("test provider intentionally cannot resolve block numbers"),
        "unexpected number-resolution failure error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// The O(1) positional ancestry probe agrees with the recursive-CTE walk
/// wherever the candidate height has a unique canonical-marked row (including
/// orphaned-fork rejection), and conservatively returns false when two
/// canonical-marked rows share the candidate height (mid-reorg window).
#[tokio::test]
async fn chain_lineage_positional_ancestry_matches_recursive_cte_for_unique_heights() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let blocks = linear_provider_blocks(5);
    for block in &blocks {
        let state = if block.block_number == 5 {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let orphaned_fork = provider_block(
        &format!("0x{:064x}", 0x0f0f_0003_u64),
        Some(&blocks[1].block_hash),
        3,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &orphaned_fork,
        CanonicalityState::Orphaned,
    )
    .await?;
    // A second canonical-marked row at height 4 (parent unknown) emulates the
    // mid-reorg window before repair flips the losing branch to orphaned.
    let duplicate_canonical = provider_block(
        &format!("0x{:064x}", 0x0f0f_0004_u64),
        Some(&format!("0x{:064x}", 0x0f0f_0002_u64)),
        4,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &duplicate_canonical,
        CanonicalityState::Canonical,
    )
    .await?;

    let descendant = &blocks[4];
    for ancestor_number in [1_i64, 2, 3] {
        let ancestor = &blocks[usize::try_from(ancestor_number)? - 1];
        let positional = bigname_storage::chain_lineage_contains_canonical_ancestor_position(
            database.pool(),
            chain,
            &descendant.block_hash,
            descendant.block_number,
            ancestor.block_number,
            &ancestor.block_hash,
        )
        .await?;
        let recursive = bigname_storage::chain_lineage_contains_ancestor(
            database.pool(),
            chain,
            &descendant.block_hash,
            &ancestor.block_hash,
        )
        .await?;
        assert!(
            positional && recursive,
            "unique canonical ancestor at height {ancestor_number} must satisfy both checks"
        );
    }

    // Orphaned fork rows are ancestors under neither check.
    assert!(
        !bigname_storage::chain_lineage_contains_canonical_ancestor_position(
            database.pool(),
            chain,
            &descendant.block_hash,
            descendant.block_number,
            orphaned_fork.block_number,
            &orphaned_fork.block_hash,
        )
        .await?
    );
    assert!(
        !bigname_storage::chain_lineage_contains_ancestor(
            database.pool(),
            chain,
            &descendant.block_hash,
            &orphaned_fork.block_hash,
        )
        .await?
    );

    // Two canonical-marked rows at height 4: the positional probe skips
    // conservatively even though the parent walk still proves ancestry.
    assert!(
        !bigname_storage::chain_lineage_contains_canonical_ancestor_position(
            database.pool(),
            chain,
            &descendant.block_hash,
            descendant.block_number,
            blocks[3].block_number,
            &blocks[3].block_hash,
        )
        .await?,
        "an ambiguous candidate height must skip the positional fast path"
    );
    assert!(
        bigname_storage::chain_lineage_contains_ancestor(
            database.pool(),
            chain,
            &descendant.block_hash,
            &blocks[3].block_hash,
        )
        .await?,
        "the recursive walk still proves parent-linked ancestry"
    );

    database.cleanup().await?;
    Ok(())
}

/// A completed topic-filtered job whose persisted topic0 set no longer equals
/// the current manifest ABI still refuses when its fact is needed solely by a
/// retired tuple's closed historical interval.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_topic_drift_for_retired_historical_tuple() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_076,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_076),
        selected_address,
    )
    .await?;

    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = $1,
            deactivated_at = now()
        WHERE chain_id = $2
          AND LOWER(address) = $3
        "#,
    )
    .bind(stored_safe_block_number)
    .bind(chain)
    .bind(selected_address)
    .execute(database.pool())
    .await
    .context("failed to retire the historical topic-drift tuple")?;
    // A completed Coinbase-SQL job intersecting the range persisted a topic0
    // set that no longer matches the current manifest ABI. Its own fact is
    // the only evidence covering the watched tuple, so promotion relies on
    // stale topic-filtered coverage and must refuse.
    let drifted_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        json!({
            "source_identity_hash": "test:drifted-topic-plan",
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {
                    "test_source_family": [
                        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    ]
                }
            }
        }),
        "drifted-topic-plan",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        drifted_job_id,
        &[address_coverage_fact(
            "test_source_family",
            selected_address,
            current.block_number + 1,
            stored_safe_block_number,
        )],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("a drifted persisted topic0 set must refuse promotion despite complete facts");
    assert!(
        format!("{error:#}")
            .contains("manifest ABI topic0 set changed after completed backfill job"),
        "unexpected topic-drift refusal error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A stale topic-filtered job is retained for audit, but the gap-free union of
/// later reruns whose persisted topics equal the current manifest ABI replaces
/// its coverage. The stale job must not poison promotion forever.
#[tokio::test]
async fn reconcile_fetched_heads_promotes_after_current_topic_rerun_replaces_stale_coverage()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_077,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_077),
        selected_address,
    )
    .await?;

    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }

    let covered_from = current.block_number + 1;
    let stale_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        covered_from,
        stored_safe_block_number,
        json!({
            "source_identity_hash": "test:stale-topic-coverage",
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {
                    "test_source_family": [
                        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    ]
                }
            }
        }),
        "stale-topic-coverage",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        stale_job_id,
        &[address_coverage_fact(
            "test_source_family",
            selected_address,
            covered_from,
            stored_safe_block_number,
        )],
    )
    .await?;

    let current_topic0s = TEST_MANIFEST_EVENT_SIGNATURES
        .iter()
        .map(|signature| format!("{:#x}", keccak256(signature.as_bytes())))
        .collect::<Vec<_>>();
    let replacement_split = covered_from + 500;
    let first_replacement_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        covered_from,
        replacement_split,
        json!({
            "source_identity_hash": "test:current-topic-replacement-first",
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {
                    "test_source_family": current_topic0s.clone()
                }
            }
        }),
        "current-topic-replacement-first",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        first_replacement_job_id,
        &[address_coverage_fact(
            "test_source_family",
            selected_address,
            covered_from,
            replacement_split,
        )],
    )
    .await?;
    let second_replacement_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        replacement_split + 1,
        stored_safe_block_number,
        json!({
            "source_identity_hash": "test:current-topic-replacement-second",
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {
                    "test_source_family": current_topic0s
                }
            }
        }),
        "current-topic-replacement-second",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        second_replacement_job_id,
        &[address_coverage_fact(
            "test_source_family",
            selected_address,
            replacement_split + 1,
            stored_safe_block_number,
        )],
    )
    .await?;

    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (_, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect("a complete current-topic replacement must supersede stale coverage")
    .expect("stored-lineage promotion must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// The hash-pinned Basenames registry scan-all persists its topic0 set at the
/// top level of the source identity. When its family-scoped fact is required
/// for promotion, the drift guard must read that shape and refuse stale
/// evidence just as it does for a nested Coinbase SQL topic plan.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_when_hash_pinned_scan_all_topic_drift_supplies_required_coverage()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_078,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_078),
        selected_address,
    )
    .await?;

    let stored_safe_block_number = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 3;
    let live_latest_block_number = stored_safe_block_number + 17;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }

    let covered_from = current.block_number + 1;
    let drifted_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        covered_from,
        stored_safe_block_number,
        json!({
            "source_identity_hash": "test:drifted-scan-all-topics",
            "source_identity_payload_format": "basenames_registry_scan_all_topics_v1",
            "topic0s_by_source_family": {
                "test_source_family": [
                    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                ]
            }
        }),
        "drifted-scan-all-topics",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        drifted_job_id,
        &[family_coverage_fact(
            "test_source_family",
            covered_from,
            stored_safe_block_number,
        )],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: latest,
            safe: Some(stored_safe),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await
    .expect_err("required stale top-level topic coverage must refuse promotion");
    assert!(
        format!("{error:#}")
            .contains("manifest ABI topic0 set changed after completed backfill job"),
        "unexpected topic-drift refusal error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Shared fixture for the watch-set-growth tests: full coverage for the
/// initial tuple, one promoted slice against a shared frontier, then a second
/// watched tuple appears with an active window starting inside the
/// already-verified span (discovery admission is checkpoint-gated, so new
/// tuples always land behind the frontier).
async fn promote_one_covered_slice(
    database: &TestDatabase,
    chain: &str,
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<(
    IntakeChainTask,
    ProviderHeadSnapshot,
    crate::provider::JsonRpcProvider,
    tokio::task::JoinHandle<()>,
)> {
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_090,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_090),
        selected_address,
    )
    .await?;

    let stored_safe_block_number = chunk * 3 + 7;
    let live_latest_block_number = stored_safe_block_number + 25;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        &[selected_address],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let heads = ProviderHeadSnapshot {
        canonical: latest,
        safe: Some(stored_safe),
        finalized: None,
    };
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, first_outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        coverage_frontiers,
    )
    .await
    .expect("fully covered first slice must promote")
    .expect("first promotion must advance the checkpoint");
    assert_eq!(
        first_outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );

    Ok((task, heads, provider, server))
}

/// Admit a resolver discovery edge from the fixture's watched contract to a
/// new address through the REAL admission funnel
/// (`reconcile_discovery_observations`), which bumps the chain's discovery
/// admission epoch in the same transaction.
async fn admit_resolver_edge_observation(
    pool: &PgPool,
    chain: &str,
    to_address: &str,
    active_from_block: i64,
) -> Result<()> {
    // reachable_from_root admission requires the from-instance to be a
    // manifest root.
    insert_manifest_root_contract_instance(
        pool,
        10_090,
        Uuid::from_u128(10_090),
        "0x0000000000000000000000000000000000000001",
    )
    .await?;
    insert_manifest_discovery_rule(
        pool,
        10_090,
        "resolver",
        "WatchedContract",
        "reachable_from_root",
    )
    .await?;
    let summary = bigname_manifests::reconcile_discovery_observations(
        pool,
        "reconcile-e2e-admission",
        &[bigname_manifests::DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: to_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "reconcile-e2e-admission".to_owned(),
            active_from_block_number: Some(active_from_block),
            active_from_block_hash: None,
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "provider": "reconcile-e2e-test",
                "observation_key": "reconcile-e2e-edge",
            }),
        }],
        bigname_manifests::FullDiscoveryReconciliationOptions::default(),
    )
    .await?;
    assert_eq!(
        summary.admitted_edge_count, 1,
        "the observation must be admitted through the real funnel"
    );
    assert_eq!(
        summary.inserted_edge_count, 1,
        "the observation must insert a new discovery edge"
    );
    Ok(())
}

/// A new watch beginning near the stored anchor verifies only its own interval.
/// The DB-backed frontier records every required-tuples block-range query, so
/// this also permanently proves the epoch refresh does not query the chain's
/// historical span before the already-promoted checkpoint.
#[tokio::test]
async fn reconcile_fetched_heads_differential_coverage_adds_near_head_watch_only() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let initial_address = "0x0000000000000000000000000000000000000001";
    let late_address = "0x0000000000000000000000000000000000000008";
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    let promoted_checkpoint = task
        .checkpoint
        .canonical_block_number
        .expect("first promotion must advance the checkpoint");
    assert!(
        !coverage_frontiers
            .take_required_tuple_range_scans_for_tests(chain)
            .is_empty(),
        "the fixture must exercise the instrumented DB range loader"
    );
    let stored_anchor = heads
        .safe
        .as_ref()
        .expect("promotion fixture must carry a stored safe anchor")
        .block_number;
    let late_active_from = stored_anchor - 16;

    delete_coverage_facts_for_addresses(database.pool(), chain, &[initial_address]).await?;
    admit_resolver_edge_observation(database.pool(), chain, late_address, late_active_from).await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        late_active_from,
        stored_anchor,
        &[late_address],
    )
    .await?;

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("the near-head watch alone is covered")
    .expect("the differential coverage proof must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(task.checkpoint.canonical_block_number, Some(2049));
    let epoch_refresh_scans = coverage_frontiers.take_required_tuple_range_scans_for_tests(chain);
    assert!(
        epoch_refresh_scans
            .iter()
            .all(|(from_block, _)| *from_block > promoted_checkpoint),
        "a near-head admission epoch refresh must not issue a historical required-tuples range query at or below checkpoint {promoted_checkpoint}: {epoch_refresh_scans:?}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Removing a watch has no newly required interval. Once both original tuple
/// proofs are durably saved, deleting every durable fact makes any historical scan
/// visible; the removal epoch still advances without one.
#[tokio::test]
async fn reconcile_fetched_heads_differential_coverage_removal_skips_historical_scan() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let late_address = "0x0000000000000000000000000000000000000009";
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    let stored_anchor = heads
        .safe
        .as_ref()
        .expect("promotion fixture must carry a stored safe anchor")
        .block_number;
    admit_resolver_edge_observation(database.pool(), chain, late_address, 100).await?;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        100,
        stored_anchor,
        &[late_address],
    )
    .await?;
    let (task, _) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("the added watch is covered")
    .expect("the second slice must promote");

    sqlx::query("DELETE FROM backfill_coverage_facts WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await
        .context("failed to remove facts behind the saved coverage proof")?;
    let summary = bigname_manifests::reconcile_discovery_observations(
        database.pool(),
        "reconcile-e2e-admission",
        &[],
        bigname_manifests::FullDiscoveryReconciliationOptions::default(),
    )
    .await?;
    assert_eq!(summary.deactivated_edge_count, 1);
    assert_eq!(summary.admission_epoch_bump_count, 1);

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("watch removal must not rescan historical coverage")
    .expect("the third slice must promote from retained tuple proofs");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(task.checkpoint.canonical_block_number, Some(3073));

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A REAL discovery admission between two promotion calls sharing a frontier:
/// the same-family tuple appears behind the verified span (fingerprint
/// unchanged), the admission bumps the chain's discovery epoch in the same
/// transaction, and the next promotion re-verifies and refuses on the
/// uncovered new tuple.
#[tokio::test]
async fn reconcile_fetched_heads_refuses_uncovered_tuple_admitted_behind_the_frontier() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let late_address = "0x0000000000000000000000000000000000000002";
    let late_active_from = chunk + 100;
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    admit_resolver_edge_observation(database.pool(), chain, late_address, late_active_from).await?;

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("the admitted tuple has no coverage facts and must refuse promotion");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("do not form gap-free coverage") && rendered.contains(late_address),
        "refusal must name the admitted uncovered tuple: {rendered}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A discovery admission may carry a known start before the checkpoint that
/// was already advanced under an older watch set. Coverage only after that
/// checkpoint must not let the next stored-lineage promotion skip the newly
/// required earlier interval.
#[tokio::test]
async fn reconcile_fetched_heads_differential_coverage_refuses_retroactive_admission() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let late_address = "0x0000000000000000000000000000000000000006";
    let late_active_from = 100;
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    let advanced_checkpoint = task
        .checkpoint
        .canonical_block_number
        .expect("first promotion must advance the canonical checkpoint");
    assert!(late_active_from < advanced_checkpoint);
    delete_coverage_facts_for_addresses(
        database.pool(),
        chain,
        &["0x0000000000000000000000000000000000000001"],
    )
    .await?;
    admit_resolver_edge_observation(database.pool(), chain, late_address, late_active_from).await?;

    let stored_anchor = heads
        .safe
        .as_ref()
        .expect("promotion fixture must carry a stored safe anchor")
        .block_number;
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        advanced_checkpoint + 1,
        stored_anchor,
        &[late_address],
    )
    .await?;

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("coverage after the checkpoint must not credit a retroactive admission");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("do not form gap-free coverage")
            && rendered.contains(late_address)
            && rendered.contains("blocks 100..=")
            && !rendered.contains("0x0000000000000000000000000000000000000001"),
        "retroactive admission refusal must name its uncovered earlier interval: {rendered}"
    );

    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        late_active_from,
        advanced_checkpoint,
        &[late_address],
    )
    .await?;
    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("repairing the retroactive interval must restore promotion")
    .expect("the repaired stored-lineage slice must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(task.checkpoint.canonical_block_number, Some(2049));

    // The successful epoch recheck publishes the widened interval through the
    // stored anchor. A later promotion step using the same frontier proceeds
    // without turning the historical recheck into a per-poll scan.
    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("a verified retroactive interval must remain durable")
    .expect("the next stored-lineage slice must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(task.checkpoint.canonical_block_number, Some(3073));

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// Stored-lineage coverage and provider preparation do not hold the admission
/// row lock. A discovery transaction can commit during that work, but the
/// promotion must revalidate the epoch and refuse checkpoint advancement.
#[tokio::test]
async fn reconcile_fetched_heads_revalidates_admission_epoch_before_checkpoint_advance()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "epoch-fence-testnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let coverage_frontiers = Arc::new(ChainCoverageFrontiers::default());
    let late_address = "0x0000000000000000000000000000000000000005";
    let late_active_from = chunk + 100;
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;

    let hook = ChainCoverageFrontiers::install_admission_epoch_verification_test_hook(
        database.pool(),
        chain,
    )
    .await;
    let reconcile_pool = database.pool().clone();
    let reconcile_task = task.clone();
    let reconcile_heads = heads.clone();
    let reconcile_provider = provider.clone();
    let reconcile_frontiers = coverage_frontiers.clone();
    let reconcile = tokio::spawn(async move {
        reconcile_fetched_heads_with_adapter_sync(
            &reconcile_pool,
            &reconcile_task,
            &reconcile_provider,
            &reconcile_heads,
            false,
            HeaderAuditMode::Minimal,
            &[],
            &reconcile_frontiers,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        hook.wait_until_verified(),
    )
    .await
    .context("stored-lineage promotion did not verify the admission epoch")?;

    // Use a separate pool so the admission transaction reaches the locked
    // epoch row instead of merely waiting for one of the reconciliation
    // pool's two connections.
    let admission_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(bigname_storage::stamp_projection_replay_version(
            (*database.pool().connect_options()).clone(),
        ))
        .await
        .context("failed to connect the concurrent admission test pool")?;
    let admission_chain = chain.to_owned();
    let admission_address = late_address.to_owned();
    let mut admission = tokio::spawn(async move {
        admit_resolver_edge_observation(
            &admission_pool,
            &admission_chain,
            &admission_address,
            late_active_from,
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut admission)
        .await
        .context("watch-set admission remained blocked after optimistic coverage verification")?
        .context("concurrent admission task panicked")??;
    hook.resume();
    let error = reconcile
        .await
        .context("stored-lineage reconciliation task panicked")?
        .expect_err("epoch drift during preparation must refuse checkpoint advancement");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("discovery admission epoch")
            && rendered.contains("refusing stored-lineage checkpoint promotion"),
        "epoch revalidation failure must be explicit: {rendered}"
    );
    let persisted_checkpoint = bigname_storage::load_chain_checkpoint(database.pool(), chain)
        .await?
        .expect("chain checkpoint must remain persisted");
    assert_eq!(
        persisted_checkpoint.canonical_block_number, task.checkpoint.canonical_block_number,
        "epoch drift must leave the verified checkpoint unchanged"
    );

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("the next promotion must reverify and refuse the admitted uncovered tuple");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("do not form gap-free coverage") && rendered.contains(late_address),
        "post-fence refusal must name the newly admitted uncovered tuple: {rendered}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn admission_epoch_checkpoint_advance_uses_one_pool_connection() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "single-connection-epoch-fence";
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let (task, _heads, _provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    let epoch = bigname_manifests::load_discovery_admission_epoch(database.pool(), chain).await?;
    let one_connection_pool = database.additional_pool(1).await?;
    let canonical = task
        .checkpoint
        .canonical_block_hash
        .as_ref()
        .zip(task.checkpoint.canonical_block_number)
        .map(|(block_hash, block_number)| CheckpointBlockRef {
            block_hash: block_hash.clone(),
            block_number,
        });
    let safe = task
        .checkpoint
        .safe_block_hash
        .as_ref()
        .zip(task.checkpoint.safe_block_number)
        .map(|(block_hash, block_number)| CheckpointBlockRef {
            block_hash: block_hash.clone(),
            block_number,
        });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ChainCoverageFrontiers::advance_checkpoint_with_promotion_epoch(
            &one_connection_pool,
            chain,
            Some(epoch),
            canonical,
            safe,
            None,
        ),
    )
    .await
    .context("admission-fenced checkpoint advancement tried to acquire a second connection")??;

    one_connection_pool.close().await;
    server.abort();
    database.cleanup().await
}

/// A competing stored branch may arrive after optimistic coverage verification.
/// Final checkpoint persistence must repeat the fork check while excluding
/// lineage writers, otherwise number-keyed coverage could promote an ambiguous
/// path.
#[tokio::test]
async fn reconcile_fetched_heads_rechecks_same_height_forks_before_checkpoint_advance() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "lineage-fork-fence-testnet";
    let coverage_frontiers = Arc::new(ChainCoverageFrontiers::default());
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;

    let hook = ChainCoverageFrontiers::install_admission_epoch_verification_test_hook(
        database.pool(),
        chain,
    )
    .await;
    let reconcile_pool = database.pool().clone();
    let reconcile_task = task.clone();
    let reconcile_heads = heads.clone();
    let reconcile_provider = provider.clone();
    let reconcile_frontiers = coverage_frontiers.clone();
    let reconcile = tokio::spawn(async move {
        reconcile_fetched_heads_with_adapter_sync(
            &reconcile_pool,
            &reconcile_task,
            &reconcile_provider,
            &reconcile_heads,
            false,
            HeaderAuditMode::Minimal,
            &[],
            &reconcile_frontiers,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        hook.wait_until_verified(),
    )
    .await
    .context("stored-lineage promotion did not reach the post-verification pause")?;

    let fork_block_number = task
        .checkpoint
        .canonical_block_number
        .expect("the first promotion must persist a canonical checkpoint")
        + 2;
    let fork_parent_hash = format!("0x{:064x}", fork_block_number - 1);
    let competing_fork = provider_block(
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff04",
        Some(&fork_parent_hash),
        fork_block_number,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &competing_fork,
        CanonicalityState::Observed,
    )
    .await?;

    hook.resume();
    let error = reconcile
        .await
        .context("stored-lineage reconciliation task panicked")?
        .expect_err("a late same-height fork must refuse checkpoint advancement");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("non-orphaned same-height fork")
            && rendered.contains(&format!("at block {fork_block_number}")),
        "late-fork refusal must name the ambiguous height: {rendered}"
    );
    let persisted_checkpoint = bigname_storage::load_chain_checkpoint(database.pool(), chain)
        .await?
        .expect("chain checkpoint must remain persisted");
    assert_eq!(
        persisted_checkpoint, task.checkpoint,
        "a late same-height fork must leave the verified checkpoint unchanged"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// The admitted tuple's family is covered by an existing family-scope fact:
/// the epoch bump forces re-verification, which passes, and promotion
/// proceeds.
#[tokio::test]
async fn reconcile_fetched_heads_promotes_after_reverifying_covered_admitted_tuple() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let stored_safe_block_number = chunk * 3 + 7;
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let late_address = "0x0000000000000000000000000000000000000003";
    let late_active_from = chunk + 100;

    // Family-scope coverage for the watched family exists before the new
    // tuple is admitted (a topics-complete family scan fetched every
    // address of the family).
    let family_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        1,
        stored_safe_block_number,
        json!({"source_identity_hash": "test:family-scan"}),
        "family-scan",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        family_job_id,
        &[family_coverage_fact(
            "test_source_family",
            1,
            stored_safe_block_number,
        )],
    )
    .await?;

    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    admit_resolver_edge_observation(database.pool(), chain, late_address, late_active_from).await?;

    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("the admitted tuple is family-covered, so re-verification must pass")
    .expect("promotion must advance the checkpoint");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(task.checkpoint.canonical_block_number, Some(chunk * 2 + 1));

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A manifest reload that changes the chain's ABI event sets invalidates the
/// persisted frontier by its topic sets: the newly admitted uncovered tuple is
/// caught on re-verification even without an explicit clamp.
#[tokio::test]
async fn reconcile_fetched_heads_manifest_abi_change_invalidates_persisted_frontier() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let late_address = "0x0000000000000000000000000000000000000004";
    let late_active_from = chunk + 100;
    // A NEW family changes the log-producing topic-set fingerprint; no clamp
    // is issued — the topic mismatch alone must invalidate the saved family proof.
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_093,
        "test",
        chain,
        "late_uncovered_family",
        Uuid::from_u128(10_093),
        late_address,
    )
    .await?;
    sqlx::query(
        "UPDATE contract_instance_addresses SET active_from_block_number = $1 WHERE chain_id = $2 AND LOWER(address) = $3",
    )
    .bind(late_active_from)
    .bind(chain)
    .bind(late_address)
    .execute(database.pool())
    .await
    .context("failed to set the late tuple's active window")?;

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("the fingerprint reset must surface the uncovered admitted tuple");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("do not form gap-free coverage") && rendered.contains(late_address),
        "fingerprint-driven re-verification must name the uncovered tuple: {rendered}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A topic selector change invalidates the changed family's tuple proofs, not
/// every source family on the chain. Both families' durable facts are removed:
/// the changed family refuses, while the unchanged family is absent from the
/// refusal and remains saved.
#[tokio::test]
async fn reconcile_fetched_heads_differential_coverage_topic_change_is_family_scoped() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let initial_address = "0x0000000000000000000000000000000000000001";
    let changed_address = "0x000000000000000000000000000000000000000a";
    let changed_family = "topic_changed_family";
    let changed_manifest_id = 10_094;
    let (task, heads, provider, server) =
        promote_one_covered_slice(&database, chain, &coverage_frontiers).await?;
    let stored_anchor = heads
        .safe
        .as_ref()
        .expect("promotion fixture must carry a stored safe anchor")
        .block_number;

    insert_reconcile_watched_manifest_contract(
        database.pool(),
        changed_manifest_id,
        "test",
        chain,
        changed_family,
        Uuid::from_u128(10_094),
        changed_address,
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        stored_anchor,
        changed_family,
        &[changed_address],
    )
    .await?;
    let (task, _) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("the added family is covered")
    .expect("the second slice must promote");
    coverage_frontiers.take_required_tuple_range_scans_for_tests(chain);

    delete_coverage_facts_for_addresses(
        database.pool(),
        chain,
        &[initial_address, changed_address],
    )
    .await?;
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $1 WHERE manifest_id = $2")
        .bind(json!({
            "abi": {
                "events": [{
                    "name": "SelectorChanged",
                    "fragment": "event SelectorChanged(uint256 indexed value)"
                }]
            }
        }))
        .bind(changed_manifest_id)
        .execute(database.pool())
        .await
        .context("failed to change the affected family's topic selector")?;

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("the changed family's removed coverage must refuse promotion");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains(changed_address) && !rendered.contains(initial_address),
        "only the topic-changed family must be reverified: {rendered}"
    );
    assert!(
        coverage_frontiers
            .take_required_tuple_range_scans_for_tests(chain)
            .is_empty(),
        "a topic-only snapshot refresh must reverify retained intervals without a block-range requirement scan"
    );

    let repair_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        1,
        stored_anchor,
        json!({
            "selector_kind": "whole_active_watched_chain",
            "source_family": null,
            "requested_watched_targets": [],
            "source_identity_hash": "test:topic-changed-family-repair",
            "selected_targets": [{
                "source_family": changed_family,
                "contract_instance_id": "00000000-0000-0000-0000-000000000094",
                "address": changed_address,
                "effective_from_block": 1,
                "effective_to_block": stored_anchor
            }]
        }),
        "topic-changed-family-repair",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        repair_job_id,
        &[address_coverage_fact(
            changed_family,
            changed_address,
            1,
            stored_anchor,
        )],
    )
    .await?;
    let (task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("repairing only the affected family must restore promotion")
    .expect("the third slice must promote");
    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(task.checkpoint.canonical_block_number, Some(3073));

    server.abort();
    database.cleanup().await?;
    Ok(())
}

/// A drifted persisted topic0 set intersecting only blocks above the current
/// promotion target must not block promoting the covered prefix; once the
/// crawl reaches the drifted job's range, promotion refuses.
#[tokio::test]
async fn reconcile_fetched_heads_topic_drift_above_target_does_not_block_covered_prefix()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let selected_address = "0x0000000000000000000000000000000000000001";
    insert_reconcile_watched_manifest_contract(
        database.pool(),
        10_094,
        "test",
        chain,
        "test_source_family",
        Uuid::from_u128(10_094),
        selected_address,
    )
    .await?;

    let stored_safe_block_number = chunk * 2 + 7;
    let live_latest_block_number = stored_safe_block_number + 25;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    insert_completed_backfill_range_coverage(
        database.pool(),
        chain,
        current.block_number + 1,
        current.block_number + chunk,
        &[selected_address],
    )
    .await?;
    // The stale job's own fact covers the next slice above the first
    // promotion target. It is therefore irrelevant to the covered prefix but
    // becomes relied-upon evidence once the crawl reaches that slice.
    let drifted_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        current.block_number + chunk + 1,
        stored_safe_block_number,
        json!({
            "source_identity_hash": "test:drifted-above-target",
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {
                    "test_source_family": [
                        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    ]
                }
            }
        }),
        "drifted-above-target",
    )
    .await?;
    insert_backfill_coverage_fact_rows(
        database.pool(),
        drifted_job_id,
        &[address_coverage_fact(
            "test_source_family",
            selected_address,
            current.block_number + chunk + 1,
            stored_safe_block_number,
        )],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let heads = ProviderHeadSnapshot {
        canonical: latest,
        safe: Some(stored_safe),
        finalized: None,
    };
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![selected_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, first_outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("drift above the promotion target must not block the covered prefix")
    .expect("first promotion must advance the checkpoint");
    assert_eq!(
        first_outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );
    assert_eq!(
        task.checkpoint.canonical_block_number,
        Some(current.block_number + chunk)
    );

    let error = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect_err("once the crawl reaches the drifted job's range, promotion must refuse");
    assert!(
        format!("{error:#}")
            .contains("manifest ABI topic0 set changed after completed backfill job"),
        "unexpected drift refusal error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

fn watched_surface_manifest(contract_addresses: &[&str]) -> String {
    let contracts = contract_addresses
        .iter()
        .enumerate()
        .map(|(index, address)| {
            format!(
                "[[contracts]]\nrole = \"watched{index}\"\naddress = \"{address}\"\nproxy_kind = \"none\"\n"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
exact_lookup = "supported"

[[roots]]
name = "RootRegistry"
address = "0x00000000000000000000000000000000000000e1"

{contracts}
[[discovery_rules]]
edge_kind = "subregistry"
from_role = "RootRegistry"
admission = "reachable_from_root"
{abi}
"#,
        abi = test_manifest_abi_toml()
    )
}

/// Mirror of the round-4 probe: the MANIFEST-DECLARED arm of the watched
/// surface grows between two promotions sharing a frontier — a real
/// `sync_repository` run adds a same-family contract entry with no discovery
/// edge and no ABI change, so only the sync-time admission-epoch bump can
/// invalidate the saved frontier. `covered` selects whether a family-scope fact covers
/// the new tuple.
async fn manifest_growth_promotion_scenario(covered: bool) -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let chunk = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;
    let root_address = "0x00000000000000000000000000000000000000e1";
    let first_address = "0x00000000000000000000000000000000000000e2";
    let late_address = "0x00000000000000000000000000000000000000e3";

    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&watched_surface_manifest(&[first_address]))?;
    bigname_manifests::sync_repository(
        database.pool(),
        &load_manifest_repository(&manifests.path)?,
    )
    .await?;

    let stored_safe_block_number = chunk * 3 + 7;
    let live_latest_block_number = stored_safe_block_number + 25;
    let blocks = linear_provider_blocks(live_latest_block_number);
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    let stored_safe = blocks
        .iter()
        .find(|block| block.block_number == stored_safe_block_number)
        .expect("test chain must include the stored safe block")
        .clone();
    for block in &blocks {
        if block.block_number > stored_safe_block_number {
            continue;
        }
        let state = if block.block_number == stored_safe_block_number {
            CanonicalityState::Safe
        } else {
            CanonicalityState::Canonical
        };
        insert_chain_lineage_for_block(database.pool(), chain, block, state).await?;
    }
    let coverage_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        current.block_number + 1,
        stored_safe_block_number,
        json!({"source_identity_hash": "test:manifest-growth"}),
        "manifest-growth",
    )
    .await?;
    let mut facts = vec![
        address_coverage_fact(
            "ens_v2_registry_l1",
            root_address,
            current.block_number + 1,
            stored_safe_block_number,
        ),
        address_coverage_fact(
            "ens_v2_registry_l1",
            first_address,
            current.block_number + 1,
            stored_safe_block_number,
        ),
    ];
    if covered {
        facts.push(family_coverage_fact(
            "ens_v2_registry_l1",
            current.block_number + 1,
            stored_safe_block_number,
        ));
    }
    insert_backfill_coverage_fact_rows(database.pool(), coverage_job_id, &facts).await?;
    insert_chain_checkpoint(
        database.pool(),
        &ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider(vec![latest.clone(), stored_safe.clone()]).await?;
    let heads = ProviderHeadSnapshot {
        canonical: latest,
        safe: Some(stored_safe),
        finalized: None,
    };
    let coverage_frontiers = ChainCoverageFrontiers::default();
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![root_address.to_owned(), first_address.to_owned()],
        manifest_root_entry_count: 1,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(current.block_hash.clone()),
            canonical_block_number: Some(current.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, first_outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await
    .expect("fully covered first slice must promote")
    .expect("first promotion must advance the checkpoint");
    assert_eq!(
        first_outcome.canonical_status,
        CanonicalReconciliationStatus::StoredLineagePromoted
    );

    // In-place manifest edit: a same-family contract entry appears (identical
    // ABI, no discovery edge). Only the sync-time epoch bump can force the
    // frontier to re-verify.
    fs::write(
        &manifest_path,
        watched_surface_manifest(&[first_address, late_address]),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;
    bigname_manifests::sync_repository(
        database.pool(),
        &load_manifest_repository(&manifests.path)?,
    )
    .await?;

    let second = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &heads,
        false,
        HeaderAuditMode::Minimal,
        &[],
        &coverage_frontiers,
    )
    .await;
    if covered {
        let (_, outcome) = second
            .expect("the grown tuple is family-covered, so re-verification must pass")
            .expect("promotion must advance the checkpoint");
        assert_eq!(
            outcome.canonical_status,
            CanonicalReconciliationStatus::StoredLineagePromoted
        );
    } else {
        let error = second.expect_err("the uncovered manifest-grown tuple must refuse promotion");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("do not form gap-free coverage") && rendered.contains(late_address),
            "refusal must name the manifest-grown uncovered tuple: {rendered}"
        );
    }

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_refuses_uncovered_tuple_grown_by_manifest_sync() -> Result<()> {
    manifest_growth_promotion_scenario(false).await
}

#[tokio::test]
async fn reconcile_fetched_heads_promotes_family_covered_tuple_grown_by_manifest_sync() -> Result<()>
{
    manifest_growth_promotion_scenario(true).await
}

/// Scale guard for the coverage query: 100k watched tuples with matching
/// facts must verify clean, and the per-tuple fact probe must be
/// index-backed (no sequential scan over backfill_coverage_facts).
#[tokio::test]
#[ignore = "scale guard; run explicitly"]
async fn coverage_fact_union_scale_guard() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "base-mainnet";
    let tuple_count = 100_000_i64;
    let covered_from_block = 1_i64;
    let covered_to_block = 100_000_i64;

    sqlx::query(
        r#"
        INSERT INTO manifest_versions (manifest_id, namespace, source_family, chain, rollout_status)
        VALUES (90000, 'test', 'test_source_family', $1, 'active')
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid, $1, 'contract'
        FROM generate_series(1, $2::bigint) g
        "#,
    )
    .bind(chain)
    .bind(tuple_count)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, source_manifest_id)
        SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
               $1,
               '0x' || lpad(to_hex(g), 40, '0'),
               90000
        FROM generate_series(1, $2::bigint) g
        "#,
    )
    .bind(chain)
    .bind(tuple_count)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id, declaration_kind, declaration_name, contract_instance_id,
            declared_address, role, proxy_kind
        )
        SELECT 90000,
               'contract',
               'Watched' || g,
               ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
               '0x' || lpad(to_hex(g), 40, '0'),
               'Watched' || g,
               'none'
        FROM generate_series(1, $1::bigint) g
        "#,
    )
    .bind(tuple_count)
    .execute(database.pool())
    .await?;

    let backfill_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        database.pool(),
        chain,
        covered_from_block,
        covered_to_block,
        source_identity_with_selected_targets(Vec::new()),
        "scale-guard",
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO backfill_coverage_facts (
            backfill_job_id, chain_id, source_family, scope, address,
            covered_from_block, covered_to_block, derivation
        )
        SELECT $1, $2, 'test_source_family', 'address', '0x' || lpad(to_hex(g), 40, '0'),
               $3, $4, 'job_completion'
        FROM generate_series(1, $5::bigint) g
        "#,
    )
    .bind(backfill_job_id)
    .bind(chain)
    .bind(covered_from_block)
    .bind(covered_to_block)
    .bind(tuple_count)
    .execute(database.pool())
    .await?;
    // Bulk-loaded fixtures need planner stats like autovacuum maintains in
    // production; without them the requirement-side joins pick degenerate
    // plans that have nothing to do with the anti-join under test.
    sqlx::query(
        "ANALYZE backfill_coverage_facts, contract_instances, contract_instance_addresses, \
         manifest_contract_instances, manifest_versions",
    )
    .execute(database.pool())
    .await?;

    let started_at = std::time::Instant::now();
    let violations = bigname_manifests::find_uncovered_watched_tuples(
        database.pool(),
        chain,
        covered_from_block,
        covered_to_block,
        &["test_source_family".to_owned()],
        20,
    )
    .await?;
    assert!(
        violations.is_empty(),
        "fully facted watch set must verify clean: {violations:?}"
    );
    assert!(
        started_at.elapsed() < std::time::Duration::from_secs(60),
        "anti-join over 100k tuples must stay inside the scale budget: {:?}",
        started_at.elapsed()
    );

    let probe_address = format!("0x{:040x}", 0x1234_5678_u64);
    let plan_rows = sqlx::query_scalar::<_, String>(&format!(
        r#"
        EXPLAIN (FORMAT TEXT)
        SELECT 1
        FROM backfill_coverage_facts
        WHERE chain_id = '{chain}'
          AND source_family = 'test_source_family'
          AND address = '{probe_address}'
          AND covered_from_block <= {covered_from_block}
          AND covered_to_block >= {covered_to_block}
        "#
    ))
    .fetch_all(database.pool())
    .await?;
    let plan = plan_rows.join("\n");
    assert!(
        plan.contains("Index"),
        "coverage containment probe must be index-backed:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on backfill_coverage_facts"),
        "coverage containment probe must not scan the facts table sequentially:\n{plan}"
    );

    database.cleanup().await?;
    Ok(())
}

async fn insert_completed_backfill_range_coverage(
    pool: &sqlx::PgPool,
    chain: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    selected_addresses: &[&str],
) -> Result<()> {
    insert_completed_backfill_range_coverage_for_source_family(
        pool,
        chain,
        range_start_block_number,
        range_end_block_number,
        "test_source_family",
        selected_addresses,
    )
    .await
}

/// Create and complete a backfill job over the range with a harmless
/// full-payload source identity (so the topic-drift guard sees a plain
/// payload), then write one durable address-scoped `backfill_coverage_facts`
/// row per selected address — the rows stored-lineage promotion consults.
async fn insert_completed_backfill_range_coverage_for_source_family(
    pool: &sqlx::PgPool,
    chain: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    source_family: &str,
    selected_addresses: &[&str],
) -> Result<()> {
    let source_identity = source_identity_with_selected_targets(
        selected_addresses
            .iter()
            .enumerate()
            .map(|(index, address)| {
                json!({
                    "source_family": source_family,
                    "contract_instance_id": format!("00000000-0000-0000-0000-{index:012x}"),
                    "address": address.to_ascii_lowercase(),
                    "effective_from_block": range_start_block_number,
                    "effective_to_block": range_end_block_number
                })
            })
            .collect(),
    );
    let backfill_job_id = insert_completed_backfill_range_coverage_with_source_identity(
        pool,
        chain,
        range_start_block_number,
        range_end_block_number,
        source_identity,
        &format!("completed-{source_family}"),
    )
    .await?;
    let facts = selected_addresses
        .iter()
        .map(|address| {
            address_coverage_fact(
                source_family,
                address,
                range_start_block_number,
                range_end_block_number,
            )
        })
        .collect::<Vec<_>>();
    insert_backfill_coverage_fact_rows(pool, backfill_job_id, &facts).await
}

async fn delete_coverage_facts_for_addresses(
    pool: &sqlx::PgPool,
    chain: &str,
    addresses: &[&str],
) -> Result<()> {
    let addresses = addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        DELETE FROM backfill_coverage_facts
        WHERE chain_id = $1
          AND LOWER(address) = ANY($2::TEXT[])
        "#,
    )
    .bind(chain)
    .bind(&addresses)
    .execute(pool)
    .await
    .context("failed to remove selected coverage facts")?;
    Ok(())
}

fn address_coverage_fact(
    source_family: &str,
    address: &str,
    covered_from_block: i64,
    covered_to_block: i64,
) -> bigname_storage::BackfillCoverageFactWrite {
    bigname_storage::BackfillCoverageFactWrite {
        source_family: source_family.to_owned(),
        scope: bigname_storage::BackfillCoverageFactScope::Address,
        address: Some(address.to_ascii_lowercase()),
        covered_from_block,
        covered_to_block,
    }
}

fn family_coverage_fact(
    source_family: &str,
    covered_from_block: i64,
    covered_to_block: i64,
) -> bigname_storage::BackfillCoverageFactWrite {
    bigname_storage::BackfillCoverageFactWrite {
        source_family: source_family.to_owned(),
        scope: bigname_storage::BackfillCoverageFactScope::Family,
        address: None,
        covered_from_block,
        covered_to_block,
    }
}

async fn insert_backfill_coverage_fact_rows(
    pool: &sqlx::PgPool,
    backfill_job_id: i64,
    facts: &[bigname_storage::BackfillCoverageFactWrite],
) -> Result<()> {
    let mut conn = pool.acquire().await?;
    bigname_storage::write_backfill_coverage_facts(
        &mut conn,
        backfill_job_id,
        bigname_storage::BackfillCoverageFactDerivation::JobCompletion,
        facts,
    )
    .await?;
    Ok(())
}

async fn insert_untrusted_backfill_coverage_fact_row(
    pool: &sqlx::PgPool,
    backfill_job_id: i64,
    chain: &str,
    source_family: &str,
    address: &str,
    covered_from_block: i64,
    covered_to_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO backfill_coverage_facts (
            backfill_job_id,
            chain_id,
            source_family,
            scope,
            address,
            covered_from_block,
            covered_to_block,
            derivation
        )
        VALUES ($1, $2, $3, 'address', lower($4), $5, $6, 'job_completion')
        "#,
    )
    .bind(backfill_job_id)
    .bind(chain)
    .bind(source_family)
    .bind(address)
    .bind(covered_from_block)
    .bind(covered_to_block)
    .execute(pool)
    .await
    .context("failed to insert intentionally untrusted backfill coverage fact")?;
    Ok(())
}

/// Create and complete a backfill job WITHOUT writing coverage facts; returns
/// the job id so callers can attach explicit fact rows.
async fn insert_completed_backfill_range_coverage_with_source_identity(
    pool: &sqlx::PgPool,
    chain: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    source_identity: Value,
    suffix: &str,
) -> Result<i64> {
    let record = bigname_storage::create_backfill_job(
        pool,
        &backfill_job_create_with_source_identity(
            chain,
            range_start_block_number,
            range_end_block_number,
            source_identity,
            suffix,
        ),
    )
    .await?;
    let lease_token = format!("stored-lineage-completed-lease-{suffix}");
    let reserved = bigname_storage::reserve_backfill_range(
        pool,
        record.job.backfill_job_id,
        "stored-lineage-test",
        &lease_token,
        backfill_lease_deadline()?,
    )
    .await?
    .expect("new backfill job must reserve its one range");
    bigname_storage::advance_backfill_range(
        pool,
        reserved.backfill_range_id,
        &lease_token,
        range_end_block_number,
    )
    .await?;
    bigname_storage::complete_backfill_range(pool, reserved.backfill_range_id, &lease_token)
        .await?;
    Ok(record.job.backfill_job_id)
}

async fn insert_incomplete_backfill_range_residue(
    pool: &sqlx::PgPool,
    chain: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    selected_addresses: &[&str],
) -> Result<()> {
    let record = bigname_storage::create_backfill_job(
        pool,
        &backfill_job_create(
            chain,
            range_start_block_number,
            range_end_block_number,
            selected_addresses,
            "incomplete",
        ),
    )
    .await?;
    let lease_token = "stored-lineage-incomplete-lease";
    let reserved = bigname_storage::reserve_backfill_range(
        pool,
        record.job.backfill_job_id,
        "stored-lineage-test",
        lease_token,
        backfill_lease_deadline()?,
    )
    .await?
    .expect("new backfill job must reserve its one range");
    bigname_storage::advance_backfill_range(
        pool,
        reserved.backfill_range_id,
        lease_token,
        range_end_block_number,
    )
    .await?;
    Ok(())
}

fn backfill_job_create(
    chain: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    selected_addresses: &[&str],
    suffix: &str,
) -> bigname_storage::BackfillJobCreate {
    backfill_job_create_with_source_identity(
        chain,
        range_start_block_number,
        range_end_block_number,
        source_identity_with_selected_targets(
            selected_addresses
                .iter()
                .enumerate()
                .map(|(index, address)| {
                    json!({
                        "source_family": "test_source_family",
                        "contract_instance_id": format!("00000000-0000-0000-0000-{index:012x}"),
                        "address": address.to_ascii_lowercase(),
                        "effective_from_block": range_start_block_number,
                        "effective_to_block": range_end_block_number
                    })
                })
                .collect(),
        ),
        suffix,
    )
}

fn backfill_job_create_with_source_identity(
    chain: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    source_identity: Value,
    suffix: &str,
) -> bigname_storage::BackfillJobCreate {
    bigname_storage::BackfillJobCreate {
        deployment_profile: "test".to_owned(),
        chain_id: chain.to_owned(),
        source_identity,
        scan_mode: "hash_pinned_logs_v1".to_owned(),
        range_start_block_number,
        range_end_block_number,
        idempotency_key: format!(
            "stored-lineage-coverage:{chain}:{range_start_block_number}:{range_end_block_number}:{suffix}"
        ),
        ranges: vec![bigname_storage::BackfillRangeSpec {
            range_start_block_number,
            range_end_block_number,
        }],
    }
}

fn source_identity_with_selected_targets(selected_targets: Vec<Value>) -> Value {
    json!({
        "selector_kind": "whole_active_watched_chain",
        "source_family": null,
        "requested_watched_targets": [],
        "source_identity_hash": "test:full-selected-targets",
        "selected_targets": selected_targets
    })
}

fn linear_provider_blocks(last_block_number: i64) -> Vec<ProviderBlock> {
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=last_block_number {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    blocks
}

async fn insert_reconcile_watched_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    chain: &str,
    source_family: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_id,
            namespace,
            source_family,
            chain,
            rollout_status
        )
        VALUES ($1, $2, $3, $4, 'active')
        "#,
    )
    .bind(manifest_id)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to insert active manifest {manifest_id} for {chain}:{source_family}")
    })?;
    insert_contract_instance(pool, contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        contract_instance_id,
        chain,
        address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        manifest_id,
        "WatchedContract",
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}

fn backfill_lease_deadline() -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
        .context("valid backfill lease deadline")
}

async fn insert_retained_full_block_payloads<'a>(
    pool: &sqlx::PgPool,
    chain: &str,
    blocks: impl IntoIterator<Item = &'a ProviderBlock>,
) -> Result<()> {
    let upserts = blocks
        .into_iter()
        .map(|block| bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: Some("keccak256".to_owned()),
            retained_digest: Some(format!("0x{:064x}", block.block_number)),
            block_number: Some(block.block_number),
            payload_size_bytes: 1,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "test",
                "method": "eth_getBlockByHash",
                "fetch_mode": "stored_lineage_promotion"
            }),
            canonicality_state: CanonicalityState::Canonical,
        })
        .collect::<Vec<_>>();
    bigname_storage::upsert_raw_payload_cache_metadata(pool, &upserts).await?;
    Ok(())
}

/// Topic0 of a `test_source_family` manifest ABI event (the default test
/// manifest payload declares `NewOwner(bytes32,bytes32,address)`), so seeded
/// logs are family-selected and demand raw companions during promotion.
fn family_selected_test_topic0() -> String {
    keccak256_hex(b"NewOwner(bytes32,bytes32,address)")
}

async fn insert_selected_raw_log_inputs(
    pool: &sqlx::PgPool,
    chain: &str,
    blocks: &[crate::provider::ProviderBlock],
    selected_address: &str,
    retain_full_payloads: bool,
) -> Result<()> {
    insert_raw_log_inputs_with_topic0(
        pool,
        chain,
        blocks,
        selected_address,
        &family_selected_test_topic0(),
        true,
        retain_full_payloads,
    )
    .await
}

async fn insert_raw_log_inputs_with_topic0(
    pool: &sqlx::PgPool,
    chain: &str,
    blocks: &[crate::provider::ProviderBlock],
    selected_address: &str,
    topic0: &str,
    seed_code_rows: bool,
    retain_full_payloads: bool,
) -> Result<()> {
    let selected_address = selected_address.to_ascii_lowercase();
    let mut transactions = Vec::new();
    let mut receipts = Vec::new();
    let mut logs = Vec::new();
    let mut code_hashes = Vec::new();
    for block in blocks {
        let transaction_hash = format!("0x{:064x}", block.block_number + 10_000);
        transactions.push(bigname_storage::RawTransaction {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash: transaction_hash.clone(),
            transaction_index: 0,
            from_address: selected_address.clone(),
            to_address: Some(selected_address.clone()),
            canonicality_state: CanonicalityState::Canonical,
        });
        receipts.push(bigname_storage::RawReceipt {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash: transaction_hash.clone(),
            transaction_index: 0,
            contract_address: None,
            status: Some(true),
            gas_used: Some(21_000),
            cumulative_gas_used: Some(21_000),
            logs_bloom: None,
            canonicality_state: CanonicalityState::Canonical,
        });
        logs.push(bigname_storage::RawLog {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash,
            transaction_index: 0,
            log_index: 0,
            emitting_address: selected_address.clone(),
            topics: vec![topic0.to_owned()],
            data: vec![1],
            canonicality_state: CanonicalityState::Canonical,
        });
        if seed_code_rows {
            code_hashes.push(bigname_storage::RawCodeHash {
                chain_id: chain.to_owned(),
                block_hash: block.block_hash.clone(),
                block_number: block.block_number,
                contract_address: selected_address.clone(),
                code_hash: format!("0x{:064x}", block.block_number + 20_000),
                code_byte_length: 1,
                canonicality_state: CanonicalityState::Canonical,
            });
        }
    }

    bigname_storage::upsert_raw_transactions(pool, &transactions).await?;
    bigname_storage::upsert_raw_receipts(pool, &receipts).await?;
    bigname_storage::upsert_raw_logs(pool, &logs).await?;
    bigname_storage::upsert_raw_code_hashes(pool, &code_hashes).await?;
    if retain_full_payloads {
        insert_retained_full_block_payloads(pool, chain, blocks.iter()).await?;
    }

    Ok(())
}

#[tokio::test]
async fn cache_fill_authorizes_full_block_metadata_from_provider_fetch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let block = provider_block(
        "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0",
        Some("0xb0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0"),
        100,
    );
    let (provider, server) = bundle_provider(vec![block.clone()]).await?;

    let bundle = provider
        .fetch_block_bundle_by_hash(&block.block_hash)
        .await?;
    let full_block_payload = bundle
        .raw_payloads
        .iter()
        .find(|payload| payload.payload_kind == provider::RAW_PAYLOAD_KIND_FULL_BLOCK)
        .expect("provider bundle fetch must retain full-block payload metadata");
    let expected_response_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": rpc_block_bundle_payload(&block),
    })
    .to_string();
    let expected_payload_size = i64::try_from(expected_response_body.len())
        .context("expected JSON-RPC response body size must fit in i64")?;
    assert_eq!(full_block_payload.digest_algorithm, "keccak256");
    assert_eq!(
        full_block_payload.retained_digest,
        keccak256_hex(expected_response_body.as_bytes())
    );
    assert_eq!(full_block_payload.payload_size_bytes, expected_payload_size);
    assert_eq!(
        full_block_payload.cache_metadata,
        json!({
            "source": "json-rpc",
            "method": "eth_getBlockByHash",
            "fetch_mode": "block_hash",
            "digest_scope": "json_rpc_response_body",
        })
    );

    let raw_block = provider_block_to_raw_block(chain, &block, CanonicalityState::Canonical);
    let upserts = provider_raw_payload_cache_metadata_to_upserts(
        chain,
        &raw_block,
        std::slice::from_ref(full_block_payload),
    );
    bigname_storage::upsert_raw_payload_cache_metadata(database.pool(), &upserts).await?;

    let persisted = bigname_storage::load_raw_payload_cache_metadata(
        database.pool(),
        chain,
        &block.block_hash,
        provider::RAW_PAYLOAD_KIND_FULL_BLOCK,
        Some(&full_block_payload.digest_algorithm),
        Some(&full_block_payload.retained_digest),
    )
    .await?
    .expect("provider fetch metadata must be persisted for later cache fill");
    assert_eq!(
        persisted.retained_digest.as_deref(),
        Some(full_block_payload.retained_digest.as_str())
    );
    assert_eq!(persisted.payload_size_bytes, expected_payload_size);

    let filled_block = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            chain,
            &block.block_hash,
            block.block_number,
        )
        .await?;
    assert_eq!(filled_block, block);

    let number_error = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            chain,
            &block.block_hash,
            block.block_number + 1,
        )
        .await
        .expect_err(
            "cache-fill must validate the returned block number after digest authorization",
        );
    assert!(
        number_error
            .to_string()
            .contains("with block number 100; expected 101"),
        "unexpected error: {number_error:#}"
    );

    let requested_block = provider_block(
        "0xc0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0",
        Some("0xd0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0"),
        200,
    );
    let returned_block = provider_block(
        "0xe0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0",
        requested_block.parent_hash.as_deref(),
        requested_block.block_number,
    );
    let mismatched_response_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": rpc_block_bundle_payload(&returned_block),
    })
    .to_string();
    let mismatched_payload_size = i64::try_from(mismatched_response_body.len())
        .context("mismatched JSON-RPC response body size must fit in i64")?;
    bigname_storage::upsert_raw_payload_cache_metadata(
        database.pool(),
        &[bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: chain.to_owned(),
            block_hash: requested_block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: Some("keccak256".to_owned()),
            retained_digest: Some(keccak256_hex(mismatched_response_body.as_bytes())),
            block_number: Some(requested_block.block_number),
            payload_size_bytes: mismatched_payload_size,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "method": "eth_getBlockByHash",
                "fetch_mode": "block_hash",
                "digest_scope": "json_rpc_response_body"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let requested_hash = requested_block.block_hash.clone();
    let returned_hash = returned_block.block_hash.clone();
    let (mismatched_url, mismatched_server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let first_param = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        match (method, first_param.as_str()) {
            ("eth_getBlockByHash", hash) if hash == requested_hash => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": rpc_block_bundle_payload(&returned_block),
            }),
            _ => panic!("unexpected RPC request: {body}"),
        }
    }))
    .await?;
    let mismatched_provider = provider::JsonRpcProvider::new(&mismatched_url)?;

    let hash_error = mismatched_provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            chain,
            &requested_block.block_hash,
            requested_block.block_number,
        )
        .await
        .expect_err("cache-fill must validate the returned block hash after digest authorization");
    assert!(
        hash_error.to_string().contains(&format!(
            "provider cache-fill returned block {returned_hash} for requested hash {}",
            requested_block.block_hash
        )),
        "unexpected error: {hash_error:#}"
    );

    mismatched_server.abort();
    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn cache_fill_requires_retained_digest() -> Result<()> {
    let database = TestDatabase::new().await?;
    let block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        Some("0xb1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1"),
        101,
    );
    bigname_storage::upsert_raw_payload_cache_metadata(
        database.pool(),
        &[bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: None,
            retained_digest: None,
            block_number: Some(block.block_number),
            payload_size_bytes: 0,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "method": "eth_getBlockByHash",
                "fetch_mode": "block_hash",
                "digest_scope": "json_rpc_response_body"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let (provider, server) = bundle_provider(vec![block.clone()]).await?;

    let error = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            "ethereum-mainnet",
            &block.block_hash,
            block.block_number,
        )
        .await
        .expect_err("cache-fill must reject metadata without a retained digest");
    assert!(
        error.to_string().contains("has no retained digest"),
        "unexpected error: {error:#}"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn cache_fill_rejects_digest_mismatch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let block = provider_block(
        "0xa2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2",
        Some("0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"),
        102,
    );
    bigname_storage::upsert_raw_payload_cache_metadata(
        database.pool(),
        &[bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: Some("keccak256".to_owned()),
            retained_digest: Some(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned(),
            ),
            block_number: Some(block.block_number),
            payload_size_bytes: 1,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "method": "eth_getBlockByHash",
                "fetch_mode": "block_hash",
                "digest_scope": "json_rpc_response_body"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let (provider, server) = bundle_provider(vec![block.clone()]).await?;

    let error = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            "ethereum-mainnet",
            &block.block_hash,
            block.block_number,
        )
        .await
        .expect_err("cache-fill must reject mismatched retained digests");
    assert!(
        error
            .to_string()
            .contains("raw payload cache digest mismatch"),
        "unexpected error: {error:#}"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_registrar_name_observation_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(32);
    let registrar_address = "0x00000000000000000000000000000000000000aa";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_registrar_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for registrar observation reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        registrar_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        None,
        42,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![rpc_registrar_name_registered_log_payload(
            &canonical_head,
            registrar_address,
            "registrar",
            canonical_head.block_timestamp_unix_secs + 31_536_000,
        )],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("registrar observation reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(42));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        6
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_registrar_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        "registrar.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'source_event' FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        "NameRegistered".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_fact_ref->>'emitting_address' FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        registrar_address.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_ensv1_reverse_claim_normalized_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x341);
    let reverse_address = "0x00000000000000000000000000000000000000ad";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for ENSv1 reverse reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xabababababababababababababababababababababababababababababababab",
        Some("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
        63,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![rpc_reverse_claimed_log_payload(
            &canonical_head,
            reverse_address,
            claimed_address,
            0,
        )],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("ENSv1 reverse reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(63));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_kind FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ReverseChanged".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT derivation_kind FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_claim".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_node' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_node_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'source_family' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_role' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_instance_id' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_contract_instance_id.to_string()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'emitting_address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_address.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_basenames_reverse_claim_normalized_events() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x346);
    let reverse_address = "0x0000000000d8e504002cc26e3ec46d81971c1664";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                1,
                1,
                'basenames',
                'basenames_base_primary',
                'base-mainnet',
                'basenames_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/basenames/basenames_base_primary/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames reverse reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xbabababababababababababababababababababababababababababababababa",
        Some("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
        63,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![rpc_l2_reverse_name_log_payload(
            &canonical_head,
            reverse_address,
            claimed_address,
            "alice.base.eth",
            0,
        )],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("Basenames reverse reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(63));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT namespace FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'source_family' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_ensv1_primary_claim_source_observations() -> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x343);
    let registry_contract_instance_id = Uuid::from_u128(0x344);
    let resolver_contract_instance_id = Uuid::from_u128(0x345);
    let reverse_address = "0x00000000000000000000000000000000000000ad";
    let registry_address = "0x00000000000000000000000000000000000000ae";
    let resolver_address = "0x00000000000000000000000000000000000000af";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";
    let reverse_node = reverse_node_for_address(claimed_address);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'ens',
                    'ens_v1_reverse_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_reverse_l1/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v1_registry_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'ens',
                    'ens_v1_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_resolver_l1/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for primary-claim source reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "public_resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacac",
        Some("0xbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbd"),
        65,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_reverse_claimed_log_payload(&canonical_head, reverse_address, claimed_address, 0),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &reverse_node,
                resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                "alice.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                7,
                3,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("primary-claim source reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE logical_name_id IS NULL AND resource_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_node
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'contract_role' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_basenames_primary_claim_source_observations()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x347);
    let registry_contract_instance_id = Uuid::from_u128(0x348);
    let resolver_contract_instance_id = Uuid::from_u128(0x349);
    let reverse_address = "0x0000000000d8e504002cc26e3ec46d81971c1664";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";
    let reverse_node = base_reverse_node_for_address(claimed_address);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'basenames',
                    'basenames_base_primary',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_primary/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context(
        "failed to insert manifest_versions for Basenames primary-claim source reconciliation test",
    )?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad",
        Some("0xbebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebe"),
        65,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_l2_reverse_name_log_payload(
                &canonical_head,
                reverse_address,
                claimed_address,
                "alice.base.eth",
                0,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &reverse_node,
                resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                "alice.base.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                7,
                3,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("Basenames primary-claim source reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND resource_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_node
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'source_family' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'contract_role' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_unwrapped_ensv1_authority_identity_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(33);
    let registry_contract_instance_id = Uuid::from_u128(34);
    let resolver_contract_instance_id = Uuid::from_u128(35);
    let registrar_address = "0x00000000000000000000000000000000000000ab";
    let registry_address = "0x00000000000000000000000000000000000000ac";
    let resolver_address = "0x00000000000000000000000000000000000000cc";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_registrar_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for unwrapped authority reconciliation test")?;
    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                2,
                1,
                'ens',
                'ens_v1_registry_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_registry_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context(
        "failed to insert registry manifest_versions for unwrapped authority reconciliation test",
    )?;
    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                3,
                1,
                'ens',
                'ens_v1_resolver_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_resolver_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context(
        "failed to insert resolver manifest_versions for unwrapped authority reconciliation test",
    )?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        registrar_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        2,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "public_resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        52,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_registrar_name_registered_log_payload(
                &canonical_head,
                registrar_address,
                "alice",
                canonical_head.block_timestamp_unix_secs + 31_536_000,
            ),
            rpc_registry_new_resolver_log_payload(
                &canonical_head,
                registry_address,
                "alice",
                resolver_address,
                1,
            ),
            rpc_resolver_text_changed_log_payload(
                &canonical_head,
                resolver_address,
                "alice",
                "com.twitter",
                2,
            ),
            rpc_resolver_addr_changed_log_payload(
                &canonical_head,
                resolver_address,
                "alice",
                "0x00000000000000000000000000000000000000aa",
                3,
            ),
            rpc_resolver_version_changed_log_payload(
                &canonical_head,
                resolver_address,
                "alice",
                7,
                4,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("unwrapped authority reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(52));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM token_lineages")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resources")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM name_surfaces")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM surface_bindings")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT binding_kind FROM surface_bindings LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "declared_registry_path".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RegistrationGranted'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'AuthorityEpochChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'SurfaceBound'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordChanged' AND canonicality_state = 'canonical'::canonicality_state"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordVersionChanged' AND canonicality_state = 'canonical'::canonicality_state"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'record_key' ORDER BY after_state->>'record_key') FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordChanged'"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        vec!["addr:60".to_owned(), "text:com.twitter".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'record_version' FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordVersionChanged'"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        "7".to_owned()
    );
    let resolver_event_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events WHERE event_kind = 'ResolverChanged'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        resolver_event_resource_id,
        sqlx::query_scalar::<_, Uuid>("SELECT resource_id FROM resources LIMIT 1")
            .fetch_one(database.pool())
            .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
        )
        .bind(resolver_event_resource_id)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resource' LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        "resource".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resolver' LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        "resolver".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_registry_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "0x00000000000000000000000000000000000000cc".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_gates_discovered_ensv1_resolver_local_facts_by_profile()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(0x381);
    let registry_contract_instance_id = Uuid::from_u128(0x382);
    let public_resolver_seed_contract_instance_id = Uuid::from_u128(0x383);
    let supported_resolver_contract_instance_id = Uuid::from_u128(0x384);
    let pending_resolver_contract_instance_id = Uuid::from_u128(0x385);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x386);
    let registrar_address = "0x0000000000000000000000000000000000000381";
    let registry_address = "0x0000000000000000000000000000000000000382";
    let public_resolver_seed_address = "0x0000000000000000000000000000000000000383";
    let supported_resolver_address = "0x0000000000000000000000000000000000000384";
    let pending_resolver_address = "0x0000000000000000000000000000000000000385";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000386";
    let public_resolver_code_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'ens',
                    'ens_v1_registrar_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_registrar_l1/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v1_registry_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'ens',
                    'ens_v1_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_resolver_l1/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for ENSv1 resolver profile gate test")?;

    for (contract_instance_id, address, manifest_id, role) in [
        (
            registrar_contract_instance_id,
            registrar_address,
            1,
            "registrar",
        ),
        (
            registry_contract_instance_id,
            registry_address,
            2,
            "registry",
        ),
        (
            public_resolver_seed_contract_instance_id,
            public_resolver_seed_address,
            3,
            "public_resolver",
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            address,
            Some(manifest_id),
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            manifest_id,
            role,
            contract_instance_id,
            address,
            "none",
            None,
            None,
        )
        .await?;
    }

    for (contract_instance_id, address) in [
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            address,
            Some(3),
        )
        .await?;
        insert_active_discovery_edge(
            database.pool(),
            "ethereum-mainnet",
            "resolver",
            registry_contract_instance_id,
            contract_instance_id,
            Some(2),
        )
        .await?;
    }

    upsert_raw_code_hashes(
        database.pool(),
        &[
            RawCodeHash {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: public_resolver_seed_address.to_owned(),
                code_hash: public_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: supported_resolver_address.to_owned(),
                code_hash: public_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: unsupported_resolver_address.to_owned(),
                code_hash: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0x3838383838383838383838383838383838383838383838383838383838383838",
        Some("0x3737373737373737373737373737373737373737373737373737373737373737"),
        52,
    );
    let alice_namehash = namehash_for_dns_name(&dns_encoded_eth_name("alice"));
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_registrar_name_registered_log_payload(
                &canonical_head,
                registrar_address,
                "alice",
                canonical_head.block_timestamp_unix_secs + 31_536_000,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                supported_resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                "supported.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                7,
                3,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                pending_resolver_address,
                4,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                "pending.eth",
                5,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                8,
                6,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                unsupported_resolver_address,
                7,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                "unsupported.eth",
                8,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                9,
                9,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("ENSv1 resolver profile gate reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        10
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'raw_name' ORDER BY log_index) FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec!["supported.eth".to_owned(), "pending.eth".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'record_version' ORDER BY log_index) FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec!["7".to_owned(), "8".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind IN ('RecordChanged', 'RecordVersionChanged') AND log_index = ANY($1::BIGINT[])"
        )
        .bind(vec![8_i64, 9])
        .fetch_one(database.pool())
        .await?,
        0
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_gates_basenames_dynamic_resolver_local_facts_by_l2_profile()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(0x391);
    let registry_contract_instance_id = Uuid::from_u128(0x392);
    let seed_resolver_contract_instance_id = Uuid::from_u128(0x393);
    let supported_resolver_contract_instance_id = Uuid::from_u128(0x394);
    let pending_resolver_contract_instance_id = Uuid::from_u128(0x395);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x396);
    let registrar_address = "0x0000000000000000000000000000000000000391";
    let registry_address = "0x0000000000000000000000000000000000000392";
    let seed_resolver_address = "0x0000000000000000000000000000000000000393";
    let supported_resolver_address = "0x0000000000000000000000000000000000000394";
    let pending_resolver_address = "0x0000000000000000000000000000000000000395";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000396";
    let l2_resolver_code_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'basenames',
                    'basenames_base_registrar',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames resolver profile gate test")?;

    for (contract_instance_id, chain, contract_kind) in [
        (registrar_contract_instance_id, "base-mainnet", "contract"),
        (registry_contract_instance_id, "base-mainnet", "root"),
        (
            seed_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
        (
            supported_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
        (
            pending_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
        (
            unsupported_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
    ] {
        insert_contract_instance(database.pool(), contract_instance_id, chain, contract_kind)
            .await?;
    }

    for (contract_instance_id, address, manifest_id) in [
        (registrar_contract_instance_id, registrar_address, 1),
        (registry_contract_instance_id, registry_address, 2),
        (seed_resolver_contract_instance_id, seed_resolver_address, 3),
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
            3,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
            3,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
            3,
        ),
    ] {
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "base-mainnet",
            address,
            Some(manifest_id),
        )
        .await?;
    }

    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        2,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "resolver",
        seed_resolver_contract_instance_id,
        seed_resolver_address,
        "none",
        None,
        None,
    )
    .await?;
    for contract_instance_id in [
        supported_resolver_contract_instance_id,
        pending_resolver_contract_instance_id,
        unsupported_resolver_contract_instance_id,
    ] {
        insert_active_discovery_edge(
            database.pool(),
            "base-mainnet",
            "resolver",
            registry_contract_instance_id,
            contract_instance_id,
            Some(2),
        )
        .await?;
    }

    upsert_raw_code_hashes(
        database.pool(),
        &[
            RawCodeHash {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: seed_resolver_address.to_owned(),
                code_hash: l2_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: supported_resolver_address.to_owned(),
                code_hash: l2_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: unsupported_resolver_address.to_owned(),
                code_hash: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0x3939393939393939393939393939393939393939393939393939393939393939",
        Some("0x3838383838383838383838383838383838383838383838383838383838383838"),
        52,
    );
    let alice_namehash = namehash_for_dns_name(&dns_encoded_base_eth_name("alice"));
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_basenames_name_registered_log_payload(
                &canonical_head,
                registrar_address,
                "alice",
                canonical_head.block_timestamp_unix_secs + 31_536_000,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                supported_resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                "supported.base.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                7,
                3,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                pending_resolver_address,
                4,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                "pending.base.eth",
                5,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                8,
                6,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                unsupported_resolver_address,
                7,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                "unsupported.base.eth",
                8,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                9,
                9,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("Basenames resolver profile gate reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        10
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "supported.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'record_version' FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "7".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind IN ('RecordChanged', 'RecordVersionChanged') AND log_index = ANY($1::BIGINT[])"
        )
        .bind(vec![5_i64, 6, 8, 9])
        .fetch_one(database.pool())
        .await?,
        0
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_ensv2_resolver_and_permission_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    create_complete_raw_log_staging_input_fixture(database.pool(), "ethereum-mainnet", 61).await?;
    let registry_contract_instance_id = Uuid::from_u128(0x371);
    let registry_address = "0x0000000000000000000000000000000000000371";
    let resolver_address = "0x0000000000000000000000000000000000000372";
    let owner_address = "0x00000000000000000000000000000000000000aa";
    let sender_address = "0x00000000000000000000000000000000000000bb";
    let operator_address = "0x00000000000000000000000000000000000000cc";
    let record_address = "0x00000000000000000000000000000000000000dd";
    let token_id = hex_string(&abi_word_u64(1));
    let upstream_resource = hex_string(&abi_word_u64(42));
    let alice_dns_name = dns_encoded_eth_name("alice");
    let alice_namehash = namehash_for_dns_name(&alice_dns_name);
    let new_role_bitmap = hex_string(&abi_word_u64(1));
    let zero_role_bitmap = hex_string(&abi_word_u64(0));

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'ens',
                    'ens_v2_registry_l1',
                    'ethereum-mainnet',
                    'ens_v2',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v2_registry_l1/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v2_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v2',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v2_resolver_l1/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for ENSv2 resolver reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        1,
        "resolver",
        "registry",
        "reachable_from_root",
    )
    .await?;
    // This test starts at block 61. Model the registry coverage a normal
    // bootstrap would already have established, while leaving the resolver
    // discovered at block 61 for the live exact-coverage recovery path.
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        "ethereum-mainnet",
        0,
        60,
        "ens_v2_registry_l1",
        &[registry_address],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
        Some("0xe1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"),
        61,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x0",
                "address": registry_address,
                "topics": [
                    ens_v2_label_registered_topic0(),
                    token_id.clone(),
                    labelhash_hex("alice"),
                    hex_string(&abi_word_address(sender_address))
                ],
                "data": encode_ens_v2_label_registered_log_data(
                    "alice",
                    owner_address,
                    canonical_head.block_timestamp_unix_secs + 31_536_000,
                )
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x1",
                "address": registry_address,
                "topics": [
                    ens_v2_token_resource_topic0(),
                    token_id.clone(),
                    upstream_resource.clone()
                ],
                "data": "0x"
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x2",
                "address": registry_address,
                "topics": [
                    ens_v2_resolver_updated_topic0(),
                    token_id.clone(),
                    hex_string(&abi_word_address(resolver_address)),
                    hex_string(&abi_word_address(sender_address))
                ],
                "data": "0x"
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x3",
                "address": resolver_address,
                "topics": [
                    ens_v2_resolver_address_changed_topic0(),
                    alice_namehash.clone()
                ],
                "data": encode_ens_v2_resolver_address_changed_log_data(
                    60,
                    &decode_hex_string(record_address),
                )
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x4",
                "address": resolver_address,
                "topics": [
                    ens_v2_named_resource_topic0(),
                    upstream_resource.clone()
                ],
                "data": encode_dynamic_bytes_log_data(&alice_dns_name)
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x5",
                "address": resolver_address,
                "topics": [
                    ens_v2_eac_roles_changed_topic0(),
                    upstream_resource.clone(),
                    hex_string(&abi_word_address(operator_address))
                ],
                "data": encode_eac_roles_changed_log_data(&zero_role_bitmap, &new_role_bitmap)
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x6",
                "address": resolver_address,
                "topics": [
                    ens_v2_alias_changed_topic0(),
                    keccak256_hex(&alice_dns_name),
                    keccak256_hex(&[])
                ],
                "data": encode_two_dynamic_bytes_log_data(&alice_dns_name, &[])
            }),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    let heads = ProviderHeadSnapshot {
        canonical: canonical_head.clone(),
        safe: None,
        finalized: None,
    };
    let missing_coverage_error =
        reconcile_fetched_heads(database.pool(), &tasks[0], &provider, &heads)
            .await
            .expect_err(
                "ENSv2 resolver reconciliation must request newly discovered resolver coverage",
            );
    let missing_coverage = missing_coverage_error
        .downcast_ref::<bigname_adapters::EnsV2MissingCoverage>()
        .context("ENSv2 reconciliation returned the wrong coverage error")?
        .clone();
    assert_eq!(missing_coverage.source_family, "ens_v2_resolver_l1");
    assert_eq!(missing_coverage.address, resolver_address);
    assert_eq!(missing_coverage.required_from_block, 61);
    assert_eq!(missing_coverage.required_to_block, 61);
    assert_eq!(
        recover_ens_v2_live_coverage_requirement(
            database.pool(),
            "test",
            &provider,
            HeaderAuditMode::Minimal,
            &missing_coverage,
        )
        .await?,
        EnsV2LiveCoverageRecoveryStatus::Recovered
    );

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &heads,
    )
    .await?
    .expect("ENSv2 resolver reconciliation must update task state after exact coverage recovery");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(61));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT cia.address FROM discovery_edges de JOIN contract_instance_addresses cia ON cia.contract_instance_id = de.to_contract_instance_id WHERE de.edge_kind = 'resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        resolver_address.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'record_key' FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        "addr:60".to_owned()
    );
    let record_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        record_resource_id,
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM surface_bindings WHERE logical_name_id = 'ens:alice.eth'"
        )
        .fetch_one(database.pool())
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        "resolver".to_owned()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT after_state->'effective_powers' ? 'set_addr' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v2_resolver_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'alias_state' FROM normalized_events WHERE event_kind = 'AliasChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        "removed".to_owned()
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT (after_state->>'active')::BOOLEAN FROM normalized_events WHERE event_kind = 'AliasChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved' AND logical_name_id IS NULL AND resource_id IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'source_event' ORDER BY after_state->>'source_event') FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            "AliasChanged".to_owned(),
            "LabelRegistered".to_owned(),
            "NamedResource".to_owned(),
        ]
    );
    let resolver_preimage_fact_refs = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT after_state->>'source_event', raw_fact_ref->>'data_hex'
        FROM normalized_events
        WHERE event_kind = 'PreimageObserved'
          AND after_state->>'source_event' IN ('AliasChanged', 'NamedResource')
        ORDER BY after_state->>'source_event'
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        resolver_preimage_fact_refs,
        vec![
            (
                "AliasChanged".to_owned(),
                encode_two_dynamic_bytes_log_data(&alice_dns_name, &[])
                    .trim_start_matches("0x")
                    .to_owned(),
            ),
            (
                "NamedResource".to_owned(),
                encode_dynamic_bytes_log_data(&alice_dns_name)
                    .trim_start_matches("0x")
                    .to_owned(),
            ),
        ]
    );

    let pre_admission_hash = "0xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0";
    let pre_admission_tx = "0x0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";
    sqlx::query(
        r#"
            INSERT INTO chain_lineage (
                chain_id,
                block_hash,
                parent_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES (
                'ethereum-mainnet',
                $1,
                '0xe0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0',
                60,
                to_timestamp($2),
                'canonical'
            )
            "#,
    )
    .bind(pre_admission_hash)
    .bind(canonical_head.block_timestamp_unix_secs - 12)
    .execute(database.pool())
    .await
    .context("failed to insert pre-admission raw block")?;
    sqlx::query(
        r#"
            INSERT INTO raw_logs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            VALUES (
                'ethereum-mainnet',
                $1,
                60,
                $2,
                0,
                0,
                $3,
                $4,
                $5,
                'canonical'
            )
            "#,
    )
    .bind(pre_admission_hash)
    .bind(pre_admission_tx)
    .bind(resolver_address)
    .bind(vec![
        ens_v2_resolver_address_changed_topic0(),
        alice_namehash.clone(),
    ])
    .bind(decode_hex_string(
        &encode_ens_v2_resolver_address_changed_log_data(60, &decode_hex_string(record_address)),
    ))
    .execute(database.pool())
    .await
    .context("failed to insert pre-admission resolver raw log")?;
    sqlx::query(
        r#"
            INSERT INTO raw_logs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            VALUES (
                'ethereum-mainnet',
                $1,
                60,
                $2,
                0,
                1,
                $3,
                $4,
                $5,
                'canonical'
            )
            "#,
    )
    .bind(pre_admission_hash)
    .bind(pre_admission_tx)
    .bind(resolver_address)
    .bind(vec![
        ens_v2_eac_roles_changed_topic0(),
        upstream_resource.clone(),
        hex_string(&abi_word_address(operator_address)),
    ])
    .bind(decode_hex_string(&encode_eac_roles_changed_log_data(
        &zero_role_bitmap,
        &new_role_bitmap,
    )))
    .execute(database.pool())
    .await
    .context("failed to insert pre-admission permissions raw log")?;

    bigname_adapters::sync_ens_v2_resolver(database.pool(), "ethereum-mainnet").await?;
    bigname_adapters::sync_ens_v2_permissions(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_basenames_unwrapped_authority_identity_rows()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(0x351);
    let registry_contract_instance_id = Uuid::from_u128(0x352);
    let resolver_contract_instance_id = Uuid::from_u128(0x353);
    let registrar_address = "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
    let alice_namehash = namehash_for_dns_name(&dns_encoded_base_eth_name("alice"));

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'basenames',
                    'basenames_base_registrar',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames authority reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        "root",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        registrar_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        2,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xedededededededededededededededededededededededededededededededed",
        Some("0xfefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe"),
        52,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_basenames_name_registered_log_payload(
                &canonical_head,
                registrar_address,
                "alice",
                canonical_head.block_timestamp_unix_secs + 31_536_000,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                resolver_address,
                1,
            ),
            rpc_resolver_text_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &alice_namehash,
                "com.twitter",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &alice_namehash,
                7,
                3,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("Basenames authority reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(52));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM token_lineages")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resources")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM name_surfaces")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM surface_bindings")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "basenames:alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT canonical_display_name FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT namespace FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT binding_kind FROM surface_bindings LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "declared_registry_path".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_registry".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT namespace FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

struct HashFailingProvider<'a> {
    inner: &'a JsonRpcProvider,
    failing_hash: String,
}

impl crate::provider::ChainProviderOps for HashFailingProvider<'_> {
    async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        self.inner.fetch_chain_heads().await
    }

    async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<crate::provider::ProviderResolvedBlock>> {
        self.inner
            .fetch_block_hashes_by_numbers(block_numbers)
            .await
    }

    async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock> {
        if block_hash.eq_ignore_ascii_case(&self.failing_hash) {
            anyhow::bail!("test provider intentionally cannot serve block hash {block_hash}");
        }
        self.inner.fetch_block_by_hash(block_hash).await
    }

    async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        self.inner
            .fetch_block_headers_by_hashes(resolved_blocks)
            .await
    }

    async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
    ) -> Result<Vec<crate::provider::ProviderBlockBundle>> {
        self.inner
            .fetch_block_bundles_by_hashes(resolved_blocks)
            .await
    }

    async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
    ) -> Result<Vec<crate::provider::ProviderBlockBundle>> {
        self.inner
            .fetch_block_bundles_without_logs_by_hashes(resolved_blocks)
            .await
    }

    async fn fetch_block_bundle_by_hash(
        &self,
        block_hash: &str,
    ) -> Result<crate::provider::ProviderBlockBundle> {
        self.inner.fetch_block_bundle_by_hash(block_hash).await
    }

    async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<std::collections::BTreeMap<i64, Vec<crate::provider::ProviderLog>>> {
        self.inner
            .fetch_logs_by_block_range(resolved_blocks, addresses)
            .await
    }

    async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<std::collections::BTreeMap<i64, Vec<crate::provider::ProviderLog>>> {
        self.inner
            .fetch_logs_by_block_range_for_topic0s_and_addresses(
                resolved_blocks,
                topic0s,
                addresses,
            )
            .await
    }

    async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[crate::provider::ProviderTransactionReceiptRequest],
    ) -> Result<Vec<crate::provider::ProviderTransactionReceiptBundle>> {
        self.inner
            .fetch_transaction_receipt_pairs_by_hashes(requests)
            .await
    }

    async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: crate::provider::ProviderBlockSelection,
    ) -> Result<Vec<crate::provider::ProviderCodeObservation>> {
        self.inner
            .fetch_code_observations_at_block(addresses, block)
            .await
    }

    async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[crate::provider::ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<crate::provider::ProviderBlockCodeObservations>> {
        self.inner
            .fetch_code_observations_at_block_hashes(requests)
            .await
    }
}

/// Fails every block-number resolution to prove such RPC failures propagate
/// out of stored-frontier anchor selection instead of degrading to a refusal.
struct NumberResolutionFailingProvider<'a> {
    inner: &'a JsonRpcProvider,
}

impl crate::provider::ChainProviderOps for NumberResolutionFailingProvider<'_> {
    async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        self.inner.fetch_chain_heads().await
    }

    async fn fetch_block_hashes_by_numbers(
        &self,
        _block_numbers: &[i64],
    ) -> Result<Vec<crate::provider::ProviderResolvedBlock>> {
        anyhow::bail!("test provider intentionally cannot resolve block numbers");
    }

    async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock> {
        self.inner.fetch_block_by_hash(block_hash).await
    }

    async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        self.inner
            .fetch_block_headers_by_hashes(resolved_blocks)
            .await
    }

    async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
    ) -> Result<Vec<crate::provider::ProviderBlockBundle>> {
        self.inner
            .fetch_block_bundles_by_hashes(resolved_blocks)
            .await
    }

    async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
    ) -> Result<Vec<crate::provider::ProviderBlockBundle>> {
        self.inner
            .fetch_block_bundles_without_logs_by_hashes(resolved_blocks)
            .await
    }

    async fn fetch_block_bundle_by_hash(
        &self,
        block_hash: &str,
    ) -> Result<crate::provider::ProviderBlockBundle> {
        self.inner.fetch_block_bundle_by_hash(block_hash).await
    }

    async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<std::collections::BTreeMap<i64, Vec<crate::provider::ProviderLog>>> {
        self.inner
            .fetch_logs_by_block_range(resolved_blocks, addresses)
            .await
    }

    async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[crate::provider::ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<std::collections::BTreeMap<i64, Vec<crate::provider::ProviderLog>>> {
        self.inner
            .fetch_logs_by_block_range_for_topic0s_and_addresses(
                resolved_blocks,
                topic0s,
                addresses,
            )
            .await
    }

    async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[crate::provider::ProviderTransactionReceiptRequest],
    ) -> Result<Vec<crate::provider::ProviderTransactionReceiptBundle>> {
        self.inner
            .fetch_transaction_receipt_pairs_by_hashes(requests)
            .await
    }

    async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: crate::provider::ProviderBlockSelection,
    ) -> Result<Vec<crate::provider::ProviderCodeObservation>> {
        self.inner
            .fetch_code_observations_at_block(addresses, block)
            .await
    }

    async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[crate::provider::ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<crate::provider::ProviderBlockCodeObservations>> {
        self.inner
            .fetch_code_observations_at_block_hashes(requests)
            .await
    }
}

fn rpc_current_name_wrapped_log_payload(block: &ProviderBlock) -> Value {
    let dns_name = dns_encoded_test_name();
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": "0x0000000000000000000000000000000000000001",
        "topics": [
            keccak256_hex(b"NameWrapped(bytes32,bytes,address,uint32,uint64)"),
            namehash_for_dns_name(&dns_name)
        ],
        "data": encode_name_wrapped_log_data(&dns_name)
    })
}
