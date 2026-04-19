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
    let safe_head = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
        41,
    );
    let finalized_head = provider_block(
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        40,
    );
    let (provider, server) = bundle_provider(vec![
        canonical_head.clone(),
        safe_head.clone(),
        finalized_head.clone(),
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
    assert_eq!(next_task.checkpoint.safe_block_number, Some(41));
    assert_eq!(next_task.checkpoint.finalized_block_number, Some(40));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_blocks")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_transactions")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_receipts")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        3
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
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 40"
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
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 40"
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
            "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_number = 40"
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
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events WHERE block_number = 42"
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
                'uts46-v1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                '{}'::jsonb
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
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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
                'uts46-v1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                '{}'::jsonb
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
    let reverse_address = "0x79ea96012eea67a83431f1701b3dff7e37f9e282";
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
                'uts46-v1',
                'manifests/basenames/basenames_base_primary/v1.toml',
                '{}'::jsonb
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
        1
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
                    'uts46-v1',
                    'manifests/ens/ens_v1_reverse_l1/v1.toml',
                    '{}'::jsonb
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v1_registry_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'uts46-v1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    '{}'::jsonb
                ),
                (
                    3,
                    1,
                    'ens',
                    'ens_v1_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'uts46-v1',
                    'manifests/ens/ens_v1_resolver_l1/v1.toml',
                    '{}'::jsonb
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
    let reverse_address = "0x79ea96012eea67a83431f1701b3dff7e37f9e282";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
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
                    'basenames',
                    'basenames_base_primary',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_primary/v1.toml',
                    '{}'::jsonb
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    '{}'::jsonb
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    '{}'::jsonb
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
                'uts46-v1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                '{}'::jsonb
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
                'uts46-v1',
                'manifests/ens/ens_v1_registry_l1/v1.toml',
                '{}'::jsonb
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
                'uts46-v1',
                'manifests/ens/ens_v1_resolver_l1/v1.toml',
                '{}'::jsonb
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
        vec!["addr:60".to_owned(), "text".to_owned()]
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
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    '{}'::jsonb
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    '{}'::jsonb
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    '{}'::jsonb
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

