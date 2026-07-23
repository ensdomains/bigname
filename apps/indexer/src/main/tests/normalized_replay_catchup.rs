#[tokio::test]
async fn normalized_replay_catchup_rewinds_for_later_older_raw_backfill() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x390);
    let reverse_address = "0x00000000000000000000000000000000000000af";
    let newer_claimed_address = "0x2222222222222222222222222222222222222222";
    let older_claimed_address = "0x3333333333333333333333333333333333333333";
    let newer_block = provider_block(
        "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0",
        Some("0x9090909090909090909090909090909090909090909090909090909090909090"),
        100,
    );
    let older_block = provider_block(
        "0x5050505050505050505050505050505050505050505050505050505050505050",
        Some("0x4040404040404040404040404040404040404040404040404040404040404040"),
        50,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        390,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &newer_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &newer_block,
        reverse_address,
        newer_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let cursor_kind = "raw_fact_normalized_events";
    let last_replayed_at = sqlx::query_scalar::<_, sqlx::types::time::OffsetDateTime>(
        r#"
        SELECT last_replayed_at
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;

    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &older_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &older_block,
        reverse_address,
        older_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        "UPDATE raw_logs SET observed_at = $1 + INTERVAL '1 second' WHERE chain_id = $2 AND block_hash = $3",
    )
        .bind(last_replayed_at)
        .bind(chain)
        .bind(&older_block.block_hash)
        .execute(database.pool())
        .await?;

    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged' AND block_number = 50"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_replay_cursors WHERE cursor_kind LIKE 'raw_fact_normalized_events:%'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn spawned_normalized_replay_beats_on_progress_and_exposes_a_later_wedge() -> Result<()> {
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new(
            "bigname_indexer_normalized_replay_heartbeat_test",
        )
        .pool_max_connections(5),
        &bigname_storage::MIGRATOR,
        "failed to migrate normalized replay heartbeat test database",
    )
    .await?;
    let chain = "ethereum-mainnet";
    let instance_id = "normalized-replay-progress-test";
    let reverse_address = "0x00000000000000000000000000000000000000be";
    let block = provider_block(
        "0xbebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebe",
        Some("0xadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad"),
        100,
    );
    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        389,
        chain,
        "ens_v1_reverse_l1",
        Uuid::from_u128(0x389),
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block,
        reverse_address,
        "0x2222222222222222222222222222222222222222",
        CanonicalityState::Canonical,
    )
    .await?;
    install_stale_indexer_heartbeat(database.pool(), instance_id).await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?
    .with_defer_projection_indexes(false);
    let hook = normalized_replay_catchup::install_after_progress_test_hook(
        database.pool(),
        "mainnet",
        chain,
    )
    .await;
    let activity = crate::run::startup_heartbeat::RequiredSubtaskActivity::default();
    let child_activity = activity.clone();
    let catchup_pool = database.pool().clone();
    let catchup = tokio::spawn(async move {
        let mut heartbeat = crate::run::startup_heartbeat::NormalizedReplayHeartbeat::new(
            instance_id.to_owned(),
            tokio::time::Duration::ZERO,
            vec![chain.to_owned()],
        );
        normalized_replay_catchup::run_required_normalized_replay_catchup_iteration_for_test(
            &catchup_pool,
            &config,
            chain,
            &mut heartbeat,
            &child_activity,
        )
        .await
    });
    tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        hook.wait_until_after_progress(),
    )
    .await
    .context("normalized replay did not reach its post-progress barrier")?;
    let heartbeat = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("normalized replay must retain its registered heartbeat")?;
    assert!(
        heartbeat.age_seconds <= 1,
        "completed replay work must refresh the process heartbeat"
    );
    assert!(
        !catchup.is_finished(),
        "the replay iteration must remain blocked after its progress beat"
    );

    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'process'
        "#,
    )
    .bind(instance_id)
    .execute(database.pool())
    .await?;
    let parent_activity = activity.clone();
    let parent_pool = database.pool().clone();
    let parent = tokio::spawn(async move {
        let _required_subtask_exclusion = parent_activity.exclude_required_subtask().await;
        let mut parent_heartbeat = crate::run::startup_heartbeat::StartupHeartbeat::new(
            instance_id.to_owned(),
            tokio::time::Duration::ZERO,
        );
        parent_heartbeat
            .record(&parent_pool, &[chain.to_owned()])
            .await
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        !parent.is_finished(),
        "the parent heartbeat must wait while replay owns required-operation liveness"
    );
    let heartbeat_age = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("normalized replay must retain its registered heartbeat")?
    .age_seconds;
    assert!(
        heartbeat_age >= 30,
        "parent polling must not hide a wedged required replay iteration"
    );

    hook.resume();
    let status = tokio::time::timeout(tokio::time::Duration::from_secs(10), catchup)
        .await
        .context("normalized replay did not finish after its barrier was released")?
        .context("normalized replay task failed")??;
    assert_eq!(
        status,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    tokio::time::timeout(tokio::time::Duration::from_secs(10), parent)
        .await
        .context("parent heartbeat did not resume after replay completed")?
        .context("parent heartbeat task failed")??;
    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_failure_journal_keeps_child_heartbeat_ownership() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let instance_id = "normalized-replay-failure-journal-test";
    normalized_replay_catchup::ensure_cursor_for_test(
        database.pool(),
        "mainnet",
        chain,
        1,
        1,
        true,
    )
    .await?;
    install_stale_indexer_heartbeat(database.pool(), instance_id).await?;
    sqlx::query("DROP TABLE raw_logs CASCADE")
        .execute(database.pool())
        .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let hook = normalized_replay_catchup::install_before_cursor_failure_record_test_hook(
        database.pool(),
        "mainnet",
        chain,
    )
    .await;
    let activity = crate::run::startup_heartbeat::RequiredSubtaskActivity::default();
    let child_activity = activity.clone();
    let catchup_pool = database.pool().clone();
    let mut catchup = tokio::spawn(async move {
        let mut heartbeat = crate::run::startup_heartbeat::NormalizedReplayHeartbeat::new(
            instance_id.to_owned(),
            tokio::time::Duration::ZERO,
            vec![chain.to_owned()],
        );
        normalized_replay_catchup::run_required_normalized_replay_catchup_iteration_for_test(
            &catchup_pool,
            &config,
            chain,
            &mut heartbeat,
            &child_activity,
        )
        .await
    });
    tokio::select! {
        () = hook.wait_until_before_record() => {}
        result = &mut catchup => {
            panic!("normalized replay returned before failure journaling was protected: {result:?}");
        }
    }

    let parent_activity = activity.clone();
    let parent_pool = database.pool().clone();
    let parent = tokio::spawn(async move {
        let _required_subtask_exclusion = parent_activity.exclude_required_subtask().await;
        let mut parent_heartbeat = crate::run::startup_heartbeat::StartupHeartbeat::new(
            instance_id.to_owned(),
            tokio::time::Duration::ZERO,
        );
        parent_heartbeat
            .record(&parent_pool, &[chain.to_owned()])
            .await
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        !parent.is_finished(),
        "the parent heartbeat must wait while the child journals its failed work unit"
    );
    let heartbeat_age = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("normalized replay must retain its registered heartbeat")?
    .age_seconds;
    assert!(
        heartbeat_age >= 30,
        "parent polling must not hide a failure-journal wedge"
    );

    hook.resume();
    let catchup_error = tokio::time::timeout(tokio::time::Duration::from_secs(10), catchup)
        .await
        .context("normalized replay did not finish after failure journaling resumed")?
        .context("normalized replay task panicked")?
        .expect_err("the injected missing raw-log table must fail the replay iteration");
    assert!(
        catchup_error.to_string().contains("raw-log bounds"),
        "unexpected normalized replay error: {catchup_error:#}"
    );
    tokio::time::timeout(tokio::time::Duration::from_secs(10), parent)
        .await
        .context("parent heartbeat did not resume after failure journaling completed")?
        .context("parent heartbeat task panicked")??;
    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_log_bound_preserves_whole_blocks() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x391);
    let reverse_address = "0x00000000000000000000000000000000000000bf";
    let block_10 = provider_block(
        "0x1010101010101010101010101010101010101010101010101010101010101010",
        Some("0x0909090909090909090909090909090909090909090909090909090909090909"),
        10,
    );
    let block_11 = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some(&block_10.block_hash),
        11,
    );
    let block_12 = provider_block(
        "0x1212121212121212121212121212121212121212121212121212121212121212",
        Some(&block_11.block_hash),
        12,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        391,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block_10,
        reverse_address,
        "0x0000000000000000000000000000000000000010",
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_11,
        reverse_address,
        "0x0000000000000000000000000000000000000011",
        CanonicalityState::Canonical,
        0,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_11,
        reverse_address,
        "0x0000000000000000000000000000000000000012",
        CanonicalityState::Canonical,
        1,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block_12,
        reverse_address,
        "0x0000000000000000000000000000000000000013",
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        2,
        1,
    )?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );

    let cursor_kind = "raw_fact_normalized_events";
    let (last_completed, next_block) = sqlx::query_as::<_, (Option<i64>, i64)>(
        r#"
        SELECT last_completed_block_number, next_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(last_completed, Some(10));
    assert_eq!(next_block, 11);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_log_bound_allows_oversized_first_block() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x392);
    let reverse_address = "0x00000000000000000000000000000000000000cf";
    let block_20 = provider_block(
        "0x2020202020202020202020202020202020202020202020202020202020202020",
        Some("0x1919191919191919191919191919191919191919191919191919191919191919"),
        20,
    );
    let block_21 = provider_block(
        "0x2121212121212121212121212121212121212121212121212121212121212121",
        Some(&block_20.block_hash),
        21,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        392,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_20,
        reverse_address,
        "0x0000000000000000000000000000000000000020",
        CanonicalityState::Canonical,
        0,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_20,
        reverse_address,
        "0x0000000000000000000000000000000000000021",
        CanonicalityState::Canonical,
        1,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block_21,
        reverse_address,
        "0x0000000000000000000000000000000000000022",
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1,
        1,
    )?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );

    let cursor_kind = "raw_fact_normalized_events";
    let (last_completed, next_block) = sqlx::query_as::<_, (Option<i64>, i64)>(
        r#"
        SELECT last_completed_block_number, next_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(last_completed, Some(20));
    assert_eq!(next_block, 21);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_does_not_use_log_bound_as_stateful_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let wrapper_address = "0x0000000000000000000000000000000000000130";
    let reverse_address = "0x0000000000000000000000000000000000000131";
    let wrapper_block = provider_block(
        "0x3030303030303030303030303030303030303030303030303030303030303030",
        Some("0x2929292929292929292929292929292929292929292929292929292929292929"),
        30,
    );
    let reverse_block = provider_block(
        "0x3131313131313131313131313131313131313131313131313131313131313131",
        Some(&wrapper_block.block_hash),
        31,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        393,
        chain,
        Uuid::from_u128(0x393),
        wrapper_address,
    )
    .await?;
    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        394,
        chain,
        "ens_v1_reverse_l1",
        Uuid::from_u128(0x394),
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &wrapper_block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &reverse_block,
        reverse_address,
        "0x0000000000000000000000000000000000000031",
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query("DROP INDEX IF EXISTS normalized_events_namespace_idx")
        .execute(database.pool())
        .await
        .context("failed to drop deferred normalized event index for stateful catch-up test")?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1,
        1,
    )?;
    let stateless_pages =
        install_stateless_page_observer(database.pool(), "mainnet", chain).await?;
    let status = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await?;
    assert_eq!(
        status,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        stateless_pages.page_ranges(),
        vec![(30, 30), (31, 31)],
        "the stateless prelude must honor the configured whole-block raw-log page cap"
    );

    let cursor_kind = "raw_fact_normalized_events";
    let (last_completed, next_block) = sqlx::query_as::<_, (Option<i64>, i64)>(
        r#"
        SELECT last_completed_block_number, next_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(last_completed, Some(31));
    assert_eq!(next_block, 32);
    assert!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?
            > 0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation' \
               AND block_hash = $1"
        )
        .bind(&wrapper_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        1,
        "automatic full-closure catch-up must run the stateless preimage pass"
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('normalized_events_namespace_idx') IS NOT NULL"
        )
        .fetch_one(database.pool())
        .await?,
        "closure replay must restore deferred projection indexes before running"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_validates_retention_before_stateless_phase() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let deployment_profile = "retention-validation-test";
    let chain = "ethereum-mainnet";
    let wrapper_address = "0x0000000000000000000000000000000000000138";
    let block = provider_block(
        "0x3838383838383838383838383838383838383838383838383838383838383838",
        Some("0x3737373737373737373737373737373737373737373737373737373737373737"),
        38,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        398,
        chain,
        Uuid::from_u128(0x398),
        wrapper_address,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), chain, 2, 1).await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        deployment_profile.to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let stateless_pages =
        install_stateless_page_observer(database.pool(), deployment_profile, chain).await?;
    let error = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await
    .expect_err("incomplete retention authority must fail closed");
    assert!(
        format!("{error:#}").contains("retention generation 1"),
        "unexpected retention validation error: {error:#}"
    );
    assert_eq!(
        stateless_pages.page_ranges(),
        Vec::<(i64, i64)>::new(),
        "retention validation must abort before any stateless replay page"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation'"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "retention validation failure must attempt zero stateless upserts"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_retries_full_closure_after_stateless_phase_failure()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let deployment_profile = "automatic-catchup-test";
    let chain = "ethereum-mainnet";
    let wrapper_address = "0x0000000000000000000000000000000000000139";
    let block = provider_block(
        "0x3939393939393939393939393939393939393939393939393939393939393939",
        Some("0x3838383838383838383838383838383838383838383838383838383838383838"),
        39,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        399,
        chain,
        Uuid::from_u128(0x399),
        wrapper_address,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        deployment_profile.to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let _failure =
        install_after_stateless_failure(database.pool(), deployment_profile, chain).await?;
    let error = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await
    .expect_err("the injected phase-boundary failure must stop catch-up");
    assert!(
        format!("{error:#}").contains("injected failure after automatic stateless replay phase"),
        "unexpected phase-boundary error: {error:#}"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>)>(
            "SELECT next_block_number, last_completed_block_number \
             FROM normalized_replay_cursors \
             WHERE deployment_profile = $1 \
               AND chain_id = $2 \
               AND cursor_kind = 'raw_fact_normalized_events'"
        )
        .bind(deployment_profile)
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (block.block_number, None),
        "the shared completion cursor must stay pending between phases"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation'"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "phase one must durably publish the stateless preimage before the injected failure"
    );

    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>)>(
            "SELECT next_block_number, last_completed_block_number \
             FROM normalized_replay_cursors \
             WHERE deployment_profile = $1 \
               AND chain_id = $2 \
               AND cursor_kind = 'raw_fact_normalized_events'"
        )
        .bind(deployment_profile)
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (block.block_number + 1, Some(block.block_number)),
        "retry must complete phase two before publishing the shared cursor"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation'"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "retrying phase one must be identity-idempotent"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_accepts_current_generation_ensv1_full_history_coverage()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let wrapper_address = "0x0000000000000000000000000000000000000132";
    let suffix_block = provider_block(
        "0x3232323232323232323232323232323232323232323232323232323232323232",
        Some("0x3131313131313131313131313131313131313131313131313131313131313131"),
        32,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        395,
        chain,
        Uuid::from_u128(0x395),
        wrapper_address,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &suffix_block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
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
        )
        VALUES ($1, 1, 1, false, clock_timestamp())
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = EXCLUDED.revision,
            retention_generation = EXCLUDED.retention_generation,
            retained_history_complete = EXCLUDED.retained_history_complete,
            incomplete_since = EXCLUDED.incomplete_since,
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let coverage_job_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            raw_log_retention_generation,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status,
            completed_at
        )
        VALUES (
            'mainnet',
            $1,
            1,
            '{}'::jsonb,
            'hash_pinned_block',
            0,
            32,
            'generation-one-wrapper-closure',
            'completed',
            now()
        )
        RETURNING backfill_job_id
        "#,
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
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
        VALUES ($1, $2, 'ens_v1_wrapper_l1', 'address', lower($3), 0, 32, 'job_completion')
        "#,
    )
    .bind(coverage_job_id)
    .bind(chain)
    .bind(wrapper_address)
    .execute(database.pool())
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let outcome = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await?;
    assert_eq!(
        outcome,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>)>(
            r#"
            SELECT next_block_number, last_completed_block_number
            FROM normalized_replay_cursors
            WHERE deployment_profile = 'mainnet'
              AND chain_id = $1
              AND cursor_kind = 'raw_fact_normalized_events'
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (
            suffix_block.block_number + 1,
            Some(suffix_block.block_number)
        ),
        "current-generation full-history coverage must authorize cursor completion"
    );
    assert!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?
            > 0,
        "authorized closure must publish normalized output"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_rejects_input_committed_before_cursor_publication() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let deployment_profile = "mainnet";
    let chain = "ethereum-mainnet";
    let wrapper_address = "0x0000000000000000000000000000000000000133";
    let initial_block = provider_block(
        "0x3333333333333333333333333333333333333333333333333333333333333333",
        Some("0x3232323232323232323232323232323232323232323232323232323232323232"),
        33,
    );
    let late_block = provider_block(
        "0x3232323232323232323232323232323232323232323232323232323232323234",
        Some("0x3131313131313131313131313131313131313131313131313131313131313131"),
        32,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        396,
        chain,
        Uuid::from_u128(0x396),
        wrapper_address,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &initial_block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
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
        )
        VALUES ($1, 1, 0, false, clock_timestamp())
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = EXCLUDED.revision,
            retention_generation = EXCLUDED.retention_generation,
            retained_history_complete = EXCLUDED.retained_history_complete,
            incomplete_since = EXCLUDED.incomplete_since,
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, $2, $3, 1)
        "#,
    )
    .bind(chain)
    .bind(&initial_block.block_hash)
    .bind(initial_block.block_number)
    .execute(database.pool())
    .await?;

    let release_hook =
        install_ownership_release_test_hook(database.pool(), deployment_profile, chain).await;
    let pool = database.pool().clone();
    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        deployment_profile.to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let task_config = config.clone();
    let replay = tokio::spawn(async move {
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            &pool,
            &task_config,
            chain,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        release_hook.wait_until_before_release(),
    )
    .await
    .context("automatic closure did not reach its ownership-release barrier")?;

    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &late_block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET revision = 2
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, $2, $3, 2)
        "#,
    )
    .bind(chain)
    .bind(&late_block.block_hash)
    .bind(late_block.block_number)
    .execute(database.pool())
    .await?;

    release_hook.resume();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), replay)
        .await
        .context("automatic closure did not resume after the ownership barrier")?
        .context("automatic closure task panicked")?
        .expect_err("a newer raw input revision must prevent cursor publication");
    assert!(
        format!("{error:#}").contains("changed before normalized replay cursor publication"),
        "unexpected publication-fence error: {error:#}"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>)>(
            r#"
            SELECT next_block_number, last_completed_block_number
            FROM normalized_replay_cursors
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = 'raw_fact_normalized_events'
            "#,
        )
        .bind(deployment_profile)
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (initial_block.block_number, None),
        "cursor publication must remain pending after an input-version race"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_rejects_late_older_commit_after_rewind_inspection() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let deployment_profile = "mainnet";
    let chain = "ethereum-mainnet";
    let reverse_address = "0x0000000000000000000000000000000000000144";
    let replay_block = provider_block(
        "0x9696969696969696969696969696969696969696969696969696969696969696",
        Some("0x9595959595959595959595959595959595959595959595959595959595959595"),
        150,
    );
    let late_block = provider_block(
        "0x7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d",
        Some("0x7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"),
        125,
    );
    let last_replayed_at = OffsetDateTime::now_utc();

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        397,
        chain,
        "ens_v1_reverse_l1",
        Uuid::from_u128(0x397),
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &replay_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &replay_block,
        reverse_address,
        "0x0000000000000000000000000000000000000150",
        CanonicalityState::Canonical,
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
        )
        VALUES ($1, 1, 0, false, clock_timestamp())
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = EXCLUDED.revision,
            retention_generation = EXCLUDED.retention_generation,
            retained_history_complete = EXCLUDED.retained_history_complete,
            incomplete_since = EXCLUDED.incomplete_since,
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, $2, $3, 1)
        "#,
    )
    .bind(chain)
    .bind(&replay_block.block_hash)
    .bind(replay_block.block_number)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number,
            last_replayed_at,
            raw_log_input_revision,
            raw_log_retention_generation
        )
        VALUES ($1, $2, 'raw_fact_normalized_events', 100, 150, 150, 149, $3, 1, 0)
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(last_replayed_at)
    .execute(database.pool())
    .await?;

    let after_rewind_hook = normalized_replay_catchup::install_after_rewind_test_hook(
        database.pool(),
        deployment_profile,
        chain,
    )
    .await;
    let pool = database.pool().clone();
    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        deployment_profile.to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let task_config = config.clone();
    let replay = tokio::spawn(async move {
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            &pool,
            &task_config,
            chain,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        after_rewind_hook.wait_until_after_rewind(),
    )
    .await
    .context("stateless catch-up did not reach its after-rewind barrier")?;

    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &late_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &late_block,
        reverse_address,
        "0x0000000000000000000000000000000000000125",
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        "UPDATE raw_logs SET observed_at = $1 - INTERVAL '1 second' WHERE chain_id = $2 AND block_hash = $3",
    )
    .bind(last_replayed_at)
    .bind(chain)
    .bind(&late_block.block_hash)
    .execute(database.pool())
    .await?;
    sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 2 WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, $2, $3, 2)
        "#,
    )
    .bind(chain)
    .bind(&late_block.block_hash)
    .bind(late_block.block_number)
    .execute(database.pool())
    .await?;

    after_rewind_hook.resume();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), replay)
        .await
        .context("stateless catch-up did not resume after the rewind barrier")?
        .context("stateless catch-up task panicked")?
        .expect_err("a late older commit must not be acknowledged after rewind inspection");
    assert!(
        format!("{error:#}").contains("changed before normalized replay cursor publication"),
        "unexpected publication-fence error: {error:#}"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT next_block_number, raw_log_input_revision
            FROM normalized_replay_cursors
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = 'raw_fact_normalized_events'
            "#,
        )
        .bind(deployment_profile)
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (replay_block.block_number, 1),
        "the cursor must not acknowledge a revision whose older block was not replayed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1",
        )
        .bind(&late_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0,
        "the late older raw fact should remain pending after publication refusal"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_accepts_commit_strictly_after_latched_closure_target()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let deployment_profile = "mainnet";
    let chain = "ethereum-mainnet";
    let wrapper_address = "0x0000000000000000000000000000000000000145";
    let replay_block = provider_block(
        "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0",
        Some("0x9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f"),
        160,
    );
    let post_target_block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        Some(&replay_block.block_hash),
        161,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        398,
        chain,
        Uuid::from_u128(0x398),
        wrapper_address,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &replay_block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
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
        )
        VALUES ($1, 1, 0, false, clock_timestamp())
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = EXCLUDED.revision,
            retention_generation = EXCLUDED.retention_generation,
            retained_history_complete = EXCLUDED.retained_history_complete,
            incomplete_since = EXCLUDED.incomplete_since,
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, $2, $3, 1)
        "#,
    )
    .bind(chain)
    .bind(&replay_block.block_hash)
    .bind(replay_block.block_number)
    .execute(database.pool())
    .await?;

    let after_rewind_hook = normalized_replay_catchup::install_after_rewind_test_hook(
        database.pool(),
        deployment_profile,
        chain,
    )
    .await;
    let pool = database.pool().clone();
    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        deployment_profile.to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let replay = tokio::spawn(async move {
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(&pool, &config, chain)
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        after_rewind_hook.wait_until_after_rewind(),
    )
    .await
    .context("automatic closure did not reach its after-rewind barrier")?;

    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &post_target_block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 2 WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, $2, $3, 2)
        "#,
    )
    .bind(chain)
    .bind(&post_target_block.block_hash)
    .bind(post_target_block.block_number)
    .execute(database.pool())
    .await?;

    after_rewind_hook.resume();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(10), replay)
            .await
            .context("automatic closure did not resume after the rewind barrier")?
            .context("automatic closure task panicked")??,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, Option<i64>, i64)>(
            r#"
            SELECT
                next_block_number,
                target_block_number,
                last_completed_block_number,
                raw_log_input_revision
            FROM normalized_replay_cursors
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = 'raw_fact_normalized_events'
            "#,
        )
        .bind(deployment_profile)
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (161, 160, Some(160), 2),
        "the closure cursor must complete its latched target and remember the accepted newer revision"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE block_number = $1",
        )
        .bind(post_target_block.block_number)
        .fetch_one(database.pool())
        .await?,
        0,
        "a post-target commit is backlog work, not part of the latched closure pass"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_recovers_multiple_new_ensv2_emitters_with_scoped_phase_one()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-sepolia";
    let root_manifest_id = 51_300;
    let registry_manifest_id = 51_301;
    let resolver_manifest_id = 51_302;
    let root_contract_instance_id = Uuid::from_u128(51_300);
    let registry_contract_instance_id = Uuid::from_u128(51_301);
    let resolver_contract_instance_id = Uuid::from_u128(51_302);
    let root_address = "0x0000000000000000000000000000000000005130";
    let registry_address = "0x0000000000000000000000000000000000005131";
    let historical_child_address = "0x0000000000000000000000000000000000005100";
    let child_address = "0x0000000000000000000000000000000000005132";
    let resolver_address = "0x0000000000000000000000000000000000005133";
    insert_normalized_replay_ens_v2_registry_manifests(
        database.pool(),
        chain,
        root_manifest_id,
        registry_manifest_id,
        root_contract_instance_id,
        registry_contract_instance_id,
        root_address,
        registry_address,
    )
    .await?;
    insert_normalized_replay_ens_v2_resolver_manifest(
        database.pool(),
        chain,
        resolver_manifest_id,
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;
    sqlx::query(
        "INSERT INTO discovery_admission_epochs (chain_id, epoch) VALUES ($1, 0) ON CONFLICT (chain_id) DO NOTHING",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let block_1 = provider_block(&format!("0x{:064x}", 51_301), None, 1);
    let block_2 = provider_block(&format!("0x{:064x}", 51_302), Some(&block_1.block_hash), 2);
    let block_3 = provider_block(
        &format!("0x{:064x}", 51_303),
        Some(&block_2.block_hash),
        3,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block_1,
        CanonicalityState::Finalized,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block_3,
        CanonicalityState::Finalized,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block_2,
        CanonicalityState::Finalized,
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(chain, &block_1, CanonicalityState::Finalized),
            provider_block_to_raw_block(chain, &block_2, CanonicalityState::Finalized),
            provider_block_to_raw_block(chain, &block_3, CanonicalityState::Finalized),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_1.block_hash.clone(),
                block_number: block_1.block_number,
                transaction_hash: transaction_hash_for_block(&block_1),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"SubregistryUpdated(uint256,address,address)"),
                    hex_string(&abi_word_u64(2)),
                    hex_string(&abi_word_address(historical_child_address)),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_3.block_hash.clone(),
                block_number: block_3.block_number,
                transaction_hash: transaction_hash_for_block(&block_3),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    ens_v2_label_registered_topic0(),
                    hex_string(&abi_word_u64(1)),
                    labelhash_hex("alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: decode_hex_string(&encode_ens_v2_label_registered_log_data(
                    "alice",
                    "0x0000000000000000000000000000000000000aaa",
                    block_3.block_timestamp_unix_secs + 31_536_000,
                )),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_1.block_hash.clone(),
                block_number: block_1.block_number,
                transaction_hash: transaction_hash_for_block(&block_1),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"SubregistryUpdated(uint256,address,address)"),
                    hex_string(&abi_word_u64(2)),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000000",
                    )),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_3.block_hash.clone(),
                block_number: block_3.block_number,
                transaction_hash: transaction_hash_for_block(&block_3),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"SubregistryUpdated(uint256,address,address)"),
                    hex_string(&abi_word_u64(1)),
                    hex_string(&abi_word_address(child_address)),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = 1,
            retained_history_complete = false,
            incomplete_since = clock_timestamp(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_root_l1",
        &[root_address],
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_resolver_l1",
        &[resolver_address],
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_registry_l1",
        &[registry_address],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retained_history_complete = true,
            incomplete_since = NULL,
            proven_retention_generation = 1,
            proven_discovery_admission_epoch = 0,
            proven_through_block = 3
        WHERE chain_id = $1
          AND retention_generation = 1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "sepolia".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?
    .with_defer_projection_indexes(false);
    let stateless_pages =
        install_stateless_page_observer(database.pool(), "sepolia", chain).await?;
    let recovery_hook = normalized_replay_catchup::install_after_coverage_recovery_test_hook(
        database.pool(),
        "sepolia",
        chain,
    )
    .await;
    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: block_1.clone(),
            logs: vec![rpc_ens_v2_label_registered_log_payload(
                &block_1,
                historical_child_address,
                2,
                "historical-recovered",
                3,
            )],
        },
        ProviderBlockFixture {
            block: block_3.clone(),
            logs: vec![rpc_ens_v2_label_registered_log_payload(
                &block_3,
                child_address,
                2,
                "recovered",
                2,
            )],
        },
    ])
    .await?;
    let pool = database.pool().clone();
    let task_config = config.clone();
    let task_provider = provider.clone();
    let replay = tokio::spawn(async move {
        normalized_replay_catchup::run_normalized_replay_catchup_iteration_with_provider_for_test(
            &pool,
            &task_config,
            chain,
            &task_provider,
            HeaderAuditMode::Minimal,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        recovery_hook.wait_until_after_coverage_recovery(),
    )
    .await
    .context("normalized replay did not reach its post-coverage-recovery barrier")?;
    let version_before_concurrent =
        bigname_storage::load_raw_log_staging_input_version(database.pool(), chain).await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: chain.to_owned(),
            block_hash: block_2.block_hash.clone(),
            block_number: block_2.block_number,
            transaction_hash: transaction_hash_for_block(&block_2),
            transaction_index: 0,
            log_index: 9,
            emitting_address: registry_address.to_owned(),
            topics: vec![
                ens_v2_label_registered_topic0(),
                hex_string(&abi_word_u64(1)),
                labelhash_hex("concurrent"),
                hex_string(&abi_word_address(
                    "0x0000000000000000000000000000000000000dad",
                )),
            ],
            data: decode_hex_string(&encode_ens_v2_label_registered_log_data(
                "concurrent",
                "0x0000000000000000000000000000000000000aaa",
                block_2.block_timestamp_unix_secs + 31_536_000,
            )),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    let concurrent_revision = sqlx::query_scalar::<_, i64>(
        "UPDATE raw_log_staging_input_revisions \
         SET revision = revision + 1 \
         WHERE chain_id = $1 \
         RETURNING revision",
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id, block_hash, block_number, revision
        )
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(chain)
    .bind(&block_2.block_hash)
    .bind(block_2.block_number)
    .bind(concurrent_revision)
    .execute(database.pool())
    .await?;
    let version_after_concurrent =
        bigname_storage::load_raw_log_staging_input_version(database.pool(), chain).await?;
    assert!(
        version_after_concurrent.revision > version_before_concurrent.revision,
        "concurrent insertion did not advance revision: before={version_before_concurrent:?}, after={version_after_concurrent:?}, row_count={}",
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM raw_logs WHERE chain_id = $1 AND block_hash = $2 AND log_index = 9"
        )
        .bind(chain)
        .bind(&block_2.block_hash)
        .fetch_one(database.pool())
        .await?
    );
    assert!(
        bigname_storage::raw_log_staging_block_range_changed_since(
            database.pool(),
            chain,
            version_before_concurrent.revision,
            2,
            2,
        )
        .await?
    );
    recovery_hook.resume();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(10), replay)
            .await
            .context("normalized replay did not resume after coverage recovery")?
            .context("normalized replay task panicked")??,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64, i64, i64)>(
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
              AND fact.source_family = 'ens_v2_registry_l1'
              AND fact.address = $2
            "#,
        )
        .bind(chain)
        .bind(child_address)
        .fetch_one(database.pool())
        .await?,
        (
            "ens_v2_registry_l1".to_owned(),
            child_address.to_owned(),
            3,
            3,
            1,
        ),
        "restart recovery must persist only exact current-generation address coverage"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>)>(
            r#"
            SELECT next_block_number, last_completed_block_number
            FROM normalized_replay_cursors
            WHERE deployment_profile = 'sepolia'
              AND chain_id = $1
              AND cursor_kind = 'raw_fact_normalized_events'
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (4, Some(3)),
        "the unchanged replay may advance only after provider-backed recovery succeeds"
    );
    assert_eq!(
        stateless_pages.page_ranges(),
        vec![(1, 3), (1, 3)],
        "coverage recovery must widen phase one when another raw-log writer changes the saved span"
    );
    assert_eq!(
        sqlx::query_as::<_, (bool, Option<i64>, Option<i64>, Option<i64>, i64)>(
            r#"
            SELECT
                retained.retained_history_complete,
                retained.proven_retention_generation,
                retained.proven_discovery_admission_epoch,
                retained.proven_through_block,
                admission.epoch
            FROM raw_log_staging_input_revisions retained
            JOIN discovery_admission_epochs admission
              ON admission.chain_id = retained.chain_id
            WHERE retained.chain_id = $1
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (true, Some(1), Some(1), Some(3), 1),
        "coverage recovery must rebuild the generation-one proof at the discovered epoch"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation' \
               AND block_hash = $1 \
               AND log_index = 2"
        )
        .bind(&block_3.block_hash)
        .fetch_one(database.pool())
        .await?,
        1,
        "phase one must derive a preimage row from the recovered raw log"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM raw_logs \
             WHERE chain_id = $1 \
               AND block_hash = $2 \
               AND log_index = 3 \
               AND emitting_address = $3"
        )
        .bind(chain)
        .bind(&block_1.block_hash)
        .bind(historical_child_address)
        .fetch_one(database.pool())
        .await?,
        1,
        "the first exact recovery must persist its raw log"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation' \
               AND block_hash = $1 \
               AND log_index = 3"
        )
        .bind(&block_1.block_hash)
        .fetch_one(database.pool())
        .await?,
        1,
        "phase one must not drop a prior recovered span while validation finds the next gap"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation' \
               AND block_hash = $1 \
               AND log_index = 9"
        )
        .bind(&block_2.block_hash)
        .fetch_one(database.pool())
        .await?,
        1,
        "a concurrent in-span raw-log write must be included before its revision is acknowledged"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_coverage_recovery_rejects_raw_log_change_below_replay_span()
-> Result<()> {
    let outcome = run_normalized_replay_coverage_fence_test(
        NormalizedReplayCoverageFenceMutation::RawLogBelowReplaySpan,
    )
    .await?;

    assert_eq!(
        bigname_adapters::ens_v2_missing_coverage(&outcome.error),
        Some(&bigname_adapters::EnsV2MissingCoverage {
            chain: NORMALIZED_REPLAY_COVERAGE_FENCE_CHAIN.to_owned(),
            retention_generation: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            address: NORMALIZED_REPLAY_COVERAGE_FENCE_HISTORICAL_CHILD.to_owned(),
            required_from_block: 1,
            required_to_block: 1,
        }),
        "the fail-closed fence must preserve the typed recovery requirement"
    );
    let error = format!("{:#}", outcome.error);
    assert!(
        error.contains(
            "raw-log staging input changed below normalized replay range start 1 during coverage recovery; replan from the durable cursor"
        ),
        "unexpected below-span recovery fence error: {error}"
    );
    assert_eq!(
        outcome.input_version_after.retention_generation,
        outcome.input_version_before.retention_generation,
        "the injected raw-log mutation must remain in the original retention generation"
    );
    assert!(
        outcome.input_version_after.revision > outcome.input_version_before.revision,
        "the injected below-span raw log must advance the committed input revision"
    );
    assert!(
        outcome.changed_below_replay_span,
        "the injected raw-log revision must be visible strictly below from_block"
    );
    assert_eq!(
        (
            outcome.cursor_after.range_start_block_number,
            outcome.cursor_after.next_block_number,
            outcome.cursor_after.target_block_number,
            outcome.cursor_after.last_completed_block_number,
        ),
        (
            outcome.cursor_before.range_start_block_number,
            outcome.cursor_before.next_block_number,
            outcome.cursor_before.target_block_number,
            outcome.cursor_before.last_completed_block_number,
        ),
        "the durable cursor must not advance after a below-span recovery fence failure"
    );
    assert_eq!(
        (
            outcome.cursor_after.raw_log_input_revision,
            outcome.cursor_after.raw_log_retention_generation,
        ),
        (
            outcome.cursor_before.raw_log_input_revision,
            outcome.cursor_before.raw_log_retention_generation,
        ),
        "the failed attempt must not acknowledge any part of the newer raw-log input version"
    );

    Ok(())
}

#[tokio::test]
async fn normalized_replay_coverage_recovery_rejects_retention_generation_change() -> Result<()> {
    let outcome = run_normalized_replay_coverage_fence_test(
        NormalizedReplayCoverageFenceMutation::RetentionGeneration,
    )
    .await?;

    assert_eq!(
        bigname_adapters::ens_v2_missing_coverage(&outcome.error),
        Some(&bigname_adapters::EnsV2MissingCoverage {
            chain: NORMALIZED_REPLAY_COVERAGE_FENCE_CHAIN.to_owned(),
            retention_generation: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            address: NORMALIZED_REPLAY_COVERAGE_FENCE_HISTORICAL_CHILD.to_owned(),
            required_from_block: 1,
            required_to_block: 1,
        }),
        "the fail-closed fence must preserve the typed recovery requirement"
    );
    let error = format!("{:#}", outcome.error);
    assert!(
        error.contains(
            "raw-log retention generation changed during normalized replay coverage recovery: expected 1, observed 2; replan the replay from current authority"
        ),
        "unexpected retention-generation recovery fence error: {error}"
    );
    assert_eq!(
        outcome.input_version_after.revision,
        outcome.input_version_before.revision,
        "rotating retention authority must not manufacture a raw-log input revision"
    );
    assert_eq!(
        outcome.input_version_after.retention_generation,
        outcome.input_version_before.retention_generation + 1,
        "the injected retention change must advance the generation exactly once"
    );
    assert_eq!(
        (
            outcome.cursor_after.range_start_block_number,
            outcome.cursor_after.next_block_number,
            outcome.cursor_after.target_block_number,
            outcome.cursor_after.last_completed_block_number,
        ),
        (
            outcome.cursor_before.range_start_block_number,
            outcome.cursor_before.next_block_number,
            outcome.cursor_before.target_block_number,
            outcome.cursor_before.last_completed_block_number,
        ),
        "the durable cursor must not advance after a retention-generation fence failure"
    );
    assert_eq!(
        (
            outcome.cursor_after.raw_log_input_revision,
            outcome.cursor_after.raw_log_retention_generation,
        ),
        (
            outcome.cursor_before.raw_log_input_revision,
            outcome.cursor_before.raw_log_retention_generation,
        ),
        "the failed attempt must not acknowledge any part of the changed raw-log input version"
    );

    Ok(())
}

#[tokio::test]
async fn normalized_replay_recovery_preserves_full_stateless_span_after_preflight_gap()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-sepolia";
    let root_manifest_id = 52_300;
    let registry_manifest_id = 52_301;
    let resolver_manifest_id = 52_302;
    let root_contract_instance_id = Uuid::from_u128(52_300);
    let registry_contract_instance_id = Uuid::from_u128(52_301);
    let resolver_contract_instance_id = Uuid::from_u128(52_302);
    let child_contract_instance_id = Uuid::from_u128(52_303);
    let root_address = "0x0000000000000000000000000000000000005230";
    let registry_address = "0x0000000000000000000000000000000000005231";
    let child_address = "0x0000000000000000000000000000000000005232";
    let resolver_address = "0x0000000000000000000000000000000000005233";
    insert_normalized_replay_ens_v2_registry_manifests(
        database.pool(),
        chain,
        root_manifest_id,
        registry_manifest_id,
        root_contract_instance_id,
        registry_contract_instance_id,
        root_address,
        registry_address,
    )
    .await?;
    insert_normalized_replay_ens_v2_resolver_manifest(
        database.pool(),
        chain,
        resolver_manifest_id,
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;
    sqlx::query(
        "INSERT INTO discovery_admission_epochs (chain_id, epoch) VALUES ($1, 0) ON CONFLICT (chain_id) DO NOTHING",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let block_1 = provider_block(&format!("0x{:064x}", 52_301), None, 1);
    let block_2 = provider_block(&format!("0x{:064x}", 52_302), Some(&block_1.block_hash), 2);
    let block_3 = provider_block(
        &format!("0x{:064x}", 52_303),
        Some(&block_2.block_hash),
        3,
    );
    for block in [&block_1, &block_2, &block_3] {
        insert_chain_lineage_for_block(
            database.pool(),
            chain,
            block,
            CanonicalityState::Finalized,
        )
        .await?;
    }
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(chain, &block_1, CanonicalityState::Finalized),
            provider_block_to_raw_block(chain, &block_2, CanonicalityState::Finalized),
            provider_block_to_raw_block(chain, &block_3, CanonicalityState::Finalized),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_1.block_hash.clone(),
                block_number: block_1.block_number,
                transaction_hash: transaction_hash_for_block(&block_1),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    ens_v2_label_registered_topic0(),
                    hex_string(&abi_word_u64(1)),
                    labelhash_hex("full-span"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: decode_hex_string(&encode_ens_v2_label_registered_log_data(
                    "full-span",
                    "0x0000000000000000000000000000000000000aaa",
                    block_1.block_timestamp_unix_secs + 31_536_000,
                )),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_3.block_hash.clone(),
                block_number: block_3.block_number,
                transaction_hash: transaction_hash_for_block(&block_3),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000052ff".to_owned(),
                topics: vec![keccak256_hex(b"Unrelated(uint256)")],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = 1,
            retained_history_complete = false,
            incomplete_since = clock_timestamp(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_root_l1",
        &[root_address],
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_resolver_l1",
        &[resolver_address],
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_registry_l1",
        &[registry_address],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retained_history_complete = true,
            incomplete_since = NULL,
            proven_retention_generation = 1,
            proven_discovery_admission_epoch = 0,
            proven_through_block = 3
        WHERE chain_id = $1
          AND retention_generation = 1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    insert_contract_instance(database.pool(), child_contract_instance_id, chain, "contract")
        .await?;
    insert_active_contract_instance_address(
        database.pool(),
        child_contract_instance_id,
        chain,
        child_address,
        Some(registry_manifest_id),
    )
    .await?;
    sqlx::query(
        "UPDATE contract_instance_addresses \
         SET active_from_block_number = 3, active_to_block_number = 3, deactivated_at = now() \
         WHERE contract_instance_id = $1 AND chain_id = $2",
    )
    .bind(child_contract_instance_id)
    .bind(chain)
    .execute(database.pool())
    .await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        chain,
        "subregistry",
        registry_contract_instance_id,
        child_contract_instance_id,
        Some(registry_manifest_id),
        Some(3),
        Some(3),
    )
    .await?;
    sqlx::query(
        "UPDATE discovery_edges \
         SET deactivated_at = now() \
         WHERE chain_id = $1 AND to_contract_instance_id = $2",
    )
    .bind(chain)
    .bind(child_contract_instance_id)
    .execute(database.pool())
    .await?;
    sqlx::query("UPDATE discovery_admission_epochs SET epoch = 1 WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "sepolia".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?
    .with_defer_projection_indexes(false);
    let stateless_pages =
        install_stateless_page_observer(database.pool(), "sepolia", chain).await?;
    let error = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await
    .expect_err("stale retained-history proof must fail before phase one without a provider");
    assert_eq!(
        bigname_adapters::ens_v2_missing_coverage(&error),
        Some(&bigname_adapters::EnsV2MissingCoverage {
            chain: chain.to_owned(),
            retention_generation: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            address: child_address.to_owned(),
            required_from_block: 3,
            required_to_block: 3,
        })
    );
    assert_eq!(
        stateless_pages.page_ranges(),
        Vec::<(i64, i64)>::new(),
        "preflight coverage validation must fail before any stateless replay page"
    );

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: block_3.clone(),
        logs: vec![rpc_ens_v2_label_registered_log_payload(
            &block_3,
            child_address,
            2,
            "recovered-preflight",
            2,
        )],
    }])
    .await?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration_with_provider_for_test(
            database.pool(),
            &config,
            chain,
            &provider,
            HeaderAuditMode::Minimal,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        stateless_pages.page_ranges(),
        vec![(1, 3)],
        "a preflight recovery must preserve the original full stateless span"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation' \
               AND block_hash = $1 \
               AND log_index = 0"
        )
        .bind(&block_1.block_hash)
        .fetch_one(database.pool())
        .await?,
        1,
        "the original full-span log must still receive its stateless preimage"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events \
             WHERE event_kind = 'PreimageObserved' \
               AND derivation_kind = 'raw_log_preimage_observation' \
               AND block_hash = $1 \
               AND log_index = 2"
        )
        .bind(&block_3.block_hash)
        .fetch_one(database.pool())
        .await?,
        1,
        "the recovered log must receive its stateless preimage"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_retention_authority_keeps_durable_ensv2_resolver_gap_retryable()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-sepolia";
    let root_address = "0x0000000000000000000000000000000000005330";
    let registry_address = "0x0000000000000000000000000000000000005331";
    let resolver_address = "0x0000000000000000000000000000000000005332";
    insert_normalized_replay_ens_v2_registry_manifests(
        database.pool(),
        chain,
        53_300,
        53_301,
        Uuid::from_u128(53_300),
        Uuid::from_u128(53_301),
        root_address,
        registry_address,
    )
    .await?;
    insert_normalized_replay_ens_v2_resolver_manifest(
        database.pool(),
        chain,
        53_302,
        Uuid::from_u128(53_302),
        resolver_address,
    )
    .await?;
    sqlx::query(
        "INSERT INTO discovery_admission_epochs (chain_id, epoch) VALUES ($1, 0)",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id, revision, retention_generation,
            retained_history_complete, incomplete_since,
            proven_retention_generation, proven_discovery_admission_epoch,
            proven_through_block
        )
        VALUES ($1, 0, 1, true, NULL, 1, 0, 3)
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_root_l1",
        &[root_address],
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_registry_l1",
        &[registry_address],
    )
    .await?;

    let adapters = [
        NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
        NormalizedEventReplayAdapter::EnsV2Resolver,
    ];
    let error = ensure_full_closure_retention_authority_for_adapters(
        database.pool(),
        chain,
        &adapters,
        3,
    )
    .await
    .expect_err("durable resolver coverage gap must remain provider-retryable");
    assert_eq!(
        bigname_adapters::ens_v2_missing_coverage(&error),
        Some(&bigname_adapters::EnsV2MissingCoverage {
            chain: chain.to_owned(),
            retention_generation: 1,
            source_family: "ens_v2_resolver_l1".to_owned(),
            address: resolver_address.to_owned(),
            required_from_block: 1,
            required_to_block: 3,
        }),
        "a restarted catch-up iteration must still recognize the durable resolver gap"
    );

    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_resolver_l1",
        &[resolver_address],
    )
    .await?;
    ensure_full_closure_retention_authority_for_adapters(
        database.pool(),
        chain,
        &adapters,
        3,
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_handoff_classifies_completed_cursor_input_versions() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let target = 200;

    let generation_chain = "generation-drift";
    insert_completed_replay_cursor_for_handoff_test(
        database.pool(),
        generation_chain,
        target,
        5,
        0,
    )
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), generation_chain, 6, 1)
        .await?;
    assert!(
        !normalized_replay_catchup::normalized_replay_cursors_complete(
            database.pool(),
            "mainnet",
            &[generation_chain.to_owned()],
        )
        .await?,
        "a completed cursor from an older retention generation must not hand off"
    );

    let post_target_chain = "strictly-post-target";
    insert_completed_replay_cursor_for_handoff_test(
        database.pool(),
        post_target_chain,
        target,
        5,
        0,
    )
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), post_target_chain, 6, 0)
        .await?;
    upsert_raw_staging_block_revision_for_handoff_test(
        database.pool(),
        post_target_chain,
        "0xpost-target",
        target + 1,
        6,
    )
    .await?;
    assert!(
        normalized_replay_catchup::normalized_replay_cursors_complete(
            database.pool(),
            "mainnet",
            &[post_target_chain.to_owned()],
        )
        .await?,
        "a witnessed revision strictly after the replay target remains backlog-owned"
    );

    let missing_evidence_chain = "missing-evidence";
    insert_completed_replay_cursor_for_handoff_test(
        database.pool(),
        missing_evidence_chain,
        target,
        5,
        0,
    )
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(
        database.pool(),
        missing_evidence_chain,
        6,
        0,
    )
    .await?;
    let missing_evidence_error = normalized_replay_catchup::normalized_replay_cursors_complete(
        database.pool(),
        "mainnet",
        &[missing_evidence_chain.to_owned()],
    )
    .await
    .expect_err("an advanced revision without block evidence is an integrity error");
    assert!(format!("{missing_evidence_error:#}").contains("without per-block revision evidence"));

    let rollback_chain = "revision-rollback";
    insert_completed_replay_cursor_for_handoff_test(database.pool(), rollback_chain, target, 6, 0)
        .await?;
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), rollback_chain, 5, 0)
        .await?;
    let rollback_error = normalized_replay_catchup::normalized_replay_cursors_complete(
        database.pool(),
        "mainnet",
        &[rollback_chain.to_owned()],
    )
    .await
    .expect_err("a raw-log revision rollback is an integrity error");
    assert!(format!("{rollback_error:#}").contains("revision moved backwards"));

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_handoff_waits_for_rewind_after_completed_prefix_replacement()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let target = 50;
    let block_hash = "0x5050505050505050505050505050505050505050505050505050505050505050";

    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, log_index, emitting_address, topics, data,
            canonicality_state
        )
        VALUES (
            $1, $2, $3,
            '0x5151515151515151515151515151515151515151515151515151515151515151',
            0, 0, '0x0000000000000000000000000000000000000050',
            '{}'::TEXT[], decode('01', 'hex'), 'canonical'
        )
        "#,
    )
    .bind(chain)
    .bind(block_hash)
    .bind(target)
    .execute(database.pool())
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), chain, 1, 0).await?;
    upsert_raw_staging_block_revision_for_handoff_test(
        database.pool(),
        chain,
        block_hash,
        target,
        1,
    )
    .await?;
    insert_completed_replay_cursor_for_handoff_test(database.pool(), chain, target, 1, 0).await?;

    let mut replacement = database.pool().begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{chain}"))
        .execute(replacement.as_mut())
        .await?;
    sqlx::query(
        "UPDATE raw_logs SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(chain)
    .bind(block_hash)
    .execute(replacement.as_mut())
    .await?;
    sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 2 WHERE chain_id = $1")
        .bind(chain)
        .execute(replacement.as_mut())
        .await?;
    sqlx::query(
        "UPDATE raw_log_staging_block_revisions SET revision = 2 WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(chain)
    .bind(block_hash)
    .execute(replacement.as_mut())
    .await?;
    replacement.commit().await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT next_block_number FROM normalized_replay_cursors WHERE deployment_profile = 'mainnet' AND chain_id = $1 AND cursor_kind = 'raw_fact_normalized_events'",
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        target + 1,
        "the startup race is observed before catch-up has rewound the persisted completed cursor"
    );
    assert!(
        !normalized_replay_catchup::normalized_replay_cursors_complete(
            database.pool(),
            "mainnet",
            &[chain.to_owned()],
        )
        .await?,
        "the stale completed cursor must not schedule backlog or adapter ownership"
    );
    assert_eq!(
        normalized_replay_catchup::rewind_cursor_for_test(database.pool(), "mainnet", chain)
            .await?,
        (target - 10, target, target)
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_handoff_requires_generation_zero_for_empty_missing_cursor() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "empty-chain";

    assert!(
        normalized_replay_catchup::normalized_replay_cursors_complete(
            database.pool(),
            "mainnet",
            &[chain.to_owned()],
        )
        .await?,
        "a fresh generation-zero chain with no canonical facts is an honest empty closure"
    );
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), chain, 1, 1).await?;
    assert!(
        !normalized_replay_catchup::normalized_replay_cursors_complete(
            database.pool(),
            "mainnet",
            &[chain.to_owned()],
        )
        .await?,
        "an empty retained suffix from a rotated generation is not an honest empty closure"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_preserves_latched_closure_target() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";

    assert_eq!(
        normalized_replay_catchup::ensure_cursor_for_test(
            database.pool(),
            "mainnet",
            chain,
            10,
            20,
            false,
        )
        .await?,
        (10, 10, 20)
    );
    assert_eq!(
        normalized_replay_catchup::ensure_cursor_for_test(
            database.pool(),
            "mainnet",
            chain,
            10,
            25,
            false,
        )
        .await?,
        (10, 10, 20)
    );
    assert_eq!(
        normalized_replay_catchup::ensure_cursor_for_test(
            database.pool(),
            "mainnet",
            chain,
            10,
            25,
            true,
        )
        .await?,
        (10, 10, 25)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_rewinds_newly_observed_logs_after_range_start() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "base-mainnet";
    let range_start = 100_i64;
    let late_block_number = 125_i64;
    let next_block = 150_i64;
    let target = 200_i64;
    let reverse_address = "0x0000000000000000000000000000000000000140";
    let late_block = provider_block(
        "0x4040404040404040404040404040404040404040404040404040404040404040",
        Some("0x3939393939393939393939393939393939393939393939393939393939393939"),
        late_block_number,
    );
    let last_replayed_at = OffsetDateTime::now_utc();

    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_replayed_at
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', $2, $3, $4, $5)
        "#,
    )
    .bind(chain)
    .bind(range_start)
    .bind(next_block)
    .bind(target)
    .bind(last_replayed_at)
    .execute(database.pool())
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &late_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &late_block,
        reverse_address,
        "0x0000000000000000000000000000000000000050",
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        "UPDATE raw_logs SET observed_at = $1 + INTERVAL '1 second' WHERE chain_id = $2 AND block_hash = $3",
    )
    .bind(last_replayed_at)
    .bind(chain)
    .bind(&late_block.block_hash)
    .execute(database.pool())
    .await?;

    assert_eq!(
        normalized_replay_catchup::rewind_cursor_for_test(database.pool(), "mainnet", chain)
            .await?,
        (range_start, late_block_number, target),
        "a late canonical raw log above range_start must rewind next_block without widening range_start"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_cursor_rewinds_for_newer_commit_revision_even_when_observed_at_is_old()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_raw_log_staging_input_revisions_table(database.pool()).await?;
    let chain = "base-mainnet";
    let range_start = 100_i64;
    let late_block_number = 125_i64;
    let next_block = 150_i64;
    let target = 200_i64;
    let last_replayed_at = OffsetDateTime::now_utc();
    let late_block = provider_block(
        "0x4141414141414141414141414141414141414141414141414141414141414141",
        Some("0x4040404040404040404040404040404040404040404040404040404040404040"),
        late_block_number,
    );

    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_replayed_at,
            raw_log_input_revision,
            raw_log_retention_generation
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', $2, $3, $4, $5, 5, 0)
        "#,
    )
    .bind(chain)
    .bind(range_start)
    .bind(next_block)
    .bind(target)
    .bind(last_replayed_at)
    .execute(database.pool())
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &late_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &late_block,
        "0x0000000000000000000000000000000000000142",
        "0x0000000000000000000000000000000000000052",
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        "UPDATE raw_logs SET observed_at = $1 - INTERVAL '1 second' WHERE chain_id = $2 AND block_hash = $3",
    )
    .bind(last_replayed_at)
    .bind(chain)
    .bind(&late_block.block_hash)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since
        )
        VALUES ($1, 6, 0, false, clock_timestamp())
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = EXCLUDED.revision,
            retention_generation = EXCLUDED.retention_generation,
            retained_history_complete = EXCLUDED.retained_history_complete,
            incomplete_since = EXCLUDED.incomplete_since,
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, $2, $3, 6)
        "#,
    )
    .bind(chain)
    .bind(&late_block.block_hash)
    .bind(late_block_number)
    .execute(database.pool())
    .await?;

    assert_eq!(
        normalized_replay_catchup::rewind_cursor_for_test(database.pool(), "mainnet", chain)
            .await?,
        (range_start, late_block_number, target),
        "commit-ordered raw input revision must rewind even when observed_at predates replay"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_replay_catchup_refuses_pending_base_rederive_below_boundary_raw_log_floor()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_base_normalized_rederive_run_table(database.pool()).await?;
    let chain = "base-mainnet";
    let boundary = bigname_storage::BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK;
    let target = boundary + 100;
    let older_block = provider_block(
        "0x2121212121212121212121212121212121212121212121212121212121212121",
        Some("0x2020202020202020202020202020202020202020202020202020202020202020"),
        boundary - 1,
    );

    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', $2, $2, $3)
        "#,
    )
    .bind(chain)
    .bind(boundary)
    .bind(target)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO base_normalized_rederive_runs (
            run_id,
            deployment_profile,
            chain_id,
            replay_target_block,
            status,
            completed_at,
            updated_at
        )
        VALUES ('base-rederive-reset-pending', 'mainnet', $1, $2, 'completed', now(), now())
        "#,
    )
    .bind(chain)
    .bind(target)
    .execute(database.pool())
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &older_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &older_block,
        "0x0000000000000000000000000000000000000141",
        "0x0000000000000000000000000000000000000051",
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let error = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await
    .expect_err("pending Base correction replay must not widen below reviewed boundary");
    assert!(
        format!("{error:?}").contains("would widen below reviewed boundary"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64)>(
            r#"
            SELECT range_start_block_number, next_block_number, target_block_number
            FROM normalized_replay_cursors
            WHERE deployment_profile = 'mainnet'
              AND chain_id = $1
              AND cursor_kind = 'raw_fact_normalized_events'
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (boundary, boundary, target),
        "catch-up must bail before ensure_cursor widens the pending correction cursor"
    );
    assert_eq!(
        normalized_replay_catchup::ensure_cursor_for_test(
            database.pool(),
            "mainnet",
            chain,
            boundary - 1,
            target,
            false,
        )
        .await?,
        (boundary - 1, boundary - 1, target),
        "normal ensure_cursor behavior remains able to widen when invoked directly"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_refuses_pending_base_rederive_without_raw_log_bounds()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_base_normalized_rederive_run_table(database.pool()).await?;
    let chain = "base-mainnet";
    let boundary = bigname_storage::BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK;
    let target = boundary + 100;

    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', $2, $2, $3)
        "#,
    )
    .bind(chain)
    .bind(boundary)
    .bind(target)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO base_normalized_rederive_runs (
            run_id,
            deployment_profile,
            chain_id,
            replay_target_block,
            status,
            completed_at,
            updated_at
        )
        VALUES ('base-rederive-reset-pending-no-logs', 'mainnet', $1, $2, 'completed', now(), now())
        "#,
    )
    .bind(chain)
    .bind(target)
    .execute(database.pool())
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    let error = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await
    .expect_err("pending Base correction replay must not idle without retained raw-log bounds");
    assert!(
        format!("{error:?}").contains("no retained canonical raw-log bounds"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_rebuilds_deferred_indexes_when_configured_chain_has_no_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    sqlx::query("CREATE TABLE invalid_projection_index_fixture (duplicate_value INTEGER NOT NULL)")
        .execute(database.pool())
        .await
        .context("failed to create invalid-index fixture table")?;
    sqlx::query("INSERT INTO invalid_projection_index_fixture (duplicate_value) VALUES (1), (1)")
        .execute(database.pool())
        .await
        .context("failed to seed invalid-index fixture rows")?;
    sqlx::query(
        "CREATE UNIQUE INDEX CONCURRENTLY normalized_events_namespace_idx \
         ON invalid_projection_index_fixture (duplicate_value)",
    )
    .execute(database.pool())
    .await
    .expect_err("duplicate fixture rows must leave an invalid concurrent index remnant");
    let (is_valid, is_ready) = sqlx::query_as::<_, (bool, bool)>(
        r#"
        SELECT index.indisvalid, index.indisready
        FROM pg_index AS index
        WHERE index.indexrelid = to_regclass('normalized_events_namespace_idx')
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to inspect invalid deferred projection index")?;
    assert!(!is_valid || !is_ready);
    sqlx::query("DROP INDEX IF EXISTS normalized_events_record_inventory_resource_replay_idx")
        .execute(database.pool())
        .await
        .context("failed to drop record inventory replay index for test")?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number
        )
        VALUES ('mainnet', 'ethereum-mainnet', 'raw_fact_normalized_events', 1, 2, 1, 1)
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert complete normalized replay cursor for test")?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec!["ethereum-mainnet".to_owned(), "base-mainnet".to_owned()],
        1_000,
        1_000,
        1,
    )?;
    // TestDatabase is capped at two connections. Retain one exactly as the
    // process-lifetime Base rederive writer guard does in production; index
    // restoration must run all fenced catalog work on the DDL guard's own
    // second connection instead of waiting forever for a third.
    let outer_runtime_guard = database
        .pool()
        .acquire()
        .await
        .context("failed to retain simulated runtime writer-guard connection")?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            "base-mainnet",
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Idle
    );

    assert!(
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT index.indisvalid AND index.indisready
            FROM pg_index AS index
            WHERE index.indexrelid = to_regclass('normalized_events_namespace_idx')
              AND index.indrelid = 'normalized_events'::regclass
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        "catch-up must replace invalid remnants with a ready index on normalized_events"
    );
    let record_inventory_predicate = sqlx::query_scalar::<_, String>(
        r#"
        SELECT pg_get_expr(index.indpred, index.indrelid)
        FROM pg_index AS index
        JOIN pg_class AS class ON class.oid = index.indexrelid
        WHERE class.relname = 'normalized_events_record_inventory_resource_replay_idx'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to inspect restored record inventory replay index predicate")?;
    assert!(
        record_inventory_predicate.contains("ens_v2_registry_resource_surface"),
        "restored record inventory replay index must cover ENSv2 registry resources: {record_inventory_predicate}"
    );

    drop(outer_runtime_guard);
    database.cleanup().await?;
    Ok(())
}

const NORMALIZED_REPLAY_COVERAGE_FENCE_CHAIN: &str = "ethereum-sepolia";
const NORMALIZED_REPLAY_COVERAGE_FENCE_HISTORICAL_CHILD: &str =
    "0x0000000000000000000000000000000000005400";

#[derive(Clone, Copy)]
enum NormalizedReplayCoverageFenceMutation {
    RawLogBelowReplaySpan,
    RetentionGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NormalizedReplayCoverageFenceCursor {
    range_start_block_number: i64,
    next_block_number: i64,
    target_block_number: i64,
    last_completed_block_number: Option<i64>,
    raw_log_input_revision: i64,
    raw_log_retention_generation: i64,
}

struct NormalizedReplayCoverageFenceOutcome {
    error: anyhow::Error,
    cursor_before: NormalizedReplayCoverageFenceCursor,
    cursor_after: NormalizedReplayCoverageFenceCursor,
    input_version_before: bigname_storage::RawLogStagingInputVersion,
    input_version_after: bigname_storage::RawLogStagingInputVersion,
    changed_below_replay_span: bool,
}

async fn run_normalized_replay_coverage_fence_test(
    mutation: NormalizedReplayCoverageFenceMutation,
) -> Result<NormalizedReplayCoverageFenceOutcome> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = NORMALIZED_REPLAY_COVERAGE_FENCE_CHAIN;
    let root_manifest_id = 54_300;
    let registry_manifest_id = 54_301;
    let resolver_manifest_id = 54_302;
    let root_contract_instance_id = Uuid::from_u128(54_300);
    let registry_contract_instance_id = Uuid::from_u128(54_301);
    let resolver_contract_instance_id = Uuid::from_u128(54_302);
    let root_address = "0x0000000000000000000000000000000000005430";
    let registry_address = "0x0000000000000000000000000000000000005431";
    let child_address = "0x0000000000000000000000000000000000005432";
    let resolver_address = "0x0000000000000000000000000000000000005433";
    insert_normalized_replay_ens_v2_registry_manifests(
        database.pool(),
        chain,
        root_manifest_id,
        registry_manifest_id,
        root_contract_instance_id,
        registry_contract_instance_id,
        root_address,
        registry_address,
    )
    .await?;
    insert_normalized_replay_ens_v2_resolver_manifest(
        database.pool(),
        chain,
        resolver_manifest_id,
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;
    sqlx::query(
        "INSERT INTO discovery_admission_epochs (chain_id, epoch) VALUES ($1, 0) ON CONFLICT (chain_id) DO NOTHING",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let block_1 = provider_block(&format!("0x{:064x}", 54_301), None, 1);
    let block_2 = provider_block(&format!("0x{:064x}", 54_302), Some(&block_1.block_hash), 2);
    let block_3 = provider_block(
        &format!("0x{:064x}", 54_303),
        Some(&block_2.block_hash),
        3,
    );
    for block in [&block_1, &block_2, &block_3] {
        insert_chain_lineage_for_block(
            database.pool(),
            chain,
            block,
            CanonicalityState::Finalized,
        )
        .await?;
    }
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(chain, &block_1, CanonicalityState::Finalized),
            provider_block_to_raw_block(chain, &block_2, CanonicalityState::Finalized),
            provider_block_to_raw_block(chain, &block_3, CanonicalityState::Finalized),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_1.block_hash.clone(),
                block_number: block_1.block_number,
                transaction_hash: transaction_hash_for_block(&block_1),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"SubregistryUpdated(uint256,address,address)"),
                    hex_string(&abi_word_u64(2)),
                    hex_string(&abi_word_address(
                        NORMALIZED_REPLAY_COVERAGE_FENCE_HISTORICAL_CHILD,
                    )),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_3.block_hash.clone(),
                block_number: block_3.block_number,
                transaction_hash: transaction_hash_for_block(&block_3),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    ens_v2_label_registered_topic0(),
                    hex_string(&abi_word_u64(1)),
                    labelhash_hex("alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: decode_hex_string(&encode_ens_v2_label_registered_log_data(
                    "alice",
                    "0x0000000000000000000000000000000000000aaa",
                    block_3.block_timestamp_unix_secs + 31_536_000,
                )),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_1.block_hash.clone(),
                block_number: block_1.block_number,
                transaction_hash: transaction_hash_for_block(&block_1),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"SubregistryUpdated(uint256,address,address)"),
                    hex_string(&abi_word_u64(2)),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000000",
                    )),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_3.block_hash.clone(),
                block_number: block_3.block_number,
                transaction_hash: transaction_hash_for_block(&block_3),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"SubregistryUpdated(uint256,address,address)"),
                    hex_string(&abi_word_u64(1)),
                    hex_string(&abi_word_address(child_address)),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000dad",
                    )),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = 1,
            retained_history_complete = false,
            incomplete_since = clock_timestamp(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_root_l1",
        &[root_address],
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_resolver_l1",
        &[resolver_address],
    )
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        1,
        3,
        "ens_v2_registry_l1",
        &[registry_address],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retained_history_complete = true,
            incomplete_since = NULL,
            proven_retention_generation = 1,
            proven_discovery_admission_epoch = 0,
            proven_through_block = 3
        WHERE chain_id = $1
          AND retention_generation = 1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "sepolia".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?
    .with_defer_projection_indexes(false);
    let recovery_hook = normalized_replay_catchup::install_after_coverage_recovery_test_hook(
        database.pool(),
        "sepolia",
        chain,
    )
    .await;
    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: block_1.clone(),
            logs: vec![rpc_ens_v2_label_registered_log_payload(
                &block_1,
                NORMALIZED_REPLAY_COVERAGE_FENCE_HISTORICAL_CHILD,
                2,
                "historical-recovered",
                3,
            )],
        },
        ProviderBlockFixture {
            block: block_3.clone(),
            logs: vec![rpc_ens_v2_label_registered_log_payload(
                &block_3,
                child_address,
                2,
                "recovered",
                2,
            )],
        },
    ])
    .await?;
    let pool = database.pool().clone();
    let task_config = config.clone();
    let replay = tokio::spawn(async move {
        normalized_replay_catchup::run_normalized_replay_catchup_iteration_with_provider_for_test(
            &pool,
            &task_config,
            chain,
            &provider,
            HeaderAuditMode::Minimal,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        recovery_hook.wait_until_after_coverage_recovery(),
    )
    .await
    .context("normalized replay did not reach its post-coverage-recovery barrier")?;

    let cursor_before =
        load_normalized_replay_coverage_fence_cursor(database.pool(), chain).await?;
    let input_version_before =
        bigname_storage::load_raw_log_staging_input_version(database.pool(), chain).await?;
    let changed_below_replay_span = match mutation {
        NormalizedReplayCoverageFenceMutation::RawLogBelowReplaySpan => {
            let block_0 = provider_block(&format!("0x{:064x}", 54_300), None, 0);
            insert_chain_lineage_for_block(
                database.pool(),
                chain,
                &block_0,
                CanonicalityState::Finalized,
            )
            .await?;
            upsert_raw_blocks(
                database.pool(),
                &[provider_block_to_raw_block(
                    chain,
                    &block_0,
                    CanonicalityState::Finalized,
                )],
            )
            .await?;
            upsert_raw_logs(
                database.pool(),
                &[RawLog {
                    chain_id: chain.to_owned(),
                    block_hash: block_0.block_hash.clone(),
                    block_number: block_0.block_number,
                    transaction_hash: transaction_hash_for_block(&block_0),
                    transaction_index: 0,
                    log_index: 9,
                    emitting_address: registry_address.to_owned(),
                    topics: vec![keccak256_hex(b"Unrelated(uint256)")],
                    data: Vec::new(),
                    canonicality_state: CanonicalityState::Finalized,
                }],
            )
            .await?;
            let revision = sqlx::query_scalar::<_, i64>(
                "UPDATE raw_log_staging_input_revisions \
                 SET revision = revision + 1 \
                 WHERE chain_id = $1 \
                 RETURNING revision",
            )
            .bind(chain)
            .fetch_one(database.pool())
            .await?;
            sqlx::query(
                r#"
                INSERT INTO raw_log_staging_block_revisions (
                    chain_id, block_hash, block_number, revision
                )
                VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind(chain)
            .bind(&block_0.block_hash)
            .bind(block_0.block_number)
            .bind(revision)
            .execute(database.pool())
            .await?;
            bigname_storage::raw_log_staging_block_range_changed_since(
                database.pool(),
                chain,
                input_version_before.revision,
                0,
                0,
            )
            .await?
        }
        NormalizedReplayCoverageFenceMutation::RetentionGeneration => {
            sqlx::query(
                r#"
                UPDATE raw_log_staging_input_revisions
                SET retention_generation = retention_generation + 1,
                    retained_history_complete = false,
                    incomplete_since = clock_timestamp(),
                    proven_retention_generation = NULL,
                    proven_discovery_admission_epoch = NULL,
                    proven_through_block = NULL
                WHERE chain_id = $1
                "#,
            )
            .bind(chain)
            .execute(database.pool())
            .await?;
            false
        }
    };
    let input_version_after =
        bigname_storage::load_raw_log_staging_input_version(database.pool(), chain).await?;

    recovery_hook.resume();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), replay)
        .await
        .context("normalized replay did not resume after coverage recovery")?
        .context("normalized replay task panicked")?
        .expect_err("the injected recovery-fence change must fail the replay attempt");
    let cursor_after =
        load_normalized_replay_coverage_fence_cursor(database.pool(), chain).await?;

    drop(recovery_hook);
    server.abort();
    database.cleanup().await?;
    Ok(NormalizedReplayCoverageFenceOutcome {
        error,
        cursor_before,
        cursor_after,
        input_version_before,
        input_version_after,
        changed_below_replay_span,
    })
}

async fn load_normalized_replay_coverage_fence_cursor(
    pool: &PgPool,
    chain: &str,
) -> Result<NormalizedReplayCoverageFenceCursor> {
    let row = sqlx::query_as::<_, (i64, i64, i64, Option<i64>, i64, i64)>(
        r#"
        SELECT
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number,
            raw_log_input_revision,
            raw_log_retention_generation
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'sepolia'
          AND chain_id = $1
          AND cursor_kind = 'raw_fact_normalized_events'
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    Ok(NormalizedReplayCoverageFenceCursor {
        range_start_block_number: row.0,
        next_block_number: row.1,
        target_block_number: row.2,
        last_completed_block_number: row.3,
        raw_log_input_revision: row.4,
        raw_log_retention_generation: row.5,
    })
}

async fn insert_completed_replay_cursor_for_handoff_test(
    pool: &PgPool,
    chain: &str,
    target: i64,
    revision: i64,
    retention_generation: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile, chain_id, cursor_kind,
            range_start_block_number, next_block_number, target_block_number,
            last_completed_block_number, last_replayed_at,
            raw_log_input_revision, raw_log_retention_generation
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', $2, $3, $4, $4, now(), $5, $6)
        "#,
    )
    .bind(chain)
    .bind(target - 10)
    .bind(target + 1)
    .bind(target)
    .bind(revision)
    .bind(retention_generation)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_normalized_replay_ens_v2_registry_manifests(
    pool: &PgPool,
    chain: &str,
    root_manifest_id: i64,
    registry_manifest_id: i64,
    root_contract_instance_id: Uuid,
    registry_contract_instance_id: Uuid,
    root_address: &str,
    registry_address: &str,
) -> Result<()> {
    let root_manifest_payload = json!({
        "roots": [{
            "name": "RootRegistry",
            "address": root_address,
            "start_block": 1
        }],
        "contracts": [],
        "abi": {"events": test_manifest_abi_events()},
    });
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
        VALUES ($1, 'ens', 'ens_v2_root_l1', $2, 'active', $3::jsonb)
        "#,
    )
    .bind(root_manifest_id)
    .bind(chain)
    .bind(serde_json::to_string(&root_manifest_payload)?)
    .execute(pool)
    .await?;
    insert_contract_instance(pool, root_contract_instance_id, chain, "root").await?;
    insert_active_contract_instance_address(
        pool,
        root_contract_instance_id,
        chain,
        root_address,
        Some(root_manifest_id),
    )
    .await?;
    insert_manifest_root_contract_instance(
        pool,
        root_manifest_id,
        root_contract_instance_id,
        root_address,
    )
    .await?;

    let registry_manifest_payload = json!({
        "roots": [{
            "name": "RootRegistry",
            "address": registry_address,
            "start_block": 1
        }],
        "contracts": [{
            "role": "registry",
            "address": registry_address,
            "start_block": 1
        }],
        "discovery_rules": [{
            "edge_kind": "subregistry",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": {"events": test_manifest_abi_events()},
    });
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
        VALUES ($1, 'ens', 'ens_v2_registry_l1', $2, 'active', $3::jsonb)
        "#,
    )
    .bind(registry_manifest_id)
    .bind(chain)
    .bind(serde_json::to_string(&registry_manifest_payload)?)
    .execute(pool)
    .await?;
    insert_contract_instance(pool, registry_contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        registry_contract_instance_id,
        chain,
        registry_address,
        Some(registry_manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        registry_manifest_id,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        pool,
        registry_manifest_id,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_discovery_rule(
        pool,
        registry_manifest_id,
        "subregistry",
        "registry",
        "reachable_from_root",
    )
    .await
}

async fn insert_normalized_replay_ens_v2_resolver_manifest(
    pool: &PgPool,
    chain: &str,
    manifest_id: i64,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    let manifest_payload = json!({
        "roots": [],
        "contracts": [{
            "role": "resolver",
            "address": address,
            "start_block": 1
        }],
        "abi": {"events": test_manifest_abi_events()},
    });
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
        VALUES ($1, 'ens', 'ens_v2_resolver_l1', $2, 'active', $3::jsonb)
        "#,
    )
    .bind(manifest_id)
    .bind(chain)
    .bind(serde_json::to_string(&manifest_payload)?)
    .execute(pool)
    .await?;
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
        "resolver",
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}

async fn insert_ready_replay_and_backlog_cursors_for_handoff_test(
    pool: &PgPool,
    chain: &str,
    target: i64,
    revision: i64,
    retention_generation: i64,
) -> Result<()> {
    insert_completed_replay_cursor_for_handoff_test(
        pool,
        chain,
        target,
        revision,
        retention_generation,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile, chain_id, cursor_kind,
            range_start_block_number, next_block_number, target_block_number,
            last_completed_block_number, last_replayed_at,
            raw_log_input_revision, raw_log_retention_generation
        )
        VALUES (
            'mainnet', $1, 'post_replay_live_adapter_backlog',
            $2, $3, $2, $2, now(), $4, $5
        )
        "#,
    )
    .bind(chain)
    .bind(target + 1)
    .bind(target + 2)
    .bind(revision)
    .bind(retention_generation)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_raw_staging_input_version_for_handoff_test(
    pool: &PgPool,
    chain: &str,
    revision: i64,
    retention_generation: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id, revision, retention_generation,
            retained_history_complete, incomplete_since
        )
        VALUES ($1, $2, $3, false, clock_timestamp())
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = EXCLUDED.revision,
            retention_generation = EXCLUDED.retention_generation,
            retained_history_complete = false,
            incomplete_since = clock_timestamp(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        "#,
    )
    .bind(chain)
    .bind(revision)
    .bind(retention_generation)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_raw_staging_block_revision_for_handoff_test(
    pool: &PgPool,
    chain: &str,
    block_hash: &str,
    block_number: i64,
    revision: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id, block_hash, block_number, revision
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (chain_id, block_hash) DO UPDATE
        SET block_number = EXCLUDED.block_number,
            revision = EXCLUDED.revision
        "#,
    )
    .bind(chain)
    .bind(block_hash)
    .bind(block_number)
    .bind(revision)
    .execute(pool)
    .await?;
    Ok(())
}

async fn commit_raw_revision_after_handoff_fence_for_test(
    pool: PgPool,
    chain: String,
    block_number: i64,
) -> Result<()> {
    let block_hash = format!("0x{block_number:064x}");
    let transaction_hash = format!("0x{:064x}", block_number + 1_000);
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{chain}"))
        .execute(transaction.as_mut())
        .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, $4, to_timestamp(1700000000 + $4), 'canonical')
        "#,
    )
    .bind(&chain)
    .bind(&block_hash)
    .bind(format!("0x{:064x}", block_number.saturating_sub(1)))
    .bind(block_number)
    .execute(transaction.as_mut())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, log_index, emitting_address, topics, data,
            canonicality_state
        )
        VALUES (
            $1, $2, $3, $4, 0, 0,
            '0x0000000000000000000000000000000000000001',
            '{}'::TEXT[], decode('01', 'hex'), 'canonical'
        )
        "#,
    )
    .bind(&chain)
    .bind(&block_hash)
    .bind(block_number)
    .bind(transaction_hash)
    .execute(transaction.as_mut())
    .await?;
    sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 2 WHERE chain_id = $1")
        .bind(&chain)
        .execute(transaction.as_mut())
        .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id, block_hash, block_number, revision
        )
        VALUES ($1, $2, $3, 2)
        ON CONFLICT (chain_id, block_hash) DO UPDATE
        SET block_number = EXCLUDED.block_number, revision = EXCLUDED.revision
        "#,
    )
    .bind(&chain)
    .bind(block_hash)
    .bind(block_number)
    .execute(transaction.as_mut())
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn create_normalized_replay_cursor_table(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE normalized_replay_cursors (
            deployment_profile TEXT NOT NULL,
            chain_id TEXT NOT NULL,
            cursor_kind TEXT NOT NULL,
            range_start_block_number BIGINT NOT NULL CHECK (range_start_block_number >= 0),
            next_block_number BIGINT NOT NULL CHECK (next_block_number >= range_start_block_number),
            target_block_number BIGINT NOT NULL CHECK (target_block_number >= range_start_block_number),
            last_completed_block_number BIGINT CHECK (last_completed_block_number IS NULL OR last_completed_block_number >= range_start_block_number),
            last_selected_block_count BIGINT NOT NULL DEFAULT 0 CHECK (last_selected_block_count >= 0),
            last_canonical_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_canonical_raw_log_count >= 0),
            last_scanned_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_scanned_raw_log_count >= 0),
            last_matched_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_matched_raw_log_count >= 0),
            last_normalized_event_synced_count BIGINT NOT NULL DEFAULT 0 CHECK (last_normalized_event_synced_count >= 0),
            last_normalized_event_inserted_count BIGINT NOT NULL DEFAULT 0 CHECK (last_normalized_event_inserted_count >= 0),
            last_replayed_at TIMESTAMPTZ,
            raw_log_input_revision BIGINT NOT NULL DEFAULT 0 CHECK (raw_log_input_revision >= 0),
            raw_log_retention_generation BIGINT NOT NULL DEFAULT 0 CHECK (raw_log_retention_generation >= 0),
            last_failure_reason TEXT,
            last_failure_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (deployment_profile, chain_id, cursor_kind),
            CHECK (next_block_number <= target_block_number + 1)
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create normalized_replay_cursors table for indexer tests")?;
    create_normalized_replay_adapter_checkpoint_tables(pool).await?;

    Ok(())
}

async fn create_base_normalized_rederive_run_table(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE base_normalized_rederive_runs (
            run_id TEXT PRIMARY KEY,
            deployment_profile TEXT NOT NULL,
            chain_id TEXT NOT NULL,
            replay_target_block BIGINT NOT NULL,
            status TEXT NOT NULL,
            completed_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create Base rederive run table for indexer tests")?;
    Ok(())
}
