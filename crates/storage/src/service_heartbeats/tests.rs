use anyhow::Result;

use super::*;

#[tokio::test]
async fn service_heartbeats_default_chain_threshold_allows_the_observed_coverage_scan() -> Result<()>
{
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new(
            "bigname_storage_service_heartbeats_long_iteration",
        ),
        &crate::MIGRATOR,
        "failed to migrate service-heartbeat long-iteration test database",
    )
    .await?;
    let instance_id = "long-iteration-test";
    let chain = "ethereum-mainnet";
    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, instance_id).await?;
    record_service_loop_heartbeat(
        database.pool(),
        INDEXER_SERVICE_NAME,
        instance_id,
        &[chain.to_owned()],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '40 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '37 minutes'
        WHERE service_name = 'indexer'
          AND instance_id = $1
        "#,
    )
    .bind(instance_id)
    .execute(database.pool())
    .await?;

    ensure_service_loop_heartbeat_recent(database.pool(), INDEXER_SERVICE_NAME, instance_id, 20)
        .await?;

    database.cleanup().await
}

#[tokio::test]
async fn service_heartbeat_registration_preserves_peer_lanes_and_missing_expected_lanes_fail()
-> Result<()> {
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new("bigname_storage_service_heartbeats_handoff"),
        &crate::MIGRATOR,
        "failed to migrate service-heartbeat handoff test database",
    )
    .await?;
    let old_instance = "rolling-old-indexer";
    let new_instance = "rolling-new-indexer";
    let chains = vec!["ethereum-mainnet".to_owned(), "base-mainnet".to_owned()];
    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, old_instance).await?;
    record_service_loop_heartbeat(database.pool(), INDEXER_SERVICE_NAME, old_instance, &chains)
        .await?;

    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, new_instance).await?;

    let old_chain_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'chain'
        "#,
    )
    .bind(old_instance)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        old_chain_count,
        i64::try_from(chains.len())?,
        "a rolling replacement must preserve the live peer's chain evidence"
    );

    let new_heartbeat =
        load_service_loop_heartbeat(database.pool(), INDEXER_SERVICE_NAME, new_instance)
            .await?
            .expect("replacement process heartbeat must be registered");
    assert!(
        new_heartbeat.missing_expected_chain_id.is_some(),
        "a replacement must inherit the peer's expected chains before its first full beat"
    );
    let missing_error = ensure_service_loop_heartbeat_recent(
        database.pool(),
        INDEXER_SERVICE_NAME,
        new_instance,
        20,
    )
    .await
    .expect_err("missing expected-chain evidence must fail closed");
    assert!(
        missing_error.to_string().contains("expected chain"),
        "missing-chain error must identify expected evidence: {missing_error:#}"
    );

    let preferred = load_preferred_service_loop_heartbeats(
        database.pool(),
        &[INDEXER_SERVICE_NAME],
        20,
        DEFAULT_WORKER_REBUILD_PHASE_MAX_AGE_SECS,
    )
    .await?;
    assert_eq!(
        preferred[0].instance_id, old_instance,
        "a fresh replacement with missing lanes must not mask a healthy live peer"
    );

    record_service_loop_heartbeat(database.pool(), INDEXER_SERVICE_NAME, new_instance, &chains)
        .await?;
    record_service_loop_heartbeat(
        database.pool(),
        INDEXER_SERVICE_NAME,
        new_instance,
        &["base-mainnet".to_owned()],
    )
    .await?;
    let replacement_chain_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT scope_id
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'chain'
        ORDER BY scope_id
        "#,
    )
    .bind(new_instance)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        replacement_chain_ids,
        vec!["base-mainnet".to_owned()],
        "a full parent beat must prune chains removed from this instance's expected set"
    );

    sqlx::query(
        r#"
        DELETE FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'process'
          AND scope_id = 'process'
        "#,
    )
    .bind(old_instance)
    .execute(database.pool())
    .await?;
    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, "cleanup-indexer").await?;
    let departed_chain_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'chain'
        "#,
    )
    .bind(old_instance)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        departed_chain_count, 0,
        "registration must prune scopes whose departed instance no longer owns a process row"
    );

    database.cleanup().await
}

#[tokio::test]
async fn service_heartbeat_registration_inherits_authoritative_empty_chain_set() -> Result<()> {
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new(
            "bigname_storage_service_heartbeats_empty_handoff",
        ),
        &crate::MIGRATOR,
        "failed to migrate empty service-heartbeat handoff test database",
    )
    .await?;
    let old_instance = "configured-chain-indexer";
    let current_instance = "decommissioned-chain-indexer";
    let replacement_instance = "empty-chain-replacement-indexer";
    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, old_instance).await?;
    record_service_loop_heartbeat(
        database.pool(),
        INDEXER_SERVICE_NAME,
        old_instance,
        &["ethereum-mainnet".to_owned()],
    )
    .await?;
    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, current_instance).await?;
    record_service_loop_heartbeat(database.pool(), INDEXER_SERVICE_NAME, current_instance, &[])
        .await?;

    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, replacement_instance).await?;

    let inherited_expected_chain_ids = sqlx::query_scalar::<_, Vec<String>>(
        r#"
        SELECT expected_chain_ids
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'process'
          AND scope_id = 'process'
        "#,
    )
    .bind(replacement_instance)
    .fetch_one(database.pool())
    .await?;
    assert!(
        inherited_expected_chain_ids.is_empty(),
        "a replacement must not resurrect chains removed by the current authoritative writer"
    );

    database.cleanup().await
}

#[tokio::test]
async fn service_heartbeat_chain_threshold_is_independent_of_the_process_threshold() -> Result<()> {
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new(
            "bigname_storage_service_heartbeats_independent_threshold",
        ),
        &crate::MIGRATOR,
        "failed to migrate independent heartbeat-threshold test database",
    )
    .await?;
    let instance_id = "independent-threshold-indexer";
    let chain = "ethereum-mainnet";
    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, instance_id).await?;
    record_service_loop_heartbeat(
        database.pool(),
        INDEXER_SERVICE_NAME,
        instance_id,
        &[chain.to_owned()],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '3 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '2 minutes'
        WHERE service_name = 'indexer'
          AND instance_id = $1
        "#,
    )
    .bind(instance_id)
    .execute(database.pool())
    .await?;

    let error = ensure_service_loop_heartbeat_recent_with_phase_and_chain(
        database.pool(),
        INDEXER_SERVICE_NAME,
        instance_id,
        3_600,
        3_600,
        60,
    )
    .await
    .expect_err("a large process threshold must not widen the chain threshold");
    assert!(
        error.to_string().contains("maximum 60"),
        "stale-chain error must report the independent chain threshold: {error:#}"
    );

    ensure_service_loop_heartbeat_recent_with_phase_and_chain(
        database.pool(),
        INDEXER_SERVICE_NAME,
        instance_id,
        1,
        1,
        180,
    )
    .await?;

    database.cleanup().await
}
