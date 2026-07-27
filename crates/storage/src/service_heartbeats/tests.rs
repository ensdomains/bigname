use anyhow::Result;
use sqlx::types::time::OffsetDateTime;

use super::*;

#[tokio::test]
async fn service_heartbeats_peer_progress_does_not_refresh_a_stale_chain() -> Result<()> {
    let database = bigname_test_support::TestDatabase::create_migrated(
        bigname_test_support::TestDatabaseConfig::new(
            "bigname_storage_service_heartbeats_peer_scope",
        ),
        &crate::MIGRATOR,
        "failed to migrate service-heartbeat peer-scope test database",
    )
    .await?;
    let instance_id = "peer-scope-test";
    let wedged_chain = "ethereum-mainnet";
    let peer_chain = "base-mainnet";
    register_service_loop(database.pool(), INDEXER_SERVICE_NAME, instance_id).await?;
    record_service_loop_heartbeat(
        database.pool(),
        INDEXER_SERVICE_NAME,
        instance_id,
        &[wedged_chain.to_owned(), peer_chain.to_owned()],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '40 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '31 minutes'
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'chain'
          AND scope_id = $2
        "#,
    )
    .bind(instance_id)
    .bind(wedged_chain)
    .execute(database.pool())
    .await?;
    let wedged_before = chain_heartbeat_at(database.pool(), instance_id, wedged_chain).await?;

    record_service_loop_heartbeat(
        database.pool(),
        INDEXER_SERVICE_NAME,
        instance_id,
        &[peer_chain.to_owned()],
    )
    .await?;

    assert_eq!(
        chain_heartbeat_at(database.pool(), instance_id, wedged_chain).await?,
        wedged_before,
        "peer progress must not refresh the wedged chain row"
    );
    let peer_age = chain_heartbeat_age(database.pool(), instance_id, peer_chain).await?;
    assert!(
        peer_age < 5,
        "peer chain heartbeat is {peer_age} seconds old"
    );

    let error = ensure_service_loop_heartbeat_recent(
        database.pool(),
        INDEXER_SERVICE_NAME,
        instance_id,
        20,
    )
    .await
    .expect_err("the stale live-chain row must fail indexer health");
    let rendered = error.to_string();
    assert!(
        rendered.contains(wedged_chain),
        "stale-chain health error must identify the wedged chain: {error:#}"
    );
    assert!(
        rendered.contains(&DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS.to_string()),
        "stale-chain health error must report the chain threshold: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn service_heartbeats_chain_floor_allows_a_long_live_iteration() -> Result<()> {
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
        SET started_at = clock_timestamp() - INTERVAL '9 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '8 minutes'
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

async fn chain_heartbeat_at(
    pool: &PgPool,
    instance_id: &str,
    chain: &str,
) -> Result<OffsetDateTime> {
    sqlx::query_scalar(
        r#"
        SELECT heartbeat_at
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'chain'
          AND scope_id = $2
        "#,
    )
    .bind(instance_id)
    .bind(chain)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}

async fn chain_heartbeat_age(pool: &PgPool, instance_id: &str, chain: &str) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT GREATEST(
            FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - heartbeat_at)))::BIGINT,
            0
        )
        FROM service_loop_heartbeats
        WHERE service_name = 'indexer'
          AND instance_id = $1
          AND scope_kind = 'chain'
          AND scope_id = $2
        "#,
    )
    .bind(instance_id)
    .bind(chain)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}
