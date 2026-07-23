use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

use super::*;

#[derive(Default)]
struct CountingProgress {
    count: usize,
}

impl ManifestRuntimeProgress for CountingProgress {
    fn record<'a>(&'a mut self, _pool: &'a PgPool) -> crate::ManifestRuntimeProgressFuture<'a> {
        self.count += 1;
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn stale_source_cleanup_pages_only_edges_without_an_active_manifest() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("stale_source_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate stale-source paging test database",
    )
    .await?;
    let pool = database.pool();
    let from_id = Uuid::from_u128(0xb01);
    let to_id = Uuid::from_u128(0xb02);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, 'managed-chain', 'test'), ($2, 'managed-chain', 'test')
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .execute(pool)
    .await?;
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions (
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
            'ens',
            'managed_family',
            'managed-chain',
            'test',
            'active',
            'test',
            'test.toml',
            '{}'::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission
        )
        SELECT
            'managed-chain',
            'test',
            $1,
            $2,
            'active-source',
            $3,
            'test'
        FROM generate_series(1, 10001)
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    let stale_edge_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission
        )
        VALUES ('managed-chain', 'test', $1, $2, 'stale-source', 'test')
        RETURNING discovery_edge_id
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .fetch_one(pool)
    .await?;

    let mut connection = pool.acquire().await?;
    let mut progress = CountingProgress::default();
    let mut progress_ref = Some(&mut progress as &mut dyn ManifestRuntimeProgress);
    let deactivated = deactivate_discovery_edges_without_active_source_manifest(
        &mut connection,
        pool,
        &mut progress_ref,
    )
    .await?;

    assert_eq!(deactivated, 1);
    assert_eq!(
        progress.count, 1,
        "only the one stale-source page must beat"
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT deactivated_at IS NOT NULL FROM discovery_edges WHERE discovery_edge_id = $1",
        )
        .bind(stale_edge_id)
        .fetch_one(pool)
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE source_manifest_id = $1 AND deactivated_at IS NULL",
        )
        .bind(manifest_id)
        .fetch_one(pool)
        .await?,
        10001
    );
    drop(connection);
    database.cleanup().await
}
