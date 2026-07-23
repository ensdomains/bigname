use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

use super::*;

#[derive(Default)]
struct CountingProgress {
    count: usize,
}

impl AdmissionStateProgress for CountingProgress {
    fn record(&mut self) -> AdmissionStateProgressFuture<'_> {
        self.count += 1;
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn active_parent_loader_pages_only_admitted_transitive_edges() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("active_parent_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate active-parent paging test database",
    )
    .await?;
    let pool = database.pool();
    let from_id = Uuid::from_u128(0xa01);
    let to_id = Uuid::from_u128(0xa02);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, 'parent-chain', 'test'), ($2, 'parent-chain', 'test')
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address
        )
        VALUES (
            $1,
            'parent-chain',
            '0x0000000000000000000000000000000000000a02'
        )
        "#,
    )
    .bind(to_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission
        )
        SELECT
            'other-chain',
            'test',
            $1,
            $2,
            'other-source',
            'test'
        FROM generate_series(1, 2001)
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
            'parent_family',
            'parent-chain',
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
            admission,
            provenance
        )
        VALUES (
            'parent-chain',
            $1,
            $2,
            $3,
            'target-source',
            $4,
            $5,
            '{"propagated_role":"registry"}'::JSONB
        )
        "#,
    )
    .bind(TRANSITIVE_DISCOVERY_EDGE_KIND)
    .bind(from_id)
    .bind(to_id)
    .bind(manifest_id)
    .bind(REACHABLE_FROM_ROOT_ADMISSION)
    .execute(pool)
    .await?;

    let mut connection = pool.acquire().await?;
    let mut progress = CountingProgress::default();
    let rows =
        load_active_discovered_parent_rows_with_progress(&mut connection, None, &mut progress)
            .await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(
        progress.count, 1,
        "only the one admitted transitive-parent page must beat"
    );
    drop(connection);
    database.cleanup().await
}
