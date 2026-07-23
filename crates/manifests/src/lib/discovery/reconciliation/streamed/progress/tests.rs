use std::sync::atomic::{AtomicUsize, Ordering};

use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

use super::*;
use crate::DiscoveryObservation;

#[derive(Default)]
struct CountingPageSource {
    progress_count: AtomicUsize,
}

impl DiscoveryObservationPageSource for CountingPageSource {
    async fn load_page(
        &self,
        _after_key: Option<&str>,
        _limit: i64,
    ) -> Result<Vec<(String, DiscoveryObservation)>> {
        Ok(Vec::new())
    }

    async fn record_progress(&self) -> Result<()> {
        self.progress_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn active_edge_summary_pages_only_the_requested_source() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("streamed_active_summary_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate streamed active-summary paging test database",
    )
    .await?;
    let pool = database.pool();
    let from_id = Uuid::from_u128(0x801);
    let to_id = Uuid::from_u128(0x802);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, 'summary-chain', 'test'), ($2, 'summary-chain', 'test')
        "#,
    )
    .bind(from_id)
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
        VALUES ('summary-chain', 'test', $1, $2, 'target-source', 'test')
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .execute(pool)
    .await?;

    let source = CountingPageSource::default();
    let mut connection = pool.acquire().await?;
    let (count, chains) =
        load_active_edge_summary_with_progress(&mut connection, "target-source", 1_000, &source)
            .await?;

    assert_eq!(count, 1);
    assert_eq!(chains, BTreeSet::from(["summary-chain".to_owned()]));
    assert_eq!(
        source.progress_count.load(Ordering::Relaxed),
        1,
        "only the one scoped active-edge page must beat"
    );
    drop(connection);
    database.cleanup().await
}
