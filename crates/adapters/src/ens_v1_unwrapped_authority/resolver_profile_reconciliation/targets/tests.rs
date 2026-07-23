use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

#[derive(Default)]
struct CountingProgress {
    count: usize,
}

impl StartupAdapterProgress for CountingProgress {
    fn record<'a>(
        &'a mut self,
        _pool: &'a PgPool,
    ) -> crate::checkpoint_context::StartupAdapterProgressFuture<'a> {
        self.count += 1;
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn resolver_replay_range_pages_only_target_emitters_on_the_requested_chain() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("resolver_replay_range_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate resolver replay-range paging test database",
    )
    .await?;
    let pool = database.pool();
    let chain = "resolver-target-chain";
    let resolver = "0x0000000000000000000000000000000000000601";

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
            canonicality_state
        )
        SELECT
            'other-chain',
            'out-of-scope-block',
            1,
            'out-of-scope-transaction',
            0,
            value,
            '0x0000000000000000000000000000000000000999',
            'canonical'::canonicality_state
        FROM generate_series(1, 2001) value
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES ($1, 'target-block', 10, now(), 'canonical'::canonicality_state)
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?;
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
            canonicality_state
        )
        VALUES (
            $1,
            'target-block',
            10,
            'target-transaction',
            0,
            0,
            $2,
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(chain)
    .bind(resolver)
    .execute(pool)
    .await?;

    let mut reconciliation = begin_reconciliation(pool, chain).await?;
    reconciliation
        .stage_addresses(&[resolver.to_owned()])
        .await?;
    let mut progress = CountingProgress::default();
    let mut progress_ref = Some(&mut progress as &mut dyn StartupAdapterProgress);
    let prepared = reconciliation.prepare(&mut progress_ref).await?;

    assert_eq!(
        prepared.replay_range,
        Some(ResolverEmitterReplayRange {
            first_block_number: 10,
            last_block_number: 10,
            resolver_block_count: 1,
        })
    );
    assert_eq!(
        progress.count, 2,
        "one target page and one scoped raw-log page must beat"
    );
    drop(reconciliation);
    database.cleanup().await
}
