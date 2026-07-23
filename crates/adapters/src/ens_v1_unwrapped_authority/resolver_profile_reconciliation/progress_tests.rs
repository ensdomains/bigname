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
async fn resolver_event_orphaning_pages_only_stale_target_events() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("resolver_event_orphan_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate resolver-event orphan paging test database",
    )
    .await?;
    let pool = database.pool();
    let chain = "resolver-orphan-chain";
    let resolver = "0x0000000000000000000000000000000000000701";
    let run_id = Uuid::from_u128(0x701);

    sqlx::query(
        r#"
        INSERT INTO resolver_profile_reconciliation_runs (
            run_id,
            chain_id,
            first_block_number,
            last_block_number,
            resolver_address_count,
            resolver_address_set_digest,
            status
        )
        VALUES ($1, $2, 10, 10, 1, 'test-digest', 'replay_complete')
        "#,
    )
    .bind(run_id)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resolver_profile_reconciliation_targets (run_id, resolver_address) VALUES ($1, $2)",
    )
    .bind(run_id)
    .bind(resolver)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state
        )
        SELECT
            'out-of-scope-orphan-event-' || value,
            'ens',
            'Other',
            'other_family',
            1,
            'other-chain',
            1,
            'out-of-scope-block',
            'out-of-scope-transaction-' || value,
            value,
            '{}'::JSONB,
            'other_derivation',
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
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state
        )
        VALUES (
            'stale-target-event',
            'ens',
            'AddrChanged',
            'ens_v1_resolver_l1',
            1,
            $1,
            10,
            'target-block',
            'target-transaction',
            0,
            '{"kind":"raw_log"}'::JSONB,
            'ens_v1_unwrapped_authority',
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?;

    let mut transaction = pool.begin().await?;
    let mut progress = CountingProgress::default();
    let mut progress_ref = Some(&mut progress as &mut dyn StartupAdapterProgress);
    let result = publish_resolver_profile_events(
        pool,
        &mut transaction,
        chain,
        ResolverEmitterReplayRange {
            first_block_number: 10,
            last_block_number: 10,
            resolver_block_count: 1,
        },
        run_id,
        "test-digest",
        1,
        &mut progress_ref,
    )
    .await?;
    transaction.commit().await?;

    assert_eq!(result, (0, 1));
    assert_eq!(
        progress.count, 1,
        "only the scoped stale-event page must beat"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_identity = 'stale-target-event'",
        )
        .fetch_one(pool)
        .await?,
        "orphaned"
    );
    database.cleanup().await
}
