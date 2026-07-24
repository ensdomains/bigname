use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

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
async fn requirement_index_exact_page_records_one_progress_beat() -> Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new().connect_lazy_with(
        bigname_storage::stamp_projection_replay_version(
            "postgres://postgres:postgres@localhost/bigname_test".parse()?,
        ),
    );
    let requirements = (0..WITNESS_PAGE_ROWS)
        .map(|index| RequiredWatchedTuple {
            source_family: "ens_v2_registry_l1".to_owned(),
            address: format!("0x{index:040x}"),
            required_from_block: 0,
            required_to_block: 20,
        })
        .collect::<Vec<_>>();
    let mut progress = CountingProgress::default();
    let mut progress_ref = Some(&mut progress as &mut dyn StartupAdapterProgress);

    requirement_indexes(Some(&pool), &requirements, &mut progress_ref).await?;

    assert_eq!(
        progress.count, 1,
        "one exact requirement page must record exactly one completed-work beat"
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn witness_pages_skip_rows_outside_the_ens_v2_chain_scope() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("ens_v2_witness_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate ENSv2 witness paging test database",
    )
    .await?;
    let pool = database.pool();
    let chain = "witness-target-chain";
    let from_id = Uuid::from_u128(0x501);
    let to_id = Uuid::from_u128(0x502);

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
            'out-of-scope-event-' || value,
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
            'target-event',
            'ens',
            'RegistryEvent',
            'ens_v2_registry_l1',
            1,
            $1,
            10,
            'target-block',
            'target-transaction',
            0,
            '{"kind":"raw_log","emitting_address":"0x0000000000000000000000000000000000000501"}'::JSONB,
            $2,
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(chain)
    .bind(DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES ($1, $3, 'test'), ($2, $3, 'test')
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .bind(chain)
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
            admission,
            active_from_block_number,
            provenance
        )
        SELECT
            'other-chain',
            'test',
            $1,
            $2,
            'other_source',
            'test',
            1,
            '{}'::JSONB
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
            admission,
            active_from_block_number,
            provenance
        )
        VALUES (
            $1,
            'subregistry',
            $2,
            $3,
            'ens_v2_registry_test',
            'reachable_from_root',
            10,
            '{
                "source":"raw_log",
                "from_address":"0x0000000000000000000000000000000000000501",
                "block_hash":"target-block",
                "log_index":"0"
            }'::JSONB
        )
        "#,
    )
    .bind(chain)
    .bind(from_id)
    .bind(to_id)
    .execute(pool)
    .await?;

    let requirements = [RequiredWatchedTuple {
        source_family: "ens_v2_registry_l1".to_owned(),
        address: "0x0000000000000000000000000000000000000501".to_owned(),
        required_from_block: 0,
        required_to_block: 20,
    }];
    let mut connection = pool.acquire().await?;
    let mut progress = CountingProgress::default();
    ensure_retained_semantic_witnesses_with_progress(
        pool,
        &mut connection,
        chain,
        &requirements,
        20,
        &mut progress,
    )
    .await?;

    assert_eq!(
        progress.count, 3,
        "one requirement page and one scoped page per witness table must beat"
    );
    database.cleanup().await
}
