use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

use super::*;

#[tokio::test]
async fn deactivation_source_pages_only_active_edges_for_the_requested_source() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("streamed_deactivation_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate streamed deactivation paging test database",
    )
    .await?;
    let pool = database.pool();
    let from_id = Uuid::from_u128(0x901);
    let to_id = Uuid::from_u128(0x902);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, 'deactivation-chain', 'test'), ($2, 'deactivation-chain', 'test')
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
            'deactivation-chain',
            '0x0000000000000000000000000000000000000902'
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
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission,
            provenance
        )
        VALUES (
            'deactivation-chain',
            'subregistry',
            $1,
            $2,
            'target-source',
            'reachable_from_root',
            '{"observation_key":"target-edge"}'::JSONB
        )
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .execute(pool)
    .await?;

    let mut transaction = pool.begin().await?;
    super::super::staging::create_streamed_reconcile_temp_tables(transaction.as_mut()).await?;
    let mut after_id = 0;
    let mut page_count = 0;
    let mut candidate_count = 0;
    loop {
        let page = load_streamed_deactivation_source_page(
            transaction.as_mut(),
            "target-source",
            after_id,
            1,
        )
        .await?;
        let Some(last_id) = page.last_edge_id else {
            break;
        };
        after_id = last_id;
        page_count += 1;
        candidate_count += page.candidates.len();
    }

    assert_eq!(candidate_count, 1);
    assert_eq!(
        page_count, 1,
        "only the requested source's one active edge must form a page"
    );
    transaction.rollback().await?;
    database.cleanup().await
}
