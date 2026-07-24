use std::sync::atomic::{AtomicUsize, Ordering};

use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use uuid::Uuid;

use super::*;
use crate::DiscoveryObservation;

const OBSERVATION_POINT_INDEX: &str = "discovery_edges_observation_point_lookup_idx";
const HUB_ENDPOINT_INDEX: &str = "discovery_edges_active_from_endpoint_scope_idx";

fn plan_has_two_key_observation_point_probe(plan: &str) -> bool {
    let lines = plan.lines().collect::<Vec<_>>();
    lines.windows(3).any(|window| {
        window[0].contains(OBSERVATION_POINT_INDEX)
            && window[1..]
                .iter()
                .any(|line| line.contains("Index Cond:") && line.contains("observation_key"))
    })
}

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

#[tokio::test]
async fn hub_skew_diff_queries_probe_by_observation_key() -> Result<()> {
    const HUB_EDGE_COUNT: i64 = 2_048;
    const PAGE_SIZE: usize = 1_000;

    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("streamed_diff_observation_point_plan"),
        &bigname_storage::MIGRATOR,
        "failed to migrate streamed diff plan test database",
    )
    .await?;
    let pool = database.pool();
    let from_id = Uuid::from_u128(0xb01);
    let to_id = Uuid::from_u128(0xb02);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, 'hub-chain', 'test'), ($2, 'hub-chain', 'test')
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
        VALUES ($1, 'hub-chain', '0x0000000000000000000000000000000000000b02')
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
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance
        )
        SELECT
            'hub-chain',
            'subregistry',
            $1,
            $2,
            'hub-source',
            'reachable_from_root',
            series,
            '0x' || lpad(series::TEXT, 64, '0'),
            jsonb_build_object(
                'observation_key', 'hub-key-' || lpad(series::TEXT, 8, '0'),
                'transaction_index', 0,
                'log_index', series
            )
        FROM generate_series(1, $3::BIGINT) AS series
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .bind(HUB_EDGE_COUNT)
    .execute(pool)
    .await?;
    let hub_shape = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
        SELECT
            COUNT(*)::BIGINT,
            COUNT(DISTINCT from_contract_instance_id)::BIGINT,
            COUNT(DISTINCT provenance ->> 'observation_key')::BIGINT
        FROM discovery_edges
        WHERE discovery_source = 'hub-source'
        "#,
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(
        hub_shape,
        (HUB_EDGE_COUNT, 1, HUB_EDGE_COUNT),
        "the plan fixture must combine one hub endpoint with selective observation keys"
    );

    let mut transaction = pool.begin().await?;
    super::super::staging::create_streamed_reconcile_temp_tables(transaction.as_mut()).await?;
    sqlx::query(
        r#"
        INSERT INTO pg_temp.reconcile_desired_edges (
            observation_key,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            active_from_transaction_index,
            active_from_log_index,
            provenance_json
        )
        SELECT
            de.provenance ->> 'observation_key',
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            COALESCE(de.source_manifest_id, -1),
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            (de.provenance ->> 'transaction_index')::BIGINT,
            (de.provenance ->> 'log_index')::BIGINT,
            (de.provenance - 'active_to_transaction_index' - 'active_to_log_index')::TEXT
        FROM discovery_edges de
        ORDER BY de.discovery_edge_id
        "#,
    )
    .execute(transaction.as_mut())
    .await?;
    create_streamed_insert_candidate_table(transaction.as_mut()).await?;
    sqlx::query("ANALYZE discovery_edges")
        .execute(transaction.as_mut())
        .await?;
    super::super::staging::analyze_temp_table(transaction.as_mut(), "reconcile_desired_edges")
        .await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(transaction.as_mut())
        .await?;
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(transaction.as_mut())
        .await?;

    let desired_row_ids = sqlx::query_scalar::<_, i64>(
        "SELECT desired_row_id FROM pg_temp.reconcile_desired_edges ORDER BY desired_row_id LIMIT $1",
    )
    .bind(i64::try_from(PAGE_SIZE)?)
    .fetch_all(transaction.as_mut())
    .await?;
    let insert_plan = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (FORMAT TEXT) {}",
        insert_candidate_page_sql()
    ))
    .bind("hub-source")
    .bind(&desired_row_ids)
    .fetch_all(transaction.as_mut())
    .await?
    .join("\n");
    eprintln!("streamed insert-candidate diff plan:\n{insert_plan}");
    assert!(
        plan_has_two_key_observation_point_probe(&insert_plan),
        "insert-candidate anti-join must constrain both observation-point index keys:\n{insert_plan}"
    );
    assert!(
        !insert_plan.contains(HUB_ENDPOINT_INDEX),
        "insert-candidate anti-join must not scan the hub endpoint index:\n{insert_plan}"
    );

    let edge_page = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT
            discovery_edge_id,
            provenance ->> 'observation_key'
        FROM discovery_edges
        WHERE discovery_source = 'hub-source'
        ORDER BY discovery_edge_id
        LIMIT $1
        "#,
    )
    .bind(i64::try_from(PAGE_SIZE)?)
    .fetch_all(transaction.as_mut())
    .await?;
    let (edge_ids, observation_keys): (Vec<_>, Vec<_>) = edge_page.into_iter().unzip();
    let deactivation_plan = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (FORMAT TEXT) {}",
        deactivation_source_page_sql()
    ))
    .bind("hub-source")
    .bind(&edge_ids)
    .bind(&observation_keys)
    .fetch_all(transaction.as_mut())
    .await?
    .join("\n");
    eprintln!("streamed deactivation diff plan:\n{deactivation_plan}");
    assert!(
        plan_has_two_key_observation_point_probe(&deactivation_plan),
        "deactivation anti-join must constrain both observation-point index keys:\n{deactivation_plan}"
    );
    assert!(
        !deactivation_plan.contains(HUB_ENDPOINT_INDEX),
        "deactivation anti-join must not scan the hub endpoint index:\n{deactivation_plan}"
    );

    transaction.rollback().await?;
    database.cleanup().await
}

#[tokio::test]
async fn historical_pages_only_desired_edges_with_successors() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("streamed_historical_successor_scoped_pages"),
        &bigname_storage::MIGRATOR,
        "failed to migrate streamed historical paging test database",
    )
    .await?;
    let pool = database.pool();
    let from_id = Uuid::from_u128(0xa01);
    let to_id = Uuid::from_u128(0xa02);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, 'historical-chain', 'test'), ($2, 'historical-chain', 'test')
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
            address,
            active_from_block_number
        )
        VALUES (
            $1,
            'historical-chain',
            '0x0000000000000000000000000000000000000a02',
            20
        )
        "#,
    )
    .bind(to_id)
    .execute(pool)
    .await?;
    let successor_edge_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance
        )
        VALUES (
            'historical-chain',
            'subregistry',
            $1,
            $2,
            'target-source',
            'reachable_from_root',
            20,
            '0x20',
            '{"observation_key":"historical"}'::JSONB
        )
        RETURNING discovery_edge_id
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .fetch_one(pool)
    .await?;

    let mut transaction = pool.begin().await?;
    super::super::staging::create_streamed_reconcile_temp_tables(transaction.as_mut()).await?;
    sqlx::query(
        r#"
        INSERT INTO pg_temp.reconcile_desired_edges (
            observation_key,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance_json
        )
        SELECT
            format('non-historical-%s', series),
            'historical-chain',
            'subregistry',
            $1,
            $2,
            'target-source',
            -1,
            'reachable_from_root',
            10,
            '0x10',
            format('{"observation_key":"non-historical-%s"}', series)
        FROM generate_series(1, 2001) AS series
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .execute(transaction.as_mut())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO pg_temp.reconcile_desired_edges (
            observation_key,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance_json
        )
        VALUES (
            'historical',
            'historical-chain',
            'subregistry',
            $1,
            $2,
            'target-source',
            -1,
            'reachable_from_root',
            10,
            '0x10',
            '{"observation_key":"historical"}'
        )
        "#,
    )
    .bind(from_id)
    .bind(to_id)
    .execute(transaction.as_mut())
    .await?;

    let source = CountingPageSource::default();
    let mut retained_newer_edge_ids = HashSet::new();
    let historical = collect_streamed_historical_edges(
        transaction.as_mut(),
        "target-source",
        1_000,
        &mut retained_newer_edge_ids,
        &source,
    )
    .await?;

    assert_eq!(historical.len(), 1);
    assert_eq!(historical[0].1.observation_key, "historical");
    assert_eq!(retained_newer_edge_ids, HashSet::from([successor_edge_id]));
    assert_eq!(
        source.progress_count.load(Ordering::Relaxed),
        2,
        "only the one true historical-successor page must beat"
    );
    transaction.rollback().await?;
    database.cleanup().await
}
