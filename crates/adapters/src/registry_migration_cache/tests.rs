use super::*;
use bigname_storage::{
    CanonicalityState, RawBlock, RawLog, default_database_url, upsert_raw_blocks, upsert_raw_logs,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for registry migration cache tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_ad_mig_{}_{}_{}", std::process::id(), sequence, unique);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for registry migration cache tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for registry migration cache tests")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for registry migration cache tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

#[test]
fn migrated_registry_nodes_snapshots_do_not_learn_later_cache_nodes() {
    let early = MigratedRegistryNodes::from_baseline(HashSet::from(["0x01".to_owned()]));
    let later =
        MigratedRegistryNodes::from_baseline(HashSet::from(["0x01".to_owned(), "0x02".to_owned()]));

    assert!(early.contains("0x01"));
    assert!(!early.contains("0x02"));
    assert!(later.contains("0x02"));
}

#[tokio::test]
async fn migration_marker_cache_drops_reorged_away_marker_without_restart() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let marker_topic0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let migrated_node = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let emitter =
        RegistryMigrationMarkerEmitter::new("0x00000000000000000000000000000000000000aa", 0, 100);
    let key = RegistryMigrationMarkerCacheKey {
        chain: chain.to_owned(),
        marker_topic0: marker_topic0.to_owned(),
        emitters: vec![emitter.clone()],
    };
    let mut cache = HashMap::new();

    upsert_raw_blocks(
        database.pool(),
        &[RawBlock {
            chain_id: chain.to_owned(),
            block_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .to_owned(),
            parent_hash: None,
            block_number: 10,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_010)?,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: chain.to_owned(),
            block_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .to_owned(),
            block_number: 10,
            transaction_hash: "0xtxcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: emitter.address.clone(),
            topics: vec![marker_topic0.to_owned(), migrated_node.to_owned()],
            data: Vec::new(),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let decode_node = |topics: &[String]| {
        topics
            .get(1)
            .cloned()
            .context("marker test log is missing node topic")
    };
    let first = load_migrated_registry_nodes_before_block_with_cache(
        database.pool(),
        chain,
        key.clone(),
        11,
        marker_topic0,
        &decode_node,
        &mut cache,
    )
    .await?;
    assert!(first.contains(migrated_node));

    sqlx::query(
        "UPDATE raw_logs SET canonicality_state = 'orphaned'::canonicality_state WHERE block_hash = $1",
    )
    .bind("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
    .execute(database.pool())
    .await
    .context("failed to orphan cached migration marker")?;

    let reloaded = load_migrated_registry_nodes_before_block_with_cache(
        database.pool(),
        chain,
        key,
        11,
        marker_topic0,
        &decode_node,
        &mut cache,
    )
    .await?;
    assert!(!reloaded.contains(migrated_node));

    database.cleanup().await
}

#[tokio::test]
async fn migration_marker_cache_reuses_finalized_prefix_and_reloads_volatile_tail() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let marker_topic0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let finalized_node = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let volatile_node = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let emitter =
        RegistryMigrationMarkerEmitter::new("0x00000000000000000000000000000000000000aa", 0, 100);
    let key = RegistryMigrationMarkerCacheKey {
        chain: chain.to_owned(),
        marker_topic0: marker_topic0.to_owned(),
        emitters: vec![emitter.clone()],
    };
    let mut cache = HashMap::new();

    upsert_raw_blocks(
        database.pool(),
        &[
            RawBlock {
                chain_id: chain.to_owned(),
                block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
                parent_hash: None,
                block_number: 10,
                block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_010)?,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Finalized,
            },
            RawBlock {
                chain_id: chain.to_owned(),
                block_hash: "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    .to_owned(),
                parent_hash: Some(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ),
                block_number: 20,
                block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_020)?,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
                block_number: 10,
                transaction_hash:
                    "0xtxbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitter.address.clone(),
                topics: vec![marker_topic0.to_owned(), finalized_node.to_owned()],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    .to_owned(),
                block_number: 20,
                transaction_hash:
                    "0xtxdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitter.address.clone(),
                topics: vec![marker_topic0.to_owned(), volatile_node.to_owned()],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let decode_calls = AtomicUsize::new(0);
    let decode_node = |topics: &[String]| {
        decode_calls.fetch_add(1, Ordering::Relaxed);
        topics
            .get(1)
            .cloned()
            .context("marker test log is missing node topic")
    };
    let first = load_migrated_registry_nodes_before_block_with_cache(
        database.pool(),
        chain,
        key.clone(),
        30,
        marker_topic0,
        &decode_node,
        &mut cache,
    )
    .await?;
    assert!(first.contains(finalized_node));
    assert!(first.contains(volatile_node));
    assert_eq!(decode_calls.load(Ordering::Relaxed), 2);

    decode_calls.store(0, Ordering::Relaxed);
    let second = load_migrated_registry_nodes_before_block_with_cache(
        database.pool(),
        chain,
        key,
        31,
        marker_topic0,
        &decode_node,
        &mut cache,
    )
    .await?;
    assert!(second.contains(finalized_node));
    assert!(second.contains(volatile_node));
    assert_eq!(
        decode_calls.load(Ordering::Relaxed),
        1,
        "finalized marker prefix should stay cached; only the volatile tail should reload"
    );

    database.cleanup().await
}
