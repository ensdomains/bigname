use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use sqlx::types::time::OffsetDateTime;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    query_scalar,
};

use super::*;
use crate::{ChainLineageBlock, default_database_url, upsert_chain_lineage_blocks};

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
            .context("failed to parse database URL for storage integration tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for storage integration tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect storage integration test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for storage integration tests")?;

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

fn lineage_block(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp,
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtx{:02x}", block_number)),
        receipts_root: Some(format!("0xrc{:02x}", block_number)),
        state_root: Some(format!("0xst{:02x}", block_number)),
        canonicality_state,
    }
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

#[tokio::test]
async fn syncs_checkpoint_rows_and_loads_snapshots() -> Result<()> {
    let database = TestDatabase::new().await?;

    let watched_chain_ids = vec![
        "base-mainnet".to_owned(),
        "eth-mainnet".to_owned(),
        "eth-mainnet".to_owned(),
    ];
    sync_chain_checkpoints(database.pool(), &watched_chain_ids).await?;

    sqlx::query(
        r#"
            UPDATE chain_checkpoints
            SET
                canonical_block_hash = '0xcanon',
                canonical_block_number = 101,
                safe_block_hash = '0xsafe',
                safe_block_number = 100,
                finalized_block_hash = '0xfinal',
                finalized_block_number = 99
            WHERE chain_id = 'eth-mainnet'
            "#,
    )
    .execute(database.pool())
    .await?;

    let snapshots = sync_chain_checkpoints(database.pool(), &watched_chain_ids).await?;

    assert_eq!(
        snapshots,
        vec![
            ChainCheckpoint {
                chain_id: "base-mainnet".to_owned(),
                canonical_block_hash: None,
                canonical_block_number: None,
                safe_block_hash: None,
                safe_block_number: None,
                finalized_block_hash: None,
                finalized_block_number: None,
            },
            ChainCheckpoint {
                chain_id: "eth-mainnet".to_owned(),
                canonical_block_hash: Some("0xcanon".to_owned()),
                canonical_block_number: Some(101),
                safe_block_hash: Some("0xsafe".to_owned()),
                safe_block_number: Some(100),
                finalized_block_hash: Some("0xfinal".to_owned()),
                finalized_block_number: Some(99),
            },
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn ensure_does_not_delete_history_when_watch_set_shrinks() -> Result<()> {
    let database = TestDatabase::new().await?;

    let initial_chain_ids = vec!["base-mainnet".to_owned(), "eth-mainnet".to_owned()];
    let shrunk_chain_ids = vec!["eth-mainnet".to_owned()];
    sync_chain_checkpoints(database.pool(), &initial_chain_ids).await?;
    let shrunk_snapshots = sync_chain_checkpoints(database.pool(), &shrunk_chain_ids).await?;

    let row_count: i64 = query_scalar("SELECT COUNT(*) FROM chain_checkpoints")
        .fetch_one(database.pool())
        .await?;
    let snapshots = sync_chain_checkpoints(database.pool(), &initial_chain_ids).await?;

    assert_eq!(row_count, 2);
    assert_eq!(
        shrunk_snapshots
            .into_iter()
            .map(|snapshot| snapshot.chain_id)
            .collect::<Vec<_>>(),
        vec!["eth-mainnet".to_owned()]
    );
    assert_eq!(
        snapshots
            .into_iter()
            .map(|snapshot| snapshot.chain_id)
            .collect::<Vec<_>>(),
        vec!["base-mainnet".to_owned(), "eth-mainnet".to_owned()]
    );

    database.cleanup().await
}

#[tokio::test]
async fn empty_chain_set_is_a_no_op() -> Result<()> {
    let database = TestDatabase::new().await?;

    let snapshots = sync_chain_checkpoints(database.pool(), &[]).await?;

    assert!(snapshots.is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn advances_checkpoints_and_promotes_lineage_states() -> Result<()> {
    let database = TestDatabase::new().await?;
    let base_timestamp = timestamp(1_717_171_717);

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block(
                "eth-mainnet",
                "0x001",
                None,
                1,
                base_timestamp,
                CanonicalityState::Observed,
            ),
            lineage_block(
                "eth-mainnet",
                "0x002",
                Some("0x001"),
                2,
                timestamp(1_717_171_729),
                CanonicalityState::Observed,
            ),
            lineage_block(
                "eth-mainnet",
                "0x003",
                Some("0x002"),
                3,
                timestamp(1_717_171_741),
                CanonicalityState::Observed,
            ),
        ],
    )
    .await?;

    let snapshot = advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "eth-mainnet".to_owned(),
            canonical: Some(CheckpointBlockRef {
                block_hash: "0x003".to_owned(),
                block_number: 3,
            }),
            safe: Some(CheckpointBlockRef {
                block_hash: "0x002".to_owned(),
                block_number: 2,
            }),
            finalized: Some(CheckpointBlockRef {
                block_hash: "0x001".to_owned(),
                block_number: 1,
            }),
        },
    )
    .await?;

    assert_eq!(
        snapshot,
        ChainCheckpoint {
            chain_id: "eth-mainnet".to_owned(),
            canonical_block_hash: Some("0x003".to_owned()),
            canonical_block_number: Some(3),
            safe_block_hash: Some("0x002".to_owned()),
            safe_block_number: Some(2),
            finalized_block_hash: Some("0x001".to_owned()),
            finalized_block_number: Some(1),
        }
    );

    let canonicality_by_hash = sqlx::query_as::<_, (String, String)>(
        r#"
            SELECT block_hash, canonicality_state::TEXT
            FROM chain_lineage
            WHERE chain_id = 'eth-mainnet'
            ORDER BY block_number
            "#,
    )
    .fetch_all(database.pool())
    .await?;

    assert_eq!(
        canonicality_by_hash,
        vec![
            ("0x001".to_owned(), "finalized".to_owned()),
            ("0x002".to_owned(), "safe".to_owned()),
            ("0x003".to_owned(), "canonical".to_owned()),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_safe_checkpoint_regression() -> Result<()> {
    let database = TestDatabase::new().await?;
    let base_timestamp = timestamp(1_717_171_717);

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block(
                "eth-mainnet",
                "0x001",
                None,
                1,
                base_timestamp,
                CanonicalityState::Observed,
            ),
            lineage_block(
                "eth-mainnet",
                "0x002",
                Some("0x001"),
                2,
                timestamp(1_717_171_729),
                CanonicalityState::Observed,
            ),
        ],
    )
    .await?;

    advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "eth-mainnet".to_owned(),
            canonical: Some(CheckpointBlockRef {
                block_hash: "0x002".to_owned(),
                block_number: 2,
            }),
            safe: Some(CheckpointBlockRef {
                block_hash: "0x002".to_owned(),
                block_number: 2,
            }),
            finalized: None,
        },
    )
    .await?;

    let error = advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "eth-mainnet".to_owned(),
            canonical: None,
            safe: Some(CheckpointBlockRef {
                block_hash: "0x001".to_owned(),
                block_number: 1,
            }),
            finalized: None,
        },
    )
    .await
    .expect_err("safe checkpoint regression must fail");

    assert!(
        error
            .to_string()
            .contains("safe checkpoint for chain eth-mainnet cannot move backward"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}
