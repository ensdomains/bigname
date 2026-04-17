use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use crate::CanonicalityState;

/// Persisted exact block-anchored call snapshot stored as an immutable raw fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCallSnapshot {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub request_hash: String,
    pub request_payload: Value,
    pub response_hash: String,
    pub response_payload: Value,
    pub canonicality_state: CanonicalityState,
}

/// Insert missing raw call snapshots or refresh canonicality for already
/// observed block-scoped call snapshots.
pub async fn upsert_raw_call_snapshots(
    pool: &PgPool,
    snapshots: &[RawCallSnapshot],
) -> Result<Vec<RawCallSnapshot>> {
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw call snapshot upsert")?;

    let persisted = upsert_raw_call_snapshots_in_transaction(&mut transaction, snapshots).await?;

    transaction
        .commit()
        .await
        .context("failed to commit raw call snapshot upsert")?;

    Ok(persisted)
}

/// Insert missing raw call snapshots or refresh canonicality inside an
/// existing transaction so intake can persist them in the same block admission
/// unit as other raw facts.
pub async fn upsert_raw_call_snapshots_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    snapshots: &[RawCallSnapshot],
) -> Result<Vec<RawCallSnapshot>> {
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    let mut persisted = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        validate_raw_call_snapshot(snapshot)?;
        persisted.push(upsert_raw_call_snapshot(transaction, snapshot).await?);
    }

    Ok(persisted)
}

/// Load stored raw call snapshots for one exact block identity.
pub async fn load_raw_call_snapshots_by_block_hash(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Vec<RawCallSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_call_snapshots
        WHERE chain_id = $1
          AND block_hash = $2
        ORDER BY request_hash
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load raw call snapshots for chain {chain_id} block {block_hash}")
    })?;

    rows.into_iter().map(decode_raw_call_snapshot).collect()
}

async fn upsert_raw_call_snapshot(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    snapshot: &RawCallSnapshot,
) -> Result<RawCallSnapshot> {
    if let Some(persisted) = sqlx::query(
        r#"
        INSERT INTO raw_call_snapshots (
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::canonicality_state)
        ON CONFLICT (chain_id, block_hash, request_hash) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&snapshot.chain_id)
    .bind(&snapshot.block_hash)
    .bind(snapshot.block_number)
    .bind(&snapshot.request_hash)
    .bind(&snapshot.request_payload)
    .bind(&snapshot.response_hash)
    .bind(&snapshot.response_payload)
    .bind(snapshot.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw call snapshot for chain {} block {} request {}",
            snapshot.chain_id, snapshot.block_hash, snapshot.request_hash
        )
    })? {
        return decode_raw_call_snapshot(persisted);
    }

    let existing = load_raw_call_snapshot_internal(
        &mut **executor,
        &snapshot.chain_id,
        &snapshot.block_hash,
        &snapshot.request_hash,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw call snapshot for chain {} block {} request {} after insert conflict",
            snapshot.chain_id, snapshot.block_hash, snapshot.request_hash
        )
    })?;

    ensure_raw_call_snapshot_identity_matches(&existing, snapshot)?;
    let next_state = merge_canonicality(existing.canonicality_state, snapshot.canonicality_state);

    let persisted = sqlx::query(
        r#"
        UPDATE raw_call_snapshots
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND request_hash = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&snapshot.chain_id)
    .bind(&snapshot.block_hash)
    .bind(&snapshot.request_hash)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw call snapshot for chain {} block {} request {}",
            snapshot.chain_id, snapshot.block_hash, snapshot.request_hash
        )
    })?;

    decode_raw_call_snapshot(persisted)
}

async fn load_raw_call_snapshot_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    request_hash: &str,
) -> Result<Option<RawCallSnapshot>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_call_snapshots
        WHERE chain_id = $1
          AND block_hash = $2
          AND request_hash = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(request_hash)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw call snapshot for chain {chain_id} block {block_hash} request {request_hash}"
        )
    })?;

    row.map(decode_raw_call_snapshot).transpose()
}

fn validate_raw_call_snapshot(snapshot: &RawCallSnapshot) -> Result<()> {
    if snapshot.block_number < 0 {
        bail!(
            "raw call snapshot for chain {} block {} request {} has negative block number {}",
            snapshot.chain_id,
            snapshot.block_hash,
            snapshot.request_hash,
            snapshot.block_number
        );
    }
    if snapshot.request_hash.is_empty() {
        bail!(
            "raw call snapshot for chain {} block {} has empty request hash",
            snapshot.chain_id,
            snapshot.block_hash
        );
    }
    if snapshot.response_hash.is_empty() {
        bail!(
            "raw call snapshot for chain {} block {} request {} has empty response hash",
            snapshot.chain_id,
            snapshot.block_hash,
            snapshot.request_hash
        );
    }
    if !snapshot.request_payload.is_object() {
        bail!(
            "raw call snapshot for chain {} block {} request {} must have object request payload",
            snapshot.chain_id,
            snapshot.block_hash,
            snapshot.request_hash
        );
    }

    Ok(())
}

fn ensure_raw_call_snapshot_identity_matches(
    existing: &RawCallSnapshot,
    incoming: &RawCallSnapshot,
) -> Result<()> {
    if existing.block_number != incoming.block_number
        || existing.request_payload != incoming.request_payload
        || existing.response_hash != incoming.response_hash
        || existing.response_payload != incoming.response_payload
    {
        bail!(
            "raw call snapshot identity mismatch for chain {} block {} request {}",
            existing.chain_id,
            existing.block_hash,
            existing.request_hash
        );
    }

    Ok(())
}

fn merge_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned {
                incoming
            } else {
                current.promote_to(incoming)
            }
        }
    }
}

fn decode_raw_call_snapshot(row: PgRow) -> Result<RawCallSnapshot> {
    Ok(RawCallSnapshot {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        request_hash: row
            .try_get("request_hash")
            .context("missing request_hash")?,
        request_payload: row
            .try_get("request_payload")
            .context("missing request_payload")?,
        response_hash: row
            .try_get("response_hash")
            .context("missing response_hash")?,
        response_payload: row
            .try_get("response_payload")
            .context("missing response_payload")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };
    use tokio::time::sleep;

    use super::*;
    use crate::{
        RawBlock, default_database_url, mark_raw_block_facts_range_orphaned, upsert_raw_blocks,
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
                .context("failed to parse database URL for raw call snapshot tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_storage_raw_call_snapshot_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for raw call snapshot tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect raw call snapshot test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for raw call snapshot tests")?;

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

    fn raw_call_snapshot(request_hash: &str, state: CanonicalityState) -> RawCallSnapshot {
        RawCallSnapshot {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xaaa".to_owned(),
            block_number: 101,
            request_hash: request_hash.to_owned(),
            request_payload: json!({
                "to": "0x0000000000000000000000000000000000000001",
                "data": format!("0xcall-{request_hash}")
            }),
            response_hash: format!("0xresponse-{request_hash}"),
            response_payload: json!({
                "result": format!("0xresult-{request_hash}")
            }),
            canonicality_state: state,
        }
    }

    fn raw_block(block_hash: &str, parent_hash: &str, block_number: i64) -> RawBlock {
        RawBlock {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: Some(parent_hash.to_owned()),
            block_number,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000 + block_number)
                .expect("valid block timestamp"),
            logs_bloom: None,
            transactions_root: Some(format!("0xtxroot-{block_hash}")),
            receipts_root: Some(format!("0xreceipts-{block_hash}")),
            state_root: Some(format!("0xstate-{block_hash}")),
            canonicality_state: CanonicalityState::Canonical,
        }
    }

    async fn load_observed_at(
        pool: &PgPool,
        chain_id: &str,
        block_hash: &str,
        request_hash: &str,
    ) -> Result<OffsetDateTime> {
        sqlx::query_scalar(
            r#"
            SELECT observed_at
            FROM raw_call_snapshots
            WHERE chain_id = $1
              AND block_hash = $2
              AND request_hash = $3
            "#,
        )
        .bind(chain_id)
        .bind(block_hash)
        .bind(request_hash)
        .fetch_one(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load observed_at for raw call snapshot chain {chain_id} block {block_hash} request {request_hash}"
            )
        })
    }

    #[tokio::test]
    async fn upserts_and_loads_raw_call_snapshots_by_exact_block_identity() -> Result<()> {
        let database = TestDatabase::new().await?;

        let mut transaction = database.pool().begin().await?;
        upsert_raw_call_snapshots_in_transaction(
            &mut transaction,
            &[
                raw_call_snapshot("0xreq-b", CanonicalityState::Canonical),
                raw_call_snapshot("0xreq-a", CanonicalityState::Observed),
                RawCallSnapshot {
                    block_hash: "0xbbb".to_owned(),
                    block_number: 102,
                    request_hash: "0xreq-c".to_owned(),
                    ..raw_call_snapshot("0xreq-c", CanonicalityState::Safe)
                },
            ],
        )
        .await?;
        transaction.commit().await?;

        let loaded =
            load_raw_call_snapshots_by_block_hash(database.pool(), "eth-mainnet", "0xaaa").await?;

        assert_eq!(
            loaded,
            vec![
                raw_call_snapshot("0xreq-a", CanonicalityState::Observed),
                raw_call_snapshot("0xreq-b", CanonicalityState::Canonical),
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn load_by_block_hash_includes_orphaned_raw_call_snapshots() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block("0x001", "0x000", 1),
                raw_block("0x002", "0x001", 2),
            ],
        )
        .await?;
        upsert_raw_call_snapshots(
            database.pool(),
            &[
                RawCallSnapshot {
                    block_hash: "0x001".to_owned(),
                    block_number: 1,
                    request_hash: "0xreq-001".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    ..raw_call_snapshot("0xreq-001", CanonicalityState::Canonical)
                },
                RawCallSnapshot {
                    block_hash: "0x002".to_owned(),
                    block_number: 2,
                    request_hash: "0xreq-002".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    ..raw_call_snapshot("0xreq-002", CanonicalityState::Canonical)
                },
            ],
        )
        .await?;

        let counts = mark_raw_block_facts_range_orphaned(
            database.pool(),
            "eth-mainnet",
            "0x002",
            Some("0x001"),
        )
        .await?;
        assert_eq!(counts.call_snapshot_count, 1);

        let orphaned =
            load_raw_call_snapshots_by_block_hash(database.pool(), "eth-mainnet", "0x002").await?;
        assert_eq!(
            orphaned,
            vec![RawCallSnapshot {
                block_hash: "0x002".to_owned(),
                block_number: 2,
                request_hash: "0xreq-002".to_owned(),
                canonicality_state: CanonicalityState::Orphaned,
                ..raw_call_snapshot("0xreq-002", CanonicalityState::Canonical)
            }]
        );

        let canonical =
            load_raw_call_snapshots_by_block_hash(database.pool(), "eth-mainnet", "0x001").await?;
        assert_eq!(
            canonical,
            vec![RawCallSnapshot {
                block_hash: "0x001".to_owned(),
                block_number: 1,
                request_hash: "0xreq-001".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                ..raw_call_snapshot("0xreq-001", CanonicalityState::Canonical)
            }]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn raw_call_snapshot_upsert_promotes_and_reobserves() -> Result<()> {
        let database = TestDatabase::new().await?;

        let inserted = upsert_raw_call_snapshots(
            database.pool(),
            &[raw_call_snapshot("0xreq-a", CanonicalityState::Observed)],
        )
        .await?;
        assert_eq!(inserted[0].canonicality_state, CanonicalityState::Observed);

        let observed_at_before =
            load_observed_at(database.pool(), "eth-mainnet", "0xaaa", "0xreq-a").await?;

        sleep(Duration::from_millis(5)).await;

        let promoted = upsert_raw_call_snapshots(
            database.pool(),
            &[raw_call_snapshot("0xreq-a", CanonicalityState::Canonical)],
        )
        .await?;
        assert_eq!(promoted[0].canonicality_state, CanonicalityState::Canonical);

        let observed_at_after_promotion =
            load_observed_at(database.pool(), "eth-mainnet", "0xaaa", "0xreq-a").await?;
        assert!(observed_at_after_promotion > observed_at_before);

        sleep(Duration::from_millis(5)).await;

        let reobserved = upsert_raw_call_snapshots(
            database.pool(),
            &[raw_call_snapshot("0xreq-a", CanonicalityState::Observed)],
        )
        .await?;
        assert_eq!(
            reobserved[0].canonicality_state,
            CanonicalityState::Canonical
        );

        let observed_at_after_reobservation =
            load_observed_at(database.pool(), "eth-mainnet", "0xaaa", "0xreq-a").await?;
        assert!(observed_at_after_reobservation > observed_at_after_promotion);

        database.cleanup().await
    }

    #[tokio::test]
    async fn raw_call_snapshot_upsert_rejects_identity_mismatch() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_raw_call_snapshots(
            database.pool(),
            &[raw_call_snapshot("0xreq-a", CanonicalityState::Canonical)],
        )
        .await?;

        let mut conflicting = raw_call_snapshot("0xreq-a", CanonicalityState::Observed);
        conflicting.response_hash = "0xresponse-conflict".to_owned();
        let error = upsert_raw_call_snapshots(database.pool(), &[conflicting])
            .await
            .expect_err("immutable raw call snapshot identity mismatch must fail");

        assert!(
            error
                .to_string()
                .contains("raw call snapshot identity mismatch for chain eth-mainnet block 0xaaa request 0xreq-a"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }
}
