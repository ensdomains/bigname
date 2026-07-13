use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::CanonicalityState;

/// Durable input calldata for one transaction selected by a
/// `requires_transaction_input` source family. Replay-required raw fact, not
/// compactable staging (docs/storage.md § Raw-log retention modes).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawTransactionInput {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub input: Vec<u8>,
    pub canonicality_state: CanonicalityState,
}

const RAW_TRANSACTION_INPUT_UPSERT_COLUMN_COUNT: usize = 6;
const RAW_TRANSACTION_INPUT_UPSERT_MAX_ROWS: usize =
    (crate::projection_helpers::POSTGRES_MAX_BIND_PARAMETERS - 1)
        / RAW_TRANSACTION_INPUT_UPSERT_COLUMN_COUNT;

/// Widening idempotent upsert: re-observing a transaction refreshes the input
/// bytes and canonicality state without minting a second fact.
pub async fn upsert_raw_transaction_inputs(
    pool: &PgPool,
    rows: &[RawTransactionInput],
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw_transaction_inputs upsert")?;
    let mut upserted_row_count = 0usize;
    for batch in rows.chunks(RAW_TRANSACTION_INPUT_UPSERT_MAX_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_transaction_inputs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                input,
                canonicality_state
            )
            "#,
        );
        builder.push_values(batch, |mut values, row| {
            values.push_bind(&row.chain_id);
            values.push_bind(&row.block_hash);
            values.push_bind(row.block_number);
            values.push_bind(&row.transaction_hash);
            values.push_bind(&row.input);
            values
                .push_bind(row.canonicality_state.as_str())
                .push_unseparated("::canonicality_state");
        });
        // Same monotonic canonicality merge as the raw_logs upsert: orphaned
        // rows may be re-adopted by a fresh observation, live rows never
        // downgrade below safe/finalized, and input bytes refresh in place
        // (one transaction in one block always carries one input).
        builder.push(
            r#"
            ON CONFLICT (chain_id, block_hash, transaction_hash) DO UPDATE
            SET
                block_number = EXCLUDED.block_number,
                input = EXCLUDED.input,
                canonicality_state = CASE
                    WHEN raw_transaction_inputs.canonicality_state = 'orphaned'::canonicality_state
                        THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state
                        THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_transaction_inputs.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_transaction_inputs.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_transaction_inputs.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_transaction_inputs.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_transaction_inputs.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            "#,
        );
        let result = builder
            .build()
            .execute(&mut *transaction)
            .await
            .context("failed to upsert raw_transaction_inputs rows")?;
        upserted_row_count += result.rows_affected() as usize;
    }
    transaction
        .commit()
        .await
        .context("failed to commit raw_transaction_inputs upsert")?;
    Ok(upserted_row_count)
}

/// Load retained inputs for exact `(block_hash, transaction_hash)` pairs on
/// one chain. Callers join through canonicality-filtered raw logs, so rows on
/// orphaned block hashes are simply never requested.
pub async fn load_raw_transaction_inputs(
    pool: &PgPool,
    chain_id: &str,
    transaction_keys: &[(String, String)],
) -> Result<Vec<RawTransactionInput>> {
    if transaction_keys.is_empty() {
        return Ok(Vec::new());
    }

    let block_hashes = transaction_keys
        .iter()
        .map(|(block_hash, _)| block_hash.clone())
        .collect::<Vec<_>>();
    let transaction_hashes = transaction_keys
        .iter()
        .map(|(_, transaction_hash)| transaction_hash.clone())
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT
            inputs.chain_id,
            inputs.block_hash,
            inputs.block_number,
            inputs.transaction_hash,
            inputs.input,
            inputs.canonicality_state::TEXT AS canonicality_state
        FROM raw_transaction_inputs AS inputs
        JOIN unnest($2::TEXT[], $3::TEXT[]) AS requested(block_hash, transaction_hash)
          ON requested.block_hash = inputs.block_hash
         AND requested.transaction_hash = inputs.transaction_hash
        WHERE inputs.chain_id = $1
        "#,
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .bind(&transaction_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load raw_transaction_inputs for chain {chain_id}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(RawTransactionInput {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                block_hash: crate::sql_row::get(&row, "block_hash")?,
                block_number: crate::sql_row::get(&row, "block_number")?,
                transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
                input: crate::sql_row::get(&row, "input")?,
                canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

    use super::*;
    use crate::default_database_url;

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
                .context("failed to parse database URL for raw_transaction_inputs tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name =
                format!("bg_raw_txin_{}_{unique:x}_{sequence:x}", std::process::id());

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for raw_transaction_inputs tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect raw_transaction_inputs test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for raw_transaction_inputs tests")?;

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

    fn input_row(
        block_hash: &str,
        transaction_hash: &str,
        canonicality_state: CanonicalityState,
    ) -> RawTransactionInput {
        RawTransactionInput {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 5_328_800,
            transaction_hash: transaction_hash.to_owned(),
            input: vec![0xe9, 0xae, 0x5c, 0x53, 0x01],
            canonicality_state,
        }
    }

    #[tokio::test]
    async fn upserts_and_loads_inputs_by_transaction_keys() -> Result<()> {
        let database = TestDatabase::new().await?;

        let rows = vec![
            input_row("0xblock1", "0xtx1", CanonicalityState::Canonical),
            input_row("0xblock1", "0xtx2", CanonicalityState::Canonical),
        ];
        assert_eq!(
            upsert_raw_transaction_inputs(database.pool(), &rows).await?,
            2
        );

        let loaded = load_raw_transaction_inputs(
            database.pool(),
            "ethereum-sepolia",
            &[("0xblock1".to_owned(), "0xtx2".to_owned())],
        )
        .await?;
        assert_eq!(loaded, vec![rows[1].clone()]);

        let other_chain = load_raw_transaction_inputs(
            database.pool(),
            "ethereum-mainnet",
            &[("0xblock1".to_owned(), "0xtx2".to_owned())],
        )
        .await?;
        assert!(other_chain.is_empty());

        database.cleanup().await
    }

    #[tokio::test]
    async fn canonicality_merges_monotonically_on_conflict() -> Result<()> {
        let database = TestDatabase::new().await?;

        let finalized = input_row("0xblock1", "0xtx1", CanonicalityState::Finalized);
        upsert_raw_transaction_inputs(database.pool(), std::slice::from_ref(&finalized)).await?;

        // A later plain observation must not downgrade a finalized fact.
        let observed = input_row("0xblock1", "0xtx1", CanonicalityState::Observed);
        upsert_raw_transaction_inputs(database.pool(), std::slice::from_ref(&observed)).await?;
        let loaded = load_raw_transaction_inputs(
            database.pool(),
            "ethereum-sepolia",
            &[("0xblock1".to_owned(), "0xtx1".to_owned())],
        )
        .await?;
        assert_eq!(loaded[0].canonicality_state, CanonicalityState::Finalized);

        // Orphaned marks stick against non-orphaned re-upserts of the same
        // observation, and a fresh canonical observation re-adopts the fact.
        let orphaned = input_row("0xblock1", "0xtx1", CanonicalityState::Orphaned);
        upsert_raw_transaction_inputs(database.pool(), std::slice::from_ref(&orphaned)).await?;
        let canonical = input_row("0xblock1", "0xtx1", CanonicalityState::Canonical);
        upsert_raw_transaction_inputs(database.pool(), std::slice::from_ref(&canonical)).await?;
        let loaded = load_raw_transaction_inputs(
            database.pool(),
            "ethereum-sepolia",
            &[("0xblock1".to_owned(), "0xtx1".to_owned())],
        )
        .await?;
        assert_eq!(loaded[0].canonicality_state, CanonicalityState::Canonical);

        database.cleanup().await
    }
}
