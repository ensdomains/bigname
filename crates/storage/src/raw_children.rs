use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use crate::CanonicalityState;

/// Persisted exact transaction fact anchored to one observed block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawTransaction {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub from_address: String,
    pub to_address: Option<String>,
    pub canonicality_state: CanonicalityState,
}

/// Persisted exact receipt fact anchored to one observed block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawReceipt {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_address: Option<String>,
    pub status: Option<bool>,
    pub gas_used: Option<i64>,
    pub cumulative_gas_used: Option<i64>,
    pub logs_bloom: Option<Vec<u8>>,
    pub canonicality_state: CanonicalityState,
}

/// Persisted exact log fact anchored to one observed block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawLog {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub emitting_address: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
    pub canonicality_state: CanonicalityState,
}

/// Counts of block-scoped raw facts orphaned during a reorg repair.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawFactOrphanCounts {
    pub block_count: u64,
    pub code_hash_count: u64,
    pub transaction_count: u64,
    pub receipt_count: u64,
    pub log_count: u64,
    pub call_snapshot_count: u64,
    pub payload_cache_metadata_count: u64,
}

/// Insert missing raw transaction rows or refresh canonicality for already
/// observed block-scoped transactions.
pub async fn upsert_raw_transactions(
    pool: &PgPool,
    transactions: &[RawTransaction],
) -> Result<Vec<RawTransaction>> {
    if transactions.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw transaction upsert")?;

    let mut snapshots = Vec::with_capacity(transactions.len());
    for raw_transaction in transactions {
        validate_raw_transaction(raw_transaction)?;
        snapshots.push(upsert_raw_transaction(&mut transaction, raw_transaction).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw transaction upsert")?;

    Ok(snapshots)
}

/// Insert missing raw receipt rows or refresh canonicality for already observed
/// block-scoped receipts.
pub async fn upsert_raw_receipts(
    pool: &PgPool,
    receipts: &[RawReceipt],
) -> Result<Vec<RawReceipt>> {
    if receipts.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw receipt upsert")?;

    let mut snapshots = Vec::with_capacity(receipts.len());
    for raw_receipt in receipts {
        validate_raw_receipt(raw_receipt)?;
        snapshots.push(upsert_raw_receipt(&mut transaction, raw_receipt).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw receipt upsert")?;

    Ok(snapshots)
}

/// Insert missing raw log rows or refresh canonicality for already observed
/// block-scoped logs.
pub async fn upsert_raw_logs(pool: &PgPool, logs: &[RawLog]) -> Result<Vec<RawLog>> {
    if logs.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw log upsert")?;

    let mut snapshots = Vec::with_capacity(logs.len());
    for raw_log in logs {
        validate_raw_log(raw_log)?;
        snapshots.push(upsert_raw_log(&mut transaction, raw_log).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw log upsert")?;

    Ok(snapshots)
}

/// Walk a stored raw-block branch and mark every block-scoped raw fact on that
/// losing branch `orphaned` until `stop_before_hash` is reached.
pub async fn mark_raw_block_facts_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<RawFactOrphanCounts> {
    if stop_before_hash == Some(from_hash) {
        return Ok(RawFactOrphanCounts::default());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw fact orphaning")?;

    let block_hashes = load_raw_block_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load raw block hash path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if block_hashes.is_empty() {
        bail!("missing stored raw block for chain {chain_id} block {from_hash}");
    }

    let block_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_blocks",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let code_hash_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_code_hashes",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let transaction_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_transactions",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let receipt_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_receipts",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let log_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_logs",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let call_snapshot_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_call_snapshots",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let payload_cache_metadata_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_payload_cache_metadata",
        "last_observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit raw fact orphaning")?;

    Ok(RawFactOrphanCounts {
        block_count,
        code_hash_count,
        transaction_count,
        receipt_count,
        log_count,
        call_snapshot_count,
        payload_cache_metadata_count,
    })
}

async fn upsert_raw_transaction(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    transaction: &RawTransaction,
) -> Result<RawTransaction> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_transactions (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::canonicality_state)
        ON CONFLICT (chain_id, block_hash, transaction_index) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&transaction.chain_id)
    .bind(&transaction.block_hash)
    .bind(transaction.block_number)
    .bind(&transaction.transaction_hash)
    .bind(transaction.transaction_index)
    .bind(&transaction.from_address)
    .bind(&transaction.to_address)
    .bind(transaction.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw transaction for chain {} block {} transaction {}",
            transaction.chain_id, transaction.block_hash, transaction.transaction_hash
        )
    })? {
        return decode_raw_transaction(snapshot);
    }

    let existing = load_raw_transaction_internal(
        &mut **executor,
        &transaction.chain_id,
        &transaction.block_hash,
        transaction.transaction_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw transaction for chain {} block {} index {} after insert conflict",
            transaction.chain_id, transaction.block_hash, transaction.transaction_index
        )
    })?;

    ensure_raw_transaction_identity_matches(&existing, transaction)?;
    let next_state =
        merge_canonicality(existing.canonicality_state, transaction.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_transactions
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&transaction.chain_id)
    .bind(&transaction.block_hash)
    .bind(transaction.transaction_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw transaction for chain {} block {} index {}",
            transaction.chain_id, transaction.block_hash, transaction.transaction_index
        )
    })?;

    decode_raw_transaction(snapshot)
}

async fn upsert_raw_receipt(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    receipt: &RawReceipt,
) -> Result<RawReceipt> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_receipts (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)
        ON CONFLICT (chain_id, block_hash, transaction_index) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&receipt.chain_id)
    .bind(&receipt.block_hash)
    .bind(receipt.block_number)
    .bind(&receipt.transaction_hash)
    .bind(receipt.transaction_index)
    .bind(&receipt.contract_address)
    .bind(receipt.status)
    .bind(receipt.gas_used)
    .bind(receipt.cumulative_gas_used)
    .bind(&receipt.logs_bloom)
    .bind(receipt.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw receipt for chain {} block {} transaction {}",
            receipt.chain_id, receipt.block_hash, receipt.transaction_hash
        )
    })? {
        return decode_raw_receipt(snapshot);
    }

    let existing = load_raw_receipt_internal(
        &mut **executor,
        &receipt.chain_id,
        &receipt.block_hash,
        receipt.transaction_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw receipt for chain {} block {} index {} after insert conflict",
            receipt.chain_id, receipt.block_hash, receipt.transaction_index
        )
    })?;

    ensure_raw_receipt_identity_matches(&existing, receipt)?;
    let next_state = merge_canonicality(existing.canonicality_state, receipt.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_receipts
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&receipt.chain_id)
    .bind(&receipt.block_hash)
    .bind(receipt.transaction_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw receipt for chain {} block {} index {}",
            receipt.chain_id, receipt.block_hash, receipt.transaction_index
        )
    })?;

    decode_raw_receipt(snapshot)
}

async fn upsert_raw_log(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    log: &RawLog,
) -> Result<RawLog> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::canonicality_state)
        ON CONFLICT (chain_id, block_hash, log_index) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&log.chain_id)
    .bind(&log.block_hash)
    .bind(log.block_number)
    .bind(&log.transaction_hash)
    .bind(log.transaction_index)
    .bind(log.log_index)
    .bind(&log.emitting_address)
    .bind(&log.topics)
    .bind(&log.data)
    .bind(log.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw log for chain {} block {} log {}",
            log.chain_id, log.block_hash, log.log_index
        )
    })? {
        return decode_raw_log(snapshot);
    }

    let existing = load_raw_log_internal(
        &mut **executor,
        &log.chain_id,
        &log.block_hash,
        log.log_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw log for chain {} block {} log {} after insert conflict",
            log.chain_id, log.block_hash, log.log_index
        )
    })?;

    ensure_raw_log_identity_matches(&existing, log)?;
    let next_state = merge_canonicality(existing.canonicality_state, log.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_logs
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND log_index = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&log.chain_id)
    .bind(&log.block_hash)
    .bind(log.log_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw log for chain {} block {} log {}",
            log.chain_id, log.block_hash, log.log_index
        )
    })?;

    decode_raw_log(snapshot)
}

async fn load_raw_transaction_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    transaction_index: i64,
) -> Result<Option<RawTransaction>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_transactions
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(transaction_index)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw transaction for chain {chain_id} block {block_hash} index {transaction_index}"
        )
    })?;

    row.map(decode_raw_transaction).transpose()
}

async fn load_raw_receipt_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    transaction_index: i64,
) -> Result<Option<RawReceipt>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_receipts
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(transaction_index)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw receipt for chain {chain_id} block {block_hash} index {transaction_index}"
        )
    })?;

    row.map(decode_raw_receipt).transpose()
}

async fn load_raw_log_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    log_index: i64,
) -> Result<Option<RawLog>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = $2
          AND log_index = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(log_index)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to load raw log for chain {chain_id} block {block_hash} log {log_index}")
    })?;

    row.map(decode_raw_log).transpose()
}

async fn load_raw_block_hash_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<String>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE raw_block_path AS (
            SELECT chain_id, block_hash, parent_hash, 0 AS depth
            FROM raw_blocks
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.chain_id, parent.block_hash, parent.parent_hash, raw_block_path.depth + 1
            FROM raw_blocks AS parent
            JOIN raw_block_path
              ON parent.chain_id = raw_block_path.chain_id
             AND parent.block_hash = raw_block_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT block_hash
        FROM raw_block_path
        ORDER BY depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("block_hash")
                .context("missing block_hash in raw block path")
        })
        .collect()
}

async fn mark_block_hash_set_orphaned<'e, E>(
    executor: E,
    table_name: &str,
    timestamp_column: &str,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<u64>
where
    E: Executor<'e, Database = Postgres>,
{
    let query = format!(
        r#"
        UPDATE {table_name}
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            {timestamp_column} = now()
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#
    );

    sqlx::query(&query)
        .bind(chain_id)
        .bind(block_hashes)
        .execute(executor)
        .await
        .with_context(|| {
            format!("failed to mark orphaned raw facts in {table_name} for chain {chain_id}")
        })
        .map(|result| result.rows_affected())
}

fn validate_raw_transaction(transaction: &RawTransaction) -> Result<()> {
    if transaction.block_number < 0 {
        bail!(
            "raw transaction for chain {} block {} has negative block number {}",
            transaction.chain_id,
            transaction.block_hash,
            transaction.block_number
        );
    }
    if transaction.transaction_index < 0 {
        bail!(
            "raw transaction for chain {} block {} transaction {} has negative transaction index {}",
            transaction.chain_id,
            transaction.block_hash,
            transaction.transaction_hash,
            transaction.transaction_index
        );
    }
    Ok(())
}

fn validate_raw_receipt(receipt: &RawReceipt) -> Result<()> {
    if receipt.block_number < 0 {
        bail!(
            "raw receipt for chain {} block {} has negative block number {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.block_number
        );
    }
    if receipt.transaction_index < 0 {
        bail!(
            "raw receipt for chain {} block {} transaction {} has negative transaction index {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.transaction_hash,
            receipt.transaction_index
        );
    }
    if let Some(cumulative_gas_used) = receipt.cumulative_gas_used
        && cumulative_gas_used < 0
    {
        bail!(
            "raw receipt for chain {} block {} transaction {} has negative cumulative gas used {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.transaction_hash,
            cumulative_gas_used
        );
    }
    if let Some(gas_used) = receipt.gas_used
        && gas_used < 0
    {
        bail!(
            "raw receipt for chain {} block {} transaction {} has negative gas used {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.transaction_hash,
            gas_used
        );
    }

    Ok(())
}

fn validate_raw_log(log: &RawLog) -> Result<()> {
    if log.block_number < 0 {
        bail!(
            "raw log for chain {} block {} has negative block number {}",
            log.chain_id,
            log.block_hash,
            log.block_number
        );
    }
    if log.transaction_index < 0 {
        bail!(
            "raw log for chain {} block {} log {} has negative transaction index {}",
            log.chain_id,
            log.block_hash,
            log.log_index,
            log.transaction_index
        );
    }
    if log.log_index < 0 {
        bail!(
            "raw log for chain {} block {} has negative log index {}",
            log.chain_id,
            log.block_hash,
            log.log_index
        );
    }

    Ok(())
}

fn ensure_raw_transaction_identity_matches(
    existing: &RawTransaction,
    incoming: &RawTransaction,
) -> Result<()> {
    if existing.transaction_hash != incoming.transaction_hash
        || existing.block_number != incoming.block_number
        || existing.from_address != incoming.from_address
        || existing.to_address != incoming.to_address
    {
        bail!(
            "raw transaction identity mismatch for chain {} block {} index {}",
            existing.chain_id,
            existing.block_hash,
            existing.transaction_index
        );
    }

    Ok(())
}

fn ensure_raw_receipt_identity_matches(existing: &RawReceipt, incoming: &RawReceipt) -> Result<()> {
    if existing.transaction_hash != incoming.transaction_hash
        || existing.block_number != incoming.block_number
        || existing.contract_address != incoming.contract_address
        || existing.status != incoming.status
        || existing.gas_used != incoming.gas_used
        || existing.cumulative_gas_used != incoming.cumulative_gas_used
        || existing.logs_bloom != incoming.logs_bloom
    {
        bail!(
            "raw receipt identity mismatch for chain {} block {} index {}",
            existing.chain_id,
            existing.block_hash,
            existing.transaction_index
        );
    }

    Ok(())
}

fn ensure_raw_log_identity_matches(existing: &RawLog, incoming: &RawLog) -> Result<()> {
    if existing.transaction_hash != incoming.transaction_hash
        || existing.block_number != incoming.block_number
        || existing.transaction_index != incoming.transaction_index
        || existing.emitting_address != incoming.emitting_address
        || existing.topics != incoming.topics
        || existing.data != incoming.data
    {
        bail!(
            "raw log identity mismatch for chain {} block {} log {}",
            existing.chain_id,
            existing.block_hash,
            existing.log_index
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

fn decode_raw_transaction(row: PgRow) -> Result<RawTransaction> {
    Ok(RawTransaction {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        transaction_index: row
            .try_get("transaction_index")
            .context("missing transaction_index")?,
        from_address: row
            .try_get("from_address")
            .context("missing from_address")?,
        to_address: row.try_get("to_address").context("missing to_address")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

fn decode_raw_receipt(row: PgRow) -> Result<RawReceipt> {
    Ok(RawReceipt {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        transaction_index: row
            .try_get("transaction_index")
            .context("missing transaction_index")?,
        contract_address: row
            .try_get("contract_address")
            .context("missing contract_address")?,
        status: row.try_get("status").context("missing status")?,
        gas_used: row.try_get("gas_used").context("missing gas_used")?,
        cumulative_gas_used: row
            .try_get("cumulative_gas_used")
            .context("missing cumulative_gas_used")?,
        logs_bloom: row.try_get("logs_bloom").context("missing logs_bloom")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

fn decode_raw_log(row: PgRow) -> Result<RawLog> {
    Ok(RawLog {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        transaction_index: row
            .try_get("transaction_index")
            .context("missing transaction_index")?,
        log_index: row.try_get("log_index").context("missing log_index")?,
        emitting_address: row
            .try_get("emitting_address")
            .context("missing emitting_address")?,
        topics: row.try_get("topics").context("missing topics")?,
        data: row.try_get("data").context("missing data")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

#[cfg(test)]
mod tests;
