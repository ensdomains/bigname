use std::str::FromStr;

use alloy_primitives::{U256, hex};
use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use sqlx::PgPool;

use super::rpc::RpcClient;

/// Seed only the readable end marker required by the phase-runner's bounded
/// redo operator contract. The ingest phase still obtains every selected
/// block, transaction, receipt, and log through its configured provider.
pub async fn seed_anvil_rpc_redo_extent(
    pool: &PgPool,
    chain_id: &str,
    rpc_url: &str,
    block_number: u64,
) -> Result<()> {
    let rpc = RpcClient::new(rpc_url.to_owned());
    let block = rpc
        .call(
            "eth_getBlockByNumber",
            serde_json::json!([format!("{block_number:#x}"), false]),
        )
        .await?;
    let hash = string_field(&block, "hash")?;
    let parent_hash = (block_number > 0)
        .then(|| string_field(&block, "parentHash"))
        .transpose()?;
    let timestamp = quantity_field(&block, "timestamp")?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')
         ON CONFLICT (chain_id, block_hash) DO UPDATE
         SET canonicality_state = 'canonical'",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(parent_hash)
    .bind(i64::try_from(block_number)?)
    .bind(i64::try_from(timestamp)?)
    .execute(pool)
    .await?;
    for phase in ["ingest", "interpret", "project", "verify", "live"] {
        sqlx::query(
            "INSERT INTO chain_phase_state (chain_id, phase_name)
             VALUES ($1, $2) ON CONFLICT (chain_id, phase_name) DO NOTHING",
        )
        .bind(chain_id)
        .bind(phase)
        .execute(pool)
        .await?;
    }
    let block_number = i64::try_from(block_number)?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = $2,
             current_block_hash = $3, target_block_number = $2,
             target_block_hash = $3, live_handoff_block_number = $2,
             live_handoff_block_hash = $3, started_at = now(),
             finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'
           AND NOT redo_in_progress",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(&hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number, target_block_number,
             last_processed_block_number, last_processed_block_hash
         ) VALUES ($1, 'e2e-rpc', 'rpc', 'new_signature_range',
                   0, $2 + 1, $2, $2, $3)
         ON CONFLICT (chain_id, source_key) DO UPDATE
         SET next_block_number = EXCLUDED.next_block_number,
             target_block_number = EXCLUDED.target_block_number,
             last_processed_block_number = EXCLUDED.last_processed_block_number,
             last_processed_block_hash = EXCLUDED.last_processed_block_hash,
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(&hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record the operator extent required before the first interpret/project
/// redo over facts produced by a real ingest phase.
pub async fn seed_downstream_redo_extents(
    pool: &PgPool,
    chain_id: &str,
    through_block: u64,
) -> Result<()> {
    let through_block = i64::try_from(through_block)?;
    let block_hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(through_block)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = $2,
             current_block_hash = $3, target_block_number = $2,
             target_block_hash = $3, input_content_hash = NULL,
             last_error = NULL, started_at = now(), finished_at = now(),
             updated_at = now()
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain_id)
    .bind(through_block)
    .bind(block_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Materialize an Anvil snapshot as the immutable schema-v2 input corpus.
///
/// Production Ethereum and Base intake deliberately require Reth and the
/// Coinbase/RPC seam respectively. Scenario chains instead supply their
/// already-observed blocks up front, matching the phase-runner production
/// tests: interpretation and projection still execute through the real
/// binary, while the stored facts retain the production chain identity used
/// by manifest and projection policy.
pub async fn seed_anvil_snapshot(
    pool: &PgPool,
    chain_id: &str,
    rpc_url: &str,
    through_block: u64,
) -> Result<()> {
    let rpc = RpcClient::new(rpc_url.to_owned());
    let mut last_hash = None;

    // A replacement branch may displace the block currently named by the
    // published head. Temporarily withdraw that pointer before orphaning the
    // old lineage; the storage trigger correctly forbids a head from naming
    // an orphaned block. The new snapshot publishes its head atomically at
    // the end of this single-writer fixture operation.
    sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;

    for block_number in 0..=through_block {
        let block = rpc
            .call(
                "eth_getBlockByNumber",
                serde_json::json!([format!("{block_number:#x}"), true]),
            )
            .await
            .with_context(|| format!("load fixture block {block_number}"))?;
        if block.is_null() {
            bail!("fixture provider omitted block {block_number}");
        }
        let hash = string_field(&block, "hash")?;
        let reported_number = quantity_field(&block, "number")?;
        anyhow::ensure!(
            reported_number == block_number,
            "fixture provider returned block {reported_number} for request {block_number}"
        );
        let parent_hash = (block_number > 0).then(|| string_field(&block, "parentHash"));
        let parent_hash = parent_hash.transpose()?;
        let timestamp = quantity_field(&block, "timestamp")?;

        // A later snapshot may describe a replacement Anvil branch. Preserve
        // the old facts, but move the displaced readable-height row to
        // `orphaned` before publishing the replacement block at that height.
        sqlx::query(
            "UPDATE chain_lineage
             SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_number = $2 AND block_hash <> $3
               AND canonicality_state IN ('canonical', 'safe', 'finalized')",
        )
        .bind(chain_id)
        .bind(i64::try_from(block_number)?)
        .bind(&hash)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')
             ON CONFLICT (chain_id, block_hash) DO UPDATE
             SET canonicality_state = 'canonical'",
        )
        .bind(chain_id)
        .bind(&hash)
        .bind(parent_hash)
        .bind(i64::try_from(block_number)?)
        .bind(i64::try_from(timestamp)?)
        .execute(pool)
        .await?;

        for transaction in array_field(&block, "transactions")? {
            seed_transaction(pool, chain_id, &hash, block_number, transaction, &rpc).await?;
        }
        last_hash = Some(hash);
    }

    let last_hash = last_hash.context("fixture snapshot contained no blocks")?;
    let through_block = i64::try_from(through_block)?;
    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number > $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(through_block)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (
             chain_id, latest_block_hash, latest_block_number
         ) VALUES ($1, $2, $3)
         ON CONFLICT (chain_id) DO UPDATE
         SET latest_block_hash = EXCLUDED.latest_block_hash,
             latest_block_number = EXCLUDED.latest_block_number,
             safe_block_hash = NULL,
             safe_block_number = NULL,
             finalized_block_hash = NULL,
             finalized_block_number = NULL,
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(&last_hash)
    .bind(through_block)
    .execute(pool)
    .await?;

    initialize_phase_state(pool, chain_id, through_block, &last_hash).await
}

async fn seed_transaction(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: u64,
    transaction: &Value,
    rpc: &RpcClient,
) -> Result<()> {
    let transaction_hash = string_field(transaction, "hash")?;
    let transaction_index = quantity_field(transaction, "transactionIndex")?;
    let from = string_field(transaction, "from")?;
    let to = optional_string_field(transaction, "to")?;
    let input = bytes_field(transaction, "input")?;
    let value = decimal_quantity_field(transaction, "value")?;

    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address, input, value
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::numeric)
         ON CONFLICT (chain_id, block_hash, transaction_hash) DO NOTHING",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(i64::try_from(block_number)?)
    .bind(&transaction_hash)
    .bind(i64::try_from(transaction_index)?)
    .bind(from)
    .bind(to)
    .bind(input)
    .bind(value)
    .execute(pool)
    .await?;

    let receipt = rpc
        .call(
            "eth_getTransactionReceipt",
            serde_json::json!([transaction_hash]),
        )
        .await
        .context("load fixture transaction receipt")?;
    if receipt.is_null() {
        bail!("fixture provider returned no receipt for {transaction_hash}");
    }
    let receipt_block_hash = string_field(&receipt, "blockHash")?;
    let receipt_block_number = quantity_field(&receipt, "blockNumber")?;
    let receipt_transaction_index = quantity_field(&receipt, "transactionIndex")?;
    anyhow::ensure!(
        receipt_block_hash.eq_ignore_ascii_case(block_hash)
            && receipt_block_number == block_number
            && receipt_transaction_index == transaction_index,
        "fixture receipt position disagrees with transaction {transaction_hash}"
    );
    let contract_address = optional_string_field(&receipt, "contractAddress")?;
    let status = optional_quantity_field(&receipt, "status")?.map(|status| status != 0);
    let gas_used = optional_decimal_quantity_field(&receipt, "gasUsed")?;
    let cumulative_gas_used = optional_decimal_quantity_field(&receipt, "cumulativeGasUsed")?;
    let logs_bloom = optional_bytes_field(&receipt, "logsBloom")?;

    sqlx::query(
        "INSERT INTO raw_receipts (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, contract_address, status, gas_used,
             cumulative_gas_used, logs_bloom
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::numeric, $9::numeric, $10)
         ON CONFLICT (chain_id, block_hash, transaction_hash) DO NOTHING",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(i64::try_from(block_number)?)
    .bind(&transaction_hash)
    .bind(i64::try_from(transaction_index)?)
    .bind(contract_address)
    .bind(status)
    .bind(gas_used)
    .bind(cumulative_gas_used)
    .bind(logs_bloom)
    .execute(pool)
    .await?;

    for log in array_field(&receipt, "logs")? {
        let log_index = quantity_field(log, "logIndex")?;
        let emitting_address = string_field(log, "address")?;
        let topics = array_field(log, "topics")?
            .iter()
            .map(|topic| {
                topic
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("fixture log topic is not a string: {topic}"))
            })
            .collect::<Result<Vec<_>>>()?;
        let data = bytes_field(log, "data")?;
        sqlx::query(
            "INSERT INTO raw_logs (
                 chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, log_index, emitting_address, topics, data
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (chain_id, block_hash, log_index) DO NOTHING",
        )
        .bind(chain_id)
        .bind(block_hash)
        .bind(i64::try_from(block_number)?)
        .bind(&transaction_hash)
        .bind(i64::try_from(transaction_index)?)
        .bind(i64::try_from(log_index)?)
        .bind(emitting_address)
        .bind(topics)
        .bind(data)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn initialize_phase_state(
    pool: &PgPool,
    chain_id: &str,
    through_block: i64,
    block_hash: &str,
) -> Result<()> {
    // Sepolia keeps its Anvil endpoint under the production dRPC source identity so a later
    // Ingest redo can read newly selected logs through the real provider path. Other fixture
    // chains retain the synthetic source because their production intake requires local data.
    let (source_kind, seed_basis) = if chain_id == "ethereum-sepolia" {
        ("drpc", "ethereum_head")
    } else {
        ("fixture", "new_signature_range")
    };
    for phase in ["ingest", "interpret", "project", "verify", "live"] {
        sqlx::query(
            "INSERT INTO chain_phase_state (chain_id, phase_name)
             VALUES ($1, $2)
             ON CONFLICT (chain_id, phase_name) DO NOTHING",
        )
        .bind(chain_id)
        .bind(phase)
        .execute(pool)
        .await?;
    }
    // Redo is the phase-runner's operator path for a fixture extent. Seed the
    // recorded range for all three spine phases, then let interpret/project
    // replace their derived output and adopt the binary's exact content hash.
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2,
             current_block_hash = $3,
             target_block_number = $2,
             target_block_hash = $3,
             live_handoff_block_number = CASE
                 WHEN phase_name = 'ingest' THEN $2
             END,
             live_handoff_block_hash = CASE
                 WHEN phase_name = 'ingest' THEN $3
             END,
             input_content_hash = NULL,
             last_error = NULL,
             started_at = now(),
             finished_at = now(),
             updated_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(chain_id)
    .bind(through_block)
    .bind(block_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number, target_block_number,
             last_processed_block_number, last_processed_block_hash
         ) VALUES ($1, 'e2e-fixture', $4, $5, 0, $2 + 1, $2, $2, $3)
         ON CONFLICT (chain_id, source_key) DO UPDATE
         SET source_kind = EXCLUDED.source_kind,
             seed_basis = EXCLUDED.seed_basis,
             next_block_number = EXCLUDED.next_block_number,
             target_block_number = EXCLUDED.target_block_number,
             last_processed_block_number = EXCLUDED.last_processed_block_number,
             last_processed_block_hash = EXCLUDED.last_processed_block_hash,
             updated_at = now()",
    )
    .bind(chain_id)
    .bind(through_block)
    .bind(block_hash)
    .bind(source_kind)
    .bind(seed_basis)
    .execute(pool)
    .await?;
    Ok(())
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value> {
    value
        .get(name)
        .ok_or_else(|| anyhow!("fixture RPC object is missing {name}: {value}"))
}

fn string_field(value: &Value, name: &str) -> Result<String> {
    field(value, name)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("fixture RPC field {name} is not a string"))
}

fn optional_string_field(value: &Value, name: &str) -> Result<Option<String>> {
    match field(value, name)? {
        Value::Null => Ok(None),
        value => value
            .as_str()
            .map(|value| Some(value.to_owned()))
            .ok_or_else(|| anyhow!("fixture RPC field {name} is not a string or null")),
    }
}

fn array_field<'a>(value: &'a Value, name: &str) -> Result<&'a [Value]> {
    field(value, name)?
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| anyhow!("fixture RPC field {name} is not an array"))
}

fn quantity_field(value: &Value, name: &str) -> Result<u64> {
    parse_quantity(field(value, name)?)
        .with_context(|| format!("parse fixture RPC quantity {name}"))
}

fn optional_quantity_field(value: &Value, name: &str) -> Result<Option<u64>> {
    match field(value, name)? {
        Value::Null => Ok(None),
        value => Ok(Some(parse_quantity(value)?)),
    }
}

fn parse_quantity(value: &Value) -> Result<u64> {
    let value = value
        .as_str()
        .ok_or_else(|| anyhow!("expected a hexadecimal quantity, got {value}"))?;
    u64::from_str_radix(value.trim_start_matches("0x"), 16).context("parse hexadecimal quantity")
}

fn decimal_quantity_field(value: &Value, name: &str) -> Result<String> {
    decimal_quantity(field(value, name)?)
}

fn optional_decimal_quantity_field(value: &Value, name: &str) -> Result<Option<String>> {
    match field(value, name)? {
        Value::Null => Ok(None),
        value => Ok(Some(decimal_quantity(value)?)),
    }
}

fn decimal_quantity(value: &Value) -> Result<String> {
    let value = value
        .as_str()
        .ok_or_else(|| anyhow!("expected a hexadecimal quantity, got {value}"))?;
    Ok(U256::from_str(value)?.to_string())
}

fn bytes_field(value: &Value, name: &str) -> Result<Vec<u8>> {
    parse_bytes(field(value, name)?).with_context(|| format!("parse fixture RPC bytes {name}"))
}

fn optional_bytes_field(value: &Value, name: &str) -> Result<Option<Vec<u8>>> {
    match field(value, name)? {
        Value::Null => Ok(None),
        value => Ok(Some(parse_bytes(value)?)),
    }
}

fn parse_bytes(value: &Value) -> Result<Vec<u8>> {
    let value = value
        .as_str()
        .ok_or_else(|| anyhow!("expected hexadecimal bytes, got {value}"))?;
    hex::decode(value).context("decode hexadecimal bytes")
}
