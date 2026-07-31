use std::collections::BTreeMap;

use sqlx::{Postgres, Row, Transaction};

use crate::{
    IngestError, Result,
    provider::{Block, Log, Receipt, Transaction as RawTransaction},
};

pub async fn verify_lineage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    expected: &[Block],
) -> Result<()> {
    if expected.is_empty() {
        return Ok(());
    }
    let hashes = expected
        .iter()
        .map(|block| block.hash.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "
        SELECT block_hash,
               parent_hash,
               block_number,
               extract(epoch FROM block_timestamp)::bigint AS timestamp,
               logs_bloom,
               transactions_root,
               receipts_root,
               state_root
        FROM chain_lineage
        LEFT JOIN chain_header_audit USING (chain_id, block_hash)
        WHERE chain_id = $1
          AND block_hash = ANY($2)
        ",
    )
    .bind(chain_id)
    .bind(&hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| IngestError::database("failed to verify stored chain lineage", error))?;
    let stored = rows
        .into_iter()
        .map(|row| {
            let hash: String = row.get("block_hash");
            (
                hash.clone(),
                Block {
                    hash,
                    parent_hash: row.get("parent_hash"),
                    number: row.get("block_number"),
                    timestamp_unix_secs: row.get("timestamp"),
                    logs_bloom: row.get("logs_bloom"),
                    transactions_root: row.get("transactions_root"),
                    receipts_root: row.get("receipts_root"),
                    state_root: row.get("state_root"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    verify_map(
        "chain lineage",
        expected.iter().map(|block| (block.hash.clone(), block)),
        &stored,
    )
}

pub async fn verify_transactions(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    expected: &[RawTransaction],
) -> Result<()> {
    if expected.is_empty() {
        return Ok(());
    }
    let hashes = block_hashes(expected.iter().map(|fact| fact.block_hash.as_str()));
    let rows = sqlx::query(
        "
        SELECT block_hash,
               block_number,
               transaction_hash,
               transaction_index,
               from_address,
               to_address,
               input,
               value::text AS value
        FROM raw_transactions
        WHERE chain_id = $1
          AND block_hash = ANY($2)
        ",
    )
    .bind(chain_id)
    .bind(&hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| IngestError::database("failed to verify raw transactions", error))?;
    let stored = rows
        .into_iter()
        .map(|row| {
            let block_hash: String = row.get("block_hash");
            let hash: String = row.get("transaction_hash");
            (
                (block_hash.clone(), hash.clone()),
                RawTransaction {
                    hash,
                    block_hash,
                    block_number: row.get("block_number"),
                    index: row.get("transaction_index"),
                    from: row.get("from_address"),
                    to: row.get("to_address"),
                    input: row.get("input"),
                    value: row.get("value"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    verify_map(
        "raw transaction",
        expected
            .iter()
            .map(|fact| ((fact.block_hash.clone(), fact.hash.clone()), fact)),
        &stored,
    )
}

pub async fn verify_receipts(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    expected: &[Receipt],
) -> Result<()> {
    if expected.is_empty() {
        return Ok(());
    }
    let hashes = block_hashes(expected.iter().map(|fact| fact.block_hash.as_str()));
    let rows = sqlx::query(
        "
        SELECT block_hash,
               block_number,
               transaction_hash,
               transaction_index,
               contract_address,
               status,
               gas_used::text AS gas_used,
               cumulative_gas_used::text AS cumulative_gas_used,
               logs_bloom
        FROM raw_receipts
        WHERE chain_id = $1
          AND block_hash = ANY($2)
        ",
    )
    .bind(chain_id)
    .bind(&hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| IngestError::database("failed to verify raw receipts", error))?;
    let stored = rows
        .into_iter()
        .map(|row| {
            let block_hash: String = row.get("block_hash");
            let transaction_hash: String = row.get("transaction_hash");
            (
                (block_hash.clone(), transaction_hash.clone()),
                Receipt {
                    transaction_hash,
                    block_hash,
                    block_number: row.get("block_number"),
                    transaction_index: row.get("transaction_index"),
                    contract_address: row.get("contract_address"),
                    status: row.get("status"),
                    gas_used: row.get("gas_used"),
                    cumulative_gas_used: row.get("cumulative_gas_used"),
                    logs_bloom: row.get("logs_bloom"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    verify_map(
        "raw receipt",
        expected.iter().map(|fact| {
            (
                (fact.block_hash.clone(), fact.transaction_hash.clone()),
                fact,
            )
        }),
        &stored,
    )
}

pub async fn verify_logs(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    expected: &[Log],
) -> Result<()> {
    if expected.is_empty() {
        return Ok(());
    }
    let hashes = block_hashes(expected.iter().map(|fact| fact.block_hash.as_str()));
    let rows = sqlx::query(
        "
        SELECT block_hash,
               block_number,
               transaction_hash,
               transaction_index,
               log_index,
               emitting_address,
               topics,
               data
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2)
        ",
    )
    .bind(chain_id)
    .bind(&hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| IngestError::database("failed to verify raw logs", error))?;
    let stored = rows
        .into_iter()
        .map(|row| {
            let block_hash: String = row.get("block_hash");
            let log_index: i64 = row.get("log_index");
            (
                (block_hash.clone(), log_index),
                Log {
                    block_hash,
                    block_number: row.get("block_number"),
                    transaction_hash: row.get("transaction_hash"),
                    transaction_index: row.get("transaction_index"),
                    log_index,
                    address: row.get("emitting_address"),
                    topics: row.get("topics"),
                    data: row.get("data"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    verify_map(
        "raw log",
        expected
            .iter()
            .map(|fact| ((fact.block_hash.clone(), fact.log_index), fact)),
        &stored,
    )
}

fn block_hashes<'a>(values: impl Iterator<Item = &'a str>) -> Vec<String> {
    values
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn verify_map<'a, K, V, I>(label: &str, expected: I, stored: &BTreeMap<K, V>) -> Result<()>
where
    K: Ord + std::fmt::Debug + 'a,
    V: Eq + std::fmt::Debug + 'a,
    I: IntoIterator<Item = (K, &'a V)>,
{
    for (key, expected) in expected {
        match stored.get(&key) {
            Some(actual) if actual == expected => {}
            Some(_) => {
                return Err(IngestError::data_integrity(format!(
                    "immutable {label} {key:?} differs from the fetched fact"
                )));
            }
            None => {
                return Err(IngestError::data_integrity(format!(
                    "immutable {label} {key:?} was not stored"
                )));
            }
        }
    }
    Ok(())
}
