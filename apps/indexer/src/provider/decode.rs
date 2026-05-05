use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result, bail};
use serde_json::Value;

use super::{
    ProviderBlock, ProviderBlockBundle, ProviderLog, ProviderReceipt, ProviderTransaction,
    ZERO_HASH,
};

impl ProviderBlock {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        let block_hash = block_hash_from_value(&value)?;
        let object = value
            .as_object()
            .context("expected block object in JSON-RPC result")?;
        let parent_hash = normalize_parent_hash(
            object
                .get("parentHash")
                .and_then(Value::as_str)
                .context("missing parent hash in JSON-RPC result")?,
        );
        let block_number = parse_hex_i64(
            object
                .get("number")
                .and_then(Value::as_str)
                .context("missing block number in JSON-RPC result")?,
        )?;
        let block_timestamp_unix_secs = parse_hex_i64(
            object
                .get("timestamp")
                .and_then(Value::as_str)
                .context("missing block timestamp in JSON-RPC result")?,
        )?;

        Ok(Self {
            block_hash,
            parent_hash,
            block_number,
            block_timestamp_unix_secs,
            logs_bloom: object
                .get("logsBloom")
                .and_then(Value::as_str)
                .map(parse_hex_bytes)
                .transpose()?,
            transactions_root: object
                .get("transactionsRoot")
                .and_then(Value::as_str)
                .map(normalize_hash),
            receipts_root: object
                .get("receiptsRoot")
                .and_then(Value::as_str)
                .map(normalize_hash),
            state_root: object
                .get("stateRoot")
                .and_then(Value::as_str)
                .map(normalize_hash),
        })
    }
}

impl ProviderBlockBundle {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        let block = ProviderBlock::from_value(value.clone())?;
        let object = value
            .as_object()
            .context("expected block object in JSON-RPC result")?;
        let transactions = object
            .get("transactions")
            .and_then(Value::as_array)
            .context("missing transactions in JSON-RPC result")?
            .iter()
            .map(ProviderTransaction::from_value)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            block,
            transactions,
            logs: Vec::new(),
            receipts: Vec::new(),
            raw_payloads: Vec::new(),
        })
    }
}

impl ProviderTransaction {
    pub(super) fn from_value(value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .context("expected transaction object in JSON-RPC result")?;
        let transaction_hash = object
            .get("hash")
            .and_then(Value::as_str)
            .context("missing transaction hash in JSON-RPC result")?;
        let block_hash = object
            .get("blockHash")
            .and_then(Value::as_str)
            .context("missing transaction block hash in JSON-RPC result")?;
        let block_number = parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing transaction block number in JSON-RPC result")?,
        )?;
        let transaction_index = parse_hex_i64(
            object
                .get("transactionIndex")
                .and_then(Value::as_str)
                .context("missing transaction index in JSON-RPC result")?,
        )?;
        let from = object
            .get("from")
            .and_then(Value::as_str)
            .context("missing transaction from address in JSON-RPC result")?;

        Ok(Self {
            transaction_hash: normalize_hash(transaction_hash),
            block_hash: normalize_hash(block_hash),
            block_number,
            transaction_index,
            from: normalize_address(from),
            to: object
                .get("to")
                .and_then(Value::as_str)
                .map(normalize_address),
        })
    }
}

impl ProviderReceipt {
    pub(super) fn from_value(value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .context("expected receipt object in JSON-RPC result")?;
        let transaction_hash = object
            .get("transactionHash")
            .and_then(Value::as_str)
            .context("missing receipt transaction hash in JSON-RPC result")?;
        let block_hash = object
            .get("blockHash")
            .and_then(Value::as_str)
            .context("missing receipt block hash in JSON-RPC result")?;
        let block_number = parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing receipt block number in JSON-RPC result")?,
        )?;
        let transaction_index = parse_hex_i64(
            object
                .get("transactionIndex")
                .and_then(Value::as_str)
                .context("missing receipt transaction index in JSON-RPC result")?,
        )?;

        Ok(Self {
            transaction_hash: normalize_hash(transaction_hash),
            block_hash: normalize_hash(block_hash),
            block_number,
            transaction_index,
            contract_address: object
                .get("contractAddress")
                .and_then(Value::as_str)
                .map(normalize_address),
            status: object
                .get("status")
                .and_then(Value::as_str)
                .map(parse_hex_i64)
                .transpose()?,
            cumulative_gas_used: object
                .get("cumulativeGasUsed")
                .and_then(Value::as_str)
                .map(parse_hex_i64)
                .transpose()?,
            gas_used: object
                .get("gasUsed")
                .and_then(Value::as_str)
                .map(parse_hex_i64)
                .transpose()?,
            logs_bloom: object
                .get("logsBloom")
                .and_then(Value::as_str)
                .map(parse_hex_bytes)
                .transpose()?,
        })
    }
}

impl ProviderLog {
    pub(super) fn from_value(
        value: &Value,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<Self> {
        let object = value
            .as_object()
            .context("expected log object in JSON-RPC result")?;
        let log_block_hash = object
            .get("blockHash")
            .and_then(Value::as_str)
            .context("missing log block hash in JSON-RPC result")?;
        let block_number = parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing log block number in JSON-RPC result")?,
        )?;
        let transaction_hash = object
            .get("transactionHash")
            .and_then(Value::as_str)
            .context("missing log transaction hash in JSON-RPC result")?;
        let transaction_index = parse_hex_i64(
            object
                .get("transactionIndex")
                .and_then(Value::as_str)
                .context("missing log transaction index in JSON-RPC result")?,
        )?;
        let log_index = parse_hex_i64(
            object
                .get("logIndex")
                .and_then(Value::as_str)
                .context("missing log index in JSON-RPC result")?,
        )?;
        let address = object
            .get("address")
            .and_then(Value::as_str)
            .context("missing log address in JSON-RPC result")?;
        let topics = object
            .get("topics")
            .and_then(Value::as_array)
            .context("missing log topics in JSON-RPC result")?
            .iter()
            .map(|topic| {
                topic
                    .as_str()
                    .context("expected log topic string in JSON-RPC result")
                    .map(normalize_hash)
            })
            .collect::<Result<Vec<_>>>()?;
        let data = object
            .get("data")
            .and_then(Value::as_str)
            .context("missing log data in JSON-RPC result")?;

        if normalize_hash(log_block_hash) != block_hash {
            bail!(
                "provider returned log {} for block {} with mismatched block hash {}",
                log_index,
                block_hash,
                normalize_hash(log_block_hash)
            );
        }
        if block_number != expected_block_number {
            bail!(
                "provider returned log {} for block {} with mismatched block number {}",
                log_index,
                block_hash,
                block_number
            );
        }

        Ok(Self {
            block_hash: normalize_hash(log_block_hash),
            block_number,
            transaction_hash: normalize_hash(transaction_hash),
            transaction_index,
            log_index,
            address: normalize_address(address),
            topics,
            data: data.to_owned(),
        })
    }

    pub(super) fn block_number_from_value(value: &Value) -> Result<i64> {
        let object = value
            .as_object()
            .context("expected log object in JSON-RPC result")?;

        parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing log block number in JSON-RPC result")?,
        )
    }
}

pub(super) fn block_hash_from_value(value: &Value) -> Result<String> {
    let object = value
        .as_object()
        .context("expected block object in JSON-RPC result")?;
    let block_hash = object
        .get("hash")
        .and_then(Value::as_str)
        .context("missing block hash in JSON-RPC result")?;

    Ok(normalize_hash(block_hash))
}

pub(super) fn parse_hex_i64(value: &str) -> Result<i64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    i64::from_str_radix(value, 16).with_context(|| format!("failed to parse hex integer {value}"))
}

pub(super) fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        bail!("invalid hex byte string with odd length");
    }

    hex::decode(value).with_context(|| format!("failed to parse hex bytes {value}"))
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(keccak256(bytes).as_slice())
}

fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub(super) fn normalize_hash(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn normalize_parent_hash(value: &str) -> Option<String> {
    let value = normalize_hash(value);
    if value == ZERO_HASH || value.is_empty() {
        None
    } else {
        Some(value)
    }
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}
