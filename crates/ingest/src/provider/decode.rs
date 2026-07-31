use std::str::FromStr;

use alloy_primitives::{Address, B256, Bytes, U256};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

use super::types::{Block, BlockBundle, Log, Receipt, Transaction};

const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

impl Block {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        Self::from_rpc(decode_value(value, "block")?)
    }

    fn from_rpc(block: RpcBlock) -> Result<Self> {
        Ok(Self {
            hash: normalize_hash(&block.hash),
            parent_hash: normalized_parent(&block.parent_hash),
            number: u256_i64(block.number, "block number")?,
            timestamp_unix_secs: u256_i64(block.timestamp, "block timestamp")?,
            logs_bloom: block.logs_bloom.map(|bytes| bytes.to_vec()),
            transactions_root: block.transactions_root.map(|value| normalize_hash(&value)),
            receipts_root: block.receipts_root.map(|value| normalize_hash(&value)),
            state_root: block.state_root.map(|value| normalize_hash(&value)),
        })
    }
}

impl BlockBundle {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        let mut block = decode_value::<RpcBlock>(value, "block")?;
        let transactions = block
            .transactions
            .take()
            .context("missing transactions in JSON-RPC block result")?
            .into_iter()
            .map(|transaction| match transaction {
                RpcBlockTransaction::Full(transaction) => Transaction::from_rpc(*transaction),
                RpcBlockTransaction::Hash(hash) => {
                    bail!("expected full transaction object, got {hash}")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            block: Block::from_rpc(block)?,
            transactions,
            logs: Vec::new(),
            receipts: Vec::new(),
        })
    }
}

impl Transaction {
    fn from_rpc(transaction: RpcTransaction) -> Result<Self> {
        Ok(Self {
            hash: hash_hex(transaction.hash),
            block_hash: hash_hex(transaction.block_hash),
            block_number: u256_i64(transaction.block_number, "transaction block number")?,
            index: u256_i64(transaction.transaction_index, "transaction index")?,
            from: address_hex(transaction.from),
            to: transaction.to.map(address_hex),
            input: transaction.input.to_vec(),
            value: transaction.value.to_string(),
        })
    }
}

impl Receipt {
    pub(super) fn from_value(value: &Value) -> Result<Self> {
        let receipt = decode_ref::<RpcReceipt>(value, "receipt")?;
        Ok(Self {
            transaction_hash: hash_hex(receipt.transaction_hash),
            block_hash: hash_hex(receipt.block_hash),
            block_number: u256_i64(receipt.block_number, "receipt block number")?,
            transaction_index: u256_i64(receipt.transaction_index, "receipt transaction index")?,
            contract_address: receipt.contract_address.map(address_hex),
            status: receipt.status.map(|status| !status.is_zero()),
            cumulative_gas_used: receipt.cumulative_gas_used.map(|value| value.to_string()),
            gas_used: receipt.gas_used.map(|value| value.to_string()),
            logs_bloom: receipt.logs_bloom.map(|bytes| bytes.to_vec()),
        })
    }
}

impl Log {
    pub(super) fn from_value(
        value: &Value,
        expected_hash: &str,
        expected_number: i64,
    ) -> Result<Self> {
        let log = decode_ref::<RpcLog>(value, "log")?;
        let block_hash = hash_hex(log.block_hash);
        let block_number = u256_i64(log.block_number, "log block number")?;
        if block_hash != expected_hash || block_number != expected_number {
            bail!("provider returned log outside resolved block {expected_number} {expected_hash}");
        }
        Ok(Self {
            block_hash,
            block_number,
            transaction_hash: hash_hex(log.transaction_hash),
            transaction_index: u256_i64(log.transaction_index, "log transaction index")?,
            log_index: u256_i64(log.log_index, "log index")?,
            address: address_hex(log.address),
            topics: log.topics.into_iter().map(hash_hex).collect(),
            data: log.data.to_vec(),
        })
    }

    pub(super) fn block_number(value: &Value) -> Result<i64> {
        u256_i64(
            decode_ref::<RpcLogNumber>(value, "log")?.block_number,
            "log block number",
        )
    }
}

pub(super) fn normalize_hash(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn hash_hex_from_str(value: &str, label: &str) -> Result<String> {
    let value = normalize_hash(value);
    B256::from_str(&value)
        .with_context(|| format!("failed to parse {label} {value}"))
        .map(hash_hex)
}

pub(super) fn address_hex_from_str(value: &str) -> Result<String> {
    Address::from_str(&value.to_ascii_lowercase())
        .with_context(|| format!("failed to parse address {value}"))
        .map(address_hex)
}

pub(super) fn hash_hex(value: B256) -> String {
    format!("{value}")
}

pub(super) fn address_hex(value: Address) -> String {
    format!("{value:#x}")
}

fn normalized_parent(value: &str) -> Option<String> {
    let value = normalize_hash(value);
    (value != ZERO_HASH && !value.is_empty()).then_some(value)
}

fn u256_i64(value: U256, label: &str) -> Result<i64> {
    let value = u64::try_from(value).with_context(|| format!("{label} exceeds u64"))?;
    i64::try_from(value).with_context(|| format!("{label} exceeds i64"))
}

fn decode_value<T: for<'de> Deserialize<'de>>(value: Value, label: &str) -> Result<T> {
    serde_json::from_value(value).with_context(|| format!("failed to decode {label} JSON"))
}

fn decode_ref<T: for<'de> Deserialize<'de>>(value: &Value, label: &str) -> Result<T> {
    decode_value(value.clone(), label)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    hash: String,
    parent_hash: String,
    number: U256,
    timestamp: U256,
    #[serde(default)]
    logs_bloom: Option<Bytes>,
    #[serde(default)]
    transactions_root: Option<String>,
    #[serde(default)]
    receipts_root: Option<String>,
    #[serde(default)]
    state_root: Option<String>,
    #[serde(default)]
    transactions: Option<Vec<RpcBlockTransaction>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RpcBlockTransaction {
    Hash(B256),
    Full(Box<RpcTransaction>),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcTransaction {
    hash: B256,
    block_hash: B256,
    block_number: U256,
    transaction_index: U256,
    from: Address,
    to: Option<Address>,
    #[serde(default)]
    input: Bytes,
    #[serde(default)]
    value: U256,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcReceipt {
    transaction_hash: B256,
    block_hash: B256,
    block_number: U256,
    transaction_index: U256,
    #[serde(default)]
    contract_address: Option<Address>,
    #[serde(default)]
    status: Option<U256>,
    #[serde(default)]
    cumulative_gas_used: Option<U256>,
    #[serde(default)]
    gas_used: Option<U256>,
    #[serde(default)]
    logs_bloom: Option<Bytes>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    block_hash: B256,
    block_number: U256,
    transaction_hash: B256,
    transaction_index: U256,
    log_index: U256,
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLogNumber {
    block_number: U256,
}
