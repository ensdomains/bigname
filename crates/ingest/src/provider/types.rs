use alloy_rpc_types_eth::BlockNumberOrTag;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use super::decode::{address_hex_from_str, hash_hex_from_str};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockBundle {
    pub block: Block,
    pub transactions: Vec<Transaction>,
    pub logs: Vec<Log>,
    pub receipts: Vec<Receipt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedBlock {
    pub number: i64,
    pub hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transaction {
    pub hash: String,
    pub block_hash: String,
    pub block_number: i64,
    pub index: i64,
    pub from: String,
    pub to: Option<String>,
    pub input: Vec<u8>,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Receipt {
    pub transaction_hash: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
    pub contract_address: Option<String>,
    pub status: Option<bool>,
    pub cumulative_gas_used: Option<String>,
    pub gas_used: Option<String>,
    pub logs_bloom: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Log {
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub address: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadSnapshot {
    pub latest: Block,
    pub safe: Option<Block>,
    pub finalized: Option<Block>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Block {
    pub hash: String,
    pub parent_hash: Option<String>,
    pub number: i64,
    pub timestamp_unix_secs: i64,
    pub logs_bloom: Option<Vec<u8>>,
    pub transactions_root: Option<String>,
    pub receipts_root: Option<String>,
    pub state_root: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BlockTag {
    Latest,
    Safe,
    Finalized,
}

impl BlockTag {
    pub(super) fn json_rpc_parameter(self) -> Result<Value> {
        let tag = match self {
            Self::Latest => BlockNumberOrTag::Latest,
            Self::Safe => BlockNumberOrTag::Safe,
            Self::Finalized => BlockNumberOrTag::Finalized,
        };
        serde_json::to_value(tag).context("failed to encode provider block tag")
    }
}

pub(super) fn block_number_parameter(number: i64) -> Result<Value> {
    if number < 0 {
        bail!("provider block number cannot be negative: {number}");
    }
    serde_json::to_value(BlockNumberOrTag::Number(number as u64))
        .context("failed to encode provider block number")
}

pub(super) fn range_log_filter(
    from: i64,
    to: i64,
    addresses: &[String],
    topics: &[String],
) -> Result<Value> {
    let addresses = addresses
        .iter()
        .map(|address| address_hex_from_str(address))
        .collect::<Result<Vec<_>>>()?;
    let topics = topics
        .iter()
        .map(|topic| hash_hex_from_str(topic, "provider log topic"))
        .collect::<Result<Vec<_>>>()?;
    let mut filter = json!({
        "fromBlock": block_number_parameter(from)?,
        "toBlock": block_number_parameter(to)?,
    });
    if !addresses.is_empty() {
        filter["address"] = json!(addresses);
    }
    if !topics.is_empty() {
        filter["topics"] = json!([topics]);
    }
    Ok(filter)
}

pub(super) fn hash_log_filter(block_hash: &str) -> Result<Value> {
    Ok(json!({
        "blockHash": hash_hex_from_str(block_hash, "provider log block hash")?
    }))
}
