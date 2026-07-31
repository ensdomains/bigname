use alloy_primitives::{Address, B256};
use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::provider::Log;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseLogRow {
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub address: String,
    pub topics: Vec<String>,
    pub decoded: bool,
}

impl CoinbaseLogRow {
    pub fn from_value(value: Value) -> Result<Self> {
        let object = value
            .as_object()
            .context("Coinbase SQL result row must be an object")?;
        let topics = match object.get("topics") {
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .context("Coinbase SQL topic must be a string")
                        .and_then(normalize_hash)
                })
                .collect::<Result<Vec<_>>>()?,
            Some(Value::String(value)) => serde_json::from_str::<Vec<String>>(value)
                .context("failed to parse Coinbase SQL topics")?
                .into_iter()
                .map(|topic| normalize_hash(&topic))
                .collect::<Result<Vec<_>>>()?,
            _ => bail!("Coinbase SQL row is missing topics"),
        };
        let event_signature = optional_string(object, "event_signature")?;
        Ok(Self {
            block_number: required_i64(object, "block_number")?,
            block_hash: normalize_hash(&required_string(object, "block_hash")?)?,
            transaction_hash: normalize_hash(&required_string(object, "transaction_hash")?)?,
            transaction_index: required_i64(object, "transaction_index")?,
            log_index: required_i64(object, "log_index")?,
            address: normalize_address(&required_string(object, "emitting_address")?)?,
            topics,
            decoded: event_signature.is_some(),
        })
    }

    pub fn identity_log(&self) -> Log {
        Log {
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            transaction_hash: self.transaction_hash.clone(),
            transaction_index: self.transaction_index,
            log_index: self.log_index,
            address: self.address.clone(),
            topics: self.topics.clone(),
            data: Vec::new(),
        }
    }

    pub fn validate(
        &self,
        from: i64,
        to: i64,
        addresses: &[String],
        topics: &[String],
    ) -> Result<()> {
        if !(from..=to).contains(&self.block_number)
            || self.transaction_index < 0
            || self.log_index < 0
        {
            bail!("Coinbase SQL row is outside the requested block window");
        }
        if !addresses
            .iter()
            .any(|address| address.eq_ignore_ascii_case(&self.address))
        {
            bail!("Coinbase SQL row has an unrequested emitting address");
        }
        if !self.topics.first().is_some_and(|topic| {
            topics
                .iter()
                .any(|expected| expected.eq_ignore_ascii_case(topic))
        }) {
            bail!("Coinbase SQL row has an unrequested topic0");
        }
        Ok(())
    }
}

fn required_string(object: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
    optional_string(object, field)?.with_context(|| format!("missing Coinbase SQL field {field}"))
}

fn optional_string(object: &serde_json::Map<String, Value>, field: &str) -> Result<Option<String>> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Number(value)) => Ok(Some(value.to_string())),
        Some(value) => bail!("Coinbase SQL field {field} must be string-like, got {value}"),
    }
}

fn required_i64(object: &serde_json::Map<String, Value>, field: &str) -> Result<i64> {
    match object.get(field) {
        Some(Value::Number(value)) => value
            .as_i64()
            .with_context(|| format!("Coinbase SQL field {field} exceeds i64")),
        Some(Value::String(value)) => value
            .parse()
            .with_context(|| format!("failed to parse Coinbase SQL field {field}")),
        _ => bail!("missing integer Coinbase SQL field {field}"),
    }
}

fn normalize_hash(value: &str) -> Result<String> {
    value
        .parse::<B256>()
        .with_context(|| format!("invalid Coinbase SQL hash {value}"))
        .map(|value| format!("{value}"))
}

fn normalize_address(value: &str) -> Result<String> {
    value
        .parse::<Address>()
        .with_context(|| format!("invalid Coinbase SQL address {value}"))
        .map(|value| format!("{value:#x}"))
}
