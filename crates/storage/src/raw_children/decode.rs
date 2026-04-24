use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::{RawLog, RawReceipt, RawTransaction};
use crate::CanonicalityState;

pub(super) fn decode_raw_transaction(row: PgRow) -> Result<RawTransaction> {
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

pub(super) fn decode_raw_receipt(row: PgRow) -> Result<RawReceipt> {
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

pub(super) fn decode_raw_log(row: PgRow) -> Result<RawLog> {
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
