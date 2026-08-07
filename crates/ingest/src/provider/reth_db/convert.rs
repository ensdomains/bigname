use std::{collections::BTreeSet, str::FromStr};

use alloy_consensus::{Transaction as _, TxReceipt};
use alloy_primitives::{Address, B256};
use anyhow::{Context, Result, bail};

use crate::provider::{Block, Log, Receipt, ResolvedBlock, Transaction};

pub(super) fn provider_block_from_header(
    block_hash: B256,
    header: &impl reth_ethereum::primitives::BlockHeader,
) -> Result<Block> {
    Ok(Block {
        hash: hash_hex(block_hash),
        parent_hash: (header.parent_hash() != B256::ZERO).then(|| hash_hex(header.parent_hash())),
        number: u64_to_i64(header.number(), "block number")?,
        timestamp_unix_secs: u64_to_i64(header.timestamp(), "block timestamp")?,
        logs_bloom: Some(header.logs_bloom().data().to_vec()),
        transactions_root: Some(hash_hex(header.transactions_root())),
        receipts_root: Some(hash_hex(header.receipts_root())),
        state_root: Some(hash_hex(header.state_root())),
    })
}

pub(super) fn provider_transactions_from_recovered(
    recovered: &reth_ethereum::primitives::RecoveredBlock<reth_ethereum::Block>,
    block: &Block,
) -> Result<Vec<Transaction>> {
    recovered
        .transactions_with_sender()
        .enumerate()
        .map(|(index, (sender, transaction))| {
            Ok(Transaction {
                hash: hash_hex(*transaction.tx_hash()),
                block_hash: block.hash.clone(),
                block_number: block.number,
                index: usize_to_i64(index, "transaction index")?,
                from: address_hex(*sender),
                to: transaction.to().map(address_hex),
                input: transaction.input().to_vec(),
                value: transaction.value().to_string(),
            })
        })
        .collect()
}

pub(super) fn provider_receipts_and_logs_from_recovered(
    receipts: &[reth_ethereum::Receipt],
    recovered: &reth_ethereum::primitives::RecoveredBlock<reth_ethereum::Block>,
    block: &Block,
) -> Result<(Vec<Receipt>, Vec<Log>)> {
    let transactions = recovered.transactions_with_sender().collect::<Vec<_>>();
    if receipts.len() != transactions.len() {
        bail!("Reth DB receipt and transaction counts differ");
    }
    let mut output_receipts = Vec::with_capacity(receipts.len());
    let mut output_logs = Vec::new();
    let mut previous_cumulative_gas = 0u64;
    let mut next_log_index = 0usize;
    for (index, (receipt, (sender, transaction))) in receipts.iter().zip(transactions).enumerate() {
        let cumulative_gas = receipt.cumulative_gas_used();
        let gas_used = cumulative_gas
            .checked_sub(previous_cumulative_gas)
            .context("Reth DB receipt cumulative gas regressed")?;
        previous_cumulative_gas = cumulative_gas;
        let status = receipt.status_or_post_state().as_eip658();
        let contract_address = if transaction.is_create() && status != Some(false) {
            Some(address_hex(sender.create(transaction.nonce())))
        } else {
            None
        };
        output_receipts.push(Receipt {
            transaction_hash: hash_hex(*transaction.tx_hash()),
            block_hash: block.hash.clone(),
            block_number: block.number,
            transaction_index: usize_to_i64(index, "transaction index")?,
            contract_address,
            status,
            cumulative_gas_used: Some(cumulative_gas.to_string()),
            gas_used: Some(gas_used.to_string()),
            logs_bloom: Some(receipt.bloom().data().to_vec()),
        });
        for log in receipt.logs() {
            output_logs.push(Log {
                block_hash: block.hash.clone(),
                block_number: block.number,
                transaction_hash: hash_hex(*transaction.tx_hash()),
                transaction_index: usize_to_i64(index, "transaction index")?,
                log_index: usize_to_i64(next_log_index, "log index")?,
                address: address_hex(log.address),
                topics: log
                    .data
                    .topics()
                    .iter()
                    .map(|topic| hash_hex(*topic))
                    .collect(),
                data: log.data.data.to_vec(),
            });
            next_log_index = next_log_index
                .checked_add(1)
                .context("Reth DB log index overflow")?;
        }
    }
    Ok((output_receipts, output_logs))
}

pub(super) fn normalized_resolved_blocks(blocks: &[ResolvedBlock]) -> Result<Vec<ResolvedBlock>> {
    let mut seen = BTreeSet::new();
    blocks
        .iter()
        .map(|block| {
            i64_to_u64(block.number, "block number")?;
            if !seen.insert(block.number) {
                bail!("provider requested duplicate block {}", block.number);
            }
            Ok(ResolvedBlock {
                number: block.number,
                hash: hash_hex(parse_b256(&block.hash, "block hash")?),
            })
        })
        .collect()
}

pub(super) fn normalized_contiguous_resolved_blocks(
    blocks: &[ResolvedBlock],
) -> Result<Vec<ResolvedBlock>> {
    let blocks = normalized_resolved_blocks(blocks)?;
    for pair in blocks.windows(2) {
        if pair[1].number != pair[0].number + 1 {
            bail!("provider log range is not contiguous");
        }
    }
    Ok(blocks)
}

pub(super) fn parse_b256(value: &str, label: &str) -> Result<B256> {
    B256::from_str(value).with_context(|| format!("failed to parse {label} {value}"))
}

pub(super) fn parse_address(value: &str) -> Result<Address> {
    Address::from_str(value).with_context(|| format!("failed to parse address {value}"))
}

pub(super) fn hash_hex(value: B256) -> String {
    format!("{value}")
}

pub(super) fn address_hex(value: Address) -> String {
    format!("{value:#x}")
}

pub(super) fn i64_to_u64(value: i64, label: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{label} cannot be negative: {value}"))
}

pub(super) fn u64_to_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} {value} does not fit in i64"))
}

fn usize_to_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} {value} does not fit in i64"))
}
