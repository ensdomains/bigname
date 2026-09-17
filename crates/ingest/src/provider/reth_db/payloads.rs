use super::{
    RethDbReader,
    convert::{address_hex, hash_hex, parse_b256, provider_block_from_header, u64_to_i64},
};
use crate::provider::{
    Log, Receipt, ResolvedBlock, SelectedPayloads, Transaction, TransactionPayload,
};
use alloy_consensus::transaction::SignerRecoverable as _;
use alloy_consensus::{Transaction as _, TxReceipt as _};
use anyhow::{Context, Result, bail};
use reth_ethereum::provider::{
    BlockBodyIndicesProvider, HeaderProvider, ReceiptProvider, TransactionsProvider,
};
use std::collections::BTreeMap;

impl RethDbReader {
    /// Use point transaction reads only in the measured sparse range. Denser
    /// blocks retain bundle reads; this conservative bound is not a claim about
    /// the sparse/dense crossover point.
    pub(super) fn transaction_payloads(
        &self,
        blocks: &[ResolvedBlock],
        logs: &[Log],
    ) -> Result<SelectedPayloads> {
        let factory = self.factory()?;
        let mut selected = BTreeMap::<&str, BTreeMap<usize, &str>>::new();
        for log in logs {
            let indices = selected.entry(&log.block_hash).or_default();
            let index = usize::try_from(log.transaction_index)?;
            if let Some(previous) = indices.insert(index, &log.transaction_hash)
                && previous != log.transaction_hash
            {
                bail!("selected transaction index has conflicting hashes");
            }
        }
        let mut body_indices = BTreeMap::new();
        let mut bundle_blocks = Vec::new();
        for expected in blocks {
            let Some(wanted) = selected.get(expected.hash.as_str()) else {
                continue;
            };
            let number =
                u64::try_from(expected.number).context("negative selected block number")?;
            // Block position supplies the ID directly: no transaction-hash index is needed.
            // (upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L2221 @ reth@88505c7f)
            let indices = factory
                .block_body_indices(number)?
                .context("missing body indices")?;
            if wanted.keys().any(|index| *index as u64 >= indices.tx_count) {
                bail!("selected transaction outside block body");
            }
            if wanted.len() as u64 > indices.tx_count / 32 {
                bundle_blocks.push(expected.clone());
                continue;
            }
            body_indices.insert(expected.number, indices);
        }
        let mut output = Vec::new();
        for expected in blocks {
            let Some(wanted) = selected.remove(expected.hash.as_str()) else {
                continue;
            };
            let Some(indices) = body_indices.get(&expected.number) else {
                continue;
            };
            let hash = parse_b256(&expected.hash, "selected block")?;
            let header = factory
                .sealed_header_by_hash(hash)?
                .context("missing selected header")?;
            if header.hash() != hash || header.number != u64::try_from(expected.number)? {
                bail!("selected header height mismatch");
            }
            let block = provider_block_from_header(hash, header.header())?;
            let receipts = factory
                .receipts_by_block(hash.into())?
                .context("missing receipts")?;
            if receipts.len() as u64 != indices.tx_count {
                bail!("pruned or incomplete receipts");
            }
            let mut cumulative = 0;
            let mut log_offset = 0u64;
            for (index, receipt) in receipts.iter().enumerate() {
                let gas = receipt
                    .cumulative_gas_used
                    .checked_sub(cumulative)
                    .context("cumulative gas regressed")?;
                cumulative = receipt.cumulative_gas_used;
                if let Some(expected_tx_hash) = wanted.get(&index) {
                    let id = indices
                        .first_tx_num
                        .checked_add(index as u64)
                        .context("transaction ID overflow")?;
                    // ID reads retrieve signed transactions from static files.
                    // (upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L1995 @ reth@88505c7f)
                    let tx = factory
                        .transaction_by_id(id)?
                        .context("missing selected transaction")?;
                    let tx_hash = hash_hex(*tx.tx_hash());
                    if tx_hash != *expected_tx_hash {
                        bail!("selected transaction hash mismatch");
                    }
                    // Stored senders may be absent, so retain the bundle reader's recovery fallback.
                    // (upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L2102 @ reth@88505c7f)
                    // (upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L1903 @ reth@88505c7f)
                    let sender = match factory.transaction_sender(id)? {
                        Some(s) => s,
                        None => tx.recover_signer_unchecked()?,
                    };
                    let status = receipt.status_or_post_state().as_eip658();
                    let transaction_index = u64_to_i64(index as u64, "transaction index")?;
                    let receipt_logs = receipt
                        .logs()
                        .iter()
                        .enumerate()
                        .map(|(offset, log)| -> Result<Log> {
                            Ok(Log {
                                block_hash: block.hash.clone(),
                                block_number: block.number,
                                transaction_hash: tx_hash.clone(),
                                transaction_index,
                                log_index: u64_to_i64(
                                    log_offset
                                        .checked_add(offset as u64)
                                        .context("log index overflow")?,
                                    "log index",
                                )?,
                                address: address_hex(log.address),
                                topics: log.data.topics().iter().map(|t| hash_hex(*t)).collect(),
                                data: log.data.data.to_vec(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    output.push(TransactionPayload {
                        transaction: Transaction {
                            hash: tx_hash.clone(),
                            block_hash: block.hash.clone(),
                            block_number: block.number,
                            index: transaction_index,
                            from: address_hex(sender),
                            to: tx.to().map(address_hex),
                            input: tx.input().to_vec(),
                            value: tx.value().to_string(),
                        },
                        receipt: Receipt {
                            transaction_hash: tx_hash,
                            block_hash: block.hash.clone(),
                            block_number: block.number,
                            transaction_index,
                            contract_address: (tx.is_create() && status != Some(false))
                                .then(|| address_hex(sender.create(tx.nonce()))),
                            status,
                            cumulative_gas_used: Some(cumulative.to_string()),
                            gas_used: Some(gas.to_string()),
                            logs_bloom: Some(receipt.bloom().data().to_vec()),
                        },
                        receipt_logs,
                        receipt_reported_status: true,
                    });
                }
                log_offset = log_offset
                    .checked_add(receipt.logs().len() as u64)
                    .context("log index overflow")?;
            }
        }
        if !selected.is_empty() {
            bail!("selected logs outside resolved window");
        }
        self.revalidate(blocks)?;
        output.sort_by(|a, b| a.transaction.hash.cmp(&b.transaction.hash));
        Ok(SelectedPayloads {
            transactions: output,
            bundle_blocks,
        })
    }
}
