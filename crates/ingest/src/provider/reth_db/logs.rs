use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use alloy_consensus::{BlockHeader as _, TxReceipt as _};
use alloy_rpc_types_eth::{Filter, Topic};
use anyhow::{Context, Result, bail};
use reth_ethereum::provider::{
    BlockBodyIndicesProvider, HeaderProvider, ReceiptProvider, TransactionsProvider,
};
use tracing::info;

use super::{
    RethDbReader,
    convert::{address_hex, hash_hex, i64_to_u64, parse_address, parse_b256},
};
use crate::provider::{Log, ResolvedBlock};

impl RethDbReader {
    pub(super) fn logs(
        &self,
        blocks: &[ResolvedBlock],
        topics: &[String],
        addresses: &[String],
    ) -> Result<Vec<Log>> {
        let started = Instant::now();
        let topics = topics
            .iter()
            .map(|topic| parse_b256(topic, "log topic"))
            .collect::<Result<BTreeSet<_>>>()?;
        let addresses = addresses
            .iter()
            .map(|address| parse_address(address))
            .collect::<Result<BTreeSet<_>>>()?;
        if blocks.is_empty() || topics.is_empty() {
            return Ok(Vec::new());
        }
        let mut filter = Filter::new().event_signature(Topic::from_iter(topics.iter().copied()));
        if !addresses.is_empty() {
            filter = filter.address(addresses.iter().copied().collect::<Vec<_>>());
        }
        let factory = self.factory()?;
        let first = i64_to_u64(blocks[0].number, "block number")?;
        let last = i64_to_u64(
            blocks.last().expect("nonempty blocks").number,
            "block number",
        )?;
        let headers = factory.sealed_headers_range(first..=last)?;
        if headers.len() != blocks.len() {
            bail!("Reth DB omitted headers from a log range");
        }

        let mut output = Vec::new();
        let mut bloom_positive_blocks = 0usize;
        let mut scanned_receipts = 0usize;
        for (expected, header) in blocks.iter().zip(headers.iter()) {
            let header_hash = hash_hex(header.hash());
            if header.number() != i64_to_u64(expected.number, "block number")?
                || header_hash != expected.hash
            {
                bail!("Reth DB log header differs from the resolved block");
            }
            if !filter.matches_bloom(header.header().logs_bloom()) {
                continue;
            }
            bloom_positive_blocks += 1;
            let receipts = factory
                .receipts_by_block(header.hash().into())?
                .with_context(|| format!("Reth DB omitted receipts for {header_hash}"))?;
            scanned_receipts += receipts.len();
            let indices = factory
                .block_body_indices(header.number())?
                .with_context(|| {
                    format!("Reth DB omitted block body indices for {}", header.number())
                })?;
            // The planning floor can be stale: the pruner deletes static files before it
            // commits the transaction whose id makes a read-only provider re-read its index
            // (upstream: .refs/reth/crates/storage/provider/src/providers/database/mod.rs:L279 @ reth@88505c7f)
            // (upstream: .refs/reth/crates/prune/prune/src/pruner.rs:L363 @ reth@88505c7f).
            // A block missing its receipts reads as an empty list, so compare against the
            // body indices, which pruning receipts leaves in place.
            if receipts.len() as u64 != indices.tx_count {
                bail!(
                    "Reth DB returned {} receipts for block {} holding {} transactions; its \
                     receipts are pruned or incomplete",
                    receipts.len(),
                    expected.number,
                    indices.tx_count
                );
            }
            let transaction_hashes =
                transaction_hashes(&factory, indices.first_tx_num, receipts.len(), &header_hash)?;
            let mut next_log_index = 0usize;
            for (transaction_index, receipt) in receipts.iter().enumerate() {
                for log in receipt.logs() {
                    if filter.matches(log) {
                        output.push(Log {
                            block_hash: header_hash.clone(),
                            block_number: expected.number,
                            transaction_hash: transaction_hashes[transaction_index].clone(),
                            transaction_index: i64::try_from(transaction_index)
                                .context("transaction index does not fit in i64")?,
                            log_index: i64::try_from(next_log_index)
                                .context("log index does not fit in i64")?,
                            address: address_hex(log.address),
                            topics: log
                                .data
                                .topics()
                                .iter()
                                .map(|topic| hash_hex(*topic))
                                .collect(),
                            data: log.data.data.to_vec(),
                        });
                    }
                    next_log_index = next_log_index
                        .checked_add(1)
                        .context("Reth DB log index overflow")?;
                }
            }
        }
        self.revalidate(blocks)?;
        info!(
            component = "ingest_reth_provider",
            from_block = blocks.first().map(|block| block.number),
            to_block = blocks.last().map(|block| block.number),
            bloom_positive_blocks,
            scanned_receipts,
            selected_logs = output.len(),
            elapsed_ms = started.elapsed().as_millis(),
            "Reth DB log lookup completed"
        );
        Ok(output)
    }
}

fn transaction_hashes(
    factory: &super::EthereumRethProviderFactory,
    first_transaction_id: u64,
    count: usize,
    block_hash: &str,
) -> Result<Vec<String>> {
    let mut hashes = BTreeMap::new();
    for index in 0..count {
        let id = first_transaction_id
            .checked_add(u64::try_from(index).context("transaction index exceeds u64")?)
            .context("Reth DB transaction id overflow")?;
        let transaction = factory
            .transaction_by_id(id)?
            .with_context(|| format!("Reth DB omitted transaction {id} in {block_hash}"))?;
        hashes.insert(index, hash_hex(*transaction.tx_hash()));
    }
    Ok(hashes.into_values().collect())
}
