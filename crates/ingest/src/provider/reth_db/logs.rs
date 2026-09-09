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
use crate::measurement::{self as memory, Measured};
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
        let mut native = NativeProgress::default();
        memory::observe("reth_native_query_start", Default::default);
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
        memory::observe("reth_log_headers", || memory::inline(&headers));
        if headers.len() != blocks.len() {
            bail!("Reth DB omitted headers from a log range");
        }

        let mut output = Vec::new();
        let mut output_size = memory::Footprint::default();
        let mut bloom_positive_blocks = 0usize;
        let mut scanned_receipts = 0usize;
        for (block_index, (expected, header)) in blocks.iter().zip(headers.iter()).enumerate() {
            let header_hash = hash_hex(header.hash());
            if header.number() != i64_to_u64(expected.number, "block number")?
                || header_hash != expected.hash
            {
                bail!("Reth DB log header differs from the resolved block");
            }
            native.visited = expected.number;
            if memory::progress_due(block_index) {
                native.emit("reth_native_progress");
            }
            if !filter.matches_bloom(header.header().logs_bloom()) {
                continue;
            }
            bloom_positive_blocks += 1;
            let receipts = factory
                .receipts_by_block(header.hash().into())?
                .with_context(|| format!("Reth DB omitted receipts for {header_hash}"))?;
            scanned_receipts += receipts.len();
            if memory::capture().is_some() {
                let mut f = memory::inline(&receipts);
                f.bytes =
                    memory::Footprint::entries::<reth_ethereum::Receipt>(receipts.len()).bytes;
                for receipt in &receipts {
                    for log in receipt.logs() {
                        f.bytes = memory::sum(
                            f.bytes,
                            memory::sum(
                                std::mem::size_of_val(log) as u64,
                                memory::sum(
                                    log.data.data.len() as u64,
                                    memory::Footprint::entries::<[u8; 32]>(log.data.topics().len())
                                        .bytes,
                                ),
                            ),
                        );
                    }
                }
                native.receipts = f;
                native.hashes = memory::Footprint::default();
                native.block = expected.number;
                native.update(output_size, memory::inline(&output).owned);
            }
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
            let transaction_hashes = transaction_hashes(
                &factory,
                indices.first_tx_num,
                receipts.len(),
                &header_hash,
                &mut native,
            )?;
            if memory::capture().is_some() {
                native.hashes = transaction_hashes.footprint();
                native.update(output_size, memory::inline(&output).owned);
            }
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
                        if memory::capture().is_some() {
                            output_size =
                                output_size.combine(output.last().expect("pushed log").footprint());
                            native.update(output_size, memory::inline(&output).owned);
                        }
                    }
                    next_log_index = next_log_index
                        .checked_add(1)
                        .context("Reth DB log index overflow")?;
                }
            }
        }
        native.emit("reth_native_progress");
        memory::observe("reth_query_output_before_revalidation", || {
            output.footprint()
        });
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
    native: &mut NativeProgress,
) -> Result<Vec<String>> {
    let mut hashes = BTreeMap::new();
    let mut map_size = memory::Footprint::default();
    for index in 0..count {
        let id = first_transaction_id
            .checked_add(u64::try_from(index).context("transaction index exceeds u64")?)
            .context("Reth DB transaction id overflow")?;
        let transaction = factory
            .transaction_by_id(id)?
            .with_context(|| format!("Reth DB omitted transaction {id} in {block_hash}"))?;
        let hash = hash_hex(*transaction.tx_hash());
        if memory::capture().is_some() {
            map_size = map_size
                .combine(hash.footprint())
                .combine(memory::Footprint::entries::<(usize, String)>(1));
            if native
                .hash_map
                .is_none_or(|(_, f)| map_size.owned > f.owned)
            {
                native.hash_map = Some((native.block, map_size));
            }
        }
        hashes.insert(index, hash);
    }
    Ok(hashes.into_values().collect())
}

// Only primitives survive iterations or an early error; receipt payload lifetimes are unchanged.
#[derive(Default)]
struct NativeProgress {
    block: i64,
    visited: i64,
    receipt_block: i64,
    hash_map: Option<(i64, memory::Footprint)>,
    receipts: memory::Footprint,
    hashes: memory::Footprint,
    output: memory::Footprint,
    receipt_peak: memory::Footprint,
    logical: Option<(i64, memory::Footprint, memory::Footprint, memory::Footprint)>,
    capacity: Option<(i64, memory::Footprint, memory::Footprint, memory::Footprint)>,
}
impl NativeProgress {
    fn update(&mut self, output: memory::Footprint, backing: u64) {
        self.output = output;
        self.output.owned = memory::sum(output.owned, backing);
        if self.logical.is_none() || self.receipts.bytes > self.receipt_peak.bytes {
            self.receipt_peak = self.receipts;
            self.receipt_block = self.block;
        }
        let point = (self.block, self.receipts, self.hashes, self.output);
        let total = self.receipts.combine(self.hashes).combine(self.output);
        if self
            .logical
            .is_none_or(|p| total.bytes > p.1.combine(p.2).combine(p.3).bytes)
        {
            self.logical = Some(point);
        }
        if self
            .capacity
            .is_none_or(|p| total.owned > p.1.combine(p.2).combine(p.3).owned)
        {
            self.capacity = Some(point);
        }
    }
    fn summaries(
        &self,
        mut publish: impl FnMut(
            &'static str,
            i64,
            memory::Footprint,
            memory::Footprint,
            memory::Footprint,
        ),
    ) {
        publish(
            "reth_complete_receipt_peak",
            self.receipt_block,
            self.receipt_peak,
            Default::default(),
            Default::default(),
        );
        if let Some((block, map)) = self.hash_map {
            publish(
                "reth_transaction_hash_map",
                block,
                Default::default(),
                map,
                Default::default(),
            );
        }
        for (stage, point) in [
            ("reth_native_logical_peak", self.logical),
            ("reth_native_capacity_peak", self.capacity),
        ] {
            if let Some((block, receipts, hashes, output)) = point {
                publish(stage, block, receipts, hashes, output);
            }
        }
    }
    fn emit(&self, stage: &'static str) {
        memory::native(
            stage,
            self.visited,
            Default::default(),
            Default::default(),
            Default::default(),
        );
        self.summaries(memory::native);
    }
}
impl Drop for NativeProgress {
    fn drop(&mut self) {
        self.emit(if self.logical.is_some() {
            "reth_native_query_loaded"
        } else {
            "reth_native_query_empty"
        });
    }
}

#[cfg(test)]
mod measurement_tests {
    use super::*;
    #[tokio::test]
    async fn maximum_windows_have_bounded_summary_event_count() {
        let session = memory::Session::new("native-volume").unwrap();
        session
            .scope(async {
                for _ in 0..8 {
                    let mut progress = NativeProgress::default();
                    for index in 0..131_072 {
                        progress.block = index as i64;
                        progress.visited = index as i64;
                        progress.receipts.bytes = 10;
                        progress.hash_map = Some((index as i64, memory::Footprint::default()));
                        progress.update(Default::default(), 0);
                        if memory::progress_due(index) {
                            progress.emit("reth_native_progress");
                            memory::observe("reth_normalization_overlap", Default::default);
                        }
                    }
                    progress.emit("reth_native_progress");
                    memory::observe("reth_normalization_overlap", Default::default);
                }
            })
            .await;
        // Six interval events plus eleven completion/drop events per maximum query.
        assert_eq!(session.observation_count(), 8 * (512 * 6 + 11));
        assert!(session.observation_count() < 25_000);
    }
    #[tokio::test]
    async fn native_peaks_keep_simultaneous_witness_and_flush_on_error() {
        let session = memory::Session::new("native-error").unwrap();
        let result: Result<()> =
            session
                .scope(async {
                    let mut progress = NativeProgress::default();
                    progress.block = 10;
                    progress.receipts = memory::Footprint {
                        bytes: 1000,
                        ..Default::default()
                    };
                    progress.update(
                        memory::Footprint {
                            bytes: 10,
                            owned: 10,
                            ..Default::default()
                        },
                        32,
                    );
                    progress.block = 11;
                    progress.receipts.bytes = 10;
                    progress.update(
                        memory::Footprint {
                            bytes: 100,
                            owned: 100,
                            ..Default::default()
                        },
                        256,
                    );
                    assert_eq!(progress.logical.unwrap().0, 10);
                    assert_eq!(progress.logical.unwrap().3.bytes, 10);
                    assert_eq!(progress.capacity.unwrap().0, 11);
                    assert_eq!(progress.receipt_peak.bytes, 1000);
                    progress.visited = 256;
                    let mut published = Vec::new();
                    progress.summaries(|stage, block, receipts, hashes, output| {
                        published.push((stage, block, receipts, hashes, output))
                    });
                    assert!(published.iter().any(|p| p.0 == "reth_native_logical_peak"
                        && p.1 == 10
                        && p.4.bytes == 10));
                    progress.emit("reth_native_progress");
                    assert_eq!(session.observation_count(), 4);
                    bail!("fixture failure before query completion")
                })
                .await;
        assert!(result.is_err());
        assert_eq!(session.observation_count(), 8);
        assert!(session.valid());
    }
}
