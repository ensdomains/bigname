use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use alloy_consensus::BlockHeader as _;
use alloy_primitives::B256;
use anyhow::{Context, Result, bail};
use reth_ethereum::{
    chainspec::ChainSpecBuilder,
    node::{EthereumNode, api::NodeTypesWithDBAdapter},
    primitives::Block as _,
    provider::{
        BlockHashReader, BlockReader, ChainStateBlockReader, HeaderProvider, ProviderFactory,
        ReceiptProvider, TransactionVariant, db::DatabaseEnv, providers::ReadOnlyConfig,
    },
};
use tokio::task;

#[path = "convert.rs"]
mod convert;
#[path = "head.rs"]
mod head;
#[path = "logs.rs"]
mod logs;
#[path = "retention.rs"]
mod retention;

use convert::{
    hash_hex, i64_to_u64, parse_b256, provider_block_from_header,
    provider_receipts_and_logs_from_recovered, provider_transactions_from_recovered, u64_to_i64,
};

use crate::provider::{Block, BlockBundle, HeadSnapshot, Log, ResolvedBlock};

type EthereumRethProviderFactory =
    ProviderFactory<NodeTypesWithDBAdapter<EthereumNode, DatabaseEnv>>;

#[derive(Clone)]
pub struct RethDbProvider {
    reader: Arc<RethDbReader>,
}

struct RethDbReader {
    chain: String,
    datadir: PathBuf,
    factory: OnceLock<Result<Arc<EthereumRethProviderFactory>, String>>,
}

impl RethDbProvider {
    pub fn new(chain: &str, datadir: &str) -> Result<Self> {
        if chain.trim().is_empty() || datadir.trim().is_empty() {
            bail!("Reth DB chain and datadir must not be empty");
        }
        Ok(Self {
            reader: Arc::new(RethDbReader {
                chain: chain.to_owned(),
                datadir: PathBuf::from(datadir),
                factory: OnceLock::new(),
            }),
        })
    }

    pub async fn heads(&self) -> Result<HeadSnapshot> {
        self.blocking("fetch heads", RethDbReader::fetch_heads)
            .await
    }

    pub async fn earliest_available_block(&self) -> Result<i64> {
        self.blocking(
            "read the earliest available block",
            RethDbReader::earliest_available_block,
        )
        .await
    }

    pub async fn resolve(&self, numbers: &[i64]) -> Result<Vec<ResolvedBlock>> {
        let numbers = numbers.to_vec();
        self.blocking("resolve blocks", move |reader| reader.resolve(&numbers))
            .await
    }

    pub async fn headers(&self, blocks: &[ResolvedBlock]) -> Result<Vec<Block>> {
        let blocks = blocks.to_vec();
        self.blocking("fetch headers", move |reader| reader.headers(&blocks))
            .await
    }

    pub async fn logs(
        &self,
        blocks: &[ResolvedBlock],
        addresses: &[String],
        topics: &[String],
    ) -> Result<Vec<Log>> {
        let blocks = convert::normalized_contiguous_resolved_blocks(blocks)?;
        let addresses = addresses.to_vec();
        let topics = topics.to_vec();
        self.blocking("fetch logs", move |reader| {
            reader.logs(&blocks, &topics, &addresses)
        })
        .await
    }

    pub async fn bundles(&self, blocks: &[ResolvedBlock]) -> Result<Vec<BlockBundle>> {
        let blocks = convert::normalized_resolved_blocks(blocks)?;
        self.blocking("fetch block bundles", move |reader| reader.bundles(&blocks))
            .await
    }

    async fn blocking<T, F>(&self, label: &'static str, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&RethDbReader) -> Result<T> + Send + 'static,
    {
        let reader = Arc::clone(&self.reader);
        task::spawn_blocking(move || operation(&reader))
            .await
            .with_context(|| format!("Reth DB task failed while trying to {label}"))?
    }
}

impl RethDbReader {
    fn factory(&self) -> Result<Arc<EthereumRethProviderFactory>> {
        if self.chain != "ethereum-mainnet" {
            bail!(
                "Reth DB ingest supports ethereum-mainnet only, got {}",
                self.chain
            );
        }
        match self.factory.get_or_init(|| {
            open_ethereum_factory(&self.datadir)
                .map(Arc::new)
                .map_err(|error| format!("{error:#}"))
        }) {
            Ok(factory) => Ok(Arc::clone(factory)),
            Err(error) => bail!(
                "failed to open Reth DB at {}: {error}",
                self.datadir.display()
            ),
        }
    }

    fn fetch_heads(&self) -> Result<HeadSnapshot> {
        let factory = self.factory()?;
        let latest_hash = head::fetch_canonical_head_with_retry(&self.chain, &factory)?;
        let provider = factory.provider()?;
        let safe = provider
            .last_safe_block_number()?
            .filter(|number| *number > 0);
        let finalized = provider
            .last_finalized_block_number()?
            .filter(|number| *number > 0);
        drop(provider);
        Ok(HeadSnapshot {
            latest: self.block_by_hash(&factory, latest_hash)?,
            safe: safe
                .map(|number| self.block_by_number(&factory, number, "safe"))
                .transpose()?,
            finalized: finalized
                .map(|number| self.block_by_number(&factory, number, "finalized"))
                .transpose()?,
        })
    }

    /// Reads the lowest block this datadir can still serve logs for.
    ///
    /// A pruned node answers reads below that block with no rows and no error
    /// (upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1996 @ reth@88505c7f)
    /// (upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1998 @ reth@88505c7f),
    /// so an intake reading the database would record the range as covered. reth's own
    /// `eth_getLogs` refuses a range below its expired-history floor
    /// (upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L584 @ reth@88505c7f)
    /// (upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L586 @ reth@88505c7f);
    /// this floor is deliberately stricter, per `docs/upstream.md` § Known divergences.
    fn earliest_available_block(&self) -> Result<i64> {
        let factory = self.factory()?;
        let readings = retention::read_retention(&factory)?;
        u64_to_i64(
            retention::earliest_servable_block(readings),
            "earliest available block",
        )
    }

    fn resolve(&self, numbers: &[i64]) -> Result<Vec<ResolvedBlock>> {
        let factory = self.factory()?;
        if let Some((from, to)) = contiguous_numbers(numbers)? {
            let headers = factory.sealed_headers_range(from..=to)?;
            if headers.len() != numbers.len() {
                bail!("Reth DB omitted headers from a contiguous range");
            }
            return headers
                .iter()
                .zip(numbers)
                .map(|(header, requested)| {
                    if header.number() != i64_to_u64(*requested, "block number")? {
                        bail!("Reth DB returned a header at the wrong height");
                    }
                    Ok(ResolvedBlock {
                        number: *requested,
                        hash: hash_hex(header.hash()),
                    })
                })
                .collect();
        }
        numbers
            .iter()
            .map(|number| {
                let number_u64 = i64_to_u64(*number, "block number")?;
                let hash = factory
                    .block_hash(number_u64)?
                    .with_context(|| format!("Reth DB omitted block {number}"))?;
                Ok(ResolvedBlock {
                    number: *number,
                    hash: hash_hex(hash),
                })
            })
            .collect()
    }

    fn headers(&self, blocks: &[ResolvedBlock]) -> Result<Vec<Block>> {
        let factory = self.factory()?;
        if let Some((from, to)) = contiguous_resolved(blocks)? {
            let headers = factory.sealed_headers_range(from..=to)?;
            if headers.len() != blocks.len() {
                bail!("Reth DB omitted headers from a resolved range");
            }
            return blocks
                .iter()
                .zip(headers.iter())
                .map(|(expected, header)| {
                    if header.number() != i64_to_u64(expected.number, "block number")?
                        || hash_hex(header.hash()) != expected.hash
                    {
                        bail!("Reth DB header differs from the resolved block");
                    }
                    provider_block_from_header(header.hash(), header.header())
                })
                .collect();
        }
        blocks
            .iter()
            .map(|block| {
                let result =
                    self.block_by_hash(&factory, parse_b256(&block.hash, "resolved block hash")?)?;
                if result.number != block.number {
                    bail!("Reth DB returned a block at the wrong height");
                }
                Ok(result)
            })
            .collect()
    }

    fn bundles(&self, blocks: &[ResolvedBlock]) -> Result<Vec<BlockBundle>> {
        let factory = self.factory()?;
        blocks
            .iter()
            .map(|expected| {
                let hash = parse_b256(&expected.hash, "resolved block hash")?;
                let recovered = factory
                    .sealed_block_with_senders(hash.into(), TransactionVariant::WithHash)?
                    .with_context(|| format!("Reth DB omitted block {}", expected.hash))?;
                let receipts = factory
                    .receipts_by_block(hash.into())?
                    .with_context(|| format!("Reth DB omitted receipts for {}", expected.hash))?;
                let block = provider_block_from_header(hash, recovered.header())?;
                if block.number != expected.number {
                    bail!("Reth DB returned a block bundle at the wrong height");
                }
                let transactions = provider_transactions_from_recovered(&recovered, &block)?;
                let (receipts, logs) =
                    provider_receipts_and_logs_from_recovered(&receipts, &recovered, &block)?;
                Ok(BlockBundle {
                    block,
                    transactions,
                    logs,
                    receipts,
                })
            })
            .collect()
    }

    fn block_by_number(
        &self,
        factory: &EthereumRethProviderFactory,
        number: u64,
        label: &str,
    ) -> Result<Block> {
        let hash = factory
            .block_hash(number)?
            .with_context(|| format!("Reth DB omitted {label} block {number}"))?;
        self.block_by_hash(factory, hash)
    }

    fn block_by_hash(&self, factory: &EthereumRethProviderFactory, hash: B256) -> Result<Block> {
        let block = factory
            .block_by_hash(hash)?
            .with_context(|| format!("Reth DB omitted block {}", hash_hex(hash)))?;
        provider_block_from_header(hash, block.header())
    }

    fn revalidate(&self, blocks: &[ResolvedBlock]) -> Result<()> {
        let numbers = blocks.iter().map(|block| block.number).collect::<Vec<_>>();
        if self.resolve(&numbers)? != blocks {
            bail!("Reth DB block hashes changed during log lookup");
        }
        Ok(())
    }
}

fn open_ethereum_factory(datadir: &Path) -> Result<EthereumRethProviderFactory> {
    validate_datadir(datadir)?;
    let runtime = reth_ethereum::tasks::Runtime::test();
    EthereumNode::provider_factory_builder()
        .open_read_only(
            ChainSpecBuilder::mainnet().build().into(),
            ReadOnlyConfig::from_datadir(datadir),
            runtime,
        )
        .map_err(|error| anyhow::anyhow!("failed to open read-only Reth DB: {error}"))
}

fn contiguous_resolved(blocks: &[ResolvedBlock]) -> Result<Option<(u64, u64)>> {
    contiguous_numbers(&blocks.iter().map(|block| block.number).collect::<Vec<_>>())
}

fn contiguous_numbers(numbers: &[i64]) -> Result<Option<(u64, u64)>> {
    let Some(first) = numbers.first() else {
        return Ok(None);
    };
    let first = i64_to_u64(*first, "block number")?;
    let mut previous = first;
    for number in &numbers[1..] {
        let number = i64_to_u64(*number, "block number")?;
        if number != previous + 1 {
            return Ok(None);
        }
        previous = number;
    }
    Ok(Some((first, previous)))
}

fn validate_datadir(datadir: &Path) -> Result<()> {
    for path in super::RETH_DB_OPENED_STORAGE_CHILDREN.map(|child| datadir.join(child)) {
        if !path.is_dir() {
            bail!("Reth DB directory {} is missing", path.display());
        }
    }
    let data_file = datadir.join("db/mdbx.dat");
    if !data_file.is_file() {
        bail!("Reth DB file {} is missing", data_file.display());
    }
    let lock_file = datadir.join("db/mdbx.lck");
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_file)
        .with_context(|| format!("Reth lock file {} must be writable", lock_file.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_database_access_remains_explicitly_unsupported() {
        let reader = RethDbReader {
            chain: "base-mainnet".to_owned(),
            datadir: PathBuf::from("/unused-base-reth"),
            factory: OnceLock::new(),
        };

        let error = reader
            .factory()
            .expect_err("the Ethereum-only reader must reject Base before opening its database");
        assert!(
            error
                .to_string()
                .contains("Reth DB ingest supports ethereum-mainnet only, got base-mainnet"),
            "{error:#}"
        );
    }
}
