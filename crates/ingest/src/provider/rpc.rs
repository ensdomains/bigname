use std::collections::{BTreeMap, VecDeque};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde_json::{Value, json};

use super::{
    JsonRpcProvider, PROVIDER_PARALLELISM,
    decode::normalize_hash,
    request::BatchCall,
    types::{
        Block, BlockBundle, BlockTag, HeadSnapshot, Log, Receipt, ResolvedBlock, Transaction,
        TransactionPayload, block_number_parameter, hash_log_filter, range_log_filter,
    },
};

const BATCH_LIMIT: usize = 32;
const MAX_RECEIPT_FALLBACK: usize = 256;

impl JsonRpcProvider {
    pub async fn heads(&self) -> Result<HeadSnapshot> {
        let latest = self
            .block_for_tag(BlockTag::Latest, false)
            .await?
            .context("provider did not return a latest block")?;
        let safe = self.optional_checkpoint(BlockTag::Safe).await?;
        let finalized = self.optional_checkpoint(BlockTag::Finalized).await?;
        Ok(HeadSnapshot {
            latest,
            safe,
            finalized,
        })
    }

    async fn optional_checkpoint(&self, tag: BlockTag) -> Result<Option<Block>> {
        match self.block_for_tag(tag, false).await {
            Ok(block) => Ok(block),
            Err(error) if unsupported_checkpoint_tag(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn block_for_tag(&self, tag: BlockTag, transactions: bool) -> Result<Option<Block>> {
        self.request(
            "eth_getBlockByNumber",
            vec![tag.json_rpc_parameter()?, Value::Bool(transactions)],
        )
        .await?
        .map(Block::from_value)
        .transpose()
    }

    /// Runs `calls` as JSON-RPC batches of [`BATCH_LIMIT`], at most
    /// [`PROVIDER_PARALLELISM`] in flight, and returns the results in call order.
    async fn parallel_batches(&self, calls: Vec<BatchCall>) -> Result<Vec<Option<Value>>> {
        let chunks = calls
            .chunks(BATCH_LIMIT)
            .map(<[BatchCall]>::to_vec)
            .collect::<Vec<_>>();
        let results = stream::iter(chunks.into_iter().map(|chunk| self.batch(chunk)))
            .buffered(PROVIDER_PARALLELISM)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(results.into_iter().flatten().collect())
    }

    pub async fn resolve(&self, numbers: &[i64]) -> Result<Vec<ResolvedBlock>> {
        let calls = numbers
            .iter()
            .map(|number| {
                Ok(BatchCall {
                    method: "eth_getBlockByNumber",
                    params: vec![block_number_parameter(*number)?, Value::Bool(false)],
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut resolved = Vec::with_capacity(numbers.len());
        for (number, value) in numbers.iter().zip(self.parallel_batches(calls).await?) {
            let block = value
                .with_context(|| format!("provider omitted block {number}"))
                .and_then(Block::from_value)?;
            if block.number != *number {
                bail!(
                    "provider returned block {} for requested {number}",
                    block.number
                );
            }
            resolved.push(ResolvedBlock {
                number: *number,
                hash: block.hash,
            });
        }
        Ok(resolved)
    }

    pub async fn headers(&self, resolved: &[ResolvedBlock]) -> Result<Vec<Block>> {
        let calls = resolved
            .iter()
            .map(|block| BatchCall {
                method: "eth_getBlockByHash",
                params: vec![Value::String(block.hash.clone()), Value::Bool(false)],
            })
            .collect();
        let mut headers = Vec::with_capacity(resolved.len());
        for (expected, value) in resolved.iter().zip(self.parallel_batches(calls).await?) {
            let block = value
                .with_context(|| format!("provider omitted block {}", expected.hash))
                .and_then(Block::from_value)?;
            validate_block(expected, &block)?;
            headers.push(block);
        }
        Ok(headers)
    }

    /// Re-resolves `resolved` by number and fails when any hash moved underneath ingest.
    ///
    /// A window runs this once, after the last range log lookup, over the union of blocks
    /// that returned a log.
    pub async fn recheck_resolved(&self, resolved: &[ResolvedBlock]) -> Result<()> {
        if resolved.is_empty() {
            return Ok(());
        }
        let numbers = resolved
            .iter()
            .map(|block| block.number)
            .collect::<Vec<_>>();
        if self.resolve(&numbers).await? != resolved {
            bail!("provider block hashes changed during range log lookup");
        }
        Ok(())
    }

    /// Range log lookup over `from..=to` with no hash re-check of its own.
    ///
    /// Logs come back carrying the block hash the provider reported. The caller pins them
    /// against the hashes the window resolved and calls [`Self::recheck_resolved`] once.
    pub async fn range_logs(
        &self,
        from: i64,
        to: i64,
        addresses: &[String],
        topics: &[String],
    ) -> Result<Vec<Log>> {
        self.range_log_values(from, to, addresses, topics)
            .await?
            .iter()
            .map(Log::from_unpinned_value)
            .collect()
    }

    /// Fetches the receipt and the transaction of every hash in `hashes`.
    ///
    /// Both methods go out as JSON-RPC batches with the provider's bounded parallelism;
    /// results keep the order of `hashes`. A null result means the transaction left the
    /// chain between the range query and this fetch, which is transient.
    pub async fn transaction_payloads(&self, hashes: &[String]) -> Result<Vec<TransactionPayload>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let calls = hashes
            .iter()
            .map(|hash| BatchCall {
                method: "eth_getTransactionReceipt",
                params: vec![json!(hash)],
            })
            .chain(hashes.iter().map(|hash| BatchCall {
                method: "eth_getTransactionByHash",
                params: vec![json!(hash)],
            }))
            .collect();
        let values = self.parallel_batches(calls).await?;
        let (receipts, transactions) = values.split_at(hashes.len());
        hashes
            .iter()
            .zip(receipts)
            .zip(transactions)
            .map(|((hash, receipt), transaction)| {
                let receipt = receipt.as_ref().with_context(|| {
                    format!("provider omitted receipt for selected transaction {hash}")
                })?;
                let transaction = transaction.as_ref().with_context(|| {
                    format!("provider omitted transaction for selected log {hash}")
                })?;
                if transaction
                    .get("blockHash")
                    .is_none_or(serde_json::Value::is_null)
                {
                    bail!("provider returned a pending transaction for selected log {hash}");
                }
                Ok(TransactionPayload {
                    transaction: Transaction::from_value(transaction)?,
                    receipt: Receipt::from_value(receipt)?,
                    receipt_logs: Receipt::logs_from_value(receipt)?,
                    receipt_reported_status: Receipt::reported_status(receipt),
                })
            })
            .collect()
    }

    pub(super) async fn verification_logs(
        &self,
        from_block: i64,
        to_block: i64,
        addresses: &[String],
        topics: &[String],
    ) -> Result<Vec<Log>> {
        let values = self
            .range_log_values(from_block, to_block, addresses, topics)
            .await?;
        let logs = values
            .iter()
            .map(Log::from_unpinned_value)
            .map(|result| {
                let log = result?;
                if !(from_block..=to_block).contains(&log.block_number) {
                    bail!(
                        "provider returned verification log at block {} outside \
                         {from_block}..={to_block}",
                        log.block_number
                    );
                }
                Ok(log)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(logs)
    }

    async fn range_log_values(
        &self,
        from_block: i64,
        to_block: i64,
        addresses: &[String],
        topics: &[String],
    ) -> Result<Vec<Value>> {
        if from_block > to_block || topics.is_empty() {
            return Ok(Vec::new());
        }
        let mut queue = VecDeque::from([(from_block, to_block)]);
        let mut values = Vec::new();
        while let Some((first, last)) = queue.pop_front() {
            let result = self
                .request(
                    "eth_getLogs",
                    vec![range_log_filter(first, last, addresses, topics)?],
                )
                .await;
            match result {
                Ok(Some(Value::Array(logs))) => values.extend(logs),
                Ok(Some(_)) => bail!("provider returned a non-array log result"),
                Ok(None) => bail!("provider returned null logs for {first}..={last}"),
                Err(error) if first < last && range_too_large(&error) => {
                    let middle = first + (last - first) / 2;
                    queue.push_front((middle + 1, last));
                    queue.push_front((first, middle));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(values)
    }

    pub async fn bundles(&self, resolved: &[ResolvedBlock]) -> Result<Vec<BlockBundle>> {
        let mut bundles = Vec::with_capacity(resolved.len());
        for expected in resolved {
            bundles.push(self.bundle(expected).await?);
        }
        Ok(bundles)
    }

    async fn bundle(&self, expected: &ResolvedBlock) -> Result<BlockBundle> {
        let value = self
            .request(
                "eth_getBlockByHash",
                vec![Value::String(expected.hash.clone()), Value::Bool(true)],
            )
            .await?
            .with_context(|| format!("provider omitted block {}", expected.hash))?;
        let mut bundle = BlockBundle::from_value(value)?;
        validate_block(expected, &bundle.block)?;
        validate_transactions(&bundle)?;
        bundle.logs = self.logs_for_block(&bundle.block).await?;
        bundle.receipts = self.receipts_for_block(&bundle).await?;
        Ok(bundle)
    }

    async fn logs_for_block(&self, block: &Block) -> Result<Vec<Log>> {
        let value = self
            .request("eth_getLogs", vec![hash_log_filter(&block.hash)?])
            .await?
            .context("provider returned null exact-block logs")?;
        value
            .as_array()
            .context("provider returned non-array exact-block logs")?
            .iter()
            .map(|value| Log::from_block_hash_value(value, &block.hash, block.number))
            .collect()
    }

    async fn receipts_for_block(&self, bundle: &BlockBundle) -> Result<Vec<Receipt>> {
        let scoped = self
            .request(
                "eth_getBlockReceipts",
                vec![Value::String(bundle.block.hash.clone())],
            )
            .await;
        let receipts = match scoped {
            Ok(Some(Value::Array(values))) => values
                .iter()
                .map(Receipt::from_value)
                .collect::<Result<Vec<_>>>()?,
            Ok(Some(_)) => bail!("provider returned non-array block receipts"),
            Err(error) if super::request::retryable(&error) => {
                return Err(error).context("block receipt lookup exhausted transient retries");
            }
            Ok(None) | Err(_) if bundle.transactions.len() <= MAX_RECEIPT_FALLBACK => {
                let calls = bundle
                    .transactions
                    .iter()
                    .map(|transaction| BatchCall {
                        method: "eth_getTransactionReceipt",
                        params: vec![json!(transaction.hash)],
                    })
                    .collect();
                self.batch(calls)
                    .await?
                    .into_iter()
                    .map(|value| {
                        value
                            .context("provider omitted transaction receipt")
                            .and_then(|value| Receipt::from_value(&value))
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            Ok(None) | Err(_) => bail!(
                "refusing to fetch {} individual receipts for block {}",
                bundle.transactions.len(),
                bundle.block.hash
            ),
        };
        order_receipts(bundle, receipts)
    }
}

/// Pins range-query logs to the hashes the window resolved.
///
/// A log for a block outside the window, or for a hash that no longer matches, is the same
/// mid-fetch reorg race the by-hash lookup used to report, and stays retryable.
pub(super) fn pin_logs_to_resolved(resolved: &[ResolvedBlock], logs: Vec<Log>) -> Result<Vec<Log>> {
    let by_number = resolved
        .iter()
        .map(|block| (block.number, block.hash.as_str()))
        .collect::<BTreeMap<_, _>>();
    for log in &logs {
        let hash = by_number
            .get(&log.block_number)
            .with_context(|| format!("provider returned log for block {}", log.block_number))?;
        if log.block_hash != *hash {
            bail!(
                "provider returned log outside resolved block {} {hash}",
                log.block_number
            );
        }
    }
    Ok(logs)
}

fn validate_block(expected: &ResolvedBlock, block: &Block) -> Result<()> {
    if block.number != expected.number || block.hash != normalize_hash(&expected.hash) {
        bail!(
            "provider returned block {} {} for requested {} {}",
            block.number,
            block.hash,
            expected.number,
            expected.hash
        );
    }
    Ok(())
}

fn validate_transactions(bundle: &BlockBundle) -> Result<()> {
    if bundle.transactions.iter().any(|transaction| {
        transaction.block_hash != bundle.block.hash
            || transaction.block_number != bundle.block.number
    }) {
        bail!(
            "provider returned a transaction outside block {}",
            bundle.block.hash
        );
    }
    Ok(())
}

fn order_receipts(bundle: &BlockBundle, receipts: Vec<Receipt>) -> Result<Vec<Receipt>> {
    let mut by_hash = receipts
        .into_iter()
        .map(|receipt| (receipt.transaction_hash.clone(), receipt))
        .collect::<BTreeMap<_, _>>();
    let mut ordered = Vec::with_capacity(bundle.transactions.len());
    for transaction in &bundle.transactions {
        let receipt = by_hash
            .remove(&transaction.hash)
            .with_context(|| format!("provider omitted receipt {}", transaction.hash))?;
        if receipt.block_hash != bundle.block.hash
            || receipt.block_number != bundle.block.number
            || receipt.transaction_index != transaction.index
        {
            bail!(
                "provider returned mismatched receipt {}",
                receipt.transaction_hash
            );
        }
        ordered.push(receipt);
    }
    if !by_hash.is_empty() {
        bail!("provider returned extra block receipts");
    }
    Ok(ordered)
}

fn range_too_large(error: &anyhow::Error) -> bool {
    let error = format!("{error:#}").to_ascii_lowercase();
    [
        "query exceeds max results",
        "query returned more than",
        "response size exceeded",
        "result size exceeded",
        "more than 10000 results",
        "-32005",
        // dRPC: "-32602: query block range exceeds server limit, narrow your filter: 1000".
        "block range exceeds",
        "exceeds server limit",
        "narrow your filter",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

fn unsupported_checkpoint_tag(error: &anyhow::Error) -> bool {
    let error = format!("{error:#}").to_ascii_lowercase();
    error.contains("json-rpc error for eth_getblockbynumber")
        && [
            "unsupported block tag",
            "unsupported block parameter",
            "unknown block tag",
            "unknown block parameter",
            "invalid block tag",
            "invalid block parameter",
        ]
        .iter()
        .any(|needle| error.contains(needle))
}

#[cfg(test)]
mod range_limit_tests {
    use super::range_too_large;

    #[test]
    fn hosted_block_range_limits_are_halved_not_terminal() {
        for message in [
            "provider returned JSON-RPC error for eth_getLogs: -32602: query block range exceeds server limit, narrow your filter: 1000",
            "provider returned JSON-RPC error for eth_getLogs: -32005: query returned more than 10000 results",
        ] {
            assert!(range_too_large(&anyhow::anyhow!(message)), "{message}");
        }
        assert!(!range_too_large(&anyhow::anyhow!(
            "provider returned JSON-RPC error for eth_getLogs: -32602: invalid argument 0: hex string"
        )));
    }
}
