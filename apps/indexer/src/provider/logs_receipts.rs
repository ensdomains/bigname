use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use super::{
    JsonRpcProvider, MAX_TRANSACTION_RECEIPT_FALLBACK, PROVIDER_BATCH_ITEM_LIMIT,
    ProviderBlockLogRequest, ProviderBlockSelection, ProviderLog, ProviderRawPayloadCacheMetadata,
    ProviderReceipt, ProviderResolvedBlock, ProviderTransaction, RAW_PAYLOAD_KIND_BLOCK_LOGS,
    RAW_PAYLOAD_KIND_BLOCK_RECEIPTS,
    decode::{normalize_address, normalize_hash},
    request::JsonRpcBatchCall,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProviderBlockLogFetch {
    Fetch,
    Skip,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderLogsPayload {
    pub(super) logs: Vec<ProviderLog>,
    pub(super) cache_metadata: ProviderRawPayloadCacheMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderReceiptsPayload {
    pub(super) receipts: Vec<ProviderReceipt>,
    pub(super) cache_metadata: Vec<ProviderRawPayloadCacheMetadata>,
}

impl JsonRpcProvider {
    #[allow(dead_code, reason = "staged provider helper covered by tests")]
    pub async fn fetch_logs_by_block_hashes(
        &self,
        requests: &[ProviderBlockLogRequest],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let mut logs_by_block_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let requests = requests
            .iter()
            .map(|request| ProviderBlockLogRequest {
                block_number: request.block_number,
                block_hash: normalize_hash(&request.block_hash),
                addresses: request
                    .addresses
                    .iter()
                    .map(|address| normalize_address(address))
                    .collect(),
            })
            .collect::<Vec<_>>();

        for request in &requests {
            if logs_by_block_number
                .insert(request.block_number, Vec::new())
                .is_some()
            {
                bail!(
                    "provider log batch requested duplicate block number {}",
                    request.block_number
                );
            }
        }

        let fetch_requests = requests
            .iter()
            .filter(|request| !request.addresses.is_empty())
            .collect::<Vec<_>>();

        for chunk in fetch_requests.chunks(PROVIDER_BATCH_ITEM_LIMIT) {
            let calls = chunk
                .iter()
                .map(|request| {
                    let mut filter = serde_json::Map::new();
                    filter.insert(
                        "blockHash".to_owned(),
                        Value::String(request.block_hash.clone()),
                    );
                    filter.insert(
                        "address".to_owned(),
                        Value::Array(
                            request
                                .addresses
                                .iter()
                                .map(|address| Value::String(address.clone()))
                                .collect(),
                        ),
                    );

                    JsonRpcBatchCall {
                        method: "eth_getLogs",
                        params: vec![Value::Object(filter)],
                    }
                })
                .collect::<Vec<_>>();
            let results = self.fetch_json_rpc_batch_results(calls).await?;

            for (request, result) in chunk.iter().zip(results) {
                let logs = result.with_context(|| {
                    format!(
                        "provider returned null logs for exact block hash lookup {}",
                        request.block_hash
                    )
                })?;
                let logs = logs
                    .as_array()
                    .context("expected logs array in JSON-RPC result")?;
                let logs = logs
                    .iter()
                    .map(|value| {
                        ProviderLog::from_value(value, &request.block_hash, request.block_number)
                    })
                    .collect::<Result<Vec<_>>>()?;
                logs_by_block_number.insert(request.block_number, logs);
            }
        }

        Ok(logs_by_block_number)
    }

    pub async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let mut logs_by_block_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let mut block_hash_by_number = BTreeMap::<i64, String>::new();
        let mut previous_block_number: Option<i64> = None;

        for resolved_block in resolved_blocks {
            if resolved_block.block_number < 0 {
                bail!(
                    "provider log range requested negative block number {}",
                    resolved_block.block_number
                );
            }

            let block_hash = normalize_hash(&resolved_block.block_hash);
            if block_hash.is_empty() {
                bail!(
                    "provider log range requested block number {} with empty block hash",
                    resolved_block.block_number
                );
            }

            if block_hash_by_number
                .insert(resolved_block.block_number, block_hash)
                .is_some()
            {
                bail!(
                    "provider log range requested duplicate block number {}",
                    resolved_block.block_number
                );
            }
            logs_by_block_number.insert(resolved_block.block_number, Vec::new());

            if let Some(previous_block_number) = previous_block_number {
                let expected_block_number =
                    previous_block_number.checked_add(1).with_context(|| {
                        format!(
                            "provider log range requested malformed block number after {previous_block_number}"
                        )
                    })?;
                if resolved_block.block_number != expected_block_number {
                    bail!(
                        "provider log range requested non-contiguous block numbers: expected {} after {}, got {}",
                        expected_block_number,
                        previous_block_number,
                        resolved_block.block_number
                    );
                }
            }

            previous_block_number = Some(resolved_block.block_number);
        }

        if resolved_blocks.is_empty() {
            return Ok(logs_by_block_number);
        }

        let addresses = addresses
            .iter()
            .map(|address| normalize_address(address))
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Ok(logs_by_block_number);
        }

        let from_block = resolved_blocks
            .first()
            .expect("resolved block range must be non-empty after validation")
            .block_number;
        let to_block = resolved_blocks
            .last()
            .expect("resolved block range must be non-empty after validation")
            .block_number;
        let mut filter = serde_json::Map::new();
        filter.insert(
            "fromBlock".to_owned(),
            ProviderBlockSelection::Number(from_block).json_rpc_parameter()?,
        );
        filter.insert(
            "toBlock".to_owned(),
            ProviderBlockSelection::Number(to_block).json_rpc_parameter()?,
        );
        filter.insert(
            "address".to_owned(),
            Value::Array(addresses.into_iter().map(Value::String).collect::<Vec<_>>()),
        );

        let logs = self
            .fetch_json_rpc_result("eth_getLogs", vec![Value::Object(filter)])
            .await?
            .context("provider returned null logs for block range lookup")?;
        let logs = logs
            .as_array()
            .context("expected logs array in JSON-RPC result")?;

        for (log_position, value) in logs.iter().enumerate() {
            let block_number = ProviderLog::block_number_from_value(value)?;
            let block_hash = block_hash_by_number.get(&block_number).with_context(|| {
                format!(
                    "provider returned log {log_position} for unrequested block number {block_number}"
                )
            })?;
            let log = ProviderLog::from_value(value, block_hash, block_number)?;
            logs_by_block_number
                .get_mut(&block_number)
                .expect("validated log block number must have an output group")
                .push(log);
        }

        self.revalidate_range_log_block_hashes(resolved_blocks)
            .await?;

        Ok(logs_by_block_number)
    }

    async fn revalidate_range_log_block_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<()> {
        let block_numbers = resolved_blocks
            .iter()
            .map(|resolved_block| resolved_block.block_number)
            .collect::<Vec<_>>();
        let revalidated_blocks = self
            .fetch_block_hashes_by_numbers(&block_numbers)
            .await
            .context("provider failed to revalidate block hashes after range log lookup")?;

        if revalidated_blocks.len() != resolved_blocks.len() {
            bail!(
                "provider revalidated {} blocks after range log lookup for {} requested blocks",
                revalidated_blocks.len(),
                resolved_blocks.len()
            );
        }

        for (expected, actual) in resolved_blocks.iter().zip(revalidated_blocks) {
            let expected_hash = normalize_hash(&expected.block_hash);
            if actual.block_number != expected.block_number {
                bail!(
                    "provider revalidated block number {} after range log lookup, but received block number {}",
                    expected.block_number,
                    actual.block_number
                );
            }
            if actual.block_hash != expected_hash {
                bail!(
                    "provider block hash changed after range log lookup for block number {}: expected {}, got {}",
                    expected.block_number,
                    expected_hash,
                    actual.block_hash
                );
            }
        }

        Ok(())
    }

    pub(super) async fn fetch_logs_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<ProviderLogsPayload> {
        let payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getLogs",
                vec![json!({
                    "blockHash": block_hash,
                })],
            )
            .await?
            .with_cache_metadata(RAW_PAYLOAD_KIND_BLOCK_LOGS, "eth_getLogs", "block_hash");
        let logs = payload
            .result
            .context("provider returned null logs for exact block hash lookup")?;
        let logs = logs
            .as_array()
            .context("expected logs array in JSON-RPC result")?;

        let logs = logs
            .iter()
            .map(|value| ProviderLog::from_value(value, block_hash, expected_block_number))
            .collect::<Result<Vec<_>>>()?;

        Ok(ProviderLogsPayload {
            logs,
            cache_metadata: payload.cache_metadata,
        })
    }

    pub(super) async fn fetch_receipts_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        match self
            .fetch_block_receipts_by_block_hash(block_hash, expected_block_number, transactions)
            .await
        {
            Ok(receipts) => Ok(receipts),
            Err(scoped_error) => self
                .fetch_receipts_by_transaction_hashes(
                    block_hash,
                    expected_block_number,
                    transactions,
                )
                .await
                .with_context(|| {
                    format!("block-scoped receipt fetch for {block_hash} failed: {scoped_error}")
                }),
        }
    }

    async fn fetch_block_receipts_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        let payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getBlockReceipts",
                vec![Value::String(block_hash.to_owned())],
            )
            .await?
            .with_cache_metadata(
                RAW_PAYLOAD_KIND_BLOCK_RECEIPTS,
                "eth_getBlockReceipts",
                "block_hash",
            );
        let receipts = payload
            .result
            .context("provider returned null receipts for exact block hash lookup")?;
        let receipts = receipts
            .as_array()
            .context("expected receipts array in JSON-RPC result")?;
        let receipts = receipts
            .iter()
            .map(ProviderReceipt::from_value)
            .collect::<Result<Vec<_>>>()?;

        let receipts = self.order_receipts_by_transaction_hash(
            block_hash,
            expected_block_number,
            receipts,
            transactions,
        )?;

        Ok(ProviderReceiptsPayload {
            receipts,
            cache_metadata: vec![payload.cache_metadata],
        })
    }

    async fn fetch_receipts_by_transaction_hashes(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        if transactions.len() > MAX_TRANSACTION_RECEIPT_FALLBACK {
            bail!(
                "refusing to fan out {} transaction receipts for block {}",
                transactions.len(),
                block_hash
            );
        }

        let mut receipts = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            let receipt = self
                .fetch_json_rpc_result(
                    "eth_getTransactionReceipt",
                    vec![Value::String(transaction.transaction_hash.clone())],
                )
                .await?
                .with_context(|| {
                    format!(
                        "provider did not return receipt for transaction {}",
                        transaction.transaction_hash
                    )
                })?;
            let receipt = ProviderReceipt::from_value(&receipt)?;
            receipts.push(receipt);
        }

        let receipts = self.order_receipts_by_transaction_hash(
            block_hash,
            expected_block_number,
            receipts,
            transactions,
        )?;

        Ok(ProviderReceiptsPayload {
            receipts,
            cache_metadata: Vec::new(),
        })
    }

    fn order_receipts_by_transaction_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        receipts: Vec<ProviderReceipt>,
        transactions: &[ProviderTransaction],
    ) -> Result<Vec<ProviderReceipt>> {
        let mut receipts_by_hash = BTreeMap::new();
        for receipt in receipts {
            if receipt.block_hash != block_hash {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block hash {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_hash
                );
            }
            if receipt.block_number != expected_block_number {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block number {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_number
                );
            }

            if receipts_by_hash
                .insert(receipt.transaction_hash.clone(), receipt)
                .is_some()
            {
                bail!("provider returned duplicate receipt for block {block_hash}");
            }
        }

        let mut ordered = Vec::new();
        for transaction in transactions {
            let receipt = receipts_by_hash
                .remove(&transaction.transaction_hash)
                .with_context(|| {
                    format!(
                        "provider did not return receipt for transaction {} in block {}",
                        transaction.transaction_hash, block_hash
                    )
                })?;

            if receipt.block_hash != block_hash {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block hash {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_hash
                );
            }
            if receipt.block_number != expected_block_number {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block number {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_number
                );
            }

            ordered.push(receipt);
        }

        if !receipts_by_hash.is_empty() {
            bail!("provider returned extra receipts for block {block_hash}");
        }

        Ok(ordered)
    }
}
