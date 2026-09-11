use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ErrorKind, IngestError, Result,
    manifest::WatchFilter,
    provider::{
        Block, ChainProvider, Log, Receipt, ResolvedBlock, Transaction, TransactionPayload,
        bloom_contains,
    },
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Default)]
pub struct FetchedBatch {
    pub blocks: Vec<Block>,
    pub transactions: Vec<Transaction>,
    pub receipts: Vec<Receipt>,
    pub logs: Vec<Log>,
}

/// Accumulates the facts a window stores, keyed exactly as the write path expects them.
#[derive(Default)]
struct SelectedFacts {
    transactions: BTreeMap<(String, String), Transaction>,
    receipts: BTreeMap<(String, String), Receipt>,
    logs: BTreeMap<(String, i64), Log>,
}

impl SelectedFacts {
    fn insert_transaction(&mut self, transaction: Transaction) {
        self.transactions.insert(
            (transaction.block_hash.clone(), transaction.hash.clone()),
            transaction,
        );
    }

    fn insert_receipt(&mut self, receipt: Receipt) {
        self.receipts.insert(
            (receipt.block_hash.clone(), receipt.transaction_hash.clone()),
            receipt,
        );
    }

    fn insert_log(&mut self, log: &Log) -> Result<()> {
        let key = (log.block_hash.clone(), log.log_index);
        if let Some(previous) = self.logs.insert(key.clone(), log.clone())
            && previous != *log
        {
            return Err(IngestError::data_integrity(format!(
                "provider returned conflicting log identity {} {}",
                key.0, key.1
            )));
        }
        Ok(())
    }

    fn into_batch(self, blocks: Vec<Block>) -> FetchedBatch {
        let mut logs = self.logs.into_values().collect::<Vec<_>>();
        logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
        FetchedBatch {
            blocks,
            transactions: self.transactions.into_values().collect(),
            receipts: self.receipts.into_values().collect(),
            logs,
        }
    }
}

pub async fn fetch_selected_facts(
    provider: &ChainProvider,
    resolved: &[ResolvedBlock],
    selected_logs: Vec<Log>,
    filter: &WatchFilter,
) -> Result<FetchedBatch> {
    let blocks = provider.headers(resolved).await.map_err(|error| {
        super::provider::provider_error("failed to fetch resolved block headers", error)
    })?;
    for pair in blocks.windows(2) {
        if pair[1].number != pair[0].number + 1
            || pair[1].parent_hash.as_deref() != Some(pair[0].hash.as_str())
        {
            return Err(IngestError::transient(format!(
                "loaded block window changes lineage between blocks {} and {}",
                pair[0].number, pair[1].number
            )));
        }
    }
    if selected_logs.is_empty() {
        return Ok(FetchedBatch {
            blocks,
            ..FetchedBatch::default()
        });
    }
    if provider.fetches_transactions() {
        fetch_selected_transactions(provider, resolved, &blocks, selected_logs, filter)
            .await
            .map(|facts| facts.into_batch(blocks))
    } else {
        fetch_selected_bundles(provider, resolved, selected_logs)
            .await
            .map(|facts| facts.into_batch(blocks))
    }
}

/// Fetches exactly the transactions the range queries selected, and nothing else.
///
/// The whole-block consistency the bundle path used to assert covered data ingest threw
/// away. What replaces it checks every fact that is actually stored: the receipt and the
/// transaction sit in the block this window resolved, the range-query log reappears
/// unchanged inside the receipt, each stored log is admitted by its own header's bloom,
/// and the receipt carries no filter-matching log the range query missed.
async fn fetch_selected_transactions(
    provider: &ChainProvider,
    resolved: &[ResolvedBlock],
    blocks: &[Block],
    selected_logs: Vec<Log>,
    filter: &WatchFilter,
) -> Result<SelectedFacts> {
    let mut selected_by_transaction = BTreeMap::<String, Vec<Log>>::new();
    for log in selected_logs {
        selected_by_transaction
            .entry(log.transaction_hash.clone())
            .or_default()
            .push(log);
    }
    let selected_identities = selected_by_transaction
        .values()
        .flatten()
        .map(|log| (log.block_hash.as_str(), log.log_index))
        .collect::<BTreeSet<_>>();
    let hashes = selected_by_transaction.keys().cloned().collect::<Vec<_>>();
    let payloads = provider
        .transaction_payloads(&hashes)
        .await
        .map_err(|error| {
            super::provider::provider_error("failed to fetch selected transactions", error)
        })?;
    if payloads.len() != hashes.len() {
        return Err(IngestError::data_integrity(format!(
            "provider returned {} payloads for {} selected transactions",
            payloads.len(),
            hashes.len()
        )));
    }
    let hash_by_number = resolved
        .iter()
        .map(|block| (block.number, block.hash.as_str()))
        .collect::<BTreeMap<_, _>>();
    let header_by_hash = blocks
        .iter()
        .map(|block| (block.hash.as_str(), block))
        .collect::<BTreeMap<_, _>>();

    let mut facts = SelectedFacts::default();
    for (hash, payload) in hashes.iter().zip(payloads) {
        let selected = &selected_by_transaction[hash];
        validate_payload_position(hash, &payload, &hash_by_number)?;
        validate_receipt_shape(hash, &payload)?;
        validate_selected_logs_present(selected, &payload)?;
        validate_filter_completeness(&payload, filter, &selected_identities)?;
        for log in &payload.receipt_logs {
            validate_bloom_membership(log, &header_by_hash)?;
            facts.insert_log(log)?;
        }
        facts.insert_transaction(payload.transaction);
        facts.insert_receipt(payload.receipt);
    }
    Ok(facts)
}

/// Pins a fetched transaction and its receipt to the block the window resolved.
///
/// A disagreement here is the chain moving underneath an in-flight window, not a corrupt
/// provider, so it stays retryable.
fn validate_payload_position(
    hash: &str,
    payload: &TransactionPayload,
    hash_by_number: &BTreeMap<i64, &str>,
) -> Result<()> {
    let receipt = &payload.receipt;
    let transaction = &payload.transaction;
    if receipt.transaction_hash != *hash || transaction.hash != *hash {
        return Err(IngestError::data_integrity(format!(
            "provider returned receipt {} and transaction {} for requested transaction {hash}",
            receipt.transaction_hash, transaction.hash
        )));
    }
    for (number, block_hash, label) in [
        (receipt.block_number, &receipt.block_hash, "receipt"),
        (
            transaction.block_number,
            &transaction.block_hash,
            "transaction",
        ),
    ] {
        let resolved = hash_by_number.get(&number).copied().ok_or_else(|| {
            IngestError::transient(format!(
                "provider returned {label} {hash} outside resolved block {number}"
            ))
        })?;
        if resolved != block_hash {
            return Err(IngestError::transient(format!(
                "provider returned {label} {hash} outside resolved block {number} {resolved}"
            )));
        }
    }
    if transaction.index != receipt.transaction_index {
        return Err(IngestError::data_integrity(format!(
            "provider returned transaction index {} and receipt index {} for {hash}",
            transaction.index, receipt.transaction_index
        )));
    }
    Ok(())
}

/// Receipt sanity that does not depend on what the window selected.
fn validate_receipt_shape(hash: &str, payload: &TransactionPayload) -> Result<()> {
    if payload.receipt_reported_status && payload.receipt.status.is_none() {
        return Err(IngestError::data_integrity(format!(
            "provider returned an undecodable receipt status for {hash}"
        )));
    }
    for pair in payload.receipt_logs.windows(2) {
        if pair[1].log_index <= pair[0].log_index {
            return Err(IngestError::data_integrity(format!(
                "provider returned receipt logs {} and {} out of order for {hash}",
                pair[0].log_index, pair[1].log_index
            )));
        }
    }
    for log in &payload.receipt_logs {
        if log.transaction_hash != *hash
            || log.block_hash != payload.receipt.block_hash
            || log.block_number != payload.receipt.block_number
            || log.transaction_index != payload.receipt.transaction_index
        {
            return Err(IngestError::data_integrity(format!(
                "provider returned a receipt log outside transaction {hash}"
            )));
        }
    }
    Ok(())
}

/// Every log the range query selected must reappear, byte for byte, inside the receipt.
fn validate_selected_logs_present(selected: &[Log], payload: &TransactionPayload) -> Result<()> {
    for log in selected {
        let actual = payload
            .receipt_logs
            .iter()
            .find(|candidate| candidate.log_index == log.log_index)
            .ok_or_else(|| {
                IngestError::data_integrity(format!(
                    "provider omitted selected log {} {} from receipt {}",
                    log.block_hash, log.log_index, log.transaction_hash
                ))
            })?;
        validate_log_identity(log, actual)?;
    }
    Ok(())
}

/// No log the window's watch filter admits may be missing from the range-query result.
///
/// The range query and the receipt are two independent reads of the same transaction. A
/// filter-matching log that only one of them reports means one of the reads is wrong, and
/// ingest would silently store an incomplete window.
fn validate_filter_completeness(
    payload: &TransactionPayload,
    filter: &WatchFilter,
    selected_identities: &BTreeSet<(&str, i64)>,
) -> Result<()> {
    for log in &payload.receipt_logs {
        let admitted = log
            .topics
            .first()
            .is_some_and(|topic0| filter.includes(&log.address, topic0, log.block_number));
        if admitted && !selected_identities.contains(&(log.block_hash.as_str(), log.log_index)) {
            return Err(IngestError::data_integrity(format!(
                "range log query missed watched log {} {} in transaction {}",
                log.block_hash, log.log_index, log.transaction_hash
            )));
        }
    }
    Ok(())
}

/// Every stored log must be admitted by the bloom of the header this window already holds.
fn validate_bloom_membership(log: &Log, header_by_hash: &BTreeMap<&str, &Block>) -> Result<()> {
    let header = header_by_hash.get(log.block_hash.as_str()).ok_or_else(|| {
        IngestError::data_integrity(format!(
            "provider returned log {} {} for an unheadered block",
            log.block_hash, log.log_index
        ))
    })?;
    let Some(bloom) = header.logs_bloom.as_deref() else {
        return Err(IngestError::data_integrity(format!(
            "provider omitted the logs bloom of block {}",
            header.hash
        )));
    };
    for value in std::iter::once(log.address.as_str()).chain(log.topics.iter().map(String::as_str))
    {
        let bytes = alloy_primitives::hex::decode(value).map_err(|_| {
            IngestError::data_integrity(format!(
                "provider returned undecodable log value {value} at {} {}",
                log.block_hash, log.log_index
            ))
        })?;
        if !bloom_contains(bloom, &bytes) {
            return Err(IngestError::data_integrity(format!(
                "log value {value} at {} {} is not in the bloom of block {}",
                log.block_hash, log.log_index, header.hash
            )));
        }
    }
    Ok(())
}

/// Whole-block fetch, kept for the datadir provider and as the reference the tests compare
/// the per-transaction path against.
async fn fetch_selected_bundles(
    provider: &ChainProvider,
    resolved: &[ResolvedBlock],
    selected_logs: Vec<Log>,
) -> Result<SelectedFacts> {
    let selected_block_hashes = selected_logs
        .iter()
        .map(|log| log.block_hash.as_str())
        .collect::<BTreeSet<_>>();
    let selected_blocks = resolved
        .iter()
        .filter(|block| selected_block_hashes.contains(block.hash.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let bundles = provider.bundles(&selected_blocks).await.map_err(|error| {
        super::provider::provider_error("failed to fetch selected block payloads", error)
    })?;
    let bundles = bundles
        .into_iter()
        .map(|bundle| (bundle.block.hash.clone(), bundle))
        .collect::<BTreeMap<_, _>>();
    let mut facts = SelectedFacts::default();

    for selected in selected_logs {
        let bundle = bundles.get(&selected.block_hash).ok_or_else(|| {
            IngestError::data_integrity(format!(
                "provider omitted selected block {}",
                selected.block_hash
            ))
        })?;
        let actual = bundle
            .logs
            .iter()
            .find(|log| log.log_index == selected.log_index)
            .ok_or_else(|| {
                IngestError::data_integrity(format!(
                    "provider omitted selected log {} {}",
                    selected.block_hash, selected.log_index
                ))
            })?;
        validate_log_identity(&selected, actual)?;
        let transaction = bundle
            .transactions
            .iter()
            .find(|transaction| transaction.hash == actual.transaction_hash)
            .cloned()
            .ok_or_else(|| {
                IngestError::data_integrity(format!(
                    "provider omitted transaction {} for selected log",
                    actual.transaction_hash
                ))
            })?;
        let receipt = bundle
            .receipts
            .iter()
            .find(|receipt| receipt.transaction_hash == actual.transaction_hash)
            .cloned()
            .ok_or_else(|| {
                IngestError::data_integrity(format!(
                    "provider omitted receipt {} for selected log",
                    actual.transaction_hash
                ))
            })?;
        facts.insert_transaction(transaction);
        facts.insert_receipt(receipt);
        for log in bundle
            .logs
            .iter()
            .filter(|log| log.transaction_hash == actual.transaction_hash)
        {
            facts.insert_log(log)?;
        }
    }
    Ok(facts)
}

fn validate_log_identity(selected: &Log, actual: &Log) -> Result<()> {
    if selected.block_hash != actual.block_hash
        || selected.block_number != actual.block_number
        || selected.transaction_hash != actual.transaction_hash
        || selected.transaction_index != actual.transaction_index
        || selected.log_index != actual.log_index
        || !selected.address.eq_ignore_ascii_case(&actual.address)
        || selected.topics != actual.topics
        || selected.data != actual.data
    {
        return Err(IngestError::new(
            ErrorKind::DataIntegrity,
            format!(
                "chain provider log identity differs from selected source at {} {}",
                selected.block_hash, selected.log_index
            ),
        ));
    }
    Ok(())
}

pub fn estimated_write_bytes(facts: &FetchedBatch) -> u64 {
    let bytes = facts
        .blocks
        .iter()
        .map(|block| block.hash.len() + block.parent_hash.as_deref().map_or(0, str::len))
        .sum::<usize>()
        + facts
            .transactions
            .iter()
            .map(|transaction| transaction.input.len() + 160)
            .sum::<usize>()
        + facts
            .receipts
            .iter()
            .map(|receipt| receipt.logs_bloom.as_deref().map_or(0, <[u8]>::len) + 128)
            .sum::<usize>()
        + facts
            .logs
            .iter()
            .map(|log| log.data.len() + log.topics.len() * 66 + 128)
            .sum::<usize>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
}
