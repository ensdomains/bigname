use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ErrorKind, IngestError, Result,
    provider::{Block, ChainProvider, Log, Receipt, ResolvedBlock, Transaction},
};

#[derive(Clone, Debug, Default)]
pub struct FetchedBatch {
    pub blocks: Vec<Block>,
    pub transactions: Vec<Transaction>,
    pub receipts: Vec<Receipt>,
    pub logs: Vec<Log>,
}

pub async fn fetch_selected_facts(
    provider: &ChainProvider,
    resolved: &[ResolvedBlock],
    selected_logs: Vec<Log>,
) -> Result<FetchedBatch> {
    let blocks = provider.headers(resolved).await.map_err(|error| {
        super::provider::provider_error("failed to fetch resolved block headers", error)
    })?;
    for pair in blocks.windows(2) {
        if pair[1].number != pair[0].number + 1
            || pair[1].parent_hash.as_deref() != Some(pair[0].hash.as_str())
        {
            return Err(IngestError::data_integrity(format!(
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
    let mut transactions = BTreeMap::<(String, String), Transaction>::new();
    let mut receipts = BTreeMap::<(String, String), Receipt>::new();
    let mut logs = BTreeMap::<(String, i64), Log>::new();

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
        transactions.insert(
            (transaction.block_hash.clone(), transaction.hash.clone()),
            transaction,
        );
        receipts.insert(
            (receipt.block_hash.clone(), receipt.transaction_hash.clone()),
            receipt,
        );
        for log in bundle
            .logs
            .iter()
            .filter(|log| log.transaction_hash == actual.transaction_hash)
        {
            let key = (log.block_hash.clone(), log.log_index);
            if let Some(previous) = logs.insert(key.clone(), log.clone())
                && previous != *log
            {
                return Err(IngestError::data_integrity(format!(
                    "provider returned conflicting log identity {} {}",
                    key.0, key.1
                )));
            }
        }
    }
    let mut logs = logs.into_values().collect::<Vec<_>>();
    logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    Ok(FetchedBatch {
        blocks,
        transactions: transactions.into_values().collect(),
        receipts: receipts.into_values().collect(),
        logs,
    })
}

fn validate_log_identity(selected: &Log, actual: &Log) -> Result<()> {
    if selected.block_hash != actual.block_hash
        || selected.block_number != actual.block_number
        || selected.transaction_hash != actual.transaction_hash
        || selected.transaction_index != actual.transaction_index
        || selected.log_index != actual.log_index
        || !selected.address.eq_ignore_ascii_case(&actual.address)
        || selected.topics != actual.topics
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
