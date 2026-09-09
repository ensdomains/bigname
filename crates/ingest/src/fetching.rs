use crate::measurement::{self as memory, Measured};
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
    memory::observe("selected_facts_input", || selected_logs.footprint());
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

    let selected_block_hashes = selected_logs
        .iter()
        .map(|log| log.block_hash.as_str())
        .collect::<BTreeSet<_>>();
    let selected_blocks = resolved
        .iter()
        .filter(|block| selected_block_hashes.contains(block.hash.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    memory::observe("selected_blocks", || selected_blocks.footprint());
    let bundles = provider.bundles(&selected_blocks).await.map_err(|error| {
        super::provider::provider_error("failed to fetch selected block payloads", error)
    })?;
    memory::observe("selected_bundles", || bundles.footprint());
    let bundles = bundles
        .into_iter()
        .map(|bundle| (bundle.block.hash.clone(), bundle))
        .collect::<BTreeMap<_, _>>();
    let mut transactions = BTreeMap::<(String, String), Transaction>::new();
    let mut receipts = BTreeMap::<(String, String), Receipt>::new();
    let mut logs = BTreeMap::<(String, i64), Log>::new();

    let mut accounting = FactOverlap::default();
    memory::observe("selected_facts_overlap_start", || {
        accounting.remaining = selected_logs.footprint();
        accounting.stable = blocks.footprint().combine(selected_blocks.footprint());
        for (key, bundle) in &bundles {
            accounting.stable = accounting
                .stable
                .combine(key.footprint())
                .combine(bundle.footprint());
        }
        accounting.stable = accounting
            .stable
            .combine(memory::Footprint::entries::<&str>(
                selected_block_hashes.len(),
            ));
        accounting.stable.combine(accounting.remaining)
    });
    for selected in selected_logs {
        if memory::capture().is_some() {
            accounting.current = selected.footprint();
        }

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
        if memory::capture().is_some() {
            accounting.pending = receipt.footprint();
        }
        accounting.insert(
            &mut transactions,
            (transaction.block_hash.clone(), transaction.hash.clone()),
            transaction,
        );
        accounting.pending = memory::Footprint::default();
        accounting.insert(
            &mut receipts,
            (receipt.block_hash.clone(), receipt.transaction_hash.clone()),
            receipt,
        );
        for log in bundle
            .logs
            .iter()
            .filter(|log| log.transaction_hash == actual.transaction_hash)
        {
            let key = (log.block_hash.clone(), log.log_index);
            if memory::capture().is_some() {
                accounting.pending = key.footprint();
            }
            if let Some(previous) = accounting.insert(&mut logs, key.clone(), log.clone())
                && previous != *log
            {
                return Err(IngestError::data_integrity(format!(
                    "provider returned conflicting log identity {} {}",
                    key.0, key.1
                )));
            }
        }
        if memory::capture().is_some() {
            accounting.remaining.bytes =
                memory::subtract(accounting.remaining.bytes, accounting.current.bytes);
            accounting.remaining.owned =
                memory::subtract(accounting.remaining.owned, accounting.current.owned);
        }
    }
    memory::observe("selected_fact_maps", || {
        let mut f =
            memory::Footprint::entries::<((String, String), Transaction)>(transactions.len())
                .combine(memory::Footprint::entries::<((String, String), Receipt)>(
                    receipts.len(),
                ))
                .combine(memory::Footprint::entries::<((String, i64), Log)>(
                    logs.len(),
                ));
        for (key, value) in &transactions {
            f = f
                .combine(key.0.footprint())
                .combine(key.1.footprint())
                .combine(value.footprint());
        }
        for (key, value) in &receipts {
            f = f
                .combine(key.0.footprint())
                .combine(key.1.footprint())
                .combine(value.footprint());
        }
        for (key, value) in &logs {
            f = f.combine(key.0.footprint()).combine(value.footprint());
        }
        f
    });
    let mut logs = logs.into_values().collect::<Vec<_>>();
    logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    Ok(FetchedBatch {
        blocks,
        transactions: transactions.into_values().collect(),
        receipts: receipts.into_values().collect(),
        logs,
    })
}

#[derive(Default)]
struct FactOverlap {
    stable: memory::Footprint,
    remaining: memory::Footprint,
    current: memory::Footprint,
    retained: memory::Footprint,
    pending: memory::Footprint,
    peak: memory::Footprint,
}
impl FactOverlap {
    fn insert<K: Ord + Measured, T: Measured>(
        &mut self,
        map: &mut BTreeMap<K, T>,
        key: K,
        value: T,
    ) -> Option<T> {
        if memory::capture().is_none() {
            return map.insert(key, value);
        }
        let key_size = key.footprint();
        let next = value.footprint();
        let overlap = self
            .stable
            .combine(self.remaining)
            .combine(self.retained)
            .combine(self.pending)
            .combine(key_size)
            .combine(next);
        self.peak.bytes = self.peak.bytes.max(overlap.bytes);
        self.peak.owned = self.peak.owned.max(overlap.owned);
        let old = map.insert(key, value);
        if let Some(old) = &old {
            let f = old.footprint();
            self.retained.bytes = memory::subtract(self.retained.bytes, f.bytes);
            self.retained.owned = memory::subtract(self.retained.owned, f.owned);
        } else {
            self.retained = self.retained.combine(key_size);
        }
        self.retained = self.retained.combine(next);
        old
    }
}
impl Drop for FactOverlap {
    fn drop(&mut self) {
        memory::observe("selected_facts_overlap_peak", || self.peak);
    }
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
