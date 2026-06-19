use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    CanonicalityState, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert, normalize_evm_address,
    normalize_evm_b256, upsert_chain_lineage_blocks_without_snapshots, upsert_raw_code_hashes,
    upsert_raw_logs_without_snapshots, upsert_raw_payload_cache_metadata,
    upsert_raw_receipts_without_snapshots, upsert_raw_transactions_without_snapshots,
};
use sqlx::Row;
use tracing::info;

use crate::{
    provider::{ChainProviderOps, ProviderLog, ProviderResolvedBlock},
    reconciliation::{
        HeaderAuditMode, provider_block_to_lineage_with_header_audit_mode,
        provider_block_to_raw_block_with_header_audit_mode,
        provider_code_observation_to_raw_code_hash, provider_raw_payload_cache_metadata_to_upserts,
        provider_receipt_to_raw_receipt, provider_transaction_to_raw_transaction,
    },
};

use super::{
    BackfillCanonicalityEvidence, RawOnlySparseBackfillTiming,
    materialization::{
        fetch_full_payload_bundles_for_log_blocks, materialize_backfill_block_payloads,
        missing_transaction_receipt_requests_from_raw_facts,
    },
    selected_addresses_for_materialized_block, uses_topic_first_source_family_scan,
};
use crate::backfill::selection::SelectedTargetIntervalIndex;
use crate::backfill::{BackfillAdapterSyncMode, BackfillBlockRange, BackfillOutcome};

#[path = "sparse/plan.rs"]
mod plan;

use plan::SparseCodeObservationPlan;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RawOnlySparseMaterialization {
    pub(super) range: BackfillBlockRange,
    pub(super) resolved_blocks: Vec<ProviderResolvedBlock>,
    pub(super) logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
}

pub(super) fn raw_only_sparse_materialization_slices(
    resolved_blocks: &[ProviderResolvedBlock],
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
    max_raw_logs_per_push: usize,
) -> Result<Vec<RawOnlySparseMaterialization>> {
    if resolved_blocks.is_empty() {
        return Ok(Vec::new());
    }
    let max_raw_logs_per_push = max_raw_logs_per_push.max(1);
    let mut materializations = Vec::new();
    let mut start_index = 0usize;
    let mut log_count = 0usize;

    for (index, block) in resolved_blocks.iter().enumerate() {
        let block_log_count = logs_by_block
            .get(&block.block_number)
            .map(Vec::len)
            .unwrap_or(0);
        if index > start_index
            && log_count > 0
            && log_count + block_log_count > max_raw_logs_per_push
        {
            materializations.push(raw_only_sparse_materialization_slice(
                resolved_blocks,
                logs_by_block,
                start_index,
                index,
            )?);
            start_index = index;
            log_count = 0;
        }

        log_count += block_log_count;
        if log_count > max_raw_logs_per_push {
            materializations.push(raw_only_sparse_materialization_slice(
                resolved_blocks,
                logs_by_block,
                start_index,
                index + 1,
            )?);
            start_index = index + 1;
            log_count = 0;
        }
    }

    if start_index < resolved_blocks.len() {
        materializations.push(raw_only_sparse_materialization_slice(
            resolved_blocks,
            logs_by_block,
            start_index,
            resolved_blocks.len(),
        )?);
    }

    Ok(materializations)
}

fn raw_only_sparse_materialization_slice(
    resolved_blocks: &[ProviderResolvedBlock],
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
    start_index: usize,
    end_index: usize,
) -> Result<RawOnlySparseMaterialization> {
    let slice = &resolved_blocks[start_index..end_index];
    let from_block = slice
        .first()
        .context("raw-only sparse materialization slice must not be empty")?
        .block_number;
    let to_block = slice
        .last()
        .context("raw-only sparse materialization slice must not be empty")?
        .block_number;
    let mut slice_logs_by_block = BTreeMap::new();
    for block in slice {
        if let Some(logs) = logs_by_block.get(&block.block_number) {
            slice_logs_by_block.insert(block.block_number, logs.clone());
        }
    }

    Ok(RawOnlySparseMaterialization {
        range: BackfillBlockRange {
            from_block,
            to_block,
        },
        resolved_blocks: slice.to_vec(),
        logs_by_block: slice_logs_by_block,
    })
}

pub(super) async fn run_split_hash_pinned_raw_only_sparse_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    materializations: Vec<RawOnlySparseMaterialization>,
    timing: RawOnlySparseBackfillTiming,
    header_audit_mode: HeaderAuditMode,
) -> Result<BackfillOutcome> {
    let mut merged = BackfillOutcome {
        chain: source_plan.watched_chain_plan.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: 0,
        raw_block_count: 0,
        raw_transaction_count: 0,
        raw_receipt_count: 0,
        raw_log_count: 0,
        raw_code_hash_count: 0,
    };
    for materialization in materializations {
        let outcome = run_hash_pinned_raw_only_sparse_backfill_range(
            pool,
            source_plan,
            selected_target_index,
            provider,
            materialization.range,
            canonicality_evidence.clone(),
            materialization.resolved_blocks,
            materialization.logs_by_block,
            RawOnlySparseBackfillTiming {
                total_started: Instant::now(),
                resolve_ms: 0,
                range_logs_ms: 0,
            },
            header_audit_mode,
        )
        .await?;
        merged.resolved_block_count += outcome.resolved_block_count;
        merged.raw_block_count += outcome.raw_block_count;
        merged.raw_transaction_count += outcome.raw_transaction_count;
        merged.raw_receipt_count += outcome.raw_receipt_count;
        merged.raw_log_count += outcome.raw_log_count;
        merged.raw_code_hash_count += outcome.raw_code_hash_count;
    }
    info!(
        service = "indexer",
        command = "backfill",
        chain = %merged.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        resolved_block_count = merged.resolved_block_count,
        raw_block_count = merged.raw_block_count,
        raw_transaction_count = merged.raw_transaction_count,
        raw_receipt_count = merged.raw_receipt_count,
        raw_log_count = merged.raw_log_count,
        raw_code_hash_count = merged.raw_code_hash_count,
        resolve_ms = timing.resolve_ms,
        range_logs_ms = timing.range_logs_ms,
        total_ms = timing.total_started.elapsed().as_millis(),
        "hash-pinned raw-only sparse split backfill completed"
    );

    Ok(merged)
}

pub(super) async fn run_hash_pinned_raw_only_sparse_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    resolved_blocks: Vec<ProviderResolvedBlock>,
    mut ranged_logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
    timing: RawOnlySparseBackfillTiming,
    header_audit_mode: HeaderAuditMode,
) -> Result<BackfillOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    ranged_logs_by_block.retain(|_, logs| !logs.is_empty());
    let topic_filtered_source_family = uses_topic_first_source_family_scan(source_plan);
    let mut full_payload_bundles_by_hash = fetch_full_payload_bundles_for_log_blocks(
        provider,
        &resolved_blocks,
        &ranged_logs_by_block,
        &watched_chain.chain,
        range,
        "hash-pinned raw-only sparse",
    )
    .await?;

    let headers_started = Instant::now();
    let blocks = provider
        .fetch_block_headers_by_hashes(&resolved_blocks)
        .await
        .with_context(|| {
            format!(
                "failed to fetch hash-pinned block header batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
    let headers_ms = headers_started.elapsed().as_millis();

    let materialize_started = Instant::now();
    let mut transactions = Vec::new();
    let mut receipts = Vec::new();
    let mut logs = Vec::<RawLog>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();
    let mut cache_metadata = Vec::<RawPayloadCacheMetadataUpsert>::new();
    let mut raw_blocks_by_hash = BTreeMap::new();
    let mut lineage_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut code_observation_plan = SparseCodeObservationPlan::default();
    let canonicality_states = canonicality_evidence
        .states_for_blocks(pool, &watched_chain.chain, provider, &blocks)
        .await?;

    for (resolved_block, block) in resolved_blocks.iter().zip(blocks.iter()) {
        if block.block_hash != resolved_block.block_hash {
            bail!(
                "provider returned block {} for requested hash {}",
                block.block_hash,
                resolved_block.block_hash
            );
        }
        if block.block_number != resolved_block.block_number {
            bail!(
                "provider resolved chain {} block number {} to hash {}, but hash-scoped fetch returned block number {}",
                watched_chain.chain,
                resolved_block.block_number,
                resolved_block.block_hash,
                block.block_number
            );
        }

        let canonicality_state = canonicality_states
            .get(&block.block_hash)
            .copied()
            .unwrap_or(bigname_storage::CanonicalityState::Observed);
        let raw_block = provider_block_to_raw_block_with_header_audit_mode(
            &watched_chain.chain,
            block,
            canonicality_state,
            header_audit_mode,
        );
        lineage_blocks.push(provider_block_to_lineage_with_header_audit_mode(
            &watched_chain.chain,
            block,
            canonicality_state,
            header_audit_mode,
        ));
        let selection_logs = ranged_logs_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
        let selected_addresses = selected_addresses_for_materialized_block(
            source_plan,
            selected_target_index,
            topic_filtered_source_family,
            resolved_block.block_number,
            &selection_logs,
        );
        if let Some(full_payload_bundle) = full_payload_bundles_by_hash.remove(&block.block_hash) {
            cache_metadata.extend(provider_raw_payload_cache_metadata_to_upserts(
                &watched_chain.chain,
                &raw_block,
                &full_payload_bundle.raw_payloads,
            ));
            let materialized_payloads = materialize_backfill_block_payloads(
                &watched_chain.chain,
                &raw_block,
                &selection_logs,
                &full_payload_bundle.logs,
                &full_payload_bundle.transactions,
                &full_payload_bundle.receipts,
                &selected_addresses,
            )?;
            transactions.extend(materialized_payloads.transactions);
            receipts.extend(materialized_payloads.receipts);
            logs.extend(materialized_payloads.logs);
        }

        if !selected_addresses.is_empty() {
            code_observation_plan.record(&raw_block, &selected_addresses);
        }

        raw_blocks_by_hash.insert(raw_block.block_hash.clone(), raw_block.clone());
    }
    if !ranged_logs_by_block.is_empty() {
        bail!("provider returned range logs for unprocessed backfill blocks");
    }
    if !full_payload_bundles_by_hash.is_empty() {
        bail!("provider returned full payloads for unprocessed sparse backfill blocks");
    }
    let materialize_ms = materialize_started.elapsed().as_millis();

    let stored_code_hash_observations = load_stored_sparse_code_observation_pairs(
        pool,
        &watched_chain.chain,
        &code_observation_plan.block_hashes(),
        &code_observation_plan.contract_addresses(),
    )
    .await?;
    code_observation_plan.retain_missing_stored_observations(&stored_code_hash_observations);

    let transaction_receipt_requests =
        missing_transaction_receipt_requests_from_raw_facts(&logs, &transactions, &receipts);
    let transaction_receipts_started = Instant::now();
    let transaction_receipt_pairs = provider
        .fetch_transaction_receipt_pairs_by_hashes(&transaction_receipt_requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch selected transaction/receipt batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
    let transaction_receipts_ms = transaction_receipts_started.elapsed().as_millis();
    let transaction_materialize_started = Instant::now();
    for pair in transaction_receipt_pairs {
        let raw_block = raw_blocks_by_hash
            .get(&pair.transaction.block_hash)
            .with_context(|| {
                format!(
                    "provider returned selected transaction {} for unprocessed block {}",
                    pair.transaction.transaction_hash, pair.transaction.block_hash
                )
            })?;
        transactions.push(provider_transaction_to_raw_transaction(
            &watched_chain.chain,
            raw_block,
            &pair.transaction,
        )?);
        receipts.push(provider_receipt_to_raw_receipt(
            &watched_chain.chain,
            raw_block,
            &pair.receipt,
        )?);
    }
    let transaction_materialize_ms = transaction_materialize_started.elapsed().as_millis();

    let code_started = Instant::now();
    let code_observation_requests = code_observation_plan.requests();
    let code_observation_batches = if code_observation_requests.is_empty() {
        Vec::new()
    } else {
        provider
            .fetch_code_observations_at_block_hashes(&code_observation_requests)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch hash-pinned code observation batch for chain {} range {}..={}",
                    watched_chain.chain, range.from_block, range.to_block
                )
            })?
    };
    let code_fetch_ms = code_started.elapsed().as_millis();
    let code_materialize_started = Instant::now();
    for batch in code_observation_batches {
        let raw_block = raw_blocks_by_hash.get(&batch.block_hash).with_context(|| {
            format!(
                "provider returned code observations for unrequested block hash {}",
                batch.block_hash
            )
        })?;
        code_hashes.extend(
            batch
                .observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(
                        &watched_chain.chain,
                        raw_block,
                        observation,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }
    let code_materialize_ms = code_materialize_started.elapsed().as_millis();

    let header_anchor_upsert_started = Instant::now();
    upsert_chain_lineage_blocks_without_snapshots(pool, &lineage_blocks).await?;
    let header_anchor_upsert_ms = header_anchor_upsert_started.elapsed().as_millis();
    let cache_metadata_upsert_started = Instant::now();
    upsert_raw_payload_cache_metadata(pool, &cache_metadata).await?;
    let cache_metadata_upsert_ms = cache_metadata_upsert_started.elapsed().as_millis();
    let transactions_upsert_started = Instant::now();
    upsert_raw_transactions_without_snapshots(pool, &transactions).await?;
    let transactions_upsert_ms = transactions_upsert_started.elapsed().as_millis();
    let receipts_upsert_started = Instant::now();
    upsert_raw_receipts_without_snapshots(pool, &receipts).await?;
    let receipts_upsert_ms = receipts_upsert_started.elapsed().as_millis();
    let logs_upsert_started = Instant::now();
    upsert_raw_logs_without_snapshots(pool, &logs).await?;
    let logs_upsert_ms = logs_upsert_started.elapsed().as_millis();
    let code_hashes_upsert_started = Instant::now();
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    let code_hashes_upsert_ms = code_hashes_upsert_started.elapsed().as_millis();
    if !logs.is_empty() {
        info!(
            service = "indexer",
            command = "backfill",
            chain = %watched_chain.chain,
            from_block = range.from_block,
            to_block = range.to_block,
            raw_log_count = logs.len(),
            adapter_sync_mode = BackfillAdapterSyncMode::RawOnly.as_str(),
            "hash-pinned backfill adapter sync skipped after raw fact persistence"
        );
    }
    info!(
        service = "indexer",
        command = "backfill",
        chain = %watched_chain.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        resolved_block_count = resolved_blocks.len(),
        raw_block_count = resolved_blocks.len(),
        raw_log_count = logs.len(),
        raw_transaction_count = transactions.len(),
        resolve_ms = timing.resolve_ms,
        range_logs_ms = timing.range_logs_ms,
        headers_ms,
        materialize_ms,
        transaction_receipts_ms,
        transaction_materialize_ms,
        code_fetch_ms,
        code_materialize_ms,
        header_anchor_upsert_ms,
        cache_metadata_upsert_ms,
        transactions_upsert_ms,
        receipts_upsert_ms,
        logs_upsert_ms,
        code_hashes_upsert_ms,
        total_ms = timing.total_started.elapsed().as_millis(),
        "hash-pinned raw-only sparse backfill timing"
    );

    let outcome = BackfillOutcome {
        chain: watched_chain.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: resolved_blocks.len(),
        raw_block_count: resolved_blocks.len(),
        raw_transaction_count: transactions.len(),
        raw_receipt_count: receipts.len(),
        raw_log_count: logs.len(),
        raw_code_hash_count: code_hashes.len(),
    };
    info!(
        service = "indexer",
        command = "backfill",
        chain = %outcome.chain,
        from_block = outcome.from_block,
        to_block = outcome.to_block,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        "hash-pinned backfill range completed"
    );

    Ok(outcome)
}

async fn load_stored_sparse_code_observation_pairs(
    pool: &sqlx::PgPool,
    chain_id: &str,
    block_hashes: &[String],
    contract_addresses: &[String],
) -> Result<BTreeMap<(String, String), CanonicalityState>> {
    if block_hashes.is_empty() || contract_addresses.is_empty() {
        return Ok(BTreeMap::new());
    }
    let block_hashes = block_hashes
        .iter()
        .map(|block_hash| normalize_evm_b256(block_hash))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let contract_addresses = contract_addresses
        .iter()
        .map(|address| normalize_evm_address(address))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT
            block_hash,
            contract_address,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_code_hashes
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND contract_address = ANY($3::TEXT[])
        "#,
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .bind(&contract_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load stored sparse code observations for chain {chain_id} across {} hashes and {} contracts",
            block_hashes.len(),
            contract_addresses.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let block_hash = row
                .try_get::<String, _>("block_hash")
                .context("missing block_hash from stored sparse code observation row")?;
            let contract_address = row
                .try_get::<String, _>("contract_address")
                .context("missing contract_address from stored sparse code observation row")?;
            let canonicality_state = row
                .try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state from stored sparse code observation row")?;
            Ok((
                (block_hash, contract_address),
                CanonicalityState::parse(&canonicality_state)?,
            ))
        })
        .collect()
}

#[cfg(test)]
#[path = "tests/sparse.rs"]
mod tests;
