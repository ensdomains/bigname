use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedSourceSelectorKind, WatchedSourceSelectorPlan};
use bigname_storage::{
    CanonicalityState, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert, RawReceipt,
    RawTransaction, upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs,
    upsert_raw_payload_cache_metadata, upsert_raw_receipts, upsert_raw_transactions,
};
use tracing::info;

use crate::{
    provider::{
        JsonRpcProvider, ProviderBlockCodeObservationRequest, ProviderLog, ProviderResolvedBlock,
    },
    reconciliation::{
        ensure_provider_bundle_matches_raw_block, provider_block_to_raw_block,
        provider_code_observation_to_raw_code_hash, provider_logs_to_selected_raw_logs,
        provider_raw_payload_cache_metadata_to_upserts, provider_receipts_to_selected_raw_receipts,
        provider_transactions_to_selected_raw_transactions,
        retained_transaction_keys_from_raw_logs, sync_adapter_state_from_persisted_raw_payloads,
        sync_adapter_state_from_scoped_persisted_raw_payloads,
    },
};

use super::{
    BackfillBlockRange, BackfillOutcome,
    range_resolution::resolve_backfill_range,
    selection::{
        selected_log_range_requests, selected_target_addresses_at_block, selected_target_sync_scope,
    },
};

pub(crate) async fn run_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &JsonRpcProvider,
    range: BackfillBlockRange,
) -> Result<BackfillOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    let source_scope = selected_target_sync_scope(source_plan);
    let resolved_blocks = resolve_backfill_range(provider, range).await?;
    let block_hashes = resolved_blocks
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let fetch_logs_by_safe_ranges = resolved_blocks.len() > 1;
    let mut ranged_logs_by_block = if fetch_logs_by_safe_ranges {
        fetch_backfill_logs_by_safe_ranges(provider, source_plan, &resolved_blocks, range).await?
    } else {
        BTreeMap::new()
    };
    let single_block_selected_addresses = resolved_blocks
        .first()
        .map(|block| selected_target_addresses_at_block(source_plan, block.block_number))
        .unwrap_or_default();
    let bundles = if fetch_logs_by_safe_ranges || single_block_selected_addresses.is_empty() {
        provider
            .fetch_block_bundles_without_logs_by_hashes(&resolved_blocks)
            .await
    } else {
        provider
            .fetch_block_bundles_by_hashes(&resolved_blocks)
            .await
    }
    .with_context(|| {
        format!(
            "failed to fetch hash-pinned payload batch for chain {} range {}..={}",
            watched_chain.chain, range.from_block, range.to_block
        )
    })?;
    let mut raw_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();
    let mut cache_metadata = Vec::<RawPayloadCacheMetadataUpsert>::new();
    let mut raw_blocks_by_hash = BTreeMap::new();
    let mut code_observation_requests = Vec::new();

    for (resolved_block, bundle) in resolved_blocks.iter().zip(bundles.iter()) {
        let selected_addresses =
            selected_target_addresses_at_block(source_plan, resolved_block.block_number);
        if bundle.block.block_number != resolved_block.block_number {
            bail!(
                "provider resolved chain {} block number {} to hash {}, but hash-scoped fetch returned block number {}",
                watched_chain.chain,
                resolved_block.block_number,
                resolved_block.block_hash,
                bundle.block.block_number
            );
        }

        let raw_block = provider_block_to_raw_block(
            &watched_chain.chain,
            &bundle.block,
            CanonicalityState::Observed,
        );
        ensure_provider_bundle_matches_raw_block(&raw_block, &bundle)?;

        cache_metadata.extend(provider_raw_payload_cache_metadata_to_upserts(
            &watched_chain.chain,
            &raw_block,
            &bundle.raw_payloads,
        ));
        let block_logs = if fetch_logs_by_safe_ranges {
            ranged_logs_by_block
                .remove(&resolved_block.block_number)
                .unwrap_or_default()
        } else {
            bundle.logs.clone()
        };
        let selected_logs = provider_logs_to_selected_raw_logs(
            &watched_chain.chain,
            &raw_block,
            &block_logs,
            &selected_addresses,
        )?;
        let retained_transaction_keys = retained_transaction_keys_from_raw_logs(&selected_logs);
        transactions.extend(provider_transactions_to_selected_raw_transactions(
            &watched_chain.chain,
            &raw_block,
            &bundle.transactions,
            &retained_transaction_keys,
        )?);
        receipts.extend(provider_receipts_to_selected_raw_receipts(
            &watched_chain.chain,
            &raw_block,
            &bundle.receipts,
            &retained_transaction_keys,
        )?);
        logs.extend(selected_logs);

        if !selected_addresses.is_empty() {
            code_observation_requests.push(ProviderBlockCodeObservationRequest {
                block_hash: raw_block.block_hash.clone(),
                addresses: selected_addresses.iter().cloned().collect(),
            });
            raw_blocks_by_hash.insert(raw_block.block_hash.clone(), raw_block.clone());
        }

        raw_blocks.push(raw_block);
    }
    if !ranged_logs_by_block.is_empty() {
        bail!("provider returned range logs for unprocessed backfill blocks");
    }

    let code_observation_batches = provider
        .fetch_code_observations_at_block_hashes(&code_observation_requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch hash-pinned code observation batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
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

    upsert_raw_blocks(pool, &raw_blocks).await?;
    upsert_raw_payload_cache_metadata(pool, &cache_metadata).await?;
    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    if source_plan.selector_kind == WatchedSourceSelectorKind::WholeActiveWatchedChain {
        sync_adapter_state_from_persisted_raw_payloads(pool, &watched_chain.chain, &block_hashes)
            .await?;
    } else {
        sync_adapter_state_from_scoped_persisted_raw_payloads(
            pool,
            &watched_chain.chain,
            &block_hashes,
            &source_scope,
        )
        .await?;
    }

    let outcome = BackfillOutcome {
        chain: watched_chain.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: resolved_blocks.len(),
        raw_block_count: raw_blocks.len(),
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

async fn fetch_backfill_logs_by_safe_ranges(
    provider: &JsonRpcProvider,
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    let mut logs_by_block = BTreeMap::new();
    for request in selected_log_range_requests(source_plan, resolved_blocks) {
        let request_blocks = &resolved_blocks[request.start_index..request.end_index];
        let from_block = request_blocks
            .first()
            .expect("selected log range request must contain at least one block")
            .block_number;
        let to_block = request_blocks
            .last()
            .expect("selected log range request must contain at least one block")
            .block_number;
        let group_logs = provider
            .fetch_logs_by_block_range(request_blocks, &request.addresses)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch hash-pinned log range {}..={} inside backfill range {}..={}",
                    from_block, to_block, range.from_block, range.to_block
                )
            })?;

        for (block_number, logs) in group_logs {
            if logs_by_block.insert(block_number, logs).is_some() {
                bail!("provider returned duplicate range logs for backfill block {block_number}");
            }
        }
    }

    Ok(logs_by_block)
}
