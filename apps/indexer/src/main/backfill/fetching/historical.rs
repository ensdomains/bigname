#[path = "historical/log_payloads.rs"]
mod log_payloads;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedSourceSelectorKind, WatchedSourceSelectorPlan};
use bigname_storage::{
    CanonicalityState, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert, RawReceipt,
    RawTransaction, upsert_chain_lineage_blocks, upsert_raw_code_hashes, upsert_raw_logs,
    upsert_raw_payload_cache_metadata, upsert_raw_receipts, upsert_raw_transactions,
};
use tracing::info;

use crate::{
    provider::{
        ChainProviderOps, ProviderBlock, ProviderBlockCodeObservationRequest, ProviderLog,
        ProviderResolvedBlock,
    },
    reconciliation::{
        HeaderAuditMode, provider_block_to_lineage_with_header_audit_mode,
        provider_block_to_raw_block_with_header_audit_mode,
        provider_code_observation_to_raw_code_hash, provider_raw_payload_cache_metadata_to_upserts,
        provider_receipt_to_raw_receipt, provider_transaction_to_raw_transaction,
        sync_adapter_state_from_persisted_raw_payloads,
        sync_adapter_state_from_scoped_persisted_raw_payloads,
    },
    source_scope::SourceScope,
};
pub(crate) use log_payloads::fill_log_payloads_from_validation_provider;

use super::{
    BackfillCanonicalityEvidence,
    log_ranges::{selected_addresses_for_materialized_block, uses_topic_first_source_family_scan},
    materialization::{
        fetch_full_payload_bundles_for_log_blocks, materialize_backfill_block_payloads,
        missing_transaction_receipt_requests_from_raw_facts, selected_seed_log_addresses,
    },
};
use crate::backfill::{
    BackfillAdapterSyncMode, BackfillBlockRange, BackfillOutcome, HistoricalLogPayload,
    selection::{SelectedTargetIntervalIndex, selected_target_addresses_at_block},
};

pub(crate) async fn materialize_historical_payload_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    resolved_blocks: &[ProviderResolvedBlock],
    block_headers: Vec<ProviderBlock>,
    mut historical_payload: HistoricalLogPayload,
    adapter_sync_mode: BackfillAdapterSyncMode,
    header_audit_mode: HeaderAuditMode,
) -> Result<BackfillOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    let chain = watched_chain.chain.as_str();

    let logs_filtered_by_selected_target_index =
        historical_payload.logs_filtered_by_selected_target_index;
    let block_hashes = resolved_blocks
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    ensure_headers_match_resolved_blocks(resolved_blocks, &block_headers)?;
    if historical_payload.logs_need_validation_provider_payload {
        historical_payload.logs_by_block = fill_log_payloads_from_validation_provider(
            validation_provider,
            resolved_blocks,
            historical_payload.logs_by_block,
            &historical_payload.validation_filters,
            historical_payload.validation_mode,
        )
        .await?;
    }
    let canonicality_states = canonicality_evidence
        .states_for_blocks(pool, chain, validation_provider, &block_headers)
        .await?;
    let mut full_payload_bundles_by_hash = fetch_full_payload_bundles_for_log_blocks(
        validation_provider,
        resolved_blocks,
        &historical_payload.logs_by_block,
        chain,
        range,
        "historical backfill",
    )
    .await?;
    let topic_filtered_source_family = uses_topic_first_source_family_scan(source_plan);
    let mut raw_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut lineage_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut logs = Vec::<RawLog>::new();
    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();
    let mut cache_metadata = Vec::<RawPayloadCacheMetadataUpsert>::new();
    let mut raw_blocks_by_hash = BTreeMap::new();
    let mut code_observation_requests = Vec::new();

    for (resolved_block, block_header) in resolved_blocks.iter().zip(block_headers.iter()) {
        let canonicality_state = canonicality_states
            .get(&block_header.block_hash)
            .copied()
            .unwrap_or(CanonicalityState::Observed);
        let raw_block = provider_block_to_raw_block_with_header_audit_mode(
            chain,
            block_header,
            canonicality_state,
            header_audit_mode,
        );
        lineage_blocks.push(provider_block_to_lineage_with_header_audit_mode(
            chain,
            block_header,
            canonicality_state,
            header_audit_mode,
        ));

        let selection_logs = historical_payload
            .logs_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
        let selected_addresses = selected_addresses_for_payload_block(
            source_plan,
            selected_target_index,
            topic_filtered_source_family,
            logs_filtered_by_selected_target_index,
            resolved_block.block_number,
            &selection_logs,
        );
        let source_transactions = historical_payload
            .transactions_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
        let source_receipts = historical_payload
            .receipts_by_block
            .remove(&resolved_block.block_number)
            .unwrap_or_default();
        let (payload_logs, payload_transactions, payload_receipts) =
            if let Some(full_payload_bundle) =
                full_payload_bundles_by_hash.remove(&block_header.block_hash)
            {
                cache_metadata.extend(provider_raw_payload_cache_metadata_to_upserts(
                    chain,
                    &raw_block,
                    &full_payload_bundle.raw_payloads,
                ));
                (
                    full_payload_bundle.logs,
                    full_payload_bundle.transactions,
                    full_payload_bundle.receipts,
                )
            } else {
                (selection_logs.clone(), source_transactions, source_receipts)
            };
        let materialized_payloads = materialize_backfill_block_payloads(
            chain,
            &raw_block,
            &selection_logs,
            &payload_logs,
            &payload_transactions,
            &payload_receipts,
            &selected_addresses,
        )?;
        transactions.extend(materialized_payloads.transactions);
        receipts.extend(materialized_payloads.receipts);
        logs.extend(materialized_payloads.logs);

        let code_observation_addresses =
            selected_seed_log_addresses(&selection_logs, &selected_addresses);
        if !code_observation_addresses.is_empty() {
            code_observation_requests.push(ProviderBlockCodeObservationRequest {
                block_hash: raw_block.block_hash.clone(),
                addresses: code_observation_addresses,
            });
        }
        raw_blocks_by_hash.insert(raw_block.block_hash.clone(), raw_block.clone());
        raw_blocks.push(raw_block);
    }
    ensure_no_unprocessed_payloads(&historical_payload)?;
    if !full_payload_bundles_by_hash.is_empty() {
        bail!(
            "validation provider returned full payloads for unprocessed historical backfill blocks"
        );
    }
    fill_missing_transaction_receipts(
        validation_provider,
        chain,
        range,
        &raw_blocks_by_hash,
        &logs,
        &mut transactions,
        &mut receipts,
    )
    .await?;
    fetch_code_observations(
        validation_provider,
        chain,
        range,
        &raw_blocks_by_hash,
        &code_observation_requests,
        &mut code_hashes,
    )
    .await?;

    upsert_chain_lineage_blocks(pool, &lineage_blocks).await?;
    upsert_raw_payload_cache_metadata(pool, &cache_metadata).await?;
    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    let adapter_sync_scope = adapter_sync_scope_for_materialized_range(source_plan, range);
    maybe_sync_adapters(
        pool,
        source_plan,
        range,
        &block_hashes,
        &logs,
        adapter_sync_mode,
        &adapter_sync_scope,
    )
    .await?;

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
        "historical backfill range completed"
    );

    Ok(outcome)
}

fn selected_addresses_for_payload_block(
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    topic_filtered_source_family: bool,
    logs_filtered_by_selected_target_index: bool,
    block_number: i64,
    block_logs: &[ProviderLog],
) -> BTreeSet<String> {
    if logs_filtered_by_selected_target_index {
        return selected_target_addresses_at_block(source_plan, block_number);
    }
    if source_plan.selector_kind == WatchedSourceSelectorKind::SourceFamily
        && source_plan.source_family.as_deref() == Some("basenames_base_registry")
    {
        return block_logs
            .iter()
            .map(|log| log.address.to_ascii_lowercase())
            .collect();
    }

    selected_addresses_for_materialized_block(
        source_plan,
        selected_target_index,
        topic_filtered_source_family,
        block_number,
        block_logs,
    )
}

fn adapter_sync_scope_for_materialized_range(
    source_plan: &WatchedSourceSelectorPlan,
    range: BackfillBlockRange,
) -> Vec<(String, String, i64, i64)> {
    SourceScope::from_watched_source_plan(source_plan, range.from_block, range.to_block)
        .adapter_sync_scope()
}

fn ensure_headers_match_resolved_blocks(
    resolved_blocks: &[ProviderResolvedBlock],
    block_headers: &[ProviderBlock],
) -> Result<()> {
    if resolved_blocks.len() != block_headers.len() {
        bail!(
            "validation provider returned {} block headers for {} resolved blocks",
            block_headers.len(),
            resolved_blocks.len()
        );
    }
    for (resolved_block, block_header) in resolved_blocks.iter().zip(block_headers.iter()) {
        if block_header.block_number != resolved_block.block_number
            || !block_header
                .block_hash
                .eq_ignore_ascii_case(&resolved_block.block_hash)
        {
            bail!(
                "validation provider header {}:{} does not match resolved block {}:{}",
                block_header.block_number,
                block_header.block_hash,
                resolved_block.block_number,
                resolved_block.block_hash
            );
        }
    }

    Ok(())
}

fn ensure_no_unprocessed_payloads(historical_payload: &HistoricalLogPayload) -> Result<()> {
    if !historical_payload.logs_by_block.is_empty() {
        bail!("historical backfill source returned logs for unprocessed blocks");
    }
    if !historical_payload.transactions_by_block.is_empty() {
        bail!("historical backfill source returned transactions for unprocessed blocks");
    }
    if !historical_payload.receipts_by_block.is_empty() {
        bail!("historical backfill source returned receipts for unprocessed blocks");
    }

    Ok(())
}

async fn fill_missing_transaction_receipts(
    validation_provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    range: BackfillBlockRange,
    raw_blocks_by_hash: &BTreeMap<String, bigname_storage::RawBlock>,
    logs: &[RawLog],
    transactions: &mut Vec<RawTransaction>,
    receipts: &mut Vec<RawReceipt>,
) -> Result<()> {
    let requests =
        missing_transaction_receipt_requests_from_raw_facts(logs, transactions, receipts);
    let pairs = validation_provider
        .fetch_transaction_receipt_pairs_by_hashes(&requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch selected transaction/receipt batch from validation provider for chain {chain} range {}..={}",
                range.from_block, range.to_block
            )
        })?;
    let mut transaction_keys = transactions
        .iter()
        .map(|transaction| {
            (
                transaction.block_hash.clone(),
                transaction.transaction_hash.clone(),
                transaction.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    let mut receipt_keys = receipts
        .iter()
        .map(|receipt| {
            (
                receipt.block_hash.clone(),
                receipt.transaction_hash.clone(),
                receipt.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    for pair in pairs {
        let raw_block = raw_blocks_by_hash
            .get(&pair.transaction.block_hash)
            .with_context(|| {
                format!(
                    "validation provider returned selected transaction {} for unprocessed block {}",
                    pair.transaction.transaction_hash, pair.transaction.block_hash
                )
            })?;
        let key = (
            pair.transaction.block_hash.clone(),
            pair.transaction.transaction_hash.clone(),
            pair.transaction.transaction_index,
        );
        if transaction_keys.insert(key.clone()) {
            transactions.push(provider_transaction_to_raw_transaction(
                chain,
                raw_block,
                &pair.transaction,
            )?);
        }
        if receipt_keys.insert(key) {
            receipts.push(provider_receipt_to_raw_receipt(
                chain,
                raw_block,
                &pair.receipt,
            )?);
        }
    }

    Ok(())
}

async fn fetch_code_observations(
    validation_provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    range: BackfillBlockRange,
    raw_blocks_by_hash: &BTreeMap<String, bigname_storage::RawBlock>,
    code_observation_requests: &[ProviderBlockCodeObservationRequest],
    code_hashes: &mut Vec<RawCodeHash>,
) -> Result<()> {
    let batches = validation_provider
        .fetch_code_observations_at_block_hashes(code_observation_requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch historical code observation batch for chain {chain} range {}..={}",
                range.from_block, range.to_block
            )
        })?;
    for batch in batches {
        let raw_block = raw_blocks_by_hash.get(&batch.block_hash).with_context(|| {
            format!(
                "validation provider returned code observations for unrequested block hash {}",
                batch.block_hash
            )
        })?;
        code_hashes.extend(
            batch
                .observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(chain, raw_block, observation)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    Ok(())
}

async fn maybe_sync_adapters(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    range: BackfillBlockRange,
    block_hashes: &[String],
    logs: &[RawLog],
    adapter_sync_mode: BackfillAdapterSyncMode,
    adapter_sync_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    let chain = source_plan.watched_chain_plan.chain.as_str();
    if !logs.is_empty() && adapter_sync_mode == BackfillAdapterSyncMode::Inline {
        if source_plan.selector_kind == WatchedSourceSelectorKind::WholeActiveWatchedChain {
            sync_adapter_state_from_persisted_raw_payloads(pool, chain, block_hashes).await?;
        } else {
            sync_adapter_state_from_scoped_persisted_raw_payloads(
                pool,
                chain,
                block_hashes,
                adapter_sync_scope,
            )
            .await?;
        }
    } else if !logs.is_empty() {
        info!(
            service = "indexer",
            command = "backfill",
            chain,
            from_block = range.from_block,
            to_block = range.to_block,
            raw_log_count = logs.len(),
            adapter_sync_mode = adapter_sync_mode.as_str(),
            "historical backfill adapter sync skipped after raw fact persistence"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_manifests::{
        WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind,
        WatchedSourceSelectorPlan,
    };
    use bigname_storage::RawBlock;

    use crate::provider::{ProviderReceipt, ProviderTransaction};

    const BLOCK_HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn provider_log(address: &str, block_number: i64) -> ProviderLog {
        ProviderLog {
            block_hash: BLOCK_HASH.to_owned(),
            block_number,
            transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            transaction_index: 0,
            log_index: 0,
            address: address.to_owned(),
            topics: Vec::new(),
            data: "0x".to_owned(),
        }
    }

    #[test]
    fn historical_code_observations_follow_selected_seed_log_emitters() {
        let selected_address = "0x0000000000000000000000000000000000000002";

        assert_eq!(
            selected_seed_log_addresses(
                &[provider_log(selected_address, 42)],
                &BTreeSet::from([selected_address.to_owned()]),
            ),
            vec![selected_address.to_owned()]
        );
    }

    #[test]
    fn historical_payload_replays_selected_source_scope() {
        let selected_address = "0x0000000000000000000000000000000000000001";
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("basenames_base_registry".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: sqlx::types::Uuid::nil(),
                address: selected_address.to_owned(),
                effective_from_block: 1,
                effective_to_block: 100,
            }],
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };

        assert_eq!(
            adapter_sync_scope_for_materialized_range(
                &source_plan,
                BackfillBlockRange::new(10, 20).unwrap(),
            ),
            vec![(
                "basenames_base_registry".to_owned(),
                selected_address.to_owned(),
                10,
                20,
            )]
        );
    }

    #[test]
    fn prefiltered_payload_uses_active_selected_targets_for_observations() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("basenames_base_registry".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(1),
                    address: "0x0000000000000000000000000000000000000001".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(2),
                    address: "0x0000000000000000000000000000000000000002".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
            ],
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };
        let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
        let logs = vec![provider_log(
            "0x0000000000000000000000000000000000000002",
            42,
        )];

        assert_eq!(
            selected_addresses_for_payload_block(
                &source_plan,
                &selected_target_index,
                false,
                true,
                42,
                &logs,
            ),
            BTreeSet::from([
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x0000000000000000000000000000000000000002".to_owned(),
            ])
        );
    }

    #[test]
    fn basenames_registry_materialization_uses_returned_log_emitters_for_observations() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("basenames_base_registry".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(1),
                    address: "0x0000000000000000000000000000000000000001".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(2),
                    address: "0x0000000000000000000000000000000000000002".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::from_u128(3),
                    address: "0x0000000000000000000000000000000000000003".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 100,
                },
            ],
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };
        let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
        let returned_emitter = "0x00000000000000000000000000000000000000ff";
        let logs = vec![provider_log(returned_emitter, 42)];

        assert_eq!(
            selected_addresses_for_payload_block(
                &source_plan,
                &selected_target_index,
                false,
                false,
                42,
                &logs,
            ),
            BTreeSet::from([returned_emitter.to_owned()])
        );
    }

    #[test]
    fn historical_materialization_retains_tx_sibling_logs() -> Result<()> {
        let raw_block = raw_block(42);
        let selected_address = "0x0000000000000000000000000000000000000001";
        let sibling_address = "0x00000000000000000000000000000000000000ff";
        let selected_tx_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let unrelated_tx_hash =
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let logs = vec![
            provider_log(selected_address, 42),
            ProviderLog {
                address: sibling_address.to_owned(),
                log_index: 1,
                ..provider_log(sibling_address, 42)
            },
            ProviderLog {
                transaction_hash: unrelated_tx_hash.to_owned(),
                transaction_index: 1,
                log_index: 2,
                ..provider_log(sibling_address, 42)
            },
        ];
        let transactions = vec![
            provider_transaction(selected_tx_hash, 0, 42),
            provider_transaction(unrelated_tx_hash, 1, 42),
        ];
        let receipts = vec![
            provider_receipt(selected_tx_hash, 0, 42),
            provider_receipt(unrelated_tx_hash, 1, 42),
        ];

        let materialized = materialize_backfill_block_payloads(
            "base-mainnet",
            &raw_block,
            &logs[..1],
            &logs,
            &transactions,
            &receipts,
            &BTreeSet::from([selected_address.to_owned()]),
        )?;

        assert_eq!(
            materialized
                .logs
                .iter()
                .map(|log| (log.emitting_address.as_str(), log.log_index))
                .collect::<Vec<_>>(),
            vec![(selected_address, 0), (sibling_address, 1)]
        );
        Ok(())
    }

    #[test]
    fn historical_missing_transaction_receipts_are_deduped_by_transaction() {
        let raw_block = raw_block(42);
        let first_log = bigname_storage::RawLog {
            chain_id: raw_block.chain_id.clone(),
            block_hash: raw_block.block_hash.clone(),
            block_number: raw_block.block_number,
            transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x0000000000000000000000000000000000000001".to_owned(),
            topics: Vec::new(),
            data: Vec::new(),
            canonicality_state: CanonicalityState::Observed,
        };
        let second_log = bigname_storage::RawLog {
            log_index: 1,
            emitting_address: "0x00000000000000000000000000000000000000ff".to_owned(),
            ..first_log.clone()
        };
        let existing_transaction = bigname_storage::RawTransaction {
            chain_id: raw_block.chain_id.clone(),
            block_hash: raw_block.block_hash.clone(),
            block_number: raw_block.block_number,
            transaction_hash: first_log.transaction_hash.clone(),
            transaction_index: first_log.transaction_index,
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: None,
            canonicality_state: CanonicalityState::Observed,
        };

        let requests = missing_transaction_receipt_requests_from_raw_facts(
            &[first_log, second_log],
            &[existing_transaction],
            &[],
        );

        assert_eq!(requests.len(), 1);
    }

    fn raw_block(block_number: i64) -> RawBlock {
        RawBlock {
            chain_id: "base-mainnet".to_owned(),
            block_hash: BLOCK_HASH.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Observed,
        }
    }

    fn provider_transaction(
        tx_hash: &str,
        tx_index: i64,
        block_number: i64,
    ) -> ProviderTransaction {
        ProviderTransaction {
            transaction_hash: tx_hash.to_owned(),
            block_hash: BLOCK_HASH.to_owned(),
            block_number,
            transaction_index: tx_index,
            from: "0x0000000000000000000000000000000000000001".to_owned(),
            to: Some("0x0000000000000000000000000000000000002".to_owned()),
        }
    }

    fn provider_receipt(tx_hash: &str, tx_index: i64, block_number: i64) -> ProviderReceipt {
        ProviderReceipt {
            transaction_hash: tx_hash.to_owned(),
            block_hash: BLOCK_HASH.to_owned(),
            block_number,
            transaction_index: tx_index,
            contract_address: None,
            status: Some(1),
            cumulative_gas_used: Some(21_000),
            gas_used: Some(21_000),
            logs_bloom: None,
        }
    }
}
