use std::{collections::BTreeMap, time::Instant};

use anyhow::{Context, Result, bail, ensure};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillJob, BackfillRange, add_backfill_job_actual_provider_queries,
    record_backfill_job_projected_minimum_provider_queries,
};

use crate::backfill::{
    BackfillBlockRange, BackfillJobRunConfig, BackfillOutcome, BackfillTopicPlan,
    CoinbaseSqlBackfillConfig, CoinbaseSqlValidationMode, HistoricalBackfillSourceOps,
    HistoricalLogPayload, HistoricalLogPayloadRequest,
    fetching::{
        BackfillCanonicalityEvidence, fill_log_payloads_from_validation_provider,
        materialize_historical_payload_range,
    },
    range_resolution::{resolve_backfill_block_numbers, resolve_backfill_range},
    selection::SelectedTargetIntervalIndex,
    stored_verification::{
        StoredLogIdentityEvidenceSource, StoredVerificationPlan, plan_stored_verification,
        stored_log_identity_evidence_request,
    },
};
use crate::provider::{ChainProviderOps, ProviderLog, ProviderResolvedBlock};
use tracing::info;

use super::{
    BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY, BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
    ReservedRangeFailure, record_reserved_range_failure, run_with_backfill_lease_heartbeat,
};

pub(super) const MAX_SAMPLE_VALIDATION_BLOCKS: usize = 512;
const MAX_SAMPLE_PROVIDER_PAYLOAD_LOGS: usize = 2_000;
pub(super) const MAX_SAMPLE_DECODED_PAYLOAD_LOGS: usize = 5_000;
pub(super) const MAX_BASENAMES_REGISTRY_SAMPLE_DECODED_PAYLOAD_LOGS: usize = 50_000;
pub(super) const MAX_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS: usize = 15_000;

#[expect(clippy::too_many_arguments)]
pub(super) async fn run_window(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    selected_target_addresses_for_chunk: &[String],
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    topic_plan: &BackfillTopicPlan,
    backfill_job_id: i64,
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    config: &BackfillJobRunConfig,
    coinbase_config: &CoinbaseSqlBackfillConfig,
) -> Result<BackfillOutcome> {
    let window_started = Instant::now();
    info!(
        service = "indexer",
        command = "backfill",
        chain = %source_plan.watched_chain_plan.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        coinbase_sql_validation_mode = coinbase_config.validation_mode.as_str(),
        "Coinbase SQL backfill window started"
    );
    let (resolved_blocks, block_headers, historical_payload) = match coinbase_config.validation_mode
    {
        CoinbaseSqlValidationMode::Full => {
            let resolved_blocks = resolve_backfill_range(validation_provider, range).await?;
            let block_headers =
                fetch_window_headers(validation_provider, &resolved_blocks, range).await?;
            let historical_payload = historical_source
                .fetch_selected_log_payloads(HistoricalLogPayloadRequest {
                    chain: &source_plan.watched_chain_plan.chain,
                    source_plan,
                    selected_target_index,
                    resolved_blocks: &resolved_blocks,
                    selected_target_addresses_for_chunk,
                    topic_plan,
                    range,
                    validation_mode: coinbase_config.validation_mode,
                })
                .await?;
            record_payload_queries(
                pool,
                backfill_job_id,
                historical_source.records_provider_query_attempts_incrementally(),
                &historical_payload,
            )
            .await?;
            log_payload_fetch(
                source_plan,
                range,
                coinbase_config.validation_mode,
                &historical_payload,
            );
            (resolved_blocks, block_headers, historical_payload)
        }
        CoinbaseSqlValidationMode::Sample => {
            let planning_blocks = planning_blocks(range);
            let mut historical_payload = historical_source
                .fetch_selected_log_payloads(HistoricalLogPayloadRequest {
                    chain: &source_plan.watched_chain_plan.chain,
                    source_plan,
                    selected_target_index,
                    resolved_blocks: &planning_blocks,
                    selected_target_addresses_for_chunk,
                    topic_plan,
                    range,
                    validation_mode: coinbase_config.validation_mode,
                })
                .await?;
            record_payload_queries(
                pool,
                backfill_job_id,
                historical_source.records_provider_query_attempts_incrementally(),
                &historical_payload,
            )
            .await?;
            log_payload_fetch(
                source_plan,
                range,
                coinbase_config.validation_mode,
                &historical_payload,
            );
            let sample_blocks =
                sample_validation_block_numbers(range, &historical_payload.logs_by_block);
            let needs_provider_payload = historical_payload.logs_need_validation_provider_payload;
            ensure_sample_validation_size(
                range,
                payload_log_count(&historical_payload),
                sample_blocks.len(),
                needs_provider_payload,
                sample_decoded_payload_log_limit(
                    source_plan,
                    &historical_payload,
                    needs_provider_payload,
                ),
            )?;
            info!(
                service = "indexer",
                command = "backfill",
                chain = %source_plan.watched_chain_plan.chain,
                from_block = range.from_block,
                to_block = range.to_block,
                sample_block_count = sample_blocks.len(),
                "Coinbase SQL sample validation range resolution started"
            );
            let resolved_blocks =
                resolve_backfill_block_numbers(validation_provider, &sample_blocks, range)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to resolve validation-provider returned log blocks for sampled Coinbase SQL range {}..={}",
                            range.from_block, range.to_block
                        )
                    })?;
            ensure_logs_match_resolved_blocks(&historical_payload.logs_by_block, &resolved_blocks)?;
            if needs_provider_payload {
                info!(
                    service = "indexer",
                    command = "backfill",
                    chain = %source_plan.watched_chain_plan.chain,
                    from_block = range.from_block,
                    to_block = range.to_block,
                    resolved_block_count = resolved_blocks.len(),
                    "Coinbase SQL sample validation log payload fill started"
                );
                let payload_fill_started = Instant::now();
                historical_payload.logs_by_block = fill_log_payloads_from_validation_provider(
                    validation_provider,
                    &resolved_blocks,
                    historical_payload.logs_by_block,
                    &historical_payload.validation_filters,
                    coinbase_config.validation_mode,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to fill validation-provider log payloads for sampled Coinbase SQL range {}..={}",
                        range.from_block, range.to_block
                    )
                })?;
                historical_payload.logs_need_validation_provider_payload = false;
                info!(
                    service = "indexer",
                    command = "backfill",
                    chain = %source_plan.watched_chain_plan.chain,
                    from_block = range.from_block,
                    to_block = range.to_block,
                    filled_log_count = payload_log_count(&historical_payload),
                    elapsed_ms = payload_fill_started.elapsed().as_millis(),
                    "Coinbase SQL sample validation log payloads filled"
                );
            } else {
                info!(
                    service = "indexer",
                    command = "backfill",
                    chain = %source_plan.watched_chain_plan.chain,
                    from_block = range.from_block,
                    to_block = range.to_block,
                    raw_log_count = payload_log_count(&historical_payload),
                    "Coinbase SQL sample validation log payload fill skipped; decoded SQL parameters supplied log data"
                );
            }
            let block_headers =
                fetch_window_headers(validation_provider, &resolved_blocks, range).await?;
            (resolved_blocks, block_headers, historical_payload)
        }
    };
    let outcome = materialize_historical_payload_range(
        pool,
        source_plan,
        selected_target_index,
        validation_provider,
        range,
        canonicality_evidence,
        &resolved_blocks,
        block_headers,
        historical_payload,
        config.adapter_sync_mode,
        config.header_audit_mode,
    )
    .await?;
    info!(
        service = "indexer",
        command = "backfill",
        chain = %source_plan.watched_chain_plan.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        resolved_block_count = outcome.resolved_block_count,
        raw_log_count = outcome.raw_log_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        elapsed_ms = window_started.elapsed().as_millis(),
        "Coinbase SQL backfill window materialized"
    );
    Ok(outcome)
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn prepare(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    job: &BackfillJob,
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    evidence_source: &dyn StoredLogIdentityEvidenceSource,
    coinbase_config: &CoinbaseSqlBackfillConfig,
) -> Result<StoredVerificationPlan> {
    let result: Result<StoredVerificationPlan> = async {
        let plan = run_with_backfill_lease_heartbeat(
            pool,
            active_range,
            config,
            plan_stored_verification(pool, job, source_plan, topic_plan, config.range),
        )
        .await?;
        record_backfill_job_projected_minimum_provider_queries(
            pool,
            active_range.backfill_job_id,
            coinbase_config.evidence_query_count(config.range)?,
        )
        .await?;
        let evidence = fetch(
            pool,
            active_range,
            config,
            source_plan,
            topic_plan,
            evidence_source,
        )
        .await?;
        plan.verify_provider_evidence(evidence)
    }
    .await;
    match result {
        Ok(plan) => Ok(plan),
        Err(error) => Err(record_reserved_range_failure(ReservedRangeFailure {
            pool,
            reserved_range: active_range,
            config,
            failure_reason: "Coinbase SQL stored verification preparation failed",
            block_number: Some(config.range.from_block),
            attempted_range: Some(config.range),
            phase: "stored_verification_prepare",
            error,
        })
        .await),
    }
}

pub(super) async fn reverify_after_fetch(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    job: &BackfillJob,
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    evidence_source: &dyn StoredLogIdentityEvidenceSource,
) -> Result<StoredVerificationPlan> {
    let result: Result<StoredVerificationPlan> = async {
        let plan = run_with_backfill_lease_heartbeat(
            pool,
            active_range,
            config,
            plan_stored_verification(pool, job, source_plan, topic_plan, config.range),
        )
        .await?;
        let evidence = fetch(
            pool,
            active_range,
            config,
            source_plan,
            topic_plan,
            evidence_source,
        )
        .await?;
        let plan = plan.verify_provider_evidence(evidence)?;
        ensure!(
            plan.is_fully_stored(),
            "Coinbase SQL stored identity verification still mismatches after true-gap fetch"
        );
        Ok(plan)
    }
    .await;
    match result {
        Ok(plan) => Ok(plan),
        Err(error) => Err(record_reserved_range_failure(ReservedRangeFailure {
            pool,
            reserved_range: active_range,
            config,
            failure_reason: "Coinbase SQL stored verification after fetch failed",
            block_number: Some(config.range.to_block),
            attempted_range: Some(config.range),
            phase: "stored_verification_reverify",
            error,
        })
        .await),
    }
}

async fn fetch(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    evidence_source: &dyn StoredLogIdentityEvidenceSource,
) -> Result<crate::backfill::stored_verification::StoredLogIdentityEvidence> {
    let evidence =
        run_with_backfill_lease_heartbeat(
            pool,
            active_range,
            config,
            evidence_source.fetch_stored_log_identity_evidence(
                stored_log_identity_evidence_request(source_plan, topic_plan, config.range)?,
            ),
        )
        .await?;
    if !evidence_source.records_provider_query_attempts_incrementally() {
        let query_count = i64::try_from(evidence.query_count)
            .context("Coinbase SQL verification query count exceeds signed 64-bit")?;
        add_backfill_job_actual_provider_queries(pool, active_range.backfill_job_id, query_count)
            .await?;
    }
    Ok(evidence)
}

pub(super) async fn record_payload_queries(
    pool: &sqlx::PgPool,
    backfill_job_id: i64,
    provider_query_attempts_already_recorded: bool,
    payload: &HistoricalLogPayload,
) -> Result<()> {
    if provider_query_attempts_already_recorded {
        return Ok(());
    }
    let count = payload
        .source_stats
        .query_count
        .checked_add(payload.source_stats.retry_count)
        .context("Coinbase SQL actual request count overflowed")?;
    let count = i64::try_from(count)
        .context("Coinbase SQL query count exceeds signed 64-bit job accounting")?;
    add_backfill_job_actual_provider_queries(pool, backfill_job_id, count).await
}

pub(super) fn payload_log_count(payload: &HistoricalLogPayload) -> usize {
    payload.logs_by_block.values().map(Vec::len).sum()
}

pub(super) fn next_window_blocks(
    current: i64,
    config: &CoinbaseSqlBackfillConfig,
    raw_log_count: usize,
) -> i64 {
    if raw_log_count >= (config.effective_page_limit() / 2).max(1) {
        (current / 2).max(1)
    } else if raw_log_count < config.effective_page_limit() {
        current
            .saturating_mul(2)
            .min(config.max_window_blocks)
            .clamp(1, 65_536)
    } else {
        current
    }
}

pub(super) fn planning_blocks(range: BackfillBlockRange) -> Vec<ProviderResolvedBlock> {
    (range.from_block..=range.to_block)
        .map(|block_number| ProviderResolvedBlock {
            block_number,
            block_hash: String::new(),
        })
        .collect()
}

pub(super) fn log_payload_fetch(
    source_plan: &WatchedSourceSelectorPlan,
    range: crate::backfill::BackfillBlockRange,
    validation_mode: crate::backfill::CoinbaseSqlValidationMode,
    payload: &HistoricalLogPayload,
) {
    tracing::info!(
        service = "indexer",
        command = "backfill",
        chain = %source_plan.watched_chain_plan.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        coinbase_sql_validation_mode = validation_mode.as_str(),
        coinbase_sql_query_count = payload.source_stats.query_count,
        coinbase_sql_page_count = payload.source_stats.page_count,
        coinbase_sql_row_count = payload.source_stats.row_count,
        coinbase_sql_retry_count = payload.source_stats.retry_count,
        coinbase_sql_union_duplicate_count = payload.source_stats.union_duplicate_count,
        coinbase_sql_log_block_count = payload.logs_by_block.len(),
        raw_log_count = payload_log_count(payload),
        validation_filter_count = payload.validation_filters.len(),
        "Coinbase SQL payload fetched"
    );
}

pub(super) fn ensure_sample_validation_size(
    range: BackfillBlockRange,
    log_count: usize,
    block_count: usize,
    requires_provider_payload: bool,
    decoded_payload_log_limit: usize,
) -> Result<()> {
    if block_count > MAX_SAMPLE_VALIDATION_BLOCKS {
        bail!(
            "Coinbase SQL sample window {}..={} returned logs across {} blocks; refusing sample materialization above {} blocks so the range can retry smaller",
            range.from_block,
            range.to_block,
            block_count,
            MAX_SAMPLE_VALIDATION_BLOCKS
        );
    }
    let max_log_count = if requires_provider_payload {
        MAX_SAMPLE_PROVIDER_PAYLOAD_LOGS
    } else {
        decoded_payload_log_limit
    };
    if log_count > max_log_count {
        let label = if requires_provider_payload {
            "provider log-payload validation"
        } else {
            "decoded SQL materialization"
        };
        bail!(
            "Coinbase SQL sample window {}..={} returned {} logs; refusing {} above {} logs so the range can retry smaller",
            range.from_block,
            range.to_block,
            log_count,
            label,
            max_log_count
        );
    }
    Ok(())
}

pub(super) fn sample_decoded_payload_log_limit(
    source_plan: &WatchedSourceSelectorPlan,
    payload: &HistoricalLogPayload,
    requires_provider_payload: bool,
) -> usize {
    let registry_scan = !requires_provider_payload
        && !source_plan.selected_targets.is_empty()
        && source_plan
            .selected_targets
            .iter()
            .all(|target| target.source_family == BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
        && !payload.validation_filters.is_empty()
        && payload
            .validation_filters
            .iter()
            .all(|filter| filter.addresses.is_empty());
    let registrar_scan = !requires_provider_payload
        && !payload.logs_filtered_by_selected_target_index
        && !source_plan.selected_targets.is_empty()
        && source_plan
            .selected_targets
            .iter()
            .all(|target| target.source_family == BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY)
        && !payload.validation_filters.is_empty()
        && payload
            .validation_filters
            .iter()
            .all(|filter| !filter.addresses.is_empty());
    if registry_scan {
        MAX_BASENAMES_REGISTRY_SAMPLE_DECODED_PAYLOAD_LOGS
    } else if registrar_scan {
        MAX_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS
    } else {
        MAX_SAMPLE_DECODED_PAYLOAD_LOGS
    }
}

pub(super) fn sample_validation_block_numbers(
    range: BackfillBlockRange,
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
) -> Vec<i64> {
    logs_by_block
        .keys()
        .copied()
        .filter(|block| *block >= range.from_block && *block <= range.to_block)
        .collect()
}

async fn fetch_window_headers(
    provider: &(impl ChainProviderOps + ?Sized),
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
) -> Result<Vec<crate::provider::ProviderBlock>> {
    provider
        .fetch_block_headers_by_hashes(resolved_blocks)
        .await
        .with_context(|| {
            format!(
                "failed to fetch validation provider headers for Coinbase SQL range {}..={}",
                range.from_block, range.to_block
            )
        })
}

pub(super) fn ensure_logs_match_resolved_blocks(
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
    resolved_blocks: &[ProviderResolvedBlock],
) -> Result<()> {
    let resolved_by_number = resolved_blocks
        .iter()
        .map(|block| (block.block_number, block.block_hash.clone()))
        .collect::<BTreeMap<_, _>>();
    for (block_number, logs) in logs_by_block {
        let expected_hash = resolved_by_number.get(block_number).with_context(|| {
            format!("Coinbase SQL returned block {block_number} that was not resolved by validation provider")
        })?;
        for log in logs {
            if log.block_number != *block_number {
                bail!(
                    "Coinbase SQL grouped log block {} under block {}",
                    log.block_number,
                    block_number
                );
            }
            if !log.block_hash.eq_ignore_ascii_case(expected_hash) {
                bail!(
                    "Coinbase SQL returned block {} hash {}, validation provider resolved {}",
                    block_number,
                    log.block_hash,
                    expected_hash
                );
            }
        }
    }
    Ok(())
}
