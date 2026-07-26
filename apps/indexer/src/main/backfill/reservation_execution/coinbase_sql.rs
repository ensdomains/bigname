#[path = "coinbase_sql/recovery.rs"]
mod recovery;
use super::{
    backfill_lease_duration_secs, coinbase_sql_uses_basenames_registry_scan_all,
    create_coinbase_sql_backfill_job, finalize_reserved_stored_verification,
    refreshed_backfill_lease_expires_at, run_with_backfill_lease_heartbeat,
};
use crate::backfill::{
    BackfillBlockRange, BackfillJobRunConfig, BackfillJobRunOutcome, BackfillTopicPlan,
    CoinbaseSqlBackfillConfig, HistoricalBackfillSourceOps,
    coinbase_sql::load_backfill_topic_plan,
    coverage_facts::complete_reserved_range_recording_plan_coverage,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
    fetching::load_backfill_canonicality_evidence,
    selection::{SelectedTargetIntervalIndex, SelectedTargetRangeCursor},
    stored_verification::{
        StoredLogIdentityEvidenceSource, VerifiedRangeSource, completed_plan, provider_only_plan,
        stored_verification_is_current,
    },
};
use crate::provider::ChainProviderOps;
use anyhow::{Context, Result, bail, ensure};
use bigname_adapters::StartupAdapterProgress;
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillJob, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange, advance_backfill_range,
    load_backfill_job, record_backfill_job_projected_minimum_provider_queries,
    reserve_backfill_range_with_coverage_recovery_fence,
};
use tracing::{info, warn};
const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
const BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY: &str = "basenames_base_registrar";
#[cfg(test)]
use crate::{
    backfill::{CoinbaseSqlValidationMode, HistoricalLogPayload},
    provider::{ProviderLog, ProviderResolvedBlock},
};
#[cfg(test)]
use recovery::{
    MAX_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS as MAX_COINBASE_SQL_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS,
    MAX_BASENAMES_REGISTRY_SAMPLE_DECODED_PAYLOAD_LOGS as MAX_COINBASE_SQL_BASENAMES_REGISTRY_SAMPLE_DECODED_PAYLOAD_LOGS,
    MAX_SAMPLE_DECODED_PAYLOAD_LOGS as MAX_COINBASE_SQL_SAMPLE_DECODED_PAYLOAD_LOGS,
    MAX_SAMPLE_VALIDATION_BLOCKS as MAX_COINBASE_SQL_SAMPLE_VALIDATION_BLOCKS,
    ensure_logs_match_resolved_blocks as ensure_coinbase_sql_logs_match_resolved_blocks,
    ensure_sample_validation_size as ensure_coinbase_sql_sample_validation_size,
    next_window_blocks as next_coinbase_sql_window_blocks,
    sample_decoded_payload_log_limit as coinbase_sql_sample_decoded_payload_log_limit,
    sample_validation_block_numbers as coinbase_sql_sample_validation_block_numbers,
};
#[cfg(test)]
const MAX_COINBASE_SQL_PRACTICAL_WINDOW_BLOCKS: i64 = 65_536;
pub(crate) async fn run_resumable_coinbase_sql_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    mut config: BackfillJobRunConfig,
    coinbase_config: CoinbaseSqlBackfillConfig,
) -> Result<BackfillJobRunOutcome> {
    coinbase_config.validate()?;
    let topic_plan = load_backfill_topic_plan(pool, source_plan).await?;
    config.adapter_sync_mode = effective_coinbase_sql_adapter_sync_mode(
        source_plan,
        &topic_plan,
        config.adapter_sync_mode,
    );
    ensure_coinbase_sql_registry_range_start_is_replay_safe(
        source_plan,
        &topic_plan,
        config.range,
    )?;
    let record =
        create_coinbase_sql_backfill_job(pool, source_plan, &config, &coinbase_config, &topic_plan)
            .await?;
    run_precreated_coinbase_sql_backfill_job_inner(
        pool,
        source_plan,
        validation_provider,
        historical_source,
        None,
        config,
        coinbase_config,
        topic_plan,
        record,
        false,
        &mut None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_precreated_verified_coinbase_sql_backfill_job_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + StoredLogIdentityEvidenceSource),
    mut config: BackfillJobRunConfig,
    coinbase_config: CoinbaseSqlBackfillConfig,
    topic_plan: BackfillTopicPlan,
    record: BackfillJobRecord,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<BackfillJobRunOutcome> {
    coinbase_config.validate()?;
    config.adapter_sync_mode = effective_coinbase_sql_adapter_sync_mode(
        source_plan,
        &topic_plan,
        config.adapter_sync_mode,
    );
    ensure_coinbase_sql_registry_range_start_is_replay_safe(
        source_plan,
        &topic_plan,
        config.range,
    )?;
    run_precreated_coinbase_sql_backfill_job_inner(
        pool,
        source_plan,
        validation_provider,
        historical_source,
        Some(historical_source),
        config,
        coinbase_config,
        topic_plan,
        record,
        true,
        &mut Some(progress),
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_precreated_verified_coinbase_sql_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + StoredLogIdentityEvidenceSource),
    mut config: BackfillJobRunConfig,
    coinbase_config: CoinbaseSqlBackfillConfig,
    topic_plan: BackfillTopicPlan,
    record: BackfillJobRecord,
) -> Result<BackfillJobRunOutcome> {
    coinbase_config.validate()?;
    config.adapter_sync_mode = effective_coinbase_sql_adapter_sync_mode(
        source_plan,
        &topic_plan,
        config.adapter_sync_mode,
    );
    ensure_coinbase_sql_registry_range_start_is_replay_safe(
        source_plan,
        &topic_plan,
        config.range,
    )?;
    run_precreated_coinbase_sql_backfill_job_inner(
        pool,
        source_plan,
        validation_provider,
        historical_source,
        Some(historical_source),
        config,
        coinbase_config,
        topic_plan,
        record,
        true,
        &mut None,
    )
    .await
}
#[expect(clippy::too_many_arguments)]
async fn run_precreated_coinbase_sql_backfill_job_inner(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    stored_evidence_source: Option<&dyn StoredLogIdentityEvidenceSource>,
    mut config: BackfillJobRunConfig,
    coinbase_config: CoinbaseSqlBackfillConfig,
    topic_plan: BackfillTopicPlan,
    record: BackfillJobRecord,
    verify_stored_ranges: bool,
    service_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<BackfillJobRunOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    config
        .idempotency_key
        .clone_from(&record.job.idempotency_key);
    let mut outcome = BackfillJobRunOutcome::new(record.job.backfill_job_id, source_plan, &config);
    let lease_duration_secs = backfill_lease_duration_secs(config.lease_expires_at)?;
    if verify_stored_ranges {
        ensure!(
            record.job.source_identity
                == super::verified_backfill_job_source_identity_payload(
                    source_plan,
                    &topic_plan,
                    Some(&coinbase_config),
                )?,
            "stored-history Coinbase SQL job identity does not match its execution topic/provider plan"
        );
    }
    info!(
        service = "indexer",
        command = "backfill",
        backfill_job_id = record.job.backfill_job_id,
        backfill_job_status = record.job.status.as_str(),
        chain = %watched_chain.chain,
        selector_kind = source_plan.selector_kind.as_str(),
        selected_target_count = source_plan.selected_targets.len(),
        deployment_profile = %config.deployment_profile,
        from_block = config.range.from_block,
        to_block = config.range.to_block,
        idempotency_key = %config.idempotency_key,
        coinbase_sql_initial_window_blocks = coinbase_config.initial_window_blocks,
        coinbase_sql_max_window_blocks = coinbase_config.max_window_blocks,
        coinbase_sql_page_limit = coinbase_config.page_limit,
        coinbase_sql_query_char_limit = coinbase_config.sql_char_limit,
        coinbase_sql_validation_mode = coinbase_config.validation_mode.as_str(),
        adapter_sync_mode = config.adapter_sync_mode.as_str(),
        header_audit_mode = config.header_audit_mode.as_str(),
        range_count = record.ranges.len(),
        "resumable Coinbase SQL backfill job loaded"
    );

    loop {
        let Some(reserved_range) = reserve_backfill_range_with_coverage_recovery_fence(
            pool,
            record.job.backfill_job_id,
            config.coverage_recovery_reservation_fence.as_ref(),
            &config.lease_owner,
            &config.lease_token,
            refreshed_backfill_lease_expires_at(lease_duration_secs)?,
        )
        .await?
        else {
            break;
        };

        outcome.reserved_range_count += 1;
        run_reserved_coinbase_sql_backfill_range_inner(
            pool,
            source_plan,
            validation_provider,
            historical_source,
            stored_evidence_source,
            &config,
            &coinbase_config,
            &topic_plan,
            &reserved_range,
            &mut outcome,
            verify_stored_ranges.then_some(&record.job),
            service_progress,
        )
        .await?;
        outcome.completed_range_count += 1;
    }

    let job = load_backfill_job(pool, record.job.backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {}", record.job.backfill_job_id))?;
    if job.status == BackfillLifecycleStatus::Completed {
        info!(
            service = "indexer",
            command = "backfill",
            backfill_job_id = outcome.backfill_job_id,
            chain = %outcome.chain,
            from_block = outcome.from_block,
            to_block = outcome.to_block,
            idempotency_key = %outcome.idempotency_key,
            adapter_sync_mode = config.adapter_sync_mode.as_str(),
            reserved_range_count = outcome.reserved_range_count,
            completed_range_count = outcome.completed_range_count,
            resolved_block_count = outcome.resolved_block_count,
            raw_block_count = outcome.raw_block_count,
            raw_transaction_count = outcome.raw_transaction_count,
            raw_receipt_count = outcome.raw_receipt_count,
            raw_log_count = outcome.raw_log_count,
            raw_code_hash_count = outcome.raw_code_hash_count,
            "resumable Coinbase SQL backfill job completed"
        );
        return Ok(outcome);
    }

    bail!(
        "backfill job {} has no reservable ranges but is {}; another active lease may still own work",
        record.job.backfill_job_id,
        job.status.as_str()
    );
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_reserved_coinbase_sql_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    config: &BackfillJobRunConfig,
    coinbase_config: &CoinbaseSqlBackfillConfig,
    topic_plan: &BackfillTopicPlan,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
) -> Result<()> {
    run_reserved_coinbase_sql_backfill_range_inner(
        pool,
        source_plan,
        validation_provider,
        historical_source,
        None,
        config,
        coinbase_config,
        topic_plan,
        reserved_range,
        aggregate,
        None,
        &mut None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn run_reserved_coinbase_sql_backfill_range_inner(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    stored_evidence_source: Option<&dyn StoredLogIdentityEvidenceSource>,
    config: &BackfillJobRunConfig,
    coinbase_config: &CoinbaseSqlBackfillConfig,
    topic_plan: &BackfillTopicPlan,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
    verification_job: Option<&BackfillJob>,
    service_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut active_range = reserved_range.clone();
    let remaining_range =
        if active_range.checkpoint_block_number < active_range.range_end_block_number {
            Some(BackfillBlockRange::new(
                active_range
                    .checkpoint_block_number
                    .checked_add(1)
                    .context(
                        "backfill checkpoint overflowed while computing Coinbase SQL resume block",
                    )?,
                active_range.range_end_block_number,
            )?)
        } else {
            None
        };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(source_plan);
    let mut selected_target_range_cursor = SelectedTargetRangeCursor::from_source_plan(source_plan);
    let reuse_current_verification = match verification_job {
        Some(job) if remaining_range.is_none() => {
            stored_verification_is_current(pool, job, config.range).await?
        }
        _ => false,
    };
    let mut initial_verification_query_minimum = 0_i64;
    let evidence_query_minimum = coinbase_config.evidence_query_count(config.range)?;
    let mut verification_plan = match verification_job {
        Some(_) if reuse_current_verification => completed_plan(),
        Some(job) => {
            let evidence_source = stored_evidence_source
                .context("verified Coinbase SQL execution has no stored-evidence source")?;
            let plan = recovery::prepare(
                pool,
                &active_range,
                config,
                job,
                source_plan,
                topic_plan,
                evidence_source,
                coinbase_config,
            )
            .await?;
            initial_verification_query_minimum = evidence_query_minimum;
            plan
        }
        None => remaining_range
            .map(provider_only_plan)
            .unwrap_or_else(completed_plan),
    };
    let has_provider_gaps = verification_plan
        .segments
        .iter()
        .any(|segment| segment.source == VerifiedRangeSource::Provider);
    let segments = if verification_job.is_some() {
        verification_plan.execution_segments(active_range.checkpoint_block_number)?
    } else {
        verification_plan.segments.clone()
    };
    let mut block_number = segments
        .first()
        .map(|segment| segment.range.from_block)
        .unwrap_or(active_range.range_end_block_number);
    let final_verification_query_minimum = initial_verification_query_minimum
        * i64::from(verification_job.is_some() && has_provider_gaps);
    record_backfill_job_projected_minimum_provider_queries(
        pool,
        active_range.backfill_job_id,
        verification_plan
            .minimum_provider_queries(coinbase_config.initial_window_blocks)?
            .checked_add(initial_verification_query_minimum)
            .and_then(|count| count.checked_add(final_verification_query_minimum))
            .context("Coinbase SQL projected query count overflowed")?,
    )
    .await?;
    let canonicality_evidence = if segments
        .iter()
        .any(|segment| segment.source == VerifiedRangeSource::Provider)
    {
        match run_with_backfill_lease_heartbeat(
            pool,
            &active_range,
            config,
            load_backfill_canonicality_evidence(
                pool,
                &source_plan.watched_chain_plan.chain,
                validation_provider,
            ),
        )
        .await
        {
            Ok(evidence) => Some(evidence),
            Err(error) => {
                return Err(record_reserved_range_failure(ReservedRangeFailure {
                    pool,
                    reserved_range: &active_range,
                    config,
                    failure_reason: "Coinbase SQL validation canonicality evidence load failed",
                    block_number: Some(block_number),
                    attempted_range: None,
                    phase: "canonicality_evidence",
                    error,
                })
                .await);
            }
        }
    } else {
        None
    };
    for segment in segments {
        if segment.source == VerifiedRangeSource::Stored {
            active_range = advance_backfill_range(
                pool,
                active_range.backfill_range_id,
                &config.lease_token,
                segment.range.to_block,
            )
            .await?;
            if let Some(progress) = service_progress.as_deref_mut() {
                progress.record(pool).await?;
            }
            continue;
        }

        let mut window_blocks = coinbase_config.initial_window_blocks;
        block_number = segment.range.from_block;
        while block_number <= segment.range.to_block {
            let window_end = block_number
                .checked_add(window_blocks - 1)
                .unwrap_or(segment.range.to_block)
                .min(segment.range.to_block);
            let window_range = BackfillBlockRange::new(block_number, window_end)?;
            let selected_target_addresses_for_chunk = selected_target_range_cursor
                .active_addresses_for_monotonic_range(
                    window_range.from_block,
                    window_range.to_block,
                );
            let window_outcome = match run_with_backfill_lease_heartbeat(
                pool,
                &active_range,
                config,
                recovery::run_window(
                    pool,
                    source_plan,
                    &selected_target_index,
                    &selected_target_addresses_for_chunk,
                    validation_provider,
                    historical_source,
                    topic_plan,
                    active_range.backfill_job_id,
                    window_range,
                    canonicality_evidence
                        .as_ref()
                        .expect("provider segment has canonicality evidence")
                        .clone(),
                    config,
                    coinbase_config,
                ),
            )
            .await
            {
                Ok(outcome) => {
                    window_blocks = recovery::next_window_blocks(
                        window_blocks,
                        coinbase_config,
                        outcome.raw_log_count,
                    );
                    outcome
                }
                Err(error) => {
                    if window_blocks > 1 {
                        let next_window_blocks = (window_blocks / 2).max(1);
                        warn!(
                            service = "indexer",
                            command = "backfill",
                            chain = %source_plan.watched_chain_plan.chain,
                            block_number,
                            attempted_from_block = window_range.from_block,
                            attempted_to_block = window_range.to_block,
                            previous_window_blocks = window_blocks,
                            next_window_blocks,
                            error = %format!("{error:#}"),
                            "Coinbase SQL backfill window failed; retrying with a smaller window before failing the range"
                        );
                        window_blocks = next_window_blocks;
                        continue;
                    }
                    return Err(record_reserved_range_failure(ReservedRangeFailure {
                        pool,
                        reserved_range: &active_range,
                        config,
                        failure_reason: "Coinbase SQL backfill failed",
                        block_number: Some(block_number),
                        attempted_range: Some(window_range),
                        phase: "coinbase_sql_intake",
                        error,
                    })
                    .await);
                }
            };
            aggregate.add_range_outcome(&window_outcome);
            active_range = match advance_backfill_range(
                pool,
                active_range.backfill_range_id,
                &config.lease_token,
                window_end,
            )
            .await
            {
                Ok(range) => range,
                Err(error) => {
                    return Err(record_reserved_range_failure(ReservedRangeFailure {
                        pool,
                        reserved_range: &active_range,
                        config,
                        failure_reason: "Coinbase SQL backfill checkpoint advance failed",
                        block_number: Some(block_number),
                        attempted_range: Some(window_range),
                        phase: "checkpoint_advance",
                        error,
                    })
                    .await);
                }
            };
            if let Some(progress) = service_progress.as_deref_mut() {
                progress.record(pool).await?;
            }
            block_number = window_end
                .checked_add(1)
                .context("Coinbase SQL backfill block number overflowed while advancing range")?;
        }
    }
    if let Some(job) = verification_job
        && !reuse_current_verification
    {
        if has_provider_gaps {
            let evidence_source = stored_evidence_source
                .context("verified Coinbase SQL execution has no stored-evidence source")?;
            verification_plan = recovery::reverify_after_fetch(
                pool,
                &active_range,
                config,
                job,
                source_plan,
                topic_plan,
                evidence_source,
            )
            .await?;
        }
        finalize_reserved_stored_verification(
            pool,
            &active_range,
            config,
            job,
            source_plan,
            topic_plan,
            &verification_plan,
            "Coinbase SQL stored verification finalization failed",
        )
        .await?;
    }
    complete_reserved_range_recording_plan_coverage(
        pool,
        &active_range,
        config,
        source_plan,
        coinbase_sql_uses_basenames_registry_scan_all(source_plan, topic_plan),
        verification_job.is_some(),
        "Coinbase SQL backfill range completion failed",
        None,
        service_progress,
    )
    .await
}
pub(crate) fn effective_coinbase_sql_adapter_sync_mode(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    requested_mode: crate::backfill::BackfillAdapterSyncMode,
) -> crate::backfill::BackfillAdapterSyncMode {
    if coinbase_sql_requires_ordered_closure_replay(source_plan, topic_plan) {
        crate::backfill::BackfillAdapterSyncMode::RawOnly
    } else {
        requested_mode.hash_pinned_backfill_mode()
    }
}

fn coinbase_sql_requires_ordered_closure_replay(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
) -> bool {
    super::coinbase_sql_uses_basenames_registry_scan_all(source_plan, topic_plan)
        || source_plan
            .selected_targets
            .iter()
            .any(|target| basenames_authority_source_family_requires_closure(&target.source_family))
}

fn basenames_authority_source_family_requires_closure(source_family: &str) -> bool {
    matches!(
        source_family,
        BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY
            | BASENAMES_BASE_REGISTRY_SOURCE_FAMILY
            | "basenames_base_resolver"
    )
}

pub(crate) fn ensure_coinbase_sql_registry_range_start_is_replay_safe(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    range: BackfillBlockRange,
) -> Result<()> {
    if source_plan.selector_kind != bigname_manifests::WatchedSourceSelectorKind::SourceFamily
        || source_plan.source_family.as_deref() != Some(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
    {
        return Ok(());
    }
    if !topic_plan
        .event_signatures_for_source_family(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
        .is_empty()
    {
        return Ok(());
    }

    let Some(earliest_effective_from_block) = source_plan
        .selected_targets
        .iter()
        .map(|target| target.effective_from_block)
        .min()
    else {
        return Ok(());
    };
    if range.from_block > earliest_effective_from_block {
        bail!(
            "Coinbase SQL Basenames registry backfill range starts at {}, after earliest selected target effective_from_block {}; start a new immutable source-identity job at or before that block instead of resuming across possible source-identity drift",
            range.from_block,
            earliest_effective_from_block
        );
    }

    Ok(())
}

#[cfg(test)]
#[path = "coinbase_sql/sample_tests.rs"]
mod sample_tests;

#[cfg(test)]
#[path = "coinbase_sql/tests.rs"]
mod tests;
