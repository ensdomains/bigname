use tracing::info;

use super::entrypoints::BootstrapBackfillOutcome;

pub(super) fn log_bootstrap_backfill_outcome(
    outcome: &BootstrapBackfillOutcome,
    hash_pinned_chunk_blocks: i64,
) {
    info!(
        service = "indexer",
        command = "run",
        bootstrap_backfill_status = "drained",
        active_chain_count = outcome.active_chain_count,
        provider_configured_chain_count = outcome.provider_configured_chain_count,
        missing_provider_chain_count = outcome.missing_provider_chain_count,
        eligible_bootstrap_target_count = outcome.eligible_target_count,
        skipped_unknown_start_target_count = outcome.skipped_unknown_start_target_count,
        drained_bootstrap_job_count = outcome.drained_job_count,
        skipped_future_target_count = outcome.skipped_future_target_count,
        bootstrap_backfill_range_policy = "authoritative_known_start_to_provider_finalized_head",
        hash_pinned_chunk_blocks,
        bootstrap_backfill_workers = outcome.requested_worker_count,
        effective_bootstrap_backfill_workers = outcome.effective_worker_count,
        bootstrap_backfill_range_blocks = outcome.range_partition_block_count,
        reserved_range_count = outcome.reserved_range_count,
        completed_range_count = outcome.completed_range_count,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        normalized_replay_job_count = outcome.normalized_replay_job_count,
        normalized_replay_synced_count = outcome.normalized_replay_synced_count,
        normalized_replay_inserted_count = outcome.normalized_replay_inserted_count,
        "startup bootstrap backfill jobs drained before live polling"
    );
}
