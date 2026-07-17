use tracing::info;

use super::super::types::PersistedRawPayloadAdapterSyncSummary;

// Each adapter identity, count, and duration stays separate for structured logging.
#[expect(clippy::too_many_arguments)]
pub(super) fn log_adapter_call_timing(
    chain: &str,
    adapter: &'static str,
    function: &'static str,
    block_hash_count: usize,
    source_scope_target_count: usize,
    scanned_log_count: usize,
    matched_log_count: usize,
    normalized_event_synced_count: usize,
    normalized_event_inserted_count: usize,
    elapsed_ms: u128,
) {
    info!(
        service = "indexer",
        chain,
        adapter,
        function,
        block_hash_count,
        source_scope_target_count,
        scanned_log_count,
        matched_log_count,
        normalized_event_synced_count,
        normalized_event_inserted_count,
        elapsed_ms,
        "normalized-event replay adapter timing completed"
    );
}

pub(super) fn log_live_poll_adapter_sync_completion(
    chain: &str,
    block_hash_count: usize,
    source_scope_target_count: usize,
    summary: &PersistedRawPayloadAdapterSyncSummary,
) {
    info!(
        service = "indexer",
        command = "poll",
        chain,
        block_hash_count,
        source_scope_target_count,
        scanned_log_count = summary.scanned_log_count,
        matched_log_count = summary.matched_log_count,
        normalized_event_sync_total_count = summary.total_synced_count,
        normalized_event_inserted_total_count = summary.total_inserted_count,
        resolver_profile_authority_epoch_guard_count =
            summary.resolver_profile_authority_epoch_guard_count,
        resolver_profile_authority_scan_count = summary.resolver_profile_authority_scan_count,
        "live poll adapter sync completed"
    );
}
