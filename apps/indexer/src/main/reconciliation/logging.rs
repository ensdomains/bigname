use tracing::info;

use super::types::{ChainReconciliationOutcome, RawFactNormalizedEventReplayOutcome};

pub(crate) fn log_chain_reconciliation_outcome(outcome: &ChainReconciliationOutcome) {
    info!(
        service = "indexer",
        chain = %outcome.chain,
        canonical_reconciliation_status = outcome.canonical_status.as_str(),
        canonical_head_changed = outcome.canonical_head_changed,
        safe_head_changed = outcome.safe_head_changed,
        finalized_head_changed = outcome.finalized_head_changed,
        fetched_parent_count = outcome.fetched_parent_count,
        orphaned_block_count = outcome.orphaned_block_count,
        canonical_block_number = outcome.canonical_block_number,
        safe_block_number = outcome.safe_block_number,
        finalized_block_number = outcome.finalized_block_number,
        "provider heads reconciled for chain"
    );
}

pub(crate) fn log_raw_fact_normalized_event_replay_outcome(
    outcome: &RawFactNormalizedEventReplayOutcome,
) {
    info!(
        service = "indexer",
        command = "replay normalized-events",
        deployment_profile = %outcome.deployment_profile,
        chain = %outcome.chain,
        selection_kind = outcome.selection_kind,
        source_scope_target_count = outcome.source_scope_target_count,
        selected_block_count = outcome.selected_block_count,
        canonical_raw_log_count = outcome.canonical_raw_log_count,
        scanned_raw_log_count = outcome.scanned_raw_log_count,
        matched_raw_log_count = outcome.matched_raw_log_count,
        normalized_event_sync_total_count = outcome.normalized_event_synced_count,
        normalized_event_inserted_total_count = outcome.normalized_event_inserted_count,
        "raw-fact normalized-event replay completed"
    );
}
