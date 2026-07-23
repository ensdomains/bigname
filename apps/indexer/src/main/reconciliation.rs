#[path = "reconciliation/adapter_sync.rs"]
mod adapter_sync;
#[path = "reconciliation/canonical.rs"]
mod canonical;
#[path = "reconciliation/guard_release.rs"]
pub(crate) mod guard_release;
#[path = "reconciliation/lineage.rs"]
mod lineage;
#[path = "reconciliation/logging.rs"]
mod logging;
#[path = "reconciliation/payload.rs"]
mod payload;
#[path = "reconciliation/persistence.rs"]
mod persistence;
#[path = "reconciliation/replay.rs"]
mod replay;
#[path = "reconciliation/types.rs"]
mod types;

#[allow(unused_imports)]
pub(crate) use adapter_sync::{
    AutomaticTwoPhaseFullClosureSyncResult, BacklogHandoffStatus,
    automatic_stateless_replay_completed, sync_adapter_state_from_persisted_raw_payloads,
    sync_adapter_state_from_scoped_persisted_raw_payloads,
    sync_automatic_two_phase_full_closure_normalized_events,
    sync_live_adapter_backlog_after_normalized_replay_with_progress,
    sync_live_adapter_state_from_persisted_raw_payloads,
    sync_live_adapter_state_from_persisted_raw_payloads_with_progress,
    validate_chain_handoff_while_guarded,
};
#[cfg(test)]
pub(crate) use adapter_sync::{
    PersistedRawPayloadAdapterSyncModeForTest, install_after_stateless_failure,
    install_backlog_after_adapter_sync_test_hook, install_ownership_release_test_hook,
    install_post_discovery_mutation_failure_for_test, install_stateless_page_observer,
    sync_ens_v2_registry_for_mode_for_test,
    sync_full_closure_normalized_events_from_persisted_raw_payloads,
    sync_live_adapter_backlog_after_normalized_replay,
    sync_manual_full_closure_normalized_events_from_persisted_raw_payloads,
};
#[cfg(test)]
pub(crate) use canonical::reconcile_canonical_head_with_adapter_progress;
#[allow(unused_imports)]
pub(crate) use canonical::{
    ChainCoverageFrontiers, EnsV2LiveCoverageRecoveryStatus, RawCodeBaselineFrontier,
    orphan_canonical_branch, orphan_reorg_losing_branch_payloads, poll_provider_heads,
    poll_provider_heads_with_adapter_sync, poll_provider_heads_with_adapter_sync_and_progress,
    reconcile_canonical_head, reconcile_fetched_heads, reconcile_fetched_heads_with_adapter_sync,
    reconcile_intake_chain_task, recover_ens_v2_live_coverage_requirement,
    recover_ens_v2_live_coverage_requirement_with_progress,
};
#[allow(unused_imports)]
pub(crate) use lineage::{
    checkpoint_ref_changed, head_change_set, lineage_block_to_provider,
    provider_block_to_checkpoint_ref, provider_block_to_lineage,
    provider_block_to_lineage_with_header_audit_mode,
};
#[allow(unused_imports)]
pub(crate) use logging::{
    log_chain_reconciliation_outcome, log_raw_fact_normalized_event_replay_outcome,
};
#[allow(unused_imports)]
pub(crate) use payload::SelectedAddressSet;
#[allow(unused_imports)]
pub(crate) use payload::{
    canonical_raw_state, ensure_block_scoped_identity, ensure_provider_bundle_matches_raw_block,
    hex_string, insert_raw_block_candidate, keccak256_hex, parse_hex_bytes, parse_receipt_status,
    preferred_canonicality, provider_block_to_raw_block,
    provider_block_to_raw_block_with_header_audit_mode, provider_code_observation_to_raw_code_hash,
    provider_log_to_raw_log, provider_logs_to_live_selected_raw_logs,
    provider_raw_payload_cache_metadata_to_upserts, provider_receipt_to_raw_receipt,
    provider_receipts_to_selected_raw_receipts, provider_transaction_to_raw_transaction,
    provider_transactions_to_selected_raw_transactions, raw_code_hash_candidate_hashes,
    raw_payload_candidate_hashes, retained_transaction_keys_from_raw_logs, selected_address_set,
};
#[allow(unused_imports)]
pub(crate) use persistence::{
    ensure_losing_branch_raw_blocks_exist, persist_reconciled_raw_blocks,
    persist_reconciled_raw_code_hashes, persist_reconciled_raw_payloads,
};
#[allow(unused_imports)]
pub(crate) use replay::{
    NormalizedEventReplayAdapter, active_closure_or_dependency_replay_adapters,
    chain_has_closure_or_dependency_replay_adapter,
    ensure_full_closure_retention_authority_for_adapters,
    ensure_legacy_registry_closure_retention_authority_for_adapters,
    replay_raw_fact_normalized_events, replay_raw_fact_normalized_events_with_progress,
    replay_stateless_normalized_events_before_full_closure_with_progress,
    replay_stateless_only_raw_fact_normalized_events, select_log_bounded_replay_to_block,
    unsupported_closure_replay_adapters,
};
#[allow(unused_imports)]
pub(crate) use types::{
    CanonicalReconciliation, CanonicalReconciliationStatus, ChainReconciliationOutcome,
    HeadChangeSet, HeaderAuditMode, PersistedRawPayloadAdapterSyncSummary,
    RawFactNormalizedEventReplayOutcome, RawFactNormalizedEventReplayRequest,
    RawFactNormalizedEventReplaySelection, RawFactNormalizedEventReplaySourceScope,
};
