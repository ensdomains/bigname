use std::collections::BTreeMap;

use super::{DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY, EnsV1UnwrappedAuthoritySyncSummary};

pub(super) fn empty_summary(scanned_log_count: usize) -> EnsV1UnwrappedAuthoritySyncSummary {
    EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_name_surface_count: 0,
        total_resource_count: 0,
        total_surface_binding_count: 0,
        total_normalized_event_count: 0,
        total_normalized_event_inserted_count: 0,
        by_kind: Default::default(),
    }
}

pub(super) fn build_summary(
    scanned_log_count: usize,
    matched_log_count: usize,
    materialized_counts: (usize, usize, usize),
    flushed_event_counts: (usize, usize),
    normalized_event_counts: (usize, usize),
    by_kind: BTreeMap<String, usize>,
) -> EnsV1UnwrappedAuthoritySyncSummary {
    EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: materialized_counts.0,
        total_resource_count: materialized_counts.1,
        total_surface_binding_count: materialized_counts.2,
        total_normalized_event_count: flushed_event_counts.0 + normalized_event_counts.0,
        total_normalized_event_inserted_count: flushed_event_counts.1 + normalized_event_counts.1,
        by_kind,
    }
}

pub(super) struct ReplayTimingLog<'a> {
    pub(super) chain: &'a str,
    pub(super) restrict_to_block_hashes: bool,
    pub(super) block_hash_count: usize,
    pub(super) source_scope_target_count: usize,
    pub(super) active_emitter_count: usize,
    pub(super) scanned_log_count: usize,
    pub(super) matched_log_count: usize,
    pub(super) materialized_counts: (usize, usize, usize, usize),
    pub(super) event_counts: (usize, usize, usize, usize),
    pub(super) timings: ReplayTimings,
}

impl<'a> ReplayTimingLog<'a> {
    pub(super) fn new(
        chain: &'a str,
        flags: (bool, usize, usize, usize),
        scan_counts: (usize, usize),
        materialized_counts: (usize, usize, usize, usize),
        event_counts: (usize, usize, usize, usize),
        timings: ReplayTimings,
    ) -> Self {
        Self {
            chain,
            restrict_to_block_hashes: flags.0,
            block_hash_count: flags.1,
            source_scope_target_count: flags.2,
            active_emitter_count: flags.3,
            scanned_log_count: scan_counts.0,
            matched_log_count: scan_counts.1,
            materialized_counts,
            event_counts,
            timings,
        }
    }
}

pub(super) struct ReplayTimings {
    pub(super) active_emitters_ms: u128,
    pub(super) raw_log_load_ms: u128,
    pub(super) canonical_blocks_ms: u128,
    pub(super) reverse_claim_sources_ms: u128,
    pub(super) resolver_profile_gate_ms: u128,
    pub(super) same_tx_name_intro_ms: u128,
    pub(super) preload_name_metadata_ms: u128,
    pub(super) preload_restricted_histories_ms: u128,
    pub(super) migrated_registry_nodes_ms: u128,
    pub(super) apply_ms: u128,
    pub(super) materialization_ms: u128,
    pub(super) normalize_ms: u128,
    pub(super) closure_ms: u128,
    pub(super) token_lineages_upsert_ms: u128,
    pub(super) resources_upsert_ms: u128,
    pub(super) surfaces_upsert_ms: u128,
    pub(super) binding_closures_upsert_ms: u128,
    pub(super) binding_overlap_repair_count: usize,
    pub(super) binding_overlap_repair_ms: u128,
    pub(super) bindings_upsert_ms: u128,
    pub(super) normalized_events_upsert_ms: u128,
    pub(super) total_ms: u128,
}

impl ReplayTimings {
    pub(super) fn new(
        intake: (u128, u128, u128, u128, u128),
        preload: (u128, u128, u128, u128, u128),
        materialization: (u128, u128, u128),
        upserts: (u128, u128, u128, u128, usize, u128, u128, u128),
        total_ms: u128,
    ) -> Self {
        Self {
            active_emitters_ms: intake.0,
            raw_log_load_ms: intake.1,
            canonical_blocks_ms: intake.2,
            reverse_claim_sources_ms: intake.3,
            resolver_profile_gate_ms: intake.4,
            same_tx_name_intro_ms: preload.0,
            preload_name_metadata_ms: preload.1,
            preload_restricted_histories_ms: preload.2,
            migrated_registry_nodes_ms: preload.3,
            apply_ms: preload.4,
            materialization_ms: materialization.0,
            normalize_ms: materialization.1,
            closure_ms: materialization.2,
            token_lineages_upsert_ms: upserts.0,
            resources_upsert_ms: upserts.1,
            surfaces_upsert_ms: upserts.2,
            binding_closures_upsert_ms: upserts.3,
            binding_overlap_repair_count: upserts.4,
            binding_overlap_repair_ms: upserts.5,
            bindings_upsert_ms: upserts.6,
            normalized_events_upsert_ms: upserts.7,
            total_ms,
        }
    }
}

pub(super) fn log_replay_timing(log: ReplayTimingLog<'_>) {
    let (history_count, token_lineage_count, resource_count, binding_count) =
        log.materialized_counts;
    let (
        normalized_event_count,
        normalized_event_inserted_count,
        flushed_normalized_event_count,
        flushed_normalized_event_inserted_count,
    ) = log.event_counts;
    let timings = log.timings;
    tracing::info!(
        service = "adapters",
        adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        chain = log.chain,
        restrict_to_block_hashes = log.restrict_to_block_hashes,
        block_hash_count = log.block_hash_count,
        source_scope_target_count = log.source_scope_target_count,
        active_emitter_count = log.active_emitter_count,
        scanned_log_count = log.scanned_log_count,
        matched_log_count = log.matched_log_count,
        history_count,
        token_lineage_count,
        resource_count,
        binding_count,
        normalized_event_count,
        flushed_normalized_event_count,
        normalized_event_inserted_count,
        flushed_normalized_event_inserted_count,
        active_emitters_ms = timings.active_emitters_ms,
        raw_log_load_ms = timings.raw_log_load_ms,
        canonical_blocks_ms = timings.canonical_blocks_ms,
        reverse_claim_sources_ms = timings.reverse_claim_sources_ms,
        resolver_profile_gate_ms = timings.resolver_profile_gate_ms,
        same_tx_name_intro_ms = timings.same_tx_name_intro_ms,
        preload_name_metadata_ms = timings.preload_name_metadata_ms,
        preload_restricted_histories_ms = timings.preload_restricted_histories_ms,
        migrated_registry_nodes_ms = timings.migrated_registry_nodes_ms,
        apply_ms = timings.apply_ms,
        materialization_ms = timings.materialization_ms,
        normalize_ms = timings.normalize_ms,
        closure_ms = timings.closure_ms,
        token_lineages_upsert_ms = timings.token_lineages_upsert_ms,
        resources_upsert_ms = timings.resources_upsert_ms,
        surfaces_upsert_ms = timings.surfaces_upsert_ms,
        binding_closures_upsert_ms = timings.binding_closures_upsert_ms,
        binding_overlap_repair_count = timings.binding_overlap_repair_count,
        binding_overlap_repair_ms = timings.binding_overlap_repair_ms,
        bindings_upsert_ms = timings.bindings_upsert_ms,
        normalized_events_upsert_ms = timings.normalized_events_upsert_ms,
        total_ms = timings.total_ms,
        "ENSv1 unwrapped-authority replay timing"
    );
}
