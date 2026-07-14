use bigname_storage::{BackfillLifecycleRow, DiscoveryTargetMissingAddress};

use super::super::backfill_coverage::BackfillCoverageGap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) enum CheckStatus {
    Pass,
    Fail,
}

impl CheckStatus {
    pub(in crate::inspect::data_completeness) const fn label(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
        }
    }

    pub(super) const fn from_pass(pass: bool) -> Self {
        if pass { Self::Pass } else { Self::Fail }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct ChainFrontier {
    pub(in crate::inspect::data_completeness) chain_id: String,
    pub(in crate::inspect::data_completeness) canonical_block_number: Option<i64>,
    pub(in crate::inspect::data_completeness) checkpoint_canonical_lineage_match: bool,
    pub(in crate::inspect::data_completeness) lineage_head_block_number: Option<i64>,
    pub(in crate::inspect::data_completeness) head_lag_blocks: Option<i64>,
    pub(in crate::inspect::data_completeness) contiguous: bool,
    pub(in crate::inspect::data_completeness) missing_block_count: i64,
    pub(in crate::inspect::data_completeness) duplicate_canonical_height_count: i64,
    pub(in crate::inspect::data_completeness) disconnected_canonical_parent_count: i64,
    pub(in crate::inspect::data_completeness) missing_from_storage: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct UnobservedTarget {
    pub(in crate::inspect::data_completeness) chain: String,
    pub(in crate::inspect::data_completeness) address: String,
    pub(in crate::inspect::data_completeness) source_family: String,
    pub(in crate::inspect::data_completeness) active_from_block_number: Option<i64>,
    pub(in crate::inspect::data_completeness) max_observed_block_number: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct HistoryTruncation {
    pub(in crate::inspect::data_completeness) chain: String,
    pub(in crate::inspect::data_completeness) declared_start_block: i64,
    pub(in crate::inspect::data_completeness) lineage_floor_block: Option<i64>,
}

/// An active chain all of whose targets have an open-ended (`NULL`) start block, so no history
/// floor can be established for it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct ChainWithoutFiniteStart {
    pub(in crate::inspect::data_completeness) chain: String,
    pub(in crate::inspect::data_completeness) open_ended_target_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct MissingManifestContent {
    pub(in crate::inspect::data_completeness) manifest_id: i64,
    pub(in crate::inspect::data_completeness) manifest_version: i64,
    pub(in crate::inspect::data_completeness) chain: String,
    pub(in crate::inspect::data_completeness) namespace: String,
    pub(in crate::inspect::data_completeness) source_family: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct MissingManifestLineage {
    pub(in crate::inspect::data_completeness) manifest_id: i64,
    pub(in crate::inspect::data_completeness) manifest_version: i64,
    pub(in crate::inspect::data_completeness) chain: String,
    pub(in crate::inspect::data_completeness) namespace: String,
    pub(in crate::inspect::data_completeness) source_family: String,
    pub(in crate::inspect::data_completeness) missing_canonical_lineage_count: i64,
}

/// `behind_by` is the block or change-id distance to the applicable target, best-effort:
/// a missing bound is treated with a `-1` sentinel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct CursorLag {
    pub(in crate::inspect::data_completeness) label: String,
    pub(in crate::inspect::data_completeness) behind_by: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::inspect::data_completeness) struct DataCompletenessReport {
    pub(in crate::inspect::data_completeness) max_head_lag_blocks: i64,
    pub(in crate::inspect::data_completeness) active_deployment_profile: Option<String>,
    pub(in crate::inspect::data_completeness) frontiers: Vec<ChainFrontier>,
    pub(in crate::inspect::data_completeness) foreign_chains: Vec<String>,
    pub(in crate::inspect::data_completeness) ignored_replay_cursors: Vec<String>,
    pub(in crate::inspect::data_completeness) active_watched_target_count: usize,
    pub(in crate::inspect::data_completeness) unobserved_targets: Vec<UnobservedTarget>,
    pub(in crate::inspect::data_completeness) manifest_targets_missing_address:
        Vec<UnobservedTarget>,
    pub(in crate::inspect::data_completeness) manifest_proxy_implementations_missing_edge:
        Vec<UnobservedTarget>,
    pub(in crate::inspect::data_completeness) discovery_targets_missing_address:
        Vec<DiscoveryTargetMissingAddress>,
    pub(in crate::inspect::data_completeness) chains_history_truncated: Vec<HistoryTruncation>,
    pub(in crate::inspect::data_completeness) chains_without_finite_start:
        Vec<ChainWithoutFiniteStart>,
    pub(in crate::inspect::data_completeness) backfill_coverage_gaps: Vec<BackfillCoverageGap>,
    pub(in crate::inspect::data_completeness) failed_replay_cursors: Vec<String>,
    pub(in crate::inspect::data_completeness) lagging_replay_cursors: Vec<CursorLag>,
    pub(in crate::inspect::data_completeness) chains_missing_raw_fact_cursor: Vec<String>,
    pub(in crate::inspect::data_completeness) lagging_projection_cursors: Vec<CursorLag>,
    pub(in crate::inspect::data_completeness) projection_apply_cursor_missing: bool,
    pub(in crate::inspect::data_completeness) pending_projection_invalidation_count: i64,
    pub(in crate::inspect::data_completeness) projection_invalidation_dead_letter_count: i64,
    pub(in crate::inspect::data_completeness) projection_replay_version: Option<i32>,
    pub(in crate::inspect::data_completeness) projection_replay_required_version: i32,
    pub(in crate::inspect::data_completeness) projection_replay_target_coverage_required: bool,
    pub(in crate::inspect::data_completeness) projection_replay_required_target_block: Option<i64>,
    pub(in crate::inspect::data_completeness) missing_projection_replay_markers: Vec<String>,
    pub(in crate::inspect::data_completeness) active_manifest_sources_without_events:
        Vec<MissingManifestContent>,
    pub(in crate::inspect::data_completeness) active_manifest_sources_with_missing_lineage:
        Vec<MissingManifestLineage>,
    pub(in crate::inspect::data_completeness) active_namespaces_without_names: Vec<String>,
    pub(in crate::inspect::data_completeness) normalized_events_null_chain_id_count: i64,
    pub(in crate::inspect::data_completeness) missing_deferred_projection_indexes: Vec<String>,
    pub(in crate::inspect::data_completeness) backfill_advisory: Vec<BackfillLifecycleRow>,
    pub(in crate::inspect::data_completeness) normalized_event_total: i64,
    pub(in crate::inspect::data_completeness) name_current_total: i64,
}

impl DataCompletenessReport {
    pub(in crate::inspect::data_completeness) fn frontier_at_head(&self) -> CheckStatus {
        CheckStatus::from_pass(self.frontiers.iter().all(|frontier| {
            frontier.checkpoint_canonical_lineage_match
                && frontier.head_lag_blocks.is_some_and(|lag| {
                    (-self.max_head_lag_blocks..=self.max_head_lag_blocks).contains(&lag)
                })
        }))
    }

    pub(in crate::inspect::data_completeness) fn lineage_contiguous(&self) -> CheckStatus {
        CheckStatus::from_pass(self.frontiers.iter().all(|frontier| frontier.contiguous))
    }

    pub(in crate::inspect::data_completeness) fn history_from_declared_start(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.chains_history_truncated.is_empty() && self.chains_without_finite_start.is_empty(),
        )
    }

    pub(in crate::inspect::data_completeness) fn watch_set_observed(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.active_watched_target_count > 0 && self.unobserved_targets.is_empty(),
        )
    }

    pub(in crate::inspect::data_completeness) fn stored_lineage_backfill_coverage(
        &self,
    ) -> CheckStatus {
        CheckStatus::from_pass(self.backfill_coverage_gaps.is_empty())
    }

    pub(in crate::inspect::data_completeness) fn manifest_declared_targets_present(
        &self,
    ) -> CheckStatus {
        CheckStatus::from_pass(
            self.manifest_targets_missing_address.is_empty()
                && self.manifest_proxy_implementations_missing_edge.is_empty(),
        )
    }

    pub(in crate::inspect::data_completeness) fn discovery_targets_present(&self) -> CheckStatus {
        CheckStatus::from_pass(self.discovery_targets_missing_address.is_empty())
    }

    pub(in crate::inspect::data_completeness) fn active_event_lineage_retained(
        &self,
    ) -> CheckStatus {
        CheckStatus::from_pass(self.active_manifest_sources_with_missing_lineage.is_empty())
    }

    pub(in crate::inspect::data_completeness) fn normalization_healthy(&self) -> CheckStatus {
        CheckStatus::from_pass(self.failed_replay_cursors.is_empty())
    }

    pub(in crate::inspect::data_completeness) fn normalization_caught_up(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.active_deployment_profile.is_some()
                && self.lagging_replay_cursors.is_empty()
                && self.chains_missing_raw_fact_cursor.is_empty(),
        )
    }

    pub(in crate::inspect::data_completeness) fn projection_drained(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.lagging_projection_cursors.is_empty() && !self.projection_apply_cursor_missing,
        )
    }

    pub(in crate::inspect::data_completeness) fn projection_invalidations_drained(
        &self,
    ) -> CheckStatus {
        CheckStatus::from_pass(self.pending_projection_invalidation_count == 0)
    }

    pub(in crate::inspect::data_completeness) fn projection_no_dead_letters(&self) -> CheckStatus {
        CheckStatus::from_pass(self.projection_invalidation_dead_letter_count == 0)
    }

    pub(in crate::inspect::data_completeness) fn projection_replay_complete(&self) -> CheckStatus {
        CheckStatus::from_pass(self.missing_projection_replay_markers.is_empty())
    }

    pub(in crate::inspect::data_completeness) fn active_dataset_non_empty(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.active_manifest_sources_without_events.is_empty()
                && self.active_namespaces_without_names.is_empty(),
        )
    }

    pub(in crate::inspect::data_completeness) fn normalized_events_chain_id_present(
        &self,
    ) -> CheckStatus {
        CheckStatus::from_pass(self.normalized_events_null_chain_id_count == 0)
    }

    pub(in crate::inspect::data_completeness) fn deferred_projection_indexes_present(
        &self,
    ) -> CheckStatus {
        CheckStatus::from_pass(self.missing_deferred_projection_indexes.is_empty())
    }

    pub(in crate::inspect::data_completeness) fn data_complete(&self) -> bool {
        [
            self.frontier_at_head(),
            self.lineage_contiguous(),
            self.history_from_declared_start(),
            self.stored_lineage_backfill_coverage(),
            self.watch_set_observed(),
            self.manifest_declared_targets_present(),
            self.discovery_targets_present(),
            self.active_event_lineage_retained(),
            self.normalization_healthy(),
            self.normalization_caught_up(),
            self.projection_drained(),
            self.projection_invalidations_drained(),
            self.projection_no_dead_letters(),
            self.projection_replay_complete(),
            self.active_dataset_non_empty(),
            self.normalized_events_chain_id_present(),
            self.deferred_projection_indexes_present(),
        ]
        .iter()
        .all(|status| *status == CheckStatus::Pass)
    }
}
