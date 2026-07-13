use bigname_manifests::WatchedContract;
use bigname_storage::DataCompletenessRead;
use std::collections::{BTreeMap, BTreeSet};

/// Blocks the reconciliation frontier may trail the stored canonical checkpoint before the
/// frontier check fails. Head advances while the read runs, so zero would be unstable.
pub(super) const DEFAULT_MAX_HEAD_LAG_BLOCKS: i64 = 8;

pub(super) const RAW_FACT_NORMALIZED_EVENTS_CURSOR: &str = "raw_fact_normalized_events";

/// A chain that has this cursor ran closure/dependency replay, which latches the
/// `raw_fact_normalized_events` cursor's target permanently below the live head; newer logs
/// are swept by the backlog cursor and then live adapter sync. On such a chain the raw-fact
/// cursor is caught up when it reaches its own latched target, not the raw-log head.
pub(super) const POST_REPLAY_LIVE_ADAPTER_BACKLOG_CURSOR: &str = "post_replay_live_adapter_backlog";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CheckStatus {
    Pass,
    Fail,
}

impl CheckStatus {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
        }
    }

    const fn from_pass(pass: bool) -> Self {
        if pass { Self::Pass } else { Self::Fail }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ChainFrontier {
    pub(super) chain_id: String,
    pub(super) canonical_block_number: Option<i64>,
    pub(super) lineage_head_block_number: Option<i64>,
    pub(super) head_lag_blocks: Option<i64>,
    pub(super) contiguous: bool,
    pub(super) missing_block_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct UnobservedTarget {
    pub(super) chain: String,
    pub(super) address: String,
    pub(super) source_family: String,
}

/// `behind_by` is the block or change-id distance to the applicable target, best-effort:
/// a missing `last_completed` is treated as `-1`, and a latched cursor with no target uses
/// the `-1` sentinel because there is no target to measure against.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CursorLag {
    pub(super) label: String,
    pub(super) behind_by: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DataCompletenessReport {
    pub(super) max_head_lag_blocks: i64,
    pub(super) frontiers: Vec<ChainFrontier>,
    pub(super) active_watched_target_count: usize,
    pub(super) unobserved_targets: Vec<UnobservedTarget>,
    pub(super) failed_replay_cursors: Vec<String>,
    pub(super) lagging_replay_cursors: Vec<CursorLag>,
    pub(super) chains_missing_raw_fact_cursor: Vec<String>,
    pub(super) lagging_projection_cursors: Vec<CursorLag>,
    pub(super) projection_apply_cursor_missing: bool,
    pub(super) pending_projection_invalidation_count: i64,
    pub(super) projection_invalidation_dead_letter_count: i64,
    pub(super) normalized_event_count: i64,
    pub(super) name_current_count: i64,
}

impl DataCompletenessReport {
    pub(super) fn frontier_at_head(&self) -> CheckStatus {
        CheckStatus::from_pass(self.frontiers.iter().all(|frontier| {
            frontier
                .head_lag_blocks
                .is_some_and(|lag| lag <= self.max_head_lag_blocks)
        }))
    }

    pub(super) fn lineage_contiguous(&self) -> CheckStatus {
        CheckStatus::from_pass(self.frontiers.iter().all(|frontier| frontier.contiguous))
    }

    pub(super) fn watch_set_observed(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.active_watched_target_count > 0 && self.unobserved_targets.is_empty(),
        )
    }

    pub(super) fn normalization_healthy(&self) -> CheckStatus {
        CheckStatus::from_pass(self.failed_replay_cursors.is_empty())
    }

    pub(super) fn normalization_caught_up(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.lagging_replay_cursors.is_empty()
                && self.chains_missing_raw_fact_cursor.is_empty(),
        )
    }

    pub(super) fn projection_drained(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.lagging_projection_cursors.is_empty() && !self.projection_apply_cursor_missing,
        )
    }

    pub(super) fn projection_invalidations_drained(&self) -> CheckStatus {
        CheckStatus::from_pass(self.pending_projection_invalidation_count == 0)
    }

    pub(super) fn projection_no_dead_letters(&self) -> CheckStatus {
        CheckStatus::from_pass(self.projection_invalidation_dead_letter_count == 0)
    }

    pub(super) fn content_present(&self) -> CheckStatus {
        CheckStatus::from_pass(self.normalized_event_count > 0 && self.name_current_count > 0)
    }

    pub(super) fn data_complete(&self) -> bool {
        [
            self.frontier_at_head(),
            self.lineage_contiguous(),
            self.watch_set_observed(),
            self.normalization_healthy(),
            self.normalization_caught_up(),
            self.projection_drained(),
            self.projection_invalidations_drained(),
            self.projection_no_dead_letters(),
            self.content_present(),
        ]
        .iter()
        .all(|status| *status == CheckStatus::Pass)
    }
}

/// A watched contract is in scope while it has no `active_to_block_number`.
fn is_active(contract: &WatchedContract) -> bool {
    contract.active_to_block_number.is_none()
}

pub(super) fn evaluate_data_completeness(
    read: &DataCompletenessRead,
    watched_contracts: &[WatchedContract],
    max_head_lag_blocks: i64,
) -> DataCompletenessReport {
    let observed = read
        .observed_code_addresses
        .iter()
        .map(|entry| (entry.chain_id.as_str(), entry.address.as_str()))
        .collect::<BTreeSet<_>>();

    // load_watched_contracts returns one row per source entry, so a target can repeat across
    // source families; coverage is a property of the (chain, address) pair.
    let mut active_targets = BTreeMap::<(&str, String), &WatchedContract>::new();
    for contract in watched_contracts
        .iter()
        .filter(|contract| is_active(contract))
    {
        active_targets
            .entry((
                contract.chain.as_str(),
                contract.address.to_ascii_lowercase(),
            ))
            .or_insert(contract);
    }

    let unobserved_targets = active_targets
        .iter()
        .filter(|((chain, address), _)| !observed.contains(&(*chain, address.as_str())))
        .map(|((chain, address), contract)| UnobservedTarget {
            chain: (*chain).to_owned(),
            address: address.clone(),
            source_family: contract.source_family.clone(),
        })
        .collect::<Vec<_>>();

    let canonical_raw_log_head = read
        .chains
        .iter()
        .map(|chain| {
            (
                chain.chain_id.as_str(),
                chain.canonical_raw_log_head_block_number,
            )
        })
        .collect::<BTreeMap<_, _>>();

    // A (deployment_profile, chain_id) with a backlog cursor ran closure replay, so its
    // raw-fact cursor is latched to its own target rather than the raw-log head.
    let latched_keys = read
        .replay_cursors
        .iter()
        .filter(|cursor| cursor.cursor_kind == POST_REPLAY_LIVE_ADAPTER_BACKLOG_CURSOR)
        .map(|cursor| (cursor.deployment_profile.as_str(), cursor.chain_id.as_str()))
        .collect::<BTreeSet<_>>();

    let failed_replay_cursors = read
        .replay_cursors
        .iter()
        .filter(|cursor| cursor.last_failure_reason.is_some())
        .map(cursor_label)
        .collect::<Vec<_>>();

    let lagging_replay_cursors = read
        .replay_cursors
        .iter()
        .filter_map(|cursor| match cursor.cursor_kind.as_str() {
            RAW_FACT_NORMALIZED_EVENTS_CURSOR => {
                let key = (cursor.deployment_profile.as_str(), cursor.chain_id.as_str());
                if latched_keys.contains(&key) {
                    latched_cursor_lag(cursor)
                } else {
                    // Non-latched: caught up when replay reached the canonical raw-log head.
                    let head = canonical_raw_log_head
                        .get(cursor.chain_id.as_str())
                        .copied()
                        .flatten()?;
                    let completed = cursor.last_completed_block_number.unwrap_or(-1);
                    (completed < head).then(|| CursorLag {
                        label: cursor_label(cursor),
                        behind_by: head - completed,
                    })
                }
            }
            POST_REPLAY_LIVE_ADAPTER_BACKLOG_CURSOR => latched_cursor_lag(cursor),
            _ => None,
        })
        .collect::<Vec<_>>();

    // Fix: a chain with retained canonical raw logs but no raw-fact cursor row would pass
    // vacuously, since a missing cursor produces no lag entry. Require the cursor to exist.
    let chains_with_raw_fact_cursor = read
        .replay_cursors
        .iter()
        .filter(|cursor| cursor.cursor_kind == RAW_FACT_NORMALIZED_EVENTS_CURSOR)
        .map(|cursor| cursor.chain_id.as_str())
        .collect::<BTreeSet<_>>();

    let chains_missing_raw_fact_cursor = read
        .chains
        .iter()
        .filter(|chain| chain.canonical_raw_log_head_block_number.is_some())
        .filter(|chain| !chains_with_raw_fact_cursor.contains(chain.chain_id.as_str()))
        .map(|chain| chain.chain_id.clone())
        .collect::<Vec<_>>();

    let lagging_projection_cursors = read
        .projection_apply_cursors
        .iter()
        .filter_map(|cursor| {
            let max_change_id = read.max_projection_change_id?;
            (cursor.last_change_id < max_change_id).then(|| CursorLag {
                label: cursor.cursor_name.clone(),
                behind_by: max_change_id - cursor.last_change_id,
            })
        })
        .collect::<Vec<_>>();

    // A non-empty change log with no apply cursor row means nothing has consumed it.
    let projection_apply_cursor_missing =
        read.max_projection_change_id.is_some() && read.projection_apply_cursors.is_empty();

    DataCompletenessReport {
        max_head_lag_blocks,
        frontiers: read.chains.iter().map(chain_frontier).collect(),
        active_watched_target_count: active_targets.len(),
        unobserved_targets,
        failed_replay_cursors,
        lagging_replay_cursors,
        chains_missing_raw_fact_cursor,
        lagging_projection_cursors,
        projection_apply_cursor_missing,
        pending_projection_invalidation_count: read.pending_projection_invalidation_count,
        projection_invalidation_dead_letter_count: read.projection_invalidation_dead_letter_count,
        normalized_event_count: read.normalized_event_count,
        name_current_count: read.name_current_count,
    }
}

/// A latched cursor is caught up only when it has both a completed block and a target and
/// has reached that target. A missing `last_completed` counts as lagging; a missing target
/// is unverifiable and fails closed with the `-1` sentinel distance.
fn latched_cursor_lag(cursor: &bigname_storage::ReplayCursorRow) -> Option<CursorLag> {
    let completed = cursor.last_completed_block_number.unwrap_or(-1);
    match cursor.target_block_number {
        Some(target) if completed >= target => None,
        Some(target) => Some(CursorLag {
            label: cursor_label(cursor),
            behind_by: target - completed,
        }),
        None => Some(CursorLag {
            label: cursor_label(cursor),
            behind_by: -1,
        }),
    }
}

fn cursor_label(cursor: &bigname_storage::ReplayCursorRow) -> String {
    format!(
        "{}/{}/{}",
        cursor.deployment_profile, cursor.chain_id, cursor.cursor_kind
    )
}

fn chain_frontier(chain: &bigname_storage::ChainCompletenessRow) -> ChainFrontier {
    let head_lag_blocks = chain
        .canonical_block_number
        .zip(chain.lineage_head_block_number)
        .map(|(canonical, lineage_head)| canonical - lineage_head);

    let expected_block_count = chain
        .lineage_head_block_number
        .zip(chain.lineage_floor_block_number)
        .map(|(head, floor)| head - floor + 1);
    let missing_block_count = expected_block_count
        .map(|expected| expected - chain.lineage_canonical_block_count)
        .unwrap_or_default();

    ChainFrontier {
        chain_id: chain.chain_id.clone(),
        canonical_block_number: chain.canonical_block_number,
        lineage_head_block_number: chain.lineage_head_block_number,
        head_lag_blocks,
        contiguous: expected_block_count.is_some() && missing_block_count == 0,
        missing_block_count,
    }
}
