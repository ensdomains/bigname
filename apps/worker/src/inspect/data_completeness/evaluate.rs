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
    pub(super) duplicate_canonical_height_count: i64,
    /// True when the chain is declared by the active watch set but has no checkpoint or
    /// lineage row, so it produced no frontier data of its own.
    pub(super) missing_from_storage: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct UnobservedTarget {
    pub(super) chain: String,
    pub(super) address: String,
    pub(super) source_family: String,
}

/// A chain whose retained lineage does not reach back to the earliest block its active
/// watched targets declare, so its history is truncated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct HistoryTruncation {
    pub(super) chain: String,
    pub(super) declared_start_block: i64,
    pub(super) lineage_floor_block: Option<i64>,
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
    pub(super) chains_history_truncated: Vec<HistoryTruncation>,
    pub(super) failed_replay_cursors: Vec<String>,
    pub(super) lagging_replay_cursors: Vec<CursorLag>,
    pub(super) chains_missing_raw_fact_cursor: Vec<String>,
    pub(super) lagging_projection_cursors: Vec<CursorLag>,
    pub(super) projection_apply_cursor_missing: bool,
    pub(super) pending_projection_invalidation_count: i64,
    pub(super) projection_invalidation_dead_letter_count: i64,
    pub(super) active_chains_without_events: Vec<String>,
    pub(super) active_namespaces_without_names: Vec<String>,
    pub(super) normalized_event_total: i64,
    pub(super) name_current_total: i64,
}

impl DataCompletenessReport {
    pub(super) fn frontier_at_head(&self) -> CheckStatus {
        // Reject a negative lag: a canonical checkpoint behind the retained lineage head is a
        // stale checkpoint writer or a mixed restore, not a caught-up frontier.
        CheckStatus::from_pass(self.frontiers.iter().all(|frontier| {
            frontier
                .head_lag_blocks
                .is_some_and(|lag| (0..=self.max_head_lag_blocks).contains(&lag))
        }))
    }

    pub(super) fn lineage_contiguous(&self) -> CheckStatus {
        CheckStatus::from_pass(self.frontiers.iter().all(|frontier| frontier.contiguous))
    }

    pub(super) fn history_from_declared_start(&self) -> CheckStatus {
        CheckStatus::from_pass(self.chains_history_truncated.is_empty())
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

    pub(super) fn active_dataset_non_empty(&self) -> CheckStatus {
        CheckStatus::from_pass(
            self.active_chains_without_events.is_empty()
                && self.active_namespaces_without_names.is_empty(),
        )
    }

    pub(super) fn data_complete(&self) -> bool {
        [
            self.frontier_at_head(),
            self.lineage_contiguous(),
            self.history_from_declared_start(),
            self.watch_set_observed(),
            self.normalization_healthy(),
            self.normalization_caught_up(),
            self.projection_drained(),
            self.projection_invalidations_drained(),
            self.projection_no_dead_letters(),
            self.active_dataset_non_empty(),
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

    // Chains the active watch set declares, each with the earliest finite start block across
    // its active targets (None if every target has an open-ended start).
    let mut active_watched_chains = BTreeMap::<String, Option<i64>>::new();
    for contract in watched_contracts
        .iter()
        .filter(|contract| is_active(contract))
    {
        let entry = active_watched_chains
            .entry(contract.chain.clone())
            .or_insert(None);
        if let Some(start) = contract.active_from_block_number {
            *entry = Some(entry.map_or(start, |current| current.min(start)));
        }
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

    let present_chains = read
        .chains
        .iter()
        .map(|chain| chain.chain_id.as_str())
        .collect::<BTreeSet<_>>();

    // Frontiers from storage, plus a synthesized failing frontier for any active watched chain
    // absent from both checkpoints and lineage, so an expected chain cannot pass by absence.
    let mut frontiers = read.chains.iter().map(chain_frontier).collect::<Vec<_>>();
    for chain_id in active_watched_chains.keys() {
        if !present_chains.contains(chain_id.as_str()) {
            frontiers.push(missing_chain_frontier(chain_id));
        }
    }

    let lineage_floor = read
        .chains
        .iter()
        .map(|chain| (chain.chain_id.as_str(), chain.lineage_floor_block_number))
        .collect::<BTreeMap<_, _>>();

    let chains_history_truncated = active_watched_chains
        .iter()
        .filter_map(|(chain, min_start)| {
            let declared_start = (*min_start)?;
            let floor = lineage_floor.get(chain.as_str()).copied().flatten();
            match floor {
                Some(f) if f <= declared_start => None,
                _ => Some(HistoryTruncation {
                    chain: chain.clone(),
                    declared_start_block: declared_start,
                    lineage_floor_block: floor,
                }),
            }
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

    // A chain with retained canonical raw logs but no raw-fact cursor row would pass vacuously,
    // since a missing cursor produces no lag entry. Require the cursor to exist.
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

    // Content scoped to the active dataset: a newly active chain with zero events must not be
    // masked by another chain's rows in the global tables.
    let active_chain_set = active_watched_chains
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    let mut events_by_chain = BTreeMap::<&str, i64>::new();
    let mut active_namespaces = BTreeSet::<&str>::new();
    for entry in &read.normalized_event_counts {
        *events_by_chain.entry(entry.chain_id.as_str()).or_insert(0) += entry.count;
        if entry.count > 0 && active_chain_set.contains(entry.chain_id.as_str()) {
            active_namespaces.insert(entry.namespace.as_str());
        }
    }

    let active_chains_without_events = active_chain_set
        .iter()
        .filter(|chain| events_by_chain.get(**chain).copied().unwrap_or(0) == 0)
        .map(|chain| (*chain).to_owned())
        .collect::<Vec<_>>();

    let names_by_namespace = read
        .name_current_counts
        .iter()
        .map(|entry| (entry.namespace.as_str(), entry.count))
        .collect::<BTreeMap<_, _>>();

    let active_namespaces_without_names = active_namespaces
        .iter()
        .filter(|namespace| names_by_namespace.get(**namespace).copied().unwrap_or(0) == 0)
        .map(|namespace| (*namespace).to_owned())
        .collect::<Vec<_>>();

    let normalized_event_total = read.normalized_event_counts.iter().map(|e| e.count).sum();
    let name_current_total = read.name_current_counts.iter().map(|e| e.count).sum();

    DataCompletenessReport {
        max_head_lag_blocks,
        frontiers,
        active_watched_target_count: active_targets.len(),
        unobserved_targets,
        chains_history_truncated,
        failed_replay_cursors,
        lagging_replay_cursors,
        chains_missing_raw_fact_cursor,
        lagging_projection_cursors,
        projection_apply_cursor_missing,
        pending_projection_invalidation_count: read.pending_projection_invalidation_count,
        projection_invalidation_dead_letter_count: read.projection_invalidation_dead_letter_count,
        active_chains_without_events,
        active_namespaces_without_names,
        normalized_event_total,
        name_current_total,
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

fn missing_chain_frontier(chain_id: &str) -> ChainFrontier {
    ChainFrontier {
        chain_id: chain_id.to_owned(),
        canonical_block_number: None,
        lineage_head_block_number: None,
        head_lag_blocks: None,
        contiguous: false,
        missing_block_count: 0,
        duplicate_canonical_height_count: 0,
        missing_from_storage: true,
    }
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
        contiguous: expected_block_count.is_some()
            && missing_block_count == 0
            && chain.duplicate_canonical_height_count == 0,
        missing_block_count,
        duplicate_canonical_height_count: chain.duplicate_canonical_height_count,
        missing_from_storage: false,
    }
}
