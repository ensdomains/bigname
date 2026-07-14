mod report;

pub(super) use report::{CheckStatus, CursorLag, DataCompletenessReport};

use crate::{projection_apply::NORMALIZED_EVENT_CURSOR, replay::ALL_CURRENT_PROJECTION_ORDER};
use bigname_manifests::WatchedContract;
use bigname_storage::{DEFERRED_NORMALIZED_EVENT_INDEXES, DataCompletenessRead};
use report::{
    ChainFrontier, ChainWithoutFiniteStart, HistoryTruncation, MissingManifestContent,
    UnobservedTarget,
};
use std::collections::{BTreeMap, BTreeSet};

/// Blocks the reconciliation frontier may lead or trail the stored canonical checkpoint before
/// the frontier check fails. Reconcile commits canonical lineage and then advances the
/// checkpoint, so on a live database the lineage head routinely leads the checkpoint by a
/// small margin (a negative lag); the tolerance is symmetric and only a larger gap in either
/// direction fails.
pub(super) const DEFAULT_MAX_HEAD_LAG_BLOCKS: i64 = 8;

pub(super) const RAW_FACT_NORMALIZED_EVENTS_CURSOR: &str = "raw_fact_normalized_events";

/// A chain that has this cursor ran closure/dependency replay, which latches the
/// `raw_fact_normalized_events` cursor's target permanently below the live head; newer logs
/// are swept by the backlog cursor and then live adapter sync. On such a chain the raw-fact
/// cursor is caught up when it reaches its own latched target, not the raw-log head.
pub(super) const POST_REPLAY_LIVE_ADAPTER_BACKLOG_CURSOR: &str = "post_replay_live_adapter_backlog";

/// A watched contract is in scope while it has no `active_to_block_number`.
fn is_active(contract: &WatchedContract) -> bool {
    contract.active_to_block_number.is_none()
}

#[derive(Default)]
struct ChainStartInfo {
    finite_min_start: Option<i64>,
    open_ended_target_count: usize,
    target_count: usize,
}

#[derive(Clone)]
struct ActiveTargetInfo {
    source_family: String,
    active_from_block_number: Option<i64>,
}

pub(super) fn evaluate_data_completeness(
    read: &DataCompletenessRead,
    watched_contracts: &[WatchedContract],
    max_head_lag_blocks: i64,
) -> DataCompletenessReport {
    let observed = read
        .observed_code_addresses
        .iter()
        .map(|entry| {
            (
                (entry.chain_id.clone(), entry.address.to_ascii_lowercase()),
                entry.max_observed_block_number,
            )
        })
        .collect::<BTreeMap<_, _>>();

    // Direct manifest declarations remain authority even if a partial restore lost the
    // contract_instance_addresses row that normally materializes them into the watch view.
    // Entries are deduplicated before deriving per-address coverage and per-chain history.
    let mut active_target_entries = BTreeSet::<(String, String, String, Option<i64>)>::new();
    for contract in watched_contracts
        .iter()
        .filter(|contract| is_active(contract))
    {
        active_target_entries.insert((
            contract.chain.clone(),
            contract.address.to_ascii_lowercase(),
            contract.source_family.clone(),
            contract.active_from_block_number,
        ));
    }
    for target in &read.manifest_declared_targets {
        active_target_entries.insert((
            target.chain.clone(),
            target.address.to_ascii_lowercase(),
            target.source_family.clone(),
            target.active_from_block_number,
        ));
    }

    // Coverage is address-scoped. If multiple active source entries share an address, the
    // latest finite start is the strictest lower bound and proves every active entry was
    // observed after its admission.
    let mut active_targets = BTreeMap::<(String, String), ActiveTargetInfo>::new();
    for (chain, address, source_family, active_from_block_number) in &active_target_entries {
        let target = active_targets
            .entry((chain.clone(), address.clone()))
            .or_insert_with(|| ActiveTargetInfo {
                source_family: source_family.clone(),
                active_from_block_number: *active_from_block_number,
            });
        if active_from_block_number > &target.active_from_block_number {
            target.source_family = source_family.clone();
            target.active_from_block_number = *active_from_block_number;
        }
    }

    let unobserved_targets = active_targets
        .iter()
        .filter_map(|((chain, address), target)| {
            let max_observed_block_number =
                observed.get(&(chain.clone(), address.clone())).copied();
            let covered = match target.active_from_block_number {
                Some(start) => max_observed_block_number.is_some_and(|block| block >= start),
                None => max_observed_block_number.is_some(),
            };
            (!covered).then(|| UnobservedTarget {
                chain: chain.clone(),
                address: address.clone(),
                source_family: target.source_family.clone(),
                active_from_block_number: target.active_from_block_number,
                max_observed_block_number,
            })
        })
        .collect::<Vec<_>>();

    // Per-chain declared start information across the deduplicated active source entries.
    let mut chain_starts = BTreeMap::<String, ChainStartInfo>::new();
    for (chain, _, _, active_from_block_number) in &active_target_entries {
        let info = chain_starts.entry(chain.clone()).or_default();
        info.target_count += 1;
        match active_from_block_number {
            Some(start) => {
                info.finite_min_start = Some(
                    info.finite_min_start
                        .map_or(*start, |current| current.min(*start)),
                );
            }
            None => info.open_ended_target_count += 1,
        }
    }

    let manifest_chains = read
        .manifest_chain_namespaces
        .iter()
        .map(|entry| entry.chain.as_str())
        .collect::<BTreeSet<_>>();
    let mut active_chains = chain_starts.keys().cloned().collect::<BTreeSet<_>>();
    active_chains.extend(manifest_chains.iter().map(|chain| (*chain).to_owned()));

    let storage_chains = read
        .chains
        .iter()
        .map(|chain| (chain.chain_id.as_str(), chain))
        .collect::<BTreeMap<_, _>>();

    // Gating frontiers cover active chains only; a foreign or retired chain with residual
    // storage rows is an advisory, not a permanent gate failure.
    let mut frontiers = Vec::new();
    for chain_id in &active_chains {
        match storage_chains.get(chain_id.as_str()) {
            Some(row) => frontiers.push(chain_frontier(row)),
            None => frontiers.push(missing_chain_frontier(chain_id)),
        }
    }
    let foreign_chains = read
        .chains
        .iter()
        .filter(|chain| !active_chains.contains(&chain.chain_id))
        .map(|chain| chain.chain_id.clone())
        .collect::<Vec<_>>();

    // History: a finite declared start requires the lineage floor to reach it; a chain whose
    // targets are all open-ended has no floor to check and fails closed.
    let mut chains_history_truncated = Vec::new();
    let mut chains_without_finite_start = Vec::new();
    for (chain, info) in &chain_starts {
        if info.target_count == 0 {
            continue;
        }
        match info.finite_min_start {
            Some(declared_start) => {
                let floor = storage_chains
                    .get(chain.as_str())
                    .and_then(|row| row.lineage_floor_block_number);
                if !matches!(floor, Some(f) if f <= declared_start) {
                    chains_history_truncated.push(HistoryTruncation {
                        chain: chain.clone(),
                        declared_start_block: declared_start,
                        lineage_floor_block: floor,
                    });
                }
            }
            None => chains_without_finite_start.push(ChainWithoutFiniteStart {
                chain: chain.clone(),
                open_ended_target_count: info.open_ended_target_count,
            }),
        }
    }

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
        .filter(|cursor| active_chains.contains(cursor.chain_id.as_str()))
        .filter_map(|cursor| match cursor.cursor_kind.as_str() {
            RAW_FACT_NORMALIZED_EVENTS_CURSOR => {
                let key = (cursor.deployment_profile.as_str(), cursor.chain_id.as_str());
                if latched_keys.contains(&key) {
                    replay_complete_lag(cursor)
                } else {
                    // Non-latched: replay must have completed its target and the target must
                    // have reached the canonical raw-log head.
                    let head = canonical_raw_log_head
                        .get(cursor.chain_id.as_str())
                        .copied()
                        .flatten()?;
                    if let Some(lag) = replay_complete_lag(cursor) {
                        return Some(lag);
                    }
                    let target = cursor.target_block_number.unwrap_or(-1);
                    (target < head).then(|| CursorLag {
                        label: cursor_label(cursor),
                        behind_by: head - target,
                    })
                }
            }
            POST_REPLAY_LIVE_ADAPTER_BACKLOG_CURSOR => replay_complete_lag(cursor),
            _ => None,
        })
        .collect::<Vec<_>>();

    let chains_with_raw_fact_cursor = read
        .replay_cursors
        .iter()
        .filter(|cursor| cursor.cursor_kind == RAW_FACT_NORMALIZED_EVENTS_CURSOR)
        .map(|cursor| cursor.chain_id.as_str())
        .collect::<BTreeSet<_>>();

    let chains_missing_raw_fact_cursor = read
        .chains
        .iter()
        .filter(|chain| active_chains.contains(&chain.chain_id))
        .filter(|chain| chain.canonical_raw_log_head_block_number.is_some())
        .filter(|chain| !chains_with_raw_fact_cursor.contains(chain.chain_id.as_str()))
        .map(|chain| chain.chain_id.clone())
        .collect::<Vec<_>>();

    let expected_projection_cursor = read
        .projection_apply_cursors
        .iter()
        .find(|cursor| cursor.cursor_name == NORMALIZED_EVENT_CURSOR);
    let lagging_projection_cursors = expected_projection_cursor
        .into_iter()
        .filter_map(|cursor| {
            let max_change_id = read.max_projection_change_id?;
            (cursor.last_change_id < max_change_id).then(|| CursorLag {
                label: cursor.cursor_name.clone(),
                behind_by: max_change_id - cursor.last_change_id,
            })
        })
        .collect::<Vec<_>>();

    let projection_apply_cursor_missing =
        read.max_projection_change_id.is_some() && expected_projection_cursor.is_none();

    // Projection replay markers: require all current projections present at the newest replay
    // version in the database. The version is read from the data, not hardcoded, so a candidate
    // built by an older image is judged complete at its own version.
    let projection_replay_version = read
        .projection_replay_markers
        .iter()
        .map(|marker| marker.replay_version)
        .max();
    let missing_projection_replay_markers = match projection_replay_version {
        Some(version) => {
            let present = read
                .projection_replay_markers
                .iter()
                .filter(|marker| marker.replay_version == version)
                .map(|marker| marker.projection.as_str())
                .collect::<BTreeSet<_>>();
            ALL_CURRENT_PROJECTION_ORDER
                .iter()
                .filter(|projection| !present.contains(*projection))
                .map(|projection| (*projection).to_owned())
                .collect()
        }
        None => ALL_CURRENT_PROJECTION_ORDER
            .iter()
            .map(|projection| (*projection).to_owned())
            .collect(),
    };

    // Content expectations come only from active manifest sources that declare normalized
    // adapter output. Counts are joined to the exact source_manifest_id by storage, so rows
    // from deprecated manifests cannot satisfy a newly active source.
    let active_manifest_sources_without_events = read
        .active_manifest_event_sources
        .iter()
        .filter(|source| source.normalized_event_count == 0)
        .map(|source| MissingManifestContent {
            manifest_id: source.manifest_id,
            manifest_version: source.manifest_version,
            chain: source.chain.clone(),
            namespace: source.namespace.clone(),
            source_family: source.source_family.clone(),
        })
        .collect::<Vec<_>>();
    let names_by_namespace = read
        .name_current_counts
        .iter()
        .map(|entry| (entry.namespace.as_str(), entry.count))
        .collect::<BTreeMap<_, _>>();
    let active_namespaces_without_names = read
        .active_manifest_event_sources
        .iter()
        .map(|source| source.namespace.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|namespace| names_by_namespace.get(namespace).copied().unwrap_or(0) == 0)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    let present_indexes = read
        .present_deferred_projection_indexes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let missing_deferred_projection_indexes = DEFERRED_NORMALIZED_EVENT_INDEXES
        .iter()
        .filter(|index| !present_indexes.contains(*index))
        .map(|index| (*index).to_owned())
        .collect::<Vec<_>>();

    let normalized_event_total = read
        .active_manifest_event_sources
        .iter()
        .map(|source| source.normalized_event_count)
        .sum();
    let name_current_total = read.name_current_counts.iter().map(|e| e.count).sum();

    DataCompletenessReport {
        max_head_lag_blocks,
        frontiers,
        foreign_chains,
        active_watched_target_count: active_targets.len(),
        unobserved_targets,
        chains_history_truncated,
        chains_without_finite_start,
        failed_replay_cursors,
        lagging_replay_cursors,
        chains_missing_raw_fact_cursor,
        lagging_projection_cursors,
        projection_apply_cursor_missing,
        pending_projection_invalidation_count: read.pending_projection_invalidation_count,
        projection_invalidation_dead_letter_count: read.projection_invalidation_dead_letter_count,
        projection_replay_version,
        missing_projection_replay_markers,
        active_manifest_sources_without_events,
        active_namespaces_without_names,
        normalized_events_null_chain_id_count: read.normalized_events_null_chain_id_count,
        missing_deferred_projection_indexes,
        backfill_advisory: read.backfill_lifecycle.clone(),
        normalized_event_total,
        name_current_total,
    }
}

/// Replay is complete for a cursor's target when `next_block_number > target_block_number`.
/// A reorg rewind lowers `next_block_number` below the target, so a stale high
/// `last_completed_block_number` no longer reads as caught up. A missing bound fails closed.
fn replay_complete_lag(cursor: &bigname_storage::ReplayCursorRow) -> Option<CursorLag> {
    match (cursor.next_block_number, cursor.target_block_number) {
        (Some(next), Some(target)) if next > target => None,
        (next, Some(target)) => Some(CursorLag {
            label: cursor_label(cursor),
            behind_by: target - next.unwrap_or(-1),
        }),
        (_, None) => Some(CursorLag {
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
        disconnected_canonical_parent_count: 0,
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
            && chain.duplicate_canonical_height_count == 0
            && chain.disconnected_canonical_parent_count == 0,
        missing_block_count,
        duplicate_canonical_height_count: chain.duplicate_canonical_height_count,
        disconnected_canonical_parent_count: chain.disconnected_canonical_parent_count,
        missing_from_storage: false,
    }
}
