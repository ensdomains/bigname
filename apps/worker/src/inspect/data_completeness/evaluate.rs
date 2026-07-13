mod report;

pub(super) use report::{CheckStatus, CursorLag, DataCompletenessReport};

use crate::replay::ALL_CURRENT_PROJECTION_ORDER;
use bigname_manifests::WatchedContract;
use bigname_storage::{DEFERRED_NORMALIZED_EVENT_INDEXES, DataCompletenessRead};
use report::{
    ChainFrontier, ChainWithoutFiniteStart, HistoryTruncation, MissingNamespaceContent,
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

    // Per-chain declared start information across active watched targets.
    let mut chain_starts = BTreeMap::<String, ChainStartInfo>::new();
    for contract in watched_contracts
        .iter()
        .filter(|contract| is_active(contract))
    {
        let info = chain_starts.entry(contract.chain.clone()).or_default();
        info.target_count += 1;
        match contract.active_from_block_number {
            Some(start) => {
                info.finite_min_start = Some(
                    info.finite_min_start
                        .map_or(start, |current| current.min(start)),
                );
            }
            None => info.open_ended_target_count += 1,
        }
    }

    // Chain authority: the watch view plus active manifest versions directly, so a partial
    // restore that lost contract_instance_addresses rows cannot delete a chain from its own
    // expectations.
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

    let projection_apply_cursor_missing =
        read.max_projection_change_id.is_some() && read.projection_apply_cursors.is_empty();

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

    // Content expectation from declared authority: every (chain, namespace) an active manifest
    // declares must have non-orphaned normalized events, and every such namespace must have
    // name_current rows.
    let events_by_chain_namespace = read
        .normalized_event_counts
        .iter()
        .map(|entry| {
            (
                (entry.chain_id.as_str(), entry.namespace.as_str()),
                entry.count,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let names_by_namespace = read
        .name_current_counts
        .iter()
        .map(|entry| (entry.namespace.as_str(), entry.count))
        .collect::<BTreeMap<_, _>>();

    let active_chain_namespaces_without_events = read
        .manifest_chain_namespaces
        .iter()
        .filter(|entry| {
            events_by_chain_namespace
                .get(&(entry.chain.as_str(), entry.namespace.as_str()))
                .copied()
                .unwrap_or(0)
                == 0
        })
        .map(|entry| MissingNamespaceContent {
            chain: entry.chain.clone(),
            namespace: entry.namespace.clone(),
        })
        .collect::<Vec<_>>();

    let active_namespaces_without_names = read
        .manifest_chain_namespaces
        .iter()
        .map(|entry| entry.namespace.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|namespace| names_by_namespace.get(namespace).copied().unwrap_or(0) == 0)
        .map(|namespace| namespace.to_owned())
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

    let normalized_event_total = read.normalized_event_counts.iter().map(|e| e.count).sum();
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
        active_chain_namespaces_without_events,
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
