use super::evaluate::{CheckStatus, DEFAULT_MAX_HEAD_LAG_BLOCKS, evaluate_data_completeness};
use crate::replay::ALL_CURRENT_PROJECTION_ORDER;
use bigname_manifests::{WatchedContract, WatchedContractSource};
use bigname_storage::{
    BackfillLifecycleRow, ChainCompletenessRow, DEFERRED_NORMALIZED_EVENT_INDEXES,
    DataCompletenessRead, ManifestChainNamespace, NameCurrentCount, NormalizedEventCount,
    ObservedCodeAddress, ProjectionApplyCursorRow, ProjectionReplayMarker, ReplayCursorRow,
};
use uuid::Uuid;

const CHAIN: &str = "ethereum-sepolia";
const NAMESPACE: &str = "ens";
const REGISTRY: &str = "0x796fff2e907449be8d5921bcc215b1b76d89d080";
const RESOLVER: &str = "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5";
const APPLY_CURSOR: &str = "normalized_events_to_projection_invalidations";

fn manifest_ns(chain: &str, namespace: &str) -> ManifestChainNamespace {
    ManifestChainNamespace {
        chain: chain.to_owned(),
        namespace: namespace.to_owned(),
    }
}

fn all_projection_markers(version: i32) -> Vec<ProjectionReplayMarker> {
    ALL_CURRENT_PROJECTION_ORDER
        .iter()
        .map(|projection| ProjectionReplayMarker {
            replay_version: version,
            projection: (*projection).to_owned(),
        })
        .collect()
}

fn all_deferred_indexes() -> Vec<String> {
    DEFERRED_NORMALIZED_EVENT_INDEXES
        .iter()
        .map(|name| (*name).to_owned())
        .collect()
}

fn watched(address: &str, source: WatchedContractSource) -> WatchedContract {
    WatchedContract {
        chain: CHAIN.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        address: address.to_owned(),
        contract_instance_id: Uuid::nil(),
        source,
        source_manifest_id: None,
        active_from_block_number: Some(1),
        active_to_block_number: None,
    }
}

fn observed(address: &str) -> ObservedCodeAddress {
    ObservedCodeAddress {
        chain_id: CHAIN.to_owned(),
        address: address.to_owned(),
    }
}

fn events(chain: &str, namespace: &str, count: i64) -> NormalizedEventCount {
    NormalizedEventCount {
        chain_id: chain.to_owned(),
        namespace: namespace.to_owned(),
        count,
    }
}

fn names(namespace: &str, count: i64) -> NameCurrentCount {
    NameCurrentCount {
        namespace: namespace.to_owned(),
        count,
    }
}

fn chain_row(
    canonical: i64,
    lineage_head: i64,
    floor: i64,
    block_count: i64,
) -> ChainCompletenessRow {
    ChainCompletenessRow {
        chain_id: CHAIN.to_owned(),
        canonical_block_number: Some(canonical),
        lineage_head_block_number: Some(lineage_head),
        lineage_floor_block_number: Some(floor),
        lineage_canonical_block_count: block_count,
        duplicate_canonical_height_count: 0,
        canonical_raw_log_head_block_number: Some(floor),
        raw_log_head_block_number: Some(floor),
    }
}

// A raw-fact cursor caught up to `target`: replay is complete when next > target.
fn replay_cursor(target: i64, failure: Option<&str>) -> ReplayCursorRow {
    ReplayCursorRow {
        deployment_profile: "sepolia".to_owned(),
        chain_id: CHAIN.to_owned(),
        cursor_kind: "raw_fact_normalized_events".to_owned(),
        next_block_number: Some(target + 1),
        target_block_number: Some(target),
        last_completed_block_number: Some(target),
        last_failure_reason: failure.map(str::to_owned),
    }
}

// A cursor whose `next` was rewound below `target` while `last_completed` stays high.
fn rewound_cursor(next: i64, target: i64, last_completed: i64) -> ReplayCursorRow {
    ReplayCursorRow {
        deployment_profile: "sepolia".to_owned(),
        chain_id: CHAIN.to_owned(),
        cursor_kind: "raw_fact_normalized_events".to_owned(),
        next_block_number: Some(next),
        target_block_number: Some(target),
        last_completed_block_number: Some(last_completed),
        last_failure_reason: None,
    }
}

// A backlog cursor caught up to `target` when `next > target`.
fn backlog_cursor(next: i64, target: i64) -> ReplayCursorRow {
    ReplayCursorRow {
        deployment_profile: "sepolia".to_owned(),
        chain_id: CHAIN.to_owned(),
        cursor_kind: "post_replay_live_adapter_backlog".to_owned(),
        next_block_number: Some(next),
        target_block_number: Some(target),
        last_completed_block_number: Some(next - 1),
        last_failure_reason: None,
    }
}

fn apply_cursor(last_change_id: i64) -> ProjectionApplyCursorRow {
    ProjectionApplyCursorRow {
        cursor_name: APPLY_CURSOR.to_owned(),
        last_change_id,
    }
}

fn healthy_read() -> DataCompletenessRead {
    DataCompletenessRead {
        chains: vec![chain_row(1_000, 1_000, 1, 1_000)],
        replay_cursors: vec![replay_cursor(1, None)],
        projection_apply_cursors: vec![apply_cursor(42)],
        max_projection_change_id: Some(42),
        pending_projection_invalidation_count: 0,
        projection_invalidation_dead_letter_count: 0,
        observed_code_addresses: vec![observed(REGISTRY)],
        normalized_event_counts: vec![events(CHAIN, NAMESPACE, 100)],
        name_current_counts: vec![names(NAMESPACE, 10)],
        normalized_events_null_chain_id_count: 0,
        projection_replay_markers: all_projection_markers(6),
        backfill_lifecycle: vec![],
        present_deferred_projection_indexes: all_deferred_indexes(),
        manifest_chain_namespaces: vec![manifest_ns(CHAIN, NAMESPACE)],
    }
}

fn evaluate(
    read: &DataCompletenessRead,
    watched_contracts: &[WatchedContract],
) -> super::evaluate::DataCompletenessReport {
    evaluate_data_completeness(read, watched_contracts, DEFAULT_MAX_HEAD_LAG_BLOCKS)
}

fn registry_only() -> Vec<WatchedContract> {
    vec![watched(REGISTRY, WatchedContractSource::ManifestContract)]
}

#[test]
fn healthy_database_is_data_complete() {
    assert!(evaluate(&healthy_read(), &registry_only()).data_complete());
}

/// The 2026-07-06 shape: every cursor reports "done" because each measures itself against the
/// previous stage's frontier, while the watch set silently excludes discovered targets.
#[test]
fn discovered_target_without_code_observation_fails_watch_set_coverage() {
    let watched_contracts = vec![
        watched(REGISTRY, WatchedContractSource::ManifestContract),
        watched(RESOLVER, WatchedContractSource::DiscoveryEdge),
    ];
    let report = evaluate(&healthy_read(), &watched_contracts);

    assert_eq!(report.watch_set_observed(), CheckStatus::Fail);
    assert_eq!(report.unobserved_targets.len(), 1);
    assert_eq!(report.unobserved_targets[0].address, RESOLVER);
    assert!(!report.data_complete());

    assert_eq!(report.normalization_healthy(), CheckStatus::Pass);
    assert_eq!(report.normalization_caught_up(), CheckStatus::Pass);
    assert_eq!(report.projection_drained(), CheckStatus::Pass);
    assert_eq!(report.active_dataset_non_empty(), CheckStatus::Pass);
}

#[test]
fn inactive_watched_target_is_not_required_to_be_observed() {
    let mut retired = watched(RESOLVER, WatchedContractSource::DiscoveryEdge);
    retired.active_to_block_number = Some(500);
    let watched_contracts = vec![
        watched(REGISTRY, WatchedContractSource::ManifestContract),
        retired,
    ];

    assert!(evaluate(&healthy_read(), &watched_contracts).data_complete());
}

#[test]
fn watch_set_coverage_matches_addresses_case_insensitively() {
    let watched_contracts = vec![watched(
        &REGISTRY.to_ascii_uppercase(),
        WatchedContractSource::ManifestContract,
    )];
    assert!(evaluate(&healthy_read(), &watched_contracts).data_complete());
}

/// A manifest-less restore reads zero watched contracts. The coverage check is the
/// load-bearing one, so it must not pass vacuously when there is nothing to observe.
#[test]
fn empty_watch_set_fails_coverage() {
    let report = evaluate(&healthy_read(), &[]);

    assert_eq!(report.active_watched_target_count, 0);
    assert_eq!(report.watch_set_observed(), CheckStatus::Fail);
    assert!(!report.data_complete());
}

#[test]
fn reconciliation_frontier_behind_head_beyond_tolerance_fails() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_000, 900, 1, 900)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.frontier_at_head(), CheckStatus::Fail);
    assert_eq!(report.frontiers[0].head_lag_blocks, Some(100));
    assert!(!report.data_complete());
}

#[test]
fn reconciliation_frontier_within_tolerance_passes() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_004, 1_000, 1, 1_000)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.frontier_at_head(), CheckStatus::Pass);
}

#[test]
fn lineage_gap_fails_contiguity() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_000, 1_000, 1, 999)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.lineage_contiguous(), CheckStatus::Fail);
    assert_eq!(report.frontiers[0].missing_block_count, 1);
    assert!(!report.data_complete());
}

/// The LabelReserved crash-loop: the cursor stops advancing and records why.
#[test]
fn replay_cursor_failure_reason_fails_normalization() {
    let mut read = healthy_read();
    read.replay_cursors = vec![replay_cursor(1, Some("LabelReserved expiry exceeds i64"))];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_healthy(), CheckStatus::Fail);
    assert_eq!(report.failed_replay_cursors.len(), 1);
    assert!(!report.data_complete());
}

#[test]
fn replay_cursor_behind_canonical_raw_log_head_fails() {
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        canonical_raw_log_head_block_number: Some(900),
        raw_log_head_block_number: Some(900),
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    read.replay_cursors = vec![replay_cursor(800, None)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_caught_up(), CheckStatus::Fail);
    assert_eq!(report.lagging_replay_cursors[0].behind_by, 100);
}

/// Replay bounds require the raw log's lineage block to be canonical. The gate must compare
/// against the canonical raw-log head, not the non-orphaned head, so trailing `observed`
/// logs that replay cannot yet consume do not read as permanent lag.
#[test]
fn replay_cursor_at_canonical_head_passes_despite_trailing_observed_logs() {
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        canonical_raw_log_head_block_number: Some(1_000),
        raw_log_head_block_number: Some(1_100),
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    read.replay_cursors = vec![replay_cursor(1_000, None)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_caught_up(), CheckStatus::Pass);
    assert!(report.lagging_replay_cursors.is_empty());
}

/// A truncated restore can retain raw logs while dropping the replay cursor row. A missing
/// cursor produces no lag entry, so the gate must check the cursor exists.
#[test]
fn chain_with_raw_logs_but_no_replay_cursor_fails() {
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        canonical_raw_log_head_block_number: Some(1_000),
        raw_log_head_block_number: Some(1_000),
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    read.replay_cursors = vec![];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_caught_up(), CheckStatus::Fail);
    assert_eq!(
        report.chains_missing_raw_fact_cursor,
        vec![CHAIN.to_owned()]
    );
    assert!(!report.data_complete());
}

/// A chain with no retained canonical raw logs has nothing to normalize, so a missing cursor
/// there is not a gap.
#[test]
fn chain_without_canonical_raw_logs_does_not_require_a_cursor() {
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        canonical_raw_log_head_block_number: None,
        raw_log_head_block_number: None,
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    read.replay_cursors = vec![];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_caught_up(), CheckStatus::Pass);
    assert!(report.chains_missing_raw_fact_cursor.is_empty());
}

#[test]
fn projection_apply_cursor_behind_max_change_fails() {
    let mut read = healthy_read();
    read.projection_apply_cursors = vec![apply_cursor(40)];
    read.max_projection_change_id = Some(42);
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.projection_drained(), CheckStatus::Fail);
    assert_eq!(report.lagging_projection_cursors[0].behind_by, 2);
    assert!(!report.data_complete());
}

/// A non-empty change log with no apply cursor row means nothing has consumed it; the old
/// CROSS JOIN returned no rows and passed vacuously.
#[test]
fn non_empty_change_log_without_apply_cursor_fails() {
    let mut read = healthy_read();
    read.projection_apply_cursors = vec![];
    read.max_projection_change_id = Some(42);
    let report = evaluate(&read, &registry_only());

    assert!(report.projection_apply_cursor_missing);
    assert_eq!(report.projection_drained(), CheckStatus::Fail);
    assert!(!report.data_complete());
}

/// Cursor equal to the derive-scan frontier only means the scan finished. Pending
/// invalidations are unapplied projection work.
#[test]
fn pending_projection_invalidation_fails() {
    let mut read = healthy_read();
    read.pending_projection_invalidation_count = 1;
    let report = evaluate(&read, &registry_only());

    // The derive scan finished (cursor == max change id), but invalidations remain unapplied.
    assert_eq!(report.projection_drained(), CheckStatus::Pass);
    assert_eq!(report.projection_invalidations_drained(), CheckStatus::Fail);
    assert!(!report.data_complete());
}

#[test]
fn projection_invalidation_dead_letter_fails() {
    let mut read = healthy_read();
    read.projection_invalidation_dead_letter_count = 1;
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.projection_no_dead_letters(), CheckStatus::Fail);
    assert!(!report.data_complete());
}

/// An empty database drains every queue trivially; only the content check catches it.
#[test]
fn empty_projections_fail_even_when_every_cursor_is_drained() {
    let mut read = healthy_read();
    read.normalized_event_counts = vec![];
    read.name_current_counts = vec![];
    read.projection_apply_cursors = vec![apply_cursor(0)];
    read.max_projection_change_id = None;
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.projection_drained(), CheckStatus::Pass);
    assert_eq!(report.active_dataset_non_empty(), CheckStatus::Fail);
    assert_eq!(
        report.active_chain_namespaces_without_events[0].chain,
        CHAIN
    );
    assert!(!report.data_complete());
}

/// The wave-2 zero floor false-failed live databases, where reconcile commits canonical
/// lineage before advancing the checkpoint. A small negative lag is tolerated; a larger one
/// (a genuinely stale checkpoint writer) still fails.
#[test]
fn small_negative_head_lag_is_tolerated() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(996, 1_000, 1, 1_000)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.frontiers[0].head_lag_blocks, Some(-4));
    assert_eq!(report.frontier_at_head(), CheckStatus::Pass);
}

/// A stale checkpoint writer or mixed restore leaves the checkpoint far behind the lineage
/// head, giving a negative lag beyond tolerance.
#[test]
fn large_negative_head_lag_fails_frontier() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(900, 1_000, 1, 1_000)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.frontiers[0].head_lag_blocks, Some(-100));
    assert_eq!(report.frontier_at_head(), CheckStatus::Fail);
    assert!(!report.data_complete());
}

/// A chain the active watch set declares but that is absent from checkpoints and lineage
/// produces no storage row, so every per-chain check would pass by absence.
#[test]
fn manifest_declared_chain_without_storage_fails_frontier() {
    let mut read = healthy_read();
    // Storage has a foreign chain; the watched registry chain has no row at all.
    read.chains = vec![ChainCompletenessRow {
        chain_id: "base-mainnet".to_owned(),
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    let report = evaluate(&read, &registry_only());

    let synthesized = report
        .frontiers
        .iter()
        .find(|frontier| frontier.chain_id == CHAIN)
        .expect("synthesized frontier for the declared chain");
    assert!(synthesized.missing_from_storage);
    assert_eq!(synthesized.head_lag_blocks, None);
    assert_eq!(report.frontier_at_head(), CheckStatus::Fail);
    assert!(!report.data_complete());
}

/// Two non-orphaned canonical hashes at one height is a canonicality violation the distinct
/// block-number contiguity count cannot see.
#[test]
fn duplicate_canonical_height_fails_contiguity() {
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        duplicate_canonical_height_count: 1,
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.lineage_contiguous(), CheckStatus::Fail);
    assert_eq!(report.frontiers[0].duplicate_canonical_height_count, 1);
    assert!(!report.data_complete());
}

/// A live-tail-only database is internally consistent — contiguous span, caught-up cursors,
/// non-empty projections — but its lineage floor sits above the earliest declared start, so
/// history is truncated.
#[test]
fn lineage_floor_above_declared_start_fails_history() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_000, 1_000, 900, 101)];
    read.replay_cursors = vec![replay_cursor(1_000, None)];
    // Registry declares a start at block 500, below the retained lineage floor of 900.
    let mut early = watched(REGISTRY, WatchedContractSource::ManifestContract);
    early.active_from_block_number = Some(500);
    let report = evaluate(&read, &[early]);

    assert_eq!(report.history_from_declared_start(), CheckStatus::Fail);
    assert_eq!(report.chains_history_truncated.len(), 1);
    assert_eq!(report.chains_history_truncated[0].declared_start_block, 500);
    assert_eq!(
        report.chains_history_truncated[0].lineage_floor_block,
        Some(900)
    );
    // The gate would otherwise pass: the truncated span is itself contiguous.
    assert_eq!(report.lineage_contiguous(), CheckStatus::Pass);
    assert!(!report.data_complete());
}

/// A chain whose active targets are all open-ended has no finite start to establish a floor,
/// so the history check fails closed rather than passing vacuously.
#[test]
fn chain_with_only_open_ended_starts_fails_history() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_000, 1_000, 900, 101)];
    read.replay_cursors = vec![replay_cursor(1_000, None)];
    let mut open_ended = watched(REGISTRY, WatchedContractSource::ManifestContract);
    open_ended.active_from_block_number = None;
    let report = evaluate(&read, &[open_ended]);

    assert_eq!(report.history_from_declared_start(), CheckStatus::Fail);
    assert_eq!(report.chains_without_finite_start[0].chain, CHAIN);
    assert_eq!(
        report.chains_without_finite_start[0].open_ended_target_count,
        1
    );
    assert!(!report.data_complete());
}

/// A chain with at least one finite start still uses that as the floor, ignoring open-ended
/// siblings.
#[test]
fn mixed_starts_use_the_finite_floor() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_000, 1_000, 1, 1_000)];
    let finite = watched(REGISTRY, WatchedContractSource::ManifestContract);
    let mut open_ended = watched(RESOLVER, WatchedContractSource::DiscoveryEdge);
    open_ended.active_from_block_number = None;
    read.observed_code_addresses = vec![observed(REGISTRY), observed(RESOLVER)];
    let report = evaluate(&read, &[finite, open_ended]);

    assert_eq!(report.history_from_declared_start(), CheckStatus::Pass);
    assert!(report.chains_without_finite_start.is_empty());
}

/// Rows from another chain satisfy a global count while a newly active chain has zero. The
/// content check must be scoped to the active dataset.
#[test]
fn foreign_chain_content_does_not_satisfy_an_empty_active_chain() {
    let mut read = healthy_read();
    // All normalized events and names belong to a chain the active watch set does not cover.
    read.normalized_event_counts = vec![events("base-mainnet", NAMESPACE, 500)];
    read.name_current_counts = vec![names(NAMESPACE, 20)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.active_dataset_non_empty(), CheckStatus::Fail);
    assert_eq!(
        report.active_chain_namespaces_without_events[0].chain,
        CHAIN
    );
    assert_eq!(
        report.active_chain_namespaces_without_events[0].namespace,
        NAMESPACE
    );
    assert!(!report.data_complete());
}

/// An active chain with events in a namespace that has no name_current rows fails: names did
/// not materialize for that namespace.
#[test]
fn active_namespace_without_names_fails_content() {
    let mut read = healthy_read();
    read.name_current_counts = vec![];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.active_dataset_non_empty(), CheckStatus::Fail);
    assert_eq!(
        report.active_namespaces_without_names,
        vec![NAMESPACE.to_owned()]
    );
    assert!(!report.data_complete());
}

fn latched_chain_read() -> DataCompletenessRead {
    // Raw-fact replay is latched at block 1000, well below the live head at 2000; the backlog
    // cursor and live sync carry the rest. The raw-log head is 2000, so a head comparison
    // would mark this chain permanently behind.
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        canonical_raw_log_head_block_number: Some(2_000),
        raw_log_head_block_number: Some(2_000),
        ..chain_row(2_000, 2_000, 1, 2_000)
    }];
    read
}

/// On a chain with closure-replay adapters the raw-fact cursor's target is latched below the
/// head. Caught-up means both the raw-fact cursor and the backlog cursor reached their
/// targets, not the raw-log head.
#[test]
fn latched_chain_at_targets_is_caught_up() {
    let mut read = latched_chain_read();
    read.replay_cursors = vec![replay_cursor(1_000, None), backlog_cursor(2_001, 2_000)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_caught_up(), CheckStatus::Pass);
    assert!(report.lagging_replay_cursors.is_empty());
    assert!(report.data_complete());
}

#[test]
fn latched_chain_with_backlog_short_of_target_fails() {
    let mut read = latched_chain_read();
    read.replay_cursors = vec![replay_cursor(1_000, None), backlog_cursor(1_900, 2_000)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_caught_up(), CheckStatus::Fail);
    assert_eq!(report.lagging_replay_cursors[0].behind_by, 100);
    assert!(!report.data_complete());
}

/// A reorg rewind lowers `next_block_number` below the target while `last_completed` stays at
/// its high-water mark. The gate must read `next`/`target`, not `last_completed`.
#[test]
fn rewound_cursor_below_target_fails_even_with_high_last_completed() {
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        canonical_raw_log_head_block_number: Some(1_000),
        raw_log_head_block_number: Some(1_000),
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    read.replay_cursors = vec![rewound_cursor(500, 1_000, 1_000)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.normalization_caught_up(), CheckStatus::Fail);
    assert_eq!(report.lagging_replay_cursors[0].behind_by, 500);
    assert!(!report.data_complete());
}

/// A candidate mid projection-bootstrap has published name_current (first in order) but not
/// the other projections, so not all markers are present at the newest replay version.
#[test]
fn incomplete_projection_replay_markers_fail() {
    let mut read = healthy_read();
    read.projection_replay_markers = vec![ProjectionReplayMarker {
        replay_version: 6,
        projection: "name_current".to_owned(),
    }];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.projection_replay_complete(), CheckStatus::Fail);
    assert!(
        report
            .missing_projection_replay_markers
            .contains(&"children_current".to_owned())
    );
    assert!(!report.data_complete());
}

/// No replay markers at all means projections were never rebuilt.
#[test]
fn no_projection_replay_markers_fail() {
    let mut read = healthy_read();
    read.projection_replay_markers = vec![];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.projection_replay_version, None);
    assert_eq!(report.projection_replay_complete(), CheckStatus::Fail);
    assert!(!report.data_complete());
}

/// Markers are judged at the newest replay version present, so a candidate built by an older
/// image with all projections at its version passes.
#[test]
fn complete_markers_at_older_version_pass() {
    let mut read = healthy_read();
    read.projection_replay_markers = all_projection_markers(5);
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.projection_replay_version, Some(5));
    assert_eq!(report.projection_replay_complete(), CheckStatus::Pass);
    assert!(report.data_complete());
}

/// A NULL `chain_id` normalized event is a data-integrity fault.
#[test]
fn null_chain_id_normalized_events_fail() {
    let mut read = healthy_read();
    read.normalized_events_null_chain_id_count = 3;
    let report = evaluate(&read, &registry_only());

    assert_eq!(
        report.normalized_events_chain_id_present(),
        CheckStatus::Fail
    );
    assert!(!report.data_complete());
}

/// A fresh replay drops the deferred projection indexes; an absent one marks a mid-replay
/// candidate not yet ready to serve.
#[test]
fn missing_deferred_projection_index_fails() {
    let mut read = healthy_read();
    read.present_deferred_projection_indexes = all_deferred_indexes()
        .into_iter()
        .filter(|name| name != "normalized_events_namespace_idx")
        .collect();
    let report = evaluate(&read, &registry_only());

    assert_eq!(
        report.deferred_projection_indexes_present(),
        CheckStatus::Fail
    );
    assert!(
        report
            .missing_deferred_projection_indexes
            .contains(&"normalized_events_namespace_idx".to_owned())
    );
    assert!(!report.data_complete());
}

/// A chain declared to produce two namespaces fails when one has no events, even though the
/// other does — the expectation comes from declared manifests, not observed events.
#[test]
fn declared_namespace_with_no_events_fails_content() {
    let mut read = healthy_read();
    read.manifest_chain_namespaces =
        vec![manifest_ns(CHAIN, "ens"), manifest_ns(CHAIN, "basenames")];
    // Only ens has events and names; basenames declared but empty.
    read.normalized_event_counts = vec![events(CHAIN, "ens", 100)];
    read.name_current_counts = vec![names("ens", 10)];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.active_dataset_non_empty(), CheckStatus::Fail);
    assert!(
        report
            .active_chain_namespaces_without_events
            .iter()
            .any(|entry| entry.namespace == "basenames")
    );
    assert!(!report.data_complete());
}

/// A chain declared only by an active manifest version (no watched-contract rows, e.g. a
/// partial restore losing contract_instance_addresses) still gets a gating frontier.
#[test]
fn manifest_only_chain_gets_a_frontier() {
    let mut read = healthy_read();
    read.chains = vec![];
    read.manifest_chain_namespaces = vec![manifest_ns(CHAIN, NAMESPACE)];
    let report = evaluate(&read, &registry_only());

    let frontier = report
        .frontiers
        .iter()
        .find(|frontier| frontier.chain_id == CHAIN)
        .expect("frontier for the manifest-declared chain");
    assert!(frontier.missing_from_storage);
    assert_eq!(report.frontier_at_head(), CheckStatus::Fail);
}

/// A foreign or retired chain with residual storage rows is an advisory, not a gate failure.
#[test]
fn foreign_chain_is_advisory_not_gating() {
    let mut read = healthy_read();
    read.chains = vec![
        chain_row(1_000, 1_000, 1, 1_000),
        ChainCompletenessRow {
            chain_id: "retired-chain".to_owned(),
            ..chain_row(1_000, 1_000, 1, 1_000)
        },
    ];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.foreign_chains, vec!["retired-chain".to_owned()]);
    assert!(
        report
            .frontiers
            .iter()
            .all(|frontier| frontier.chain_id != "retired-chain")
    );
    assert!(report.data_complete());
}

/// Backfill failures are surfaced as an advisory with counts, not gated.
#[test]
fn backfill_failures_are_advisory() {
    let mut read = healthy_read();
    read.backfill_lifecycle = vec![BackfillLifecycleRow {
        deployment_profile: "sepolia".to_owned(),
        failed_job_count: 22,
        failed_range_count: 22,
        incomplete_range_count: 274,
        expired_lease_range_count: 1,
    }];
    let report = evaluate(&read, &registry_only());

    assert_eq!(report.backfill_advisory[0].failed_job_count, 22);
    // Advisory only: backfill failures do not fail the gate.
    assert!(report.data_complete());
}
