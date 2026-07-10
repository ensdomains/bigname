use super::evaluate::{CheckStatus, DEFAULT_MAX_HEAD_LAG_BLOCKS, evaluate_data_completeness};
use bigname_manifests::{WatchedContract, WatchedContractSource};
use bigname_storage::{
    ChainCompletenessRow, DataCompletenessRead, ObservedCodeAddress, ProjectionApplyCursorRow,
    ReplayCursorRow,
};
use uuid::Uuid;

const CHAIN: &str = "ethereum-sepolia";
const REGISTRY: &str = "0x796fff2e907449be8d5921bcc215b1b76d89d080";
const RESOLVER: &str = "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5";

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
        raw_log_head_block_number: Some(floor),
    }
}

fn replay_cursor(last_completed: i64, failure: Option<&str>) -> ReplayCursorRow {
    ReplayCursorRow {
        deployment_profile: "sepolia".to_owned(),
        chain_id: CHAIN.to_owned(),
        cursor_kind: "raw_fact_normalized_events".to_owned(),
        last_completed_block_number: Some(last_completed),
        target_block_number: Some(last_completed),
        last_failure_reason: failure.map(str::to_owned),
    }
}

fn healthy_read() -> DataCompletenessRead {
    DataCompletenessRead {
        chains: vec![chain_row(1_000, 1_000, 1, 1_000)],
        replay_cursors: vec![replay_cursor(1, None)],
        projection_apply_cursors: vec![ProjectionApplyCursorRow {
            cursor_name: "normalized_events_to_projection_invalidations".to_owned(),
            last_change_id: 42,
            max_change_id: Some(42),
        }],
        observed_code_addresses: vec![observed(REGISTRY)],
        normalized_event_count: 100,
        name_current_count: 10,
    }
}

fn evaluate(
    read: &DataCompletenessRead,
    watched_contracts: &[WatchedContract],
) -> super::evaluate::DataCompletenessReport {
    evaluate_data_completeness(read, watched_contracts, DEFAULT_MAX_HEAD_LAG_BLOCKS)
}

#[test]
fn healthy_database_is_data_complete() {
    let watched_contracts = vec![watched(REGISTRY, WatchedContractSource::ManifestContract)];
    assert!(evaluate(&healthy_read(), &watched_contracts).data_complete());
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
    assert_eq!(report.content_present(), CheckStatus::Pass);
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

#[test]
fn reconciliation_frontier_behind_head_beyond_tolerance_fails() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_000, 900, 1, 900)];
    let report = evaluate(
        &read,
        &[watched(REGISTRY, WatchedContractSource::ManifestContract)],
    );

    assert_eq!(report.frontier_at_head(), CheckStatus::Fail);
    assert_eq!(report.frontiers[0].head_lag_blocks, Some(100));
    assert!(!report.data_complete());
}

#[test]
fn reconciliation_frontier_within_tolerance_passes() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_004, 1_000, 1, 1_000)];
    let report = evaluate(
        &read,
        &[watched(REGISTRY, WatchedContractSource::ManifestContract)],
    );

    assert_eq!(report.frontier_at_head(), CheckStatus::Pass);
}

#[test]
fn lineage_gap_fails_contiguity() {
    let mut read = healthy_read();
    read.chains = vec![chain_row(1_000, 1_000, 1, 999)];
    let report = evaluate(
        &read,
        &[watched(REGISTRY, WatchedContractSource::ManifestContract)],
    );

    assert_eq!(report.lineage_contiguous(), CheckStatus::Fail);
    assert_eq!(report.frontiers[0].missing_block_count, 1);
    assert!(!report.data_complete());
}

/// The LabelReserved crash-loop: the cursor stops advancing and records why.
#[test]
fn replay_cursor_failure_reason_fails_normalization() {
    let mut read = healthy_read();
    read.replay_cursors = vec![replay_cursor(1, Some("LabelReserved expiry exceeds i64"))];
    let report = evaluate(
        &read,
        &[watched(REGISTRY, WatchedContractSource::ManifestContract)],
    );

    assert_eq!(report.normalization_healthy(), CheckStatus::Fail);
    assert_eq!(report.failed_replay_cursors.len(), 1);
    assert!(!report.data_complete());
}

#[test]
fn replay_cursor_behind_raw_log_head_fails() {
    let mut read = healthy_read();
    read.chains = vec![ChainCompletenessRow {
        raw_log_head_block_number: Some(900),
        ..chain_row(1_000, 1_000, 1, 1_000)
    }];
    read.replay_cursors = vec![replay_cursor(800, None)];
    let report = evaluate(
        &read,
        &[watched(REGISTRY, WatchedContractSource::ManifestContract)],
    );

    assert_eq!(report.normalization_caught_up(), CheckStatus::Fail);
    assert_eq!(report.lagging_replay_cursors[0].behind_by, 100);
}

#[test]
fn projection_apply_cursor_behind_max_change_fails() {
    let mut read = healthy_read();
    read.projection_apply_cursors = vec![ProjectionApplyCursorRow {
        cursor_name: "normalized_events_to_projection_invalidations".to_owned(),
        last_change_id: 40,
        max_change_id: Some(42),
    }];
    let report = evaluate(
        &read,
        &[watched(REGISTRY, WatchedContractSource::ManifestContract)],
    );

    assert_eq!(report.projection_drained(), CheckStatus::Fail);
    assert_eq!(report.lagging_projection_cursors[0].behind_by, 2);
}

/// An empty database drains every queue trivially; only the content check catches it.
#[test]
fn empty_projections_fail_even_when_every_cursor_is_drained() {
    let mut read = healthy_read();
    read.normalized_event_count = 0;
    read.name_current_count = 0;
    read.projection_apply_cursors = vec![ProjectionApplyCursorRow {
        cursor_name: "normalized_events_to_projection_invalidations".to_owned(),
        last_change_id: 0,
        max_change_id: None,
    }];
    let report = evaluate(
        &read,
        &[watched(REGISTRY, WatchedContractSource::ManifestContract)],
    );

    assert_eq!(report.projection_drained(), CheckStatus::Pass);
    assert_eq!(report.content_present(), CheckStatus::Fail);
    assert!(!report.data_complete());
}
