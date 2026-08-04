use super::*;

mod topology {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/resolution_verified/topology.rs"
    ));
}

mod execution_summary {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/resolution_verified/execution_summary.rs"
    ));
}

mod readback {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/resolution_verified/readback.rs"
    ));
}

mod response {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/resolution_verified/response.rs"
    ));
}

pub(crate) use readback::{PartialCompactHits, ResolutionVerifiedOutcomeLookup};

pub(crate) fn build_resolution_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    response::build_resolution_declared_state(row, record_inventory_row, records)
}

pub(crate) fn build_resolution_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    persisted_outcome: Option<&ExecutionOutcome>,
) -> Result<JsonValue> {
    response::build_resolution_verified_state(row, records, persisted_outcome)
}

pub(crate) fn build_resolution_execution_explain_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    response::build_resolution_execution_explain_verified_state(row, records, trace, outcome)
}

pub(crate) async fn lookup_resolution_verified_outcome(
    pool: &PgPool,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    partial_compact_hits: PartialCompactHits,
) -> std::result::Result<readback::ResolutionVerifiedOutcomeLookup, SnapshotSelectionError> {
    readback::lookup_resolution_verified_outcome(
        pool,
        row,
        records,
        record_inventory_row,
        selected_snapshot,
        partial_compact_hits,
    )
    .await
}

pub(crate) fn build_resolution_execution_cache_key(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    chain_positions: JsonValue,
) -> Result<ExecutionCacheKey> {
    readback::build_resolution_execution_cache_key(
        row,
        records,
        record_inventory_row,
        chain_positions,
    )
}

pub(crate) fn resolution_execution_cache_lookup_records(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    readback::resolution_execution_cache_lookup_records(row, records)
}

pub(crate) fn validate_loaded_resolution_verified_outcome(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    outcome: &ExecutionOutcome,
) -> std::result::Result<(), SnapshotSelectionError> {
    readback::validate_loaded_resolution_verified_outcome(row, records, outcome)
}

pub(crate) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    readback::load_supported_record_inventory_current_for_snapshot(pool, row, selected_snapshot)
        .await
}

pub(crate) async fn load_explicit_unsupported_record_inventory_current(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    readback::load_explicit_unsupported_record_inventory_current(pool, row).await
}

pub(crate) async fn load_record_inventory_current_matching_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    readback::load_record_inventory_current_matching_selected_snapshot(
        pool,
        row,
        selected_snapshot,
        allow_selected_superset,
    )
    .await
}

#[cfg(test)]
pub(crate) fn record_inventory_chain_positions_match_selected_snapshot(
    projected: &ChainPositions,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
) -> bool {
    readback::record_inventory_chain_positions_match_selected_snapshot(
        projected,
        selected_snapshot,
        allow_selected_superset,
    )
}

pub(crate) fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<bigname_storage::VerifiedResolutionSupportBoundary> {
    readback::resolution_verified_support_boundary(row, record_inventory_row)
}
