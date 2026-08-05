use super::*;

const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";

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

impl bigname_storage::VerifiedResolutionRecord for ResolutionRecordKey {
    fn record_key(&self) -> &str {
        &self.record_key
    }

    fn record_family(&self) -> &str {
        &self.record_family
    }

    fn selector_key(&self) -> Option<&str> {
        self.selector_key.as_deref()
    }
}

pub(crate) fn build_resolution_execution_explain_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    response::build_resolution_execution_explain_verified_state(row, records, trace, outcome)
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

pub(crate) fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<bigname_storage::VerifiedResolutionSupportBoundary> {
    readback::resolution_verified_support_boundary(row, record_inventory_row)
}
