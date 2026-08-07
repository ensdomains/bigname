use super::*;

mod readback {
    use super::*;

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/v2/support/resolution_verified/readback.rs"
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
