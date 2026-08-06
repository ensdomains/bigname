#[path = "readback/record_inventory.rs"]
mod record_inventory;

pub(super) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    record_inventory::load_supported_record_inventory_current_for_snapshot(
        pool,
        row,
        selected_snapshot,
    )
    .await
}

pub(super) async fn load_record_inventory_current_matching_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    record_inventory::load_record_inventory_current_matching_selected_snapshot(
        pool,
        row,
        selected_snapshot,
        allow_selected_superset,
    )
    .await
}
