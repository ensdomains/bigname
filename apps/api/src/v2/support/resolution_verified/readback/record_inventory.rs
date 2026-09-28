use super::*;

pub(super) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {

bigname_storage::families::records::load_family_supported_record_inventory_for_snapshot(pool, row, &selected_snapshot.chain_positions).await
}

pub(super) async fn load_indexed_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {

bigname_storage::families::records::load_family_record_inventory_for_snapshot(pool, row, &selected_snapshot.chain_positions).await
}



pub(super) async fn load_record_inventory_current_matching_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
    _allow_selected_superset: bool,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {

bigname_storage::families::records::load_family_supported_record_inventory_for_snapshot(pool, row, &selected_snapshot.chain_positions).await
}
