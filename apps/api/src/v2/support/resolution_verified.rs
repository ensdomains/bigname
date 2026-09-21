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

pub(crate) async fn load_indexed_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    readback::load_indexed_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await
}

/// The record inventory `GET /v1/names/{name}/records` reads for every source: the served row at
/// the selected snapshot on whichever chain the deployment indexes, with the binding,
/// serving-resource, chain-position, version-boundary, and snapshot checks of the any-chain
/// readback. It feeds the default key set, indexed answers, and `include=inventory`. It does not
/// admit verified execution, which the lookup engine checks separately; name detail and
/// diagnostics keep their own loaders.
pub(crate) async fn load_records_route_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    load_indexed_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await
}
