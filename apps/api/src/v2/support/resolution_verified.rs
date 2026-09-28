use super::*;

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

/// The record inventory at the mainnet-profile lookup key: name detail's verified source and the
/// records diagnostic. It is the family inventory at the family publication.
pub(crate) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    bigname_storage::families::records::load_family_supported_record_inventory_for_snapshot(
        pool,
        row,
        &selected_snapshot.chain_positions,
    )
    .await
}

/// The record inventory at the any-chain lookup key: name detail's indexed source and the records
/// route. It is the family inventory at the family publication.
pub(crate) async fn load_indexed_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    bigname_storage::families::records::load_family_record_inventory_for_snapshot(
        pool,
        row,
        &selected_snapshot.chain_positions,
    )
    .await
}

/// The record inventory `GET /v1/names/{name}/records` reads for every source: the inventory at
/// the selected snapshot on whichever chain the deployment indexes, with the binding,
/// serving-resource, chain-position, version-boundary, and snapshot checks of the any-chain
/// readback. It feeds the default key set, indexed answers, and `include=inventory`. It does not
/// admit verified execution, which the lookup engine checks separately; name detail and
/// diagnostics keep their own loaders.
///
/// The inventory is the family inventory of the resource the name
/// serves records through, at the family publication
/// (`bigname_storage::families::records::load_family_record_inventory_for_snapshot`).
pub(crate) async fn load_records_route_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    load_indexed_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await
}
