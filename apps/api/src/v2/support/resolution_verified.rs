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

/// The record inventory at the mainnet-profile lookup key: name detail's verified source and the
/// records diagnostic. Under the publication switch it is the family inventory at the family
/// publication (TYR-36 step 7b); the diagnostic keeps serving under the switch.
pub(crate) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    if bigname_storage::publication_source::serve_from_families() {
        return bigname_storage::families::records::load_family_supported_record_inventory_for_snapshot(
            pool,
            row,
            &selected_snapshot.chain_positions,
        )
        .await;
    }
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

/// The record inventory at the any-chain lookup key: name detail's indexed source and the records
/// route. Under the publication switch it is the family inventory at the family publication.
pub(crate) async fn load_indexed_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    if bigname_storage::publication_source::serve_from_families() {
        return bigname_storage::families::records::load_family_record_inventory_for_snapshot(
            pool,
            row,
            &selected_snapshot.chain_positions,
        )
        .await;
    }
    readback::load_indexed_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await
}

/// The record inventory `GET /v1/names/{name}/records` reads for every source: the served row at
/// the selected snapshot on whichever chain the deployment indexes, with the binding,
/// serving-resource, chain-position, version-boundary, and snapshot checks of the any-chain
/// readback. It feeds the default key set, indexed answers, and `include=inventory`. It does not
/// admit verified execution, which the lookup engine checks separately; name detail and
/// diagnostics keep their own loaders.
///
/// Under the publication switch the inventory is the family inventory of the resource the name
/// serves records through, at the family publication
/// (`bigname_storage::families::records::load_family_record_inventory_for_snapshot`); the
/// verified lookup engine keeps reading the served row until the flip (TYR-36 step 7b).
pub(crate) async fn load_records_route_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    load_indexed_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await
}
