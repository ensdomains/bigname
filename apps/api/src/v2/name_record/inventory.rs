use bigname_storage::{
    NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot, SnapshotSelectionError,
};
use sqlx::PgPool;

use crate::v2::support::load_records_route_inventory;

/// The record inventory name detail reads for both sources: the records route's chain-neutral
/// inventory, so indexed detail, verified detail and `GET /v1/names/{name}/records` derive their
/// record keys from one row on whichever chain the deployment indexes. It is an inventory
/// readback, not an execution admission; verified execution keeps its own admission in the
/// lookup engine.
pub(super) async fn load_name_record_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    // A family inventory describes the family publication, which is the only position a composed
    // name row serves, so there is no older served row to widen the match to.
    load_records_route_inventory(pool, row, selected_snapshot).await
}
