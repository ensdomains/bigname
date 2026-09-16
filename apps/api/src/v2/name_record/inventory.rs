use bigname_storage::{
    NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot, SnapshotSelectionError,
    SnapshotSelectionErrorKind,
};
use sqlx::PgPool;

use super::Source;
use crate::v2::support::{
    load_indexed_record_inventory_current_for_snapshot,
    load_record_inventory_current_matching_selected_snapshot,
    load_supported_record_inventory_current_for_snapshot,
};

pub(super) async fn load_name_record_inventory(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
    source: Source,
) -> Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let inventory = if source == Source::Indexed {
        load_indexed_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await
    } else {
        load_supported_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await
    };
    match inventory {
        Ok(Some(record_inventory)) => Ok(Some(record_inventory)),
        Ok(None) if allow_selected_superset => {
            load_record_inventory_current_matching_selected_snapshot(
                pool,
                row,
                selected_snapshot,
                true,
            )
            .await
        }
        Ok(None) => Ok(None),
        Err(error)
            if allow_selected_superset && error.kind() == SnapshotSelectionErrorKind::Stale =>
        {
            match load_record_inventory_current_matching_selected_snapshot(
                pool,
                row,
                selected_snapshot,
                true,
            )
            .await?
            {
                Some(record_inventory) => Ok(Some(record_inventory)),
                None => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}
