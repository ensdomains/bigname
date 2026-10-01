use bigname_storage::NameCurrentRow;

use crate::AppState;

use super::super::{V2Error, V2Result};

pub(super) async fn load_current_name_row(
    state: &AppState,
    logical_name_id: &str,
    normalized_name: &str,
) -> V2Result<Option<NameCurrentRow>> {
    bigname_storage::load_name_current(&state.pool, logical_name_id)
        .await
        .map_err(crate::v2::name_rows_error(
            crate::v2::SnapshotReadResource::Resource,
            |_| {
                V2Error::internal_error(format!(
                    "failed to resolve current resource for name {normalized_name}"
                ))
            },
        ))
}
