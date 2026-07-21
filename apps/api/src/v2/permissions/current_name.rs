use bigname_storage::NameCurrentRow;

use crate::AppState;

use super::super::{V2Error, V2Result};

pub(super) async fn load_current_name_row(
    state: &AppState,
    namespace: &str,
    normalized_name: &str,
) -> V2Result<Option<NameCurrentRow>> {
    let logical_name_id = format!("{namespace}:{normalized_name}");
    bigname_storage::load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|_| {
            V2Error::internal_error(format!(
                "failed to resolve current resource for name {namespace}/{normalized_name}"
            ))
        })
}
