use std::collections::BTreeMap;

use bigname_storage::NameCurrentRow;
use sqlx::{PgConnection, types::Uuid};

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

/// The current name of each resource, read on the chains of the request's publication: the keys
/// of `block_bounds`.
pub(super) async fn load_publication_names(
    reads: &mut PgConnection,
    resource_ids: &[Uuid],
    block_bounds: &BTreeMap<String, i64>,
) -> V2Result<BTreeMap<Uuid, NameCurrentRow>> {
    let chains = block_bounds.keys().cloned().collect::<Vec<_>>();
    bigname_storage::load_current_names_by_resource_ids(reads, resource_ids, &chains)
        .await
        .map_err(crate::v2::name_rows_error(
            crate::v2::SnapshotReadResource::Resource,
            |_| V2Error::internal_error("failed to load permission names"),
        ))
}
