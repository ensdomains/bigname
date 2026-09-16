use bigname_storage::{NameCurrentRow, SelectedSnapshot};

use crate::AppState;
use crate::v2::collection_snapshot::CollectionSnapshot;
use crate::v2::{Status, V2Error, V2Result, name_chain_id, snapshot_meta};

use super::NameRecord;

pub(super) async fn apply(
    state: &AppState,
    row: &NameCurrentRow,
    selected: &SelectedSnapshot,
    snapshot: Option<&CollectionSnapshot>,
    record: &mut NameRecord,
) -> V2Result<()> {
    let Some(snapshot) = snapshot.filter(|_| record.status != Status::Unsupported) else {
        return Ok(());
    };
    #[cfg(test)]
    crate::v2::search_public_namespace_read_test_hooks::run(&state.pool).await?;
    let counts = load_name_counts(&state.pool, row).await?;
    let current = snapshot.finish(state).await?;
    let selected_meta = snapshot_meta(selected)?;
    let chain_id = name_chain_id(row)
        .and_then(|chain| crate::v2::slug_to_numeric(&chain))
        .ok_or_else(counts_unavailable)?;
    let chain = chain_id.to_string();
    let current_position = current
        .as_of
        .as_ref()
        .and_then(|positions| positions.get(&chain));
    let selected_position = selected_meta
        .as_of
        .as_ref()
        .and_then(|positions| positions.get(&chain));
    if current_position.is_none() || current_position != selected_position {
        return Err(counts_unavailable());
    }
    record.subname_count = Some(counts.subname_count);
    record.record_count = counts.record_count;
    Ok(())
}

fn counts_unavailable() -> V2Error {
    V2Error::stale(
        "name counts are only available at the current publication; retry without historical selectors",
    )
}

pub(super) struct NameCounts {
    pub(super) subname_count: u64,
    pub(super) record_count: Option<u64>,
}

/// The direct readable subname count and, when the row has current record inventory, its known
/// record-selector count; both are the same bounded reads the subnames and address-name routes run.
pub(super) async fn load_name_counts(
    pool: &sqlx::PgPool,
    row: &NameCurrentRow,
) -> V2Result<NameCounts> {
    let subname_count = bigname_storage::load_children_current_summaries(
        pool,
        std::slice::from_ref(&row.logical_name_id),
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load subname counts"))?
    .into_iter()
    .next()
    .and_then(|summary| u64::try_from(summary.child_count).ok())
    .unwrap_or_default();
    let record_count = match bigname_storage::resolution_record_inventory_lookup_key_any_chain(row)
    {
        Some(key) => bigname_storage::count_record_inventory_selectors_by_lookup_keys(pool, &[key])
            .await
            .map_err(|_| V2Error::internal_error("failed to load record counts"))?
            .into_iter()
            .next()
            .flatten(),
        None => None,
    };
    Ok(NameCounts {
        subname_count,
        record_count,
    })
}

pub(super) fn include_counts(include: &[String]) -> V2Result<bool> {
    let mut include_counts = false;
    for value in include {
        match value.as_str() {
            "counts" => include_counts = true,
            _ => return Err(V2Error::invalid_input("include must contain only counts")),
        }
    }
    Ok(include_counts)
}
