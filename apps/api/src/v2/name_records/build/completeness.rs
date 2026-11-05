//! `meta.completeness` on an indexed records read: whether an inventory an admitted resolver
//! backs stands behind the answer (docs/api-v1-routes.md, `GET /v1/names/{name}/records`). The
//! per-key answers keep their own status and reason.
use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot};

use crate::v2::{Meta, Source, V2Result, snapshot_meta};

/// The envelope metadata of `GET /v1/names/{name}/records`.
pub(crate) fn records_meta(
    selected: &SelectedSnapshot,
    source: Source,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> V2Result<Meta> {
    let mut meta = snapshot_meta(selected)?;
    meta.source = Some(source);
    apply_name_completeness(&mut meta, row, record_inventory)?;
    Ok(meta)
}

fn apply_name_completeness(
    _meta: &mut Meta,
    _row: &NameCurrentRow,
    _record_inventory: Option<&RecordInventoryCurrentRow>,
) -> V2Result<()> {
    Ok(())
}

pub(crate) fn apply_inventory_completeness(
    _meta: &mut Meta,
    _record_inventory: Option<&RecordInventoryCurrentRow>,
) {
}

#[cfg(test)]
mod tests;
