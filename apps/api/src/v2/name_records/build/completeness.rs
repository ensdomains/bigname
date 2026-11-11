//! `meta.completeness` on an indexed records read: whether an inventory an admitted resolver
//! backs stands behind the answer (docs/api-v1-routes.md, `GET /v1/names/{name}/records`). The
//! per-key answers keep their own status and reason.
use bigname_storage::{
    AbiContentTypesUnavailable, NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot,
};

use super::{
    authority_unsupported_reason, product_record_reason, row_has_current_registration,
    serving_record_inventory,
};
use crate::v2::{Completeness, Meta, Source, V2Result, snapshot_meta};

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
    meta: &mut Meta,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> V2Result<()> {
    if meta.source != Some(Source::Indexed) {
        return Ok(());
    }
    if let Some(reason) = name_level_reason(row)? {
        mark_unsupported(meta, reason);
        return Ok(());
    }
    // The checks of `build_indexed_name_records`: such a name answers no key from an inventory,
    // whichever row it reached.
    let serves_records = row_has_current_registration(row)
        && row.unresolvable_reason().is_none()
        && !row.released_ens_v2_registration();
    apply_inventory_completeness(meta, record_inventory.filter(|_| serves_records));
    Ok(())
}

/// The reason every key of the name carries before any inventory is consulted.
fn name_level_reason(row: &NameCurrentRow) -> V2Result<Option<String>> {
    if let Some(reason) = authority_unsupported_reason(row)? {
        return Ok(Some(reason));
    }
    row.resolution_unsupported_reason()
        .map(product_record_reason)
        .transpose()
}

/// Reports the inventory an indexed read answered from, the row of a resolver or `None`.
pub(crate) fn apply_inventory_completeness(
    meta: &mut Meta,
    record_inventory: Option<&RecordInventoryCurrentRow>,
) {
    let reason = match record_inventory {
        None => AbiContentTypesUnavailable::InventoryNotAvailable,
        Some(_) if serving_record_inventory(record_inventory).is_some() => {
            meta.completeness = Some(Completeness::Full);
            return;
        }
        Some(_) => AbiContentTypesUnavailable::InventoryNotAuthoritative,
    };
    mark_unsupported(meta, reason.as_str().to_owned());
}

fn mark_unsupported(meta: &mut Meta, reason: String) {
    meta.completeness = Some(Completeness::Unsupported);
    meta.unsupported_fields = Some(vec!["records".to_owned()]);
    meta.unsupported_reason = Some(reason);
}

#[cfg(test)]
mod tests;
