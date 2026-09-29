//! The page's cursor and sorted-row helpers.
use anyhow::{Result, bail};
use sqlx::postgres::PgRow;

use crate::UnixSeconds;

use super::super::{
    decode::decode_address_name_current_entry,
    types::{
        AddressNameCurrentEntry, AddressNamesCurrentCursor, AddressNamesCurrentSort,
        AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    },
};

pub(super) struct AddressNameCurrentSortedEntry {
    pub(super) entry: AddressNameCurrentEntry,
    pub(super) sort_timestamp: Option<UnixSeconds>,
}

pub(super) fn decode_address_name_current_sorted_entry(
    row: PgRow,
    sort: AddressNamesCurrentSort,
) -> Result<AddressNameCurrentSortedEntry> {
    let sort_timestamp = sort
        .is_timestamp()
        .then(|| crate::sql_row::get::<Option<UnixSeconds>>(&row, "sort_timestamp"))
        .transpose()?
        .flatten();
    let entry = decode_address_name_current_entry(row)?;

    Ok(AddressNameCurrentSortedEntry {
        entry,
        sort_timestamp,
    })
}

pub(super) fn address_names_current_sorted_cursor_from_entry(
    row: &AddressNameCurrentSortedEntry,
    sort: AddressNamesCurrentSort,
) -> AddressNamesCurrentSortedCursor {
    AddressNamesCurrentSortedCursor {
        sort_value: match sort {
            AddressNamesCurrentSort::Name => {
                AddressNamesCurrentSortedCursorValue::Name(row.entry.canonical_display_name.clone())
            }
            AddressNamesCurrentSort::ExpiresAt
            | AddressNamesCurrentSort::RegisteredAt
            | AddressNamesCurrentSort::CreatedAt => {
                AddressNamesCurrentSortedCursorValue::Timestamp(row.sort_timestamp)
            }
        },
        logical_name_id: row.entry.logical_name_id.clone(),
        resource_id: row.entry.resource_id,
    }
}

pub(super) fn address_names_current_sorted_cursor_from_legacy(
    cursor: &AddressNamesCurrentCursor,
) -> AddressNamesCurrentSortedCursor {
    AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name(
            cursor.canonical_display_name.clone(),
        ),
        logical_name_id: cursor.logical_name_id.clone(),
        resource_id: cursor.resource_id,
    }
}

pub(super) fn address_names_current_legacy_cursor_from_sorted(
    cursor: AddressNamesCurrentSortedCursor,
) -> Result<AddressNamesCurrentCursor> {
    let AddressNamesCurrentSortedCursorValue::Name(canonical_display_name) = cursor.sort_value
    else {
        bail!("address_names_current sorted cursor cannot be converted to legacy name cursor");
    };

    Ok(AddressNamesCurrentCursor {
        canonical_display_name,
        logical_name_id: cursor.logical_name_id,
        resource_id: cursor.resource_id,
    })
}

pub(super) fn ensure_address_names_current_cursor_matches_sort(
    sort: AddressNamesCurrentSort,
    cursor: &AddressNamesCurrentSortedCursor,
) -> Result<()> {
    match (sort, &cursor.sort_value) {
        (AddressNamesCurrentSort::Name, AddressNamesCurrentSortedCursorValue::Name(_))
        | (
            AddressNamesCurrentSort::ExpiresAt
            | AddressNamesCurrentSort::RegisteredAt
            | AddressNamesCurrentSort::CreatedAt,
            AddressNamesCurrentSortedCursorValue::Timestamp(_),
        ) => Ok(()),
        _ => bail!(
            "address_names_current page cursor sort value does not match sort {}",
            sort.as_str()
        ),
    }
}
