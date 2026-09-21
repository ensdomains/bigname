//! Cursor encoding for the permissions collection: the storage cursor becomes the opaque page
//! cursor and back, bound to the sort key and the filters the page was built with.

use std::collections::BTreeMap;

use bigname_storage::PermissionsCurrentAccountResourceCursor;
use sqlx::types::Uuid;

use super::super::cursor::{cursor_value, invalid_cursor_error};
use super::super::{CursorPayload, V2Result};

const PERMISSIONS_SORT: &str = "address_registration_scope_asc";
const SUBJECT_CURSOR_KEY: &str = "subject";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
const SCOPE_CURSOR_KEY: &str = "scope";

pub(super) fn permissions_cursor_payload(
    cursor: &PermissionsCurrentAccountResourceCursor,
    filters: &BTreeMap<String, String>,
) -> CursorPayload {
    CursorPayload::new(
        PERMISSIONS_SORT,
        filters.clone(),
        BTreeMap::from([
            (SUBJECT_CURSOR_KEY.to_owned(), cursor.subject.clone()),
            (
                RESOURCE_ID_CURSOR_KEY.to_owned(),
                cursor.resource_id.to_string(),
            ),
            (SCOPE_CURSOR_KEY.to_owned(), cursor.scope.clone()),
        ]),
        None,
    )
}

pub(super) fn permissions_storage_cursor(
    payload: &CursorPayload,
    expected_filters: &BTreeMap<String, String>,
) -> V2Result<PermissionsCurrentAccountResourceCursor> {
    if payload.sort != PERMISSIONS_SORT {
        return Err(invalid_cursor_error());
    }
    if &payload.filters != expected_filters {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 3 {
        return Err(invalid_cursor_error());
    }

    let resource_id = cursor_value(payload, RESOURCE_ID_CURSOR_KEY, invalid_cursor_error)?
        .parse::<Uuid>()
        .map_err(|_| invalid_cursor_error())?;

    Ok(PermissionsCurrentAccountResourceCursor {
        subject: cursor_value(payload, SUBJECT_CURSOR_KEY, invalid_cursor_error)?,
        resource_id,
        scope: cursor_value(payload, SCOPE_CURSOR_KEY, invalid_cursor_error)?,
    })
}
