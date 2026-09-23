use std::collections::BTreeMap;

use bigname_storage::{HistoryCursor, HistoryOrder};

use crate::v2::{
    CursorPayload, HistoryScope, QueryParams, V2Result,
    cursor::{cursor_value, invalid_cursor_error},
};

use super::{
    EVENT_IDENTITY_CURSOR_KEY, NAME_FILTER_KEY, NAMESPACE_FILTER_KEY,
    NORMALIZED_EVENT_ID_CURSOR_KEY, SCOPE_FILTER_KEY, history_sort_token,
    insert_history_filter_keys,
};

#[derive(Clone, Debug)]
pub(crate) struct HistoryCursorBinding<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) parent_logical_name_id: &'a str,
    pub(crate) scope: HistoryScope,
    pub(crate) order: HistoryOrder,
    pub(crate) params: &'a QueryParams,
    /// `include=child_registrations`, which changes the collection's rows.
    pub(crate) child_registrations: bool,
}

fn history_cursor_filters(binding: &HistoryCursorBinding<'_>) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            binding.namespace.to_owned(),
        ),
        (
            NAME_FILTER_KEY.to_owned(),
            binding.parent_logical_name_id.to_owned(),
        ),
        (
            SCOPE_FILTER_KEY.to_owned(),
            binding.scope.as_str().to_owned(),
        ),
    ]);
    insert_history_filter_keys(&mut filters, binding.params);
    super::children::insert_children_filter_key(&mut filters, binding.child_registrations);
    filters
}

pub(crate) fn history_cursor_payload(
    cursor: &HistoryCursor,
    binding: &HistoryCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        history_sort_token(binding.order),
        history_cursor_filters(binding),
        BTreeMap::from([
            (
                NORMALIZED_EVENT_ID_CURSOR_KEY.to_owned(),
                cursor.normalized_event_id.unwrap_or_default().to_string(),
            ),
            (
                EVENT_IDENTITY_CURSOR_KEY.to_owned(),
                cursor.event_identity.clone(),
            ),
        ]),
        None,
    )
}

pub(crate) fn history_storage_cursor(
    payload: &CursorPayload,
    binding: &HistoryCursorBinding<'_>,
) -> V2Result<HistoryCursor> {
    if payload.sort != history_sort_token(binding.order) {
        return Err(invalid_cursor_error());
    }
    if payload.filters != history_cursor_filters(binding) {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 2 {
        return Err(invalid_cursor_error());
    }

    let normalized_event_id = cursor_value(
        payload,
        NORMALIZED_EVENT_ID_CURSOR_KEY,
        invalid_cursor_error,
    )?
    .parse::<i64>()
    .map_err(|_| invalid_cursor_error())?;
    let event_identity = cursor_value(payload, EVENT_IDENTITY_CURSOR_KEY, invalid_cursor_error)?;

    Ok(HistoryCursor {
        normalized_event_id: Some(normalized_event_id),
        event_identity,
        position: None,
    })
}
