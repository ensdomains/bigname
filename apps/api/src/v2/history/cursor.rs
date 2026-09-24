use std::collections::BTreeMap;

use bigname_storage::{HistoryCursor, HistoryOrder};

use crate::v2::history_keyset::{self, RequestCursor};
use crate::v2::{CursorPayload, HistoryScope, QueryParams, V2Result};

use super::{
    NAME_FILTER_KEY, NAMESPACE_FILTER_KEY, SCOPE_FILTER_KEY, history_sort_token,
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
    history_keyset::cursor_payload(
        cursor,
        history_sort_token(binding.order),
        history_cursor_filters(binding),
    )
}

pub(crate) fn history_storage_cursor(
    payload: &CursorPayload,
    binding: &HistoryCursorBinding<'_>,
) -> V2Result<RequestCursor> {
    history_keyset::decode_cursor(
        payload,
        history_sort_token(binding.order),
        &history_cursor_filters(binding),
    )
}
