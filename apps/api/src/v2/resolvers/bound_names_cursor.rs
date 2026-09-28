use bigname_storage::{NameCurrentListCursor, NameCurrentListCursorValue};

use crate::v2::{
    V2Result,
    cursor::invalid_cursor_error,
    list_cursor::{ListCursor, ListPosition},
};

const CHAIN_ID_FILTER_KEY: &str = "chain_id";
const RESOLVER_FILTER_KEY: &str = "resolver";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const CURSOR_NAMESPACE_KEY: &str = "namespace";
const NORMALIZED_NAME_CURSOR_KEY: &str = "normalized_name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";
const NONE_FILTER_VALUE: &str = "";
const POSITION_KEYS: [&str; 4] = [
    SORT_VALUE_CURSOR_KEY,
    CURSOR_NAMESPACE_KEY,
    NORMALIZED_NAME_CURSOR_KEY,
    NAMEHASH_CURSOR_KEY,
];

/// What a bound-names cursor binds besides its position: the resolver, the namespace filter,
/// the sort and, when the request pinned `at`, its `at` token (`list_cursor`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundNamesCursorBinding<'a> {
    pub(crate) chain_id: u64,
    pub(crate) resolver_address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) sort: &'a str,
    pub(crate) at: Option<&'a str>,
}

fn bound_names_list_cursor(binding: &BoundNamesCursorBinding<'_>) -> ListCursor {
    ListCursor::new(
        binding.sort,
        std::collections::BTreeMap::from([
            (CHAIN_ID_FILTER_KEY.to_owned(), binding.chain_id.to_string()),
            (
                RESOLVER_FILTER_KEY.to_owned(),
                binding.resolver_address.to_owned(),
            ),
            (
                NAMESPACE_FILTER_KEY.to_owned(),
                option_filter(binding.namespace),
            ),
        ]),
    )
    .pinned_at(binding.at.map(str::to_owned))
}

/// The continuation after `cursor`'s row.
pub(crate) fn bound_names_next_cursor(
    cursor: &NameCurrentListCursor,
    binding: &BoundNamesCursorBinding<'_>,
) -> String {
    bound_names_list_cursor(binding).next(ListPosition::new([
        (SORT_VALUE_CURSOR_KEY, cursor_sort_value(cursor)),
        (CURSOR_NAMESPACE_KEY, cursor.namespace.clone()),
        (NORMALIZED_NAME_CURSOR_KEY, cursor.normalized_name.clone()),
        (NAMEHASH_CURSOR_KEY, cursor.namehash.clone()),
    ]))
}

/// Refuses a `cursor` this binding could not have written, `at` aside: the overview runs it
/// before reading anything, when the request's `at` token is not yet known.
pub(crate) fn check_bound_names_cursor_shape(
    cursor: Option<&str>,
    binding: &BoundNamesCursorBinding<'_>,
    pinned: bool,
) -> V2Result<()> {
    bound_names_list_cursor(binding).check_shape(cursor, &POSITION_KEYS, pinned)
}

/// The storage position a request's `cursor` continues from; `400 invalid_input` for a cursor
/// this binding did not write.
pub(crate) fn bound_names_storage_cursor(
    cursor: &str,
    binding: &BoundNamesCursorBinding<'_>,
) -> V2Result<NameCurrentListCursor> {
    let position = bound_names_list_cursor(binding)
        .read(Some(cursor), &POSITION_KEYS)?
        .ok_or_else(invalid_cursor_error)?;
    Ok(NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(
            position.get(SORT_VALUE_CURSOR_KEY)?.to_owned(),
        ),
        namespace: position.get(CURSOR_NAMESPACE_KEY)?.to_owned(),
        normalized_name: position.get(NORMALIZED_NAME_CURSOR_KEY)?.to_owned(),
        namehash: position.get(NAMEHASH_CURSOR_KEY)?.to_owned(),
    })
}

fn cursor_sort_value(cursor: &NameCurrentListCursor) -> String {
    match &cursor.sort_value {
        NameCurrentListCursorValue::Name(value) => value.clone(),
        NameCurrentListCursorValue::Timestamp(_) => String::new(),
    }
}

fn option_filter(value: Option<&str>) -> String {
    value.unwrap_or(NONE_FILTER_VALUE).to_owned()
}
