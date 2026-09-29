use std::collections::BTreeMap;

use bigname_storage::{AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue};
use sqlx::types::Uuid;

use crate::v2::{
    AddressNamesDedupe, AddressNamesSort, AuthoritySet, CursorPayload, RelationSet, SortOrder,
    V2Result,
    cursor::{cursor_value, invalid_cursor_error},
    format_timestamp,
    name_filter::NameMatch,
};

pub(crate) const ADDRESS_FILTER_KEY: &str = "address";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const DEDUPE_FILTER_KEY: &str = "dedupe";
const Q_FILTER_KEY: &str = "q";
const AUTHORITY_FILTER_KEY: &str = "authority";
const MATCH_FILTER_KEY: &str = "match";
pub(crate) const ORDER_FILTER_KEY: &str = "order";
pub(crate) const SORT_KIND_CURSOR_KEY: &str = "sort_kind";
pub(crate) const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const LOGICAL_NAME_ID_CURSOR_KEY: &str = "logical_name_id";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
/// The digest of the address's registry-child renderings the page was read with
/// (`AddressNamesCurrentSortedPage::registry_children_digest`).
pub(crate) const REGISTRY_CHILDREN_KEY: &str = "registry_children";
pub(crate) const SORT_KIND_NAME: &str = "name";
pub(crate) const SORT_KIND_TIMESTAMP_NULL: &str = "timestamp_null";
pub(crate) const SORT_KIND_TIMESTAMP_VALUE: &str = "timestamp_value";
const NONE_FILTER_VALUE: &str = "";

#[derive(Clone, Debug)]
pub(crate) struct AddressNamesCursorBinding<'a> {
    pub(crate) address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) relation: Option<&'a RelationSet>,
    pub(crate) dedupe: AddressNamesDedupe,
    pub(crate) q: Option<&'a str>,
    pub(crate) name_match: NameMatch,
    pub(crate) authority: Option<&'a AuthoritySet>,
    pub(crate) is_migrated: Option<bool>,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
}

/// Every filter an ownership-relation cursor binds.
fn cursor_filters(binding: &AddressNamesCursorBinding<'_>) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
        (ADDRESS_FILTER_KEY.to_owned(), binding.address.to_owned()),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            option_filter(binding.namespace),
        ),
        (
            RELATION_FILTER_KEY.to_owned(),
            relation_filter_value(binding.relation),
        ),
        (
            DEDUPE_FILTER_KEY.to_owned(),
            binding.dedupe.as_str().to_owned(),
        ),
        (Q_FILTER_KEY.to_owned(), option_filter(binding.q)),
        (
            "is_migrated".to_owned(),
            binding
                .is_migrated
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ),
        (
            AUTHORITY_FILTER_KEY.to_owned(),
            authority_filter_value(binding.authority),
        ),
        (
            ORDER_FILTER_KEY.to_owned(),
            binding.order.as_str().to_owned(),
        ),
    ]);
    insert_match_filter(&mut filters, binding.q, binding.name_match);
    filters
}

pub(crate) fn address_names_cursor_payload(
    cursor: &AddressNamesCurrentSortedCursor,
    binding: &AddressNamesCursorBinding<'_>,
    registry_children_digest: &str,
) -> CursorPayload {
    CursorPayload::new(
        binding.sort.as_str(),
        {
            let mut filters = cursor_filters(binding);
            filters.insert(
                REGISTRY_CHILDREN_KEY.to_owned(),
                registry_children_digest.to_owned(),
            );
            filters
        },
        cursor_last_item(cursor),
        None,
    )
}

pub(crate) fn address_names_storage_cursor(
    payload: &CursorPayload,
    binding: &AddressNamesCursorBinding<'_>,
) -> V2Result<AddressNamesCurrentSortedCursor> {
    // The storage read checks the registry-children digest (`registry_children_digest`); every
    // other filter must be the request's own.
    let mut filters = payload.filters.clone();
    if payload.sort != binding.sort.as_str()
        || filters.remove(REGISTRY_CHILDREN_KEY).is_none()
        || filters != cursor_filters(binding)
    {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_cursor_error());
    }

    let sort_value = cursor_sort_value(payload, binding.sort)?;
    let logical_name_id = cursor_value(payload, LOGICAL_NAME_ID_CURSOR_KEY, invalid_cursor_error)?;
    let resource_id = Uuid::parse_str(&cursor_value(
        payload,
        RESOURCE_ID_CURSOR_KEY,
        invalid_cursor_error,
    )?)
    .map_err(|_| invalid_cursor_error())?;

    Ok(AddressNamesCurrentSortedCursor {
        sort_value,
        logical_name_id,
        resource_id,
    })
}

pub(super) fn cursor_last_item(
    cursor: &AddressNamesCurrentSortedCursor,
) -> BTreeMap<String, String> {
    let (sort_kind, sort_value) = match &cursor.sort_value {
        AddressNamesCurrentSortedCursorValue::Name(value) => {
            (SORT_KIND_NAME.to_owned(), value.clone())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(None) => {
            (SORT_KIND_TIMESTAMP_NULL.to_owned(), String::new())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)) => (
            SORT_KIND_TIMESTAMP_VALUE.to_owned(),
            format_timestamp(*value),
        ),
    };

    BTreeMap::from([
        (SORT_KIND_CURSOR_KEY.to_owned(), sort_kind),
        (SORT_VALUE_CURSOR_KEY.to_owned(), sort_value),
        (
            LOGICAL_NAME_ID_CURSOR_KEY.to_owned(),
            cursor.logical_name_id.clone(),
        ),
        (
            RESOURCE_ID_CURSOR_KEY.to_owned(),
            cursor.resource_id.to_string(),
        ),
    ])
}

pub(super) fn cursor_sort_value(
    payload: &CursorPayload,
    sort: AddressNamesSort,
) -> V2Result<AddressNamesCurrentSortedCursorValue> {
    let sort_kind = cursor_value(payload, SORT_KIND_CURSOR_KEY, invalid_cursor_error)?;
    let sort_value = payload
        .last_item
        .get(SORT_VALUE_CURSOR_KEY)
        .cloned()
        .ok_or_else(invalid_cursor_error)?;

    match (sort, sort_kind.as_str()) {
        (AddressNamesSort::Name, SORT_KIND_NAME) if !sort_value.trim().is_empty() => {
            Ok(AddressNamesCurrentSortedCursorValue::Name(sort_value))
        }
        (
            AddressNamesSort::ExpiresAt
            | AddressNamesSort::RegisteredAt
            | AddressNamesSort::CreatedAt,
            SORT_KIND_TIMESTAMP_NULL,
        ) if sort_value.is_empty() => Ok(AddressNamesCurrentSortedCursorValue::Timestamp(None)),
        (
            AddressNamesSort::ExpiresAt
            | AddressNamesSort::RegisteredAt
            | AddressNamesSort::CreatedAt,
            SORT_KIND_TIMESTAMP_VALUE,
        ) if !sort_value.trim().is_empty() => {
            let value = bigname_storage::parse_rfc3339_utc_timestamp(&sort_value)
                .map_err(|_| invalid_cursor_error())?;
            Ok(AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)))
        }
        _ => Err(invalid_cursor_error()),
    }
}

pub(super) fn option_filter(value: Option<&str>) -> String {
    value.unwrap_or(NONE_FILTER_VALUE).to_owned()
}

/// The bound `authority`: the set's comma-joined wire values, so a one-value set binds exactly
/// what a single `authority` cursor bound before sets existed.
pub(super) fn authority_filter_value(value: Option<&AuthoritySet>) -> String {
    value
        .map(AuthoritySet::canonical_value)
        .unwrap_or_else(|| NONE_FILTER_VALUE.to_owned())
}

/// Binds `match=contains` only when it narrows a present `q`; a prefix match, the default,
/// adds no key, so prefix and unfiltered cursors keep the shape they had before `match`.
pub(super) fn insert_match_filter(
    filters: &mut BTreeMap<String, String>,
    q: Option<&str>,
    name_match: NameMatch,
) {
    if q.is_some() && name_match == NameMatch::Contains {
        filters.insert(MATCH_FILTER_KEY.to_owned(), name_match.as_str().to_owned());
    }
}

fn relation_filter_value(value: Option<&RelationSet>) -> String {
    value
        .map(RelationSet::canonical_value)
        .unwrap_or_else(|| NONE_FILTER_VALUE.to_owned())
}

/// The registry-child digest a continuation's cursor was issued with, which the storage read
/// compares with the page's before it validates the cursor's anchor: a label preimage that arrived
/// for one of the children (or one appearing or leaving) can move it across the cursor in a
/// name-sorted page, so the read must restart.
pub(crate) fn registry_children_digest(payload: &CursorPayload) -> Option<&str> {
    payload
        .filters
        .get(REGISTRY_CHILDREN_KEY)
        .map(String::as_str)
}
