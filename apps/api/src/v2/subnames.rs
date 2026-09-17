use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentOrder, ChildrenCurrentPageFilter,
    ChildrenCurrentRow, ChildrenCurrentSort, ChildrenCurrentSortValue, ChildrenCurrentSummary,
    NameCurrentRow,
};
use serde::{Deserialize, Serialize};

use crate::AppState;

use super::cursor::{cursor_value, invalid_cursor_error};
use super::name_filter::normalize_name_prefix;
use super::support::normalize_inferred_route_name;
use super::{
    AddressNamesSort, CursorPayload, Envelope, Page, QueryParamAllowlist, RegistrationStatus,
    RegistryRef, SortOrder, StrictQueryParams, V2Error, V2Result, decode, encode, format_timestamp,
    load_subregistry_refs, name_record::name_registration_fields,
    validate_latest_collection_selectors,
};

/// Sort tag of cursors issued before `sort`/`order`/`q`/`include_expired` existed. Such a cursor
/// names the default `name` ascending page over every child, so it stays valid for a request
/// that asks for exactly that.
const LEGACY_SUBNAMES_SORT: &str = "display_name_asc";
const DISPLAY_NAME_CURSOR_KEY: &str = "display_name";
const CHILD_LOGICAL_NAME_ID_CURSOR_KEY: &str = "child_logical_name_id";
const SORT_KIND_CURSOR_KEY: &str = "sort_kind";
const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const SORT_KIND_NAME: &str = "name";
const SORT_KIND_TIMESTAMP_NULL: &str = "timestamp_null";
const SORT_KIND_TIMESTAMP_VALUE: &str = "timestamp_value";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const PARENT_FILTER_KEY: &str = "parent";
const ORDER_FILTER_KEY: &str = "order";
const Q_FILTER_KEY: &str = "q";
const INCLUDE_EXPIRED_FILTER_KEY: &str = "include_expired";
/// Today's behaviour, kept as the default: a page lists released and past-expiry children.
const DEFAULT_INCLUDE_EXPIRED: bool = true;

pub(crate) struct SubnamesQueryParams;

impl QueryParamAllowlist for SubnamesQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "at",
        "finality",
        "q",
        "sort",
        "order",
        "include_expired",
        "include",
        "cursor",
        "page_size",
    ];
}

pub(crate) type SubnamesQuery = StrictQueryParams<SubnamesQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Subname {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) labelhash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registrant: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subregistry: Option<RegistryRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subname_count: Option<u64>,
}

pub(crate) async fn get_subnames(
    Path(input_name): Path<String>,
    params: SubnamesQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Subname>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let include_counts = subnames_include_counts(&params.include)?;

    let logical_name_id =
        bigname_storage::logical_name_id_for_name(&namespace, &normalized.normalized_name);
    let snapshot = super::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        params.cursor.as_deref(),
        Some(&namespace),
    )
    .await?;
    let parent = bigname_storage::load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|_| {
            V2Error::internal_error(format!(
                "failed to load subnames for {}/{}",
                namespace, normalized.normalized_name
            ))
        })?
        .ok_or_else(|| {
            V2Error::not_found(format!(
                "name {} was not found in namespace {namespace}",
                normalized.normalized_name
            ))
        })?;

    let normalized_q = params.q.as_deref().map(normalize_name_prefix).transpose()?;
    let binding = SubnamesCursorBinding {
        namespace: &namespace,
        parent_logical_name_id: &parent.logical_name_id,
        q: normalized_q.as_deref(),
        include_expired: params.include_expired.unwrap_or(DEFAULT_INCLUDE_EXPIRED),
        sort: params.sort,
        order: params.order.unwrap_or(SortOrder::Asc),
    };
    let filter = ChildrenCurrentPageFilter {
        evaluated_at: Some(snapshot.evaluated_at()),
        q: binding.q,
        include_expired: binding.include_expired,
        sort: sort_to_storage(binding.sort),
        order: order_to_storage(binding.order),
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            let cursor = subname_storage_cursor(&payload, &binding)?;
            snapshot.validate_cursor(&payload)?;
            Ok(cursor)
        })
        .transpose()?;

    let storage_page = bigname_storage::load_children_current_page_filtered(
        &state.pool,
        &parent.logical_name_id,
        &filter,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load subnames for {}/{}",
            namespace, normalized.normalized_name
        ))
    })?;

    let child_logical_name_ids = storage_page
        .rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();
    let child_name_rows = bigname_storage::load_name_current_by_logical_name_ids(
        &state.pool,
        &child_logical_name_ids,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load subname registration summaries for {}/{}",
            namespace, normalized.normalized_name
        ))
    })?;
    let child_summaries = if include_counts {
        bigname_storage::load_children_current_summaries(&state.pool, &child_logical_name_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load subname counts for {}/{}",
                    namespace, normalized.normalized_name
                ))
            })?
            .into_iter()
            .map(|summary| (summary.parent_logical_name_id.clone(), summary))
            .collect()
    } else {
        std::collections::BTreeMap::new()
    };
    let mut pointer_names_by_chain = BTreeMap::<String, Vec<String>>::new();
    for row in child_name_rows.values() {
        if let Some(chain) = super::name_chain_id(row) {
            pointer_names_by_chain
                .entry(chain)
                .or_default()
                .push(row.logical_name_id.clone());
        }
    }
    let mut subregistries = BTreeMap::new();
    let bounds = snapshot.block_bounds();
    for (chain, names) in pointer_names_by_chain {
        if let Some(block) = bounds.get(&chain) {
            subregistries.extend(load_subregistry_refs(&state.pool, &names, Some(*block)).await?);
        }
    }

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| encode(&snapshot.bind_cursor(subname_cursor_payload(cursor, &binding))));
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .map(|row| {
            let mut subname = build_subname(
                row,
                child_name_rows.get(&row.child_logical_name_id),
                child_summaries.get(&row.child_logical_name_id),
                include_counts,
            );
            subname.subregistry = subregistries.remove(&row.child_logical_name_id);
            subname
        })
        .collect();
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: Some(storage_page.total_count),
            has_more,
        }),
        meta: snapshot.finish(&state).await?,
    }))
}

pub(crate) fn build_subname(
    row: &ChildrenCurrentRow,
    name_row: Option<&NameCurrentRow>,
    summary: Option<&ChildrenCurrentSummary>,
    include_counts: bool,
) -> Subname {
    let registration = name_registration_fields(name_row, &row.namespace);
    let (owner, registrant) = if name_row.is_some() {
        (
            registration.owner.or_else(|| {
                name_row
                    .filter(|name| {
                        name.resource_id.is_none()
                            && name.serving_resource_id.is_some()
                            && name
                                .declared_summary
                                .pointer("/coverage/enumeration_basis")
                                .and_then(serde_json::Value::as_str)
                                == Some("event_linked_registry_resolver")
                    })
                    .and_then(|_| row.owner.clone())
            }),
            registration.registrant,
        )
    } else {
        (row.owner.clone(), row.registrant.clone())
    };

    Subname {
        name: row.normalized_name.clone(),
        display_name: row.canonical_display_name.clone(),
        namespace: row.namespace.clone(),
        namehash: row.namehash.clone(),
        labelhash: row.labelhash.clone(),
        owner,
        registrant,
        registration_status: registration.registration_status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        subregistry: None,
        subname_count: include_counts.then(|| {
            summary
                .and_then(|summary| u64::try_from(summary.child_count).ok())
                .unwrap_or_default()
        }),
    }
}

/// Everything a subnames cursor binds besides its keyset position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubnamesCursorBinding<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) parent_logical_name_id: &'a str,
    pub(crate) q: Option<&'a str>,
    pub(crate) include_expired: bool,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
}

impl SubnamesCursorBinding<'_> {
    /// True when the request asks for the page a legacy cursor was issued for.
    fn is_legacy_default(&self) -> bool {
        self.q.is_none()
            && self.include_expired == DEFAULT_INCLUDE_EXPIRED
            && self.sort == AddressNamesSort::Name
            && self.order == SortOrder::Asc
    }

    fn filters(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (NAMESPACE_FILTER_KEY.to_owned(), self.namespace.to_owned()),
            (
                PARENT_FILTER_KEY.to_owned(),
                self.parent_logical_name_id.to_owned(),
            ),
            (ORDER_FILTER_KEY.to_owned(), self.order.as_str().to_owned()),
            (
                Q_FILTER_KEY.to_owned(),
                self.q.unwrap_or_default().to_owned(),
            ),
            (
                INCLUDE_EXPIRED_FILTER_KEY.to_owned(),
                self.include_expired.to_string(),
            ),
        ])
    }
}

pub(crate) fn sort_to_storage(sort: AddressNamesSort) -> ChildrenCurrentSort {
    match sort {
        AddressNamesSort::Name => ChildrenCurrentSort::Name,
        AddressNamesSort::ExpiresAt => ChildrenCurrentSort::ExpiresAt,
        AddressNamesSort::RegisteredAt => ChildrenCurrentSort::RegisteredAt,
    }
}

pub(crate) fn order_to_storage(order: SortOrder) -> ChildrenCurrentOrder {
    match order {
        SortOrder::Asc => ChildrenCurrentOrder::Asc,
        SortOrder::Desc => ChildrenCurrentOrder::Desc,
    }
}

pub(crate) fn subname_cursor_payload(
    cursor: &ChildrenCurrentKeysetCursor,
    binding: &SubnamesCursorBinding<'_>,
) -> CursorPayload {
    let (sort_kind, sort_value) = match &cursor.sort_value {
        ChildrenCurrentSortValue::Name => (SORT_KIND_NAME, String::new()),
        ChildrenCurrentSortValue::Timestamp(None) => (SORT_KIND_TIMESTAMP_NULL, String::new()),
        ChildrenCurrentSortValue::Timestamp(Some(value)) => {
            (SORT_KIND_TIMESTAMP_VALUE, format_timestamp(*value))
        }
    };
    CursorPayload::new(
        binding.sort.as_str(),
        binding.filters(),
        BTreeMap::from([
            (SORT_KIND_CURSOR_KEY.to_owned(), sort_kind.to_owned()),
            (SORT_VALUE_CURSOR_KEY.to_owned(), sort_value),
            (
                DISPLAY_NAME_CURSOR_KEY.to_owned(),
                cursor.canonical_display_name.clone(),
            ),
            (
                CHILD_LOGICAL_NAME_ID_CURSOR_KEY.to_owned(),
                cursor.child_logical_name_id.clone(),
            ),
        ]),
        None,
    )
}

pub(crate) fn subname_storage_cursor(
    payload: &CursorPayload,
    binding: &SubnamesCursorBinding<'_>,
) -> V2Result<ChildrenCurrentKeysetCursor> {
    if payload.sort == LEGACY_SUBNAMES_SORT {
        return legacy_subname_storage_cursor(payload, binding);
    }
    if payload.sort != binding.sort.as_str() || payload.filters != binding.filters() {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_cursor_error());
    }

    let sort_kind = cursor_value(payload, SORT_KIND_CURSOR_KEY, invalid_cursor_error)?;
    let sort_value = payload
        .last_item
        .get(SORT_VALUE_CURSOR_KEY)
        .cloned()
        .ok_or_else(invalid_cursor_error)?;
    let sort_value = match (binding.sort, sort_kind.as_str()) {
        (AddressNamesSort::Name, SORT_KIND_NAME) if sort_value.is_empty() => {
            ChildrenCurrentSortValue::Name
        }
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_NULL,
        ) if sort_value.is_empty() => ChildrenCurrentSortValue::Timestamp(None),
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_VALUE,
        ) if !sort_value.trim().is_empty() => ChildrenCurrentSortValue::Timestamp(Some(
            bigname_storage::parse_rfc3339_utc_timestamp(&sort_value)
                .map_err(|_| invalid_cursor_error())?,
        )),
        _ => return Err(invalid_cursor_error()),
    };
    let canonical_display_name =
        cursor_value(payload, DISPLAY_NAME_CURSOR_KEY, invalid_cursor_error)?;
    let child_logical_name_id = cursor_value(
        payload,
        CHILD_LOGICAL_NAME_ID_CURSOR_KEY,
        invalid_cursor_error,
    )?;

    Ok(ChildrenCurrentKeysetCursor {
        sort_value,
        canonical_display_name,
        child_logical_name_id,
    })
}

fn legacy_subname_storage_cursor(
    payload: &CursorPayload,
    binding: &SubnamesCursorBinding<'_>,
) -> V2Result<ChildrenCurrentKeysetCursor> {
    if !binding.is_legacy_default() {
        return Err(invalid_cursor_error());
    }
    if payload.filters.len() != 2
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(binding.namespace)
        || payload.filters.get(PARENT_FILTER_KEY).map(String::as_str)
            != Some(binding.parent_logical_name_id)
    {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 2 {
        return Err(invalid_cursor_error());
    }

    let canonical_display_name =
        cursor_value(payload, DISPLAY_NAME_CURSOR_KEY, invalid_cursor_error)?;
    let child_logical_name_id = cursor_value(
        payload,
        CHILD_LOGICAL_NAME_ID_CURSOR_KEY,
        invalid_cursor_error,
    )?;

    Ok(ChildrenCurrentKeysetCursor {
        sort_value: ChildrenCurrentSortValue::Name,
        canonical_display_name,
        child_logical_name_id,
    })
}

fn subnames_include_counts(include: &[String]) -> V2Result<bool> {
    let mut include_counts = false;
    for value in include {
        match value.as_str() {
            "counts" => include_counts = true,
            _ => return Err(V2Error::invalid_input("include must contain only counts")),
        }
    }
    Ok(include_counts)
}

#[cfg(test)]
mod tests;
