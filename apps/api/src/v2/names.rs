//! `GET /v1/names`: the namespace-wide listing of current names by canonical registration deadlines.
//!
//! This is the collection parent of `GET /v1/names/{name}`, kept under `/v1/names` rather than a
//! `/v1/registrations` route because its rows are names — the same dictionary shape search
//! serves — each carrying its selected current registration, and because no other route
//! addresses a registration as a resource: registrations appear only as `registration_id` on
//! history and permissions. The reader is index-backed and refuses an unbounded scan.

mod query;
mod windows;

#[cfg(test)]
mod windows_tests;

use std::collections::BTreeMap;

use windows::{ExpiryWindows, WINDOW_KEY};

use axum::{Json, extract::State};
use bigname_storage::UnixSeconds;
use bigname_storage::{
    NameCurrentDeadline, NameCurrentExpiringFilter, NameCurrentExpiryWindow, NameCurrentListCursor,
    NameCurrentListCursorValue, NameCurrentListOrder,
};

use super::collection_snapshot::CollectionSnapshot;
use crate::AppState;

use super::cursor::invalid_cursor_error;
use super::list_cursor::{ListCursor, ListPosition};
use super::search::{SearchName, build_search_name};
use super::support::{ensure_public_namespace, normalize_inferred_route_name};
use super::vocab::AuthoritySet;
use super::{
    Envelope, Page, QueryParamAllowlist, QueryParams, SortOrder, V2Error, V2Result,
    api_error_to_v2, validate_latest_collection_selectors,
};

const NAMESPACE_FILTER_KEY: &str = "namespace";
const EXPIRES_AFTER_FILTER_KEY: &str = "expires_after";
const EXPIRES_BEFORE_FILTER_KEY: &str = "expires_before";
const ORDER_FILTER_KEY: &str = "order";
const AUTHORITY_FILTER_KEY: &str = "authority";
const PARENT_FILTER_KEY: &str = "parent";
const NAME_CURSOR_KEY: &str = "name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";
const NONE_FILTER_VALUE: &str = "";
fn position_keys(deadline: NameCurrentDeadline) -> [&'static str; 4] {
    [
        deadline.column(),
        NAMESPACE_FILTER_KEY,
        NAME_CURSOR_KEY,
        NAMEHASH_CURSOR_KEY,
    ]
}

pub(crate) struct NamesQueryParams;

impl QueryParamAllowlist for NamesQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "expires_after",
        "expires_before",
        "expires_window",
        "grace_ends_after",
        "grace_ends_before",
        "grace_ends_window",
        "authority",
        "parent",
        "sort",
        "order",
        "at",
        "finality",
        "cursor",
        "page_size",
    ];
}

pub(crate) use query::NamesQuery;

/// Everything a names-listing cursor binds besides its keyset position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NamesCursorBinding<'a> {
    pub(crate) deadline: NameCurrentDeadline,
    pub(crate) namespace: &'a str,
    pub(crate) expires_after: Option<UnixSeconds>,
    pub(crate) expires_before: Option<UnixSeconds>,
    windows: Option<&'a ExpiryWindows>,
    pub(crate) authority: Option<&'a AuthoritySet>,
    pub(crate) parent: Option<&'a str>,
    pub(crate) order: SortOrder,
}

pub(crate) async fn get_names(
    params: NamesQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<SearchName>>>> {
    get_names_page(params.params, params.windows, params.deadline, state).await
}

async fn get_names_page(
    params: QueryParams,
    windows: Option<ExpiryWindows>,
    deadline: NameCurrentDeadline,
    state: AppState,
) -> V2Result<Json<Envelope<Vec<SearchName>>>> {
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let namespace = params.namespace.clone().ok_or_else(|| {
        V2Error::invalid_input("namespace is required because this listing is namespace-scoped")
    })?;
    ensure_public_namespace(&namespace).map_err(api_error_to_v2)?;
    match params.sort_wire.as_deref() {
        None => {}
        Some(sort) if sort == deadline.column() => {}
        Some(_) => {
            return Err(V2Error::invalid_input(
                "sort must match the selected date family",
            ));
        }
    }
    if windows.is_none() && params.expires_after.is_none() && params.expires_before.is_none() {
        return Err(V2Error::invalid_input(
            "a scalar date bound or date window is required so the listing is bounded",
        ));
    }
    if let (Some(after), Some(before)) = (params.expires_after, params.expires_before)
        && after >= before
    {
        return Err(V2Error::invalid_input(
            "the after bound must be earlier than the before bound",
        ));
    }

    let parent = params.parent.as_deref().map(normalize_parent).transpose()?;

    let order = params.order.unwrap_or(SortOrder::Asc);
    let binding = NamesCursorBinding {
        deadline,
        namespace: &namespace,
        expires_after: params.expires_after,
        expires_before: params.expires_before,
        windows: windows.as_ref(),
        authority: params.authority.as_ref(),
        parent: parent.as_deref(),
        order,
    };
    // The cursor holds the window, order and position only; a continuation reads what is
    // published now (`list_cursor`).
    let list = names_list_cursor(&binding);
    let storage_cursor = list
        .read(params.cursor.as_deref(), &position_keys(deadline))?
        .map(|position| names_storage_cursor(&position, deadline))
        .transpose()?;

    let mut snapshot =
        CollectionSnapshot::capture_for_namespace(&state, None, Some(&namespace)).await?;

    let filter = NameCurrentExpiringFilter {
        deadline,
        namespace: namespace.clone(),
        windows: windows
            .as_ref()
            .map(ExpiryWindows::storage_windows)
            .unwrap_or_else(|| {
                vec![NameCurrentExpiryWindow {
                    expires_after: params.expires_after,
                    expires_before: params.expires_before,
                }]
            }),
        authorities: params
            .authority
            .as_ref()
            .map(|set| set.wire_values().into_iter().map(str::to_owned).collect()),
        parent: parent.clone(),
    };
    // The rows are composed from the owned key families.
    let storage_page = {
        let chains: Vec<String> = snapshot.block_bounds().into_keys().collect();
        bigname_storage::families::name::load_family_expiring_page(
            snapshot.conn().await?,
            &filter,
            order_to_storage(order),
            storage_cursor.as_ref(),
            params.page_size,
            &chains,
        )
        .await
    }
    .map_err(crate::v2::name_rows_error(
        crate::v2::SnapshotReadResource::Name,
        |_| V2Error::internal_error(format!("failed to load names for {namespace}")),
    ))?;

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| names_position(cursor, deadline).map(|position| list.next(position)))
        .transpose()?;
    let has_more = next_cursor.is_some();
    let mut data = storage_page
        .rows
        .iter()
        .map(|row| {
            let window_index = windows
                .as_ref()
                .map(|windows| {
                    windows.index_of(deadline.value(row)).ok_or_else(|| {
                        V2Error::internal_error(
                            "selected deadline does not belong to a requested window",
                        )
                    })
                })
                .transpose()?;
            Ok(SearchName {
                expires_window_index: (deadline == NameCurrentDeadline::Expiry)
                    .then_some(window_index)
                    .flatten(),
                grace_ends_window_index: (deadline == NameCurrentDeadline::GraceEnds)
                    .then_some(window_index)
                    .flatten(),
                lapsed_registration: super::name_record::lapsed_registration(
                    &row.row.declared_summary,
                ),
                ..build_search_name(row)?
            })
        })
        .collect::<V2Result<Vec<_>>>()?;
    super::name_record::fill_wrapper_expiries(
        snapshot.conn().await?,
        data.iter_mut().filter_map(|name| name.ens_v1.as_mut()),
    )
    .await?;

    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta: snapshot.finish(&state).await?,
    }))
}

fn order_to_storage(order: SortOrder) -> NameCurrentListOrder {
    match order {
        SortOrder::Asc => NameCurrentListOrder::Asc,
        SortOrder::Desc => NameCurrentListOrder::Desc,
    }
}

/// A name in its normalized form, as name routes normalize a path name; blank or invalid is
/// refused. `GET /v1/addresses/{address}/names` takes the same `parent`.
pub(crate) fn normalize_parent(value: &str) -> V2Result<String> {
    normalize_inferred_route_name(value)
        .map(|name| name.normalized_name)
        .map_err(|error| {
            V2Error::invalid_input(format!(
                "parent must be a valid ENSIP-15 name: {}",
                error.message
            ))
        })
}

/// `authority` and `parent` add a key only when sent, so an unfiltered cursor keeps the shape it
/// had before the filters existed.
fn cursor_filters(binding: &NamesCursorBinding<'_>) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
        (
            "date_family".to_owned(),
            binding.deadline.column().to_owned(),
        ),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            binding.namespace.to_owned(),
        ),
        (
            EXPIRES_AFTER_FILTER_KEY.to_owned(),
            option_timestamp_filter(binding.expires_after),
        ),
        (
            EXPIRES_BEFORE_FILTER_KEY.to_owned(),
            option_timestamp_filter(binding.expires_before),
        ),
        (
            ORDER_FILTER_KEY.to_owned(),
            binding.order.as_str().to_owned(),
        ),
    ]);
    if let Some(windows) = binding.windows {
        filters.remove(EXPIRES_AFTER_FILTER_KEY);
        filters.remove(EXPIRES_BEFORE_FILTER_KEY);
        filters.insert(WINDOW_KEY.to_owned(), windows.canonical());
    }
    if binding.deadline == NameCurrentDeadline::GraceEnds {
        for (expiry, grace) in [
            (EXPIRES_AFTER_FILTER_KEY, "grace_ends_after"),
            (EXPIRES_BEFORE_FILTER_KEY, "grace_ends_before"),
            (WINDOW_KEY, "grace_ends_window"),
        ] {
            if let Some(value) = filters.remove(expiry) {
                filters.insert(grace.to_owned(), value);
            }
        }
    }
    if let Some(authority) = binding.authority {
        filters.insert(AUTHORITY_FILTER_KEY.to_owned(), authority.canonical_value());
    }
    if let Some(parent) = binding.parent {
        filters.insert(PARENT_FILTER_KEY.to_owned(), parent.to_owned());
    }
    filters
}

fn option_timestamp_filter(value: Option<UnixSeconds>) -> String {
    value.map_or_else(|| NONE_FILTER_VALUE.to_owned(), |value| value.to_string())
}

fn names_list_cursor(binding: &NamesCursorBinding<'_>) -> ListCursor {
    ListCursor::new(binding.deadline.column(), cursor_filters(binding))
}

fn names_position(
    cursor: &NameCurrentListCursor,
    deadline: NameCurrentDeadline,
) -> V2Result<ListPosition> {
    let NameCurrentListCursorValue::Timestamp(Some(expires_at)) = cursor.sort_value else {
        return Err(V2Error::internal_error(
            "names listing cursor must carry an expiry timestamp",
        ));
    };

    Ok(ListPosition::new([
        (deadline.column(), expires_at.internal_string()),
        (NAMESPACE_FILTER_KEY, cursor.namespace.clone()),
        (NAME_CURSOR_KEY, cursor.normalized_name.clone()),
        (NAMEHASH_CURSOR_KEY, cursor.namehash.clone()),
    ]))
}

fn names_storage_cursor(
    position: &ListPosition,
    deadline: NameCurrentDeadline,
) -> V2Result<NameCurrentListCursor> {
    let expires_at = position
        .get(deadline.column())?
        .parse::<UnixSeconds>()
        .map_err(|_| invalid_cursor_error())?;

    Ok(NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Timestamp(Some(expires_at)),
        namespace: position.get(NAMESPACE_FILTER_KEY)?.to_owned(),
        normalized_name: position.get(NAME_CURSOR_KEY)?.to_owned(),
        namehash: position.get(NAMEHASH_CURSOR_KEY)?.to_owned(),
    })
}

#[cfg(test)]
mod tests;
