//! `GET /v1/names`: the namespace-wide listing of current names by registration expiry.
//!
//! This is the collection parent of `GET /v1/names/{name}`, kept under `/v1/names` rather than a
//! `/v1/registrations` route because its rows are names — the same dictionary shape search
//! serves — each carrying its selected current registration, and because no other route
//! addresses a registration as a resource: registrations appear only as `registration_id` on
//! history and permissions. The reader is index-backed and refuses an unbounded scan.

use std::collections::BTreeMap;

use axum::{Json, extract::State};
use bigname_storage::UnixSeconds;
use bigname_storage::{
    NameCurrentExpiringFilter, NameCurrentListCursor, NameCurrentListCursorValue,
    NameCurrentListOrder,
};

use super::collection_snapshot::CollectionSnapshot;
use crate::AppState;

use super::cursor::invalid_cursor_error;
use super::list_cursor::{ListCursor, ListPosition};
use super::search::{SearchName, build_search_name};
use super::support::{ensure_public_namespace, normalize_inferred_route_name};
use super::vocab::AuthoritySet;
use super::{
    Envelope, Page, QueryParamAllowlist, SortOrder, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2, validate_latest_collection_selectors,
};

const NAMES_SORT: &str = "expires_at";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const EXPIRES_AFTER_FILTER_KEY: &str = "expires_after";
const EXPIRES_BEFORE_FILTER_KEY: &str = "expires_before";
const ORDER_FILTER_KEY: &str = "order";
const AUTHORITY_FILTER_KEY: &str = "authority";
const PARENT_FILTER_KEY: &str = "parent";
const EXPIRES_AT_CURSOR_KEY: &str = "expires_at";
const NAME_CURSOR_KEY: &str = "name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";
const NONE_FILTER_VALUE: &str = "";
const POSITION_KEYS: [&str; 4] = [
    EXPIRES_AT_CURSOR_KEY,
    NAMESPACE_FILTER_KEY,
    NAME_CURSOR_KEY,
    NAMEHASH_CURSOR_KEY,
];

pub(crate) struct NamesQueryParams;

impl QueryParamAllowlist for NamesQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "expires_after",
        "expires_before",
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

pub(crate) type NamesQuery = StrictQueryParams<NamesQueryParams>;

/// Everything a names-listing cursor binds besides its keyset position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NamesCursorBinding<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) expires_after: Option<UnixSeconds>,
    pub(crate) expires_before: Option<UnixSeconds>,
    pub(crate) authority: Option<&'a AuthoritySet>,
    pub(crate) parent: Option<&'a str>,
    pub(crate) order: SortOrder,
}

pub(crate) async fn get_names(
    params: NamesQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<SearchName>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let namespace = params.namespace.clone().ok_or_else(|| {
        V2Error::invalid_input("namespace is required because this listing is namespace-scoped")
    })?;
    ensure_public_namespace(&namespace).map_err(api_error_to_v2)?;
    match params.sort_wire.as_deref() {
        None | Some(NAMES_SORT) => {}
        Some(_) => {
            return Err(V2Error::invalid_input(
                "sort must be expires_at on this listing",
            ));
        }
    }
    if params.expires_after.is_none() && params.expires_before.is_none() {
        return Err(V2Error::invalid_input(
            "expires_after or expires_before is required so the listing is bounded",
        ));
    }
    if let (Some(after), Some(before)) = (params.expires_after, params.expires_before)
        && after >= before
    {
        return Err(V2Error::invalid_input(
            "expires_after must be earlier than expires_before",
        ));
    }

    let parent = params.parent.as_deref().map(normalize_parent).transpose()?;

    let order = params.order.unwrap_or(SortOrder::Asc);
    let binding = NamesCursorBinding {
        namespace: &namespace,
        expires_after: params.expires_after,
        expires_before: params.expires_before,
        authority: params.authority.as_ref(),
        parent: parent.as_deref(),
        order,
    };
    // The cursor holds the window, order and position only; a continuation reads what is
    // published now (`list_cursor`).
    let list = names_list_cursor(&binding);
    let storage_cursor = list
        .read(params.cursor.as_deref(), &POSITION_KEYS)?
        .map(|position| names_storage_cursor(&position))
        .transpose()?;

    let mut snapshot =
        CollectionSnapshot::capture_for_namespace(&state, None, Some(&namespace)).await?;

    let filter = NameCurrentExpiringFilter {
        namespace: namespace.clone(),
        expires_after: params.expires_after,
        expires_before: params.expires_before,
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
        .map(|cursor| names_position(cursor).map(|position| list.next(position)))
        .transpose()?;
    let has_more = next_cursor.is_some();
    // A released row names its last holder, as name detail does (TYR-63).
    let data = storage_page
        .rows
        .iter()
        .map(|row| {
            Ok(SearchName {
                lapsed_registration: super::name_record::lapsed_registration(
                    &row.row.declared_summary,
                ),
                ..build_search_name(row)?
            })
        })
        .collect::<V2Result<_>>()?;

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
/// refused.
fn normalize_parent(value: &str) -> V2Result<String> {
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
    ListCursor::new(NAMES_SORT, cursor_filters(binding))
}

fn names_position(cursor: &NameCurrentListCursor) -> V2Result<ListPosition> {
    let NameCurrentListCursorValue::Timestamp(Some(expires_at)) = cursor.sort_value else {
        return Err(V2Error::internal_error(
            "names listing cursor must carry an expiry timestamp",
        ));
    };

    Ok(ListPosition::new([
        (EXPIRES_AT_CURSOR_KEY, expires_at.internal_string()),
        (NAMESPACE_FILTER_KEY, cursor.namespace.clone()),
        (NAME_CURSOR_KEY, cursor.normalized_name.clone()),
        (NAMEHASH_CURSOR_KEY, cursor.namehash.clone()),
    ]))
}

fn names_storage_cursor(position: &ListPosition) -> V2Result<NameCurrentListCursor> {
    let expires_at = position
        .get(EXPIRES_AT_CURSOR_KEY)?
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
mod tests {
    use super::*;

    fn timestamp(value: &str) -> UnixSeconds {
        value.parse::<UnixSeconds>().expect("timestamp must parse")
    }

    fn binding<'a>() -> NamesCursorBinding<'a> {
        NamesCursorBinding {
            namespace: "ens",
            expires_after: Some(timestamp("2026-09-01T00:00:00Z")),
            expires_before: None,
            authority: None,
            parent: None,
            order: SortOrder::Asc,
        }
    }

    fn cursor() -> NameCurrentListCursor {
        NameCurrentListCursor {
            sort_value: NameCurrentListCursorValue::Timestamp(Some(timestamp(
                "2026-10-01T00:00:00Z",
            ))),
            namespace: "ens".to_owned(),
            normalized_name: "beta.eth".to_owned(),
            namehash: "0xbeta".to_owned(),
        }
    }

    fn read(binding: &NamesCursorBinding<'_>, cursor: &str) -> V2Result<NameCurrentListCursor> {
        let position = names_list_cursor(binding)
            .read(Some(cursor), &POSITION_KEYS)?
            .expect("a cursor was sent");
        names_storage_cursor(&position)
    }

    #[test]
    fn names_cursor_round_trips_and_binds_window_and_order() {
        let binding = binding();
        let cursor_text = names_list_cursor(&binding)
            .next(names_position(&cursor()).expect("position must build"));
        let payload = crate::v2::decode(&cursor_text).expect("cursor must decode");
        assert_eq!(payload.sort, "expires_at");
        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("namespace".to_owned(), "ens".to_owned()),
                ("expires_after".to_owned(), "1788220800".to_owned()),
                ("expires_before".to_owned(), String::new()),
                ("order".to_owned(), "asc".to_owned()),
            ])
        );
        assert_eq!(
            read(&binding, &cursor_text).expect("cursor must decode"),
            cursor()
        );

        for other in [
            NamesCursorBinding {
                namespace: "basenames",
                ..binding
            },
            NamesCursorBinding {
                expires_after: None,
                ..binding
            },
            NamesCursorBinding {
                expires_before: Some(timestamp("2027-01-01T00:00:00Z")),
                ..binding
            },
            NamesCursorBinding {
                order: SortOrder::Desc,
                ..binding
            },
        ] {
            assert!(
                read(&other, &cursor_text).is_err(),
                "{other:?} must reject a cursor bound to {binding:?}"
            );
        }

        let mut wrong_sort = payload.clone();
        wrong_sort.sort = "name".to_owned();
        assert!(read(&binding, &crate::v2::encode(&wrong_sort)).is_err());
    }

    #[test]
    fn names_cursor_binds_authority_and_parent_only_when_sent() {
        let ens_v1 = AuthoritySet::from(super::super::vocab::Authority::EnsV1);
        let both = AuthoritySet::from_authorities([
            super::super::vocab::Authority::EnsV1,
            super::super::vocab::Authority::EnsV0,
        ])
        .expect("non-empty set");
        let filtered = NamesCursorBinding {
            authority: Some(&both),
            parent: Some("eth"),
            ..binding()
        };
        let cursor_text = names_list_cursor(&filtered)
            .next(names_position(&cursor()).expect("position must build"));
        let filters = crate::v2::decode(&cursor_text)
            .expect("cursor must decode")
            .filters;
        assert_eq!(filters["authority"], "ens_v0,ens_v1");
        assert_eq!(filters["parent"], "eth");
        assert_eq!(
            read(&filtered, &cursor_text).expect("cursor must decode"),
            cursor()
        );
        for other in [
            binding(),
            NamesCursorBinding {
                authority: Some(&ens_v1),
                ..filtered
            },
            NamesCursorBinding {
                parent: None,
                ..filtered
            },
            NamesCursorBinding {
                parent: Some("base.eth"),
                ..filtered
            },
        ] {
            assert!(
                read(&other, &cursor_text).is_err(),
                "{other:?} must reject a cursor bound to {filtered:?}"
            );
        }
        assert!(
            !crate::v2::decode(
                &names_list_cursor(&binding())
                    .next(names_position(&cursor()).expect("position must build"))
            )
            .expect("cursor must decode")
            .filters
            .keys()
            .any(|key| key == "authority" || key == "parent")
        );
    }

    #[test]
    fn names_position_refuses_a_name_or_null_sort_value() {
        for sort_value in [
            NameCurrentListCursorValue::Name("beta.eth".to_owned()),
            NameCurrentListCursorValue::Timestamp(None),
        ] {
            let cursor = NameCurrentListCursor {
                sort_value,
                ..cursor()
            };
            assert!(names_position(&cursor).is_err());
        }
    }
}
