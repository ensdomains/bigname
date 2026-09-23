use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{HistoryCursor, HistoryOrder, HistorySummaryMode};

use crate::AppState;

use super::address_names::relation_set_to_storage;
use super::cursor::{cursor_value, invalid_cursor_error};
use super::support::{ensure_public_namespace, parse_evm_address};
use super::{
    CursorPayload, Envelope, Event, HISTORY_TOTAL_COUNT_CAP, HistoryScope, Page,
    QueryParamAllowlist, QueryParams, RelationSet, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2, build_event, decode, encode, history_include, history_page_options,
    history_sort_token, history_storage_order, history_storage_scope, history_total_count,
    insert_history_filter_keys, map_history_page_error, resolve_history_block_window,
    validate_latest_collection_selectors,
};

const ADDRESS_FILTER_KEY: &str = "address";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const SCOPE_FILTER_KEY: &str = "scope";
const NORMALIZED_EVENT_ID_CURSOR_KEY: &str = "normalized_event_id";
const EVENT_IDENTITY_CURSOR_KEY: &str = "event_identity";

pub(crate) struct AddressHistoryQueryParams;

impl QueryParamAllowlist for AddressHistoryQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "at",
        "finality",
        "relation",
        "scope",
        "type",
        "order",
        "from_timestamp",
        "to_timestamp",
        "include",
        "cursor",
        "page_size",
    ];
}

pub(crate) type AddressHistoryQuery = StrictQueryParams<AddressHistoryQueryParams>;

pub(crate) async fn get_address_history(
    Path(address): Path<String>,
    params: AddressHistoryQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Event>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    if params
        .relation
        .as_ref()
        .is_some_and(RelationSet::is_resolves_to)
    {
        return Err(V2Error::invalid_input(
            "relation=resolves_to is not supported for address history",
        ));
    }
    let include = history_include(&params.include)?;
    let normalized_address = parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let namespace = params.namespace.clone().unwrap_or_else(|| "ens".to_owned());
    ensure_public_namespace(&namespace).map_err(api_error_to_v2)?;
    let storage_relations = params
        .relation
        .as_ref()
        .map(relation_set_to_storage)
        .unwrap_or_default();
    let storage_relations = (!storage_relations.is_empty()).then_some(storage_relations.as_slice());
    let storage_scope = history_storage_scope(params.scope);

    let cursor_binding = AddressHistoryCursorBinding {
        address: &normalized_address,
        namespace: &namespace,
        relation: params.relation.as_ref(),
        scope: params.scope,
        order: history_storage_order(params.order),
        params: Some(&params),
    };
    let cursor = params.cursor.as_deref().map(decode).transpose()?;
    let storage_cursor = cursor
        .as_ref()
        .map(|payload| address_history_storage_cursor(payload, &cursor_binding))
        .transpose()?;
    let history = super::collection_binding::HistoryCollection::capture(
        &state,
        cursor.as_ref(),
        Some(&namespace),
    )
    .await?;
    let block_window = Some(super::history::bound_history_block_window(
        resolve_history_block_window(&state.pool, &params).await?,
        &history.block_bounds(),
    ));
    let mut options = history_page_options(&params, block_window);
    options.publication_block_bounds = Some(history.block_bounds());

    let storage_page = match bigname_storage::load_address_history_page_for_relations(
        &state.pool,
        &normalized_address,
        Some(&namespace),
        storage_relations,
        storage_scope,
        true,
        storage_cursor.as_ref(),
        params.page_size,
        if params.include.iter().any(|v| v == "total_count") {
            HistorySummaryMode::Count
        } else {
            HistorySummaryMode::CappedCount(HISTORY_TOTAL_COUNT_CAP)
        },
        &options,
        true,
    )
    .await
    {
        Ok(page) => page,
        Err(error) => {
            let error = map_history_page_error(error, "failed to load address history");
            return Err(history.fail(&state, error).await);
        }
    };

    #[cfg(test)]
    bigname_storage::history_anchor_read_test_hooks::run(
        &state.pool,
        bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterPage,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to run history read test hook"))?;

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&history.bind_cursor(address_history_cursor_payload(cursor, &cursor_binding)))
    });
    let has_more = next_cursor.is_some();
    let total_count = if params.include.iter().any(|v| v == "total_count") {
        storage_page.summary.as_ref().map(|s| s.total_count)
    } else {
        history_total_count(storage_page.summary.as_ref())
    };
    let logical_name_ids = storage_page
        .rows
        .iter()
        .filter_map(|row| row.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let names = bigname_storage::load_name_current_by_logical_name_ids(
        &state.pool,
        &logical_name_ids,
    )
    .await
    .map_err(|error| {
        tracing::error!(error = ?error, "failed to load address-history names from phase projections");
        V2Error::internal_error("failed to load address history")
    })?;
    if let Some(fence) = storage_page.interpret_redo_fence.as_ref() {
        bigname_storage::revalidate_interpret_redo_fence(&state.pool, fence)
            .await
            .map_err(|error| map_history_page_error(error, "failed to load address history"))?;
    }
    let data = storage_page
        .rows
        .iter()
        .filter_map(|row| {
            let name = row
                .logical_name_id
                .as_ref()
                .and_then(|logical_name_id| names.get(logical_name_id))
                .map(|row| row.normalized_name.as_str());
            build_event(row, name, include)
        })
        .collect();
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count,
            has_more,
        }),
        meta: history.finish(&state).await?,
    }))
}

#[derive(Clone, Debug)]
pub(crate) struct AddressHistoryCursorBinding<'a> {
    pub(crate) address: &'a str,
    pub(crate) namespace: &'a str,
    pub(crate) relation: Option<&'a RelationSet>,
    pub(crate) scope: HistoryScope,
    pub(crate) order: HistoryOrder,
    /// Request parameters whose `type` set and timestamp bounds the cursor binds;
    /// `None` binds an unfiltered request.
    pub(crate) params: Option<&'a QueryParams>,
}

pub(crate) fn address_history_cursor_payload(
    cursor: &HistoryCursor,
    binding: &AddressHistoryCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        history_sort_token(binding.order),
        address_history_cursor_filters(binding),
        BTreeMap::from([
            (
                NORMALIZED_EVENT_ID_CURSOR_KEY.to_owned(),
                cursor.normalized_event_id.to_string(),
            ),
            (
                EVENT_IDENTITY_CURSOR_KEY.to_owned(),
                cursor.event_identity.clone(),
            ),
        ]),
        None,
    )
}

pub(crate) fn address_history_storage_cursor(
    payload: &CursorPayload,
    binding: &AddressHistoryCursorBinding<'_>,
) -> V2Result<HistoryCursor> {
    if payload.sort != history_sort_token(binding.order) {
        return Err(invalid_cursor_error());
    }
    if payload.filters != address_history_cursor_filters(binding) {
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
        normalized_event_id,
        event_identity,
    })
}

fn address_history_cursor_filters(
    binding: &AddressHistoryCursorBinding<'_>,
) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
        (ADDRESS_FILTER_KEY.to_owned(), binding.address.to_owned()),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            binding.namespace.to_owned(),
        ),
        (
            SCOPE_FILTER_KEY.to_owned(),
            binding.scope.as_str().to_owned(),
        ),
    ]);
    if let Some(relation) = binding.relation {
        filters.insert(RELATION_FILTER_KEY.to_owned(), relation.canonical_value());
    }
    if let Some(params) = binding.params {
        insert_history_filter_keys(&mut filters, params);
    }
    filters
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::Relation;

    const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";
    const OTHER_ADDRESS: &str = "0x00000000000000000000000000000000000000bb";

    fn sample_cursor() -> HistoryCursor {
        HistoryCursor {
            normalized_event_id: 42,
            event_identity: "event:42".to_owned(),
        }
    }

    fn sample_binding() -> AddressHistoryCursorBinding<'static> {
        let relation = Box::leak(Box::new(RelationSet::from(Relation::Manager)));
        AddressHistoryCursorBinding {
            address: ADDRESS,
            namespace: "ens",
            relation: Some(relation),
            scope: HistoryScope::Both,
            order: HistoryOrder::Desc,
            params: None,
        }
    }

    #[test]
    fn address_history_cursor_payload_round_trips_storage_cursor() {
        let cursor = sample_cursor();
        let binding = sample_binding();
        let payload = address_history_cursor_payload(&cursor, &binding);

        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("address".to_owned(), ADDRESS.to_owned()),
                ("namespace".to_owned(), "ens".to_owned()),
                ("relation".to_owned(), "manager".to_owned()),
                ("scope".to_owned(), "both".to_owned()),
            ])
        );
        assert_eq!(
            address_history_storage_cursor(&payload, &binding).expect("cursor must decode"),
            cursor
        );
        assert!(payload.snapshot.is_none());
    }

    #[test]
    fn address_history_cursor_omits_unset_relation_filter() {
        let cursor = sample_cursor();
        let binding = AddressHistoryCursorBinding {
            relation: None,
            ..sample_binding()
        };
        let payload = address_history_cursor_payload(&cursor, &binding);

        assert!(!payload.filters.contains_key("relation"));
        assert_eq!(
            address_history_storage_cursor(&payload, &binding).expect("cursor must decode"),
            cursor
        );
    }

    #[test]
    fn address_history_cursor_binds_order_type_set_and_timestamps() {
        let cursor = sample_cursor();
        let params = crate::v2::QueryParams::try_from(crate::v2::RawQueryParams {
            order: Some("asc".to_owned()),
            event_type: Some("record,authority".to_owned()),
            to_timestamp: Some("2023-11-14T22:15:07Z".to_owned()),
            ..crate::v2::RawQueryParams::default()
        })
        .expect("params must parse");
        let binding = AddressHistoryCursorBinding {
            order: HistoryOrder::Asc,
            params: Some(&params),
            ..sample_binding()
        };
        let payload = address_history_cursor_payload(&cursor, &binding);

        assert_eq!(payload.sort, "chain_position_asc");
        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("address".to_owned(), ADDRESS.to_owned()),
                ("namespace".to_owned(), "ens".to_owned()),
                ("relation".to_owned(), "manager".to_owned()),
                ("scope".to_owned(), "both".to_owned()),
                ("type".to_owned(), "authority,record".to_owned()),
                ("to_timestamp".to_owned(), "2023-11-14T22:15:07Z".to_owned()),
            ])
        );
        assert_eq!(
            address_history_storage_cursor(&payload, &binding).expect("cursor must decode"),
            cursor
        );
        assert!(address_history_storage_cursor(&payload, &sample_binding()).is_err());
        let desc_binding = AddressHistoryCursorBinding {
            order: HistoryOrder::Desc,
            ..binding.clone()
        };
        assert!(address_history_storage_cursor(&payload, &desc_binding).is_err());
    }

    #[test]
    fn address_history_cursor_rejects_wrong_sort_or_filters() {
        let cursor = sample_cursor();
        let binding = sample_binding();

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload.sort = "name".to_owned();
        assert!(address_history_storage_cursor(&payload, &binding).is_err());

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload
            .filters
            .insert("address".to_owned(), OTHER_ADDRESS.to_owned());
        assert!(address_history_storage_cursor(&payload, &binding).is_err());

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload
            .filters
            .insert("scope".to_owned(), "name".to_owned());
        assert!(address_history_storage_cursor(&payload, &binding).is_err());

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload.filters.remove("relation");
        assert!(address_history_storage_cursor(&payload, &binding).is_err());
    }

    #[test]
    fn address_history_cursor_ignores_legacy_snapshot_component() {
        let cursor = sample_cursor();
        let binding = sample_binding();
        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload.snapshot = Some("legacy-snapshot".to_owned());

        assert_eq!(
            address_history_storage_cursor(&payload, &binding)
                .expect("legacy snapshot component must not bind a latest-state cursor"),
            cursor
        );
    }
}
