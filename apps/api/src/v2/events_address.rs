//! `GET /v1/events` when `address` is its only anchor. The read pages through the
//! address-history walk that `GET /v1/addresses/{address}/history` serves with `scope=both` and
//! every relation, so a page reads a bounded prefix of the address's history, not all of it.

use std::collections::BTreeMap;

use axum::Json;

use crate::AppState;

use super::events::{Event, events_page, events_summary_mode};
use super::history_keyset;
use super::{
    Envelope, HistoryInclude, QueryParams, V2Result, decode, history_page_options,
    history_sort_token, history_storage_order, map_history_page_error,
    resolve_history_block_window,
};

pub(super) async fn get_address_events(
    state: &AppState,
    params: &QueryParams,
    address: &str,
    namespace: &str,
    include: HistoryInclude,
    cursor_filters: BTreeMap<String, String>,
) -> V2Result<Json<Envelope<Vec<Event>>>> {
    let order = history_storage_order(params.order);
    let (mut snapshot, request_cursor) =
        super::collection_snapshot::CollectionSnapshot::capture_address_history(
            state,
            Some(namespace),
            || {
                params
                    .cursor
                    .as_deref()
                    .map(|cursor| {
                        history_keyset::decode_cursor(
                            &decode(cursor)?,
                            history_sort_token(order),
                            &cursor_filters,
                        )
                    })
                    .transpose()
            },
        )
        .await?;
    let storage_cursor = match request_cursor {
        Some(cursor) => {
            Some(history_keyset::resolve_on(snapshot.conn().await?.into(), cursor).await?)
        }
        None => None,
    };
    let block_bounds = snapshot.block_bounds();
    let block_window = Some(super::history::bound_history_block_window(
        resolve_history_block_window(snapshot.conn().await?, params).await?,
        &block_bounds,
    ));
    let mut options = history_page_options(params, block_window);
    options.publication_block_bounds = Some(block_bounds.clone());
    options.catalogue_publication = Some(snapshot.history_catalogue_publication());
    options.from_block = params.from_block;
    options.to_block = params.to_block;
    options.contract_address = params.contract_address.clone();
    let summary_mode = events_summary_mode(params, true);

    let storage_page = bigname_storage::load_address_history_page_for_relations_on(
        snapshot.conn().await?,
        address,
        Some(namespace),
        None,
        bigname_storage::HistoryScope::Both,
        storage_cursor.as_ref(),
        params.page_size,
        summary_mode,
        &options,
    )
    .await
    .map_err(|error| map_history_page_error(error, "failed to load events"))?;

    #[cfg(test)]
    bigname_storage::history_anchor_read_test_hooks::run_on(
        snapshot.conn().await?,
        bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterPage,
    )
    .await
    .map_err(|_| super::V2Error::internal_error("failed to run history read test hook"))?;

    // The snapshot holds a pool connection until it ends; the page's context reads follow it.
    let meta = snapshot.finish(state).await?;
    let (data, page) = events_page(
        state,
        params,
        include,
        &cursor_filters,
        summary_mode,
        &storage_page,
        &block_bounds,
    )
    .await?;
    Ok(Json(Envelope {
        data,
        page: Some(page),
        meta,
    }))
}
