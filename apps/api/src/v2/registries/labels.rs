use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{ChildrenCurrentKeysetCursor, ChildrenCurrentSortValue};

use super::super::cursor::{cursor_value, invalid_cursor_error};
use super::super::subnames::{Subname, build_subname};
use super::super::support::parse_evm_address;
use super::super::{
    CursorPayload, Envelope, Meta, Page, QueryParamAllowlist, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2, decode, encode, parse_numeric_chain_id, validate_latest_collection_selectors,
};
use super::load_subregistry_refs;
use crate::AppState;

const LABELS_SORT: &str = "display_name_asc";
const REGISTRY_FILTER_KEY: &str = "registry";
const DISPLAY_NAME_CURSOR_KEY: &str = "display_name";
const CHILD_ID_CURSOR_KEY: &str = "child_id";

pub(crate) struct RegistryLabelsQueryParams;

impl QueryParamAllowlist for RegistryLabelsQueryParams {
    const ALLOWED: &'static [&'static str] = &["at", "finality", "include", "cursor", "page_size"];
}

pub(crate) type RegistryLabelsQuery = StrictQueryParams<RegistryLabelsQueryParams>;

/// The labels one ENSv2 registry currently holds: the direct subnames of the name it serves
/// whose registration that registry emitted, in the subname row shape.
pub(crate) async fn get_registry_labels(
    Path((chain_id, address)): Path<(String, String)>,
    params: RegistryLabelsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Subname>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let (numeric_chain_id, chain_id_slug) = parse_numeric_chain_id(&chain_id)?;
    let normalized_address = parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let include_counts = labels_include_counts(&params.include)?;
    let internal_error = || {
        V2Error::internal_error(format!(
            "failed to load labels for registry {normalized_address} on chain {chain_id_slug}"
        ))
    };

    bigname_storage::load_registry_contract(&state.pool, chain_id_slug, &normalized_address, None)
        .await
        .map_err(|_| internal_error())?
        .ok_or_else(|| {
            V2Error::not_found(format!(
                "registry {normalized_address} was not found on chain {numeric_chain_id}"
            ))
        })?;
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            labels_storage_cursor(&payload, numeric_chain_id, &normalized_address)
        })
        .transpose()?;
    let serving = bigname_storage::load_registry_serving_pointer(
        &state.pool,
        chain_id_slug,
        &normalized_address,
        None,
    )
    .await
    .map_err(|_| internal_error())?;
    let Some(serving) = serving else {
        return Ok(Json(Envelope {
            data: Vec::new(),
            page: Some(Page {
                cursor: params.cursor.clone(),
                next_cursor: None,
                page_size: params.page_size,
                total_count: Some(0),
                has_more: false,
            }),
            meta: Meta::default(),
        }));
    };

    let storage_page = bigname_storage::load_registry_children_current_page(
        &state.pool,
        &serving.logical_name_id,
        &normalized_address,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|_| internal_error())?;
    let child_ids = storage_page
        .rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();
    let child_name_rows =
        bigname_storage::load_name_current_by_logical_name_ids(&state.pool, &child_ids)
            .await
            .map_err(|_| internal_error())?;
    let child_summaries = if include_counts {
        bigname_storage::load_children_current_summaries(&state.pool, &child_ids)
            .await
            .map_err(|_| internal_error())?
            .into_iter()
            .map(|summary| (summary.parent_logical_name_id.clone(), summary))
            .collect()
    } else {
        BTreeMap::new()
    };
    let mut subregistries = load_subregistry_refs(&state.pool, &child_ids, None).await?;

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&labels_cursor_payload(
            cursor,
            numeric_chain_id,
            &normalized_address,
        ))
    });
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
            next_cursor: next_cursor.clone(),
            page_size: params.page_size,
            total_count: u64::try_from(storage_page.label_count).ok(),
            has_more: next_cursor.is_some(),
        }),
        meta: Meta::default(),
    }))
}

fn labels_include_counts(include: &[String]) -> V2Result<bool> {
    let mut include_counts = false;
    for value in include {
        match value.as_str() {
            "counts" => include_counts = true,
            _ => return Err(V2Error::invalid_input("include must contain only counts")),
        }
    }
    Ok(include_counts)
}

fn registry_filter_value(chain_id: u64, address: &str) -> String {
    format!("{chain_id}:{address}")
}

pub(crate) fn labels_cursor_payload(
    cursor: &ChildrenCurrentKeysetCursor,
    chain_id: u64,
    address: &str,
) -> CursorPayload {
    CursorPayload::new(
        LABELS_SORT,
        BTreeMap::from([(
            REGISTRY_FILTER_KEY.to_owned(),
            registry_filter_value(chain_id, address),
        )]),
        BTreeMap::from([
            (
                DISPLAY_NAME_CURSOR_KEY.to_owned(),
                cursor.canonical_display_name.clone(),
            ),
            (
                CHILD_ID_CURSOR_KEY.to_owned(),
                cursor.child_logical_name_id.clone(),
            ),
        ]),
        None,
    )
}

pub(crate) fn labels_storage_cursor(
    payload: &CursorPayload,
    chain_id: u64,
    address: &str,
) -> V2Result<ChildrenCurrentKeysetCursor> {
    if payload.sort != LABELS_SORT
        || payload.filters.len() != 1
        || payload.filters.get(REGISTRY_FILTER_KEY).map(String::as_str)
            != Some(registry_filter_value(chain_id, address).as_str())
        || payload.last_item.len() != 2
    {
        return Err(invalid_cursor_error());
    }
    Ok(ChildrenCurrentKeysetCursor {
        sort_value: ChildrenCurrentSortValue::Name,
        canonical_display_name: cursor_value(
            payload,
            DISPLAY_NAME_CURSOR_KEY,
            invalid_cursor_error,
        )?,
        child_logical_name_id: cursor_value(payload, CHILD_ID_CURSOR_KEY, invalid_cursor_error)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGISTRY: &str = "0x00000000000000000000000000000000000000ab";

    #[test]
    fn labels_cursor_round_trips_and_binds_registry() {
        let cursor = ChildrenCurrentKeysetCursor {
            sort_value: ChildrenCurrentSortValue::Name,
            canonical_display_name: "one.alpha.eth".to_owned(),
            child_logical_name_id: "ens:0xone".to_owned(),
        };
        let payload = labels_cursor_payload(&cursor, 1, REGISTRY);
        assert_eq!(
            payload.filters,
            BTreeMap::from([("registry".to_owned(), format!("1:{REGISTRY}"))])
        );
        assert_eq!(
            labels_storage_cursor(&payload, 1, REGISTRY).expect("cursor must decode"),
            cursor
        );
        assert!(labels_storage_cursor(&payload, 8453, REGISTRY).is_err());
        assert!(
            labels_storage_cursor(&payload, 1, "0x00000000000000000000000000000000000000ac")
                .is_err()
        );
    }
}
