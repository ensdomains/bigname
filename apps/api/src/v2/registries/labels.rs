use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentSortValue, RegistryLabelOwnerFilter,
};
use serde::Serialize;

use super::super::cursor::{cursor_value, invalid_cursor_error};
use super::super::subnames::{Subname, build_subname};
use super::super::support::parse_evm_address;
use super::super::{
    CursorPayload, Envelope, Page, QueryParamAllowlist, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2, decode, encode, parse_numeric_chain_id, validate_latest_collection_selectors,
};
use super::load_subregistry_refs;
use crate::AppState;

const LABELS_SORT: &str = "display_name_asc";
const REGISTRY_FILTER_KEY: &str = "registry";
const OWNER_FILTER_KEY: &str = "owner";
const EXCLUDE_OWNER_FILTER_KEY: &str = "exclude_owner";
const DISPLAY_NAME_CURSOR_KEY: &str = "display_name";
const CHILD_ID_CURSOR_KEY: &str = "child_id";

pub(crate) struct RegistryLabelsQueryParams;

impl QueryParamAllowlist for RegistryLabelsQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "at",
        "finality",
        "include",
        "owner",
        "exclude_owner",
        "cursor",
        "page_size",
    ];
}

pub(crate) type RegistryLabelsQuery = StrictQueryParams<RegistryLabelsQueryParams>;

#[derive(Serialize)]
pub(crate) struct RegistryLabel {
    #[serde(flatten)]
    name: Subname,
    #[serde(skip_serializing_if = "Option::is_none")]
    role_holder_count: Option<u64>,
}

/// The labels one ENSv2 registry currently holds: the direct subnames of the name it serves
/// whose registration that registry emitted, in the subname row shape. `owner` keeps the labels
/// served with that owner; `exclude_owner` keeps every other label, the ownerless ones included.
/// Both narrow the collection before paging and `total_count`.
pub(crate) async fn get_registry_labels(
    Path((chain_id, address)): Path<(String, String)>,
    params: RegistryLabelsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<RegistryLabel>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let (numeric_chain_id, chain_id_slug) = parse_numeric_chain_id(&chain_id)?;
    let normalized_address = parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let include_counts = labels_include_counts(&params.include)?;
    let owner = labels_owner_filter(params.owner.as_deref(), params.exclude_owner.as_deref())?;
    let mut collection =
        super::super::collection_snapshot::CollectionSnapshot::capture_for_namespace(
            &state,
            params.cursor.as_deref(),
            Some("ens"),
        )
        .await?;
    let selected = super::super::resolve_v2_snapshot_for(
        &state.pool,
        &super::super::resolver_snapshot_scope(chain_id_slug)?,
        None,
        params.finality,
        super::super::SnapshotReadResource::Registry,
    )
    .await?;
    let as_of_block = super::snapshot_block_for_chain(&selected, chain_id_slug);
    let internal_error = || {
        V2Error::internal_error(format!(
            "failed to load labels for registry {normalized_address} on chain {chain_id_slug}"
        ))
    };

    bigname_storage::load_registry_contract(
        collection.conn().await?,
        chain_id_slug,
        &normalized_address,
        as_of_block,
    )
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
            labels_storage_cursor(&payload, numeric_chain_id, &normalized_address, owner)
        })
        .transpose()?;
    let serving = bigname_storage::load_registry_serving_pointer(
        collection.conn().await?,
        chain_id_slug,
        &normalized_address,
        as_of_block,
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
            meta: collection.finish(&state).await?,
        }));
    };

    let storage_page = bigname_storage::load_registry_children_current_page(
        collection.conn().await?,
        &serving.logical_name_id,
        &normalized_address,
        owner,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(crate::v2::name_rows_error(
        crate::v2::SnapshotReadResource::Registry,
        |_| internal_error(),
    ))?;
    let child_ids = storage_page
        .rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();
    let child_name_rows = bigname_storage::load_name_current_by_logical_name_ids(
        collection.conn().await?,
        &child_ids,
    )
    .await
    .map_err(crate::v2::name_rows_error(
        crate::v2::SnapshotReadResource::Registry,
        |_| internal_error(),
    ))?;
    let child_summaries = if include_counts {
        bigname_storage::load_children_current_summaries(collection.conn().await?, &child_ids)
            .await
            .map_err(crate::v2::name_rows_error(
                crate::v2::SnapshotReadResource::Registry,
                |_| internal_error(),
            ))?
            .into_iter()
            .map(|summary| (summary.parent_logical_name_id.clone(), summary))
            .collect()
    } else {
        BTreeMap::new()
    };
    let role_counts = if include_counts {
        let resources = child_name_rows
            .values()
            .filter_map(|row| row.resource_id)
            .collect::<Vec<_>>();
        super::role_counts::label_role_counts(
            collection.conn().await?,
            chain_id_slug,
            &normalized_address,
            &resources,
            as_of_block,
        )
        .await?
    } else {
        BTreeMap::new()
    };
    let mut subregistries =
        load_subregistry_refs(collection.conn().await?, &child_ids, as_of_block).await?;

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&labels_cursor_payload(
            cursor,
            numeric_chain_id,
            &normalized_address,
            owner,
        ))
    });
    let mut data = storage_page
        .rows
        .iter()
        .map(|row| {
            let mut subname = build_subname(
                row,
                child_name_rows.get(&row.child_logical_name_id),
                child_summaries.get(&row.child_logical_name_id),
                include_counts,
            )?;
            subname.subregistry = subregistries.remove(&row.child_logical_name_id);
            let role_holder_count = include_counts.then(|| {
                child_name_rows
                    .get(&row.child_logical_name_id)
                    .and_then(|name| name.resource_id)
                    .and_then(|resource| role_counts.get(&resource).copied())
                    .unwrap_or_default()
            });
            Ok(RegistryLabel {
                name: subname,
                role_holder_count,
            })
        })
        .collect::<V2Result<Vec<_>>>()?;
    crate::v2::name_record::fill_children_ens_v1(
        collection.conn().await?,
        Some(chain_id_slug),
        &storage_page.rows,
        &child_name_rows,
        data.iter_mut().map(|label| label.name.ens_v1.as_mut()),
    )
    .await?;
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor: next_cursor.clone(),
            page_size: params.page_size,
            total_count: u64::try_from(storage_page.label_count).ok(),
            has_more: next_cursor.is_some(),
        }),
        meta: collection.finish(&state).await?,
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

/// The owner filter of a labels request; `owner` and `exclude_owner` together are refused.
fn labels_owner_filter<'a>(
    owner: Option<&'a str>,
    exclude_owner: Option<&'a str>,
) -> V2Result<Option<RegistryLabelOwnerFilter<'a>>> {
    match (owner, exclude_owner) {
        (Some(_), Some(_)) => Err(V2Error::invalid_input(
            "owner and exclude_owner cannot be combined",
        )),
        (Some(owner), None) => Ok(Some(RegistryLabelOwnerFilter::Owner(owner))),
        (None, Some(owner)) => Ok(Some(RegistryLabelOwnerFilter::ExcludeOwner(owner))),
        (None, None) => Ok(None),
    }
}

fn registry_filter_value(chain_id: u64, address: &str) -> String {
    format!("{chain_id}:{address}")
}

/// Everything a labels cursor binds besides its position: the registry, and the owner filter
/// when there is one, so an unfiltered cursor keeps its earlier shape.
fn labels_cursor_filters(
    chain_id: u64,
    address: &str,
    owner: Option<RegistryLabelOwnerFilter<'_>>,
) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([(
        REGISTRY_FILTER_KEY.to_owned(),
        registry_filter_value(chain_id, address),
    )]);
    match owner {
        Some(RegistryLabelOwnerFilter::Owner(owner)) => {
            filters.insert(OWNER_FILTER_KEY.to_owned(), owner.to_owned());
        }
        Some(RegistryLabelOwnerFilter::ExcludeOwner(owner)) => {
            filters.insert(EXCLUDE_OWNER_FILTER_KEY.to_owned(), owner.to_owned());
        }
        None => {}
    }
    filters
}

pub(crate) fn labels_cursor_payload(
    cursor: &ChildrenCurrentKeysetCursor,
    chain_id: u64,
    address: &str,
    owner: Option<RegistryLabelOwnerFilter<'_>>,
) -> CursorPayload {
    CursorPayload::new(
        LABELS_SORT,
        labels_cursor_filters(chain_id, address, owner),
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
    owner: Option<RegistryLabelOwnerFilter<'_>>,
) -> V2Result<ChildrenCurrentKeysetCursor> {
    if payload.sort != LABELS_SORT
        || payload.filters != labels_cursor_filters(chain_id, address, owner)
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
        let payload = labels_cursor_payload(&cursor, 1, REGISTRY, None);
        assert_eq!(
            payload.filters,
            BTreeMap::from([("registry".to_owned(), format!("1:{REGISTRY}"))])
        );
        assert_eq!(
            labels_storage_cursor(&payload, 1, REGISTRY, None).expect("cursor must decode"),
            cursor
        );
        assert!(labels_storage_cursor(&payload, 8453, REGISTRY, None).is_err());
        assert!(
            labels_storage_cursor(
                &payload,
                1,
                "0x00000000000000000000000000000000000000ac",
                None
            )
            .is_err()
        );
    }

    #[test]
    fn labels_cursor_binds_the_owner_filter() {
        const OWNER: &str = "0x00000000000000000000000000000000000000a1";
        const OTHER: &str = "0x00000000000000000000000000000000000000a2";
        let cursor = ChildrenCurrentKeysetCursor {
            sort_value: ChildrenCurrentSortValue::Name,
            canonical_display_name: "one.alpha.eth".to_owned(),
            child_logical_name_id: "ens:0xone".to_owned(),
        };
        let filters = [
            None,
            Some(RegistryLabelOwnerFilter::Owner(OWNER)),
            Some(RegistryLabelOwnerFilter::Owner(OTHER)),
            Some(RegistryLabelOwnerFilter::ExcludeOwner(OWNER)),
            Some(RegistryLabelOwnerFilter::ExcludeOwner(OTHER)),
        ];
        for issued in filters {
            let payload = labels_cursor_payload(&cursor, 1, REGISTRY, issued);
            for presented in filters {
                let decoded = labels_storage_cursor(&payload, 1, REGISTRY, presented);
                if issued == presented {
                    assert_eq!(decoded.expect("the same filter continues"), cursor);
                } else {
                    assert!(decoded.is_err(), "{issued:?} cursor under {presented:?}");
                }
            }
        }
        assert_eq!(
            labels_cursor_payload(
                &cursor,
                1,
                REGISTRY,
                Some(RegistryLabelOwnerFilter::ExcludeOwner(OWNER))
            )
            .filters,
            BTreeMap::from([
                ("exclude_owner".to_owned(), OWNER.to_owned()),
                ("registry".to_owned(), format!("1:{REGISTRY}")),
            ])
        );
    }

    #[test]
    fn labels_owner_filters_do_not_combine() {
        const OWNER: &str = "0x00000000000000000000000000000000000000a1";
        assert_eq!(labels_owner_filter(None, None).expect("no filter"), None);
        assert_eq!(
            labels_owner_filter(Some(OWNER), None).expect("owner"),
            Some(RegistryLabelOwnerFilter::Owner(OWNER))
        );
        assert_eq!(
            labels_owner_filter(None, Some(OWNER)).expect("exclude_owner"),
            Some(RegistryLabelOwnerFilter::ExcludeOwner(OWNER))
        );
        assert!(labels_owner_filter(Some(OWNER), Some(OWNER)).is_err());
    }
}
