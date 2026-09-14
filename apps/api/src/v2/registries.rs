use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    ChainPositions, NameCurrentRow, RegistryContractRow, RegistryCreationBasis,
    RegistryReferenceKeysetCursor, SelectedSnapshot, SubregistryPointer,
};
use serde::{Deserialize, Serialize};

use super::cursor::{cursor_value, invalid_cursor_error};
use super::support::parse_evm_address;
use super::{
    CursorPayload, Envelope, Page, QueryParamAllowlist, SnapshotReadResource, StrictQueryParams,
    V2Error, V2Result, api_error_to_v2, decode, encode, format_timestamp, parse_numeric_chain_id,
    product_history_event_kinds, resolve_v2_snapshot_for, resolver_snapshot_scope, slug_to_numeric,
    snapshot_meta,
};
use crate::AppState;

#[path = "registries/labels.rs"]
mod labels;
#[path = "registries/role_counts.rs"]
mod role_counts;
pub(crate) use labels::get_registry_labels;

const REFERENCED_BY_SORT: &str = "display_name_asc";
const CHAIN_ID_FILTER_KEY: &str = "chain_id";
const REGISTRY_FILTER_KEY: &str = "registry";
const DISPLAY_NAME_CURSOR_KEY: &str = "display_name";
const NAME_ID_CURSOR_KEY: &str = "name_id";

pub(crate) struct RegistryQueryParams;

impl QueryParamAllowlist for RegistryQueryParams {
    const ALLOWED: &'static [&'static str] = &["include", "at", "finality", "cursor", "page_size"];
}

pub(crate) type RegistryQuery = StrictQueryParams<RegistryQueryParams>;

/// A registry contract reference in product vocabulary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RegistryRef {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
}

/// The name a registry serves, or one name whose subregistry pointer targets it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RegistryName {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RegistryCounts {
    pub(crate) labels: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) roles: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) events: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ReferencedBy {
    pub(crate) data: Vec<RegistryName>,
    pub(crate) page: Page,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RegistryOverview {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
    pub(crate) name: Option<RegistryName>,
    pub(crate) parent_registry: Option<RegistryRef>,
    pub(crate) created_block_number: Option<i64>,
    pub(crate) created_at: Option<String>,
    pub(crate) created_transaction_hash: Option<String>,
    pub(crate) created_basis: String,
    pub(crate) counts: RegistryCounts,
    pub(crate) referenced_by: ReferencedBy,
}

pub(crate) async fn get_registry(
    Path((chain_id, address)): Path<(String, String)>,
    params: RegistryQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<RegistryOverview>>> {
    let params = params.into_inner();
    let (numeric_chain_id, chain_id_slug) = parse_numeric_chain_id(&chain_id)?;
    let normalized_address = parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let include_event_count = registry_include_counts(&params.include)?;
    let collection =
        super::collection_snapshot::CollectionSnapshot::capture(&state, params.cursor.as_deref())
            .await?;

    let scope = resolver_snapshot_scope(chain_id_slug)?;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::Registry,
    )
    .await?;
    let as_of_block = snapshot_block_for_chain(&selected_snapshot, chain_id_slug);
    let selected_token = super::encode_at_token(&selected_snapshot);

    let registry = bigname_storage::load_registry_contract(
        &state.pool,
        chain_id_slug,
        &normalized_address,
        as_of_block,
    )
    .await
    .map_err(|_| internal_error(chain_id_slug, &normalized_address))?
    .ok_or_else(|| {
        V2Error::not_found(format!(
            "registry {normalized_address} was not found on chain {numeric_chain_id}"
        ))
    })?;
    let serving = bigname_storage::load_registry_serving_pointer(
        &state.pool,
        chain_id_slug,
        &normalized_address,
        as_of_block,
    )
    .await
    .map_err(|_| internal_error(chain_id_slug, &normalized_address))?;

    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let mut payload = decode(cursor)?;
            collection.validate_cursor(&payload)?;
            if payload.filters.remove("at").as_deref() != Some(selected_token.as_str()) {
                return Err(invalid_cursor_error());
            }
            referenced_by_storage_cursor(&payload, numeric_chain_id, &normalized_address)
        })
        .transpose()?;
    let references = bigname_storage::load_registry_references_page(
        &state.pool,
        chain_id_slug,
        &normalized_address,
        as_of_block,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|_| internal_error(chain_id_slug, &normalized_address))?;

    let labels = match serving.as_ref() {
        Some(pointer) => bigname_storage::count_registry_children_current(
            &state.pool,
            &pointer.logical_name_id,
            &normalized_address,
        )
        .await
        .map_err(|_| internal_error(chain_id_slug, &normalized_address))?,
        None => 0,
    };
    let roles = if include_event_count {
        Some(
            role_counts::registry_role_count(
                &state.pool,
                chain_id_slug,
                &normalized_address,
                as_of_block,
            )
            .await?,
        )
    } else {
        None
    };
    let events = if include_event_count {
        Some(
            bigname_storage::count_contract_events(
                &state.pool,
                chain_id_slug,
                &normalized_address,
                &product_history_event_kinds(),
                as_of_block,
            )
            .await
            .map_err(|_| internal_error(chain_id_slug, &normalized_address))?,
        )
    } else {
        None
    };

    let next_cursor = references.next_cursor.as_ref().map(|cursor| {
        let mut payload =
            referenced_by_cursor_payload(cursor, numeric_chain_id, &normalized_address);
        payload
            .filters
            .insert("at".to_owned(), selected_token.clone());
        encode(&collection.bind_cursor(payload))
    });
    let referenced_by = ReferencedBy {
        page: Page {
            cursor: params.cursor.clone(),
            next_cursor: next_cursor.clone(),
            page_size: params.page_size,
            total_count: None,
            has_more: next_cursor.is_some(),
        },
        data: references.rows.iter().map(registry_name).collect(),
    };
    let current_meta = collection.finish(&state).await?;
    let selected_meta = snapshot_meta(&selected_snapshot)?;
    let is_current = current_meta
        .as_of
        .as_ref()
        .and_then(|positions| positions.get(&numeric_chain_id.to_string()))
        == selected_meta
            .as_of
            .as_ref()
            .and_then(|positions| positions.get(&numeric_chain_id.to_string()));
    let mut data = build_registry_overview(
        registry,
        numeric_chain_id,
        serving.as_ref(),
        labels,
        events,
        referenced_by,
    );
    data.counts.roles = roles;
    if !is_current {
        data.counts.labels = None;
    }
    Ok(Json(Envelope {
        data,
        page: None,
        meta: selected_meta,
    }))
}

fn build_registry_overview(
    registry: RegistryContractRow,
    chain_id: u64,
    serving: Option<&SubregistryPointer>,
    labels: i64,
    events: Option<i64>,
    referenced_by: ReferencedBy,
) -> RegistryOverview {
    let created = registry.created;
    RegistryOverview {
        chain_id,
        address: registry.address,
        name: serving.map(registry_name),
        parent_registry: serving.and_then(|pointer| {
            pointer.registry.as_ref().map(|address| RegistryRef {
                chain_id,
                address: address.clone(),
            })
        }),
        created_block_number: created.block_number,
        created_at: created.block_timestamp.map(format_timestamp),
        created_transaction_hash: created.transaction_hash,
        created_basis: created_basis(created.basis).to_owned(),
        counts: RegistryCounts {
            labels: u64::try_from(labels).ok(),
            roles: None,
            events: events.map(|count| u64::try_from(count).unwrap_or_default()),
        },
        referenced_by,
    }
}

const fn created_basis(basis: RegistryCreationBasis) -> &'static str {
    match basis {
        RegistryCreationBasis::Announcement => "registry_created",
        RegistryCreationBasis::SubregistryPointer => "subregistry_pointer",
        RegistryCreationBasis::Declared => "declared",
    }
}

fn registry_name(pointer: &SubregistryPointer) -> RegistryName {
    RegistryName {
        name: pointer.display_name.clone(),
        display_name: pointer.display_name.clone(),
        namespace: pointer.namespace.clone(),
        namehash: pointer.namehash.clone(),
    }
}

/// The current subregistry reference of each requested name, keyed by logical identity.
/// Names without a current pointer are absent.
pub(crate) async fn load_subregistry_refs(
    pool: &sqlx::PgPool,
    logical_name_ids: &[String],
    as_of_block: Option<i64>,
) -> V2Result<BTreeMap<String, RegistryRef>> {
    let pointers =
        bigname_storage::load_subregistry_pointers_for_names(pool, logical_name_ids, as_of_block)
            .await
            .map_err(|error| {
                tracing::error!(error = ?error, "failed to load subregistry pointers");
                V2Error::internal_error("failed to load subregistry pointers")
            })?;
    Ok(pointers
        .into_iter()
        .filter_map(|(logical_name_id, pointer)| {
            subregistry_ref(&pointer).map(|reference| (logical_name_id, reference))
        })
        .collect())
}

fn subregistry_ref(pointer: &SubregistryPointer) -> Option<RegistryRef> {
    let address = pointer.subregistry.clone()?;
    let chain_id = slug_to_numeric(&pointer.chain_id)?;
    Some(RegistryRef { chain_id, address })
}

/// The selected block for one storage chain id, used to bound event-derived reads to the
/// served position.
pub(crate) fn snapshot_block_for_chain(selected: &SelectedSnapshot, chain_id: &str) -> Option<i64> {
    selected
        .chain_positions
        .as_map()
        .values()
        .find(|position| position.chain_id == chain_id)
        .map(|position| position.block_number)
}

/// The storage chain id a projected name row was published from.
pub(crate) fn name_chain_id(row: &NameCurrentRow) -> Option<String> {
    ChainPositions::from_value(&row.chain_positions)
        .ok()?
        .as_map()
        .values()
        .next()
        .map(|position| position.chain_id.clone())
}

fn registry_include_counts(include: &[String]) -> V2Result<bool> {
    let mut include_counts = false;
    for value in include {
        match value.as_str() {
            "counts" => include_counts = true,
            _ => return Err(V2Error::invalid_input("include must contain only counts")),
        }
    }
    Ok(include_counts)
}

fn internal_error(chain_id: &str, address: &str) -> V2Error {
    V2Error::internal_error(format!(
        "failed to load registry data for chain_id {chain_id} address {address}"
    ))
}

fn registry_filter_value(chain_id: u64, address: &str) -> String {
    format!("{chain_id}:{address}")
}

pub(crate) fn referenced_by_cursor_payload(
    cursor: &RegistryReferenceKeysetCursor,
    chain_id: u64,
    address: &str,
) -> CursorPayload {
    CursorPayload::new(
        REFERENCED_BY_SORT,
        BTreeMap::from([
            (CHAIN_ID_FILTER_KEY.to_owned(), chain_id.to_string()),
            (
                REGISTRY_FILTER_KEY.to_owned(),
                registry_filter_value(chain_id, address),
            ),
        ]),
        BTreeMap::from([
            (
                DISPLAY_NAME_CURSOR_KEY.to_owned(),
                cursor.display_name.clone(),
            ),
            (
                NAME_ID_CURSOR_KEY.to_owned(),
                cursor.logical_name_id.clone(),
            ),
        ]),
        None,
    )
}

pub(crate) fn referenced_by_storage_cursor(
    payload: &CursorPayload,
    chain_id: u64,
    address: &str,
) -> V2Result<RegistryReferenceKeysetCursor> {
    if payload.sort != REFERENCED_BY_SORT
        || payload.filters.len() != 2
        || payload.filters.get(CHAIN_ID_FILTER_KEY).map(String::as_str)
            != Some(chain_id.to_string().as_str())
        || payload.filters.get(REGISTRY_FILTER_KEY).map(String::as_str)
            != Some(registry_filter_value(chain_id, address).as_str())
        || payload.last_item.len() != 2
    {
        return Err(invalid_cursor_error());
    }
    Ok(RegistryReferenceKeysetCursor {
        display_name: cursor_value(payload, DISPLAY_NAME_CURSOR_KEY, invalid_cursor_error)?,
        logical_name_id: cursor_value(payload, NAME_ID_CURSOR_KEY, invalid_cursor_error)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGISTRY: &str = "0x00000000000000000000000000000000000000ab";

    fn sample_cursor() -> RegistryReferenceKeysetCursor {
        RegistryReferenceKeysetCursor {
            display_name: "alpha.eth".to_owned(),
            logical_name_id: "ens:0xalpha".to_owned(),
        }
    }

    #[test]
    fn referenced_by_cursor_round_trips_and_binds_registry() {
        let payload = referenced_by_cursor_payload(&sample_cursor(), 1, REGISTRY);
        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("chain_id".to_owned(), "1".to_owned()),
                ("registry".to_owned(), format!("1:{REGISTRY}")),
            ])
        );
        assert_eq!(
            referenced_by_storage_cursor(&payload, 1, REGISTRY).expect("cursor must decode"),
            sample_cursor()
        );
        assert!(referenced_by_storage_cursor(&payload, 8453, REGISTRY).is_err());
        assert!(
            referenced_by_storage_cursor(&payload, 1, "0x00000000000000000000000000000000000000ac")
                .is_err()
        );

        let mut wrong_sort = payload.clone();
        wrong_sort.sort = "wrong".to_owned();
        assert!(referenced_by_storage_cursor(&wrong_sort, 1, REGISTRY).is_err());
    }

    #[test]
    fn subregistry_ref_drops_cleared_pointers_and_unknown_chains() {
        let pointer = SubregistryPointer {
            logical_name_id: "ens:0xalpha".to_owned(),
            namespace: "ens".to_owned(),
            display_name: "alpha.eth".to_owned(),
            namehash: "0xalpha".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            subregistry: Some(REGISTRY.to_owned()),
            registry: None,
            block_number: Some(1),
            block_hash: None,
            transaction_hash: None,
            block_timestamp: None,
        };
        assert_eq!(
            subregistry_ref(&pointer),
            Some(RegistryRef {
                chain_id: 1,
                address: REGISTRY.to_owned(),
            })
        );
        let cleared = SubregistryPointer {
            subregistry: None,
            ..pointer.clone()
        };
        assert_eq!(subregistry_ref(&cleared), None);
        let unknown_chain = SubregistryPointer {
            chain_id: "unknown".to_owned(),
            ..pointer
        };
        assert_eq!(subregistry_ref(&unknown_chain), None);
    }
}
