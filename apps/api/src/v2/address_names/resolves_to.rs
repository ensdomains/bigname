//! `relation=resolves_to` on `GET /v1/addresses/{address}/names`: the names whose current
//! `addr:<coin_type>` resolver record resolves to the address (feature request F8).
//!
//! The rows come from the `address_records_current` projection rather than
//! `address_names_current`, so this relation is served on its own: it is never mixed with the
//! authority relations, `relation=any` does not include it, and its cursor additionally binds the
//! coin type.

use std::collections::{BTreeMap, BTreeSet};

use axum::Json;
use bigname_storage::{
    AddressNameCurrentEntry, AddressNamesCurrentSortedCursor, AddressRecordCurrentEntry,
    PrimaryNameClaimStatus,
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::v2::{
    AddressNamesDedupe, AddressNamesSort, CursorPayload, Envelope, Meta, Page, QueryParams,
    Relation, SortOrder, V2Error, V2Result, api_error_to_v2,
    cursor::{cursor_value, invalid_cursor_error},
    decode, encode,
    permission_support::{apply_role_summary_support_meta, permission_support_for_resources},
    support::parse_primary_name_coin_type,
};

use super::cursor::{
    ADDRESS_FILTER_KEY, ORDER_FILTER_KEY, cursor_last_item, cursor_sort_value, option_filter,
};
use super::{
    AddressName, address_names_include, build_address_name, build_address_name_role_summary,
    dedupe_to_storage, load_address_name_record_counts, order_to_storage, sort_to_storage,
};
use crate::v2::name_record::load_migrated_at;
use crate::v2::vocab::Authority;

const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const COIN_TYPE_FILTER_KEY: &str = "coin_type";
const DEDUPE_FILTER_KEY: &str = "dedupe";
const Q_FILTER_KEY: &str = "q";
const LOGICAL_NAME_ID_CURSOR_KEY: &str = "logical_name_id";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
const DEFAULT_COIN_TYPE: &str = "60";

/// Why a `resolves_to` row matched: the coin type the caller asked about and the inventory
/// record key that answered it (`addr:<coin_type>`, or `addr:2147483648` when the ENSIP-19
/// default EVM address answered).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNameResolution {
    pub(crate) coin_type: u64,
    pub(crate) record_key: String,
}

/// Parse the request coin type for a `resolves_to` read: decimal, default `60`.
pub(crate) fn parse_resolves_to_coin_type(coin_type: Option<&str>) -> V2Result<(String, u64)> {
    let coin_type = parse_primary_name_coin_type(Some(coin_type.unwrap_or(DEFAULT_COIN_TYPE)))
        .map_err(api_error_to_v2)?;
    let numeric = coin_type
        .parse::<u64>()
        .map_err(|_| V2Error::invalid_input("coin_type must fit in an unsigned 64-bit integer"))?;
    Ok((coin_type, numeric))
}

pub(crate) fn address_name_resolution(
    entry: &AddressRecordCurrentEntry,
    coin_type: u64,
) -> AddressNameResolution {
    AddressNameResolution {
        coin_type,
        record_key: entry.record_key.clone(),
    }
}

/// View a `resolves_to` row through the shared address-name entry shape so the row builders,
/// role summaries, and record counts written for `address_names_current` apply unchanged. The
/// relation facet is supplied by the caller; the projection row has no authority relation.
pub(crate) fn address_name_entry(entry: &AddressRecordCurrentEntry) -> AddressNameCurrentEntry {
    AddressNameCurrentEntry {
        address: entry.address.clone(),
        logical_name_id: entry.logical_name_id.clone(),
        namespace: entry.namespace.clone(),
        canonical_display_name: entry.canonical_display_name.clone(),
        normalized_name: entry.normalized_name.clone(),
        namehash: entry.namehash.clone(),
        surface_binding_id: entry.surface_binding_id,
        resource_id: entry.resource_id,
        token_lineage_id: None,
        binding_kind: entry.binding_kind,
        relations: Vec::new(),
        provenance: entry.provenance.clone(),
        coverage: entry.coverage.clone(),
        chain_positions: entry.chain_positions.clone(),
        canonicality_summary: entry.canonicality_summary.clone(),
        manifest_version: entry.manifest_version,
        last_recomputed_at: entry.last_recomputed_at,
    }
}

pub(super) async fn get_address_resolves_to(
    state: &AppState,
    normalized_address: &str,
    params: &QueryParams,
) -> V2Result<Json<Envelope<Vec<AddressName>>>> {
    let (coin_type, numeric_coin_type) = parse_resolves_to_coin_type(params.coin_type.as_deref())?;
    let include = address_names_include(&params.include)?;
    let include_role_summary = include.role_summary;
    let normalized_q = params
        .q
        .as_deref()
        .map(super::normalize_name_prefix)
        .transpose()?;
    let namespaces = params
        .namespace
        .as_ref()
        .map(|namespace| vec![namespace.clone()]);
    let order = params.order.unwrap_or(SortOrder::Asc);

    let cursor_binding = ResolvesToCursorBinding {
        address: normalized_address,
        namespace: params.namespace.as_deref(),
        coin_type: &coin_type,
        dedupe: params.dedupe,
        q: normalized_q.as_deref(),
        sort: params.sort,
        order,
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            resolves_to_storage_cursor(&payload, &cursor_binding)
        })
        .transpose()?;

    let storage_page = bigname_storage::load_address_records_current_page(
        &state.pool,
        normalized_address,
        &coin_type,
        namespaces.as_deref(),
        dedupe_to_storage(params.dedupe),
        normalized_q.as_deref(),
        sort_to_storage(params.sort),
        order_to_storage(order),
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|error| {
        if storage_cursor.is_some()
            && error
                .to_string()
                .contains("page cursor does not match a grouped entry")
        {
            return invalid_cursor_error();
        }
        V2Error::internal_error(format!(
            "failed to load names resolving to {normalized_address}"
        ))
    })?;

    let entries = storage_page
        .entries
        .iter()
        .map(address_name_entry)
        .collect::<Vec<_>>();
    let logical_name_ids = entries
        .iter()
        .map(|entry| entry.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let name_rows = bigname_storage::load_name_current_by_logical_name_ids(
        &state.pool,
        &logical_name_ids,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load registration summaries for names resolving to {normalized_address}"
        ))
    })?;
    let migrated_logical_name_ids = name_rows
        .values()
        .filter(|row| Authority::from_provenance(&row.provenance) == Some(Authority::EnsV2))
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let migrated_at_by_name = load_migrated_at(&state.pool, &migrated_logical_name_ids).await?;
    let primary_names_by_namespace = load_primary_names_by_namespace(
        &state.pool,
        normalized_address,
        &coin_type,
        entries.iter().map(|entry| entry.namespace.as_str()),
    )
    .await?;
    let role_resource_ids = include_role_summary.then(|| {
        entries
            .iter()
            .map(|entry| entry.resource_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    });
    let permissions_by_resource = if let Some(resource_ids) = role_resource_ids.as_deref() {
        bigname_storage::load_permissions_current_by_resource_ids(&state.pool, resource_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load role summaries for names resolving to {normalized_address}"
                ))
            })?
    } else {
        BTreeMap::new()
    };
    let permission_summaries = if let Some(resource_ids) = role_resource_ids.as_deref() {
        bigname_storage::load_permissions_current_resource_summaries(&state.pool, resource_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load role support for names resolving to {normalized_address}"
                ))
            })?
    } else {
        BTreeMap::new()
    };
    let subname_counts_by_name = if include.counts {
        bigname_storage::load_children_current_summaries(&state.pool, &logical_name_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load subname counts for names resolving to {normalized_address}"
                ))
            })?
            .into_iter()
            .map(|summary| {
                (
                    summary.parent_logical_name_id,
                    u64::try_from(summary.child_count).unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    } else {
        BTreeMap::new()
    };
    let record_counts_by_name = if include_role_summary || include.counts {
        load_address_name_record_counts(&state.pool, &entries, &name_rows)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load record counts for names resolving to {normalized_address}"
                ))
            })?
    } else {
        BTreeMap::new()
    };

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| encode(&resolves_to_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let data = storage_page
        .entries
        .iter()
        .zip(&entries)
        .map(|(record, entry)| {
            let role_summary = if include_role_summary {
                Some(build_address_name_role_summary(
                    permissions_by_resource
                        .get(&entry.resource_id)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                )?)
            } else {
                None
            };
            let mut row = build_address_name(
                entry,
                name_rows.get(&entry.logical_name_id),
                primary_names_by_namespace
                    .get(&entry.namespace)
                    .and_then(Option::as_deref),
                migrated_at_by_name.get(&entry.logical_name_id).cloned(),
                include.counts.then(|| {
                    subname_counts_by_name
                        .get(&entry.logical_name_id)
                        .copied()
                        .unwrap_or_default()
                }),
                record_counts_by_name.get(&entry.logical_name_id).copied(),
                role_summary,
            );
            row.relations = vec![Relation::ResolvesTo];
            row.resolution = Some(address_name_resolution(record, numeric_coin_type));
            Ok(row)
        })
        .collect::<V2Result<Vec<_>>>()?;
    let mut meta = Meta::default();
    if let Some(resource_ids) = role_resource_ids.as_deref() {
        let permission_support =
            permission_support_for_resources(resource_ids, &permission_summaries);
        apply_role_summary_support_meta(&mut meta, permission_support);
    }

    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta,
    }))
}

/// `is_primary` for a `resolves_to` row compares against the requested coin type's claim in the
/// row's namespace, so a name resolving to an address on another EVM chain is marked primary by
/// that chain's claim rather than by the coin-60 claim the authority relations use.
async fn load_primary_names_by_namespace<'a>(
    pool: &sqlx::PgPool,
    address: &str,
    coin_type: &str,
    namespaces: impl Iterator<Item = &'a str>,
) -> V2Result<BTreeMap<String, Option<String>>> {
    let namespaces = namespaces.collect::<BTreeSet<_>>();
    let mut primary_names = BTreeMap::new();
    for namespace in namespaces {
        let primary_name = bigname_storage::load_primary_name_current_snapshot(
            pool, address, namespace, coin_type,
        )
        .await
        .map_err(|_| {
            V2Error::internal_error(format!("failed to load primary name for address {address}"))
        })?
        .filter(|snapshot| snapshot.row.claim_status == PrimaryNameClaimStatus::Success)
        .and_then(|snapshot| {
            snapshot
                .normalized_claim_name
                .map(|name| name.trim().to_owned())
                .filter(|name| !name.is_empty())
        });
        primary_names.insert(namespace.to_owned(), primary_name);
    }
    Ok(primary_names)
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvesToCursorBinding<'a> {
    pub(crate) address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) coin_type: &'a str,
    pub(crate) dedupe: AddressNamesDedupe,
    pub(crate) q: Option<&'a str>,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
}

fn cursor_filters(binding: &ResolvesToCursorBinding<'_>) -> BTreeMap<String, String> {
    BTreeMap::from([
        (ADDRESS_FILTER_KEY.to_owned(), binding.address.to_owned()),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            option_filter(binding.namespace),
        ),
        (
            RELATION_FILTER_KEY.to_owned(),
            Relation::ResolvesTo.as_str().to_owned(),
        ),
        (
            COIN_TYPE_FILTER_KEY.to_owned(),
            binding.coin_type.to_owned(),
        ),
        (
            DEDUPE_FILTER_KEY.to_owned(),
            binding.dedupe.as_str().to_owned(),
        ),
        (Q_FILTER_KEY.to_owned(), option_filter(binding.q)),
        (
            ORDER_FILTER_KEY.to_owned(),
            binding.order.as_str().to_owned(),
        ),
    ])
}

pub(crate) fn resolves_to_cursor_payload(
    cursor: &AddressNamesCurrentSortedCursor,
    binding: &ResolvesToCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        binding.sort.as_str(),
        cursor_filters(binding),
        cursor_last_item(cursor),
        None,
    )
}

pub(crate) fn resolves_to_storage_cursor(
    payload: &CursorPayload,
    binding: &ResolvesToCursorBinding<'_>,
) -> V2Result<AddressNamesCurrentSortedCursor> {
    if payload.sort != binding.sort.as_str() || payload.filters != cursor_filters(binding) {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_cursor_error());
    }
    let sort_value = cursor_sort_value(payload, binding.sort)?;
    let logical_name_id = cursor_value(payload, LOGICAL_NAME_ID_CURSOR_KEY, invalid_cursor_error)?;
    let resource_id = sqlx::types::Uuid::parse_str(&cursor_value(
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

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_storage::AddressNamesCurrentSortedCursorValue;
    use sqlx::types::Uuid;

    fn binding(coin_type: &'static str) -> ResolvesToCursorBinding<'static> {
        ResolvesToCursorBinding {
            address: "0x00000000000000000000000000000000000000aa",
            namespace: None,
            coin_type,
            dedupe: AddressNamesDedupe::Name,
            q: None,
            sort: AddressNamesSort::Name,
            order: SortOrder::Asc,
        }
    }

    #[test]
    fn resolves_to_cursor_binds_relation_and_coin_type() {
        let cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Name("alice.eth".to_owned()),
            logical_name_id: "ens:alice.eth".to_owned(),
            resource_id: Uuid::from_u128(0x1234),
        };
        let payload = resolves_to_cursor_payload(&cursor, &binding("60"));
        assert_eq!(payload.filters["relation"], "resolves_to");
        assert_eq!(payload.filters["coin_type"], "60");
        assert_eq!(
            resolves_to_storage_cursor(&payload, &binding("60")).expect("cursor must decode"),
            cursor
        );
        assert!(resolves_to_storage_cursor(&payload, &binding("2147483658")).is_err());

        // An authority-relation cursor for the same address never resumes a resolves_to page.
        let authority = crate::v2::address_names::address_names_cursor_payload(
            &cursor,
            &crate::v2::address_names::AddressNamesCursorBinding {
                address: "0x00000000000000000000000000000000000000aa",
                namespace: None,
                relation: None,
                dedupe: AddressNamesDedupe::Name,
                q: None,
                authority: None,
                sort: AddressNamesSort::Name,
                order: SortOrder::Asc,
            },
        );
        assert!(resolves_to_storage_cursor(&authority, &binding("60")).is_err());
    }

    #[test]
    fn resolves_to_coin_type_defaults_to_sixty() {
        assert_eq!(
            parse_resolves_to_coin_type(None).expect("default must parse"),
            ("60".to_owned(), 60)
        );
        assert_eq!(
            parse_resolves_to_coin_type(Some("2147483658")).expect("coin type must parse"),
            ("2147483658".to_owned(), 2_147_483_658)
        );
        assert!(parse_resolves_to_coin_type(Some("-1")).is_err());
        assert!(parse_resolves_to_coin_type(Some("abc")).is_err());
    }
}
