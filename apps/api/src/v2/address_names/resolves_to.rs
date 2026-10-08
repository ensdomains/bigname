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
    AddressNamesCurrentSortedCursor, AddressRecordCurrentEntry, PrimaryNameClaimStatus,
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::v2::{
    AddressNamesDedupe, AddressNamesSort, CursorPayload, Envelope, Page, QueryParams, Relation,
    SnapshotReadResource::Resource,
    SortOrder, V2Error, V2Result, api_error_to_v2,
    cursor::{cursor_value, invalid_cursor_error},
    decode, encode, name_rows_error,
    permission_support::{apply_role_summary_support_meta, permission_support_for_resources},
    restrictions::ResourceRestrictions,
    support::parse_primary_name_coin_type,
};

use super::cursor::{
    ADDRESS_FILTER_KEY, ORDER_FILTER_KEY, authority_filter_value, cursor_last_item,
    cursor_sort_value, insert_match_filter, insert_parent_filter, option_filter,
};
use super::resolves_to_evm::{
    ResolvesToCoins, ResolvesToMatches, ResolvesToRow, evm_primary_flags, parse_resolves_to_coins,
    reject_rows_past_coin_type_limit,
};
use super::{
    AddressName, address_names_include, build_address_name_role_summary, dedupe_to_storage,
    load_address_name_record_counts, name_registration_fields, order_to_storage, sort_to_storage,
};
use crate::v2::name_filter::NameMatch;
use crate::v2::name_record::{ens_v1_of_row, load_migrated_at, registration_id};
use crate::v2::vocab::{Authority, AuthoritySet};

const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const COIN_TYPE_FILTER_KEY: &str = "coin_type";
const DEDUPE_FILTER_KEY: &str = "dedupe";
const Q_FILTER_KEY: &str = "q";
const AUTHORITY_FILTER_KEY: &str = "authority";
const LOGICAL_NAME_ID_CURSOR_KEY: &str = "logical_name_id";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
const DEFAULT_COIN_TYPE: &str = "60";

/// Why a `resolves_to` row matched: the coin type the caller asked about and the inventory
/// record key that answered it (`addr:<coin_type>`, or `addr:2147483648` when the ENSIP-19
/// default EVM address answered; upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L10 @
/// ens_v1@91c966f).
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

pub(super) async fn get_address_resolves_to(
    state: &AppState,
    normalized_address: &str,
    params: &QueryParams,
    parent: Option<&str>,
) -> V2Result<Json<Envelope<Vec<AddressName>>>> {
    let coins = parse_resolves_to_coins(params.coin_type.as_deref())?;
    if params.is_migrated.is_some() {
        return Err(V2Error::invalid_input(
            "is_migrated requires an ownership relation",
        ));
    }
    let include = address_names_include(&params.include)?;
    if include.total_count {
        return Err(V2Error::invalid_input(
            "include=total_count requires an ownership relation",
        ));
    }
    let include_role_summary = include.role_summary;
    let normalized_q = params
        .q
        .as_deref()
        .map(|q| params.name_match.normalize(q))
        .transpose()?;
    let storage_q = normalized_q
        .as_deref()
        .map(|q| params.name_match.to_storage(q));
    let authorities = params.authority.as_ref().map(AuthoritySet::wire_values);
    let namespaces = params
        .namespace
        .as_ref()
        .map(|namespace| vec![namespace.clone()]);
    let order = params.order.unwrap_or(SortOrder::Asc);

    let cursor_binding = ResolvesToCursorBinding {
        address: normalized_address,
        namespace: params.namespace.as_deref(),
        coin_type: coins.cursor_value(),
        dedupe: params.dedupe,
        q: normalized_q.as_deref(),
        name_match: params.name_match,
        authority: params.authority.as_ref(),
        parent,
        sort: params.sort,
        order,
    };
    let mut snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        state,
        params.cursor.as_deref(),
        params.namespace.as_deref(),
    )
    .await?;
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            let cursor = resolves_to_storage_cursor(&payload, &cursor_binding)?;
            Ok(cursor)
        })
        .transpose()?;

    let load_error = |error: anyhow::Error| {
        // A chain whose families are not published is stale.
        let message = format!("failed to load names resolving to {normalized_address}");
        name_rows_error(Resource, |_| V2Error::internal_error(message))(error)
    };
    let (rows, next_storage_cursor) = match &coins {
        ResolvesToCoins::Single { coin_type, numeric } => {
            let page = bigname_storage::load_address_records_current_page(
                snapshot.conn().await?,
                normalized_address,
                coin_type,
                namespaces.as_deref(),
                dedupe_to_storage(params.dedupe),
                storage_q,
                authorities.as_deref(),
                parent,
                sort_to_storage(params.sort),
                order_to_storage(order),
                storage_cursor.as_ref(),
                params.page_size,
            )
            .await
            .map_err(load_error)?;
            let rows = page.entries.into_iter().map(|entry| ResolvesToRow {
                matches: ResolvesToMatches::Single(address_name_resolution(&entry, *numeric)),
                entry,
            });
            (rows.collect::<Vec<_>>(), page.next_cursor)
        }
        ResolvesToCoins::Evm => {
            let page = bigname_storage::load_address_records_current_evm_page(
                snapshot.conn().await?,
                normalized_address,
                namespaces.as_deref(),
                dedupe_to_storage(params.dedupe),
                storage_q,
                authorities.as_deref(),
                parent,
                sort_to_storage(params.sort),
                order_to_storage(order),
                storage_cursor.as_ref(),
                params.page_size,
            )
            .await
            .map_err(load_error)?;
            if let Err(unsupported) = reject_rows_past_coin_type_limit(&page.entries) {
                return Err(snapshot.refuse(state, unsupported).await);
            }
            let rows = page.entries.into_iter().map(ResolvesToRow::from_evm);
            (rows.collect::<V2Result<Vec<_>>>()?, page.next_cursor)
        }
    };

    let entries = rows.iter().map(|row| &row.entry).collect::<Vec<_>>();
    let logical_name_ids = entries
        .iter()
        .map(|entry| entry.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let name_rows = bigname_storage::load_name_current_by_logical_name_ids(
        snapshot.conn().await?,
        &logical_name_ids,
    )
    .await
    .map_err(crate::v2::name_rows_error(
        crate::v2::SnapshotReadResource::Resource,
        |_| {
            V2Error::internal_error(format!(
                "failed to load registration summaries for names resolving to {normalized_address}"
            ))
        },
    ))?;
    let migrated_logical_name_ids = name_rows
        .values()
        .filter(|row| Authority::from_provenance(&row.provenance) == Some(Authority::EnsV2))
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let migrated_at_by_name =
        load_migrated_at(snapshot.conn().await?, &migrated_logical_name_ids).await?;
    let primary_flags = match &coins {
        ResolvesToCoins::Single { coin_type, .. } => {
            let primary_names_by_namespace = load_primary_names_by_namespace(
                snapshot.conn().await?,
                normalized_address,
                coin_type,
                entries.iter().map(|entry| entry.namespace.as_str()),
            )
            .await?;
            entries
                .iter()
                .map(|entry| {
                    primary_names_by_namespace
                        .get(&entry.namespace)
                        .and_then(Option::as_deref)
                        == Some(entry.normalized_name.as_str())
                })
                .collect::<Vec<_>>()
        }
        ResolvesToCoins::Evm => {
            evm_primary_flags(snapshot.conn().await?, normalized_address, &rows).await?
        }
    };
    let role_resource_ids = include_role_summary.then(|| {
        entries
            .iter()
            .filter_map(|entry| entry.resource_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    });
    let permissions_by_resource = if let Some(resource_ids) = role_resource_ids.as_deref() {
        super::role_summary::load_rows(
            state,
            &mut snapshot,
            resource_ids,
            params.namespace.as_deref(),
            entries.iter().filter_map(|entry| entry.resource_id),
        )
        .await?
        .into_iter()
        .fold(BTreeMap::new(), |mut grouped, row| {
            grouped
                .entry(row.resource_id)
                .or_insert_with(Vec::new)
                .push(row);
            grouped
        })
    } else {
        BTreeMap::new()
    };
    let permission_summaries =
        if let Some(resource_ids) = role_resource_ids.as_deref() {
            bigname_storage::load_serving_permission_summaries(snapshot.conn().await?, resource_ids)
            .await
            .map_err(crate::v2::name_rows_error(crate::v2::SnapshotReadResource::Resource, |_| {
                V2Error::internal_error(format!(
                    "failed to load role support for names resolving to {normalized_address}"
                ))
            }))?
        } else {
            BTreeMap::new()
        };
    let subname_counts_by_name = if include.counts {
        use crate::v2::SnapshotReadResource::Resource;
        bigname_storage::load_children_current_summaries(snapshot.conn().await?, &logical_name_ids)
            .await
            .map_err(crate::v2::name_rows_error(Resource, |_| {
                V2Error::internal_error(format!(
                    "failed to load subname counts for names resolving to {normalized_address}"
                ))
            }))?
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
        load_address_name_record_counts(
            snapshot.conn().await?,
            entries.iter().map(|entry| entry.logical_name_id.as_str()),
            &name_rows,
        )
        .await
        .map_err(name_rows_error(Resource, |_| {
            V2Error::internal_error(format!(
                "failed to load record counts for names resolving to {normalized_address}"
            ))
        }))?
    } else {
        BTreeMap::new()
    };

    let next_cursor = next_storage_cursor
        .as_ref()
        .map(|cursor| encode(&resolves_to_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let mut data = rows
        .iter()
        .zip(primary_flags)
        .map(|(resolves_to_row, is_primary)| {
            let entry = &resolves_to_row.entry;
            let (resolution, resolutions) = resolves_to_row.resolution_fields();
            let role_summary = if include_role_summary {
                Some(build_address_name_role_summary(
                    entry
                        .resource_id
                        .and_then(|id| permissions_by_resource.get(&id))
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                )?)
            } else {
                None
            };
            let name_row = name_rows.get(&entry.logical_name_id);
            let registration = name_registration_fields(name_row, &entry.namespace);
            let mut row = AddressName {
                name: entry.normalized_name.clone(),
                display_name: entry.canonical_display_name.clone(),
                namespace: entry.namespace.clone(),
                namehash: entry.namehash.clone(),
                permission_resource_id: entry
                    .resource_id
                    .map(|id| super::permission_resource_handle(name_row, id)),
                owner: registration.owner,
                manager: registration.manager,
                status: registration.status,
                registered_at: registration.registered_at,
                created_at: registration.created_at,
                expires_at: registration.expires_at,
                expires_at_reason: registration.expires_at_reason,
                grace_ends_at: registration.grace_ends_at,
                authority: name_row.and_then(|row| Authority::from_provenance(&row.provenance)),
                ens_v1: ens_v1_of_row(name_row)?,
                migrated_at: migrated_at_by_name.get(&entry.logical_name_id).cloned(),
                relations: vec![Relation::ResolvesTo],
                is_primary,
                resolution,
                resolutions,
                lapsed_registration: None,
                subname_count: include.counts.then(|| {
                    subname_counts_by_name
                        .get(&entry.logical_name_id)
                        .copied()
                        .unwrap_or_default()
                }),
                record_count: record_counts_by_name.get(&entry.logical_name_id).copied(),
                role_summary,
                restrictions: None,
            };
            if include_role_summary {
                row.restrictions = entry
                    .resource_id
                    .and_then(|id| permission_summaries.get(&id))
                    .map(ResourceRestrictions::from_summary)
                    .transpose()?
                    .flatten()
                    .map(|restrictions| {
                        restrictions.for_registration(
                            name_row.and_then(|row| registration_id(&row.declared_summary, None)),
                        )
                    });
            }
            Ok(row)
        })
        .collect::<V2Result<Vec<_>>>()?;
    crate::v2::name_record::fill_wrapper_expiries(
        snapshot.conn().await?,
        data.iter_mut().filter_map(|row| row.ens_v1.as_mut()),
    )
    .await?;
    let mut meta = snapshot.finish(state).await?;
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
    db: impl Into<bigname_storage::ReadDb<'_>>,
    address: &str,
    coin_type: &str,
    namespaces: impl Iterator<Item = &'a str>,
) -> V2Result<BTreeMap<String, Option<String>>> {
    let namespaces = namespaces.collect::<BTreeSet<_>>();
    let mut db = db.into();
    let mut primary_names = BTreeMap::new();
    for namespace in namespaces {
        let primary_name = bigname_storage::load_primary_name_current_snapshot(
            db.reborrow(),
            address,
            namespace,
            coin_type,
        )
        .await
        .map_err(name_rows_error(Resource, |_| {
            V2Error::internal_error(format!("failed to load primary name for address {address}"))
        }))?
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
    pub(crate) name_match: NameMatch,
    pub(crate) authority: Option<&'a AuthoritySet>,
    pub(crate) parent: Option<&'a str>,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
}

fn cursor_filters(binding: &ResolvesToCursorBinding<'_>) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
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
            AUTHORITY_FILTER_KEY.to_owned(),
            authority_filter_value(binding.authority),
        ),
        (
            ORDER_FILTER_KEY.to_owned(),
            binding.order.as_str().to_owned(),
        ),
    ]);
    insert_match_filter(&mut filters, binding.q, binding.name_match);
    insert_parent_filter(&mut filters, binding.parent);
    filters
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
mod tests;
