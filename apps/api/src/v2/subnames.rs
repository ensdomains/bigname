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
use super::name_filter::NameMatch;
use super::support::normalize_inferred_route_name;
use super::{
    AddressNamesSort, Authority, CursorPayload, Envelope, Page, QueryParamAllowlist,
    RegistrationStatus, RegistryRef, SortOrder, StrictQueryParams, V2Error, V2Result, decode,
    encode, load_subregistry_refs,
    name_record::{
        ens_v1_of_registry_child, ens_v1_of_row, name_registration_fields, served_manager,
    },
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
const MATCH_FILTER_KEY: &str = "match";
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
        "match",
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
    pub(crate) manager: Option<String>,
    pub(crate) status: RegistrationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<crate::v2::timestamps::ExpiryTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) grace_ends_at: Option<crate::v2::timestamps::ExpiryTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) authority: Option<Authority>,
    /// Present while the name's authority is `ens_v1` or `ens_v0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ens_v1: Option<crate::v2::name_record::EnsV1>,
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
    let storage_sort = sort_to_storage(params.sort)?;
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let include_counts = subnames_include_counts(&params.include)?;

    let logical_name_id = normalized.logical_name_id(&namespace);
    let mut snapshot = super::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        params.cursor.as_deref(),
        Some(&namespace),
    )
    .await?;
    let parent = bigname_storage::load_name_current(snapshot.conn().await?, &logical_name_id)
        .await
        .map_err(super::name_rows_error(
            super::SnapshotReadResource::Name,
            |_| {
                V2Error::internal_error(format!(
                    "failed to load subnames for {}/{}",
                    namespace, normalized.normalized_name
                ))
            },
        ))?;
    let Some(parent) = parent else {
        snapshot
            .ensure_families_published(&state, super::SnapshotReadResource::Name)
            .await?;
        snapshot.finish(&state).await?;
        return Err(V2Error::not_found(format!(
            "name {} was not found in namespace {namespace}",
            normalized.normalized_name
        )));
    };

    let normalized_q = params
        .q
        .as_deref()
        .map(|q| params.name_match.normalize(q))
        .transpose()?;
    let binding = SubnamesCursorBinding {
        namespace: &namespace,
        parent_logical_name_id: &parent.logical_name_id,
        q: normalized_q.as_deref(),
        name_match: params.name_match,
        include_expired: params.include_expired.unwrap_or(DEFAULT_INCLUDE_EXPIRED),
        sort: params.sort,
        order: params.order.unwrap_or(SortOrder::Asc),
    };
    let filter = ChildrenCurrentPageFilter {
        evaluated_at: Some(snapshot.evaluated_at()),
        q: binding.q.map(|q| binding.name_match.to_storage(q)),
        include_expired: binding.include_expired,
        sort: storage_sort,
        order: order_to_storage(binding.order),
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            let cursor = subname_storage_cursor(&payload, &binding)?;
            Ok(cursor)
        })
        .transpose()?;

    let storage_page = bigname_storage::load_children_current_page_filtered(
        snapshot.conn().await?,
        &parent.logical_name_id,
        &filter,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(super::name_rows_error(
        super::SnapshotReadResource::Name,
        |_| {
            V2Error::internal_error(format!(
                "failed to load subnames for {}/{}",
                namespace, normalized.normalized_name
            ))
        },
    ))?;

    let child_logical_name_ids = storage_page
        .rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();
    let child_name_rows = bigname_storage::load_name_current_by_logical_name_ids(
        snapshot.conn().await?,
        &child_logical_name_ids,
    )
    .await
    .map_err(super::name_rows_error(
        super::SnapshotReadResource::Name,
        |_| {
            V2Error::internal_error(format!(
                "failed to load subname registration summaries for {}/{}",
                namespace, normalized.normalized_name
            ))
        },
    ))?;
    let child_summaries = if include_counts {
        bigname_storage::load_children_current_summaries(
            snapshot.conn().await?,
            &child_logical_name_ids,
        )
        .await
        .map_err(super::name_rows_error(
            super::SnapshotReadResource::Name,
            |_| {
                V2Error::internal_error(format!(
                    "failed to load subname counts for {}/{}",
                    namespace, normalized.normalized_name
                ))
            },
        ))?
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
            subregistries
                .extend(load_subregistry_refs(snapshot.conn().await?, &names, Some(*block)).await?);
        }
    }

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| encode(&subname_cursor_payload(cursor, &binding)));
    let has_more = next_cursor.is_some();
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
            Ok(subname)
        })
        .collect::<V2Result<Vec<_>>>()?;
    super::name_record::fill_children_ens_v1(
        snapshot.conn().await?,
        super::name_chain_id(&parent).as_deref(),
        &storage_page.rows,
        &child_name_rows,
        data.iter_mut().map(|subname| subname.ens_v1.as_mut()),
    )
    .await?;
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
) -> V2Result<Subname> {
    let registration = name_registration_fields(name_row, &row.namespace);
    // A child with no name row serves the authority of the registry that owns its node.
    let authority = match name_row {
        Some(name) => Authority::from_provenance(&name.provenance),
        None => row
            .registry_authority
            .as_deref()
            .and_then(Authority::from_wire),
    };
    let ens_v1 = match name_row {
        Some(_) => ens_v1_of_row(name_row)?,
        None => ens_v1_of_registry_child(authority, row.lifecycle_shadow)?,
    };
    // A child with no name row serves its registry owner, or an ENSv2 child its token holder,
    // as manager and, unless the registrar retains a lease on its node whose token has moved
    // without the registry record, as owner too
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f),
    // but not the NameWrapper holding it for a token holder bigname does not record
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579-L581 @ ens_v1@91c966f),
    // and nobody once its lease is released, though expiry leaves the registry record in
    // place: the registrar writes it only on a registration other than `registerOnly`
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L149 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L122-L128 @ ens_v1@91c966f)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L414-L425 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L265-L276 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L248-L250 @ basenames@1809bbc)
    // or a `reclaim` by a live token's holder or an address it approved
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L327-L330 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L458-L466 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L173-L176 @ basenames@1809bbc).
    let (owner, manager) = match name_row {
        Some(name) => {
            let linked = registration
                .owner
                .is_none()
                .then(|| {
                    (name.resource_id.is_none()
                        && name.serving_resource_id.is_some()
                        && name
                            .declared_summary
                            .pointer("/coverage/enumeration_basis")
                            .and_then(serde_json::Value::as_str)
                            == Some("event_linked_registry_resolver"))
                    .then(|| row.owner.clone())
                    .flatten()
                })
                .flatten();
            let manager = registration.manager.or_else(|| {
                served_manager(&name.declared_summary, linked.as_ref(), linked.as_ref())
            });
            (registration.owner.or(linked), manager)
        }
        None if row.released_lease => (None, None),
        None => {
            let manager = row.owner.clone().or_else(|| row.registrant.clone());
            (
                row.token_holder
                    .clone()
                    .or_else(|| manager.clone())
                    .filter(|_| !row.wrapper_held),
                manager.filter(|_| !row.lifecycle_shadow),
            )
        }
    };

    Ok(Subname {
        name: row.normalized_name.clone(),
        display_name: row.canonical_display_name.clone(),
        namespace: row.namespace.clone(),
        namehash: row.namehash.clone(),
        labelhash: row.labelhash.clone(),
        owner,
        manager,
        // A child with no name row whose lease was released describes that lapsed registration.
        status: if name_row.is_none() && row.released_lease {
            RegistrationStatus::Released
        } else {
            registration.status
        },
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        expires_at_reason: registration.expires_at_reason,
        grace_ends_at: registration.grace_ends_at,
        authority,
        ens_v1,
        subregistry: None,
        subname_count: include_counts.then(|| {
            summary
                .and_then(|summary| u64::try_from(summary.child_count).ok())
                .unwrap_or_default()
        }),
    })
}

/// Everything a subnames cursor binds besides its keyset position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubnamesCursorBinding<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) parent_logical_name_id: &'a str,
    pub(crate) q: Option<&'a str>,
    pub(crate) name_match: NameMatch,
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
        let mut filters = BTreeMap::from([
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
        ]);
        // As on address names: only a contains match that narrows a present `q` adds a key,
        // so prefix cursors keep the shape they had before `match`.
        if self.q.is_some() && self.name_match == NameMatch::Contains {
            filters.insert(
                MATCH_FILTER_KEY.to_owned(),
                self.name_match.as_str().to_owned(),
            );
        }
        filters
    }
}

/// The children sorts. `created_at` is an address-name sort only; subnames refuse it.
pub(crate) fn sort_to_storage(sort: AddressNamesSort) -> V2Result<ChildrenCurrentSort> {
    match sort {
        AddressNamesSort::Name => Ok(ChildrenCurrentSort::Name),
        AddressNamesSort::ExpiresAt => Ok(ChildrenCurrentSort::ExpiresAt),
        AddressNamesSort::RegisteredAt => Ok(ChildrenCurrentSort::RegisteredAt),
        AddressNamesSort::CreatedAt => Err(V2Error::invalid_input("sort is invalid")),
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
            (SORT_KIND_TIMESTAMP_VALUE, value.internal_string())
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
            sort_value
                .parse::<bigname_storage::UnixSeconds>()
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
