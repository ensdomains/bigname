use std::collections::{BTreeMap, BTreeSet};

use crate::AppState;
use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    AddressNameCurrentEntry, EffectivePermissionRow, NameCurrentRow, PrimaryNameClaimStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::cursor::invalid_cursor_error;
use super::name_filter::normalize_name_prefix;
use super::permission_support::{
    apply_role_summary_support_meta, permission_support_for_resources,
};
use super::support::{ensure_public_namespace, parse_evm_address};
use super::{
    Authority, Envelope, GrantRelation, Page, QueryParamAllowlist, RegistrationStatus, Relation,
    RelationSet, SortOrder, StrictQueryParams, V2Error, V2Result, api_error_to_v2, decode,
    effective_permission_scope_value, encode,
    name_record::{load_migrated_at, name_registration_fields, registration_id},
    permission_powers_value,
    restrictions::ResourceRestrictions,
    validate_latest_collection_selectors,
};

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use self::cursor::{
    ADDRESS_FILTER_KEY, ORDER_FILTER_KEY, SORT_KIND_CURSOR_KEY, SORT_KIND_NAME,
    SORT_KIND_TIMESTAMP_NULL, SORT_KIND_TIMESTAMP_VALUE, SORT_VALUE_CURSOR_KEY,
};
pub(crate) use self::cursor::{
    AddressNamesCursorBinding, address_names_cursor_payload, address_names_storage_cursor,
};

mod cursor;
mod resolves_to;
mod role_summary;
mod storage_mapping;

pub(crate) use self::storage_mapping::{
    dedupe_to_storage, order_to_storage, relation_from_storage, relation_set_to_storage,
    sort_to_storage,
};

pub(crate) use self::resolves_to::{AddressNameResolution, address_name_resolution};
#[cfg(test)]
pub(crate) use self::role_summary::grant_read_test_hooks;

pub(crate) struct AddressNamesQueryParams;

impl QueryParamAllowlist for AddressNamesQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "at",
        "finality",
        "relation",
        "coin_type",
        "authority",
        "is_migrated",
        "q",
        "sort",
        "order",
        "dedupe",
        "include",
        "cursor",
        "page_size",
    ];
}

pub(crate) type AddressNamesQuery = StrictQueryParams<AddressNamesQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct AddressName {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    /// Absent only on a `relation=resolves_to` row whose name has no permission authority
    /// resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) permission_resource_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registrant: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) authority: Option<Authority>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) migrated_at: Option<String>,
    pub(crate) relations: Vec<Relation>,
    pub(crate) is_primary: bool,
    /// Present only on `relation=resolves_to` rows: the coin type asked about and the record
    /// key that answered it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resolution: Option<AddressNameResolution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subname_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) record_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) role_summary: Option<Vec<AddressNameRoleSummary>>,
    /// Present with `include=role_summary` when the row's registration has a resource-level
    /// constraint model; the same block `GET /v1/permissions` serves.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) restrictions: Option<ResourceRestrictions>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNameRoleSummary {
    pub(crate) address: String,
    pub(crate) grants: Vec<AddressNameGrant>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNameGrant {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) grant_relation: Option<GrantRelation>,
    pub(crate) grant_scope: Value,
    pub(crate) powers: Value,
}

pub(crate) async fn get_address_names(
    Path(address): Path<String>,
    params: AddressNamesQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<AddressName>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let normalized_address = parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    if let Some(namespace) = params.namespace.as_deref() {
        ensure_public_namespace(namespace).map_err(api_error_to_v2)?;
    }
    if params
        .relation
        .as_ref()
        .is_some_and(RelationSet::is_resolves_to)
    {
        return resolves_to::get_address_resolves_to(&state, &normalized_address, &params).await;
    }
    if params.coin_type.is_some() {
        return Err(V2Error::invalid_input(
            "coin_type requires relation=resolves_to",
        ));
    }
    let namespace_filter = params.namespace.clone();
    let include = address_names_include(&params.include)?;
    let include_role_summary = include.role_summary;
    let storage_relations = params
        .relation
        .as_ref()
        .map(relation_set_to_storage)
        .unwrap_or_default();
    let storage_relations = (!storage_relations.is_empty()).then_some(storage_relations.as_slice());
    let storage_dedupe = dedupe_to_storage(params.dedupe);
    let storage_sort = sort_to_storage(params.sort);
    let order = params.order.unwrap_or(SortOrder::Asc);
    let storage_order = order_to_storage(order);
    let normalized_q = params.q.as_deref().map(normalize_name_prefix).transpose()?;

    let cursor_binding = AddressNamesCursorBinding {
        address: &normalized_address,
        namespace: namespace_filter.as_deref(),
        relation: params.relation.as_ref(),
        dedupe: params.dedupe,
        q: normalized_q.as_deref(),
        authority: params.authority,
        is_migrated: params.is_migrated,
        sort: params.sort,
        order,
    };
    let snapshot = super::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        params.cursor.as_deref(),
        params.namespace.as_deref(),
    )
    .await?;
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            let cursor = address_names_storage_cursor(&payload, &cursor_binding)?;
            snapshot.validate_cursor(&payload)?;
            Ok(cursor)
        })
        .transpose()?;

    let storage_page = bigname_storage::load_address_names_current_page_filtered(
        &state.pool,
        &normalized_address,
        namespace_filter.as_deref(),
        storage_relations,
        storage_dedupe,
        normalized_q.as_deref(),
        params.authority.map(Authority::as_str),
        params.is_migrated,
        storage_sort,
        storage_order,
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
            "failed to load address names for {normalized_address}"
        ))
    })?;

    let logical_name_ids = storage_page
        .entries
        .iter()
        .map(|entry| entry.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let name_rows =
        bigname_storage::load_name_current_by_logical_name_ids(&state.pool, &logical_name_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load address-name registration summaries for {normalized_address}"
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
        &normalized_address,
        storage_page
            .entries
            .iter()
            .map(|entry| entry.namespace.as_str()),
    )
    .await?;
    let role_resource_ids = include_role_summary.then(|| {
        storage_page
            .entries
            .iter()
            .map(|entry| entry.resource_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    });
    let permission_namespace = namespace_filter.as_deref();
    let permissions_by_resource = if let Some(resource_ids) = role_resource_ids.as_deref() {
        role_summary::load_rows(
            &state,
            &snapshot,
            resource_ids,
            permission_namespace,
            storage_page.entries.iter().map(|entry| entry.resource_id),
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
        std::collections::BTreeMap::new()
    };
    let permission_summaries = if let Some(resource_ids) = role_resource_ids.as_deref() {
        bigname_storage::load_permissions_current_resource_summaries(&state.pool, resource_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load address-name role support for {normalized_address}"
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
                    "failed to load address-name subname counts for {normalized_address}"
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
        load_address_name_record_counts(
            &state.pool,
            storage_page
                .entries
                .iter()
                .map(|entry| entry.logical_name_id.as_str()),
            &name_rows,
        )
        .await
        .map_err(|_| {
            V2Error::internal_error(format!(
                "failed to load address-name record counts for {normalized_address}"
            ))
        })?
    } else {
        BTreeMap::new()
    };

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&snapshot.bind_cursor(address_names_cursor_payload(cursor, &cursor_binding)))
    });
    let has_more = next_cursor.is_some();
    let data = storage_page
        .entries
        .iter()
        .map(|entry| {
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
            if include_role_summary {
                row.restrictions = permission_summaries
                    .get(&entry.resource_id)
                    .map(ResourceRestrictions::from_summary)
                    .transpose()?
                    .flatten()
                    .map(|restrictions| {
                        restrictions.for_registration(
                            name_rows
                                .get(&entry.logical_name_id)
                                .and_then(|row| registration_id(&row.declared_summary, None)),
                        )
                    });
            }
            Ok(row)
        })
        .collect::<V2Result<Vec<_>>>()?;
    let mut meta = snapshot.finish(&state).await?;
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
            total_count: Some(storage_page.summary.grouped_entry_count),
            has_more,
        }),
        meta,
    }))
}

async fn load_primary_names_by_namespace<'a>(
    pool: &sqlx::PgPool,
    address: &str,
    namespaces: impl Iterator<Item = &'a str>,
) -> V2Result<BTreeMap<String, Option<String>>> {
    let namespaces = namespaces.collect::<BTreeSet<_>>();
    let mut primary_names = BTreeMap::new();
    for namespace in namespaces {
        let primary_name =
            bigname_storage::load_primary_name_current_snapshot(pool, address, namespace, "60")
                .await
                .map_err(|_| {
                    V2Error::internal_error(format!(
                        "failed to load primary name for address {address}"
                    ))
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

async fn load_address_name_record_counts<'a>(
    pool: &sqlx::PgPool,
    names: impl Iterator<Item = &'a str>,
    name_rows: &BTreeMap<String, NameCurrentRow>,
) -> anyhow::Result<BTreeMap<String, u64>> {
    let mut logical_name_ids = Vec::new();
    let mut keys = Vec::new();
    for logical_name_id in names {
        let Some(name_row) = name_rows.get(logical_name_id) else {
            continue;
        };
        let Some((resource_id, boundary)) =
            bigname_storage::resolution_record_inventory_lookup_key_any_chain(name_row)
        else {
            continue;
        };
        logical_name_ids.push(logical_name_id.to_owned());
        keys.push((resource_id, boundary));
    }

    let counts =
        bigname_storage::count_record_inventory_selectors_by_lookup_keys(pool, &keys).await?;
    Ok(logical_name_ids
        .into_iter()
        .zip(counts)
        .filter_map(|(logical_name_id, count)| count.map(|count| (logical_name_id, count)))
        .collect())
}

pub(crate) fn build_address_name(
    entry: &AddressNameCurrentEntry,
    name_row: Option<&NameCurrentRow>,
    primary_name: Option<&str>,
    migrated_at: Option<String>,
    subname_count: Option<u64>,
    record_count: Option<u64>,
    role_summary: Option<Vec<AddressNameRoleSummary>>,
) -> AddressName {
    let registration = name_registration_fields(name_row, &entry.namespace);

    AddressName {
        name: entry.normalized_name.clone(),
        display_name: entry.canonical_display_name.clone(),
        namespace: entry.namespace.clone(),
        namehash: entry.namehash.clone(),
        permission_resource_id: Some(permission_resource_handle(name_row, entry.resource_id)),
        owner: registration.owner,
        registrant: registration.registrant,
        registration_status: registration.registration_status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        authority: name_row.and_then(|row| Authority::from_provenance(&row.provenance)),
        migrated_at,
        relations: entry
            .relations
            .iter()
            .copied()
            .map(relation_from_storage)
            .collect(),
        is_primary: primary_name == Some(entry.normalized_name.as_str()),
        resolution: None,
        subname_count,
        record_count,
        role_summary,
        restrictions: None,
    }
}

/// The value `GET /v1/permissions?registration_id=` resolves to this row's permission resource:
/// the registration the name currently serves (its BaseRegistrar lease for a wrapped `.eth`
/// name, whose rows live on the NameWrapper resource), or the resource itself when no
/// current registration claims it. Unsupported name coverage does not erase a retained handle.
/// A wrapped subname has no lease, so it keeps its NameWrapper resource.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240-L305 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390-L414 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
pub(crate) fn permission_resource_handle(
    name_row: Option<&NameCurrentRow>,
    resource_id: sqlx::types::Uuid,
) -> String {
    name_row
        .filter(|row| super::permissions::registration_row(row))
        .and_then(|row| registration_id(&row.declared_summary, None))
        .unwrap_or_else(|| resource_id.to_string())
}

pub(crate) fn build_address_name_role_summary(
    rows: &[EffectivePermissionRow],
) -> V2Result<Vec<AddressNameRoleSummary>> {
    let mut subjects = BTreeMap::<String, Vec<&EffectivePermissionRow>>::new();

    for row in rows {
        subjects.entry(row.subject.clone()).or_default().push(row);
    }

    subjects
        .into_iter()
        .map(|(address, mut rows)| {
            rows.sort_by(|left, right| left.scope.storage_key().cmp(&right.scope.storage_key()));
            Ok(AddressNameRoleSummary {
                address,
                grants: rows
                    .into_iter()
                    .map(|row| {
                        Ok(AddressNameGrant {
                            grant_relation: super::permission_grant_relation(row.grant_relation),
                            grant_scope: effective_permission_scope_value(&row.scope)?,
                            powers: permission_powers_value(&row.effective_powers)?,
                        })
                    })
                    .collect::<V2Result<Vec<_>>>()?,
            })
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct AddressNamesInclude {
    pub(super) role_summary: bool,
    pub(super) counts: bool,
}

pub(super) fn address_names_include(include: &[String]) -> V2Result<AddressNamesInclude> {
    let mut parsed = AddressNamesInclude::default();
    for value in include {
        match value.as_str() {
            "role_summary" => parsed.role_summary = true,
            "counts" => parsed.counts = true,
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only role_summary or counts",
                ));
            }
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests;
