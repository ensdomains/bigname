use std::collections::{BTreeMap, BTreeSet};

use axum::{Json, extract::State};
use bigname_storage::{
    EffectivePermissionRow, PermissionGrantRelation, PermissionsCurrentAccountResourceCursor,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::AppState;

use super::cursor::{cursor_value, invalid_cursor_error};
use super::name_record::wrapper_metadata;
use super::permission_support::{
    PermissionRequestScope, PermissionSupport, apply_permissions_collection_support_meta,
    permission_support_for_resources,
};
use super::{
    AddressNameGrant, CursorPayload, Envelope, GrantRelation, Meta, Page, QueryParamAllowlist,
    QueryParams, StrictQueryParams, V2Error, V2Result, decode, effective_permission_scope_value,
    encode, permission_powers_value, validate_latest_collection_selectors,
    vocab::{AuthorityContext, WrapperFuses, WrapperState},
};

#[path = "permissions/lineage.rs"]
mod lineage;
use lineage::permission_lineage;
#[path = "permissions/current_name.rs"]
mod current_name;
use current_name::load_current_name_row;

mod filter;
use filter::{EmptyPermissionsSelection, permissions_filter_inputs, resolve_permissions_filter};

const PERMISSIONS_SORT: &str = "address_registration_scope_asc";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const NAME_FILTER_KEY: &str = "name";
const ADDRESS_FILTER_KEY: &str = "address";
const REGISTRATION_ID_FILTER_KEY: &str = "registration_id";
const INCLUDE_FILTER_KEY: &str = "include";
const SUBJECT_CURSOR_KEY: &str = "subject";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
const SCOPE_CURSOR_KEY: &str = "scope";

pub(crate) struct PermissionsQueryParams;

impl QueryParamAllowlist for PermissionsQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "name",
        "registration_id",
        "address",
        "namespace",
        "at",
        "finality",
        "include",
        "cursor",
        "page_size",
    ];
}

pub(crate) type PermissionsQuery = StrictQueryParams<PermissionsQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct PermissionRow {
    pub(crate) address: String,
    #[serde(flatten)]
    pub(crate) grant: AddressNameGrant,
    pub(crate) registration_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) authority_context: AuthorityContext,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wrapper_state: Option<WrapperState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wrapper_fuses: Option<WrapperFuses>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lineage: Option<PermissionLineage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct PermissionLineage {
    pub(crate) grant: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) revocation: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) inheritance_path: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transfer_behavior: Option<Value>,
}

pub(crate) async fn get_permissions(
    params: PermissionsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<PermissionRow>>>> {
    let params = params.into_inner();
    validate_latest_collection_selectors(params.at.as_ref(), params.finality)?;
    let include_lineage = permissions_include_lineage(&params.include)?;
    let filter_inputs = permissions_filter_inputs(&params)?;

    let resolved =
        resolve_permissions_filter(&state, &params, include_lineage, &filter_inputs).await?;
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            permissions_storage_cursor(&payload, &resolved.cursor_filters)
        })
        .transpose()?;

    if let Some(selection) = resolved.empty_selection {
        return Ok(empty_permissions_response(&params, selection));
    }

    let storage_page = bigname_storage::load_effective_permissions_account_resource_page(
        &state.pool,
        resolved.subject.as_deref(),
        resolved.resource_id,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load permissions"))?;

    let resource_ids = storage_page
        .rows
        .iter()
        .map(|row| row.resource_id)
        .collect::<Vec<_>>();
    let support_resource_ids = resolved
        .resource_id
        .into_iter()
        .chain(resource_ids.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let permission_summaries = bigname_storage::load_permissions_current_resource_summaries(
        &state.pool,
        &support_resource_ids,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load permission support"))?;
    let current_names =
        bigname_storage::load_current_names_by_resource_ids(&state.pool, &resource_ids)
            .await
            .map_err(|_| V2Error::internal_error("failed to load permission names"))?;
    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&permissions_cursor_payload(
            cursor,
            &resolved.cursor_filters,
        ))
    });
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .map(|row| {
            let current_name = current_names.get(&row.resource_id);
            build_permission_row(
                row,
                current_name.map(|name| name.normalized_name.as_str()),
                current_name.map(|name| &name.declared_summary),
                include_lineage,
                resolved.authority_context,
            )
        })
        .collect::<V2Result<Vec<_>>>()?;
    let mut meta = Meta::default();
    let permission_support =
        permission_support_for_resources(&support_resource_ids, &permission_summaries);
    apply_permissions_collection_support_meta(
        &mut meta,
        permission_support,
        if resolved.resource_id.is_some() {
            PermissionRequestScope::ResourceBound
        } else {
            PermissionRequestScope::AccountWide
        },
    );

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

fn empty_permissions_response(
    params: &QueryParams,
    selection: EmptyPermissionsSelection,
) -> Json<Envelope<Vec<PermissionRow>>> {
    let mut meta = Meta::default();

    match selection {
        EmptyPermissionsSelection::MissingOrUnsupportedNameAnchor => {
            apply_permissions_collection_support_meta(
                &mut meta,
                PermissionSupport::Unknown,
                PermissionRequestScope::AccountWide,
            );
        }
        EmptyPermissionsSelection::SupersededNameRegistrationPair => {
            apply_permissions_collection_support_meta(
                &mut meta,
                PermissionSupport::Full,
                PermissionRequestScope::ResourceBound,
            );
        }
    }

    Json(Envelope {
        data: Vec::new(),
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor: None,
            page_size: params.page_size,
            total_count: None,
            has_more: false,
        }),
        meta,
    })
}

pub(crate) fn build_permission_row(
    row: &EffectivePermissionRow,
    name: Option<&str>,
    declared_summary: Option<&Value>,
    include_lineage: bool,
    authority_context: AuthorityContext,
) -> V2Result<PermissionRow> {
    let (wrapper_state, wrapper_fuses) = declared_summary
        .map(wrapper_metadata)
        .transpose()?
        .flatten()
        .map_or((None, None), |(state, fuses)| (Some(state), Some(fuses)));
    Ok(PermissionRow {
        address: row.subject.clone(),
        grant: AddressNameGrant {
            grant_relation: row.grant_relation.map(|relation| match relation {
                PermissionGrantRelation::Operator => GrantRelation::Operator,
            }),
            grant_scope: effective_permission_scope_value(&row.scope)?,
            powers: permission_powers_value(&row.effective_powers)?,
        },
        registration_id: row.resource_id.to_string(),
        name: name.map(str::to_owned),
        authority_context,
        wrapper_state,
        wrapper_fuses,
        lineage: include_lineage
            .then(|| permission_lineage(row))
            .transpose()?,
    })
}

fn permissions_cursor_payload(
    cursor: &PermissionsCurrentAccountResourceCursor,
    filters: &BTreeMap<String, String>,
) -> CursorPayload {
    CursorPayload::new(
        PERMISSIONS_SORT,
        filters.clone(),
        BTreeMap::from([
            (SUBJECT_CURSOR_KEY.to_owned(), cursor.subject.clone()),
            (
                RESOURCE_ID_CURSOR_KEY.to_owned(),
                cursor.resource_id.to_string(),
            ),
            (SCOPE_CURSOR_KEY.to_owned(), cursor.scope.clone()),
        ]),
        None,
    )
}

fn permissions_storage_cursor(
    payload: &CursorPayload,
    expected_filters: &BTreeMap<String, String>,
) -> V2Result<PermissionsCurrentAccountResourceCursor> {
    if payload.sort != PERMISSIONS_SORT {
        return Err(invalid_cursor_error());
    }
    if &payload.filters != expected_filters {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 3 {
        return Err(invalid_cursor_error());
    }

    let resource_id = cursor_value(payload, RESOURCE_ID_CURSOR_KEY, invalid_cursor_error)?
        .parse::<Uuid>()
        .map_err(|_| invalid_cursor_error())?;

    Ok(PermissionsCurrentAccountResourceCursor {
        subject: cursor_value(payload, SUBJECT_CURSOR_KEY, invalid_cursor_error)?,
        resource_id,
        scope: cursor_value(payload, SCOPE_CURSOR_KEY, invalid_cursor_error)?,
    })
}

fn permissions_include_lineage(include: &[String]) -> V2Result<bool> {
    let mut include_lineage = false;
    for value in include {
        match value.as_str() {
            "lineage" => include_lineage = true,
            _ => return Err(V2Error::invalid_input("include must contain only lineage")),
        }
    }
    Ok(include_lineage)
}

#[cfg(test)]
mod tests {
    use bigname_storage::{EffectivePermissionScope, PermissionsCurrentAccountResourceCursor};
    use serde_json::json;
    use sqlx::types::time::OffsetDateTime;

    use super::*;

    const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";
    const REGISTRATION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn sample_storage_cursor() -> PermissionsCurrentAccountResourceCursor {
        PermissionsCurrentAccountResourceCursor {
            subject: ADDRESS.to_owned(),
            resource_id: Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse"),
            scope: "resource".to_owned(),
        }
    }

    fn sample_filters() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("address".to_owned(), ADDRESS.to_owned()),
            ("include".to_owned(), "lineage".to_owned()),
            ("registration_id".to_owned(), REGISTRATION_ID.to_owned()),
        ])
    }

    fn sample_permissions_row(
        inheritance_path: Value,
        transfer_behavior: Value,
    ) -> EffectivePermissionRow {
        EffectivePermissionRow {
            resource_id: Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse"),
            subject: ADDRESS.to_owned(),
            scope: EffectivePermissionScope::Direct(bigname_storage::PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000ABC".to_owned(),
            }),
            grant_relation: None,
            effective_powers: json!(["set_resolver"]),
            grant_source: json!({
                "kind": "raw_log",
                "source_event": "EACRolesChanged",
                "upstream_resource": "root",
                "root_resource": true,
                "changed_powers": ["set_resolver"],
                "resolver_contract_instance_id": "00000000-0000-0000-0000-000000000010"
            }),
            revocation_source: Some(json!({
                "kind": "raw_log",
                "source_event": "EACRolesChanged",
                "upstream_resource": "root",
                "root_resource": true,
                "changed_powers": ["set_resolver"],
                "resolver_contract_instance_id": "00000000-0000-0000-0000-000000000011"
            })),
            inheritance_path,
            transfer_behavior,
            provenance: json!({}),
            coverage: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn permissions_cursor_payload_round_trips_storage_cursor() {
        let cursor = sample_storage_cursor();
        let filters = sample_filters();
        let payload = permissions_cursor_payload(&cursor, &filters);

        assert_eq!(payload.filters, filters);
        assert_eq!(
            permissions_storage_cursor(&payload, &sample_filters()).expect("cursor must decode"),
            cursor
        );
        assert!(payload.snapshot.is_none());
    }

    #[test]
    fn permissions_cursor_rejects_wrong_sort_or_filters() {
        let cursor = sample_storage_cursor();
        let filters = sample_filters();

        let mut payload = permissions_cursor_payload(&cursor, &filters);
        payload.sort = "name".to_owned();
        assert!(permissions_storage_cursor(&payload, &filters).is_err());

        let mut payload = permissions_cursor_payload(&cursor, &filters);
        payload
            .filters
            .insert("namespace".to_owned(), "ens".to_owned());
        assert!(permissions_storage_cursor(&payload, &filters).is_err());

        let mut payload = permissions_cursor_payload(&cursor, &filters);
        payload.filters.remove("address");
        assert!(permissions_storage_cursor(&payload, &filters).is_err());
    }

    #[test]
    fn permissions_cursor_ignores_legacy_snapshot_component() {
        let cursor = sample_storage_cursor();
        let filters = sample_filters();
        let mut payload = permissions_cursor_payload(&cursor, &filters);
        payload.snapshot = Some("legacy-snapshot".to_owned());

        assert_eq!(
            permissions_storage_cursor(&payload, &filters)
                .expect("legacy snapshot component must not bind a latest-state cursor"),
            cursor
        );
    }

    #[test]
    fn build_permission_row_maps_scope_powers_name_and_lineage() {
        let row = sample_permissions_row(
            json!([{
                "kind": "resolver_root_fallback",
                "chain_id": "ethereum-mainnet",
                "resolver_address": "0x0000000000000000000000000000000000000ABC",
                "upstream_resource": "root"
            }]),
            Value::Null,
        );
        let name = "alice.eth".to_owned();
        let mapped = build_permission_row(
            &row,
            Some(&name),
            None,
            true,
            AuthorityContext::CurrentForName,
        )
        .expect("known storage chain id must map");

        assert_eq!(mapped.address, ADDRESS);
        assert_eq!(mapped.registration_id, REGISTRATION_ID);
        assert_eq!(mapped.name, Some("alice.eth".to_owned()));
        assert_eq!(mapped.grant.powers, json!(["set_resolver"]));
        assert_eq!(
            mapped.grant.grant_scope,
            json!({
                "kind": "resolver",
                "detail": {
                    "resolver": {
                        "chain_id": 1,
                        "address": "0x0000000000000000000000000000000000000abc"
                    }
                }
            })
        );
        assert_eq!(
            mapped.lineage,
            Some(PermissionLineage {
                grant: json!({"kind": "event"}),
                revocation: Some(json!({
                    "kind": "event"
                })),
                inheritance_path: Some(json!([{
                    "kind": "resolver_root_fallback",
                    "resolver": {
                        "chain_id": 1,
                        "address": "0x0000000000000000000000000000000000000abc"
                    }
                }])),
                transfer_behavior: None,
            })
        );
    }

    #[test]
    fn registry_operator_grants_emit_operator_relation() {
        let mut row = sample_permissions_row(json!([]), json!({"mode": "owner_scoped"}));
        row.grant_relation = Some(PermissionGrantRelation::Operator);
        row.scope = EffectivePermissionScope::Account {
            chain_id: "ethereum-mainnet".to_owned(),
            authority_kind: "registry".to_owned(),
            authority_contract: "0x0000000000000000000000000000000000000c33".to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        };
        let mapped = build_permission_row(&row, None, None, false, AuthorityContext::ResourceAudit)
            .expect("registry operator grant must map");
        let value = serde_json::to_value(mapped).expect("permission row must serialize");

        assert_eq!(value["grant_relation"], json!("operator"));
    }

    #[test]
    fn lineage_omits_absent_optional_members() {
        let mut row = sample_permissions_row(json!([]), Value::Null);
        row.revocation_source = None;
        let mapped = build_permission_row(&row, None, None, true, AuthorityContext::ResourceAudit)
            .expect("known storage chain id must map");
        let lineage = mapped.lineage.expect("lineage must be present");

        assert_eq!(mapped.name, None);
        assert_eq!(lineage.grant, json!({"kind": "event"}));
        assert_eq!(lineage.revocation, None);
        assert_eq!(lineage.inheritance_path, None);
        assert_eq!(lineage.transfer_behavior, None);
    }
}
