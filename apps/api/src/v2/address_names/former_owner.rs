//! `relation=former_owner` on `GET /v1/addresses/{address}/names` (TYR-63): the released
//! names whose ended registration the address last held, for renewal reminders.
//!
//! The rows are the composed names whose `lapsed_registration.owner` is the address
//! (`bigname_storage::families::records::former_owners`). Like `resolves_to` the relation
//! is served on its own: `any` does not include it, it never combines with another relation,
//! and it is not an authority relation, so the rows carry no `owner`. It is
//! windowed by `expires_after` and `expires_before` and sorted by `expires_at` only; an app
//! still checks `grace_ends_at` and the contract's renewal rules before offering a renewal.
use std::collections::BTreeMap;

use axum::Json;
use bigname_storage::UnixSeconds;
use bigname_storage::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListOrder, NameCurrentRow,
    families::records::{FormerOwnerFilter, load_family_former_owner_page},
};

use crate::AppState;
use crate::v2::{
    AddressNamesDedupe, AddressNamesSort, Envelope, Page, QueryParams, Relation, SortOrder,
    V2Error, V2Result,
    collection_snapshot::CollectionSnapshot,
    cursor::invalid_cursor_error,
    list_cursor::{ListCursor, ListPosition},
    name_record::{ens_v1_of_row, lapsed_registration, load_migrated_at},
    name_rows_error,
    vocab::Authority,
};

use super::cursor::insert_parent_filter;
use super::{AddressName, load_primary_names_by_namespace, name_registration_fields};

const SORT: &str = "former_owner:expires_at";
const ADDRESS_FILTER_KEY: &str = "address";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const EXPIRES_AFTER_FILTER_KEY: &str = "expires_after";
const EXPIRES_BEFORE_FILTER_KEY: &str = "expires_before";
const ORDER_FILTER_KEY: &str = "order";
const EXPIRES_AT_CURSOR_KEY: &str = "expires_at";
const NAME_CURSOR_KEY: &str = "name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";
// A position field must be nonempty; this cannot be confused with an RFC 3339 timestamp.
const NO_EXPIRY_CURSOR_VALUE: &str = "none";
const POSITION_KEYS: [&str; 4] = [
    EXPIRES_AT_CURSOR_KEY,
    NAMESPACE_FILTER_KEY,
    NAME_CURSOR_KEY,
    NAMEHASH_CURSOR_KEY,
];

pub(super) async fn get_address_former_owners(
    state: &AppState,
    normalized_address: &str,
    params: &QueryParams,
    parent: Option<&str>,
) -> V2Result<Json<Envelope<Vec<AddressName>>>> {
    reject_unsupported(params)?;
    if let (Some(after), Some(before)) = (params.expires_after, params.expires_before)
        && after >= before
    {
        return Err(V2Error::invalid_input(
            "expires_after must be earlier than expires_before",
        ));
    }
    let order = params.order.unwrap_or(SortOrder::Asc);
    let mut filters = BTreeMap::from([
        (ADDRESS_FILTER_KEY.to_owned(), normalized_address.to_owned()),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            params.namespace.clone().unwrap_or_default(),
        ),
        (
            EXPIRES_AFTER_FILTER_KEY.to_owned(),
            timestamp_filter(params.expires_after),
        ),
        (
            EXPIRES_BEFORE_FILTER_KEY.to_owned(),
            timestamp_filter(params.expires_before),
        ),
        (ORDER_FILTER_KEY.to_owned(), order.as_str().to_owned()),
    ]);
    insert_parent_filter(&mut filters, parent);
    let list = ListCursor::new(SORT, filters);
    let storage_cursor = list
        .read(params.cursor.as_deref(), &POSITION_KEYS)?
        .map(|position| storage_cursor(&position))
        .transpose()?;
    let mut snapshot =
        CollectionSnapshot::capture_for_namespace(state, None, params.namespace.as_deref()).await?;
    let filter = FormerOwnerFilter {
        address: normalized_address,
        namespace: params.namespace.as_deref(),
        expires_after: params.expires_after,
        expires_before: params.expires_before,
        parent,
    };
    let page = load_family_former_owner_page(
        snapshot.conn().await?,
        &filter,
        match order {
            SortOrder::Asc => NameCurrentListOrder::Asc,
            SortOrder::Desc => NameCurrentListOrder::Desc,
        },
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(name_rows_error(
        crate::v2::SnapshotReadResource::Resource,
        |_| {
            V2Error::internal_error(format!(
                "failed to load the former names of {normalized_address}"
            ))
        },
    ))?;
    let primary_names = load_primary_names_by_namespace(
        snapshot.conn().await?,
        normalized_address,
        page.rows.iter().map(|row| row.namespace.as_str()),
    )
    .await?;
    let migrated_logical_name_ids = page
        .rows
        .iter()
        .filter(|row| Authority::from_provenance(&row.provenance) == Some(Authority::EnsV2))
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let migrated_at_by_name =
        load_migrated_at(snapshot.conn().await?, &migrated_logical_name_ids).await?;
    let mut data = page
        .rows
        .iter()
        .map(|row| {
            former_row(
                row,
                primary_names.get(&row.namespace).and_then(Option::as_deref),
                migrated_at_by_name.get(&row.logical_name_id).cloned(),
            )
        })
        .collect::<V2Result<Vec<_>>>()?;
    crate::v2::name_record::fill_wrapper_expiries(
        snapshot.conn().await?,
        data.iter_mut().filter_map(|row| row.ens_v1.as_mut()),
    )
    .await?;
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map(|cursor| list.next(position(cursor)));
    let has_more = next_cursor.is_some();
    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta: snapshot.finish(state).await?,
    }))
}

/// The parameters of the authority listing this relation does not take.
fn reject_unsupported(params: &QueryParams) -> V2Result<()> {
    if params.dedupe == AddressNamesDedupe::Registration {
        return Err(V2Error::invalid_input(
            "dedupe=registration is not supported with relation=former_owner",
        ));
    }
    let unsupported = [
        ("coin_type", params.coin_type.is_some()),
        ("authority", params.authority.is_some()),
        ("is_migrated", params.is_migrated.is_some()),
        ("q", params.q.is_some()),
        ("include", !params.include.is_empty()),
    ];
    if let Some((name, _)) = unsupported.iter().find(|(_, present)| *present) {
        return Err(V2Error::invalid_input(format!(
            "{name} is not supported with relation=former_owner"
        )));
    }
    if params.sort_wire.is_some() && params.sort != AddressNamesSort::ExpiresAt {
        return Err(V2Error::invalid_input(
            "relation=former_owner sorts by expires_at only",
        ));
    }
    Ok(())
}

fn former_row(
    row: &NameCurrentRow,
    primary_name: Option<&str>,
    migrated_at: Option<String>,
) -> V2Result<AddressName> {
    let registration = name_registration_fields(Some(row), &row.namespace);
    Ok(AddressName {
        name: row.normalized_name.clone(),
        display_name: row.canonical_display_name.clone(),
        namespace: row.namespace.clone(),
        namehash: row.namehash.clone(),
        permission_resource_id: row
            .resource_id
            .map(|resource| super::permission_resource_handle(Some(row), resource)),
        // A released name has no current owner; the former holder is in
        // `lapsed_registration`.
        owner: registration.owner,
        manager: registration.manager,
        registration_status: registration.registration_status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        expires_at_reason: registration.expires_at_reason,
        grace_ends_at: registration.grace_ends_at,
        authority: Authority::from_provenance(&row.provenance),
        ens_v1: ens_v1_of_row(Some(row))?,
        migrated_at,
        relations: vec![Relation::FormerOwner],
        is_primary: primary_name == Some(row.normalized_name.as_str()),
        resolution: None,
        resolutions: None,
        lapsed_registration: lapsed_registration(&row.declared_summary),
        subname_count: None,
        record_count: None,
        role_summary: None,
        restrictions: None,
    })
}

fn timestamp_filter(value: Option<UnixSeconds>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn position(cursor: &NameCurrentListCursor) -> ListPosition {
    let expires_at = match cursor.sort_value {
        NameCurrentListCursorValue::Timestamp(Some(at)) => at.internal_string(),
        _ => NO_EXPIRY_CURSOR_VALUE.to_owned(),
    };
    ListPosition::new([
        (EXPIRES_AT_CURSOR_KEY, expires_at),
        (NAMESPACE_FILTER_KEY, cursor.namespace.clone()),
        (NAME_CURSOR_KEY, cursor.normalized_name.clone()),
        (NAMEHASH_CURSOR_KEY, cursor.namehash.clone()),
    ])
}

fn storage_cursor(position: &ListPosition) -> V2Result<NameCurrentListCursor> {
    let expires_at = match position.get(EXPIRES_AT_CURSOR_KEY)? {
        NO_EXPIRY_CURSOR_VALUE => None,
        value => Some(
            value
                .parse::<UnixSeconds>()
                .map_err(|_| invalid_cursor_error())?,
        ),
    };
    Ok(NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Timestamp(expires_at),
        namespace: position.get(NAMESPACE_FILTER_KEY)?.to_owned(),
        normalized_name: position.get(NAME_CURSOR_KEY)?.to_owned(),
        namehash: position.get(NAMEHASH_CURSOR_KEY)?.to_owned(),
    })
}
