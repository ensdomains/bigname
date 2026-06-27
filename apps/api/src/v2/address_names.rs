use std::collections::BTreeMap;

use bigname_storage::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe,
    AddressNamesCurrentOrder, AddressNamesCurrentSort, AddressNamesCurrentSortedCursor,
    AddressNamesCurrentSortedCursorValue, NameCurrentRow, PermissionScope, PermissionsCurrentRow,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::types::{
    Uuid,
    time::{OffsetDateTime, UtcOffset},
};

use super::{
    AddressNamesDedupe, AddressNamesSort, CursorPayload, RegistrationStatus, Relation, SortOrder,
    V2Error, V2Result, name_record::name_registration_fields,
};

const ADDRESS_NAMES_SORT_NAME: &str = "name";
const ADDRESS_NAMES_SORT_EXPIRES_AT: &str = "expires_at";
const ADDRESS_NAMES_SORT_REGISTERED_AT: &str = "registered_at";
const ADDRESS_FILTER_KEY: &str = "address";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const DEDUPE_FILTER_KEY: &str = "dedupe";
const Q_FILTER_KEY: &str = "q";
const ORDER_FILTER_KEY: &str = "order";
const SORT_KIND_CURSOR_KEY: &str = "sort_kind";
const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const LOGICAL_NAME_ID_CURSOR_KEY: &str = "logical_name_id";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
const SORT_KIND_NAME: &str = "name";
const SORT_KIND_TIMESTAMP_NULL: &str = "timestamp_null";
const SORT_KIND_TIMESTAMP_VALUE: &str = "timestamp_value";
const NONE_FILTER_VALUE: &str = "";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct AddressName {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    pub(crate) owner: Option<String>,
    pub(crate) registrant: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    pub(crate) registered_at: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) expires_at: Option<String>,
    pub(crate) relations: Vec<Relation>,
    pub(crate) is_primary: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) role_summary: Option<Vec<AddressNameRoleSummary>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNameRoleSummary {
    pub(crate) address: String,
    pub(crate) grants: Vec<AddressNameGrant>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNameGrant {
    pub(crate) grant_scope: Value,
    pub(crate) powers: Value,
}

pub(crate) fn build_address_name(
    entry: &AddressNameCurrentEntry,
    name_row: Option<&NameCurrentRow>,
    primary_name: Option<&str>,
    role_summary: Option<Vec<AddressNameRoleSummary>>,
) -> AddressName {
    let registration = name_registration_fields(name_row, &entry.namespace);

    AddressName {
        name: entry.normalized_name.clone(),
        display_name: entry.canonical_display_name.clone(),
        namespace: entry.namespace.clone(),
        namehash: entry.namehash.clone(),
        owner: registration.owner,
        registrant: registration.registrant,
        registration_status: registration.registration_status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        relations: entry
            .relations
            .iter()
            .copied()
            .map(relation_from_storage)
            .collect(),
        is_primary: primary_name == Some(entry.normalized_name.as_str()),
        role_summary,
    }
}

pub(crate) fn relation_to_storage(relation: Relation) -> AddressNameRelation {
    match relation {
        Relation::Owner => AddressNameRelation::TokenHolder,
        Relation::Manager => AddressNameRelation::EffectiveController,
        Relation::Registrant => AddressNameRelation::Registrant,
    }
}

pub(crate) fn relation_from_storage(relation: AddressNameRelation) -> Relation {
    match relation {
        AddressNameRelation::TokenHolder => Relation::Owner,
        AddressNameRelation::EffectiveController => Relation::Manager,
        AddressNameRelation::Registrant => Relation::Registrant,
    }
}

pub(crate) fn dedupe_to_storage(dedupe: AddressNamesDedupe) -> AddressNamesCurrentDedupe {
    match dedupe {
        AddressNamesDedupe::Name => AddressNamesCurrentDedupe::Surface,
        AddressNamesDedupe::Registration => AddressNamesCurrentDedupe::Resource,
    }
}

pub(crate) fn sort_to_storage(sort: AddressNamesSort) -> AddressNamesCurrentSort {
    match sort {
        AddressNamesSort::Name => AddressNamesCurrentSort::Name,
        AddressNamesSort::ExpiresAt => AddressNamesCurrentSort::ExpiresAt,
        AddressNamesSort::RegisteredAt => AddressNamesCurrentSort::RegisteredAt,
    }
}

pub(crate) fn order_to_storage(order: SortOrder) -> AddressNamesCurrentOrder {
    match order {
        SortOrder::Asc => AddressNamesCurrentOrder::Asc,
        SortOrder::Desc => AddressNamesCurrentOrder::Desc,
    }
}

pub(crate) fn build_address_name_role_summary(
    rows: &[PermissionsCurrentRow],
) -> Vec<AddressNameRoleSummary> {
    let mut subjects = BTreeMap::<String, Vec<&PermissionsCurrentRow>>::new();

    for row in rows {
        subjects.entry(row.subject.clone()).or_default().push(row);
    }

    subjects
        .into_iter()
        .map(|(address, mut rows)| {
            rows.sort_by(|left, right| left.scope.storage_key().cmp(&right.scope.storage_key()));
            AddressNameRoleSummary {
                address,
                grants: rows
                    .into_iter()
                    .map(|row| AddressNameGrant {
                        grant_scope: permission_scope_value(&row.scope),
                        powers: row.effective_powers.clone(),
                    })
                    .collect(),
            }
        })
        .collect()
}

pub(crate) fn address_names_cursor_payload(
    cursor: &AddressNamesCurrentSortedCursor,
    binding: &AddressNamesCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        binding.sort.as_str(),
        BTreeMap::from([
            (ADDRESS_FILTER_KEY.to_owned(), binding.address.to_owned()),
            (
                NAMESPACE_FILTER_KEY.to_owned(),
                option_filter(binding.namespace),
            ),
            (
                RELATION_FILTER_KEY.to_owned(),
                binding
                    .relation
                    .map(Relation::as_str)
                    .unwrap_or(NONE_FILTER_VALUE)
                    .to_owned(),
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
        ]),
        cursor_last_item(cursor),
        Some(binding.snapshot_token.to_owned()),
    )
}

pub(crate) fn address_names_storage_cursor(
    payload: &CursorPayload,
    binding: &AddressNamesCursorBinding<'_>,
) -> V2Result<AddressNamesCurrentSortedCursor> {
    if payload.sort != binding.sort.as_str() {
        return Err(invalid_address_names_cursor());
    }
    if payload.snapshot.as_deref() != Some(binding.snapshot_token) {
        return Err(invalid_address_names_cursor());
    }
    if payload.filters.len() != 6
        || payload.filters.get(ADDRESS_FILTER_KEY).map(String::as_str) != Some(binding.address)
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(option_filter(binding.namespace).as_str())
        || payload.filters.get(RELATION_FILTER_KEY).map(String::as_str)
            != Some(
                binding
                    .relation
                    .map(Relation::as_str)
                    .unwrap_or(NONE_FILTER_VALUE),
            )
        || payload.filters.get(DEDUPE_FILTER_KEY).map(String::as_str)
            != Some(binding.dedupe.as_str())
        || payload.filters.get(Q_FILTER_KEY).map(String::as_str)
            != Some(option_filter(binding.q).as_str())
        || payload.filters.get(ORDER_FILTER_KEY).map(String::as_str) != Some(binding.order.as_str())
    {
        return Err(invalid_address_names_cursor());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_address_names_cursor());
    }

    let sort_value = cursor_sort_value(payload, binding.sort)?;
    let logical_name_id = cursor_nonempty_value(payload, LOGICAL_NAME_ID_CURSOR_KEY)?;
    let resource_id = Uuid::parse_str(&cursor_nonempty_value(payload, RESOURCE_ID_CURSOR_KEY)?)
        .map_err(|_| invalid_address_names_cursor())?;

    Ok(AddressNamesCurrentSortedCursor {
        sort_value,
        logical_name_id,
        resource_id,
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AddressNamesCursorBinding<'a> {
    pub(crate) address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) relation: Option<Relation>,
    pub(crate) dedupe: AddressNamesDedupe,
    pub(crate) q: Option<&'a str>,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
    pub(crate) snapshot_token: &'a str,
}

fn cursor_last_item(cursor: &AddressNamesCurrentSortedCursor) -> BTreeMap<String, String> {
    let (sort_kind, sort_value) = match &cursor.sort_value {
        AddressNamesCurrentSortedCursorValue::Name(value) => {
            (SORT_KIND_NAME.to_owned(), value.clone())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(None) => {
            (SORT_KIND_TIMESTAMP_NULL.to_owned(), String::new())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)) => (
            SORT_KIND_TIMESTAMP_VALUE.to_owned(),
            format_timestamp(*value),
        ),
    };

    BTreeMap::from([
        (SORT_KIND_CURSOR_KEY.to_owned(), sort_kind),
        (SORT_VALUE_CURSOR_KEY.to_owned(), sort_value),
        (
            LOGICAL_NAME_ID_CURSOR_KEY.to_owned(),
            cursor.logical_name_id.clone(),
        ),
        (
            RESOURCE_ID_CURSOR_KEY.to_owned(),
            cursor.resource_id.to_string(),
        ),
    ])
}

fn cursor_sort_value(
    payload: &CursorPayload,
    sort: AddressNamesSort,
) -> V2Result<AddressNamesCurrentSortedCursorValue> {
    let sort_kind = cursor_nonempty_value(payload, SORT_KIND_CURSOR_KEY)?;
    let sort_value = payload
        .last_item
        .get(SORT_VALUE_CURSOR_KEY)
        .cloned()
        .ok_or_else(invalid_address_names_cursor)?;

    match (sort, sort_kind.as_str()) {
        (AddressNamesSort::Name, SORT_KIND_NAME) if !sort_value.trim().is_empty() => {
            Ok(AddressNamesCurrentSortedCursorValue::Name(sort_value))
        }
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_NULL,
        ) if sort_value.is_empty() => Ok(AddressNamesCurrentSortedCursorValue::Timestamp(None)),
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_VALUE,
        ) if !sort_value.trim().is_empty() => {
            let value = bigname_storage::parse_rfc3339_utc_timestamp(&sort_value)
                .map_err(|_| invalid_address_names_cursor())?;
            Ok(AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)))
        }
        _ => Err(invalid_address_names_cursor()),
    }
}

fn cursor_nonempty_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_address_names_cursor)
}

fn invalid_address_names_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

fn option_filter(value: Option<&str>) -> String {
    value.unwrap_or(NONE_FILTER_VALUE).to_owned()
}

fn permission_scope_value(scope: &PermissionScope) -> Value {
    json!({
        "kind": scope.kind(),
        "detail": scope.detail(),
    })
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::decode;

    fn binding(sort: AddressNamesSort) -> AddressNamesCursorBinding<'static> {
        AddressNamesCursorBinding {
            address: "0x00000000000000000000000000000000000000aa",
            namespace: Some("ens"),
            relation: Some(Relation::Owner),
            dedupe: AddressNamesDedupe::Name,
            q: Some("al"),
            sort,
            order: SortOrder::Asc,
            snapshot_token: "snapshot-1",
        }
    }

    #[test]
    fn address_names_cursor_payload_round_trips_name_cursor() {
        let cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
            logical_name_id: "ens:alice.eth".to_owned(),
            resource_id: Uuid::from_u128(0x1234),
        };
        let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));

        assert_eq!(
            address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name))
                .expect("cursor must decode"),
            cursor
        );
        assert_eq!(payload.last_item[SORT_KIND_CURSOR_KEY], SORT_KIND_NAME);
    }

    #[test]
    fn address_names_cursor_payload_distinguishes_timestamp_null_and_value() {
        let null_cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(None),
            logical_name_id: "ens:missing-expiry.eth".to_owned(),
            resource_id: Uuid::from_u128(0x1235),
        };
        let null_payload =
            address_names_cursor_payload(&null_cursor, &binding(AddressNamesSort::ExpiresAt));
        assert_eq!(
            null_payload.last_item[SORT_KIND_CURSOR_KEY],
            SORT_KIND_TIMESTAMP_NULL
        );
        assert_eq!(null_payload.last_item[SORT_VALUE_CURSOR_KEY], "");
        assert_eq!(
            address_names_storage_cursor(&null_payload, &binding(AddressNamesSort::ExpiresAt))
                .expect("null timestamp cursor must decode"),
            null_cursor
        );

        let value = bigname_storage::parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z")
            .expect("timestamp must parse");
        let value_cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)),
            logical_name_id: "ens:alice.eth".to_owned(),
            resource_id: Uuid::from_u128(0x1236),
        };
        let value_payload =
            address_names_cursor_payload(&value_cursor, &binding(AddressNamesSort::ExpiresAt));
        assert_eq!(
            value_payload.last_item[SORT_KIND_CURSOR_KEY],
            SORT_KIND_TIMESTAMP_VALUE
        );
        assert_eq!(
            value_payload.last_item[SORT_VALUE_CURSOR_KEY],
            "2027-01-02T03:04:05Z"
        );
        assert_eq!(
            address_names_storage_cursor(&value_payload, &binding(AddressNamesSort::ExpiresAt))
                .expect("timestamp cursor must decode"),
            value_cursor
        );
    }

    #[test]
    fn address_names_cursor_rejects_cross_sort_filter_order_or_snapshot() {
        let cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
            logical_name_id: "ens:alice.eth".to_owned(),
            resource_id: Uuid::from_u128(0x1234),
        };

        let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
        assert!(
            address_names_storage_cursor(&payload, &binding(AddressNamesSort::ExpiresAt)).is_err()
        );

        let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
        payload.filters.insert(
            ADDRESS_FILTER_KEY.to_owned(),
            "0x00000000000000000000000000000000000000bb".to_owned(),
        );
        assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());

        let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
        payload
            .filters
            .insert(ORDER_FILTER_KEY.to_owned(), "desc".to_owned());
        assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());

        let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
        payload.snapshot = Some("snapshot-2".to_owned());
        assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());
    }

    #[test]
    fn address_names_cursor_rejects_cross_timestamp_sort_reuse() {
        let cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(Some(
                bigname_storage::parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z")
                    .expect("timestamp must parse"),
            )),
            logical_name_id: "ens:alice.eth".to_owned(),
            resource_id: Uuid::from_u128(0x1234),
        };
        let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::ExpiresAt));

        assert!(
            address_names_storage_cursor(&payload, &binding(AddressNamesSort::RegisteredAt))
                .is_err()
        );
    }

    #[test]
    fn address_names_cursor_token_decodes_to_bound_payload() {
        let cursor = AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
            logical_name_id: "ens:alice.eth".to_owned(),
            resource_id: Uuid::from_u128(0x1234),
        };
        let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
        let encoded = crate::v2::encode(&payload);

        assert_eq!(
            decode(&encoded).expect("encoded cursor must decode"),
            payload
        );
    }
}
