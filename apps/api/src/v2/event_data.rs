//! Row expansions for the history collections: `include=data` payloads and the
//! `include=raw` storage kind.
//!
//! With `include=data`, each friendly `type` exposes only the fields its stored
//! normalized event actually carries, translated into dictionary vocabulary
//! (`expires_at`, `resolver: {chain_id, address}`, `powers`, ...). Absent or
//! null source fields are omitted rather than serialized as `null`. The raw
//! storage event kind is deliberately not part of that payload; it is the one
//! documented pipeline term the product tier exposes, and only behind the
//! separate `include=raw` opt-in, as `kind`.

use bigname_storage::HistoryEvent as StorageHistoryEvent;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::types::time::OffsetDateTime;

use super::slug_to_numeric;
use super::{HistoryEventType, V2Error, V2Result, format_timestamp, permission_powers_value};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Row expansion added by `include=data`. `contract_address` is the emitting
/// contract when the row came from an on-chain log and `null` otherwise.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct EventDetail {
    pub(crate) contract_address: Option<String>,
    pub(crate) data: Map<String, Value>,
}

/// Row expansions the history collections accept on `include`. `data` adds the
/// friendly payload (`contract_address`, `data`); `raw` adds the raw storage
/// event `kind` for explorer and diagnostic use. The flags are independent and
/// compose in any order.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct HistoryInclude {
    pub(crate) data: bool,
    pub(crate) raw: bool,
}

#[cfg(test)]
impl HistoryInclude {
    pub(crate) const DATA: Self = Self {
        data: true,
        raw: false,
    };
    pub(crate) const RAW: Self = Self {
        data: false,
        raw: true,
    };
}

/// Validate history expansions; `total_count` controls the query, not event payload fields.
pub(crate) fn history_include(include: &[String]) -> V2Result<HistoryInclude> {
    let mut parsed = HistoryInclude::default();
    for value in include {
        match value.as_str() {
            "data" => parsed.data = true,
            "raw" => parsed.raw = true,
            "total_count" => {}
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only data, raw, or total_count",
                ));
            }
        }
    }
    Ok(parsed)
}

/// The raw storage event kind, present only with `include=raw`.
pub(crate) fn raw_event_kind(row: &StorageHistoryEvent, include: HistoryInclude) -> Option<String> {
    include.raw.then(|| row.event_kind.clone())
}

pub(crate) fn build_event_detail(
    row: &StorageHistoryEvent,
    event_type: HistoryEventType,
) -> EventDetail {
    EventDetail {
        contract_address: string_field(&row.raw_fact_ref, "emitting_address")
            .map(|address| address.to_ascii_lowercase()),
        data: build_event_data(row, event_type),
    }
}

fn build_event_data(row: &StorageHistoryEvent, event_type: HistoryEventType) -> Map<String, Value> {
    let after = &row.after_state;
    let before = &row.before_state;
    let mut data = Map::new();
    match event_type {
        HistoryEventType::Registration => {
            insert(&mut data, "registrant", address_field(after, "registrant"));
            insert(&mut data, "owner", address_field(after, "owner"));
            insert(&mut data, "expires_at", timestamp_field(after, "expiry"));
            insert(&mut data, "resolver", contract_ref(row, after, "resolver"));
            insert(
                &mut data,
                "subregistry",
                contract_ref(row, after, "subregistry"),
            );
        }
        HistoryEventType::Renewal | HistoryEventType::Release => {
            insert(&mut data, "expires_at", timestamp_field(after, "expiry"));
        }
        HistoryEventType::Expiry => {
            insert(&mut data, "expires_at", timestamp_field(after, "expiry"));
            insert(&mut data, "fuses", unsigned_field(after, "fuses"));
        }
        HistoryEventType::Transfer => {
            insert(&mut data, "from", address_field(before, "from"));
            insert(&mut data, "to", address_field(after, "to"));
            insert(&mut data, "fuses", unsigned_field(after, "fuses"));
        }
        HistoryEventType::Authority => {
            insert(
                &mut data,
                "owner",
                address_field(after, "owner").or_else(|| address_field(after, "registry_owner")),
            );
            insert(
                &mut data,
                "from",
                address_field(before, "owner").or_else(|| address_field(before, "registry_owner")),
            );
        }
        HistoryEventType::Resolver => {
            insert(&mut data, "resolver", contract_ref(row, after, "resolver"));
        }
        HistoryEventType::Record => {
            let key = string_field(after, "record_key");
            insert(
                &mut data,
                "coin_type",
                unsigned_field(after, "coin_type").or_else(|| {
                    key.as_deref()
                        .and_then(|key| key.strip_prefix("addr:"))
                        .and_then(|coin_type| coin_type.parse::<u64>().ok())
                        .map(Value::from)
                }),
            );
            insert(&mut data, "key", key.map(Value::String));
            insert(&mut data, "value", present(after.get("value")).cloned());
        }
        HistoryEventType::PrimaryName => {
            insert(&mut data, "address", address_field(after, "address"));
            insert(&mut data, "coin_type", unsigned_field(after, "coin_type"));
        }
        HistoryEventType::Permission => {
            insert(&mut data, "address", address_field(after, "subject"));
            insert(
                &mut data,
                "powers",
                present(after.get("effective_powers"))
                    .or_else(|| present(after.get("powers")))
                    .and_then(|powers| permission_powers_value(powers).ok()),
            );
            insert(&mut data, "fuses", unsigned_field(after, "fuses"));
        }
        HistoryEventType::Subregistry => {
            insert(
                &mut data,
                "subregistry",
                contract_ref(row, after, "subregistry"),
            );
        }
    }
    data
}

fn insert(data: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        data.insert(key.to_owned(), value);
    }
}

fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

fn string_field(state: &Value, key: &str) -> Option<String> {
    state
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Lower-cased EVM address; product output never carries checksum casing.
fn address_field(state: &Value, key: &str) -> Option<Value> {
    string_field(state, key).map(|address| Value::String(address.to_ascii_lowercase()))
}

/// `{chain_id, address}` for a non-zero contract pointer stored under `key`,
/// using the row's own chain. A zero or absent pointer means "cleared" and is
/// omitted.
fn contract_ref(row: &StorageHistoryEvent, state: &Value, key: &str) -> Option<Value> {
    let address = string_field(state, key)?.to_ascii_lowercase();
    if address == ZERO_ADDRESS {
        return None;
    }
    let chain_id = slug_to_numeric(row.chain_id.as_deref()?)?;
    Some(json!({ "chain_id": chain_id, "address": address }))
}

fn unsigned_field(state: &Value, key: &str) -> Option<Value> {
    match state.get(key)? {
        Value::Number(number) => number.as_u64().map(Value::from),
        Value::String(text) => text.trim().parse::<u64>().ok().map(Value::from),
        _ => None,
    }
}

/// Unix-second expiry fields become RFC 3339 `expires_at` values.
fn timestamp_field(state: &Value, key: &str) -> Option<Value> {
    let seconds = match state.get(key)? {
        Value::Number(number) => number.as_i64()?,
        Value::String(text) => text.trim().parse::<i64>().ok()?,
        _ => return None,
    };
    OffsetDateTime::from_unix_timestamp(seconds)
        .ok()
        .map(|value| Value::String(format_timestamp(value)))
}

#[cfg(test)]
mod tests {
    use bigname_storage::CanonicalityState;
    use serde_json::json;

    use super::*;

    fn row(event_kind: &str, before: Value, after: Value) -> StorageHistoryEvent {
        StorageHistoryEvent {
            normalized_event_id: 1,
            event_identity: "event:1".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some("ens:alice.eth".to_owned()),
            resource_id: None,
            registration_id: None,
            event_kind: event_kind.to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(100),
            block_hash: Some("0xblock".to_owned()),
            block_timestamp: None,
            transaction_hash: Some("0xtx".to_owned()),
            log_index: Some(0),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "emitting_address": "0x00000000000000000000000000000000000000AA",
            }),
            derivation_kind: "direct".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: before,
            after_state: after,
            migration_correlation_ids: Vec::new(),
            consumer_visibility: "activated".to_owned(),
            migration_associations: json!([]),
            provenance: json!({}),
            coverage: json!({}),
        }
    }

    #[test]
    fn include_accepts_only_data_and_raw_in_any_order() {
        assert_eq!(
            history_include(&[]).expect("empty include is valid"),
            HistoryInclude::default()
        );
        assert_eq!(
            history_include(&["data".to_owned()]).expect("data is valid"),
            HistoryInclude::DATA
        );
        assert_eq!(
            history_include(&["raw".to_owned()]).expect("raw is valid"),
            HistoryInclude::RAW
        );
        let both = HistoryInclude {
            data: true,
            raw: true,
        };
        assert_eq!(
            history_include(&["data".to_owned(), "raw".to_owned()]).expect("both are valid"),
            both
        );
        assert_eq!(
            history_include(&["raw".to_owned(), "data".to_owned()]).expect("both are valid"),
            both
        );
        assert!(history_include(&["bogus".to_owned()]).is_err());
        assert!(history_include(&["kind".to_owned()]).is_err());
        assert!(history_include(&["data".to_owned(), "events".to_owned()]).is_err());
    }

    #[test]
    fn raw_kind_is_exposed_only_behind_include_raw() {
        let row = row("RegistrationRenewed", json!({}), json!({}));
        assert_eq!(raw_event_kind(&row, HistoryInclude::default()), None);
        assert_eq!(raw_event_kind(&row, HistoryInclude::DATA), None);
        assert_eq!(
            raw_event_kind(&row, HistoryInclude::RAW),
            Some("RegistrationRenewed".to_owned())
        );
    }

    #[test]
    fn detail_exposes_lower_cased_emitter_without_the_raw_kind() {
        let detail = build_event_detail(
            &row(
                "RegistrationRenewed",
                json!({}),
                json!({ "expiry": 1_950_000_000_i64 }),
            ),
            HistoryEventType::Renewal,
        );
        assert_eq!(
            detail.contract_address,
            Some("0x00000000000000000000000000000000000000aa".to_owned())
        );
        assert!(
            serde_json::to_value(&detail)
                .expect("detail must serialize")
                .get("kind")
                .is_none()
        );
        assert_eq!(
            Value::Object(detail.data),
            json!({ "expires_at": "2031-10-17T10:40:00Z" })
        );

        let mut state_derived = row("ExpiryChanged", json!({}), json!({ "expiry": null }));
        state_derived.raw_fact_ref = json!({ "kind": "interpreter_state" });
        let detail = build_event_detail(&state_derived, HistoryEventType::Expiry);
        assert_eq!(detail.contract_address, None);
        assert!(detail.data.is_empty());
    }

    #[test]
    fn registration_and_pointer_types_use_dictionary_shapes() {
        let detail = build_event_detail(
            &row(
                "RegistrationGranted",
                json!({}),
                json!({
                    "owner": "0x00000000000000000000000000000000000000BB",
                    "expiry": "1900000000",
                    "resolver": "0x0000000000000000000000000000000000000abc",
                    "subregistry": ZERO_ADDRESS,
                }),
            ),
            HistoryEventType::Registration,
        );
        assert_eq!(
            Value::Object(detail.data),
            json!({
                "owner": "0x00000000000000000000000000000000000000bb",
                "expires_at": "2030-03-17T17:46:40Z",
                "resolver": {
                    "chain_id": 1,
                    "address": "0x0000000000000000000000000000000000000abc",
                },
            })
        );

        let cleared = build_event_detail(
            &row(
                "ResolverChanged",
                json!({ "resolver": "0x0000000000000000000000000000000000000abc" }),
                json!({ "resolver": ZERO_ADDRESS }),
            ),
            HistoryEventType::Resolver,
        );
        assert!(cleared.data.is_empty());

        let mut unknown_chain = row(
            "SubregistryChanged",
            json!({}),
            json!({ "subregistry": "0x0000000000000000000000000000000000000abc" }),
        );
        unknown_chain.chain_id = Some("unknown-chain".to_owned());
        assert!(
            build_event_detail(&unknown_chain, HistoryEventType::Subregistry)
                .data
                .is_empty()
        );
    }

    #[test]
    fn transfer_authority_record_primary_name_and_permission_payloads() {
        let transfer = build_event_detail(
            &row(
                "TokenControlTransferred",
                json!({ "from": "0x00000000000000000000000000000000000000AA" }),
                json!({ "to": "0x00000000000000000000000000000000000000bb", "fuses": 65537 }),
            ),
            HistoryEventType::Transfer,
        );
        assert_eq!(
            Value::Object(transfer.data),
            json!({
                "from": "0x00000000000000000000000000000000000000aa",
                "to": "0x00000000000000000000000000000000000000bb",
                "fuses": 65537,
            })
        );

        let authority = build_event_detail(
            &row(
                "AuthorityEpochChanged",
                json!({ "registry_owner": "0x00000000000000000000000000000000000000aa" }),
                json!({ "registry_owner": "0x00000000000000000000000000000000000000cc" }),
            ),
            HistoryEventType::Authority,
        );
        assert_eq!(
            Value::Object(authority.data),
            json!({
                "owner": "0x00000000000000000000000000000000000000cc",
                "from": "0x00000000000000000000000000000000000000aa",
            })
        );

        let record = build_event_detail(
            &row(
                "RecordChanged",
                json!({}),
                json!({
                    "record_key": "text:avatar",
                    "record_family": "text",
                    "value": "ipfs://avatar",
                    "value_retained": true,
                }),
            ),
            HistoryEventType::Record,
        );
        assert_eq!(
            Value::Object(record.data),
            json!({ "key": "text:avatar", "value": "ipfs://avatar" })
        );
        let unretained = build_event_detail(
            &row(
                "RecordChanged",
                json!({}),
                json!({ "record_key": "addr:2147483658", "coin_type": "2147483658" }),
            ),
            HistoryEventType::Record,
        );
        assert_eq!(
            Value::Object(unretained.data),
            json!({ "key": "addr:2147483658", "coin_type": 2_147_483_658_u64 })
        );
        let version = build_event_detail(
            &row("RecordVersionChanged", json!({}), json!({ "version": 2 })),
            HistoryEventType::Record,
        );
        assert!(version.data.is_empty());

        let primary = build_event_detail(
            &row(
                "ReverseChanged",
                json!({}),
                json!({
                    "address": "0x00000000000000000000000000000000000000AA",
                    "coin_type": "60",
                    "reverse_name": "aa.addr.reverse",
                }),
            ),
            HistoryEventType::PrimaryName,
        );
        assert_eq!(
            Value::Object(primary.data),
            json!({ "address": "0x00000000000000000000000000000000000000aa", "coin_type": 60 })
        );

        let permission = build_event_detail(
            &row(
                "EACRolesChanged",
                json!({}),
                json!({
                    "subject": "0x00000000000000000000000000000000000000DD",
                    "effective_powers": ["resource_control", "set_resolver"],
                    "scope": { "kind": "resolver" },
                    "role_bitmap": "0x1",
                }),
            ),
            HistoryEventType::Permission,
        );
        assert_eq!(
            Value::Object(permission.data),
            json!({
                "address": "0x00000000000000000000000000000000000000dd",
                "powers": ["registration_control", "set_resolver"],
            })
        );
        let fuses = build_event_detail(
            &row(
                "PermissionScopeChanged",
                json!({ "fuses": 0 }),
                json!({ "fuses": 196609, "wrapper_state": "locked" }),
            ),
            HistoryEventType::Permission,
        );
        assert_eq!(Value::Object(fuses.data), json!({ "fuses": 196609 }));
    }
}
