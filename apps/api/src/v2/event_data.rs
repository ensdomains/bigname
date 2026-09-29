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

use bigname_storage::{HistoryEvent as StorageHistoryEvent, PermissionScope};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::history_context::HistoryRowContext;
use super::slug_to_numeric;
use super::{HistoryEventType, V2Error, V2Result, permission_powers_value, permission_scope_value};

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
    context: &HistoryRowContext,
) -> EventDetail {
    EventDetail {
        contract_address: string_field(&row.raw_fact_ref, "emitting_address")
            .map(|address| address.to_ascii_lowercase()),
        data: build_event_data(row, event_type, context),
    }
}

fn build_event_data(
    row: &StorageHistoryEvent,
    event_type: HistoryEventType,
    context: &HistoryRowContext,
) -> Map<String, Value> {
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
            if let Some((action_id, role)) = registration_action(row) {
                data.insert("action_id".to_owned(), Value::String(action_id));
                data.insert("action_role".to_owned(), Value::String(role.to_owned()));
            }
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
            // Where the record lives, so a write no single name can be given for stays
            // identifiable: the resolver, and its node or its record ID.
            insert(&mut data, "resolver", record_resolver(row));
            if string_field(after, "storage_model").as_deref() == Some("resolver_record_id") {
                insert(
                    &mut data,
                    "record_id",
                    string_field(after, "resolver_record_id").map(Value::String),
                );
            } else {
                // A `NameForAddrChanged` companion row keeps the reverse node it names under
                // `reverse_node` (crates/adapters/src/schema_v2/protocol/v1/reverse.rs).
                let node = string_field(after, "node").or_else(|| {
                    (string_field(after, "source_event").as_deref() == Some("NameForAddrChanged"))
                        .then(|| string_field(after, "reverse_node"))
                        .flatten()
                });
                insert(
                    &mut data,
                    "node",
                    node.map(|node| Value::String(node.to_ascii_lowercase())),
                );
            }
        }
        HistoryEventType::PrimaryName => {
            insert(&mut data, "address", address_field(after, "address"));
            insert(&mut data, "coin_type", unsigned_field(after, "coin_type"));
            if let Some(recorded) = context.recorded_primary_name(row) {
                let (name, status) = recorded_primary_name(recorded);
                insert(&mut data, "name", name.map(Value::String));
                data.insert("name_status".to_owned(), Value::String(status.to_owned()));
            }
        }
        HistoryEventType::Permission => {
            insert(&mut data, "address", address_field(after, "subject"));
            insert(&mut data, "grant_scope", grant_scope(row));
            if after["scope"]["kind"].as_str() == Some("registrar_controller") {
                insert(
                    &mut data,
                    "approved",
                    after
                        .get("approved")
                        .filter(|value| value.is_boolean())
                        .cloned(),
                );
            }
            let powers = permission_powers(after);
            if let (Some(powers), Some(previous)) = (&powers, logged_previous_powers(before)) {
                data.insert("added_powers".to_owned(), difference(powers, &previous));
                data.insert("removed_powers".to_owned(), difference(&previous, powers));
            }
            insert(&mut data, "powers", powers);
            insert(&mut data, "fuses", unsigned_field(after, "fuses"));
        }
        HistoryEventType::Subregistry => {
            insert(
                &mut data,
                "subregistry",
                contract_ref(row, after, "subregistry"),
            );
        }
        HistoryEventType::Migration => {
            insert(
                &mut data,
                "migration_path",
                string_field(after, "migration_path").map(Value::String),
            );
        }
    }
    data
}

/// The registration action a `registration` row belongs to, and what the row is within it.
///
/// One ENSv2 registration stores a grant at the registry's `LabelRegistered` log and a copy at the
/// `TokenResource` log the same `register` call emits once the token has its resource. That
/// action is the registration of one token by one contract in one transaction: the transaction
/// hash, the emitting contract and the token (its `token_id`, else its `labelhash`, else the row's
/// name). Several registrations in one transaction differ by token.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L491-L498 @ ens_v2_sepolia_20260916@366de741)
///
/// A grant is also stored each time an already registered label becomes reachable under a name.
/// One parent `SubregistryUpdated` can make a whole subtree reachable, and every resulting grant
/// carries the parent's log, so the emitter and token do not tell two descendant registries apart.
/// A reachability action is therefore one grant: the triggering log (transaction and log index),
/// the registry that holds the label, the token, and the name it became reachable under.
///
/// Rows with no transaction or emitting contract (derived from interpreter state) have no action.
fn registration_action(row: &StorageHistoryEvent) -> Option<(String, &'static str)> {
    use sha2::{Digest, Sha256};

    let transaction = row.transaction_hash.as_deref()?.to_ascii_lowercase();
    let chain = row.chain_id.as_deref()?;
    let emitter = string_field(&row.raw_fact_ref, "emitting_address")?.to_ascii_lowercase();
    let token = string_field(&row.after_state, "token_id")
        .or_else(|| string_field(&row.after_state, "labelhash"))
        .or_else(|| row.logical_name_id.clone())?
        .to_ascii_lowercase();
    let (key, role) = if let Some((_, reachable)) = row
        .event_identity
        .split_once(":RegistrationGranted:topology:")
    {
        // The identity suffix is `{registry}:{token}` followed by the ordinal.
        let registry = reachable.split(':').next().unwrap_or_default();
        let log_index = row.log_index?;
        let name = row.logical_name_id.as_deref().unwrap_or_default();
        (
            format!(
                "reachability-action\0{chain}\0{transaction}\0{log_index}\0{}\0{token}\0{name}",
                registry.to_ascii_lowercase()
            ),
            "reachable",
        )
    } else {
        let role = if row.event_identity.contains(":RegistrationGranted:linked:") {
            "linked"
        } else {
            "registered"
        };
        (
            format!("registration-action\0{chain}\0{transaction}\0{emitter}\0{token}"),
            role,
        )
    };
    Some((hex::encode(Sha256::digest(key.as_bytes())), role))
}

/// The subject's powers under the event's grant after the change: the whole set of that grant,
/// in the product vocabulary.
fn permission_powers(after: &Value) -> Option<Value> {
    present(after.get("effective_powers"))
        .or_else(|| present(after.get("powers")))
        .and_then(|powers| permission_powers_value(powers).ok())
}

/// The subject's powers under the grant before the change, only when the log itself stated
/// them. An ENSv2 `EACRolesChanged` carries the account's old role bitmap beside the new one,
/// and the adapter keeps it as `role_bitmap` with its decoded powers.
/// (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L17-L27 @ ens_v2@a971bd64)
/// Every other permission event's before state is the adapter's template for the grant it
/// records, not an observation, so no previous set is derived from it.
fn logged_previous_powers(before: &Value) -> Option<Value> {
    before.get("role_bitmap").and_then(Value::as_str)?;
    permission_powers_value(before.get("effective_powers")?).ok()
}

/// The powers in `left` that `right` lacks, in `left`'s order.
fn difference(left: &Value, right: &Value) -> Value {
    let right = right.as_array().map_or(&[][..], Vec::as_slice);
    Value::Array(
        left.as_array()
            .into_iter()
            .flatten()
            .filter(|power| !right.contains(power))
            .cloned()
            .collect(),
    )
}

/// The scope of the grant a permission event changed, in the `grant_scope` shape of permission
/// rows, plus the history-only registrar-controller scope. Omitted for events without a
/// modeled scope, such as a NameWrapper fuse change.
fn grant_scope(row: &StorageHistoryEvent) -> Option<Value> {
    let scope = row.after_state.get("scope")?;
    let scope = match string_field(scope, "kind")?.as_str() {
        "root" | "registry_root" => PermissionScope::Root,
        "registry" => PermissionScope::Registry,
        "resource" => PermissionScope::Resource,
        "registrar_controller" => {
            return Some(json!({
                "kind": "registrar_controller",
                "detail": {"registrar": contract_ref(row, &row.raw_fact_ref, "emitting_address")?},
            }));
        }
        "resolver" => PermissionScope::Resolver {
            chain_id: string_field(scope, "chain_id")?,
            resolver_address: string_field(scope, "resolver_address")?,
        },
        "record_manager" => PermissionScope::RecordManager {
            chain_id: string_field(scope, "chain_id")?,
            manager_address: string_field(scope, "manager_address")?,
        },
        _ => return None,
    };
    permission_scope_value(&scope).ok()
}

/// The name a primary-name event recorded and whether it set, cleared or recorded no name. An
/// empty name clears the reverse record; bytes that are not valid UTF-8, or contain NUL, set a
/// name that cannot be shown as text, so only the status is returned.
fn recorded_primary_name(recorded: Option<&Value>) -> (Option<String>, &'static str) {
    match recorded {
        None => (None, "unknown"),
        Some(Value::String(name)) if name.is_empty() => (None, "cleared"),
        Some(Value::String(name)) => (Some(name.clone()), "set"),
        Some(bytes) => match bytes.get("bytes").and_then(Value::as_str) {
            Some("0x" | "") => (None, "cleared"),
            _ => (None, "set"),
        },
    }
}

/// `{chain_id, address}` of the resolver holding a record: the address the write names, else the
/// emitting contract.
fn record_resolver(row: &StorageHistoryEvent) -> Option<Value> {
    let address = string_field(&row.after_state, "resolver")
        .or_else(|| string_field(&row.raw_fact_ref, "emitting_address"))?
        .to_ascii_lowercase();
    if address == ZERO_ADDRESS {
        return None;
    }
    let chain_id = slug_to_numeric(row.chain_id.as_deref()?)?;
    Some(json!({ "chain_id": chain_id, "address": address }))
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

/// Unix-second expiry fields become RFC 3339 `expires_at` values under the rule every expiry read
/// applies: a value outside 1970..=9999 is omitted, and one inside keeps its whole seconds.
fn timestamp_field(state: &Value, key: &str) -> Option<Value> {
    super::name_record::seconds_timestamp(state.get(key)?).map(Value::String)
}

#[cfg(test)]
mod tests;
