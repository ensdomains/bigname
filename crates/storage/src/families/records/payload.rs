//! The value a record write serves, computed from its payload as the record inventory's
//! `record_rollups` computes it (record_inventory.rs): status, value and unsupported reason of the
//! entry, whether the record takes part in the selector list, and the coin-60 zero-address rule.
//! The functions read a payload the way the SQL operators read `after_state`: `->>` returns a
//! string as it is and any other value as its JSON text, and a missing key and a JSON null both
//! read as SQL null.
use serde_json::{Map, Value, json};

use super::ZERO_ADDRESS;

/// `payload ->> key`.
fn text(payload: &Value, key: &str) -> Option<String> {
    match payload.get(key)? {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// `text IN ('', '0x')`, null for a null text.
fn blank(text: Option<&str>) -> Option<bool> {
    text.map(|text| text.is_empty() || text == "0x")
}

/// Whether the family is one whose empty value is a clear.
fn clearable(payload: &Value) -> bool {
    matches!(
        text(payload, "record_family").as_deref(),
        Some("contenthash" | "addr")
    )
}

/// `after ->> 'value' IN ('', '0x') OR COALESCE(after #>> '{value,bytes}' IN ('', '0x'), false)`
/// in three-valued logic.
fn value_cleared(payload: &Value) -> Option<bool> {
    let bytes = payload
        .get("value")
        .filter(|value| value.is_object())
        .and_then(|value| text(value, "bytes"));
    let second = blank(bytes.as_deref()).unwrap_or(false);
    match blank(text(payload, "value").as_deref()) {
        Some(true) => Some(true),
        Some(false) => Some(second),
        None => second.then_some(true),
    }
}

/// The entry status before the coin-60 zero-address rule.
pub(crate) fn status(payload: &Value) -> &'static str {
    if payload.get("value").is_some() {
        if clearable(payload) && value_cleared(payload) == Some(true) {
            "not_found"
        } else {
            "success"
        }
    } else if payload.get("contenthash_hex").is_some() {
        if blank(text(payload, "contenthash_hex").as_deref()) == Some(true) {
            "not_found"
        } else {
            "success"
        }
    } else if payload.get("address_bytes_hex").is_some() {
        if blank(text(payload, "address_bytes_hex").as_deref()) == Some(true) {
            "not_found"
        } else {
            "success"
        }
    } else {
        "unsupported"
    }
}

/// The entry value before the coin-60 zero-address rule.
fn value(payload: &Value) -> Value {
    if payload.get("value").is_some() {
        let serve = if clearable(payload) {
            value_cleared(payload).map(|cleared| !cleared)
        } else {
            Some(true)
        };
        if serve == Some(true) {
            return payload.get("value").cloned().unwrap_or(Value::Null);
        }
    }
    if payload.get("contenthash_hex").is_some()
        && blank(text(payload, "contenthash_hex").as_deref()) == Some(false)
    {
        return json!({"encoding": "hex", "bytes": text(payload, "contenthash_hex")});
    }
    if payload.get("address_bytes_hex").is_some()
        && blank(text(payload, "address_bytes_hex").as_deref()) == Some(false)
    {
        return payload
            .get("address_bytes_hex")
            .cloned()
            .unwrap_or(Value::Null);
    }
    Value::Null
}

/// Recognizes a native ENSv1 or Basenames coin-60 zero-address write for inventory not-found
/// classification. In ENSv1, address(0) is stored as twenty zero bytes. Those bytes are nonempty
/// and suppress ENSIP-19 fallback; an empty exact record can instead return a nonzero default.
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L22-L30 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L80-L85 @ ens_v1@91c966f)
/// The Basenames resolver stores and reads the same way: `setAddr(node, a)` writes the twenty
/// address bytes under coin 60, and the coin read falls back to the default only for empty bytes.
/// (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L48-L50 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L93-L100 @ basenames@1809bbc)
pub(crate) fn coin60_zero_address_is_absent(
    payload: &Value,
    event_source_family: &str,
    pointer_source_family: &str,
) -> bool {
    let native = (event_source_family == "ens_v1_resolver_l1"
        && matches!(
            pointer_source_family,
            "ens_v1_registry_l1" | "ens_v1_registrar_l1" | "ens_v1_wrapper_l1"
        ))
        || (event_source_family == "basenames_base_resolver"
            && pointer_source_family == "basenames_base_registry");
    let address = payload
        .get("value")
        .filter(|value| value.is_object())
        .and_then(|value| text(value, "bytes"))
        .or_else(|| text(payload, "value"));
    native
        && text(payload, "record_key").as_deref() == Some("addr:60")
        && text(payload, "record_family").as_deref() == Some("addr")
        && text(payload, "selector_key").as_deref() == Some("60")
        && address.is_some_and(|address| address.to_ascii_lowercase() == ZERO_ADDRESS)
}

/// Whether the record is the forward `name` record (`NameChanged` on a node-keyed resolver,
/// `NameUpdated` on an ENSv2 record ID). Its value is the event's `raw_name`, retained on the
/// value row for reverse claims.
fn is_name_record(payload: &Value) -> bool {
    text(payload, "record_family").as_deref() == Some("name")
        && text(payload, "record_key").as_deref() == Some("name")
}

/// The forward name record's status and value from its retained `raw_name`: a nonempty name is a
/// success, an empty one a clear (`not_found`), and a write whose name was not retained is
/// unsupported. A name that is not valid UTF-8 keeps the event's `{encoding, bytes}` form.
/// `setName` stores the string per record version and `name` reads it back, so an empty string
/// reads as unset and a `VersionChanged` reset hides it like any other record.
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13-L29 @ ens_v1@91c966f)
fn name_record(payload: &Value) -> (&'static str, Value) {
    match payload.get("raw_name") {
        Some(Value::String(name)) if name.is_empty() => ("not_found", Value::Null),
        Some(Value::String(name)) => ("success", json!(name)),
        Some(value @ Value::Object(_)) => match blank(text(value, "bytes").as_deref()) {
            Some(false) => ("success", value.clone()),
            _ => ("not_found", Value::Null),
        },
        _ => ("unsupported", Value::Null),
    }
}

/// The inventory entry of a served record, nulls stripped: `None` for a family the inventory
/// does not list. `stored_status` is the status the family row keeps, used in place of the one
/// derived from a payload rebuilt from its columns.
pub(crate) fn entry(
    payload: &Value,
    stored_status: Option<&str>,
    zero_absent: bool,
) -> Option<Value> {
    let family = text(payload, "record_family")?;
    if is_name_record(payload) {
        let (status, value) = name_record(payload);
        let mut entry = Map::new();
        entry.insert("record_key".into(), json!("name"));
        entry.insert("record_family".into(), json!("name"));
        entry.insert("status".into(), json!(status));
        entry.insert("value".into(), value);
        if status == "unsupported" {
            entry.insert(
                "unsupported_reason".into(),
                json!("value_not_retained_in_normalized_events"),
            );
        }
        return Some(strip_nulls(Value::Object(entry)));
    }
    if !matches!(family.as_str(), "text" | "addr" | "contenthash") {
        return None;
    }
    let status = if zero_absent {
        "not_found"
    } else {
        stored_status.unwrap_or_else(|| status(payload))
    };
    // From the status, not from which keys the payload has: a payload rebuilt from the family
    // row's columns has no `value` for an explicit JSON null, which serves as a success.
    let unsupported = status == "unsupported";
    let mut entry = Map::new();
    entry.insert("record_key".into(), json!(text(payload, "record_key")));
    entry.insert("record_family".into(), json!(family));
    entry.insert(
        "selector_key".into(),
        payload.get("selector_key").cloned().unwrap_or(Value::Null),
    );
    entry.insert("status".into(), json!(status));
    entry.insert(
        "value".into(),
        if zero_absent {
            Value::Null
        } else {
            value(payload)
        },
    );
    if unsupported {
        entry.insert(
            "unsupported_reason".into(),
            json!("value_not_retained_in_normalized_events"),
        );
    }
    super::rows::text_hydration::apply(&mut entry, payload);
    Some(strip_nulls(Value::Object(entry)))
}

/// The selector a served record contributes to the inventory's selector list, if any.
pub(crate) fn selector(payload: &Value) -> Option<Value> {
    let key = text(payload, "record_key")?;
    let family = text(payload, "record_family")?;
    let selector_key = text(payload, "selector_key");
    let listed = match family.as_str() {
        "text" => {
            key == "text"
                || selector_key
                    .as_ref()
                    .is_some_and(|selector| key == format!("text:{selector}"))
        }
        "addr" => selector_key
            .as_ref()
            .is_some_and(|selector| key == format!("addr:{selector}")),
        "contenthash" => key == "contenthash",
        _ => false,
    };
    listed.then(|| {
        json!({
            "record_key": key,
            "record_family": family,
            "selector_key": payload.get("selector_key").cloned().unwrap_or(Value::Null),
            "cacheable": true,
        })
    })
}

/// The record family a served record lists as unsupported, if any.
pub(crate) fn unsupported_family(payload: &Value) -> Option<String> {
    if is_name_record(payload) {
        return None;
    }
    text(payload, "record_family")
        .filter(|family| !matches!(family.as_str(), "text" | "addr" | "contenthash"))
}

/// `jsonb_strip_nulls`: every object field whose value is null goes, at any depth.
pub(crate) fn strip_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(key, value)| (key, strip_nulls(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(strip_nulls).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleared_addresses_and_contenthashes_answer_not_found() {
        let cleared = json!({"record_key": "addr:60", "record_family": "addr",
                             "selector_key": "60", "value": "0x"});
        assert_eq!(status(&cleared), "not_found");
        assert_eq!(
            entry(&cleared, None, false),
            Some(json!({"record_key": "addr:60", "record_family": "addr",
                        "selector_key": "60", "status": "not_found"}))
        );
        let nested = json!({"record_key": "contenthash", "record_family": "contenthash",
                            "value": {"encoding": "hex", "bytes": ""}});
        assert_eq!(status(&nested), "not_found");
    }

    #[test]
    fn address_bytes_serve_as_the_value_and_a_missing_value_is_unsupported() {
        let bytes = json!({"record_key": "addr:0", "record_family": "addr", "selector_key": "0",
                           "address_bytes_hex": "0x0102"});
        assert_eq!(
            entry(&bytes, None, false),
            Some(
                json!({"record_key": "addr:0", "record_family": "addr", "selector_key": "0",
                        "status": "success", "value": "0x0102"})
            )
        );
        let text = json!({"record_key": "text:url", "record_family": "text",
                          "selector_key": "url"});
        assert_eq!(
            entry(&text, None, false),
            Some(json!({"record_key": "text:url", "record_family": "text",
                        "selector_key": "url", "status": "unsupported",
                        "unsupported_reason": "value_not_retained_in_normalized_events"}))
        );
    }

    #[test]
    fn a_retained_success_without_a_value_column_is_not_unsupported() {
        let rebuilt = json!({"record_key": "text:url", "record_family": "text",
                             "selector_key": "url"});
        assert_eq!(
            entry(&rebuilt, Some("success"), false),
            Some(json!({"record_key": "text:url", "record_family": "text",
                        "selector_key": "url", "status": "success"}))
        );
    }

    #[test]
    fn the_forward_name_record_serves_its_retained_name() {
        let set = json!({"record_key": "name", "record_family": "name", "raw_name": "alice.eth"});
        assert_eq!(
            entry(&set, Some("unsupported"), false),
            Some(json!({"record_key": "name", "record_family": "name",
                        "status": "success", "value": "alice.eth"}))
        );
        assert_eq!(unsupported_family(&set), None);
        assert_eq!(selector(&set), None);
        let cleared = json!({"record_key": "name", "record_family": "name", "raw_name": ""});
        assert_eq!(
            entry(&cleared, Some("unsupported"), false),
            Some(json!({"record_key": "name", "record_family": "name", "status": "not_found"}))
        );
        let hex = json!({"record_key": "name", "record_family": "name",
                         "raw_name": {"encoding": "hex", "bytes": "0xff"}});
        assert_eq!(
            entry(&hex, None, false),
            Some(
                json!({"record_key": "name", "record_family": "name", "status": "success",
                        "value": {"encoding": "hex", "bytes": "0xff"}})
            )
        );
        let lost = json!({"record_key": "name", "record_family": "name"});
        assert_eq!(
            entry(&lost, None, false),
            Some(
                json!({"record_key": "name", "record_family": "name", "status": "unsupported",
                        "unsupported_reason": "value_not_retained_in_normalized_events"})
            )
        );
    }

    #[test]
    fn a_native_zero_coin_60_value_is_absent() {
        let zero = json!({"record_key": "addr:60", "record_family": "addr", "selector_key": "60",
                          "value": ZERO_ADDRESS});
        assert!(coin60_zero_address_is_absent(
            &zero,
            "ens_v1_resolver_l1",
            "ens_v1_registry_l1"
        ));
        assert!(!coin60_zero_address_is_absent(
            &zero,
            "ens_v1_resolver_l1",
            "ens_v2_registry_l1"
        ));
        assert_eq!(
            entry(&zero, Some("success"), true),
            Some(json!({"record_key": "addr:60", "record_family": "addr",
                        "selector_key": "60", "status": "not_found"}))
        );
    }
}
