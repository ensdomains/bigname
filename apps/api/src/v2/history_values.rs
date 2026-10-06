//! Retained history values and the event-local ENSv2 canonical key.
mod details;

use alloy_primitives::U256;
use bigname_storage::HistoryEvent;
use serde_json::{Map, Value, json};

use super::{HistoryEventType, history_context::HistoryRowContext, slug_to_numeric};

pub(super) fn append(
    data: &mut Map<String, Value>,
    row: &HistoryEvent,
    event_type: HistoryEventType,
    context: &HistoryRowContext,
) {
    details::append(data, row);
    if let Some(canonical) = canonical_id(row) {
        data.insert("canonical_id".into(), Value::String(canonical));
    }
    if event_type == HistoryEventType::Transfer
        && matches!(
            row.source_family.as_str(),
            "ens_v1_wrapper_l1" | "ens_v2_registry_l1" | "ens_v2_root_l1"
        )
        && let Some(operator) = address(row.after_state.get("operator"))
    {
        data.insert("operator".into(), Value::String(operator));
    }
    let source_event = row.after_state["source_event"].as_str();
    let supplement = context.payment(row);
    let registration = event_type == HistoryEventType::Registration
        && (supplement.is_some()
            || (row.source_family == "ens_v1_registrar_l1"
                && source_event == Some("NameRegistered")));
    let renewal = event_type == HistoryEventType::Renewal
        && source_event == Some("NameRenewed")
        && matches!(
            row.source_family.as_str(),
            "ens_v1_registrar_l1" | "ens_v2_registrar_l1"
        );
    if !registration && !renewal {
        return;
    }
    let values = supplement.unwrap_or(&row.after_state);
    let v2 = row.source_family.starts_with("ens_v2_");
    if registration {
        amount(data, "cost", values.get("cost"));
        amount(
            data,
            "base_cost",
            values.get(if v2 { "base" } else { "base_cost" }),
        );
        amount(data, "premium", values.get("premium"));
    } else {
        let cost = if v2 {
            present(values.get("amount")).or_else(|| values.get("base"))
        } else {
            values.get("cost")
        };
        amount(data, "cost", cost);
    }
    if let Some(referrer) = fixed_hex(values.get("referrer"), 32) {
        data.insert("referrer".into(), Value::String(referrer));
    }
    if v2
        && let Some(address) = address(values.get("payment_token"))
        && let Some(chain) = row.chain_id.as_deref().and_then(slug_to_numeric)
    {
        data.insert(
            "payment_token".into(),
            json!({"chain_id":chain,"address":address}),
        );
    }
}

fn amount(data: &mut Map<String, Value>, key: &str, value: Option<&Value>) {
    if let Some(value) = value.and_then(uint256) {
        data.insert(key.into(), Value::String(value.to_string()));
    }
}

fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

fn uint256(value: &Value) -> Option<U256> {
    let word = value.as_str()?;
    if let Some(hex) = word.strip_prefix("0x") {
        if hex.is_empty() || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        U256::from_str_radix(hex, 16).ok()
    } else {
        if word.is_empty() || !word.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        U256::from_str_radix(word, 10).ok()
    }
}

fn address(value: Option<&Value>) -> Option<String> {
    fixed_hex(value, 20)
}

fn fixed_hex(value: Option<&Value>, bytes: usize) -> Option<String> {
    let word = value?.as_str()?;
    let hex = word.strip_prefix("0x")?;
    (hex.len() == bytes * 2 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| word.to_ascii_lowercase())
}

fn canonical_id(row: &HistoryEvent) -> Option<String> {
    if !matches!(
        row.source_family.as_str(),
        "ens_v2_registry_l1" | "ens_v2_root_l1" | "ens_v2_registrar_l1"
    ) || row.after_state["root_resource"] == true
        || row.after_state["scope"]["kind"] == "registry_root"
    {
        return None;
    }
    let after = &row.after_state;
    let primary = present(after.get("token_id")).or_else(|| match row.event_kind.as_str() {
        "PermissionChanged" => present(after.get("upstream_resource")),
        "RegistrationGranted" | "LabelRegistered" => present(after.get("labelhash")),
        _ => None,
    })?;
    let mask = !U256::from(u32::MAX);
    let key = uint256(primary)? & mask;
    // A malformed or contradictory retained identifier cannot be replaced by another field.
    for name in [
        "token_id",
        "current_token_id",
        "upstream_resource",
        "labelhash",
    ] {
        if let Some(value) = present(after.get(name))
            && (uint256(value)? & mask) != key
        {
            return None;
        }
    }
    Some(key.to_string())
}
