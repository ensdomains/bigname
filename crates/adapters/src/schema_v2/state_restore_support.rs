use serde_json::Value;

use crate::schema_v2::{
    common::stable_uuid,
    model::PriorEventInput,
    state::{State, V1NameState, V1RegistryReadAnchor},
};

pub(super) fn missing_replacement_role(
    state: &State,
    emitter: &str,
    token: &str,
    event: &PriorEventInput,
    field: &str,
) -> bool {
    matches!(
        event
            .after_state
            .get("source_event")
            .and_then(Value::as_str),
        Some("LabelRegistered" | "LabelReserved")
    ) && event.after_state.get(field).is_some_and(Value::is_null)
        && state.v2_token(emitter, token).is_none()
}

pub(super) fn expiry_retirement_is_projection_only(event: &PriorEventInput) -> bool {
    [
        ("source_event", "RegistryPathExpired"),
        ("derived_from", "interpreter_state"),
        ("terminal_reason", "registry_name_binding_expired"),
    ]
    .into_iter()
    .all(|(key, value)| event.after_state.get(key).and_then(Value::as_str) == Some(value))
}

pub(super) fn raw_label(after_state: &Value) -> Option<Vec<u8>> {
    after_state
        .get("raw_label_hex")
        .and_then(Value::as_str)
        .and_then(|value| alloy_primitives::hex::decode(value).ok())
        .or_else(|| {
            after_state
                .get("label")
                .and_then(Value::as_str)
                .map(|label| label.as_bytes().to_vec())
        })
        .or_else(|| {
            after_state
                .get("raw_labels")
                .and_then(Value::as_array)
                .and_then(|labels| labels.first())
                .and_then(Value::as_str)
                .map(|label| label.as_bytes().to_vec())
        })
}

pub(super) fn v1_registry_read_anchor(
    event: &PriorEventInput,
    namehash: &str,
) -> V1RegistryReadAnchor {
    V1RegistryReadAnchor {
        logical_name_id: event
            .logical_name_id
            .clone()
            .unwrap_or_else(|| format!("{}:{namehash}", event.namespace)),
        surface_known: event.logical_name_id.is_some(),
        resource_id: stable_uuid(&format!(
            "resource:registry-only:{}:{namehash}",
            event.chain_id
        )),
        source_family: event.source_family.clone(),
        source_manifest_id: event.source_manifest_id,
        registry_contract: event.emitting_address.as_deref().map(str::to_lowercase),
    }
}

pub(super) fn v1_registry_authority(
    event: &PriorEventInput,
    namehash: &str,
    owner_getter: &str,
    anchor: &V1RegistryReadAnchor,
) -> V1NameState {
    V1NameState {
        logical_name_id: anchor.logical_name_id.clone(),
        surface_known: anchor.surface_known,
        resource_id: anchor.resource_id,
        token_lineage_id: None,
        authority_source_family: event.source_family.clone(),
        source_manifest_id: event.source_manifest_id,
        labelhash: event
            .after_state
            .get("labelhash")
            .and_then(Value::as_str)
            .map(str::to_owned),
        expiry: None,
        owner: Some(owner_getter.to_owned()),
        registry_contract: event.emitting_address.as_deref().map(str::to_lowercase),
        authority_key: Some(format!("registry-only:{}:{namehash}", event.chain_id)),
        wrapper_fallback: false,
    }
}

pub(super) fn parse_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

pub(super) fn parse_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

pub(super) fn parse_u32(value: &Value) -> Option<u32> {
    parse_u64(value).and_then(|value| u32::try_from(value).ok())
}
