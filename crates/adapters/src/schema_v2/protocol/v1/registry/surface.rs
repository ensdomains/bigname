use serde_json::Value;

use super::{authority_kind, merge_observation};
use crate::schema_v2::{
    common::{event_time, stable_uuid},
    model::RawLogInput,
    protocol::{BindingDraft, EventDraft, Interpreted},
    state::V1NameState,
};

pub(super) fn append_binding(
    output: &mut Interpreted,
    authority: &V1NameState,
    authority_arm: &str,
    raw: &RawLogInput,
    active_from: Option<time::OffsetDateTime>,
) {
    output.bindings.push(BindingDraft {
        logical_name_id: authority.logical_name_id.clone(),
        resource_id: authority.resource_id,
        binding_kind: "declared_registry_path".to_owned(),
        authority_arm: authority_arm.to_owned(),
        surface_binding_id: authority.authority_key.as_ref().map(|authority_key| {
            stable_uuid(&format!(
                "binding:{authority_key}:{}",
                event_time(raw).unix_timestamp_nanos()
            ))
        }),
        active_from,
    });
}

pub(super) fn append_bound_event(
    output: &mut Interpreted,
    authority: &V1NameState,
    raw: &RawLogInput,
    observation_state: &Value,
) {
    let source_event = observation_state
        .get("source_event")
        .and_then(Value::as_str)
        .unwrap_or("AuthorityTransferred");
    output.events.push(EventDraft {
        event_kind: "SurfaceBound".to_owned(),
        logical_name_id: Some(authority.logical_name_id.clone()),
        resource_id: Some(authority.resource_id),
        identity_suffix: format!("SurfaceBound:{source_event}:{}", authority.resource_id),
        explicit_before: Some(serde_json::json!({})),
        after_state: merge_observation(
            observation_state,
            serde_json::json!({
                "source_event":source_event,
                "authority_kind":authority_kind(authority),
                "authority_key":authority.authority_key,
                "active_from":raw.block_timestamp.unix_timestamp(),
                "binding_kind":"declared_registry_path",
            }),
        ),
        state_scope: String::new(),
    });
}
