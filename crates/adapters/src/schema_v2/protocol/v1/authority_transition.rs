use serde_json::{Value, json};

use super::super::{BindingClosureDraft, BindingDraft, EventDraft, Interpreted, ResourceDraft};
use crate::schema_v2::{
    common::{event_time, stable_uuid},
    model::RawLogInput,
    state::V1NameState,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn append_authority_transition(
    output: &mut Interpreted,
    authority_arm: &str,
    previous: Option<&V1NameState>,
    linked: Option<&V1NameState>,
    raw: &RawLogInput,
    observation_state: &Value,
    resolver: Option<String>,
    binding_active_from: Option<time::OffsetDateTime>,
) {
    if let Some(linked) = linked.filter(|state| state.token_lineage_id.is_none()) {
        output.resources.push(ResourceDraft {
            resource_id: linked.resource_id,
            token_lineage_id: None,
        });
    }
    if previous.map(|authority| authority.resource_id)
        == linked.map(|authority| authority.resource_id)
    {
        return;
    }
    if let Some(linked) = linked.filter(|authority| authority.surface_known) {
        output.bindings.push(BindingDraft {
            logical_name_id: linked.logical_name_id.clone(),
            resource_id: linked.resource_id,
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: authority_arm.to_owned(),
            surface_binding_id: linked.authority_key.as_ref().map(|authority_key| {
                stable_uuid(&format!(
                    "binding:{authority_key}:{}",
                    event_time(raw).unix_timestamp_nanos()
                ))
            }),
            active_from: binding_active_from,
        });
    } else if let Some(previous) = previous.filter(|authority| authority.surface_known) {
        output.binding_closures.push(BindingClosureDraft {
            logical_name_id: previous.logical_name_id.clone(),
            authority_arm: authority_arm.to_owned(),
        });
    }
    let logical_name_id = linked
        .filter(|authority| authority.surface_known || authority.token_lineage_id.is_some())
        .map(|authority| authority.logical_name_id.clone())
        .or_else(|| {
            previous
                .filter(|authority| authority.surface_known || authority.token_lineage_id.is_some())
                .map(|authority| authority.logical_name_id.clone())
        });
    let Some(logical_name_id) = logical_name_id else {
        return;
    };
    let source_event = observation_state
        .get("source_event")
        .and_then(Value::as_str)
        .unwrap_or("AuthorityTransferred");
    if let Some(previous) = previous.filter(|authority| authority.surface_known) {
        output.events.push(EventDraft {
            event_kind: "SurfaceUnbound".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(previous.resource_id),
            identity_suffix: format!("SurfaceUnbound:{source_event}:{}", previous.resource_id),
            explicit_before: Some(json!({
                "authority_kind":authority_kind(previous),
                "authority_key":previous.authority_key,
            })),
            after_state: merge_observation(
                observation_state,
                json!({
                    "source_event":source_event,
                    "authority_kind":authority_kind(previous),
                    "authority_key":previous.authority_key,
                    "active_to":raw.block_timestamp.unix_timestamp(),
                }),
            ),
            state_scope: String::new(),
        });
    }
    if let Some(linked) = linked.filter(|authority| authority.surface_known) {
        output.events.push(EventDraft {
            event_kind: "SurfaceBound".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(linked.resource_id),
            identity_suffix: format!("SurfaceBound:{source_event}:{}", linked.resource_id),
            explicit_before: Some(json!({})),
            after_state: merge_observation(
                observation_state,
                json!({
                    "source_event":source_event,
                    "authority_kind":authority_kind(linked),
                    "authority_key":linked.authority_key,
                    "active_from":raw.block_timestamp.unix_timestamp(),
                    "binding_kind":"declared_registry_path",
                }),
            ),
            state_scope: String::new(),
        });
    }
    output.events.push(EventDraft {
        event_kind: "AuthorityEpochChanged".to_owned(),
        logical_name_id: Some(logical_name_id.clone()),
        resource_id: linked
            .map(|authority| authority.resource_id)
            .or_else(|| previous.map(|authority| authority.resource_id)),
        identity_suffix: format!("AuthorityEpochChanged:{source_event}:{logical_name_id}"),
        explicit_before: Some(json!({
            "authority_kind":previous.map(authority_kind),
            "authority_key":previous.and_then(|authority| authority.authority_key.clone()),
        })),
        after_state: merge_observation(
            observation_state,
            json!({
                "source_event":source_event,
                "authority_kind":linked.map(authority_kind),
                "authority_key":linked.and_then(|authority| authority.authority_key.clone()),
            }),
        ),
        state_scope: String::new(),
    });
    if let (Some(linked), Some(resolver)) = (linked, resolver) {
        output.events.push(EventDraft {
            event_kind: "ResolverChanged".to_owned(),
            logical_name_id: Some(logical_name_id),
            resource_id: Some(linked.resource_id),
            identity_suffix: format!("ResolverChanged:authority:{source_event}:{resolver}"),
            explicit_before: Some(json!({"resolver":Value::Null})),
            after_state: merge_observation(
                observation_state,
                json!({
                    "source_event":"AuthorityEpochChanged",
                    "resolver":resolver,
                }),
            ),
            state_scope: String::new(),
        });
    }
}

fn merge_observation(observation: &Value, fields: Value) -> Value {
    let mut merged = observation.clone();
    merged
        .as_object_mut()
        .expect("authority observation is an object")
        .extend(
            fields
                .as_object()
                .expect("authority boundary fields are an object")
                .clone(),
        );
    merged
}

pub(super) fn authority_kind(authority: &V1NameState) -> &'static str {
    match authority.authority_source_family.as_str() {
        "ens_v1_wrapper_l1" => "wrapper",
        "ens_v1_registrar_l1" | "basenames_base_registrar" => "registrar",
        _ => "registry_only",
    }
}
