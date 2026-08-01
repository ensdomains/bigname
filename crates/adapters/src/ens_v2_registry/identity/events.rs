use bigname_storage::NormalizedEvent;
use serde_json::json;

use crate::ens_v2_registry::{
    constants::{
        EVENT_KIND_AUTHORITY_TRANSFERRED, EVENT_KIND_EXPIRY_CHANGED,
        EVENT_KIND_REGISTRATION_GRANTED, EVENT_KIND_SURFACE_BOUND,
        EVENT_KIND_TOKEN_RESOURCE_LINKED,
    },
    normalized::normalized_event,
    types::{RegistryNameState, RegistryResourceLink},
};

pub(in crate::ens_v2_registry) fn build_resource_events(
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Vec<NormalizedEvent> {
    let mut events = Vec::new();
    let initial_binding = link.binding_ref == link.linked_ref;
    if initial_binding {
        events.push(build_token_resource_linked_event(state, link));
    }
    let mut surface_bound_after = json!({
        "binding_kind": state.binding_kind.as_str(),
        "surface_binding_id": link.surface_binding_id.to_string(),
        "logical_name_id": state.name.logical_name_id,
        "resource_id": link.resource_id.to_string(),
        "upstream_resource": link.upstream_resource,
        "token_id": link.observed_token_id,
        "current_token_id": link.observed_token_id,
    });
    if !initial_binding {
        surface_bound_after["source_event"] = json!(link.binding_source_event);
    }
    events.push(normalized_event(
        &link.binding_ref,
        Some(state.name.logical_name_id.clone()),
        Some(link.resource_id),
        EVENT_KIND_SURFACE_BOUND,
        json!({}),
        surface_bound_after,
        format!("surface-bound:{}", link.surface_binding_id),
    ));
    let fact_reference = if initial_binding {
        &state.first_ref
    } else {
        &link.binding_ref
    };
    let event_suffix = |kind: &str| {
        if initial_binding {
            format!("{kind}:{}", link.upstream_resource)
        } else {
            format!("{kind}-rebound:{}", link.surface_binding_id)
        }
    };
    if state.status == "registered" {
        events.push(normalized_event(
            fact_reference,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_REGISTRATION_GRANTED,
            json!({}),
            json!({
                "authority_kind": "ens_v2_registry",
                "authority_key": format!(
                    "ens-v2-registry:{}:{}:{}",
                    state.first_ref.chain_id, state.registry_contract_instance_id, link.upstream_resource
                ),
                "registrant": state.owner,
                "expiry": link.observed_expiry,
                "labelhash": state.labelhash,
                "token_id": link.observed_token_id,
                "current_token_id": link.observed_token_id,
                "upstream_resource": link.upstream_resource,
                "status": "registered",
                "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
            }),
            event_suffix("registration-granted"),
        ));
        events.push(normalized_event(
            fact_reference,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_AUTHORITY_TRANSFERRED,
            json!({}),
            json!({
                "owner": state.owner,
                "token_id": link.observed_token_id,
                "current_token_id": link.observed_token_id,
                "upstream_resource": link.upstream_resource,
            }),
            event_suffix("authority-transferred"),
        ));
    }
    if let Some(expiry) = link.observed_expiry {
        events.push(normalized_event(
            fact_reference,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_EXPIRY_CHANGED,
            json!({}),
            json!({
                "expiry": expiry,
                "token_id": link.observed_token_id,
                "current_token_id": link.observed_token_id,
                "upstream_resource": link.upstream_resource,
            }),
            event_suffix("expiry-current"),
        ));
    }
    events
}

pub(in crate::ens_v2_registry) fn build_token_resource_linked_event(
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> NormalizedEvent {
    normalized_event(
        &link.linked_ref,
        link.linked_logical_name_id.clone(),
        Some(link.resource_id),
        EVENT_KIND_TOKEN_RESOURCE_LINKED,
        json!({}),
        json!({
            "source_event": "TokenResource",
            "token_id": link.observed_token_id,
            "upstream_resource": link.upstream_resource,
            "resource_id": link.resource_id.to_string(),
            "token_lineage_id": link.token_lineage_id.to_string(),
            "current_token_id": link.observed_token_id,
            "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
        }),
        format!("token-resource-linked:{}", link.upstream_resource),
    )
}
