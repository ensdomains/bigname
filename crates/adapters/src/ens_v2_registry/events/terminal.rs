use bigname_manifests::DiscoveryObservation;
use serde_json::json;

use super::RegistryObservationContext;
use crate::ens_v2_registry::{
    constants::{
        EVENT_KIND_REGISTRATION_RELEASED, EVENT_KIND_RESOLVER_CHANGED,
        EVENT_KIND_SUBREGISTRY_CHANGED, EVENT_KIND_SURFACE_UNBOUND, EVENT_KIND_TOKEN_REGENERATED,
        RESOLVER_EDGE_KIND, SUBREGISTRY_EDGE_KIND, ZERO_ADDRESS,
    },
    discovery::{ens_v2_resolver_discovery_source, ens_v2_subregistry_discovery_source},
    names::{
        closed_surface_binding_for_terminal, deactivate_registry_suffix,
        remember_linked_resource_state, replace_token_alias, resolve_token_key,
        take_state_for_unregister,
    },
    normalized::normalized_event,
    types::{ObservationRef, RegistryNameState},
    util::normalize_address,
};

pub(super) fn apply_label_unregistered(
    token_id: String,
    sender: String,
    reference: ObservationRef,
    context: &mut RegistryObservationContext<'_>,
) {
    let Some(mut state) = take_state_for_unregister(
        context.states_by_registry_token,
        context.token_aliases,
        &reference.emitting_address,
        &token_id,
    ) else {
        return;
    };
    append_terminal_discovery_observations(
        &state,
        &token_id,
        &reference,
        "LabelUnregistered",
        "unregistered",
        context.observations,
    );
    append_terminal_role_events(
        &state,
        &token_id,
        &reference,
        "LabelUnregistered",
        "unregistered",
        context.graph_events,
    );
    if let Some(binding) = closed_surface_binding_for_terminal(&state, &reference) {
        context
            .closed_bindings
            .insert(binding.surface_binding_id, binding);
    }
    state.status = "unregistered";
    state.current_ref = reference.clone();
    deactivate_registry_suffix(
        context.registry_suffix_by_address,
        state.subregistry.as_deref(),
        &state.full_name,
    );
    context.graph_events.push(normalized_event(
        &reference,
        Some(state.name.logical_name_id.clone()),
        state.resource.as_ref().map(|link| link.resource_id),
        EVENT_KIND_REGISTRATION_RELEASED,
        json!({"status": "registered"}),
        json!({
            "source_event": "LabelUnregistered",
            "status": "unregistered",
            "token_id": token_id,
            "sender": sender,
            "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
        }),
        format!("label-unregistered:{token_id}"),
    ));
}

pub(super) fn append_terminal_role_events(
    state: &RegistryNameState,
    observed_token_id: &str,
    reference: &ObservationRef,
    source_event: &str,
    terminal_reason: &str,
    graph_events: &mut Vec<bigname_storage::NormalizedEvent>,
) {
    let logical_name_id = Some(state.name.logical_name_id.clone());
    let resource_id = state.resource.as_ref().map(|link| link.resource_id);
    if state
        .subregistry
        .as_deref()
        .is_some_and(|target| normalize_address(target) != ZERO_ADDRESS)
    {
        graph_events.push(normalized_event(
            reference,
            logical_name_id.clone(),
            resource_id,
            EVENT_KIND_SUBREGISTRY_CHANGED,
            json!({"subregistry": state.subregistry}),
            json!({
                "source_event": source_event,
                "terminal_reason": terminal_reason,
                "token_id": observed_token_id,
                "subregistry": null,
                "from_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                "to_contract_instance_id": null,
            }),
            format!("terminal-subregistry:{source_event}:{observed_token_id}"),
        ));
    }
    if state
        .resolver
        .as_deref()
        .is_some_and(|target| normalize_address(target) != ZERO_ADDRESS)
    {
        graph_events.push(normalized_event(
            reference,
            logical_name_id,
            resource_id,
            EVENT_KIND_RESOLVER_CHANGED,
            json!({"resolver": state.resolver}),
            json!({
                "source_event": source_event,
                "terminal_reason": terminal_reason,
                "token_id": observed_token_id,
                "resolver": null,
            }),
            format!("terminal-resolver:{source_event}:{observed_token_id}"),
        ));
    }
}

pub(super) fn append_terminal_surface_unbound_event(
    state: &RegistryNameState,
    observed_token_id: &str,
    reference: &ObservationRef,
    source_event: &str,
    terminal_reason: &str,
    graph_events: &mut Vec<bigname_storage::NormalizedEvent>,
) {
    let Some(link) = state.resource.as_ref() else {
        return;
    };
    graph_events.push(normalized_event(
        reference,
        Some(state.name.logical_name_id.clone()),
        Some(link.resource_id),
        EVENT_KIND_SURFACE_UNBOUND,
        json!({
            "binding_kind": state.binding_kind.as_str(),
            "surface_binding_id": link.surface_binding_id.to_string(),
            "resource_id": link.resource_id.to_string(),
        }),
        json!({
            "source_event": source_event,
            "terminal_reason": terminal_reason,
            "token_id": observed_token_id,
            "binding_kind": state.binding_kind.as_str(),
            "surface_binding_id": link.surface_binding_id.to_string(),
            "resource_id": link.resource_id.to_string(),
        }),
        format!(
            "terminal-surface-unbound:{source_event}:{}:{observed_token_id}",
            link.surface_binding_id
        ),
    ));
}

pub(super) fn append_terminal_discovery_observations(
    state: &RegistryNameState,
    observed_token_id: &str,
    reference: &ObservationRef,
    source_event: &str,
    terminal_reason: &str,
    observations: &mut Vec<DiscoveryObservation>,
) {
    for (target, edge_kind, discovery_source, observation_key) in [
        (
            state.subregistry.as_deref(),
            SUBREGISTRY_EDGE_KIND,
            ens_v2_subregistry_discovery_source(&reference.chain_id),
            format!("{}:{}", reference.emitting_address, state.name.namehash),
        ),
        (
            state.resolver.as_deref(),
            RESOLVER_EDGE_KIND,
            ens_v2_resolver_discovery_source(&reference.chain_id),
            format!(
                "resolver:{}:{}",
                reference.emitting_address, state.name.namehash
            ),
        ),
    ] {
        let Some(target) = target else {
            continue;
        };
        if normalize_address(target) == ZERO_ADDRESS {
            continue;
        }
        observations.push(DiscoveryObservation {
            chain: reference.chain_id.clone(),
            from_address: reference.emitting_address.clone(),
            to_address: ZERO_ADDRESS.to_owned(),
            edge_kind: edge_kind.to_owned(),
            discovery_source,
            active_from_block_number: Some(reference.block_number),
            active_from_block_hash: Some(reference.block_hash.clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "source": "raw_log",
                "source_event": source_event,
                "terminal_reason": terminal_reason,
                "observation_key": observation_key,
                "token_id": observed_token_id,
                "replaced_token_id": state.token_id,
                "from_address": reference.emitting_address,
                "to_address": ZERO_ADDRESS,
                "previous_to_address": target,
                "logical_name_id": state.name.logical_name_id,
                "resource_id": state.resource.as_ref().map(|link| link.resource_id.to_string()),
                "chain_id": reference.chain_id,
                "block_hash": reference.block_hash,
                "block_number": reference.block_number,
                "transaction_hash": reference.transaction_hash,
                "transaction_index": reference.transaction_index,
                "log_index": reference.log_index,
                "tombstone": true,
            }),
        });
    }
}

pub(super) fn apply_token_regenerated(
    old_token_id: String,
    new_token_id: String,
    reference: ObservationRef,
    context: &mut RegistryObservationContext<'_>,
) {
    let canonical_key = resolve_token_key(
        context.token_aliases,
        &reference.emitting_address,
        &old_token_id,
    )
    .unwrap_or_else(|| (reference.emitting_address.clone(), old_token_id.clone()));
    let Some(state) = context.states_by_registry_token.get_mut(&canonical_key) else {
        return;
    };
    let previous_token_id = state.token_id.clone();
    state.token_id = new_token_id.clone();
    state.current_ref = reference.clone();
    remember_linked_resource_state(context.linked_resource_states, state);
    replace_token_alias(
        context.token_aliases,
        &reference.emitting_address,
        &new_token_id,
        &canonical_key,
    );
    context.graph_events.push(normalized_event(
        &reference,
        Some(state.name.logical_name_id.clone()),
        state.resource.as_ref().map(|link| link.resource_id),
        EVENT_KIND_TOKEN_REGENERATED,
        json!({"token_id": previous_token_id}),
        json!({
            "source_event": "TokenRegenerated",
            "old_token_id": old_token_id,
            "new_token_id": new_token_id,
            "resource_id": state.resource.as_ref().map(|link| link.resource_id.to_string()),
        }),
        format!("token-regenerated:{old_token_id}:{new_token_id}"),
    ));
}
