use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use bigname_manifests::DiscoveryObservation;
use bigname_storage::{
    NormalizedEvent, SurfaceBinding, SurfaceBindingKind, ens_v2_registry_resource_id,
};
use serde_json::json;
use sqlx::types::Uuid;

use super::{
    constants::*,
    discovery::{ens_v2_resolver_discovery_source, ens_v2_subregistry_discovery_source},
    names::{
        RegistryNameKey, RegistryTokenKey, closed_surface_binding_for_terminal,
        deactivate_registry_suffix, discovery_observation_key, insert_registry_name_state,
        name_under_registry, observe_name, remember_linked_resource_state, state_for_token_mut,
        take_states_for_name,
    },
    normalized::normalized_event,
    types::{ObservationRef, RegistryNameState, RegistryObservation, RegistryResourceLink},
    util::{deterministic_uuid, normalize_address, null_if_zero_address},
};

mod hydration;
mod terminal;
mod transfer;

pub(super) use hydration::hydrate_subregistry_event_target_ids;
use terminal::{
    append_terminal_discovery_observations, append_terminal_role_events,
    append_terminal_surface_unbound_event, apply_label_unregistered, apply_token_regenerated,
};
use transfer::apply_token_control_transferred;

pub(super) struct RegistryObservationContext<'a> {
    pub(super) registry_suffix_by_address: &'a mut HashMap<String, String>,
    pub(super) registry_contract_by_address: &'a mut HashMap<String, Uuid>,
    pub(super) states_by_registry_token: &'a mut BTreeMap<(String, String), RegistryNameState>,
    pub(super) state_keys_by_registry_namehash:
        &'a mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    pub(super) linked_resource_states: &'a mut BTreeMap<Uuid, RegistryNameState>,
    pub(super) closed_bindings: &'a mut BTreeMap<Uuid, SurfaceBinding>,
    pub(super) token_aliases: &'a mut HashMap<(String, String), (String, String)>,
    pub(super) current_token_alias_by_canonical_key:
        &'a mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    pub(super) observations: &'a mut Vec<DiscoveryObservation>,
    pub(super) graph_events: &'a mut Vec<NormalizedEvent>,
}

pub(super) fn apply_registry_observation(
    observation: RegistryObservation,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    match observation {
        RegistryObservation::LabelRegistered {
            token_id,
            labelhash,
            label,
            owner,
            expiry,
            sender,
            reference,
        } => {
            let Some(full_name) = name_under_registry(
                &reference.emitting_address,
                &label,
                context.registry_suffix_by_address,
            ) else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
            let Ok(name) = observe_name(&reference.namespace, &full_name, &reference, &label)
            else {
                return Ok(());
            };
            let state = RegistryNameState {
                token_id,
                labelhash,
                label,
                full_name,
                name,
                owner: Some(owner),
                expiry: Some(expiry),
                status: "registered",
                first_ref: reference.clone(),
                current_ref: reference.clone(),
                registry_address: reference.emitting_address.clone(),
                registry_contract_instance_id: reference.emitting_contract_instance_id,
                source_manifest_id: reference.source_manifest_id,
                source_family: reference.source_family.clone(),
                manifest_version: reference.manifest_version,
                resource: None,
                resolver: None,
                subregistry: None,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            };
            retire_replaced_name_states(
                &state,
                &reference,
                "LabelRegistered",
                "replacement_registration",
                false,
                context,
            );
            context.graph_events.push(normalized_event(
                &reference,
                Some(state.name.logical_name_id.clone()),
                None,
                EVENT_KIND_REGISTRATION_GRANTED,
                json!({}),
                json!({
                    "source_event": "LabelRegistered",
                    "status": "registered",
                    "token_id": state.token_id,
                    "label": state.label,
                    "labelhash": state.labelhash,
                    "registrant": state.owner,
                    "expiry": expiry,
                    "sender": sender,
                    "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                    "resource_pending": true,
                }),
                format!("label-registered:{}", state.token_id),
            ));
            insert_registry_name_state(
                context.states_by_registry_token,
                context.state_keys_by_registry_namehash,
                key,
                state,
            );
        }
        RegistryObservation::LabelReserved {
            token_id,
            labelhash,
            label,
            expiry,
            sender,
            reference,
        } => {
            let Some(full_name) = name_under_registry(
                &reference.emitting_address,
                &label,
                context.registry_suffix_by_address,
            ) else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
            let Ok(name) = observe_name(&reference.namespace, &full_name, &reference, &label)
            else {
                return Ok(());
            };
            let state = RegistryNameState {
                token_id: token_id.clone(),
                labelhash: labelhash.clone(),
                label,
                full_name,
                name,
                owner: None,
                expiry: Some(expiry),
                status: "reserved",
                first_ref: reference.clone(),
                current_ref: reference.clone(),
                registry_address: reference.emitting_address.clone(),
                registry_contract_instance_id: reference.emitting_contract_instance_id,
                source_manifest_id: reference.source_manifest_id,
                source_family: reference.source_family.clone(),
                manifest_version: reference.manifest_version,
                resource: None,
                resolver: None,
                subregistry: None,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            };
            retire_replaced_name_states(
                &state,
                &reference,
                "LabelReserved",
                "replacement_reservation",
                true,
                context,
            );
            insert_registry_name_state(
                context.states_by_registry_token,
                context.state_keys_by_registry_namehash,
                key,
                state,
            );
            context.graph_events.push(normalized_event(
                &reference,
                None,
                None,
                EVENT_KIND_REGISTRATION_RESERVED,
                json!({}),
                json!({
                    "source_event": "LabelReserved",
                    "status": "reserved",
                    "token_id": token_id,
                    "labelhash": labelhash,
                    "expiry": expiry,
                    "sender": sender,
                }),
                format!("label-reserved:{token_id}"),
            ));
        }
        RegistryObservation::LabelUnregistered {
            token_id,
            sender,
            reference,
        } => apply_label_unregistered(token_id, sender, reference, context),
        RegistryObservation::ExpiryUpdated {
            token_id,
            new_expiry,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before_expiry = state.expiry;
                state.expiry = Some(new_expiry);
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_EXPIRY_CHANGED,
                    json!({"expiry": before_expiry}),
                    json!({
                        "source_event": "ExpiryUpdated",
                        "token_id": token_id,
                        "expiry": new_expiry,
                        "sender": sender,
                    }),
                    format!("expiry-updated:{token_id}"),
                ));
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_REGISTRATION_RENEWED,
                    json!({"expiry": before_expiry}),
                    json!({
                        "source_event": "ExpiryUpdated",
                        "token_id": token_id,
                        "expiry": new_expiry,
                        "labelhash": state.labelhash,
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                    }),
                    format!("registration-renewed:{token_id}"),
                ));
            }
        }
        RegistryObservation::SubregistryUpdated {
            token_id,
            subregistry,
            sender,
            reference,
        } => {
            let mut logical_name_id = None;
            let mut resource_id = None;
            let observation_key = discovery_observation_key(&reference.emitting_address, &token_id);
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.subregistry.clone();
                if before.as_deref() != Some(subregistry.as_str()) {
                    deactivate_registry_suffix(
                        context.registry_suffix_by_address,
                        before.as_deref(),
                        &state.full_name,
                    );
                }
                state.subregistry = Some(subregistry.clone());
                state.current_ref = reference.clone();
                logical_name_id = Some(state.name.logical_name_id.clone());
                resource_id = state.resource.as_ref().map(|link| link.resource_id);
                if subregistry != ZERO_ADDRESS {
                    context
                        .registry_suffix_by_address
                        .insert(subregistry.clone(), state.full_name.clone());
                }
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
                    &reference,
                    logical_name_id.clone(),
                    resource_id,
                    EVENT_KIND_SUBREGISTRY_CHANGED,
                    json!({"subregistry": before}),
                    json!({
                        "source_event": "SubregistryUpdated",
                        "token_id": token_id,
                        "subregistry": null_if_zero_address(&subregistry),
                        "sender": sender,
                        "from_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                        "to_contract_instance_id": context.registry_contract_by_address
                            .get(&subregistry)
                            .map(ToString::to_string),
                    }),
                    format!("subregistry-updated:{token_id}"),
                ));
            }
            context.observations.push(DiscoveryObservation {
                chain: reference.chain_id.clone(),
                from_address: reference.emitting_address.clone(),
                to_address: subregistry.clone(),
                edge_kind: SUBREGISTRY_EDGE_KIND.to_owned(),
                discovery_source: ens_v2_subregistry_discovery_source(&reference.chain_id),
                active_from_block_number: Some(reference.block_number),
                active_from_block_hash: Some(reference.block_hash.clone()),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: json!({
                    "source": "raw_log",
                    "source_event": "SubregistryUpdated",
                    "observation_key": observation_key,
                    "token_id": token_id,
                    "from_address": reference.emitting_address,
                    "to_address": subregistry,
                    "logical_name_id": logical_name_id,
                    "resource_id": resource_id.map(|value| value.to_string()),
                    "chain_id": reference.chain_id,
                    "block_hash": reference.block_hash,
                    "block_number": reference.block_number,
                    "transaction_hash": reference.transaction_hash,
                    "transaction_index": reference.transaction_index,
                    "log_index": reference.log_index,
                    "tombstone": normalize_address(&subregistry) == ZERO_ADDRESS,
                }),
            });
        }
        RegistryObservation::ResolverUpdated {
            token_id,
            resolver,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.resolver.clone();
                state.resolver = Some(resolver.clone());
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_RESOLVER_CHANGED,
                    json!({"resolver": before}),
                    json!({
                        "source_event": "ResolverUpdated",
                        "token_id": token_id,
                        "resolver": null_if_zero_address(&resolver),
                        "sender": sender,
                    }),
                    format!("resolver-updated:{token_id}"),
                ));
                context.observations.push(DiscoveryObservation {
                    chain: reference.chain_id.clone(),
                    from_address: reference.emitting_address.clone(),
                    to_address: resolver.clone(),
                    edge_kind: RESOLVER_EDGE_KIND.to_owned(),
                    discovery_source: ens_v2_resolver_discovery_source(&reference.chain_id),
                    active_from_block_number: Some(reference.block_number),
                    active_from_block_hash: Some(reference.block_hash.clone()),
                    active_to_block_number: None,
                    active_to_block_hash: None,
                    provenance: json!({
                        "source": "raw_log",
                        "source_event": "ResolverUpdated",
                        "observation_key": format!("resolver:{}", discovery_observation_key(&reference.emitting_address, &token_id)),
                        "token_id": token_id,
                        "from_address": reference.emitting_address,
                        "to_address": resolver.clone(),
                        "logical_name_id": state.name.logical_name_id,
                        "resource_id": state.resource.as_ref().map(|link| link.resource_id.to_string()),
                        "chain_id": reference.chain_id,
                        "block_hash": reference.block_hash,
                        "block_number": reference.block_number,
                        "transaction_hash": reference.transaction_hash,
                        "transaction_index": reference.transaction_index,
                        "log_index": reference.log_index,
                        "tombstone": normalize_address(&resolver) == ZERO_ADDRESS,
                    }),
                });
            }
        }
        RegistryObservation::TokenResource {
            token_id,
            upstream_resource,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let resource_id = ens_v2_registry_resource_id(
                    &reference.chain_id,
                    reference.emitting_contract_instance_id,
                    &upstream_resource,
                );
                let token_lineage_id = deterministic_uuid(&format!(
                    "ens-v2-token-lineage:{}:{}:{}",
                    reference.chain_id, reference.emitting_contract_instance_id, upstream_resource
                ));
                let surface_binding_id = deterministic_uuid(&format!(
                    "ens-v2-surface-binding:{}:{}:{}:{}",
                    reference.chain_id,
                    reference.emitting_contract_instance_id,
                    upstream_resource,
                    state.name.logical_name_id
                ));
                state.resource = Some(RegistryResourceLink {
                    upstream_resource,
                    observed_token_id: token_id.clone(),
                    observed_expiry: state.expiry,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                    linked_ref: reference.clone(),
                });
                state.current_ref = reference;
                remember_linked_resource_state(context.linked_resource_states, state);
            }
        }
        RegistryObservation::TokenRegenerated {
            old_token_id,
            new_token_id,
            reference,
        } => apply_token_regenerated(old_token_id, new_token_id, reference, context),
        RegistryObservation::TokenControlTransferred {
            token_id,
            operator,
            from,
            to,
            amount,
            source_event,
            transfer_index,
            reference,
        } => apply_token_control_transferred(
            token_id,
            operator,
            from,
            to,
            amount,
            source_event,
            transfer_index,
            reference,
            context,
        ),
        RegistryObservation::ParentUpdated {
            parent,
            label,
            sender,
            reference,
        } => {
            if let Some(full_name) =
                name_under_registry(&parent, &label, context.registry_suffix_by_address)
            {
                context
                    .registry_suffix_by_address
                    .insert(reference.emitting_address.clone(), full_name.clone());
                context.graph_events.push(normalized_event(
                    &reference,
                    None,
                    None,
                    EVENT_KIND_PARENT_CHANGED,
                    json!({}),
                    json!({
                        "source_event": "ParentUpdated",
                        "parent": null_if_zero_address(&parent),
                        "label": label,
                        "registry_name": full_name,
                        "sender": sender,
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                        "parent_contract_instance_id": context.registry_contract_by_address
                            .get(&parent)
                            .map(ToString::to_string),
                    }),
                    format!("parent-updated:{}", reference.emitting_address),
                ));
            }
        }
    }

    Ok(())
}

fn retire_replaced_name_states(
    next_state: &RegistryNameState,
    reference: &ObservationRef,
    source_event: &str,
    terminal_reason: &str,
    close_surface_binding: bool,
    context: &mut RegistryObservationContext<'_>,
) {
    let replaced = take_states_for_name(
        context.states_by_registry_token,
        context.token_aliases,
        context.state_keys_by_registry_namehash,
        context.current_token_alias_by_canonical_key,
        &next_state.registry_address,
        &next_state.name.namehash,
    );
    let latest = replaced.iter().max_by_key(|state| {
        (
            state.current_ref.block_number,
            state.current_ref.transaction_index,
            state.current_ref.log_index,
        )
    });
    if let Some(latest) = latest {
        append_terminal_discovery_observations(
            latest,
            &next_state.token_id,
            reference,
            source_event,
            terminal_reason,
            context.observations,
        );
        append_terminal_role_events(
            latest,
            &next_state.token_id,
            reference,
            source_event,
            terminal_reason,
            context.graph_events,
        );
    }
    for state in replaced {
        if close_surface_binding {
            if let Some(binding) = closed_surface_binding_for_terminal(&state, reference) {
                context
                    .closed_bindings
                    .insert(binding.surface_binding_id, binding);
            }
            append_terminal_surface_unbound_event(
                &state,
                &next_state.token_id,
                reference,
                source_event,
                terminal_reason,
                context.graph_events,
            );
        }
        deactivate_registry_suffix(
            context.registry_suffix_by_address,
            state.subregistry.as_deref(),
            &state.full_name,
        );
    }
}
