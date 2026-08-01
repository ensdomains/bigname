use std::collections::{BTreeSet, HashMap};

use anyhow::Result;
use serde_json::json;

use crate::ens_v2_registry::{
    constants::{EVENT_KIND_PARENT_CHANGED, EVENT_KIND_REGISTRATION_RELEASED, ZERO_ADDRESS},
    events::terminal::append_terminal_surface_unbound_event,
    names::{
        closed_surface_binding_for_terminal, index_registry_name_state, name_with_suffix,
        normalized_label, observe_name, recompute_registry_suffixes,
        remember_linked_resource_state, unindex_registry_name_state, versionless_token_id,
    },
    normalized::normalized_event,
    types::{CurrentSubregistryLink, ObservationRef, RegistryEntryTopology, RegistryObservation},
    util::{deterministic_uuid, normalize_address},
};

use super::RegistryObservationContext;

pub(super) fn refresh_expired_topology_before_non_topology_event(
    observation: &RegistryObservation,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let (reference, source_event) = match observation {
        RegistryObservation::RegistryCreated { reference } => (reference, "RegistryCreated"),
        RegistryObservation::Upgraded { reference, .. } => (reference, "Upgraded"),
        RegistryObservation::ResolverUpdated { reference, .. } => (reference, "ResolverUpdated"),
        RegistryObservation::TokenResource { reference, .. } => (reference, "TokenResource"),
        RegistryObservation::TokenRegenerated { reference, .. } => (reference, "TokenRegenerated"),
        RegistryObservation::TokenControlTransferred {
            reference,
            source_event,
            ..
        } => (reference, *source_event),
        RegistryObservation::LabelRegistered { .. }
        | RegistryObservation::LabelReserved { .. }
        | RegistryObservation::LabelUnregistered { .. }
        | RegistryObservation::ExpiryUpdated { .. }
        | RegistryObservation::SubregistryUpdated { .. }
        | RegistryObservation::ParentUpdated { .. } => return Ok(()),
    };
    refresh_registry_suffixes(reference, source_event, None, context)
}

pub(super) fn apply_label_topology(
    token_id: &str,
    label: &str,
    expiry: u64,
    reference: &ObservationRef,
    source_event: &str,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let registry = &reference.emitting_address;
    let normalized = normalized_label(label);
    let replaced_labels = context
        .entry_topology_by_registry_token
        .iter()
        .filter(|((entry_registry, _), entry)| {
            entry_registry == registry
                && (entry.label == label
                    || normalized.as_ref().is_some_and(|normalized| {
                        normalized_label(&entry.label).as_ref() == Some(normalized)
                    }))
        })
        .map(|(_, entry)| entry.label.clone())
        .collect::<BTreeSet<_>>();
    context
        .entry_topology_by_registry_token
        .retain(|(entry_registry, _), entry| {
            entry_registry != registry || !replaced_labels.contains(&entry.label)
        });
    for replaced_label in replaced_labels {
        context
            .current_subregistry_by_parent_label
            .remove(&(registry.clone(), replaced_label));
    }
    context.entry_topology_by_registry_token.insert(
        topology_key(registry, token_id),
        RegistryEntryTopology {
            label: label.to_owned(),
            expiry: Some(expiry),
            subregistry: None,
        },
    );
    refresh_registry_suffixes(reference, source_event, None, context)
}

pub(super) fn apply_expiry_topology(
    token_id: &str,
    new_expiry: u64,
    reference: &ObservationRef,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let key = topology_key(&reference.emitting_address, token_id);
    let Some(entry) = context.entry_topology_by_registry_token.get_mut(&key) else {
        return refresh_registry_suffixes(reference, "ExpiryUpdated", None, context);
    };
    entry.expiry = Some(new_expiry);
    let link_key = (reference.emitting_address.clone(), entry.label.clone());
    if let Some(link) = context
        .current_subregistry_by_parent_label
        .get_mut(&link_key)
        && entry.subregistry.as_deref() == Some(link.subregistry.as_str())
    {
        link.expiry = Some(new_expiry);
    }
    refresh_registry_suffixes(reference, "ExpiryUpdated", None, context)
}

pub(super) fn apply_subregistry_topology(
    token_id: &str,
    subregistry: &str,
    reference: &ObservationRef,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let key = topology_key(&reference.emitting_address, token_id);
    let Some(entry) = context.entry_topology_by_registry_token.get_mut(&key) else {
        return refresh_registry_suffixes(reference, "SubregistryUpdated", None, context);
    };
    entry.subregistry = (subregistry != ZERO_ADDRESS).then(|| subregistry.to_owned());
    let link_key = (reference.emitting_address.clone(), entry.label.clone());
    if subregistry == ZERO_ADDRESS {
        context
            .current_subregistry_by_parent_label
            .remove(&link_key);
    } else {
        context.current_subregistry_by_parent_label.insert(
            link_key,
            CurrentSubregistryLink {
                subregistry: subregistry.to_owned(),
                expiry: entry.expiry,
            },
        );
    }
    refresh_registry_suffixes(reference, "SubregistryUpdated", None, context)
}

pub(super) fn apply_unregister_topology(
    token_id: &str,
    reference: &ObservationRef,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let key = topology_key(&reference.emitting_address, token_id);
    if let Some(entry) = context.entry_topology_by_registry_token.remove(&key) {
        context
            .current_subregistry_by_parent_label
            .remove(&(reference.emitting_address.clone(), entry.label));
    }
    refresh_registry_suffixes(reference, "LabelUnregistered", None, context)
}

pub(super) fn refresh_registry_suffixes(
    reference: &ObservationRef,
    source_event: &str,
    skip_parent_history_for: Option<&str>,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let previous_suffixes = context.registry_suffix_by_address.clone();
    recompute_registry_suffixes(
        context.registry_suffix_by_address,
        context.root_registry_addresses,
        context.current_subregistry_by_parent_label,
        context.current_parent_claim_by_registry,
        reference,
    );
    let changed_registries = changed_registry_addresses(
        &previous_suffixes,
        context.registry_suffix_by_address,
        context.root_registry_addresses,
    );
    rebind_registry_name_states(&changed_registries, reference, source_event, context)?;
    append_topology_parent_changes(
        &changed_registries,
        &previous_suffixes,
        reference,
        source_event,
        skip_parent_history_for,
        context,
    );
    Ok(())
}

fn changed_registry_addresses(
    previous_suffixes: &HashMap<String, String>,
    current_suffixes: &HashMap<String, String>,
    roots: &std::collections::HashSet<String>,
) -> BTreeSet<String> {
    previous_suffixes
        .keys()
        .chain(current_suffixes.keys())
        .filter(|address| !roots.contains(*address))
        .filter(|address| previous_suffixes.get(*address) != current_suffixes.get(*address))
        .cloned()
        .collect()
}

fn rebind_registry_name_states(
    changed_registries: &BTreeSet<String>,
    reference: &ObservationRef,
    source_event: &str,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let state_keys = context
        .states_by_registry_token
        .iter()
        .filter(|(_, state)| changed_registries.contains(&state.registry_address))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();

    for key in state_keys {
        let Some(previous_state) = context.states_by_registry_token.get(&key).cloned() else {
            continue;
        };
        let next_full_name = context
            .registry_suffix_by_address
            .get(&previous_state.registry_address)
            .and_then(|suffix| name_with_suffix(&previous_state.label, suffix));
        if previous_state.name_reachable
            && next_full_name.as_deref() == Some(previous_state.full_name.as_str())
        {
            continue;
        }

        if previous_state.name_reachable {
            unindex_registry_name_state(
                context.state_keys_by_registry_namehash,
                &key,
                &previous_state.registry_address,
                &previous_state.name.namehash,
            );
            if let Some(link) = previous_state.resource.as_ref() {
                if let Some(binding) =
                    closed_surface_binding_for_terminal(&previous_state, reference)
                {
                    context
                        .closed_bindings
                        .insert(binding.surface_binding_id, binding);
                }
                context
                    .retired_binding_states
                    .insert(link.surface_binding_id, previous_state.clone());
                append_terminal_surface_unbound_event(
                    &previous_state,
                    &previous_state.token_id,
                    reference,
                    source_event,
                    "registry_name_binding_changed",
                    context.graph_events,
                );
                context.graph_events.push(normalized_event(
                    reference,
                    Some(previous_state.name.logical_name_id.clone()),
                    Some(link.resource_id),
                    EVENT_KIND_REGISTRATION_RELEASED,
                    json!({"status": previous_state.status}),
                    json!({
                        "source_event": source_event,
                        "terminal_reason": "registry_name_binding_changed",
                        "status": "released",
                        "token_id": previous_state.token_id,
                        "registry_contract_instance_id": previous_state
                            .registry_contract_instance_id
                            .to_string(),
                    }),
                    format!("registry-name-released:{}", link.surface_binding_id),
                ));
            }
        }

        let next_name = next_full_name.as_deref().and_then(|full_name| {
            observe_name(
                &reference.namespace,
                full_name,
                reference,
                &previous_state.label,
            )
            .ok()
        });
        let Some(state) = context.states_by_registry_token.get_mut(&key) else {
            continue;
        };
        state.name_reachable = false;
        state.current_ref = reference.clone();
        if let Some(next_name) = next_name {
            let next_full_name = next_full_name.expect("observed rebound name has a full name");
            state.full_name = next_full_name;
            state.name = next_name;
            state.name_reachable = true;
            if let Some(link) = state.resource.as_mut() {
                link.surface_binding_id = deterministic_uuid(&format!(
                    "ens-v2-surface-binding-rebound:{}:{}:{}:{}:{}:{}:{}",
                    reference.chain_id,
                    state.registry_contract_instance_id,
                    link.upstream_resource,
                    state.name.logical_name_id,
                    reference.block_hash,
                    reference.transaction_index,
                    reference.log_index,
                ));
                link.binding_ref = reference.clone();
                link.binding_source_event = source_event.to_owned();
                link.observed_expiry = state.expiry;
            }
            index_registry_name_state(
                context.state_keys_by_registry_namehash,
                &key,
                &state.registry_address,
                &state.name.namehash,
            );
        }
        remember_linked_resource_state(context.linked_resource_states, state);
        let rebound_state = state.clone();
        append_rebound_pointer_events(&rebound_state, reference, source_event, context);
    }
    Ok(())
}

fn append_rebound_pointer_events(
    state: &crate::ens_v2_registry::types::RegistryNameState,
    reference: &ObservationRef,
    source_event: &str,
    context: &mut RegistryObservationContext<'_>,
) {
    if !state.name_reachable {
        return;
    }
    let resource_id = state.resource.as_ref().map(|link| link.resource_id);
    if let Some(subregistry) = state
        .subregistry
        .as_deref()
        .filter(|target| normalize_address(target) != ZERO_ADDRESS)
    {
        context.graph_events.push(normalized_event(
            reference,
            Some(state.name.logical_name_id.clone()),
            resource_id,
            crate::ens_v2_registry::constants::EVENT_KIND_SUBREGISTRY_CHANGED,
            json!({}),
            json!({
                "source_event": source_event,
                "token_id": state.token_id,
                "subregistry": subregistry,
                "from_contract_instance_id": state.registry_contract_instance_id.to_string(),
                "to_contract_instance_id": context.registry_contract_by_address
                    .get(subregistry)
                    .map(ToString::to_string),
            }),
            format!(
                "subregistry-rebound:{}:{}",
                state.registry_address, state.name.logical_name_id
            ),
        ));
    }
    if let Some(resolver) = state
        .resolver
        .as_deref()
        .filter(|target| normalize_address(target) != ZERO_ADDRESS)
    {
        context.graph_events.push(normalized_event(
            reference,
            Some(state.name.logical_name_id.clone()),
            resource_id,
            crate::ens_v2_registry::constants::EVENT_KIND_RESOLVER_CHANGED,
            json!({}),
            json!({
                "source_event": source_event,
                "token_id": state.token_id,
                "resolver": resolver,
            }),
            format!(
                "resolver-rebound:{}:{}",
                state.registry_address, state.name.logical_name_id
            ),
        ));
    }
}

fn append_topology_parent_changes(
    changed_registries: &BTreeSet<String>,
    previous_suffixes: &HashMap<String, String>,
    reference: &ObservationRef,
    source_event: &str,
    skip_parent_history_for: Option<&str>,
    context: &mut RegistryObservationContext<'_>,
) {
    for registry in changed_registries {
        if skip_parent_history_for == Some(registry.as_str()) {
            continue;
        }
        let Some(claim) = context.current_parent_claim_by_registry.get(registry) else {
            continue;
        };
        let Some(registry_contract_instance_id) =
            context.registry_contract_by_address.get(registry)
        else {
            continue;
        };
        context.graph_events.push(normalized_event(
            reference,
            None,
            None,
            EVENT_KIND_PARENT_CHANGED,
            json!({
                "parent": claim.parent,
                "label": claim.label,
                "registry_name": previous_suffixes.get(registry),
            }),
            json!({
                "source_event": source_event,
                "parent": claim.parent,
                "label": claim.label,
                "registry_name": context.registry_suffix_by_address.get(registry),
                "registry_contract_instance_id": registry_contract_instance_id.to_string(),
                "parent_contract_instance_id": context.registry_contract_by_address
                    .get(&claim.parent)
                    .map(ToString::to_string),
                "trigger_emitting_address": reference.emitting_address,
            }),
            format!("parent-topology:{source_event}:{registry}"),
        ));
    }
}

fn topology_key(registry: &str, token_id: &str) -> (String, String) {
    (registry.to_owned(), versionless_token_id(token_id))
}
