use anyhow::Result;
use bigname_manifests::DiscoveryObservation;
use bigname_storage::{SurfaceBindingKind, ens_v2_registry_resource_id};
use serde_json::json;

use super::{
    constants::*,
    discovery::{ens_v2_resolver_discovery_source, ens_v2_subregistry_discovery_source},
    names::{
        discovery_observation_key, insert_registry_name_state, name_under_registry, observe_name,
        remember_linked_resource_state, state_for_token_mut,
    },
    normalized::{normalized_event, proxy_upgraded_event},
    types::{RegistryNameState, RegistryObservation, RegistryResourceLink},
    util::{deterministic_uuid, normalize_address, null_if_zero_address},
};
mod context;
mod hydration;
mod parent;
mod terminal;
mod topology;
mod transfer;

pub(super) use context::RegistryObservationContext;
pub(super) use hydration::hydrate_subregistry_event_target_ids;
use parent::apply_parent_updated;
use terminal::{apply_label_unregistered, apply_token_regenerated, retire_replaced_name_states};
use topology::{
    apply_expiry_topology, apply_label_topology, apply_subregistry_topology,
    apply_unregister_topology, refresh_expired_topology_before_non_topology_event,
};
use transfer::apply_token_control_transferred;

pub(super) fn apply_registry_observation(
    observation: RegistryObservation,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    refresh_expired_topology_before_non_topology_event(&observation, context)?;
    match observation {
        RegistryObservation::RegistryCreated { reference } => {
            context.graph_events.push(normalized_event(
                &reference,
                None,
                None,
                EVENT_KIND_REGISTRY_CREATED,
                json!({}),
                json!({
                    "source_event": "RegistryCreated",
                    "registry_address": reference.emitting_address,
                    "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                }),
                format!("registry-created:{}", reference.emitting_address),
            ));
        }
        RegistryObservation::Upgraded {
            implementation,
            reference,
        } => {
            context
                .graph_events
                .push(proxy_upgraded_event(&reference, implementation));
        }
        RegistryObservation::LabelRegistered {
            token_id,
            labelhash,
            label,
            owner,
            expiry,
            sender,
            reference,
        } => {
            apply_label_topology(
                &token_id,
                &label,
                expiry,
                &reference,
                "LabelRegistered",
                context,
            )?;
            let observed_name = name_under_registry(
                &reference.emitting_address,
                &label,
                context.registry_suffix_by_address,
                context.root_registry_addresses,
                context.current_subregistry_by_parent_label,
                context.current_parent_claim_by_registry,
                &reference,
            )
            .and_then(|full_name| {
                observe_name(&reference.namespace, &full_name, &reference, &label)
                    .ok()
                    .map(|name| (full_name, name))
            });
            retire_replaced_name_states(
                &reference.emitting_address,
                &label,
                observed_name
                    .as_ref()
                    .map(|(_, name)| name.namehash.as_str()),
                &token_id,
                &reference,
                "LabelRegistered",
                "replacement_registration",
                false,
                context,
            );
            let Some((full_name, name)) = observed_name else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
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
                name_reachable: true,
            };
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
            apply_label_topology(
                &token_id,
                &label,
                expiry,
                &reference,
                "LabelReserved",
                context,
            )?;
            let observed_name = name_under_registry(
                &reference.emitting_address,
                &label,
                context.registry_suffix_by_address,
                context.root_registry_addresses,
                context.current_subregistry_by_parent_label,
                context.current_parent_claim_by_registry,
                &reference,
            )
            .and_then(|full_name| {
                observe_name(&reference.namespace, &full_name, &reference, &label)
                    .ok()
                    .map(|name| (full_name, name))
            });
            retire_replaced_name_states(
                &reference.emitting_address,
                &label,
                observed_name
                    .as_ref()
                    .map(|(_, name)| name.namehash.as_str()),
                &token_id,
                &reference,
                "LabelReserved",
                "replacement_reservation",
                true,
                context,
            );
            let Some((full_name, name)) = observed_name else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
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
                name_reachable: true,
            };
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
        } => {
            apply_unregister_topology(&token_id, &reference, context)?;
            apply_label_unregistered(token_id, sender, reference, context);
        }
        RegistryObservation::ExpiryUpdated {
            token_id,
            new_expiry,
            sender,
            reference,
        } => {
            apply_expiry_topology(&token_id, new_expiry, &reference, context)?;
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before_expiry = state.expiry;
                state.expiry = Some(new_expiry);
                state.current_ref = reference.clone();
                let logical_name_id = state
                    .name_reachable
                    .then(|| state.name.logical_name_id.clone());
                if state.name_reachable {
                    remember_linked_resource_state(context.linked_resource_states, state);
                }
                context.graph_events.push(normalized_event(
                    &reference,
                    logical_name_id.clone(),
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
                    logical_name_id,
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
            apply_subregistry_topology(&token_id, &subregistry, &reference, context)?;
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
                state.subregistry = Some(subregistry.clone());
                state.current_ref = reference.clone();
                logical_name_id = state
                    .name_reachable
                    .then(|| state.name.logical_name_id.clone());
                resource_id = state.resource.as_ref().map(|link| link.resource_id);
                if state.name_reachable {
                    remember_linked_resource_state(context.linked_resource_states, state);
                }
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
                let logical_name_id = state
                    .name_reachable
                    .then(|| state.name.logical_name_id.clone());
                if state.name_reachable {
                    remember_linked_resource_state(context.linked_resource_states, state);
                }
                context.graph_events.push(normalized_event(
                    &reference,
                    logical_name_id.clone(),
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
                        "logical_name_id": logical_name_id,
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
                    linked_logical_name_id: state
                        .name_reachable
                        .then(|| state.name.logical_name_id.clone()),
                    binding_ref: reference.clone(),
                    binding_source_event: "TokenResource".to_owned(),
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
        } => apply_parent_updated(parent, label, sender, reference, context)?,
    }

    Ok(())
}
