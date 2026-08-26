use alloy_primitives::{Address, U256};
use anyhow::{Context, bail};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    EACRolesChanged, LabelUnregistered, TokenRegenerated, TokenResource, TransferBatch,
    TransferSingle, single_event, token_state_event,
    topology::{
        append_terminal_boundaries, append_token_discovery_closures, append_v2_name_transitions,
        discovery_observation_key, resolver_discovery_keys,
    },
};
use crate::{
    evm_abi::{address_hex, decode_event_log, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::{ens_v2_registry_resource_id, ens_v2_registry_token_lineage_id, stable_uuid},
        model::RawLogInput,
        protocol::{
            DiscoveryDraft, EventDraft, Interpreted, NameDraft, ResourceDraft, ensure_declared,
            permissions::{V2PermissionState, V2Vocabulary, v2_states},
        },
        state::{State, V2TokenState},
    },
};

pub(super) fn token_resource(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<TokenResource>(
        &raw.topics,
        &raw.data,
        "TokenResource log is malformed",
    )?;
    ensure_declared(selected, &["TokenResourceLinked"])?;
    let (resource_id, token_lineage_id) = upstream_identity(raw, selected, event.resource);
    let token_id = u256_word_hex(event.tokenId);
    let resource_word = u256_word_hex(event.resource);
    if let Some(reserved) = state.v2_token(&raw.emitting_address, &token_id)
        && reserved
            .resource_id
            .is_some_and(|reserved_id| reserved_id != resource_id)
    {
        bail!("TokenResource for reserved token {token_id} does not confirm its retained resource");
    }
    let linked = state.link_v2_resource(
        &raw.emitting_address,
        &token_id,
        resource_word.clone(),
        resource_id,
        token_lineage_id,
    );
    let logical_name_id = linked
        .name
        .as_ref()
        .map(|name| name.logical_name_id.clone());
    let mut output = single_event(
        "TokenResourceLinked",
        logical_name_id.clone(),
        Some(resource_id),
        json!({
            "source_event":"TokenResource",
            "token_id": token_id,
            "current_token_id": token_id,
            "upstream_resource": resource_word,
            "resource_id":resource_id.to_string(),
            "token_lineage_id": token_lineage_id.map(|id| id.to_string()),
            "registry_contract_instance_id":selected.contract_instance_id.to_string(),
        }),
    );
    output.resources.push(ResourceDraft {
        resource_id,
        token_lineage_id,
    });
    if linked.registration.is_some()
        && let Some(name) = linked.name
    {
        let surface_binding_id = stable_uuid(&format!(
            "ens-v2-surface-binding:{}:{}:{}:{}",
            raw.chain_id, selected.contract_instance_id, resource_word, name.logical_name_id,
        ));
        output.names.push(NameDraft {
            labels: name.labels,
            namehash: name.namehash,
            resource_id: Some(resource_id),
            token_lineage_id,
            surface_binding_id: Some(surface_binding_id),
            bind: true,
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: "ens_v2".to_owned(),
            source_kind: "TokenResource_name".to_owned(),
            preimage_metadata: None,
        });
        output.events.push(EventDraft {
            event_kind: "SurfaceBound".to_owned(),
            logical_name_id: logical_name_id.clone(),
            resource_id: Some(resource_id),
            identity_suffix: format!("SurfaceBound:{token_id}"),
            explicit_before: None,
            after_state: json!({
                "source_event":"TokenResource",
                "token_id":token_id,
                "current_token_id":token_id,
                "upstream_resource":resource_word,
                "binding_kind":"declared_registry_path",
                "surface_binding_id":surface_binding_id.to_string(),
                "logical_name_id":name.logical_name_id,
                "resource_id":resource_id.to_string(),
            }),
            state_scope: String::new(),
        });
        if let Some(registration) = linked.registration.as_ref() {
            let mut linked_registration = registration.clone();
            linked_registration
                .as_object_mut()
                .expect("registration state is an object")
                .extend(
                    json!({
                        "authority_kind":"ens_v2_registry",
                        "authority_key":format!(
                            "ens-v2-registry:{}:{}:{}",
                            raw.chain_id, selected.contract_instance_id, resource_word,
                        ),
                        "upstream_resource": resource_word,
                        "resource_pending": false,
                        "token_lineage_id": token_lineage_id.map(|id| id.to_string()),
                        "current_token_id":token_id,
                        "status":"registered",
                        "registry_contract_instance_id":selected.contract_instance_id.to_string(),
                    })
                    .as_object()
                    .expect("registration state is an object")
                    .clone(),
                );
            output.events.push(EventDraft {
                event_kind: "RegistrationGranted".to_owned(),
                logical_name_id: logical_name_id.clone(),
                resource_id: Some(resource_id),
                identity_suffix: format!("RegistrationGranted:linked:{token_id}"),
                explicit_before: None,
                after_state: linked_registration,
                state_scope: String::new(),
            });
            if let Some(owner) = registration
                .get("registrant")
                .or_else(|| registration.get("owner"))
                .and_then(Value::as_str)
            {
                output.events.push(EventDraft {
                    event_kind: "AuthorityTransferred".to_owned(),
                    logical_name_id: logical_name_id.clone(),
                    resource_id: Some(resource_id),
                    identity_suffix: format!("AuthorityTransferred:{token_id}"),
                    explicit_before: None,
                    after_state: json!({
                        "source_event":"LabelRegistered",
                        "token_id":token_id,
                        "current_token_id":token_id,
                        "upstream_resource":resource_word,
                        "owner":owner,
                    }),
                    state_scope: String::new(),
                });
            }
            if let Some(expiry) = registration.get("expiry") {
                output.events.push(EventDraft {
                    event_kind: "ExpiryChanged".to_owned(),
                    logical_name_id,
                    resource_id: Some(resource_id),
                    identity_suffix: format!("ExpiryChanged:{token_id}"),
                    explicit_before: None,
                    after_state: json!({
                        "source_event":"LabelRegistered",
                        "token_id":token_id,
                        "current_token_id":token_id,
                        "upstream_resource":resource_word,
                        "expiry":expiry,
                    }),
                    state_scope: String::new(),
                });
            }
        }
    }
    Ok(output)
}

pub(super) fn label_unregistered(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<LabelUnregistered>(
        &raw.topics,
        &raw.data,
        "LabelUnregistered log is malformed",
    )?;
    let token_id = u256_word_hex(event.tokenId);
    let linked = state.release_v2_token(&raw.emitting_address, &token_id);
    let mut candidates = linked
        .as_ref()
        .map(|token| token.resolver_discovery_aliases.clone())
        .unwrap_or_default();
    candidates.insert(token_id.clone());
    let protected_tokens =
        state.live_v2_resolver_tokens_sharing(&raw.emitting_address, &candidates);
    let protected_resolver_keys = resolver_discovery_keys(raw, None, &protected_tokens)?;
    // Registration events are emitter-partitioned. PermissionedRegistry emits this event from the
    // registry's public unregister path.
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L197 @ ens_v2@ccaeb58)
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L201 @ ens_v2@ccaeb58)
    let mut output = token_state_event(
        selected,
        "RegistrationReleased",
        event.tokenId,
        linked.as_ref(),
        json!({
            "source_event":"LabelUnregistered",
            "sender":address_hex(event.sender),
            "registry_contract_instance_id":selected.contract_instance_id.to_string(),
        }),
    )?;
    append_terminal_boundaries(
        &mut output,
        state,
        linked.as_ref(),
        &token_id,
        "LabelUnregistered",
    );
    let transitions = state.refresh_dirty_v2_names(raw.block_timestamp.unix_timestamp());
    append_v2_name_transitions(&mut output, transitions, raw, "LabelUnregistered", None);
    append_token_discovery_closures(
        &mut output,
        raw,
        event.tokenId,
        linked.as_ref(),
        &protected_resolver_keys,
    )?;
    Ok(output)
}

pub(super) fn transfer_single(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<TransferSingle>(
        &raw.topics,
        &raw.data,
        "TransferSingle log is malformed",
    )?;
    if event.value == U256::ZERO || event.from == Address::ZERO || event.to == Address::ZERO {
        return Ok(Interpreted::new());
    }
    transfer_event(
        selected,
        raw,
        state,
        event.id,
        json!({"source_event":"TransferSingle","operator":address_hex(event.operator),"to":address_hex(event.to),"amount":event.value.to_string()}),
        address_hex(event.from),
        address_hex(event.to),
    )
}

pub(super) fn transfer_batch(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<TransferBatch>(
        &raw.topics,
        &raw.data,
        "TransferBatch log is malformed",
    )?;
    if event.ids.len() != event.values.len() {
        bail!("TransferBatch ids and values differ in length");
    }
    ensure_declared(selected, &["TokenControlTransferred"])?;
    let mut output = Interpreted::new();
    if event.from == Address::ZERO || event.to == Address::ZERO {
        return Ok(output);
    }
    for (index, (id, value)) in event.ids.into_iter().zip(event.values).enumerate() {
        if value == U256::ZERO {
            continue;
        }
        let mut item = transfer_event(
            selected,
            raw,
            state,
            id,
            json!({"source_event":"TransferBatch","operator":address_hex(event.operator),"to":address_hex(event.to),"amount":value.to_string(),"transfer_index":index}),
            address_hex(event.from),
            address_hex(event.to),
        )?;
        if let Some(event) = item.events.first_mut() {
            event.identity_suffix =
                format!("TokenControlTransferred:{index}:{}", u256_word_hex(id));
        }
        output.events.append(&mut item.events);
        output.resources.append(&mut item.resources);
    }
    Ok(output)
}

fn transfer_event(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    token_id: U256,
    mut after: Value,
    from: String,
    to: String,
) -> anyhow::Result<Interpreted> {
    ensure_declared(selected, &["TokenControlTransferred"])?;
    let token_word = u256_word_hex(token_id);
    let linked = state.transfer_v2_registrant(&raw.emitting_address, &token_word, to);
    let object = after.as_object_mut().expect("transfer state is an object");
    object.insert(
        "upstream_resource".to_owned(),
        linked
            .as_ref()
            .and_then(|state| state.upstream_resource.clone())
            .map_or(Value::Null, Value::String),
    );
    object.insert(
        "token_lineage_id".to_owned(),
        linked
            .as_ref()
            .and_then(|state| state.token_lineage_id)
            .map(|id| Value::String(id.to_string()))
            .unwrap_or(Value::Null),
    );
    object.insert(
        "registry_contract_instance_id".to_owned(),
        Value::String(selected.contract_instance_id.to_string()),
    );
    if linked.is_none() {
        object.insert("registry_hydration_pending".to_owned(), Value::Bool(true));
    }
    let mut output = token_state_event(
        selected,
        "TokenControlTransferred",
        token_id,
        linked.as_ref(),
        after,
    )?;
    output.events[0].explicit_before = Some(json!({"from":from}));
    Ok(output)
}

pub(super) fn permission(
    selected: &Selected,
    raw: &RawLogInput,
    state: &State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<EACRolesChanged>(
        &raw.topics,
        &raw.data,
        "EACRolesChanged log is malformed",
    )?;
    let root = event.resource == U256::ZERO
        && matches!(
            selected.source.source_family.as_str(),
            "ens_v2_registry_l1" | "ens_v2_root_l1"
        );
    let kind = if root {
        "RootPermissionChanged"
    } else {
        "PermissionChanged"
    };
    ensure_declared(selected, &[kind])?;
    let (resource_id, token_lineage_id) = upstream_identity(raw, selected, event.resource);
    let upstream_resource = u256_word_hex(event.resource);
    let linked = state.v2_token_by_upstream_resource(&raw.emitting_address, &upstream_resource)?;
    let (before, after) = v2_states(
        selected,
        raw,
        V2Vocabulary::Registry,
        V2PermissionState {
            upstream_resource: &upstream_resource,
            account: address_hex(event.account),
            old_bitmap: event.oldRoleBitmap,
            new_bitmap: event.newRoleBitmap,
            root_resource: root,
            selector: json!({
                "kind":"resource",
                "key":Value::Null,
                "hash":Value::Null,
                "normalized_name":Value::Null,
                "dns_encoded_name":Value::Null,
            }),
        },
    );
    let mut output = single_event(
        kind,
        linked
            .as_ref()
            .and_then(|state| state.name.as_ref())
            .map(|name| name.logical_name_id.clone()),
        Some(resource_id),
        after,
    );
    output.events[0].explicit_before = Some(before);
    output.resources.push(ResourceDraft {
        resource_id,
        token_lineage_id,
    });
    Ok(output)
}

pub(super) fn token_regenerated(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<TokenRegenerated>(
        &raw.topics,
        &raw.data,
        "TokenRegenerated log is malformed",
    )?;
    ensure_declared(selected, &["TokenRegenerated"])?;
    let old_token = u256_word_hex(event.oldTokenId);
    let new_token = u256_word_hex(event.newTokenId);
    let (linked, displaced) = state
        .regenerate_v2_token(&raw.emitting_address, &old_token, &new_token)
        .with_context(|| {
            format!("TokenRegenerated {old_token} has no retained TokenResource predecessor")
        })?;
    let mut output = single_event(
        "TokenRegenerated",
        linked
            .name
            .as_ref()
            .map(|name| name.logical_name_id.clone()),
        linked.resource_id,
        json!({
            "source_event":"TokenRegenerated",
            "old_token_id":old_token,
            "new_token_id":new_token,
            "resource":linked.upstream_resource,
            "token_lineage_id":linked.token_lineage_id.map(|id| id.to_string()),
            "resolver_discovery_aliases":linked.resolver_discovery_aliases,
        }),
    );
    if let Some(resource_id) = linked.resource_id {
        output.resources.push(ResourceDraft {
            resource_id,
            token_lineage_id: linked.token_lineage_id,
        });
    }
    if let Some(displaced) = displaced.as_ref() {
        let displaced_registration = V2TokenState {
            resolver: None,
            subregistry: None,
            ..displaced.clone()
        };
        let mut release = token_state_event(
            selected,
            "RegistrationReleased",
            event.newTokenId,
            Some(&displaced_registration),
            json!({
                "source_event":"TokenRegenerated",
                "terminal_reason":"registry_name_binding_changed",
                "registry_contract_instance_id":selected.contract_instance_id.to_string(),
            }),
        )?;
        append_terminal_boundaries(
            &mut release,
            state,
            Some(&displaced_registration),
            &new_token,
            "TokenRegenerated",
        );
        let mut candidates = displaced.resolver_discovery_aliases.clone();
        candidates.insert(new_token.clone());
        let mut protected_resolver_tokens =
            state.live_v2_resolver_tokens_sharing(&raw.emitting_address, &candidates);
        protected_resolver_tokens.extend(linked.resolver_discovery_aliases.iter().cloned());
        let protected_resolver_keys =
            resolver_discovery_keys(raw, Some(event.oldTokenId), &protected_resolver_tokens)?;
        append_token_discovery_closures(
            &mut release,
            raw,
            event.newTokenId,
            Some(displaced),
            &protected_resolver_keys,
        )?;
        output.append(&mut release);
    }
    match selected.event.name.as_str() {
        "TokenRegenerated" => {
            let subregistry_keys =
                discovery_reassertion_keys(raw, &event, false, displaced.is_some());
            if let (Some(target), Some((old_observation_key, new_observation_key))) =
                (linked.subregistry.as_deref(), subregistry_keys)
            {
                output.discovery.push(DiscoveryDraft::Close {
                    edge_kind: "subregistry".to_owned(),
                    observation_key: old_observation_key,
                });
                output.discovery.push(DiscoveryDraft::Edge {
                    edge_kind: "subregistry".to_owned(),
                    to_address: target.to_owned(),
                    admission_basis: "linked_subregistry_event".to_owned(),
                    observation_key: new_observation_key,
                });
            }
        }
        _ => unreachable!("token regeneration dispatch selected another manifest event"),
    }
    let transitions = state.refresh_dirty_v2_names(raw.block_timestamp.unix_timestamp());
    append_v2_name_transitions(&mut output, transitions, raw, "TokenRegenerated", None);
    Ok(output)
}

fn discovery_reassertion_keys(
    raw: &RawLogInput,
    event: &TokenRegenerated,
    resolver: bool,
    destination_was_occupied: bool,
) -> Option<(String, String)> {
    let old = discovery_observation_key(raw, event.oldTokenId, resolver);
    let new = discovery_observation_key(raw, event.newTokenId, resolver);
    (destination_was_occupied || old != new).then_some((old, new))
}

fn upstream_identity(
    raw: &RawLogInput,
    selected: &Selected,
    resource: U256,
) -> (Uuid, Option<Uuid>) {
    let upstream_resource = u256_word_hex(resource);
    let resource_id = ens_v2_registry_resource_id(
        &raw.chain_id,
        selected.contract_instance_id,
        &upstream_resource,
    );
    let token_lineage_id = (resource != U256::ZERO).then(|| {
        ens_v2_registry_token_lineage_id(
            &raw.chain_id,
            selected.contract_instance_id,
            &upstream_resource,
        )
    });
    (resource_id, token_lineage_id)
}
