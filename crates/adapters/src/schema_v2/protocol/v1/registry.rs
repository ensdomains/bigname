use alloy_primitives::{B256, keccak256};
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use super::super::{
    BindingClosureDraft, BindingDraft, EventDraft, Interpreted, ResourceDraft, ensure_declared,
    permissions::{v1_grant_states, v1_revoke_states},
};
use super::{support::events, unmasked_word};
use crate::evm_abi::{
    address_hex, decode_event_log_tolerant_address_word, decode_event_log_tolerant_uint64_word,
    hex_string,
};
use crate::schema_v2::{
    catalog::Selected,
    common::{event_time, stable_uuid},
    model::RawLogInput,
    state::{State, V1NameState},
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const ROOT_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

mod transfer {
    use super::*;
    sol! { event Transfer(bytes32 indexed node, address owner); }
}

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NewTTL(bytes32 indexed node, uint64 ttl);
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    // Only ens_v1_registry_l1 has an LLL-era emitter whose address and uint64 words can be
    // unmasked (#361); basenames_base_registry shares this adapter but keeps the strict decode.
    let tolerate_unmasked_words = selected.source.source_family == "ens_v1_registry_l1";
    let (mut kinds, mut after, affected_node) = match selected.event.name.as_str() {
        "NewOwner" => {
            let decoded = unmasked_word::decode_registry_event::<NewOwner>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "NewOwner log is malformed",
                decode_event_log_tolerant_address_word::<NewOwner>,
            )?;
            let child = child_node(decoded.event.node, decoded.event.label);
            let mut body = json!({"source_event":"NewOwner","node":hex_string(decoded.event.node),"child_node":child,"labelhash":hex_string(decoded.event.label),"owner":address_hex(decoded.event.owner)});
            if let Some(word) = decoded.unmasked_word.as_ref() {
                unmasked_word::mark_unmasked_word(&mut body, "owner", word);
            }
            (vec!["SubregistryChanged"], body, child)
        }
        "Transfer" => {
            let decoded = unmasked_word::decode_registry_event::<transfer::Transfer>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "registry Transfer log is malformed",
                decode_event_log_tolerant_address_word::<transfer::Transfer>,
            )?;
            let mut body = json!({"source_event":"Transfer","node":hex_string(decoded.event.node),"owner":address_hex(decoded.event.owner)});
            if let Some(word) = decoded.unmasked_word.as_ref() {
                unmasked_word::mark_unmasked_word(&mut body, "owner", word);
            }
            (vec![], body, hex_string(decoded.event.node))
        }
        "NewResolver" => {
            let decoded = unmasked_word::decode_registry_event::<NewResolver>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "NewResolver log is malformed",
                decode_event_log_tolerant_address_word::<NewResolver>,
            )?;
            let mut body = json!({"source_event":"NewResolver","node":hex_string(decoded.event.node),"resolver":address_hex(decoded.event.resolver)});
            if let Some(word) = decoded.unmasked_word.as_ref() {
                unmasked_word::mark_unmasked_word(&mut body, "resolver", word);
            }
            (
                vec!["ResolverChanged"],
                body,
                hex_string(decoded.event.node),
            )
        }
        "NewTTL" => {
            unmasked_word::decode_registry_event::<NewTTL>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "NewTTL log is malformed",
                decode_event_log_tolerant_uint64_word::<NewTTL>,
            )?;
            return Ok(Interpreted::new());
        }
        name => bail!("unsupported registry event {name}"),
    };
    let emitter_role = selected.emitter_role.as_deref();
    if emitter_role == Some("registry_old")
        && state.v1_is_migrated(&selected.source.namespace, &affected_node)
        && !(selected.event.name == "NewResolver" && affected_node == ROOT_NODE)
    {
        return Ok(Interpreted::new());
    }
    if selected.event.name == "NewOwner" && emitter_role == Some("registry") {
        state.mark_v1_migrated(&selected.source.namespace, &affected_node);
    }
    if let Some(role) = emitter_role {
        after
            .as_object_mut()
            .expect("registry state is an object")
            .insert("emitter_role".to_owned(), Value::String(role.to_owned()));
    }
    let previous = state.v1_name(&selected.source.namespace, &affected_node);
    let owner = matches!(selected.event.name.as_str(), "NewOwner" | "Transfer")
        .then(|| after.get("owner").and_then(Value::as_str))
        .flatten()
        .map(str::to_owned);
    let previous_registry_owner = owner
        .as_ref()
        .and_then(|_| state.v1_registry_owner(&selected.source.namespace, &affected_node));
    let linked = if unmasked_word::body_has_unmasked_owner_word(&after) {
        kinds.push("AuthorityTransferred");
        unmasked_word::close_authority_for_unmasked_owner(
            state,
            &selected.source.namespace,
            &affected_node,
            previous.as_ref(),
        )
    } else {
        if let Some(owner) = owner.as_ref() {
            state.set_v1_registry_owner(&selected.source.namespace, &affected_node, owner.clone());
            if previous_registry_owner
                .as_deref()
                .is_none_or(|previous| !previous.eq_ignore_ascii_case(owner))
            {
                kinds.push("AuthorityTransferred");
            }
        }
        let registry_authority = owner
            .as_deref()
            .filter(|owner| !owner.eq_ignore_ascii_case(ZERO_ADDRESS))
            .map(|owner| {
                let resource_id = stable_uuid(&format!(
                    "resource:registry-only:{}:{affected_node}",
                    raw.chain_id
                ));
                let authority = V1NameState {
                    logical_name_id: format!("{}:{affected_node}", selected.source.namespace),
                    surface_known: previous.as_ref().is_some_and(|state| state.surface_known),
                    resource_id,
                    token_lineage_id: None,
                    authority_source_family: selected.source.source_family.clone(),
                    source_manifest_id: Some(selected.source.manifest_id),
                    labelhash: after
                        .get("labelhash")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    expiry: None,
                    owner: Some(owner.to_owned()),
                    authority_key: Some(format!("registry-only:{}:{affected_node}", raw.chain_id)),
                    wrapper_fallback: false,
                };
                state.remember_v1_registry_authority(
                    &selected.source.namespace,
                    &affected_node,
                    authority.clone(),
                );
                authority
            });
        match owner.as_deref() {
            Some(owner) if owner.eq_ignore_ascii_case(ZERO_ADDRESS) => {
                if previous
                    .as_ref()
                    .is_some_and(|authority| authority.token_lineage_id.is_none())
                {
                    state.activate_v1_authority(&selected.source.namespace, &affected_node, None);
                    None
                } else {
                    previous.clone()
                }
            }
            Some(_)
                if previous.as_ref().is_some_and(|authority| {
                    authority.authority_source_family == "ens_v1_wrapper_l1"
                }) =>
            {
                previous.clone()
            }
            Some(owner) => {
                if let Some(registrar) = state.reactivate_v1_registrar_for_owner(
                    &selected.source.namespace,
                    &affected_node,
                    owner,
                    raw.block_timestamp.unix_timestamp(),
                ) {
                    Some(registrar)
                } else {
                    let authority = registry_authority
                        .clone()
                        .expect("nonzero registry owner has a registry authority");
                    state.activate_v1_authority(
                        &selected.source.namespace,
                        &affected_node,
                        Some(authority.clone()),
                    );
                    Some(authority)
                }
            }
            None => previous.clone(),
        }
    };
    ensure_declared(selected, &kinds)?;
    if owner.is_some() {
        let object = after.as_object_mut().expect("registry state is an object");
        object.insert(
            "authority_kind".to_owned(),
            linked
                .as_ref()
                .map(authority_kind)
                .map_or(Value::Null, |kind| Value::String(kind.to_owned())),
        );
        object.insert(
            "authority_key".to_owned(),
            linked
                .as_ref()
                .and_then(|authority| authority.authority_key.clone())
                .map_or(Value::Null, Value::String),
        );
    }
    let mut output = events(kinds, after.clone());
    if let Some(event) = output
        .events
        .iter_mut()
        .find(|event| event.event_kind == "AuthorityTransferred")
    {
        event.explicit_before = Some(json!({"owner":previous_registry_owner}));
    }
    let event_authority = linked
        .as_ref()
        .or_else(|| owner.is_some().then_some(previous.as_ref()).flatten());
    if let Some(linked) = event_authority {
        for event in &mut output.events {
            event.logical_name_id = linked.surface_known.then(|| linked.logical_name_id.clone());
            event.resource_id = Some(linked.resource_id);
        }
    }
    append_authority_transition(
        &mut output,
        super::authority_arm(&selected.source.namespace),
        previous.as_ref(),
        linked.as_ref(),
        raw,
        &after,
        state.v1_resolver(&selected.source.namespace, &affected_node),
    );
    if selected.event.name == "NewResolver" {
        let resolver = after
            .get("resolver")
            .and_then(Value::as_str)
            .filter(|resolver| !resolver.eq_ignore_ascii_case(ZERO_ADDRESS))
            .map(str::to_owned);
        let previous_resolver =
            state.set_v1_resolver(&selected.source.namespace, &affected_node, resolver.clone());
        if let Some(event) = output.events.first_mut() {
            event.explicit_before = Some(json!({"resolver":previous_resolver}));
        }
        if let Some(authority) = linked.as_ref()
            && let Some(subject) = authority.owner.as_deref()
            && previous_resolver != resolver
        {
            if let Some(previous_resolver) = previous_resolver {
                push_permission_change(
                    &mut output,
                    authority,
                    subject,
                    json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":previous_resolver}),
                    "resolver_control",
                    false,
                    "ResolverChanged",
                    "resolver-revoke",
                );
            }
            if let Some(resolver) = resolver {
                push_permission_change(
                    &mut output,
                    authority,
                    subject,
                    json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
                    "resolver_control",
                    true,
                    "ResolverChanged",
                    "resolver-grant",
                );
            }
        }
    } else if owner.is_some()
        && previous
            .as_ref()
            .map(|state| (&state.owner, state.resource_id))
            != linked
                .as_ref()
                .map(|state| (&state.owner, state.resource_id))
    {
        append_authority_permissions(
            &mut output,
            previous.as_ref(),
            linked.as_ref(),
            state.v1_resolver(&selected.source.namespace, &affected_node),
            raw,
        );
    }
    Ok(output)
}

pub(super) fn append_authority_transition(
    output: &mut Interpreted,
    authority_arm: &str,
    previous: Option<&V1NameState>,
    linked: Option<&V1NameState>,
    raw: &RawLogInput,
    observation_state: &Value,
    resolver: Option<String>,
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

#[allow(clippy::too_many_arguments)]
fn push_permission_change(
    output: &mut Interpreted,
    authority: &V1NameState,
    subject: &str,
    scope: Value,
    power: &str,
    grant: bool,
    source_event_kind: &str,
    suffix: &str,
) {
    let Some(authority_key) = authority.authority_key.as_deref() else {
        return;
    };
    let (before, after) = if grant {
        v1_grant_states(
            subject,
            scope,
            power,
            authority_kind(authority),
            authority_key,
            source_event_kind,
        )
    } else {
        v1_revoke_states(
            subject,
            scope,
            power,
            authority_kind(authority),
            authority_key,
            source_event_kind,
        )
    };
    output.events.push(EventDraft {
        event_kind: "PermissionChanged".to_owned(),
        logical_name_id: authority
            .surface_known
            .then(|| authority.logical_name_id.clone()),
        resource_id: Some(authority.resource_id),
        identity_suffix: format!("PermissionChanged:{suffix}:{subject}"),
        explicit_before: Some(before),
        after_state: after,
        state_scope: String::new(),
    });
}

fn append_authority_permissions(
    output: &mut Interpreted,
    previous: Option<&V1NameState>,
    current: Option<&V1NameState>,
    resolver: Option<String>,
    raw: &RawLogInput,
) {
    if let Some(previous) = previous
        && let Some(subject) = previous.owner.as_deref()
    {
        push_permission_change(
            output,
            previous,
            subject,
            json!({"kind":"resource"}),
            "resource_control",
            false,
            "AuthorityTransferred",
            "resource-revoke",
        );
        if let Some(resolver) = resolver.as_deref() {
            push_permission_change(
                output,
                previous,
                subject,
                json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
                "resolver_control",
                false,
                "AuthorityTransferred",
                "resolver-revoke",
            );
        }
    }
    if let Some(current) = current
        && let Some(subject) = current.owner.as_deref()
    {
        push_permission_change(
            output,
            current,
            subject,
            json!({"kind":"resource"}),
            "resource_control",
            true,
            "AuthorityTransferred",
            "resource-grant",
        );
        if let Some(resolver) = resolver {
            push_permission_change(
                output,
                current,
                subject,
                json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
                "resolver_control",
                true,
                "AuthorityTransferred",
                "resolver-grant",
            );
        }
    }
}

fn child_node(parent: B256, labelhash: B256) -> String {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(labelhash.as_slice());
    format!("{:#x}", keccak256(input))
}
