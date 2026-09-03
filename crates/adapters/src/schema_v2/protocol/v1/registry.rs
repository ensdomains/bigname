use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use super::super::{
    BindingClosureDraft, EventDraft, Interpreted, ResourceDraft, ensure_declared,
    permissions::{v1_grant_states, v1_revoke_states},
};
use super::{support::events, unmasked_word};
use crate::evm_abi::{
    address_hex, decode_event_log_tolerant_address_word, decode_event_log_tolerant_uint64_word,
    hex_string,
};
use crate::schema_v2::{
    catalog::Selected,
    common::stable_uuid,
    model::RawLogInput,
    state::{State, V1NameState, V1RegistryReadAnchor},
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const ROOT_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const LLL_REGISTRY: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
mod node;
mod owner;
mod surface;
use node::child_node;
use owner::{RegistryOwnerView, classify as classify_registry_owner};
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
    // Only ENSv1 admits the LLL-era unmasked-word tolerance (#361).
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
    let owner_view = owner.as_deref().map(|owner| {
        classify_registry_owner(
            owner,
            &raw.emitting_address,
            unmasked_word::body_has_unmasked_owner_word(&after),
            !raw.emitting_address.eq_ignore_ascii_case(LLL_REGISTRY),
        )
    });
    if let Some(view) = owner_view.as_ref() {
        let object = after.as_object_mut().expect("registry state is an object");
        match view {
            RegistryOwnerView::Authentic { owner } => {
                object.insert("owner_getter".to_owned(), Value::String(owner.clone()));
            }
            RegistryOwnerView::ZeroEquivalent { reason } => {
                object.insert(
                    "owner_getter".to_owned(),
                    Value::String(ZERO_ADDRESS.to_owned()),
                );
                object.insert(
                    "owner_getter_reason".to_owned(),
                    Value::String(reason.as_str().to_owned()),
                );
            }
            RegistryOwnerView::UnavailableUnmasked => {}
        }
    }
    let previous_registry_owner_word = owner
        .as_ref()
        .and_then(|_| state.v1_registry_owner_word(&selected.source.namespace, &affected_node));
    let previous_registry_owner_getter = owner
        .as_ref()
        .and_then(|_| state.v1_registry_owner(&selected.source.namespace, &affected_node));
    let previous_registry_owner_reason = owner
        .as_ref()
        .and_then(|_| state.v1_registry_owner_reason(&selected.source.namespace, &affected_node));
    let surface_known = previous.as_ref().is_some_and(|state| state.surface_known)
        || state
            .v1_registry_read_anchor(&selected.source.namespace, &affected_node)
            .is_some_and(|anchor| anchor.surface_known)
        || state.v1_active_surface_materialized(&selected.source.namespace, &affected_node);
    let read_anchor = (owner_view
        .as_ref()
        .is_some_and(|view| !matches!(view, RegistryOwnerView::UnavailableUnmasked))
        || selected.event.name == "NewResolver")
        .then(|| {
            let anchor = state
                .v1_registry_read_anchor(&selected.source.namespace, &affected_node)
                .unwrap_or_else(|| V1RegistryReadAnchor {
                    logical_name_id: format!("{}:{affected_node}", selected.source.namespace),
                    resource_id: stable_uuid(&format!(
                        "resource:registry-only:{}:{affected_node}",
                        raw.chain_id
                    )),
                    surface_known,
                    source_family: selected.source.source_family.clone(),
                    source_manifest_id: Some(selected.source.manifest_id),
                });
            state.remember_v1_registry_read_anchor(
                &selected.source.namespace,
                &affected_node,
                anchor.clone(),
            );
            anchor
        });
    let linked = if matches!(owner_view, Some(RegistryOwnerView::UnavailableUnmasked)) {
        kinds.push("AuthorityTransferred");
        unmasked_word::close_authority_for_unmasked_owner(
            state,
            &selected.source.namespace,
            &affected_node,
            previous.as_ref(),
        )
    } else {
        if let (Some(owner), Some(view)) = (owner.as_ref(), owner_view.as_ref()) {
            let (owner_getter, reason) = match view {
                RegistryOwnerView::Authentic { owner } => (owner.clone(), None),
                RegistryOwnerView::ZeroEquivalent { reason } => {
                    (ZERO_ADDRESS.to_owned(), Some(reason.as_str().to_owned()))
                }
                RegistryOwnerView::UnavailableUnmasked => unreachable!(),
            };
            let owner_view_changed = previous_registry_owner_getter
                .as_deref()
                .is_none_or(|previous| !previous.eq_ignore_ascii_case(&owner_getter))
                || previous_registry_owner_reason != reason;
            state.set_v1_registry_owner_views(
                &selected.source.namespace,
                &affected_node,
                owner.clone(),
                owner_getter,
                reason,
            );
            if previous_registry_owner_word
                .as_deref()
                .is_none_or(|previous| !previous.eq_ignore_ascii_case(owner))
                || owner_view_changed
            {
                kinds.push("AuthorityTransferred");
            }
        }
        let registry_authority = owner_view.as_ref().and_then(|view| match view {
            RegistryOwnerView::Authentic { owner } => {
                let anchor = read_anchor.as_ref().expect("owner event has read anchor");
                let authority = V1NameState {
                    logical_name_id: anchor.logical_name_id.clone(),
                    surface_known: anchor.surface_known,
                    resource_id: anchor.resource_id,
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
                Some(authority)
            }
            RegistryOwnerView::ZeroEquivalent { .. } | RegistryOwnerView::UnavailableUnmasked => {
                None
            }
        });
        match owner_view.as_ref() {
            Some(RegistryOwnerView::ZeroEquivalent { .. }) => {
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
            Some(RegistryOwnerView::Authentic { .. })
                if previous.as_ref().is_some_and(|authority| {
                    authority.authority_source_family == "ens_v1_wrapper_l1"
                }) =>
            {
                previous.clone()
            }
            Some(RegistryOwnerView::Authentic { owner }) => {
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
            None | Some(RegistryOwnerView::UnavailableUnmasked) => previous.clone(),
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
        let mut before = json!({"owner": previous_registry_owner_word});
        if !matches!(owner_view, Some(RegistryOwnerView::UnavailableUnmasked)) {
            let object = before
                .as_object_mut()
                .expect("owner before state is an object");
            object.insert(
                "owner_getter".to_owned(),
                previous_registry_owner_getter.map_or(Value::Null, Value::String),
            );
            if let Some(reason) = previous_registry_owner_reason {
                object.insert("owner_getter_reason".to_owned(), Value::String(reason));
            }
        }
        event.explicit_before = Some(before);
    }
    let ownerless_read_context =
        matches!(owner_view, Some(RegistryOwnerView::ZeroEquivalent { .. })) && linked.is_none();
    let event_context = if ownerless_read_context {
        read_anchor.as_ref().map(|anchor| {
            (
                anchor.surface_known.then(|| anchor.logical_name_id.clone()),
                anchor.resource_id,
            )
        })
    } else {
        linked
            .as_ref()
            .or_else(|| owner.is_some().then_some(previous.as_ref()).flatten())
            .map(|authority| {
                (
                    authority
                        .surface_known
                        .then(|| authority.logical_name_id.clone()),
                    authority.resource_id,
                )
            })
    };
    if let Some((logical_name_id, resource_id)) = event_context {
        for event in &mut output.events {
            event.logical_name_id = logical_name_id.clone();
            event.resource_id = Some(resource_id);
        }
    }
    if matches!(owner_view, Some(RegistryOwnerView::ZeroEquivalent { .. }))
        && let Some(anchor) = read_anchor.as_ref()
        && let Some(event) = output
            .events
            .iter_mut()
            .find(|event| event.event_kind == "AuthorityTransferred")
    {
        event.logical_name_id = anchor.surface_known.then(|| anchor.logical_name_id.clone());
        event.resource_id = Some(anchor.resource_id);
        output.resources.push(ResourceDraft {
            resource_id: anchor.resource_id,
            token_lineage_id: None,
        });
    }
    let linked_resolver = state.v1_resolver_link(&selected.source.namespace, &affected_node);
    append_authority_transition(
        &mut output,
        super::authority_arm(&selected.source.namespace),
        previous.as_ref(),
        linked.as_ref(),
        raw,
        &after,
        linked_resolver
            .as_ref()
            .and_then(|link| link.resource_id.map(|_| link.resolver_address.clone())),
        None,
    );
    if selected.event.name == "NewResolver" {
        let resolver = after
            .get("resolver")
            .and_then(Value::as_str)
            .filter(|resolver| !resolver.eq_ignore_ascii_case(ZERO_ADDRESS))
            .map(str::to_owned);
        let registry_anchor = read_anchor
            .as_ref()
            .filter(|anchor| anchor.surface_known)
            .map(|anchor| (anchor.resource_id, Some(anchor.logical_name_id.clone())));
        let control_anchor = linked.as_ref().map(|authority| {
            (
                authority.resource_id,
                authority
                    .surface_known
                    .then(|| authority.logical_name_id.clone()),
            )
        });
        let resolver_anchor = registry_anchor.clone().or_else(|| control_anchor.clone());
        let event_anchor = match (&control_anchor, &registry_anchor) {
            (Some(control), Some(registry)) if control.0 != registry.0 => Some(control),
            _ => resolver_anchor.as_ref(),
        };
        let previous_resolver = state
            .set_v1_resolver_link(
                &selected.source.namespace,
                &affected_node,
                resolver.clone(),
                resolver_anchor
                    .as_ref()
                    .map(|(resource_id, _)| *resource_id),
                resolver_anchor
                    .as_ref()
                    .and_then(|(_, logical_name_id)| logical_name_id.clone()),
            )
            .as_ref()
            .map(|link| link.resolver_address.clone());
        if let Some(event) = output.events.first_mut() {
            event.explicit_before = Some(json!({"resolver":previous_resolver}));
            if let Some((resource_id, logical_name_id)) = event_anchor {
                event.resource_id = Some(*resource_id);
                event.logical_name_id = logical_name_id.clone();
            }
        }
        if let Some((resource_id, _)) = registry_anchor.as_ref() {
            output.resources.push(ResourceDraft {
                resource_id: *resource_id,
                token_lineage_id: None,
            });
        }
        if let (Some(control), Some(anchor)) = (&control_anchor, &registry_anchor)
            && control.0 != anchor.0
            && let Some(anchor_logical_name_id) = anchor.1.as_ref()
        {
            output.events.push(EventDraft {
                event_kind: "ResolverChanged".to_owned(),
                logical_name_id: Some(anchor_logical_name_id.clone()),
                resource_id: Some(anchor.0),
                identity_suffix: format!(
                    "ResolverChanged:registry-read:{}",
                    resolver.as_deref().unwrap_or(ZERO_ADDRESS)
                ),
                explicit_before: Some(json!({"resolver":previous_resolver})),
                after_state: after.clone(),
                state_scope: String::new(),
            });
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
        if let (Some(previous), Some(linked)) = (previous, linked)
            && !previous.surface_known
            && linked.surface_known
        {
            surface::append_binding(output, linked, authority_arm, raw, binding_active_from);
            surface::append_bound_event(output, linked, raw, observation_state);
        }
        return;
    }
    if let Some(linked) = linked.filter(|authority| authority.surface_known) {
        surface::append_binding(output, linked, authority_arm, raw, binding_active_from);
    } else if let Some(previous) = previous.filter(|authority| authority.surface_known) {
        output.binding_closures.push(BindingClosureDraft {
            logical_name_id: previous.logical_name_id.clone(),
            authority_arm: authority_arm.to_owned(),
        });
    }
    let logical_name_id = linked
        .filter(|authority| authority.surface_known)
        .map(|authority| authority.logical_name_id.clone())
        .or_else(|| {
            previous
                .filter(|authority| authority.surface_known)
                .map(|authority| authority.logical_name_id.clone())
        });
    let Some(identity_name_id) = linked
        .filter(|authority| authority.surface_known || authority.token_lineage_id.is_some())
        .or_else(|| {
            previous
                .filter(|authority| authority.surface_known || authority.token_lineage_id.is_some())
        })
        .map(|authority| authority.logical_name_id.clone())
    else {
        return;
    };
    let source_event = observation_state
        .get("source_event")
        .and_then(Value::as_str)
        .unwrap_or("AuthorityTransferred");
    if let Some(previous) = previous.filter(|authority| authority.surface_known) {
        output.events.push(EventDraft {
            event_kind: "SurfaceUnbound".to_owned(),
            logical_name_id: Some(previous.logical_name_id.clone()),
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
        surface::append_bound_event(output, linked, raw, observation_state);
    }
    output.events.push(EventDraft {
        event_kind: "AuthorityEpochChanged".to_owned(),
        logical_name_id: logical_name_id.clone(),
        resource_id: linked
            .map(|authority| authority.resource_id)
            .or_else(|| previous.map(|authority| authority.resource_id)),
        identity_suffix: format!("AuthorityEpochChanged:{source_event}:{identity_name_id}"),
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
            logical_name_id,
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

pub(super) fn merge_observation(observation: &Value, fields: Value) -> Value {
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
