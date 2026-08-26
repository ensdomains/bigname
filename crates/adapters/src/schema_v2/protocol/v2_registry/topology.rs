use std::collections::BTreeSet;

use alloy_primitives::U256;
use anyhow::Context;
use serde_json::{Value, json};

use crate::schema_v2::{
    protocol::{
        BindingClosureDraft, DiscoveryDraft, EventDraft, Interpreted, NameDraft, ShadowNameDraft,
    },
    seam::{ARM_WIDE_BINDING_CLOSE_KEY, CLOSED_AUTHORITY_ARM_KEY, SURFACE_BINDING_ID_KEY},
    state::{State, V2NameTransition, V2TokenState},
};

pub(super) fn append_v2_name_transitions(
    output: &mut Interpreted,
    transitions: Vec<V2NameTransition>,
    raw: &crate::schema_v2::RawLogInput,
    source_event: &str,
    skip: Option<(&str, &str)>,
) {
    for transition in transitions {
        let identity_reassertion = transition.previous == transition.current
            && transition.previous_shadow == transition.current_shadow;
        if !identity_reassertion
            && skip.is_some_and(|(registry, token_id)| {
                registry.eq_ignore_ascii_case(&transition.registry)
                    && token_id.eq_ignore_ascii_case(&transition.token_id)
            })
        {
            continue;
        }
        if !identity_reassertion && let Some(previous) = transition.previous.as_ref() {
            if transition.registration.is_some() {
                output.binding_closures.push(BindingClosureDraft {
                    logical_name_id: previous.logical_name_id.clone(),
                    authority_arm: "ens_v2".to_owned(),
                });
            }
            if let Some(resource_id) = transition
                .registration
                .as_ref()
                .and(transition.resource_id.as_ref())
                .copied()
            {
                output.events.push(EventDraft {
                    event_kind: "SurfaceUnbound".to_owned(),
                    logical_name_id: Some(previous.logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    identity_suffix: format!(
                        "SurfaceUnbound:topology:{}:{}",
                        transition.registry, transition.token_id
                    ),
                    explicit_before: None,
                    after_state: json!({
                        "source_event": source_event,
                        "topology_rebind": true,
                        "registry": &transition.registry,
                        "token_id": &transition.token_id,
                        "previous_namehash": &previous.namehash,
                        "current_namehash": transition.current.as_ref().map(|name| &name.namehash),
                    }),
                    state_scope: transition_scope(&transition, source_event),
                });
                output.events.push(EventDraft {
                    event_kind: "RegistrationReleased".to_owned(),
                    logical_name_id: Some(previous.logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    identity_suffix: format!(
                        "RegistrationReleased:topology:{}:{}",
                        transition.registry, transition.token_id
                    ),
                    explicit_before: Some(json!({
                        "status":if transition.registration.is_some() {"registered"} else {"reserved"},
                    })),
                    after_state: json!({
                        "source_event":source_event,
                        "terminal_reason":"registry_name_binding_changed",
                        "status":"released",
                        "token_id":transition.token_id,
                        "registry_contract_instance_id":transition.registry_contract_instance_id.map(|id| id.to_string()),
                    }),
                    state_scope: transition_scope(&transition, source_event),
                });
            }
        }
        if let Some(current) = transition.current_shadow.as_ref() {
            output.shadow_names.push(ShadowNameDraft {
                raw_labels: current.raw_labels.clone(),
                namehash: current.namehash.clone(),
                source_kind: format!("{source_event}_registry_suffix"),
            });
        }
        let Some(ref current) = transition.current else {
            continue;
        };
        let bound_resource = transition
            .registration
            .as_ref()
            .and(transition.resource_id.as_ref())
            .copied();
        let surface_binding_id = bound_resource.map(|_| {
            crate::schema_v2::common::stable_uuid(&format!(
                "ens-v2-surface-binding-rebound:{}:{}:{}:{}:{}:{}:{}",
                raw.chain_id,
                transition
                    .registry_contract_instance_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| transition.registry.clone()),
                transition.upstream_resource.as_deref().unwrap_or("-"),
                current.logical_name_id,
                raw.block_hash,
                raw.transaction_index,
                raw.log_index,
            ))
        });
        let preimage_metadata = identity_reassertion.then(|| {
            json!({
                (ARM_WIDE_BINDING_CLOSE_KEY):true,
                (CLOSED_AUTHORITY_ARM_KEY):"ens_v2",
                (SURFACE_BINDING_ID_KEY):surface_binding_id.map(|id| id.to_string()),
            })
        });
        let reasserted_name = NameDraft {
            labels: current.labels.clone(),
            namehash: current.namehash.clone(),
            resource_id: transition.resource_id,
            token_lineage_id: transition.token_lineage_id,
            surface_binding_id,
            bind: bound_resource.is_some(),
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: "ens_v2".to_owned(),
            source_kind: format!("{source_event}_registry_suffix"),
            preimage_metadata,
        };
        if identity_reassertion
            && let Some(direct_name) = output
                .names
                .iter_mut()
                .find(|name| name.namehash == reasserted_name.namehash)
        {
            direct_name.resource_id = reasserted_name.resource_id;
            direct_name.token_lineage_id = reasserted_name.token_lineage_id;
            direct_name.surface_binding_id = reasserted_name.surface_binding_id;
            direct_name.bind = reasserted_name.bind;
            direct_name.binding_kind = reasserted_name.binding_kind;
            direct_name.authority_arm = reasserted_name.authority_arm;
            direct_name.preimage_metadata = reasserted_name.preimage_metadata;
        } else {
            output.names.push(reasserted_name);
        }
        if !identity_reassertion && let Some(resource_id) = bound_resource {
            output.events.push(EventDraft {
                event_kind: "SurfaceBound".to_owned(),
                logical_name_id: Some(current.logical_name_id.clone()),
                resource_id: Some(resource_id),
                identity_suffix: format!(
                    "SurfaceBound:topology:{}:{}",
                    transition.registry, transition.token_id
                ),
                explicit_before: None,
                after_state: json!({
                    "source_event": source_event,
                    "topology_rebind": true,
                    "registry": &transition.registry,
                    "token_id": &transition.token_id,
                    "current_namehash": &current.namehash,
                }),
                state_scope: transition_scope(&transition, source_event),
            });
            append_rebound_state_events(output, &transition, current, resource_id, source_event);
        }
    }
}

pub(super) fn boundary_expiration(transition: V2NameTransition) -> anyhow::Result<Interpreted> {
    if transition.current.is_some()
        || transition.current_shadow.is_some()
        || (transition.previous.is_none() && transition.previous_shadow.is_none())
    {
        anyhow::bail!("block-boundary ENSv2 transition is not an expiration");
    }
    let mut output = Interpreted::new();
    if transition.previous.is_some() {
        append_removed_name(&mut output, &transition, "RegistryPathExpired");
    }
    Ok(output)
}

pub(in crate::schema_v2) fn boundary_reassertion(
    transition: &V2NameTransition,
    block: &crate::schema_v2::RawBlockInput,
) -> Option<Interpreted> {
    if transition.previous != transition.current
        || transition.previous_shadow != transition.current_shadow
    {
        return None;
    }
    let raw = crate::schema_v2::RawLogInput {
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        block_timestamp: block.block_timestamp,
        canonicality_state: block.canonicality_state.clone(),
        transaction_hash: format!("block-boundary:{}", block.block_hash),
        transaction_index: -1,
        log_index: -1,
        emitting_address: transition.registry.clone(),
        topics: Vec::new(),
        data: Vec::new(),
    };
    let mut output = Interpreted::new();
    append_v2_name_transitions(
        &mut output,
        vec![transition.clone()],
        &raw,
        "RegistryPathExpired",
        None,
    );
    Some(output)
}

fn append_removed_name(
    output: &mut Interpreted,
    transition: &V2NameTransition,
    source_event: &str,
) {
    let Some(previous) = transition.previous.as_ref() else {
        return;
    };
    if transition.registration.is_some() {
        output.binding_closures.push(BindingClosureDraft {
            logical_name_id: previous.logical_name_id.clone(),
            authority_arm: "ens_v2".to_owned(),
        });
    }
    let Some(resource_id) = transition
        .registration
        .as_ref()
        .and(transition.resource_id.as_ref())
        .copied()
    else {
        return;
    };
    output.events.push(EventDraft {
        event_kind: "SurfaceUnbound".to_owned(),
        logical_name_id: Some(previous.logical_name_id.clone()),
        resource_id: Some(resource_id),
        identity_suffix: format!(
            "SurfaceUnbound:topology:{}:{}",
            transition.registry, transition.token_id
        ),
        explicit_before: None,
        after_state: json!({
            "source_event": source_event,
            "topology_rebind": true,
            "registry": &transition.registry,
            "token_id": &transition.token_id,
            "previous_namehash": &previous.namehash,
            "current_namehash": Value::Null,
        }),
        state_scope: transition_scope(transition, source_event),
    });
    output.events.push(EventDraft {
        event_kind: "RegistrationReleased".to_owned(),
        logical_name_id: Some(previous.logical_name_id.clone()),
        resource_id: Some(resource_id),
        identity_suffix: format!(
            "RegistrationReleased:topology:{}:{}",
            transition.registry, transition.token_id
        ),
        explicit_before: Some(json!({
            "status":if transition.registration.is_some() {"registered"} else {"reserved"},
        })),
        after_state: json!({
            "source_event":source_event,
            "terminal_reason":"registry_name_binding_expired",
            "status":"released",
            "token_id":transition.token_id,
            "registry_contract_instance_id":transition.registry_contract_instance_id.map(|id| id.to_string()),
        }),
        state_scope: transition_scope(transition, source_event),
    });
}

fn append_rebound_state_events(
    output: &mut Interpreted,
    transition: &V2NameTransition,
    current: &crate::schema_v2::state::V2NameState,
    resource_id: uuid::Uuid,
    source_event: &str,
) {
    let registry_instance = transition
        .registry_contract_instance_id
        .map(|id| id.to_string());
    let upstream_resource = transition.upstream_resource.as_deref();
    if let Some(registration) = transition.registration.as_ref() {
        let registrant = registration
            .get("registrant")
            .or_else(|| registration.get("owner"))
            .cloned()
            .unwrap_or(Value::Null);
        let expiry = registration.get("expiry").cloned().unwrap_or(Value::Null);
        let labelhash = registration
            .get("labelhash")
            .cloned()
            .unwrap_or(Value::Null);
        let authority_key = registry_instance.as_ref().zip(upstream_resource).map(
            |(registry_instance, upstream_resource)| {
                format!(
                    "ens-v2-registry:{}:{registry_instance}:{upstream_resource}",
                    transition.registry
                )
            },
        );
        output.events.push(EventDraft {
            event_kind: "RegistrationGranted".to_owned(),
            logical_name_id: Some(current.logical_name_id.clone()),
            resource_id: Some(resource_id),
            identity_suffix: format!(
                "RegistrationGranted:topology:{}:{}",
                transition.registry, transition.token_id
            ),
            explicit_before: Some(json!({})),
            after_state: json!({
                "source_event":source_event,
                "authority_kind":"ens_v2_registry",
                "authority_key":authority_key,
                "registrant":registrant,
                "expiry":expiry,
                "labelhash":labelhash,
                "token_id":transition.token_id,
                "current_token_id":transition.token_id,
                "upstream_resource":upstream_resource,
                "status":"registered",
                "registry_contract_instance_id":registry_instance,
            }),
            state_scope: transition_scope(transition, source_event),
        });
        output.events.push(EventDraft {
            event_kind: "AuthorityTransferred".to_owned(),
            logical_name_id: Some(current.logical_name_id.clone()),
            resource_id: Some(resource_id),
            identity_suffix: format!(
                "AuthorityTransferred:topology:{}:{}",
                transition.registry, transition.token_id
            ),
            explicit_before: Some(json!({})),
            after_state: json!({
                "source_event":source_event,
                "owner":registrant,
                "token_id":transition.token_id,
                "current_token_id":transition.token_id,
                "upstream_resource":upstream_resource,
            }),
            state_scope: transition_scope(transition, source_event),
        });
        if !expiry.is_null() {
            output.events.push(EventDraft {
                event_kind: "ExpiryChanged".to_owned(),
                logical_name_id: Some(current.logical_name_id.clone()),
                resource_id: Some(resource_id),
                identity_suffix: format!(
                    "ExpiryChanged:topology:{}:{}",
                    transition.registry, transition.token_id
                ),
                explicit_before: Some(json!({})),
                after_state: json!({
                    "source_event":source_event,
                    "expiry":expiry,
                    "token_id":transition.token_id,
                    "current_token_id":transition.token_id,
                    "upstream_resource":upstream_resource,
                }),
                state_scope: transition_scope(transition, source_event),
            });
        }
    }
    for (event_kind, field, target) in [
        (
            "ResolverChanged",
            "resolver",
            transition.resolver.as_deref(),
        ),
        (
            "SubregistryChanged",
            "subregistry",
            transition.subregistry.as_deref(),
        ),
    ] {
        let Some(target) = target else { continue };
        output.events.push(EventDraft {
            event_kind: event_kind.to_owned(),
            logical_name_id: Some(current.logical_name_id.clone()),
            resource_id: Some(resource_id),
            identity_suffix: format!(
                "{event_kind}:topology:{}:{}",
                transition.registry, transition.token_id
            ),
            explicit_before: Some(json!({})),
            after_state: json!({
                "source_event":source_event,
                "token_id":transition.token_id,
                (field):target,
            }),
            state_scope: transition_scope(transition, source_event),
        });
    }
}

fn transition_scope(transition: &V2NameTransition, source_event: &str) -> String {
    format!(
        "{}:-:{}:-:{source_event}",
        transition.registry.to_ascii_lowercase(),
        transition.token_id,
    )
}

pub(super) fn append_terminal_boundaries(
    output: &mut Interpreted,
    state: &mut State,
    linked: Option<&V2TokenState>,
    token_id: &str,
    source_event: &str,
) {
    let Some(linked) = linked else { return };
    let logical_name_id = linked
        .name
        .as_ref()
        .map(|name| name.logical_name_id.clone());
    if linked.registration.is_some()
        && let Some(logical_name_id) = logical_name_id.as_ref()
    {
        output.binding_closures.push(BindingClosureDraft {
            logical_name_id: logical_name_id.clone(),
            authority_arm: "ens_v2".to_owned(),
        });
        state.record_v2_terminal_closure_hit(logical_name_id, "ens_v2");
    }
    if linked.registration.is_some() && linked.resource_id.is_some() && logical_name_id.is_some() {
        output.events.push(EventDraft {
            event_kind: "SurfaceUnbound".to_owned(),
            logical_name_id: logical_name_id.clone(),
            resource_id: linked.resource_id,
            identity_suffix: format!("SurfaceUnbound:{token_id}"),
            explicit_before: None,
            after_state: json!({"source_event":source_event,"token_id":token_id}),
            state_scope: String::new(),
        });
    }
    for (event_kind, field, prior) in [
        ("ResolverChanged", "resolver", linked.resolver.as_ref()),
        (
            "SubregistryChanged",
            "subregistry",
            linked.subregistry.as_ref(),
        ),
    ] {
        let Some(prior) = prior else { continue };
        let (before_state, after_state) = if field == "resolver" {
            (
                json!({"resolver": prior}),
                json!({
                    "source_event": source_event,
                    "token_id": token_id,
                    "resolver": Value::Null,
                }),
            )
        } else {
            (
                json!({"subregistry": prior}),
                json!({
                    "source_event": source_event,
                    "token_id": token_id,
                    "subregistry": Value::Null,
                }),
            )
        };
        output.events.push(EventDraft {
            event_kind: event_kind.to_owned(),
            logical_name_id: logical_name_id.clone(),
            resource_id: linked.resource_id,
            identity_suffix: format!("{event_kind}:terminal:{token_id}"),
            explicit_before: Some(before_state),
            after_state,
            state_scope: String::new(),
        });
    }
}

pub(super) fn discovery_observation_key(
    raw: &crate::schema_v2::RawLogInput,
    token_id: U256,
    resolver: bool,
) -> String {
    let mut bytes = token_id.to_be_bytes::<32>();
    bytes[28..].fill(0);
    let base = format!(
        "{}:{:#x}",
        raw.emitting_address.to_ascii_lowercase(),
        U256::from_be_bytes(bytes)
    );
    if resolver {
        format!("resolver:{base}")
    } else {
        base
    }
}

pub(super) fn resolver_discovery_keys(
    raw: &crate::schema_v2::RawLogInput,
    current_token_id: Option<U256>,
    aliases: &BTreeSet<String>,
) -> anyhow::Result<BTreeSet<String>> {
    let mut keys = current_token_id
        .map(|token_id| discovery_observation_key(raw, token_id, true))
        .into_iter()
        .collect::<BTreeSet<_>>();
    for alias in aliases {
        let token_id = alias
            .parse::<U256>()
            .with_context(|| format!("stored ENSv2 resolver token ID {alias} is malformed"))?;
        keys.insert(discovery_observation_key(raw, token_id, true));
    }
    Ok(keys)
}

pub(super) fn append_token_discovery_closures(
    output: &mut Interpreted,
    raw: &crate::schema_v2::RawLogInput,
    token_id: U256,
    token: Option<&V2TokenState>,
    protected_resolver_keys: &BTreeSet<String>,
) -> anyhow::Result<()> {
    output.discovery.push(DiscoveryDraft::Close {
        edge_kind: "subregistry".to_owned(),
        observation_key: discovery_observation_key(raw, token_id, false),
    });
    let empty = BTreeSet::new();
    let aliases = token
        .map(|token| &token.resolver_discovery_aliases)
        .unwrap_or(&empty);
    append_resolver_discovery_closures(
        output,
        raw,
        Some(token_id),
        aliases,
        protected_resolver_keys,
    )
}

pub(super) fn append_resolver_discovery_closures(
    output: &mut Interpreted,
    raw: &crate::schema_v2::RawLogInput,
    current_token_id: Option<U256>,
    aliases: &BTreeSet<String>,
    protected_resolver_keys: &BTreeSet<String>,
) -> anyhow::Result<()> {
    for observation_key in
        resolver_discovery_keys(raw, current_token_id, aliases)?.difference(protected_resolver_keys)
    {
        output.discovery.push(DiscoveryDraft::Close {
            edge_kind: "resolver".to_owned(),
            observation_key: observation_key.clone(),
        });
    }
    Ok(())
}
