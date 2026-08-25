use super::{model::PriorEventInput, protocol::v1::unmasked_word, state::State};
use {serde_json::Value, uuid::Uuid};
#[path = "state_restore_v1_transfer.rs"]
mod v1_transfer;
pub(super) fn rebuild_v2_indexes(state: &mut State) {
    state.rebuild_v2_token_indexes();
}
pub(super) fn v2(state: &mut State, event: &PriorEventInput) {
    if event.source_family == "ens_v2_resolver_l1" && event.event_kind == "PreimageObserved" {
        if event
            .after_state
            .get("source_event")
            .and_then(Value::as_str)
            == Some("AliasChanged")
            && event
                .after_state
                .get("visibility_state")
                .and_then(Value::as_str)
                != Some("shadow")
            && let Some(logical_name_id) = event.logical_name_id.as_ref()
        {
            state.observe_name_surface(logical_name_id.clone());
        }
        if let (Some(resolver), Some(upstream_resource), Some(logical_name_id)) = (
            event.after_state.get("resolver").and_then(Value::as_str),
            event
                .after_state
                .get("upstream_resource")
                .and_then(Value::as_str),
            event.logical_name_id.as_ref(),
        ) {
            state.observe_name_surface(logical_name_id.clone());
            state.observe_v2_resolver_hint(
                resolver,
                upstream_resource,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("selector")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({})),
            );
        }
        return;
    }
    if !matches!(
        event.source_family.as_str(),
        "ens_v2_registry_l1" | "ens_v2_root_l1"
    ) {
        return;
    }
    let Some(emitter) = event
        .state_scope
        .as_deref()
        .and_then(|scope| scope.split(':').next())
    else {
        return;
    };
    let token = event.after_state.get("token_id").and_then(Value::as_str);
    match event.event_kind.as_str() {
        "RegistrationGranted" | "RegistrationReserved" => {
            let Some(token) = token else { return };
            let raw_label = raw_label(&event.after_state);
            let expiry = event.after_state.get("expiry").and_then(parse_u64);
            let (Some(raw_label), Some(expiry)) = (raw_label, expiry) else {
                return;
            };
            state.restore_v2_registration(
                emitter,
                token,
                event
                    .after_state
                    .get("registry_contract_instance_id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok()),
                &event.namespace,
                &raw_label,
                expiry,
                (event.event_kind == "RegistrationGranted").then(|| event.after_state.clone()),
            );
            state.restore_v2_unbound_resource(emitter, token, event);
        }
        "TokenResourceLinked" => {
            let (Some(token), Some(resource_id)) = (token, event.resource_id) else {
                return;
            };
            let upstream = event
                .after_state
                .get("resource")
                .or_else(|| event.after_state.get("upstream_resource"))
                .and_then(Value::as_str)
                .unwrap_or("-")
                .to_owned();
            let lineage = event
                .after_state
                .get("token_lineage_id")
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok());
            state.link_v2_resource(emitter, token, upstream, resource_id, lineage);
        }
        "TokenRegenerated" => {
            let old = event
                .after_state
                .get("old_token_id")
                .and_then(Value::as_str);
            let new = event
                .after_state
                .get("new_token_id")
                .and_then(Value::as_str);
            if let (Some(old), Some(new)) = (old, new) {
                state.regenerate_v2_token(emitter, old, new);
            }
        }
        "TokenControlTransferred" => {
            let (Some(token), Some(registrant)) =
                (token, event.after_state.get("to").and_then(Value::as_str))
            else {
                return;
            };
            state.transfer_v2_registrant(emitter, token, registrant.to_owned());
        }
        "RegistrationReleased" => {
            if !matches!(
                event
                    .after_state
                    .get("terminal_reason")
                    .and_then(Value::as_str),
                Some("registry_name_binding_changed" | "registry_name_binding_expired")
            ) && let Some(token) = token
            {
                state.release_v2_token(emitter, token);
            }
        }
        "ResolverChanged" => {
            let Some(token) = token else { return };
            let resolver = event
                .after_state
                .get("resolver")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.set_v2_resolver(emitter, token, resolver);
        }
        "SubregistryChanged" => {
            let Some(token) = token else { return };
            let subregistry = event
                .after_state
                .get("subregistry")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.set_v2_subregistry(emitter, token, subregistry);
        }
        "ExpiryChanged" => {
            let (Some(token), Some(expiry)) =
                (token, event.after_state.get("expiry").and_then(parse_u64))
            else {
                return;
            };
            state.set_v2_expiry(emitter, token, expiry);
        }
        "ParentChanged" => {
            let Some(raw_label) = raw_label(&event.after_state) else {
                return;
            };
            let parent = event
                .after_state
                .get("parent")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.set_v2_parent_claim(emitter, parent, &raw_label);
        }
        _ => {}
    }
}
fn raw_label(after_state: &Value) -> Option<Vec<u8>> {
    after_state
        .get("raw_label_hex")
        .and_then(Value::as_str)
        .and_then(|value| alloy_primitives::hex::decode(value).ok())
        .or_else(|| {
            after_state
                .get("label")
                .and_then(Value::as_str)
                .map(|label| label.as_bytes().to_vec())
        })
        .or_else(|| {
            after_state
                .get("raw_labels")
                .and_then(Value::as_array)
                .and_then(|labels| labels.first())
                .and_then(Value::as_str)
                .map(|label| label.as_bytes().to_vec())
        })
}

pub(super) fn v1(state: &mut State, event: &PriorEventInput) {
    let source_event = event
        .after_state
        .get("source_event")
        .and_then(Value::as_str);
    if event.source_family == "ens_v2_migration_l1"
        && source_event == Some("NameRenewed")
        && let (Some(namehash), Some(expiry)) = (
            event.after_state.get("namehash").and_then(Value::as_str),
            event.after_state.get("wrapper_expiry").and_then(parse_u64),
        )
    {
        state.restore_v1_correlated_wrapper_expiry(&event.namespace, namehash, expiry);
    }
    if !(event.source_family.starts_with("ens_v1_")
        || event.source_family.starts_with("basenames_"))
    {
        return;
    }
    if event.event_kind == "PreimageObserved"
        && event.logical_name_id.is_some()
        && let Some(namehash) = event.after_state.get("namehash").and_then(Value::as_str)
    {
        state.observe_v1_surface(&event.namespace, namehash);
    }
    if matches!(source_event, Some("NewOwner" | "Transfer"))
        && let Some(namehash) = event
            .after_state
            .get("child_node")
            .or_else(|| event.after_state.get("node"))
            .and_then(Value::as_str)
    {
        if unmasked_word::body_has_unmasked_owner_word(&event.after_state) {
            state.forget_v1_registry_owner(&event.namespace, namehash);
        } else if let Some(owner) = event.after_state.get("owner").and_then(Value::as_str) {
            state.set_v1_registry_owner(&event.namespace, namehash, owner.to_owned());
            state.remember_v1_registry_authority(
                &event.namespace,
                namehash,
                super::state::V1NameState {
                    logical_name_id: event
                        .logical_name_id
                        .clone()
                        .unwrap_or_else(|| format!("{}:{namehash}", event.namespace)),
                    surface_known: event.logical_name_id.is_some(),
                    resource_id: super::common::stable_uuid(&format!(
                        "resource:registry-only:{}:{namehash}",
                        event.chain_id
                    )),
                    token_lineage_id: None,
                    authority_source_family: event.source_family.clone(),
                    source_manifest_id: event.source_manifest_id,
                    labelhash: event
                        .after_state
                        .get("labelhash")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    expiry: None,
                    owner: Some(owner.to_owned()),
                    authority_key: Some(format!("registry-only:{}:{namehash}", event.chain_id)),
                    wrapper_fallback: false,
                },
            );
        }
    }
    if source_event == Some("NewOwner") {
        let node = event.after_state.get("child_node").and_then(Value::as_str);
        if event
            .after_state
            .get("emitter_role")
            .and_then(Value::as_str)
            == Some("registry")
            && let Some(node) = node
        {
            state.mark_v1_migrated(&event.namespace, node);
        }
    }
    if source_event == Some("NewResolver")
        && let Some(namehash) = event.after_state.get("node").and_then(Value::as_str)
    {
        let resolver = event
            .after_state
            .get("resolver")
            .and_then(Value::as_str)
            .filter(|resolver| {
                !resolver.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
            })
            .map(str::to_owned);
        state.set_v1_resolver(&event.namespace, namehash, resolver);
    }
    if event.source_family == "ens_v1_wrapper_l1"
        && event.event_kind == "PermissionScopeChanged"
        && let (Some(namehash), Some(fuses), Some(expiry)) = (
            event.after_state.get("node").and_then(Value::as_str),
            event.after_state.get("fuses").and_then(parse_u32),
            event.after_state.get("expiry").and_then(parse_u64),
        )
    {
        state.restore_v1_wrapper_data(&event.namespace, namehash, fuses, expiry);
    }
    if source_event == Some("ExpiryExtended")
        && event.event_kind == "ExpiryChanged"
        && let (Some(namehash), Some(expiry)) = (
            event.after_state.get("node").and_then(Value::as_str),
            event.after_state.get("expiry").and_then(parse_u64),
        )
    {
        state.update_v1_wrapper_expiry(&event.namespace, namehash, expiry);
    }
    if event.source_family == "ens_v1_registrar_l1"
        && event.event_kind == "ExpiryChanged"
        && event
            .after_state
            .get("authority_kind")
            .and_then(Value::as_str)
            == Some("wrapper")
        && let (Some(namehash), Some(expiry)) = (
            event.after_state.get("node").and_then(Value::as_str),
            event.after_state.get("expiry").and_then(parse_u64),
        )
    {
        state.update_v1_wrapper_expiry(&event.namespace, namehash, expiry);
    }
    if event.event_kind == "AuthorityTransferred"
        && matches!(source_event, Some("NewOwner" | "Transfer"))
        && let Some(namehash) = event
            .after_state
            .get("child_node")
            .or_else(|| event.after_state.get("node"))
            .and_then(Value::as_str)
    {
        match event
            .after_state
            .get("authority_kind")
            .and_then(Value::as_str)
        {
            Some("registrar") => {
                // Load-bearing assumption for masked writes, whose live arm deliberately
                // leaves the registrar untouched: state.rs keeps the v1_registrars snapshot
                // in sync with v1_names while registrar authority is current, which is what
                // makes this reactivate an equivalent replay.
                if let Some(owner) = event.after_state.get("owner").and_then(Value::as_str) {
                    state.reactivate_v1_registrar_for_owner(
                        &event.namespace,
                        namehash,
                        owner,
                        event
                            .block_timestamp
                            .map(|timestamp| timestamp.unix_timestamp())
                            .unwrap_or(i64::MIN),
                    );
                }
                return;
            }
            None => {
                if state
                    .v1_name(&event.namespace, namehash)
                    .is_some_and(|authority| authority.token_lineage_id.is_none())
                {
                    state.release_v1_name(&event.namespace, namehash);
                }
                return;
            }
            _ => {}
        }
    }
    if event.event_kind == "AuthorityTransferred"
        && event
            .after_state
            .get("authority_kind")
            .and_then(Value::as_str)
            == Some("registry_only")
        && let (Some(resource_id), Some(namehash)) = (
            event.resource_id,
            event
                .after_state
                .get("child_node")
                .or_else(|| event.after_state.get("node"))
                .and_then(Value::as_str),
        )
    {
        state.observe_v1_registry(
            &event.namespace,
            namehash,
            event
                .logical_name_id
                .clone()
                .unwrap_or_else(|| format!("{}:{namehash}", event.namespace)),
            event.logical_name_id.is_some(),
            resource_id,
            event.source_family.clone(),
            event
                .after_state
                .get("owner")
                .and_then(Value::as_str)
                .map(str::to_owned),
            event
                .after_state
                .get("authority_key")
                .and_then(Value::as_str)
                .map(str::to_owned),
        );
        return;
    }
    if event.event_kind == "RegistrationReleased"
        && source_event == Some("RegistrationReleased")
        && let Some(namehash) = event.after_state.get("namehash").and_then(Value::as_str)
    {
        state.restore_v1_registration_release(&event.namespace, namehash);
        return;
    }
    let (Some(logical_name_id), Some(resource_id)) =
        (event.logical_name_id.as_ref(), event.resource_id)
    else {
        return;
    };
    let lineage = event
        .after_state
        .get("token_lineage_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    let expiry = event.after_state.get("expiry").and_then(parse_i64);
    match source_event {
        Some("NameRegistered" | "NameRenewed") if event.event_kind == "RegistrationGranted" => {
            let (Some(namehash), Some(lineage)) = (
                event.after_state.get("namehash").and_then(Value::as_str),
                lineage,
            ) else {
                return;
            };
            let registration = source_event == Some("NameRegistered");
            let current = state.v1_name(&event.namespace, namehash);
            let retained_authority_owner = event
                .after_state
                .get("authority_owner")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let registration_registry_owner = registration
                .then(|| state.v1_registry_owner(&event.namespace, namehash))
                .flatten()
                .filter(|owner| {
                    !owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
                });
            let event_registrant = event
                .after_state
                .get("registrant")
                .or_else(|| event.after_state.get("owner"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let registrar_owner = retained_authority_owner
                .or(registration_registry_owner)
                .or(event_registrant);
            let make_current = current.is_none_or(|current| {
                let same_family = current.authority_source_family == event.source_family;
                current.authority_source_family != "ens_v1_wrapper_l1"
                    && (registration || same_family)
            });
            state.observe_v1_registrar(
                &event.namespace,
                namehash,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("surface_known")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                resource_id,
                lineage,
                event.source_family.clone(),
                event.source_manifest_id,
                event
                    .after_state
                    .get("labelhash")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                expiry,
                registrar_owner,
                event
                    .after_state
                    .get("authority_key")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                false,
                make_current,
            );
        }
        Some("NameRenewed") if event.event_kind == "RegistrationRenewed" => {
            let (Some(namehash), Some(lineage)) = (
                event.after_state.get("namehash").and_then(Value::as_str),
                lineage,
            ) else {
                return;
            };
            let make_current = state
                .v1_name(&event.namespace, namehash)
                .is_none_or(|current| current.authority_source_family == event.source_family);
            let retained = state.v1_registrar(&event.namespace, namehash);
            state.observe_v1_registrar(
                &event.namespace,
                namehash,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("surface_known")
                    .and_then(Value::as_bool)
                    .or_else(|| retained.as_ref().map(|state| state.surface_known))
                    .unwrap_or(true),
                resource_id,
                lineage,
                event.source_family.clone(),
                event.source_manifest_id,
                event
                    .after_state
                    .get("labelhash")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| retained.as_ref().and_then(|state| state.labelhash.clone())),
                expiry,
                event
                    .after_state
                    .get("registrant")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| retained.as_ref().and_then(|state| state.owner.clone())),
                event
                    .after_state
                    .get("authority_key")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        retained
                            .as_ref()
                            .and_then(|state| state.authority_key.clone())
                    }),
                false,
                make_current,
            );
        }
        Some("NameWrapped") if event.event_kind == "TokenControlTransferred" => {
            let Some(namehash) = event.after_state.get("node").and_then(Value::as_str) else {
                return;
            };
            state.observe_v1_name(
                &event.namespace,
                namehash,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("surface_known")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                resource_id,
                lineage,
                event.source_family.clone(),
                expiry,
                event
                    .after_state
                    .get("to")
                    .or_else(|| event.after_state.get("owner"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                event
                    .after_state
                    .get("authority_key")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            );
        }
        Some("NameUnwrapped") => {
            let Some(namehash) = event.after_state.get("node").and_then(Value::as_str) else {
                return;
            };
            state.release_v1_name(&event.namespace, namehash);
            if event.after_state.get("reactivated_resource_id").is_some() {
                let at = event
                    .after_state
                    .get("unwrapped_at")
                    .and_then(Value::as_i64)
                    .unwrap_or(i64::MIN);
                state.reactivate_v1_registrar(&event.namespace, namehash, at);
            }
        }
        _ => {}
    }

    v1_transfer::restore(state, event);
}

fn parse_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn parse_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn parse_u32(value: &Value) -> Option<u32> {
    parse_u64(value).and_then(|value| u32::try_from(value).ok())
}
