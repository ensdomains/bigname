mod registrar;
mod topology;
mod transfer;

use alloy_primitives::{Address, U256, hex, keccak256};
use alloy_sol_types::sol;
use anyhow::{Context, bail};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    evm_abi::{address_hex, decode_event_log, decode_event_log_data_as, hex_string, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::{
            admitted_label, decoded_label, ens_v2_registry_resource_id,
            ens_v2_registry_token_lineage_id,
        },
        model::RawLogInput,
        state::{State, V2NameTransition, V2TokenState},
    },
};

use super::{
    BindingClosureDraft, DiscoveryDraft, EventDraft, Interpreted, LabelDraft, NameDraft,
    ResourceDraft, ShadowNameDraft, ensure_declared,
};
pub(in crate::schema_v2) use topology::boundary_reassertion;
use topology::{
    append_resolver_discovery_closures, append_terminal_boundaries,
    append_token_discovery_closures, append_v2_name_transitions, discovery_observation_key,
};

pub(super) fn boundary_expiration(transition: V2NameTransition) -> anyhow::Result<Interpreted> {
    topology::boundary_expiration(transition)
}

sol! {
    event RegistryCreated();
    event RawLabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, bytes label, address owner, uint64 expiry, address indexed sender);
    event RawLabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, bytes label, uint64 expiry, address indexed sender);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
    event RawParentUpdated(address indexed parent, bytes label, address indexed sender);
    event Upgraded(address indexed implementation);
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    if selected.source.source_family == "ens_v2_registrar_l1" {
        return registrar::interpret(selected, raw, state);
    }
    registry(selected, raw, state)
}

fn registry(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let initial_transitions = state.refresh_dirty_v2_names(raw.block_timestamp.unix_timestamp());
    let mut initial_output = Interpreted::new();
    append_v2_name_transitions(
        &mut initial_output,
        initial_transitions,
        raw,
        &selected.event.name,
        None,
    );
    let mut output = match selected.event.name.as_str() {
        "RegistryCreated" => {
            decode_event_log::<RegistryCreated>(
                &raw.topics,
                &raw.data,
                "RegistryCreated log is malformed",
            )?;
            ensure_declared(selected, &["RegistryCreated"])?;
            let mut output = single_event(
                "RegistryCreated",
                None,
                None,
                json!({"source_event":"RegistryCreated","registry":raw.emitting_address,"contract_instance_id":selected.contract_instance_id.to_string()}),
            );
            output.discovery.push(DiscoveryDraft::RegistryAnnouncement);
            Ok(output)
        }
        "LabelRegistered" => label_event(selected, raw, state, true),
        "LabelReserved" => label_event(selected, raw, state, false),
        "LabelUnregistered" => transfer::label_unregistered(selected, raw, state),
        "ExpiryUpdated" => {
            let e = decode_event_log::<ExpiryUpdated>(
                &raw.topics,
                &raw.data,
                "ExpiryUpdated log is malformed",
            )?;
            let token_id = u256_word_hex(e.tokenId);
            let Some(before) = state.v2_token(&raw.emitting_address, &token_id) else {
                return Ok(initial_output);
            };
            state.set_v2_expiry(&raw.emitting_address, &token_id, e.newExpiry);
            let transitions = state.refresh_dirty_v2_names(raw.block_timestamp.unix_timestamp());
            ensure_declared(selected, &["ExpiryChanged"])?;
            let after = state
                .v2_token(&raw.emitting_address, &token_id)
                .expect("expiry update retains its token state");
            let logical_name_id = after.name.as_ref().map(|name| name.logical_name_id.clone());
            let event_state = json!({
                "source_event":"ExpiryUpdated",
                "token_id":token_id,
                "expiry":e.newExpiry,
                "sender":address_hex(e.sender),
                "labelhash":after.registration.as_ref().and_then(|registration| registration.get("labelhash")).cloned(),
                "registry_contract_instance_id":selected.contract_instance_id.to_string(),
            });
            let mut output = single_event(
                "ExpiryChanged",
                logical_name_id.clone(),
                after.resource_id,
                event_state.clone(),
            );
            output.events[0].explicit_before = Some(json!({"expiry":before.expiry}));
            if after.registration.is_some() {
                output.events.push(EventDraft {
                    event_kind: "RegistrationRenewed".to_owned(),
                    logical_name_id,
                    resource_id: after.resource_id,
                    identity_suffix: format!("RegistrationRenewed:{token_id}"),
                    explicit_before: Some(json!({"expiry":before.expiry})),
                    after_state: event_state,
                    state_scope: String::new(),
                });
            }
            append_v2_name_transitions(&mut output, transitions, raw, "ExpiryUpdated", None);
            Ok(output)
        }
        "SubregistryUpdated" => {
            let e = decode_event_log::<SubregistryUpdated>(
                &raw.topics,
                &raw.data,
                "SubregistryUpdated log is malformed",
            )?;
            let address = address_hex(e.subregistry);
            let mut output = token_event(
                selected,
                raw,
                state,
                "SubregistryChanged",
                e.tokenId,
                json!({"source_event":"SubregistryUpdated","subregistry":nullable_address(e.subregistry),"sender":address_hex(e.sender)}),
            )?;
            let invalidated = state.apply_v2_subregistry_update(
                &raw.emitting_address,
                &u256_word_hex(e.tokenId),
                (e.subregistry != Address::ZERO).then(|| address.clone()),
            );
            if !invalidated.is_empty() {
                output.events[0]
                    .after_state
                    .as_object_mut()
                    .expect("token event state is an object")
                    .insert(
                        "subregistry_invalidated_token_ids".to_owned(),
                        json!(invalidated),
                    );
            }
            let transitions = state.refresh_dirty_v2_names(raw.block_timestamp.unix_timestamp());
            append_v2_name_transitions(&mut output, transitions, raw, "SubregistryUpdated", None);
            output.discovery.push(DiscoveryDraft::Edge {
                edge_kind: "subregistry".to_owned(),
                to_address: address,
                admission_basis: "linked_subregistry_event".to_owned(),
                observation_key: discovery_observation_key(raw, e.tokenId, false),
            });
            Ok(output)
        }
        "ResolverUpdated" => {
            let e = decode_event_log::<ResolverUpdated>(
                &raw.topics,
                &raw.data,
                "ResolverUpdated log is malformed",
            )?;
            let address = address_hex(e.resolver);
            let mut output = token_event(
                selected,
                raw,
                state,
                "ResolverChanged",
                e.tokenId,
                json!({"source_event":"ResolverUpdated","resolver":nullable_address(e.resolver),"sender":address_hex(e.sender)}),
            )?;
            let aliases = state.set_v2_resolver(
                &raw.emitting_address,
                &u256_word_hex(e.tokenId),
                (e.resolver != Address::ZERO).then(|| address.clone()),
            );
            let protected_tokens =
                state.live_v2_resolver_tokens_sharing(&raw.emitting_address, &aliases);
            let protected = topology::resolver_discovery_keys(raw, None, &protected_tokens)?;
            append_resolver_discovery_closures(&mut output, raw, None, &aliases, &protected)?;
            output.discovery.push(DiscoveryDraft::Edge {
                edge_kind: "resolver".to_owned(),
                to_address: address,
                admission_basis: "protocol_event".to_owned(),
                observation_key: discovery_observation_key(raw, e.tokenId, true),
            });
            Ok(output)
        }
        "TokenResource" => transfer::token_resource(selected, raw, state),
        "TransferSingle" => transfer::transfer_single(selected, raw, state),
        "TransferBatch" => transfer::transfer_batch(selected, raw, state),
        "EACRolesChanged" => transfer::permission(selected, raw, state),
        "TokenRegenerated" => transfer::token_regenerated(selected, raw, state),
        "ParentUpdated" => {
            let e = decode_event_log_data_as::<RawParentUpdated>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "ParentUpdated log is malformed",
            )?;
            let raw_label = e.label.to_vec();
            ensure_declared(selected, &["ParentChanged"])?;
            let decoded_label = decoded_label(&raw_label);
            let label = admitted_label(&raw_label);
            let parent = (e.parent != Address::ZERO).then(|| address_hex(e.parent));
            state.set_v2_parent_claim(&raw.emitting_address, parent.clone(), &raw_label);
            let transitions = state.refresh_dirty_v2_names(raw.block_timestamp.unix_timestamp());
            let mut output = single_event(
                "ParentChanged",
                None,
                None,
                json!({
                    "source_event":"ParentUpdated",
                    "parent":nullable_address(e.parent),
                    "label":label,
                    "decoded_label":decoded_label,
                    "raw_label_hex":hex::encode(&raw_label),
                    "sender":address_hex(e.sender),
                }),
            );
            output.labels.push(LabelDraft {
                raw_label: raw_label.clone(),
                source_kind: "ParentUpdated_label".to_owned(),
            });
            if label.is_none()
                && let Some(parent) = parent.as_deref()
                && let Some((raw_labels, namehash)) = state.v2_shadow_name_for_parent_claim(
                    parent,
                    &selected.source.namespace,
                    &raw_label,
                    raw.block_timestamp.unix_timestamp(),
                )
            {
                output.shadow_names.push(ShadowNameDraft {
                    raw_labels,
                    namehash,
                    source_kind: "ParentUpdated_label".to_owned(),
                });
            }
            append_v2_name_transitions(&mut output, transitions, raw, "ParentUpdated", None);
            Ok(output)
        }
        "Upgraded" => upgraded(selected, raw),
        name => bail!("unsupported ENSv2 registry event {name}"),
    }?;
    initial_output.append(&mut output);
    Ok(initial_output)
}

fn label_event(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    registered: bool,
) -> anyhow::Result<Interpreted> {
    let (token_id, label_hash, label, after) = if registered {
        let e = decode_event_log_data_as::<RawLabelRegistered>(
            &raw.topics,
            &raw.data,
            &selected.event.topic0,
            "LabelRegistered log is malformed",
        )?;
        (
            e.tokenId,
            e.labelHash,
            e.label.to_vec(),
            json!({"source_event":"LabelRegistered","registrant":address_hex(e.owner),"expiry":e.expiry,"sender":address_hex(e.sender)}),
        )
    } else {
        let e = decode_event_log_data_as::<RawLabelReserved>(
            &raw.topics,
            &raw.data,
            &selected.event.topic0,
            "LabelReserved log is malformed",
        )?;
        (
            e.tokenId,
            e.labelHash,
            e.label.to_vec(),
            json!({"source_event":"LabelReserved","expiry":e.expiry,"sender":address_hex(e.sender)}),
        )
    };
    if keccak256(&label) != label_hash {
        bail!("{} label does not hash to labelHash", selected.event.name);
    }
    let decoded_label = decoded_label(&label);
    let surface_label = admitted_label(&label);
    let kind = if registered {
        "RegistrationGranted"
    } else {
        "RegistrationReserved"
    };
    ensure_declared(selected, &[kind])?;
    let name = surface_label.as_deref().and_then(|label| {
        state.v2_name_for_registration(
            &raw.emitting_address,
            &selected.source.namespace,
            label,
            raw.block_timestamp.unix_timestamp(),
        )
    });
    let direct_name = name.is_some();
    let logical_name_id = name.as_ref().map(|name| name.logical_name_id.clone());
    let token_id = u256_word_hex(token_id);
    let reservation_resource = (!registered && token_id.ends_with("00000000")).then(|| {
        let resource_id =
            ens_v2_registry_resource_id(&raw.chain_id, selected.contract_instance_id, &token_id);
        let token_lineage_id = ens_v2_registry_token_lineage_id(
            &raw.chain_id,
            selected.contract_instance_id,
            &token_id,
        );
        (token_id.clone(), resource_id, token_lineage_id)
    });
    let mut event_state = merge(
        after,
        json!({
            "label": surface_label,
            "decoded_label": decoded_label,
            "raw_label_hex": hex::encode(&label),
            "labelhash": hex_string(label_hash),
            "token_id": token_id,
            "namehash": name.as_ref().map(|name| &name.namehash),
            "raw_labels": name.as_ref().map(|name| &name.labels),
            "resource_pending": registered,
            "status":if registered {"registered"} else {"reserved"},
            "registry_contract_instance_id": selected.contract_instance_id.to_string(),
        }),
    );
    if let Some((upstream_resource, resource_id, token_lineage_id)) = reservation_resource.as_ref()
    {
        event_state = merge(
            event_state,
            json!({
                "upstream_resource":upstream_resource,
                "resource_id":resource_id.to_string(),
                "token_lineage_id":token_lineage_id.to_string(),
                "reservation_resource":true,
            }),
        );
    }
    let replaced = state.replace_v2_registration(
        &raw.emitting_address,
        &token_id,
        selected.contract_instance_id,
        &selected.source.namespace,
        &label,
        event_state
            .get("expiry")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        registered.then(|| event_state.clone()),
    );
    if let Some((upstream_resource, resource_id, token_lineage_id)) = reservation_resource {
        state.attach_v2_unbound_resource(
            &raw.emitting_address,
            &token_id,
            upstream_resource,
            resource_id,
            Some(token_lineage_id),
        );
    }
    let linked = state
        .v2_token(&raw.emitting_address, &token_id)
        .expect("label event installs its token state");
    if let (Some(upstream_resource), Some(resource_id)) =
        (linked.upstream_resource.as_ref(), linked.resource_id)
    {
        event_state = merge(
            event_state,
            json!({
                "upstream_resource":upstream_resource,
                "resource_id":resource_id.to_string(),
            }),
        );
    }
    let mut output = single_event(kind, logical_name_id, linked.resource_id, event_state);
    if let Some(resource_id) = linked.resource_id {
        output.resources.push(ResourceDraft {
            resource_id,
            token_lineage_id: linked.token_lineage_id,
        });
    }
    output.labels.push(LabelDraft {
        raw_label: label,
        source_kind: format!("{}_label", selected.event.name),
    });
    if let Some(name) = name {
        output.names.push(NameDraft {
            labels: name.labels,
            namehash: name.namehash,
            resource_id: linked.resource_id,
            token_lineage_id: linked.token_lineage_id,
            surface_binding_id: None,
            bind: false,
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: "ens_v2".to_owned(),
            source_kind: format!("{}_label", selected.event.name),
            preimage_metadata: None,
        });
        // Closures are arm-wide per logical name, so only a registration assert may clear the
        // name's stale bindings; a reservation would close another holder's live binding.
        if registered {
            output.binding_closures.push(BindingClosureDraft {
                logical_name_id: name.logical_name_id,
                authority_arm: "ens_v2".to_owned(),
            });
        }
    }
    for (replaced_token, previous) in &replaced {
        append_terminal_boundaries(
            &mut output,
            state,
            Some(previous),
            replaced_token,
            selected.event.name.as_str(),
        );
        let replaced_token_id = replaced_token
            .parse::<U256>()
            .with_context(|| format!("stored ENSv2 token ID {replaced_token} is malformed"))?;
        let mut candidates = previous.resolver_discovery_aliases.clone();
        candidates.insert(replaced_token.clone());
        let protected_tokens =
            state.live_v2_resolver_tokens_sharing(&raw.emitting_address, &candidates);
        let protected_resolver_keys =
            topology::resolver_discovery_keys(raw, None, &protected_tokens)?;
        append_token_discovery_closures(
            &mut output,
            selected,
            raw,
            state,
            replaced_token_id,
            Some(previous),
            &protected_resolver_keys,
        )?;
    }
    let transitions = state.refresh_dirty_v2_names(raw.block_timestamp.unix_timestamp());
    append_v2_name_transitions(
        &mut output,
        transitions,
        raw,
        &selected.event.name,
        direct_name.then_some((&raw.emitting_address, token_id.as_str())),
    );
    let mut candidates = linked.resolver_discovery_aliases.clone();
    candidates.insert(token_id.clone());
    let protected_tokens =
        state.live_v2_resolver_tokens_sharing(&raw.emitting_address, &candidates);
    let protected_resolver_keys = topology::resolver_discovery_keys(raw, None, &protected_tokens)?;
    append_token_discovery_closures(
        &mut output,
        selected,
        raw,
        state,
        token_id
            .parse::<U256>()
            .with_context(|| format!("stored ENSv2 token ID {token_id} is malformed"))?,
        Some(&linked),
        &protected_resolver_keys,
    )?;
    Ok(output)
}

fn token_event(
    selected: &Selected,
    raw: &RawLogInput,
    state: &State,
    kind: &str,
    token_id: U256,
    after: Value,
) -> anyhow::Result<Interpreted> {
    let linked = state.v2_token(&raw.emitting_address, &u256_word_hex(token_id));
    token_state_event(selected, kind, token_id, linked.as_ref(), after)
}

fn token_state_event(
    selected: &Selected,
    kind: &str,
    token_id: U256,
    linked: Option<&V2TokenState>,
    after: Value,
) -> anyhow::Result<Interpreted> {
    ensure_declared(selected, &[kind])?;
    let logical_name_id = linked
        .and_then(|state| state.name.as_ref())
        .map(|name| name.logical_name_id.clone());
    let resource_id = linked.and_then(|state| state.resource_id);
    let mut output = single_event(
        kind,
        logical_name_id,
        resource_id,
        merge(after, json!({"token_id":u256_word_hex(token_id)})),
    );
    if let Some(resource_id) = resource_id {
        output.resources.push(ResourceDraft {
            resource_id,
            token_lineage_id: linked.and_then(|state| state.token_lineage_id),
        });
    }
    Ok(output)
}

fn upgraded(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<Interpreted> {
    let e = decode_event_log::<Upgraded>(&raw.topics, &raw.data, "Upgraded log is malformed")?;
    ensure_declared(selected, &["Upgraded"])?;
    let implementation = address_hex(e.implementation);
    let mut output = single_event(
        "Upgraded",
        None,
        None,
        json!({"source_event":"Upgraded","proxy_address":raw.emitting_address,"implementation":implementation}),
    );
    output.discovery.push(DiscoveryDraft::Edge {
        edge_kind: "proxy_implementation".to_owned(),
        to_address: implementation,
        admission_basis: "erc1967_upgrade_event".to_owned(),
        observation_key: format!(
            "proxy-implementation:{}",
            raw.emitting_address.to_ascii_lowercase()
        ),
    });
    Ok(output)
}

fn nullable_address(address: Address) -> Value {
    if address == Address::ZERO {
        Value::Null
    } else {
        Value::String(address_hex(address))
    }
}
fn merge(mut left: Value, right: Value) -> Value {
    left.as_object_mut()
        .expect("event state is object")
        .extend(right.as_object().expect("event state is object").clone());
    left
}
fn single_event(
    kind: &str,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    after_state: Value,
) -> Interpreted {
    let mut output = Interpreted::new();
    output.events.push(EventDraft {
        event_kind: kind.to_owned(),
        logical_name_id,
        resource_id,
        identity_suffix: kind.to_owned(),
        explicit_before: None,
        after_state,
        state_scope: String::new(),
    });
    output
}
