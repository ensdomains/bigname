mod registrar;
mod topology;
mod transfer;

use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::sol;
use anyhow::{Context, bail};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    evm_abi::{address_hex, decode_event_log, hex_string, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::require_label,
        model::RawLogInput,
        state::{State, V2TokenState},
    },
};

use super::{
    BindingClosureDraft, DiscoveryDraft, EventDraft, Interpreted, LabelDraft, NameDraft,
    ResourceDraft, ensure_declared,
};
use topology::{append_terminal_boundaries, append_v2_name_transitions};

pub(super) fn boundary_expiration(
    transition: crate::schema_v2::state::V2NameTransition,
) -> anyhow::Result<Interpreted> {
    topology::boundary_expiration(transition)
}

sol! {
    event RegistryCreated();
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
    event ParentUpdated(address indexed parent, string label, address indexed sender);
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
    let initial_transitions = state.refresh_v2_names(raw.block_timestamp.unix_timestamp());
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
        "LabelUnregistered" => label_unregistered(selected, raw, state),
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
            let transitions = state.refresh_v2_names(raw.block_timestamp.unix_timestamp());
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
            output.events.push(EventDraft {
                event_kind: "RegistrationRenewed".to_owned(),
                logical_name_id,
                resource_id: after.resource_id,
                identity_suffix: format!("RegistrationRenewed:{token_id}"),
                explicit_before: Some(json!({"expiry":before.expiry})),
                after_state: event_state,
                state_scope: String::new(),
            });
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
            state.set_v2_subregistry(
                &raw.emitting_address,
                &u256_word_hex(e.tokenId),
                (e.subregistry != Address::ZERO).then(|| address.clone()),
            );
            let transitions = state.refresh_v2_names(raw.block_timestamp.unix_timestamp());
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
            state.set_v2_resolver(
                &raw.emitting_address,
                &u256_word_hex(e.tokenId),
                (e.resolver != Address::ZERO).then(|| address.clone()),
            );
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
            let e = decode_event_log::<ParentUpdated>(
                &raw.topics,
                &raw.data,
                "ParentUpdated log is malformed",
            )?;
            require_label(&e.label)?;
            ensure_declared(selected, &["ParentChanged"])?;
            let parent = address_hex(e.parent);
            state.set_v2_parent_claim(
                &raw.emitting_address,
                (e.parent != Address::ZERO).then(|| parent.clone()),
                e.label.clone(),
            );
            let transitions = state.refresh_v2_names(raw.block_timestamp.unix_timestamp());
            let mut output = single_event(
                "ParentChanged",
                None,
                None,
                json!({"source_event":"ParentUpdated","parent":nullable_address(e.parent),"label":e.label,"sender":address_hex(e.sender)}),
            );
            output.labels.push(LabelDraft {
                raw_label: e.label,
                source_kind: "ParentUpdated_label".to_owned(),
            });
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
        let e = decode_event_log::<LabelRegistered>(
            &raw.topics,
            &raw.data,
            "LabelRegistered log is malformed",
        )?;
        (
            e.tokenId,
            e.labelHash,
            e.label,
            json!({"source_event":"LabelRegistered","registrant":address_hex(e.owner),"expiry":e.expiry,"sender":address_hex(e.sender)}),
        )
    } else {
        let e = decode_event_log::<LabelReserved>(
            &raw.topics,
            &raw.data,
            "LabelReserved log is malformed",
        )?;
        (
            e.tokenId,
            e.labelHash,
            e.label,
            json!({"source_event":"LabelReserved","expiry":e.expiry,"sender":address_hex(e.sender)}),
        )
    };
    require_label(&label)?;
    if keccak256(label.as_bytes()) != label_hash {
        bail!("{} label does not hash to labelHash", selected.event.name);
    }
    let kind = if registered {
        "RegistrationGranted"
    } else {
        "RegistrationReserved"
    };
    ensure_declared(selected, &[kind])?;
    let name = state.v2_name_for_registration(
        &raw.emitting_address,
        &selected.source.namespace,
        &label,
        raw.block_timestamp.unix_timestamp(),
    );
    let logical_name_id = name.as_ref().map(|name| name.logical_name_id.clone());
    let event_state = merge(
        after,
        json!({
            "label": label,
            "labelhash": hex_string(label_hash),
            "token_id": u256_word_hex(token_id),
            "namehash": name.as_ref().map(|name| &name.namehash),
            "raw_labels": name.as_ref().map(|name| &name.labels),
            "resource_pending": registered,
            "status":if registered {"registered"} else {"reserved"},
            "registry_contract_instance_id": selected.contract_instance_id.to_string(),
        }),
    );
    let replaced = state.replace_v2_registration(
        &raw.emitting_address,
        &u256_word_hex(token_id),
        selected.contract_instance_id,
        &selected.source.namespace,
        &label,
        event_state
            .get("expiry")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        registered.then(|| event_state.clone()),
    );
    let transitions = state.refresh_v2_names(raw.block_timestamp.unix_timestamp());
    let mut output = single_event(kind, logical_name_id, None, event_state);
    output.labels.push(LabelDraft {
        raw_label: label.clone(),
        source_kind: format!("{}_label", selected.event.name),
    });
    if let Some(name) = name {
        output.names.push(NameDraft {
            labels: name.labels,
            namehash: name.namehash,
            resource_id: None,
            token_lineage_id: None,
            surface_binding_id: None,
            bind: false,
            binding_kind: "declared_registry_path".to_owned(),
            source_kind: format!("{}_label", selected.event.name),
            preimage_metadata: None,
        });
        output.binding_closures.push(BindingClosureDraft {
            logical_name_id: name.logical_name_id,
        });
    }
    for (replaced_token, previous) in &replaced {
        append_terminal_boundaries(
            &mut output,
            Some(previous),
            replaced_token,
            selected.event.name.as_str(),
        );
        let replaced_token_id = replaced_token
            .parse::<U256>()
            .with_context(|| format!("stored ENSv2 token ID {replaced_token} is malformed"))?;
        for (edge_kind, resolver) in [("subregistry", false), ("resolver", true)] {
            output.discovery.push(DiscoveryDraft::Close {
                edge_kind: edge_kind.to_owned(),
                observation_key: discovery_observation_key(raw, replaced_token_id, resolver),
            });
        }
    }
    append_v2_name_transitions(
        &mut output,
        transitions,
        raw,
        &selected.event.name,
        Some((&raw.emitting_address, &u256_word_hex(token_id))),
    );
    for (edge_kind, resolver) in [("subregistry", false), ("resolver", true)] {
        output.discovery.push(DiscoveryDraft::Close {
            edge_kind: edge_kind.to_owned(),
            observation_key: discovery_observation_key(raw, token_id, resolver),
        });
    }
    Ok(output)
}

fn label_unregistered(
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
    let transitions = state.refresh_v2_names(raw.block_timestamp.unix_timestamp());
    let mut output = token_state_event(
        selected,
        "RegistrationReleased",
        event.tokenId,
        linked.as_ref(),
        json!({"source_event":"LabelUnregistered","sender":address_hex(event.sender)}),
    )?;
    append_terminal_boundaries(&mut output, linked.as_ref(), &token_id, "LabelUnregistered");
    append_v2_name_transitions(&mut output, transitions, raw, "LabelUnregistered", None);
    for (edge_kind, resolver) in [("subregistry", false), ("resolver", true)] {
        output.discovery.push(DiscoveryDraft::Close {
            edge_kind: edge_kind.to_owned(),
            observation_key: discovery_observation_key(raw, event.tokenId, resolver),
        });
    }
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

fn discovery_observation_key(raw: &RawLogInput, token_id: U256, resolver: bool) -> String {
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
