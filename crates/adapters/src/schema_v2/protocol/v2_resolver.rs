use alloy_primitives::U256;
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    evm_abi::{address_hex, decode_event_log, hex_string, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::{decode_dns_labels, namehash, namehash_raw, surface_labels},
        model::RawLogInput,
        state::State,
    },
};

use super::{
    DiscoveryDraft, EventDraft, Interpreted, NameDraft, ResourceDraft, ShadowNameDraft,
    ensure_declared,
    permissions::{V2PermissionState, V2Vocabulary, v2_states},
};

sol! {
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event ContenthashChanged(bytes32 indexed node, bytes hash);
    event NameChanged(bytes32 indexed node, string name);
    event VersionChanged(bytes32 indexed node, uint64 newVersion);
    event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName);
    event NamedResource(uint256 indexed resource, bytes name);
    event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);
    event NamedAddrResource(uint256 indexed resource, bytes name, uint256 indexed coinType);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event Upgraded(address indexed implementation);
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    match selected.event.name.as_str() {
        "AddressChanged" => {
            let event = decode_event_log::<AddressChanged>(
                &raw.topics,
                &raw.data,
                "AddressChanged log is malformed",
            )?;
            let node = hex_string(event.node);
            record(
                selected,
                raw,
                state,
                &node,
                json!({
                    "source_event": "AddressChanged",
                    "resolver": raw.emitting_address,
                    "resolver_contract_instance_id": selected.contract_instance_id.to_string(),
                    "node": node,
                    "record_key": format!("addr:{}", event.coinType),
                    "record_family": "addr",
                    "selector_key": event.coinType.to_string(),
                    "value_retained": false,
                    "coin_type": event.coinType.to_string(),
                    "address_bytes_hex": hex_string(event.newAddress),
                }),
            )
        }
        "TextChanged" => {
            let event = decode_event_log::<TextChanged>(
                &raw.topics,
                &raw.data,
                "TextChanged log is malformed",
            )?;
            if event.key.trim().is_empty() {
                return Ok(Interpreted::new());
            }
            let node = hex_string(event.node);
            let value_length = event.value.len();
            record(
                selected,
                raw,
                state,
                &node,
                json!({
                    "source_event": "TextChanged",
                    "resolver": raw.emitting_address,
                    "resolver_contract_instance_id": selected.contract_instance_id.to_string(),
                    "node": node,
                    "record_key": format!("text:{}", event.key),
                    "record_family": "text",
                    "selector_key": event.key,
                    "text_key": event.key,
                    "value_retained": true,
                    "value": event.value,
                    "value_length": value_length,
                }),
            )
        }
        "ContenthashChanged" => {
            let event = decode_event_log::<ContenthashChanged>(
                &raw.topics,
                &raw.data,
                "ContenthashChanged log is malformed",
            )?;
            let node = hex_string(event.node);
            record(
                selected,
                raw,
                state,
                &node,
                json!({
                    "source_event": "ContenthashChanged",
                    "resolver": raw.emitting_address,
                    "resolver_contract_instance_id": selected.contract_instance_id.to_string(),
                    "node": node,
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": Value::Null,
                    "value_retained": false,
                    "contenthash_hex": hex_string(event.hash),
                }),
            )
        }
        "NameChanged" => {
            let event = decode_event_log::<NameChanged>(
                &raw.topics,
                &raw.data,
                "NameChanged log is malformed",
            )?;
            let node = hex_string(event.node);
            record(
                selected,
                raw,
                state,
                &node,
                json!({
                    "source_event": "NameChanged",
                    "resolver": raw.emitting_address,
                    "resolver_contract_instance_id": selected.contract_instance_id.to_string(),
                    "node": node,
                    "record_key": "name",
                    "record_family": "name",
                    "selector_key": Value::Null,
                    "value_retained": false,
                    "value_length": event.name.len(),
                }),
            )
        }
        "VersionChanged" => {
            let event = decode_event_log::<VersionChanged>(
                &raw.topics,
                &raw.data,
                "VersionChanged log is malformed",
            )?;
            ensure_declared(selected, &["RecordVersionChanged"])?;
            let node = hex_string(event.node);
            let (logical_name_id, resource_id) = state
                .name_link_by_namehash(&selected.source.namespace, &node)
                .map_or((None, None), |(logical, resource)| {
                    (Some(logical), resource)
                });
            Ok(single_event(
                "RecordVersionChanged",
                logical_name_id,
                resource_id,
                json!({
                    "source_event": "VersionChanged",
                    "resolver": raw.emitting_address,
                    "resolver_contract_instance_id": selected.contract_instance_id.to_string(),
                    "node": node,
                    "record_version": event.newVersion.to_string(),
                }),
            ))
        }
        "AliasChanged" => alias(selected, raw, state),
        "NamedResource" => named_resource(selected, raw, state, NamedKind::Whole),
        "NamedTextResource" => named_resource(selected, raw, state, NamedKind::Text),
        "NamedAddrResource" => named_resource(selected, raw, state, NamedKind::Address),
        "EACRolesChanged" => permission(selected, raw, state),
        "Upgraded" => upgraded(selected, raw),
        name => bail!("unsupported ENSv2 resolver event {name}"),
    }
}

fn record(
    selected: &Selected,
    _raw: &RawLogInput,
    state: &State,
    node: &str,
    after: Value,
) -> anyhow::Result<Interpreted> {
    ensure_declared(selected, &["RecordChanged"])?;
    let (logical_name_id, resource_id) = state
        .name_link_by_namehash(&selected.source.namespace, node)
        .map_or((None, None), |(logical, resource)| {
            (Some(logical), resource)
        });
    Ok(single_event(
        "RecordChanged",
        logical_name_id,
        resource_id,
        after,
    ))
}

fn alias(selected: &Selected, raw: &RawLogInput, state: &mut State) -> anyhow::Result<Interpreted> {
    let event =
        decode_event_log::<AliasChanged>(&raw.topics, &raw.data, "AliasChanged log is malformed")?;
    ensure_declared(selected, &["AliasChanged", "PreimageObserved"])?;
    let Ok(from_raw_labels) = decode_dns_labels(&event.fromName) else {
        return Ok(Interpreted::new());
    };
    let to_raw_labels = if event.toName.is_empty() {
        None
    } else {
        decode_dns_labels(&event.toName).ok()
    };
    let from_labels = surface_labels(&from_raw_labels);
    let to_labels = to_raw_labels
        .as_ref()
        .and_then(|labels| surface_labels(labels));
    let from_namehash = namehash_raw(from_raw_labels.iter().map(Vec::as_slice));
    let from_logical_name_id = format!("{}:{from_namehash}", selected.source.namespace);
    let to_namehash = to_raw_labels
        .as_ref()
        .map(|labels| namehash_raw(labels.iter().map(Vec::as_slice)));
    let same_endpoint = to_namehash.as_ref() == Some(&from_namehash);
    let to_logical_name_id = to_namehash
        .as_ref()
        .map(|namehash| format!("{}:{namehash}", selected.source.namespace));
    let to_resource_id = to_namehash
        .as_deref()
        .filter(|_| to_labels.is_some())
        .and_then(|namehash| state.name_link_by_namehash(&selected.source.namespace, namehash))
        .and_then(|(_, resource_id)| resource_id);
    let alias_removed = event.toName.is_empty();
    let alias_unknown = !alias_removed && to_raw_labels.is_none();
    let mut output = single_event(
        "AliasChanged",
        Some(from_logical_name_id.clone()),
        to_resource_id,
        json!({
            "source_event": "AliasChanged",
            "resolver": raw.emitting_address,
            "resolver_contract_instance_id": selected.contract_instance_id.to_string(),
            "from_dns_encoded_name": hex_string(&event.fromName),
            "to_dns_encoded_name": hex_string(&event.toName),
            "alias_state": if alias_removed { "removed" } else if alias_unknown { "unknown" } else { "active" },
            "active": !alias_removed && !alias_unknown,
            "from_name": from_labels.as_ref().map(|labels| labels.join(".")),
            "to_name": to_labels.as_ref().map(|labels| labels.join(".")),
            "to_logical_name_id": to_logical_name_id,
            "to_resource_id": to_resource_id.map(|value| value.to_string()),
            "from_namehash": from_namehash,
            "to_namehash": to_namehash,
        }),
    );
    observe_resolver_name(
        selected,
        state,
        &mut output,
        from_raw_labels,
        from_labels,
        None,
    );
    if let Some(to_raw_labels) = to_raw_labels {
        if !same_endpoint {
            observe_resolver_name(selected, state, &mut output, to_raw_labels, to_labels, None);
        }
    }
    Ok(output)
}

enum NamedKind {
    Whole,
    Text,
    Address,
}

fn named_resource(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    kind: NamedKind,
) -> anyhow::Result<Interpreted> {
    let (resource, encoded_name, selector) = match kind {
        NamedKind::Whole => {
            let event = decode_event_log::<NamedResource>(
                &raw.topics,
                &raw.data,
                "NamedResource log is malformed",
            )?;
            (
                event.resource,
                event.name.to_vec(),
                json!({"kind":"resource","key":Value::Null,"hash":Value::Null}),
            )
        }
        NamedKind::Text => {
            let event = decode_event_log::<NamedTextResource>(
                &raw.topics,
                &raw.data,
                "NamedTextResource log is malformed",
            )?;
            (
                event.resource,
                event.name.to_vec(),
                json!({"kind":"text","key":event.key,"hash":hex_string(event.keyHash)}),
            )
        }
        NamedKind::Address => {
            let event = decode_event_log::<NamedAddrResource>(
                &raw.topics,
                &raw.data,
                "NamedAddrResource log is malformed",
            )?;
            (
                event.resource,
                event.name.to_vec(),
                json!({"kind":"address","key":event.coinType.to_string(),"hash":Value::Null}),
            )
        }
    };
    if encoded_name.is_empty() {
        return Ok(Interpreted::new());
    }
    let Ok(raw_labels) = decode_dns_labels(&encoded_name) else {
        return Ok(Interpreted::new());
    };
    ensure_declared(selected, &["PreimageObserved"])?;
    let upstream_resource = u256_word_hex(resource);
    let mut output = Interpreted::new();
    let labels = surface_labels(&raw_labels);
    let (_, logical_name_id, admitted) = observe_resolver_name(
        selected,
        state,
        &mut output,
        raw_labels,
        labels,
        Some(json!({
            "resolver":raw.emitting_address,
            "resolver_contract_instance_id":selected.contract_instance_id.to_string(),
            "upstream_resource":upstream_resource,
            "selector":selector,
        })),
    );
    if admitted {
        state.observe_v2_resolver_hint(
            &raw.emitting_address,
            &upstream_resource,
            logical_name_id,
            selector,
        );
    }
    Ok(output)
}

fn permission(
    selected: &Selected,
    raw: &RawLogInput,
    state: &State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<EACRolesChanged>(
        &raw.topics,
        &raw.data,
        "EACRolesChanged log is malformed",
    )?;
    ensure_declared(selected, &["PermissionChanged"])?;
    let upstream_resource = u256_word_hex(event.resource);
    let resource_id = resource_id(raw, selected, event.resource);
    let (logical_name_id, selector) = state
        .v2_resolver_hint(&raw.emitting_address, &upstream_resource)
        .map(|(logical_name_id, _, selector)| (Some(logical_name_id), selector))
        .unwrap_or_else(|| {
            (
                None,
                json!({
                    "kind":"resource",
                    "key":Value::Null,
                    "hash":Value::Null,
                    "normalized_name":Value::Null,
                    "dns_encoded_name":Value::Null,
                }),
            )
        });
    let (before, after) = v2_states(
        selected,
        raw,
        V2Vocabulary::Resolver,
        V2PermissionState {
            upstream_resource: &upstream_resource,
            account: address_hex(event.account),
            old_bitmap: event.oldRoleBitmap,
            new_bitmap: event.newRoleBitmap,
            root_resource: event.resource == U256::ZERO,
            selector,
        },
    );
    let mut output = single_event(
        "PermissionChanged",
        logical_name_id,
        Some(resource_id),
        after,
    );
    output.events[0].explicit_before = Some(before);
    output.resources.push(ResourceDraft {
        resource_id,
        token_lineage_id: None,
    });
    Ok(output)
}

fn upgraded(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<Upgraded>(&raw.topics, &raw.data, "Upgraded log is malformed")?;
    ensure_declared(selected, &["Upgraded"])?;
    let implementation = address_hex(event.implementation);
    let mut output = single_event(
        "Upgraded",
        None,
        None,
        json!({
            "source_event": "Upgraded",
            "proxy_address": raw.emitting_address,
            "implementation": implementation,
        }),
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

fn name_draft(
    labels: Vec<String>,
    resource_id: Option<Uuid>,
    bind: bool,
    preimage_metadata: Option<Value>,
) -> NameDraft {
    NameDraft {
        namehash: namehash(&labels),
        labels,
        resource_id,
        token_lineage_id: None,
        surface_binding_id: None,
        bind,
        binding_kind: "resolver_alias_path".to_owned(),
        source_kind: "resolver_dns_name".to_owned(),
        preimage_metadata,
    }
}

fn observe_resolver_name(
    selected: &Selected,
    state: &mut State,
    output: &mut Interpreted,
    raw_labels: Vec<Vec<u8>>,
    labels: Option<Vec<String>>,
    preimage_metadata: Option<Value>,
) -> (String, String, bool) {
    let raw_namehash = namehash_raw(raw_labels.iter().map(Vec::as_slice));
    let logical_name_id = format!("{}:{raw_namehash}", selected.source.namespace);
    let admitted = labels.is_some();
    if let Some(labels) = labels {
        output
            .names
            .push(name_draft(labels, None, false, preimage_metadata));
        state.observe_name_surface(logical_name_id.clone());
    } else {
        output.shadow_names.push(ShadowNameDraft {
            raw_labels,
            namehash: raw_namehash.clone(),
            source_kind: "resolver_dns_name".to_owned(),
        });
    }
    (raw_namehash, logical_name_id, admitted)
}

fn resource_id(raw: &RawLogInput, selected: &Selected, resource: U256) -> Uuid {
    crate::schema_v2::common::ens_v2_resolver_resource_id(
        &raw.chain_id,
        selected.contract_instance_id,
        &u256_word_hex(resource),
    )
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
