use alloy_primitives::{U256, keccak256};
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use super::{
    Interpreted, ensure_declared,
    v2_resolver::{observe_resolver_name, single_event, upgraded},
};
use crate::{
    evm_abi::{address_hex, decode_event_log, decode_event_log_data_as, hex_string},
    schema_v2::{
        catalog::Selected,
        common::{
            decode_dns_labels, event_string_selector, event_string_value, namehash_raw,
            surface_labels,
        },
        model::RawLogInput,
        state::State,
    },
};

pub(in crate::schema_v2) mod permissions;

pub(super) fn selected_generation(selected: &Selected) -> bool {
    selected
        .source
        .events
        .iter()
        .any(|event| event.name == "Linked")
}

// Record-ID event ABI, separate from the historical node-keyed resolver generation.
// (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/interfaces/IRecordResolver.sol:L38 @ ens_v2_sepolia_20260903@5da83f6a)
sol! {
    event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name);
    event AddressUpdated(uint256 indexed recordId, uint256 coinType, bytes addressBytes);
    event ContenthashUpdated(uint256 indexed recordId, bytes hash);
    event ABIUpdated(uint256 indexed recordId, uint256 indexed contentType);
    event InterfaceUpdated(uint256 indexed recordId, bytes4 indexed interfaceId, address implementer);
    event RawTextUpdated(uint256 indexed recordId, bytes32 indexed keyHash, bytes key, bytes value);
    event RawDataUpdated(uint256 indexed recordId, bytes32 indexed keyHash, bytes key, bytes value);
    event RawNameUpdated(uint256 indexed recordId, bytes primaryName);
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let mut name_observation = None;
    let (record_id, mut after) = match selected.event.name.as_str() {
        "Linked" => return linked(selected, raw, state),
        "ResourceArgument" => return permissions::argument(selected, raw, state),
        "EACRolesChanged" => return permissions::permission(selected, raw, state),
        "Upgraded" => return upgraded(selected, raw),
        "AddressUpdated" => {
            let e = decode_event_log::<AddressUpdated>(
                &raw.topics,
                &raw.data,
                "AddressUpdated log is malformed",
            )?;
            (
                e.recordId,
                json!({"record_family":"addr", "record_key":format!("addr:{}", e.coinType),
                "selector_key":e.coinType.to_string(), "coin_type":e.coinType.to_string(),
                "value_retained":false, "address_bytes_hex":hex_string(e.addressBytes)}),
            )
        }
        "ContenthashUpdated" => {
            let e = decode_event_log::<ContenthashUpdated>(
                &raw.topics,
                &raw.data,
                "ContenthashUpdated log is malformed",
            )?;
            (
                e.recordId,
                json!({"record_family":"contenthash", "record_key":"contenthash",
                "selector_key":Value::Null, "value_retained":false, "contenthash_hex":hex_string(e.hash)}),
            )
        }
        "ABIUpdated" => {
            let e = decode_event_log::<ABIUpdated>(
                &raw.topics,
                &raw.data,
                "ABIUpdated log is malformed",
            )?;
            (
                e.recordId,
                json!({"record_family":"abi", "record_key":format!("abi:{}", e.contentType),
                "selector_key":e.contentType.to_string(), "content_type":e.contentType.to_string(), "value_retained":false}),
            )
        }
        "InterfaceUpdated" => {
            let e = decode_event_log::<InterfaceUpdated>(
                &raw.topics,
                &raw.data,
                "InterfaceUpdated log is malformed",
            )?;
            (
                e.recordId,
                json!({"record_family":"interface", "record_key":format!("interface:{}", hex_string(e.interfaceId)),
                "selector_key":hex_string(e.interfaceId), "interface_id":hex_string(e.interfaceId),
                "value_retained":false, "implementer":address_hex(e.implementer)}),
            )
        }
        "NameUpdated" => {
            let e = decode_event_log_data_as::<RawNameUpdated>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameUpdated log is malformed",
            )?;
            name_observation = Some(super::raw_name_observation(
                &e.primaryName,
                "NameUpdated_primaryName",
            ));
            (
                e.recordId,
                json!({"record_family":"name", "record_key":"name", "selector_key":Value::Null,
                "value_retained":false, "raw_name":event_string_value(&e.primaryName), "value_length":e.primaryName.len()}),
            )
        }
        "TextUpdated" | "DataUpdated" => {
            let (id, key, key_hash, value) = if selected.event.name == "TextUpdated" {
                let e = decode_event_log_data_as::<RawTextUpdated>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "TextUpdated log is malformed",
                )?;
                (e.recordId, e.key, e.keyHash, e.value)
            } else {
                let e = decode_event_log_data_as::<RawDataUpdated>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "DataUpdated log is malformed",
                )?;
                (e.recordId, e.key, e.keyHash, e.value)
            };
            if key_hash != keccak256(&key) {
                return Ok(Interpreted::new());
            }
            let family = if selected.event.name == "TextUpdated" {
                "text"
            } else {
                "data"
            };
            let selector = event_string_selector(family, &key);
            let mut after = json!({"record_family":selector.record_family, "record_key":selector.record_key,
                "selector_key":selector.selector_key, "value_retained":true, "value_length":value.len(),
                "value":if family == "text" { event_string_value(&value) } else { json!(hex_string(&value)) }});
            selector.retain_raw_selector(&mut after);
            (id, after)
        }
        name => bail!("unsupported record-ID resolver event {name}"),
    };
    ensure_declared(selected, &["RecordChanged"])?;
    metadata(selected, raw, record_id, &mut after);
    let mut output = single_event("RecordChanged", None, None, after);
    if let Some((labels, shadow_names)) = name_observation {
        output.labels = labels;
        output.shadow_names = shadow_names;
    }
    output.events[0].state_scope = format!(
        "{}:record:{}:{}",
        selected.contract_instance_id,
        record_id,
        output.events[0].after_state["record_key"]
            .as_str()
            .unwrap_or("-")
    );
    Ok(output)
}

fn metadata(selected: &Selected, raw: &RawLogInput, id: U256, after: &mut Value) {
    after["source_event"] = json!(selected.event.name);
    after["storage_model"] = json!("resolver_record_id");
    after["resolver"] = json!(raw.emitting_address);
    after["resolver_contract_instance_id"] = json!(selected.contract_instance_id.to_string());
    after["resolver_record_id"] = json!(id.to_string());
}

fn linked(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let e = decode_event_log::<Linked>(&raw.topics, &raw.data, "Linked log is malformed")?;
    ensure_declared(selected, &["ResolverRecordLinked"])?;
    let node = hex_string(e.node);
    let mut after = json!({"node":node, "dns_encoded_name":hex_string(&e.name)});
    metadata(selected, raw, e.recordId, &mut after);
    let mut output = single_event("ResolverRecordLinked", None, None, after);
    output.events[0].state_scope = format!("{}:link:{node}", selected.contract_instance_id);
    if let Ok(raw_labels) = decode_dns_labels(&e.name)
        && !raw_labels.is_empty()
        && namehash_raw(raw_labels.iter().map(Vec::as_slice)) == node
    {
        ensure_declared(selected, &["PreimageObserved"])?;
        let labels = surface_labels(&raw_labels);
        observe_resolver_name(selected, state, &mut output, raw_labels, labels, None);
    }
    Ok(output)
}
