use alloy_primitives::keccak256;
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Map, Value, json};

use super::super::{Interpreted, ensure_declared};
use super::support::{name_labels, single_event};
use crate::evm_abi::{address_hex, decode_event_log, hex_string};
use crate::schema_v2::{catalog::Selected, model::RawLogInput, state::State};

mod text_without_value {
    use super::*;
    sol! { event TextChanged(bytes32 indexed node, string indexed indexedKey, string key); }
}

sol! {
    event AddrChanged(bytes32 indexed node, address a);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event NameChanged(bytes32 indexed node, string name);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event ContentChanged(bytes32 indexed node, bytes32 hash);
    event ContenthashChanged(bytes32 indexed node, bytes hash);
    event ABIChanged(bytes32 indexed node, uint256 indexed contentType);
    event DNSRecordChanged(bytes32 indexed node, bytes name, uint16 resource, bytes record);
    event DNSRecordDeleted(bytes32 indexed node, bytes name, uint16 resource);
    event DNSZonehashChanged(bytes32 indexed node, bytes lastzonehash, bytes zonehash);
    event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData);
    event InterfaceChanged(bytes32 indexed node, bytes4 indexed interfaceID, address implementer);
    event VersionChanged(bytes32 indexed node, uint64 newVersion);
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let (kind, after, labels) = match selected.event.name.as_str() {
        "AddrChanged" => {
            let event = decode_event_log::<AddrChanged>(
                &raw.topics,
                &raw.data,
                "AddrChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "AddrChanged",
                    hex_string(event.node),
                    "addr:60".to_owned(),
                    "addr",
                    Some("60".to_owned()),
                    Some(json!(address_hex(event.a))),
                    None,
                ),
                vec![],
            )
        }
        "AddressChanged" => {
            let event = decode_event_log::<AddressChanged>(
                &raw.topics,
                &raw.data,
                "AddressChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "AddressChanged",
                    hex_string(event.node),
                    format!("addr:{}", event.coinType),
                    "addr",
                    Some(event.coinType.to_string()),
                    Some(
                        if event.coinType == alloy_primitives::U256::from(60)
                            && event.newAddress.len() == 20
                        {
                            json!(hex_string(event.newAddress))
                        } else {
                            json!({"encoding":"hex","bytes":hex_string(event.newAddress)})
                        },
                    ),
                    None,
                ),
                vec![],
            )
        }
        "NameChanged" => {
            let event = decode_event_log::<NameChanged>(
                &raw.topics,
                &raw.data,
                "NameChanged log is malformed",
            )?;
            let labels = name_labels(&event.name, "NameChanged_name").unwrap_or_default();
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "NameChanged",
                    hex_string(event.node),
                    "name".to_owned(),
                    "name",
                    None,
                    None,
                    Some(event.name),
                ),
                labels,
            )
        }
        "TextChanged"
            if selected.event.signature == "TextChanged(bytes32,string,string,string)" =>
        {
            let event = decode_event_log::<TextChanged>(
                &raw.topics,
                &raw.data,
                "TextChanged log is malformed",
            )?;
            if event.key.trim().is_empty()
                || hex_string(event.indexedKey) != format!("{:#x}", keccak256(event.key.as_bytes()))
            {
                return Ok(Interpreted::new());
            }
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "TextChanged",
                    hex_string(event.node),
                    format!("text:{}", event.key),
                    "text",
                    Some(event.key),
                    Some(json!(event.value)),
                    None,
                ),
                vec![],
            )
        }
        "TextChanged" => {
            let event = decode_event_log::<text_without_value::TextChanged>(
                &raw.topics,
                &raw.data,
                "TextChanged log is malformed",
            )?;
            if event.key.trim().is_empty()
                || hex_string(event.indexedKey) != format!("{:#x}", keccak256(event.key.as_bytes()))
            {
                return Ok(Interpreted::new());
            }
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "TextChanged",
                    hex_string(event.node),
                    format!("text:{}", event.key),
                    "text",
                    Some(event.key),
                    None,
                    None,
                ),
                vec![],
            )
        }
        "VersionChanged" => {
            let event = decode_event_log::<VersionChanged>(
                &raw.topics,
                &raw.data,
                "VersionChanged log is malformed",
            )?;
            (
                "RecordVersionChanged",
                json!({
                    "source_event":"VersionChanged",
                    "resolver":raw.emitting_address,
                    "resolver_contract_instance_id":selected.contract_instance_id.to_string(),
                    "node":hex_string(event.node),
                    "record_version":event.newVersion,
                }),
                vec![],
            )
        }
        "ContentChanged" => {
            let event = decode_event_log::<ContentChanged>(
                &raw.topics,
                &raw.data,
                "ContentChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "ContentChanged",
                    hex_string(event.node),
                    "content".to_owned(),
                    "content",
                    None,
                    Some(json!(hex_string(event.hash))),
                    None,
                ),
                vec![],
            )
        }
        "ContenthashChanged" => {
            let event = decode_event_log::<ContenthashChanged>(
                &raw.topics,
                &raw.data,
                "ContenthashChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "ContenthashChanged",
                    hex_string(event.node),
                    "contenthash".to_owned(),
                    "contenthash",
                    None,
                    Some(json!({"encoding":"hex","bytes":hex_string(event.hash)})),
                    None,
                ),
                vec![],
            )
        }
        "ABIChanged" => {
            let event = decode_event_log::<ABIChanged>(
                &raw.topics,
                &raw.data,
                "ABIChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "ABIChanged",
                    hex_string(event.node),
                    format!("abi:{}", event.contentType),
                    "abi",
                    Some(event.contentType.to_string()),
                    Some(json!(event.contentType.to_string())),
                    None,
                ),
                vec![],
            )
        }
        "DNSRecordChanged" => {
            let event = decode_event_log::<DNSRecordChanged>(
                &raw.topics,
                &raw.data,
                "DNSRecordChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "DNSRecordChanged",
                    hex_string(event.node),
                    format!("dns:{}:{}", event.resource, hex_string(&event.name)),
                    "dns",
                    Some(format!("{}:{}", event.resource, hex_string(&event.name))),
                    Some(json!({"encoding":"hex","bytes":hex_string(event.record)})),
                    None,
                ),
                vec![],
            )
        }
        "DNSRecordDeleted" => {
            let event = decode_event_log::<DNSRecordDeleted>(
                &raw.topics,
                &raw.data,
                "DNSRecordDeleted log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "DNSRecordDeleted",
                    hex_string(event.node),
                    format!("dns:{}:{}", event.resource, hex_string(&event.name)),
                    "dns",
                    Some(format!("{}:{}", event.resource, hex_string(&event.name))),
                    Some(json!({"deleted":true})),
                    None,
                ),
                vec![],
            )
        }
        "DNSZonehashChanged" => {
            let event = decode_event_log::<DNSZonehashChanged>(
                &raw.topics,
                &raw.data,
                "DNSZonehashChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "DNSZonehashChanged",
                    hex_string(event.node),
                    "dns:zonehash".to_owned(),
                    "dns",
                    Some("zonehash".to_owned()),
                    Some(json!({
                        "previous":{"encoding":"hex","bytes":hex_string(event.lastzonehash)},
                        "current":{"encoding":"hex","bytes":hex_string(event.zonehash)},
                    })),
                    None,
                ),
                vec![],
            )
        }
        "DataChanged" => {
            let event = decode_event_log::<DataChanged>(
                &raw.topics,
                &raw.data,
                "DataChanged log is malformed",
            )?;
            if hex_string(event.indexedKey) != format!("{:#x}", keccak256(event.key.as_bytes())) {
                return Ok(Interpreted::new());
            }
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "DataChanged",
                    hex_string(event.node),
                    format!("data:{}", event.key),
                    "data",
                    Some(event.key),
                    Some(json!({"indexed_data_hash":hex_string(event.indexedData)})),
                    None,
                ),
                vec![],
            )
        }
        "InterfaceChanged" => {
            let event = decode_event_log::<InterfaceChanged>(
                &raw.topics,
                &raw.data,
                "InterfaceChanged log is malformed",
            )?;
            (
                "RecordChanged",
                record_after(
                    selected,
                    raw,
                    "InterfaceChanged",
                    hex_string(event.node),
                    format!("interface:{}", hex_string(event.interfaceID)),
                    "interface",
                    Some(hex_string(event.interfaceID)),
                    Some(json!(address_hex(event.implementer))),
                    None,
                ),
                vec![],
            )
        }
        name => bail!("unsupported resolver event {name}"),
    };
    ensure_declared(selected, &[kind])?;
    let affected_node = after
        .get("node")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let mut output = single_event(kind, None, None, after);
    if let Some(linked) = affected_node
        .as_deref()
        .and_then(|node| state.v1_name(&selected.source.namespace, node))
    {
        output.events[0].logical_name_id = Some(linked.logical_name_id);
        output.events[0].resource_id = Some(linked.resource_id);
    }
    output.labels = labels;
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn record_after(
    selected: &Selected,
    raw: &RawLogInput,
    source_event: &str,
    node: String,
    record_key: String,
    record_family: &str,
    selector_key: Option<String>,
    value: Option<Value>,
    raw_name: Option<String>,
) -> Value {
    let value_retained = value.is_some();
    let mut after = Map::from_iter([
        ("source_event".to_owned(), json!(source_event)),
        ("resolver".to_owned(), json!(raw.emitting_address)),
        (
            "resolver_contract_instance_id".to_owned(),
            json!(selected.contract_instance_id.to_string()),
        ),
        ("node".to_owned(), json!(node)),
        ("record_key".to_owned(), json!(record_key)),
        ("record_family".to_owned(), json!(record_family)),
        ("selector_key".to_owned(), json!(selector_key)),
        ("value_retained".to_owned(), json!(value_retained)),
    ]);
    if let Some(value) = value {
        after.insert("value".to_owned(), value);
    }
    if let Some(raw_name) = raw_name {
        after.insert("raw_name".to_owned(), json!(raw_name));
    }
    Value::Object(after)
}
