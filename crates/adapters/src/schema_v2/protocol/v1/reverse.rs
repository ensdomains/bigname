use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::json;

use super::super::{Interpreted, ensure_declared};
use super::support::{events, single_event};
use crate::evm_abi::{address_hex, decode_event_log, hex_string};
use crate::schema_v2::{catalog::Selected, common::namehash, model::RawLogInput};

sol! {
    event NameForAddrChanged(address indexed addr, string name);
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
}

pub(super) fn interpret(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<Interpreted> {
    match selected.event.name.as_str() {
        "NameForAddrChanged" => {
            let event = decode_event_log::<NameForAddrChanged>(
                &raw.topics,
                &raw.data,
                "NameForAddrChanged log is malformed",
            )?;
            let kinds = vec!["ReverseChanged", "RecordChanged"];
            ensure_declared(selected, &kinds)?;
            let address = address_hex(event.addr);
            let reverse = reverse_identity(&selected.source.source_family, &address)?;
            let claim_provenance = claim_provenance(selected, raw);
            let primary_claim_source = json!({
                "address": address,
                "namespace": selected.source.namespace,
                "coin_type": reverse.coin_type,
                "reverse_name": reverse.name,
                "reverse_node": reverse.node,
                "claim_provenance": claim_provenance,
            });
            let mut output = events(
                kinds,
                json!({
                    "source_event":"NameForAddrChanged",
                    "address":address,
                    "coin_type":reverse.coin_type,
                    "namespace":selected.source.namespace,
                    "reverse_namespace":selected.source.namespace,
                    "reverse_label":reverse.label,
                    "reverse_name":reverse.name,
                    "reverse_node":reverse.node,
                    "claim_provenance":claim_provenance,
                }),
            );
            output.events[1].after_state = json!({
                "source_event":"NameForAddrChanged",
                "address":address,
                "reverse_node":reverse.node,
                "record_key":"name",
                "record_family":"name",
                "selector_key":serde_json::Value::Null,
                "raw_name":event.name,
                "primary_claim_source":primary_claim_source,
            });
            Ok(output)
        }
        "ReverseClaimed" => {
            let event = decode_event_log::<ReverseClaimed>(
                &raw.topics,
                &raw.data,
                "ReverseClaimed log is malformed",
            )?;
            let address = address_hex(event.addr);
            let reverse = reverse_identity(&selected.source.source_family, &address)?;
            if hex_string(event.node) != reverse.node {
                return Ok(Interpreted::new());
            }
            ensure_declared(selected, &["ReverseChanged"])?;
            Ok(single_event(
                "ReverseChanged",
                None,
                None,
                json!({
                    "source_event":"ReverseClaimed",
                    "address":address,
                    "coin_type":reverse.coin_type,
                    "namespace":selected.source.namespace,
                    "reverse_namespace":selected.source.namespace,
                    "reverse_label":reverse.label,
                    "reverse_name":reverse.name,
                    "reverse_node":reverse.node,
                    "claim_provenance":claim_provenance(selected, raw),
                }),
            ))
        }
        name => bail!("unsupported reverse event {name}"),
    }
}

struct ReverseIdentity {
    coin_type: &'static str,
    label: String,
    name: String,
    node: String,
}

fn reverse_identity(source_family: &str, address: &str) -> anyhow::Result<ReverseIdentity> {
    let label = address
        .strip_prefix("0x")
        .unwrap_or(address)
        .to_ascii_lowercase();
    let (coin_type, suffix) = match source_family {
        "ens_v1_reverse_l1" => ("60", vec!["addr".to_owned(), "reverse".to_owned()]),
        "basenames_base_primary" => (
            "2147492101",
            vec!["80002105".to_owned(), "reverse".to_owned()],
        ),
        family => bail!("unsupported reverse source family {family}"),
    };
    let mut labels = vec![label.clone()];
    labels.extend(suffix);
    Ok(ReverseIdentity {
        coin_type,
        label,
        name: labels.join("."),
        node: namehash(&labels),
    })
}

fn claim_provenance(selected: &Selected, raw: &RawLogInput) -> serde_json::Value {
    json!({
        "source_family":selected.source.source_family,
        "contract_role":"reverse_registrar",
        "contract_instance_id":selected.contract_instance_id.to_string(),
        "emitting_address":raw.emitting_address,
    })
}
