use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::json;

use super::super::{Interpreted, ensure_declared, raw_name_observation};
use super::support::{events, single_event};
use crate::evm_abi::{address_hex, decode_event_log, decode_event_log_data_as, hex_string};
use crate::schema_v2::{
    catalog::Selected,
    common::{decoded_label, event_string_value, namehash},
    manifest::{ManifestEvent, ManifestSource},
    model::RawLogInput,
};

mod raw_strings {
    use super::*;
    sol! {
        event RawNameForAddrChanged(address indexed addr, bytes name);
    }
}

sol! {
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
}

/// Both reverse families decode `NameForAddrChanged`; only the `addr.reverse` registrar emits
/// `ReverseClaimed`.
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L23 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
/// The `default.reverse` registrar's setters store the name through the standalone registrar,
/// which emits `NameForAddrChanged` instead.
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/DefaultReverseRegistrar.sol:L16-L20 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/DefaultReverseRegistrar.sol:L26-L57 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L28-L31 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/reverseRegistrar/IStandaloneReverseRegistrar.sol:L10 @ ens_v1@91c966f)
pub(in crate::schema_v2) fn supports(source_family: &str, signature: &str) -> bool {
    match signature {
        "NameForAddrChanged(address,string)" => true,
        "ReverseClaimed(address,bytes32)" => source_family == "ens_v1_reverse_l1",
        _ => false,
    }
}

/// The reverse identity follows the emitter role, so each `ens_v1_reverse_l1` event must come from
/// its own registrar: ReverseClaimed claims `addr.reverse`, NameForAddrChanged writes the
/// `default.reverse` name.
pub(in crate::schema_v2) fn validate_roles(
    source: &ManifestSource,
    event: &ManifestEvent,
) -> anyhow::Result<()> {
    if source.source_family != "ens_v1_reverse_l1" {
        return Ok(());
    }
    let role = match event.name.as_str() {
        "NameForAddrChanged" => "default_reverse_registrar",
        _ => "reverse_registrar",
    };
    if event.emitter_roles != [role] {
        bail!(
            "manifest {} source family ens_v1_reverse_l1 declares {} for roles other than {role}",
            source.manifest_id,
            event.name
        );
    }
    Ok(())
}

pub(super) fn interpret(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<Interpreted> {
    match selected.event.name.as_str() {
        "NameForAddrChanged" => {
            let event = decode_event_log_data_as::<raw_strings::RawNameForAddrChanged>(
                &raw.topics,
                &raw.data,
                &selected.event.topic0,
                "NameForAddrChanged log is malformed",
            )?;
            let raw_name = event.name.to_vec();
            let (labels, shadow_names) = raw_name_observation(&raw_name, "NameForAddrChanged_name");
            let kinds = vec!["ReverseChanged", "RecordChanged"];
            ensure_declared(selected, &kinds)?;
            let address = address_hex(event.addr);
            let reverse = reverse_identity(selected, &address)?;
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
                "raw_name":decoded_label(&raw_name),
                "primary_claim_source":primary_claim_source,
            });
            if output.events[1].after_state["raw_name"].is_null()
                && let Some(after_state) = output.events[1].after_state.as_object_mut()
            {
                after_state.insert("raw_name_bytes".to_owned(), event_string_value(&raw_name));
            }
            output.labels = labels;
            output.shadow_names = shadow_names;
            Ok(output)
        }
        "ReverseClaimed" => {
            let event = decode_event_log::<ReverseClaimed>(
                &raw.topics,
                &raw.data,
                "ReverseClaimed log is malformed",
            )?;
            let address = address_hex(event.addr);
            let reverse = reverse_identity(selected, &address)?;
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

fn reverse_identity(selected: &Selected, address: &str) -> anyhow::Result<ReverseIdentity> {
    let label = address
        .strip_prefix("0x")
        .unwrap_or(address)
        .to_ascii_lowercase();
    // ENSIP-19 reverse names: `addr` for coin type 60, `default` for coin type 2^31.
    // (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L14 @ ens_v1@91c966f)
    let (coin_type, suffix) = match selected.source.source_family.as_str() {
        "ens_v1_reverse_l1" if contract_role(selected) == "default_reverse_registrar" => (
            "2147483648",
            vec!["default".to_owned(), "reverse".to_owned()],
        ),
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
        "contract_role":contract_role(selected),
        "contract_instance_id":selected.contract_instance_id.to_string(),
        "emitting_address":raw.emitting_address,
    })
}

fn contract_role(selected: &Selected) -> &str {
    selected
        .emitter_role
        .as_deref()
        .unwrap_or("reverse_registrar")
}
