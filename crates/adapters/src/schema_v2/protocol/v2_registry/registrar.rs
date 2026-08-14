use alloy_primitives::{Address, hex};
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use super::super::{Interpreted, NameDraft, ResourceDraft, ShadowNameDraft, ensure_declared};
use crate::{
    evm_abi::{address_hex, decode_event_log_data_as, hex_string, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::{admitted_label, decoded_label},
        model::RawLogInput,
        state::State,
    },
};

mod legacy {
    use super::*;
    sol! {
        event RawNameRegistered(uint256 indexed tokenId, bytes label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 referrer, uint256 base, uint256 premium);
        event RawNameRenewed(uint256 indexed tokenId, bytes label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 referrer, uint256 base);
    }
}

mod current {
    use super::*;
    sol! {
        event RawNameRegistered(uint256 indexed tokenId, bytes label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
        event RawNameRenewed(uint256 indexed tokenId, bytes label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
    }
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &State,
) -> anyhow::Result<Interpreted> {
    let (kind, token_id, raw_label, after) = match selected.event.signature.as_str() {
        "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)" => {
            if raw.topics.len() == 2 {
                let event = decode_event_log_data_as::<legacy::RawNameRegistered>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "legacy ENSv2 NameRegistered log is malformed",
                )?;
                (
                    "RegistrarNameRegistered",
                    event.tokenId,
                    event.label.to_vec(),
                    json!({"source_event":"NameRegistered","owner":address_hex(event.owner),"subregistry":nullable_address(event.subregistry),"resolver":nullable_address(event.resolver),"duration":event.duration,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"base":event.base.to_string(),"premium":event.premium.to_string()}),
                )
            } else {
                let event = decode_event_log_data_as::<current::RawNameRegistered>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "ENSv2 NameRegistered log is malformed",
                )?;
                (
                    "RegistrarNameRegistered",
                    event.tokenId,
                    event.label.to_vec(),
                    json!({"source_event":"NameRegistered","owner":address_hex(event.owner),"subregistry":nullable_address(event.subregistry),"resolver":nullable_address(event.resolver),"duration":event.duration,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"base":event.base.to_string(),"premium":event.premium.to_string()}),
                )
            }
        }
        "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)" => {
            if raw.topics.len() == 2 {
                let event = decode_event_log_data_as::<legacy::RawNameRenewed>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "legacy ENSv2 NameRenewed log is malformed",
                )?;
                (
                    "RegistrationRenewed",
                    event.tokenId,
                    event.label.to_vec(),
                    json!({"source_event":"NameRenewed","duration":event.duration,"expiry":event.newExpiry,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"base":event.base.to_string()}),
                )
            } else {
                let event = decode_event_log_data_as::<current::RawNameRenewed>(
                    &raw.topics,
                    &raw.data,
                    &selected.event.topic0,
                    "ENSv2 NameRenewed log is malformed",
                )?;
                (
                    "RegistrationRenewed",
                    event.tokenId,
                    event.label.to_vec(),
                    json!({"source_event":"NameRenewed","duration":event.duration,"expiry":event.newExpiry,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"amount":event.amount.to_string(),"base":event.amount.to_string()}),
                )
            }
        }
        signature => bail!("unsupported ENSv2 registrar event {signature}"),
    };
    ensure_declared(selected, &[kind])?;
    let decoded_label = decoded_label(&raw_label);
    let label = admitted_label(&raw_label);
    let labels = label.map(|label| vec![label, "eth".to_owned()]);
    let raw_namehash = crate::schema_v2::common::namehash_raw(
        [raw_label.as_slice(), b"eth".as_slice()].into_iter(),
    );
    let logical_name_id = format!("{}:{raw_namehash}", selected.source.namespace);
    let token_word = u256_word_hex(token_id);
    let linked = state.v2_token_for_logical_name(&token_word, &logical_name_id)?;
    if let Some(linked_name) = linked.as_ref().and_then(|state| {
        state
            .name
            .as_ref()
            .map(|name| &name.logical_name_id)
            .or_else(|| state.shadow_name.as_ref().map(|name| &name.logical_name_id))
    }) && linked_name != &logical_name_id
    {
        bail!(
            "ENSv2 registrar token {token_word} names {logical_name_id}, but its registry resource is bound to {}",
            linked_name
        );
    }
    let resource_id = linked.as_ref().and_then(|state| state.resource_id);
    let token_lineage_id = linked.as_ref().and_then(|state| state.token_lineage_id);
    let mut output = single_event(
        kind,
        Some(logical_name_id),
        resource_id,
        merge(
            after,
            json!({
                "token_id":token_word,
                "namehash":raw_namehash,
                "raw_label_hex":hex::encode(&raw_label),
                "decoded_label":decoded_label,
                "surface_known":labels.is_some(),
                "resource_pending":resource_id.is_none(),
            }),
        ),
    );
    if let Some(labels) = labels {
        output.names.push(NameDraft {
            labels,
            namehash: raw_namehash,
            resource_id,
            token_lineage_id,
            surface_binding_id: None,
            bind: false,
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: "ens_v2".to_owned(),
            source_kind: format!("{}_label", selected.event.name),
            preimage_metadata: None,
        });
    } else {
        output.shadow_names.push(ShadowNameDraft {
            raw_labels: vec![raw_label, b"eth".to_vec()],
            namehash: raw_namehash,
            source_kind: format!("{}_label", selected.event.name),
        });
        if let Some(resource_id) = resource_id {
            output.resources.push(ResourceDraft {
                resource_id,
                token_lineage_id,
            });
        }
    }
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
    resource_id: Option<uuid::Uuid>,
    after_state: Value,
) -> Interpreted {
    let mut output = Interpreted::new();
    output.events.push(super::super::EventDraft {
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
