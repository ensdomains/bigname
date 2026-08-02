use alloy_primitives::Address;
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use super::super::{Interpreted, NameDraft, ensure_declared};
use crate::{
    evm_abi::{address_hex, decode_event_log, hex_string, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::{namehash, require_label},
        model::RawLogInput,
        state::State,
    },
};

mod legacy {
    use super::*;
    sol! {
        event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 referrer, uint256 base, uint256 premium);
        event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 referrer, uint256 base);
    }
}

mod current {
    use super::*;
    sol! {
        event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
        event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
    }
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &State,
) -> anyhow::Result<Interpreted> {
    let (kind, token_id, label, after) = match selected.event.signature.as_str() {
        "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)" => {
            if raw.topics.len() == 2 {
                let event = decode_event_log::<legacy::NameRegistered>(
                    &raw.topics,
                    &raw.data,
                    "legacy ENSv2 NameRegistered log is malformed",
                )?;
                (
                    "RegistrarNameRegistered",
                    event.tokenId,
                    event.label,
                    json!({"source_event":"NameRegistered","owner":address_hex(event.owner),"subregistry":nullable_address(event.subregistry),"resolver":nullable_address(event.resolver),"duration":event.duration,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"base":event.base.to_string(),"premium":event.premium.to_string()}),
                )
            } else {
                let event = decode_event_log::<current::NameRegistered>(
                    &raw.topics,
                    &raw.data,
                    "ENSv2 NameRegistered log is malformed",
                )?;
                (
                    "RegistrarNameRegistered",
                    event.tokenId,
                    event.label,
                    json!({"source_event":"NameRegistered","owner":address_hex(event.owner),"subregistry":nullable_address(event.subregistry),"resolver":nullable_address(event.resolver),"duration":event.duration,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"base":event.base.to_string(),"premium":event.premium.to_string()}),
                )
            }
        }
        "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)" => {
            if raw.topics.len() == 2 {
                let event = decode_event_log::<legacy::NameRenewed>(
                    &raw.topics,
                    &raw.data,
                    "legacy ENSv2 NameRenewed log is malformed",
                )?;
                (
                    "RegistrationRenewed",
                    event.tokenId,
                    event.label,
                    json!({"source_event":"NameRenewed","duration":event.duration,"expiry":event.newExpiry,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"base":event.base.to_string()}),
                )
            } else {
                let event = decode_event_log::<current::NameRenewed>(
                    &raw.topics,
                    &raw.data,
                    "ENSv2 NameRenewed log is malformed",
                )?;
                (
                    "RegistrationRenewed",
                    event.tokenId,
                    event.label,
                    json!({"source_event":"NameRenewed","duration":event.duration,"expiry":event.newExpiry,"payment_token":address_hex(event.paymentToken),"referrer":hex_string(event.referrer),"amount":event.amount.to_string(),"base":event.amount.to_string()}),
                )
            }
        }
        signature => bail!("unsupported ENSv2 registrar event {signature}"),
    };
    ensure_declared(selected, &[kind])?;
    require_label(&label)?;
    let labels = vec![label, "eth".to_owned()];
    let raw_namehash = namehash(&labels);
    let logical_name_id = format!("{}:{raw_namehash}", selected.source.namespace);
    let token_word = u256_word_hex(token_id);
    let linked = state.v2_token_for_logical_name(&token_word, &logical_name_id)?;
    if let Some(linked_name) = linked.as_ref().and_then(|state| state.name.as_ref())
        && linked_name.logical_name_id != logical_name_id
    {
        bail!(
            "ENSv2 registrar token {token_word} names {logical_name_id}, but its registry resource is bound to {}",
            linked_name.logical_name_id
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
                "resource_pending":resource_id.is_none(),
            }),
        ),
    );
    output.names.push(NameDraft {
        labels,
        namehash: raw_namehash,
        resource_id,
        token_lineage_id,
        surface_binding_id: None,
        bind: false,
        binding_kind: "declared_registry_path".to_owned(),
        source_kind: format!("{}_label", selected.event.name),
        preimage_metadata: None,
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
