use alloy_primitives::{B256, keccak256};
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use super::super::{
    EventDraft, Interpreted, NameDraft, ResourceDraft, ensure_declared,
    permissions::{v1_grant_states, v1_revoke_states},
};
use super::registry::append_authority_transition;
use super::support::{events_linked, single_event};
use crate::evm_abi::{address_hex, decode_event_log, hex_string, u256_word_hex};
use crate::schema_v2::{
    catalog::Selected,
    common::{namehash, require_label, stable_uuid},
    model::RawLogInput,
    state::{State, V1NameState},
};

mod simple {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires);
        event NameRenewed(string name, bytes32 indexed label, uint256 expires);
    }
}

mod cost {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
    }
}

mod premium {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires);
    }
}

mod premium_referrer {
    use super::*;
    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires, bytes32 referrer);
    }
}

mod renew_referrer {
    use super::*;
    sol! {
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires, bytes32 referrer);
    }
}

mod transfer {
    use super::*;
    sol! { event Transfer(address indexed from, address indexed to, uint256 indexed tokenId); }
}

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    match selected.event.name.as_str() {
        "NameRegistered" => name_event(selected, raw, state, true),
        "NameRenewed" => name_event(selected, raw, state, false),
        "Transfer" => transfer(selected, raw, state),
        "Upgraded" => super::upgrade::interpret(selected, raw),
        name => bail!("unsupported registrar event {name}"),
    }
}

fn transfer(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    ensure_declared(selected, &["TokenControlTransferred"])?;
    let event = decode_event_log::<transfer::Transfer>(
        &raw.topics,
        &raw.data,
        "registrar Transfer log is malformed",
    )?;
    let from = address_hex(event.from);
    let to = address_hex(event.to);
    if from == ZERO_ADDRESS || to == ZERO_ADDRESS {
        return Ok(Interpreted::new());
    }
    let labelhash = B256::from(event.tokenId.to_be_bytes::<32>());
    let raw_namehash = registrar_namehash(selected, labelhash);
    let previous_active = state.v1_name(&selected.source.namespace, &raw_namehash);
    let Some((before, linked)) =
        state.transfer_v1_registrar_owner(&selected.source.namespace, &raw_namehash, to.clone())
    else {
        return Ok(Interpreted::new());
    };
    let mut active_after = state.converge_v1_registrar_transfer(
        &selected.source.namespace,
        &raw_namehash,
        raw.block_timestamp.unix_timestamp(),
    );
    if active_after.is_none()
        && state
            .v1_registry_owner(&selected.source.namespace, &raw_namehash)
            .is_some_and(|owner| !owner.eq_ignore_ascii_case(ZERO_ADDRESS))
    {
        let registry_owner = state
            .v1_registry_owner(&selected.source.namespace, &raw_namehash)
            .expect("checked registry owner");
        let authority = V1NameState {
            logical_name_id: linked.logical_name_id.clone(),
            surface_known: linked.surface_known,
            resource_id: stable_uuid(&format!(
                "resource:registry-only:{}:{raw_namehash}",
                raw.chain_id
            )),
            token_lineage_id: None,
            authority_source_family: if selected.source.source_family == "basenames_base_registrar"
            {
                "basenames_base_registry"
            } else {
                "ens_v1_registry_l1"
            }
            .to_owned(),
            source_manifest_id: None,
            labelhash: Some(format!("{labelhash:#x}")),
            expiry: None,
            owner: Some(registry_owner),
            authority_key: Some(format!("registry-only:{}:{raw_namehash}", raw.chain_id)),
        };
        state.remember_v1_registry_authority(
            &selected.source.namespace,
            &raw_namehash,
            authority.clone(),
        );
        state.activate_v1_authority(
            &selected.source.namespace,
            &raw_namehash,
            Some(authority.clone()),
        );
        active_after = Some(authority);
    }
    let mut output = single_event(
        "TokenControlTransferred",
        Some(linked.logical_name_id.clone()),
        Some(linked.resource_id),
        json!({
            "source_event": "Transfer",
            "to": to,
            "token_id": u256_word_hex(event.tokenId),
            "namehash": raw_namehash,
            "token_lineage_id": linked.token_lineage_id.map(|id| id.to_string()),
        }),
    );
    output.events[0].explicit_before = Some(json!({"from": from}));
    output.resources.push(ResourceDraft {
        resource_id: linked.resource_id,
        token_lineage_id: linked.token_lineage_id,
    });
    append_transfer_permissions(
        &mut output,
        &before,
        &linked,
        state.v1_resolver(&selected.source.namespace, &raw_namehash),
        &raw.chain_id,
    );
    append_authority_transition(
        &mut output,
        previous_active.as_ref(),
        active_after.as_ref(),
        raw,
        &json!({"source_event":"Transfer"}),
        state.v1_resolver(&selected.source.namespace, &raw_namehash),
    );
    Ok(output)
}

fn append_transfer_permissions(
    output: &mut Interpreted,
    before: &crate::schema_v2::state::V1NameState,
    after: &crate::schema_v2::state::V1NameState,
    resolver: Option<String>,
    chain_id: &str,
) {
    let (Some(from), Some(to), Some(authority_key)) = (
        before.owner.as_deref(),
        after.owner.as_deref(),
        after.authority_key.as_deref(),
    ) else {
        return;
    };
    if from.eq_ignore_ascii_case(to) {
        return;
    }
    let mut scopes = vec![(json!({"kind":"resource"}), "resource_control")];
    if let Some(resolver) = resolver {
        scopes.push((
            json!({"kind":"resolver","chain_id":chain_id,"resolver_address":resolver}),
            "resolver_control",
        ));
    }
    for (index, (scope, power)) in scopes.into_iter().enumerate() {
        for (grant, subject, action) in [(false, from, "revoke"), (true, to, "grant")] {
            let (before_state, after_state) = if grant {
                v1_grant_states(
                    subject,
                    scope.clone(),
                    power,
                    "registrar",
                    authority_key,
                    "TokenControlTransferred",
                )
            } else {
                v1_revoke_states(
                    subject,
                    scope.clone(),
                    power,
                    "registrar",
                    authority_key,
                    "TokenControlTransferred",
                )
            };
            output.events.push(EventDraft {
                event_kind: "PermissionChanged".to_owned(),
                logical_name_id: Some(after.logical_name_id.clone()),
                resource_id: Some(after.resource_id),
                identity_suffix: format!("PermissionChanged:transfer:{index}:{action}:{subject}"),
                explicit_before: Some(before_state),
                after_state,
                state_scope: String::new(),
            });
        }
    }
}

fn name_event(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    registration: bool,
) -> anyhow::Result<Interpreted> {
    let (label, explicit_labelhash, mut after) = decode_name(selected, raw)?;
    require_label(&label)?;
    if keccak256(label.as_bytes()) != explicit_labelhash {
        bail!(
            "{} label does not hash to its indexed label",
            selected.event.name
        );
    }
    let suffix = if selected.source.source_family == "basenames_base_registrar" {
        vec!["base".to_owned(), "eth".to_owned()]
    } else {
        vec!["eth".to_owned()]
    };
    let labels = std::iter::once(label.clone())
        .chain(suffix)
        .collect::<Vec<_>>();
    let raw_namehash = registrar_namehash(selected, explicit_labelhash);
    let logical_name_id = format!("{}:{raw_namehash}", selected.source.namespace);
    let previous_active = state.v1_name(&selected.source.namespace, &raw_namehash);
    let prior_registrar = state.v1_registrar(&selected.source.namespace, &raw_namehash);
    let existing = (!registration).then(|| prior_registrar.clone()).flatten();
    let synthetic_grant = !registration && existing.is_none();
    let (token_lineage_id, resource_id, authority_key) = existing
        .as_ref()
        .map(|state| {
            (
                state
                    .token_lineage_id
                    .expect("registrar authority has token lineage"),
                state.resource_id,
                None,
            )
        })
        .unwrap_or_else(|| {
            new_registrar_identity(selected, raw, &format!("{explicit_labelhash:#x}"))
        });
    let expiry = after.get("expiry").and_then(Value::as_i64);
    let owner = after
        .get("registrant")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| existing.as_ref().and_then(|state| state.owner.clone()))
        .or_else(|| synthetic_grant.then(|| ZERO_ADDRESS.to_owned()));
    let retained_authority_key = authority_key.clone().or_else(|| {
        existing
            .as_ref()
            .and_then(|state| state.authority_key.clone())
    });
    let make_current = registration
        || state
            .v1_name(&selected.source.namespace, &raw_namehash)
            .is_none_or(|current| current.authority_source_family == selected.source.source_family);
    state.observe_v1_registrar(
        &selected.source.namespace,
        &raw_namehash,
        logical_name_id.clone(),
        resource_id,
        token_lineage_id,
        selected.source.source_family.clone(),
        Some(selected.source.manifest_id),
        Some(format!("{explicit_labelhash:#x}")),
        expiry,
        owner.clone(),
        retained_authority_key.clone(),
        make_current,
    );
    let after_object = after.as_object_mut().expect("registrar state is an object");
    after_object.insert("namehash".to_owned(), Value::String(raw_namehash.clone()));
    after_object.insert(
        "labelhash".to_owned(),
        Value::String(format!("{explicit_labelhash:#x}")),
    );
    after_object.insert(
        "token_lineage_id".to_owned(),
        Value::String(token_lineage_id.to_string()),
    );
    if let Some(owner) = owner {
        after_object.insert("registrant".to_owned(), Value::String(owner));
    }
    if let Some(authority_key) = retained_authority_key.as_ref() {
        after_object.insert(
            "authority_kind".to_owned(),
            Value::String("registrar".to_owned()),
        );
        after_object.insert(
            "authority_key".to_owned(),
            Value::String(authority_key.clone()),
        );
    }
    let event_kinds = if registration {
        vec!["RegistrationGranted", "ExpiryChanged", "PermissionChanged"]
    } else if synthetic_grant {
        vec![
            "RegistrationGranted",
            "RegistrationRenewed",
            "ExpiryChanged",
        ]
    } else {
        vec!["RegistrationRenewed", "ExpiryChanged"]
    };
    ensure_declared(selected, &[event_kinds[0]])?;
    let mut output = events_linked(
        event_kinds,
        logical_name_id.clone(),
        resource_id,
        after.clone(),
    );
    if registration || synthetic_grant {
        if let Some(grant) = output
            .events
            .iter_mut()
            .find(|event| event.event_kind == "RegistrationGranted")
        {
            grant.explicit_before = Some(json!({
                "authority_kind":previous_active.as_ref().map(super::registry::authority_kind),
                "registrant":prior_registrar.as_ref().and_then(|state| state.owner.clone()),
            }));
        }
        if let Some(expiry_event) = output
            .events
            .iter_mut()
            .find(|event| event.event_kind == "ExpiryChanged")
        {
            expiry_event.explicit_before = Some(json!({
                "expiry":prior_registrar.as_ref().and_then(|state| state.expiry),
            }));
        }
    }
    if !registration {
        let before_expiry = existing.as_ref().and_then(|state| state.expiry);
        for event in output.events.iter_mut().filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "RegistrationRenewed" | "ExpiryChanged"
            )
        }) {
            event.explicit_before = Some(json!({"expiry":before_expiry}));
        }
    }
    if registration
        && let (Some(subject), Some(authority_key), Some(permission)) = (
            after.get("registrant").and_then(Value::as_str),
            after.get("authority_key").and_then(Value::as_str),
            output
                .events
                .iter_mut()
                .find(|event| event.event_kind == "PermissionChanged"),
        )
    {
        let (before, after) = v1_grant_states(
            subject,
            json!({"kind":"resource"}),
            "resource_control",
            "registrar",
            authority_key,
            "RegistrationGranted",
        );
        permission.explicit_before = Some(before);
        permission.after_state = after;
    }
    if registration
        && let (Some(subject), Some(authority_key), Some(resolver)) = (
            after.get("registrant").and_then(Value::as_str),
            after.get("authority_key").and_then(Value::as_str),
            state.v1_resolver(&selected.source.namespace, &raw_namehash),
        )
    {
        let (before, after_state) = v1_grant_states(
            subject,
            json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
            "resolver_control",
            "registrar",
            authority_key,
            "RegistrationGranted",
        );
        output.events.push(EventDraft {
            event_kind: "PermissionChanged".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(resource_id),
            identity_suffix: format!("PermissionChanged:registration-resolver:{subject}"),
            explicit_before: Some(before),
            after_state,
            state_scope: String::new(),
        });
    }
    let active_after = state.v1_name(&selected.source.namespace, &raw_namehash);
    if registration || synthetic_grant {
        append_authority_transition(
            &mut output,
            previous_active.as_ref(),
            active_after.as_ref(),
            raw,
            &after,
            state.v1_resolver(&selected.source.namespace, &raw_namehash),
        );
    }
    output.names.push(NameDraft {
        labels,
        namehash: raw_namehash,
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        surface_binding_id: authority_key.as_ref().map(|authority_key| {
            stable_uuid(&format!(
                "binding:{authority_key}:{}",
                raw.block_timestamp.unix_timestamp()
            ))
        }),
        bind: false,
        binding_kind: "declared_registry_path".to_owned(),
        source_kind: format!("{}_name", selected.event.name),
        preimage_metadata: None,
    });
    Ok(output)
}

fn registrar_namehash(selected: &Selected, labelhash: B256) -> String {
    let suffix = if selected.source.source_family == "basenames_base_registrar" {
        vec!["base".to_owned(), "eth".to_owned()]
    } else {
        vec!["eth".to_owned()]
    };
    let parent = namehash(&suffix)
        .parse::<B256>()
        .expect("namehash helper returns a bytes32 value");
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(labelhash.as_slice());
    format!("{:#x}", keccak256(input))
}

fn new_registrar_identity(
    selected: &Selected,
    raw: &RawLogInput,
    labelhash: &str,
) -> (uuid::Uuid, uuid::Uuid, Option<String>) {
    let authority_key = format!(
        "registrar:{}:{}:{}:{}:{}",
        raw.chain_id, selected.source.manifest_id, labelhash, raw.block_hash, raw.log_index,
    );
    let token_lineage_id = stable_uuid(&format!("token-lineage:{authority_key}"));
    let resource_id = stable_uuid(&format!("resource:{authority_key}"));
    (token_lineage_id, resource_id, Some(authority_key))
}

fn decode_name(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<(String, B256, Value)> {
    match selected.event.signature.as_str() {
        "NameRegistered(string,bytes32,address,uint256)" => {
            let event = decode_event_log::<simple::NameRegistered>(
                &raw.topics,
                &raw.data,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name,
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?}),
            ))
        }
        "NameRegistered(string,bytes32,address,uint256,uint256)" => {
            let event = decode_event_log::<cost::NameRegistered>(
                &raw.topics,
                &raw.data,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name,
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"cost":event.cost.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?}),
            ))
        }
        "NameRegistered(string,bytes32,address,uint256,uint256,uint256)" => {
            let event = decode_event_log::<premium::NameRegistered>(
                &raw.topics,
                &raw.data,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name,
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"base_cost":event.baseCost.to_string(),"premium":event.premium.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?}),
            ))
        }
        "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)" => {
            let event = decode_event_log::<premium_referrer::NameRegistered>(
                &raw.topics,
                &raw.data,
                "NameRegistered log is malformed",
            )?;
            Ok((
                event.name,
                event.label,
                json!({"source_event":"NameRegistered","registrant":address_hex(event.owner),"base_cost":event.baseCost.to_string(),"premium":event.premium.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRegistered expiry")?,"referrer":hex_string(event.referrer)}),
            ))
        }
        "NameRenewed(string,bytes32,uint256)" => {
            let event = decode_event_log::<simple::NameRenewed>(
                &raw.topics,
                &raw.data,
                "NameRenewed log is malformed",
            )?;
            Ok((
                event.name,
                event.label,
                json!({"source_event":"NameRenewed","expiry":crate::evm_abi::u256_i64(event.expires, "NameRenewed expiry")?}),
            ))
        }
        "NameRenewed(string,bytes32,uint256,uint256)" => {
            let event = decode_event_log::<cost::NameRenewed>(
                &raw.topics,
                &raw.data,
                "NameRenewed log is malformed",
            )?;
            Ok((
                event.name,
                event.label,
                json!({"source_event":"NameRenewed","cost":event.cost.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRenewed expiry")?}),
            ))
        }
        "NameRenewed(string,bytes32,uint256,uint256,bytes32)" => {
            let event = decode_event_log::<renew_referrer::NameRenewed>(
                &raw.topics,
                &raw.data,
                "NameRenewed log is malformed",
            )?;
            Ok((
                event.name,
                event.label,
                json!({"source_event":"NameRenewed","cost":event.cost.to_string(),"expiry":crate::evm_abi::u256_i64(event.expires, "NameRenewed expiry")?,"referrer":hex_string(event.referrer)}),
            ))
        }
        signature => bail!("unsupported registrar ABI event {signature}"),
    }
}
