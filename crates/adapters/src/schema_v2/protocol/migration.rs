use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use crate::{
    evm_abi::{address_hex, decode_event_log, decode_event_log_data_as, hex_string, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::{event_string_value, namehash, stable_uuid},
        model::RawLogInput,
        state::State,
    },
};

use super::{EventDraft, Interpreted, MigrationObservation, ensure_declared};

sol! {
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
    event RawBridgeNameRenewed(uint256 indexed tokenId, bytes label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
    event ControllerAdded(address indexed controller);
    event ControllerRemoved(address indexed controller);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
}

mod base_registrar {
    use alloy_sol_types::sol;

    sol! {
        event NameRenewed(uint256 indexed id, uint256 expires);
    }
}

pub(super) fn interpret(selected: &Selected, raw: &RawLogInput) -> anyhow::Result<Interpreted> {
    let mut output = Interpreted::new();
    let decoded = match selected.event.name.as_str() {
        "ProxyDeployed" => proxy_deployed(selected, raw, &mut output)?,
        "NameRenewed" if selected.emitter_role.as_deref() == Some("ens_v1_renewal_bridge") => {
            bridge_renewed(selected, raw, &mut output)?
        }
        event => bail!("unsupported ENSv2 migration event {event}"),
    };
    output.migration_observations.push(MigrationObservation {
        source_family: selected.source.source_family.clone(),
        source_manifest_id: selected.source.manifest_id,
        event_name: selected.event.name.clone(),
        emitter_role: selected.emitter_role.clone(),
        contract_instance_id: selected.contract_instance_id,
        raw: raw.clone(),
        decoded,
        correlated_wrapper_expiry: None,
    });
    Ok(output)
}

/// Decode a BaseRegistrar log attributed by `ens_v1_registrar_l1`. Event drafts retain
/// `ens_v2_migration_l1` provenance and are materialized only when that manifest's launch-bounded
/// correlation admits the observation.
pub(super) fn interpret_base_registrar(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let mut output = Interpreted::new();
    let (decoded, correlated_wrapper_expiry) = match selected.event.signature.as_str() {
        "ControllerAdded(address)" => (controller(selected, raw, state, &mut output, true)?, None),
        "ControllerRemoved(address)" => {
            (controller(selected, raw, state, &mut output, false)?, None)
        }
        "NameRegistered(uint256,address,uint256)" => {
            (name_registered(selected, raw, &mut output)?, None)
        }
        "NameRenewed(uint256,uint256)" => base_renewed(selected, raw, state, &mut output)?,
        "Transfer(address,address,uint256)" => (transfer(raw)?, None),
        signature => bail!("unsupported ENSv1 BaseRegistrar event {signature}"),
    };
    output.migration_events = std::mem::take(&mut output.events);
    output.migration_observations.push(MigrationObservation {
        source_family: selected.source.source_family.clone(),
        source_manifest_id: selected.source.manifest_id,
        event_name: selected.event.name.clone(),
        emitter_role: selected.emitter_role.clone(),
        contract_instance_id: selected.contract_instance_id,
        raw: raw.clone(),
        decoded,
        correlated_wrapper_expiry,
    });
    Ok(output)
}

pub(in crate::schema_v2) fn is_graveyard_cleanup(
    selected: &Selected,
    raw: &RawLogInput,
    graveyard: &str,
) -> anyhow::Result<bool> {
    if selected.event.signature != "NameRegistered(uint256,address,uint256)" {
        return Ok(false);
    }
    let event = decode_event_log::<NameRegistered>(
        &raw.topics,
        &raw.data,
        "BaseRegistrar NameRegistered log is malformed",
    )?;
    Ok(crate::schema_v2::migration::is_graveyard_cleanup(
        &json!({"owner":address_hex(event.owner), "expiry":u64::try_from(event.expires).ok()}),
        graveyard,
    ))
}

fn proxy_deployed(
    selected: &Selected,
    raw: &RawLogInput,
    output: &mut Interpreted,
) -> anyhow::Result<Value> {
    let event = decode_event_log::<ProxyDeployed>(
        &raw.topics,
        &raw.data,
        "ProxyDeployed log is malformed",
    )?;
    ensure_declared(selected, &["ContractDiscovered"])?;
    let decoded = json!({
        "sender":address_hex(event.sender),
        "proxy_address":address_hex(event.proxyAddress),
        "salt":u256_word_hex(event.salt),
        "implementation":address_hex(event.implementation),
    });
    output.events.push(event_draft(
        "ContractDiscovered",
        None,
        format!("ContractDiscovered:{}", address_hex(event.proxyAddress)),
        decoded.clone(),
        format!("migration-factory:{}", address_hex(event.proxyAddress)),
    ));
    Ok(decoded)
}

fn bridge_renewed(
    selected: &Selected,
    raw: &RawLogInput,
    output: &mut Interpreted,
) -> anyhow::Result<Value> {
    let event = decode_event_log_data_as::<RawBridgeNameRenewed>(
        &raw.topics,
        &raw.data,
        &selected.event.topic0,
        "migration bridge NameRenewed log is malformed",
    )?;
    ensure_declared(selected, &["RegistrationRenewed", "PreimageObserved"])?;
    let raw_label = event.label.to_vec();
    let labelhash = keccak256(&raw_label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("{}:{namehash}", selected.source.namespace);
    let decoded = json!({
        "token_id":u256_word_hex(event.tokenId),
        "base_token_id":u256_word_hex(U256::from_be_bytes(labelhash.0)),
        "raw_label":event_string_value(&raw_label),
        "labelhash":format!("{labelhash:#x}"),
        "namehash":namehash,
        "duration":event.duration,
        "expiry":event.newExpiry,
        "payment_token":address_hex(event.paymentToken),
        "referrer":hex_string(event.referrer),
        "amount":event.amount.to_string(),
    });
    // This scope is a persisted interpreter-state identity. Keep the bridge stream distinct
    // from the launch-bounded ENSv1 registrar stream and stable across candidate/activated
    // re-derivation.
    let scope = format!("migration-renewal:bridge:{logical_name_id}");
    output.events.push(event_draft(
        "RegistrationRenewed",
        Some(logical_name_id.clone()),
        format!("RegistrationRenewed:{}", u256_word_hex(event.tokenId)),
        decoded.clone(),
        scope.clone(),
    ));
    output.events.push(event_draft(
        "PreimageObserved",
        Some(logical_name_id),
        format!("PreimageObserved:{labelhash:#x}"),
        decoded.clone(),
        scope,
    ));
    Ok(decoded)
}

fn controller(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    output: &mut Interpreted,
    approved: bool,
) -> anyhow::Result<Value> {
    let account = if approved {
        address_hex(
            decode_event_log::<ControllerAdded>(
                &raw.topics,
                &raw.data,
                "ControllerAdded log is malformed",
            )?
            .controller,
        )
    } else {
        address_hex(
            decode_event_log::<ControllerRemoved>(
                &raw.topics,
                &raw.data,
                "ControllerRemoved log is malformed",
            )?
            .controller,
        )
    };
    ensure_declared(selected, &["PermissionChanged"])?;
    let source_event = if approved {
        "ControllerAdded"
    } else {
        "ControllerRemoved"
    };
    state.set_v1_registrar_controller(&account, approved, raw);
    let decoded = json!({
        "subject":account,
        "scope":{"kind":"registrar_controller"},
        "approved":approved,
        "source_event":source_event,
    });
    output.events.push(EventDraft {
        event_kind: "PermissionChanged".to_owned(),
        logical_name_id: None,
        resource_id: None,
        identity_suffix: format!("PermissionChanged:{account}:{source_event}"),
        explicit_before: None,
        after_state: decoded.clone(),
        // Adds and removals for one controller must share retained interpreter state.
        state_scope: format!("registrar-controller:{account}"),
    });
    Ok(decoded)
}

fn name_registered(
    selected: &Selected,
    raw: &RawLogInput,
    output: &mut Interpreted,
) -> anyhow::Result<Value> {
    let event = decode_event_log::<NameRegistered>(
        &raw.topics,
        &raw.data,
        "BaseRegistrar NameRegistered log is malformed",
    )?;
    ensure_declared(selected, &["RegistrationReleased"])?;
    let labelhash = B256::from(event.id.to_be_bytes::<32>());
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("{}:{namehash}", selected.source.namespace);
    let decoded = json!({
        "token_id":u256_word_hex(event.id),
        "labelhash":format!("{labelhash:#x}"),
        "namehash":namehash,
        "owner":address_hex(event.owner),
        "expiry":u64::try_from(event.expires).map_or_else(
            |_| Value::String(event.expires.to_string()), Value::from),
        "source_event":"NameRegistered",
    });
    output.events.push(event_draft(
        "RegistrationReleased",
        Some(logical_name_id.clone()),
        format!("RegistrationReleased:{}", u256_word_hex(event.id)),
        decoded.clone(),
        format!("graveyard-cleanup:{logical_name_id}"),
    ));
    Ok(decoded)
}

fn base_renewed(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    output: &mut Interpreted,
) -> anyhow::Result<(Value, Option<u64>)> {
    let event = decode_event_log::<base_registrar::NameRenewed>(
        &raw.topics,
        &raw.data,
        "BaseRegistrar NameRenewed log is malformed",
    )?;
    ensure_declared(selected, &["RegistrationRenewed", "ExpiryChanged"])?;
    let labelhash = B256::from(event.id.to_be_bytes::<32>());
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("{}:{namehash}", selected.source.namespace);
    let registrar_expiry = u64::try_from(event.expires).ok();
    let decoded = json!({
        "token_id":u256_word_hex(event.id),
        "labelhash":format!("{labelhash:#x}"),
        "namehash":namehash,
        "expiry":registrar_expiry.map_or_else(
            || Value::String(event.expires.to_string()), Value::from),
        "source_event":"NameRenewed",
        "resource_anchor":{
            "candidate_resource_id":stable_uuid(&format!(
                "ens-v1-migration-registrar-resource:{}:{}:{}",
                raw.chain_id,
                selected.contract_instance_id,
                u256_word_hex(event.id),
            )).to_string(),
            "anchor_kind":"registrar_backed_registration",
            "contract_instance_id":selected.contract_instance_id,
            "token_id":u256_word_hex(event.id),
            "labelhash":format!("{labelhash:#x}"),
            "selection":"current_registrar_resource_at_observation",
            "consumer_visibility":"candidate",
        },
    });
    let correlated_wrapper_expiry = registrar_expiry.and_then(|expiry| {
        state.correlated_v1_wrapper_expiry(&selected.source.namespace, &namehash, expiry, raw)
    });
    let mut persisted = decoded.clone();
    // BaseRegistrar renewals retained as ENSv1→ENSv2 migration evidence share one
    // [interpreter state key](../../../../../docs/glossary.md#interpreter-state-key) per event
    // kind and name. Carry the latest completed correlation onto every later numeric renewal so
    // cold restore's latest readable row is self-contained.
    if let Some(wrapper_expiry) =
        state.retained_v1_correlated_wrapper_expiry(&selected.source.namespace, &namehash)
    {
        persisted["wrapper_expiry"] = Value::from(wrapper_expiry);
    }
    // This scope is a persisted interpreter-state identity. It deliberately names the emitter
    // class rather than the candidate resource UUID so slice 2 reproduces the same scope.
    let scope = format!("migration-renewal:base-registrar:{logical_name_id}");
    let token_id = u256_word_hex(event.id);
    for event_kind in ["RegistrationRenewed", "ExpiryChanged"] {
        output.events.push(event_draft(
            event_kind,
            Some(logical_name_id.clone()),
            format!("{event_kind}:{token_id}"),
            persisted.clone(),
            scope.clone(),
        ));
    }
    Ok((decoded, correlated_wrapper_expiry))
}

fn transfer(raw: &RawLogInput) -> anyhow::Result<Value> {
    let event = decode_event_log::<Transfer>(
        &raw.topics,
        &raw.data,
        "BaseRegistrar Transfer log is malformed",
    )?;
    let labelhash = B256::from(event.tokenId.to_be_bytes::<32>());
    Ok(json!({
        "from":address_hex(event.from),
        "to":address_hex(event.to),
        "token_id":u256_word_hex(event.tokenId),
        "labelhash":format!("{labelhash:#x}"),
        "namehash":eth_namehash(labelhash),
    }))
}

fn event_draft(
    event_kind: &str,
    logical_name_id: Option<String>,
    identity_suffix: String,
    mut after_state: Value,
    state_scope: String,
) -> EventDraft {
    after_state
        .as_object_mut()
        .expect("migration event state is an object")
        .entry("source_event")
        .or_insert_with(|| Value::String(event_kind.to_owned()));
    EventDraft {
        event_kind: event_kind.to_owned(),
        logical_name_id,
        resource_id: None,
        identity_suffix,
        explicit_before: None,
        after_state,
        state_scope,
    }
}

fn eth_namehash(labelhash: B256) -> String {
    let parent = namehash(&["eth".to_owned()])
        .parse::<B256>()
        .expect("namehash helper returns bytes32");
    let mut input = [0_u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(labelhash.as_slice());
    format!("{:#x}", keccak256(input))
}
