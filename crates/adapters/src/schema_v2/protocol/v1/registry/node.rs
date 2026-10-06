use crate::{
    evm_abi::{address_hex, decode_event_log_tolerant_address_word},
    schema_v2::{catalog::Selected, identity::NodeIdentityDraft, model::RawLogInput, state::State},
};

use super::{NewOwner, Transfer, child_node, unmasked_word};

pub(super) fn retain_identity(
    output: &mut crate::schema_v2::protocol::Interpreted,
    selected: &Selected,
    raw: &RawLogInput,
    state: &State,
    identity: Option<NodeIdentityDraft>,
) {
    let Some(identity) = identity else { return };
    let event = output
        .events
        .iter_mut()
        .find(|event| event.event_kind == "SubregistryChanged")
        .expect("NewOwner retains its structural observation");
    event.logical_name_id = Some(format!(
        "{}:{}",
        selected.source.namespace, identity.namehash
    ));
    event.after_state[crate::schema_v2::seam::NAME_IDENTITY_OBSERVED_KEY] = serde_json::json!(true);
    event.after_state["labelhashes"] = serde_json::json!(identity.labelhashes);
    event.after_state["surface_known"] = serde_json::json!(
        state.v1_active_surface_materialized(&selected.source.namespace, &identity.namehash)
    );
    event.state_scope = format!(
        "{}:name-identity:{}",
        raw.emitting_address.to_ascii_lowercase(),
        identity.namehash
    );
    output.node_identities.push(identity);
}

pub(super) fn observe_identity(
    selected: &Selected,
    state: &mut State,
    after: &serde_json::Value,
) -> anyhow::Result<Option<NodeIdentityDraft>> {
    if selected.source.source_family != "ens_v1_registry_l1" || selected.event.name != "NewOwner" {
        return Ok(None);
    }
    let namespace = &selected.source.namespace;
    let (Some(parent), Some(label), Some(child)) = (
        after["node"].as_str(),
        after["labelhash"].as_str(),
        after["child_node"].as_str(),
    ) else {
        return Ok(None);
    };
    let Some(mut path) = state.v1_node_path(namespace, parent) else {
        return Ok(None);
    };
    path.insert(0, label.to_owned());
    state.remember_v1_path(namespace, child, &path)?;
    state.observe_v1_active_surface(namespace, child);
    Ok(Some(NodeIdentityDraft {
        namehash: child.to_owned(),
        labelhashes: path,
    }))
}

pub(in crate::schema_v2) fn registration_setup_node(
    selected: &Selected,
    raw: &RawLogInput,
) -> anyhow::Result<Option<(String, String)>> {
    if selected.emitter_role.as_deref() != Some("registry") {
        return Ok(None);
    }
    let tolerate_unmasked_words = selected.source.source_family == "ens_v1_registry_l1";
    match selected.event.name.as_str() {
        "NewOwner" => {
            let decoded = unmasked_word::decode_registry_event::<NewOwner>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "NewOwner log is malformed",
                decode_event_log_tolerant_address_word::<NewOwner>,
            )?;
            Ok(Some((
                child_node(decoded.event.node, decoded.event.label),
                address_hex(decoded.event.owner),
            )))
        }
        "Transfer" => {
            let decoded = unmasked_word::decode_registry_event::<Transfer>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "registry Transfer log is malformed",
                decode_event_log_tolerant_address_word::<Transfer>,
            )?;
            Ok(Some((
                format!("{:#x}", decoded.event.node),
                address_hex(decoded.event.owner),
            )))
        }
        _ => Ok(None),
    }
}
