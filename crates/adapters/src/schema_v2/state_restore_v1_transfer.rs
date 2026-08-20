use serde_json::Value;
use uuid::Uuid;

use crate::schema_v2::{model::PriorEventInput, state::State};

pub(super) fn restore(state: &mut State, event: &PriorEventInput) {
    if event.event_kind != "TokenControlTransferred"
        || !event
            .after_state
            .get("source_event")
            .and_then(Value::as_str)
            .is_some_and(|source| source.starts_with("Transfer"))
    {
        return;
    }
    let (Some(namehash), Some(to)) = (
        event
            .after_state
            .get("namehash")
            .or_else(|| event.after_state.get("node"))
            .and_then(Value::as_str),
        event.after_state.get("to").and_then(Value::as_str),
    ) else {
        return;
    };
    restore_wrapper_fallback(state, event, namehash);
    if event.source_family == "ens_v1_wrapper_l1" {
        state.transfer_v1_wrapper_owner(
            &event.namespace,
            namehash,
            &event.source_family,
            to.to_owned(),
        );
    } else if matches!(
        event.source_family.as_str(),
        "ens_v1_registrar_l1" | "basenames_base_registrar"
    ) {
        state.transfer_v1_registrar_owner(&event.namespace, namehash, to.to_owned());
        state.converge_v1_registrar_transfer(
            &event.namespace,
            namehash,
            event
                .block_timestamp
                .map(|timestamp| timestamp.unix_timestamp())
                .unwrap_or(i64::MIN),
        );
    }
}

fn restore_wrapper_fallback(state: &mut State, event: &PriorEventInput, namehash: &str) {
    if event.source_family != "ens_v1_registrar_l1"
        || event
            .after_state
            .get("fallback_from_wrapper")
            .and_then(Value::as_bool)
            != Some(true)
        || state.v1_registrar(&event.namespace, namehash).is_some()
    {
        return;
    }
    let lineage = event
        .after_state
        .get("token_lineage_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    let (Some(logical_name_id), Some(resource_id), Some(lineage), Some(labelhash)) = (
        event.logical_name_id.as_ref(),
        event.resource_id,
        lineage,
        event.after_state.get("labelhash").and_then(Value::as_str),
    ) else {
        return;
    };
    state.observe_v1_registrar(
        &event.namespace,
        namehash,
        logical_name_id.clone(),
        event
            .after_state
            .get("surface_known")
            .and_then(Value::as_bool)
            == Some(true),
        resource_id,
        lineage,
        event.source_family.clone(),
        event
            .after_state
            .get("authority_source_manifest_id")
            .and_then(Value::as_i64),
        Some(labelhash.to_owned()),
        event.after_state.get("expiry").and_then(super::parse_i64),
        event
            .after_state
            .get("fallback_from")
            .and_then(Value::as_str)
            .map(str::to_owned),
        event
            .after_state
            .get("authority_key")
            .and_then(Value::as_str)
            .map(str::to_owned),
        true,
        false,
    );
}
