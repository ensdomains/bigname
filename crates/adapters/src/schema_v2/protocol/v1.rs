mod reconcile_support;
mod registrar;
mod registry;
mod resolver;
mod reverse;
mod support;
pub(in crate::schema_v2) mod unmasked_word;
mod upgrade;
mod wrapper;

use anyhow::bail;

use super::Interpreted;
use crate::schema_v2::{
    catalog::Selected,
    model::{BatchOutput, NormalizedEvent, RawLogInput},
    seam::{INTERPRETER_STATE_KEY, STATE_SCOPE_KEY},
    state::State,
    state_key::interpreter_state_key,
};

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    registrar_context: super::super::migration::RegistrarContext,
) -> anyhow::Result<Interpreted> {
    match selected.source.source_family.as_str() {
        "ens_v1_registrar_l1" | "basenames_base_registrar" => {
            registrar::interpret(selected, raw, state, registrar_context)
        }
        "ens_v1_registry_l1" | "basenames_base_registry" => {
            registry::interpret(selected, raw, state)
        }
        "ens_v1_resolver_l1" | "basenames_base_resolver" => {
            resolver::interpret(selected, raw, state)
        }
        "ens_v1_wrapper_l1" => wrapper::interpret(selected, raw, state),
        "ens_v1_reverse_l1" | "basenames_base_primary" => reverse::interpret(selected, raw),
        family if family.ends_with("_execution") || family == "basenames_l1_compat" => {
            Ok(Interpreted::new())
        }
        family => bail!("source family {family} has no ENSv1/Basenames adapter"),
    }
}

pub(super) fn reconcile_same_transaction_setups(output: &mut BatchOutput) {
    reconcile_support::reconcile(output);
}

fn authority_arm(namespace: &str) -> &'static str {
    if namespace == "basenames" {
        "basenames"
    } else {
        "ens_v1"
    }
}

fn retarget_permission_authority(state: &mut serde_json::Value, authority_key: &str) {
    for field in ["grant_source", "revocation_source"] {
        let Some(source) = state
            .get_mut(field)
            .and_then(serde_json::Value::as_object_mut)
            .filter(|source| {
                source.get("kind").and_then(serde_json::Value::as_str) == Some("ens_v1_authority")
            })
        else {
            continue;
        };
        source.insert(
            "authority_kind".to_owned(),
            serde_json::Value::String("registrar".to_owned()),
        );
        source.insert(
            "authority_key".to_owned(),
            serde_json::Value::String(authority_key.to_owned()),
        );
    }
}

fn refresh_interpreter_state_key(event: &mut NormalizedEvent) {
    let state_scope = event
        .raw_fact_ref
        .get(STATE_SCOPE_KEY)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let state_key = interpreter_state_key(
        &event.namespace,
        event.logical_name_id.as_deref(),
        event.resource_id,
        &event.event_kind,
        &event.source_family,
        &state_scope,
    );
    if let Some(raw_fact_ref) = event.raw_fact_ref.as_object_mut() {
        raw_fact_ref.insert(
            INTERPRETER_STATE_KEY.to_owned(),
            serde_json::Value::String(state_key),
        );
    }
}
