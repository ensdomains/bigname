mod registrar;
mod registry;
mod resolver;
mod reverse;
mod support;
mod upgrade;
mod wrapper;

use anyhow::bail;

use super::Interpreted;
use crate::schema_v2::{
    catalog::Selected,
    model::{BatchOutput, NormalizedEvent, RawLogInput},
    state::State,
};

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    match selected.source.source_family.as_str() {
        "ens_v1_registrar_l1" | "basenames_base_registrar" => {
            registrar::interpret(selected, raw, state)
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
    let registrations = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationGranted")
        .filter_map(|event| {
            Some((
                event.namespace.clone(),
                event.logical_name_id.clone()?,
                event.resource_id?,
                event.block_hash.clone()?,
                event.transaction_hash.clone()?,
                event.log_index?,
                event.after_state.get("namehash")?.as_str()?.to_owned(),
            ))
        })
        .collect::<Vec<_>>();
    for (
        namespace,
        logical_name_id,
        resource_id,
        block_hash,
        transaction_hash,
        log_index,
        namehash,
    ) in registrations
    {
        let pending_positions = output
            .normalized_events
            .iter()
            .filter(|event| {
                is_pending_setup(
                    event,
                    &namespace,
                    &block_hash,
                    &transaction_hash,
                    log_index,
                    &namehash,
                )
            })
            .filter_map(|event| {
                Some((
                    event.block_number?,
                    event.transaction_index?,
                    event.log_index?,
                ))
            })
            .collect::<std::collections::BTreeSet<_>>();
        let stale_registry_resources = output
            .normalized_events
            .iter()
            .filter(|event| {
                is_pending_setup(
                    event,
                    &namespace,
                    &block_hash,
                    &transaction_hash,
                    log_index,
                    &namehash,
                ) && event
                    .after_state
                    .get("authority_kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("registry_only")
            })
            .filter_map(|event| event.resource_id)
            .collect::<std::collections::BTreeSet<_>>();
        let last_owner_log = output
            .normalized_events
            .iter()
            .filter(|event| {
                is_pending_setup(
                    event,
                    &namespace,
                    &block_hash,
                    &transaction_hash,
                    log_index,
                    &namehash,
                ) && event
                    .after_state
                    .get("source_event")
                    .and_then(serde_json::Value::as_str)
                    == Some("NewOwner")
            })
            .filter_map(|event| event.log_index)
            .max();
        if let Some(last_owner_log) = last_owner_log {
            output.normalized_events.retain(|event| {
                !is_pending_setup(
                    event,
                    &namespace,
                    &block_hash,
                    &transaction_hash,
                    log_index,
                    &namehash,
                ) || event
                    .after_state
                    .get("source_event")
                    .and_then(serde_json::Value::as_str)
                    != Some("NewOwner")
                    || event.log_index.is_none_or(|index| index >= last_owner_log)
            });
        }
        for event in output.normalized_events.iter_mut().filter(|event| {
            is_pending_setup(
                event,
                &namespace,
                &block_hash,
                &transaction_hash,
                log_index,
                &namehash,
            )
        }) {
            event.logical_name_id = Some(logical_name_id.clone());
            event.resource_id = Some(resource_id);
            event.before_state = serde_json::json!({});
            if let Some(state) = event.after_state.as_object_mut() {
                state.remove("authority_kind");
                state.remove("authority_key");
            }
        }
        output
            .resources
            .retain(|resource| !stale_registry_resources.contains(&resource.resource_id));
        output.surface_bindings.retain(|binding| {
            !stale_registry_resources.contains(&binding.resource_id)
                || !pending_positions.contains(&(
                    binding.block_number,
                    binding
                        .provenance
                        .get("transaction_index")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or_default(),
                    binding
                        .provenance
                        .get("log_index")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or_default(),
                ))
        });
        output.binding_closures.retain(|closure| {
            closure.logical_name_id != logical_name_id
                || !pending_positions.contains(&(
                    closure.block_number,
                    closure.transaction_index,
                    closure.log_index,
                ))
        });
    }
}

fn is_pending_setup(
    event: &NormalizedEvent,
    namespace: &str,
    block_hash: &str,
    transaction_hash: &str,
    registration_log_index: i64,
    namehash: &str,
) -> bool {
    event.namespace == namespace
        && matches!(
            event.source_family.as_str(),
            "ens_v1_registry_l1" | "basenames_base_registry"
        )
        && event.block_hash.as_deref() == Some(block_hash)
        && event.transaction_hash.as_deref() == Some(transaction_hash)
        && event
            .log_index
            .is_some_and(|index| index < registration_log_index)
        && event_target_namehash(event).is_some_and(|target| target.eq_ignore_ascii_case(namehash))
}

fn event_target_namehash(event: &NormalizedEvent) -> Option<&str> {
    event
        .after_state
        .get("child_node")
        .or_else(|| event.after_state.get("node"))
        .and_then(serde_json::Value::as_str)
}
