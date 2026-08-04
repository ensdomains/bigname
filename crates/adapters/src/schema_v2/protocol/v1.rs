mod reconcile_support;
mod registrar;
mod registry;
mod resolver;
mod reverse;
mod support;
mod upgrade;
mod wrapper;

use anyhow::bail;

use self::reconcile_support::{
    event_position, is_pending_resolver_setup, is_pending_setup, is_registry_ownership_setup,
};

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
        // A first-seen renewal also synthesizes RegistrationGranted for persistence, but only
        // NameRegistered is a same-transaction registration setup.
        .filter(|event| {
            event
                .after_state
                .get("source_event")
                .and_then(serde_json::Value::as_str)
                == Some("NameRegistered")
        })
        .filter_map(|event| {
            let registrant = event
                .after_state
                .get("registrant")
                .and_then(serde_json::Value::as_str);
            let registration_emitter = event
                .raw_fact_ref
                .get("emitting_address")
                .and_then(serde_json::Value::as_str);
            debug_assert!(
                registrant.is_some(),
                "RegistrationGranted event {} must carry after_state.registrant for same-transaction reconciliation",
                event.event_identity,
            );
            debug_assert!(
                registration_emitter.is_some(),
                "RegistrationGranted event {} must carry raw_fact_ref.emitting_address for same-transaction reconciliation",
                event.event_identity,
            );
            registrant?;
            Some((
                event.namespace.clone(),
                event.logical_name_id.clone()?,
                event.resource_id?,
                event.block_hash.clone()?,
                event.transaction_hash.clone()?,
                event.log_index?,
                event.after_state.get("namehash")?.as_str()?.to_owned(),
                event
                    .after_state
                    .get("authority_key")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                registration_emitter?.to_owned(),
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
        authority_key,
        registration_emitter,
    ) in registrations
    {
        let last_ownership_setup_log_index = output
            .normalized_events
            .iter()
            .filter(|event| {
                is_registry_ownership_setup(
                    event,
                    &namespace,
                    &block_hash,
                    &transaction_hash,
                    log_index,
                    &namehash,
                )
            })
            .filter_map(|event| event.log_index)
            .max();
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
            .filter_map(event_position)
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
        let owner_positions = output
            .normalized_events
            .iter()
            .filter_map(|event| {
                (is_pending_setup(
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
                    == Some("NewOwner"))
                .then(|| {
                    Some((
                        event_position(event)?,
                        event.after_state.get("owner")?.as_str()?.to_owned(),
                    ))
                })
                .flatten()
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let last_owner_position = owner_positions.keys().next_back().copied();
        // Transient controller artifact removal considers only pending registry NewOwner logs.
        // Its canonical admitted case is the retired controller stream: BaseRegistrar's reclaim
        // path emits that shape through setSubnodeOwner.
        // (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
        // (upstream: .refs/ens_subgraph/subgraph.yaml:L148 @ ens_subgraph@723f1b6)
        // (upstream: .refs/ens_subgraph/subgraph.yaml:L162 @ ens_subgraph@723f1b6)
        // (upstream: .refs/ens_subgraph/subgraph.yaml:L165 @ ens_subgraph@723f1b6)
        // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L174 @ ens_v1@91c966f)
        // The current controller's resolver path uses setRecord, whose ownership write emits
        // Transfer rather than NewOwner, so it cannot fire this trigger.
        // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L294 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L301 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L68 @ ens_v1@91c966f)
        let transient_owner_positions = owner_positions
            .iter()
            .filter(|(position, owner)| {
                Some(**position) != last_owner_position
                    && owner.eq_ignore_ascii_case(&registration_emitter)
                    && output.normalized_events.iter().any(|event| {
                        event_position(event) == Some(**position)
                            && is_resource_permission_grant(event, owner)
                            && event.resource_id.is_some_and(|resource| {
                                stale_registry_resources.contains(&resource)
                            })
                    })
            })
            .map(|(position, _)| *position)
            .collect::<std::collections::BTreeSet<_>>();
        // A false transient match requires a non-canonical admitted emitter to be the real prior
        // owner and perform an ownership round trip inside its own registration transaction.
        let predecessor_owner_positions = owner_positions
            .keys()
            .filter(|position| {
                Some(**position) != last_owner_position
                    && !transient_owner_positions.contains(position)
            })
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let transient_owners = transient_owner_positions
            .iter()
            .filter_map(|position| owner_positions.get(position))
            .map(|owner| owner.to_ascii_lowercase())
            .collect::<std::collections::BTreeSet<_>>();
        output.normalized_events.retain(|event| {
            let position = event_position(event);
            let references_stale_registry_resource = event
                .resource_id
                .is_some_and(|resource| stale_registry_resources.contains(&resource));
            let transient_permission = event.event_kind == "PermissionChanged"
                && references_stale_registry_resource
                && position.is_some_and(|position| pending_positions.contains(&position))
                && transient_owners
                    .iter()
                    .any(|owner| permission_subject_is(event, owner));
            !(transient_permission
                || event.event_kind != "PermissionChanged"
                    && references_stale_registry_resource
                    && position
                        .is_some_and(|position| transient_owner_positions.contains(&position)))
        });
        let setup_revocations = output
            .normalized_events
            .iter()
            .filter_map(|event| {
                let position = event_position(event)?;
                let resource_id = event.resource_id?;
                if !pending_positions.contains(&position)
                    || !is_permission_revocation(event)
                    || !stale_registry_resources.contains(&resource_id)
                {
                    return None;
                }
                Some(PermissionRevocation {
                    resource_id,
                    subject: permission_subject(event)?.to_owned(),
                    scope: event.after_state.get("scope")?.clone(),
                    position,
                })
            })
            .collect::<Vec<_>>();
        for event in output.normalized_events.iter_mut().filter(|event| {
            concerns_predecessor_epoch(
                event,
                &stale_registry_resources,
                &predecessor_owner_positions,
                &setup_revocations,
            )
        }) {
            event.logical_name_id = Some(logical_name_id.clone());
            refresh_interpreter_state_key(event);
        }
        let mut retargeted_resolver_starts = std::collections::BTreeMap::new();
        for event in output.normalized_events.iter_mut().filter(|event| {
            if concerns_predecessor_epoch(
                event,
                &stale_registry_resources,
                &predecessor_owner_positions,
                &setup_revocations,
            ) {
                return false;
            }
            let targets_registration = is_pending_setup(
                event,
                &namespace,
                &block_hash,
                &transaction_hash,
                log_index,
                &namehash,
            ) || is_pending_resolver_setup(
                event,
                &namespace,
                &block_hash,
                &transaction_hash,
                log_index,
                &namehash,
                last_ownership_setup_log_index,
                &stale_registry_resources,
            );
            let references_pending_resource = event_position(event)
                .is_some_and(|position| pending_positions.contains(&position))
                && event
                    .resource_id
                    .is_some_and(|resource| stale_registry_resources.contains(&resource));
            targets_registration || references_pending_resource
        }) {
            let resolver_event = matches!(
                event.source_family.as_str(),
                "ens_v1_resolver_l1" | "basenames_base_resolver"
            );
            event.logical_name_id = Some(logical_name_id.clone());
            event.resource_id = Some(resource_id);
            if let Some(authority_key) = authority_key.as_deref() {
                retarget_permission_authority(&mut event.after_state, authority_key);
            }
            if let Some(state) = event.after_state.as_object_mut() {
                state.remove("authority_kind");
                state.remove("authority_key");
            }
            refresh_interpreter_state_key(event);
            event.before_state = serde_json::json!({});
            if resolver_event
                && let (Some(state_key), Some(position)) = (
                    event
                        .raw_fact_ref
                        .get(INTERPRETER_STATE_KEY)
                        .and_then(serde_json::Value::as_str),
                    event_position(event),
                )
            {
                retargeted_resolver_starts
                    .entry(state_key.to_owned())
                    .and_modify(|start: &mut (i64, i64, i64)| *start = (*start).min(position))
                    .or_insert(position);
            }
        }
        let mut resolver_after_by_state_key = std::collections::BTreeMap::new();
        for event in &mut output.normalized_events {
            let Some(state_key) = event
                .raw_fact_ref
                .get(INTERPRETER_STATE_KEY)
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let Some(start) = retargeted_resolver_starts.get(state_key) else {
                continue;
            };
            if event_position(event).is_none_or(|position| position < *start) {
                continue;
            }
            event.before_state = resolver_after_by_state_key
                .insert(state_key.to_owned(), event.after_state.clone())
                .unwrap_or_else(|| serde_json::json!({}));
        }
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
        let retained_resource_references = output
            .normalized_events
            .iter()
            .filter_map(|event| event.resource_id)
            .chain(
                output
                    .surface_bindings
                    .iter()
                    .map(|binding| binding.resource_id),
            )
            .collect::<std::collections::BTreeSet<_>>();
        output.resources.retain(|resource| {
            !stale_registry_resources.contains(&resource.resource_id)
                || retained_resource_references.contains(&resource.resource_id)
        });
    }
}

struct PermissionRevocation {
    resource_id: uuid::Uuid,
    subject: String,
    scope: serde_json::Value,
    position: (i64, i64, i64),
}

fn concerns_predecessor_epoch(
    event: &NormalizedEvent,
    stale_registry_resources: &std::collections::BTreeSet<uuid::Uuid>,
    predecessor_owner_positions: &std::collections::BTreeSet<(i64, i64, i64)>,
    setup_revocations: &[PermissionRevocation],
) -> bool {
    if event.event_kind != "PermissionChanged" {
        return event_position(event)
            .is_some_and(|position| predecessor_owner_positions.contains(&position));
    }
    let Some(resource_id) = event
        .resource_id
        .filter(|resource| stale_registry_resources.contains(resource))
    else {
        return false;
    };
    if is_permission_revocation(event) {
        return true;
    }
    let (Some(position), Some(subject), Some(scope)) = (
        event_position(event),
        permission_subject(event),
        event.after_state.get("scope"),
    ) else {
        return false;
    };
    event
        .after_state
        .get("grant_source")
        .is_some_and(|source| !source.is_null())
        && setup_revocations.iter().any(|revocation| {
            revocation.resource_id == resource_id
                && revocation.subject.eq_ignore_ascii_case(subject)
                && &revocation.scope == scope
                && revocation.position > position
        })
}

fn is_permission_revocation(event: &NormalizedEvent) -> bool {
    event.event_kind == "PermissionChanged"
        && event
            .after_state
            .get("revocation_source")
            .is_some_and(|source| !source.is_null())
}

fn permission_subject(event: &NormalizedEvent) -> Option<&str> {
    event.after_state.get("subject")?.as_str()
}

fn permission_subject_is(event: &NormalizedEvent, subject: &str) -> bool {
    permission_subject(event).is_some_and(|candidate| candidate.eq_ignore_ascii_case(subject))
}

fn is_resource_permission_grant(event: &NormalizedEvent, subject: &str) -> bool {
    event.event_kind == "PermissionChanged"
        && permission_subject_is(event, subject)
        && event.after_state["scope"]["kind"] == "resource"
        && event
            .after_state
            .get("grant_source")
            .is_some_and(|source| !source.is_null())
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
