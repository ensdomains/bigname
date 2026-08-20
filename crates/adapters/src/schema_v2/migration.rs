use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use serde_json::{Value, json};

use super::{
    BatchOutput, MigrationCandidateEffect, NormalizedEvent,
    catalog::{Catalog, Selected},
    manifest::ManifestSource,
    protocol::MigrationObservation,
};

mod child;
mod registry;
mod support;
use registry::correlate_registry_creation;
#[cfg(any(test, feature = "test-activation"))]
pub use support::inject_activated_transition_for_test;
use support::*;

const MIGRATION_FAMILY: &str = "ens_v2_migration_l1";
const V1_REGISTRAR_FAMILY: &str = "ens_v1_registrar_l1";
const V1_REGISTRAR_ROLE: &str = "registrar";
const CANDIDATE: &str = "candidate";
const TRANSITION_KIND: &str = "authority_transition";

pub(super) fn correlated_registrar_source(
    catalog: &Catalog,
    selected: &Selected,
    raw: &super::RawLogInput,
) -> anyhow::Result<Option<ManifestSource>> {
    if selected.source.source_family != V1_REGISTRAR_FAMILY
        || selected.emitter_role.as_deref() != Some(V1_REGISTRAR_ROLE)
    {
        return Ok(None);
    }
    let Some(migration_source) = catalog.source_for_family(MIGRATION_FAMILY) else {
        return Ok(None);
    };
    if migration_source.namespace != selected.source.namespace
        || migration_source.chain_id != selected.source.chain_id
    {
        return Ok(None);
    }
    let registrar = catalog
        .correlation_address(MIGRATION_FAMILY, "ens_v1_base_registrar")
        .context("migration manifest has no ENSv1 BaseRegistrar correlation address")?;
    let launch_block = catalog
        .declared_start_block_for_role(MIGRATION_FAMILY, "graveyard")
        .context("migration manifest has no launch-bounded Graveyard declaration")?;
    if !raw.emitting_address.eq_ignore_ascii_case(registrar) || raw.block_number < launch_block {
        return Ok(None);
    }
    Ok(Some(migration_source.clone()))
}

fn is_v1_registrar_observation(observation: &MigrationObservation) -> bool {
    observation.source_family == V1_REGISTRAR_FAMILY
        && observation.emitter_role.as_deref() == Some(V1_REGISTRAR_ROLE)
}

pub(super) fn correlate(
    catalog: &Catalog,
    mut observations: Vec<MigrationObservation>,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    observations.sort_by_key(|observation| position_key(&observation.raw));
    let migration_source = catalog.source_for_family(MIGRATION_FAMILY);
    if observations.is_empty() && migration_source.is_none() {
        return Ok(());
    }
    let migration_source = migration_source
        .context("ENSv2 migration observations have no active migration manifest")?;
    let graveyard = declared_address(catalog, "graveyard")?;
    let unlocked = declared_address(catalog, "unlocked_migration_controller")?;
    let locked = declared_address(catalog, "locked_migration_controller")?;
    let name_wrapper = catalog
        .correlation_address(MIGRATION_FAMILY, "ens_v1_name_wrapper")
        .context("migration manifest has no ENSv1 NameWrapper correlation address")?;
    let base_registrar = catalog
        .correlation_address(MIGRATION_FAMILY, "ens_v1_base_registrar")
        .context("migration manifest has no ENSv1 BaseRegistrar correlation address")?;
    let mut by_transaction = BTreeMap::<(String, String), Vec<&MigrationObservation>>::new();
    for observation in &observations {
        by_transaction
            .entry((
                observation.raw.block_hash.clone(),
                observation.raw.transaction_hash.clone(),
            ))
            .or_default()
            .push(observation);
    }
    let mut boundaries = Vec::new();
    let mut registries = Vec::new();
    for transaction_observations in by_transaction.values() {
        correlate_renewals(transaction_observations, output)?;
        correlate_controllers(name_wrapper, transaction_observations, output)?;
        correlate_historical_renewals(transaction_observations, output)?;
        correlate_cleanups(transaction_observations, &graveyard, output)?;
        let registry_groups =
            correlate_registry_creation(catalog, transaction_observations, &locked, output)?;
        correlate_authority_transitions(
            migration_source,
            transaction_observations,
            &graveyard,
            &unlocked,
            &locked,
            catalog,
            base_registrar,
            name_wrapper,
            &registry_groups,
            output,
            &mut boundaries,
        )?;
        registries.extend(registry_groups);
    }

    associate_restored_registry_effects(catalog, output)?;
    // Child correlation runs over the whole batch, not one transaction: a child's parent registry
    // is normally proven in an earlier transaction or an earlier batch entirely.
    child::correlate_children(
        catalog,
        migration_source,
        &observations,
        &registries,
        name_wrapper,
        &graveyard,
        output,
        &mut boundaries,
    )?;
    // Logs from the ENSv1→ENSv2 migration source that do not match an admitted shape are omitted;
    // unrelated factory logs stay out.
    output.normalized_events.retain(|event| {
        event.source_family != MIGRATION_FAMILY || event.consumer_visibility == CANDIDATE
    });
    insert_boundaries(output, boundaries);
    sort_and_deduplicate(output);
    Ok(())
}

fn correlate_renewals(
    observations: &[&MigrationObservation],
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let bridges = observations.iter().copied().filter(|observation| {
        observation.event_name == "NameRenewed"
            && observation.emitter_role.as_deref() == Some("ens_v1_renewal_bridge")
    });
    for bridge in bridges {
        let base_token_id = value_str(&bridge.decoded, "base_token_id")?;
        let logical_name_id = logical_name_from_decoded(&bridge.decoded)?;
        let Some(bridge_expiry) = bridge.decoded.get("expiry").and_then(Value::as_u64) else {
            continue;
        };
        let v2_events = matching_events(output, &bridge.raw, |event| {
            event.source_family == "ens_v2_registry_l1"
                && event.logical_name_id.as_deref() == Some(logical_name_id.as_str())
                && matches!(
                    event.event_kind.as_str(),
                    "ExpiryChanged" | "RegistrationRenewed"
                )
                && event.after_state.get("expiry").and_then(Value::as_u64) == Some(bridge_expiry)
                && event
                    .log_index
                    .is_some_and(|index| index < bridge.raw.log_index)
        });
        if !v2_events
            .iter()
            .any(|event| event.event_kind == "ExpiryChanged")
        {
            continue;
        }
        let Some(v2_position) = v2_events.iter().filter_map(|event| event.log_index).max() else {
            continue;
        };
        let Some(base) = observations
            .iter()
            .copied()
            .filter(|observation| {
                observation.event_name == "NameRenewed"
                    && is_v1_registrar_observation(observation)
                    && observation.decoded.get("token_id").and_then(Value::as_str)
                        == Some(base_token_id)
                    && observation.raw.log_index > v2_position
                    && observation.raw.log_index < bridge.raw.log_index
            })
            .max_by_key(|observation| observation.raw.log_index)
        else {
            continue;
        };
        let mut resources = v2_events
            .iter()
            .filter_map(|event| event.resource_id)
            .collect::<BTreeSet<_>>();
        if resources.len() > 1 {
            continue;
        }
        let successor_resource = resources.pop_first();
        let mut evidence = vec![observation_evidence(bridge), observation_evidence(base)];
        evidence.extend(v2_events.iter().map(event_evidence));
        let correlation_id =
            correlation_id("synchronized_renewal", Some(&logical_name_id), &evidence);
        mark_direct_position(output, &bridge.raw, &correlation_id);
        if let Some(successor_resource) = successor_resource {
            anchor_direct_position(output, &bridge.raw, successor_resource);
        }
        mark_direct_position(output, &base.raw, &correlation_id);
        for event in &v2_events {
            associate_event(
                output,
                &event.event_identity,
                &correlation_id,
                "synchronized_renewal",
                evidence.clone(),
            )?;
        }
    }
    Ok(())
}

fn correlate_controllers(
    name_wrapper: &str,
    observations: &[&MigrationObservation],
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let controllers = observations
        .iter()
        .copied()
        .filter(|observation| {
            is_v1_registrar_observation(observation)
                && matches!(
                    observation.event_name.as_str(),
                    "ControllerAdded" | "ControllerRemoved"
                )
        })
        .collect::<Vec<_>>();
    let mut participating = BTreeSet::new();
    for added in controllers.iter().copied().filter(|observation| {
        observation.event_name == "ControllerAdded"
            && observation
                .decoded
                .get("subject")
                .and_then(Value::as_str)
                .is_some_and(|subject| subject.eq_ignore_ascii_case(name_wrapper))
    }) {
        let Some(removed) = controllers.iter().copied().find(|observation| {
            observation.event_name == "ControllerRemoved"
                && observation.raw.log_index > added.raw.log_index
                && observation
                    .decoded
                    .get("subject")
                    .and_then(Value::as_str)
                    .is_some_and(|subject| subject.eq_ignore_ascii_case(name_wrapper))
        }) else {
            continue;
        };
        let renewed = observations.iter().copied().filter(|observation| {
            observation.event_name == "NameRenewed"
                && is_v1_registrar_observation(observation)
                && observation.raw.log_index > added.raw.log_index
                && observation.raw.log_index < removed.raw.log_index
        });
        for base in renewed {
            let logical_name_id = logical_name_from_decoded(&base.decoded)?;
            let evidence = vec![
                observation_evidence(added),
                observation_evidence(base),
                observation_evidence(removed),
            ];
            let id = correlation_id("synchronized_renewal", Some(&logical_name_id), &evidence);
            mark_direct_position(output, &added.raw, &id);
            mark_direct_position(output, &base.raw, &id);
            if let Some(wrapper_expiry) = base.correlated_wrapper_expiry {
                for event in output.normalized_events.iter_mut().filter(|event| {
                    event.source_family == MIGRATION_FAMILY && same_position(event, &base.raw)
                }) {
                    let wrapper_expiry = event
                        .after_state
                        .get("wrapper_expiry")
                        .and_then(Value::as_u64)
                        .map_or(wrapper_expiry, |retained| retained.max(wrapper_expiry));
                    event.after_state["wrapper_expiry"] = Value::from(wrapper_expiry);
                }
            }
            mark_direct_position(output, &removed.raw, &id);
            participating.insert((added.event_name.as_str(), added.raw.log_index));
            participating.insert((removed.event_name.as_str(), removed.raw.log_index));
        }
    }
    for observation in observations.iter().copied().filter(|observation| {
        is_v1_registrar_observation(observation)
            && matches!(
                observation.event_name.as_str(),
                "ControllerAdded" | "ControllerRemoved"
            )
    }) {
        if !participating.contains(&(observation.event_name.as_str(), observation.raw.log_index)) {
            let subject = value_str(&observation.decoded, "subject")?;
            let evidence = vec![observation_evidence(observation)];
            let id = correlation_id(
                "controller_configuration",
                Some(&format!(
                    "{}:{subject}:{}",
                    observation.raw.emitting_address.to_ascii_lowercase(),
                    observation.event_name
                )),
                &evidence,
            );
            mark_direct_position(output, &observation.raw, &id);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn correlate_authority_transitions(
    migration_source: &super::manifest::ManifestSource,
    observations: &[&MigrationObservation],
    graveyard: &str,
    unlocked: &str,
    locked: &str,
    catalog: &Catalog,
    base_registrar: &str,
    name_wrapper: &str,
    registry_groups: &[RegistryGroup],
    output: &mut BatchOutput,
    boundaries: &mut Vec<NormalizedEvent>,
) -> anyhow::Result<()> {
    let registrations = output
        .normalized_events
        .iter()
        .filter(|event| {
            observations
                .first()
                .is_some_and(|observation| same_transaction(event, &observation.raw))
                && event.source_family == "ens_v2_registry_l1"
                && event.event_kind == "RegistrationGranted"
                && event
                    .after_state
                    .get("source_event")
                    .and_then(Value::as_str)
                    == Some("LabelRegistered")
                && event
                    .after_state
                    .get("resource_pending")
                    .and_then(Value::as_bool)
                    == Some(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    for registration in registrations {
        let sender = value_str(&registration.after_state, "sender")?;
        let logical_name_id = registration
            .logical_name_id
            .clone()
            .context("migration registration has no logical name")?;
        let labelhash = value_str(&registration.after_state, "labelhash")?;
        let registration_log = registration
            .log_index
            .context("registration has no log index")?;
        let transfers = observations
            .iter()
            .copied()
            .filter(|observation| {
                observation.event_name == "Transfer"
                    && is_v1_registrar_observation(observation)
                    && observation.raw.log_index < registration_log
                    && observation.decoded.get("labelhash").and_then(Value::as_str)
                        == Some(labelhash)
                    && observation
                        .decoded
                        .get("to")
                        .and_then(Value::as_str)
                        .is_some_and(|to| to.eq_ignore_ascii_case(graveyard))
            })
            .collect::<Vec<_>>();
        let (migration_path, registry_group) =
            if sender.eq_ignore_ascii_case(unlocked) && !transfers.is_empty() {
                let controller_held_v1_token = transfers.iter().any(|observation| {
                    observation
                        .decoded
                        .get("from")
                        .and_then(Value::as_str)
                        .is_some_and(|from| from.eq_ignore_ascii_case(unlocked))
                });
                (
                    if controller_held_v1_token {
                        "unwrapped"
                    } else {
                        "unlocked_wrapped"
                    },
                    None,
                )
            } else if sender.eq_ignore_ascii_case(locked) {
                let group = registry_groups.iter().find(|group| {
                    group.logical_name_id == logical_name_id
                        && group.completion_log_index < registration_log
                });
                let Some(group) = group else { continue };
                ("locked_wrapped", Some(group))
            } else {
                continue;
            };
        let mut evidence = transfers
            .iter()
            .map(|observation| observation_evidence(observation))
            .collect::<Vec<_>>();
        if let Some(group) = registry_group {
            evidence.extend(group.evidence.clone());
        }
        let registry_address = registration
            .raw_fact_ref
            .get("emitting_address")
            .and_then(Value::as_str)
            .context("migration registration has no registry emitter")?;
        let correlated_events = matching_events(output, &observations[0].raw, |event| {
            authority_transition_event(
                event,
                &logical_name_id,
                registry_address,
                sender,
                registration_log,
            )
        });
        let independently_admitted_transfer_events = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.source_family == V1_REGISTRAR_FAMILY
                    && transfers
                        .iter()
                        .any(|observation| same_position(event, &observation.raw))
            })
            .cloned()
            .collect::<Vec<_>>();
        let successor_registry_instance = registration
            .after_state
            .get("registry_contract_instance_id")
            .and_then(Value::as_str)
            .context("migration registration has no successor registry instance")?;
        let Some(successor_binding_event) = correlated_events.iter().find(|event| {
            event.source_family == "ens_v2_registry_l1"
                && event.event_kind == "SurfaceBound"
                && event.resource_id.is_some()
                && event
                    .after_state
                    .get("surface_binding_id")
                    .and_then(Value::as_str)
                    .is_some()
        }) else {
            continue;
        };
        let successor_resource = successor_binding_event
            .resource_id
            .expect("selected successor binding has a resource");
        let successor_binding =
            value_str(&successor_binding_event.after_state, "surface_binding_id")?;
        evidence.extend(correlated_events.iter().map(event_evidence));
        let id = correlation_id(TRANSITION_KIND, Some(&logical_name_id), &evidence);
        for event in correlated_events
            .iter()
            .chain(&independently_admitted_transfer_events)
        {
            associate_event(
                output,
                &event.event_identity,
                &id,
                TRANSITION_KIND,
                evidence.clone(),
            )?;
        }
        let registrar_cleanup_sender = match migration_path {
            "unwrapped" => Some(unlocked),
            "unlocked_wrapped" => Some(name_wrapper),
            _ => None,
        };
        let registrar_cleanup = registrar_cleanup_sender
            .and_then(|cleanup_sender| {
                transfers.iter().copied().find(|observation| {
                    observation.decoded["from"]
                        .as_str()
                        .is_some_and(|from| from.eq_ignore_ascii_case(cleanup_sender))
                })
            })
            .map(|observation| {
                let event_identity = independently_admitted_transfer_events
                    .iter()
                    .find(|event| {
                        event.event_kind == "TokenControlTransferred"
                            && same_position(event, &observation.raw)
                    })
                    .map(|event| event.event_identity.clone())
                    .unwrap_or_else(|| registrar_transfer_event_identity(observation));
                json!({
                    "event_identity":event_identity,
                    "source_event":"Transfer",
                    "block_number":observation.raw.block_number,
                    "transaction_index":observation.raw.transaction_index,
                    "log_index":observation.raw.log_index,
                })
            });
        let predecessor_resource = match migration_path {
            "unwrapped" | "unlocked_wrapped" => {
                let base_registrar_instance = catalog
                    .contract_instance_for_address(
                        base_registrar,
                        registration
                            .block_number
                            .context("migration registration has no block number")?,
                    )?
                    .context("ENSv1 BaseRegistrar has no active contract instance")?;
                json!({
                    "anchor_kind":"registrar_backed_registration",
                    "contract_instance_id":base_registrar_instance,
                    "token_id":labelhash,
                    "labelhash":labelhash,
                    "selection":"current_registrar_resource_immediately_before_predecessor_cleanup",
                })
            }
            "locked_wrapped" => json!({
                "anchor_kind":"wrapper_backed_control",
                "contract_address":name_wrapper,
                "wrapper_token_id":logical_name_id.split_once(':').map(|(_, hash)| hash),
                "namehash":logical_name_id.split_once(':').map(|(_, hash)| hash),
                "selection":"current_wrapper_resource_immediately_before_boundary",
            }),
            _ => unreachable!("migration path was selected above"),
        };
        let mut before = json!({
            "authority_epoch":"ens_v1",
            "logical_name_id":logical_name_id,
            "selection":if registrar_cleanup.is_some() {
                "active_immediately_before_predecessor_cleanup"
            } else {
                "active_immediately_before_boundary"
            },
            "resource":predecessor_resource,
        });
        if let Some(cleanup) = registrar_cleanup {
            before["predecessor_cleanup"] = cleanup;
        }
        let after = json!({
            "source_event":"MigrationApplied",
            "logical_name_id":logical_name_id,
            "namehash":logical_name_id.split_once(':').map(|(_, hash)| hash),
            "correlation_kind":TRANSITION_KIND,
            "migration_path":migration_path,
            "predecessor_binding":before,
            "successor_binding":{
                "authority_epoch":"ens_v2",
                "binding_id":successor_binding,
                "resource_id":successor_resource.to_string(),
            },
            "successor_registry_contract_instance_id":successor_registry_instance,
            "v2_registration_boundary":{
                "block_number":registration.block_number,
                "block_hash":registration.block_hash,
                "transaction_hash":registration.transaction_hash,
                "transaction_index":registration.transaction_index,
                "log_index":registration.log_index,
            },
            "stored_expiry":registration.after_state.get("expiry").cloned(),
            "evidence":evidence,
            "migration_correlation_ids":[id.clone()],
            "consumer_visibility":CANDIDATE,
            "candidate_authority_transition":true,
        });
        boundaries.push(boundary_event(
            migration_source,
            &registration,
            &id,
            before,
            after.clone(),
        )?);
        let (block_number, transaction_index, log_index) = required_position(&registration)?;
        output
            .migration_candidate_identity_effects
            .push(MigrationCandidateEffect {
                effect_identity: format!("migration-authority-transition:{id}"),
                migration_correlation_ids: vec![id],
                correlation_kind: TRANSITION_KIND.to_owned(),
                effect_kind: "surface_binding_transition".to_owned(),
                proposed_effect: after,
                evidence_refs: Value::Array(evidence),
                chain_id: registration.chain_id.clone(),
                block_number,
                block_hash: registration.block_hash.clone().expect("required position"),
                transaction_hash: registration
                    .transaction_hash
                    .clone()
                    .expect("required position"),
                transaction_index,
                log_index,
                canonicality_state: registration.canonicality_state.clone(),
                consumer_visibility: CANDIDATE.to_owned(),
            });
    }
    Ok(())
}
