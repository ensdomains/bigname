use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use serde_json::{Value, json};

use super::{
    BatchOutput, MigrationCandidateEffect, MigrationDiscoveryAssociation, NormalizedEvent,
    catalog::Catalog, protocol::MigrationObservation,
};

mod support;
#[cfg(any(test, feature = "test-activation"))]
pub use support::inject_activated_transition_for_test;
use support::*;

const MIGRATION_FAMILY: &str = "ens_v2_migration_l1";
const CANDIDATE: &str = "candidate";

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
    let base_registrar_instance = catalog
        .declared_contract_instance_for_role(MIGRATION_FAMILY, "ens_v1_base_registrar")
        .context("migration manifest has no ENSv1 BaseRegistrar contract instance")?;

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
    for transaction_observations in by_transaction.values() {
        correlate_renewals(transaction_observations, output)?;
        correlate_controllers(name_wrapper, transaction_observations, output)?;
        correlate_cleanups(transaction_observations, &graveyard, output)?;
        let registry_groups =
            correlate_registry_creation(catalog, transaction_observations, &locked, output)?;
        correlate_authority_transitions(
            migration_source,
            transaction_observations,
            &graveyard,
            &unlocked,
            &locked,
            base_registrar_instance,
            name_wrapper,
            &registry_groups,
            output,
            &mut boundaries,
        )?;
    }

    associate_restored_registry_effects(catalog, output)?;
    // Direct ENSv1→ENSv2 migration-family events that did not satisfy a complete supported
    // shape are not admitted as normalized facts. This keeps unrelated factory and registrar
    // logs out.
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
        if !["ExpiryChanged", "RegistrationRenewed"]
            .into_iter()
            .all(|kind| v2_events.iter().any(|event| event.event_kind == kind))
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
                    && observation.emitter_role.as_deref() == Some("ens_v1_base_registrar")
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
        if resources.len() != 1 {
            continue;
        }
        let successor_resource = resources.pop_first().expect("one renewal resource");
        let mut evidence = vec![observation_evidence(bridge), observation_evidence(base)];
        evidence.extend(v2_events.iter().map(event_evidence));
        let correlation_id =
            correlation_id("synchronized_renewal", Some(&logical_name_id), &evidence);
        mark_direct_position(output, &bridge.raw, &correlation_id);
        anchor_direct_position(output, &bridge.raw, successor_resource);
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
            matches!(
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
                && observation.emitter_role.as_deref() == Some("ens_v1_base_registrar")
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
            mark_direct_position(output, &removed.raw, &id);
            participating.insert((added.event_name.as_str(), added.raw.log_index));
            participating.insert((removed.event_name.as_str(), removed.raw.log_index));
        }
    }
    for observation in observations.iter().copied().filter(|observation| {
        matches!(
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

fn correlate_registry_creation(
    catalog: &Catalog,
    observations: &[&MigrationObservation],
    locked_controller: &str,
    output: &mut BatchOutput,
) -> anyhow::Result<Vec<RegistryGroup>> {
    let mut groups = Vec::new();
    for factory in observations.iter().copied().filter(|observation| {
        observation.event_name == "ProxyDeployed"
            && observation
                .decoded
                .get("sender")
                .and_then(Value::as_str)
                .is_some_and(|sender| sender.eq_ignore_ascii_case(locked_controller))
    }) {
        let proxy = value_str(&factory.decoded, "proxy_address")?.to_ascii_lowercase();
        let logical_name_id = format!("ens:{}", value_str(&factory.decoded, "salt")?);
        let Some(registry_event) = output
            .normalized_events
            .iter()
            .find(|event| {
                event.source_family == "ens_v2_registry_l1"
                    && event.event_kind == "RegistryCreated"
                    && same_transaction(event, &factory.raw)
                    && event
                        .log_index
                        .is_some_and(|index| index < factory.raw.log_index)
                    && event
                        .after_state
                        .get("registry")
                        .and_then(Value::as_str)
                        .is_some_and(|address| address.eq_ignore_ascii_case(&proxy))
            })
            .cloned()
        else {
            continue;
        };
        let registry_instance = registry_event
            .after_state
            .get("contract_instance_id")
            .and_then(Value::as_str)
            .context("RegistryCreated event has no contract instance")?
            .parse::<uuid::Uuid>()
            .context("RegistryCreated contract instance is malformed")?;
        let edge = output
            .discovery_edges
            .iter()
            .find(|edge| {
                edge.edge_kind == "registry_announcement"
                    && edge.to_contract_instance_id == registry_instance
                    && edge.active_from_block_hash
                        == registry_event.block_hash.as_deref().unwrap_or("")
            })
            .cloned()
            .context("RegistryCreated event has no ordinary registry-announcement edge")?;
        let evidence = vec![
            event_evidence(&registry_event),
            observation_evidence(factory),
        ];
        let id = correlation_id(
            "migration_registry_creation",
            Some(&logical_name_id),
            &evidence,
        );
        mark_direct_position(output, &factory.raw, &id);
        associate_event(
            output,
            &registry_event.event_identity,
            &id,
            "migration_registry_creation",
            evidence.clone(),
        )?;
        let source = catalog
            .source(edge.source_manifest_id)
            .context("registry-announcement edge has no active source manifest")?;
        output
            .migration_discovery_associations
            .push(MigrationDiscoveryAssociation {
                logical_edge_identity: logical_edge_identity(&edge, source)?,
                migration_correlation_id: id.clone(),
                registry_contract_instance_id: registry_instance,
                registry_address: proxy.clone(),
                source_manifest_id: edge.source_manifest_id,
                evidence_refs: Value::Array(evidence.clone()),
                chain_id: registry_event.chain_id.clone(),
                block_number: required_position(&registry_event)?.0,
                block_hash: registry_event
                    .block_hash
                    .clone()
                    .expect("required position"),
                transaction_hash: registry_event
                    .transaction_hash
                    .clone()
                    .expect("required position"),
                transaction_index: registry_event.transaction_index.expect("required position"),
                log_index: registry_event.log_index.expect("required position"),
                canonicality_state: registry_event.canonicality_state.clone(),
                consumer_visibility: CANDIDATE.to_owned(),
            });
        groups.push(RegistryGroup {
            correlation_id: id,
            logical_name_id,
            registry_address: proxy,
            evidence,
            completion_log_index: factory.raw.log_index,
        });
    }
    for group in &groups {
        let identities = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.source_family != MIGRATION_FAMILY
                    && event
                        .raw_fact_ref
                        .get("emitting_address")
                        .and_then(Value::as_str)
                        .is_some_and(|address| {
                            address.eq_ignore_ascii_case(&group.registry_address)
                        })
            })
            .map(|event| event.event_identity.clone())
            .collect::<Vec<_>>();
        for identity in identities {
            associate_event(
                output,
                &identity,
                &group.correlation_id,
                "migration_registry_creation",
                group.evidence.clone(),
            )?;
        }
    }
    Ok(groups)
}

#[allow(clippy::too_many_arguments)]
fn correlate_authority_transitions(
    migration_source: &super::manifest::ManifestSource,
    observations: &[&MigrationObservation],
    graveyard: &str,
    unlocked: &str,
    locked: &str,
    base_registrar_instance: uuid::Uuid,
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
        let id = correlation_id("authority_transition", Some(&logical_name_id), &evidence);
        for event in &correlated_events {
            associate_event(
                output,
                &event.event_identity,
                &id,
                "authority_transition",
                evidence.clone(),
            )?;
        }
        let predecessor_resource = match migration_path {
            "unwrapped" => json!({
                "anchor_kind":"registrar_backed_registration",
                "contract_instance_id":base_registrar_instance,
                "token_id":labelhash,
                "labelhash":labelhash,
                "selection":"current_registrar_resource_immediately_before_boundary",
            }),
            "unlocked_wrapped" | "locked_wrapped" => json!({
                "anchor_kind":"wrapper_backed_control",
                "contract_address":name_wrapper,
                "wrapper_token_id":logical_name_id.split_once(':').map(|(_, hash)| hash),
                "namehash":logical_name_id.split_once(':').map(|(_, hash)| hash),
                "selection":"current_wrapper_resource_immediately_before_boundary",
            }),
            _ => unreachable!("migration path was selected above"),
        };
        let before = json!({
            "authority_epoch":"ens_v1",
            "logical_name_id":logical_name_id,
            "selection":"active_immediately_before_boundary",
            "resource":predecessor_resource,
        });
        let after = json!({
            "source_event":"MigrationApplied",
            "logical_name_id":logical_name_id,
            "namehash":logical_name_id.split_once(':').map(|(_, hash)| hash),
            "correlation_kind":"authority_transition",
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
                correlation_kind: "authority_transition".to_owned(),
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
