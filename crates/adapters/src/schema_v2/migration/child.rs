//! Direct-child ENSv1→ENSv2 migration correlation through a parent `WrapperRegistry`.
//!
//! A child never reaches a migration controller: the batch helper routes child groups to the
//! already-migrated parent's own registry, which inherits the wrapper receiver and registers the
//! child into itself.
//! (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L124 @ ens_v2_sepolia_20260629@ccaeb58)
//! (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L32 @ ens_v2@a971bd64)
//! Correlation is therefore per child registration, never per transaction: one transaction may
//! carry a parent migration and several children.

use std::collections::BTreeMap;

use alloy_primitives::{B256, keccak256};
use anyhow::Context;
use serde_json::{Value, json};

use super::registry;
use super::support::{
    RegistryGroup, associate_event, authority_transition_event, boundary_event, correlation_id,
    event_evidence, required_position, value_str,
};
use super::{CANDIDATE, TRANSITION_KIND};
use crate::schema_v2::{
    BatchOutput, MigrationCandidateEffect, NormalizedEvent, catalog::Catalog,
    manifest::ManifestSource, protocol::MigrationObservation,
};

const REGISTRY_FAMILY: &str = "ens_v2_registry_l1";

/// One registry proven to have been created by an ENSv1→ENSv2 migration, with the namehash of the
/// name it holds children for. That namehash is the factory CREATE2 salt, so the parent's own
/// migration evidence names the parent without any `.eth` assumption.
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L151 @ ens_v2_sepolia_20260629@ccaeb58)
struct MigrationRegistry {
    correlation_id: String,
    evidence: Vec<Value>,
    namehash: String,
    position: (i64, i64, i64),
}

#[allow(clippy::too_many_arguments)]
pub(super) fn correlate_children(
    catalog: &Catalog,
    migration_source: &ManifestSource,
    observations: &[MigrationObservation],
    groups: &[RegistryGroup],
    name_wrapper: &str,
    graveyard: &str,
    output: &mut BatchOutput,
    boundaries: &mut Vec<NormalizedEvent>,
) -> anyhow::Result<()> {
    let mut known = BTreeMap::new();
    for group in groups {
        remember(&mut known, group);
    }
    admit_child_registries(catalog, observations, &mut known, output)?;
    derive_boundaries(
        catalog,
        migration_source,
        name_wrapper,
        graveyard,
        &known,
        output,
        boundaries,
    )
}

fn remember(known: &mut BTreeMap<String, MigrationRegistry>, group: &RegistryGroup) {
    let Some((_, namehash)) = group.logical_name_id.split_once(':') else {
        return;
    };
    known.insert(
        group.registry_address.to_ascii_lowercase(),
        MigrationRegistry {
            correlation_id: group.correlation_id.clone(),
            evidence: group.evidence.clone(),
            namehash: namehash.to_owned(),
            position: group.completion_position,
        },
    );
}

/// Admits the registry a locked child receives, which its parent registry — not a controller —
/// deploys. Observations arrive in position order, so a registry admitted here is available to
/// every later log, including deeper levels of the same chain.
fn admit_child_registries(
    catalog: &Catalog,
    observations: &[MigrationObservation],
    known: &mut BTreeMap<String, MigrationRegistry>,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for observation in observations {
        let Some(sender) = registry::proxy_sender(observation).map(str::to_ascii_lowercase) else {
            continue;
        };
        if !known.contains_key(&sender)
            && let Some(restored) =
                restored_registry(catalog, &sender, observation.raw.block_number)
        {
            known.insert(sender.clone(), restored);
        }
        let at = position(observation);
        if known
            .get(&sender)
            .is_none_or(|parent| parent.position >= at)
        {
            continue;
        }
        let Some(group) = registry::admit(catalog, observation, output)? else {
            continue;
        };
        registry::back_associate(std::slice::from_ref(&group), output)?;
        remember(known, &group);
    }
    Ok(())
}

/// Recovers a registry admitted in an earlier batch from the association evidence its restored
/// admission carries. This is the only cross-batch path, so a split replay derives the same
/// parent, correlation ID, and namehash a single pass does.
///
/// The recovered evidence reaches a correlation identity, so its serialization has to be stable
/// across the round trip through stored admission state. `correlation_id` hashes each evidence
/// entry's rendered JSON, which is key-ordered only because `serde_json` orders object keys; a
/// build that enabled insertion-ordered maps would let a restored parent hash differently from a
/// same-batch one.
fn restored_registry(
    catalog: &Catalog,
    address: &str,
    block_number: i64,
) -> Option<MigrationRegistry> {
    catalog
        .migration_registry_correlations(address, block_number)
        .into_iter()
        // Correlations arrive ordered by ID, so picking the first is deterministic; an address
        // carrying two migration correlations would be a discovery defect, not a choice to make.
        .find_map(|correlation| {
            let factory = correlation.evidence.iter().find(|entry| {
                entry.get("kind").and_then(Value::as_str) == Some("raw_log")
                    && entry.get("event").and_then(Value::as_str) == Some("ProxyDeployed")
            })?;
            Some(MigrationRegistry {
                namehash: factory.get("decoded")?.get("salt")?.as_str()?.to_owned(),
                position: (
                    factory.get("block_number")?.as_i64()?,
                    factory.get("transaction_index")?.as_i64()?,
                    factory.get("log_index")?.as_i64()?,
                ),
                correlation_id: correlation.id,
                evidence: correlation.evidence,
            })
        })
}

#[allow(clippy::too_many_arguments)]
fn derive_boundaries(
    catalog: &Catalog,
    migration_source: &ManifestSource,
    name_wrapper: &str,
    graveyard: &str,
    known: &BTreeMap<String, MigrationRegistry>,
    output: &mut BatchOutput,
    boundaries: &mut Vec<NormalizedEvent>,
) -> anyhow::Result<()> {
    let registrations = output
        .normalized_events
        .iter()
        .filter(|event| receiver_registered_itself(event))
        .cloned()
        .collect::<Vec<_>>();
    for registration in registrations {
        let emitter =
            value_str(&registration.raw_fact_ref, "emitting_address")?.to_ascii_lowercase();
        let at = required_position(&registration)?;
        let restored;
        let parent = match known.get(&emitter) {
            Some(parent) => parent,
            None => {
                restored = restored_registry(catalog, &emitter, at.0);
                let Some(parent) = restored.as_ref() else {
                    continue;
                };
                parent
            }
        };
        if parent.position >= at {
            continue;
        }
        let labelhash = value_str(&registration.after_state, "labelhash")?;
        let namehash = child_namehash(&parent.namehash, labelhash)?;
        let logical_name_id = format!("{}:{namehash}", migration_source.namespace);
        // The child's ENSv1 identity is derived from the parent's own migration evidence and the
        // registered labelhash. A registry topology that resolves the label to a different name
        // means the evidence chain is incomplete, and no boundary is provable.
        if registration.logical_name_id.as_deref() != Some(logical_name_id.as_str()) {
            continue;
        }
        // A boundary asserts that ENSv1 authority ended. Only the child's own ENSv1 cleanup shows
        // that: the receiver parks a locked child's wrapper token in the Graveyard, and unwraps an
        // emancipated child into it. Without that evidence the self-claim is an ordinary ENSv2
        // registration, whatever its sender.
        // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58)
        // (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64)
        let Some((cleanup_kind, cleanup)) = v1_cleanup(
            output,
            &registration,
            at,
            &namehash,
            name_wrapper,
            graveyard,
        ) else {
            continue;
        };
        let child_registry = known
            .values()
            .find(|candidate| candidate.namehash == namehash && candidate.position < at);
        // Each branch upstream performs its own cleanup and its own registry handling together,
        // and there is no third branch, so the two halves must agree. Classifying by registry
        // presence alone would relabel a locked child whose registry evidence is missing as an
        // emancipated one, inventing a shape the chain does not show.
        // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58)
        // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
        // (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64)
        // (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L190 @ ens_v2@a971bd64)
        let migration_path = match (cleanup_kind, child_registry.is_some()) {
            (Cleanup::Parked, true) => "locked_child",
            (Cleanup::Unwrapped, false) => "emancipated_child",
            _ => continue,
        };
        let mut evidence = parent.evidence.clone();
        if let Some(child_registry) = child_registry {
            evidence.extend(child_registry.evidence.clone());
        }
        evidence.push(event_evidence(&cleanup));
        let correlated = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.block_hash == registration.block_hash
                    && event.transaction_hash == registration.transaction_hash
                    && authority_transition_event(event, &logical_name_id, &emitter, &emitter, at.2)
            })
            .cloned()
            .collect::<Vec<_>>();
        let Some(binding) = correlated.iter().find(|event| {
            event.source_family == REGISTRY_FAMILY
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
        let successor_resource = binding
            .resource_id
            .expect("selected successor binding has a resource");
        let successor_binding = value_str(&binding.after_state, "surface_binding_id")?;
        let successor_registry_instance =
            value_str(&registration.after_state, "registry_contract_instance_id")?;
        evidence.extend(correlated.iter().map(event_evidence));
        let id = correlation_id(TRANSITION_KIND, Some(&logical_name_id), &evidence);
        for event in &correlated {
            associate_event(
                output,
                &event.event_identity,
                &id,
                TRANSITION_KIND,
                evidence.clone(),
            )?;
        }
        // A child's ENSv1 authority ends at its cleanup, which precedes the registration in the
        // same transaction. The emancipated branch's unwrap closes the child's wrapper binding
        // there, so nothing is open at the registration's own position; selecting the predecessor
        // relative to the boundary would name a binding that no longer exists. The locked branch
        // only moves the token's owner and closes nothing, and resolves to the same binding either
        // way, so both shapes record the cleanup and select against it.
        let cleanup_at = required_position(&cleanup)?;
        let before = json!({
            "authority_epoch":"ens_v1",
            "logical_name_id":logical_name_id,
            "selection":"active_immediately_before_predecessor_cleanup",
            "predecessor_cleanup":{
                "event_identity":cleanup.event_identity,
                "source_event":cleanup.after_state.get("source_event"),
                "block_number":cleanup_at.0,
                "transaction_index":cleanup_at.1,
                "log_index":cleanup_at.2,
            },
            "resource":{
                "anchor_kind":"wrapper_backed_child_control",
                "contract_address":name_wrapper,
                "wrapper_token_id":namehash,
                "namehash":namehash,
                "parent_namehash":parent.namehash,
                "labelhash":labelhash,
                "parent_migration_correlation_id":parent.correlation_id,
                "selection":"current_wrapper_resource_immediately_before_predecessor_cleanup",
            },
        });
        let after = json!({
            "source_event":"MigrationApplied",
            "logical_name_id":logical_name_id,
            "namehash":namehash,
            "correlation_kind":TRANSITION_KIND,
            "migration_path":migration_path,
            "predecessor_binding":before,
            "successor_binding":{
                "authority_epoch":"ens_v2",
                "binding_id":successor_binding,
                "resource_id":successor_resource.to_string(),
            },
            "successor_registry_contract_instance_id":successor_registry_instance,
            "parent_migration_registry":{
                "address":emitter,
                "namehash":parent.namehash,
                "migration_correlation_id":parent.correlation_id,
            },
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
                block_number: at.0,
                block_hash: registration.block_hash.clone().expect("required position"),
                transaction_hash: registration
                    .transaction_hash
                    .clone()
                    .expect("required position"),
                transaction_index: at.1,
                log_index: at.2,
                canonicality_state: registration.canonicality_state.clone(),
                consumer_visibility: CANDIDATE.to_owned(),
            });
    }
    Ok(())
}

/// A direct-child migration is the receiving registry registering the child into itself: the
/// wrapper receiver re-enters through an external self-call restricted to itself, so
/// `LabelRegistered` reports the emitting registry as its sender. A parent owner registering an
/// unprotected child reports an ordinary account instead, and is not migration evidence.
/// (upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L149 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L167 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64)
fn receiver_registered_itself(event: &NormalizedEvent) -> bool {
    if event.source_family != REGISTRY_FAMILY
        || event.event_kind != "RegistrationGranted"
        || event
            .after_state
            .get("source_event")
            .and_then(Value::as_str)
            != Some("LabelRegistered")
        || event
            .after_state
            .get("resource_pending")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return false;
    }
    let sender = event.after_state.get("sender").and_then(Value::as_str);
    let emitter = event
        .raw_fact_ref
        .get("emitting_address")
        .and_then(Value::as_str);
    matches!((sender, emitter), (Some(sender), Some(emitter)) if sender.eq_ignore_ascii_case(emitter))
}

/// The child's ENSv1 cleanup earlier in the registration's own transaction: its wrapper token
/// transferred to the Graveyard, or its node unwrapped into the Graveyard. Both are emitted by the
/// ENSv1 NameWrapper the migration manifest declares, so the evidence is manifest-anchored rather
/// than inferred from whichever ENSv1 family a deployment happens to admit.
///
/// The receiver retires the ENSv1 name and only then injects the successor label, so a cleanup at
/// or after the registration is not that sequence and proves nothing about it
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58)
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2_sepolia_20260629@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@a971bd64).
fn v1_cleanup(
    output: &BatchOutput,
    registration: &NormalizedEvent,
    at: (i64, i64, i64),
    namehash: &str,
    name_wrapper: &str,
    graveyard: &str,
) -> Option<(Cleanup, NormalizedEvent)> {
    output.normalized_events.iter().find_map(|event| {
        if event.block_hash != registration.block_hash
            || event.transaction_hash != registration.transaction_hash
            || required_position(event).ok()? >= at
            || !event
                .raw_fact_ref
                .get("emitting_address")
                .and_then(Value::as_str)
                .is_some_and(|address| address.eq_ignore_ascii_case(name_wrapper))
        {
            return None;
        }
        retired_into(event, namehash, graveyard).map(|kind| (kind, event.clone()))
    })
}

/// How a child's ENSv1 control ended, which upstream pairs with whether that child receives a
/// registry of its own.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cleanup {
    /// The wrapper token parked in the Graveyard, still wrapped — the locked branch.
    Parked,
    /// The node unwrapped into the Graveyard — the emancipated branch.
    Unwrapped,
}

fn retired_into(event: &NormalizedEvent, namehash: &str, graveyard: &str) -> Option<Cleanup> {
    let field = |key: &str| event.after_state.get(key).and_then(Value::as_str);
    let into_graveyard =
        |key: &str| field(key).is_some_and(|address| address.eq_ignore_ascii_case(graveyard));
    if event.event_kind == "TokenControlTransferred"
        && field("namehash") == Some(namehash)
        && into_graveyard("to")
    {
        return Some(Cleanup::Parked);
    }
    if field("source_event") == Some("NameUnwrapped")
        && field("node") == Some(namehash)
        && into_graveyard("owner")
    {
        return Some(Cleanup::Unwrapped);
    }
    None
}

fn child_namehash(parent_namehash: &str, labelhash: &str) -> anyhow::Result<String> {
    let parent = parent_namehash
        .parse::<B256>()
        .context("parent migration registry namehash is malformed")?;
    let label = labelhash
        .parse::<B256>()
        .context("child registration labelhash is malformed")?;
    let mut input = [0_u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(label.as_slice());
    Ok(format!("{:#x}", keccak256(input)))
}

fn position(observation: &MigrationObservation) -> (i64, i64, i64) {
    (
        observation.raw.block_number,
        observation.raw.transaction_index,
        observation.raw.log_index,
    )
}
