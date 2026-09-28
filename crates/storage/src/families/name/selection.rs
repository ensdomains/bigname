//! The name authority selection over the owned key families, computed
//! at read for one name from its F1 binding candidates and history, its F2a retained lifecycle
//! events, its F2c registry node and the readable resources. It gives what the served row keeps
//! in `provenance.authority_selection` and the selected binding the row is bound to.
//!
//! Every "latest" here is the latest in the canonical event order (docs/glossary.md
//! #canonical-event-order) where the served statement orders by the generated event id, and the
//! binding order (block, transaction, log, binding id) where it orders binding candidates.
use std::collections::BTreeSet;

use anyhow::Result;
use serde_json::{Value, json};

use super::NameHistory;
use crate::families::control::{
    lifecycle::{AuthoritySelection, Clock, NameFacts, released_v2, staged_as_own},
    registry::{OwnerEvent, registry_generation},
    rows::{BindingCandidate, LifecycleEvent, family_arm},
};

const LIFECYCLE_KINDS: [&str; 3] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
];
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// The latest MigrationApplied of a name (F1 `project_name_state`), with its generated id read
/// back by identity: served history, which decides nothing about authority.
#[derive(Clone, Debug, Default)]
pub struct MigrationProof {
    pub event_identity: String,
    pub normalized_event_id: Option<i64>,
    pub transition_id: Option<String>,
}

/// What the selection of one name decided.
#[derive(Clone, Debug, Default)]
pub struct NameSelection {
    pub selection: AuthoritySelection,
    /// The selected binding.
    pub binding: Option<BindingCandidate>,
    /// The released ENSv2 tombstone's resource when ENSv2 is selected.
    pub released_v2_resource: Option<String>,
    /// The node's latest AuthorityTransferred when its owner getter is the zero address, the
    /// served `project_latest_registry_owner` row.
    pub ownerless_transfer: Option<OwnerEvent>,
    pub registry_generation: Option<&'static str>,
    pub registry_handoff_block_number: Option<i64>,
    pub proof: Option<MigrationProof>,
    /// The selected binding's position, `authority_epoch_start_position`.
    pub epoch_start: Value,
}

impl NameSelection {
    /// `provenance.authority_selection` as the served row strips its nulls.
    pub fn provenance(&self) -> Value {
        let selection = &self.selection;
        let mut out = serde_json::Map::new();
        let mut put = |key: &str, value: Value| {
            if !value.is_null() {
                out.insert(key.to_owned(), value);
            }
        };
        put("authority_arm", json!(selection.authority_arm));
        put("surface_binding_id", json!(selection.surface_binding_id));
        put("resource_id", json!(selection.resource_id));
        put("epoch_start_position", self.epoch_start.clone());
        if let Some(proof) = &self.proof {
            put(
                "proof_kind",
                json!(crate::MIGRATION_AUTHORITY_TRANSITION_PROOF_KIND),
            );
            put("proof_event_id", json!(proof.normalized_event_id));
            put("proof_event_identity", json!(proof.event_identity));
            put("transition_id", json!(proof.transition_id));
        }
        put("unsupported_reason", json!(selection.unsupported_reason));
        put("registry_generation", json!(self.registry_generation));
        put(
            "registry_handoff_block_number",
            json!(self.registry_handoff_block_number),
        );
        if selection.ownerless_registry {
            put("ownerless_registry", json!(true));
        }
        let mut context = serde_json::Map::new();
        if let Some(arm) = &selection.authority_arm {
            context.insert("authority_arm".into(), json!(arm));
        }
        if let Some(binding) = &self.binding {
            if let Some(kind) = &binding.binding_kind {
                context.insert("binding_kind".into(), json!(kind));
            }
            context.insert("resource_id".into(), json!(binding.resource_id));
            context.insert(
                "surface_binding_id".into(),
                json!(binding.surface_binding_id),
            );
        }
        if selection.released_tombstone {
            context.insert("released_tombstone".into(), json!("ens_v1"));
        }
        put("resource_authority_context", Value::Object(context));
        Value::Object(out)
    }
}

/// The binding order key with a missing transaction or log read as -1.
fn bound(candidate: &BindingCandidate) -> (i64, i64, i64) {
    let (block, transaction, log, _) = candidate.order();
    (block, transaction, log)
}

fn latest_binding<'a>(
    candidates: impl Iterator<Item = &'a BindingCandidate>,
) -> Option<&'a BindingCandidate> {
    candidates.max_by(|left, right| left.order().cmp(&right.order()))
}

/// The name's latest ENSv1 lifecycle fact.
fn latest_v1_lifecycle(facts: &NameFacts) -> Option<&LifecycleEvent> {
    facts
        .events
        .iter()
        .filter(|event| {
            event.resource_id.is_some()
                && event.source_family.starts_with("ens_v1_")
                && LIFECYCLE_KINDS.contains(&event.event_kind.as_str())
                && staged_as_own(facts, event)
        })
        .max_by(|left, right| facts.order.lateral(&left.position, &right.position))
}

/// Whether a NameWrapper candidate stands for the released lease `lease`:
/// its wrap recorded the lease, or a named grant of the lease shares the wrap's transaction.
fn wrapper_stands_for(facts: &NameFacts, candidate: &BindingCandidate, lease: &str) -> bool {
    candidate.is_wrapper()
        && (candidate.wrapped_registrar_resource_id.as_deref() == Some(lease)
            || facts.events.iter().any(|grant| {
                grant.resource_id.as_deref() == Some(lease)
                    && grant.source_family == "ens_v1_registrar_l1"
                    && grant.event_kind == "RegistrationGranted"
                    && grant.transaction_hash.is_some()
                    && grant.transaction_hash == candidate.transaction_hash
                    && staged_as_own(facts, grant)
            }))
}

/// The binding standing for a released ENSv1 lease that was not revived, none when the latest
/// ENSv1 lifecycle fact is not such a release.
fn released_v1_binding<'a>(
    facts: &'a NameFacts,
    open: &[&'a BindingCandidate],
    ownerless: bool,
) -> Option<&'a BindingCandidate> {
    let lifecycle = latest_v1_lifecycle(facts)?;
    if lifecycle.event_kind != "RegistrationReleased" || ownerless {
        return None;
    }
    let lease = lifecycle.resource_id.as_deref()?;
    let at = lifecycle.position.bound();
    let binding = latest_binding(facts.candidates.iter().filter(|candidate| {
        candidate.authority_arm == "ens_v1"
            && (candidate.resource_id == lease || wrapper_stands_for(facts, candidate, lease))
            && bound(candidate) <= at
    }));
    // The registry-only binding a transfer without `reclaim` opened, when it is the name's only
    // open binding and its lease lapsed after it opened.
    let handoff = match open {
        [only] if only.registry_only && only.authority_arm == "ens_v1" => Some(*only),
        _ => None,
    }
    .filter(|handoff| {
        handoff.lease_resource_id.as_deref() == Some(lease)
            && lifecycle.source_family == "ens_v1_registrar_l1"
            && lifecycle.authority_kind == "registrar"
            && bound(handoff) < at
    });
    handoff.or(binding.filter(|_| open.is_empty()))
}

/// The arms the name's authority events vote: the stored votes plus an ENSv2 root or registry
/// release beside a matching ENSv2 binding at or before it.
fn event_arms(facts: &NameFacts, history: Option<&NameHistory>) -> BTreeSet<String> {
    let mut arms: BTreeSet<String> = history
        .map(|history| history.event_arms.iter().cloned().collect())
        .unwrap_or_default();
    let name = facts.input.logical_name_id.as_str();
    let voting_release = facts.events.iter().any(|event| {
        event.event_kind == "RegistrationReleased"
            && matches!(
                event.source_family.as_str(),
                "ens_v2_root_l1" | "ens_v2_registry_l1"
            )
            && event.original_logical_name_id.as_deref() == Some(name)
            && facts.candidates.iter().any(|binding| {
                binding.authority_arm == "ens_v2"
                    && event.resource_id.as_deref() == Some(binding.resource_id.as_str())
                    && bound(binding) <= event.position.bound()
            })
    });
    if voting_release {
        arms.insert("ens_v2".into());
    }
    // An unnamed `.eth` registrar row the staging passes give the name votes as the name's
    // own.
    if facts.events.iter().any(|event| {
        event.original_logical_name_id.is_none()
            && event.source_family == "ens_v1_registrar_l1"
            && staged_as_own(facts, event)
    }) {
        arms.insert("ens_v1".into());
    }
    arms
}

/// The one arm of a set, none when it holds zero or several.
fn sole(arms: &BTreeSet<String>) -> Option<String> {
    (arms.len() == 1)
        .then(|| arms.iter().next().cloned())
        .flatten()
}

/// Select the authority of one name. `facts` must be loaded with an empty input selection: the
/// loader reads every candidate's resource, so the selected one's events are among them.
pub fn select(
    facts: &NameFacts,
    clock: &Clock,
    history: Option<&NameHistory>,
    proof: Option<MigrationProof>,
    readable_resources: &BTreeSet<String>,
) -> Result<NameSelection> {
    let open: Vec<&BindingCandidate> = facts
        .candidates
        .iter()
        .filter(|candidate| candidate.open_at(clock.timestamp_seconds))
        .collect();
    let open_arms: BTreeSet<String> = open
        .iter()
        .map(|candidate| candidate.authority_arm.clone())
        .collect();
    let voted = event_arms(facts, history);
    let released = released_v2(facts, clock)?;
    let latest_transfer = facts
        .registry_node
        .as_ref()
        .and_then(|node| node.latest_transfer())
        .filter(|transfer| transfer.owner_getter.as_deref() == Some(ZERO_ADDRESS))
        .cloned();
    let released_v1 = released_v1_binding(facts, &open, latest_transfer.is_some());

    let arm = if open_arms.contains("ens_v2") || released.is_some() {
        Some("ens_v2".to_owned())
    } else if open_arms.len() == 1 {
        sole(&open_arms)
    } else if open.is_empty() && voted.contains("ens_v1") {
        Some("ens_v1".to_owned())
    } else if open.is_empty() {
        sole(&voted)
    } else {
        None
    };
    let released_resource = released.as_ref().map(|(resource, _)| resource.as_str());
    let binding = arm.as_deref().and_then(|arm| {
        latest_binding(facts.candidates.iter().filter(|candidate| {
            candidate.authority_arm == arm
                && readable_resources.contains(&candidate.resource_id)
                && released_resource.is_none_or(|resource| candidate.resource_id == resource)
                && (released_resource.is_some()
                    || released_v1.is_some_and(|released| {
                        released.surface_binding_id == candidate.surface_binding_id
                    })
                    || candidate.open_at(clock.timestamp_seconds))
        }))
    });
    let ownerless =
        latest_transfer.is_some() && binding.is_none() && arm.as_deref() != Some("ens_v2");
    let released_tombstone = released_v1.is_some_and(|released| {
        binding.is_some_and(|binding| binding.surface_binding_id == released.surface_binding_id)
    });
    let unsupported_reason = if ownerless {
        None
    } else if binding.is_none() {
        Some("current_authority_not_projected".to_owned())
    } else {
        None
    };
    let (generation, handoff) = registry_generation(facts.registry_node.as_ref(), arm.as_deref());
    let epoch_start = binding.map_or(json!({}), |binding| {
        let mut position = serde_json::Map::new();
        position.insert("block_number".into(), json!(binding.block_number));
        if let Some(transaction) = binding.transaction_index {
            position.insert("transaction_index".into(), json!(transaction));
        }
        if let Some(log) = binding.log_index {
            position.insert("log_index".into(), json!(log));
        }
        Value::Object(position)
    });
    let selection = AuthoritySelection {
        authority_arm: arm.clone(),
        surface_binding_id: binding.map(|binding| binding.surface_binding_id.clone()),
        resource_id: binding.map(|binding| binding.resource_id.clone()),
        epoch_start: binding.map(bound),
        has_proof: proof.is_some(),
        unsupported_reason,
        ownerless_registry: ownerless,
        released_tombstone,
    };
    Ok(NameSelection {
        selection,
        binding: binding.cloned(),
        released_v2_resource: released
            .filter(|_| arm.as_deref() == Some("ens_v2"))
            .map(|(resource, _)| resource),
        ownerless_transfer: latest_transfer,
        registry_generation: generation,
        registry_handoff_block_number: handoff,
        proof,
        epoch_start,
    })
}

/// The arm an event's family belongs to, for the coverage and resolver rules.
pub(super) fn arm_of(source_family: &str) -> Option<&'static str> {
    family_arm(source_family)
}
