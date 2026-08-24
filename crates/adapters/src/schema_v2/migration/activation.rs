use std::collections::BTreeSet;

use crate::schema_v2::{BatchOutput, MigrationAuthorityTransition};

const ACTIVATED: &str = "activated";
const CANDIDATE: &str = "candidate";
const AUTHORITY_TRANSITION: &str = "authority_transition";
const SURFACE_BINDING_TRANSITION: &str = "surface_binding_transition";

/// Materializes visibility only after every correlation path has finished assembling the batch.
/// The public test seam below deliberately calls this exact implementation again, which makes
/// production/seam byte equality an idempotence check instead of a second activation path.
pub(super) fn activate_complete_groups(output: &mut BatchOutput) {
    let authority_ids = authority_correlation_ids(output);
    let mut complete_ids = all_correlation_ids(output);
    for id in &authority_ids {
        complete_ids.remove(id);
    }

    let mut transitions = Vec::new();
    for id in authority_ids {
        if let Some(transition) = complete_authority_transition(output, &id) {
            complete_ids.insert(id);
            transitions.push(transition);
        }
    }
    transitions.sort_by(|left, right| {
        (
            &left.chain_id,
            left.block_number,
            left.transaction_index,
            left.log_index,
            &left.migration_correlation_id,
        )
            .cmp(&(
                &right.chain_id,
                right.block_number,
                right.transaction_index,
                right.log_index,
                &right.migration_correlation_id,
            ))
    });

    for event in &mut output.normalized_events {
        if !event.migration_correlation_ids.is_empty()
            && all_complete(&event.migration_correlation_ids, &complete_ids)
        {
            event.consumer_visibility = ACTIVATED.to_owned();
            if event.event_kind == "MigrationApplied" {
                event.after_state["consumer_visibility"] = ACTIVATED.into();
                event.after_state["candidate_authority_transition"] = false.into();
            }
        }
    }
    for association in &mut output.migration_event_associations {
        if complete_ids.contains(&association.migration_correlation_id) {
            association.consumer_visibility = ACTIVATED.to_owned();
        }
    }
    for association in &mut output.migration_discovery_associations {
        if complete_ids.contains(&association.migration_correlation_id) {
            association.consumer_visibility = ACTIVATED.to_owned();
        }
    }
    // Candidate-effect tables are diagnostic evidence and their storage contract is
    // candidate-only. Activated normalized rows and associations carry visibility; the effect
    // payload remains the deterministic source from which the transition is materialized.
    output.migration_authority_transitions = transitions;
}

fn authority_correlation_ids(output: &BatchOutput) -> BTreeSet<String> {
    output
        .migration_candidate_identity_effects
        .iter()
        .filter(|effect| effect.correlation_kind == AUTHORITY_TRANSITION)
        .flat_map(|effect| effect.migration_correlation_ids.iter().cloned())
        .chain(
            output
                .migration_event_associations
                .iter()
                .filter(|association| association.correlation_kind == AUTHORITY_TRANSITION)
                .map(|association| association.migration_correlation_id.clone()),
        )
        .chain(
            output
                .normalized_events
                .iter()
                .filter(|event| event.event_kind == "MigrationApplied")
                .flat_map(|event| event.migration_correlation_ids.iter().cloned()),
        )
        .collect()
}

fn all_correlation_ids(output: &BatchOutput) -> BTreeSet<String> {
    output
        .normalized_events
        .iter()
        .flat_map(|event| event.migration_correlation_ids.iter().cloned())
        .chain(
            output
                .migration_event_associations
                .iter()
                .map(|association| association.migration_correlation_id.clone()),
        )
        .chain(
            output
                .migration_discovery_associations
                .iter()
                .map(|association| association.migration_correlation_id.clone()),
        )
        .chain(
            output
                .migration_candidate_identity_effects
                .iter()
                .chain(&output.migration_candidate_discovery_effects)
                .flat_map(|effect| effect.migration_correlation_ids.iter().cloned()),
        )
        .collect()
}

fn complete_authority_transition(
    output: &BatchOutput,
    correlation_id: &str,
) -> Option<MigrationAuthorityTransition> {
    let effects = output
        .migration_candidate_identity_effects
        .iter()
        .filter(|effect| {
            effect.correlation_kind == AUTHORITY_TRANSITION
                && effect.effect_kind == SURFACE_BINDING_TRANSITION
                && effect.migration_correlation_ids == [correlation_id]
        })
        .collect::<Vec<_>>();
    let [effect] = effects.as_slice() else {
        return None;
    };
    let boundaries = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "MigrationApplied"
                && event.migration_correlation_ids == [correlation_id]
        })
        .collect::<Vec<_>>();
    let [boundary] = boundaries.as_slice() else {
        return None;
    };
    let mut candidate_boundary_payload = boundary.after_state.clone();
    if boundary.consumer_visibility == ACTIVATED {
        candidate_boundary_payload["consumer_visibility"] = CANDIDATE.into();
        candidate_boundary_payload["candidate_authority_transition"] = true.into();
    }
    if effect.proposed_effect != candidate_boundary_payload {
        return None;
    }

    let proposed = &effect.proposed_effect;
    let logical_name_id = proposed.get("logical_name_id")?.as_str()?;
    let predecessor = proposed.get("predecessor_binding")?;
    let successor = proposed.get("successor_binding")?;
    let successor_binding_id = successor.get("binding_id")?.as_str()?.parse().ok()?;
    let successor_resource_id = successor.get("resource_id")?.as_str()?.parse().ok()?;
    let successor_arm = successor.get("authority_epoch")?.as_str()?;
    let matching_successors = output
        .surface_bindings
        .iter()
        .filter(|binding| {
            binding.surface_binding_id == successor_binding_id
                && binding.resource_id == successor_resource_id
                && binding.logical_name_id == logical_name_id
                && binding.authority_arm == successor_arm
        })
        .count();
    if matching_successors != 1 {
        return None;
    }

    Some(MigrationAuthorityTransition {
        boundary_event_identity: boundary.event_identity.clone(),
        migration_correlation_id: correlation_id.to_owned(),
        logical_name_id: logical_name_id.to_owned(),
        predecessor_selector: predecessor.clone(),
        expected_predecessor_arm: predecessor.get("authority_epoch")?.as_str()?.to_owned(),
        successor_surface_binding_id: successor_binding_id,
        successor_resource_id,
        successor_arm: successor_arm.to_owned(),
        chain_id: effect.chain_id.clone(),
        block_number: effect.block_number,
        transaction_index: effect.transaction_index,
        log_index: effect.log_index,
    })
}

fn all_complete(ids: &[String], complete: &BTreeSet<String>) -> bool {
    ids.iter().all(|id| complete.contains(id))
}

#[cfg(any(test, feature = "test-activation"))]
pub fn inject_activated_transition_for_test(output: &mut BatchOutput) -> anyhow::Result<()> {
    activate_complete_groups(output);
    Ok(())
}
