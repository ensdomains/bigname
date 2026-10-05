use super::{
    event_index::{EventFields, EventIndex, Position, Registration},
    refresh_interpreter_state_key,
};
use crate::schema_v2::{model::BatchOutput, seam::NAME_IDENTITY_OBSERVED_KEY};

pub(super) fn name_registration(
    output: &mut BatchOutput,
    events: &EventIndex,
    registration: &mut Registration,
) {
    if registration.surface_known
        || !events
            .by_target
            .get(&registration.key)
            .is_some_and(|indices| {
                indices.iter().any(|index| {
                    let state = &output.normalized_events[*index].after_state;
                    state[NAME_IDENTITY_OBSERVED_KEY] == true && state["surface_known"] == true
                })
            })
    {
        return;
    }
    registration.surface_known = true;
    let grant = &output.normalized_events[registration.event_index];
    let block_hash = grant.block_hash.clone();
    let transaction_hash = grant.transaction_hash.clone();
    for index in events
        .by_resource
        .get(&registration.resource_id)
        .into_iter()
        .flatten()
    {
        let event = &mut output.normalized_events[*index];
        if event.block_hash == block_hash && event.transaction_hash == transaction_hash {
            event.logical_name_id = Some(registration.logical_name_id.clone());
            event.after_state["surface_known"] = true.into();
            refresh_interpreter_state_key(event);
        }
    }
}

/// A structural observation can first name an already-created registrar resource later in the
/// transaction. Its binding and closure are the materialization, not a redundant successor.
pub(super) fn materialization_positions(
    events: &EventIndex,
    registration: &Registration,
) -> std::collections::BTreeSet<Position> {
    events
        .by_resource
        .get(&registration.resource_id)
        .into_iter()
        .flatten()
        .filter_map(|index| {
            let fields = &events.fields[*index];
            fields
                .surface_materialization
                .then_some(fields.position?)
                .filter(|position| {
                    position.0 == registration.position.0 && position.1 == registration.position.1
                })
        })
        .collect()
}

/// Naming an existing resource can replay its resolver at the same position. Any such
/// materialization keeps its binding; other successor epochs of the registration are redundant.
pub(super) fn redundant_successor_positions(
    events: &EventIndex,
    candidates: &[usize],
    registration: &Registration,
    materializations: &std::collections::BTreeSet<Position>,
    eligible: impl Fn(&EventFields) -> bool,
) -> std::collections::BTreeSet<Position> {
    candidates
        .iter()
        .filter_map(|index| {
            let fields = &events.fields[*index];
            (fields.resource_id == Some(registration.resource_id)
                && !fields.surface_materialization
                && fields.position.is_some_and(|position| {
                    position > registration.position
                        && eligible(fields)
                        && !materializations.contains(&position)
                }))
            .then_some(fields.position?)
        })
        .collect()
}
