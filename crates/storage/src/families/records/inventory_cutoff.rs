//! The version cutoff and the per-key winner of one resource's record inventory: the combined
//! boundary, which writes it leaves eligible, the latest eligible write per record key, and the
//! served half of a coin-60 pair.
use std::collections::{BTreeMap, HashMap};

use super::{FamilyPosition, assemble::ServedRecord, facts::ProbedEvent, rows::RecordCandidate};

/// A version boundary candidate: its position, its event kind and, for a link, its event id.
pub(super) type Boundary = (FamilyPosition, &'static str, Option<i64>);

/// The combined version boundary, the latest partition version event or selected link in the
/// canonical event order, and the cutoff it sets: only an ordinary `RecordVersionChanged` cuts
/// off the writes before it.
pub(super) fn combined_boundary(
    boundaries: Vec<Boundary>,
) -> (Option<Boundary>, Option<FamilyPosition>) {
    let boundary = boundaries.into_iter().max_by(|a, b| a.0.cmp(&b.0));
    let cutoff = boundary
        .as_ref()
        .filter(|(_, kind, _)| *kind == "RecordVersionChanged")
        .map(|(position, _, _)| position.clone());
    (boundary, cutoff)
}

/// Whether a write at `position` is after the cutoff, when there is one.
pub(super) fn eligible(cutoff: Option<&FamilyPosition>, position: &FamilyPosition) -> bool {
    cutoff.is_none_or(|cut| position > cut)
}

/// The latest eligible write per record key across the union, in the canonical event order.
pub(super) fn latest_eligible(
    candidates: Vec<RecordCandidate>,
    cutoff: Option<&FamilyPosition>,
) -> BTreeMap<String, RecordCandidate> {
    let mut winners: BTreeMap<String, RecordCandidate> = BTreeMap::new();
    for candidate in candidates
        .into_iter()
        .filter(|candidate| eligible(cutoff, &candidate.position))
    {
        match winners.get(&candidate.record_key) {
            Some(current) if current.position >= candidate.position => {}
            _ => {
                winners.insert(candidate.record_key.clone(), candidate);
            }
        }
    }
    winners
}

/// The served record of each winner: the `AddressChanged` half of an eligible coin-60 pair, read
/// back from its event, else the winner's own row.
pub(super) fn served_records(
    winners: BTreeMap<String, RecordCandidate>,
    cutoff: Option<&FamilyPosition>,
    probed: &HashMap<String, ProbedEvent>,
) -> Vec<ServedRecord> {
    let mut served = Vec::new();
    for winner in winners.into_values() {
        let sibling = winner
            .pair_sibling()
            .filter(|sibling| eligible(cutoff, sibling))
            .and_then(|sibling| {
                probed
                    .get(&sibling.event_identity)
                    .map(|event| (sibling.clone(), event))
            });
        match sibling {
            Some((sibling, event)) => served.push(ServedRecord {
                record_key: winner.record_key,
                position: sibling,
                normalized_event_id: Some(event.normalized_event_id),
                source_family: event.source_family.clone(),
                stored_status: None,
                payload: event.after_state.clone(),
            }),
            None => served.push(ServedRecord {
                record_key: winner.record_key,
                position: winner.position,
                normalized_event_id: winner.normalized_event_id,
                source_family: winner.source_family,
                stored_status: Some(winner.status),
                payload: winner.payload,
            }),
        }
    }
    served
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;
