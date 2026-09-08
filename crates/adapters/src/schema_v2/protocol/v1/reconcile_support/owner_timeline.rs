use std::collections::BTreeSet;

use super::event_index::{EventIndex, Position, Registration, SourceEvent, SourceFamily};
use crate::schema_v2::model::BatchOutput;
use uuid::Uuid;

pub(super) struct OwnerTimeline {
    registrar_transfers: Vec<(Position, String)>,
    divergence_start: Option<Position>,
    confirmed_registry_positions: BTreeSet<Position>,
    reconciled_transfer_positions: BTreeSet<Position>,
}
impl OwnerTimeline {
    pub(super) fn new(
        events: &EventIndex,
        target_candidates: &[usize],
        registration: &Registration,
    ) -> Self {
        let mut registrar_transfers = target_candidates
            .iter()
            .filter_map(|index| {
                let fields = &events.fields[*index];
                (fields.source_event == SourceEvent::Transfer
                    && fields.family == SourceFamily::Other
                    && fields.resource_id == Some(registration.resource_id))
                .then_some((fields.position?, fields.owner.clone()?))
            })
            .collect::<Vec<_>>();
        registrar_transfers.sort_by_key(|(position, _)| *position);
        let registry_owners = target_candidates
            .iter()
            .filter_map(|index| {
                let fields = &events.fields[*index];
                (fields.family == SourceFamily::Registry
                    && matches!(
                        fields.source_event,
                        SourceEvent::NewOwner | SourceEvent::Transfer
                    ))
                .then_some((fields.position?, fields.owner.clone()?))
            })
            .collect::<Vec<_>>();
        #[rustfmt::skip]
        let confirmed_registry_positions = confirmed_registry_positions(events, target_candidates, &registrar_transfers, &registry_owners);
        let divergence_start = registry_owners
            .iter()
            .filter(|(position, _)| position > &registration.position)
            .filter(|(position, owner)| {
                let current_owner = registrar_transfers
                    .iter()
                    .rev()
                    .find(|(transfer_position, _)| transfer_position < position)
                    .map(|(_, owner)| owner)
                    .unwrap_or(&registration.provisional_owner);
                owner != current_owner && !confirmed_registry_positions.contains(position)
            })
            .map(|(position, _)| *position)
            .min();
        let reconciled_transfer_positions = registrar_transfers
            .iter()
            .filter(|(position, _)| divergence_start.is_none_or(|start| *position < start))
            .filter(|(position, owner)| {
                let matches_prior_setup = registry_owner_before(&registry_owners, *position)
                    .filter(|(_, registry_owner)| registry_owner == owner)
                    .is_some_and(|(registry_position, _)| {
                        !registrar_transfers.iter().any(
                            |(intervening_position, intervening_owner)| {
                                registry_position < intervening_position
                                    && intervening_position < position
                                    && intervening_owner != owner
                            },
                        )
                    });
                matches_prior_setup
                    || registry_owner_after(&registry_owners, *position)
                        .is_some_and(|(_, registry_owner)| registry_owner == owner)
            })
            .map(|(position, _)| *position)
            .collect();
        Self {
            registrar_transfers,
            divergence_start,
            confirmed_registry_positions,
            reconciled_transfer_positions,
        }
    }
    pub(super) fn divergence_start(&self) -> Option<Position> {
        self.divergence_start
    }
    pub(super) fn registry_owner_matches_transfer(
        &self,
        position: Position,
        owner: &String,
    ) -> bool {
        self.registrar_transfers
            .iter()
            .rev()
            .find(|(transfer_position, _)| *transfer_position < position)
            .map(|(_, transfer_owner)| transfer_owner)
            == Some(owner)
            || self.confirmed_registry_positions.contains(&position)
    }
    pub(super) fn reconciled_transfer_positions(&self) -> &BTreeSet<Position> {
        &self.reconciled_transfer_positions
    }
}
// In its resolver-bearing path, the current controller registers to itself, writes the requested
// registry owner, transfers the token to that owner, and then emits NameRegistered; its zero-resolver
// path registers directly to that owner.
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L287-L317 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L333-L341 @ ens_v1@91c966f)
#[rustfmt::skip]
fn confirmed_registry_positions(events: &EventIndex, targets: &[usize], transfers: &[(Position, String)], owners: &[(Position, String)]) -> BTreeSet<Position> {
    owners.iter().filter_map(|(registry_position, owner)| {
        let transfer_position = transfers.iter().find(|(position, transfer_owner)| position > registry_position && transfer_owner == owner)?.0;
        targets.iter().map(|index| &events.fields[*index]).any(|fields| fields.source_event == SourceEvent::NameRegistered && fields.owner.as_ref() == Some(owner) && fields.position.is_some_and(|position| position > transfer_position)).then_some(*registry_position)
    }).collect()
}
#[rustfmt::skip]
fn registry_owner_before(owners: &[(Position, String)], position: Position) -> Option<&(Position, String)> {
    owners.iter().filter(|(owner_position, _)| *owner_position < position).max_by_key(|(owner_position, _)| *owner_position)
}
#[rustfmt::skip]
fn registry_owner_after(owners: &[(Position, String)], position: Position) -> Option<&(Position, String)> {
    owners.iter().filter(|(owner_position, _)| *owner_position > position).min_by_key(|(owner_position, _)| *owner_position)
}

pub(super) fn remove_reconciled_transfer_structure(
    output: &BatchOutput,
    events: &mut EventIndex,
    registration: &Registration,
    stale_resources: &BTreeSet<Uuid>,
    positions: &BTreeSet<Position>,
) {
    for position in positions {
        for index in events.candidates_at(*position) {
            let event = &output.normalized_events[index];
            if events.active[index]
                && event.resource_id.is_some_and(|resource_id| {
                    resource_id == registration.resource_id
                        || stale_resources.contains(&resource_id)
                })
                && matches!(
                    event.event_kind.as_str(),
                    "SurfaceBound" | "SurfaceUnbound" | "AuthorityEpochChanged"
                )
            {
                events.active[index] = false;
            }
        }
    }
}

pub(super) fn remove_redundant_successor_epochs(
    output: &BatchOutput,
    events: &mut EventIndex,
    registration: &Registration,
    target_candidates: &[usize],
    redundant_positions: &BTreeSet<Position>,
) {
    for index in target_candidates.iter().copied() {
        let event = &output.normalized_events[index];
        let Some(position) = events.fields[index].position else {
            continue;
        };
        if !events.active[index]
            || !redundant_positions.contains(&position)
            || event.resource_id != Some(registration.resource_id)
            || event.event_kind != "AuthorityEpochChanged"
        {
            continue;
        }
        let before_kind = event.before_state["authority_kind"].as_str();
        let after_kind = event.after_state["authority_kind"].as_str();
        let has_predecessor = before_kind.is_some_and(|before_kind| {
            target_candidates.iter().copied().any(|candidate| {
                events.active[candidate]
                    && events.fields[candidate]
                        .position
                        .is_some_and(|candidate_position| candidate_position < position)
                    && output.normalized_events[candidate].event_kind == "AuthorityEpochChanged"
                    && output.normalized_events[candidate].after_state["authority_kind"]
                        == before_kind
            })
        });
        if before_kind != after_kind && !has_predecessor {
            events.active[index] = false;
        }
    }
}
