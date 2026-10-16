use std::collections::BTreeSet;

use uuid::Uuid;

use super::event_index::{EventIndex, Position};

pub(super) fn remove_transient_events(
    events: &mut EventIndex,
    pending_positions: &BTreeSet<Position>,
    stale_resources: &BTreeSet<Uuid>,
    transient_positions: &BTreeSet<Position>,
    transient_owners: &BTreeSet<String>,
) -> BTreeSet<usize> {
    let mut removed = BTreeSet::new();
    for position in pending_positions {
        for index in events.candidates_at(*position) {
            let fields = &events.fields[index];
            if events.active[index]
                && fields.permission
                && fields
                    .resource_id
                    .is_some_and(|resource| stale_resources.contains(&resource))
                && fields
                    .subject
                    .as_ref()
                    .is_some_and(|subject| transient_owners.contains(subject))
            {
                removed.insert(index);
            }
        }
    }
    let kept = keep_revokes_of_kept_grants(events, &mut removed);
    for index in removed {
        events.active[index] = false;
    }
    for position in transient_positions {
        for index in events.candidates_at(*position) {
            let fields = &events.fields[index];
            if events.active[index]
                && !fields.permission
                && fields
                    .resource_id
                    .is_some_and(|resource| stale_resources.contains(&resource))
            {
                events.active[index] = false;
            }
        }
    }
    kept
}

/// Keeps each revoke in `removed` that ends a grant the reconcile keeps, and returns them.
/// Folded in log order, the transaction's own surviving rows for the revoke's resource, subject
/// and scope must not leave the subject live where the first pass ended it. A contract
/// registrant that hands its name on in its registration's transaction gets a grant at its
/// registrar transfer, which is no setup position, and loses it at the registry write that
/// follows, which is one.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L287-L292 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
fn keep_revokes_of_kept_grants(
    events: &EventIndex,
    removed: &mut BTreeSet<usize>,
) -> BTreeSet<usize> {
    let mut kept = BTreeSet::new();
    let mut revokes = removed
        .iter()
        .copied()
        .filter(|index| events.fields[*index].revocation)
        .filter_map(|index| Some((events.fields[index].position?, index)))
        .collect::<Vec<_>>();
    revokes.sort_unstable();
    for (position, index) in revokes {
        let fields = &events.fields[index];
        let Some(resource_id) = fields.resource_id else {
            continue;
        };
        let live = events
            .by_resource
            .get(&resource_id)
            .into_iter()
            .flatten()
            .copied()
            .filter(|other| events.active[*other] && !removed.contains(other))
            .filter(|other| {
                let other = &events.fields[*other];
                other.permission
                    && other.subject == fields.subject
                    && other.scope == fields.scope
                    && other.position.is_some_and(|other| {
                        (other.0, other.1) == (position.0, position.1) && other < position
                    })
            })
            .max_by_key(|other| events.fields[*other].position)
            .is_some_and(|other| events.fields[other].grant);
        if live {
            removed.remove(&index);
            kept.insert(index);
        }
    }
    kept
}
