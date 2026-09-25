//! The control block's registry owner and latest kind (build.sql:649-694), restated over the
//! name's admitted retained events and the F2c registry node.
use super::{
    NameFacts,
    admission::{Authority, Probe},
    laterals::{admitted_epochs, admitted_registry_only},
    served::{Tagged, latest},
};
use crate::families::control::position::Position;

/// The control block's registry owner and latest kind (build.sql:649-694), from what the
/// families keep: the latest admitted ENSv2 transfer or registrar snapshot grant, F1's latest
/// admitted AuthorityEpochChanged with the owner it reports, an admitted registry-only
/// SurfaceBound with its bound owner, and, for an ENSv1 or Basenames name, each of the name's
/// registry AuthorityTransferred events F2c keeps (`project_registry_owner_event`) that the
/// admission holds. A SubregistryChanged never counts, as in the served lateral. Only the node
/// row carries the registry_owner and unmasked-word facts, and only for its latest
/// owner-setting event. Under a NewOwner that event is the SubregistryChanged of the same log,
/// never the transfer, so a transfer always reports its owner as it stands: an unmasked owner
/// word, which the served block reports as null, shows as the plain owner and fails.
pub(super) fn control_owner(
    facts: &NameFacts,
    authority: &Authority<'_>,
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> (Option<String>, Option<String>) {
    let mut owners: Vec<(Position, Option<String>)> = Vec::new();
    let mut kinds: Vec<(Position, &str)> = Vec::new();
    for tagged in in_scope {
        let event = tagged.event;
        let masked = event.owner_word_unmasked == Some(true);
        if event.event_kind == "TokenControlTransferred" {
            kinds.push((event.position.clone(), "TokenControlTransferred"));
            if is_v2 {
                let owner = if masked {
                    None
                } else {
                    event.to_address.clone()
                };
                owners.push((event.position.clone(), owner));
            }
        }
        if event.event_kind == "RegistrationGranted"
            && event.state_derived == Some(true)
            && event.registrar_surface_snapshot == Some(true)
        {
            let owner = if masked {
                None
            } else {
                event.owner_getter.clone()
            };
            owners.push((event.position.clone(), owner));
        }
    }
    // The name's own registry transfers the admission holds (build.sql:666 reads the name's
    // admitted AuthorityTransferred rows), from every owner-setting event F2c keeps.
    if !is_v2 && let Some(node) = &facts.registry_node {
        let name = facts.input.logical_name_id.as_str();
        for event in &node.owner_events {
            if event.event_kind != "AuthorityTransferred"
                || event.logical_name_id.as_deref() != Some(name)
            {
                continue;
            }
            let admitted = authority.admits(&Probe {
                event_kind: "AuthorityTransferred",
                source_family: &event.source_family,
                resource_id: event.resource_id.as_deref(),
                authority_kind: event
                    .authority_kind
                    .as_deref()
                    .filter(|kind| !kind.is_empty())
                    .unwrap_or("registrar"),
                position: &event.position,
                transaction_hash: event.transaction_hash.as_deref(),
                to_address: None,
                namehash: None,
                wrapper_linked: false,
            });
            if admitted {
                owners.push((event.position.clone(), node.reported_owner(event)));
                kinds.push((event.position.clone(), "AuthorityTransferred"));
            }
        }
    }
    for epoch in admitted_epochs(facts, authority, is_v2, selected_key) {
        owners.push((epoch.position.clone(), epoch.owner));
        kinds.push((epoch.position, "AuthorityEpochChanged"));
    }
    for (position, candidate) in admitted_registry_only(facts, authority, is_v2, selected_key) {
        owners.push((position.clone(), candidate.bound_owner.clone()));
    }
    let owner = latest(&facts.order, owners, |(position, _)| position).and_then(|(_, owner)| owner);
    let kind =
        latest(&facts.order, kinds, |(position, _)| position).map(|(_, kind)| kind.to_owned());
    (owner, kind)
}
