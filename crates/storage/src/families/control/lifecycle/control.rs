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
/// SurfaceBound with its bound owner, and, for an ENSv1 or Basenames name, the F2c node's
/// owner group when the event that set it was an AuthorityTransferred the name's admission
/// holds (its position and resource are kept apart from the row's last write). F2c keeps only
/// the latest owner-setting event, so when a SubregistryChanged or an AuthorityTransferred the
/// admission leaves out set it last, an earlier admitted AuthorityTransferred is not seen and
/// the comparison fails (fixture
/// `an_excluded_later_transfer_is_not_the_control_owner`, a step 2 retention follow-up).
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
    if !is_v2
        && let Some(node) = &facts.registry_node
        && node.owner_event_kind.as_deref() == Some("AuthorityTransferred")
        && let Some(position) = &node.owner_position
        && authority.admits(&Probe {
            event_kind: "AuthorityTransferred",
            source_family: registry_family(&node.namespace),
            resource_id: node.owner_resource_id.as_deref(),
            authority_kind: "registrar",
            position,
            transaction_hash: None,
            to_address: None,
            namehash: None,
            wrapper_linked: false,
        })
    {
        let owner = if node.owner_word_unmasked == Some(true) {
            None
        } else {
            node.registry_owner.clone().or_else(|| node.owner.clone())
        };
        owners.push((position.clone(), owner));
        kinds.push((position.clone(), "AuthorityTransferred"));
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

/// The registry family that writes a node of `namespace`.
fn registry_family(namespace: &str) -> &'static str {
    if namespace == "basenames" {
        "basenames_base_registry"
    } else {
        "ens_v1_registry_l1"
    }
}
