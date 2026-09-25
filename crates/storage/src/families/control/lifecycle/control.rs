//! The control block's registry owner and latest kind (build.sql:649-694), restated over the
//! name's admitted retained events and the F2c registry node.
use super::{
    NameFacts,
    admission::Authority,
    laterals::admitted_epochs,
    served::{Tagged, latest},
};
use crate::families::control::position::Position;

/// The control block's registry owner and latest kind (build.sql:649-694), from what the
/// families keep: the latest admitted ENSv2 transfer or registrar snapshot grant, and the F2c
/// node's latest owner for an ENSv1 or Basenames name. F2c keeps the owner without the position,
/// resource or admission of the AuthorityTransferred that set it, and no family keeps an
/// AuthorityEpochChanged's owner, so this part is an approximation: when a later transfer the
/// name's admission leaves out set the node's owner, this read serves that owner and the
/// comparison fails (fixture `an_excluded_later_transfer_is_not_the_control_owner_and_is_not_excused`,
/// a step 2 retention follow-up).
pub(super) fn control_owner(
    facts: &NameFacts,
    authority: &Authority<'_>,
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> (Option<String>, Option<String>) {
    let mut owners: Vec<(Position, Option<String>, &str)> = Vec::new();
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
                owners.push((event.position.clone(), owner, "TokenControlTransferred"));
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
            owners.push((event.position.clone(), owner, "RegistrationGranted"));
        }
    }
    if !is_v2
        && let Some(node) = &facts.registry_node
        && let Some(position) = &node.position
    {
        let owner = if node.owner_word_unmasked == Some(true) {
            None
        } else {
            node.registry_owner.clone().or_else(|| node.owner.clone())
        };
        owners.push((position.clone(), owner, "AuthorityTransferred"));
        kinds.push((position.clone(), "AuthorityTransferred"));
    }
    // An AuthorityEpochChanged decides the kind; F1 keeps no owner for it.
    for (position, _, _) in admitted_epochs(facts, authority, is_v2, selected_key) {
        kinds.push((position, "AuthorityEpochChanged"));
    }
    let owner =
        latest(&facts.order, owners, |(position, _, _)| position).and_then(|(_, owner, _)| owner);
    let kind =
        latest(&facts.order, kinds, |(position, _)| position).map(|(_, kind)| kind.to_owned());
    (owner, kind)
}
