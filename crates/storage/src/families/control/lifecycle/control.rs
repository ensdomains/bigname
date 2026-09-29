//! The control block's registry owner and latest kind, restated over the
//! name's admitted retained events and the F2c registry node.
use super::{
    NameFacts,
    admission::{Authority, Probe},
    laterals::{admitted_epochs, admitted_registry_only},
    served::{Tagged, latest},
};
use crate::families::control::{
    position::Position,
    registry::{OwnerEvent, ZERO_ADDRESS},
    rows::family_arm,
};

/// The control block's registry owner and latest kind, from what the
/// families keep: the latest admitted ENSv2 transfer or registrar snapshot grant, F1's latest
/// admitted AuthorityEpochChanged with the owner it reports, an admitted registry-only
/// SurfaceBound with its bound owner, the registry owner an admitted registrar-authority
/// SurfaceBound of the selected resource recorded (`admitted_registrar_bindings`), and each of
/// the name's registry AuthorityTransferred
/// events F2c keeps (`project_registry_owner_event`) that the admission holds, each read as the
/// registry getter's view (`OwnerEvent::reported_owner`). For an ENSv1 or Basenames name that is
/// not NameWrapper-selected, the node's newest registry write decides instead, unless a newer
/// explicit clear follows it (`newest_registry_write`). For an ENSv2 name those are its ENSv2
/// registry's transfers on the selected lifecycle key, so the owner a registration names counts
/// until a later ERC1155 transfer, and an earlier registration's owner never reaches a later
/// one on another resource. A SubregistryChanged never counts, as in the served lateral.
pub(super) fn control_owner(
    facts: &NameFacts,
    authority: &Authority<'_>,
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> (FoldedOwner, Option<String>) {
    // Each owner fact, with whether it is an explicit clear: an epoch whose owner is null, as a
    // release's (`newest_registry_write`).
    let mut owners: Vec<(Position, Option<String>, bool)> = Vec::new();
    let mut kinds: Vec<(Position, &str)> = Vec::new();
    // A NameWrapper-selected name serves the wrapped token's holder, which a NameWrapper token
    // transfer moves; a registrar ERC721 transfer does not, since the NameWrapper holds that
    // token while the name is wrapped.
    // (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L155-L197 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L281-L303 @ ens_v1@91c966f)
    let wrapper_selected = wrapper_selected(authority);
    for tagged in in_scope {
        let event = tagged.event;
        let masked = event.owner_word_unmasked == Some(true);
        if event.event_kind == "TokenControlTransferred" {
            kinds.push((event.position.clone(), "TokenControlTransferred"));
            if is_v2 || (wrapper_selected && event.source_family == "ens_v1_wrapper_l1") {
                let owner = if masked {
                    None
                } else {
                    event.to_address.clone()
                };
                owners.push((event.position.clone(), owner, false));
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
            owners.push((event.position.clone(), owner, false));
        }
    }
    // The name's own registry transfers the admission holds (its admitted AuthorityTransferred
    // rows), from every owner-setting event F2c keeps; an ENSv2 name's only on its selected
    // lifecycle key, whose events are keyed by that resource.
    if let Some(node) = &facts.registry_node {
        let name = facts.input.logical_name_id.as_str();
        for event in &node.owner_events {
            if event.event_kind != "AuthorityTransferred"
                || event.logical_name_id.as_deref() != Some(name)
            {
                continue;
            }
            let v2_family = family_arm(&event.source_family) == Some("ens_v2");
            if is_v2
                && !(v2_family
                    && event.resource_id.is_some()
                    && event.resource_id.as_deref() == selected_key)
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
            });
            if admitted {
                owners.push((event.position.clone(), event.reported_owner(), false));
                kinds.push((event.position.clone(), "AuthorityTransferred"));
            }
        }
    }
    for epoch in admitted_epochs(facts, authority, is_v2, selected_key) {
        // An epoch that states no owner leaves the owner as the earlier facts set it.
        if let Some(owner) = epoch.owner {
            let clear = owner.is_none();
            owners.push((epoch.position.clone(), owner, clear));
        }
        kinds.push((epoch.position, "AuthorityEpochChanged"));
    }
    for (position, candidate) in admitted_registry_only(facts, authority, is_v2, selected_key) {
        owners.push((position.clone(), candidate.bound_owner.clone(), false));
    }
    for (position, owner) in admitted_registrar_bindings(facts, authority, is_v2) {
        owners.push((position.clone(), Some(owner.to_owned()), false));
    }
    let folded = latest(owners, |(position, _, _)| position);
    let owner = match newest_registry_write(facts, authority, is_v2) {
        Some(write) => Some(match folded {
            Some((position, owner, true)) if position > write.position => owner,
            _ => write.reported_owner(),
        }),
        None => folded.map(|(_, owner, _)| owner),
    };
    let kind = latest(kinds, |(position, _)| position).map(|(_, kind)| kind.to_owned());
    (
        FoldedOwner {
            found: owner.is_some(),
            owner: owner.flatten(),
        },
        kind,
    )
}

/// The registry owner write that decides the served owner of a name the registry holds: the
/// node's newest ENSv1 or Basenames registry `NewOwner` or `Transfer` in canonical order,
/// whatever resource the adapter anchored it on, read as the getter's view. Only a registry write
/// sets `owner(node)`; every other owner fact of such a name restates one (a binding's snapshot
/// of the getter, a registrar transfer's retained registry owner, a registry-only or boundary
/// epoch's owner, `NameUnwrapped`'s raw controller argument) or clears it (a release's explicit
/// null). So the newest write wins over every older fact, and a newer fact overrides it only when
/// it is such a clear. A NameWrapper-selected name keeps the fold: the owner it serves is the
/// wrapped token's holder, while the registry names the NameWrapper. Once the admitted Graveyard
/// holds its current registry record, though, that record decides even for a NameWrapper-selected
/// name: a subname wrapped without `PARENT_CANNOT_CONTROL` keeps a transferable token after the
/// Graveyard clears its record, and no later token transfer gives it an owner again. ENSv2 names
/// have their own registry and keep the fold.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20260916@366de741)
fn newest_registry_write<'a>(
    facts: &'a NameFacts,
    authority: &Authority<'_>,
    is_v2: bool,
) -> Option<&'a OwnerEvent> {
    if is_v2 {
        return None;
    }
    let write = facts.registry_node.as_ref()?.latest_transfer()?;
    (!wrapper_selected(authority) || write.graveyard_held()).then_some(write)
}

/// Whether the selected binding is a NameWrapper authority. The binding's authority kind, not
/// the family that bound it: a `NameUnwrapped` binds the reactivated lease through the
/// NameWrapper's events, as a registrar authority.
fn wrapper_selected(authority: &Authority<'_>) -> bool {
    authority
        .binding
        .is_some_and(|binding| binding.authority_kind.as_deref() == Some("wrapper"))
}

/// What the owner fold found: the owner its latest owner fact reports (none for a clear or an
/// unmasked owner word), and whether it found any owner fact at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct FoldedOwner {
    pub(super) owner: Option<String>,
    pub(super) found: bool,
}

/// A registered ENSv1 or Basenames name whose authority is its registrar lease or its registry
/// record has a registry owner on chain, zero included, and the families could not produce it.
/// The name must not be served or published without it.
#[derive(Debug)]
pub struct RequiredOwnerMissing {
    pub logical_name_id: String,
}

impl std::fmt::Display for RequiredOwnerMissing {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "registered name {} has a registry record but no registry owner the families can \
             serve",
            self.logical_name_id
        )
    }
}

impl std::error::Error for RequiredOwnerMissing {}

/// The registry owner the control block serves. A name that needs one — an unwrapped ENSv1 or
/// Basenames name whose `active` registration stands on its registrar lease or on its registry
/// record (`owner_required`) — always has one on chain: the registry answers `owner(node)` for
/// every node, zero when it holds no record. When the fold found no owner fact, or its latest
/// fact cleared the owner, the owner is the node's latest registry `NewOwner` or `Transfer`
/// (F2c keeps every one, whatever name it carried), read as the registry getter's view; with no registry record at all it is the
/// zero address. An unmasked owner word, or a record the admitted Graveyard holds
/// (`OwnerEvent::names_no_owner`), names no owner, on the node or in the fold, and is served as
/// none. A node that has a registry record but no owner-setting event the families
/// kept is an integrity failure, never an absent owner.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
pub(super) fn served_owner(
    facts: &NameFacts,
    folded: FoldedOwner,
    owner_required: bool,
) -> Result<Option<String>, RequiredOwnerMissing> {
    if !owner_required || folded.owner.is_some() {
        return Ok(folded.owner);
    }
    let node = facts.registry_node.as_ref();
    if let Some(transfer) = node.and_then(|node| node.latest_transfer()) {
        if transfer.names_no_owner() {
            return Ok(None);
        }
        if let Some(owner) = transfer.reported_owner() {
            return Ok(Some(owner));
        }
    }
    let has_record = node.is_some_and(|node| {
        node.has_old_record
            || node.first_current_record_block.is_some()
            || node.latest_transfer().is_some()
    });
    if !has_record {
        return Ok(Some(ZERO_ADDRESS.to_owned()));
    }
    Err(RequiredOwnerMissing {
        logical_name_id: facts.input.logical_name_id.clone(),
    })
}

/// The registry owner each admitted, non-state-derived registrar-authority SurfaceBound of the
/// name recorded when it opened a binding on the selected resource (F1 `bound_owner`: the owner
/// its event reports, else the registry's owner getter the registrar adapter read from retained
/// registry state). A registrar token transfer that hands a registry-only name back to its lease
/// carries no owner of its own; this is the fact that keeps the registry owner a registry
/// `Transfer` wrote while the registry-only binding was selected, whose AuthorityTransferred sits
/// on the registry-only resource and so is not admitted once the lease is selected again.
/// NameWrapper bindings are left to their own epochs, and ENSv2 names have no registrar binding.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
fn admitted_registrar_bindings<'a>(
    facts: &'a NameFacts,
    authority: &Authority<'_>,
    is_v2: bool,
) -> Vec<(&'a Position, &'a str)> {
    if is_v2 {
        return Vec::new();
    }
    facts
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.state_derived != Some(true)
                && candidate.authority_kind.as_deref() == Some("registrar")
                && !candidate.is_wrapper()
        })
        .filter_map(|candidate| {
            let position = candidate.surface_bound_position.as_ref()?;
            let owner = candidate.bound_owner.as_deref()?;
            authority
                .admits(&Probe {
                    event_kind: "SurfaceBound",
                    source_family: "registrar_binding",
                    resource_id: Some(&candidate.resource_id),
                    authority_kind: "registrar",
                    position,
                    transaction_hash: None,
                    to_address: None,
                    namehash: None,
                })
                .then_some((position, owner))
        })
        .collect()
}
