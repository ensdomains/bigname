//! The address relations of one composed name at its publication (F13), from the name's
//! controller candidates, its composed row and its NameWrapper row.
//!
//! - The controller is the fold, in the canonical event order, of the candidates the served
//!   admission keeps (for the controller kinds: no unsupported reason, and the selected resource,
//!   or no resource on the selected arm), plus the registry-only predecessor window:
//!   AuthorityTransferred events on the resource of the binding a registry-only selected binding
//!   replaced, from that binding's block up to the selected binding's position. An
//!   AuthorityTransferred or state-derived SurfaceBound sets the controller; a PermissionChanged
//!   acts only on the name's resource, sets it when its powers hold `resource_control` and the
//!   NameWrapper mask allows, and otherwise revokes it from its subject only.
//! - The token holder (served as `owner`) is the composed `control.owner`, the owner the row
//!   serves, where the NameWrapper mask allows: the registrant, else the registry owner, and no
//!   one once the owner lapsed or the admitted Graveyard holds the record. A name with no token
//!   lineage is owned by its registry owner (a registry-only subname), or by its registrant (a
//!   lease transferred without `reclaim`, under the registry-only binding that handoff opened).
//!   The lapse also removes wrapper_state, so both readers then withhold the token-holder
//!   relation under the same modifier mask.
//! - The effective controller is the controller, else (with a token lineage) the token holder,
//!   where the mask allows: no NameWrapper modifier, or a wrapper state outside the `.eth` grace
//!   period, since NameWrapper's `canModifyName` has no wrapper-state condition
//!   (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L222 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1082-L1089 @ ens_v1@91c966f).
//! - The role holders are the holders of an ENSv2 registry role on the name's selected resource
//!   (`address_roles.rs`). Holding a role does not make an address the effective controller.
//!
//! The mask reads the NameWrapper row of the name's resource: whether a PermissionScopeChanged
//! ever set it (`scope_modifiers`), the composed `wrapper_state`, and the grace test at the
//! publication clock, which is unknown (and so not false) when the fuses or expiry are.
use std::collections::BTreeSet;

use serde_json::Value;

use super::{FamilyPosition, address_roles::RoleHolder};
use crate::{
    NameCurrentRow,
    families::control::{
        lifecycle::AuthoritySelection,
        rows::{BindingCandidate, WrapperRow},
        wrapper::{GRACE_PERIOD_SECONDS, IS_DOT_ETH},
    },
};

const ZERO: &str = "0x0000000000000000000000000000000000000000";

/// One `project_address_controller_candidate` row.
#[derive(Clone, Debug)]
pub(super) struct ControllerCandidate {
    pub(super) logical_name_id: String,
    pub(super) position: FamilyPosition,
    pub(super) resource_id: Option<String>,
    pub(super) event_kind: String,
    pub(super) source_family: String,
    /// `set`, before any read-time mask.
    pub(super) set: bool,
    pub(super) subject: Option<String>,
}

/// What one name's relations are computed from.
pub(super) struct NameRelationsInput<'a> {
    pub(super) row: &'a NameCurrentRow,
    pub(super) candidates: &'a [ControllerCandidate],
    /// The selected binding's F1 candidate row.
    pub(super) binding: Option<&'a BindingCandidate>,
    /// The NameWrapper row of the name's resource.
    pub(super) wrapper: Option<&'a WrapperRow>,
    pub(super) clock_seconds: i64,
    /// The ENSv2 registry role holders of the name's selected resource.
    pub(super) role_holders: &'a [RoleHolder],
}

/// The relation names, in the served relation rank order.
pub(super) const TOKEN_HOLDER: &str = "token_holder";
pub(super) const EFFECTIVE_CONTROLLER: &str = "effective_controller";
pub(super) const ROLE_HOLDER: &str = "role_holder";

/// The (address, relation) pairs of one name; empty for a name without a bound, registered row.
pub(super) fn relations(input: &NameRelationsInput<'_>) -> Vec<(String, &'static str)> {
    let row = input.row;
    let summary = &row.declared_summary;
    if row.surface_binding_id.is_none()
        || row.resource_id.is_none()
        || row.binding_kind.is_none()
        || summary.pointer("/control/status").and_then(Value::as_str) == Some("unregistered")
    {
        return Vec::new();
    }
    let lineage = row.token_lineage_id.is_some();
    let modifier = input.wrapper.filter(|wrapper| wrapper.has_modifier);
    let wrapper_state = summary.get("wrapper_state").and_then(Value::as_str);
    let registrant = summary
        .pointer("/registration/registrant")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let open = mask_open(input);
    let controller = controller(input, open).map(|(address, _)| address);

    let owner = summary
        .pointer("/control/owner")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let supported = AuthoritySelection::from_provenance(&row.provenance)
        .unsupported_reason
        .is_none();
    let mut out = Vec::new();
    if lineage
        && (modifier.is_none()
            || matches!(wrapper_state, Some("wrapped" | "emancipated" | "locked")))
        || !lineage && supported
    {
        out.push((owner, TOKEN_HOLDER));
    }
    let released = summary
        .pointer("/registration/status")
        .and_then(Value::as_str)
        == Some("released");
    if (!lineage || open) && !released {
        let effective = if lineage {
            controller.or(registrant)
        } else {
            controller
        };
        out.push((effective, EFFECTIVE_CONTROLLER));
    }
    for holder in input.role_holders {
        out.push((Some(holder.subject.clone()), ROLE_HOLDER));
    }
    out.into_iter()
        .filter_map(|(address, relation)| {
            let address = address?.to_ascii_lowercase();
            (address != ZERO).then_some((address, relation))
        })
        .collect()
}

/// Whether the NameWrapper lets the name's token holder change its registry record: no
/// modifier, or a wrapper state outside the `.eth` grace period. The mask also decides whether a
/// `resource_control` PermissionChanged sets rather than revokes.
fn mask_open(input: &NameRelationsInput<'_>) -> bool {
    let Some(modifier) = input.wrapper.filter(|wrapper| wrapper.has_modifier) else {
        return true;
    };
    input
        .row
        .declared_summary
        .get("wrapper_state")
        .and_then(Value::as_str)
        .is_some()
        && in_grace(modifier, input.clock_seconds) == Some(false)
}

/// The served `scope_modifiers.in_grace`: unknown when the fuses or the expiry is.
fn in_grace(wrapper: &WrapperRow, clock_seconds: i64) -> Option<bool> {
    let fuses = wrapper.fuses?;
    let expiry: i128 = wrapper.expiry_seconds.as_deref()?.parse().ok()?;
    let clock = i128::from(clock_seconds);
    Some(fuses & IS_DOT_ETH != 0 && expiry >= clock && expiry - GRACE_PERIOD_SECONDS < clock)
}

fn arm_of(source_family: &str) -> Option<&'static str> {
    if source_family.starts_with("ens_v1_") {
        Some("ens_v1")
    } else if source_family.starts_with("ens_v2_") {
        Some("ens_v2")
    } else if source_family.starts_with("basenames_") {
        Some("basenames")
    } else {
        None
    }
}

/// The folded controller. `permission_mask_open` is the served condition under which a
/// `resource_control` PermissionChanged sets rather than revokes.
fn controller(
    input: &NameRelationsInput<'_>,
    permission_mask_open: bool,
) -> Option<(String, FamilyPosition)> {
    let selection = AuthoritySelection::from_provenance(&input.row.provenance);
    if selection.unsupported_reason.is_some() {
        // No controller event is admitted, and the predecessor window reads only names
        // without an unsupported reason.
        return None;
    }
    let selected_resource = selection.resource_id.as_deref();
    let selected_arm = selection.authority_arm.as_deref();
    let admitted = |candidate: &ControllerCandidate| match candidate.resource_id.as_deref() {
        Some(resource) => Some(resource) == selected_resource,
        None => selected_arm.is_some() && arm_of(&candidate.source_family) == selected_arm,
    };
    let window = registry_only_window(&selection, input.binding);
    let in_window = |candidate: &ControllerCandidate| {
        let Some((resource, lower, upper)) = &window else {
            return false;
        };
        candidate.event_kind == "AuthorityTransferred"
            && candidate.resource_id.as_deref() == Some(resource.as_str())
            && candidate.position.block_number >= *lower
            && (
                candidate.position.block_number,
                candidate.position.transaction_index.unwrap_or(-1),
                candidate.position.log_index.unwrap_or(-1),
            ) <= *upper
    };
    let mut seen = BTreeSet::new();
    let mut events: Vec<&ControllerCandidate> = input
        .candidates
        .iter()
        .filter(|candidate| admitted(candidate) || in_window(candidate))
        .filter(|candidate| seen.insert(candidate.position.event_identity.clone()))
        .collect();
    events.sort_by(|left, right| left.position.cmp(&right.position));

    let name_resource = input.row.resource_id.map(|resource| resource.to_string());
    let mut controller: Option<(String, FamilyPosition)> = None;
    for event in events {
        let set = match event.event_kind.as_str() {
            "AuthorityTransferred" | "SurfaceBound" => true,
            "PermissionChanged" => {
                if name_resource.is_none() || event.resource_id != name_resource {
                    continue;
                }
                event.set && permission_mask_open
            }
            _ => continue,
        };
        if set {
            controller = event
                .subject
                .clone()
                .map(|subject| (subject, event.position.clone()));
        } else if controller
            .as_ref()
            .is_some_and(|(address, _)| Some(address) == event.subject.as_ref())
        {
            controller = None;
        }
    }
    controller
}

/// The registry-only predecessor window of a name whose selected binding is registry-only on the
/// selected resource of the ENSv1 or Basenames arm: the predecessor's resource, its block, and
/// the selected binding's position with a missing index read as -1.
fn registry_only_window(
    selection: &AuthoritySelection,
    binding: Option<&BindingCandidate>,
) -> Option<(String, i64, (i64, i64, i64))> {
    if !matches!(
        selection.authority_arm.as_deref(),
        Some("ens_v1" | "basenames")
    ) {
        return None;
    }
    let binding = binding?;
    if !binding.registry_only
        || Some(binding.resource_id.as_str()) != selection.resource_id.as_deref()
    {
        return None;
    }
    let resource = binding.predecessor_resource_id.clone()?;
    let lower = binding
        .predecessor_position
        .as_ref()?
        .get("block_number")?
        .as_i64()?;
    let (block, transaction, log, _) = binding.order();
    Some((resource, lower, (block, transaction, log)))
}

#[cfg(test)]
#[path = "address_relations_tests.rs"]
mod tests;

/// The actual event that supplied `address`'s current relation, for bounded history
/// attribution. Reuses the controller fold, the role holder's grant row and the registration
/// fold's selected event; it does not infer an acquisition time from the publication time.
pub(super) fn relation_position(
    input: &NameRelationsInput<'_>,
    address: &str,
    relation: &str,
) -> Option<FamilyPosition> {
    let controlled = relation == EFFECTIVE_CONTROLLER
        || relation == TOKEN_HOLDER
            && input.row.token_lineage_id.is_none()
            && input
                .row
                .declared_summary
                .pointer("/registration/registrant")
                .is_none_or(Value::is_null);
    if controlled
        && let Some((controller, position)) = controller(input, mask_open(input))
        && (relation == EFFECTIVE_CONTROLLER || controller.eq_ignore_ascii_case(address))
    {
        return Some(position);
    }
    if relation == ROLE_HOLDER {
        return input
            .role_holders
            .iter()
            .find(|holder| holder.subject.eq_ignore_ascii_case(address))
            .map(|holder| holder.position.clone());
    }
    FamilyPosition::from_json(input.row.provenance.get("registrant_position")?)
}
