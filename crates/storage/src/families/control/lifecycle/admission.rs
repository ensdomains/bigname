//! The authority admission of authority_events.sql, evaluated per retained event against the F1
//! selection (design, note "F2a authority admission", table at design:88-95), and the two
//! staging passes that name an unnamed `.eth` registrar row (name_authority/stage.rs:149-158 and
//! :174-198). The passes are recomputed from the immutable original name and the current binding
//! candidates on every read, never read from the name step 2 decoded (design:111); step 2 does
//! the same at write time in crates/project/src/families/decode.rs:110-140, and this is a
//! deliberate second copy because the storage crate cannot depend on the Project crate.
use crate::families::control::{
    position::{Position, bound_of},
    rows::{BindingCandidate, LifecycleEvent},
};

use super::AuthoritySelection;

pub(crate) const REGISTRAR: &str = "ens_v1_registrar_l1";
const WRAPPER: &str = "ens_v1_wrapper_l1";
/// The registrar lifecycle kinds staging names (stage.rs:153-157).
const STAGED_KINDS: [&str; 5] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "ExpiryChanged",
    "TokenControlTransferred",
];
const SIX_KINDS: [&str; 6] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "RegistrationReserved",
    "ExpiryChanged",
    "TokenControlTransferred",
];

/// The name an event carries once staging has run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StagedName {
    /// This name: emitted with it, or named by one of the two passes.
    Ours,
    /// Emitted with another name.
    Other,
    /// Emitted unnamed and named by neither pass (for this name).
    Unnamed,
}

/// Something the admission tests: a retained lifecycle event, or a stand-in for an F1 fact that
/// the laterals read from the same admitted set (a wrapper SurfaceBound, an
/// AuthorityEpochChanged).
pub(crate) struct Probe<'a> {
    pub(crate) event_kind: &'a str,
    pub(crate) source_family: &'a str,
    pub(crate) resource_id: Option<&'a str>,
    pub(crate) authority_kind: &'a str,
    pub(crate) position: &'a Position,
    pub(crate) transaction_hash: Option<&'a str>,
    pub(crate) to_address: Option<&'a str>,
    pub(crate) namehash: Option<&'a str>,
    pub(crate) wrapper_linked: bool,
}

impl<'a> Probe<'a> {
    pub(crate) fn of(event: &'a LifecycleEvent, wrapper_linked: bool) -> Self {
        Self {
            event_kind: &event.event_kind,
            source_family: &event.source_family,
            resource_id: event.resource_id.as_deref(),
            authority_kind: &event.authority_kind,
            position: &event.position,
            transaction_hash: event.transaction_hash.as_deref(),
            to_address: event.to_address.as_deref(),
            namehash: event.namehash.as_deref(),
            wrapper_linked,
        }
    }
}

/// The inputs the admission reads for one name.
pub(crate) struct Authority<'a> {
    pub(crate) name: &'a str,
    pub(crate) selection: &'a AuthoritySelection,
    /// Every binding candidate of the name.
    pub(crate) candidates: &'a [BindingCandidate],
    /// The selected binding (`project_bindings`, name_authority/stage.rs:277-280).
    pub(crate) binding: Option<&'a BindingCandidate>,
    /// Whether the selected resource has a NameWrapper PermissionScopeChanged (F2b).
    pub(crate) wrapper_modifier: bool,
    /// Every retained event loaded for the name, read as witnesses.
    pub(crate) events: &'a [LifecycleEvent],
}

fn distinct(left: Option<&str>, right: Option<&str>) -> bool {
    left != right
}

impl<'a> Authority<'a> {
    /// Staging pass one (stage.rs:149-158): a binding candidate of the row's resource whose
    /// surface namehash is the row's namehash.
    fn direct_binding(&self, event: &LifecycleEvent) -> bool {
        let Some(resource) = event.resource_id.as_deref() else {
            return false;
        };
        self.candidates.iter().any(|candidate| {
            candidate.resource_id == resource
                && candidate.surface_namehash.is_some()
                && candidate.surface_namehash.as_deref() == event.namehash.as_deref()
        })
    }

    /// Staging pass two (stage.rs:174-198): a NameWrapper SurfaceBound of the name that
    /// recorded the row's resource as its lease at the row's node, leaving out the transfer
    /// into the wrapper in the wrap's own transaction.
    fn wrapper_binding(&self, event: &LifecycleEvent) -> bool {
        let Some(resource) = event.resource_id.as_deref() else {
            return false;
        };
        self.candidates.iter().any(|wrapper| {
            wrapper.is_wrapper()
                && wrapper.wrapped_registrar_resource_id.as_deref() == Some(resource)
                && wrapper.node.is_some()
                && wrapper.node.as_deref() == event.namehash.as_deref()
                && custody_passes(
                    &event.event_kind,
                    event.transaction_hash.as_deref(),
                    event.to_address.as_deref(),
                    wrapper,
                )
        })
    }

    fn stageable(event: &LifecycleEvent) -> bool {
        event.original_logical_name_id.is_none()
            && event.source_family == REGISTRAR
            && STAGED_KINDS.contains(&event.event_kind.as_str())
    }

    /// The name the event carries after both staging passes.
    pub(crate) fn staged_name(&self, event: &LifecycleEvent) -> StagedName {
        match event.original_logical_name_id.as_deref() {
            Some(name) if name == self.name => StagedName::Ours,
            Some(_) => StagedName::Other,
            None if Self::stageable(event)
                && (self.direct_binding(event) || self.wrapper_binding(event)) =>
            {
                StagedName::Ours
            }
            None => StagedName::Unnamed,
        }
    }

    /// Membership in `project_wrapper_linked_events`: emitted unnamed, not named by pass one,
    /// named by pass two (design:111).
    pub(crate) fn wrapper_linked(&self, event: &LifecycleEvent) -> bool {
        Self::stageable(event) && !self.direct_binding(event) && self.wrapper_binding(event)
    }

    /// The selected NameWrapper SurfaceBounds: wrapper candidates of the name at the selected
    /// resource (authority_events.sql:119-125, :291-297).
    fn selected_wrappers(&self) -> impl Iterator<Item = &'a BindingCandidate> + '_ {
        let selected = self.selection.resource_id.as_deref();
        self.candidates.iter().filter(move |candidate| {
            candidate.is_wrapper() && Some(candidate.resource_id.as_str()) == selected
        })
    }

    /// The latest ENSv1 candidate strictly before the selected binding (authority_events.sql
    /// :38-81).
    fn predecessor(&self) -> Option<&'a BindingCandidate> {
        let binding = self.binding?;
        let (block, transaction, log, _) = binding.order();
        self.candidates
            .iter()
            .filter(|candidate| {
                candidate.authority_arm == "ens_v1" && {
                    let (b, t, l, _) = candidate.order();
                    (b, t, l) < (block, transaction, log)
                }
            })
            .max_by(|left, right| left.order().cmp(&right.order()))
    }

    fn wrapped_lease(&self, probe: &Probe<'_>) -> bool {
        if self.selection.authority_arm.as_deref() != Some("ens_v1")
            || !STAGED_KINDS.contains(&probe.event_kind)
            || probe.source_family != REGISTRAR
            || probe.authority_kind != "registrar"
            || !self.wrapper_modifier
        {
            return false;
        }
        let Some(resource) = probe.resource_id else {
            return false;
        };
        let by_predecessor = self
            .predecessor()
            .is_some_and(|predecessor| predecessor.resource_id == resource);
        by_predecessor
            || self.selected_wrappers().any(|wrapper| {
                self.events.iter().any(|registration| {
                    registration.resource_id.as_deref() == Some(resource)
                        && registration.source_family == REGISTRAR
                        && registration.event_kind == "RegistrationGranted"
                        && self.wrapper_relationship(probe, wrapper, registration)
                })
            })
    }

    /// The two rules of authority_events.sql:91-118 between the event, the selected wrapper
    /// and a registrar grant of the event's resource.
    fn wrapper_relationship(
        &self,
        probe: &Probe<'_>,
        wrapper: &BindingCandidate,
        registration: &LifecycleEvent,
    ) -> bool {
        let named = self.staged_name(registration);
        let rule_one = named == StagedName::Ours
            && registration.transaction_hash.is_some()
            && registration.transaction_hash == wrapper.transaction_hash
            && probe.event_kind != "TokenControlTransferred";
        let rule_two = wrapper.wrapped_registrar_resource_id.as_deref()
            == registration.resource_id.as_deref()
            && custody_passes(
                probe.event_kind,
                probe.transaction_hash,
                probe.to_address,
                wrapper,
            )
            && (named == StagedName::Ours
                || (named == StagedName::Unnamed
                    && registration.namehash.is_some()
                    && registration.namehash == wrapper.node));
        rule_one || rule_two
    }

    /// The registry-only handoff window (authority_events.sql:136-249).
    fn handoff(&self, probe: &Probe<'_>) -> bool {
        let arm = self.selection.authority_arm.as_deref();
        if !matches!(arm, Some("ens_v1" | "basenames")) || !SIX_KINDS.contains(&probe.event_kind) {
            return false;
        }
        let Some(binding) = self.registry_only_binding() else {
            return false;
        };
        let predecessor = binding.predecessor_resource_id.as_deref();
        let lease = binding.lease_resource_id.as_deref();
        let resource = probe.resource_id;
        let on_handoff = resource.is_some() && (resource == predecessor || resource == lease);
        let through_wrapper = matches!(
            probe.event_kind,
            "RegistrationGranted" | "RegistrationReleased"
        ) && probe.source_family == REGISTRAR
            && resource.is_some()
            && self.candidates.iter().any(|wrapper| {
                wrapper.is_wrapper()
                    && Some(wrapper.resource_id.as_str()) == predecessor
                    && wrapper.wrapped_registrar_resource_id.as_deref() == resource
                    && wrapper.node.is_some()
                    && wrapper.node.as_deref() == probe.namehash
            });
        if !(on_handoff || through_wrapper) {
            return false;
        }
        let lower = (probe.source_family == REGISTRAR
            && (on_handoff || probe.event_kind == "RegistrationGranted"))
            || binding
                .predecessor_position
                .as_ref()
                .and_then(bound_of)
                .is_some_and(|start| probe.position.bound() >= start);
        let (block, transaction, log, _) = binding.order();
        let upper = probe.position.bound() <= (block, transaction, log)
            || (arm == Some("ens_v1")
                && probe.source_family == REGISTRAR
                && matches!(
                    probe.event_kind,
                    "RegistrationGranted"
                        | "RegistrationRenewed"
                        | "ExpiryChanged"
                        | "RegistrationReleased"
                )
                && probe.authority_kind == "registrar"
                && lease.is_some()
                && lease == resource);
        lower && upper
    }

    /// The selected binding when it is a registry-only binding at the selected resource: the
    /// AuthorityEpochChanged registry_only of authority_events.sql:143-149 and the handoff row
    /// of :156-157.
    pub(crate) fn registry_only_binding(&self) -> Option<&'a BindingCandidate> {
        self.binding.filter(|binding| {
            binding.registry_only
                && Some(binding.resource_id.as_str()) == self.selection.resource_id.as_deref()
        })
    }

    /// The epoch bound and its wrapper-linked exception (authority_events.sql:262-311).
    fn epoch(&self, probe: &Probe<'_>) -> bool {
        if !self.selection.has_proof {
            return true;
        }
        if self
            .selection
            .epoch_start
            .is_some_and(|start| probe.position.bound() >= start)
        {
            return true;
        }
        probe.source_family == REGISTRAR
            && matches!(
                probe.event_kind,
                "RegistrationGranted"
                    | "RegistrationRenewed"
                    | "ExpiryChanged"
                    | "TokenControlTransferred"
            )
            && probe.wrapper_linked
            && probe.resource_id.is_some()
            && self.selected_wrappers().any(|wrapper| {
                custody_passes(
                    probe.event_kind,
                    probe.transaction_hash,
                    probe.to_address,
                    wrapper,
                ) && wrapper.wrapped_registrar_resource_id.as_deref() == probe.resource_id
                    && wrapper.node.is_some()
                    && wrapper.node.as_deref() == probe.namehash
            })
    }

    /// Whether `project_authority_events` holds the probe for this name (authority_events.sql
    /// :12-312). The caller has already established that the probe carries the name.
    pub(crate) fn admits(&self, probe: &Probe<'_>) -> bool {
        let arm_rules = match self.selection.unsupported_reason.as_deref() {
            None => {
                let selected = self.selection.resource_id.as_deref();
                (probe.resource_id.is_some() && probe.resource_id == selected)
                    || (probe.resource_id.is_none()
                        && crate::families::control::rows::family_arm(probe.source_family)
                            .is_some()
                        && crate::families::control::rows::family_arm(probe.source_family)
                            == self.selection.authority_arm.as_deref())
                    || self.wrapped_lease(probe)
                    || self.handoff(probe)
            }
            Some("current_authority_not_projected") => {
                probe.source_family == REGISTRAR && STAGED_KINDS.contains(&probe.event_kind)
            }
            Some(_) => false,
        };
        arm_rules && self.epoch(probe)
    }

    /// A NameWrapper SurfaceBound of the name as a member of the admitted set, for the custody
    /// exclusion of registration_events.sql:63-83.
    pub(crate) fn admits_wrapper_binding(&self, wrapper: &BindingCandidate) -> bool {
        let fallback = Position {
            block_number: wrapper.block_number,
            transaction_index: wrapper.transaction_index,
            log_index: wrapper.log_index,
            event_identity: wrapper.surface_binding_id.clone(),
        };
        let position = wrapper.surface_bound_position.as_ref().unwrap_or(&fallback);
        self.admits(&Probe {
            event_kind: "SurfaceBound",
            source_family: WRAPPER,
            resource_id: Some(&wrapper.resource_id),
            authority_kind: wrapper.authority_kind.as_deref().unwrap_or("registrar"),
            position,
            transaction_hash: wrapper.transaction_hash.as_deref(),
            to_address: None,
            namehash: None,
            wrapper_linked: false,
        })
    }
}

/// The custody exclusion of stage.rs:189-194 and authority_events.sql:104-111: the transfer
/// that moves the registrar token into the NameWrapper in the wrap's own transaction.
pub(crate) fn custody_passes(
    event_kind: &str,
    transaction_hash: Option<&str>,
    to_address: Option<&str>,
    wrapper: &BindingCandidate,
) -> bool {
    event_kind != "TokenControlTransferred"
        || distinct(transaction_hash, wrapper.transaction_hash.as_deref())
        || distinct(to_address, wrapper.emitting_address.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, resource: &str, block: i64, identity: &str) -> BindingCandidate {
        BindingCandidate {
            surface_binding_id: id.into(),
            logical_name_id: "ens:0x01".into(),
            authority_arm: "ens_v1".into(),
            resource_id: resource.into(),
            canonicality_state: None,
            surface_namehash: None,
            block_number: block,
            transaction_index: Some(0),
            log_index: Some(1),
            state_derived: None,
            authority_kind: None,
            authority_key: None,
            authority_key_stored: false,
            registry_only: false,
            predecessor_resource_id: None,
            predecessor_position: None,
            lease_resource_id: None,
            wrapped_registrar_resource_id: None,
            node: None,
            transaction_hash: None,
            emitting_address: None,
            surface_bound_position: Some(Position {
                block_number: block,
                transaction_index: Some(0),
                log_index: Some(1),
                event_identity: identity.into(),
            }),
        }
    }

    /// Item 5 of the TYR-36 step 3 review (Q8): two ENSv1 bindings of one name at the same block,
    /// transaction and log, whose binding ids sort opposite to their SurfaceBound identities.
    /// The binding order mirrors stage.rs and ends with the binding id, so the predecessor is
    /// the binding with the larger id, not the one whose event is later in the canonical order.
    /// The D12 claim covers event-derived latest selections only; this binding-id tie-break is
    /// pinned as it is today.
    #[test]
    fn equal_position_bindings_break_the_tie_by_binding_id_not_event_identity() {
        let by_id = candidate("binding-b", "lease-1", 10, "event-a");
        let by_identity = candidate("binding-a", "lease-2", 10, "event-b");
        assert!(
            by_identity.surface_bound_position > by_id.surface_bound_position,
            "event-b is the later event in the canonical order"
        );
        let selected = candidate("binding-z", "lease-3", 12, "event-z");
        let candidates = [by_id.clone(), by_identity, selected.clone()];
        let selection = AuthoritySelection {
            authority_arm: Some("ens_v1".into()),
            surface_binding_id: Some("binding-z".into()),
            resource_id: Some("lease-3".into()),
            ..AuthoritySelection::default()
        };
        let authority = Authority {
            name: "ens:0x01",
            selection: &selection,
            candidates: &candidates,
            binding: Some(&selected),
            wrapper_modifier: false,
            events: &[],
        };
        assert_eq!(
            authority
                .predecessor()
                .map(|predecessor| predecessor.surface_binding_id.as_str()),
            Some("binding-b")
        );
    }
}
