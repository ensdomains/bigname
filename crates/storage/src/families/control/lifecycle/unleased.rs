//! The names that serve no registration: nothing proves a canonical allocation, so the composed
//! name is `unregistered` with no registration field, control, owner or manager.
//!
//! A name in the ownerless-registry profile with no implied lease is one. So is a `.eth`
//! second-level name only the 2017 registry recorded and of which bigname holds no BaseRegistrar
//! row. The current registry answers `owner(node)` from the 2017 registry while it holds no record
//! for the node, but every write checks the current registry's own record, so that owner can change
//! nothing. No token was ever minted, so the name is free to register.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29-L35 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L19 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L103 @ ens_v1@91c966f)
use anyhow::Result;
use serde_json::{Map, Value, json};

use super::{
    NameFacts, NamePlace,
    admission::{Authority, REGISTRAR, StagedName},
    expiry::live_entry,
    served::{Selected, Tagged},
};
use crate::families::control::registry::registry_generation;

/// Whether the name serves no registration. The trace records a never-registered name, which the
/// composition reads to drop its binding and its authority.
pub(super) fn of<'a>(
    facts: &'a NameFacts,
    authority: &Authority<'a>,
    selected: &Selected<'a>,
    tagged: &[Tagged<'a>],
    trace: &mut Map<String, Value>,
) -> Result<bool> {
    let selection = &facts.input.selection;
    if selection.ownerless_registry && authority.implied_lease.is_none() {
        return Ok(true);
    }
    let never_registered = selection.unsupported_reason.is_none()
        && facts.input.place == NamePlace::EthSecondLevel
        && authority.registry_only_binding().is_some()
        && authority.implied_lease.is_none()
        && selected.event.is_none()
        && registry_generation(
            facts.registry_node.as_ref(),
            selection.authority_arm.as_deref(),
        )
        .0 == Some("old")
        && !tagged
            .iter()
            .any(|item| item.event.source_family == REGISTRAR && item.staged == StagedName::Ours)
        && live_entry(facts, tagged)?.is_none();
    if never_registered {
        trace.insert("never_registered".into(), json!(true));
    }
    Ok(never_registered)
}
