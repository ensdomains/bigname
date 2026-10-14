//! The grants on a node's registry-only resource follow its ENSv1 registry owner.
//!
//! The registry lets a node's owner, or an operator it approved, write the node's record,
//! whichever authority is selected for the name.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
//! A `wrap` writes the registry to the NameWrapper while the registry-only authority is
//! selected, so the registry-only resource holds the NameWrapper's grants, and the wrapper
//! authority is selected after it. The registry writes made while the wrapper authority stays
//! selected move the record on: the NameWrapper's own unwrap writes the new owner before it
//! releases the authority, and a parent's `setSubnodeOwner` needs no NameWrapper consent.
//! Each such write moves the retained grants to the owner it names, or ends them when it names
//! none, so the registry-only resource never serves an owner the record has left.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L364-L374 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
//! A `.eth` second-level name is wrapped from its registrar lease, which stays selected through
//! the wrap's registry write, so its registry-only resource holds no grants to move.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)

use serde_json::json;

use super::{push_permission_change, wrapper_custody::WRAPPER_FAMILY};
use crate::schema_v2::{
    model::RawLogInput,
    protocol::Interpreted,
    state::{State, V1NameState},
};

/// The registry-only authority whose resource still holds the registry owner's grants while
/// the wrapper authority is selected, with that owner, read before the write changes it.
pub(super) fn retained(
    state: &State,
    namespace: &str,
    node: &str,
    active: Option<&V1NameState>,
) -> Option<V1NameState> {
    let active = active.filter(|authority| authority.authority_source_family == WRAPPER_FAMILY)?;
    if state.v1_registrar(namespace, node).is_some() {
        return None;
    }
    state
        .v1_current_registry_authority(namespace, node)
        .filter(|authority| authority.resource_id != active.resource_id)
}

/// Moves the retained grants to the registry owner an owner write names, or ends them when it
/// names none. A write that selects the registry-only authority grants its owner there itself,
/// and when `grant` is false a successor later in the transaction does, so only the retained
/// owner's grants are revoked then.
pub(super) fn reconcile_owner(
    output: &mut Interpreted,
    retained: &V1NameState,
    owner: Option<&str>,
    grant: bool,
    linked: Option<&V1NameState>,
    resolver: Option<&str>,
    raw: &RawLogInput,
) {
    let Some(previous) = retained.owner.as_deref() else {
        return;
    };
    if owner.is_some_and(|owner| owner.eq_ignore_ascii_case(previous)) {
        return;
    }
    push_owner_grants(output, retained, previous, resolver, false, raw);
    if let Some(owner) = owner
        && grant
        && linked.is_none_or(|linked| linked.resource_id != retained.resource_id)
    {
        push_owner_grants(output, retained, owner, resolver, true, raw);
    }
}

/// Moves the retained owner's resolver control to the resolver a registry write sets.
pub(super) fn reconcile_resolver(
    output: &mut Interpreted,
    retained: &V1NameState,
    previous_resolver: Option<&str>,
    resolver: Option<&str>,
    raw: &RawLogInput,
) {
    let Some(owner) = retained.owner.as_deref() else {
        return;
    };
    if previous_resolver.map(str::to_ascii_lowercase) == resolver.map(str::to_ascii_lowercase) {
        return;
    }
    if let Some(previous_resolver) = previous_resolver {
        push_resolver_grant(output, retained, owner, previous_resolver, false, raw);
    }
    if let Some(resolver) = resolver {
        push_resolver_grant(output, retained, owner, resolver, true, raw);
    }
}

fn push_owner_grants(
    output: &mut Interpreted,
    retained: &V1NameState,
    subject: &str,
    resolver: Option<&str>,
    grant: bool,
    raw: &RawLogInput,
) {
    let change = if grant { "grant" } else { "revoke" };
    push_permission_change(
        output,
        retained,
        subject,
        json!({"kind":"resource"}),
        "resource_control",
        grant,
        "AuthorityTransferred",
        &format!("registry-owner-resource-{change}"),
    );
    if let Some(resolver) = resolver {
        push_permission_change(
            output,
            retained,
            subject,
            json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
            "resolver_control",
            grant,
            "AuthorityTransferred",
            &format!("registry-owner-resolver-{change}"),
        );
    }
}

fn push_resolver_grant(
    output: &mut Interpreted,
    retained: &V1NameState,
    subject: &str,
    resolver: &str,
    grant: bool,
    raw: &RawLogInput,
) {
    let change = if grant { "grant" } else { "revoke" };
    push_permission_change(
        output,
        retained,
        subject,
        json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
        "resolver_control",
        grant,
        "ResolverChanged",
        &format!("registry-owner-resolver-{change}"),
    );
}
