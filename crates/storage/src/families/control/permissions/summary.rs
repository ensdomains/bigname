//! The resource summary's restriction block (resource_summary.rs:306-325): a wrapper resource's
//! effective wrapper state, and an ENSv2 registration's locked roles from the admin powers held
//! on it and on its registry root.
use std::collections::BTreeSet;

use serde_json::{Value, json};

use crate::families::control::{
    rows::{LifecycleEvent, WrapperRow},
    wrapper::restrictions,
};

use super::grants::GrantRow;

/// The ENSv2 roles a token-scoped admin can lock, with the admin power that keeps each open
/// (resource_summary.rs:427-433).
const ROLES: [(&str, &str); 5] = [
    ("unregister", "admin_unregister"),
    ("renew", "admin_renew"),
    ("set_subregistry", "admin_set_subregistry"),
    ("set_resolver", "admin_set_resolver"),
    ("transfer", "can_transfer_admin"),
];

/// The sorted distinct union of a `project_resource_admin_aggregate.admin_powers` map, whose
/// values are each holder's admin powers (step 2 families/permissions.rs:300-339).
pub fn admin_powers(aggregate: &Value) -> Vec<String> {
    aggregate
        .as_object()
        .into_iter()
        .flatten()
        .flat_map(|(_, powers)| powers.as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The roles neither the resource's nor its root's admins hold.
pub fn locked_roles(own: &[String], root: &[String]) -> Value {
    Value::Array(
        ROLES
            .iter()
            .filter(|(_, admin)| !own.iter().chain(root).any(|held| held == admin))
            .map(|(role, _)| json!(role))
            .collect(),
    )
}

/// Whether the wrapper's newest mint, holder grant, holder revocation or unwrap is not a mint or
/// holder grant (resource_summary.rs:172-197). The families keep the wrapper's mint as a retained
/// TokenControlTransferred with source event NameWrapped and every holder grant or revocation as
/// a grant row; they keep no NameUnwrapped AuthorityEpochChanged or SurfaceUnbound, so an unwrap
/// that revokes no holder grant is not seen and the restriction block stays (fixture
/// `an_unwrap_that_revokes_no_holder_grant_is_not_seen`, a step 2 retention follow-up).
pub fn wrapper_unwrapped(resource: &str, events: &[LifecycleEvent], grants: &[GrantRow]) -> bool {
    let mint = events
        .iter()
        .filter(|event| {
            event.resource_id.as_deref() == Some(resource)
                && event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
                && event.source_event.as_deref() == Some("NameWrapped")
        })
        .map(|event| (event.position.clone(), false));
    let holders = grants
        .iter()
        .filter(|grant| {
            grant.resource_id == resource
                && grant.scope_kind.as_deref() == Some("resource")
                && [&grant.grant_source, &grant.revocation_source]
                    .iter()
                    .find_map(|source| source.get("relation_kind").and_then(Value::as_str))
                    == Some("holder")
        })
        .map(|grant| {
            let empty = grant
                .effective_powers
                .as_array()
                .is_some_and(|powers| powers.is_empty());
            (grant.position.clone(), empty)
        });
    mint.chain(holders)
        .max_by(|left, right| left.0.cmp(&right.0))
        .is_some_and(|(_, unwrapped)| unwrapped)
}

/// The restriction block for a resource of `authority_kind`.
pub fn resource_restrictions(
    authority_kind: Option<&str>,
    wrapper: Option<&WrapperRow>,
    unwrapped: bool,
    clock_seconds: i64,
    has_served_rows: bool,
    own_admins: &[String],
    root_admins: &[String],
) -> Option<Value> {
    match authority_kind? {
        "wrapper" if !unwrapped => wrapper.and_then(|row| restrictions(row, clock_seconds)),
        "ens_v2_registry" if has_served_rows => Some(json!({
            "kind": "ens_v2_registry",
            "locked_roles": locked_roles(own_admins, root_admins),
        })),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locked_roles_are_the_roles_no_admin_holds() {
        let own = vec!["admin_renew".to_owned()];
        let root = vec!["can_transfer_admin".to_owned()];
        assert_eq!(
            locked_roles(&own, &root),
            json!(["unregister", "set_subregistry", "set_resolver"])
        );
        assert_eq!(
            admin_powers(
                &json!({"a|registry": ["admin_renew"], "b|root": ["admin_renew", "can_transfer_admin"]})
            ),
            vec!["admin_renew".to_owned(), "can_transfer_admin".to_owned()]
        );
    }
}
