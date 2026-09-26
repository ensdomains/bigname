//! The resource summary's restriction block (resource_summary.rs:306-325): a wrapper resource's
//! effective wrapper state, and an ENSv2 registration's locked roles from the admin powers held
//! on it and on its registry root.
use std::collections::BTreeSet;

use serde_json::{Value, json};

use crate::families::control::{rows::WrapperRow, wrapper::restrictions};

/// The token-scoped ENSv2 roles, each with the admin power that can still change it
/// (resource_summary.rs:427-433). A role is locked when neither the registration's nor its
/// root's admins hold that power: an account can grant a regular role only while it holds the
/// matching admin role, its roles on the registry root count on every token, and a token's
/// settable roles are regular roles only, so no admin role can be granted on a registration
/// after it is registered. `transfer` has no regular role: `can_transfer_admin` is itself the
/// role that authorizes token transfers.
/// (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L409-L425 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L452-L455 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L545-L573 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L23-L45 @ ens_v2@a971bd64)
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

/// Whether the wrapper's newest mint, holder grant, holder revocation or unwrap leaves it
/// unwrapped (resource_summary.rs:172-197): step 2 keeps that verdict on the wrapper row
/// (`project_wrapper_state.lifecycle_unwrapped`), NameUnwrapped included.
pub fn wrapper_unwrapped(wrapper: Option<&WrapperRow>) -> bool {
    wrapper.is_some_and(|row| row.lifecycle_unwrapped == Some(true))
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
