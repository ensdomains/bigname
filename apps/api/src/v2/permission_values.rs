use bigname_storage::PermissionScope;
use serde_json::{Value, json};

use super::{V2Error, V2Result, slug_to_numeric};

pub(crate) fn permission_scope_value(scope: &PermissionScope) -> V2Result<Value> {
    let detail = match scope {
        PermissionScope::Root | PermissionScope::Registry | PermissionScope::Resource => json!({}),
        PermissionScope::Resolver {
            chain_id,
            resolver_address,
        } => json!({
            "resolver": {
                "chain_id": permission_scope_chain_id(chain_id)?,
                "address": resolver_address.to_ascii_lowercase(),
            }
        }),
        PermissionScope::RecordManager {
            chain_id,
            manager_address,
        } => json!({
            "chain_id": permission_scope_chain_id(chain_id)?,
            "manager": manager_address.to_ascii_lowercase(),
        }),
    };
    let kind = match scope {
        PermissionScope::Resource => "registration",
        _ => scope.kind(),
    };

    Ok(json!({
        "kind": kind,
        "detail": detail,
    }))
}

pub(crate) fn permission_powers_value(powers: &Value) -> V2Result<Value> {
    let Some(items) = powers.as_array() else {
        return Err(V2Error::internal_error(
            "permission powers must be a JSON array",
        ));
    };

    items
        .iter()
        .map(|power| {
            let Some(power) = power.as_str() else {
                return Err(V2Error::internal_error(
                    "permission powers must be string values",
                ));
            };
            product_permission_power(power).map(Value::String)
        })
        .collect::<V2Result<Vec<_>>>()
        .map(Value::Array)
}

fn product_permission_power(power: &str) -> V2Result<String> {
    match power {
        "resource_control" => Ok("registration_control".to_owned()),
        _ if power == "resource" || power.contains("resource_") || power.contains("_resource") => {
            Err(V2Error::internal_error(
                "permission power uses unmapped storage vocabulary",
            ))
        }
        _ => Ok(power.to_owned()),
    }
}

fn permission_scope_chain_id(storage_chain_id: &str) -> V2Result<u64> {
    slug_to_numeric(storage_chain_id).ok_or_else(|| {
        V2Error::internal_error(format!(
            "permission scope uses unmapped chain_id {storage_chain_id}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::product_permission_power;

    const DOCS: &str = include_str!("../../../../docs/api-v2.md");
    const V2_ROLE_TABLES: &str =
        include_str!("../../../../crates/adapters/src/schema_v2/protocol/permissions.rs");
    const V2_RECORD_RESOLVER_ROLE_TABLE: &str = include_str!(
        "../../../../crates/adapters/src/schema_v2/protocol/v2_record_resolver/permissions.rs"
    );
    const V1_PROJECTION: &str =
        include_str!("../../../../crates/project/src/builders/permissions.rs");
    /// The only powers today's ENSv1 and Basenames interpreters emit.
    const V1_EMITTED_STORAGE_POWERS: &[&str] = &["resource_control", "resolver_control"];

    fn documented_powers() -> BTreeSet<String> {
        let start = DOCS
            .find("<!-- powers-vocabulary:start -->")
            .expect("docs/api-v2.md must carry the powers vocabulary start marker");
        let end = DOCS
            .find("<!-- powers-vocabulary:end -->")
            .expect("docs/api-v2.md must carry the powers vocabulary end marker");
        DOCS[start..end]
            .lines()
            .filter_map(|line| line.strip_prefix("| `"))
            .map(|rest| {
                rest.split('`')
                    .next()
                    .expect("a table row's first cell must close its backtick")
                    .to_owned()
            })
            .collect()
    }

    /// Names from the `(bit, "name"),` entries of one named role table, so selector tables in
    /// the same file are not mistaken for powers.
    fn role_table_powers<'a>(source: &'a str, table: &str) -> impl Iterator<Item = String> + 'a {
        let start = source
            .find(&format!("{table}: &[(usize, &str)] = &["))
            .unwrap_or_else(|| panic!("role table {table} must exist"));
        let end = start
            + source[start..]
                .find("];")
                .unwrap_or_else(|| panic!("role table {table} must close"));
        source[start..end].lines().filter_map(|line| {
            let line = line.trim();
            let entry = line.strip_prefix('(')?.strip_suffix("),")?;
            let (bit, name) = entry.split_once(',')?;
            bit.trim().parse::<usize>().ok()?;
            Some(name.trim().strip_prefix('"')?.strip_suffix('"')?.to_owned())
        })
    }

    /// Names the ENSv1 wrapper fuse mask recognises: the `WHEN '<power>' THEN` arms of the
    /// `CASE power.value` expression in the projection builder.
    fn wrapper_mask_powers() -> impl Iterator<Item = String> {
        let start = V1_PROJECTION
            .find("CASE power.value")
            .expect("projection builder must mask powers with CASE power.value");
        let end = start
            + V1_PROJECTION[start..]
                .find("ELSE false")
                .expect("wrapper mask CASE must end with ELSE false");
        V1_PROJECTION[start..end].lines().filter_map(|line| {
            let (_, rest) = line.split_once("WHEN '")?;
            let (power, _) = rest.split_once('\'')?;
            Some(power.to_owned())
        })
    }

    fn code_powers() -> BTreeSet<String> {
        V1_EMITTED_STORAGE_POWERS
            .iter()
            .map(|power| (*power).to_owned())
            .chain(wrapper_mask_powers())
            .chain(role_table_powers(V2_ROLE_TABLES, "REGISTRY_ROLE_BITS"))
            .chain(role_table_powers(V2_ROLE_TABLES, "RESOLVER_ROLE_BITS"))
            .chain(role_table_powers(
                V2_RECORD_RESOLVER_ROLE_TABLE,
                "ROLE_BITS",
            ))
            .map(|power| {
                product_permission_power(&power)
                    .unwrap_or_else(|_| panic!("power {power} must map to product vocabulary"))
            })
            .collect()
    }

    #[test]
    fn documented_powers_vocabulary_matches_code() {
        let documented = documented_powers();
        let code = code_powers();
        assert!(
            code.len() > 40,
            "the producing sources must yield the full vocabulary, got {code:?}"
        );
        let undocumented = code.difference(&documented).collect::<Vec<_>>();
        let stale = documented.difference(&code).collect::<Vec<_>>();
        assert!(
            undocumented.is_empty() && stale.is_empty(),
            "docs/api-v2.md powers vocabulary drifted from code: undocumented {undocumented:?}, stale {stale:?}"
        );
    }

    #[test]
    fn storage_resource_control_is_served_as_registration_control() {
        assert_eq!(
            product_permission_power("resource_control").expect("must map"),
            "registration_control"
        );
        assert!(product_permission_power("resource").is_err());
        assert!(product_permission_power("upstream_resource").is_err());
        assert_eq!(
            product_permission_power("was_reserved").expect("must pass through"),
            "was_reserved"
        );
    }
}
