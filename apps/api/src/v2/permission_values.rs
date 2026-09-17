use bigname_storage::{EffectivePermissionScope, PermissionGrantRelation, PermissionScope};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{V2Error, V2Result, slug_to_numeric};

/// How an effective permission row reaches its registration when it is not a direct grant.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GrantRelation {
    Operator,
}

pub(crate) fn permission_grant_relation(
    relation: Option<PermissionGrantRelation>,
) -> Option<GrantRelation> {
    relation.map(|relation| match relation {
        PermissionGrantRelation::Operator => GrantRelation::Operator,
    })
}

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

pub(crate) fn effective_permission_scope_value(
    scope: &EffectivePermissionScope,
) -> V2Result<Value> {
    match scope {
        EffectivePermissionScope::Direct(scope) => permission_scope_value(scope),
        EffectivePermissionScope::Account {
            chain_id,
            authority_kind,
            authority_contract,
            owner,
        } => Ok(json!({"kind":"account","detail":{
            "chain_id": permission_scope_chain_id(chain_id)?,
            "authority_kind": authority_kind,
            "authority_contract": authority_contract.to_ascii_lowercase(),
            "owner": owner.to_ascii_lowercase(),
        }})),
    }
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

/// The record an argument-scoped resolver resource is about, from the selector the
/// interpreter decoded out of the setter argument: an address family keyed by coin
/// type, a text or data key, an ABI content type, an interface id, or — when one
/// argument authorizes several setters — the list of those. The interpreter reads
/// the argument under the union of the old and new role bitmaps so it can describe
/// the revoked side of a change too; a served row keeps only the readings whose
/// setter the holder currently has (`powers`), so a revoked family drops out; an
/// `admin_set_*` role is authority to grant or revoke the setter, not the setter,
/// and does not count. An
/// argument the interpreter could not decode describes nothing and is omitted, as
/// is a resource whose every reading was revoked. A text or data key that is not
/// printable UTF-8 is served as `key_bytes` (hex), never as a lookalike string.
/// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L307-L338 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L252-L259 @ ens_v2@a971bd64)
pub(crate) fn record_resource_value(selector: &Value, powers: &Value) -> V2Result<Option<Value>> {
    let Some(object) = selector.as_object() else {
        return Err(record_resource_error());
    };
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(record_resource_error)?;
    if kind == "resource" {
        return Ok(None);
    }
    let hash = object
        .get("hash")
        .and_then(Value::as_str)
        .ok_or_else(record_resource_error)?;
    let key = || {
        object
            .get("key")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(record_resource_error)
    };
    let held = |family: &str| {
        powers.as_array().is_some_and(|powers| {
            powers
                .iter()
                .filter_map(Value::as_str)
                .any(|power| power == format!("set_{family}"))
        })
    };
    let value = match kind {
        "address" if held("addr") => {
            json!({"kind": "address", "hash": hash, "coin_type": key()?})
        }
        "text" | "data" if held(kind) => {
            if object.contains_key("raw_selector_key") {
                json!({"kind": kind, "hash": hash, "key_bytes": key()?})
            } else {
                json!({"kind": kind, "hash": hash, "key": key()?})
            }
        }
        "abi" if held("abi") => json!({"kind": "abi", "hash": hash, "content_type": key()?}),
        "interface" if held("interface") => {
            json!({"kind": "interface", "hash": hash, "interface_id": key()?})
        }
        "address" | "text" | "data" | "abi" | "interface" => return Ok(None),
        "argument" => {
            let selectors = object
                .get("selectors")
                .and_then(Value::as_array)
                .ok_or_else(record_resource_error)?
                .iter()
                .map(|selector| record_resource_value(selector, powers))
                .collect::<V2Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            match selectors.len() {
                0 => return Ok(None),
                1 => selectors.into_iter().next().expect("one selector"),
                _ => json!({"kind": "argument", "hash": hash, "selectors": selectors}),
            }
        }
        _ => return Err(record_resource_error()),
    };
    Ok(Some(value))
}

fn record_resource_error() -> V2Error {
    V2Error::internal_error("failed to map resolver record resource")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::{
        EffectivePermissionScope, effective_permission_scope_value, permission_powers_value,
        product_permission_power,
    };

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
    /// The power a served registry `ApprovalForAll` operator row carries.
    const V1_REGISTRY_OPERATOR_POWERS: &[&str] = &["registry_control"];
    const V1_STANDARD_APPROVALS: &str =
        include_str!("../../../../crates/adapters/src/schema_v2/protocol/standard_approvals.rs");
    const V1_WRAPPER_INTERPRETER: &str = include_str!(
        "../../../../crates/adapters/src/schema_v2/protocol/v1/wrapper/permissions.rs"
    );

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

    /// Names the NameWrapper interpreter grants: every string literal inside the
    /// `WRAPPER_HOLDER_POWERS`, `WRAPPER_DELEGATE_POWERS`, and `RESOLVER_CONTROL_POWERS` slices.
    fn wrapper_interpreter_powers() -> impl Iterator<Item = String> {
        [
            "WRAPPER_HOLDER_POWERS",
            "WRAPPER_DELEGATE_POWERS",
            "RESOLVER_CONTROL_POWERS",
        ]
        .into_iter()
        .flat_map(|constant| {
            let start = V1_WRAPPER_INTERPRETER
                .find(&format!("const {constant}: &[&str] = &["))
                .unwrap_or_else(|| panic!("wrapper interpreter must define {constant}"));
            let body = &V1_WRAPPER_INTERPRETER[start..];
            let end = body.find("];").expect("wrapper power slice must close");
            body[..end]
                .split('"')
                .skip(1)
                .step_by(2)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
    }

    fn code_powers() -> BTreeSet<String> {
        V1_EMITTED_STORAGE_POWERS
            .iter()
            .chain(V1_REGISTRY_OPERATOR_POWERS)
            .map(|power| (*power).to_owned())
            .chain(wrapper_interpreter_powers())
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
        for power in V1_REGISTRY_OPERATOR_POWERS {
            assert!(
                V1_STANDARD_APPROVALS.contains(&format!("\"{power}\"")),
                "standard approvals interpreter must still emit {power}"
            );
        }
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

    #[test]
    fn record_resource_maps_each_decoded_selector_and_omits_undecoded_ones() {
        use serde_json::json;

        use super::record_resource_value;

        let hash = "0x00000000000000000000000000000000000000000000000000000000000000aa";
        let all = json!([
            "set_addr",
            "set_text",
            "set_data",
            "set_abi",
            "set_interface"
        ]);
        assert_eq!(
            record_resource_value(&json!({"kind": "address", "key": "60", "hash": hash}), &all)
                .unwrap(),
            Some(json!({"kind": "address", "hash": hash, "coin_type": "60"}))
        );
        assert_eq!(
            record_resource_value(&json!({"kind": "text", "key": "url", "hash": hash}), &all)
                .unwrap(),
            Some(json!({"kind": "text", "hash": hash, "key": "url"}))
        );
        // A key that is not printable UTF-8 is bytes, not a string that looks like hex.
        assert_eq!(
            record_resource_value(
                &json!({"kind": "text", "key": "0xff", "hash": hash,
                "raw_selector_key": {"encoding": "hex", "bytes": "0xff"}}),
                &all
            )
            .unwrap(),
            Some(json!({"kind": "text", "hash": hash, "key_bytes": "0xff"}))
        );
        assert_eq!(
            record_resource_value(
                &json!({"kind": "data", "key": "0xdead", "hash": hash}),
                &all
            )
            .unwrap(),
            Some(json!({"kind": "data", "hash": hash, "key": "0xdead"}))
        );
        assert_eq!(
            record_resource_value(&json!({"kind": "abi", "key": "1", "hash": hash}), &all).unwrap(),
            Some(json!({"kind": "abi", "hash": hash, "content_type": "1"}))
        );
        assert_eq!(
            record_resource_value(
                &json!({"kind": "interface", "key": "0x01ffc9a7", "hash": hash}),
                &json!(["set_interface"])
            )
            .unwrap(),
            Some(json!({"kind": "interface", "hash": hash, "interface_id": "0x01ffc9a7"}))
        );
        // The admin role grants the setter to others; it is not the setter.
        assert_eq!(
            record_resource_value(
                &json!({"kind": "interface", "key": "0x01ffc9a7", "hash": hash}),
                &json!(["admin_set_interface"])
            )
            .unwrap(),
            None
        );
        // A reading whose setter the holder no longer has is not what the holder may set.
        assert_eq!(
            record_resource_value(
                &json!({"kind": "text", "key": "url", "hash": hash}),
                &json!(["set_data"])
            )
            .unwrap(),
            None
        );
        // One argument read as text and data: keep only the readings still held, and a
        // single survivor is served as that reading.
        let multi = json!({"kind": "argument", "key": null, "hash": hash, "argument_hex": "0x00",
            "selectors": [{"kind": "text", "key": "k", "hash": hash}, {"kind": "data", "key": "k", "hash": hash}]});
        assert_eq!(
            record_resource_value(&multi, &all).unwrap(),
            Some(json!({"kind": "argument", "hash": hash, "selectors": [
                {"kind": "text", "hash": hash, "key": "k"},
                {"kind": "data", "hash": hash, "key": "k"}]}))
        );
        assert_eq!(
            record_resource_value(&multi, &json!(["set_data"])).unwrap(),
            Some(json!({"kind": "data", "hash": hash, "key": "k"}))
        );
        assert_eq!(
            record_resource_value(&multi, &json!(["link"])).unwrap(),
            None
        );
        // An argument the interpreter never saw describes nothing.
        assert_eq!(
            record_resource_value(
                &json!({"kind": "resource", "key": null, "hash": null}),
                &all
            )
            .unwrap(),
            None
        );
        assert!(record_resource_value(&json!({"kind": "text", "hash": hash}), &all).is_err());
        assert!(
            record_resource_value(&json!({"kind": "mystery", "key": "x", "hash": hash}), &all)
                .is_err()
        );
    }

    #[test]
    fn permission_powers_value_preserves_registry_control() {
        assert_eq!(
            permission_powers_value(&json!(["registry_control"])).unwrap(),
            json!(["registry_control"])
        );
    }

    #[test]
    fn effective_permission_scope_value_maps_account_detail() {
        let scope = EffectivePermissionScope::Account {
            chain_id: "ethereum-mainnet".to_owned(),
            authority_kind: "registry".to_owned(),
            authority_contract: "0x0000000000000000000000000000000000000c33".to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        };
        assert_eq!(
            effective_permission_scope_value(&scope).unwrap(),
            json!({"kind":"account","detail":{"chain_id":1,"authority_kind":"registry",
                "authority_contract":"0x0000000000000000000000000000000000000c33",
                "owner":"0x0000000000000000000000000000000000000a11"}})
        );
    }
}
