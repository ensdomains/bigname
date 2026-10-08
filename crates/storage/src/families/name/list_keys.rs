//! What decides whether, and under which public `authority`, a composed name row is listed by
//! registration expiry (`GET /v1/names`). The summary writer stores the answers
//! (`summary.rs`, `expiry_listable` and `public_authority`) and the expiry page statement
//! applies the same rules to the composed rows (`name_current/expiring.rs`), so the stored
//! selector and the served page cannot drift apart.
use serde_json::Value;

/// Whether the composed row's authority coverage is unsupported, independently of lifecycle
/// discovery eligibility.
pub(crate) fn unsupported(coverage: &Value) -> bool {
    coverage.get("status").and_then(Value::as_str) == Some("unsupported")
}

/// Lifecycle discovery can serve a proved canonical allocation without a current binding.
/// Preserve unsupported coverage: this exception grants no current control or resolver access.
pub(crate) fn listing_eligible(coverage: &Value, summary: &Value) -> bool {
    !unsupported(coverage)
        || coverage.get("unsupported_reason").and_then(Value::as_str)
            == Some("current_authority_not_projected")
            && summary.pointer("/registration/canonical_allocation") == Some(&Value::Bool(true))
}

/// The public `authority` a composed row serves, from its `provenance.authority_selection`:
/// `ens_v2`, `ens_v1`, or `ens_v0`, which is the `ens_v1` arm while only the 2017 registry holds
/// an ownership record for the node
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L157 @ ens_v1@91c966f).
/// `None` for Basenames, an unresolved selection, or an ownerless registry row.
pub(crate) fn public_authority(provenance: &Value) -> Option<&'static str> {
    let selection = provenance.get("authority_selection")?;
    if selection.get("ownerless_registry") == Some(&Value::Bool(true)) {
        return None;
    }
    match selection.get("authority_arm").and_then(Value::as_str)? {
        "ens_v1" if selection.get("registry_generation").and_then(Value::as_str) == Some("old") => {
            Some("ens_v0")
        }
        "ens_v1" => Some("ens_v1"),
        "ens_v2" => Some("ens_v2"),
        _ => None,
    }
}

/// SQL over a composed row aliased `nc`: its registration carries a finite expiry, an exact
/// decimal count of Unix seconds. A row that passes has a non-null expiry under both the
/// summary's and the listing's expiry reads, and the same one: the composition writes a
/// registration's expiry only as `registration.expiry`, which both read first among the fields
/// it writes.
pub(crate) const FINITE_REGISTRATION_EXPIRY_SQL: &str =
    "(nc.declared_summary #>> '{registration,expiry}') ~ '^-?[0-9]+(\\.[0-9]+)?$'";

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lifecycle_discovery_requires_selected_allocation_and_exact_reason() {
        let absent = crate::families::search_dictionary::shape::from_composed(None, false).unwrap();
        assert!(!absent.search_supported && absent.search_fields.is_none());
        let allocation = json!({"registration":{"canonical_allocation":true,"expiry":"100","lifecycle_status":"active"}});
        assert!(listing_eligible(&json!({"status":"projected"}), &json!({})));
        for reason in [
            None,
            Some(""),
            Some("unknown"),
            Some("manifest_not_projected"),
            Some("current_authority_not_projected"),
        ] {
            let coverage = json!({"status":"unsupported","unsupported_reason":reason});
            assert_eq!(
                listing_eligible(&coverage, &allocation),
                reason == Some("current_authority_not_projected")
            );
            for marker in [Value::Null, json!(false), json!("true")] {
                let unproved = json!({"registration":{"canonical_allocation":marker,"expiry":"100","lifecycle_status":"active","identity_resource_id":"resource"}});
                assert!(!listing_eligible(&coverage, &unproved));
            }
        }
        let no_expiry =
            json!({"registration":{"canonical_allocation":true,"expires_at_reason":"no_expiry"}});
        assert!(listing_eligible(
            &json!({"status":"unsupported","unsupported_reason":"current_authority_not_projected"}),
            &no_expiry
        ));
    }
}
