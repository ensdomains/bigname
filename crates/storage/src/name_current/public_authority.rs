//! The public `authority` value of a `name_current` row, from the selection Project stored in
//! `provenance.authority_selection`. The Rust mapping and the address-name filter's SQL
//! expression below must agree; see `docs/api-v2.md` § Naming Dictionary.

use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};

use super::name_current_authority_arm;

/// The [registry generation](../../../../docs/glossary.md#registry-generation) Project recorded
/// for a name on the `ens_v1` arm: `old` while only the 2017 registry holds an ownership record
/// for its node, `current` after. Absent on other arms and on rows projected before Project
/// recorded it.
pub fn name_current_registry_generation(provenance: &Value) -> Option<&str> {
    provenance
        .pointer("/authority_selection/registry_generation")
        .and_then(Value::as_str)
}

/// The block of the node's first current ENSv1 registry ownership record, where a name on the
/// ENSv1 arm stopped being read from the 2017 registry. Absent before that record and for the
/// root. A diagnostics fact: product responses never carry it.
pub fn name_current_registry_handoff_block_number(provenance: &Value) -> Option<i64> {
    provenance
        .pointer("/authority_selection/registry_handoff_block_number")
        .and_then(Value::as_i64)
}

/// Whether Project classified the row as the supported, unregistered ownerless registry
/// profile. Such a row keeps its selected arm but serves no public `authority`.
pub fn name_current_is_ownerless_registry(provenance: &Value) -> bool {
    provenance.pointer("/authority_selection/ownerless_registry") == Some(&Value::Bool(true))
}

/// `ens_v2`, `ens_v1`, or `ens_v0` (the `ens_v1` arm while the 2017 registry answers for the
/// node); `None` for Basenames, an unresolved selection, or an ownerless registry row.
pub fn name_current_public_authority(provenance: &Value) -> Option<&'static str> {
    if name_current_is_ownerless_registry(provenance) {
        return None;
    }
    match name_current_authority_arm(provenance)? {
        "ens_v1" if name_current_registry_generation(provenance) == Some("old") => Some("ens_v0"),
        "ens_v1" => Some("ens_v1"),
        "ens_v2" => Some("ens_v2"),
        _ => None,
    }
}

/// Keeps rows whose exact-name row serves the public `authority` value, as
/// [`name_current_public_authority`] maps it. One primary-key probe per candidate row.
pub(crate) fn push_public_authority_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    logical_name_id: &str,
    authority: &'a str,
) {
    builder.push(format!(
        r#" AND EXISTS (
                SELECT 1
                FROM bigname_phase.name_current authority_nc
                WHERE authority_nc.logical_name_id = {logical_name_id}
                  AND authority_nc.provenance #> '{{authority_selection,ownerless_registry}}'
                      IS DISTINCT FROM 'true'::jsonb
                  AND CASE authority_nc.provenance #>> '{{authority_selection,authority_arm}}'
                          WHEN 'ens_v1' THEN CASE
                              WHEN authority_nc.provenance
                                       #>> '{{authority_selection,registry_generation}}' = 'old'
                                  THEN 'ens_v0' ELSE 'ens_v1' END
                          WHEN 'ens_v2' THEN 'ens_v2'
                      END = "#
    ));
    builder.push_bind(authority);
    builder.push(")");
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::name_current_public_authority;

    #[test]
    fn public_authority_refines_the_ens_v1_arm_and_omits_ownerless_rows() {
        for (selection, expected) in [
            (
                json!({"authority_arm": "ens_v1", "registry_generation": "old"}),
                Some("ens_v0"),
            ),
            (
                json!({"authority_arm": "ens_v1", "registry_generation": "current"}),
                Some("ens_v1"),
            ),
            (json!({"authority_arm": "ens_v1"}), Some("ens_v1")),
            (json!({"authority_arm": "ens_v2"}), Some("ens_v2")),
            (json!({"authority_arm": "basenames"}), None),
            (
                json!({"unsupported_reason": "current_authority_not_projected"}),
                None,
            ),
            (
                json!({"authority_arm": "ens_v1", "registry_generation": "current",
                       "ownerless_registry": true}),
                None,
            ),
            (
                json!({"authority_arm": "ens_v1", "ownerless_registry": false}),
                Some("ens_v1"),
            ),
        ] {
            assert_eq!(
                name_current_public_authority(&json!({"authority_selection": selection})),
                expected,
                "{selection}"
            );
        }
    }
}
