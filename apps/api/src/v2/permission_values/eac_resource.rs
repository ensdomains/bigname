use alloy_primitives::U256;
use serde_json::Value;

use crate::v2::{V2Error, V2Result};

pub(crate) fn permission_eac_resource_value(
    row: &bigname_storage::EffectivePermissionRow,
) -> V2Result<Option<String>> {
    use bigname_storage::{EffectivePermissionScope, PermissionScope};
    match &row.scope {
        EffectivePermissionScope::Direct(PermissionScope::Resolver { .. }) => {
            resolver_eac_resource_value(&row.grant_source)
        }
        _ => Ok(None),
    }
}

/// Map retained evidence for a direct resolver grant. Other authority sources have no EAC target.
pub(crate) fn resolver_eac_resource_value(source: &Value) -> V2Result<Option<String>> {
    if source.get("source_event").and_then(Value::as_str) != Some("EACRolesChanged") {
        return Ok(None);
    }
    let error = || V2Error::internal_error("failed to map resolver EAC resource");
    let word = source
        .get("upstream_resource")
        .and_then(Value::as_str)
        .ok_or_else(error)?;
    let (digits, radix) = word.strip_prefix("0x").map_or((word, 10), |hex| (hex, 16));
    if digits.is_empty()
        || !digits.bytes().all(|byte| match radix {
            16 => byte.is_ascii_hexdigit(),
            _ => byte.is_ascii_digit(),
        })
    {
        return Err(error());
    }
    U256::from_str_radix(digits, radix)
        .map(|resource| Some(resource.to_string()))
        .map_err(|_| error())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exact_uint256_evidence_is_required_only_for_identified_eac_grants() {
        let source =
            |word: Value| json!({"source_event":"EACRolesChanged", "upstream_resource":word});
        for (word, expected) in [
            ("0x0000", "0"),
            ("00018446744073709551617", "18446744073709551617"),
            (
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "115792089237316195423570985008687907853269984665640564039457584007913129639935",
            ),
        ] {
            assert_eq!(
                resolver_eac_resource_value(&source(json!(word)))
                    .unwrap()
                    .as_deref(),
                Some(expected)
            );
        }
        for word in [
            Value::Null,
            json!(0),
            json!(""),
            json!("0x"),
            json!("-1"),
            json!("+1"),
            json!(" 1"),
            json!("1e2"),
            json!("1_000"),
            json!("0xgg"),
            json!("115792089237316195423570985008687907853269984665640564039457584007913129639936"),
        ] {
            assert!(resolver_eac_resource_value(&source(word)).is_err());
        }
        assert!(resolver_eac_resource_value(&json!({"source_event":"EACRolesChanged"})).is_err());
        for source in [
            Value::Null,
            json!({}),
            json!({"kind":"ens_v1_authority"}),
            json!({"upstream_resource":"0x00"}),
        ] {
            assert_eq!(resolver_eac_resource_value(&source).unwrap(), None);
        }
    }
}
