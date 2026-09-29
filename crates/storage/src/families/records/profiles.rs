//! Which singleton record families an inventory's resolver can hold, read from the classification
//! the inventory captured in its snapshot (`provenance.abi_observation_classification`; for a
//! mirror, the mirrored ENSv1 resolver's). An authoritative inventory that holds no write for such
//! a family may call it unset only when the resolver has the getter.

use serde_json::Value;

/// The admitted ENSv1 public resolver generations whose getter surface lacks a record family,
/// by `(source_family, role, record_family)`. Both lack `IContentHashResolver`; both keep
/// `INameResolver`. The manifest declares them with the same profiles
/// (manifests/mainnet/ethereum/ens/ens_v1_resolver_l1/v1.toml).
/// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L121-L134 @ ens_app_v3@7175858)
/// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L135-L147 @ ens_app_v3@7175858)
const LACKING: &[(&str, &str, &str)] = &[
    (
        "ens_v1_resolver_l1",
        "public_resolver_5ffc0143",
        "contenthash",
    ),
    (
        "ens_v1_resolver_l1",
        "public_resolver_1da02271",
        "contenthash",
    ),
];

/// Whether the resolver behind an inventory with this `provenance` has the getter for
/// `record_family`. An inventory that names no classification fails closed.
pub fn inventory_resolver_holds_family(provenance: &Value, record_family: &str) -> bool {
    let Some(classification) = provenance
        .get("abi_observation_classification")
        .filter(|classification| classification.is_object())
    else {
        return false;
    };
    let field = |name: &str| classification.get(name).and_then(Value::as_str);
    let (Some(source_family), Some(role)) = (field("source_family"), field("role")) else {
        return false;
    };
    !LACKING.iter().any(|(family, lacking_role, lacking)| {
        *family == source_family && *lacking_role == role && *lacking == record_family
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::inventory_resolver_holds_family;

    fn provenance(role: &str) -> serde_json::Value {
        json!({"abi_observation_classification":
            {"source_family": "ens_v1_resolver_l1", "role": role}})
    }

    #[test]
    fn legacy_public_resolvers_lack_contenthash_but_hold_name() {
        for role in ["public_resolver_5ffc0143", "public_resolver_1da02271"] {
            assert!(!inventory_resolver_holds_family(
                &provenance(role),
                "contenthash"
            ));
            assert!(inventory_resolver_holds_family(&provenance(role), "name"));
        }
        assert!(inventory_resolver_holds_family(
            &provenance("public_resolver_226159d5"),
            "contenthash"
        ));
    }

    #[test]
    fn an_inventory_without_a_classification_holds_nothing() {
        assert!(!inventory_resolver_holds_family(&json!({}), "contenthash"));
        assert!(!inventory_resolver_holds_family(
            &json!({"abi_observation_classification": null}),
            "name"
        ));
    }
}
