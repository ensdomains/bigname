//! Record families an admitted resolver has no getter for. An inventory assembled for such a
//! resolver lists the family in `unsupported_families`, so no route reads the absence of a write
//! as "unset" (the records route's per-key answer, and the grouped records' singleton default).
//! A mirrored inventory is assembled with the mirrored ENSv1 resolver's classification, so a
//! mirror of such a resolver inherits the listing.

/// The reason an inventory gives for a family its resolver cannot hold.
pub(crate) const FAMILY_WITHOUT_GETTER_REASON: &str = "record_family_not_supported_by_resolver";

/// The admitted ENSv1 public resolver generations whose getter surface lacks a record family.
/// Both lack `IContentHashResolver`; both keep `INameResolver`. The manifest declares them with
/// the same profiles (manifests/mainnet/ethereum/ens/ens_v1_resolver_l1/v1.toml).
/// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L121-L134 @ ens_app_v3@7175858)
/// (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L135-L147 @ ens_app_v3@7175858)
const WITHOUT_GETTER: &[(&str, &str, &[&str])] = &[
    (
        "ens_v1_resolver_l1",
        "public_resolver_5ffc0143",
        &["contenthash"],
    ),
    (
        "ens_v1_resolver_l1",
        "public_resolver_1da02271",
        &["contenthash"],
    ),
];

/// The record families the classified resolver `(source_family, role)` has no getter for.
pub(crate) fn families_without_getter(
    source_family: Option<&str>,
    role: Option<&str>,
) -> &'static [&'static str] {
    WITHOUT_GETTER
        .iter()
        .find(|(family, lacking_role, _)| {
            source_family == Some(*family) && role == Some(*lacking_role)
        })
        .map_or(&[], |(_, _, families)| families)
}

#[cfg(test)]
mod tests {
    use super::families_without_getter;

    #[test]
    fn legacy_public_resolvers_lack_contenthash_but_hold_name() {
        for role in ["public_resolver_5ffc0143", "public_resolver_1da02271"] {
            assert_eq!(
                families_without_getter(Some("ens_v1_resolver_l1"), Some(role)),
                ["contenthash"]
            );
        }
        for (family, role) in [
            (Some("ens_v1_resolver_l1"), Some("public_resolver_226159d5")),
            (Some("ens_v1_resolver_l1"), Some("public_resolver")),
            (Some("ens_v2_resolver_l1"), Some("public_resolver_5ffc0143")),
            (None, None),
        ] {
            assert!(families_without_getter(family, role).is_empty(), "{role:?}");
        }
    }
}
