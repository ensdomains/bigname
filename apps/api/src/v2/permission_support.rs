use std::collections::BTreeMap;

use bigname_storage::{
    PermissionCoverageStatus, PermissionCoverageUnsupportedReason,
    PermissionsCurrentResourceSummary,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

use super::{Completeness, Meta};

const PERMISSION_SUPPORT_UNKNOWN_REASON: &str = "permission_support_unknown";
const PERMISSIONS_PARTIALLY_LISTED_REASON: &str = "permissions_partially_listed";

/// A permission surface whose holders the served rows do not list. Declaration order is the
/// serialized sort order. The list shrinks as later parts of issue #605 add these surfaces.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UnlistedPermissionSurface {
    /// Operators that a name's owner approved on the ENSv2 registry itself. The registry adds
    /// the owner's roles to the roles of every operator the owner approved with
    /// `setApprovalForAll`.
    /// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64)
    /// (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L73-L75 @ ens_v2@a971bd64)
    /// (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L165-L167 @ ens_v2@a971bd64)
    EnsV2RegistryOperators,
    /// BaseRegistrar ERC-721 per-token and operator approvals. An approved spender passes the
    /// same check as the token owner, which also gates `reclaim`.
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
    /// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L327-L330 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L458-L466 @ basenames@1809bbc)
    RegistrarApprovals,
    /// Resolver operator approvals and per-name delegates. The resolver authorises the node
    /// owner, the owner's approved operators, and the owner's delegates for that node.
    /// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L79-L87 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L98-L103 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114-L129 @ ens_v1@91c966f)
    /// (upstream: .refs/basenames/src/L2/L2Resolver.sol:L142-L147 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/L2Resolver.sol:L162-L167 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
    /// An ENSv2 name has the same gap. The ENSv2 `PublicResolverV2` stores owner-wide operators
    /// and per-name delegates, and authorises the ENSv2 registry owner of the name, that
    /// owner's operators, and that owner's delegates for the name.
    /// (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L51-L59 @ ens_v2@a971bd64)
    /// (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L131-L147 @ ens_v2@a971bd64)
    /// (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L174-L184 @ ens_v2@a971bd64)
    ResolverApprovals,
    /// The parent name's control over a wrapped subname that is not emancipated. The parent's
    /// token owner may replace the subname's owner until `PARENT_CANNOT_CONTROL` is burned.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L577 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L701-L731 @ ens_v1@91c966f)
    WrapperParentControl,
}

use UnlistedPermissionSurface::{
    EnsV2RegistryOperators, RegistrarApprovals, ResolverApprovals, WrapperParentControl,
};

/// Every surface, in serialized sort order.
const ALL_SURFACES: [UnlistedPermissionSurface; 4] = [
    EnsV2RegistryOperators,
    RegistrarApprovals,
    ResolverApprovals,
    WrapperParentControl,
];

/// The authority kind the permission summary records for an ENSv2 registry resource.
const ENS_V2_REGISTRY_AUTHORITY_KIND: &str = "ens_v2_registry";

const fn surface_bits(surfaces: &[UnlistedPermissionSurface]) -> u8 {
    let mut bits = 0;
    let mut index = 0;
    while index < surfaces.len() {
        bits |= 1 << surfaces[index] as u8;
        index += 1;
    }
    bits
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionRequestScope {
    ResourceBound,
    AccountWide,
}

/// Which permission surfaces the served rows leave unlisted. A set of registrations reports the
/// union of its members' unlisted surfaces; indeterminate support takes precedence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PermissionSupport {
    unknown: bool,
    /// One bit for each unlisted `UnlistedPermissionSurface`, by declaration position.
    unlisted: u8,
}

impl PermissionSupport {
    /// The summary independently proves that every surface is listed.
    pub(crate) const FULL: Self = Self::partial(&[]);
    /// An ENSv1 or Basenames registrar- or registry-held registration. Effective registry
    /// operators are rows.
    pub(crate) const REGISTRAR_RESOLVER_PARTIAL: Self =
        Self::partial(&[RegistrarApprovals, ResolverApprovals]);
    /// An ENSv1 NameWrapper registration. Holders, operators, and per-token delegates are rows.
    pub(crate) const WRAPPER_PARTIAL: Self =
        Self::partial(&[ResolverApprovals, WrapperParentControl]);
    /// An ENSv2 registry resource. It has no BaseRegistrar token, so `registrar_approvals` does
    /// not apply. Its direct role holders are rows, while operators approved on the registry
    /// and resolver operators and delegates are not.
    pub(crate) const ENS_V2_REGISTRY_PARTIAL: Self =
        Self::partial(&[EnsV2RegistryOperators, ResolverApprovals]);
    /// An address-only read, which may reach registrations of every kind.
    pub(crate) const ACCOUNT_WIDE_PARTIAL: Self = Self::partial(&ALL_SURFACES);
    /// Support is missing or indeterminate.
    pub(crate) const UNKNOWN: Self = Self {
        unknown: true,
        unlisted: 0,
    };

    const fn partial(surfaces: &[UnlistedPermissionSurface]) -> Self {
        Self {
            unknown: false,
            unlisted: surface_bits(surfaces),
        }
    }

    fn merge(self, other: Self) -> Self {
        if self.unknown || other.unknown {
            return Self::UNKNOWN;
        }
        Self {
            unknown: false,
            unlisted: self.unlisted | other.unlisted,
        }
    }

    fn unlisted_surfaces(self) -> Vec<UnlistedPermissionSurface> {
        ALL_SURFACES
            .into_iter()
            .filter(|surface| self.unlisted & surface_bits(&[*surface]) != 0)
            .collect()
    }

    /// Returns whether any completeness metadata was written.
    fn apply(self, meta: &mut Meta) -> bool {
        let reason = if self.unknown {
            PERMISSION_SUPPORT_UNKNOWN_REASON
        } else if self.unlisted == 0 {
            return false;
        } else {
            PERMISSIONS_PARTIALLY_LISTED_REASON
        };
        meta.completeness = Some(Completeness::Partial);
        meta.unsupported_reason = Some(reason.to_owned());
        let surfaces = self.unlisted_surfaces();
        meta.unlisted_permission_surfaces = (!surfaces.is_empty()).then_some(surfaces);
        true
    }
}

fn permission_support_for_summary(
    summary: &PermissionsCurrentResourceSummary,
) -> PermissionSupport {
    match (
        summary.coverage.status(),
        summary.coverage.unsupported_reason(),
    ) {
        (PermissionCoverageStatus::Full, None) => PermissionSupport::FULL,
        (
            PermissionCoverageStatus::Partial,
            Some(PermissionCoverageUnsupportedReason::OperatorApprovalSurfacesNotIngested),
        ) => {
            // The projection records one reason for every registration whose approvals are not
            // ingested. The authority kind says which approvals those are.
            if summary.authority_kind.as_deref() == Some(ENS_V2_REGISTRY_AUTHORITY_KIND) {
                PermissionSupport::ENS_V2_REGISTRY_PARTIAL
            } else {
                PermissionSupport::REGISTRAR_RESOLVER_PARTIAL
            }
        }
        (
            PermissionCoverageStatus::Partial,
            Some(
                PermissionCoverageUnsupportedReason::WrapperParentAndResolverDelegationNotProjected,
            ),
        ) => PermissionSupport::WRAPPER_PARTIAL,
        _ => PermissionSupport::UNKNOWN,
    }
}

pub(crate) fn permission_support_for_resources(
    resource_ids: &[Uuid],
    summaries: &BTreeMap<Uuid, PermissionsCurrentResourceSummary>,
) -> PermissionSupport {
    resource_ids
        .iter()
        .map(|resource_id| {
            summaries
                .get(resource_id)
                .map_or(PermissionSupport::UNKNOWN, permission_support_for_summary)
        })
        .reduce(PermissionSupport::merge)
        .unwrap_or(PermissionSupport::FULL)
}

pub(crate) fn apply_permissions_collection_support_meta(
    meta: &mut Meta,
    support: PermissionSupport,
    request_scope: PermissionRequestScope,
) {
    // An address-only read cannot list approvals and parent control across every registration
    // the address may reach, whatever each visible registration supports.
    let support = match request_scope {
        PermissionRequestScope::ResourceBound => support,
        PermissionRequestScope::AccountWide => {
            support.merge(PermissionSupport::ACCOUNT_WIDE_PARTIAL)
        }
    };
    support.apply(meta);
}

pub(crate) fn apply_role_summary_support_meta(meta: &mut Meta, support: PermissionSupport) {
    if support.apply(meta) {
        meta.unsupported_fields = Some(vec!["role_summary".to_owned()]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_storage::ResourcePermissionCoverage;
    use serde_json::json;
    use sqlx::types::time::OffsetDateTime;

    fn summary(
        resource_id: Uuid,
        coverage: ResourcePermissionCoverage,
    ) -> PermissionsCurrentResourceSummary {
        PermissionsCurrentResourceSummary {
            resource_id,
            authority_kind: None,
            root_resource_id: None,
            coverage,
            resource_restrictions: None,
            provenance: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn collection_meta(support: PermissionSupport, scope: PermissionRequestScope) -> Meta {
        let mut meta = Meta::default();
        apply_permissions_collection_support_meta(&mut meta, support, scope);
        meta
    }

    #[test]
    fn permission_collection_support_lists_unlisted_surfaces_by_scope() {
        let resource = PermissionRequestScope::ResourceBound;
        let full = collection_meta(PermissionSupport::FULL, resource);
        assert_eq!(full, Meta::default());

        let registrar = collection_meta(PermissionSupport::REGISTRAR_RESOLVER_PARTIAL, resource);
        assert_eq!(registrar.completeness, Some(Completeness::Partial));
        assert_eq!(
            registrar.unsupported_reason.as_deref(),
            Some(PERMISSIONS_PARTIALLY_LISTED_REASON)
        );
        assert_eq!(
            serde_json::to_value(&registrar.unlisted_permission_surfaces).unwrap(),
            json!(["registrar_approvals", "resolver_approvals"])
        );

        let wrapper = collection_meta(PermissionSupport::WRAPPER_PARTIAL, resource);
        assert_eq!(
            serde_json::to_value(&wrapper.unlisted_permission_surfaces).unwrap(),
            json!(["resolver_approvals", "wrapper_parent_control"])
        );

        for support in [PermissionSupport::FULL, PermissionSupport::WRAPPER_PARTIAL] {
            let account = collection_meta(support, PermissionRequestScope::AccountWide);
            assert_eq!(account.completeness, Some(Completeness::Partial));
            assert_eq!(
                serde_json::to_value(&account.unlisted_permission_surfaces).unwrap(),
                json!([
                    "ens_v2_registry_operators",
                    "registrar_approvals",
                    "resolver_approvals",
                    "wrapper_parent_control"
                ])
            );
        }

        let ens_v2 = collection_meta(PermissionSupport::ENS_V2_REGISTRY_PARTIAL, resource);
        assert_eq!(
            serde_json::to_value(&ens_v2.unlisted_permission_surfaces).unwrap(),
            json!(["ens_v2_registry_operators", "resolver_approvals"])
        );

        for scope in [resource, PermissionRequestScope::AccountWide] {
            let unknown = collection_meta(PermissionSupport::UNKNOWN, scope);
            assert_eq!(unknown.completeness, Some(Completeness::Partial));
            assert_eq!(
                unknown.unsupported_reason.as_deref(),
                Some(PERMISSION_SUPPORT_UNKNOWN_REASON)
            );
            assert_eq!(unknown.unlisted_permission_surfaces, None);
        }
    }

    #[test]
    fn permission_support_uses_typed_reason_and_declared_precedence() {
        let full_id = Uuid::from_u128(1);
        let wrapper_id = Uuid::from_u128(2);
        let partial_id = Uuid::from_u128(3);
        let missing_id = Uuid::from_u128(4);
        let ens_v2_id = Uuid::from_u128(5);
        let mut ens_v2_summary = summary(
            ens_v2_id,
            ResourcePermissionCoverage::operator_approval_surfaces_not_ingested(),
        );
        ens_v2_summary.authority_kind = Some(ENS_V2_REGISTRY_AUTHORITY_KIND.to_owned());
        let summaries = BTreeMap::from([
            (ens_v2_id, ens_v2_summary),
            (
                full_id,
                summary(
                    full_id,
                    ResourcePermissionCoverage::authoritative(["permissions_current"]),
                ),
            ),
            (
                wrapper_id,
                summary(
                    wrapper_id,
                    ResourcePermissionCoverage::wrapper_parent_and_resolver_delegation_not_projected(),
                ),
            ),
            (
                partial_id,
                summary(
                    partial_id,
                    ResourcePermissionCoverage::operator_approval_surfaces_not_ingested(),
                ),
            ),
        ]);

        assert_eq!(
            permission_support_for_resources(&[full_id], &summaries),
            PermissionSupport::FULL
        );
        assert_eq!(
            permission_support_for_resources(&[wrapper_id], &summaries),
            PermissionSupport::WRAPPER_PARTIAL
        );
        assert_eq!(
            permission_support_for_resources(&[full_id, wrapper_id], &summaries),
            PermissionSupport::WRAPPER_PARTIAL
        );
        assert_eq!(
            permission_support_for_resources(&[full_id, partial_id], &summaries),
            PermissionSupport::REGISTRAR_RESOLVER_PARTIAL
        );
        assert_eq!(
            permission_support_for_resources(&[wrapper_id, partial_id], &summaries)
                .unlisted_surfaces(),
            vec![RegistrarApprovals, ResolverApprovals, WrapperParentControl]
        );
        assert_eq!(
            permission_support_for_resources(&[ens_v2_id], &summaries),
            PermissionSupport::ENS_V2_REGISTRY_PARTIAL
        );
        assert_eq!(
            permission_support_for_resources(&[ens_v2_id, partial_id, wrapper_id], &summaries),
            PermissionSupport::ACCOUNT_WIDE_PARTIAL
        );
        assert_eq!(
            permission_support_for_resources(&[partial_id, missing_id], &summaries),
            PermissionSupport::UNKNOWN
        );
    }

    #[test]
    fn role_summary_support_marks_only_the_expansion_non_authoritative() {
        assert_eq!(
            PermissionSupport::WRAPPER_PARTIAL.merge(PermissionSupport::UNKNOWN),
            PermissionSupport::UNKNOWN
        );

        let mut full = Meta::default();
        apply_role_summary_support_meta(&mut full, PermissionSupport::FULL);
        assert_eq!(full, Meta::default());

        let mut wrapper = Meta::default();
        apply_role_summary_support_meta(&mut wrapper, PermissionSupport::WRAPPER_PARTIAL);
        assert_eq!(wrapper.completeness, Some(Completeness::Partial));
        assert_eq!(
            wrapper.unsupported_fields,
            Some(vec!["role_summary".to_owned()])
        );
        assert_eq!(
            wrapper.unsupported_reason.as_deref(),
            Some(PERMISSIONS_PARTIALLY_LISTED_REASON)
        );
        assert_eq!(
            wrapper.unlisted_permission_surfaces,
            Some(vec![ResolverApprovals, WrapperParentControl])
        );

        let mut unknown = Meta::default();
        apply_role_summary_support_meta(&mut unknown, PermissionSupport::UNKNOWN);
        assert_eq!(
            unknown.unsupported_fields,
            Some(vec!["role_summary".to_owned()])
        );
        assert_eq!(
            unknown.unsupported_reason.as_deref(),
            Some(PERMISSION_SUPPORT_UNKNOWN_REASON)
        );
        assert_eq!(unknown.unlisted_permission_surfaces, None);
    }
}
