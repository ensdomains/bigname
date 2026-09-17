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
    /// BaseRegistrar ERC-721 per-token and operator approvals.
    RegistrarApprovals,
    /// Resolver operator approvals and per-name delegates.
    ResolverApprovals,
    /// The parent name's control over a wrapped subname that is not emancipated.
    WrapperParentControl,
}

use UnlistedPermissionSurface::{RegistrarApprovals, ResolverApprovals, WrapperParentControl};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionRequestScope {
    ResourceBound,
    AccountWide,
}

/// Which permission surfaces the served rows leave unlisted. A set of registrations reports the
/// union of its members' unlisted surfaces; indeterminate support takes precedence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionSupport {
    Full,
    /// Registrar- and registry-held rows with effective registry operators.
    RegistrarResolverPartial,
    /// NameWrapper holders, operators, and per-token delegates are rows.
    WrapperPartial,
    /// A set that mixes NameWrapper and non-wrapper registrations, or an account-wide read.
    RegistrarResolverWrapperPartial,
    Unknown,
}

impl PermissionSupport {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::Full, support) | (support, Self::Full) => support,
            (left, right) if left == right => left,
            _ => Self::RegistrarResolverWrapperPartial,
        }
    }

    fn unlisted_surfaces(self) -> &'static [UnlistedPermissionSurface] {
        match self {
            Self::Full | Self::Unknown => &[],
            Self::RegistrarResolverPartial => &[RegistrarApprovals, ResolverApprovals],
            Self::WrapperPartial => &[ResolverApprovals, WrapperParentControl],
            Self::RegistrarResolverWrapperPartial => {
                &[RegistrarApprovals, ResolverApprovals, WrapperParentControl]
            }
        }
    }

    /// Returns whether any completeness metadata was written.
    fn apply(self, meta: &mut Meta) -> bool {
        let reason = match self {
            Self::Full => return false,
            Self::Unknown => PERMISSION_SUPPORT_UNKNOWN_REASON,
            _ => PERMISSIONS_PARTIALLY_LISTED_REASON,
        };
        meta.completeness = Some(Completeness::Partial);
        meta.unsupported_reason = Some(reason.to_owned());
        let surfaces = self.unlisted_surfaces();
        meta.unlisted_permission_surfaces = (!surfaces.is_empty()).then(|| surfaces.to_vec());
        true
    }
}

pub(crate) fn permission_support_for_resources(
    resource_ids: &[Uuid],
    summaries: &BTreeMap<Uuid, PermissionsCurrentResourceSummary>,
) -> PermissionSupport {
    resource_ids
        .iter()
        .map(|resource_id| {
            match summaries.get(resource_id) {
                Some(summary) => match (
                    summary.coverage.status(),
                    summary.coverage.unsupported_reason(),
                ) {
                    (PermissionCoverageStatus::Full, None) => PermissionSupport::Full,
                    (
                        PermissionCoverageStatus::Partial,
                        Some(
                            PermissionCoverageUnsupportedReason::OperatorApprovalSurfacesNotIngested,
                        ),
                    ) => PermissionSupport::RegistrarResolverPartial,
                    (
                        PermissionCoverageStatus::Partial,
                        Some(
                            PermissionCoverageUnsupportedReason::WrapperParentAndResolverDelegationNotProjected,
                        ),
                    ) => PermissionSupport::WrapperPartial,
                    _ => PermissionSupport::Unknown,
                },
                None => PermissionSupport::Unknown,
            }
        })
        .reduce(PermissionSupport::merge)
        .unwrap_or(PermissionSupport::Full)
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
            support.merge(PermissionSupport::RegistrarResolverWrapperPartial)
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
        let full = collection_meta(PermissionSupport::Full, resource);
        assert_eq!(full, Meta::default());

        let registrar = collection_meta(PermissionSupport::RegistrarResolverPartial, resource);
        assert_eq!(registrar.completeness, Some(Completeness::Partial));
        assert_eq!(
            registrar.unsupported_reason.as_deref(),
            Some(PERMISSIONS_PARTIALLY_LISTED_REASON)
        );
        assert_eq!(
            serde_json::to_value(&registrar.unlisted_permission_surfaces).unwrap(),
            json!(["registrar_approvals", "resolver_approvals"])
        );

        let wrapper = collection_meta(PermissionSupport::WrapperPartial, resource);
        assert_eq!(
            serde_json::to_value(&wrapper.unlisted_permission_surfaces).unwrap(),
            json!(["resolver_approvals", "wrapper_parent_control"])
        );

        for support in [PermissionSupport::Full, PermissionSupport::WrapperPartial] {
            let account = collection_meta(support, PermissionRequestScope::AccountWide);
            assert_eq!(account.completeness, Some(Completeness::Partial));
            assert_eq!(
                serde_json::to_value(&account.unlisted_permission_surfaces).unwrap(),
                json!([
                    "registrar_approvals",
                    "resolver_approvals",
                    "wrapper_parent_control"
                ])
            );
        }

        for scope in [resource, PermissionRequestScope::AccountWide] {
            let unknown = collection_meta(PermissionSupport::Unknown, scope);
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
        let summaries = BTreeMap::from([
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
            PermissionSupport::Full
        );
        assert_eq!(
            permission_support_for_resources(&[wrapper_id], &summaries),
            PermissionSupport::WrapperPartial
        );
        assert_eq!(
            permission_support_for_resources(&[full_id, wrapper_id], &summaries),
            PermissionSupport::WrapperPartial
        );
        assert_eq!(
            permission_support_for_resources(&[full_id, partial_id], &summaries),
            PermissionSupport::RegistrarResolverPartial
        );
        assert_eq!(
            permission_support_for_resources(&[wrapper_id, partial_id], &summaries),
            PermissionSupport::RegistrarResolverWrapperPartial
        );
        assert_eq!(
            permission_support_for_resources(&[partial_id, missing_id], &summaries),
            PermissionSupport::Unknown
        );
    }

    #[test]
    fn role_summary_support_marks_only_the_expansion_non_authoritative() {
        assert_eq!(
            PermissionSupport::WrapperPartial.merge(PermissionSupport::Unknown),
            PermissionSupport::Unknown
        );

        let mut full = Meta::default();
        apply_role_summary_support_meta(&mut full, PermissionSupport::Full);
        assert_eq!(full, Meta::default());

        let mut wrapper = Meta::default();
        apply_role_summary_support_meta(&mut wrapper, PermissionSupport::WrapperPartial);
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
        apply_role_summary_support_meta(&mut unknown, PermissionSupport::Unknown);
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
