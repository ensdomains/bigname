use std::collections::BTreeMap;

use bigname_storage::{
    PermissionCoverageStatus, PermissionCoverageUnsupportedReason,
    PermissionsCurrentResourceSummary,
};
use sqlx::types::Uuid;

use super::{Completeness, Meta};

const PERMISSION_SUPPORT_UNKNOWN_REASON: &str = "permission_support_unknown";
const REGISTRAR_RESOLVER_PARTIAL_REASON: &str =
    "registrar_erc721_approvals_and_resolver_approvals_delegates_not_supported";
const REGISTRAR_RESOLVER_WRAPPER_PARTIAL_REASON: &str =
    "registrar_erc721_approvals_resolver_approvals_delegates_and_wrapper_permissions_not_supported";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionRequestScope {
    ResourceBound,
    AccountWide,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionSupport {
    Full,
    RegistrarResolverPartial,
    RegistrarResolverWrapperPartial,
    Unknown,
}

impl PermissionSupport {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::RegistrarResolverWrapperPartial, _)
            | (_, Self::RegistrarResolverWrapperPartial) => Self::RegistrarResolverWrapperPartial,
            (Self::RegistrarResolverPartial, _) | (_, Self::RegistrarResolverPartial) => {
                Self::RegistrarResolverPartial
            }
            (Self::Full, Self::Full) => Self::Full,
        }
    }

    fn product_reason(self) -> &'static str {
        match self {
            Self::Full | Self::RegistrarResolverPartial => REGISTRAR_RESOLVER_PARTIAL_REASON,
            Self::RegistrarResolverWrapperPartial => REGISTRAR_RESOLVER_WRAPPER_PARTIAL_REASON,
            Self::Unknown => PERMISSION_SUPPORT_UNKNOWN_REASON,
        }
    }
}

pub(crate) fn permission_support_for_resources(
    resource_ids: &[Uuid],
    summaries: &BTreeMap<Uuid, PermissionsCurrentResourceSummary>,
) -> PermissionSupport {
    resource_ids
        .iter()
        .fold(PermissionSupport::Full, |support, resource_id| {
            let resource_support = match summaries.get(resource_id) {
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
                        PermissionCoverageStatus::Unsupported,
                        Some(
                            PermissionCoverageUnsupportedReason::Ensv1WrapperHolderPermissionsNotProjected,
                        ),
                    ) => PermissionSupport::RegistrarResolverWrapperPartial,
                    _ => PermissionSupport::Unknown,
                },
                None => PermissionSupport::Unknown,
            };
            support.merge(resource_support)
        })
}

pub(crate) fn apply_permissions_collection_support_meta(
    meta: &mut Meta,
    support: PermissionSupport,
    request_scope: PermissionRequestScope,
) {
    let reason = match (request_scope, support) {
        (_, PermissionSupport::Unknown) => PERMISSION_SUPPORT_UNKNOWN_REASON,
        (PermissionRequestScope::AccountWide, _)
        | (_, PermissionSupport::RegistrarResolverWrapperPartial) => {
            REGISTRAR_RESOLVER_WRAPPER_PARTIAL_REASON
        }
        (PermissionRequestScope::ResourceBound, _) => REGISTRAR_RESOLVER_PARTIAL_REASON,
    };
    meta.completeness = Some(Completeness::Partial);
    meta.unsupported_reason = Some(reason.to_owned());
}

pub(crate) fn apply_role_summary_support_meta(meta: &mut Meta, support: PermissionSupport) {
    meta.completeness = Some(Completeness::Partial);
    meta.unsupported_fields = Some(vec!["role_summary".to_owned()]);
    meta.unsupported_reason = Some(support.product_reason().to_owned());
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
            provenance: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn permission_collection_support_distinguishes_resource_and_account_scope() {
        let mut resource_meta = Meta::default();
        apply_permissions_collection_support_meta(
            &mut resource_meta,
            PermissionSupport::RegistrarResolverWrapperPartial,
            PermissionRequestScope::ResourceBound,
        );
        assert_eq!(resource_meta.completeness, Some(Completeness::Partial));
        assert_eq!(
            resource_meta.unsupported_reason.as_deref(),
            Some(REGISTRAR_RESOLVER_WRAPPER_PARTIAL_REASON)
        );

        let mut account_meta = Meta::default();
        apply_permissions_collection_support_meta(
            &mut account_meta,
            PermissionSupport::Full,
            PermissionRequestScope::AccountWide,
        );
        assert_eq!(account_meta.completeness, Some(Completeness::Partial));
        assert_eq!(
            account_meta.unsupported_reason.as_deref(),
            Some(REGISTRAR_RESOLVER_WRAPPER_PARTIAL_REASON)
        );
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
                    ResourcePermissionCoverage::ensv1_wrapper_holder_permissions_not_projected(),
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
            permission_support_for_resources(&[full_id, wrapper_id], &summaries),
            PermissionSupport::RegistrarResolverWrapperPartial
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
            PermissionSupport::RegistrarResolverWrapperPartial.merge(PermissionSupport::Unknown),
            PermissionSupport::Unknown
        );

        let mut meta = Meta::default();
        apply_role_summary_support_meta(&mut meta, PermissionSupport::Unknown);

        assert_eq!(meta.completeness, Some(Completeness::Partial));
        assert_eq!(
            meta.unsupported_fields,
            Some(vec!["role_summary".to_owned()])
        );
        assert_eq!(
            meta.unsupported_reason.as_deref(),
            Some(PERMISSION_SUPPORT_UNKNOWN_REASON)
        );
    }
}
