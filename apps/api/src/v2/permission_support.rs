use std::collections::BTreeMap;

use bigname_storage::{
    PermissionCoverageStatus, PermissionCoverageUnsupportedReason,
    PermissionsCurrentResourceSummary,
};
use sqlx::types::Uuid;

use super::{Completeness, Meta};

const PERMISSION_SUPPORT_UNKNOWN_REASON: &str = "permission_support_unknown";
const WRAPPER_HOLDER_PERMISSIONS_NOT_SUPPORTED_REASON: &str =
    "wrapper_holder_permissions_not_supported";
const APPROVAL_AND_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON: &str =
    "approval_and_delegation_permissions_not_supported";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionSupport {
    Full,
    ApprovalDelegationPartial,
    WrapperUnsupported,
    Unknown,
}

impl PermissionSupport {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::ApprovalDelegationPartial, _) | (_, Self::ApprovalDelegationPartial) => {
                Self::ApprovalDelegationPartial
            }
            (Self::WrapperUnsupported, _) | (_, Self::WrapperUnsupported) => {
                Self::WrapperUnsupported
            }
            (Self::Full, Self::Full) => Self::Full,
        }
    }

    fn product_reason(self) -> Option<&'static str> {
        match self {
            Self::Full => None,
            Self::ApprovalDelegationPartial => {
                Some(APPROVAL_AND_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON)
            }
            Self::WrapperUnsupported => Some(WRAPPER_HOLDER_PERMISSIONS_NOT_SUPPORTED_REASON),
            Self::Unknown => Some(PERMISSION_SUPPORT_UNKNOWN_REASON),
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
                    ) => PermissionSupport::ApprovalDelegationPartial,
                    (
                        PermissionCoverageStatus::Unsupported,
                        Some(
                            PermissionCoverageUnsupportedReason::Ensv1WrapperHolderPermissionsNotProjected,
                        ),
                    ) => PermissionSupport::WrapperUnsupported,
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
    resource_bound: bool,
) {
    let (completeness, reason) = match (resource_bound, support) {
        (true, PermissionSupport::Full) => return,
        (true, PermissionSupport::ApprovalDelegationPartial) => (
            Completeness::Partial,
            APPROVAL_AND_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON,
        ),
        (true, PermissionSupport::WrapperUnsupported) => (
            Completeness::Unsupported,
            WRAPPER_HOLDER_PERMISSIONS_NOT_SUPPORTED_REASON,
        ),
        (true, PermissionSupport::Unknown) => {
            (Completeness::Partial, PERMISSION_SUPPORT_UNKNOWN_REASON)
        }
        (false, PermissionSupport::Unknown) => {
            (Completeness::Partial, PERMISSION_SUPPORT_UNKNOWN_REASON)
        }
        (
            false,
            PermissionSupport::Full
            | PermissionSupport::ApprovalDelegationPartial
            | PermissionSupport::WrapperUnsupported,
        ) => (
            Completeness::Partial,
            APPROVAL_AND_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON,
        ),
    };

    meta.completeness = Some(completeness);
    meta.unsupported_reason = Some(reason.to_owned());
}

pub(crate) fn apply_role_summary_support_meta(meta: &mut Meta, support: PermissionSupport) {
    let Some(reason) = support.product_reason() else {
        return;
    };

    meta.completeness = Some(Completeness::Partial);
    meta.unsupported_fields = Some(vec!["role_summary".to_owned()]);
    meta.unsupported_reason = Some(reason.to_owned());
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
            PermissionSupport::WrapperUnsupported,
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
            PermissionSupport::WrapperUnsupported.merge(PermissionSupport::Unknown),
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
