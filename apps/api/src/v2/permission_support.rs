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
const PARENT_AND_RESOLVER_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON: &str =
    "parent_and_resolver_delegation_permissions_not_supported";
const REGISTRAR_RESOLVER_WRAPPER_PARTIAL_REASON: &str =
    "registrar_erc721_approvals_resolver_approvals_delegates_and_wrapper_permissions_not_supported";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionRequestScope {
    ResourceBound,
    AccountWide,
}

/// How completely the served rows enumerate a registration's permissions. A mixed set of
/// registrations reports the reason that covers every member's absent surfaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionSupport {
    Full,
    /// Registrar- and registry-held rows with effective registry operators; registrar ERC-721
    /// approvals and resolver approvals and delegates are not enumerated.
    RegistrarResolverPartial,
    /// NameWrapper holders, operators, and per-token delegates are enumerated; parent control and
    /// resolver-side delegation are not.
    WrapperPartial,
    /// A set that mixes NameWrapper and non-wrapper registrations.
    RegistrarResolverWrapperPartial,
    Unknown,
}

impl PermissionSupport {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::RegistrarResolverWrapperPartial, _)
            | (_, Self::RegistrarResolverWrapperPartial) => Self::RegistrarResolverWrapperPartial,
            (Self::WrapperPartial, Self::WrapperPartial) => Self::WrapperPartial,
            (Self::WrapperPartial, _) | (_, Self::WrapperPartial) => {
                Self::RegistrarResolverWrapperPartial
            }
            (Self::RegistrarResolverPartial, _) | (_, Self::RegistrarResolverPartial) => {
                Self::RegistrarResolverPartial
            }
            (Self::Full, Self::Full) => Self::Full,
        }
    }

    fn product_reason(self) -> &'static str {
        match self {
            Self::Full | Self::RegistrarResolverPartial => REGISTRAR_RESOLVER_PARTIAL_REASON,
            Self::WrapperPartial => PARENT_AND_RESOLVER_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON,
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
    let reason = match (request_scope, support) {
        (_, PermissionSupport::Unknown) => PERMISSION_SUPPORT_UNKNOWN_REASON,
        // An address-only read cannot enumerate approvals and delegations across every
        // registration the address may reach, whatever each visible registration supports.
        (PermissionRequestScope::AccountWide, _) => REGISTRAR_RESOLVER_WRAPPER_PARTIAL_REASON,
        (PermissionRequestScope::ResourceBound, support) => support.product_reason(),
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
            resource_restrictions: None,
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
            PermissionSupport::WrapperPartial,
            PermissionRequestScope::ResourceBound,
        );
        assert_eq!(resource_meta.completeness, Some(Completeness::Partial));
        assert_eq!(
            resource_meta.unsupported_reason.as_deref(),
            Some(PARENT_AND_RESOLVER_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON)
        );

        let mut mixed_meta = Meta::default();
        apply_permissions_collection_support_meta(
            &mut mixed_meta,
            PermissionSupport::RegistrarResolverWrapperPartial,
            PermissionRequestScope::ResourceBound,
        );
        assert_eq!(
            mixed_meta.unsupported_reason.as_deref(),
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

        let mut wrapper_account_meta = Meta::default();
        apply_permissions_collection_support_meta(
            &mut wrapper_account_meta,
            PermissionSupport::WrapperPartial,
            PermissionRequestScope::AccountWide,
        );
        assert_eq!(
            wrapper_account_meta.unsupported_reason.as_deref(),
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
            PermissionSupport::WrapperPartial.merge(PermissionSupport::Unknown),
            PermissionSupport::Unknown
        );

        let mut wrapper_meta = Meta::default();
        apply_role_summary_support_meta(&mut wrapper_meta, PermissionSupport::WrapperPartial);
        assert_eq!(
            wrapper_meta.unsupported_reason.as_deref(),
            Some(PARENT_AND_RESOLVER_DELEGATION_PERMISSIONS_NOT_SUPPORTED_REASON)
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
