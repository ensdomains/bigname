use std::collections::BTreeMap;

use bigname_storage::{PermissionCoverageStatus, PermissionsCurrentResourceSummary};
use sqlx::types::Uuid;

use super::{Completeness, Meta};

const PERMISSION_SUPPORT_UNKNOWN_REASON: &str = "permission_support_unknown";
const WRAPPER_HOLDER_PERMISSIONS_NOT_SUPPORTED_REASON: &str =
    "wrapper_holder_permissions_not_supported";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionSupport {
    Full,
    WrapperUnsupported,
    Unknown,
}

impl PermissionSupport {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::WrapperUnsupported, _) | (_, Self::WrapperUnsupported) => {
                Self::WrapperUnsupported
            }
            (Self::Full, Self::Full) => Self::Full,
        }
    }

    fn product_reason(self) -> Option<&'static str> {
        match self {
            Self::Full => None,
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
                Some(summary) => match summary.coverage.status() {
                    PermissionCoverageStatus::Full => PermissionSupport::Full,
                    PermissionCoverageStatus::Partial => PermissionSupport::Unknown,
                    PermissionCoverageStatus::Unsupported => PermissionSupport::WrapperUnsupported,
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
        (false, PermissionSupport::Full | PermissionSupport::WrapperUnsupported) => (
            Completeness::Partial,
            WRAPPER_HOLDER_PERMISSIONS_NOT_SUPPORTED_REASON,
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

    #[test]
    fn permission_collection_support_distinguishes_resource_and_account_scope() {
        let mut resource_meta = Meta::default();
        apply_permissions_collection_support_meta(
            &mut resource_meta,
            PermissionSupport::WrapperUnsupported,
            true,
        );
        assert_eq!(resource_meta.completeness, Some(Completeness::Unsupported));
        assert_eq!(
            resource_meta.unsupported_reason.as_deref(),
            Some(WRAPPER_HOLDER_PERMISSIONS_NOT_SUPPORTED_REASON)
        );

        let mut account_meta = Meta::default();
        apply_permissions_collection_support_meta(
            &mut account_meta,
            PermissionSupport::Full,
            false,
        );
        assert_eq!(account_meta.completeness, Some(Completeness::Partial));
        assert_eq!(
            account_meta.unsupported_reason.as_deref(),
            Some(WRAPPER_HOLDER_PERMISSIONS_NOT_SUPPORTED_REASON)
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
