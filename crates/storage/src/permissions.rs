mod resource_summary;
mod serving;
mod types;

pub use resource_summary::{
    load_registry_permission_registration_map, permission_resource_matches_namespace,
    resource_is_registry_control_for_registrar_lease, resource_wrapped_a_registrar_lease,
};
pub use serving::{load_serving_effective_permissions_page, load_serving_permission_summaries};
pub use types::{
    EffectivePermissionRow, EffectivePermissionScope, EffectivePermissionsAccountResourcePage,
    PermissionCoverageExhaustiveness, PermissionCoverageStatus,
    PermissionCoverageUnsupportedReason, PermissionGrantRelation, PermissionScope,
    PermissionsCurrentAccountResourceCursor, PermissionsCurrentAccountResourcePage,
    PermissionsCurrentFullFilterSummary, PermissionsCurrentKeysetCursor, PermissionsCurrentPage,
    PermissionsCurrentResourceSummary, PermissionsCurrentRow, ResourcePermissionCoverage,
};

/// Bounded inline permission expansion using the published family state.
pub async fn load_bounded_effective_permissions_by_resource_ids(
    pool: &sqlx::PgPool,
    ids: &[uuid::Uuid],
    namespace: Option<&str>,
    max_rows: u64,
) -> anyhow::Result<Vec<EffectivePermissionRow>> {
    crate::families::control::permissions::page::load_family_bounded_permissions(
        pool, ids, namespace, max_rows,
    )
    .await
}
