async fn compact_roles_support_for_resource(
    pool: &PgPool,
    resource_id: Option<Uuid>,
    route: &'static str,
) -> ApiResult<CompactRolesSupport> {
    let Some(resource_id) = resource_id else {
        return Ok(CompactRolesSupport::AccountWideWrapperCoveragePartial);
    };
    let summary = bigname_storage::load_permissions_current_resource_summary(pool, resource_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                route = route,
                resource_id = %resource_id,
                error = ?load_error,
                "failed to load projection-owned roles support metadata"
            );
            ApiError::internal_error("failed to load roles support metadata")
        })?;

    Ok(match summary.map(|summary| summary.coverage.status()) {
        Some(bigname_storage::PermissionCoverageStatus::Full) => CompactRolesSupport::Supported,
        Some(bigname_storage::PermissionCoverageStatus::Partial) | None => {
            CompactRolesSupport::ResourceAuthorityPartial
        }
        Some(bigname_storage::PermissionCoverageStatus::Unsupported) => {
            CompactRolesSupport::WrapperHolderPermissionsUnsupported
        }
    })
}
