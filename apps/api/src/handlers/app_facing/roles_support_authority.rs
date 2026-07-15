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

    Ok(match summary.as_ref().and_then(projected_support_status) {
        Some("full")
            if summary
                .as_ref()
                .and_then(|summary| {
                    summary
                        .coverage
                        .get("exhaustiveness")
                        .and_then(JsonValue::as_str)
                })
                == Some("authoritative") =>
        {
            CompactRolesSupport::Supported
        }
        Some("unsupported")
            if summary
                .as_ref()
                .and_then(projected_unsupported_reason)
                == Some(ENSV1_WRAPPER_PERMISSIONS_UNSUPPORTED_REASON) =>
        {
            CompactRolesSupport::WrapperHolderPermissionsUnsupported
        }
        _ => CompactRolesSupport::ResourceAuthorityPartial,
    })
}

fn projected_support_status(
    summary: &bigname_storage::PermissionsCurrentResourceSummary,
) -> Option<&str> {
    summary.coverage.get("status").and_then(JsonValue::as_str)
}

fn projected_unsupported_reason(
    summary: &bigname_storage::PermissionsCurrentResourceSummary,
) -> Option<&str> {
    summary
        .coverage
        .get("unsupported_reason")
        .and_then(JsonValue::as_str)
}
