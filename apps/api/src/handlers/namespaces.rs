use super::*;

pub(super) async fn namespace_metadata(
    Path(namespace): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<NamespaceMetadataResponse>> {
    ensure_public_namespace(&namespace)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load namespace metadata"
            );
            ApiError::internal_error(format!(
                "failed to load namespace metadata for namespace {namespace}"
            ))
        })?;

    Ok(Json(build_namespace_metadata_response(namespace, snapshot)))
}

pub(super) async fn namespace_manifests(
    Path(namespace): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<NamespaceManifestsResponse>> {
    ensure_public_namespace(&namespace)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load manifest snapshot for namespace"
            );
            ApiError::internal_error(format!(
                "failed to load manifest snapshot for namespace {namespace}"
            ))
        })?;

    Ok(Json(build_namespace_manifests_response(
        namespace, snapshot,
    )))
}
