use super::*;

pub(super) async fn name_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!("failed to load current projection for name {namespace}/{name}"),
        )
    })?;
    let row =
        load_name_current_for_selected_snapshot(&state.pool, &namespace, &name, &selected_snapshot)
            .await
            .map_err(|error| {
                map_internal_api_error(
                    error,
                    format!("failed to load current projection for name {namespace}/{name}"),
                )
            })?;

    let record_inventory_current =
        load_supported_record_inventory_current_for_snapshot(&state.pool, &row, &selected_snapshot)
            .await
            .map_err(snapshot_selection_api_error)?;

    Ok(Json(build_name_response(
        row,
        record_inventory_current.as_ref(),
        &selected_snapshot,
    )))
}

pub(super) async fn coverage_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!("failed to load current projection for name {namespace}/{name}"),
        )
    })?;
    let row =
        load_name_current_for_selected_snapshot(&state.pool, &namespace, &name, &selected_snapshot)
            .await
            .map_err(|error| {
                map_internal_api_error(
                    error,
                    format!("failed to load current projection for name {namespace}/{name}"),
                )
            })?;

    Ok(Json(build_name_coverage_response(row, &selected_snapshot)))
}

pub(super) async fn explain_surface_binding_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!(
                "failed to load surface-binding explain projection for name {namespace}/{name}"
            ),
        )
    })?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!(
                "failed to load surface-binding explain projection for name {namespace}/{name}"
            ),
        )
    })?;

    Ok(Json(build_name_surface_binding_explain_response(
        row,
        &selected_snapshot,
    )))
}

pub(super) async fn explain_authority_control_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!(
                "failed to load authority-control explain projection for name {namespace}/{name}"
            ),
        )
    })?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!(
                "failed to load authority-control explain projection for name {namespace}/{name}"
            ),
        )
    })?;

    Ok(Json(build_name_authority_control_explain_response(
        row,
        &selected_snapshot,
    )))
}
