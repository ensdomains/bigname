pub(super) async fn explain_resolution_execution_current(
    Path((namespace, name)): Path<(String, String)>,
    query: std::result::Result<Query<ResolutionExecutionExplainQuery>, QueryRejection>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    let Query(query) = query.map_err(|rejection| {
        error!(
            service = "api",
            error = ?rejection,
            "rejected invalid resolution execution explain query parameters"
        );
        ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "query parameters are invalid".to_owned(),
        }
    })?;
    if query.at.is_some() || query.chain_positions.is_some() || query.consistency.is_some() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "query parameters are invalid".to_owned(),
        });
    }
    let name = parse_exact_name_path_name(&namespace, &name)?;

    let records = parse_resolution_record_keys(query.records.as_deref(), ResolutionMode::Verified)?;
    let logical_name_id = format!("{namespace}:{name}");
    let ExactNameRead {
        row,
        selected_snapshot,
    } = load_exact_name_read_for_route(
        &state.pool,
        ExactNameReadRequest::new(&namespace, &name, ExactNameSnapshotSelector::default())
            .include_resolution_auxiliary(namespace == BASENAMES_NAMESPACE)
            .with_projection_kind("resolution execution explain"),
    )
    .await?;

    let record_inventory_current = load_record_inventory_current_for_route_snapshot(
        &state.pool,
        &row,
        true,
        &selected_snapshot,
    )
    .await
    .map_err(|load_error| {
        let api_error = snapshot_selection_api_error(load_error);
        error!(
            service = "api",
            namespace = %namespace,
            name = %name,
            logical_name_id = %logical_name_id,
            records = ?records,
            status = %api_error.status,
            code = %api_error.code,
            message = %api_error.message,
            "failed to load declared record inventory for resolution execution explain route"
        );
        map_internal_api_error(
            api_error,
            format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ),
        )
    })?;

    if resolution_verified_support_boundary(&row, record_inventory_current.as_ref()).is_none()
        || bigname_storage::supported_resolution_verified_readback_records(&row, &records).len()
            != records.len()
    {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!(
                "persisted resolution execution explain was not found for name {name} in namespace {namespace}"
            ),
        });
    }
    let outcome = lookup_resolution_verified_outcome(
        &state.pool,
        &row,
        &records,
        record_inventory_current.as_ref(),
        &selected_snapshot,
        PartialCompactHits::TreatAsMiss,
    )
        .await
        .map_err(|load_error| {
            let api_error = snapshot_selection_api_error(load_error);
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                records = ?records,
                status = %api_error.status,
                code = %api_error.code,
                message = %api_error.message,
                "failed to load persisted execution outcome for resolution execution explain route"
            );
            map_internal_api_error(
                api_error,
                format!(
                    "failed to load resolution execution explain projection for name {namespace}/{name}"
                ),
            )
        })?;

    let outcome = match outcome {
        ResolutionVerifiedOutcomeLookup::Found(outcome) => outcome,
        ResolutionVerifiedOutcomeLookup::CacheMiss
        | ResolutionVerifiedOutcomeLookup::NotSupported => return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!(
                "persisted resolution execution explain was not found for name {name} in namespace {namespace}"
            ),
        }),
    };

    let trace = load_execution_trace(&state.pool, outcome.execution_trace_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                execution_trace_id = %outcome.execution_trace_id,
                error = ?load_error,
                "failed to load persisted execution trace for resolution execution explain route"
            );
            ApiError::internal_error(format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ))
        })?;

    let Some(trace) = trace else {
        return Err(ApiError::internal_error(format!(
            "failed to load resolution execution explain projection for name {namespace}/{name}"
        )));
    };

    let response = build_resolution_execution_explain_response(
        row,
        &records,
        &trace,
        &outcome,
        &selected_snapshot,
    )
    .map_err(|build_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                execution_trace_id = %outcome.execution_trace_id,
                error = ?build_error,
                "failed to build resolution execution explain response"
            );
            ApiError::internal_error(format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ))
        })?;

    Ok(Json(response))
}
