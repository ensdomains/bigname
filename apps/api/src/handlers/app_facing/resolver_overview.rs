use super::*;

pub(super) async fn resolver_overview(
    Path((chain_id, resolver_address)): Path<(String, String)>,
    Query(query): Query<ResolverOverviewQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactResolverOverviewResponse>> {
    let view = parse_response_view(query.view.as_deref(), ResponseView::Compact)?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let include = parse_resolver_overview_include(query.include.as_deref())?;
    let normalized_address = normalize_address(&resolver_address);
    let row = load_resolver_current(&state.pool, &chain_id, &normalized_address)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                chain_id = %chain_id,
                resolver_address = %normalized_address,
                error = ?load_error,
                "failed to load resolver_current projection for compact overview"
            );
            ApiError::internal_error(format!(
                "failed to load resolver projection for chain_id {chain_id} resolver_address {normalized_address}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("resolver {normalized_address} was not found on chain {chain_id}"),
        });
    };

    match view {
        ResponseView::Compact => Ok(Json(build_compact_resolver_overview_response(
            row, include, meta,
        ))),
        ResponseView::Full => serde_json::to_value(build_resolver_response(row))
            .map(Json)
            .map_err(|serialize_error| {
                error!(
                    service = "api",
                    chain_id = %chain_id,
                    resolver_address = %normalized_address,
                    error = ?serialize_error,
                    "failed to serialize full resolver overview response"
                );
                ApiError::internal_error("failed to serialize resolver overview response")
            }),
    }
}
