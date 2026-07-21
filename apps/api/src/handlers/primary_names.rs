use super::*;

pub(super) async fn primary_names(
    Path(address): Path<String>,
    Query(query): Query<PrimaryNameQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<PrimaryNameResponse>> {
    let address = parse_primary_name_address(&address)?;
    let namespace = parse_primary_name_namespace(query.namespace.as_deref().or(Some("ens")))?;
    let coin_type = parse_primary_name_coin_type(query.coin_type.as_deref().or(Some("60")))?;
    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let read = load_primary_name_route_read(&state, &address, &namespace, &coin_type, mode).await?;

    Ok(Json(build_primary_name_response(
        address,
        namespace,
        coin_type,
        mode,
        &read.lookup_state,
        read.selected_snapshot.as_ref(),
    )))
}
