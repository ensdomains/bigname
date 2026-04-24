use super::*;

pub(super) async fn primary_names(
    Path(address): Path<String>,
    Query(query): Query<PrimaryNameQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<PrimaryNameResponse>> {
    let address = parse_primary_name_address(&address)?;
    let namespace = parse_primary_name_namespace(query.namespace.as_deref())?;
    let coin_type = parse_primary_name_coin_type(query.coin_type.as_deref())?;
    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let lookup_state =
        load_primary_name_lookup_state(&state.pool, &address, &namespace, &coin_type, mode).await?;

    Ok(Json(build_primary_name_response(
        address,
        namespace,
        coin_type,
        mode,
        &lookup_state,
    )))
}
