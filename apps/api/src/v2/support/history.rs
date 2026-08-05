use super::*;

pub(crate) async fn resource_ids_for_name(
    pool: &PgPool,
    logical_name_id: &str,
) -> ApiResult<Vec<Uuid>> {
    let bindings = load_surface_bindings_by_logical_name_id(pool, logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load surface bindings for name history"
            );
            ApiError::internal_error(format!(
                "failed to load history bindings for logical name {logical_name_id}"
            ))
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}
