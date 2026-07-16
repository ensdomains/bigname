async fn load_ensv2_root_resource_id_for_name_resource(
    pool: &PgPool,
    resource_id: Uuid,
    route: &'static str,
) -> ApiResult<Option<Uuid>> {
    ensure_permissions_current_projection_available(pool, route).await?;
    let summary = bigname_storage::load_permissions_current_resource_summary(pool, resource_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                route = route,
                resource_id = %resource_id,
                error = ?load_error,
                "failed to load projection-owned ENSv2 root role anchor"
            );
            ApiError::internal_error("failed to load roles support metadata")
        })?;

    Ok(distinct_root_resource_id(
        resource_id,
        summary.and_then(|summary| summary.root_resource_id),
    ))
}

fn distinct_root_resource_id(resource_id: Uuid, root_resource_id: Option<Uuid>) -> Option<Uuid> {
    root_resource_id.filter(|root| *root != resource_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_root_self_anchor_is_not_composed_twice() {
        let root_resource_id = Uuid::from_u128(0xe201);

        assert_eq!(
            distinct_root_resource_id(root_resource_id, Some(root_resource_id)),
            None
        );
        assert_eq!(
            distinct_root_resource_id(Uuid::from_u128(0xe202), Some(root_resource_id)),
            Some(root_resource_id)
        );
    }
}
