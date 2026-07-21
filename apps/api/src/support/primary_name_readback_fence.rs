use super::*;

pub(super) async fn load_persisted_primary_name_verified_readback(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<Option<PersistedPrimaryNameVerifiedReadback>> {
    let mut connection = pool.acquire().await.map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            error = ?load_error,
            "failed to acquire persisted verified primary-name readback connection"
        );
        ApiError::internal_error(format!(
            "failed to load persisted verified primary-name outcome for address {address}"
        ))
    })?;
    load_persisted_primary_name_verified_readback_from_connection(
        &mut connection,
        address,
        namespace,
        coin_type,
        PrimaryNameVerifiedReadbackFence::ProjectedClaim,
    )
    .await
}

pub(super) async fn load_persisted_primary_name_route_fallback_readback(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    selected_snapshot: &SelectedSnapshot,
) -> ApiResult<Option<PersistedPrimaryNameVerifiedReadback>> {
    let mut transaction = pool.begin().await.map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            error = ?load_error,
            "failed to open persisted route-local primary-name readback transaction"
        );
        ApiError::internal_error(format!(
            "failed to load persisted verified primary-name outcome for address {address}"
        ))
    })?;
    // A missing tuple cannot be row-locked. Use the same short table-level
    // fence as route-local persistence so projection writes cannot cross the
    // absence check and trace readback as two different database states.
    sqlx::query("LOCK TABLE primary_names_current IN SHARE MODE")
        .execute(&mut *transaction)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to lock route-local primary-name readback fence"
            );
            ApiError::internal_error(format!(
                "failed to load persisted verified primary-name outcome for address {address}"
            ))
        })?;
    let anchor_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM primary_names_current
            WHERE address = $1
              AND namespace = $2
              AND coin_type = $3
        )
        "#,
    )
    .bind(address)
    .bind(namespace)
    .bind(coin_type)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            error = ?load_error,
            "failed to check route-local primary-name readback fence"
        );
        ApiError::internal_error(format!(
            "failed to load persisted verified primary-name outcome for address {address}"
        ))
    })?;
    let readback = if anchor_exists {
        None
    } else {
        load_persisted_primary_name_verified_readback_from_connection(
            &mut transaction,
            address,
            namespace,
            coin_type,
            PrimaryNameVerifiedReadbackFence::RouteLocalFallback(selected_snapshot),
        )
        .await?
    };
    transaction.commit().await.map_err(|commit_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            error = ?commit_error,
            "failed to commit persisted route-local primary-name readback transaction"
        );
        ApiError::internal_error(format!(
            "failed to load persisted verified primary-name outcome for address {address}"
        ))
    })?;
    Ok(readback)
}
