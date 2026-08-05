use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PermissionsCurrentReadToken {
    data_revision: i64,
}

pub(crate) async fn begin_permissions_current_read(
    pool: &PgPool,
    route: &'static str,
) -> ApiResult<PermissionsCurrentReadToken> {
    let data_revision = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT data_revision
        FROM permissions_current_publication
        WHERE projection = 'permissions_current'
          AND publication_version = $1
        "#,
    )
    .bind(bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION)
    .fetch_optional(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            route = route,
            error = ?load_error,
            "failed to check permissions_current publication compatibility"
        );
        ApiError::internal_error("failed to check permission projection compatibility")
    })?;

    data_revision
        .map(|data_revision| PermissionsCurrentReadToken { data_revision })
        .ok_or_else(incompatible_permission_publication)
}

pub(crate) async fn finish_permissions_current_read(
    pool: &PgPool,
    route: &'static str,
    token: PermissionsCurrentReadToken,
) -> ApiResult<()> {
    let current = begin_permissions_current_read(pool, route).await?;
    if current == token {
        return Ok(());
    }

    Err(ApiError {
        status: StatusCode::CONFLICT,
        code: "stale",
        message: "permissions_current projection changed during the request".to_owned(),
    })
}

fn incompatible_permission_publication() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "stale",
        message: "permissions_current projection publication is not compatible".to_owned(),
    }
}
