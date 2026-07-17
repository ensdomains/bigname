use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PermissionsCurrentReadToken {
    data_revision: i64,
}

pub(super) async fn begin_permissions_current_read(
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

pub(super) async fn finish_permissions_current_read(
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

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use super::{ApiError, ApiResult};

    #[derive(Clone)]
    pub(crate) struct PermissionReadInterleaveHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct PermissionReadInterleaveControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl PermissionReadInterleaveControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, PermissionReadInterleaveHook> =
        ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, PermissionReadInterleaveHook>,
        PermissionReadInterleaveControl,
    )> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            PermissionReadInterleaveHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, PermissionReadInterleaveControl { reached, resume }))
    }

    pub(crate) async fn run(pool: &PgPool) -> ApiResult<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| ApiError::internal_error("failed to run permission read test hook"))?;
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}
