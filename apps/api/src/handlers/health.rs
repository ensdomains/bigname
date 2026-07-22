use super::*;

pub(crate) const HEALTH_DATABASE_CHECK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct HealthDatabasePool(pub(crate) PgPool);

pub(super) async fn health(
    axum::Extension(health_pool): axum::Extension<HealthDatabasePool>,
) -> (StatusCode, Json<HealthResponse>) {
    let database_reachable = match tokio::time::timeout(
        HEALTH_DATABASE_CHECK_TIMEOUT,
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&health_pool.0),
    )
    .await
    {
        Ok(Ok(_)) => true,
        Ok(Err(readiness_error)) => {
            warn!(
                service = "api",
                build_sha = BUILD_SHA,
                error = ?readiness_error,
                "database readiness probe failed"
            );
            false
        }
        Err(_) => {
            warn!(
                service = "api",
                build_sha = BUILD_SHA,
                timeout_ms = HEALTH_DATABASE_CHECK_TIMEOUT.as_millis(),
                "database readiness probe timed out"
            );
            false
        }
    };

    let (http_status, status, database) = match database_reachable {
        true => (
            StatusCode::OK,
            "ready",
            HealthDatabaseResponse {
                status: "reachable",
                reachable: true,
                check: "select_1",
                error: None,
            },
        ),
        false => (
            StatusCode::SERVICE_UNAVAILABLE,
            "degraded",
            HealthDatabaseResponse {
                status: "unreachable",
                reachable: false,
                check: "select_1",
                error: Some("database readiness query failed"),
            },
        ),
    };

    (
        http_status,
        Json(HealthResponse {
            service: "api",
            identity: HealthIdentityResponse::current(),
            status,
            process: HealthProcessResponse { status: "running" },
            database,
        }),
    )
}
