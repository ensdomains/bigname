use super::*;

pub(super) async fn health(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let database_reachable = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await;

    let (http_status, status, database) = match database_reachable {
        Ok(_) => (
            StatusCode::OK,
            "ready",
            HealthDatabaseResponse {
                status: "reachable",
                reachable: true,
                check: "select_1",
                error: None,
            },
        ),
        Err(readiness_error) => {
            warn!(
                service = "api",
                phase = state.phase,
                error = ?readiness_error,
                "database readiness probe failed"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "degraded",
                HealthDatabaseResponse {
                    status: "unreachable",
                    reachable: false,
                    check: "select_1",
                    error: Some("database readiness query failed"),
                },
            )
        }
    };

    (
        http_status,
        Json(HealthResponse {
            service: "api",
            phase: state.phase,
            status,
            process: HealthProcessResponse { status: "running" },
            database,
        }),
    )
}
