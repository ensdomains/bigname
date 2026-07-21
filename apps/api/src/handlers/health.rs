use super::*;

pub(super) async fn health(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let database_reachable = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await;

    let (database, loops, loops_ready) = match database_reachable {
        Ok(_) => {
            let database = HealthDatabaseResponse {
                status: "reachable",
                reachable: true,
                check: "select_1",
                error: None,
            };
            match bigname_storage::load_latest_service_loop_heartbeats(
                &state.pool,
                &[
                    bigname_storage::INDEXER_SERVICE_NAME,
                    bigname_storage::WORKER_SERVICE_NAME,
                ],
            )
            .await
            {
                Ok(heartbeats) => {
                    let indexer = loop_health_response(
                        heartbeats.iter().find(|heartbeat| {
                            heartbeat.service_name == bigname_storage::INDEXER_SERVICE_NAME
                        }),
                        state.heartbeat_max_age_secs,
                    );
                    let worker = loop_health_response(
                        heartbeats.iter().find(|heartbeat| {
                            heartbeat.service_name == bigname_storage::WORKER_SERVICE_NAME
                        }),
                        state.heartbeat_max_age_secs,
                    );
                    let loops_ready = indexer.status == "running" && worker.status == "running";
                    (database, HealthLoopsResponse { indexer, worker }, loops_ready)
                }
                Err(readiness_error) => {
                    warn!(
                        service = "api",
                        build_sha = BUILD_SHA,
                        error = ?readiness_error,
                        "service loop heartbeat readiness probe failed"
                    );
                    (
                        database,
                        unavailable_loop_health(state.heartbeat_max_age_secs),
                        false,
                    )
                }
            }
        }
        Err(readiness_error) => {
            warn!(
                service = "api",
                build_sha = BUILD_SHA,
                error = ?readiness_error,
                "database readiness probe failed"
            );
            let database =
                HealthDatabaseResponse {
                    status: "unreachable",
                    reachable: false,
                    check: "select_1",
                    error: Some("database readiness query failed"),
                };
            (
                database,
                unavailable_loop_health(state.heartbeat_max_age_secs),
                false,
            )
        }
    };
    let ready = database.reachable && loops_ready;
    let http_status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let status = if ready { "ready" } else { "degraded" };

    (
        http_status,
        Json(HealthResponse {
            service: "api",
            identity: HealthIdentityResponse::current(),
            status,
            process: HealthProcessResponse { status: "running" },
            database,
            loops,
        }),
    )
}

fn loop_health_response(
    heartbeat: Option<&bigname_storage::ServiceLoopHeartbeat>,
    max_age_seconds: i64,
) -> HealthLoopResponse {
    let Some(heartbeat) = heartbeat else {
        return HealthLoopResponse {
            status: "not_started",
            started_at: None,
            heartbeat_at: None,
            heartbeat_age_seconds: None,
            max_age_seconds,
        };
    };
    HealthLoopResponse {
        status: if heartbeat.age_seconds <= max_age_seconds {
            "running"
        } else {
            "stale"
        },
        started_at: Some(format_timestamp(heartbeat.started_at)),
        heartbeat_at: Some(format_timestamp(heartbeat.heartbeat_at)),
        heartbeat_age_seconds: Some(heartbeat.age_seconds),
        max_age_seconds,
    }
}

fn unavailable_loop_health(max_age_seconds: i64) -> HealthLoopsResponse {
    let unavailable = || HealthLoopResponse {
        status: "unavailable",
        started_at: None,
        heartbeat_at: None,
        heartbeat_age_seconds: None,
        max_age_seconds,
    };
    HealthLoopsResponse {
        indexer: unavailable(),
        worker: unavailable(),
    }
}
