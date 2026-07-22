use super::*;

pub(crate) const HEALTH_DATABASE_CHECK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct HealthDatabasePool(pub(crate) PgPool);

pub(super) async fn health(
    State(state): State<AppState>,
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

    let (database, loops, loops_ready) = match database_reachable {
        true => {
            let database = HealthDatabaseResponse {
                status: "reachable",
                reachable: true,
                check: "select_1",
                error: None,
            };
            match bigname_storage::load_preferred_service_loop_heartbeats(
                &health_pool.0,
                &[
                    bigname_storage::INDEXER_SERVICE_NAME,
                    bigname_storage::WORKER_SERVICE_NAME,
                ],
                state.heartbeat_max_age_secs,
                state.worker_rebuild_phase_max_age_secs,
            )
            .await
            {
                Ok(heartbeats) => {
                    let indexer = loop_health_response(
                        heartbeats.iter().find(|heartbeat| {
                            heartbeat.service_name == bigname_storage::INDEXER_SERVICE_NAME
                        }),
                        state.heartbeat_max_age_secs,
                        state.heartbeat_max_age_secs,
                    );
                    let worker = loop_health_response(
                        heartbeats.iter().find(|heartbeat| {
                            heartbeat.service_name == bigname_storage::WORKER_SERVICE_NAME
                        }),
                        state.heartbeat_max_age_secs,
                        state.worker_rebuild_phase_max_age_secs,
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
        false => {
            let database = HealthDatabaseResponse {
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
    let api_ready = database.reachable;
    let aggregate_ready = api_ready && loops_ready;
    let http_status = if api_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let status = if aggregate_ready {
        "ready"
    } else {
        "degraded"
    };
    let api_status = if api_ready { "ready" } else { "degraded" };

    (
        http_status,
        Json(HealthResponse {
            service: "api",
            identity: HealthIdentityResponse::current(),
            status,
            api_status,
            process: HealthProcessResponse { status: "running" },
            database,
            loops,
        }),
    )
}

fn loop_health_response(
    heartbeat: Option<&bigname_storage::ServiceLoopHeartbeat>,
    max_age_seconds: i64,
    phase_max_age_seconds: i64,
) -> HealthLoopResponse {
    let Some(heartbeat) = heartbeat else {
        return HealthLoopResponse {
            status: "not_started",
            phase: None,
            started_at: None,
            heartbeat_at: None,
            heartbeat_age_seconds: None,
            max_age_seconds,
        };
    };
    if let Some(phase) = heartbeat.active_phase.as_ref() {
        return HealthLoopResponse {
            status: if phase.age_seconds <= phase_max_age_seconds {
                "running"
            } else {
                "stale"
            },
            phase: Some(phase.phase.clone()),
            started_at: Some(format_timestamp(phase.started_at)),
            heartbeat_at: Some(format_timestamp(phase.heartbeat_at)),
            heartbeat_age_seconds: Some(phase.age_seconds),
            max_age_seconds: phase_max_age_seconds,
        };
    }
    HealthLoopResponse {
        status: if heartbeat.age_seconds <= max_age_seconds {
            "running"
        } else {
            "stale"
        },
        phase: None,
        started_at: Some(format_timestamp(heartbeat.started_at)),
        heartbeat_at: Some(format_timestamp(heartbeat.heartbeat_at)),
        heartbeat_age_seconds: Some(heartbeat.age_seconds),
        max_age_seconds,
    }
}

fn unavailable_loop_health(max_age_seconds: i64) -> HealthLoopsResponse {
    let unavailable = || HealthLoopResponse {
        status: "unavailable",
        phase: None,
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
