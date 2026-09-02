use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use tracing::warn;

use crate::{AppState, BUILD_SHA, SOFTWARE_VERSION, v2::format_timestamp};

pub(crate) const HEALTH_DATABASE_CHECK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2);
const DATABASE_IDENTITY_SQL: &str = "SELECT database.oid::text,
            extract(epoch FROM pg_postmaster_start_time())::text,
            COALESCE(inet_server_addr()::text, 'local-socket'),
            COALESCE(inet_server_port()::text, 'local-socket')
     FROM pg_database database
     WHERE database.datname = current_database()";

#[derive(Clone)]
pub(crate) struct HealthDatabasePool(pub(crate) PgPool);

#[derive(Serialize)]
pub(crate) struct HealthResponse {
    pub(crate) service: &'static str,
    pub(crate) identity: HealthIdentityResponse,
    pub(crate) status: &'static str,
    pub(crate) api_status: &'static str,
    pub(crate) process: HealthProcessResponse,
    pub(crate) database: HealthDatabaseResponse,
    pub(crate) loops: HealthLoopsResponse,
}

#[derive(Serialize)]
pub(crate) struct HealthIdentityResponse {
    pub(crate) version: &'static str,
    pub(crate) build_sha: &'static str,
    pub(crate) interpreter_content_hash: &'static str,
}

#[derive(Serialize)]
pub(crate) struct HealthProcessResponse {
    pub(crate) status: &'static str,
}

#[derive(Serialize)]
pub(crate) struct HealthDatabaseResponse {
    pub(crate) status: &'static str,
    pub(crate) reachable: bool,
    pub(crate) check: &'static str,
    pub(crate) error: Option<&'static str>,
    pub(crate) identity: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct HealthLoopsResponse {
    pub(crate) phase_runner: HealthLoopResponse,
}

#[derive(Serialize)]
pub(crate) struct HealthLoopResponse {
    pub(crate) status: &'static str,
    pub(crate) phase: Option<String>,
    pub(crate) started_at: Option<String>,
    pub(crate) heartbeat_at: Option<String>,
    pub(crate) heartbeat_age_seconds: Option<i64>,
    pub(crate) max_age_seconds: i64,
}

pub(crate) async fn health(
    State(state): State<AppState>,
    axum::Extension(health_pool): axum::Extension<HealthDatabasePool>,
) -> (StatusCode, Json<HealthResponse>) {
    let (readiness_probe, serving_probe) = tokio::join!(
        tokio::time::timeout(
            HEALTH_DATABASE_CHECK_TIMEOUT,
            load_database_instance_identity(&health_pool.0),
        ),
        tokio::time::timeout(
            HEALTH_DATABASE_CHECK_TIMEOUT,
            load_database_instance_identity(&state.pool),
        ),
    );
    let readiness_identity = match readiness_probe {
        Ok(Ok(identity)) => Some(identity),
        Ok(Err(error)) => {
            warn!(
                service = "api",
                build_sha = BUILD_SHA,
                ?error,
                "database readiness probe failed"
            );
            None
        }
        Err(_) => {
            warn!(
                service = "api",
                build_sha = BUILD_SHA,
                timeout_ms = HEALTH_DATABASE_CHECK_TIMEOUT.as_millis(),
                "database readiness probe timed out"
            );
            None
        }
    };
    let serving_identity = match serving_probe {
        Ok(Ok(identity)) => Some(identity),
        Ok(Err(error)) => {
            warn!(
                service = "api",
                build_sha = BUILD_SHA,
                ?error,
                "serving database identity probe failed"
            );
            None
        }
        Err(_) => {
            warn!(
                service = "api",
                build_sha = BUILD_SHA,
                timeout_ms = HEALTH_DATABASE_CHECK_TIMEOUT.as_millis(),
                "serving database identity probe timed out"
            );
            None
        }
    };
    let database_identity = readiness_identity
        .as_ref()
        .filter(|identity| serving_identity.as_ref() == Some(*identity))
        .cloned();
    if readiness_identity.is_some() && database_identity.is_none() {
        warn!(
            service = "api",
            build_sha = BUILD_SHA,
            "serving and readiness database identities could not be matched"
        );
    }

    let database = if readiness_identity.is_some() {
        HealthDatabaseResponse {
            status: "reachable",
            reachable: true,
            check: "database_identity",
            error: None,
            identity: database_identity,
        }
    } else {
        HealthDatabaseResponse {
            status: "unreachable",
            reachable: false,
            check: "database_identity",
            error: Some("database readiness query failed"),
            identity: None,
        }
    };

    let phase_runner = if database.reachable {
        match tokio::time::timeout(
            HEALTH_DATABASE_CHECK_TIMEOUT,
            load_phase_runner_health(&health_pool.0, state.phase_heartbeat_max_age_secs),
        )
        .await
        {
            Ok(Ok(health)) => health,
            Ok(Err(error)) => {
                warn!(
                    service = "api",
                    build_sha = BUILD_SHA,
                    ?error,
                    "phase-runner heartbeat readiness probe failed"
                );
                unavailable_phase_runner(state.phase_heartbeat_max_age_secs)
            }
            Err(_) => {
                warn!(
                    service = "api",
                    build_sha = BUILD_SHA,
                    timeout_ms = HEALTH_DATABASE_CHECK_TIMEOUT.as_millis(),
                    "phase-runner heartbeat readiness probe timed out"
                );
                unavailable_phase_runner(state.phase_heartbeat_max_age_secs)
            }
        }
    } else {
        unavailable_phase_runner(state.phase_heartbeat_max_age_secs)
    };

    let api_ready = database.reachable;
    let aggregate_ready = api_ready && phase_runner.status == "running";
    let http_status = if api_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        http_status,
        Json(HealthResponse {
            service: "api",
            identity: HealthIdentityResponse {
                version: SOFTWARE_VERSION,
                build_sha: BUILD_SHA,
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH,
            },
            status: if aggregate_ready { "ready" } else { "degraded" },
            api_status: if api_ready { "ready" } else { "degraded" },
            process: HealthProcessResponse { status: "running" },
            database,
            loops: HealthLoopsResponse { phase_runner },
        }),
    )
}

async fn load_database_instance_identity(pool: &PgPool) -> anyhow::Result<String> {
    let (database_oid, postmaster_started_at, server_address, server_port): (
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(DATABASE_IDENTITY_SQL)
        .fetch_one(pool)
        .await?;
    Ok(database_instance_identity(
        &database_oid,
        &postmaster_started_at,
        &server_address,
        &server_port,
    ))
}

fn database_instance_identity(
    database_oid: &str,
    postmaster_started_at: &str,
    server_address: &str,
    server_port: &str,
) -> String {
    format!(
        "keccak256:{:#x}",
        alloy_primitives::keccak256(format!(
            "{database_oid}:{postmaster_started_at}:{server_address}:{server_port}"
        ))
    )
    .replace("keccak256:0x", "keccak256:")
}

/// Judge the phase runner by its worst expected chain, not by the single freshest heartbeat row.
/// One stalled chain must not be masked by another chain still writing heartbeats, so an expected
/// chain with no heartbeat at all, or a freshest-per-chain heartbeat older than the configured
/// age, reports `stale`. The expected chain set is the one `/v2/status` reads, so the two
/// readiness surfaces cannot disagree about which chains a missing heartbeat is missing from.
async fn load_phase_runner_health(
    pool: &PgPool,
    max_age_seconds: i64,
) -> anyhow::Result<HealthLoopResponse> {
    let expected_chains = bigname_storage::PHASE_EXPECTED_CHAIN_IDS_SELECT;
    let row = sqlx::query(&format!(
        r#"
        WITH expected_chains AS ({expected_chains}),
        chain_heartbeats AS (
            SELECT expected.chain_id, freshest.phase_name, freshest.started_at,
                   freshest.heartbeat_at
            FROM expected_chains expected
            LEFT JOIN LATERAL (
                SELECT phase_name, started_at, heartbeat_at
                FROM bigname_phase.service_heartbeats
                WHERE service_name = 'phase-runner'
                  AND chain_id = expected.chain_id
                ORDER BY heartbeat_at DESC, instance_id, phase_name
                LIMIT 1
            ) freshest ON TRUE
        ),
        oldest AS (
            SELECT phase_name, started_at, heartbeat_at
            FROM chain_heartbeats
            WHERE heartbeat_at IS NOT NULL
            ORDER BY heartbeat_at ASC, chain_id, phase_name
            LIMIT 1
        )
        SELECT
            (
                SELECT COUNT(*) FROM chain_heartbeats WHERE heartbeat_at IS NULL
            )::BIGINT AS missing_chain_count,
            oldest.phase_name,
            oldest.started_at,
            oldest.heartbeat_at,
            FLOOR(
                EXTRACT(EPOCH FROM (clock_timestamp() - oldest.heartbeat_at))
            )::BIGINT AS age_seconds
        FROM (SELECT 1) probe
        LEFT JOIN oldest ON TRUE
        "#
    ))
    .fetch_one(pool)
    .await?;
    let not_started = HealthLoopResponse {
        status: "not_started",
        phase: None,
        started_at: None,
        heartbeat_at: None,
        heartbeat_age_seconds: None,
        max_age_seconds,
    };
    let Some(heartbeat_at) = row.try_get::<Option<OffsetDateTime>, _>("heartbeat_at")? else {
        return Ok(not_started);
    };
    if row.try_get::<i64, _>("missing_chain_count")? > 0 {
        return Ok(HealthLoopResponse {
            status: "stale",
            ..not_started
        });
    }
    let age_seconds: i64 = row.try_get("age_seconds")?;
    let started_at: OffsetDateTime = row.try_get("started_at")?;
    Ok(HealthLoopResponse {
        status: if age_seconds <= max_age_seconds {
            "running"
        } else {
            "stale"
        },
        phase: Some(row.try_get("phase_name")?),
        started_at: Some(format_timestamp(started_at)),
        heartbeat_at: Some(format_timestamp(heartbeat_at)),
        heartbeat_age_seconds: Some(age_seconds),
        max_age_seconds,
    })
}

fn unavailable_phase_runner(max_age_seconds: i64) -> HealthLoopResponse {
    HealthLoopResponse {
        status: "unavailable",
        phase: None,
        started_at: None,
        heartbeat_at: None,
        heartbeat_age_seconds: None,
        max_age_seconds,
    }
}
