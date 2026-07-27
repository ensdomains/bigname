use anyhow::{Context, Result, bail};
use sqlx::{PgPool, types::time::OffsetDateTime};

use super::{
    DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS, PROCESS_SCOPE_ID, PROCESS_SCOPE_KIND,
    ServiceLoopChainHeartbeat, ServiceLoopHeartbeat, ServiceLoopPhaseHeartbeat, validate_identity,
    validate_service_name,
};

type ServiceLoopHeartbeatRow = (
    String,
    String,
    OffsetDateTime,
    OffsetDateTime,
    i64,
    Option<String>,
    Option<OffsetDateTime>,
    Option<OffsetDateTime>,
    Option<i64>,
    Option<String>,
    Option<OffsetDateTime>,
    Option<OffsetDateTime>,
    Option<i64>,
);

pub async fn load_service_loop_heartbeat(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
) -> Result<Option<ServiceLoopHeartbeat>> {
    validate_identity(service_name, instance_id)?;

    let row = sqlx::query_as::<_, ServiceLoopHeartbeatRow>(
        r#"
        SELECT
            process.service_name,
            process.instance_id,
            process.started_at,
            process.heartbeat_at,
            GREATEST(
                FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - process.heartbeat_at)))::BIGINT,
                0
            ) AS age_seconds,
            phase.scope_id AS phase,
            phase.started_at AS phase_started_at,
            phase.heartbeat_at AS phase_heartbeat_at,
            CASE
                WHEN phase.heartbeat_at IS NULL THEN NULL
                ELSE GREATEST(
                    FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - phase.heartbeat_at)))::BIGINT,
                    0
                )
            END AS phase_age_seconds,
            oldest_chain.scope_id AS oldest_chain_id,
            oldest_chain.started_at AS oldest_chain_started_at,
            oldest_chain.heartbeat_at AS oldest_chain_heartbeat_at,
            CASE
                WHEN oldest_chain.heartbeat_at IS NULL THEN NULL
                ELSE GREATEST(
                    FLOOR(EXTRACT(EPOCH FROM (
                        clock_timestamp() - oldest_chain.heartbeat_at
                    )))::BIGINT,
                    0
                )
            END AS oldest_chain_age_seconds
        FROM service_loop_heartbeats AS process
        LEFT JOIN LATERAL (
            SELECT scope_id, started_at, heartbeat_at
            FROM service_loop_heartbeats
            WHERE service_name = process.service_name
              AND instance_id = process.instance_id
              AND scope_kind = 'phase'
            ORDER BY heartbeat_at DESC, scope_id
            LIMIT 1
        ) AS phase ON TRUE
        LEFT JOIN LATERAL (
            SELECT scope_id, started_at, heartbeat_at
            FROM service_loop_heartbeats
            WHERE service_name = process.service_name
              AND instance_id = process.instance_id
              AND scope_kind = 'chain'
            ORDER BY heartbeat_at, scope_id
            LIMIT 1
        ) AS oldest_chain ON TRUE
        WHERE process.service_name = $1
          AND process.instance_id = $2
          AND process.scope_kind = $3
          AND process.scope_id = $4
        "#,
    )
    .bind(service_name)
    .bind(instance_id)
    .bind(PROCESS_SCOPE_KIND)
    .bind(PROCESS_SCOPE_ID)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load {service_name} service loop heartbeat for {instance_id}")
    })?;

    Ok(row.map(heartbeat_from_row))
}

pub async fn ensure_service_loop_heartbeat_recent(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
    max_age_seconds: i64,
) -> Result<ServiceLoopHeartbeat> {
    ensure_service_loop_heartbeat_recent_with_phase(
        pool,
        service_name,
        instance_id,
        max_age_seconds,
        max_age_seconds,
    )
    .await
}

pub async fn ensure_service_loop_heartbeat_recent_with_phase(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
    max_age_seconds: i64,
    phase_max_age_seconds: i64,
) -> Result<ServiceLoopHeartbeat> {
    if max_age_seconds <= 0 {
        bail!("heartbeat maximum age must be greater than zero seconds");
    }
    if phase_max_age_seconds <= 0 {
        bail!("heartbeat phase maximum age must be greater than zero seconds");
    }

    let heartbeat = load_service_loop_heartbeat(pool, service_name, instance_id)
        .await?
        .with_context(|| {
            format!(
                "{service_name} loop heartbeat was not found for instance {instance_id}; the process loop never started"
            )
        })?;
    if let Some(oldest_chain) = heartbeat.oldest_chain.as_ref() {
        let chain_max_age_seconds =
            max_age_seconds.max(DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS);
        if oldest_chain.age_seconds > chain_max_age_seconds {
            bail!(
                "{service_name} loop heartbeat for chain {} on instance {instance_id} is stale ({} seconds old; maximum {chain_max_age_seconds}); the chain lane stopped or wedged",
                oldest_chain.chain_id,
                oldest_chain.age_seconds
            );
        }
    }
    if let Some(phase) = heartbeat.active_phase.as_ref() {
        if phase.age_seconds > phase_max_age_seconds {
            bail!(
                "{service_name} loop phase {} for instance {instance_id} is stale ({} seconds old; maximum {}); the phase stopped or wedged",
                phase.phase,
                phase.age_seconds,
                phase_max_age_seconds
            );
        }
    } else if heartbeat.oldest_chain.is_none() && heartbeat.age_seconds > max_age_seconds {
        bail!(
            "{service_name} loop heartbeat for instance {instance_id} is stale ({} seconds old; maximum {}); the process loop stopped or wedged",
            heartbeat.age_seconds,
            max_age_seconds
        );
    }

    Ok(heartbeat)
}

pub async fn load_preferred_service_loop_heartbeats(
    pool: &PgPool,
    service_names: &[&str],
    max_age_seconds: i64,
    worker_phase_max_age_seconds: i64,
) -> Result<Vec<ServiceLoopHeartbeat>> {
    load_preferred_service_loop_heartbeats_with_indexer_chain_max_age(
        pool,
        service_names,
        max_age_seconds,
        worker_phase_max_age_seconds,
        DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS,
    )
    .await
}

pub async fn load_preferred_service_loop_heartbeats_with_indexer_chain_max_age(
    pool: &PgPool,
    service_names: &[&str],
    max_age_seconds: i64,
    worker_phase_max_age_seconds: i64,
    indexer_chain_max_age_seconds: i64,
) -> Result<Vec<ServiceLoopHeartbeat>> {
    for service_name in service_names {
        validate_service_name(service_name)?;
    }
    if max_age_seconds <= 0 {
        bail!("heartbeat maximum age must be greater than zero seconds");
    }
    if worker_phase_max_age_seconds <= 0 {
        bail!("worker heartbeat phase maximum age must be greater than zero seconds");
    }
    if indexer_chain_max_age_seconds <= 0 {
        bail!("indexer chain heartbeat maximum age must be greater than zero seconds");
    }

    let rows = sqlx::query_as::<_, ServiceLoopHeartbeatRow>(
        r#"
        WITH candidate_heartbeats AS (
            SELECT
            process.service_name,
            process.instance_id,
            process.started_at AS process_started_at,
            process.heartbeat_at AS process_heartbeat_at,
            GREATEST(
                FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - process.heartbeat_at)))::BIGINT,
                0
            ) AS age_seconds,
            phase.scope_id AS phase,
            phase.started_at AS phase_started_at,
            phase.heartbeat_at AS phase_heartbeat_at,
            CASE
                WHEN phase.heartbeat_at IS NULL THEN NULL
                ELSE GREATEST(
                    FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - phase.heartbeat_at)))::BIGINT,
                    0
                )
            END AS phase_age_seconds,
            oldest_chain.scope_id AS oldest_chain_id,
            oldest_chain.started_at AS oldest_chain_started_at,
            oldest_chain.heartbeat_at AS oldest_chain_heartbeat_at,
            CASE
                WHEN oldest_chain.heartbeat_at IS NULL THEN NULL
                ELSE GREATEST(
                    FLOOR(EXTRACT(EPOCH FROM (
                        clock_timestamp() - oldest_chain.heartbeat_at
                    )))::BIGINT,
                    0
                )
            END AS oldest_chain_age_seconds
            FROM service_loop_heartbeats AS process
            LEFT JOIN LATERAL (
                SELECT scope_id, started_at, heartbeat_at
                FROM service_loop_heartbeats
                WHERE service_name = process.service_name
                  AND instance_id = process.instance_id
                  AND scope_kind = 'phase'
                ORDER BY heartbeat_at DESC, scope_id
                LIMIT 1
            ) AS phase ON TRUE
            LEFT JOIN LATERAL (
                SELECT scope_id, started_at, heartbeat_at
                FROM service_loop_heartbeats
                WHERE service_name = process.service_name
                  AND instance_id = process.instance_id
                  AND scope_kind = 'chain'
                ORDER BY heartbeat_at, scope_id
                LIMIT 1
            ) AS oldest_chain ON TRUE
            WHERE process.service_name = ANY($1::TEXT[])
              AND process.scope_kind = $2
              AND process.scope_id = $3
        ),
        ranked_heartbeats AS (
            SELECT
                candidate_heartbeats.*,
                ROW_NUMBER() OVER (
                    PARTITION BY service_name
                    ORDER BY
                        CASE
                            WHEN service_name = 'indexer'
                                 AND oldest_chain_heartbeat_at IS NOT NULL
                                THEN oldest_chain_age_seconds <= GREATEST($4, $6)
                                     AND (
                                         phase_heartbeat_at IS NULL
                                         OR phase_age_seconds <= $4
                                     )
                            WHEN phase_heartbeat_at IS NOT NULL
                                THEN phase_age_seconds <= CASE
                                    WHEN service_name = 'worker' THEN $5
                                    ELSE $4
                                END
                            ELSE age_seconds <= $4
                        END DESC,
                        process_heartbeat_at DESC,
                        instance_id
                ) AS preference
            FROM candidate_heartbeats
        )
        SELECT
            service_name,
            instance_id,
            process_started_at,
            process_heartbeat_at,
            age_seconds,
            phase,
            phase_started_at,
            phase_heartbeat_at,
            phase_age_seconds,
            oldest_chain_id,
            oldest_chain_started_at,
            oldest_chain_heartbeat_at,
            oldest_chain_age_seconds
        FROM ranked_heartbeats
        WHERE preference = 1
        ORDER BY service_name
        "#,
    )
    .bind(service_names)
    .bind(PROCESS_SCOPE_KIND)
    .bind(PROCESS_SCOPE_ID)
    .bind(max_age_seconds)
    .bind(worker_phase_max_age_seconds)
    .bind(indexer_chain_max_age_seconds)
    .fetch_all(pool)
    .await
    .context("failed to load preferred service loop heartbeats")?;

    Ok(rows.into_iter().map(heartbeat_from_row).collect())
}

fn heartbeat_from_row(row: ServiceLoopHeartbeatRow) -> ServiceLoopHeartbeat {
    let active_phase = match (row.5, row.6, row.7, row.8) {
        (Some(phase), Some(started_at), Some(heartbeat_at), Some(age_seconds)) => {
            Some(ServiceLoopPhaseHeartbeat {
                phase,
                started_at,
                heartbeat_at,
                age_seconds,
            })
        }
        (None, None, None, None) => None,
        _ => unreachable!("phase heartbeat columns must all be null or all be present"),
    };
    let oldest_chain = match (row.9, row.10, row.11, row.12) {
        (Some(chain_id), Some(started_at), Some(heartbeat_at), Some(age_seconds)) => {
            Some(ServiceLoopChainHeartbeat {
                chain_id,
                started_at,
                heartbeat_at,
                age_seconds,
            })
        }
        (None, None, None, None) => None,
        _ => unreachable!("chain heartbeat columns must all be null or all be present"),
    };
    ServiceLoopHeartbeat {
        service_name: row.0,
        instance_id: row.1,
        started_at: row.2,
        heartbeat_at: row.3,
        age_seconds: row.4,
        active_phase,
        oldest_chain,
    }
}
