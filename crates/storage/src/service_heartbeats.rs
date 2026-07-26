use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, types::time::OffsetDateTime};

pub const INDEXER_SERVICE_NAME: &str = "indexer";
pub const WORKER_SERVICE_NAME: &str = "worker";
pub const DEFAULT_WORKER_REBUILD_PHASE_MAX_AGE_SECS: i64 = 43_200;

const PROCESS_SCOPE_KIND: &str = "process";
const PROCESS_SCOPE_ID: &str = "process";
const CHAIN_SCOPE_KIND: &str = "chain";
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceLoopHeartbeat {
    pub service_name: String,
    pub instance_id: String,
    pub started_at: OffsetDateTime,
    pub heartbeat_at: OffsetDateTime,
    pub age_seconds: i64,
    pub active_phase: Option<ServiceLoopPhaseHeartbeat>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceLoopPhaseHeartbeat {
    pub phase: String,
    pub started_at: OffsetDateTime,
    pub heartbeat_at: OffsetDateTime,
    pub age_seconds: i64,
}

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
);

pub fn resolve_service_instance_id(configured: Option<&str>) -> Result<String> {
    let instance_id = match configured {
        Some(instance_id) => instance_id.trim().to_owned(),
        None => std::env::var("HOSTNAME").unwrap_or_else(|_| "default".to_owned()),
    };
    if instance_id.trim().is_empty() {
        bail!("heartbeat instance id must not be blank");
    }
    Ok(instance_id)
}

pub async fn register_service_loop(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
) -> Result<()> {
    validate_identity(service_name, instance_id)?;

    sqlx::query(
        r#"
        WITH retired_scopes AS (
            DELETE FROM service_loop_heartbeats
            WHERE service_name = $1
              AND scope_kind <> 'process'
        ),
        observed AS (
            SELECT clock_timestamp() AS observed_at
        )
        INSERT INTO service_loop_heartbeats (
            service_name,
            instance_id,
            scope_kind,
            scope_id,
            started_at,
            heartbeat_at
        )
        SELECT $1, $2, $3, $4, observed_at, observed_at
        FROM observed
        ON CONFLICT (service_name, instance_id, scope_kind, scope_id)
        DO UPDATE SET
            started_at = EXCLUDED.started_at,
            heartbeat_at = EXCLUDED.heartbeat_at
        "#,
    )
    .bind(service_name)
    .bind(instance_id)
    .bind(PROCESS_SCOPE_KIND)
    .bind(PROCESS_SCOPE_ID)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to register {service_name} service loop heartbeat for {instance_id}")
    })?;

    Ok(())
}

pub async fn record_service_loop_heartbeat(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
    chain_ids: &[String],
) -> Result<()> {
    validate_identity(service_name, instance_id)?;
    if service_name != INDEXER_SERVICE_NAME && !chain_ids.is_empty() {
        bail!("only the indexer service may record chain-scoped heartbeats");
    }

    let mut unique_chain_ids = BTreeSet::new();
    for chain_id in chain_ids {
        let chain_id = chain_id.trim();
        if chain_id.is_empty() || chain_id == PROCESS_SCOPE_ID {
            bail!("heartbeat chain id must be non-blank and must not equal process");
        }
        unique_chain_ids.insert(chain_id.to_owned());
    }

    let mut scope_kinds = Vec::with_capacity(unique_chain_ids.len() + 1);
    let mut scope_ids = Vec::with_capacity(unique_chain_ids.len() + 1);
    scope_kinds.push(PROCESS_SCOPE_KIND.to_owned());
    scope_ids.push(PROCESS_SCOPE_ID.to_owned());
    for chain_id in unique_chain_ids {
        scope_kinds.push(CHAIN_SCOPE_KIND.to_owned());
        scope_ids.push(chain_id);
    }

    let recorded = sqlx::query(
        r#"
        WITH registered_process AS MATERIALIZED (
            /* service_loop_heartbeat_registration_fence */ SELECT scope_id
            FROM service_loop_heartbeats
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'process'
              AND scope_id = 'process'
            FOR UPDATE
        ),
        retired_phases AS (
            DELETE FROM service_loop_heartbeats
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'phase'
              AND service_name = 'worker'
              AND EXISTS (SELECT 1 FROM registered_process)
        ),
        observed AS (
            SELECT clock_timestamp() AS observed_at
        )
        INSERT INTO service_loop_heartbeats (
            service_name,
            instance_id,
            scope_kind,
            scope_id,
            started_at,
            heartbeat_at
        )
        SELECT
            $1,
            $2,
            scope.scope_kind,
            scope.scope_id,
            observed.observed_at,
            observed.observed_at
        FROM UNNEST($3::TEXT[], $4::TEXT[]) AS scope(scope_kind, scope_id)
        CROSS JOIN observed
        CROSS JOIN registered_process
        ON CONFLICT (service_name, instance_id, scope_kind, scope_id)
        DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at
        "#,
    )
    .bind(service_name)
    .bind(instance_id)
    .bind(&scope_kinds)
    .bind(&scope_ids)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to record {service_name} service loop heartbeat for {instance_id}")
    })?;
    if recorded.rows_affected() == 0 {
        bail!("{service_name} service loop heartbeat for {instance_id} is not registered");
    }

    Ok(())
}

pub async fn begin_service_loop_phase(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
    phase: &str,
) -> Result<()> {
    validate_identity(service_name, instance_id)?;
    validate_phase(phase)?;

    let recorded = sqlx::query(
        r#"
        WITH registered_process AS MATERIALIZED (
            /* begin_service_loop_phase_registration_fence */ SELECT scope_id
            FROM service_loop_heartbeats
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'process'
              AND scope_id = 'process'
            FOR UPDATE
        ),
        retired_phases AS (
            DELETE FROM service_loop_heartbeats
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'phase'
              AND EXISTS (SELECT 1 FROM registered_process)
        ),
        observed AS (
            SELECT clock_timestamp() AS observed_at
        ),
        process_heartbeat AS (
            UPDATE service_loop_heartbeats
            SET heartbeat_at = observed.observed_at
            FROM observed
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'process'
              AND scope_id = 'process'
              AND EXISTS (SELECT 1 FROM registered_process)
            RETURNING service_loop_heartbeats.scope_id
        )
        INSERT INTO service_loop_heartbeats (
            service_name,
            instance_id,
            scope_kind,
            scope_id,
            started_at,
            heartbeat_at
        )
        SELECT $1, $2, 'phase', $3, observed_at, observed_at
        FROM observed
        CROSS JOIN process_heartbeat
        ON CONFLICT (service_name, instance_id, scope_kind, scope_id)
        DO UPDATE SET
            started_at = EXCLUDED.started_at,
            heartbeat_at = EXCLUDED.heartbeat_at
        "#,
    )
    .bind(service_name)
    .bind(instance_id)
    .bind(phase.trim())
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to begin {service_name} service loop phase {} for {instance_id}",
            phase.trim()
        )
    })?;
    if recorded.rows_affected() == 0 {
        bail!("{service_name} service loop heartbeat for {instance_id} is not registered");
    }

    Ok(())
}

pub async fn finish_service_loop_phase(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
    phase: &str,
) -> Result<()> {
    validate_identity(service_name, instance_id)?;
    validate_phase(phase)?;

    sqlx::query(
        r#"
        WITH retired_phase AS (
            DELETE FROM service_loop_heartbeats
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'phase'
              AND scope_id = $3
        )
        UPDATE service_loop_heartbeats
        SET heartbeat_at = clock_timestamp()
        WHERE service_name = $1
          AND instance_id = $2
          AND scope_kind = 'process'
          AND scope_id = 'process'
        "#,
    )
    .bind(service_name)
    .bind(instance_id)
    .bind(phase.trim())
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to finish {service_name} service loop phase {} for {instance_id}",
            phase.trim()
        )
    })?;

    Ok(())
}

pub async fn deregister_service_loop(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
) -> Result<()> {
    validate_identity(service_name, instance_id)?;
    if service_name != WORKER_SERVICE_NAME {
        bail!("only the worker service may deregister its service loop");
    }

    let mut transaction = pool
        .begin()
        .await
        .with_context(|| format!("failed to begin {service_name} loop deregistration"))?;
    sqlx::query("DELETE FROM service_loop_heartbeats WHERE service_name = $1 AND instance_id = $2 AND scope_kind = 'process' AND scope_id = 'process'")
    .bind(service_name)
    .bind(instance_id)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to fence {service_name} service loop writers for {instance_id}")
    })?;
    sqlx::query("DELETE FROM service_loop_heartbeats WHERE service_name = $1 AND instance_id = $2")
        .bind(service_name)
        .bind(instance_id)
        .execute(&mut *transaction)
        .await
        .with_context(|| {
            format!("failed to clear {service_name} service loop rows for {instance_id}")
        })?;
    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit {service_name} loop deregistration"))?;

    Ok(())
}

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
            END AS phase_age_seconds
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
    if let Some(phase) = heartbeat.active_phase.as_ref() {
        if phase.age_seconds > phase_max_age_seconds {
            bail!(
                "{service_name} loop phase {} for instance {instance_id} is stale ({} seconds old; maximum {}); the phase stopped or wedged",
                phase.phase,
                phase.age_seconds,
                phase_max_age_seconds
            );
        }
    } else if heartbeat.age_seconds > max_age_seconds {
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
    for service_name in service_names {
        validate_service_name(service_name)?;
    }
    if max_age_seconds <= 0 {
        bail!("heartbeat maximum age must be greater than zero seconds");
    }
    if worker_phase_max_age_seconds <= 0 {
        bail!("worker heartbeat phase maximum age must be greater than zero seconds");
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
            END AS phase_age_seconds
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
            phase_age_seconds
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
    ServiceLoopHeartbeat {
        service_name: row.0,
        instance_id: row.1,
        started_at: row.2,
        heartbeat_at: row.3,
        age_seconds: row.4,
        active_phase,
    }
}

fn validate_identity(service_name: &str, instance_id: &str) -> Result<()> {
    validate_service_name(service_name)?;
    if instance_id.trim().is_empty() {
        bail!("heartbeat instance id must not be blank");
    }
    Ok(())
}

fn validate_service_name(service_name: &str) -> Result<()> {
    if !matches!(service_name, INDEXER_SERVICE_NAME | WORKER_SERVICE_NAME) {
        bail!("unsupported heartbeat service name {service_name}");
    }
    Ok(())
}

fn validate_phase(phase: &str) -> Result<()> {
    let phase = phase.trim();
    if phase.is_empty() || phase == PROCESS_SCOPE_ID {
        bail!("heartbeat phase must be non-blank and must not equal process");
    }
    Ok(())
}
