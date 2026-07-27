use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, types::time::OffsetDateTime};

pub const INDEXER_SERVICE_NAME: &str = "indexer";
pub const WORKER_SERVICE_NAME: &str = "worker";
pub const DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS: i64 = 1_800;
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
    pub oldest_chain: Option<ServiceLoopChainHeartbeat>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceLoopPhaseHeartbeat {
    pub phase: String,
    pub started_at: OffsetDateTime,
    pub heartbeat_at: OffsetDateTime,
    pub age_seconds: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceLoopChainHeartbeat {
    pub chain_id: String,
    pub started_at: OffsetDateTime,
    pub heartbeat_at: OffsetDateTime,
    pub age_seconds: i64,
}

mod health;
pub use health::{
    ensure_service_loop_heartbeat_recent, ensure_service_loop_heartbeat_recent_with_phase,
    load_preferred_service_loop_heartbeats,
    load_preferred_service_loop_heartbeats_with_indexer_chain_max_age, load_service_loop_heartbeat,
};

#[cfg(test)]
mod tests;

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
