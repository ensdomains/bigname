use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, types::time::OffsetDateTime};

pub const INDEXER_SERVICE_NAME: &str = "indexer";
pub const WORKER_SERVICE_NAME: &str = "worker";
pub const DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS: i64 = 14_400;
pub const DEFAULT_WORKER_REBUILD_PHASE_MAX_AGE_SECS: i64 = 43_200;

const PROCESS_SCOPE_KIND: &str = "process";
const PROCESS_SCOPE_ID: &str = "process";
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceLoopHeartbeat {
    pub service_name: String,
    pub instance_id: String,
    pub started_at: OffsetDateTime,
    pub heartbeat_at: OffsetDateTime,
    pub age_seconds: i64,
    pub active_phase: Option<ServiceLoopPhaseHeartbeat>,
    pub oldest_chain: Option<ServiceLoopChainHeartbeat>,
    pub missing_expected_chain_id: Option<String>,
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
    ensure_service_loop_heartbeat_recent_with_phase_and_chain,
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
        WITH inherited_expected_candidates AS MATERIALIZED (
            SELECT
                process.instance_id,
                process.heartbeat_at,
                ARRAY(
                    SELECT inherited.chain_id
                    FROM (
                        SELECT UNNEST(process.expected_chain_ids) AS chain_id
                        UNION
                        SELECT chain.scope_id AS chain_id
                        FROM service_loop_heartbeats AS chain
                        WHERE chain.service_name = process.service_name
                          AND chain.instance_id = process.instance_id
                          AND chain.scope_kind = 'chain'
                    ) AS inherited
                    ORDER BY inherited.chain_id
                ) AS expected_chain_ids
            FROM service_loop_heartbeats AS process
            WHERE process.service_name = $1
              AND process.scope_kind = 'process'
              AND process.scope_id = 'process'
        ),
        inherited_expected_chains AS MATERIALIZED (
            SELECT expected_chain_ids
            FROM inherited_expected_candidates
            ORDER BY
                heartbeat_at DESC,
                (instance_id = $2) DESC,
                instance_id
            LIMIT 1
        ),
        retired_scopes AS (
            DELETE FROM service_loop_heartbeats AS scoped
            WHERE scoped.service_name = $1
              AND scoped.scope_kind <> 'process'
              AND (
                  scoped.service_name = 'worker'
                  OR scoped.instance_id = $2
                  OR NOT EXISTS (
                      SELECT 1
                      FROM service_loop_heartbeats AS process
                      WHERE process.service_name = scoped.service_name
                        AND process.instance_id = scoped.instance_id
                        AND process.scope_kind = 'process'
                        AND process.scope_id = 'process'
                  )
              )
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
            heartbeat_at,
            expected_chain_ids
        )
        SELECT
            $1,
            $2,
            $3,
            $4,
            observed_at,
            observed_at,
            CASE
                WHEN $1 = 'indexer' THEN COALESCE(
                    (
                        SELECT expected_chain_ids
                        FROM inherited_expected_chains
                    ),
                    ARRAY[]::TEXT[]
                )
                ELSE ARRAY[]::TEXT[]
            END
        FROM observed
        ON CONFLICT (service_name, instance_id, scope_kind, scope_id)
        DO UPDATE SET
            started_at = EXCLUDED.started_at,
            heartbeat_at = EXCLUDED.heartbeat_at,
            expected_chain_ids = EXCLUDED.expected_chain_ids
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

    let expected_chain_ids = validated_chain_ids(chain_ids)?;

    let registered = sqlx::query_scalar::<_, bool>(
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
        ),
        process_heartbeat AS (
            UPDATE service_loop_heartbeats
            SET
                heartbeat_at = observed.observed_at,
                expected_chain_ids = CASE
                    WHEN $1 = 'indexer' THEN $3
                    ELSE ARRAY[]::TEXT[]
                END
            FROM observed
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'process'
              AND scope_id = 'process'
              AND EXISTS (SELECT 1 FROM registered_process)
            RETURNING observed.observed_at
        ),
        retired_chains AS (
            DELETE FROM service_loop_heartbeats
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'chain'
              AND NOT (scope_id = ANY($3))
              AND EXISTS (SELECT 1 FROM process_heartbeat)
        ),
        chain_heartbeats AS (
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
                'chain',
                chain_id,
                process_heartbeat.observed_at,
                process_heartbeat.observed_at
            FROM UNNEST($3::TEXT[]) AS expected(chain_id)
            CROSS JOIN process_heartbeat
            ON CONFLICT (service_name, instance_id, scope_kind, scope_id)
            DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at
        )
        SELECT EXISTS (SELECT 1 FROM process_heartbeat)
        "#,
    )
    .bind(service_name)
    .bind(instance_id)
    .bind(&expected_chain_ids)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to record {service_name} service loop heartbeat for {instance_id}")
    })?;
    if !registered {
        bail!("{service_name} service loop heartbeat for {instance_id} is not registered");
    }

    Ok(())
}

pub async fn record_service_loop_chain_heartbeat(
    pool: &PgPool,
    instance_id: &str,
    chain_id: &str,
) -> Result<()> {
    validate_identity(INDEXER_SERVICE_NAME, instance_id)?;
    let chain_ids = validated_chain_ids(&[chain_id.to_owned()])?;
    let chain_id = &chain_ids[0];

    let (registered, expected) = sqlx::query_as::<_, (bool, bool)>(
        r#"
        WITH registered_process AS MATERIALIZED (
            /* service_loop_chain_heartbeat_registration_fence */
            SELECT expected_chain_ids
            FROM service_loop_heartbeats
            WHERE service_name = 'indexer'
              AND instance_id = $1
              AND scope_kind = 'process'
              AND scope_id = 'process'
            FOR UPDATE
        ),
        expected_chain AS MATERIALIZED (
            SELECT 1
            FROM registered_process
            WHERE $2 = ANY(expected_chain_ids)
        ),
        observed AS (
            SELECT clock_timestamp() AS observed_at
        ),
        process_heartbeat AS (
            UPDATE service_loop_heartbeats
            SET heartbeat_at = observed.observed_at
            FROM observed
            WHERE service_name = 'indexer'
              AND instance_id = $1
              AND scope_kind = 'process'
              AND scope_id = 'process'
              AND EXISTS (SELECT 1 FROM expected_chain)
            RETURNING observed.observed_at
        ),
        chain_heartbeat AS (
            INSERT INTO service_loop_heartbeats (
                service_name,
                instance_id,
                scope_kind,
                scope_id,
                started_at,
                heartbeat_at
            )
            SELECT
                'indexer',
                $1,
                'chain',
                $2,
                process_heartbeat.observed_at,
                process_heartbeat.observed_at
            FROM process_heartbeat
            ON CONFLICT (service_name, instance_id, scope_kind, scope_id)
            DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at
        )
        SELECT
            EXISTS (SELECT 1 FROM registered_process),
            EXISTS (SELECT 1 FROM expected_chain)
        "#,
    )
    .bind(instance_id)
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to record indexer service loop heartbeat for chain {chain_id} on {instance_id}"
        )
    })?;
    if !registered {
        bail!("indexer service loop heartbeat for {instance_id} is not registered");
    }
    if !expected {
        bail!(
            "indexer service loop heartbeat for chain {chain_id} on {instance_id} is not in the expected live-chain set"
        );
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

fn validated_chain_ids(chain_ids: &[String]) -> Result<Vec<String>> {
    let mut unique_chain_ids = BTreeSet::new();
    for chain_id in chain_ids {
        let chain_id = chain_id.trim();
        if chain_id.is_empty() || chain_id == PROCESS_SCOPE_ID {
            bail!("heartbeat chain id must be non-blank and must not equal process");
        }
        unique_chain_ids.insert(chain_id.to_owned());
    }
    Ok(unique_chain_ids.into_iter().collect())
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
