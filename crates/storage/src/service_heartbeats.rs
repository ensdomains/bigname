use anyhow::{Context, Result, bail};
use sqlx::{PgPool, types::time::OffsetDateTime};

pub const INDEXER_SERVICE_NAME: &str = "indexer";
pub const WORKER_SERVICE_NAME: &str = "worker";

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
}

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
        WITH retired_chain_scopes AS (
            DELETE FROM service_loop_heartbeats
            WHERE service_name = $1
              AND instance_id = $2
              AND scope_kind = 'chain'
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

    let mut scope_kinds = Vec::with_capacity(chain_ids.len() + 1);
    let mut scope_ids = Vec::with_capacity(chain_ids.len() + 1);
    scope_kinds.push(PROCESS_SCOPE_KIND.to_owned());
    scope_ids.push(PROCESS_SCOPE_ID.to_owned());
    for chain_id in chain_ids {
        let chain_id = chain_id.trim();
        if chain_id.is_empty() || chain_id == PROCESS_SCOPE_ID {
            bail!("heartbeat chain id must be non-blank and must not equal process");
        }
        scope_kinds.push(CHAIN_SCOPE_KIND.to_owned());
        scope_ids.push(chain_id.to_owned());
    }

    sqlx::query(
        r#"
        WITH observed AS (
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

    Ok(())
}

pub async fn load_service_loop_heartbeat(
    pool: &PgPool,
    service_name: &str,
    instance_id: &str,
) -> Result<Option<ServiceLoopHeartbeat>> {
    validate_identity(service_name, instance_id)?;

    let row = sqlx::query_as::<_, (String, String, OffsetDateTime, OffsetDateTime, i64)>(
        r#"
        SELECT
            service_name,
            instance_id,
            started_at,
            heartbeat_at,
            GREATEST(
                FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - heartbeat_at)))::BIGINT,
                0
            ) AS age_seconds
        FROM service_loop_heartbeats
        WHERE service_name = $1
          AND instance_id = $2
          AND scope_kind = $3
          AND scope_id = $4
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
    if max_age_seconds <= 0 {
        bail!("heartbeat maximum age must be greater than zero seconds");
    }

    let heartbeat = load_service_loop_heartbeat(pool, service_name, instance_id)
        .await?
        .with_context(|| {
            format!(
                "{service_name} loop heartbeat was not found for instance {instance_id}; the process loop never started"
            )
        })?;
    if heartbeat.age_seconds > max_age_seconds {
        bail!(
            "{service_name} loop heartbeat for instance {instance_id} is stale ({} seconds old; maximum {}); the process loop stopped or wedged",
            heartbeat.age_seconds,
            max_age_seconds
        );
    }

    Ok(heartbeat)
}

pub async fn load_latest_service_loop_heartbeats(
    pool: &PgPool,
    service_names: &[&str],
) -> Result<Vec<ServiceLoopHeartbeat>> {
    for service_name in service_names {
        validate_service_name(service_name)?;
    }

    let rows = sqlx::query_as::<_, (String, String, OffsetDateTime, OffsetDateTime, i64)>(
        r#"
        SELECT DISTINCT ON (service_name)
            service_name,
            instance_id,
            started_at,
            heartbeat_at,
            GREATEST(
                FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - heartbeat_at)))::BIGINT,
                0
            ) AS age_seconds
        FROM service_loop_heartbeats
        WHERE service_name = ANY($1::TEXT[])
          AND scope_kind = $2
          AND scope_id = $3
        ORDER BY service_name, heartbeat_at DESC, instance_id
        "#,
    )
    .bind(service_names)
    .bind(PROCESS_SCOPE_KIND)
    .bind(PROCESS_SCOPE_ID)
    .fetch_all(pool)
    .await
    .context("failed to load latest service loop heartbeats")?;

    Ok(rows.into_iter().map(heartbeat_from_row).collect())
}

fn heartbeat_from_row(
    row: (String, String, OffsetDateTime, OffsetDateTime, i64),
) -> ServiceLoopHeartbeat {
    ServiceLoopHeartbeat {
        service_name: row.0,
        instance_id: row.1,
        started_at: row.2,
        heartbeat_at: row.3,
        age_seconds: row.4,
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
