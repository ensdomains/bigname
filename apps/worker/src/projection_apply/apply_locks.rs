use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result, bail};
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};
use tokio::time::timeout;

use super::apply::ClaimedInvalidation;

const APPLY_LOCK_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const APPLY_LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct InvalidationApplyLocks {
    version_fence: Option<Transaction<'static, Postgres>>,
    conn: PgConnection,
    keys: Vec<String>,
}

pub(super) async fn acquire_invalidation_apply_locks(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
) -> Result<InvalidationApplyLocks> {
    acquire_invalidation_apply_locks_with_timeout(pool, invalidations, APPLY_LOCK_ACQUIRE_TIMEOUT)
        .await
}

pub(super) async fn acquire_invalidation_apply_locks_with_timeout(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
    acquire_timeout: Duration,
) -> Result<InvalidationApplyLocks> {
    let mut keys = invalidations
        .iter()
        .map(invalidation_apply_lock_key)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut version_fence = pool
        .begin()
        .await
        .context("failed to open projection invalidation apply version-fence transaction")?;
    bigname_storage::projection_staging::lock_current_projection_replay_version_for_projection_write_in_transaction(
        &mut version_fence,
    )
    .await?;
    let connect_options =
        bigname_storage::stamp_projection_replay_version(pool.connect_options().as_ref().clone());
    let mut conn = timeout(
        APPLY_LOCK_CONNECT_TIMEOUT,
        PgConnection::connect_with(&connect_options),
    )
    .await
    .context("timed out opening projection invalidation apply lock connection")?
    .context("failed to open projection invalidation apply lock connection")?;

    if acquire_timeout.is_zero() {
        bail!("projection invalidation apply lock acquire timeout must be positive");
    }

    ensure_invalidation_apply_locks_connection_alive(&mut conn)
        .await
        .context("projection invalidation apply lock connection failed initial liveness check")?;

    for key in &keys {
        timeout(
            acquire_timeout,
            sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
                .bind(key)
                .execute(&mut conn),
        )
        .await
        .with_context(|| format!("timed out acquiring projection invalidation apply lock {key}"))?
        .with_context(|| format!("failed to acquire projection invalidation apply lock {key}"))?;
        ensure_invalidation_apply_locks_connection_alive(&mut conn)
            .await
            .with_context(|| {
                format!("projection invalidation apply lock connection failed liveness check after acquiring {key}")
            })?;
    }
    keys.reverse();

    Ok(InvalidationApplyLocks {
        version_fence: Some(version_fence),
        conn,
        keys,
    })
}

pub(super) async fn ensure_invalidation_apply_locks_alive(
    locks: &mut InvalidationApplyLocks,
) -> Result<()> {
    let version_fence = locks
        .version_fence
        .as_mut()
        .context("projection invalidation apply version fence was already released")?;
    sqlx::query("SELECT 1")
        .execute(&mut **version_fence)
        .await
        .context("projection invalidation apply version fence is not alive")?;
    ensure_invalidation_apply_locks_connection_alive(&mut locks.conn)
        .await
        .context("projection invalidation apply lock connection is not alive")
}

async fn ensure_invalidation_apply_locks_connection_alive(conn: &mut PgConnection) -> Result<()> {
    ensure_invalidation_apply_locks_connection_alive_with_probe(
        conn,
        APPLY_LOCK_ACQUIRE_TIMEOUT,
        "SELECT 1",
    )
    .await
}

async fn ensure_invalidation_apply_locks_connection_alive_with_probe(
    conn: &mut PgConnection,
    probe_timeout: Duration,
    probe_sql: &str,
) -> Result<()> {
    let probe: i32 = timeout(probe_timeout, sqlx::query_scalar(probe_sql).fetch_one(conn))
        .await
        .context("timed out running projection invalidation apply lock liveness probe")?
        .context("failed to run projection invalidation apply lock liveness probe")?;
    if probe != 1 {
        bail!("projection invalidation apply lock liveness probe returned {probe}");
    }

    Ok(())
}

#[cfg(test)]
pub(super) async fn ensure_invalidation_apply_locks_probe_alive_for_test(
    conn: &mut PgConnection,
    probe_timeout: Duration,
    probe_sql: &str,
) -> Result<()> {
    ensure_invalidation_apply_locks_connection_alive_with_probe(conn, probe_timeout, probe_sql)
        .await
}

#[cfg(test)]
pub(super) async fn open_invalidation_apply_locks_connection_for_test(
    pool: &PgPool,
) -> Result<PgConnection> {
    let connect_options =
        bigname_storage::stamp_projection_replay_version(pool.connect_options().as_ref().clone());
    PgConnection::connect_with(&connect_options)
        .await
        .context("failed to open projection invalidation apply lock test connection")
}

#[cfg(test)]
pub(super) async fn invalidation_apply_locks_backend_pid(
    locks: &mut InvalidationApplyLocks,
) -> Result<i32> {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut locks.conn)
        .await
        .context("failed to load projection invalidation apply lock backend pid")
}

pub(super) async fn release_invalidation_apply_locks(
    locks: &mut InvalidationApplyLocks,
) -> Result<()> {
    let mut release_error = None;
    for key in &locks.keys {
        if let Err(error) =
            sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
                .bind(key)
                .execute(&mut locks.conn)
                .await
                .with_context(|| {
                    format!("failed to release projection invalidation apply lock {key}")
                })
        {
            release_error = Some(error);
            break;
        }
    }

    let version_fence = locks
        .version_fence
        .take()
        .context("projection invalidation apply version fence was already released")?;
    let fence_release = version_fence
        .commit()
        .await
        .context("failed to release projection invalidation apply version fence");
    if let Some(error) = release_error {
        fence_release?;
        return Err(error);
    }
    fence_release
}

pub(super) fn invalidation_apply_lock_key(invalidation: &ClaimedInvalidation) -> String {
    format!(
        "{}:{};{}:{}",
        invalidation.projection.len(),
        invalidation.projection,
        invalidation.projection_key.len(),
        invalidation.projection_key
    )
}
