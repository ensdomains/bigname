use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, PgPool, Postgres, pool::PoolConnection};
use tokio::time::timeout;
use tracing::warn;

const ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY: i64 = 0x4249474e414d4501_i64;
const REPLAY_LOCK_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn try_acquire_replay_lock(
    pool: &PgPool,
) -> Result<Option<PoolConnection<Postgres>>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire all-current replay lock connection")?;
    let acquired = try_acquire_replay_lock_on_connection(&mut connection).await?;

    Ok(acquired.then_some(connection))
}

pub(crate) async fn release_replay_lock(connection: &mut PoolConnection<Postgres>) -> Result<()> {
    release_replay_lock_on_connection(connection).await
}

pub(crate) async fn try_acquire_dedicated_replay_lock(
    pool: &PgPool,
) -> Result<Option<PgConnection>> {
    let connect_options =
        bigname_storage::stamp_projection_replay_version(pool.connect_options().as_ref().clone());
    let mut connection = timeout(
        REPLAY_LOCK_CONNECT_TIMEOUT,
        PgConnection::connect_with(&connect_options),
    )
    .await
    .context("timed out opening dedicated all-current replay lock connection")?
    .context("failed to open dedicated all-current replay lock connection")?;
    let acquired = try_acquire_replay_lock_on_connection(&mut connection).await?;
    if acquired {
        return Ok(Some(connection));
    }

    connection
        .close()
        .await
        .context("failed to close unacquired dedicated all-current replay lock connection")?;
    Ok(None)
}

pub(crate) async fn release_dedicated_replay_lock(mut connection: PgConnection) -> Result<()> {
    let release_result = release_replay_lock_on_connection(&mut connection).await;
    let close_result = connection
        .close()
        .await
        .context("failed to close dedicated all-current replay lock connection");
    release_result?;
    close_result
}

async fn try_acquire_replay_lock_on_connection(connection: &mut PgConnection) -> Result<bool> {
    sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(connection)
        .await
        .context("failed to acquire all-current replay advisory lock")
}

async fn release_replay_lock_on_connection(connection: &mut PgConnection) -> Result<()> {
    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(connection)
        .await
        .context("failed to release all-current replay advisory lock")?;
    if !released {
        warn!(
            service = "worker",
            replay = "all_current_projections",
            "all-current projection replay advisory lock was already released"
        );
    }
    Ok(())
}
