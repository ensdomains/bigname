use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use tracing::warn;

const ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY: i64 = 0x4249474e414d4501_i64;

pub(crate) async fn try_acquire_replay_lock(
    pool: &PgPool,
) -> Result<Option<PoolConnection<Postgres>>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire all-current replay lock connection")?;
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(&mut *connection)
        .await
        .context("failed to acquire all-current replay advisory lock")?;

    Ok(acquired.then_some(connection))
}

pub(crate) async fn release_replay_lock(connection: &mut PoolConnection<Postgres>) -> Result<()> {
    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(&mut **connection)
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
