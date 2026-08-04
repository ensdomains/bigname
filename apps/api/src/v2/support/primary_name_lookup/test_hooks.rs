use std::sync::Arc;

use anyhow::Result;
use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::{PgConnection, PgPool};
use tokio::sync::Barrier;

use super::*;

#[derive(Clone)]
pub(crate) struct OutcomeReadHook {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

pub(crate) struct OutcomeReadControl {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl OutcomeReadControl {
    pub(crate) async fn wait_until_reached(&self) {
        self.reached.wait().await;
    }

    pub(crate) async fn resume(&self) {
        self.resume.wait().await;
    }
}

static HOOKS: ScopedTestHookRegistry<String, OutcomeReadHook> = ScopedTestHookRegistry::new();

pub(crate) async fn install(
    pool: &PgPool,
) -> Result<(
    ScopedTestHookGuard<String, OutcomeReadHook>,
    OutcomeReadControl,
)> {
    let database = current_test_database(pool).await?;
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let guard = HOOKS.install(
        database,
        OutcomeReadHook {
            reached: Arc::clone(&reached),
            resume: Arc::clone(&resume),
        },
    );
    Ok((guard, OutcomeReadControl { reached, resume }))
}

pub(super) async fn run(connection: &mut PgConnection) -> ApiResult<()> {
    let database = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| ApiError::internal_error("failed to run primary-name readback test hook"))?;
    if let Some(hook) = HOOKS.take(&database) {
        hook.reached.wait().await;
        hook.resume.wait().await;
    }
    Ok(())
}
