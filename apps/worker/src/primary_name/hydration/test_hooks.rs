use std::sync::Arc;

use anyhow::Result;
use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::PgPool;
use tokio::sync::Barrier;

#[derive(Clone)]
pub(crate) struct HydrationAfterInvalidationHook {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

pub(crate) struct HydrationAfterInvalidationControl {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl HydrationAfterInvalidationControl {
    pub(crate) async fn wait_until_reached(&self) {
        self.reached.wait().await;
    }

    pub(crate) async fn resume(&self) {
        self.resume.wait().await;
    }
}

static HOOKS: ScopedTestHookRegistry<String, HydrationAfterInvalidationHook> =
    ScopedTestHookRegistry::new();

pub(crate) async fn install(
    pool: &PgPool,
) -> Result<(
    ScopedTestHookGuard<String, HydrationAfterInvalidationHook>,
    HydrationAfterInvalidationControl,
)> {
    let database = current_test_database(pool).await?;
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let guard = HOOKS.install(
        database,
        HydrationAfterInvalidationHook {
            reached: Arc::clone(&reached),
            resume: Arc::clone(&resume),
        },
    );
    Ok((guard, HydrationAfterInvalidationControl { reached, resume }))
}

pub(super) async fn run(pool: &PgPool) -> Result<()> {
    let database = current_test_database(pool).await?;
    if let Some(hook) = HOOKS.take(&database) {
        hook.reached.wait().await;
        hook.resume.wait().await;
    }
    Ok(())
}
