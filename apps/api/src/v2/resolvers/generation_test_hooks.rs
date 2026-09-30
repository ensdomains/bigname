//! Pause a resolver request immediately before rechecking its admitted generation.

use std::sync::Arc;

use anyhow::Result;
use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::PgPool;
use tokio::sync::Barrier;

use crate::v2::{V2Error, V2Result};

#[derive(Clone)]
pub(crate) struct RevalidationHook {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

pub(crate) struct RevalidationControl {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl RevalidationControl {
    pub(crate) async fn wait_until_reached(&self) {
        self.reached.wait().await;
    }

    pub(crate) async fn resume(&self) {
        self.resume.wait().await;
    }
}

static HOOKS: ScopedTestHookRegistry<String, RevalidationHook> = ScopedTestHookRegistry::new();

pub(crate) async fn install(
    pool: &PgPool,
) -> Result<(
    ScopedTestHookGuard<String, RevalidationHook>,
    RevalidationControl,
)> {
    let database = current_test_database(pool).await?;
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let guard = HOOKS.install(
        database,
        RevalidationHook {
            reached: Arc::clone(&reached),
            resume: Arc::clone(&resume),
        },
    );
    Ok((guard, RevalidationControl { reached, resume }))
}

pub(crate) async fn run(pool: &PgPool) -> V2Result<()> {
    let database = current_test_database(pool).await.map_err(|_| {
        V2Error::internal_error("failed to run resolver generation recheck test hook")
    })?;
    if let Some(hook) = HOOKS.take(&database) {
        hook.reached.wait().await;
        hook.resume.wait().await;
    }
    Ok(())
}
