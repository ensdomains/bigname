use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use anyhow::{Result, ensure};
use sqlx::PgPool;
use tokio::sync::Barrier;

#[derive(Clone)]
struct HistoryAnchorReadHook {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

pub struct HistoryAnchorReadHookGuard {
    key: (String, HistoryReadHookPoint),
}

impl Drop for HistoryAnchorReadHookGuard {
    fn drop(&mut self) {
        hooks()
            .lock()
            .expect("history anchor-read test-hook registry must not be poisoned")
            .remove(&self.key);
    }
}

pub struct HistoryAnchorReadControl {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl HistoryAnchorReadControl {
    pub async fn wait_until_reached(&self) {
        self.reached.wait().await;
    }

    pub async fn resume(&self) {
        self.resume.wait().await;
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HistoryReadHookPoint {
    AfterAnchors,
    AfterPage,
}

static HOOKS: OnceLock<Mutex<HashMap<(String, HistoryReadHookPoint), HistoryAnchorReadHook>>> =
    OnceLock::new();

fn hooks() -> &'static Mutex<HashMap<(String, HistoryReadHookPoint), HistoryAnchorReadHook>> {
    HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn current_database(pool: &PgPool) -> Result<String> {
    Ok(sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?)
}

pub async fn install(
    pool: &PgPool,
    point: HistoryReadHookPoint,
) -> Result<(HistoryAnchorReadHookGuard, HistoryAnchorReadControl)> {
    let database = current_database(pool).await?;
    let key = (database, point);
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let previous = hooks()
        .lock()
        .expect("history anchor-read test-hook registry must not be poisoned")
        .insert(
            key.clone(),
            HistoryAnchorReadHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
    ensure!(
        previous.is_none(),
        "history anchor-read test hook already installed"
    );
    Ok((
        HistoryAnchorReadHookGuard { key },
        HistoryAnchorReadControl { reached, resume },
    ))
}

pub async fn run(pool: &PgPool, point: HistoryReadHookPoint) -> Result<()> {
    let database = current_database(pool).await?;
    let hook = hooks()
        .lock()
        .expect("history anchor-read test-hook registry must not be poisoned")
        .remove(&(database, point));
    if let Some(hook) = hook {
        hook.reached.wait().await;
        hook.resume.wait().await;
    }
    Ok(())
}

pub async fn run_if(pool: &PgPool, enabled: bool) -> Result<()> {
    if enabled {
        run(pool, HistoryReadHookPoint::AfterAnchors).await?;
    }
    Ok(())
}
