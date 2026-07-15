use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, Mutex},
};

use sqlx::PgPool;
use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct NormalizedReplayAfterRewindTestHook {
    after_rewind: Arc<Notify>,
    resume: Arc<Notify>,
}

impl NormalizedReplayAfterRewindTestHook {
    pub(crate) async fn wait_until_after_rewind(&self) {
        self.after_rewind.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.resume.notify_one();
    }
}

static HOOKS: LazyLock<
    Mutex<BTreeMap<(String, String, String), NormalizedReplayAfterRewindTestHook>>,
> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub(crate) async fn install_after_rewind(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> NormalizedReplayAfterRewindTestHook {
    let database = current_database(pool).await;
    let hook = NormalizedReplayAfterRewindTestHook {
        after_rewind: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    HOOKS
        .lock()
        .expect("normalized replay after-rewind hook lock must not be poisoned")
        .insert(
            (database, deployment_profile.to_owned(), chain.to_owned()),
            hook.clone(),
        );
    hook
}

pub(super) async fn pause_after_rewind(pool: &PgPool, deployment_profile: &str, chain: &str) {
    let database = current_database(pool).await;
    let hook = HOOKS
        .lock()
        .expect("normalized replay after-rewind hook lock must not be poisoned")
        .remove(&(database, deployment_profile.to_owned(), chain.to_owned()));
    if let Some(hook) = hook {
        hook.after_rewind.notify_one();
        hook.resume.notified().await;
    }
}

async fn current_database(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .expect("normalized replay test hook must identify its database")
}
