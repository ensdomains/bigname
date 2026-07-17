use std::sync::Arc;

use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::PgPool;
use tokio::sync::Notify;

pub(crate) struct BacklogAfterAdapterSyncTestHook {
    state: HookState,
    _registration: ScopedTestHookGuard<HookKey, HookState>,
}

#[derive(Clone)]
struct HookState {
    after_adapter_sync: Arc<Notify>,
    resume: Arc<Notify>,
}

impl BacklogAfterAdapterSyncTestHook {
    pub(crate) async fn wait_until_after_adapter_sync(&self) {
        self.state.after_adapter_sync.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.state.resume.notify_one();
    }
}

impl Drop for BacklogAfterAdapterSyncTestHook {
    fn drop(&mut self) {
        self.state.resume.notify_one();
    }
}

type HookKey = (String, String, String);

static HOOKS: ScopedTestHookRegistry<HookKey, HookState> = ScopedTestHookRegistry::new();

pub(crate) async fn install_after_adapter_sync(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> BacklogAfterAdapterSyncTestHook {
    let database = current_test_database(pool)
        .await
        .expect("backlog test hook must identify its database");
    let state = HookState {
        after_adapter_sync: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let registration = HOOKS.install(
        (database, deployment_profile.to_owned(), chain.to_owned()),
        state.clone(),
    );
    BacklogAfterAdapterSyncTestHook {
        state,
        _registration: registration,
    }
}

pub(super) async fn pause_after_adapter_sync(pool: &PgPool, deployment_profile: &str, chain: &str) {
    let database = current_test_database(pool)
        .await
        .expect("backlog test hook must identify its database");
    let hook = HOOKS.take(&(database, deployment_profile.to_owned(), chain.to_owned()));
    if let Some(hook) = hook {
        hook.after_adapter_sync.notify_one();
        hook.resume.notified().await;
    }
}
