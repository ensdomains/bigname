use std::sync::Arc;

use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::PgPool;
use tokio::sync::Notify;

pub(crate) struct NormalizedReplayAfterRewindTestHook {
    state: NormalizedReplayAfterRewindTestHookState,
    _registration: ScopedTestHookGuard<HookKey, NormalizedReplayAfterRewindTestHookState>,
}

#[derive(Clone)]
struct NormalizedReplayAfterRewindTestHookState {
    after_rewind: Arc<Notify>,
    resume: Arc<Notify>,
}

impl NormalizedReplayAfterRewindTestHook {
    pub(crate) async fn wait_until_after_rewind(&self) {
        self.state.after_rewind.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.state.resume.notify_one();
    }
}

impl Drop for NormalizedReplayAfterRewindTestHook {
    fn drop(&mut self) {
        self.state.resume.notify_one();
    }
}

type HookKey = (String, String, String);

static HOOKS: ScopedTestHookRegistry<HookKey, NormalizedReplayAfterRewindTestHookState> =
    ScopedTestHookRegistry::new();

pub(crate) async fn install_after_rewind(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> NormalizedReplayAfterRewindTestHook {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let state = NormalizedReplayAfterRewindTestHookState {
        after_rewind: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let registration = HOOKS.install(
        (database, deployment_profile.to_owned(), chain.to_owned()),
        state.clone(),
    );
    NormalizedReplayAfterRewindTestHook {
        state,
        _registration: registration,
    }
}

pub(super) async fn pause_after_rewind(pool: &PgPool, deployment_profile: &str, chain: &str) {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let hook = HOOKS.take(&(database, deployment_profile.to_owned(), chain.to_owned()));
    if let Some(hook) = hook {
        hook.after_rewind.notify_one();
        hook.resume.notified().await;
    }
}
