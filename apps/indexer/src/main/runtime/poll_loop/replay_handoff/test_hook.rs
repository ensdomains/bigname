use std::sync::Arc;

use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::PgPool;
use tokio::sync::Notify;

pub(crate) struct ReplayHandoffBeforeLatchTestHook {
    state: HookState,
    _registration: ScopedTestHookGuard<HookKey, HookState>,
}

#[derive(Clone)]
struct HookState {
    before_latch: Arc<Notify>,
    resume: Arc<Notify>,
}

impl ReplayHandoffBeforeLatchTestHook {
    pub(crate) async fn wait_until_before_latch(&self) {
        self.state.before_latch.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.state.resume.notify_one();
    }
}

impl Drop for ReplayHandoffBeforeLatchTestHook {
    fn drop(&mut self) {
        self.state.resume.notify_one();
    }
}

type HookKey = (String, String);

static HOOKS: ScopedTestHookRegistry<HookKey, HookState> = ScopedTestHookRegistry::new();

pub(crate) async fn install_before_latch(
    pool: &PgPool,
    deployment_profile: &str,
) -> ReplayHandoffBeforeLatchTestHook {
    let database = current_test_database(pool)
        .await
        .expect("replay handoff test hook must identify its database");
    let state = HookState {
        before_latch: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let registration = HOOKS.install((database, deployment_profile.to_owned()), state.clone());
    ReplayHandoffBeforeLatchTestHook {
        state,
        _registration: registration,
    }
}

pub(super) async fn pause_before_latch(database: &str, deployment_profile: &str) {
    let hook = HOOKS.take(&(database.to_owned(), deployment_profile.to_owned()));
    if let Some(hook) = hook {
        hook.before_latch.notify_one();
        hook.resume.notified().await;
    }
}
