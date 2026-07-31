use std::sync::Arc;

use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::PgPool;
use tokio::sync::Notify;

pub(crate) struct NormalizedReplayAfterRewindTestHook {
    state: NormalizedReplayAfterRewindTestHookState,
    _registration: ScopedTestHookGuard<HookKey, NormalizedReplayAfterRewindTestHookState>,
}

pub(crate) struct NormalizedReplayAfterProgressTestHook {
    state: NormalizedReplayAfterRewindTestHookState,
    _registration: ScopedTestHookGuard<HookKey, NormalizedReplayAfterRewindTestHookState>,
}

pub(crate) struct NormalizedReplayBeforeCursorFailureRecordTestHook {
    state: NormalizedReplayAfterRewindTestHookState,
    _registration: ScopedTestHookGuard<HookKey, NormalizedReplayAfterRewindTestHookState>,
}

pub(crate) struct NormalizedReplayBeforeCoverageAttemptTestHook {
    state: NormalizedReplayAfterRewindTestHookState,
    _registration: ScopedTestHookGuard<HookKey, NormalizedReplayAfterRewindTestHookState>,
}

pub(crate) struct NormalizedReplayAfterTerminalFailureRecordTestHook {
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

impl NormalizedReplayAfterProgressTestHook {
    pub(crate) async fn wait_until_after_progress(&self) {
        self.state.after_rewind.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.state.resume.notify_one();
    }
}

impl NormalizedReplayBeforeCursorFailureRecordTestHook {
    pub(crate) async fn wait_until_before_record(&self) {
        self.state.after_rewind.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.state.resume.notify_one();
    }
}

impl NormalizedReplayBeforeCoverageAttemptTestHook {
    pub(crate) async fn wait_until_before_attempt(&self) {
        self.state.after_rewind.notified().await;
    }

    pub(crate) fn resume(&self) {
        self.state.resume.notify_one();
    }
}

impl NormalizedReplayAfterTerminalFailureRecordTestHook {
    pub(crate) async fn wait(&self) {
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

impl Drop for NormalizedReplayAfterProgressTestHook {
    fn drop(&mut self) {
        self.state.resume.notify_one();
    }
}

impl Drop for NormalizedReplayBeforeCursorFailureRecordTestHook {
    fn drop(&mut self) {
        self.state.resume.notify_one();
    }
}

impl Drop for NormalizedReplayBeforeCoverageAttemptTestHook {
    fn drop(&mut self) {
        self.state.resume.notify_one();
    }
}

impl Drop for NormalizedReplayAfterTerminalFailureRecordTestHook {
    fn drop(&mut self) {
        self.state.resume.notify_one();
    }
}

type HookKey = (String, String, String);

static HOOKS: ScopedTestHookRegistry<HookKey, NormalizedReplayAfterRewindTestHookState> =
    ScopedTestHookRegistry::new();
static PROGRESS_HOOKS: ScopedTestHookRegistry<HookKey, NormalizedReplayAfterRewindTestHookState> =
    ScopedTestHookRegistry::new();
static CURSOR_FAILURE_HOOKS: ScopedTestHookRegistry<
    HookKey,
    NormalizedReplayAfterRewindTestHookState,
> = ScopedTestHookRegistry::new();
static COVERAGE_ATTEMPT_HOOKS: ScopedTestHookRegistry<
    HookKey,
    NormalizedReplayAfterRewindTestHookState,
> = ScopedTestHookRegistry::new();
static TERMINAL_FAILURE_RECORD_HOOKS: ScopedTestHookRegistry<
    HookKey,
    NormalizedReplayAfterRewindTestHookState,
> = ScopedTestHookRegistry::new();

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

pub(crate) async fn install_after_progress(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> NormalizedReplayAfterProgressTestHook {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let state = NormalizedReplayAfterRewindTestHookState {
        after_rewind: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let registration = PROGRESS_HOOKS.install(
        (database, deployment_profile.to_owned(), chain.to_owned()),
        state.clone(),
    );
    NormalizedReplayAfterProgressTestHook {
        state,
        _registration: registration,
    }
}

pub(crate) async fn install_before_cursor_failure_record(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> NormalizedReplayBeforeCursorFailureRecordTestHook {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let state = NormalizedReplayAfterRewindTestHookState {
        after_rewind: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let registration = CURSOR_FAILURE_HOOKS.install(
        (database, deployment_profile.to_owned(), chain.to_owned()),
        state.clone(),
    );
    NormalizedReplayBeforeCursorFailureRecordTestHook {
        state,
        _registration: registration,
    }
}

pub(crate) async fn install_before_coverage_attempt(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> NormalizedReplayBeforeCoverageAttemptTestHook {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let state = NormalizedReplayAfterRewindTestHookState {
        after_rewind: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let registration = COVERAGE_ATTEMPT_HOOKS.install(
        (database, deployment_profile.to_owned(), chain.to_owned()),
        state.clone(),
    );
    NormalizedReplayBeforeCoverageAttemptTestHook {
        state,
        _registration: registration,
    }
}

pub(crate) async fn install_after_terminal_failure_record(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> NormalizedReplayAfterTerminalFailureRecordTestHook {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let state = NormalizedReplayAfterRewindTestHookState {
        after_rewind: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
    };
    let registration = TERMINAL_FAILURE_RECORD_HOOKS.install(
        (database, deployment_profile.to_owned(), chain.to_owned()),
        state.clone(),
    );
    NormalizedReplayAfterTerminalFailureRecordTestHook {
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

pub(super) async fn pause_after_progress(pool: &PgPool, deployment_profile: &str, chain: &str) {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let hook = PROGRESS_HOOKS.take(&(database, deployment_profile.to_owned(), chain.to_owned()));
    if let Some(hook) = hook {
        hook.after_rewind.notify_one();
        hook.resume.notified().await;
    }
}

pub(super) async fn pause_before_cursor_failure_record(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let hook =
        CURSOR_FAILURE_HOOKS.take(&(database, deployment_profile.to_owned(), chain.to_owned()));
    if let Some(hook) = hook {
        hook.after_rewind.notify_one();
        hook.resume.notified().await;
    }
}

pub(super) async fn pause_before_coverage_attempt(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let hook =
        COVERAGE_ATTEMPT_HOOKS.take(&(database, deployment_profile.to_owned(), chain.to_owned()));
    if let Some(hook) = hook {
        hook.after_rewind.notify_one();
        hook.resume.notified().await;
    }
}

pub(super) async fn pause_after_terminal_failure_record(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) {
    let database = current_test_database(pool)
        .await
        .expect("normalized replay test hook must identify its database");
    let hook = TERMINAL_FAILURE_RECORD_HOOKS.take(&(
        database,
        deployment_profile.to_owned(),
        chain.to_owned(),
    ));
    if let Some(hook) = hook {
        hook.after_rewind.notify_one();
        hook.resume.notified().await;
    }
}
