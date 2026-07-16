use std::sync::Arc;

use anyhow::Result;
use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::Notify;

pub(in crate::identity) struct RowPathReloadHook {
    state: RowPathReloadHookState,
    _registration: ScopedTestHookGuard<HookKey, RowPathReloadHookState>,
}

#[derive(Clone)]
struct RowPathReloadHookState {
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl RowPathReloadHook {
    pub(in crate::identity) async fn install(
        pool: &PgPool,
        table_name: &'static str,
        row_id: impl Into<String>,
    ) -> Result<Self> {
        let database = current_test_database(pool).await?;
        let state = RowPathReloadHookState {
            reached: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        let registration =
            ROW_PATH_RELOAD_HOOKS.install((database, table_name, row_id.into()), state.clone());
        Ok(Self {
            state,
            _registration: registration,
        })
    }

    pub(in crate::identity) async fn wait_until_reached(&self) {
        self.state.reached.notified().await;
    }

    pub(in crate::identity) fn release(&self) {
        self.state.release.notify_one();
    }
}

impl Drop for RowPathReloadHook {
    fn drop(&mut self) {
        self.state.release.notify_one();
    }
}

type HookKey = (String, &'static str, String);

static ROW_PATH_RELOAD_HOOKS: ScopedTestHookRegistry<HookKey, RowPathReloadHookState> =
    ScopedTestHookRegistry::new();

pub(super) async fn maybe_wait_after_reload(
    executor: &mut Transaction<'_, Postgres>,
    table_name: &'static str,
    row_id: String,
) {
    let database = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut **executor)
        .await
        .expect("row-path reload test hook must identify its database");
    let hook = ROW_PATH_RELOAD_HOOKS.take(&(database, table_name, row_id));

    let Some(hook) = hook else {
        return;
    };

    hook.reached.notify_one();
    hook.release.notified().await;
}
