use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;

#[derive(Clone)]
pub(in crate::identity) struct RowPathReloadHook {
    table_name: &'static str,
    row_id: String,
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl RowPathReloadHook {
    pub(in crate::identity) fn new(table_name: &'static str, row_id: impl Into<String>) -> Self {
        Self {
            table_name,
            row_id: row_id.into(),
            reached: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    pub(in crate::identity) async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    pub(in crate::identity) fn release(&self) {
        self.release.notify_waiters();
    }
}

static ROW_PATH_RELOAD_HOOKS: OnceLock<Mutex<Vec<RowPathReloadHook>>> = OnceLock::new();

pub(in crate::identity) fn install_row_path_reload_hook(hook: RowPathReloadHook) {
    let lock = ROW_PATH_RELOAD_HOOKS.get_or_init(|| Mutex::new(Vec::new()));
    lock.lock()
        .expect("row-path reload hook mutex poisoned")
        .push(hook);
}

fn clear_row_path_reload_hook(table_name: &'static str, row_id: &str) {
    if let Some(lock) = ROW_PATH_RELOAD_HOOKS.get() {
        lock.lock()
            .expect("row-path reload hook mutex poisoned")
            .retain(|hook| hook.table_name != table_name || hook.row_id != row_id);
    }
}

pub(super) async fn maybe_wait_after_reload(table_name: &'static str, row_id: String) {
    let hook = ROW_PATH_RELOAD_HOOKS.get().and_then(|lock| {
        lock.lock()
            .expect("row-path reload hook mutex poisoned")
            .iter()
            .find(|hook| hook.table_name == table_name && hook.row_id == row_id)
            .cloned()
    });

    let Some(hook) = hook else {
        return;
    };

    hook.reached.notify_waiters();
    hook.release.notified().await;
    clear_row_path_reload_hook(table_name, &row_id);
}
