use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    metrics::RunnerLoopHeartbeat,
    phase::{RedoAttemptFence, RunMode},
    progress_monitor::RunnerPhaseProgress,
};

use super::PhaseRunner;

pub(super) struct Execution {
    pub(super) mode: RunMode,
    pub(super) redo_attempt: Option<RedoAttemptFence>,
}

impl Execution {
    pub(super) const fn new(mode: RunMode, redo_attempt: Option<RedoAttemptFence>) -> Self {
        Self { mode, redo_attempt }
    }
}

pub(super) type BeforeRedoProgressWrite =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

impl PhaseRunner {
    pub fn with_loop_heartbeat(mut self, heartbeat: RunnerLoopHeartbeat) -> Self {
        self.loop_heartbeat = Some(heartbeat);
        self
    }

    pub fn with_phase_progress(mut self, progress: RunnerPhaseProgress) -> Self {
        self.phase_progress = progress;
        self
    }

    pub(super) fn record_loop_progress(&self, chain_id: &str) {
        if let Some(heartbeat) = &self.loop_heartbeat {
            heartbeat.record_progress(chain_id);
        }
    }

    #[doc(hidden)]
    pub fn with_before_redo_progress_write<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.before_redo_progress_write = Some(Arc::new(move || Box::pin(hook())));
        self
    }

    pub(super) async fn before_redo_progress_write(&self) {
        if let Some(hook) = self.before_redo_progress_write.as_deref() {
            hook().await;
        }
    }
}
