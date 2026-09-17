use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    heads::publish_heads,
    ingest_progress,
    metrics::RunnerLoopHeartbeat,
    phase::{
        PhaseBatchOutcome, PhaseContext, PhaseName, PhaseProgress, RedoAttemptFence, RunMode,
        VerificationLevel,
    },
    phase_lock::PhaseLock,
    progress_monitor::{ProgressToken, RunnerPhaseProgress},
    runner_support::HeartbeatThrottle,
    state_persistence::validate_progress,
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

    /// How long required work -- start-up recovery, a finished batch's settlement --
    /// may run past an accepted stop before the run gives it up. Ten seconds
    /// unless a test shortens it.
    pub fn with_stop_deadline(mut self, deadline: std::time::Duration) -> Self {
        self.stop_budget = deadline;
        self.chain_stop_clocks
            .lock()
            .expect("stop clocks lock")
            .clear();
        self
    }

    /// The stop budget for a chain's own waits -- start-up recovery, fence
    /// releases, mismatch records, recovery lookups -- as opposed to a phase
    /// attempt's, which each attempt starts for itself at its batch boundary.
    pub(super) fn chain_stop_clock(&self, chain_id: &str) -> Arc<crate::runner_support::StopClock> {
        Arc::clone(
            self.chain_stop_clocks
                .lock()
                .expect("stop clocks lock")
                .entry(chain_id.to_owned())
                .or_insert_with(|| {
                    Arc::new(crate::runner_support::StopClock::new(self.stop_budget))
                }),
        )
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

impl PhaseRunner {
    /// Record a finished batch: probe the lock, publish heads, write progress,
    /// confirm completion, heartbeat. The batch's own writes are already
    /// committed, so this runs to completion; the caller bounds it after a stop.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn settle_batch(
        &self,
        chain: &ChainConfig,
        phase_name: PhaseName,
        mode: &RunMode,
        redo_attempt: Option<RedoAttemptFence>,
        context: PhaseContext,
        outcome: RunnerResult<PhaseBatchOutcome>,
        progress_token: ProgressToken,
        retained_verification_level: Option<VerificationLevel>,
        heartbeat: &mut HeartbeatThrottle,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<(PhaseBatchOutcome, PhaseProgress)> {
        phase_lock.check_alive().await?;
        let outcome = outcome?;
        let progress = outcome.progress().clone();
        validate_progress(
            phase_name,
            &progress,
            matches!(&outcome, PhaseBatchOutcome::Complete(_)),
        )?;
        if phase_name == PhaseName::Verify {
            crate::verify_phase::validate_reported_level(
                &chain.chain_id,
                &chain.sources,
                progress.verification_level,
            )?;
        }
        if phase_name == PhaseName::Ingest && matches!(mode, RunMode::Normal) {
            ingest_progress::validate(
                &chain.intake_sources(),
                &progress,
                matches!(&outcome, PhaseBatchOutcome::Complete(_)),
            )?;
        }
        if progress.heads.is_some() && !matches!(phase_name, PhaseName::Ingest | PhaseName::Live) {
            return Err(RunnerError::data_integrity(format!(
                "phase {phase_name} cannot publish chain heads; only ingest and live own \
                 chain-head updates"
            )));
        }
        if matches!(mode, RunMode::Normal)
            && let Some(heads) = &progress.heads
        {
            phase_lock.check_alive().await?;
            let heads =
                crate::heads::retain_stored_finality(self.store.pool(), &chain.chain_id, heads)
                    .await?;
            publish_heads(self.store.pool(), &chain.chain_id, &heads).await?;
        }
        phase_lock.check_alive().await?;
        if mode.is_redo() {
            self.before_redo_progress_write().await;
        }
        if phase_name == PhaseName::Ingest && matches!(mode, RunMode::Normal) {
            phase_lock.check_alive().await?;
            self.store
                .record_ingest_progress(&chain.chain_id, &chain.intake_sources(), &progress)
                .await?;
        } else {
            self.store
                .record_progress(&chain.chain_id, phase_name, mode, redo_attempt, &progress)
                .await?;
        }
        self.phase_progress
            .record_committed(progress_token, &outcome);
        if matches!(outcome, PhaseBatchOutcome::Complete(_)) {
            self.confirm_progress(context).await?;
        }
        if phase_name == PhaseName::Verify && matches!(mode, RunMode::Normal) {
            crate::verify_level::warn_optional_downgrade(
                &chain.chain_id,
                retained_verification_level,
                progress.verification_level,
            );
        }
        phase_lock.check_alive().await?;
        heartbeat
            .record_if_due(&self.store, &self.instance_id, &chain.chain_id, phase_name)
            .await?;
        Ok((outcome, progress))
    }
}
