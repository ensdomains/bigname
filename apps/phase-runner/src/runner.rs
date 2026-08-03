use std::sync::{Arc, OnceLock};

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    capacity::CapacityGuard,
    config::{ChainConfig, RuntimeConfig, TimingConfig},
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError, RunnerResult, VERIFICATION_MISMATCH_PREFIX},
    heads::publish_heads,
    ingest_progress,
    phase::{BlockRange, Phase, PhaseBatchOutcome, PhaseName, PhaseSet, RunMode},
    phase_lock::PhaseLock,
    runner_support::{
        Backoff, HeartbeatThrottle, PhaseLoopResult, cancelled_redo_error,
        finish_failed_redo_start, record_live_mismatch_with_lock, redo_outcome,
    },
    state::{PhaseStore, StartDisposition},
    state_persistence::{record_live_verification_mismatch, validate_progress},
};

#[path = "runner_context.rs"]
mod context;

type LiveMismatchReason = Arc<OnceLock<String>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedoPhase {
    Phase(PhaseName),
    RecomputeFlags,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SupervisorReport {
    pub stopped_chains: Vec<(String, RunnerError)>,
}

#[derive(Clone)]
pub struct PhaseRunner {
    database: RunnerDatabase,
    store: PhaseStore,
    phases: PhaseSet,
    capacity: CapacityGuard,
    instance_id: Arc<str>,
    timing: TimingConfig,
}

impl PhaseRunner {
    pub fn new(
        database: RunnerDatabase,
        phases: PhaseSet,
        capacity: CapacityGuard,
        instance_id: impl Into<String>,
        timing: TimingConfig,
    ) -> RunnerResult<Self> {
        timing.validate()?;
        let instance_id = instance_id.into();
        if instance_id.trim().is_empty() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "runner instance id must not be empty",
            ));
        }
        let store = PhaseStore::new(database.pool().clone());
        Ok(Self {
            database,
            store,
            phases,
            capacity,
            instance_id: Arc::from(instance_id),
            timing,
        })
    }

    pub async fn run(
        self: Arc<Self>,
        config: &RuntimeConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<SupervisorReport> {
        crate::supervisor::run(self, config, cancellation).await
    }

    pub async fn run_chain(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        self.store.initialize_chain(&chain.chain_id).await?;
        for phase in [PhaseName::Ingest, PhaseName::Interpret, PhaseName::Project] {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.run_phase_with_restart(chain, phase, RunMode::Normal, cancellation.clone())
                .await?;
        }

        if chain.verify_before_live {
            self.run_phase_with_restart(
                chain,
                PhaseName::Verify,
                RunMode::Normal,
                cancellation.clone(),
            )
            .await?;
            return self
                .run_phase_with_restart(chain, PhaseName::Live, RunMode::Normal, cancellation)
                .await;
        }
        self.run_verify_and_live(chain, cancellation).await
    }

    pub async fn redo(
        &self,
        chain: &ChainConfig,
        selection: RedoPhase,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        if selection == RedoPhase::RecomputeFlags {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                bigname_interpret::RECOMPUTE_FLAGS_UNAVAILABLE_REASON,
            ));
        }
        self.store.initialize_chain(&chain.chain_id).await?;
        let (phase, mode) = match selection {
            RedoPhase::Phase(phase) => (phase, RunMode::Redo(range)),
            RedoPhase::RecomputeFlags => unreachable!("unavailable mode returned above"),
        };
        self.run_phase_with_restart(chain, phase, mode, cancellation)
            .await
    }

    async fn run_verify_and_live(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let pair_cancellation = cancellation.child_token();
        let live_mismatch = Arc::new(OnceLock::new());
        let verify = self.run_phase_with_restart(
            chain,
            PhaseName::Verify,
            RunMode::Normal,
            pair_cancellation.clone(),
        );
        let live = self.run_phase_with_restart_inner(
            chain,
            PhaseName::Live,
            RunMode::Normal,
            pair_cancellation.clone(),
            Some(Arc::clone(&live_mismatch)),
        );
        tokio::pin!(verify);
        tokio::pin!(live);

        tokio::select! {
            verify_result = &mut verify => {
                if let Err(error) = verify_result {
                    if error.kind() == ErrorKind::VerificationMismatch {
                        let _ = live_mismatch.set(error.to_string());
                    }
                    pair_cancellation.cancel();
                    return match live.await {
                        Ok(()) => Err(error),
                        Err(live_error) => Err(error.with_secondary(
                            "stop live after verification failed",
                            live_error,
                        )),
                    };
                }
                live.await
            }
            live_result = &mut live => {
                if let Err(error) = live_result {
                    pair_cancellation.cancel();
                    let _ = verify.await;
                    return Err(error);
                }
                pair_cancellation.cancel();
                verify.await
            }
        }
    }

    async fn run_phase_with_restart(
        &self,
        chain: &ChainConfig,
        phase_name: PhaseName,
        mode: RunMode,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        self.run_phase_with_restart_inner(chain, phase_name, mode, cancellation, None)
            .await
    }

    async fn run_phase_with_restart_inner(
        &self,
        chain: &ChainConfig,
        phase_name: PhaseName,
        mode: RunMode,
        cancellation: CancellationToken,
        live_mismatch: Option<LiveMismatchReason>,
    ) -> RunnerResult<()> {
        let phase = self.phases.get(phase_name);
        let mut backoff = Backoff::new(&self.timing);
        loop {
            if cancellation.is_cancelled() {
                if mode.is_redo() {
                    return Err(
                        cancelled_redo_error(&self.store, &chain.chain_id, phase_name).await?,
                    );
                }
                if phase_name == PhaseName::Live
                    && matches!(mode, RunMode::Normal)
                    && let Some(reason) = live_mismatch.as_deref().and_then(OnceLock::get)
                {
                    record_live_mismatch_with_lock(
                        &self.database,
                        &self.store,
                        &chain.chain_id,
                        reason,
                    )
                    .await?;
                }
                return Ok(());
            }
            match self
                .run_phase_once(
                    chain,
                    Arc::clone(&phase),
                    mode.clone(),
                    cancellation.clone(),
                    live_mismatch.as_deref(),
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if error.is_retryable() => {
                    let delay = backoff.next_delay();
                    warn!(
                        chain_id = chain.chain_id,
                        phase = %phase_name,
                        error = %error,
                        retry_delay_ms = delay.as_millis(),
                        "phase failed with a retryable error"
                    );
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            if mode.is_redo() {
                                return Err(cancelled_redo_error(
                                    &self.store,
                                    &chain.chain_id,
                                    phase_name,
                                )
                                .await?);
                            }
                            if phase_name == PhaseName::Live
                                && let Some(reason) =
                                    live_mismatch.as_deref().and_then(OnceLock::get)
                            {
                                record_live_mismatch_with_lock(
                                    &self.database,
                                    &self.store,
                                    &chain.chain_id,
                                    reason,
                                )
                                .await?;
                            }
                            return Ok(());
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn run_phase_once(
        &self,
        chain: &ChainConfig,
        phase: Arc<dyn Phase>,
        mode: RunMode,
        cancellation: CancellationToken,
        live_mismatch: Option<&OnceLock<String>>,
    ) -> RunnerResult<()> {
        let phase_name = phase.name();
        let mut phase_lock =
            PhaseLock::acquire(self.database.connect_options(), &chain.chain_id, phase_name)
                .await?;
        phase_lock.check_alive().await?;
        let result = self
            .run_locked_phase(
                chain,
                phase,
                mode,
                cancellation,
                live_mismatch,
                &mut phase_lock,
            )
            .await;
        let release = phase_lock.release().await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => {
                warn!(
                    chain_id = chain.chain_id,
                    phase = %phase_name,
                    error = %release_error,
                    "phase lock release also failed"
                );
                Err(error)
            }
        }
    }

    async fn run_locked_phase(
        &self,
        chain: &ChainConfig,
        phase: Arc<dyn Phase>,
        mode: RunMode,
        cancellation: CancellationToken,
        live_mismatch: Option<&OnceLock<String>>,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<()> {
        let phase_name = phase.name();
        let redo_session = if mode.is_redo() {
            Some(
                self.store
                    .begin_redo(&chain.chain_id, phase_name, &mode, chain.sources.as_ref())
                    .await?,
            )
        } else {
            match self
                .store
                .start_phase(&chain.chain_id, phase_name, &mode)
                .await?
            {
                StartDisposition::AlreadyCompleted => return Ok(()),
                StartDisposition::Started => {}
            }
            None
        };
        phase_lock.check_alive().await?;
        if let Err(error) = self
            .store
            .start_heartbeat(&self.instance_id, &chain.chain_id, phase_name)
            .await
        {
            phase_lock.check_alive().await?;
            if let Some(session) = redo_session {
                return finish_failed_redo_start(
                    &self.store,
                    &chain.chain_id,
                    phase_name,
                    session,
                    error,
                )
                .await;
            }
            return Err(error);
        }
        let mut heartbeat = HeartbeatThrottle::new();
        let result = self
            .run_phase_batches(
                chain,
                phase,
                mode.clone(),
                cancellation,
                &mut heartbeat,
                phase_lock,
            )
            .await;
        let result = match result {
            Ok(PhaseLoopResult::Cancelled) if mode.is_redo() => {
                Err(cancelled_redo_error(&self.store, &chain.chain_id, phase_name).await?)
            }
            result => result,
        };
        if let Err(error) = &result
            && !error.permits_pool_writes_after_error()
        {
            return Err(error.clone());
        }
        if let Some(session) = redo_session {
            phase_lock.check_alive().await?;
            let restore = self
                .store
                .finish_redo(&chain.chain_id, phase_name, session, redo_outcome(&result))
                .await;
            return match (result, restore) {
                (Ok(_), Ok(())) => Ok(()),
                (Ok(_), Err(error)) => Err(error),
                (Err(error), Ok(())) => Err(error),
                (Err(error), Err(restore_error)) => {
                    Err(error.with_secondary("record phase state after redo", restore_error))
                }
            };
        }
        match result {
            Ok(PhaseLoopResult::Completed(progress)) => {
                phase_lock.check_alive().await?;
                self.store
                    .complete_phase(&chain.chain_id, phase_name, &progress)
                    .await
            }
            Ok(PhaseLoopResult::Cancelled) => {
                phase_lock.check_alive().await?;
                if phase_name == PhaseName::Live
                    && let Some(reason) = live_mismatch.and_then(OnceLock::get)
                    && !record_live_verification_mismatch(
                        self.store.pool(),
                        &chain.chain_id,
                        reason,
                    )
                    .await?
                {
                    return Err(RunnerError::data_integrity(format!(
                        "verification mismatch could not mark live failed for chain {}",
                        chain.chain_id
                    )));
                }
                Ok(())
            }
            Err(error) => {
                phase_lock.check_alive().await?;
                let failure_reason = if phase_name == PhaseName::Verify
                    && error.kind() == ErrorKind::VerificationMismatch
                {
                    format!("{VERIFICATION_MISMATCH_PREFIX}{error}")
                } else {
                    error.to_string()
                };
                if let Err(record_error) = self
                    .store
                    .fail_phase(&chain.chain_id, phase_name, &failure_reason)
                    .await
                {
                    return Err(error.with_secondary("record phase failure", record_error));
                }
                Err(error)
            }
        }
    }

    async fn run_phase_batches(
        &self,
        chain: &ChainConfig,
        phase: Arc<dyn Phase>,
        mode: RunMode,
        cancellation: CancellationToken,
        heartbeat: &mut HeartbeatThrottle,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<PhaseLoopResult> {
        let phase_name = phase.name();
        let mut reserved_write_bytes = 0;
        loop {
            if cancellation.is_cancelled() {
                return Ok(PhaseLoopResult::Cancelled);
            }
            phase_lock.check_alive().await?;
            if self
                .wait_for_capacity(
                    chain,
                    phase_name,
                    reserved_write_bytes,
                    &cancellation,
                    heartbeat,
                    phase_lock,
                )
                .await?
            {
                return Ok(PhaseLoopResult::Cancelled);
            }
            let context = self.phase_context(chain, phase_name, mode.clone()).await?;
            let outcome = phase_lock
                .run_while_alive(self.timing.live_poll_interval, phase.run_batch(context))
                .await;
            phase_lock.check_alive().await?;
            let outcome = outcome?;
            let progress = outcome.progress().clone();
            validate_progress(
                phase_name,
                &progress,
                matches!(&outcome, PhaseBatchOutcome::Complete(_)),
            )?;
            if phase_name == PhaseName::Ingest && matches!(mode, RunMode::Normal) {
                ingest_progress::validate(
                    &chain.sources,
                    &progress,
                    matches!(&outcome, PhaseBatchOutcome::Complete(_)),
                )?;
            }
            if progress.heads.is_some()
                && !matches!(phase_name, PhaseName::Ingest | PhaseName::Live)
            {
                return Err(RunnerError::data_integrity(format!(
                    "phase {phase_name} cannot publish chain heads; only ingest and live own \
                     chain-head updates"
                )));
            }
            if matches!(mode, RunMode::Normal)
                && let Some(heads) = &progress.heads
            {
                phase_lock.check_alive().await?;
                publish_heads(self.store.pool(), &chain.chain_id, heads).await?;
            }
            phase_lock.check_alive().await?;
            self.store
                .record_progress(&chain.chain_id, phase_name, &mode, &progress)
                .await?;
            if phase_name == PhaseName::Ingest && matches!(mode, RunMode::Normal) {
                phase_lock.check_alive().await?;
                self.store
                    .update_ingest_cursors(&chain.sources, &progress)
                    .await?;
            }
            phase_lock.check_alive().await?;
            heartbeat
                .record_if_due(&self.store, &self.instance_id, &chain.chain_id, phase_name)
                .await?;
            reserved_write_bytes = progress.estimated_write_bytes;

            match outcome {
                PhaseBatchOutcome::Complete(_) => {
                    return Ok(PhaseLoopResult::Completed(Box::new(progress)));
                }
                PhaseBatchOutcome::Continue(_) => {}
                PhaseBatchOutcome::Idle(_) => {
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            return Ok(PhaseLoopResult::Cancelled);
                        }
                        () = tokio::time::sleep(self.timing.live_poll_interval) => {}
                    }
                }
            }
        }
    }

    async fn wait_for_capacity(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        reserved_write_bytes: u64,
        cancellation: &CancellationToken,
        heartbeat: &mut HeartbeatThrottle,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<bool> {
        let mut paused = false;
        loop {
            phase_lock.check_alive().await?;
            let status = self
                .capacity
                .check(self.store.pool(), reserved_write_bytes)
                .await;
            phase_lock.check_alive().await?;
            let status = status?;
            if status.is_available() {
                if paused {
                    phase_lock.check_alive().await?;
                    self.store.resume_phase(&chain.chain_id, phase).await?;
                }
                return Ok(false);
            }
            if !paused {
                phase_lock.check_alive().await?;
                self.store.pause_phase(&chain.chain_id, phase).await?;
                paused = true;
            }
            warn!(
                chain_id = chain.chain_id,
                phase = %phase,
                breach_reasons = ?status.breach_reasons,
                database_size_bytes = status.measurement.database_size_bytes,
                free_disk_bytes = status.measurement.free_disk_bytes,
                reserved_write_bytes,
                "phase paused until storage capacity recovers"
            );
            phase_lock.check_alive().await?;
            heartbeat
                .record_if_due(&self.store, &self.instance_id, &chain.chain_id, phase)
                .await?;
            tokio::select! {
                () = cancellation.cancelled() => return Ok(true),
                () = tokio::time::sleep(self.capacity.poll_interval()) => {}
            }
        }
    }
}
