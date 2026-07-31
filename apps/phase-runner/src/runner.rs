use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    capacity::CapacityGuard,
    config::{ChainConfig, RuntimeConfig, TimingConfig},
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{load_available_heads, load_marker, publish_heads},
    phase::{BlockRange, Phase, PhaseBatchOutcome, PhaseContext, PhaseName, PhaseSet, RunMode},
    phase_lock::PhaseLock,
    runner_support::{Backoff, HeartbeatThrottle, PhaseLoopResult},
    state::{PhaseStore, StartDisposition},
};

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
        self.store.initialize_chain(&chain.chain_id).await?;
        let (phase, mode) = match selection {
            RedoPhase::Phase(phase) => (phase, RunMode::Redo(range)),
            // Normalization flags are interpreter-owned, so this mode uses the interpreter lock.
            RedoPhase::RecomputeFlags => (PhaseName::Interpret, RunMode::RecomputeFlags(range)),
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
        let verify = self.run_phase_with_restart(
            chain,
            PhaseName::Verify,
            RunMode::Normal,
            pair_cancellation.clone(),
        );
        let live = self.run_phase_with_restart(
            chain,
            PhaseName::Live,
            RunMode::Normal,
            pair_cancellation.clone(),
        );
        tokio::pin!(verify);
        tokio::pin!(live);

        tokio::select! {
            verify_result = &mut verify => {
                if let Err(error) = verify_result {
                    pair_cancellation.cancel();
                    let _ = live.await;
                    return Err(error);
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
        let phase = self.phases.get(phase_name);
        let mut backoff = Backoff::new(&self.timing);
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            match self
                .run_phase_once(
                    chain,
                    Arc::clone(&phase),
                    mode.clone(),
                    cancellation.clone(),
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
                        () = cancellation.cancelled() => return Ok(()),
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
    ) -> RunnerResult<()> {
        let phase_name = phase.name();
        let phase_lock =
            PhaseLock::acquire(self.database.connect_options(), &chain.chain_id, phase_name)
                .await?;
        let result = self
            .run_locked_phase(chain, phase, mode, cancellation)
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
    ) -> RunnerResult<()> {
        let phase_name = phase.name();
        let redo_session = if mode.is_redo() {
            Some(
                self.store
                    .begin_redo(&chain.chain_id, phase_name, &mode)
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
        if let Err(error) = self
            .store
            .start_heartbeat(&self.instance_id, &chain.chain_id, phase_name)
            .await
        {
            if let Some(session) = redo_session {
                self.store
                    .finish_redo(&chain.chain_id, phase_name, session, false)
                    .await
                    .map_err(|restore| {
                        RunnerError::transient(format!(
                            "{error}; additionally failed to restore phase state: {restore}"
                        ))
                    })?;
            }
            return Err(error);
        }
        let mut heartbeat = HeartbeatThrottle::new();
        let result = self
            .run_phase_batches(chain, phase, mode, cancellation, &mut heartbeat)
            .await;
        if let Some(session) = redo_session {
            let completed = matches!(&result, Ok(PhaseLoopResult::Completed(_)));
            let restore = self
                .store
                .finish_redo(&chain.chain_id, phase_name, session, completed)
                .await;
            return match (result, restore) {
                (Ok(_), Ok(())) => Ok(()),
                (Ok(_), Err(error)) => Err(error),
                (Err(error), Ok(())) => Err(error),
                (Err(error), Err(restore_error)) => Err(RunnerError::transient(format!(
                    "{error}; additionally failed to restore phase state after redo: \
                     {restore_error}"
                ))),
            };
        }
        match result {
            Ok(PhaseLoopResult::Completed(progress)) => {
                self.store
                    .complete_phase(&chain.chain_id, phase_name, &progress)
                    .await
            }
            Ok(PhaseLoopResult::Cancelled) => Ok(()),
            Err(error) => {
                if let Err(record_error) = self
                    .store
                    .fail_phase(&chain.chain_id, phase_name, &error.to_string())
                    .await
                {
                    return Err(RunnerError::transient(format!(
                        "{error}; additionally failed to record phase failure: {record_error}"
                    )));
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
    ) -> RunnerResult<PhaseLoopResult> {
        let phase_name = phase.name();
        let mut reserved_write_bytes = 0;
        loop {
            if cancellation.is_cancelled() {
                return Ok(PhaseLoopResult::Cancelled);
            }
            if self
                .wait_for_capacity(
                    chain,
                    phase_name,
                    reserved_write_bytes,
                    &cancellation,
                    heartbeat,
                )
                .await?
            {
                return Ok(PhaseLoopResult::Cancelled);
            }
            let context = self.phase_context(chain, phase_name, mode.clone()).await?;
            let outcome = phase.run_batch(context).await?;
            let progress = outcome.progress().clone();
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
                publish_heads(self.store.pool(), &chain.chain_id, heads).await?;
            }
            self.store
                .record_progress(&chain.chain_id, phase_name, &progress)
                .await?;
            if phase_name == PhaseName::Ingest && matches!(mode, RunMode::Normal) {
                self.store
                    .update_ingest_cursors(&chain.sources, &progress)
                    .await?;
            }
            heartbeat
                .record_if_due(&self.store, &self.instance_id, &chain.chain_id, phase_name)
                .await?;
            reserved_write_bytes = progress.estimated_write_bytes;

            match outcome {
                PhaseBatchOutcome::Complete(_) => {
                    return Ok(PhaseLoopResult::Completed(Box::new(progress)));
                }
                PhaseBatchOutcome::Continue(_) => {
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

    async fn phase_context(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        mode: RunMode,
    ) -> RunnerResult<PhaseContext> {
        let available_heads = match mode.range() {
            Some(range) => load_marker(self.store.pool(), &chain.chain_id, range.to)
                .await?
                .map(|latest| crate::heads::HeadMarkers {
                    latest,
                    safe: None,
                    finalized: None,
                }),
            None => load_available_heads(self.store.pool(), &chain.chain_id).await?,
        };
        let live_handoff = if phase == PhaseName::Live && matches!(mode, RunMode::Normal) {
            let handoff = self.store.ingest_handoff(&chain.chain_id).await?;
            if handoff.is_none() {
                return Err(RunnerError::data_integrity(format!(
                    "cannot start live phase for chain {} without the ingest handoff block",
                    chain.chain_id
                )));
            }
            handoff
        } else {
            None
        };
        let resume = self.store.phase_resume(&chain.chain_id, phase).await?;
        Ok(PhaseContext {
            chain_id: chain.chain_id.clone(),
            phase,
            mode,
            sources: Arc::clone(&chain.sources),
            available_heads,
            live_handoff,
            resume,
        })
    }

    async fn wait_for_capacity(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        reserved_write_bytes: u64,
        cancellation: &CancellationToken,
        heartbeat: &mut HeartbeatThrottle,
    ) -> RunnerResult<bool> {
        let mut paused = false;
        loop {
            let status = self
                .capacity
                .check(self.store.pool(), reserved_write_bytes)
                .await?;
            if status.is_available() {
                if paused {
                    self.store.resume_phase(&chain.chain_id, phase).await?;
                }
                return Ok(false);
            }
            if !paused {
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
