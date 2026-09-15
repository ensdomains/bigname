use std::{collections::BTreeSet, sync::Arc};

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    config::{ChainConfig, RuntimeConfig},
    error::{RunnerError, RunnerResult},
    phase::{PhaseName, RunMode},
    phase_lock::PhaseLock,
    runner_support::StopClock,
};

use super::{PhaseRunner, RedoPhase, SupervisorReport};

impl RedoPhase {
    pub const fn requires_intake_sources(self) -> bool {
        matches!(
            self,
            Self::Phase(
                PhaseName::Ingest | PhaseName::Interpret | PhaseName::Project | PhaseName::Verify
            ) | Self::All
        )
    }

    pub const fn requires_verify(self) -> bool {
        matches!(self, Self::Phase(PhaseName::Verify) | Self::All)
    }
}
impl PhaseRunner {
    pub async fn run(
        self: Arc<Self>,
        config: &RuntimeConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<SupervisorReport> {
        // Settlement closes out phases recorded against chains this start no longer
        // configures. It is required cleanup, not new work: abandoning it midway
        // leaves those phases active and blocks the next start, so it runs to
        // completion even when a stop is already pending. It is bounded in time
        // instead, so an accepted stop cannot wait on it past the grace period.
        bounded_recovery(
            &self.stop_clock,
            "start-up settlement",
            "",
            &cancellation,
            self.settle_unconfigured_phases(config),
        )
        .await?;
        if cancellation.is_cancelled() {
            return Ok(SupervisorReport::default());
        }
        crate::supervisor::run(self, config, cancellation).await
    }

    async fn settle_unconfigured_phases(&self, config: &RuntimeConfig) -> RunnerResult<()> {
        let configured = config
            .chains
            .iter()
            .map(|chain| chain.chain_id.as_str())
            .collect::<BTreeSet<_>>();
        for (chain_id, phase, observed_updated_at) in self.store.active_normal_phases().await? {
            if configured.contains(chain_id.as_str()) {
                continue;
            }
            let mut phase_lock =
                PhaseLock::acquire(self.database.connect_options(), &chain_id, phase).await?;
            let result = self
                .store
                .complete_unconfigured_phase(
                    phase_lock.connection(),
                    &chain_id,
                    phase,
                    observed_updated_at,
                )
                .await;
            let release = phase_lock.release().await;
            let settled = match (result, release) {
                (Ok(settled), Ok(())) => settled,
                (Ok(_), Err(error)) | (Err(error), Ok(())) => return Err(error),
                (Err(error), Err(release_error)) => {
                    return Err(error.with_secondary(
                        "release unconfigured phase lock after startup recovery",
                        release_error,
                    ));
                }
            };
            if settled {
                info!(
                    chain_id,
                    phase = %phase,
                    "settled active phase for unconfigured chain during startup"
                );
            } else {
                return Err(RunnerError::transient(format!(
                    "refused to settle chain {chain_id} phase {phase} because its state changed after startup discovery; retry startup"
                )));
            }
        }
        Ok(())
    }

    pub async fn run_chain(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        chain.require_intake_sources()?;
        self.record_loop_progress(&chain.chain_id);
        // Recovery settles phases a previous run left `running`, and `initialize_chain`
        // writes the rows it settles. Callers restart a chain with an already-cancelled
        // token precisely to run this cleanup, so it must not be skipped or abandoned
        // on a pending stop: doing so leaves those phases stuck and the next phase
        // start refuses. Both are bounded in time instead, so an accepted stop cannot
        // wait on them past the grace period.
        bounded_recovery(
            &self.stop_clock,
            "start-up recovery",
            &chain.chain_id,
            &cancellation,
            async {
                self.store.initialize_chain(&chain.chain_id).await?;
                self.recover_stopped_phases(chain).await
            },
        )
        .await?;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        self.run_spine_phase(chain, PhaseName::Ingest, cancellation.clone())
            .await?;
        self.repair_discovery_coverage(chain, cancellation.clone())
            .await?;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        self.run_spine_phase(chain, PhaseName::Project, cancellation.clone())
            .await?;
        // This barrier applies to both serial Verify and the Verify/live combined path.
        if !self
            .require_no_pending_ingest_unless_stopped(&chain.chain_id, &cancellation)
            .await?
        {
            return Ok(());
        }
        self.run_required_verify_redo(chain, cancellation.clone())
            .await?;

        // The tail futures are boxed here and at the redo dispatch: the phase state
        // machines are large in debug builds, and a caller that awaits one inline
        // (a test body, the supervisor) embeds it several times over in its own
        // frame, which overflowed the 2 MiB test-thread stack in CI.
        if Self::verify_before_live(chain)? {
            self.phases.get(PhaseName::Verify).preflight(
                &chain.chain_id,
                &chain.sources,
                &RunMode::Normal,
            )?;
            self.run_phase_with_restart(
                chain,
                PhaseName::Verify,
                RunMode::Normal,
                cancellation.clone(),
            )
            .await?;
            return Box::pin(self.run_live_follow(chain, cancellation)).await;
        }
        Box::pin(self.run_verify_and_live(chain, cancellation)).await
    }

    pub(super) async fn run_required_verify_redo(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let Some(Some(range)) = self
            .required_redo_range_unless_stopped(&chain.chain_id, PhaseName::Verify, &cancellation)
            .await?
        else {
            return Ok(());
        };
        let mode = RunMode::Redo(range);
        self.phases
            .get(PhaseName::Verify)
            .preflight(&chain.chain_id, &chain.sources, &mode)?;
        self.run_phase_with_restart(chain, PhaseName::Verify, mode, cancellation)
            .await
    }
}

/// Required work that a pending stop must not abandon -- start-up recovery, and
/// the writes that record a finished batch -- is bounded in wall-clock instead:
/// none of those statements carries its own timeout, so without this a stalled
/// connection or a row-lock wait could hold an accepted stop until the
/// supervisor escalates to SIGKILL.
pub(super) async fn bounded_recovery<T>(
    clock: &StopClock,
    what: &str,
    chain_id: &str,
    cancellation: &CancellationToken,
    work: impl std::future::Future<Output = RunnerResult<T>>,
) -> RunnerResult<T> {
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => return result,
        () = cancellation.cancelled() => {}
    }
    // A stop is pending from here on. Recovery is required cleanup, so it still runs
    // to completion -- but only within the deadline, because an accepted stop must not
    // wait it out past the supervisor's grace period. With no stop pending there is no
    // deadline at all: ordinary contention on a `chain_phase_state` row should delay a
    // start, not truncate its settlement pass.
    let deadline = clock.remaining();
    match tokio::time::timeout(deadline, work).await {
        Ok(result) => result,
        Err(_elapsed) => Err(RunnerError::stop_bound_expired(format!(
            "{what}{} did not finish within the {:.1} s left of the stop budget; another process may hold its rows",
            if chain_id.is_empty() {
                String::new()
            } else {
                format!(" for chain {chain_id}")
            },
            deadline.as_secs_f64()
        ))),
    }
}

#[cfg(test)]
mod recovery_deadline_tests {
    use tokio_util::sync::CancellationToken;

    const DEADLINE: std::time::Duration = std::time::Duration::from_millis(50);

    fn clock() -> crate::runner_support::StopClock {
        crate::runner_support::StopClock::new(DEADLINE)
    }

    #[tokio::test]
    async fn recovery_that_never_finishes_fails_once_a_stop_is_accepted() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = super::bounded_recovery(
            &clock(),
            "start-up recovery",
            "some-chain",
            &cancellation,
            async { std::future::pending::<super::RunnerResult<()>>().await },
        )
        .await
        .expect_err("a recovery that never finishes must not return Ok");
        assert!(!error.is_retryable(), "the stopping run must not retry it");
        let message = error.to_string();
        assert!(message.contains("did not finish within"), "{message}");
        assert!(message.contains("some-chain"), "{message}");
    }

    #[tokio::test]
    async fn recovery_outlasts_the_deadline_when_no_stop_is_pending() {
        // No stop, so contention must delay the start rather than truncate it.
        super::bounded_recovery(
            &clock(),
            "recovery",
            "some-chain",
            &CancellationToken::new(),
            async {
                tokio::time::sleep(DEADLINE * 3).await;
                Ok(())
            },
        )
        .await
        .expect("without a stop there is no deadline");
    }

    #[tokio::test]
    async fn recovery_that_finishes_inside_the_deadline_is_untouched() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        super::bounded_recovery(&clock(), "recovery", "some-chain", &cancellation, async {
            tokio::time::sleep(DEADLINE / 5).await;
            Ok(())
        })
        .await
        .expect("recovery inside the deadline must pass through");
    }
}
