use std::{collections::BTreeSet, sync::Arc};

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    config::{ChainConfig, RuntimeConfig},
    error::{RunnerError, RunnerResult},
    phase::{PhaseName, RunMode},
    phase_lock::PhaseLock,
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
        self.settle_unconfigured_phases(config).await?;
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
        self.store.initialize_chain(&chain.chain_id).await?;
        self.recover_stopped_phases(chain).await?;
        self.run_spine_phase(chain, PhaseName::Ingest, cancellation.clone())
            .await?;
        self.repair_discovery_coverage(chain, cancellation.clone())
            .await?;
        self.run_spine_phase(chain, PhaseName::Project, cancellation.clone())
            .await?;
        // This barrier applies to both serial Verify and the Verify/live combined path.
        self.reject_pending_required_ingest(&chain.chain_id).await?;
        self.run_required_verify_redo(chain, cancellation.clone())
            .await?;

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
            return self.run_live_follow(chain, cancellation).await;
        }
        self.run_verify_and_live(chain, cancellation).await
    }

    pub(super) async fn run_required_verify_redo(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let Some(range) = self
            .store
            .required_redo_range(&chain.chain_id, PhaseName::Verify)
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
