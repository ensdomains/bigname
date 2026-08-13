use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::{
    config::{ChainConfig, RuntimeConfig},
    error::RunnerResult,
    phase::{PhaseName, RunMode},
};

use super::{PhaseRunner, SupervisorReport};

impl PhaseRunner {
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
        self.record_loop_progress(&chain.chain_id);
        self.store.initialize_chain(&chain.chain_id).await?;
        self.run_spine_phase(chain, PhaseName::Ingest, cancellation.clone())
            .await?;
        self.recover_stopped_live(chain).await?;
        self.catch_up_for_required_redo(chain, cancellation.clone())
            .await?;
        for phase in [PhaseName::Interpret, PhaseName::Project] {
            self.run_spine_phase(chain, phase, cancellation.clone())
                .await?;
        }

        if chain.verify_before_live {
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
}
