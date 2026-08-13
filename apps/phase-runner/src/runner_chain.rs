use std::{collections::BTreeSet, sync::Arc};

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    config::{ChainConfig, RuntimeConfig},
    error::RunnerResult,
    phase::{PhaseName, RunMode},
    phase_lock::PhaseLock,
};

use super::{PhaseRunner, SupervisorReport};

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
        for (chain_id, phase) in self.store.active_normal_phases().await? {
            if configured.contains(chain_id.as_str()) {
                continue;
            }
            let phase_lock =
                PhaseLock::acquire(self.database.connect_options(), &chain_id, phase).await?;
            let result = self.store.complete_stopped_phase(&chain_id, phase).await;
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
            }
        }
        Ok(())
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
