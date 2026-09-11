use std::sync::{Arc, OnceLock};

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    config::ChainConfig,
    error::RunnerResult,
    phase::{PhaseName, RunMode},
    runner_support::{Backoff, cancelled_redo_error, record_live_mismatch_with_lock},
};

use super::{LiveMismatchReason, PhaseRunner};

impl PhaseRunner {
    pub(super) async fn run_phase_with_restart_inner(
        &self,
        chain: &ChainConfig,
        phase_name: PhaseName,
        mode: RunMode,
        cancellation: CancellationToken,
        live_mismatch: Option<LiveMismatchReason>,
        automatic_discovery_ingest: bool,
    ) -> RunnerResult<()> {
        let phase = self.phases.get(phase_name);
        let mut backoff = Backoff::new(&self.timing);
        loop {
            self.record_loop_progress(&chain.chain_id);
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
            let result = self
                .run_phase_once(
                    chain,
                    Arc::clone(&phase),
                    mode.clone(),
                    cancellation.clone(),
                    live_mismatch.as_deref(),
                    automatic_discovery_ingest,
                )
                .await;
            self.record_loop_progress(&chain.chain_id);
            match result {
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
}
