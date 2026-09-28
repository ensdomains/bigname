use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::RunnerResult,
    phase::{BlockRange, PhaseName, RunMode},
    phase_lock::PhaseLock,
    runner_support::{cancelled_redo_error, release_lock_racing_stop, resumable_recompute_marker},
};

use super::PhaseRunner;

impl PhaseRunner {
    pub(super) async fn redo_recompute_flags(
        &self,
        chain: &ChainConfig,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let mode = RunMode::RecomputeFlags(range);
        self.phases
            .get(PhaseName::Interpret)
            .preflight(&chain.chain_id, &chain.sources, &mode)?;
        self.prepared_for_redo(chain, PhaseName::Interpret, range, true, &cancellation)
            .await?;

        // Keep Project out while Interpret updates normalization metadata and atomically
        // records any visibility transitions for ordinary Interpret and Project replay.
        let acquired = crate::shutdown::until_cancelled(&cancellation, async {
            resumable_recompute_marker(&self.store, &chain.chain_id, range).await?;
            let mut project_lock = PhaseLock::acquire(
                self.database.connect_options(),
                &chain.chain_id,
                PhaseName::Project,
            )
            .await?;
            project_lock.check_alive().await?;
            Ok(project_lock)
        })
        .await?;
        let Some(mut project_lock) = acquired else {
            return Err(cancelled_redo_error(
                &self.chain_stop_clock(&chain.chain_id),
                &self.store,
                &chain.chain_id,
                PhaseName::Interpret,
            )
            .await?);
        };
        let stopped = cancellation.clone();
        let result = project_lock
            .run_while_alive(
                self.timing.live_poll_interval,
                self.run_phase_with_restart(chain, PhaseName::Interpret, mode, cancellation),
            )
            .await;
        let release = release_lock_racing_stop(
            &self.chain_stop_clock(&chain.chain_id),
            project_lock,
            &chain.chain_id,
            PhaseName::Project,
            &stopped,
        )
        .await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => Err(error.with_secondary(
                "release project lock after interpret recompute-flags",
                release_error,
            )),
        }
    }
}
