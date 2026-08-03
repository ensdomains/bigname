use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    heads::load_marker,
    phase::{BlockRange, PhaseName, RunMode},
    phase_lock::PhaseLock,
};

use super::PhaseRunner;

impl PhaseRunner {
    pub(super) async fn catch_up_for_required_redo(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        loop {
            let Some(range) = self
                .store
                .required_redo_range(&chain.chain_id, PhaseName::Interpret)
                .await?
            else {
                return Ok(());
            };
            if required_range_is_readable(self.store.pool(), &chain.chain_id, range).await? {
                return Ok(());
            }
            self.run_phase_with_restart(
                chain,
                PhaseName::Live,
                RunMode::Normal,
                cancellation.clone(),
            )
            .await?;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if required_range_is_readable(self.store.pool(), &chain.chain_id, range).await? {
                return Ok(());
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.timing.live_poll_interval) => {}
            }
        }
    }

    pub(super) async fn run_spine_phase(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        if let Some(range) = self
            .store
            .required_redo_range(&chain.chain_id, phase)
            .await?
        {
            if phase == PhaseName::Interpret {
                self.recover_stopped_live(chain).await?;
            }
            self.run_phase_with_restart(chain, phase, RunMode::Redo(range), cancellation.clone())
                .await?;
        }
        self.run_phase_with_restart(chain, phase, RunMode::Normal, cancellation)
            .await
    }

    pub(super) async fn recover_stopped_live(&self, chain: &ChainConfig) -> RunnerResult<()> {
        let live_lock = PhaseLock::acquire(
            self.database.connect_options(),
            &chain.chain_id,
            PhaseName::Live,
        )
        .await?;
        let result = self.store.complete_stopped_live(&chain.chain_id).await;
        let release = live_lock.release().await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => {
                Err(error.with_secondary("release stopped live lock before redo", release_error))
            }
        }
    }
}

async fn required_range_is_readable(
    pool: &sqlx::PgPool,
    chain_id: &str,
    range: BlockRange,
) -> RunnerResult<bool> {
    let latest: Option<i64> =
        sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| {
                RunnerError::database(
                    format!(
                        "failed to load canonical head before required redo for chain {chain_id}"
                    ),
                    error,
                )
            })?;
    if latest.is_none_or(|latest| latest < range.to) {
        return Ok(false);
    }
    Ok(load_marker(pool, chain_id, range.to).await?.is_some())
}
