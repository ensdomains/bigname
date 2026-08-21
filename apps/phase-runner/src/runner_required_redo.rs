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
            self.catch_up_required_range(chain, range, cancellation.clone())
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

    pub(super) async fn catch_up_required_range(
        &self,
        chain: &ChainConfig,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        while !required_range_is_readable(self.store.pool(), &chain.chain_id, range).await? {
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
            if !required_range_is_readable(self.store.pool(), &chain.chain_id, range).await? {
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    () = tokio::time::sleep(self.timing.live_poll_interval) => {}
                }
            }
        }
        Ok(())
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
            if phase == PhaseName::Ingest {
                let mut current = range;
                loop {
                    self.catch_up_required_range(chain, current, cancellation.clone())
                        .await?;
                    if cancellation.is_cancelled() {
                        return Ok(());
                    }
                    let Some(updated) = self
                        .store
                        .required_redo_range(&chain.chain_id, PhaseName::Ingest)
                        .await?
                    else {
                        return self
                            .run_phase_with_restart(
                                chain,
                                PhaseName::Ingest,
                                RunMode::Normal,
                                cancellation,
                            )
                            .await;
                    };
                    if updated == current {
                        return Err(crate::transitions::required_ingest_redo_error(
                            &chain.chain_id,
                            current,
                        ));
                    }
                    current = updated;
                }
            }
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
        let mut live_lock = PhaseLock::acquire(
            self.database.connect_options(),
            &chain.chain_id,
            PhaseName::Live,
        )
        .await?;
        let result = self
            .store
            .complete_stopped_live(live_lock.connection(), &chain.chain_id)
            .await;
        let release = live_lock.release().await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => {
                Err(error.with_secondary("release stopped live lock before redo", release_error))
            }
        }
    }

    pub(super) async fn recover_stopped_phases(&self, chain: &ChainConfig) -> RunnerResult<()> {
        for phase in [PhaseName::Interpret, PhaseName::Project, PhaseName::Verify] {
            let mut phase_lock =
                PhaseLock::acquire(self.database.connect_options(), &chain.chain_id, phase).await?;
            let result =
                resolve_stopped_phase(phase_lock.connection(), &chain.chain_id, phase).await;
            let release = phase_lock.release().await;
            match (result, release) {
                (Ok(()), Ok(())) => {}
                (Ok(()), Err(error)) | (Err(error), Ok(())) => return Err(error),
                (Err(error), Err(release_error)) => {
                    return Err(error.with_secondary(
                        "release stopped finite-phase lock during runner restart",
                        release_error,
                    ));
                }
            }
        }
        self.recover_stopped_live(chain).await
    }
}

async fn resolve_stopped_phase(
    lock_connection: &mut sqlx::PgConnection,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<()> {
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = CASE
                 WHEN current_block_number IS NOT NULL
                   AND current_block_number = target_block_number
                   AND current_block_hash IS NOT NULL
                   AND current_block_hash = target_block_hash
                   AND phase_name <> 'verify'
                 THEN 'completed'
                 ELSE 'failed'
             END,
             last_error = CASE
                 WHEN current_block_number IS NOT NULL
                   AND current_block_number = target_block_number
                   AND current_block_hash IS NOT NULL
                   AND current_block_hash = target_block_hash
                   AND phase_name <> 'verify'
                 THEN NULL
                 WHEN phase_name = 'verify'
                   AND current_block_number IS NOT NULL
                   AND current_block_number = target_block_number
                   AND current_block_hash IS NOT NULL
                   AND current_block_hash = target_block_hash
                   AND verification_level IS NOT NULL
                 THEN $3 || 'runner stopped after Verify saved its final checkpoint; \
                     revalidate retained verification before completion'
                 ELSE 'phase stopped before completion; its advisory lock was free at \
                     runner restart'
             END,
             finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2
           AND phase_status IN ('running', 'paused')
           AND NOT redo_in_progress",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(crate::error::COMPLETED_VALIDATION_FAILURE_PREFIX)
    .execute(lock_connection)
    .await
    .map_err(|error| {
        RunnerError::lock_connection_lost(format!(
            "advisory-lock connection was lost while resolving stopped phase {phase} for chain \
             {chain_id}; stopping so the next runner can recheck durable phase state: {error}"
        ))
    })?;
    Ok(())
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
