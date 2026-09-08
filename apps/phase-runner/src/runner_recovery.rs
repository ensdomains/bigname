//! Start-up recovery: settling the phases a previous run left `running`.

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    phase::PhaseName,
    phase_lock::PhaseLock,
};

use super::PhaseRunner;

impl PhaseRunner {
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
        for phase in [
            PhaseName::Ingest,
            PhaseName::Interpret,
            PhaseName::Project,
            PhaseName::Verify,
        ] {
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
    if phase == PhaseName::Ingest {
        sqlx::query(
            "UPDATE chain_phase_state
             SET last_error = $3 || substring(last_error FROM char_length($4) + 1),
                 updated_at = now()
             WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress
               AND last_error LIKE $5",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(crate::redo_stamp::REQUIRED_REDO_PREFIX)
        .bind(crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX)
        .bind(format!(
            "{}%",
            crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX
        ))
        .execute(lock_connection)
        .await
        .map_err(|error| {
            RunnerError::lock_connection_lost(format!(
                "advisory-lock connection was lost while settling a stopped required Ingest redo \
                 for chain {chain_id}; stopping so the next runner can recheck durable phase \
                 state: {error}"
            ))
        })?;
        return Ok(());
    }
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
