use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
    phase_lock::PhaseLock,
    runner_support::{cancelled_redo_error, resumable_recompute_marker},
};

use super::{PendingProjectRedoRow, PhaseRunner};

impl PhaseRunner {
    pub(super) async fn redo_recompute_flags(
        &self,
        chain: &ChainConfig,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let mode = RunMode::RecomputeFlags(range);
        for phase in [PhaseName::Project, PhaseName::Interpret] {
            self.phases
                .get(phase)
                .preflight(&chain.chain_id, &chain.sources, &mode)?;
        }
        self.prepared_for_redo(chain, PhaseName::Interpret, range, true, &cancellation)
            .await?;

        // The Project refresh preparation takes the Project lock and commits redo
        // state, so it is raced too: a stop must not go on to create it.
        let setup = crate::shutdown::until_cancelled(&cancellation, async {
            if resumable_recompute_marker(&self.store, &chain.chain_id, range).await? {
                return Ok(None);
            }
            self.prepare_project_recompute(chain, range).await.map(Some)
        })
        .await?;
        let Some(setup) = setup else {
            return Err(
                cancelled_redo_error(&self.store, &chain.chain_id, PhaseName::Interpret).await?,
            );
        };
        let Some((run_project_now, project_range)) = setup else {
            return self
                .run_recompute_interpret_with_project_lock(chain, mode, cancellation)
                .await;
        };
        if run_project_now {
            self.run_phase_with_restart(
                chain,
                PhaseName::Project,
                RunMode::Redo(project_range),
                cancellation.clone(),
            )
            .await?;
        }
        if cancellation.is_cancelled() {
            return Err(RunnerError::new(
                ErrorKind::InvalidTransition,
                format!(
                    "recompute-flags for chain {} stopped after its scoped Project refresh; \
                     rerun `phase-runner redo --chain {} --phase recompute-flags --from-block {} \
                     --to-block {}`",
                    chain.chain_id, chain.chain_id, range.from, range.to
                ),
            ));
        }
        self.run_recompute_interpret_with_project_lock(chain, mode, cancellation)
            .await
    }

    async fn run_recompute_interpret_with_project_lock(
        &self,
        chain: &ChainConfig,
        mode: RunMode,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        // The lock's connection and probe are raced; the batch under it is not,
        // so it can report its own cancellation through the redo marker.
        let acquired = crate::shutdown::until_cancelled(&cancellation, async {
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
            return Err(
                cancelled_redo_error(&self.store, &chain.chain_id, PhaseName::Interpret).await?,
            );
        };
        let result = project_lock
            .run_while_alive(
                self.timing.live_poll_interval,
                self.run_phase_with_restart(chain, PhaseName::Interpret, mode, cancellation),
            )
            .await;
        let release = project_lock.release().await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => Err(error.with_secondary(
                "release project lock after interpret recompute-flags",
                release_error,
            )),
        }
    }

    async fn prepare_project_recompute(
        &self,
        chain: &ChainConfig,
        range: BlockRange,
    ) -> RunnerResult<(bool, BlockRange)> {
        let mut phase_lock = PhaseLock::acquire(
            self.database.connect_options(),
            &chain.chain_id,
            PhaseName::Project,
        )
        .await?;
        phase_lock.check_alive().await?;
        let result = async {
            self.reject_pending_required_ingest(&chain.chain_id).await?;
            let mut transaction = self.store.pool().begin().await.map_err(|error| {
                RunnerError::database(
                    format!(
                        "failed to begin queued project refresh for chain {}",
                        chain.chain_id
                    ),
                    error,
                )
            })?;
            let pending: Option<PendingProjectRedoRow> = sqlx::query_as(
                "SELECT redo_mode, last_error,
                        redo_from_block_number, redo_to_block_number
                 FROM chain_phase_state
                 WHERE chain_id = $1
                   AND phase_name = 'project'
                   AND redo_in_progress
                 FOR UPDATE",
            )
            .bind(&chain.chain_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| {
                RunnerError::database(
                    format!(
                        "failed to inspect pending project redo for chain {}",
                        chain.chain_id
                    ),
                    error,
                )
            })?;
            if let Some((redo_mode, _, _, _)) = pending.as_ref()
                && redo_mode != "redo"
            {
                return Err(RunnerError::data_integrity(format!(
                    "project phase for chain {} has unsupported redo mode {redo_mode}",
                    chain.chain_id
                )));
            }
            let pending_range = pending
                .as_ref()
                .map(|(_, _, from, to)| {
                    let (Some(from), Some(to)) = (*from, *to) else {
                        return Err(RunnerError::data_integrity(
                            "active project redo is missing its persisted range",
                        ));
                    };
                    BlockRange::new(from, to)
                })
                .transpose()?;
            let staged_refresh = pending
                .as_ref()
                .and_then(|(_, last_error, _, _)| last_error.as_deref())
                .is_some_and(crate::redo_recompute::is_staged_project_refresh);
            let resume_queued_refresh = pending
                .as_ref()
                .and_then(|(_, last_error, _, _)| last_error.as_deref())
                .is_some_and(crate::redo_recompute::owns_project_refresh);
            if (staged_refresh || resume_queued_refresh)
                && pending_range
                    .is_some_and(|persisted| range.from > persisted.from || range.to < persisted.to)
            {
                let persisted = pending_range.expect("checked staged project range");
                return Err(RunnerError::data_integrity(format!(
                    "recompute-flags for chain {} has interrupted scoped Project work; rerun the \
                     full persisted range {}..={}",
                    chain.chain_id, persisted.from, persisted.to
                )));
            }
            let created_refresh = pending.is_none();
            let stamped = crate::redo_stamp::stamp_required_in_transaction(
                &mut transaction,
                &chain.chain_id,
                PhaseName::Project,
                range,
                crate::redo_recompute::PROJECT_REFRESH_REASON,
            )
            .await?;
            if !stamped {
                return Err(RunnerError::data_integrity(format!(
                    "cannot queue recompute-flags project refresh for chain {} range {}..={}: \
                     the recorded project extent does not cover the range",
                    chain.chain_id, range.from, range.to
                )));
            }
            let persisted: (i64, i64) = sqlx::query_as(
                "SELECT redo_from_block_number, redo_to_block_number
                 FROM chain_phase_state
                 WHERE chain_id = $1
                   AND phase_name = 'project'
                   AND redo_in_progress",
            )
            .bind(&chain.chain_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| {
                RunnerError::database(
                    format!(
                        "failed to load queued project refresh for chain {}",
                        chain.chain_id
                    ),
                    error,
                )
            })?;
            let persisted_range = BlockRange::new(persisted.0, persisted.1)?;
            let widened_staged_refresh = staged_refresh && pending_range != Some(persisted_range);
            if widened_staged_refresh {
                let result = sqlx::query(
                    "UPDATE chain_phase_state
                     SET last_error = $2, updated_at = now()
                     WHERE chain_id = $1
                       AND phase_name = 'project'
                       AND redo_in_progress
                       AND last_error = $3",
                )
                .bind(&chain.chain_id)
                .bind(format!(
                    "{}{}",
                    crate::redo_stamp::REQUIRED_REDO_PREFIX,
                    crate::redo_recompute::PROJECT_REFRESH_REASON
                ))
                .bind(crate::redo_recompute::PROJECT_REFRESH_STAGED_REASON)
                .execute(&mut *transaction)
                .await
                .map_err(|error| {
                    RunnerError::database(
                        format!(
                            "failed to requeue widened project refresh for chain {}",
                            chain.chain_id
                        ),
                        error,
                    )
                })?;
                if result.rows_affected() != 1 {
                    return Err(RunnerError::data_integrity(format!(
                        "widened staged project refresh lost its marker for chain {}",
                        chain.chain_id
                    )));
                }
            }
            transaction.commit().await.map_err(|error| {
                RunnerError::database(
                    format!(
                        "failed to commit queued project refresh for chain {}",
                        chain.chain_id
                    ),
                    error,
                )
            })?;
            tracing::info!(
                chain_id = chain.chain_id,
                from_block = persisted_range.from,
                to_block = persisted_range.to,
                created_refresh,
                resume_queued_refresh,
                "prepared recompute-flags scoped project refresh"
            );
            Ok((
                created_refresh
                    || (resume_queued_refresh && !staged_refresh)
                    || widened_staged_refresh,
                persisted_range,
            ))
        }
        .await;
        let release = phase_lock.release().await;
        match (result, release) {
            (Ok(disposition), Ok(())) => Ok(disposition),
            (Ok(_), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => {
                Err(error
                    .with_secondary("release project lock after queuing refresh", release_error))
            }
        }
    }
}
