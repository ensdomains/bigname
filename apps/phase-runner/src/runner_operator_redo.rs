use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
    phase_lock::PhaseLock,
    state_persistence::load_redo_marker,
};

use super::{PhaseRunner, RedoPhase, SupervisorReport};

type PendingRedoRow = (
    String,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);
type PendingProjectRedoRow = (String, Option<String>, Option<i64>, Option<i64>);

impl PhaseRunner {
    pub async fn redo(
        &self,
        chain: &ChainConfig,
        selection: RedoPhase,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        match selection {
            RedoPhase::RecomputeFlags => {
                self.redo_recompute_flags(chain, range, cancellation).await
            }
            RedoPhase::All => self.redo_all_phases(chain, range, cancellation).await,
            RedoPhase::Phase(PhaseName::Live) => Err(RunnerError::new(
                ErrorKind::Configuration,
                "live does not support historical redo",
            )),
            RedoPhase::Phase(phase) => self.redo_phase(chain, phase, range, cancellation).await,
        }
    }

    async fn redo_recompute_flags(
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
        self.store.initialize_chain(&chain.chain_id).await?;
        self.require_readable_redo_end(&chain.chain_id, range)
            .await?;

        if let Some((redo_mode, from, to)) =
            load_redo_marker(self.store.pool(), &chain.chain_id, PhaseName::Interpret).await?
        {
            if redo_mode != "recompute_flags" {
                return Err(RunnerError::data_integrity(format!(
                    "interpret phase for chain {} already has an ordinary redo; complete it \
                     before starting recompute-flags",
                    chain.chain_id
                )));
            }
            let persisted = BlockRange::new(from, to)?;
            if persisted != range {
                return Err(RunnerError::data_integrity(format!(
                    "recompute-flags for chain {} is interrupted; rerun the exact persisted \
                     range {}..={}",
                    chain.chain_id, persisted.from, persisted.to
                )));
            }
            return self
                .run_recompute_interpret_with_project_lock(chain, mode, cancellation)
                .await;
        }

        let (run_project_now, project_range) = self.prepare_project_recompute(chain, range).await?;
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
        let mut project_lock = PhaseLock::acquire(
            self.database.connect_options(),
            &chain.chain_id,
            PhaseName::Project,
        )
        .await?;
        project_lock.check_alive().await?;
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

    async fn redo_phase(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        self.redo_phase_only(chain, phase, range, cancellation.clone())
            .await?;
        if phase == PhaseName::Interpret
            && self.store.status(&chain.chain_id, phase).await?
                == crate::state::PhaseStatus::Completed
            && let Some(range) = self
                .store
                .required_redo_range(&chain.chain_id, PhaseName::Project)
                .await?
        {
            self.redo_phase_only(chain, PhaseName::Project, range, cancellation)
                .await?;
        }
        Ok(())
    }

    async fn redo_phase_only(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let mode = RunMode::Redo(range);
        self.phases
            .get(phase)
            .preflight(&chain.chain_id, &chain.sources, &mode)?;
        self.store.initialize_chain(&chain.chain_id).await?;
        self.require_readable_redo_end(&chain.chain_id, range)
            .await?;
        self.run_phase_with_restart(chain, phase, mode, cancellation)
            .await
    }

    pub async fn redo_chains(
        &self,
        chains: &[ChainConfig],
        selection: RedoPhase,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<SupervisorReport> {
        let mut report = SupervisorReport::default();
        for chain in chains {
            if let Err(error) = self
                .redo(chain, selection, range, cancellation.clone())
                .await
            {
                report.stopped_chains.push((chain.chain_id.clone(), error));
            }
            if cancellation.is_cancelled() {
                break;
            }
        }
        Ok(report)
    }

    async fn redo_all_phases(
        &self,
        chain: &ChainConfig,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let phases = [
            PhaseName::Ingest,
            PhaseName::Interpret,
            PhaseName::Project,
            PhaseName::Verify,
        ];
        let mode = RunMode::Redo(range);
        for phase in phases {
            self.phases
                .get(phase)
                .preflight(&chain.chain_id, &chain.sources, &mode)?;
        }

        self.store.initialize_chain(&chain.chain_id).await?;
        self.require_readable_redo_end(&chain.chain_id, range)
            .await?;
        self.require_no_pending_redo_for_all(&chain.chain_id, None)
            .await?;

        self.run_all_redo_phase(chain, PhaseName::Ingest, range, cancellation.clone())
            .await?;
        self.run_all_redo_phase(chain, PhaseName::Interpret, range, cancellation.clone())
            .await?;

        let project_stamp = self
            .store
            .required_redo_range(&chain.chain_id, PhaseName::Project)
            .await?;
        self.require_no_pending_redo_for_all(&chain.chain_id, project_stamp)
            .await?;
        let project_range = project_stamp.unwrap_or(range);
        self.run_all_redo_phase(
            chain,
            PhaseName::Project,
            project_range,
            cancellation.clone(),
        )
        .await?;
        self.require_no_pending_redo_for_all(&chain.chain_id, None)
            .await?;
        self.run_all_redo_phase(chain, PhaseName::Verify, range, cancellation)
            .await?;
        Ok(())
    }

    async fn run_all_redo_phase(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        match self
            .run_phase_with_restart(chain, phase, RunMode::Redo(range), cancellation)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                match self
                    .require_no_pending_redo_for_all(&chain.chain_id, None)
                    .await
                {
                    Err(recovery) if recovery.kind() == ErrorKind::DataIntegrity => Err(
                        RunnerError::new(error.kind(), format!("{error}; {recovery}")),
                    ),
                    Err(recovery) => Err(error
                        .with_secondary("load the all-phase redo recovery instruction", recovery)),
                    Ok(()) => Err(error),
                }
            }
        }
    }

    async fn require_no_pending_redo_for_all(
        &self,
        chain_id: &str,
        allowed_project_stamp: Option<BlockRange>,
    ) -> RunnerResult<()> {
        let pending: Vec<PendingRedoRow> = sqlx::query_as(
            "SELECT phase_name, redo_mode, redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1
               AND redo_in_progress
             ORDER BY array_position(
                 ARRAY['ingest','interpret','project','verify','live'], phase_name
             )",
        )
        .bind(chain_id)
        .fetch_all(self.store.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to inspect pending redo for chain {chain_id}"),
                error,
            )
        })?;
        for (phase, mode, from, to, last_error) in pending {
            let persisted_range = from.zip(to).and_then(|(from, to)| {
                (from >= 0 && to >= from).then_some(BlockRange { from, to })
            });
            let allowed = allowed_project_stamp.is_some()
                && phase == PhaseName::Project.as_str()
                && mode.as_deref() == Some("redo")
                && persisted_range == allowed_project_stamp
                && last_error
                    .as_deref()
                    .is_some_and(crate::redo_stamp::owns_required_redo);
            if allowed {
                continue;
            }
            let Some(range) = persisted_range else {
                return Err(RunnerError::data_integrity(format!(
                    "cannot redo all phases for chain {chain_id}: the pending {phase} redo has an \
                     invalid persisted range"
                )));
            };
            let phase_argument = if mode.as_deref() == Some("recompute_flags")
                || last_error
                    .as_deref()
                    .is_some_and(crate::redo_recompute::is_staged_project_refresh)
            {
                "recompute-flags"
            } else {
                phase.as_str()
            };
            return Err(RunnerError::data_integrity(format!(
                "cannot redo all phases for chain {chain_id}: a pending {phase} redo must be \
                 completed first; rerun `phase-runner redo --chain {chain_id} --phase \
                 {phase_argument} --from-block {} --to-block {}`, then rerun `phase-runner redo \
                 --chain {chain_id} --phase all`",
                range.from, range.to
            )));
        }
        Ok(())
    }

    async fn require_readable_redo_end(
        &self,
        chain_id: &str,
        range: BlockRange,
    ) -> RunnerResult<()> {
        if crate::heads::load_marker(self.store.pool(), chain_id, range.to)
            .await?
            .is_none()
        {
            return Err(RunnerError::data_integrity(format!(
                "redo range end {} for chain {chain_id} is not readable (canonical, safe, or finalized)",
                range.to
            )));
        }
        Ok(())
    }
}
