use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
};

use super::{PhaseRunner, RedoPhase, SupervisorReport};

type PendingRedoRow = (
    String,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

impl PhaseRunner {
    pub async fn redo(
        &self,
        chain: &ChainConfig,
        selection: RedoPhase,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        match selection {
            RedoPhase::RecomputeFlags => Err(RunnerError::new(
                ErrorKind::Configuration,
                bigname_interpret::RECOMPUTE_FLAGS_UNAVAILABLE_REASON,
            )),
            RedoPhase::All => self.redo_all_phases(chain, range, cancellation).await,
            RedoPhase::Phase(PhaseName::Live) => Err(RunnerError::new(
                ErrorKind::Configuration,
                "live does not support historical redo",
            )),
            RedoPhase::Phase(phase) => self.redo_phase(chain, phase, range, cancellation).await,
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
            let phase_argument = if mode.as_deref() == Some("recompute_flags") {
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
