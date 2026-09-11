use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
    runner_support::{
        cancelled_redo_error, report_undispatched_redo, require_all_phase_range_within_verify,
    },
};

use super::{PhaseRunner, RedoPhase, SupervisorReport};

#[path = "runner_operator_redo_pending.rs"]
mod pending;
#[path = "runner_operator_redo_recompute.rs"]
mod recompute;

type PendingProjectRedoRow = (String, Option<String>, Option<i64>, Option<i64>);

impl PhaseRunner {
    pub async fn redo(
        &self,
        chain: &ChainConfig,
        selection: RedoPhase,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        if selection.requires_intake_sources() {
            chain.require_intake_sources()?;
        }
        let generation_token = self
            .preflight_watch_set_coverage_attestation(chain, selection)
            .await?;
        self.scope_manifest_attestation(
            &chain.chain_id,
            generation_token,
            self.redo_after_attestation_preflight(chain, selection, range, cancellation),
        )
        .await
    }

    async fn redo_after_attestation_preflight(
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

    /// Race the pre-batch setup against a stop. The batch loop that follows handles
    /// cancellation itself and records the incomplete redo, so only this setup is
    /// raced: wrapping the batch would drop it before it could report. Returns
    /// `false` when the stop won and the caller should return without running.
    /// A stop during setup still leaves the redo unfinished, so losing the race
    /// reports the same incomplete-redo error the batch loop would have raised.
    async fn prepared_for_redo(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        range: BlockRange,
        reject_pending_ingest: bool,
        cancellation: &CancellationToken,
    ) -> RunnerResult<()> {
        let chain_id = chain.chain_id.as_str();
        let prepared = crate::shutdown::until_cancelled(cancellation, async {
            self.store.initialize_chain(chain_id).await?;
            if reject_pending_ingest {
                self.reject_pending_required_ingest(chain_id).await?;
            }
            self.require_readable_redo_end(chain_id, range).await
        })
        .await?;
        match prepared {
            Some(()) => Ok(()),
            None => Err(cancelled_redo_error(&self.store, chain_id, phase).await?),
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
        if phase == PhaseName::Interpret {
            self.repair_discovery_after_operator_interpret(chain, cancellation.clone())
                .await?;
        }
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
        let reject_pending_ingest = matches!(phase, PhaseName::Interpret | PhaseName::Project);
        self.prepared_for_redo(chain, phase, range, reject_pending_ingest, &cancellation)
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
        let mut generation_tokens = Vec::with_capacity(chains.len());
        for chain in chains {
            if let Some(heartbeat) = &self.loop_heartbeat {
                heartbeat.record_progress(&chain.chain_id);
            }
            // Raced, not sampled: a stop must not go on to create redo state.
            let preflight = crate::shutdown::until_cancelled(
                &cancellation,
                self.preflight_watch_set_coverage_attestation(chain, selection),
            )
            .await;
            if let Some(heartbeat) = &self.loop_heartbeat {
                heartbeat.remove_progress(&chain.chain_id);
            }
            match preflight {
                Ok(Some(generation_token)) => generation_tokens.push(generation_token),
                Ok(None) => {
                    report_undispatched_redo(&mut report, chains);
                    return Ok(report);
                }
                Err(error) => report.stopped_chains.push((chain.chain_id.clone(), error)),
            }
        }
        if !report.stopped_chains.is_empty() {
            return Ok(report);
        }
        for (index, (chain, generation_token)) in chains.iter().zip(generation_tokens).enumerate() {
            if cancellation.is_cancelled() {
                report_undispatched_redo(&mut report, &chains[index..]);
                break;
            }
            if let Some(heartbeat) = &self.loop_heartbeat {
                heartbeat.record_progress(&chain.chain_id);
            }
            let result = self
                .scope_manifest_attestation(
                    &chain.chain_id,
                    generation_token,
                    self.redo_after_attestation_preflight(
                        chain,
                        selection,
                        range,
                        cancellation.clone(),
                    ),
                )
                .await;
            if let Some(heartbeat) = &self.loop_heartbeat {
                heartbeat.remove_progress(&chain.chain_id);
            }
            if let Err(error) = result {
                report.stopped_chains.push((chain.chain_id.clone(), error));
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
        let chain_id = chain.chain_id.as_str();
        let prepared = crate::shutdown::until_cancelled(&cancellation, async {
            self.store.initialize_chain(chain_id).await?;
            self.require_no_pending_redo_for_all(chain_id, None, None, None)
                .await?;
            require_all_phase_range_within_verify(&self.store, chain_id, range).await?;
            self.require_readable_redo_end(chain_id, range).await
        })
        .await?;
        if prepared.is_none() {
            return Err(cancelled_redo_error(&self.store, chain_id, PhaseName::Ingest).await?);
        }
        self.run_all_redo_phase(chain, PhaseName::Ingest, range, range, cancellation.clone())
            .await?;
        self.run_all_redo_phase(
            chain,
            PhaseName::Interpret,
            range,
            range,
            cancellation.clone(),
        )
        .await?;
        self.repair_discovery_after_operator_interpret(chain, cancellation.clone())
            .await?;
        let project_stamp = self
            .store
            .required_redo_range(&chain.chain_id, PhaseName::Project)
            .await?;
        let verify_stamp = self
            .store
            .required_redo_range(&chain.chain_id, PhaseName::Verify)
            .await?;
        self.require_no_pending_redo_for_all(&chain.chain_id, project_stamp, verify_stamp, None)
            .await?;
        let project_range = project_stamp.unwrap_or(range);
        self.run_all_redo_phase(
            chain,
            PhaseName::Project,
            project_range,
            range,
            cancellation.clone(),
        )
        .await?;
        let verify_stamp = self
            .store
            .required_redo_range(&chain.chain_id, PhaseName::Verify)
            .await?;
        self.require_no_pending_redo_for_all(&chain.chain_id, None, verify_stamp, None)
            .await?;
        let verify_range = verify_stamp.unwrap_or(range);
        self.run_all_redo_phase(chain, PhaseName::Verify, verify_range, range, cancellation)
            .await?;
        Ok(())
    }

    async fn repair_discovery_after_operator_interpret(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        if !matches!(
            self.discovery_required_ingest_pending(&chain.chain_id, &cancellation)
                .await?,
            Some(true)
        ) {
            return Ok(());
        }
        self.scope_manifest_attestation(
            &chain.chain_id,
            None,
            self.repair_discovery_coverage(chain, cancellation),
        )
        .await
    }

    async fn run_all_redo_phase(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        phase_range: BlockRange,
        recovery_all_range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        match self
            .run_phase_with_restart(chain, phase, RunMode::Redo(phase_range), cancellation)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                match self
                    .require_no_pending_redo_for_all(
                        &chain.chain_id,
                        None,
                        None,
                        Some(recovery_all_range),
                    )
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
