use std::sync::Arc;

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    heads::{HeadMarkers, load_available_heads, load_marker},
    phase::{Phase, PhaseContext, PhaseName, RunMode},
    phase_lock::PhaseLock,
    state::{PhaseStatus, StartDisposition},
    state_persistence::validate_progress,
};

use super::PhaseRunner;

impl PhaseRunner {
    pub(super) fn verify_before_live(chain: &ChainConfig) -> RunnerResult<bool> {
        Ok(chain.verify_before_live
            || crate::verify_phase::provider_trusted_verify_required(
                &chain.chain_id,
                &chain.sources,
            )?)
    }

    pub(super) async fn check_ingest_identity(
        &self,
        phase: PhaseName,
        chain: &ChainConfig,
        mode: &RunMode,
    ) -> RunnerResult<()> {
        if phase == PhaseName::Verify
            && crate::verify_phase::provider_trusted_verify_chain(&chain.chain_id)
        {
            self.store
                .validate_completed_ingest_sources(&chain.chain_id, &chain.sources)
                .await?;
        }
        if phase == PhaseName::Ingest {
            let status = self.store.status(&chain.chain_id, phase).await?;
            let validates_retained_completion = matches!(mode, RunMode::Normal)
                && (status == crate::state::PhaseStatus::Completed
                    || self
                        .store
                        .pending_completed_validation(&chain.chain_id, phase)
                        .await?);
            if validates_retained_completion {
                self.store
                    .validate_completed_ingest_sources(&chain.chain_id, &chain.sources)
                    .await?;
                if crate::verify_phase::provider_trusted_verify_chain(&chain.chain_id) {
                    crate::verify_phase::provider_trusted_verify_required(
                        &chain.chain_id,
                        &chain.sources,
                    )?;
                }
            } else {
                if crate::verify_phase::provider_trusted_verify_chain(&chain.chain_id) {
                    self.store
                        .validate_existing_ingest_sources(&chain.chain_id, &chain.sources)
                        .await?;
                    crate::verify_phase::provider_trusted_verify_required(
                        &chain.chain_id,
                        &chain.sources,
                    )?;
                }
                self.store
                    .ensure_ingest_sources(&chain.chain_id, &chain.sources)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn finish_completed_phase(
        &self,
        chain: &ChainConfig,
        phase: Arc<dyn Phase>,
        phase_lock: &mut PhaseLock,
        recovering: bool,
    ) -> RunnerResult<()> {
        let phase_name = phase.name();
        let result = async {
            self.validate_completed_config(chain, phase_name).await?;
            if phase.revalidates_completed(&chain.chain_id, &chain.sources)? {
                self.revalidate_completed_phase(chain, phase, phase_lock)
                    .await?;
            }
            if recovering {
                phase_lock.check_alive().await?;
                self.store
                    .complete_revalidated_phase(
                        phase_lock.connection(),
                        &chain.chain_id,
                        phase_name,
                    )
                    .await?;
            } else {
                phase_lock.check_alive().await?;
                self.store
                    .clear_unconfigured_settlement(
                        phase_lock.connection(),
                        &chain.chain_id,
                        phase_name,
                    )
                    .await?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.record_phase_validation_failure(
                    &chain.chain_id,
                    phase_name,
                    &RunMode::Normal,
                    phase_lock,
                    error,
                )
                .await
            }
        }
    }

    pub(super) async fn start_normal_phase(
        &self,
        chain: &ChainConfig,
        phase: Arc<dyn Phase>,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<bool> {
        match self
            .store
            .start_phase(&chain.chain_id, phase.name(), &RunMode::Normal)
            .await?
        {
            StartDisposition::Started => Ok(true),
            StartDisposition::AlreadyCompleted => {
                self.finish_completed_phase(chain, phase, phase_lock, false)
                    .await?;
                Ok(false)
            }
            StartDisposition::RecoveringCompleted => {
                self.finish_completed_phase(chain, phase, phase_lock, true)
                    .await?;
                Ok(false)
            }
        }
    }

    pub(super) async fn record_phase_validation_failure(
        &self,
        chain_id: &str,
        phase: PhaseName,
        mode: &RunMode,
        phase_lock: &mut PhaseLock,
        error: RunnerError,
    ) -> RunnerResult<()> {
        if error.is_retryable() || !matches!(mode, RunMode::Normal) {
            return Err(error);
        }
        let status = match self.store.status(chain_id, phase).await {
            Ok(status) => status,
            Err(status_error) => {
                return Err(error.with_secondary(
                    "load phase state before recording completed-phase failure",
                    status_error,
                ));
            }
        };
        if status != PhaseStatus::Completed {
            return Err(error);
        }
        if let Err(lock_error) = phase_lock.check_alive().await {
            return Err(error.with_secondary(
                "confirm phase lock before recording completed-phase failure",
                lock_error,
            ));
        }
        let failure_reason = format!(
            "{}{error}",
            crate::error::COMPLETED_VALIDATION_FAILURE_PREFIX
        );
        match self
            .store
            .fail_completed_validation(chain_id, phase, &failure_reason)
            .await
        {
            Ok(()) => Err(error),
            Err(record_error) => {
                Err(error.with_secondary("record completed-phase validation failure", record_error))
            }
        }
    }

    pub(super) async fn validate_completed_config(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
    ) -> RunnerResult<()> {
        if phase == PhaseName::Verify {
            let resume = self
                .store
                .phase_resume(&chain.chain_id, phase, &RunMode::Normal)
                .await?;
            crate::verify_phase::validate_reported_level(
                &chain.chain_id,
                &chain.sources,
                resume.verification_level,
            )?;
        }
        Ok(())
    }

    pub(super) async fn revalidate_completed_phase(
        &self,
        chain: &ChainConfig,
        phase: Arc<dyn Phase>,
        phase_lock: &mut PhaseLock,
    ) -> RunnerResult<()> {
        let phase_name = phase.name();
        let context = self
            .phase_context(chain, phase_name, RunMode::Normal)
            .await?;
        let progress = phase_lock
            .run_while_alive(
                self.timing.live_poll_interval,
                phase.revalidate_completed(context),
            )
            .await?;
        let Some(progress) = progress else {
            return Ok(());
        };
        phase_lock.check_alive().await?;
        validate_progress(phase_name, &progress, true)?;
        if phase_name == PhaseName::Verify {
            crate::verify_phase::validate_reported_level(
                &chain.chain_id,
                &chain.sources,
                progress.verification_level,
            )?;
        }
        if progress.heads.is_some() {
            return Err(RunnerError::data_integrity(format!(
                "completed phase {phase_name} cannot publish chain heads during revalidation"
            )));
        }
        phase_lock.check_alive().await?;
        self.store
            .record_progress(&chain.chain_id, phase_name, &RunMode::Normal, &progress)
            .await
    }

    pub(super) async fn phase_context(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        mode: RunMode,
    ) -> RunnerResult<PhaseContext> {
        let available_heads = match mode.range() {
            Some(_) if phase == PhaseName::Interpret && matches!(mode, RunMode::Redo(_)) => {
                interpret_redo_heads(self.store.pool(), &chain.chain_id).await?
            }
            Some(range) => load_marker(self.store.pool(), &chain.chain_id, range.to)
                .await?
                .map(|latest| HeadMarkers {
                    latest,
                    safe: None,
                    finalized: None,
                }),
            None => load_available_heads(self.store.pool(), &chain.chain_id).await?,
        };
        let live_handoff = if phase == PhaseName::Live && matches!(mode, RunMode::Normal) {
            let handoff = self.store.ingest_handoff(&chain.chain_id).await?;
            if handoff.is_none() {
                return Err(RunnerError::data_integrity(format!(
                    "cannot start live phase for chain {} without the ingest handoff block",
                    chain.chain_id
                )));
            }
            handoff
        } else {
            None
        };
        let resume = self
            .store
            .phase_resume(&chain.chain_id, phase, &mode)
            .await?;
        Ok(PhaseContext {
            chain_id: chain.chain_id.clone(),
            phase,
            mode,
            sources: Arc::clone(&chain.sources),
            available_heads,
            live_handoff,
            resume,
        })
    }
}

async fn interpret_redo_heads(
    pool: &sqlx::PgPool,
    chain_id: &str,
) -> RunnerResult<Option<HeadMarkers>> {
    let number: Option<i64> = sqlx::query_scalar(
        "SELECT current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load recorded interpret head for redo on chain {chain_id}"),
            error,
        )
    })?
    .flatten();
    let number = number.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "cannot redo interpret on chain {chain_id}: the phase has no recorded head"
        ))
    })?;
    let latest = load_marker(pool, chain_id, number).await?.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "cannot redo interpret on chain {chain_id}: recorded head {number} is not readable (canonical, safe, or finalized)"
        ))
    })?;
    Ok(Some(HeadMarkers {
        latest,
        safe: None,
        finalized: None,
    }))
}
