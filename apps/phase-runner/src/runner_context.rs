use std::sync::Arc;

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    heads::{HeadMarkers, load_available_heads, load_marker},
    phase::{Phase, PhaseContext, PhaseName, RunMode},
    phase_lock::PhaseLock,
    state_persistence::validate_progress,
};

use super::PhaseRunner;

impl PhaseRunner {
    pub(super) async fn check_ingest_identity(
        &self,
        phase: PhaseName,
        chain: &ChainConfig,
        mode: &RunMode,
    ) -> RunnerResult<()> {
        if phase == PhaseName::Ingest {
            let status = self.store.status(&chain.chain_id, phase).await?;
            if matches!(mode, RunMode::Normal) && status == crate::state::PhaseStatus::Completed {
                self.store
                    .validate_existing_ingest_source_kinds(&chain.sources)
                    .await?;
            } else {
                self.store.ensure_ingest_sources(&chain.sources).await?;
            }
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
