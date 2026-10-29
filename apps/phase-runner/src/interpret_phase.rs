use bigname_interpret::{
    BatchRequest, Engine, ErrorKind as InterpretErrorKind, Marker, RunMode as InterpretRunMode,
};
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    metrics::RunnerMetricsFeed,
    phase::{
        AfterProgress, AfterProgressFuture, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture,
        PhaseName, PhaseProgress, RunMode,
    },
};

pub struct InterpretPhase {
    engine: Engine,
    metrics_feed: Option<RunnerMetricsFeed>,
}

impl InterpretPhase {
    /// The Interpret settings an operator controls, taken from the runner's capacity settings.
    pub fn from_capacity(pool: PgPool, capacity: &crate::config::CapacityConfig) -> Self {
        let engine =
            Engine::with_state_cache_capacity(pool, capacity.interpreter_state_cache_entries)
                .with_blocks_per_batch(capacity.interpret_blocks_per_batch)
                .with_full_state_loader_forced(capacity.interpret_force_full_state_loader)
                .with_lookahead_statement_timeout_secs(
                    capacity.interpret_lookahead_statement_timeout_secs,
                );
        Self {
            engine,
            metrics_feed: None,
        }
    }

    /// Counts the inconsistent event logs each committed batch skipped in the runner's metrics.
    pub fn with_metrics(mut self, feed: RunnerMetricsFeed) -> Self {
        self.metrics_feed = Some(feed);
        self
    }

    /// Size redo batches by their whole blocks' raw input. Other runs keep fixed batches.
    pub fn with_adaptive_batching(
        mut self,
        config: bigname_interpret::AdaptiveBatchConfig,
    ) -> Self {
        self.engine = self.engine.with_adaptive_batching(config);
        self
    }

    pub fn new(pool: PgPool) -> Self {
        Self {
            engine: Engine::new(pool),
            metrics_feed: None,
        }
    }

    pub fn with_state_cache_capacity(pool: PgPool, entries: usize) -> Self {
        Self {
            engine: Engine::with_state_cache_capacity(pool, entries),
            metrics_feed: None,
        }
    }
}

impl Phase for InterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let Some(request) = batch_request(&context)? else {
                return Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()));
            };
            let outcome = self.engine.run_batch(request).await.map_err(runner_error)?;
            if let Some(feed) = &self.metrics_feed {
                feed.interpret_inconsistent_log_skips(
                    &context.chain_id,
                    &outcome.inconsistent_log_skips,
                );
            }
            let progress = PhaseProgress {
                current: Some(runner_marker(outcome.current)?),
                target: Some(runner_marker(outcome.target)?),
                estimated_write_bytes: outcome.estimated_write_bytes,
                ..PhaseProgress::default()
            };
            if outcome.complete {
                Ok(PhaseBatchOutcome::Complete(progress))
            } else {
                Ok(PhaseBatchOutcome::Continue(progress))
            }
        })
    }

    /// Interpret refreshes the planner statistics of `normalized_events` here, between
    /// batches. A failed refresh is logged by the engine and never fails the phase run.
    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        let chain_id = chain_id.to_owned();
        Box::pin(async move {
            self.engine.refresh_statistics(&chain_id).await;
            Ok(AfterProgress::Done)
        })
    }

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.engine.statistics_refresh_due(chain_id)
    }
}

/// The engine request for one Interpret batch of `context`, or `None` while no head is
/// readable. A redo runs from its attempt's execution range, the start the runner settled at
/// begin: a bounded start, a resumed audited one, or the requested one. The engine's opening
/// reset, search reset and final refresh all cover `from_block..=to_block`, so nothing below
/// that start is rewritten. A redo batch without its attempt is an internal error: the runner
/// begins every redo before its first batch.
#[doc(hidden)]
pub fn batch_request(context: &PhaseContext) -> RunnerResult<Option<BatchRequest>> {
    let range = match (&context.mode, context.redo_attempt) {
        (RunMode::Redo(_), Some(attempt)) => Some(attempt.execution_range),
        (RunMode::Redo(_), None) => {
            return Err(RunnerError::data_integrity(format!(
                "interpret redo batch for chain {} has no redo attempt",
                context.chain_id
            )));
        }
        (mode, _) => mode.range(),
    };
    let Some(available) = context.available_heads.as_ref() else {
        return Ok(None);
    };
    let from_block = range.map_or_else(
        || {
            context
                .sources
                .iter()
                .map(|source| source.start_block_number)
                .min()
                .unwrap_or(0)
        },
        |range| range.from,
    );
    let to_block = if matches!(context.mode, RunMode::Redo(_)) {
        available.latest.number
    } else {
        range.map_or(available.latest.number, |range| range.to)
    };
    let mode = match context.mode {
        RunMode::Normal => InterpretRunMode::Normal,
        RunMode::Redo(_) => InterpretRunMode::Redo,
        RunMode::RecomputeFlags(_) => InterpretRunMode::RecomputeFlags,
    };
    Ok(Some(BatchRequest {
        chain_id: context.chain_id.clone(),
        from_block,
        to_block,
        resume_current: context.resume.current.as_ref().map(interpret_marker),
        mode,
    }))
}

fn interpret_marker(marker: &BlockMarker) -> Marker {
    Marker {
        number: marker.number,
        hash: marker.hash.clone(),
    }
}

fn runner_marker(marker: Marker) -> RunnerResult<BlockMarker> {
    BlockMarker::new(marker.number, marker.hash)
}

fn runner_error(error: bigname_interpret::InterpretError) -> RunnerError {
    let kind = match error.kind() {
        InterpretErrorKind::Transient => ErrorKind::Transient,
        InterpretErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        InterpretErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
