use bigname_interpret::{
    BatchRequest, Engine, ErrorKind as InterpretErrorKind, Marker, RunMode as InterpretRunMode,
};
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{
        Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseProgress, RunMode,
    },
};

pub struct InterpretPhase {
    engine: Engine,
}

impl InterpretPhase {
    pub fn new(pool: PgPool) -> Self {
        Self {
            engine: Engine::new(pool),
        }
    }

    pub fn with_state_cache_capacity(pool: PgPool, entries: usize) -> Self {
        Self {
            engine: Engine::with_state_cache_capacity(pool, entries),
        }
    }
}

impl Phase for InterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let Some(available) = context.available_heads.as_ref() else {
                return Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()));
            };
            let range = context.mode.range();
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
            let outcome = self
                .engine
                .run_batch(BatchRequest {
                    chain_id: context.chain_id,
                    from_block,
                    to_block,
                    resume_current: context.resume.current.as_ref().map(interpret_marker),
                    mode,
                })
                .await
                .map_err(runner_error)?;
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
