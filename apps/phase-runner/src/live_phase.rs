use std::sync::Arc;

use bigname_ingest::{
    Engine, ErrorKind as IngestErrorKind, LiveBatchRequest, Marker, SourceDescriptor,
};
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers},
    phase::{
        Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseProgress, RunMode,
    },
};

pub struct LivePhase {
    engine: Arc<Engine>,
}

impl LivePhase {
    pub fn new(pool: PgPool) -> Self {
        Self::with_engine(Arc::new(Engine::new(pool)))
    }

    pub fn with_engine(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

impl Phase for LivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if !matches!(context.mode, RunMode::Normal) {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    "live follows the canonical head and cannot run a historical redo",
                ));
            }
            let handoff = context.live_handoff.as_ref().ok_or_else(|| {
                RunnerError::data_integrity(format!(
                    "live phase for chain {} is missing its ingest handoff",
                    context.chain_id
                ))
            })?;
            let outcome = self
                .engine
                .run_live_batch(LiveBatchRequest {
                    chain_id: context.chain_id,
                    sources: context
                        .sources
                        .iter()
                        .map(|source| SourceDescriptor {
                            key: source.source_key.clone(),
                            kind: source.source_kind.clone(),
                            start_block: source.start_block_number,
                            endpoint: source.endpoint().to_owned(),
                        })
                        .collect(),
                    live_handoff: ingest_marker(handoff),
                })
                .await
                .map_err(runner_error)?;
            let progress = PhaseProgress {
                current: Some(runner_marker(outcome.current)?),
                target: Some(runner_marker(outcome.target)?),
                heads: outcome.heads.map(runner_heads).transpose()?,
                estimated_write_bytes: outcome.estimated_write_bytes,
                ..PhaseProgress::default()
            };
            if outcome.caught_up {
                Ok(PhaseBatchOutcome::Complete(progress))
            } else {
                Ok(PhaseBatchOutcome::Continue(progress))
            }
        })
    }
}

fn ingest_marker(marker: &BlockMarker) -> Marker {
    Marker {
        number: marker.number,
        hash: marker.hash.clone(),
    }
}

fn runner_marker(marker: Marker) -> RunnerResult<BlockMarker> {
    BlockMarker::new(marker.number, marker.hash)
}

fn runner_heads(heads: bigname_ingest::HeadMarkers) -> RunnerResult<HeadMarkers> {
    Ok(HeadMarkers {
        latest: runner_marker(heads.latest)?,
        safe: heads.safe.map(runner_marker).transpose()?,
        finalized: heads.finalized.map(runner_marker).transpose()?,
    })
}

fn runner_error(error: bigname_ingest::IngestError) -> RunnerError {
    let kind = match error.kind() {
        IngestErrorKind::Transient => ErrorKind::Transient,
        IngestErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        IngestErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
