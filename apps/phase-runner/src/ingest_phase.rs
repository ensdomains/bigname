use std::sync::Arc;

use bigname_ingest::{
    BatchRequest, Engine, ErrorKind as IngestErrorKind, Marker, SourceCursor, SourceDescriptor,
};
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers},
    phase::{
        Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseProgress, RunMode,
        SourceProgress,
    },
};

pub struct IngestPhase {
    engine: Arc<Engine>,
}

impl IngestPhase {
    pub fn new(pool: PgPool) -> Self {
        Self::with_engine(Arc::new(Engine::new(pool)))
    }

    pub fn with_engine(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

impl Phase for IngestPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let request = BatchRequest {
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
                cursors: context
                    .resume
                    .ingest_cursors
                    .iter()
                    .map(|cursor| SourceCursor {
                        key: cursor.source_key.clone(),
                        next_block: cursor.next_block_number,
                        target_block: cursor.target_block_number,
                        last_processed: cursor.last_processed.as_ref().map(ingest_marker),
                    })
                    .collect(),
                redo_range: match context.mode {
                    RunMode::Normal => None,
                    RunMode::Redo(range) | RunMode::RecomputeFlags(range) => {
                        Some((range.from, range.to))
                    }
                },
                resume_current: context.resume.current.as_ref().map(ingest_marker),
            };
            let outcome = self.engine.run_batch(request).await.map_err(runner_error)?;
            let progress = PhaseProgress {
                current: Some(runner_marker(outcome.current)?),
                target: Some(runner_marker(outcome.target)?),
                live_handoff: outcome.live_handoff.map(runner_marker).transpose()?,
                heads: outcome.heads.map(runner_heads).transpose()?,
                source_progress: outcome
                    .sources
                    .into_iter()
                    .map(|source| {
                        Ok(SourceProgress {
                            source_key: source.key,
                            current: source.current.map(runner_marker).transpose()?,
                            target: Some(runner_marker(source.target)?),
                        })
                    })
                    .collect::<RunnerResult<Vec<_>>>()?,
                verification_level: None,
                estimated_write_bytes: outcome.estimated_write_bytes,
            };
            if outcome.complete {
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
