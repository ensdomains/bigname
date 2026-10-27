use std::sync::Arc;

use bigname_ingest::{
    Engine, ErrorKind as IngestErrorKind, LiveBatchRequest, Marker, SourceDescriptor,
};
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers},
    metrics::RunnerMetricsFeed,
    phase::{
        Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseProgress, RunMode,
    },
};

pub struct LivePhase {
    engine: Arc<Engine>,
    metrics_feed: Option<RunnerMetricsFeed>,
    publication_lag_blocks: u32,
}

impl LivePhase {
    pub fn new(pool: PgPool) -> Self {
        Self::with_engine(Arc::new(Engine::new(pool)))
    }

    pub fn with_engine(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            metrics_feed: None,
            publication_lag_blocks: 0,
        }
    }

    /// Publishes the readable head `blocks` below the newest loaded block (heads/lag.rs).
    #[must_use]
    pub fn with_publication_lag_blocks(mut self, blocks: u32) -> Self {
        self.publication_lag_blocks = blocks;
        self
    }

    /// Reports an RPC chain check failure to `feed` before the phase fails on it.
    #[must_use]
    pub fn with_metrics(mut self, feed: RunnerMetricsFeed) -> Self {
        self.metrics_feed = Some(feed);
        self
    }

    fn runner_error(&self, error: bigname_ingest::IngestError) -> RunnerError {
        if let Some(feed) = &self.metrics_feed {
            feed.observe_ingest_error(&error);
        }
        runner_error(error)
    }
}

impl Phase for LivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn publication_lag_blocks(&self) -> u32 {
        self.publication_lag_blocks
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if !matches!(context.mode, RunMode::Normal) {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    "live follows the canonical head and cannot run a historical redo",
                ));
            }
            if let Some(source) = context
                .sources
                .iter()
                .find(|source| !source.role.serves_intake())
            {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    format!(
                        "live context for chain {} contains non-intake source {}",
                        context.chain_id, source.source_key
                    ),
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
                            recorded_genesis: source.recorded_genesis(),
                        })
                        .collect(),
                    live_handoff: ingest_marker(handoff),
                })
                .await
                .map_err(|error| self.runner_error(error))?;
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
