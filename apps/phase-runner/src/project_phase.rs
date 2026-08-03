use bigname_project::{
    BatchRequest, Engine, ErrorKind as ProjectErrorKind, Marker, RunMode as ProjectRunMode,
};
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{
        Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseProgress, RunMode,
    },
};

pub struct ProjectPhase {
    pool: PgPool,
    engine: Engine,
}

impl ProjectPhase {
    pub fn new(pool: PgPool) -> Self {
        Self {
            engine: Engine::new(pool.clone()),
            pool,
        }
    }

    async fn redo_target(&self, chain_id: &str) -> RunnerResult<BlockMarker> {
        let position: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
            "SELECT current_block_number, current_block_hash
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'project'",
        )
        .bind(chain_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to load recorded project head for redo on chain {chain_id}"),
                error,
            )
        })?;
        let (number, hash) = position
            .and_then(|(number, hash)| number.zip(hash))
            .ok_or_else(|| {
                RunnerError::new(
                    ErrorKind::DataIntegrity,
                    format!(
                        "cannot redo project on chain {chain_id}: the phase has no recorded head"
                    ),
                )
            })?;
        let recorded = BlockMarker::new(number, hash)?;
        let canonical = crate::heads::load_marker(&self.pool, chain_id, number)
            .await?
            .ok_or_else(|| {
                RunnerError::new(
                    ErrorKind::DataIntegrity,
                    format!(
                        "cannot redo project on chain {chain_id}: recorded head {number} is not canonical"
                    ),
                )
            })?;
        if canonical != recorded {
            return Err(RunnerError::new(
                ErrorKind::DataIntegrity,
                format!(
                    "cannot redo project on chain {chain_id}: recorded head {number} hash {} is not canonical",
                    recorded.hash
                ),
            ));
        }
        Ok(recorded)
    }
}

impl Phase for ProjectPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if matches!(context.mode, RunMode::RecomputeFlags(_)) {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    "project recompute-flags is owned by the later redo-tooling lane",
                ));
            }
            let redo_target = if matches!(context.mode, RunMode::Redo(_)) {
                Some(self.redo_target(&context.chain_id).await?)
            } else {
                None
            };
            let Some(available) = context.available_heads.as_ref() else {
                if let Some(range) = context.mode.range() {
                    return Err(RunnerError::new(
                        ErrorKind::DataIntegrity,
                        format!(
                            "project redo block {} for chain {} is not canonical",
                            range.to, context.chain_id
                        ),
                    ));
                }
                return Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()));
            };
            let target_block = redo_target
                .as_ref()
                .map_or(available.latest.number, |marker| marker.number);
            let redo_to = context.mode.range().map(|range| range.to);
            let (affected_from_block, affected_to_block) = match context.mode {
                RunMode::Normal => {
                    let from = context.resume.current.as_ref().map_or_else(
                        || {
                            context
                                .sources
                                .iter()
                                .map(|source| source.start_block_number)
                                .min()
                                .unwrap_or(0)
                        },
                        |marker| marker.number.saturating_add(1).min(target_block),
                    );
                    (from, target_block)
                }
                RunMode::Redo(range) => (range.from, range.to),
                RunMode::RecomputeFlags(_) => unreachable!("handled above"),
            };
            let outcome = self
                .engine
                .run_batch(BatchRequest {
                    chain_id: context.chain_id.clone(),
                    target_block,
                    affected_from_block,
                    affected_to_block,
                    resume_current: context.resume.current.as_ref().map(project_marker),
                    mode: if matches!(context.mode, RunMode::Normal) {
                        ProjectRunMode::Normal
                    } else {
                        ProjectRunMode::Redo
                    },
                })
                .await
                .map_err(runner_error)?;
            let progress_marker = match redo_to {
                Some(block_number) => crate::heads::load_marker(
                    &self.pool,
                    &context.chain_id,
                    block_number,
                )
                .await?
                .ok_or_else(|| {
                    RunnerError::new(
                        ErrorKind::DataIntegrity,
                        format!(
                            "project redo block {block_number} for chain {} is not canonical",
                            context.chain_id
                        ),
                    )
                })?,
                None => runner_marker(outcome.current)?,
            };
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(progress_marker.clone()),
                target: Some(progress_marker),
                estimated_write_bytes: outcome.estimated_write_bytes,
                ..PhaseProgress::default()
            }))
        })
    }
}

fn project_marker(marker: &BlockMarker) -> Marker {
    Marker {
        number: marker.number,
        hash: marker.hash.clone(),
    }
}

fn runner_marker(marker: Marker) -> RunnerResult<BlockMarker> {
    BlockMarker::new(marker.number, marker.hash)
}

fn runner_error(error: bigname_project::ProjectError) -> RunnerError {
    let kind = match error.kind() {
        ProjectErrorKind::Transient => ErrorKind::Transient,
        ProjectErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        ProjectErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
