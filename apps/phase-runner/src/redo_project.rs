//! Project keeps requested invalidation separate from its physical repair envelope. Existing
//! redo bounds fence and contain progress; requested bounds retain their meaning across restart.
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::load_marker,
    phase::BlockRange,
    transitions::PhaseStateRow,
};

pub(crate) async fn execution_range(
    pool: &PgPool,
    chain_id: &str,
    previous: &PhaseStateRow,
    requested: BlockRange,
    resumes: bool,
) -> RunnerResult<BlockRange> {
    let standing: Option<i64> = sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| RunnerError::database("failed to plan Project redo standing marker", error))?
    .flatten();
    let latest: Option<i64> = sqlx::query_scalar(
        "SELECT max(block_number) FROM chain_lineage WHERE chain_id = $1
         AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .map_err(|error| RunnerError::database("failed to plan Project redo readable extent", error))?;
    let target = [
        Some(requested.to),
        standing,
        // A covering restart can change the request while the family marker has already
        // moved back. Retain all pending replay work independently of prefix reuse below.
        previous
            .redo_in_progress
            .then_some(previous.redo_to_block_number)
            .flatten(),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(requested.to)
    .min(latest.ok_or_else(|| RunnerError::data_integrity("Project redo has no readable target"))?);
    let target = load_marker(pool, chain_id, target)
        .await?
        .ok_or_else(|| RunnerError::data_integrity("Project redo target is not readable"))?;
    let options = bigname_project::families::FamilyOptions::new(
        bigname_content_hash::INTERPRETER_CONTENT_HASH,
    )
    .with_resumed_redo(resumes);
    let (from, to) = bigname_project::families::redo_extent(
        pool,
        chain_id,
        &bigname_project::Marker {
            number: target.number,
            hash: target.hash,
        },
        requested.from,
        previous.redo_attempt_generation + 1,
        &options,
    )
    .await
    .map_err(|error| {
        let kind = match error.kind() {
            bigname_project::ErrorKind::Configuration => ErrorKind::Configuration,
            bigname_project::ErrorKind::Transient => ErrorKind::Transient,
            bigname_project::ErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        };
        RunnerError::new(kind, format!("failed to plan Project redo: {error}"))
    })?;
    BlockRange::new(from, to.max(requested.to))
}
