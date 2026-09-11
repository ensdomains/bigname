use std::collections::BTreeSet;

use sqlx::PgPool;

use super::chain_position::ChainPositions;
use super::error::{SnapshotSelectionError, SnapshotSelectionResult};

pub const CURRENT_PROJECT_PUBLICATION_JOIN: &str = r#"
JOIN bigname_phase.chain_phase_state project
  ON project.chain_id = head.chain_id
 AND project.phase_name = 'project'
 AND project.phase_status IN ('completed', 'running')
 AND project.current_block_number = head.latest_block_number
 AND project.current_block_hash = head.latest_block_hash
"#;

/// How far the project phase's completed publication may trail the stored chain head and
/// still be served.
///
/// Live-follow stores a new head as soon as a block arrives; Project publishes for it a few
/// seconds later. Within this many blocks the publication is served as the snapshot position
/// (reported as `as_of`). Beyond it the chain is treated as not published and reads are stale,
/// so a wedged or paused Project still surfaces instead of serving arbitrarily old data.
pub const PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS: i64 = 32;

/// The project phase's completed publication for this binary's interpreter generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProjectPublication {
    pub block_number: i64,
    pub block_hash: String,
}

pub(super) async fn load_current_project_publication(
    pool: &PgPool,
    chain_id: &str,
) -> SnapshotSelectionResult<Option<ProjectPublication>> {
    let row: Option<(i64, String)> = sqlx::query_as(
        r#"
        SELECT project.current_block_number, project.current_block_hash
        FROM bigname_phase.chain_phase_state project
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = project.chain_id
         AND lineage.block_number = project.current_block_number
         AND lineage.block_hash = project.current_block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE project.chain_id = $1
          AND project.phase_name = 'project'
          AND project.phase_status IN ('completed', 'running')
          AND project.input_content_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check the current project phase for chain {chain_id}: {error}"
        ))
    })?;
    Ok(row.map(|(block_number, block_hash)| ProjectPublication {
        block_number,
        block_hash,
    }))
}

/// The project phase row generation (`xmin`) for the publication that serves `block_number` /
/// `block_hash` on `chain_id`, or `None` when no such publication is servable.
///
/// The publication must be completed for this binary's interpreter generation, sit on the
/// readable lineage (a reorg that orphans it makes it unservable until Project republishes),
/// and trail the stored head by at most [`PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS`]. With
/// `require_position_is_publication` the publication must sit exactly at the given position
/// (the position a head-consistency selection returns); otherwise the position is left to the
/// per-row projection-target checks, as for historical `at` reads. With
/// `require_interpret_not_redo` a history-rewriting interpret redo also makes the publication
/// unservable.
pub async fn load_served_project_generation(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    require_position_is_publication: bool,
    require_interpret_not_redo: bool,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT project.xmin::TEXT
        FROM bigname_phase.chain_heads head
        JOIN bigname_phase.chain_phase_state project
          ON project.chain_id = head.chain_id
         AND project.phase_name = 'project'
         AND project.phase_status IN ('completed', 'running')
         AND project.input_content_hash = $4
         AND head.latest_block_number - project.current_block_number BETWEEN 0 AND $5
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = project.chain_id
         AND lineage.block_number = project.current_block_number
         AND lineage.block_hash = project.current_block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE head.chain_id = $1
          AND (
              NOT $6
              OR (project.current_block_number = $2 AND project.current_block_hash = $3)
          )
          AND (
              NOT $7
              OR EXISTS (
                  SELECT 1
                  FROM bigname_phase.chain_phase_state interpret
                  WHERE interpret.chain_id = head.chain_id
                    AND interpret.phase_name = 'interpret'
                    AND interpret.redo_in_progress = false
              )
          )
        "#,
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .bind(PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS)
    .bind(require_position_is_publication)
    .bind(require_interpret_not_redo)
    .fetch_optional(pool)
    .await
}

/// Every selected chain must have a completed, readable project publication at most
/// [`PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS`] behind the stored head. Historical `at`
/// positions are not bounded by the publication here; the per-row projection-target checks
/// keep rows ahead of the selected position out of the read.
pub(super) async fn validate_current_project_publications(
    pool: &PgPool,
    chain_positions: &ChainPositions,
) -> SnapshotSelectionResult<()> {
    let chains = chain_positions
        .as_map()
        .values()
        .map(|position| position.chain_id.as_str())
        .collect::<BTreeSet<_>>();

    for chain_id in chains {
        let latest_block_number: Option<i64> = sqlx::query_scalar(
            "SELECT latest_block_number FROM bigname_phase.chain_heads WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load the schema-v2 head for chain {chain_id}: {error}"
            ))
        })?;
        let Some(latest_block_number) = latest_block_number else {
            return Err(SnapshotSelectionError::stale(format!(
                "chain {chain_id} project phase is not published at its current schema-v2 head"
            )));
        };
        let publication = load_current_project_publication(pool, chain_id)
            .await?
            .ok_or_else(|| {
                SnapshotSelectionError::stale(format!(
                    "chain {chain_id} project phase is not published at its current schema-v2 head"
                ))
            })?;
        if latest_block_number - publication.block_number > PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS
        {
            return Err(SnapshotSelectionError::stale(format!(
                "chain {chain_id} project phase is not published at its current schema-v2 head \
                 (publication at {} lags head {latest_block_number})",
                publication.block_number
            )));
        }
    }

    Ok(())
}
