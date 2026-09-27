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

/// How far the project phase's publication may trail the stored chain head and still be
/// served.
///
/// Live-follow stores a new head as soon as a block arrives; Project publishes for it well
/// under a second later. Only the publication for the block just before the head is served
/// as the snapshot position (reported as `as_of`); anything further behind is stale, so a
/// wedged or paused Project surfaces within one block instead of serving old data.
pub const PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS: i64 = 1;

/// The project phase's completed publication for this binary's interpreter generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProjectPublication {
    pub block_number: i64,
    pub block_hash: String,
}

/// The current publication: the family marker's block while the
/// [publication switch](crate::publication_source) is on, the Project row's otherwise.
pub(super) async fn load_current_project_publication(
    pool: &PgPool,
    chain_id: &str,
) -> SnapshotSelectionResult<Option<ProjectPublication>> {
    let sql = if crate::publication_source::serve_from_families() {
        CURRENT_FAMILY_MARKER_PUBLICATION
    } else {
        CURRENT_PROJECT_ROW_PUBLICATION
    };
    let row: Option<(i64, String)> = sqlx::query_as(sql)
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

const CURRENT_PROJECT_ROW_PUBLICATION: &str = r#"
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
        "#;

/// A marker is servable once it is `live`: `bootstrap_pending` means a rebuild is still
/// populating the families, which is unservable (the first-sync fence).
const CURRENT_FAMILY_MARKER_PUBLICATION: &str = r#"
        SELECT marker.current_block_number, marker.current_block_hash
        FROM bigname_phase.project_family_marker marker
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = marker.chain_id
         AND lineage.block_number = marker.current_block_number
         AND lineage.block_hash = marker.current_block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE marker.chain_id = $1
          AND marker.state = 'live'
          AND marker.input_content_hash = $2
        "#;

/// The served generation for the publication that serves `block_number` / `block_hash` on
/// `chain_id`, or `None` when no such publication is servable. The generation is the project
/// phase row's `xmin`, or the family marker's `sequence` while the
/// [publication switch](crate::publication_source) is on; callers compare it as an opaque
/// string.
///
/// The publication must be completed for this binary's interpreter generation, sit on the
/// readable lineage (a reorg that orphans it makes it unservable until Project republishes),
/// and trail the stored head by at most [`PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS`]. With
/// `require_position_is_publication` the publication must sit exactly at the given position
/// (the position a head-consistency selection returns); otherwise the position is left to the
/// per-row projection-target checks, as for historical `at` reads. With
/// `require_interpret_not_redo` a history-rewriting interpret redo also makes the publication
/// unservable. With the switch on the marker must also be `live`.
pub async fn load_served_project_generation(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    require_position_is_publication: bool,
    require_interpret_not_redo: bool,
) -> Result<Option<String>, sqlx::Error> {
    let sql = if crate::publication_source::serve_from_families() {
        SERVED_FAMILY_MARKER_GENERATION
    } else {
        SERVED_PROJECT_ROW_GENERATION
    };
    sqlx::query_scalar::<_, String>(sql)
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

const SERVED_PROJECT_ROW_GENERATION: &str = r#"
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
        "#;

/// The marker's `sequence` grows with every family block and undo, so it is the generation a
/// same-request recheck compares. The Interpret-not-in-redo clause stays on `chain_phase_state`.
const SERVED_FAMILY_MARKER_GENERATION: &str = r#"
        SELECT marker.sequence::TEXT
        FROM bigname_phase.chain_heads head
        JOIN bigname_phase.project_family_marker marker
          ON marker.chain_id = head.chain_id
         AND marker.state = 'live'
         AND marker.input_content_hash = $4
         AND head.latest_block_number - marker.current_block_number BETWEEN 0 AND $5
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = marker.chain_id
         AND lineage.block_number = marker.current_block_number
         AND lineage.block_hash = marker.current_block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE head.chain_id = $1
          AND (
              NOT $6
              OR (marker.current_block_number = $2 AND marker.current_block_hash = $3)
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
        "#;

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
