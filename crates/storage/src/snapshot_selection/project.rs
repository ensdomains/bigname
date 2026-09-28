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

/// Why a chain has no servable publication, for a `409 stale`: the Project row's wording with the
/// [publication switch](crate::publication_source) off (unchanged from before the switch), the
/// family marker's with it on, so an operator reading it during a family rebuild looks at the
/// marker.
pub(super) fn unpublished_message(chain_id: &str) -> String {
    {
        format!(
            "chain {chain_id} owned key families are not published at its current schema-v2 head"
        )
    }
}

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
    let sql = { CURRENT_FAMILY_MARKER_PUBLICATION };
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

/// The servable family marker of a chain, as a `FROM ... WHERE` clause binding the chain as `$1`
/// and this build's interpreter content hash as `$2`, with the marker as `marker`. A marker is
/// servable once it is `live` (`bootstrap_pending` means a rebuild is still populating the
/// families: the first-sync fence), written by this build, and on the readable lineage (a reorg
/// that orphans its block makes it unservable until the families republish). The publication
/// fence and the composed name reader (families/name/batch.rs) share it, so a composed read never
/// serves a marker the fence would refuse.
macro_rules! servable_family_marker {
    () => {
        r#"
        FROM bigname_phase.project_family_marker marker
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = marker.chain_id
         AND lineage.block_number = marker.current_block_number
         AND lineage.block_hash = marker.current_block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE marker.chain_id = $1
          AND marker.state = 'live'
          AND marker.input_content_hash = $2
        "#
    };
}
pub(crate) use servable_family_marker;

/// Interpret identity is read alongside the families. A redo that overlaps their publication
/// can change that identity before Project republishes it. Keep the old marker unavailable
/// through the Interpret-to-Project handoff, even after Interpret has cleared its own redo.
/// This predicate is for composed reads and their generation fences; raw diagnostic snapshot
/// selection still uses `servable_family_marker` alone.
macro_rules! family_inputs_not_in_redo {
    () => {
        r#"
          AND NOT EXISTS (
              SELECT 1 FROM bigname_phase.chain_phase_state input_phase
              WHERE input_phase.chain_id = marker.chain_id
                AND input_phase.phase_name IN ('interpret', 'project')
                AND input_phase.redo_in_progress
                AND input_phase.redo_from_block_number <= marker.current_block_number
          )
        "#
    };
}
pub(crate) use family_inputs_not_in_redo;

const CURRENT_FAMILY_MARKER_PUBLICATION: &str = concat!(
    "SELECT marker.current_block_number, marker.current_block_hash",
    servable_family_marker!()
);

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
/// unservable. With the switch on the marker must also be `live`, and any Interpret or Project
/// redo that overlaps the publication makes it unservable regardless of that option: the
/// composed readers also consume mutable Interpret identity, including surface visibility.
pub async fn load_served_project_generation(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    require_position_is_publication: bool,
    require_interpret_not_redo: bool,
) -> Result<Option<String>, sqlx::Error> {
    let sql = { SERVED_FAMILY_MARKER_GENERATION };
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

/// The marker's `sequence` grows with every family block and undo, so it is the generation a
/// same-request recheck compares. The Interpret-not-in-redo clause stays on `chain_phase_state`.
///
/// Interim gap (TYR-36 step 7b, disclosed in docs/api-v1.md under the publication switch): until
/// slices 2 to 4 move a route's rows onto the owned key families, that route still reads the
/// served tables, which the served Project batch commits before the family block moves the
/// marker. A served batch landing between a read's capture and its recheck therefore leaves
/// `sequence` unchanged and passes this check; only the per-row snapshot checks (no row target
/// newer than the selected position) still refuse. The guard closes for a route once its rows
/// come from the families, whose block commit advances `sequence` in the same transaction.
const SERVED_FAMILY_MARKER_GENERATION: &str = concat!(
    r#"
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
        "#,
    family_inputs_not_in_redo!()
);

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
            return Err(SnapshotSelectionError::stale(unpublished_message(chain_id)));
        };
        let publication = load_current_project_publication(pool, chain_id)
            .await?
            .ok_or_else(|| SnapshotSelectionError::stale(unpublished_message(chain_id)))?;
        if latest_block_number - publication.block_number > PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS
        {
            return Err(SnapshotSelectionError::stale(format!(
                "{} (publication at {} lags head {latest_block_number})",
                unpublished_message(chain_id),
                publication.block_number
            )));
        }
    }

    Ok(())
}
