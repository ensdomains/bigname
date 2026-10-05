use std::collections::BTreeSet;

use sqlx::PgConnection;

use super::chain_position::ChainPositions;
use super::error::{SnapshotSelectionError, SnapshotSelectionResult};

/// The default for how many blocks the project phase's publication may trail the stored chain
/// head and still be served. The API takes its value from
/// `BIGNAME_API_PUBLICATION_LAG_TOLERANCE_BLOCKS`.
///
/// Live-follow stores a new head as soon as a block arrives; Project publishes for it well
/// under a second later. By default only the publication for the block just before the head
/// is served as the snapshot position (reported as `as_of`); anything further behind is
/// stale, so a wedged or paused Project surfaces within one block instead of serving old data.
pub const PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS: i64 = 1;

/// Why a chain's family publication is unavailable, for an operator-visible `409 stale`.
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

/// The current live family marker for this binary's interpreter generation.
pub(super) async fn load_current_project_publication(
    connection: &mut PgConnection,
    chain_id: &str,
) -> SnapshotSelectionResult<Option<ProjectPublication>> {
    let sql = { CURRENT_FAMILY_MARKER_PUBLICATION };
    let row: Option<(i64, String)> = sqlx::query_as(sql)
        .bind(chain_id)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_optional(&mut *connection)
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
/// selection still uses `servable_family_marker` alone. Project execution can include an
/// unchanged predecessor checkpoint; only its requested invalidation determines overlap.
macro_rules! family_inputs_not_in_redo {
    () => {
        r#"
          AND NOT EXISTS (
              SELECT 1 FROM bigname_phase.chain_phase_state input_phase
              WHERE input_phase.chain_id = marker.chain_id
                AND input_phase.phase_name IN ('interpret', 'project')
                AND input_phase.redo_in_progress
                AND COALESCE(input_phase.redo_requested_from_block_number, input_phase.redo_from_block_number) <= marker.current_block_number
          )
        "#
    };
}
pub(crate) use family_inputs_not_in_redo;

const CURRENT_FAMILY_MARKER_PUBLICATION: &str = concat!(
    "SELECT marker.current_block_number, marker.current_block_hash",
    servable_family_marker!()
);

/// The family marker sequence serving `block_number` / `block_hash`, or `None` when unavailable.
/// Callers compare the sequence as an opaque string. The marker must be live, on readable
/// lineage, from this interpreter build and at most `lag_tolerance_blocks` behind the head.
/// `require_position_is_publication` additionally requires the exact selected position.
/// `require_interpret_not_redo` requires a completed/running Interpret row without redo;
/// any Interpret or Project redo overlapping the publication is refused regardless of it.
pub async fn load_served_project_generation<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    require_position_is_publication: bool,
    require_interpret_not_redo: bool,
    lag_tolerance_blocks: i64,
) -> Result<Option<String>, sqlx::Error> {
    let sql = { SERVED_FAMILY_MARKER_GENERATION };
    sqlx::query_scalar::<_, String>(sql)
        .bind(chain_id)
        .bind(block_number)
        .bind(block_hash)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .bind(lag_tolerance_blocks)
        .bind(require_position_is_publication)
        .bind(require_interpret_not_redo)
        .fetch_optional(executor)
        .await
}

/// Each family publication and undo advances the sequence in the same transaction as its rows.
/// Rechecks also refuse overlapping Interpret/Project redo while those inputs can change.
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

/// Every selected chain must have a live, readable family-marker publication at or below the
/// stored head and at most `lag_tolerance_blocks` behind it. Historical `at`
/// positions are not bounded by the publication here; the per-row projection-target checks
/// keep rows ahead of the selected position out of the read.
pub(super) async fn validate_current_project_publications(
    connection: &mut PgConnection,
    chain_positions: &ChainPositions,
    lag_tolerance_blocks: i64,
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
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load the schema-v2 head for chain {chain_id}: {error}"
            ))
        })?;
        let Some(latest_block_number) = latest_block_number else {
            return Err(SnapshotSelectionError::stale(unpublished_message(chain_id)));
        };
        let publication = load_current_project_publication(&mut *connection, chain_id)
            .await?
            .ok_or_else(|| SnapshotSelectionError::stale(unpublished_message(chain_id)))?;
        if !(0..=lag_tolerance_blocks).contains(&(latest_block_number - publication.block_number)) {
            return Err(SnapshotSelectionError::stale(format!(
                "{} (publication at {} is outside the lag tolerance of head {latest_block_number})",
                unpublished_message(chain_id),
                publication.block_number
            )));
        }
    }

    Ok(())
}
