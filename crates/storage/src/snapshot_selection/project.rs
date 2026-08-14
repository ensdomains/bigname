use std::collections::BTreeSet;

use sqlx::PgPool;

use super::chain_position::ChainPositions;
use super::error::{SnapshotSelectionError, SnapshotSelectionResult};

pub const CURRENT_PROJECT_PUBLICATION_JOIN: &str = r#"
JOIN bigname_phase.chain_phase_state project
  ON project.chain_id = head.chain_id
 AND project.phase_name = 'project'
 AND project.phase_status = 'completed'
 AND project.current_block_number = head.latest_block_number
 AND project.current_block_hash = head.latest_block_hash
"#;

pub(super) async fn validate_current_project_publications(
    pool: &PgPool,
    chain_positions: &ChainPositions,
) -> SnapshotSelectionResult<()> {
    let chain_ids = chain_positions
        .as_map()
        .values()
        .map(|position| position.chain_id.as_str())
        .collect::<BTreeSet<_>>();

    for chain_id in chain_ids {
        let query = format!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM bigname_phase.chain_heads head
                {CURRENT_PROJECT_PUBLICATION_JOIN}
                WHERE head.chain_id = $1
                  AND project.input_content_hash = $2
            )
            "#,
        );
        let project_is_current: bool = sqlx::query_scalar(&query)
            .bind(chain_id)
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .fetch_one(pool)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to check the current project phase for chain {chain_id}: {error}"
                ))
            })?;
        if !project_is_current {
            return Err(SnapshotSelectionError::stale(format!(
                "chain {chain_id} project phase is not published at its current schema-v2 head"
            )));
        }
    }

    Ok(())
}
