mod chain_position;
mod consistency;
mod error;
mod parsing;
mod project;
mod selection;

pub use chain_position::{
    ChainPosition, ChainPositions, SnapshotPositionRequirement, SnapshotSelectionScope,
};
pub use consistency::SnapshotConsistency;
pub use error::{SnapshotSelectionError, SnapshotSelectionErrorKind, SnapshotSelectionResult};
pub use parsing::parse_rfc3339_utc_timestamp;
pub use project::{PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS, load_served_project_generation};
pub(crate) use project::{family_inputs_not_in_redo, servable_family_marker};
pub use selection::{
    SelectedSnapshot, SnapshotAt, SnapshotProjectionRead, SnapshotSelectorInput,
    ensure_projection_chain_positions_match, resolve_exact_name_snapshot_selection_on,
};

pub async fn resolve_exact_name_snapshot_selection(
    pool: &sqlx::PgPool,
    scope: &SnapshotSelectionScope,
    input: &SnapshotSelectorInput,
) -> SnapshotSelectionResult<SelectedSnapshot> {
    let mut connection = pool.acquire().await.map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to acquire snapshot selection connection: {error}"
        ))
    })?;
    resolve_exact_name_snapshot_selection_on(&mut connection, scope, input).await
}

pub async fn snapshot_chain_has_head(
    pool: &sqlx::PgPool,
    chain_id: &str,
) -> SnapshotSelectionResult<bool> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM chain_heads WHERE chain_id = $1)")
        .bind(chain_id)
        .fetch_one(pool)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to check current schema-v2 head for chain {chain_id}: {error}"
            ))
        })
}

#[cfg(test)]
mod tests;
