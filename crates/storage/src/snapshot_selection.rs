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
pub use project::CURRENT_PROJECT_PUBLICATION_JOIN;
pub use selection::{
    SelectedSnapshot, SnapshotAt, SnapshotProjectionRead, SnapshotSelectorInput,
    ensure_projection_chain_positions_match, resolve_exact_name_snapshot_selection,
    snapshot_chain_has_head,
};

#[cfg(test)]
mod tests;
