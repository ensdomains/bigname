mod chain_position;
mod consistency;
mod error;
mod parsing;
mod selection;

pub use chain_position::{
    ChainPosition, ChainPositions, SnapshotPositionRequirement, SnapshotSelectionScope,
};
pub use consistency::SnapshotConsistency;
pub use error::{SnapshotSelectionError, SnapshotSelectionErrorKind, SnapshotSelectionResult};
pub use parsing::parse_rfc3339_utc_timestamp;
pub use selection::{
    SelectedSnapshot, SnapshotAt, SnapshotProjectionRead, SnapshotSelectorInput,
    ensure_projection_chain_positions_match, resolve_exact_name_snapshot_selection,
};

#[cfg(test)]
mod tests;
