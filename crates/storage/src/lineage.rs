mod canonicality;
mod decode;
mod orphaning;
mod reads;
mod types;
mod upserts;
mod validation;

/// Largest canonical-lineage gap the live reconciler may persist before advancing the stored
/// checkpoint. Larger gaps are handed to bounded catch-up or hash-pinned backfill.
pub const MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS: i64 = 1_024;

pub use orphaning::mark_chain_lineage_range_orphaned;
pub use reads::{
    chain_lineage_contains_ancestor, chain_lineage_contains_canonical_ancestor_position,
    load_chain_lineage_block, load_chain_lineage_canonical_child_path,
    load_highest_canonical_chain_lineage_block,
};
pub use types::{CanonicalityState, ChainLineageBlock};
pub use upserts::{
    upsert_chain_lineage_blocks, upsert_chain_lineage_blocks_recanonicalizing_orphaned,
    upsert_chain_lineage_blocks_without_snapshots,
    upsert_chain_lineage_blocks_without_snapshots_recanonicalizing_orphaned,
};

pub(crate) use canonicality::promote_chain_lineage_path;
pub(crate) use reads::{chain_lineage_contains_ancestor_internal, ensure_chain_lineage_block};

#[cfg(test)]
mod tests;
