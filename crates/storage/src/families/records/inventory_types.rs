//! Shared inventory output and attribution policy for explicit-publication composition.
use crate::RecordInventoryCurrentRow;
use std::collections::BTreeSet;

/// Where a family row's `provenance.attributed_event_ids` comes from. It is the history
/// attribution, not a family fact.
#[derive(Clone, Debug)]
pub enum FamilyAttribution {
    /// Read it with `load_bounded_record_attribution` at the current publication.
    Load,
    /// Use the given ids for every resource read, as read by the caller.
    Given(BTreeSet<i64>),
    /// Leave the field out. Serving reads never consult it, and the history attribution costs
    /// seconds per resource on a resolver with many writes.
    Omit,
}

/// A family inventory row and the storage key of its record version boundary.
#[derive(Clone, Debug)]
pub struct FamilyRecordInventory {
    pub row: RecordInventoryCurrentRow,
    pub record_version_boundary_key: String,
    /// The mirror decision, for rows read through a mirror resolver.
    pub mirrored: bool,
}
