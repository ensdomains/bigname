use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use crate::UnixSeconds;

/// Persisted current child-collection row for declared direct children only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentRow {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub surface_class: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub labelhash: Option<String>,
    pub owner: Option<String>,
    pub registrant: Option<String>,
    /// The `authority` an ENSv1 registry child with no name row serves: the registry
    /// generation that owns its node.
    pub registry_authority: Option<String>,
    pub provenance: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Sort key for declared direct child page reads. The timestamp sorts read the child's
/// `name_current.declared_summary` through the same COALESCE the address-name sorts use, so a
/// child with no current name row, or no timestamp at that path, sorts as a null.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildrenCurrentSort {
    Name,
    ExpiresAt,
    RegisteredAt,
}

impl ChildrenCurrentSort {
    pub const fn is_timestamp(self) -> bool {
        matches!(self, Self::ExpiresAt | Self::RegisteredAt)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildrenCurrentOrder {
    Asc,
    Desc,
}

/// Page-narrowing controls for declared direct child page reads. `q` is caller-normalized text
/// compared byte-wise against the served child name as a prefix or a substring; `include_expired=false` omits
/// children whose current registration is released or whose expiry is earlier than the
/// supplied fixed evaluation time, which the page then requires.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentPageFilter<'a> {
    pub q: Option<crate::NameQuery<'a>>,
    pub include_expired: bool,
    /// Fixed evaluation time for expiry filtering across count and continuation requests;
    /// required when `include_expired` is false.
    pub evaluated_at: Option<OffsetDateTime>,
    pub sort: ChildrenCurrentSort,
    pub order: ChildrenCurrentOrder,
}

impl Default for ChildrenCurrentPageFilter<'_> {
    fn default() -> Self {
        Self {
            q: None,
            include_expired: true,
            evaluated_at: None,
            sort: ChildrenCurrentSort::Name,
            order: ChildrenCurrentOrder::Asc,
        }
    }
}

impl ChildrenCurrentPageFilter<'_> {
    /// True when the page admits every row the per-parent summary counts.
    pub const fn admits_every_child(&self) -> bool {
        self.q.is_none() && self.include_expired
    }
}

/// Sort-specific keyset position for declared direct child page reads. The name sort orders by
/// the served name the cursor already carries, so it needs no value of its own; the timestamp
/// sorts carry the row's (possibly absent) timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildrenCurrentSortValue {
    Name,
    Timestamp(Option<UnixSeconds>),
}

/// Storage-local keyset cursor for declared direct child collection reads. Every sort breaks ties
/// by served name and then by child id, so the page order is stable and readable whatever the
/// sort; the name is therefore part of every cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentKeysetCursor {
    pub sort_value: ChildrenCurrentSortValue,
    pub canonical_display_name: String,
    pub child_logical_name_id: String,
}

/// Compact metadata for the full declared direct child filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentSummary {
    pub parent_logical_name_id: String,
    pub child_count: i64,
    pub provenance_inputs: Vec<Value>,
    pub chain_positions: Vec<Value>,
    pub canonicality_summaries: Vec<Value>,
    pub last_recomputed_at: Option<OffsetDateTime>,
}

/// Bounded declared direct child page plus full-filter summary metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentPage {
    /// Exact number of children admitted by the page filters, before its cursor.
    pub total_count: u64,
    pub rows: Vec<ChildrenCurrentRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
    pub summary: ChildrenCurrentSummary,
}

/// A registry labels page's filter on the owner each label serves: the owner of the label's
/// composed name row, absent when it composes none (`project_name_summary.owner`). Addresses are
/// lower-cased by the caller. `ExcludeOwner` keeps every label not served with that owner,
/// including the ownerless ones.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryLabelOwnerFilter<'a> {
    Owner(&'a str),
    ExcludeOwner(&'a str),
}

/// Bounded page of the declared children one registry contract holds, with the exact count of
/// every such child the page's owner filter admits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryChildrenPage {
    pub rows: Vec<ChildrenCurrentRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
    pub label_count: i64,
}
