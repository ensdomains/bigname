use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

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

/// Page-narrowing controls for declared direct child page reads. `q` is a caller-normalized
/// prefix compared byte-wise against the served child name; `include_expired=false` omits
/// children whose current registration is released or whose expiry is earlier than the
/// database's transaction time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentPageFilter<'a> {
    pub q: Option<&'a str>,
    pub include_expired: bool,
    pub sort: ChildrenCurrentSort,
    pub order: ChildrenCurrentOrder,
}

impl Default for ChildrenCurrentPageFilter<'_> {
    fn default() -> Self {
        Self {
            q: None,
            include_expired: true,
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

    const fn needs_name_current(&self) -> bool {
        self.sort.is_timestamp() || !self.include_expired
    }

    pub(super) const fn joins_name_current(&self) -> bool {
        self.needs_name_current()
    }
}

/// Sort-specific keyset position for declared direct child page reads. The name sort orders by
/// the served name the cursor already carries, so it needs no value of its own; the timestamp
/// sorts carry the row's (possibly absent) timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildrenCurrentSortValue {
    Name,
    Timestamp(Option<OffsetDateTime>),
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
    pub rows: Vec<ChildrenCurrentRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
    pub summary: ChildrenCurrentSummary,
}

/// Bounded page of the declared children one registry contract holds, with the exact count of
/// every such child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryChildrenPage {
    pub rows: Vec<ChildrenCurrentRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
    pub label_count: i64,
}
