use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::{SurfaceBindingKind, UnixSeconds};

/// Persisted ENSv1 address-to-surface relation row for current address collections.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNameCurrentRow {
    pub address: String,
    pub logical_name_id: String,
    pub relation: AddressNameRelation,
    pub namespace: String,
    pub canonical_display_name: String,
    pub namehash: String,
    pub surface_binding_id: Uuid,
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub binding_kind: SurfaceBindingKind,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Supported current-relation facets for the first ENSv1 address-name slice.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AddressNameRelation {
    Registrant,
    TokenHolder,
    EffectiveController,
    /// The address holds an ENSv2 registry role on the name's current registration.
    RoleHolder,
}

impl AddressNameRelation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registrant => "registrant",
            Self::TokenHolder => "token_holder",
            Self::EffectiveController => "effective_controller",
            Self::RoleHolder => "role_holder",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "registrant" => Ok(Self::Registrant),
            "token_holder" => Ok(Self::TokenHolder),
            "effective_controller" => Ok(Self::EffectiveController),
            "role_holder" => Ok(Self::RoleHolder),
            _ => bail!("unknown address_names_current relation {value}"),
        }
    }
}

/// Storage-local grouping mode for collapsing relation rows into stable collection representatives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressNamesCurrentDedupe {
    Surface,
    Resource,
}

impl AddressNamesCurrentDedupe {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Resource => "resource",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressNamesCurrentSort {
    Name,
    ExpiresAt,
    RegisteredAt,
    /// The name's first observation (`registration.created_at`), not its registration time.
    CreatedAt,
}

impl AddressNamesCurrentSort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::ExpiresAt => "expires_at",
            Self::RegisteredAt => "registered_at",
            Self::CreatedAt => "created_at",
        }
    }

    pub(super) const fn is_timestamp(self) -> bool {
        matches!(self, Self::ExpiresAt | Self::RegisteredAt | Self::CreatedAt)
    }
}

/// How a name-list text filter compares its already-normalized text with the stored normalized
/// name: as a leading prefix or as a substring anywhere in the name. Both are literal byte
/// comparisons; `%`, `_` and `\` in the text match themselves.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NameQueryMatch {
    #[default]
    Prefix,
    Contains,
}

/// A name-list text filter: normalized text and how it matches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameQuery<'a> {
    pub text: &'a str,
    pub mode: NameQueryMatch,
}

impl<'a> NameQuery<'a> {
    pub const fn prefix(text: &'a str) -> Self {
        Self {
            text,
            mode: NameQueryMatch::Prefix,
        }
    }

    pub const fn contains(text: &'a str) -> Self {
        Self {
            text,
            mode: NameQueryMatch::Contains,
        }
    }

    /// The `LIKE ... ESCAPE '\'` pattern that matches the text literally in this mode.
    pub fn like_pattern(&self) -> String {
        let escaped = self
            .text
            .replace('\\', r"\\")
            .replace('%', r"\%")
            .replace('_', r"\_");
        match self.mode {
            NameQueryMatch::Prefix => format!("{escaped}%"),
            NameQueryMatch::Contains => format!("%{escaped}%"),
        }
    }
}

impl std::fmt::Display for NameQuery<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.mode {
            NameQueryMatch::Prefix => write!(formatter, "prefix {}", self.text),
            NameQueryMatch::Contains => write!(formatter, "contains {}", self.text),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressNamesCurrentOrder {
    Asc,
    Desc,
}

impl AddressNamesCurrentOrder {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

/// Storage-local grouped collection item built from one or more relation rows.
///
/// Non-relation fields come from the stable representative row chosen by the default collection
/// sort order. This helper exists for storage-side dedupe only; it does not define public API
/// representative-selection semantics.
///
/// An ENSv1 registry child with no name surface (`families::records::registry_children`) has no
/// surface binding or binding kind; its `canonical_display_name` and `normalized_name` are the
/// name its parent's subnames route serves, its `resource_id` is the node's registry-only
/// resource, `served_owner` is its current registry owner, `served_authority` the registry
/// generation that owns its node and `served_lifecycle_shadow` whether its only surface is a shadow
/// one a NameWrapper or ENSv1 registrar event observed. Every other entry has a surface binding and binding kind, no `served_owner` or
/// `served_authority`, and `served_lifecycle_shadow` false.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNameCurrentEntry {
    pub address: String,
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub surface_binding_id: Option<Uuid>,
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub binding_kind: Option<SurfaceBindingKind>,
    pub relations: Vec<AddressNameRelation>,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
    pub served_owner: Option<String>,
    pub served_authority: Option<String>,
    pub served_lifecycle_shadow: bool,
}

impl AddressNameCurrentEntry {
    /// True for a surface-less ENSv1 registry child's entry.
    pub fn is_registry_child(&self) -> bool {
        self.surface_binding_id.is_none()
    }
}

/// Keyset cursor fields for storage-side address-name collection pagination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentCursor {
    pub canonical_display_name: String,
    pub logical_name_id: String,
    pub resource_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddressNamesCurrentSortedCursorValue {
    Name(String),
    Timestamp(Option<UnixSeconds>),
}

/// Sort-specific keyset cursor for v2 address-name collection reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentSortedCursor {
    pub sort_value: AddressNamesCurrentSortedCursorValue,
    pub logical_name_id: String,
    pub resource_id: Uuid,
}

/// Compact metadata for the full filtered grouped address-name collection.
///
/// These fields are derived from the same representative rows returned by
/// [`crate::collapse_address_name_current_rows`], but without returning every grouped entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentSummary {
    pub grouped_entry_count: u64,
    pub provenance: AddressNamesCurrentProvenanceSummary,
    pub chain_positions: Value,
    pub consistency: String,
    pub last_recomputed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentProvenanceSummary {
    pub normalized_event_ids: Value,
    pub raw_fact_refs: Value,
    pub manifest_versions: Value,
    pub derivation_kind: Option<String>,
}

/// Bounded page of grouped current address-name entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentPage {
    pub entries: Vec<AddressNameCurrentEntry>,
    pub next_cursor: Option<AddressNamesCurrentCursor>,
    pub summary: AddressNamesCurrentSummary,
}

/// Bounded sorted page of grouped current address-name entries for the extended v2 read path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentSortedPage {
    pub entries: Vec<AddressNameCurrentEntry>,
    pub next_cursor: Option<AddressNamesCurrentSortedCursor>,
    pub summary: AddressNamesCurrentSummary,
}
