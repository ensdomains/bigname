//! Readers of the record families: the resource resolver pointer (F5), the registry-node
//! pointer and the ENSv1 mirror walk over it (F4), the record inventory assembled from the
//! node-keyed and record-id record families (F6, F7), the link selection, the inverse address
//! index (F14) and the reverse claim (F12).
//!
//! Every reader builds the served value for its key from the family rows. Events are ordered in the
//! canonical event order of the families (block number, transaction index, log index, then the
//! emission ordinal (docs/glossary.md#emission-ordinal) when both indexes are present, then the
//! event identity as bytes, with a synthesised event's missing positions first; see
//! `FamilyPosition`), never by the generated event id.
mod address_names;
mod address_relations;
mod address_roles;
mod assemble;
mod candidates;
mod facts;
mod former_registrants;
mod inventory;
mod links;
mod mirror;
mod payload;
mod pointer;
mod primary;
mod profiles;
mod registry_children;
mod resolves_to;
mod resolves_to_serving;
mod reverse;
mod reverse_page;
mod rows;
pub mod seams;
mod serving;

use std::cmp::Ordering;

use serde_json::Value;
use sqlx::{Row, postgres::PgRow};

pub use address_names::load_family_address_names_page;
pub(crate) use address_names::{compose_address_name_rows, name_relations_on};
pub use facts::{
    ResolverClassification as FamilyResolverClassification,
    load_classification as load_family_resolver_classification,
};
pub use former_registrants::{
    FormerRegistrantFilter, FormerRegistrantPage, lapsed_registrant,
    load_family_former_registrant_page,
};
pub use inventory::{
    FamilyAttribution, FamilyRecordInventory, load_family_record_counts,
    load_family_record_inventories_on, load_family_record_inventory,
    load_family_record_inventory_detail, load_family_record_inventory_detail_on,
    load_family_record_inventory_for_snapshot, load_family_supported_record_inventory_for_snapshot,
};
pub use links::{
    DEFAULT_RECORD_NODE, FamilyAliasSourcePointer, FamilyLink, FamilyWildcardSource, LinkSelection,
    load_family_alias_source_pointer, load_family_link_selection, load_family_wildcard_source,
};
pub use pointer::{FamilyResourcePointer, load_family_resource_pointer};
pub use primary::{load_family_primary_name_snapshot, load_family_primary_name_snapshots};
pub use resolves_to_serving::{load_family_resolves_to_evm_page, load_family_resolves_to_page};
pub use reverse::{FamilyReverseClaim, load_family_reverse_claim};
pub use reverse_page::{
    load_family_reverse_identity_groups, load_family_reverse_primary_snapshots,
};

/// The resolver address a clear writes: the zero address, or the empty string for a pointer event
/// without a resolver.
pub(crate) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Whether a stored resolver address is a clear.
pub(crate) fn is_cleared(address: Option<&str>) -> bool {
    address.is_none_or(|address| address.is_empty() || address == ZERO_ADDRESS)
}

/// A position in the canonical event order (docs/glossary.md#canonical-event-order; the project
/// crate's families/position.rs): block number, transaction index, log index, then, when the event has
/// both a transaction and a log index, the emission ordinal its identity ends with
/// (docs/glossary.md#emission-ordinal), then the event identity by its bytes. `None` sorts first
/// at each step. Equality is field equality; the order ends with the full identity, so two
/// positions compare equal exactly when they are equal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyPosition {
    pub block_number: i64,
    pub transaction_index: Option<i64>,
    pub log_index: Option<i64>,
    pub event_identity: String,
}

impl FamilyPosition {
    /// A secondary position stored as a JSON object beside a row's own position.
    pub fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            block_number: value.get("block_number")?.as_i64()?,
            transaction_index: value.get("transaction_index").and_then(Value::as_i64),
            log_index: value.get("log_index").and_then(Value::as_i64),
            event_identity: value.get("event_identity")?.as_str()?.to_owned(),
        })
    }

    /// The four position columns every family row carries for its last owning event.
    pub(crate) fn from_row(row: &PgRow) -> anyhow::Result<Self> {
        Ok(Self {
            block_number: row.try_get("block_number")?,
            transaction_index: row.try_get("transaction_index")?,
            log_index: row.try_get("log_index")?,
            event_identity: row.try_get("event_identity")?,
        })
    }

    /// The emission ordinal, as the project crate's `families::emission_ordinal` reads it
    /// (docs/glossary.md#emission-ordinal): the identity's final `:`-separated segment when
    /// the event has a transaction and a log index and that segment is a nonempty run of ASCII
    /// digits no greater than `u32::MAX` (leading zeros allowed). Boundary facts and
    /// family-internal identities have none. A copy, since storage does not depend on project.
    pub fn emission_ordinal(&self) -> Option<u32> {
        self.transaction_index?;
        self.log_index?;
        let (_, tail) = self.event_identity.rsplit_once(':')?;
        if tail.is_empty() || !tail.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        tail.parse().ok()
    }
}

impl Ord for FamilyPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        self.block_number
            .cmp(&other.block_number)
            .then_with(|| self.transaction_index.cmp(&other.transaction_index))
            .then_with(|| self.log_index.cmp(&other.log_index))
            .then_with(|| self.emission_ordinal().cmp(&other.emission_ordinal()))
            .then_with(|| {
                self.event_identity
                    .as_bytes()
                    .cmp(other.event_identity.as_bytes())
            })
    }
}

impl PartialOrd for FamilyPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod position_tests;
