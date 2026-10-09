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
mod address_publication;
mod address_relation_inputs;
mod address_relations;
mod address_roles;
mod assemble;
mod candidates;
pub(crate) mod facts;
mod former_owners;
mod inventory;
mod inventory_cutoff;
mod inventory_publication;
mod inventory_selection;
mod inventory_types;
pub use inventory_publication::{compose_lookup_inventories_at, compose_lookup_record_keys_at};
mod links;
pub(crate) mod mirror;
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
pub(crate) mod serving;

use sqlx::{Row, postgres::PgRow};

pub use address_names::load_family_address_names_page;
pub(crate) use address_names::{
    AddressComposer, ComposedName, address_name_candidates, compose_candidate_rows, includes_roles,
};
pub(crate) use address_names::{compose_address_name_rows, name_relations_at};
pub use address_publication::CurrentHistoryRelation;
pub(crate) use address_publication::publication_relations;
pub use facts::{
    ResolverClassification as FamilyResolverClassification,
    load_classification as load_family_resolver_classification,
};
pub use former_owners::{
    FormerOwnerFilter, FormerOwnerPage, lapsed_owner, load_family_former_owner_page,
};
pub use inventory::{
    FamilyAttribution, FamilyRecordInventory, load_family_record_counts,
    load_family_record_inventories_on, load_family_record_inventory,
    load_family_record_inventory_detail, load_family_record_inventory_detail_on,
    load_family_record_inventory_for_snapshot, load_family_supported_record_inventory_for_snapshot,
};
pub use links::{
    DEFAULT_RECORD_NODE, FamilyLink, FamilyWildcardSource, LinkSelection,
    load_family_link_selection, load_family_wildcard_source,
};
pub(crate) use mirror::{SAME_LABELS, lowercase_hashes, suffix_namehash};
pub use pointer::{FamilyResourcePointer, load_family_resource_pointer};
pub use primary::{load_family_primary_name_snapshot, load_family_primary_name_snapshots};
pub(crate) use registry_children::{load_registry_children, registry_child_rows};
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

pub use super::position::Position as FamilyPosition;

impl FamilyPosition {
    /// The four position columns every family row carries for its last owning event.
    pub(crate) fn from_row(row: &PgRow) -> anyhow::Result<Self> {
        Ok(Self {
            block_number: row.try_get("block_number")?,
            transaction_index: row.try_get("transaction_index")?,
            log_index: row.try_get("log_index")?,
            event_identity: row.try_get("event_identity")?,
        })
    }
}

#[cfg(test)]
mod position_tests;
