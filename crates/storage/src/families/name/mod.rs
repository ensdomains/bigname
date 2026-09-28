//! The composed name reader (TYR-36 step 7b, ruling J3): a `name_current`-shaped row assembled at
//! read from the owned key families (docs/projections.md, "Owned key families") and the identity
//! input tables, with no stored per-name row. It serves the fields the API and the verified
//! lookup read from `name_current`:
//!
//! - identity: the surface (`name_surfaces`), the selected binding and its resource's token
//!   lineage (`surface_bindings`, `resources`);
//! - `provenance.authority_selection`, the name authority selection, computed here from F1 binding
//!   candidates and history, F2a retained lifecycle events and the F2c registry node
//!   (`selection.rs`);
//! - `declared_summary.registration` and `.control` from the F2a lifecycle read
//!   (`control::lifecycle::evaluate`), with `registration.created_at` from F1 name history
//!   (ruling J4) and a released ENSv1 tombstone's lapsed authority;
//! - the NameWrapper state and fuses (F2b) masked at the publication's block time;
//! - the serving pointer and the resolver block from the F4 and F5 pointers (`serving.rs`);
//! - the coverage and support columns.
//!
//! The whole-history evidence the served row also carries (`provenance.selected_event_ids`,
//! `raw_fact_refs`, `manifest_versions`, `declared_summary.history`) is not read by any route and
//! is not composed. `declared_summary.topology` is not composed here either: the alias and
//! wildcard arms come from `topology::load_name_topology_shadow`.
//!
//! Every row describes the family marker's publication (the "publication" below): its
//! `chain_positions` and `canonicality_summary` name the marker's block, so a row carries no
//! position of its own and an `at` below the publication cannot be served from it
//! (docs/api-v1.md, "Publication switch").
mod batch;
mod compose;
mod loaders;
pub mod selection;
pub mod serving;

use sqlx::types::time::OffsetDateTime;

pub use batch::{
    load_family_name, load_family_names_by_logical_name_ids, load_family_names_by_resource_ids,
    load_family_publication,
};

/// The publication the composed rows describe: a chain's family marker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyPublication {
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub block_timestamp: OffsetDateTime,
    /// The block timestamp as `to_jsonb` renders it, the form the served row stores.
    pub block_timestamp_json: serde_json::Value,
}

impl FamilyPublication {
    pub fn timestamp_seconds(&self) -> i64 {
        self.block_timestamp.unix_timestamp()
    }
}

/// One `project_name_history` row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameHistory {
    pub first_block_number: i64,
    /// `to_jsonb(created_at)`, the form the served row stores.
    pub created_at: serde_json::Value,
    pub has_ens_v2_events: bool,
    pub event_arms: Vec<String>,
}

/// Which coverage shape the caller serves: the single-name reader adds the source classes and
/// enumeration basis to the coverage column, the batch readers do not (name_current.rs).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageShape {
    WithBasis,
    Plain,
}
