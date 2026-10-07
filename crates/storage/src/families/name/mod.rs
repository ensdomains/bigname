//! The composed name reader: a `name_current`-shaped row assembled at
//! read from the owned key families (docs/projections.md, "Owned key families") and the identity
//! input tables, without persisting the composed row. It serves the fields the API routes read from
//! `name_current`, including the verified lookup inputs. The row carries:
//!
//! - identity: the surface (`name_surfaces`), the selected binding and its resource's token
//!   lineage (`surface_bindings`, `resources`);
//! - `provenance.authority_selection`, the name authority selection, computed here from F1 binding
//!   candidates and history, F2a retained lifecycle events and the F2c registry node
//!   (`selection.rs`);
//! - `declared_summary.registration` and `.control` from the F2a lifecycle read
//!   (`control::lifecycle::evaluate`), with `registration.created_at` from F1 name history
//!   and a released ENSv1 tombstone's lapsed authority;
//! - the NameWrapper state and fuses (F2b) masked at the publication's block time;
//! - the serving pointer and the resolver block from the F4 and F5 pointers (`serving.rs`);
//! - `declared_summary.history`, the name's latest events, read by key from `normalized_events`
//!   (`heads.rs`), which the binding diagnostics route serves;
//! - the coverage and support columns;
//! - the whole declared resolution topology, composed on the same database snapshot: wildcard
//!   sources, direct and ownerless ENS, and admitted Basenames L1 transport.
//!
//! The listings over composed rows are `list.rs` (search, expiring) and `bound.rs` (the names
//! bound to a resolver).
//!
//! Whole-history evidence (`selected_event_ids`, `raw_fact_refs`) is not composed. Basenames
//! execution admission is retained in `manifest_versions` because verified lookup consumes it.
//!
//! Every row describes the family marker's publication (the "publication" below): its
//! `chain_positions` and `canonicality_summary` name the marker's block, so a row carries no
//! position of its own and an `at` below the publication cannot be served from it.
mod batch;
mod bound;
mod compose;
mod heads;
mod list;
mod list_keys;
mod loaders;
pub mod rendered;
mod resolution_path;
mod resolvability;
pub mod seams;
pub mod selection;
pub mod serving;
mod summary;
mod topology;
mod wrapper_fields;

use sqlx::types::time::OffsetDateTime;

pub(crate) use batch::{
    all_servable_publications, load as load_composed, load_base as load_composed_base,
    servable_publication,
};
#[cfg(test)]
pub(crate) use bound::BOUND_CANDIDATES_SQL;
pub use bound::load_family_bound_names;
#[cfg(test)]
pub(crate) use list::SEARCH_CANDIDATES_SQL;
pub use list::{load_family_expiring_page, load_family_search_page};
pub(crate) use list_keys::{FINITE_REGISTRATION_EXPIRY_SQL, public_authority};
#[cfg(test)]
pub(crate) use loaders::{MIGRATIONS_SQL, RESOURCE_POINTERS_SQL, RESOURCES_SQL, canonical_uuid};
pub use resolution_path::PHYSICAL_POINTER_EVENT_SQL;
pub use summary::{
    NameSummaryPublication, compose_name_resolution_summaries, compose_name_summaries,
    compose_name_summary_publication,
};

pub use batch::{
    ensure_family_publications, load_family_name, load_family_name_on,
    load_family_names_by_logical_name_ids, load_family_names_by_resource_ids,
    load_family_publication,
};
pub(crate) use batch::{ensure_published, read_snapshot};
/// The composed loads and the marker read on a caller's connection, for readers of other
/// families that join composed name rows inside their own snapshot.
pub(crate) use batch::{load as load_names_on, publication as publication_on};

/// A composed read reached a chain whose family marker is not servable: missing, not `live` (a
/// rebuild is still populating the families) or written by another interpreter build. The rule
/// is the publication fence's (snapshot_selection/project.rs), so a caller that did not fence
/// first, or whose fence passed before a rebuild began, still cannot compose from half-built
/// families. API callers answer it with the stale 409.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyPublicationUnavailable {
    pub chain_id: String,
}

impl std::fmt::Display for FamilyPublicationUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "chain {} owned key families are not published for this build",
            self.chain_id
        )
    }
}

impl std::error::Error for FamilyPublicationUnavailable {}

/// Whether `error` is, or was caused by, a [`FamilyPublicationUnavailable`].
pub fn is_publication_unavailable(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.is::<FamilyPublicationUnavailable>())
}

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
