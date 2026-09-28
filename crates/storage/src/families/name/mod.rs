//! The composed name reader (TYR-36 step 7b, ruling J3): a `name_current`-shaped row assembled at
//! read from the owned key families (docs/projections.md, "Owned key families") and the identity
//! input tables, with no stored per-name row. It serves the fields the API routes read from
//! `name_current`; the verified lookup (crates/lookup) stays on the served tables until the flip
//! slice. The row carries:
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
//! - `declared_summary.history`, the name's latest events, read by key from `normalized_events`
//!   (`heads.rs`), which the binding diagnostics route serves;
//! - the coverage and support columns.
//!
//! The listings over composed rows are `list.rs` (search, expiring) and `bound.rs` (the names
//! bound to a resolver).
//!
//! The whole-history evidence the served row also carries (`provenance.selected_event_ids`,
//! `raw_fact_refs`, `manifest_versions`) is not read by any route and is not composed.
//! `declared_summary.topology` is not composed yet. The alias and wildcard arms have a family
//! reader (`topology::load_name_topology_shadow`); the direct arm takes its version boundary from
//! the record inventory (crates/project/src/builders/name_topology/direct.rs), so the topology
//! joins the row with the record inventory reads. Until then a composed row carries none, which
//! the records route's verified admission and avatar readback read (docs/api-v1.md).
//!
//! Every row describes the family marker's publication (the "publication" below): its
//! `chain_positions` and `canonicality_summary` name the marker's block, so a row carries no
//! position of its own and an `at` below the publication cannot be served from it
//! (docs/api-v1.md, "Publication switch").
mod batch;
mod bound;
mod compose;
mod heads;
mod list;
mod loaders;
pub mod seams;
pub mod selection;
pub mod serving;
mod summary;

use sqlx::types::time::OffsetDateTime;

pub(crate) use batch::{load as load_composed, servable_publication};
pub use bound::load_family_bound_names;
pub use list::{load_family_expiring_page, load_family_search_page};
pub use summary::compose_name_summaries;

pub use batch::{
    ensure_family_publications, load_family_name, load_family_names_by_logical_name_ids,
    load_family_names_by_resource_ids, load_family_publication,
};
pub(crate) use batch::{ensure_published, read_snapshot};
/// The composed loads and the marker read on a caller's connection, for readers of other
/// families that join composed name rows inside their own snapshot (TYR-36 step 7b slice 4).
pub(crate) use batch::{load as load_names_on, publication as publication_on};

/// A composed read reached a chain whose family marker is not servable: missing, not `live` (a
/// rebuild is still populating the families) or written by another interpreter build. The rule
/// is the publication fence's (snapshot_selection/project.rs), so a caller that did not fence
/// first, or whose fence passed before a rebuild began, still cannot compose from half-built
/// families. API callers answer it with the stale 409 (docs/api-v1.md, "Publication switch").
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
