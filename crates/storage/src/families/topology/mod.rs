//! Shadow readers over the owned key families (docs/projections.md, "Owned key families") for
//! the subnames page, the registry labels page, the child counts, the alias and wildcard parts
//! of a name's topology, the resolver overview's classification and bound names, and the
//! resolver `/aliases`, `/links` and `/roles` collections. They read the family tables
//! (`project_child_edge_candidate`, `project_parent_subregistry`,
//! `project_child_registration_state`, `project_wrapper_state`, `project_name_state`,
//! `project_binding_candidate`, `project_resource_pointer`, `project_name_summary`,
//! `project_resolver_classification`, `project_name_alias`, `project_resolver_alias`,
//! `project_resolver_link`, `project_grant`), the identity and lineage tables, and the interim
//! reads [`shims`] lists, some of which are still inline reads of `name_current`,
//! `resolver_current` and `normalized_events`. Under the publication switch
//! (`publication_source::serve_from_families`) the child readers serve the subnames and registry
//! labels pages and the child counts (`crate::children`); the phase-runner harness compares every
//! reader with the served one at one publication.
//!
//! Every reader takes a storage keyset position and nothing else: no publication token,
//! generation or request time. Time-dependent filters read the block timestamp of the family
//! marker (`project_family_marker`). Where a reader compares family positions it uses the
//! canonical event order (docs/glossary.md#canonical-event-order): block number, transaction
//! index, log index, the emission ordinal (docs/glossary.md#emission-ordinal), then event
//! identity, never the generated normalized event id. The declaration fallback takes each
//! manifest's latest update by normalized event id as today's manifest staging does.
mod children;
mod children_page;
mod collections;
mod name_summary;
mod name_topology;
mod pointers;
mod resolver;
mod shims;

pub use children_page::{
    FamilyChildRow, FamilyChildrenPage, count_children_shadow, load_children_shadow_page,
    load_registry_children_shadow_page,
};
pub(crate) use children_page::{
    count as count_children_on, counts as count_children_of_parents_on, page as children_page_on,
    require_publication,
};
pub use collections::{
    FamilyCollectionPage, load_resolver_aliases_shadow, load_resolver_links_shadow,
    load_resolver_roles_shadow,
};
pub use name_topology::load_name_topology_shadow;
pub use pointers::{
    FamilyAliasSourcePointer, FamilyLink, FamilyWildcardSource, LinkSelection,
    load_family_alias_source_pointer, load_family_link_selection, load_family_wildcard_source,
};
pub use resolver::{
    ClassificationSource, FamilyResolverClassification, load_bound_names_shadow,
    load_resolver_shadow,
};
