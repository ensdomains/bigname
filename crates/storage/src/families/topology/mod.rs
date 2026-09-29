//! Production readers for child pages/counts, aliases, wildcard topology and resolver
//! overview/collections. They use the owned family tables plus retained identity, lineage and
//! normalized events, composing attached names in the same publication snapshot.
//!
//! Readers accept storage keyset positions. Time-dependent filters use the family marker's
//! block timestamp; event ordering uses block, transaction, log, emission ordinal and event
//! identity, never the generated normalized event id.
mod children;
mod children_page;
mod collections;
mod name_summary;
mod name_topology;
mod overview;
mod pointers;
mod registry_children;
mod shims;

pub use children_page::{
    FamilyChildRow, FamilyChildrenPage, count_children_shadow, load_children_shadow_page,
};
pub(crate) use children_page::{
    RegistryLabels, count as count_children_on, counts as count_children_of_parents_on,
    page as children_page_on, require_publication,
};
pub use collections::{
    FamilyCollectionPage, load_resolver_aliases_shadow, load_resolver_links_shadow,
    load_resolver_roles_shadow,
};
pub(crate) use name_topology::load_name_topology_on;
pub use name_topology::load_name_topology_shadow;
pub use overview::load_family_resolver_current;
pub(crate) use overview::{
    FAMILY_RESOLVER_SERVED_ROWS, FAMILY_RESOLVER_SUMMARY, resolver_classification_relation,
};
pub(crate) use pointers::load_family_wildcard_source_on;
pub use pointers::{
    FamilyAliasSourcePointer, FamilyLink, FamilyWildcardSource, LinkSelection,
    load_family_alias_source_pointer, load_family_link_selection, load_family_wildcard_source,
};
pub(crate) use registry_children::{
    RegistryChildRow, load_owned_registry_children, published_surface_exists,
};
