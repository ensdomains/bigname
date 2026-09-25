//! TYR-36 step 5 shadow readers: child edges (F11 with F2a, F2b and F2c), the alias and wildcard
//! arms of name topology (F10 with F5), the resolver overview's classification (F3), bound names
//! over the F5 resolver index, and the resolver `/aliases`, `/links` and `/roles` collections
//! (F10, F7 and F8). They read the family tables, the identity tables and the interim shims of
//! [`shims`] only. No served route calls them: the phase-runner harness compares them with the
//! served readers at one publication (design section 6, "Reader cutover order").
//!
//! Every reader takes a storage keyset position and evaluates time at the block clock of the
//! family marker, never a publication token, generation or request time (D6, D10). Events are
//! ordered by block number, transaction index, log index, then event identity (D12).
mod children;
mod children_page;
mod collections;
mod name_topology;
mod pointers;
mod resolver;
mod shims;

pub use children_page::{FamilyChildRow, FamilyChildrenPage, load_children_shadow_page};
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
