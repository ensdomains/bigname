//! The storage families code (`crates/storage/src/families`) mixes the composition Project's
//! family step stores with read-only serving queries. Only the composition decides persisted rows,
//! so only its files are hashed. A reader edit leaves both hashes alone.
//!
//! Every production `.rs` file under the root must be listed in exactly one of the two lists below.
//! A listed composition file that is missing, or an unlisted `.rs` file, fails the hash, so a file
//! cannot move into or out of the composition without a reviewed edit here. Keep the composition
//! list closed under what Project's summary, path and lookup compositors reach: a composition file that
//! starts calling a reader file, or embeds another file (such as SQL), lists that file in
//! COMPOSITION_FILES in the same change.

use std::{collections::BTreeSet, ffi::OsStr, fs, io, path::Path};

use crate::{
    classes::{Classified, both, project},
    compute::{Input, collect_file, relative_key, source_exclusion},
};

pub(crate) const ROOT: &str = "crates/storage/src/families";

/// Lookup decisions, per-key output and inventory dependency emission.
const LOOKUP: &str = "Project's lookup publication";
/// Family positions and the emission ordinal Project orders journal rows and hydration by.
const POSITIONS: &str = "Project's family positions and emission ordinal";
/// The summary composition and the composed-row loader it calls (`batch::load_chain`).
const SUMMARY: &str = "Project's name summary composition";
/// The expiry listing's eligibility and public authority, which the summary stores.
const EXPIRY: &str = "the expiry eligibility and authority Project's summary stores";
/// Current ENSv2 path decisions also schedule summary deadlines.
const PATHS: &str = "ENSv2 path decisions that schedule Project's summary deadlines";
/// The lifecycle evaluation and the control rows it loads and decides from.
const LIFECYCLE: &str = "the lifecycle evaluation Project's summary decides from";

/// Reached from Project's family step (`crates/project/src/families`): the name summary
/// composition (`derived/summary.rs`), its `RequiredOwnerMissing` check, and the family position
/// ordinals (`position.rs`, `hydrate/{reverse,text}.rs`). Every entry is project-class except one
/// Interpret also reaches.
pub(crate) const COMPOSITION_FILES: &[Classified] = &[
    project("crates/storage/src/families/lookup/mod.rs", LOOKUP),
    project("crates/storage/src/families/lookup/types.rs", LOOKUP),
    project("crates/storage/src/families/lookup/inventory.rs", LOOKUP),
    project("crates/storage/src/families/name/lookup.rs", LOOKUP),
    project(
        "crates/storage/src/families/records/address_names.rs",
        LOOKUP,
    ),
    project("crates/storage/src/families/records/assemble.rs", LOOKUP),
    project(
        "crates/storage/src/families/records/inventory_publication.rs",
        LOOKUP,
    ),
    project(
        "crates/storage/src/families/records/inventory_selection.rs",
        LOOKUP,
    ),
    project(
        "crates/storage/src/families/records/inventory_types.rs",
        LOOKUP,
    ),
    project(
        "crates/storage/src/families/records/inventory_cutoff.rs",
        LOOKUP,
    ),
    project("crates/storage/src/families/records/links.rs", LOOKUP),
    project("crates/storage/src/families/records/payload.rs", LOOKUP),
    project("crates/storage/src/families/records/pointer.rs", LOOKUP),
    project("crates/storage/src/families/records/profiles.rs", LOOKUP),
    project("crates/storage/src/families/records/rows.rs", LOOKUP),
    project(
        "crates/storage/src/families/records/text_hydration.rs",
        LOOKUP,
    ),
    project("crates/storage/src/families/position.rs", POSITIONS),
    project("crates/storage/src/families/name/mod.rs", SUMMARY),
    project("crates/storage/src/families/name/summary.rs", SUMMARY),
    project("crates/storage/src/families/name/batch.rs", SUMMARY),
    project("crates/storage/src/families/name/compose.rs", SUMMARY),
    project("crates/storage/src/families/name/heads.rs", SUMMARY),
    project(
        "crates/storage/src/families/name/wrapper_fields.rs",
        SUMMARY,
    ),
    project(
        "crates/storage/src/families/search_dictionary/shape.rs",
        SUMMARY,
    ),
    project("crates/storage/src/families/name/list_keys.rs", EXPIRY),
    project("crates/storage/src/families/name/loaders.rs", EXPIRY),
    both(
        "crates/storage/src/families/name/rendered.rs",
        "Interpret writes identity search documents through rendered_name_sql_unqualified",
    ),
    project("crates/storage/src/families/name/resolvability.rs", EXPIRY),
    project("crates/storage/src/families/name/resolution_path.rs", PATHS),
    project(
        "crates/storage/src/families/name/resolution_path/facts.rs",
        PATHS,
    ),
    project(
        "crates/storage/src/families/name/resolution_path/physical.rs",
        PATHS,
    ),
    project(
        "crates/storage/src/families/name/resolution_path/wrapper.rs",
        PATHS,
    ),
    project(
        "crates/storage/src/families/control/permissions/registry_support.rs",
        PATHS,
    ),
    project("crates/storage/src/families/records/facts.rs", PATHS),
    project("crates/storage/src/families/records/mirror.rs", PATHS),
    project("crates/storage/src/families/records/mod.rs", PATHS),
    project("crates/storage/src/families/records/serving.rs", PATHS),
    project("crates/storage/src/families/name/selection.rs", PATHS),
    project("crates/storage/src/families/name/serving.rs", PATHS),
    project(
        "crates/storage/src/families/records/address_publication.rs",
        PATHS,
    ),
    project(
        "crates/storage/src/families/records/address_relation_inputs.rs",
        PATHS,
    ),
    project(
        "crates/storage/src/families/records/address_relations.rs",
        PATHS,
    ),
    project(
        "crates/storage/src/families/records/address_roles.rs",
        PATHS,
    ),
    project(
        "crates/storage/src/families/control/permissions/grants.rs",
        PATHS,
    ),
    project("crates/storage/src/families/control/cutover.rs", LIFECYCLE),
    project("crates/storage/src/families/control/position.rs", LIFECYCLE),
    project("crates/storage/src/families/control/registry.rs", LIFECYCLE),
    project("crates/storage/src/families/control/rows.rs", LIFECYCLE),
    project("crates/storage/src/families/control/wrapper.rs", LIFECYCLE),
    project(
        "crates/storage/src/families/control/lifecycle/mod.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/admission.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/canonical.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/policy.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/control.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/expiry.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/laterals.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/load.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/membership.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/select.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/served.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/tombstone.rs",
        LIFECYCLE,
    ),
    project(
        "crates/storage/src/families/control/lifecycle/view.rs",
        LIFECYCLE,
    ),
];

/// Serving reads and module wiring the composition does not call.
const READER_FILES: &[&str] = &[
    // The direct read's alias path walk (`alias_path_tests.rs` pins its twin statements).
    "crates/storage/src/families/alias_path.rs",
    "crates/storage/src/families/lookup/read.rs",
    "crates/storage/src/families/lookup/read_inventory.rs",
    "crates/storage/src/families/lookup/reverse.rs",
    "crates/storage/src/families/records/primary/batch.rs",
    "crates/storage/src/families/records/reverse/batch.rs",
    "crates/storage/src/families/search_dictionary.rs",
    "crates/storage/src/families/search_dictionary/candidates.rs",
    "crates/storage/src/families/search_dictionary/pointer_parity.rs",
    "crates/storage/src/families/name/basenames_context.rs",
    "crates/storage/src/families/mod.rs",
    "crates/storage/src/families/control/mod.rs",
    "crates/storage/src/families/control/permissions/candidates.rs",
    "crates/storage/src/families/control/permissions/ens_v2.rs",
    "crates/storage/src/families/control/permissions/facts.rs",
    "crates/storage/src/families/control/permissions/mod.rs",
    "crates/storage/src/families/control/permissions/operators.rs",
    "crates/storage/src/families/control/permissions/page.rs",
    "crates/storage/src/families/control/permissions/restrictions.rs",
    "crates/storage/src/families/control/permissions/wrapper_registry.rs",
    "crates/storage/src/families/control/permissions/summary.rs",
    // The composed-row listings and the API-side topology enrichment (`batch::load`).
    "crates/storage/src/families/name/bound.rs",
    "crates/storage/src/families/name/list.rs",
    "crates/storage/src/families/name/list/search_rows.rs",
    "crates/storage/src/families/name/list/search_test_support.rs",
    "crates/storage/src/families/name/list/expiring.rs",
    // Test-support measurement of the read-only selector; no Project caller.
    "crates/storage/src/families/name/list/expiring/measure.rs",
    // The pointer of nodes served without a composed row, read beside the composed rows.
    "crates/storage/src/families/name/registry_pointer.rs",
    "crates/storage/src/families/name/seams.rs",
    "crates/storage/src/families/name/topology.rs",
    "crates/storage/src/families/records/candidates.rs",
    "crates/storage/src/families/records/former_owners.rs",
    "crates/storage/src/families/records/inventory.rs",
    "crates/storage/src/families/records/node_inventory.rs",
    "crates/storage/src/families/records/primary.rs",
    "crates/storage/src/families/records/registry_children.rs",
    "crates/storage/src/families/records/resolves_to.rs",
    "crates/storage/src/families/records/resolves_to_serving.rs",
    "crates/storage/src/families/records/reverse.rs",
    "crates/storage/src/families/records/reverse_page.rs",
    "crates/storage/src/families/records/seams.rs",
    "crates/storage/src/families/topology/children.rs",
    "crates/storage/src/families/topology/children_page.rs",
    "crates/storage/src/families/topology/children_page/child_flags.rs",
    "crates/storage/src/families/topology/collections.rs",
    "crates/storage/src/families/topology/mod.rs",
    "crates/storage/src/families/topology/name_summary.rs",
    "crates/storage/src/families/topology/name_topology.rs",
    "crates/storage/src/families/topology/overview.rs",
    "crates/storage/src/families/topology/pointers.rs",
    "crates/storage/src/families/topology/registry_children.rs",
    "crates/storage/src/families/topology/shims.rs",
];

pub(crate) fn collect(
    workspace_root: &Path,
    cfg_test_sources: &BTreeSet<String>,
    inputs: &mut Vec<Input>,
) -> io::Result<()> {
    for entry in COMPOSITION_FILES {
        let relative_path = entry.path;
        let path = workspace_root.join(relative_path);
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "the content hashes require storage family composition source \
                     {relative_path}; if it moved, update COMPOSITION_FILES in the same change"
                ),
            ));
        }
        collect_file(workspace_root, &path, entry.class, inputs)?;
    }
    check_classified(workspace_root, &workspace_root.join(ROOT), cfg_test_sources)
}

fn check_classified(
    workspace_root: &Path,
    directory: &Path,
    cfg_test_sources: &BTreeSet<String>,
) -> io::Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            check_classified(workspace_root, &path, cfg_test_sources)?;
            continue;
        }
        if path.extension() != Some(OsStr::new("rs"))
            || source_exclusion(workspace_root, &path, cfg_test_sources)?.is_some()
        {
            continue;
        }
        let relative_path = relative_key(workspace_root, &path)?;
        if !COMPOSITION_FILES
            .iter()
            .any(|entry| entry.path == relative_path)
            && !READER_FILES.contains(&relative_path.as_str())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "storage family source {relative_path} is unclassified; list it in \
                     COMPOSITION_FILES if Project's family step reaches it, else in READER_FILES"
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn reader_files() -> &'static [&'static str] {
    READER_FILES
}
