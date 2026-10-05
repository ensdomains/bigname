//! The storage families code (`crates/storage/src/families`) mixes the composition Project's
//! family step stores with read-only serving queries. Only the composition decides persisted rows,
//! so only its files are hashed; a reader edit leaves the hash alone.
//!
//! Every production `.rs` file under the root must be listed in exactly one of the two lists below.
//! A listed composition file that is missing, or an unlisted `.rs` file, fails the hash, so a file
//! cannot move into or out of the composition without a reviewed edit here. Keep the composition
//! list closed under what `compose_name_summary_publication` reaches: a composition file that
//! starts calling a reader file, or embeds another file (such as SQL), lists that file in
//! COMPOSITION_FILES in the same change.

use std::{collections::BTreeSet, ffi::OsStr, fs, io, path::Path};

use crate::compute::{Input, collect_file, relative_key, source_exclusion};

pub(crate) const ROOT: &str = "crates/storage/src/families";

/// Reached from Project's family step (`crates/project/src/families`): the name summary
/// composition (`derived/summary.rs`), its `RequiredOwnerMissing` check, and the family position
/// ordinals (`position.rs`, `hydrate/{reverse,text}.rs`).
pub(crate) const COMPOSITION_FILES: &[&str] = &[
    // Family positions and the emission ordinal Project orders journal rows and hydration by.
    "crates/storage/src/families/position.rs",
    // The summary composition and the composed-row loader it calls (`batch::load_chain`).
    "crates/storage/src/families/name/mod.rs",
    "crates/storage/src/families/name/summary.rs",
    "crates/storage/src/families/name/batch.rs",
    "crates/storage/src/families/name/compose.rs",
    "crates/storage/src/families/name/heads.rs",
    // The expiry listing's eligibility and public authority, which the summary stores.
    "crates/storage/src/families/name/list_keys.rs",
    "crates/storage/src/families/name/loaders.rs",
    "crates/storage/src/families/name/resolvability.rs",
    "crates/storage/src/families/name/selection.rs",
    "crates/storage/src/families/name/serving.rs",
    "crates/storage/src/families/records/address_publication.rs",
    "crates/storage/src/families/records/address_relation_inputs.rs",
    "crates/storage/src/families/records/address_relations.rs",
    "crates/storage/src/families/records/address_roles.rs",
    "crates/storage/src/families/control/permissions/grants.rs",
    // The lifecycle evaluation and the control rows it loads and decides from.
    "crates/storage/src/families/control/cutover.rs",
    "crates/storage/src/families/control/position.rs",
    "crates/storage/src/families/control/registry.rs",
    "crates/storage/src/families/control/rows.rs",
    "crates/storage/src/families/control/wrapper.rs",
    "crates/storage/src/families/control/lifecycle/mod.rs",
    "crates/storage/src/families/control/lifecycle/admission.rs",
    "crates/storage/src/families/control/lifecycle/control.rs",
    "crates/storage/src/families/control/lifecycle/expiry.rs",
    "crates/storage/src/families/control/lifecycle/laterals.rs",
    "crates/storage/src/families/control/lifecycle/load.rs",
    "crates/storage/src/families/control/lifecycle/membership.rs",
    "crates/storage/src/families/control/lifecycle/select.rs",
    "crates/storage/src/families/control/lifecycle/served.rs",
    "crates/storage/src/families/control/lifecycle/tombstone.rs",
    "crates/storage/src/families/control/lifecycle/view.rs",
];

/// Serving reads and module wiring the composition does not call.
const READER_FILES: &[&str] = &[
    "crates/storage/src/families/mod.rs",
    "crates/storage/src/families/control/mod.rs",
    "crates/storage/src/families/control/permissions/candidates.rs",
    "crates/storage/src/families/control/permissions/facts.rs",
    "crates/storage/src/families/control/permissions/mod.rs",
    "crates/storage/src/families/control/permissions/operators.rs",
    "crates/storage/src/families/control/permissions/page.rs",
    "crates/storage/src/families/control/permissions/restrictions.rs",
    "crates/storage/src/families/control/permissions/summary.rs",
    // The composed-row listings and the API-side topology enrichment (`batch::load`).
    "crates/storage/src/families/name/bound.rs",
    "crates/storage/src/families/name/list.rs",
    "crates/storage/src/families/name/seams.rs",
    "crates/storage/src/families/name/topology.rs",
    "crates/storage/src/families/records/address_names.rs",
    "crates/storage/src/families/records/assemble.rs",
    "crates/storage/src/families/records/candidates.rs",
    "crates/storage/src/families/records/facts.rs",
    "crates/storage/src/families/records/former_owners.rs",
    "crates/storage/src/families/records/inventory.rs",
    "crates/storage/src/families/records/inventory_cutoff.rs",
    "crates/storage/src/families/records/links.rs",
    "crates/storage/src/families/records/mirror.rs",
    "crates/storage/src/families/records/mod.rs",
    "crates/storage/src/families/records/payload.rs",
    "crates/storage/src/families/records/pointer.rs",
    "crates/storage/src/families/records/primary.rs",
    "crates/storage/src/families/records/profiles.rs",
    "crates/storage/src/families/records/registry_children.rs",
    "crates/storage/src/families/records/resolves_to.rs",
    "crates/storage/src/families/records/resolves_to_serving.rs",
    "crates/storage/src/families/records/reverse.rs",
    "crates/storage/src/families/records/reverse_page.rs",
    "crates/storage/src/families/records/rows.rs",
    "crates/storage/src/families/records/seams.rs",
    "crates/storage/src/families/records/serving.rs",
    "crates/storage/src/families/records/text_hydration.rs",
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
    for relative_path in COMPOSITION_FILES {
        let path = workspace_root.join(relative_path);
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "interpreter content hash requires storage family composition source \
                     {relative_path}; if it moved, update COMPOSITION_FILES in the same change"
                ),
            ));
        }
        collect_file(workspace_root, &path, inputs)?;
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
        if !COMPOSITION_FILES.contains(&relative_path.as_str())
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
