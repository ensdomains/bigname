//! Stable hashes of the source inputs that define interpreted and projected data.

use std::{io, path::Path};

mod classes;
mod compute;
mod lockfile;
mod source_paths;
mod storage_families;

include!(concat!(env!("OUT_DIR"), "/interpreter_content_hash.rs"));

/// Compute the interpreter content hash for a workspace tree: the inputs Interpret's persistence
/// or manifest sync can be changed by.
pub fn interpreter_content_hash(workspace_root: impl AsRef<Path>) -> io::Result<String> {
    compute::compute(workspace_root.as_ref()).map(|hashes| hashes.interpreter)
}

/// Compute the projection content hash for a workspace tree: the inputs only Project's persisted
/// output depends on, together with the interpreter content hash.
pub fn projection_content_hash(workspace_root: impl AsRef<Path>) -> io::Result<String> {
    compute::compute(workspace_root.as_ref()).map(|hashes| hashes.projection)
}

/// Every input of both hashes as `class\tkey\tkeccak256(content)` lines under a header, sorted
/// by key. The class is `interpret`, `project` or `both`.
pub fn content_hash_inputs(workspace_root: impl AsRef<Path>) -> io::Result<String> {
    compute::compute(workspace_root.as_ref()).map(|hashes| hashes.inputs_tsv())
}

/// Compute the deployment-manifest profile fingerprint used to bind runtime manifests to this
/// binary. The normalizer version is deliberately excluded because flag recomputation owns that
/// version transition.
pub fn manifest_profile_hash(manifest_root: impl AsRef<Path>) -> io::Result<String> {
    compute::manifest_profile_hash(manifest_root.as_ref())
}

#[cfg(test)]
mod tests;
