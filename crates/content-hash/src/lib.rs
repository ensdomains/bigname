//! Stable hash of the source inputs that define interpreted and projected data.

use std::{io, path::Path};

mod compute;
mod lockfile;
mod source_paths;

include!(concat!(env!("OUT_DIR"), "/interpreter_content_hash.rs"));

/// Compute the source-input hash for a workspace tree.
pub fn interpreter_content_hash(workspace_root: impl AsRef<Path>) -> io::Result<String> {
    compute::compute(workspace_root.as_ref())
}

/// Compute the deployment-manifest profile fingerprint used to bind runtime manifests to this
/// binary. The normalizer version is deliberately excluded because flag recomputation owns that
/// version transition.
pub fn manifest_profile_hash(manifest_root: impl AsRef<Path>) -> io::Result<String> {
    compute::manifest_profile_hash(manifest_root.as_ref())
}

#[cfg(test)]
mod tests;
