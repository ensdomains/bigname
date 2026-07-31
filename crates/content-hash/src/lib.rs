//! Stable hash of the source inputs that define interpreted and projected data.

use std::{io, path::Path};

mod compute;

include!(concat!(env!("OUT_DIR"), "/interpreter_content_hash.rs"));

/// Compute the source-input hash for a workspace tree.
pub fn interpreter_content_hash(workspace_root: impl AsRef<Path>) -> io::Result<String> {
    compute::compute(workspace_root.as_ref())
}

#[cfg(test)]
mod tests;
