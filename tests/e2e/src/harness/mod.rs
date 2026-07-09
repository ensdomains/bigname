pub mod anvil;
pub mod artifacts;
pub mod db;
pub mod ens_v1;
pub mod manifests;
pub mod pipeline;
pub mod rpc;

use std::path::PathBuf;

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}
