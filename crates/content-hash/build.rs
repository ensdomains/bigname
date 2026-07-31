use std::{env, path::PathBuf};

#[path = "src/compute.rs"]
mod compute;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("content-hash must be two directories below the workspace root");

    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/compute.rs").display()
    );
    for path in compute::watched_paths(workspace_root) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let hash =
        compute::compute(workspace_root).expect("interpreter content hash must be computable");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("build output directory"));
    std::fs::write(
        out_dir.join("interpreter_content_hash.rs"),
        format!("pub const INTERPRETER_CONTENT_HASH: &str = {hash:?};\n"),
    )
    .expect("interpreter content hash constant must be writable");
}
