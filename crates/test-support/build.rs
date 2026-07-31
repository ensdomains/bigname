use std::{env, path::PathBuf};

#[path = "src/interpreter_content_hash_impl.rs"]
mod interpreter_content_hash_impl;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("test-support must be two directories below the workspace root");

    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir
            .join("src/interpreter_content_hash_impl.rs")
            .display()
    );
    for path in interpreter_content_hash_impl::watched_paths(workspace_root) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let hash = interpreter_content_hash_impl::compute(workspace_root)
        .expect("interpreter content hash must be computable");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("build output directory"));
    std::fs::write(
        out_dir.join("interpreter_content_hash.rs"),
        format!("pub const INTERPRETER_CONTENT_HASH: &str = {hash:?};\n"),
    )
    .expect("interpreter content hash constant must be writable");
}
