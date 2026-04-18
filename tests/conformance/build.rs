use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let source_path = manifest_dir.join("../../apps/api/src/main.rs");
    let source = fs::read_to_string(&source_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", source_path.display()));
    let rewritten = source.replace("\n#[cfg(test)]\nmod tests;\n", "\n");
    let out_path = PathBuf::from(env::var("OUT_DIR").expect("out dir")).join("api_main.rs");

    fs::write(&out_path, rewritten)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", out_path.display()));

    println!("cargo:rerun-if-changed={}", source_path.display());
}
