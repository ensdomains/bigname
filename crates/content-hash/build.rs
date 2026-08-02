use std::{env, path::PathBuf};

#[path = "src/compute.rs"]
mod compute;
#[path = "src/source_paths.rs"]
mod source_paths;

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
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/source_paths.rs").display()
    );
    for path in compute::watched_paths(workspace_root) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let hash =
        compute::compute(workspace_root).expect("interpreter content hash must be computable");
    let profiles = manifest_profile_hashes(workspace_root);
    let mut generated = format!("pub const INTERPRETER_CONTENT_HASH: &str = {hash:?};\n");
    generated.push_str("pub const HASHED_MANIFEST_PROFILES: &[(&str, &str)] = &[\n");
    for (profile, profile_hash) in profiles {
        generated.push_str(&format!("    ({profile:?}, {profile_hash:?}),\n"));
    }
    generated.push_str("];\n");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("build output directory"));
    std::fs::write(out_dir.join("interpreter_content_hash.rs"), generated)
        .expect("interpreter content hash constant must be writable");
}

fn manifest_profile_hashes(workspace_root: &std::path::Path) -> Vec<(String, String)> {
    let manifest_root = workspace_root.join("manifests");
    let mut entries = std::fs::read_dir(&manifest_root)
        .expect("manifest root must be readable")
        .collect::<Result<Vec<_>, _>>()
        .expect("manifest profile entries must be readable");
    entries.sort_by_key(|entry| entry.file_name());
    let profiles = entries
        .into_iter()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| {
            let name = entry
                .file_name()
                .into_string()
                .expect("manifest profile names must be UTF-8");
            let hash = compute::manifest_profile_hash(&entry.path())
                .expect("manifest profile hash must be computable");
            (name, hash)
        })
        .collect::<Vec<_>>();
    assert!(!profiles.is_empty(), "manifest profiles must be present");
    profiles
}
