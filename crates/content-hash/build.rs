use std::{env, path::PathBuf};

const E2E_MANIFEST_PROFILE_ENV: &str = "BIGNAME_E2E_MANIFEST_PROFILE_ROOT";
const E2E_MANIFEST_PROFILE_PREFIX: &str = ".bigname-e2e-runtime-profile-";

#[path = "src/classes.rs"]
mod classes;
#[path = "src/compute.rs"]
mod compute;
#[path = "src/lockfile.rs"]
mod lockfile;
#[path = "src/source_paths.rs"]
mod source_paths;
#[path = "src/storage_families.rs"]
mod storage_families;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("content-hash must be two directories below the workspace root");

    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/classes.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/compute.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/lockfile.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/source_paths.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/source_paths/paths.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/source_paths/syntax.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/source_paths/targets.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/source_paths/walked.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src/storage_families.rs").display()
    );
    for path in compute::watched_paths(workspace_root) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    println!("cargo:rerun-if-env-changed={E2E_MANIFEST_PROFILE_ENV}");

    let hashes = compute::compute(workspace_root).expect("content hashes must be computable");
    let mut profiles = manifest_profile_hashes(workspace_root);
    if let Some(profile) = e2e_manifest_profile_hash(workspace_root) {
        profiles.push(profile);
    }
    let mut generated = format!(
        "pub const INTERPRETER_CONTENT_HASH: &str = {:?};\n\
         pub const PROJECTION_CONTENT_HASH: &str = {:?};\n",
        hashes.interpreter, hashes.projection
    );
    generated.push_str("pub const HASHED_MANIFEST_PROFILES: &[(&str, &str)] = &[\n");
    for (profile, profile_hash) in profiles {
        generated.push_str(&format!("    ({profile:?}, {profile_hash:?}),\n"));
    }
    generated.push_str("];\n");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("build output directory"));
    std::fs::write(out_dir.join("interpreter_content_hash.rs"), generated)
        .expect("content hash constants must be writable");
    // Not compiled in: release tooling reads it to attribute a rotation to its inputs.
    std::fs::write(out_dir.join("content_hash_inputs.tsv"), hashes.inputs_tsv())
        .expect("content hash input list must be writable");
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
        .filter(|entry| {
            entry.path().is_dir()
                && !compute::is_hidden_directory_name(entry.file_name().as_os_str())
        })
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

fn e2e_manifest_profile_hash(workspace_root: &std::path::Path) -> Option<(String, String)> {
    let path = PathBuf::from(env::var_os(E2E_MANIFEST_PROFILE_ENV)?);
    let manifest_root = workspace_root.join("manifests");
    assert_eq!(
        path.parent(),
        Some(manifest_root.as_path()),
        "explicit e2e deployment-profile mirror must be directly below the manifest root"
    );
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("explicit e2e deployment-profile mirror name must be UTF-8");
    assert!(
        name.starts_with(E2E_MANIFEST_PROFILE_PREFIX),
        "explicit e2e deployment-profile mirror must use the harness prefix"
    );
    println!("cargo:rerun-if-changed={}", path.display());
    let hash = compute::manifest_profile_hash(&path)
        .expect("explicit e2e deployment-profile hash must be computable");
    Some((name.to_owned(), hash))
}
