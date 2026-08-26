use std::{collections::BTreeSet, env, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let profiles_root = manifest_dir.join("../..").join("manifests");
    println!("cargo:rerun-if-changed={}", profiles_root.display());
    println!("cargo:rerun-if-env-changed=BIGNAME_E2E_MANIFEST_PROFILE_ROOT");

    let mut namespaces = BTreeSet::new();
    for (profile, _) in bigname_content_hash::HASHED_MANIFEST_PROFILES {
        let profile_root = profiles_root.join(profile);
        let repository =
            bigname_manifests::load_repository(&profile_root).unwrap_or_else(|error| {
                panic!(
                    "failed to load binary-approved manifest profile {}: {error:#}",
                    profile_root.display()
                )
            });
        for loaded in repository.manifests() {
            namespaces.insert((
                loaded.manifest.chain.clone(),
                loaded.manifest.namespace.clone(),
            ));
        }
    }

    let mut generated = String::from("pub const COMPILED_CHAIN_NAMESPACES: &[(&str, &str)] = &[\n");
    for (chain, namespace) in namespaces {
        generated.push_str(&format!("    ({chain:?}, {namespace:?}),\n"));
    }
    generated.push_str("];\n");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("build output directory"));
    std::fs::write(out_dir.join("compiled_chain_namespaces.rs"), generated)
        .expect("compiled chain namespaces must be writable");
}
