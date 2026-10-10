//! Module declarations whose files the hash must cover whoever declares them, files hashed by
//! name that production never reaches, and absent paths the build cannot watch for.

use std::path::{Path, PathBuf};

use super::{SampleTree, assert_names, interpreter_content_hash};

/// Replaces `from` with `to` in a sample file, which must hold it.
fn replace(tree: &SampleTree, relative_path: &str, from: &str, to: &str) {
    let path = tree.path().join(relative_path);
    let contents = std::fs::read_to_string(&path).expect("sample file must be readable");
    assert!(
        contents.contains(from),
        "{relative_path} must hold {from:?}"
    );
    std::fs::write(path, contents.replace(from, to)).expect("sample file must be writable");
}

/// Expects the hash to fail with a message naming each of `names`.
fn refused(tree: &SampleTree, names: &[&str]) {
    let error = interpreter_content_hash(tree.path())
        .expect_err("the shape must fail the hash")
        .to_string();
    assert_names(&error, names);
}

const ADAPTERS_LIB: &str = "crates/adapters/src/lib.rs";
const LOOKUP_LIB: &str = "crates/lookup/src/lib.rs";
const STORAGE_LIB: &str = "crates/storage/src/lib.rs";

#[test]
fn a_semantic_module_redirected_by_path_is_refused() {
    let tree = SampleTree::new();
    replace(
        &tree,
        LOOKUP_LIB,
        "pub mod abi;\n",
        "#[path = \"abi_alt.rs\"]\npub mod abi;\n",
    );
    tree.write(
        "crates/lookup/src/abi_alt.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &["mod abi", LOOKUP_LIB, "crates/lookup/src/abi_alt.rs"],
    );
}

#[test]
fn a_semantic_module_with_a_cfg_attr_alternate_is_refused() {
    // Codex's shape: an unhashed crate root gives a module hashed by name an alternate file.
    let tree = SampleTree::new();
    replace(
        &tree,
        LOOKUP_LIB,
        "pub mod abi;\n",
        "#[cfg_attr(feature = \"alt\", path = \"abi_alt.rs\")]\npub mod abi;\n",
    );
    tree.write(
        "crates/lookup/src/abi_alt.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &["mod abi", LOOKUP_LIB, "crates/lookup/src/abi_alt.rs"],
    );
}

#[test]
fn nested_and_wider_cfg_attr_alternates_are_refused_and_a_test_one_is_not() {
    for (attribute, refuses) in [
        (
            "#[cfg_attr(feature = \"alt\", cfg_attr(feature = \"more\", path = \"abi_alt.rs\"))]",
            true,
        ),
        (
            "#[cfg_attr(any(test, feature = \"alt\"), path = \"abi_alt.rs\")]",
            true,
        ),
        ("#[cfg_attr(test, path = \"abi_alt.rs\")]", false),
    ] {
        let tree = SampleTree::new();
        replace(
            &tree,
            LOOKUP_LIB,
            "pub mod abi;\n",
            &format!("{attribute}\npub mod abi;\n"),
        );
        tree.write(
            "crates/lookup/src/abi_alt.rs",
            "pub fn semantics() -> bool { false }\n",
        );
        if refuses {
            refused(&tree, &["mod abi", "crates/lookup/src/abi_alt.rs"]);
        } else {
            interpreter_content_hash(tree.path()).expect("a test-only alternate must hash");
        }
    }
}

#[test]
fn an_unhashed_default_beside_a_semantic_cfg_attr_path_is_refused() {
    let tree = SampleTree::new();
    let mut lib = std::fs::read_to_string(tree.path().join(LOOKUP_LIB)).unwrap();
    lib.push_str("#[cfg_attr(feature = \"alt\", path = \"abi.rs\")]\nmod abi_impl;\n");
    tree.write(LOOKUP_LIB, &lib);
    tree.write(
        "crates/lookup/src/abi_impl.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &["mod abi_impl", LOOKUP_LIB, "crates/lookup/src/abi_impl.rs"],
    );
}

#[test]
fn a_semantic_module_kept_under_another_name_is_still_refused_when_redirected() {
    let tree = SampleTree::new();
    replace(
        &tree,
        LOOKUP_LIB,
        "pub mod abi;\n",
        "#[path = \"abi.rs\"]\nmod abi_orig;\n#[path = \"abi_alt.rs\"]\npub mod abi;\n",
    );
    tree.write(
        "crates/lookup/src/abi_alt.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &["mod abi", LOOKUP_LIB, "crates/lookup/src/abi_alt.rs"],
    );
}

#[test]
fn a_child_a_path_file_moves_from_a_semantic_default_is_refused() {
    // `row` resolves beside the moved file, but unmoved it would be the hashed
    // `name_current/row.rs`.
    let tree = SampleTree::new();
    replace(
        &tree,
        STORAGE_LIB,
        "pub mod name_current;\n",
        "#[path = \"moved/name_current.rs\"]\npub mod name_current;\n",
    );
    tree.write("crates/storage/src/moved/name_current.rs", "pub mod row;\n");
    tree.write(
        "crates/storage/src/moved/row.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &[
            "mod row",
            "crates/storage/src/moved/name_current.rs",
            "crates/storage/src/moved/row.rs",
        ],
    );
}

#[test]
fn a_child_an_inline_path_moves_from_a_semantic_default_is_refused() {
    let tree = SampleTree::new();
    replace(
        &tree,
        STORAGE_LIB,
        "pub mod name_current;\n",
        "#[path = \"moved\"]\npub mod name_current {\n    pub mod row;\n}\n",
    );
    tree.write(
        "crates/storage/src/moved/row.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &["mod row", STORAGE_LIB, "crates/storage/src/moved/row.rs"],
    );
}

#[test]
fn a_path_beside_a_non_mod_rs_file_away_from_its_semantic_default_is_refused() {
    // `query`'s default is the hashed `address_names/query.rs` in the stem directory, but a
    // `#[path]` in `address_names.rs` resolves beside the file.
    let tree = SampleTree::new();
    replace(
        &tree,
        "crates/storage/src/address_names.rs",
        "pub mod query;\n",
        "#[path = \"query_alt.rs\"]\npub mod query;\n",
    );
    tree.write(
        "crates/storage/src/query_alt.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &[
            "mod query",
            "crates/storage/src/address_names.rs",
            "crates/storage/src/query_alt.rs",
        ],
    );
}

#[test]
fn a_path_in_a_moved_directory_away_from_its_semantic_default_is_refused() {
    // The inline module moves into `history/`, where the default `source.rs` is hashed by name.
    let tree = SampleTree::new();
    let mut lib = std::fs::read_to_string(tree.path().join(STORAGE_LIB)).unwrap();
    lib.push_str(
        "#[path = \"history\"]\nmod moved_history {\n    #[path = \"z.rs\"]\n    pub mod source;\n}\n",
    );
    tree.write(STORAGE_LIB, &lib);
    tree.write(
        "crates/storage/src/history/z.rs",
        "pub fn semantics() -> bool { false }\n",
    );
    refused(
        &tree,
        &["mod source", STORAGE_LIB, "crates/storage/src/history/z.rs"],
    );
}

#[test]
fn a_file_hashed_by_name_that_production_never_reaches_is_refused() {
    let tree = SampleTree::new();
    replace(
        &tree,
        LOOKUP_LIB,
        "pub mod abi;\n",
        "#[cfg(test)]\npub mod abi;\n",
    );
    refused(
        &tree,
        &["crates/lookup/src/abi.rs", "only test code reaches it"],
    );

    replace(&tree, LOOKUP_LIB, "#[cfg(test)]\npub mod abi;\n", "");
    refused(
        &tree,
        &["crates/lookup/src/abi.rs", "no module route reaches it"],
    );
}

#[test]
fn an_absent_path_the_build_cannot_watch_is_refused() {
    // Each would be watched through the workspace root, which holds the build's own output, or
    // through a directory outside the workspace.
    for (lib, generated, path) in [
        (
            "#[cfg(feature = \"x\")]\nmod generated;\n",
            "include!(\"../../../generated.rs\");\n",
            "generated.rs",
        ),
        (
            "#[cfg(feature = \"x\")]\nmod generated;\n",
            "include!(\"/nonexistent/x.rs\");\n",
            "/nonexistent/x.rs",
        ),
        (
            "#[cfg_attr(feature = \"x\", path = \"../../../alt.rs\")]\nmod alt;\n",
            "",
            "alt.rs",
        ),
        (
            "#[cfg(feature = \"x\")]\n#[path = \"../../../p.rs\"]\nmod p;\n",
            "",
            "p.rs",
        ),
        (
            "#[cfg(feature = \"x\")]\n#[path = \"../../../q\"]\nmod q {\n    mod r;\n}\n",
            "",
            "q/r",
        ),
        (
            "const DATA: &str = include_str!(\"../../../data.txt\");\n",
            "",
            "data.txt",
        ),
    ] {
        let tree = SampleTree::new();
        tree.write(
            "crates/adapters/src/lib.rs",
            &format!("pub fn interpret() -> bool {{ true }}\n{lib}"),
        );
        let declarer = if generated.is_empty() {
            "crates/adapters/src/lib.rs"
        } else {
            tree.write("crates/adapters/src/generated.rs", generated);
            "crates/adapters/src/generated.rs"
        };
        refused(&tree, &[declarer, path, "cannot watch"]);
    }
}

#[test]
fn a_second_route_to_a_declaring_file_is_held_to_the_slot_of_the_first() {
    // Through `nc2`, `name_current.rs` is a `#[path]` file, so its children resolve in `src/`.
    // The first route makes each of them a slot, so the second may not reach unhashed files.
    let tree = SampleTree::new();
    let mut lib = std::fs::read_to_string(tree.path().join(STORAGE_LIB)).unwrap();
    lib.push_str("#[path = \"name_current.rs\"]\nmod nc2;\n");
    tree.write(STORAGE_LIB, &lib);
    for child in ["public_authority", "row", "wrapper_expiry"] {
        tree.write(
            &format!("crates/storage/src/{child}.rs"),
            "pub fn served() {}\n",
        );
    }
    refused(
        &tree,
        &[
            "mod public_authority",
            "crates/storage/src/name_current.rs",
            "crates/storage/src/public_authority.rs",
        ],
    );
}

#[test]
fn a_parent_directory_at_the_root_folds_to_the_root() {
    let normal = crate::source_paths::lexically_normal;
    assert_eq!(normal(Path::new("/a/../../b/./c")), Path::new("/b/c"));
    assert_eq!(normal(Path::new("/..")), Path::new("/"));
}

/// The watched paths a build with its output under the tree's `target/` would emit.
fn guarded(tree: &SampleTree) -> std::io::Result<Vec<PathBuf>> {
    let target = tree.path().join("target");
    let out_dir = target.join("debug/build/bigname-content-hash-0/out");
    crate::compute::guarded_watched_paths(tree.path(), &out_dir, &target)
}

#[test]
fn an_absent_path_watched_through_the_target_directory_is_refused() {
    let tree = SampleTree::new();
    tree.write("target/debug/.cargo-lock", "");
    let mut lib = std::fs::read_to_string(tree.path().join(ADAPTERS_LIB)).unwrap();
    lib.push_str("#[cfg(any())]\n#[path = \"../../../target/gen.rs\"]\nmod generated;\n");
    tree.write(ADAPTERS_LIB, &lib);
    interpreter_content_hash(tree.path()).expect("an optional absent module must hash");
    let error = guarded(&tree).expect_err("a watch in target/ must fail the build");
    assert_names(&error.to_string(), &["target", "the absent", "gen.rs"]);

    // With the target directory elsewhere, a watch holding `OUT_DIR` is refused all the same.
    let out_dir = tree
        .path()
        .join("target/debug/build/bigname-content-hash-0/out");
    let elsewhere = tree.path().join("elsewhere");
    let error = crate::compute::guarded_watched_paths(tree.path(), &out_dir, &elsewhere)
        .expect_err("a watch holding OUT_DIR must fail the build");
    assert_names(&error.to_string(), &["target", "gen.rs"]);
}

#[test]
fn a_watch_in_a_target_directory_known_only_from_out_dir_is_refused() {
    // The build's output is under `custom/`, set only in Cargo's config, so neither
    // `CARGO_TARGET_DIR` nor `<root>/target` names it, and `custom/x` holds no `OUT_DIR`.
    let tree = SampleTree::new();
    tree.write("custom/x/.keep", "");
    let mut lib = std::fs::read_to_string(tree.path().join(ADAPTERS_LIB)).unwrap();
    lib.push_str("#[cfg(any())]\n#[path = \"../../../custom/x/gen.rs\"]\nmod generated;\n");
    tree.write(ADAPTERS_LIB, &lib);
    let out_dir = tree
        .path()
        .join("custom/debug/build/bigname-content-hash-0/out");
    let target = tree.path().join("target");
    let error = crate::compute::guarded_watched_paths(tree.path(), &out_dir, &target)
        .expect_err("a watch in the derived target directory must fail the build");
    assert_names(&error.to_string(), &["custom/x", "the absent", "gen.rs"]);

    // Without a `build` component in `OUT_DIR`, the other rules still apply and nothing fails.
    let plain = tree.path().join("custom/out");
    crate::compute::guarded_watched_paths(tree.path(), &plain, &target)
        .expect("with no build component the target cannot be derived");
}

#[test]
fn a_present_data_read_is_watched_as_its_file_only() {
    let tree = SampleTree::new();
    tree.write("Cargo.toml", "[workspace]\n");
    let mut lib = std::fs::read_to_string(tree.path().join(STORAGE_LIB)).unwrap();
    lib.push_str("const MANIFEST: &str = include_str!(\"../../../Cargo.toml\");\n");
    tree.write(STORAGE_LIB, &lib);
    let watched = guarded(&tree).expect("a data read's file is safe to watch");
    assert!(watched.contains(&tree.path().join("Cargo.toml")));
    assert!(!watched.iter().any(|path| path == tree.path()));
}

#[test]
fn a_test_only_module_at_the_root_is_refused_as_a_watch() {
    let tree = SampleTree::new();
    tree.write("root.rs", "pub fn root() {}\n");
    let mut lib = std::fs::read_to_string(tree.path().join(ADAPTERS_LIB)).unwrap();
    lib.push_str("#[cfg(test)]\n#[path = \"../../../root.rs\"]\nmod root;\n");
    tree.write(ADAPTERS_LIB, &lib);
    interpreter_content_hash(tree.path()).expect("a test-only module must hash");
    let error = guarded(&tree).expect_err("watching the workspace root must fail the build");
    assert_names(&error.to_string(), &["the directory of walked", "root.rs"]);
}
