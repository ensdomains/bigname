//! Where modules resolve outside the walk's own tree: target roots outside `src/`, optional
//! files that do not exist yet, and the modules of the files hashed by name.

use super::{SampleTree, assert_names, interpreter_content_hash, is_input, rotates};

#[test]
fn a_crate_root_outside_its_source_root_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        "crates/adapters/Cargo.toml",
        "[package]\nbuild = false\n\n[lib]\npath = \"lib/root.rs\"\n",
    );
    tree.write(
        "crates/adapters/lib/root.rs",
        "pub fn interpret() -> bool { true }\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a crate root outside the hashed sources must fail the hash");
    assert_names(
        &error.to_string(),
        &["crates/adapters/lib/root.rs", "crate root"],
    );
}

#[test]
fn a_walked_crate_must_turn_build_scripts_off() {
    let tree = SampleTree::new();
    tree.write(
        "crates/storage/src/gen.rs",
        "fn main() { println!(\"cargo:rustc-cfg=alt\"); }\n",
    );
    // Any walked crate, hashed or not, and a build script inside `src/` too.
    for (krate, manifest) in [
        ("adapters", "[package]\nname = \"adapters\"\n"),
        ("adapters", "[package]\nbuild = true\n"),
        ("adapters", "[package]\nbuild = \"tools/gen.rs\"\n"),
        ("adapters", "[package]\nbuild = [\"gen_a.rs\"]\n"),
        ("storage", "[package]\nbuild = \"src/gen.rs\"\n"),
    ] {
        let path = format!("crates/{krate}/Cargo.toml");
        tree.write(&path, manifest);
        let error = interpreter_content_hash(tree.path())
            .expect_err("a walked crate that may run a build script must fail the hash");
        assert_names(&error.to_string(), &[&path, "package.build = false"]);
        tree.write(&path, "[package]\nbuild = false\n");
    }

    // With `build = false` Cargo never runs a `build.rs`, so a file only it and test code
    // declare stays test-only.
    tree.write("crates/adapters/Cargo.toml", "[package]\nbuild = false\n");
    let lib = "pub fn interpret() -> bool { true }\n#[cfg(test)]\nmod gated;\n";
    tree.write("crates/adapters/src/lib.rs", lib);
    tree.write("crates/adapters/src/gated.rs", "pub fn gated() {}\n");
    tree.write(
        "crates/adapters/build.rs",
        "#[path = \"src/gated.rs\"]\nmod gated;\nfn main() {}\n",
    );
    interpreter_content_hash(tree.path()).expect("a crate with build scripts off must hash");
    assert!(!is_input(&tree, "crates/adapters/src/gated.rs"));
}

#[test]
fn a_walked_source_root_without_a_manifest_is_refused() {
    let tree = SampleTree::new();
    std::fs::remove_file(tree.path().join("crates/adapters/Cargo.toml"))
        .expect("sample manifest must be removable");
    let error = interpreter_content_hash(tree.path())
        .expect_err("a walked src/ without a manifest must fail the hash");
    assert_names(&error.to_string(), &["crates/adapters/src", "Cargo.toml"]);
}

#[test]
fn a_walked_file_that_is_not_text_is_named() {
    let tree = SampleTree::new();
    super::write_mounted(&tree, "crates/lookup/src/abi.rs", "mod tables;\n");
    tree.write("crates/lookup/src/abi/tables.rs", "");
    std::fs::write(
        tree.path().join("crates/lookup/src/abi/tables.rs"),
        [0xff, 0xfe],
    )
    .expect("sample file must be writable");
    let error = interpreter_content_hash(tree.path())
        .expect_err("a file that is not UTF-8 must fail the hash");
    assert_names(&error.to_string(), &["crates/lookup/src/abi/tables.rs"]);
}

#[test]
fn the_directory_of_an_absent_default_module_is_watched() {
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "#[path = \"../optional\"]\nmod implementation {\n    #[cfg_attr(feature = \"alt\", \
         path = \"../../adapters/src/alt.rs\")]\n    mod tables;\n}\n",
    );
    tree.write("crates/lookup/optional/README", "not a module\n");
    tree.write("crates/adapters/src/alt.rs", "pub fn alt() {}\n");
    let watched = crate::compute::watched_paths(tree.path());
    let directory = tree.path().join("crates/lookup/optional");
    assert!(
        watched.contains(&directory),
        "{} is not watched",
        directory.display()
    );
}

/// Gives `krate` a `[[bin]]` at `src/tool.txt`, which is not Rust.
fn text_binary(tree: &SampleTree, krate: &str) {
    tree.write(
        &format!("crates/{krate}/Cargo.toml"),
        "[package]\nbuild = false\n\n[[bin]]\nname = \"tool\"\npath = \"src/tool.txt\"\n",
    );
    tree.write(&format!("crates/{krate}/src/tool.txt"), "fn main() {}\n");
}

#[test]
fn a_root_that_is_not_rust_is_refused_when_an_include_reaches_it_first() {
    // Lookup is the last seed, after the hashed crates and the other crates of files hashed by
    // name, so its roots pop first and its `include!` is the first route to adapters' binary.
    // Its `lib.rs` is not hashed, so the include itself is not refused.
    let tree = SampleTree::new();
    text_binary(&tree, "adapters");
    tree.write(
        "crates/lookup/src/lib.rs",
        "include!(\"../../adapters/src/tool.txt\");\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a root that is not Rust must fail the hash whatever reaches it first");
    assert_names(
        &error.to_string(),
        &[
            "crates/adapters/src/tool.txt",
            "or is not Rust",
            "crates/lookup/src/lib.rs (include!(",
        ],
    );
}

#[test]
fn a_root_that_is_not_rust_is_refused_when_an_include_reaches_it_later() {
    // Lookup's roots pop before storage's, so its binary is reached as a crate root first, and
    // storage's unhashed `lib.rs` includes it later.
    let tree = SampleTree::new();
    text_binary(&tree, "lookup");
    tree.write(
        "crates/storage/src/lib.rs",
        "include!(\"../../lookup/src/tool.txt\");\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a root that is not Rust must fail the hash whatever reaches it later");
    assert_names(
        &error.to_string(),
        &[
            "crates/lookup/src/tool.txt",
            "or is not Rust",
            "crates/lookup/src/tool.txt (crate root)",
        ],
    );
}

#[test]
fn the_directory_of_an_absent_optional_include_is_watched() {
    let tree = SampleTree::new();
    tree.write(
        "crates/adapters/src/lib.rs",
        "pub fn interpret() -> bool { true }\n#[cfg(feature = \"x\")]\nmod generated;\n",
    );
    tree.write(
        "crates/adapters/src/generated.rs",
        "include!(\"../generated/table.rs\");\n",
    );
    interpreter_content_hash(tree.path()).expect("an optional absent include target must hash");
    // The target would sit in the crate's `generated/`, so the crate directory is watched.
    let watched = crate::compute::watched_paths(tree.path());
    let directory = tree.path().join("crates/adapters");
    assert!(
        watched.contains(&directory),
        "{} is not watched",
        directory.display()
    );
}

#[test]
fn a_manifest_target_that_is_not_rust_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        "crates/adapters/Cargo.toml",
        "[package]\nbuild = false\n\n[lib]\npath = \"src/root.txt\"\n",
    );
    tree.write(
        "crates/adapters/src/root.txt",
        "pub fn interpret() -> bool { true }\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a crate root that is not a hashed source must fail the hash");
    assert_names(
        &error.to_string(),
        &["crates/adapters/src/root.txt", "crate root"],
    );
}

#[test]
fn the_directory_of_an_absent_optional_path_is_watched() {
    let tree = super::adapters_tree(
        "#[cfg_attr(feature = \"alt\", path = \"../../shared/imp.rs\")]\nmod imp;\n",
    );
    tree.write("crates/adapters/src/schema_v2/imp.rs", "pub fn imp() {}\n");
    tree.write("crates/shared/README", "not a module\n");
    let watched = crate::compute::watched_paths(tree.path());
    let directory = tree.path().join("crates/shared");
    assert!(
        watched.contains(&directory),
        "{} is not watched",
        directory.display()
    );
}

#[test]
fn a_module_target_of_a_semantic_source_is_walked_in_turn() {
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "#[path = \"../../adapters/src/undeclared.rs\"]\nmod tables;\n",
    );
    tree.write(
        "crates/adapters/src/undeclared.rs",
        "const TABLES: &str = include_str!(\"tables.txt\");\n",
    );
    let data = "crates/adapters/src/tables.txt";
    assert!(rotates(&tree, data), "{data} must rotate the hash");
    let watched = crate::compute::watched_paths(tree.path());
    let file = tree.path().join(data);
    assert!(watched.iter().any(|watched| file.starts_with(watched)));

    // Its own modules are held to the same rule.
    tree.write(
        "crates/adapters/src/undeclared.rs",
        "#[path = \"../outside.rs\"]\nmod outside;\n",
    );
    tree.write("crates/adapters/outside.rs", "pub fn outside() {}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an unhashed module of a walked target must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "mod outside",
            "crates/adapters/src/undeclared.rs",
            "crates/adapters/outside.rs",
        ],
    );
}

#[test]
fn a_module_of_a_semantic_source_resolves_only_where_rustc_looks() {
    // `identity_search.rs` owns `identity_search/`, so an unrelated `tokens.rs` beside it is not
    // its `tokens` module.
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/storage/src/identity_search.rs",
        "pub mod tokens;\n",
    );
    tree.write_keeping_mounts("crates/storage/src/tokens.rs", "pub fn unrelated() {}\n");
    interpreter_content_hash(tree.path()).expect("an unrelated sibling must not fail the hash");
    assert!(is_input(
        &tree,
        "crates/storage/src/identity_search/tokens.rs"
    ));
}

#[test]
fn an_absent_path_is_watched_through_its_nearest_existing_directory() {
    // A watched path that does not exist reruns the build script on every build.
    let tree = super::adapters_tree(
        "#[cfg_attr(feature = \"alt\", path = \"../../shared/deep/imp.rs\")]\nmod imp;\n\
         const DATA: &str = include_str!(\"../../data/absent/table.txt\");\n",
    );
    tree.write("crates/adapters/src/schema_v2/imp.rs", "pub fn imp() {}\n");
    let watched = crate::compute::watched_paths(tree.path());
    for absent in [
        "crates/shared/deep",
        "crates/shared",
        "crates/data/absent/table.txt",
        "crates/data/absent",
        "crates/data",
    ] {
        assert!(
            !watched.contains(&tree.path().join(absent)),
            "{absent} is watched"
        );
    }
    assert!(watched.contains(&tree.path().join("crates")));
}

#[test]
fn the_directory_of_an_absent_optional_path_of_a_semantic_source_is_watched() {
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/storage/src/identity_search.rs",
        "#[cfg_attr(feature = \"alt\", path = \"../alt/tokens.rs\")]\npub mod tokens;\n",
    );
    tree.write("crates/storage/alt/README", "not a module\n");
    let watched = crate::compute::watched_paths(tree.path());
    let directory = tree.path().join("crates/storage/alt");
    assert!(
        watched.contains(&directory),
        "{} is not watched",
        directory.display()
    );
}

#[test]
fn a_cfg_attr_path_on_an_inline_module_of_a_semantic_source_is_refused() {
    // The default directory holds a hashed child, but a feature build would use `alt_dir`.
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/storage/src/identity_search.rs",
        "#[cfg_attr(feature = \"alt\", path = \"alt_dir\")]\n#[path = \"identity_search\"]\n\
         mod inline {\n    mod tokens;\n}\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a cfg_attr path on an inline module must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "unsupported cfg_attr path on inline module inline",
            "crates/storage/src/identity_search.rs",
        ],
    );
}

#[test]
fn a_semantic_source_walked_as_a_module_is_checked_again_when_included() {
    // `identity_search.rs` is walked first as a module, where `tokens` resolves under
    // `identity_search/`. Included into `expiry.rs`, the same `mod tokens;` would resolve beside
    // the includer instead, so the inclusion must still be refused.
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/storage/src/identity_search.rs",
        "pub mod tokens;\n",
    );
    super::write_mounted(
        &tree,
        "crates/storage/src/expiry.rs",
        "include!(\"identity_search.rs\");\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("an included file declaring a module must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "crates/storage/src/identity_search.rs",
            "declares mod tokens",
        ],
    );
}

#[test]
fn a_semantic_source_reached_by_path_resolves_its_modules_beside_it() {
    // rustc treats a `#[path]` file like a `mod.rs`, so `tokens` is the sibling `src/tokens.rs`,
    // not the hashed `src/identity_search/tokens.rs`.
    let tree = SampleTree::new();
    tree.write(
        "crates/storage/src/lib.rs",
        "#[path = \"identity_search.rs\"]\npub mod identity_search;\n",
    );
    tree.write("crates/storage/src/identity_search.rs", "pub mod tokens;\n");
    tree.write("crates/storage/src/tokens.rs", "pub fn compiled() {}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("the compiled unhashed sibling must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "mod tokens",
            "crates/storage/src/identity_search.rs",
            "crates/storage/src/tokens.rs",
        ],
    );
}

#[test]
fn a_semantic_source_reached_by_a_second_route_is_resolved_on_each() {
    // Through `again`, `identity_search.rs` is a `#[path]` file, so its `tokens` is `src/tokens.rs`.
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/storage/src/identity_search.rs",
        "pub mod tokens;\n",
    );
    super::write_mounted(
        &tree,
        "crates/storage/src/label_preimages.rs",
        "#[path = \"identity_search.rs\"]\nmod again;\n",
    );
    tree.write("crates/storage/src/tokens.rs", "pub fn compiled() {}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("the second route's unhashed child must fail the hash");
    assert_names(
        &error.to_string(),
        &["mod tokens", "crates/storage/src/tokens.rs"],
    );
}

#[test]
fn an_out_of_line_module_under_a_cfg_that_may_be_off_may_include_an_absent_file() {
    let tree = SampleTree::new();
    tree.write_keeping_mounts("crates/interpret/src/lib.rs", "mod write;\n");
    tree.write_keeping_mounts(
        "crates/interpret/src/write.rs",
        "#[cfg(any())]\nmod optional;\n",
    );
    tree.write_keeping_mounts(
        "crates/interpret/src/write/optional.rs",
        "include!(\"absent.rs\");\n",
    );
    interpreter_content_hash(tree.path()).expect("an optional absent include target must hash");

    // A present target that is not a hashed source is still refused.
    tree.write_keeping_mounts(
        "crates/interpret/src/write/optional.rs",
        "include!(\"table.txt\");\n",
    );
    tree.write_keeping_mounts("crates/interpret/src/write/table.txt", "1\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("a present unhashed include target must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "crates/interpret/src/write/optional.rs",
            "crates/interpret/src/write/table.txt",
        ],
    );
}

#[test]
fn a_file_hashed_by_name_outside_a_crate_src_is_refused() {
    let tree = SampleTree::new();
    let error = match crate::source_paths::walk_crates(tree.path(), &[], &["tools/semantics.rs"]) {
        Ok(_) => panic!("a file hashed by name outside a crate's src must fail the walk"),
        Err(error) => error,
    };
    assert_names(&error.to_string(), &["tools/semantics.rs", "src/"]);
}
