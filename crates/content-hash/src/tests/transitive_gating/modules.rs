//! Where modules resolve outside the walk's own tree: target roots outside `src/`, optional
//! files that do not exist yet, and the modules of the files hashed by name.

use super::{SampleTree, assert_names, interpreter_content_hash, is_input, rotates};

#[test]
fn a_crate_root_outside_its_source_root_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        "crates/adapters/Cargo.toml",
        "[lib]\npath = \"lib/root.rs\"\n",
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
fn a_module_target_of_a_semantic_source_is_scanned_in_turn() {
    let tree = SampleTree::new();
    tree.write(
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
        .expect_err("an unhashed module of a scanned target must fail the hash");
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
    tree.write("crates/storage/src/identity_search.rs", "pub mod tokens;\n");
    tree.write("crates/storage/src/tokens.rs", "pub fn unrelated() {}\n");
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
    tree.write(
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
