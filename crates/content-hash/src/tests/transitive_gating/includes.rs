//! How include macros reach files: each inclusion is its own visit, and production data reads
//! keep their files in the hash.

use super::{
    ADAPTERS_LIB, SampleTree, assert_names, hash_error, interpreter_content_hash, is_input, rotates,
};

const TESTS: &str = "crates/adapters/src/tests.rs";
const SHARED: &str = "crates/adapters/src/shared.rs";

#[test]
fn an_ordinary_module_visit_does_not_skip_an_inclusion_of_the_same_file() {
    // `shared.rs` is walked as `mod shared` first. Its inclusion from `via_include.rs` must still
    // be checked, or the inclusion's `leaf` route is never seen and the test route to the
    // sibling `leaf.rs` takes it out of the hash.
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "mod via_include;\nmod shared;\n#[cfg(test)]\nmod tests;\n",
    );
    tree.write(
        "crates/adapters/src/via_include.rs",
        "include!(\"shared.rs\");\n",
    );
    tree.write(SHARED, "pub mod leaf;\n");
    tree.write("crates/adapters/src/shared/leaf.rs", "fn ordinary() {}\n");
    tree.write(TESTS, "include!(\"leaf.rs\");\n");
    let sibling = "crates/adapters/src/leaf.rs";
    tree.write(sibling, "fn sibling() {}\n");
    let message = hash_error(&tree, sibling);
    assert_names(&message, &["include! target", SHARED, "mod leaf"]);
}

#[test]
fn a_production_include_str_keeps_a_test_included_file_in_the_hash() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn value() -> u8 {\n    include_str!(\"shared.rs\").trim().parse::<u8>().unwrap()\n}\n\n\
         #[cfg(test)]\nmod tests {\n    const VALUE: u8 = include!(\"shared.rs\");\n}\n",
    );
    tree.write(SHARED, "1\n");
    assert!(is_input(&tree, SHARED));
}

#[test]
fn a_production_include_bytes_read_after_the_test_inclusion_keeps_the_file_in_the_hash() {
    // `tests` is walked before `prod`, so the test inclusion reaches the file first.
    let tree = SampleTree::new();
    tree.write(ADAPTERS_LIB, "mod prod;\n#[cfg(test)]\nmod tests;\n");
    tree.write(
        "crates/adapters/src/prod.rs",
        "pub fn value() -> &'static [u8] {\n    include_bytes!(\"shared.rs\")\n}\n",
    );
    tree.write(TESTS, "const VALUE: u8 = include!(\"shared.rs\");\n");
    tree.write(SHARED, "1\n");
    assert!(is_input(&tree, SHARED));
}

#[test]
fn a_production_include_str_nested_in_a_macro_keeps_the_file_in_the_hash() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn value() -> String {\n    format!(\"{}\", include_str!(\"shared.rs\"))\n}\n\n\
         #[cfg(test)]\nmod tests;\n",
    );
    tree.write(TESTS, "const VALUE: u8 = include!(\"shared.rs\");\n");
    tree.write(SHARED, "1\n");
    assert!(is_input(&tree, SHARED));
}

#[test]
fn a_test_only_include_str_does_not_keep_a_test_module_in_the_hash() {
    let tree = SampleTree::new();
    tree.write(ADAPTERS_LIB, "#[cfg(test)]\nmod tests;\n");
    tree.write(
        TESTS,
        "mod helper;\nconst HELPER: &str = include_str!(\"tests/helper.rs\");\n",
    );
    let helper = "crates/adapters/src/tests/helper.rs";
    tree.write(helper, "fn helper() {}\n");
    assert!(!is_input(&tree, helper));
}

#[test]
fn an_include_str_with_a_computed_path_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "const DATA: &str = include_str!(concat!(env!(\"OUT_DIR\"), \"/data.txt\"));\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a computed include_str! path must fail the hash");
    assert_names(
        &error.to_string(),
        &["include_str!", "computed path", ADAPTERS_LIB],
    );
}

#[test]
fn a_macro_that_reads_a_file_as_data_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "macro_rules! data {\n    () => {\n        include_bytes!(\"shared.rs\")\n    };\n}\n",
    );
    tree.write(SHARED, "1\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("a macro calling include_bytes! must fail the hash");
    assert_names(
        &error.to_string(),
        &["macro_rules!", "data", "calls include_bytes!", ADAPTERS_LIB],
    );
}

#[test]
fn an_edit_to_an_included_sql_file_rotates_the_hash() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn query() -> &'static str {\n    include_str!(\"queries/load.sql\")\n}\n",
    );
    let sql = "crates/adapters/src/queries/load.sql";
    assert!(rotates(&tree, sql));
    assert!(is_input(&tree, sql));
}

#[test]
fn a_data_file_a_hashed_file_reads_outside_src_rotates_the_hash_and_is_watched() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn seed() -> &'static [u8] {\n    include_bytes!(\"../fixtures/seed.bin\")\n}\n",
    );
    let seed = "crates/adapters/fixtures/seed.bin";
    assert!(rotates(&tree, seed));
    let watched = crate::compute::watched_paths(tree.path());
    let file = tree.path().join(seed);
    assert!(
        watched.iter().any(|watched| file.starts_with(watched)),
        "{seed} is hashed but not watched"
    );
}

#[test]
fn a_data_file_an_unhashed_file_reads_stays_out_of_the_hash() {
    // `crates/interpret/src/load` is walked but not hashed, like interpret's input loader, so the
    // SQL it compiles in stays out with it.
    let tree = SampleTree::new();
    tree.write("crates/interpret/src/lib.rs", "mod load;\n");
    tree.write(
        "crates/interpret/src/load.rs",
        "pub fn events() -> &'static str {\n    include_str!(\"load/events.sql\")\n}\n",
    );
    let sql = "crates/interpret/src/load/events.sql";
    assert!(!rotates(&tree, sql));
    assert!(!is_input(&tree, sql));
}

#[test]
fn a_hashed_file_including_a_file_that_is_not_rust_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn value() -> u8 {\n    include!(\"value.txt\")\n}\n",
    );
    tree.write("crates/adapters/src/value.txt", "1\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an include! of an unhashed file must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            ADAPTERS_LIB,
            "crates/adapters/src/value.txt",
            "not a hashed source",
        ],
    );
}

#[test]
fn a_hashed_file_including_rust_outside_the_hashed_roots_is_refused() {
    let tree = SampleTree::new();
    tree.write(ADAPTERS_LIB, "include!(\"../shared/items.rs\");\n");
    tree.write("crates/adapters/shared/items.rs", "fn shared() {}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an include! of an unhashed file must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            ADAPTERS_LIB,
            "crates/adapters/shared/items.rs",
            "not a hashed source",
        ],
    );
}

#[test]
fn an_unhashed_file_may_include_a_file_that_is_not_rust() {
    let tree = SampleTree::new();
    tree.write("crates/interpret/src/lib.rs", "mod load;\n");
    tree.write(
        "crates/interpret/src/load.rs",
        "pub fn value() -> u8 {\n    include!(\"load/value.txt\")\n}\n",
    );
    tree.write("crates/interpret/src/load/value.txt", "1\n");
    interpreter_content_hash(tree.path()).expect("an unhashed file's include! is not hashed");
}

#[test]
fn a_hashed_file_reading_data_outside_the_workspace_is_refused() {
    let tree = SampleTree::new();
    let workspace = tree.path();
    let name = format!(
        "{}-outside.txt",
        workspace
            .file_name()
            .expect("tree directory")
            .to_string_lossy()
    );
    let outside = workspace.parent().expect("tree parent").join(&name);
    std::fs::write(&outside, "1\n").expect("write outside file");
    tree.write(
        ADAPTERS_LIB,
        &format!(
            "pub fn data() -> &'static str {{\n    include_str!(\"../../../../{name}\")\n}}\n"
        ),
    );
    let result = interpreter_content_hash(workspace);
    std::fs::remove_file(&outside).expect("remove outside file");
    let error = result.expect_err("a data read outside the workspace must fail the hash");
    assert_names(&error.to_string(), &[&name, "outside workspace root"]);
}

#[test]
fn a_data_file_a_semantic_source_reads_rotates_the_hash_and_is_watched() {
    // `text_records.rs` is hashed by name, outside the hashed roots, and still reads data like
    // any hashed file.
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/text_records.rs",
        "pub const MULTICALL3_ADDRESS: &str = include_str!(\"multicall3.txt\");\n",
    );
    let data = "crates/lookup/src/multicall3.txt";
    assert!(rotates(&tree, data));
    let watched = crate::compute::watched_paths(tree.path());
    let file = tree.path().join(data);
    assert!(
        watched.iter().any(|watched| file.starts_with(watched)),
        "{data} is hashed but not watched"
    );
}

#[test]
fn a_semantic_source_including_an_unhashed_file_is_refused() {
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "include!(\"abi_tables.rs\");\n",
    );
    tree.write("crates/lookup/src/abi_tables.rs", "fn tables() {}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an include! of an unhashed file must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "crates/lookup/src/abi.rs",
            "crates/lookup/src/abi_tables.rs",
            "not a hashed source",
        ],
    );
}

#[test]
fn a_data_file_read_by_a_target_a_semantic_source_includes_rotates_the_hash_and_is_watched() {
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "include!(\"../../adapters/src/undeclared_tables.rs\");\n",
    );
    tree.write(
        "crates/adapters/src/undeclared_tables.rs",
        "const TABLES: &str = include_str!(\"tables.txt\");\n",
    );
    let data = "crates/adapters/src/tables.txt";
    assert!(rotates(&tree, data));
    let watched = crate::compute::watched_paths(tree.path());
    let file = tree.path().join(data);
    assert!(
        watched.iter().any(|watched| file.starts_with(watched)),
        "{data} is hashed but not watched"
    );
}

#[test]
fn a_macro_invocation_declaring_a_module_in_a_semantic_source_is_refused() {
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "my_macro! {\n    mod decl;\n}\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a macro invocation declaring a module must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "my_macro!",
            "crates/lookup/src/abi.rs",
            "may declare a module",
        ],
    );
}

#[test]
fn an_include_str_with_a_trailing_comma_is_followed() {
    let walked = super::adapters_tree("const T: &str = include_str!(\"table.txt\",);\n");
    let semantic = SampleTree::new();
    super::write_mounted(
        &semantic,
        "crates/lookup/src/abi.rs",
        "const T: &str = include_str!(\"table.txt\",);\n",
    );
    for (tree, data) in [
        (&walked, "crates/adapters/src/table.txt"),
        (&semantic, "crates/lookup/src/table.txt"),
    ] {
        assert!(rotates(tree, data), "{data} must rotate the hash");
        assert!(is_input(tree, data), "{data} must be hashed");
    }
}

#[test]
fn a_semantic_source_may_include_an_absent_file_under_a_cfg_that_may_be_off() {
    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "#[cfg(any())]\nmod disabled {\n    include!(\"absent.rs\");\n}\n",
    );
    interpreter_content_hash(tree.path()).expect("an optional absent include target must hash");

    // A present target under the same cfg is still walked and held to the hashed-source rule.
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "#[cfg(feature = \"x\")]\nmod enabled {\n    include!(\"abi_tables.rs\");\n}\n",
    );
    tree.write("crates/lookup/src/abi_tables.rs", "const T: u8 = 1;\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("a present unhashed include target must fail the hash");
    assert_names(
        &error.to_string(),
        &[
            "crates/lookup/src/abi.rs",
            "crates/lookup/src/abi_tables.rs",
        ],
    );
}
