//! The shapes the walk refuses rather than guess, and the files it must parse.

use super::interpreter_content_hash;
use super::{ADAPTERS_LIB, SampleTree, adapters_tree, assert_names, hash_error, is_input};

#[test]
fn a_walked_file_without_module_text_must_parse() {
    let tree = adapters_tree("mod present;\n");
    tree.write("crates/adapters/src/schema_v2/present.rs", "fn broken( {\n");
    let error = interpreter_content_hash(tree.path()).expect_err("every walked file must parse");
    assert!(
        error.to_string().contains("could not parse"),
        "unexpected error: {error}"
    );
}

#[test]
fn an_unparseable_production_file_behind_a_feature_fails_the_hash() {
    let tree = adapters_tree("#[cfg(feature = \"reth\")]\nmod present;\n");
    tree.write("crates/adapters/src/schema_v2/present.rs", "mod broken {\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an unparseable production module must fail the hash");
    assert!(
        error.to_string().contains("could not parse"),
        "unexpected error: {error}"
    );
}

#[test]
fn a_cfg_attr_path_on_an_inline_module_is_refused() {
    let tree = adapters_tree("#[cfg_attr(test, path = \"other\")]\nmod inline {\n    mod x;\n}\n");
    tree.write("crates/adapters/src/schema_v2/inline/x.rs", "fn x() {}\n");
    tree.write("crates/adapters/src/other/x.rs", "fn x() {}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an inline cfg_attr path must fail the hash");
    assert_names(
        &error.to_string(),
        &["cfg_attr", "inline", "crates/adapters/src/schema_v2.rs"],
    );
}

#[test]
fn an_include_with_a_computed_path_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "include!(concat!(env!(\"OUT_DIR\"), \"/generated.rs\"));\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a computed include! path must fail the hash");
    assert_names(&error.to_string(), &["include!", ADAPTERS_LIB]);
}

#[test]
fn a_macro_that_declares_a_module_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "macro_rules! mount {\n    ($name:ident) => {\n        mod $name;\n    };\n}\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a macro declaring a module must fail the hash");
    assert_names(&error.to_string(), &["macro_rules!", "mount", ADAPTERS_LIB]);
}

#[test]
fn a_macro_invocation_that_declares_a_module_is_refused() {
    // `items! { mod shared; }` emits a module the walk cannot resolve, so a test route to the same
    // file must not be enough to drop it from the hash.
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "items! {\n    mod shared;\n}\n\n#[cfg(test)]\nmod tests;\n",
    );
    tree.write(
        "crates/adapters/src/tests.rs",
        "#[path = \"shared.rs\"]\nmod shared;\n",
    );
    let shared = "crates/adapters/src/shared.rs";
    tree.write(shared, "fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(&message, &["items!", ADAPTERS_LIB]);
}

#[test]
fn a_macro_that_includes_a_file_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "macro_rules! helpers {\n    () => {\n        include!(\"helpers.rs\");\n    };\n}\n",
    );
    tree.write("crates/adapters/src/helpers.rs", "fn helpers() {}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("a macro calling include! must fail the hash");
    assert_names(
        &error.to_string(),
        &["macro_rules!", "helpers", "include!", ADAPTERS_LIB],
    );
}

#[test]
fn a_cfg_attr_alternate_need_not_have_its_children() {
    // The alternate applies only when its predicate holds, so a child it declares may be absent.
    let tree = adapters_tree("#[cfg_attr(feature = \"alt\", path = \"alt.rs\")]\nmod imp;\n");
    tree.write("crates/adapters/src/schema_v2/imp.rs", "fn default() {}\n");
    let alternate = "crates/adapters/src/alt.rs";
    tree.write(alternate, "mod missing;\n");
    assert!(is_input(&tree, alternate));
}

fn assert_refused(tree: &SampleTree, sites: &[&str]) {
    let error = interpreter_content_hash(tree.path())
        .expect_err("a module outside the hashed sources must fail the hash");
    assert_names(&error.to_string(), sites);
}

#[test]
fn a_path_module_from_a_crate_root_outside_its_source_root_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn interpret() -> bool { true }\n#[path = \"../shared.rs\"]\nmod shared;\n",
    );
    tree.write("crates/adapters/shared.rs", "pub fn shared() {}\n");
    assert_refused(
        &tree,
        &["mod shared", ADAPTERS_LIB, "crates/adapters/shared.rs"],
    );
}

#[test]
fn a_path_module_reaching_an_unhashed_crate_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn interpret() -> bool { true }\n\
         #[cfg_attr(feature = \"alt\", path = \"../../api/src/shared.rs\")]\nmod shared;\n",
    );
    tree.write("crates/adapters/src/shared.rs", "pub fn shared() {}\n");
    tree.write("crates/api/src/shared.rs", "pub fn shared() {}\n");
    assert_refused(
        &tree,
        &["mod shared", ADAPTERS_LIB, "crates/api/src/shared.rs"],
    );
}

#[test]
fn a_path_module_from_a_nested_file_outside_the_source_root_is_refused() {
    let tree = adapters_tree("#[path = \"../shared/v2.rs\"]\nmod v2;\n");
    tree.write("crates/adapters/shared/v2.rs", "pub fn shared() {}\n");
    assert_refused(
        &tree,
        &[
            "mod v2",
            "crates/adapters/src/schema_v2.rs",
            "crates/adapters/shared/v2.rs",
        ],
    );
}

#[test]
fn a_test_only_path_module_outside_the_source_root_stays_allowed() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn interpret() -> bool { true }\n#[cfg(test)]\n#[path = \"../tests/shared.rs\"]\n\
         mod shared;\n",
    );
    tree.write("crates/adapters/tests/shared.rs", "pub fn shared() {}\n");
    interpreter_content_hash(tree.path()).expect("a test-only #[path] module must hash");
}

#[test]
fn a_module_of_a_semantic_source_outside_the_hashed_sources_is_refused() {
    let tree = SampleTree::new();
    super::write_mounted(&tree, "crates/lookup/src/abi.rs", "mod helper;\n");
    tree.write("crates/lookup/src/abi/helper.rs", "pub fn helper() {}\n");
    assert_refused(
        &tree,
        &[
            "mod helper",
            "crates/lookup/src/abi.rs",
            "crates/lookup/src/abi/helper.rs",
        ],
    );
}

#[test]
fn a_module_assembled_from_macro_arguments_is_refused() {
    // The definition holds no `mod`, and the invocation's `mod` is not followed by `name;`, but
    // the expansion is `mod shared;`. A test route to the same file must not drop it.
    let tree = adapters_tree(
        "macro_rules! emit {\n    ($k:ident, $n:ident) => {\n        $k $n;\n    };\n}\n\
         emit!(mod, shared);\n\
         #[cfg(test)]\n#[path = \"schema_v2/shared.rs\"]\nmod shared_tests;\n",
    );
    let shared = "crates/adapters/src/schema_v2/shared.rs";
    tree.write(shared, "pub fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(
        &message,
        &["emit!", "crates/adapters/src/schema_v2.rs", "mod"],
    );
}

#[test]
fn a_macro_body_holding_mod_anywhere_is_refused() {
    let tree =
        adapters_tree("macro_rules! wrap {\n    ($n:ident) => {\n        mod $n {}\n    };\n}\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("a macro body holding mod must fail the hash");
    assert_names(
        &error.to_string(),
        &["macro_rules!", "wrap", "crates/adapters/src/schema_v2.rs"],
    );
}

#[test]
fn a_child_of_an_inline_path_module_outside_the_hashed_sources_is_refused() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn interpret() -> bool { true }\n#[path = \"../outside\"]\nmod wrap {\n    pub mod y;\n}\n",
    );
    tree.write("crates/adapters/outside/y.rs", "pub fn y() {}\n");
    assert_refused(
        &tree,
        &["mod y", ADAPTERS_LIB, "crates/adapters/outside/y.rs"],
    );
}

#[test]
fn a_child_of_a_nested_inline_path_module_outside_the_hashed_sources_is_refused() {
    let tree = SampleTree::new();
    tree.write("crates/interpret/src/lib.rs", "mod write;\n");
    tree.write("crates/interpret/src/write.rs", "mod x;\n");
    tree.write(
        "crates/interpret/src/write/x.rs",
        "#[path = \"../../other\"]\nmod w {\n    mod inner {\n        mod y;\n    }\n}\n",
    );
    tree.write("crates/interpret/other/inner/y.rs", "pub fn y() {}\n");
    assert_refused(
        &tree,
        &[
            "mod y",
            "crates/interpret/src/write/x.rs",
            "crates/interpret/other/inner/y.rs",
        ],
    );
}

#[test]
fn a_use_that_may_alias_an_include_macro_is_refused() {
    let tree =
        adapters_tree("use std::include_str as load;\nconst T: &str = load!(\"table.txt\");\n");
    tree.write("crates/adapters/src/table.txt", "a\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an aliased include macro must fail the hash");
    assert_names(
        &error.to_string(),
        &["include_str", "crates/adapters/src/schema_v2.rs", "alias"],
    );

    let tree = SampleTree::new();
    super::write_mounted(
        &tree,
        "crates/lookup/src/abi.rs",
        "use core::{include_bytes};\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("an imported include macro in a semantic source must fail the hash");
    assert_names(
        &error.to_string(),
        &["include_bytes", "crates/lookup/src/abi.rs"],
    );
}

#[test]
fn a_path_in_a_nested_cfg_attr_is_a_production_route() {
    // Both features select `shared.rs` in production, so a test route to it cannot drop it.
    let tree = adapters_tree(
        "#[cfg_attr(feature = \"a\", cfg_attr(feature = \"b\", path = \"shared.rs\"))]\nmod imp;\n\
         #[cfg(test)]\n#[path = \"shared.rs\"]\nmod shared_tests;\n",
    );
    tree.write("crates/adapters/src/schema_v2/imp.rs", "pub fn imp() {}\n");
    let shared = "crates/adapters/src/shared.rs";
    tree.write(shared, "pub fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(&message, &[shared, "mod imp", "mod shared_tests"]);
}

#[test]
fn a_path_under_a_nested_test_predicate_is_a_test_only_route() {
    let tree = adapters_tree(
        "#[cfg_attr(feature = \"a\", cfg_attr(test, path = \"imp_tests.rs\"))]\nmod imp;\n",
    );
    tree.write("crates/adapters/src/schema_v2/imp.rs", "pub fn imp() {}\n");
    let gated = "crates/adapters/src/imp_tests.rs";
    tree.write(gated, "pub fn imp() {}\n");
    assert!(!is_input(&tree, gated));
}

#[test]
fn an_include_macro_passed_as_a_token_is_refused() {
    let tree = adapters_tree(
        "macro_rules! emit {\n    ($m:ident, $p:literal) => {\n        $m!($p)\n    };\n}\n\
         const T: &str = emit!(include_str, \"table.txt\");\n",
    );
    tree.write("crates/adapters/src/table.txt", "a\n");
    let error = interpreter_content_hash(tree.path())
        .expect_err("an include macro passed as a token must fail the hash");
    assert_names(
        &error.to_string(),
        &["emit!", "include_str", "crates/adapters/src/schema_v2.rs"],
    );

    let tree = adapters_tree(
        "macro_rules! load {\n    ($p:literal) => {\n        call!(include_str, $p)\n    };\n}\n",
    );
    let error = interpreter_content_hash(tree.path())
        .expect_err("a macro body naming an include macro must fail the hash");
    assert_names(&error.to_string(), &["macro_rules!", "load", "include_str"]);
}
