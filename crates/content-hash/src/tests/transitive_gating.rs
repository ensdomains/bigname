use std::io::ErrorKind;

use super::{SampleTree, hashed_source_paths, interpreter_content_hash, workspace_root};

mod includes;
mod modules;
mod refusals;
mod slots;

const ADAPTERS_LIB: &str = "crates/adapters/src/lib.rs";
const SCHEMA_V2: &str = "crates/adapters/src/schema_v2.rs";
const STATE: &str = "crates/adapters/src/schema_v2/state.rs";
const V2_TESTS: &str = "crates/adapters/src/schema_v2/state_v2_tests.rs";

fn is_input(tree: &SampleTree, relative_path: &str) -> bool {
    hashed_source_paths(tree.path())
        .expect("sample tree must hash")
        .iter()
        .any(|input| input == relative_path)
}

fn rotates(tree: &SampleTree, relative_path: &str) -> bool {
    tree.write(relative_path, "fn fixture() -> u8 { 1 }\n");
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");
    tree.write(relative_path, "fn fixture() -> u8 { 2 }\n");
    first != interpreter_content_hash(tree.path()).expect("updated tree must hash")
}

fn hash_error(tree: &SampleTree, relative_path: &str) -> String {
    match interpreter_content_hash(tree.path()) {
        Ok(_) => panic!(
            "the hash accepted an ambiguous module. {relative_path} is an input: {}",
            is_input(tree, relative_path)
        ),
        Err(error) => {
            assert_eq!(error.kind(), ErrorKind::InvalidData);
            error.to_string()
        }
    }
}

/// Writes a file hashed by name and declares it from its crate root, as the real crates do, so
/// the walk compiles it. A file hashed by name that no crate declares is not compiled.
fn write_mounted(tree: &SampleTree, relative_path: &str, contents: &str) {
    tree.write(relative_path, contents);
    let (krate, module) = relative_path
        .split_once("/src/")
        .expect("a file hashed by name sits in a crate's src");
    let root = format!("{krate}/src/lib.rs");
    let declaration = format!("pub mod {};\n", module.trim_end_matches(".rs"));
    let mut lib = std::fs::read_to_string(tree.path().join(&root)).unwrap_or_default();
    if !lib.contains(&declaration) {
        lib.push_str(&declaration);
        tree.write(&root, &lib);
    }
}

fn assert_names(message: &str, sites: &[&str]) {
    for site in sites {
        assert!(message.contains(site), "{site} missing from: {message}");
    }
}

/// A sample tree whose adapter crate root declares `schema_v2`, with `schema_v2.rs` as given.
fn adapters_tree(schema_v2: &str) -> SampleTree {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn interpret() -> bool { true }\nmod schema_v2;\n",
    );
    tree.write(SCHEMA_V2, schema_v2);
    tree
}

/// The real adapter shape: a `#[path]` test file gated in `state.rs`, which declares its own
/// `#[path]` siblings without repeating the attribute.
fn gated_state_tree(v2_tests: &str) -> SampleTree {
    let tree = adapters_tree("mod state;\n");
    tree.write(
        STATE,
        "#[path = \"state_v2_maps.rs\"]\npub(super) mod maps;\n\n\
         #[cfg(test)]\n#[path = \"state_v2_tests.rs\"]\nmod v2_tests;\n",
    );
    tree.write(
        "crates/adapters/src/schema_v2/state_v2_maps.rs",
        "pub fn covered() {}\n",
    );
    tree.write(V2_TESTS, v2_tests);
    tree
}

#[test]
fn a_directly_gated_module_stays_out_of_the_hash() {
    let tree = adapters_tree("mod state;\n");
    tree.write(STATE, "#[cfg(test)]\nmod tests;\n");
    let gated = "crates/adapters/src/schema_v2/state/tests.rs";
    assert!(!rotates(&tree, gated));
    assert!(!is_input(&tree, gated));
}

#[test]
fn a_path_sibling_declared_in_a_gated_file_stays_out_of_the_hash() {
    let tree = gated_state_tree("#[path = \"state_v2_expiry_tests.rs\"]\nmod expiry_tests;\n");
    let sibling = "crates/adapters/src/schema_v2/state_v2_expiry_tests.rs";
    assert!(!rotates(&tree, sibling));
    assert!(!is_input(&tree, sibling));
    assert!(!is_input(&tree, V2_TESTS));
}

#[test]
fn a_module_two_levels_below_a_gated_file_stays_out_of_the_hash() {
    let tree = gated_state_tree("#[path = \"state_v2_pointer_tests.rs\"]\nmod pointer_tests;\n");
    tree.write(
        "crates/adapters/src/schema_v2/state_v2_pointer_tests.rs",
        "#[path = \"state_v2_pointer_fixtures.rs\"]\nmod fixtures;\n",
    );
    let nested = "crates/adapters/src/schema_v2/state_v2_pointer_fixtures.rs";
    assert!(!rotates(&tree, nested));
    assert!(!is_input(&tree, nested));
}

#[test]
fn a_plain_module_declared_in_a_gated_file_stays_out_of_the_hash() {
    // rustc resolves the children of a `#[path]` file beside it, as for a `mod.rs`.
    let tree = gated_state_tree("mod support;\n");
    let child = "crates/adapters/src/schema_v2/support.rs";
    assert!(!rotates(&tree, child));
    assert!(!is_input(&tree, child));
}

#[test]
fn a_production_module_beside_a_gated_one_stays_in_the_hash() {
    let tree = gated_state_tree("#[path = \"state_v2_expiry_tests.rs\"]\nmod expiry_tests;\n");
    tree.write(
        "crates/adapters/src/schema_v2/state_v2_expiry_tests.rs",
        "fn test_only() {}\n",
    );
    assert!(is_input(&tree, STATE));
    for production in [
        "crates/adapters/src/schema_v2/state_v2_maps.rs",
        "crates/adapters/src/schema_v2/protocol.rs",
    ] {
        assert!(is_input(&tree, production), "{production} must stay hashed");
        assert!(
            rotates(&tree, production),
            "{production} must rotate the hash"
        );
    }
}

#[test]
fn a_module_behind_a_wider_cfg_stays_in_the_hash() {
    // `cfg(any(test, feature = ...))` also compiles into a feature build, so it is not test-only.
    let tree = adapters_tree("mod migration;\n");
    tree.write(
        "crates/adapters/src/schema_v2/migration.rs",
        "#[cfg(any(test, feature = \"test-activation\"))]\nmod activation;\n",
    );
    let wider = "crates/adapters/src/schema_v2/migration/activation.rs";
    assert!(rotates(&tree, wider));
    assert!(is_input(&tree, wider));
}

#[test]
fn an_inline_module_in_a_gated_file_gates_its_external_children() {
    // The inline module is a directory below the `#[path]` file's own directory.
    let tree = gated_state_tree("mod inner {\n    mod x;\n}\n");
    let child = "crates/adapters/src/schema_v2/inner/x.rs";
    assert!(!rotates(&tree, child));
    assert!(!is_input(&tree, child));
}

#[test]
fn a_file_declared_by_gated_and_production_parents_fails_the_hash() {
    let tree = gated_state_tree("#[path = \"shared.rs\"]\nmod shared;\n");
    tree.write(SCHEMA_V2, "mod registry;\nmod state;\n");
    tree.write(
        "crates/adapters/src/schema_v2/registry.rs",
        "#[path = \"shared.rs\"]\nmod shared;\n",
    );
    let shared = "crates/adapters/src/schema_v2/shared.rs";
    tree.write(shared, "pub fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(
        &message,
        &[
            V2_TESTS,
            "crates/adapters/src/schema_v2/registry.rs",
            "mod shared",
        ],
    );
}

#[test]
fn a_file_under_a_gated_directory_declared_by_a_production_parent_stays_in_the_hash() {
    let tree = adapters_tree("mod bar;\nmod foo;\n");
    tree.write(
        "crates/adapters/src/schema_v2/foo.rs",
        "#[cfg(test)]\nmod tests;\n",
    );
    tree.write(
        "crates/adapters/src/schema_v2/foo/tests.rs",
        "fn test_only() {}\n",
    );
    tree.write(
        "crates/adapters/src/schema_v2/bar.rs",
        "#[path = \"foo/tests/helpers.rs\"]\nmod helpers;\n",
    );
    let helpers = "crates/adapters/src/schema_v2/foo/tests/helpers.rs";
    assert!(rotates(&tree, helpers));
    assert!(is_input(&tree, helpers));
}
#[test]
fn a_gated_file_reached_through_a_parent_directory_path_is_not_a_production_parent() {
    let tree = adapters_tree("#[cfg(test)]\nmod tests;\n");
    tree.write(
        "crates/adapters/src/schema_v2/tests.rs",
        "#[path = \"../other/mod.rs\"]\nmod helper;\n",
    );
    let helper = "crates/adapters/src/other/mod.rs";
    let sub = "crates/adapters/src/other/sub.rs";
    tree.write(helper, "mod sub;\n");
    tree.write(sub, "fn test_only() {}\n");
    let inputs = hashed_source_paths(tree.path()).expect("a test-only tree must hash");
    for gated in [helper, sub] {
        assert!(
            !inputs.iter().any(|input| input == gated),
            "{gated} is test-only but entered the hash"
        );
    }
}

#[test]
fn a_production_inline_module_reaching_a_gated_file_fails_the_hash() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub mod production {\n    pub mod shared;\n}\n\n#[cfg(test)]\nmod tests;\n",
    );
    tree.write(
        "crates/adapters/src/tests.rs",
        "#[path = \"production/shared.rs\"]\nmod shared;\n",
    );
    let shared = "crates/adapters/src/production/shared.rs";
    tree.write(shared, "pub fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(
        &message,
        &[ADAPTERS_LIB, "crates/adapters/src/tests.rs", "mod shared"],
    );
}

#[test]
fn module_text_in_a_gated_string_or_comment_declares_nothing() {
    let tree = gated_state_tree("fn test_only() {}\n");
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");
    for payload in [
        "const SOURCE: &str = r#\"\nmod absent;\n\"#;\n",
        "/*\nmod absent;\n*/\n",
        "/* mod absent; */\nfn test_only() {}\n",
    ] {
        tree.write(V2_TESTS, payload);
        let changed = interpreter_content_hash(tree.path()).expect("test text must hash");
        assert_eq!(first, changed, "{payload:?} must not change the hash");
    }
}

#[test]
fn a_plain_child_of_a_path_mounted_production_file_resolves_beside_it() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "#[path = \"special_production.rs\"]\nmod production;\n",
    );
    tree.write("crates/adapters/src/special_production.rs", "mod shared;\n");
    let shared = "crates/adapters/src/shared.rs";
    tree.write(shared, "pub fn shared() {}\n");
    assert!(is_input(&tree, shared));

    tree.write(
        ADAPTERS_LIB,
        "#[path = \"special_production.rs\"]\nmod production;\n\n#[cfg(test)]\nmod tests;\n",
    );
    tree.write(
        "crates/adapters/src/tests.rs",
        "#[path = \"shared.rs\"]\nmod shared;\n",
    );
    let message = hash_error(&tree, shared);
    assert_names(
        &message,
        &[
            "crates/adapters/src/special_production.rs",
            "crates/adapters/src/tests.rs",
            "mod shared",
        ],
    );
}

#[test]
fn an_undeclared_file_under_a_gated_directory_stays_in_the_hash() {
    // Only a file proven reached through `#[cfg(test)]` leaves the hash. Nothing declares
    // `orphan.rs`, so it and the file it names stay hashed.
    let tree = SampleTree::new();
    tree.write(ADAPTERS_LIB, "#[cfg(test)]\nmod tests;\n");
    tree.write("crates/adapters/src/tests.rs", "fn test_only() {}\n");
    let orphan = "crates/adapters/src/tests/orphan.rs";
    tree.write(orphan, "#[path = \"../fixture.rs\"]\nmod fixture;\n");
    let fixture = "crates/adapters/src/fixture.rs";
    assert!(rotates(&tree, fixture));
    assert!(is_input(&tree, fixture));
    assert!(is_input(&tree, orphan));
}
#[test]
fn an_inline_path_in_a_non_mod_rs_file_is_relative_to_the_file_directory() {
    // rustc joins an inline module's `#[path]` to the declaring file's directory, without the
    // `schema_v2/` offset a plain child of `schema_v2.rs` would take.
    let tree = adapters_tree("#[cfg(test)]\n#[path = \"p\"]\nmod z {\n    mod w;\n}\n");
    let child = "crates/adapters/src/p/w.rs";
    assert!(!rotates(&tree, child));
    assert!(!is_input(&tree, child));
}

#[test]
fn a_declaration_behind_a_non_test_cfg_may_have_no_file() {
    for attribute in [
        "#[cfg(windows)]",
        "#[cfg(not(test))]",
        "#[cfg(feature = \"reth\")]",
    ] {
        let tree = adapters_tree(&format!("{attribute}\nmod win;\n"));
        interpreter_content_hash(tree.path())
            .unwrap_or_else(|error| panic!("{attribute} mod win; must hash: {error}"));
    }
    let tree = gated_state_tree("fn test_only() {}\n");
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");
    tree.write(V2_TESTS, "#[cfg(windows)]\nmod win;\nfn test_only() {}\n");
    let changed = interpreter_content_hash(tree.path()).expect("a cfg'd-out module must hash");
    assert_eq!(first, changed);
}

#[test]
fn a_production_module_declared_in_a_block_reaching_a_gated_file_fails_the_hash() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn helper() {\n    #[path = \"production/shared.rs\"]\n    mod shared;\n}\n\n\
         #[cfg(test)]\nmod tests;\n",
    );
    tree.write(
        "crates/adapters/src/tests.rs",
        "#[path = \"production/shared.rs\"]\nmod shared;\n",
    );
    let shared = "crates/adapters/src/production/shared.rs";
    tree.write(shared, "pub fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(
        &message,
        &[ADAPTERS_LIB, "crates/adapters/src/tests.rs", "mod shared"],
    );
}

#[test]
fn every_production_cargo_target_root_is_walked() {
    for (target, declaration) in [
        (
            "crates/adapters/src/bin/tool.rs",
            "#[path = \"../shared.rs\"]",
        ),
        (
            "crates/adapters/src/bin/daemon/main.rs",
            "#[path = \"../../shared.rs\"]",
        ),
        (
            "crates/adapters/tools/listed.rs",
            "#[path = \"../src/shared.rs\"]",
        ),
    ] {
        let tree = SampleTree::new();
        tree.write(
            "crates/adapters/Cargo.toml",
            "[package]\nname = \"adapters\"\nbuild = false\n\n[[bin]]\nname = \"listed\"\npath = \"tools/listed.rs\"\n",
        );
        tree.write(ADAPTERS_LIB, "#[cfg(test)]\nmod tests;\n");
        tree.write(
            "crates/adapters/src/tests.rs",
            "#[path = \"shared.rs\"]\nmod shared;\n",
        );
        let shared = "crates/adapters/src/shared.rs";
        tree.write(shared, "pub fn shared() {}\n");
        tree.write(
            target,
            &format!("{declaration}\nmod shared;\nfn main() {{}}\n"),
        );
        let message = hash_error(&tree, shared);
        assert_names(&message, &[target, "crates/adapters/src/tests.rs"]);
    }
}

#[test]
fn a_cfg_attr_path_follows_its_predicate() {
    let tree = adapters_tree(
        "#[cfg_attr(test, path = \"test_impl.rs\")]\n\
         #[cfg_attr(not(test), path = \"prod_impl.rs\")]\nmod imp;\n",
    );
    let test_impl = "crates/adapters/src/test_impl.rs";
    let prod_impl = "crates/adapters/src/prod_impl.rs";
    tree.write(test_impl, "fn test_only() {}\n");
    tree.write(prod_impl, "fn production() {}\n");
    assert!(!rotates(&tree, test_impl));
    assert!(!is_input(&tree, test_impl));
    assert!(rotates(&tree, prod_impl));
    assert!(is_input(&tree, prod_impl));
}

#[test]
fn every_declaration_of_a_file_contributes_its_children() {
    // `mod regular;` resolves children under `regular/`, the `#[path]` mount beside `lib.rs`.
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "mod regular;\n#[path = \"regular.rs\"]\nmod special;\n\
         #[cfg(test)]\n#[path = \"regular/shared.rs\"]\nmod test_shared;\n",
    );
    tree.write("crates/adapters/src/regular.rs", "mod shared;\n");
    tree.write("crates/adapters/src/shared.rs", "pub fn shared() {}\n");
    let shared = "crates/adapters/src/regular/shared.rs";
    tree.write(shared, "pub fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(
        &message,
        &[
            ADAPTERS_LIB,
            "crates/adapters/src/regular.rs",
            "mod test_shared",
        ],
    );
}

#[test]
fn a_disabled_enclosing_cfg_may_leave_child_files_absent() {
    let tree = adapters_tree(
        "#[cfg(any())]\nmod disabled {\n    mod absent;\n}\n\n\
         #[cfg(feature = \"reth\")]\nmod present;\n",
    );
    tree.write("crates/adapters/src/schema_v2/present.rs", "mod absent;\n");
    interpreter_content_hash(tree.path()).expect("modules under a disabled cfg must hash");
}

#[test]
fn a_file_beside_a_test_module_that_nothing_declares_stays_in_the_hash() {
    let tree = SampleTree::new();
    tree.write(ADAPTERS_LIB, "#[cfg(test)]\nmod tests;\n");
    tree.write(
        "crates/adapters/src/tests.rs",
        "#[cfg(test)]\n#[path = \"other.rs\"]\nmod other;\n",
    );
    tree.write("crates/adapters/src/other.rs", "fn test_only() {}\n");
    assert!(!is_input(&tree, "crates/adapters/src/other.rs"));
    let fixture = "crates/adapters/src/other/fixture.rs";
    assert!(rotates(&tree, fixture));
    assert!(is_input(&tree, fixture));
}
#[test]
fn an_inline_module_in_a_block_drops_the_file_offset() {
    // rustc resolves `inner` beside `y.rs`, not under `y/`, because a block owns no directory.
    let tree = SampleTree::new();
    tree.write(ADAPTERS_LIB, "mod x;\n");
    tree.write("crates/adapters/src/x.rs", "mod y;\n");
    tree.write(
        "crates/adapters/src/x/y.rs",
        "fn f() {\n    mod inner {\n        #[path = \"a.rs\"]\n        mod a;\n    }\n}\n",
    );
    let production = "crates/adapters/src/x/inner/a.rs";
    tree.write(production, "fn production() {}\n");
    assert!(is_input(&tree, production));
}

#[test]
fn a_binary_target_outside_src_is_watched() {
    let tree = SampleTree::new();
    tree.write(
        "crates/adapters/Cargo.toml",
        "[package]\nname = \"adapters\"\nbuild = false\n\n[[bin]]\nname = \"listed\"\npath = \"tools/listed.rs\"\n",
    );
    let listed = tree.path().join("crates/adapters/tools/listed.rs");
    tree.write("crates/adapters/tools/listed.rs", "fn main() {}\n");
    let watched = crate::compute::watched_paths(tree.path());
    assert!(
        watched.iter().any(|watched| listed.starts_with(watched)),
        "{} is a target root but not watched",
        listed.display()
    );
}

#[test]
fn a_production_include_keeps_its_target_in_the_hash() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "include!(\"tests/helpers.rs\");\n\n#[cfg(test)]\nmod tests;\n",
    );
    tree.write("crates/adapters/src/tests.rs", "fn test_only() {}\n");
    let helpers = "crates/adapters/src/tests/helpers.rs";
    assert!(rotates(&tree, helpers));
    assert!(is_input(&tree, helpers));
}

#[test]
fn a_test_module_also_included_by_production_fails_the_hash() {
    let tree = SampleTree::new();
    tree.write(
        ADAPTERS_LIB,
        "pub fn f() -> u8 {\n    include!(\"tests/helpers.rs\")\n}\n\n#[cfg(test)]\nmod tests;\n",
    );
    tree.write("crates/adapters/src/tests.rs", "mod helpers;\n");
    let helpers = "crates/adapters/src/tests/helpers.rs";
    tree.write(helpers, "1\n");
    let message = hash_error(&tree, helpers);
    assert_names(&message, &[ADAPTERS_LIB, "crates/adapters/src/tests.rs"]);
}

#[test]
fn every_directory_the_walk_reads_outside_src_is_watched() {
    let tree = SampleTree::new();
    tree.write(
        "crates/adapters/Cargo.toml",
        "[package]\nname = \"adapters\"\nbuild = false\n\n[[bin]]\nname = \"listed\"\npath = \"tools/listed.rs\"\n",
    );
    tree.write(
        "crates/adapters/tools/listed.rs",
        "#[path = \"wiring/mod.rs\"]\nmod wiring;\nfn main() {}\n",
    );
    let wiring = tree.path().join("crates/adapters/tools/wiring/mod.rs");
    tree.write("crates/adapters/tools/wiring/mod.rs", "fn wire() {}\n");
    let watched = crate::compute::watched_paths(tree.path());
    assert!(
        watched.iter().any(|watched| wiring.starts_with(watched)),
        "{} is walked but not watched",
        wiring.display()
    );
}

#[test]
fn a_raw_path_attribute_names_the_module_file() {
    // rustc reads `#[r#path]` as `#[path]`, so production loads `shared.rs`, not `imp.rs`.
    let tree = adapters_tree(
        "#[r#path = \"shared.rs\"]\nmod imp;\n\n#[cfg(test)]\n#[path = \"shared.rs\"]\nmod helper;\n",
    );
    tree.write(
        "crates/adapters/src/schema_v2/imp.rs",
        "fn conventional() {}\n",
    );
    let shared = "crates/adapters/src/shared.rs";
    tree.write(shared, "fn shared() {}\n");
    let message = hash_error(&tree, shared);
    assert_names(&message, &[shared, "mod imp", "mod helper"]);
}

#[test]
fn checked_in_state_v2_test_siblings_are_not_hash_inputs() {
    let workspace_root = workspace_root();
    let hashed = hashed_source_paths(&workspace_root).expect("checked-in sources must hash");
    for sibling in [
        "crates/adapters/src/schema_v2/state_v2_tests.rs",
        "crates/adapters/src/schema_v2/state_v2_expiry_tests.rs",
        "crates/adapters/src/schema_v2/state_v2_mount_tests.rs",
        "crates/adapters/src/schema_v2/state_v2_pointer_tests.rs",
    ] {
        assert!(
            workspace_root.join(sibling).is_file(),
            "{sibling} moved; update this test"
        );
        assert!(
            !hashed.iter().any(|input| input == sibling),
            "{sibling} is test-only but entered the hash"
        );
    }
}
