use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

use super::{
    SampleTree, content_hash_inputs, interpreter_content_hash, lockfile_document,
    projection_content_hash, sample_lockfile_packages, workspace_root,
};
use crate::{
    classes::{Class, ROOTS, SEMANTIC_SOURCE_FILES, listed, reason},
    compute::classed_inputs,
    storage_families::COMPOSITION_FILES,
};

fn hashes(tree: &SampleTree) -> (String, String) {
    (
        interpreter_content_hash(tree.path()).expect("tree must hash"),
        projection_content_hash(tree.path()).expect("tree must hash"),
    )
}

/// Whether editing `relative_path` rotates the interpreter hash and the projection hash.
fn rotation(relative_path: &str) -> (bool, bool) {
    let tree = SampleTree::new();
    tree.write(relative_path, "pub fn edited() -> u8 { 1 }\n");
    let before = hashes(&tree);
    tree.write(relative_path, "pub fn edited() -> u8 { 2 }\n");
    let after = hashes(&tree);
    (before.0 != after.0, before.1 != after.1)
}

#[test]
fn project_class_edits_rotate_only_the_projection_hash() {
    for relative_path in [
        "crates/project/src/projection.rs",
        "crates/project/src/builders/name_current/build.sql",
        "crates/storage/src/families/name/summary.rs",
        "crates/storage/src/name_current/row.rs",
        "crates/domain/src/resolution_topology.rs",
        "crates/lookup/src/abi.rs",
    ] {
        assert_eq!(
            rotation(relative_path),
            (false, true),
            "{relative_path} is project-class"
        );
    }
}

#[test]
fn interpreter_class_edits_rotate_both_hashes() {
    for relative_path in [
        "crates/adapters/src/lib.rs",
        "crates/manifests/src/lib.rs",
        "crates/interpret/src/write/identity_names.rs",
        "crates/interpret/src/write.rs",
        "crates/storage/src/families/name/rendered.rs",
        "crates/storage/src/identity_search/documents.rs",
        "crates/domain/src/normalization.rs",
        "crates/domain/src/resolver_read.rs",
    ] {
        assert_eq!(
            rotation(relative_path),
            (true, true),
            "{relative_path} feeds the interpreter hash"
        );
    }
}

#[test]
fn manifest_event_and_semantic_crate_edits_rotate_both_hashes() {
    let tree = SampleTree::new();
    let before = hashes(&tree);
    tree.write_example_manifest("[\"ResolverChanged\"]");
    let after_event = hashes(&tree);
    assert!(before.0 != after_event.0 && before.1 != after_event.1);

    let mut packages = sample_lockfile_packages();
    packages
        .iter_mut()
        .find(|(name, _, _)| *name == "serde")
        .expect("sample lockfile carries serde")
        .1 = "9.9.9";
    tree.write("Cargo.lock", &lockfile_document(&packages));
    let after_crate = hashes(&tree);
    assert!(after_event.0 != after_crate.0 && after_event.1 != after_crate.1);
}

#[test]
fn every_hash_input_has_one_class_and_a_reason() {
    let entries = SEMANTIC_SOURCE_FILES
        .iter()
        .chain(COMPOSITION_FILES)
        .chain(ROOTS)
        .collect::<Vec<_>>();
    let mut paths = BTreeSet::new();
    for entry in &entries {
        assert!(
            !entry.reason.trim().is_empty(),
            "{} has no reason",
            entry.path
        );
        assert!(paths.insert(entry.path), "{} is listed twice", entry.path);
    }
    for entry in SEMANTIC_SOURCE_FILES.iter().chain(COMPOSITION_FILES) {
        assert!(
            !ROOTS
                .iter()
                .any(|root| entry.path.starts_with(&format!("{}/", root.path))),
            "{} is listed by name under a classified root",
            entry.path
        );
    }

    let inputs = classed_inputs(&workspace_root()).expect("checked-in tree must hash");
    let data_reads = inputs
        .iter()
        .filter_map(|input| input.key.strip_prefix("source:"))
        .filter(|key| listed(key).is_none())
        .collect::<Vec<_>>();
    for input in &inputs {
        assert!(!reason(&input.key).trim().is_empty(), "{}", input.key);
        if let Some(key) = input.key.strip_prefix("source:") {
            if let Some(entry) = listed(key) {
                assert_eq!(input.class, entry.class, "{key} took another class");
            } else {
                assert!(
                    !key.ends_with(".rs"),
                    "{key} is hashed without a listed class: {data_reads:?}"
                );
            }
        } else if input.key.starts_with("manifest-event:") {
            assert_eq!(input.class, Class::Interpret, "{}", input.key);
        } else {
            assert!(input.key.starts_with("semantic-crate:"), "{}", input.key);
            assert_eq!(input.class, Class::Both, "{}", input.key);
        }
    }
}

#[test]
fn the_input_list_names_every_input_once() {
    let workspace_root = workspace_root();
    let tsv = content_hash_inputs(&workspace_root).expect("checked-in tree must list inputs");
    let mut lines = tsv.lines();
    assert_eq!(lines.next(), Some("class\tkey\tcontent_keccak256"));
    let listed = lines
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(fields.len(), 3, "{line}");
            assert!(
                ["interpret", "project", "both"].contains(&fields[0]),
                "{line}"
            );
            assert!(fields[2].starts_with("keccak256:"), "{line}");
            (fields[1].to_owned(), fields[0].to_owned())
        })
        .collect::<Vec<_>>();
    let unique = listed.iter().cloned().collect::<BTreeMap<_, _>>();
    assert_eq!(unique.len(), listed.len(), "an input is listed twice");
    let inputs = classed_inputs(&workspace_root).expect("checked-in tree must hash");
    assert_eq!(
        unique,
        inputs
            .into_iter()
            .map(|input| (input.key, input.class.as_str().to_owned()))
            .collect()
    );
}

/// The workspace crates a crate compiles against in production, directly or through another.
fn production_closure(name: &str) -> BTreeSet<String> {
    let root = workspace_root();
    let workspace = fs::read_to_string(root.join("Cargo.toml"))
        .expect("workspace manifest must read")
        .parse::<toml::Table>()
        .expect("workspace manifest must parse");
    let members = workspace["workspace"]["dependencies"]
        .as_table()
        .expect("workspace dependencies")
        .iter()
        .filter_map(|(name, spec)| Some((name.clone(), spec.get("path")?.as_str()?.to_owned())))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut pending = vec![name.to_owned()];
    while let Some(current) = pending.pop() {
        let manifest = fs::read_to_string(root.join(&members[&current]).join("Cargo.toml"))
            .expect("member manifest must read")
            .parse::<toml::Table>()
            .expect("member manifest must parse");
        let mut tables = vec![
            manifest.get("dependencies"),
            manifest.get("build-dependencies"),
        ];
        if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
            for target in targets.values() {
                tables.push(target.get("dependencies"));
                tables.push(target.get("build-dependencies"));
            }
        }
        for dependency in tables
            .into_iter()
            .flatten()
            .filter_map(toml::Value::as_table)
            .flat_map(|table| table.keys())
        {
            if members.contains_key(dependency) && seen.insert(dependency.clone()) {
                pending.push(dependency.clone());
            }
        }
    }
    seen
}

#[test]
fn the_crate_graph_keeps_project_and_interpret_roots_apart() {
    for interpret_side in [
        "bigname-adapters",
        "bigname-manifests",
        "bigname-interpret",
        "bigname-storage",
        "bigname-domain",
        "bigname-lookup",
    ] {
        assert!(
            !production_closure(interpret_side).contains("bigname-project"),
            "{interpret_side} compiles against bigname-project, so the project root is not \
             project-only"
        );
    }
    let project = production_closure("bigname-project");
    for interpret_root in ["bigname-adapters", "bigname-manifests", "bigname-interpret"] {
        assert!(
            !project.contains(interpret_root),
            "bigname-project compiles against {interpret_root}"
        );
    }
}
