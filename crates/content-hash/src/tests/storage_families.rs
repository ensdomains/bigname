use std::{collections::BTreeSet, fs, io::ErrorKind};

use super::{SampleTree, hashed_source_paths, interpreter_content_hash, workspace_root};
use crate::storage_families::{COMPOSITION_FILES, reader_files};

fn rotates(relative_path: &str) -> bool {
    let tree = SampleTree::new();
    tree.write(relative_path, "fn compose() -> u8 { 1 }\n");
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");
    tree.write(relative_path, "fn compose() -> u8 { 2 }\n");
    first != interpreter_content_hash(tree.path()).expect("updated tree must hash")
}

#[test]
fn family_composition_edits_rotate_the_hash_and_family_reader_edits_do_not() {
    for relative_path in COMPOSITION_FILES {
        assert!(
            rotates(relative_path),
            "{relative_path} must rotate the hash"
        );
    }
    for relative_path in reader_files() {
        assert!(
            !rotates(relative_path),
            "{relative_path} only serves reads and must not rotate the hash"
        );
    }
}

#[test]
fn a_moved_family_composition_source_fails_the_hash_instead_of_narrowing_it() {
    let tree = SampleTree::new();
    interpreter_content_hash(tree.path()).expect("baseline must hash");
    let summary = "crates/storage/src/families/name/summary.rs";
    fs::rename(
        tree.path().join(summary),
        tree.path().join("crates/storage/src/compose_summary.rs"),
    )
    .expect("composition source must be movable");

    let error = interpreter_content_hash(tree.path())
        .expect_err("a missing composition source must fail loudly");
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(error.to_string().contains(summary), "{error}");
}

#[test]
fn an_unclassified_family_source_fails_the_hash() {
    let tree = SampleTree::new();
    interpreter_content_hash(tree.path()).expect("baseline must hash");
    let added = "crates/storage/src/families/name/helpers.rs";
    tree.write(added, "pub fn compose_helper() {}\n");

    let error = interpreter_content_hash(tree.path())
        .expect_err("an unclassified family source must fail loudly");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains(added), "{error}");
}

#[test]
fn checked_in_family_sources_are_classified_once_and_only_composition_is_hashed() {
    let workspace_root = workspace_root();
    let hashed = hashed_source_paths(&workspace_root)
        .expect("checked-in source paths must be collectable")
        .into_iter()
        .collect::<BTreeSet<_>>();
    let composition = COMPOSITION_FILES.iter().collect::<BTreeSet<_>>();

    for relative_path in COMPOSITION_FILES {
        assert!(
            hashed.contains(*relative_path),
            "composition source {relative_path} is not content-hash covered"
        );
    }
    for relative_path in reader_files() {
        assert!(
            !composition.contains(relative_path),
            "{relative_path} is listed as both composition and reader"
        );
        assert!(
            workspace_root.join(relative_path).is_file(),
            "reader source {relative_path} must exist on disk"
        );
        assert!(
            !hashed.contains(*relative_path),
            "reader source {relative_path} must stay outside the hash"
        );
    }
}
