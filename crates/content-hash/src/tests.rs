use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::{INTERPRETER_CONTENT_HASH, interpreter_content_hash};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[test]
fn build_time_hash_matches_checked_in_sources() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_eq!(
        interpreter_content_hash(workspace_root).expect("source hash must compute"),
        INTERPRETER_CONTENT_HASH
    );
}

#[test]
fn hash_changes_for_sources_and_manifest_event_mappings() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    tree.write(
        "crates/adapters/src/lib.rs",
        "pub fn interpret() -> bool { false }\n",
    );
    let changed_source = interpreter_content_hash(tree.path()).expect("source must hash");
    assert_ne!(first, changed_source);

    tree.write(
        "crates/adapters/src/lib.rs",
        "pub fn interpret() -> bool { true }\n",
    );
    tree.write_example_manifest("[\"ResolverChanged\"]");
    let changed_mapping = interpreter_content_hash(tree.path()).expect("mapping must hash");
    assert_ne!(first, changed_mapping);
}

#[test]
fn normalizer_version_and_test_only_sources_do_not_change_hash() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    tree.write(
        "apps/worker/src/name_current/tests.rs",
        "fn test_only_change() {}\n",
    );
    tree.write_example_manifest_with_normalizer("ensip15@new", "[\"RecordChanged\"]");
    let changed = interpreter_content_hash(tree.path()).expect("updated tree must hash");
    assert_eq!(first, changed);
}

struct SampleTree {
    root: PathBuf,
}

impl SampleTree {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "bigname-content-hash-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("sample tree must be creatable");
        let tree = Self { root };
        tree.write(
            "crates/adapters/src/lib.rs",
            "pub fn interpret() -> bool { true }\n",
        );
        tree.write(
            "crates/manifests/src/lib.rs",
            "pub fn admit() -> bool { true }\n",
        );
        tree.write(
            "apps/worker/src/name_current.rs",
            "pub fn project() -> bool { true }\n",
        );
        tree.write_manifest_floor();
        tree
    }

    fn path(&self) -> &Path {
        &self.root
    }

    fn write(&self, relative_path: &str, contents: &str) {
        let path = self.root.join(relative_path);
        fs::create_dir_all(path.parent().expect("sample file must have a parent"))
            .expect("sample parent must be creatable");
        fs::write(path, contents).expect("sample file must be writable");
    }

    fn write_manifest_floor(&self) {
        self.write_example_manifest("[\"RecordChanged\"]");
        for index in 0..15 {
            self.write(
                &format!("manifests/generated/manifest-{index:02}.toml"),
                &manifest_document(
                    "ensip15@old",
                    &format!("Stable{index}"),
                    "[\"RecordChanged\"]",
                    7,
                ),
            );
        }
    }

    fn write_example_manifest(&self, normalized_events: &str) {
        self.write_example_manifest_with_normalizer("ensip15@old", normalized_events);
    }

    fn write_example_manifest_with_normalizer(
        &self,
        normalizer_version: &str,
        normalized_events: &str,
    ) {
        self.write(
            "manifests/mainnet/example.toml",
            &manifest_document(normalizer_version, "Changed", normalized_events, 7),
        );
    }
}

impl Drop for SampleTree {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).expect("sample tree must be removable");
    }
}

fn manifest_document(
    normalizer_version: &str,
    event_name: &str,
    normalized_events: &str,
    event_count: usize,
) -> String {
    let mut document = format!("normalizer_version = {normalizer_version:?}\n");
    for index in 0..event_count {
        let name = format!("{event_name}{index}");
        document.push_str(&format!(
            "[[abi.events]]\n\
             name = {name:?}\n\
             fragment = \"event {name}(bytes32 indexed node)\"\n\
             emitter_roles = [\"registry\"]\n\
             normalized_events = {normalized_events}\n"
        ));
    }
    document
}
