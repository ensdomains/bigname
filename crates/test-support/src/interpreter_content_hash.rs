use std::{io, path::Path};

include!(concat!(env!("OUT_DIR"), "/interpreter_content_hash.rs"));

/// Compute the interpreter content hash for a source tree.
pub fn interpreter_content_hash(workspace_root: impl AsRef<Path>) -> io::Result<String> {
    super::interpreter_content_hash_impl::compute(workspace_root.as_ref())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn build_time_hash_matches_the_checked_in_sources() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        assert_eq!(
            interpreter_content_hash(workspace_root).expect("source hash must compute"),
            INTERPRETER_CONTENT_HASH
        );
    }

    #[test]
    fn hash_is_stable_and_changes_only_for_included_content() {
        let tree = SampleTree::new();
        tree.write(
            "crates/adapters/src/lib.rs",
            "pub fn interpret() -> bool { true }\n",
        );
        tree.write(
            "apps/worker/src/name_current.rs",
            "fn build_projection() {}\n",
        );
        tree.write("apps/worker/src/name_current/tests.rs", "ignored test\n");
        tree.write(
            "apps/worker/src/address_names.rs",
            "fn address_names() {}\n",
        );
        tree.write("apps/worker/src/children.rs", "fn children() {}\n");
        tree.write("apps/worker/src/permissions.rs", "fn permissions() {}\n");
        tree.write("apps/worker/src/primary_name.rs", "fn primary_name() {}\n");
        tree.write(
            "apps/worker/src/record_inventory.rs",
            "fn record_inventory() {}\n",
        );
        tree.write("apps/worker/src/resolver.rs", "fn resolver() {}\n");
        tree.write("apps/worker/src/projection_json.rs", "fn json() {}\n");
        tree.write(
            "manifests/mainnet/example.toml",
            concat!(
                "normalizer_version = \"ensip15@old\"\n",
                "[[abi.events]]\n",
                "name = \"Changed\"\n",
                "fragment = \"event Changed(bytes32 indexed node)\"\n",
            ),
        );

        let first = interpreter_content_hash(tree.path()).expect("first hash must compute");
        let repeated = interpreter_content_hash(tree.path()).expect("repeat hash must compute");
        assert_eq!(first, repeated, "an unchanged tree must have one hash");

        tree.write(
            "crates/adapters/src/lib.rs",
            "pub fn interpret() -> bool { false }\n",
        );
        let adapter_change =
            interpreter_content_hash(tree.path()).expect("adapter hash must compute");
        assert_ne!(first, adapter_change, "adapter source must affect the hash");

        tree.write(
            "crates/adapters/src/lib.rs",
            "pub fn interpret() -> bool { true }\n",
        );
        tree.write(
            "apps/worker/src/name_current.rs",
            "fn build_projection() { let changed = true; }\n",
        );
        let builder_change =
            interpreter_content_hash(tree.path()).expect("builder hash must compute");
        assert_ne!(
            first, builder_change,
            "projection-builder source must affect the hash"
        );

        tree.write(
            "apps/worker/src/name_current.rs",
            "fn build_projection() {}\n",
        );
        tree.write(
            "apps/worker/src/name_current/tests.rs",
            "changed test fixture\n",
        );
        let test_change =
            interpreter_content_hash(tree.path()).expect("excluded test hash must compute");
        assert_eq!(first, test_change, "separate test sources must be excluded");

        tree.write(
            "manifests/mainnet/example.toml",
            concat!(
                "normalizer_version = \"ensip15@new\"\n",
                "[[abi.events]]\n",
                "name = \"Changed\"\n",
                "fragment = \"event Changed(bytes32 indexed node)\"\n",
            ),
        );
        let normalizer_change =
            interpreter_content_hash(tree.path()).expect("normalizer hash must compute");
        assert_eq!(
            first, normalizer_change,
            "the normalizer version is owned by flag recomputation"
        );

        tree.write(
            "manifests/mainnet/example.toml",
            concat!(
                "normalizer_version = \"ensip15@new\"\n",
                "[[abi.events]]\n",
                "name = \"Changed\"\n",
                "fragment = \"event Changed(bytes32 indexed node, address owner)\"\n",
            ),
        );
        let event_change = interpreter_content_hash(tree.path()).expect("event hash must compute");
        assert_ne!(
            first, event_change,
            "manifest ABI event fragments must affect the hash"
        );
    }

    struct SampleTree {
        root: PathBuf,
    }

    impl SampleTree {
        fn new() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "bigname-interpreter-content-hash-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("sample tree root must be creatable");
            Self { root }
        }

        fn path(&self) -> &Path {
            &self.root
        }

        fn write(&self, relative_path: &str, contents: &str) {
            let path = self.root.join(relative_path);
            fs::create_dir_all(path.parent().expect("sample file must have a parent"))
                .expect("sample parent directory must be creatable");
            fs::write(path, contents).expect("sample file must be writable");
        }
    }

    impl Drop for SampleTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}
