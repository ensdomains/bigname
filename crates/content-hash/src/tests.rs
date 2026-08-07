use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::{
    HASHED_MANIFEST_PROFILES, INTERPRETER_CONTENT_HASH, interpreter_content_hash,
    manifest_profile_hash,
};
use crate::compute::{
    cfg_test_source_exclusions, excluded_source_reason, hashed_source_paths, semantic_source_files,
};

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
fn build_time_manifest_profiles_match_checked_in_profiles() {
    let manifests_root = workspace_root().join("manifests");
    assert!(!HASHED_MANIFEST_PROFILES.is_empty());
    for (profile, expected_hash) in HASHED_MANIFEST_PROFILES {
        assert_eq!(
            manifest_profile_hash(manifests_root.join(profile))
                .expect("checked-in manifest profile must hash"),
            *expected_hash,
            "compiled manifest profile fingerprint drifted for {profile}"
        );
    }
}

#[test]
fn manifest_profile_fingerprint_excludes_only_normalizer_version() {
    let tree = SampleTree::empty();
    let profile_root = tree.path().join("manifests/mainnet");
    tree.write(
        "manifests/mainnet/example.toml",
        "source_family = \"ens_v1_registry_l1\"\nnormalizer_version = \"ensip15@old\"\n",
    );
    let initial = manifest_profile_hash(&profile_root).expect("manifest profile must hash");

    tree.write(
        "manifests/mainnet/example.toml",
        "source_family = \"ens_v1_registry_l1\"\nnormalizer_version = \"ensip15@new\"\n",
    );
    assert_eq!(
        manifest_profile_hash(&profile_root).expect("manifest profile must hash"),
        initial,
        "normalizer-version changes belong to recompute-flags"
    );

    tree.write(
        "manifests/mainnet/example.toml",
        "source_family = \"ens_v2_registry_l1\"\nnormalizer_version = \"ensip15@new\"\n",
    );
    assert_ne!(
        manifest_profile_hash(&profile_root).expect("manifest profile must hash"),
        initial,
        "other runtime manifest changes must not pass the compiled manifest-profile gate"
    );

    let before_hidden_namespace =
        manifest_profile_hash(&profile_root).expect("manifest profile must hash");
    tree.write(
        "manifests/mainnet/.hidden-namespace/extra.toml",
        &manifest_document(
            "ensip15@hidden",
            "Hidden",
            "event Hidden0(bytes32 indexed node)",
            "[\"registry\"]",
            "[\"RecordChanged\"]",
            1,
        ),
    );
    assert_ne!(
        manifest_profile_hash(&profile_root).expect("manifest profile must hash"),
        before_hidden_namespace,
        "a nested hidden namespace is still runtime deployment-profile input"
    );
}

#[test]
fn hash_changes_for_sources_and_manifest_event_mappings() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");
    let repeated = interpreter_content_hash(tree.path()).expect("repeat must hash");
    assert_eq!(first, repeated, "an unchanged tree must have one hash");

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
fn projection_and_whole_manifest_event_blocks_change_the_hash() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    tree.write(
        "crates/project/src/projection.rs",
        "fn derive_invalidations() { let changed = true; }\n",
    );
    let projection_change =
        interpreter_content_hash(tree.path()).expect("projection source must hash");
    assert_ne!(
        first, projection_change,
        "projection invalidation derivation must affect the hash"
    );

    tree.write(
        "crates/project/src/projection.rs",
        "fn derive_invalidations() {}\n",
    );
    tree.write_example_manifest_details(
        "ensip15@old",
        "event Changed0(bytes32 indexed node, address owner)",
        "[\"registry\"]",
        "[\"RecordChanged\"]",
    );
    let fragment_change = interpreter_content_hash(tree.path()).expect("event fragment must hash");
    assert_ne!(
        first, fragment_change,
        "manifest ABI event fragments must affect the hash"
    );

    tree.write_example_manifest_details(
        "ensip15@old",
        "event Changed0(bytes32 indexed node)",
        "[\"resolver\"]",
        "[\"RecordChanged\"]",
    );
    let emitter_change = interpreter_content_hash(tree.path()).expect("emitter mapping must hash");
    assert_ne!(
        first, emitter_change,
        "manifest emitter-role mappings must affect the hash"
    );

    tree.write_example_manifest_details(
        "ensip15@old",
        "event Changed0(bytes32 indexed node)",
        "[\"registry\"]",
        "[\"ResolverChanged\"]",
    );
    let normalized_change =
        interpreter_content_hash(tree.path()).expect("normalized mapping must hash");
    assert_ne!(
        first, normalized_change,
        "manifest normalized-event mappings must affect the hash"
    );
}

#[test]
fn normalizer_version_and_test_only_sources_do_not_change_hash() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    tree.write(
        "manifests/.bigname-e2e-runtime-profile-999999/local.toml",
        &manifest_document(
            "ensip15@hidden",
            "Hidden",
            "event Hidden0(bytes32 indexed node)",
            "[\"registry\"]",
            "[\"RecordChanged\"]",
            1,
        ),
    );
    tree.write("crates/project/src/tests.rs", "fn test_only_change() {}\n");
    tree.write_example_manifest_with_normalizer("ensip15@new", "[\"RecordChanged\"]");
    let changed = interpreter_content_hash(tree.path()).expect("updated tree must hash");
    assert_eq!(first, changed);
}

#[test]
fn phase_orchestration_does_not_change_hash() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    tree.write(
        "apps/phase-runner/src/interpret_phase.rs",
        "fn run_interpret_phase() {}\n",
    );
    tree.write("crates/interpret/src/engine.rs", "fn size_batches() {}\n");

    let changed = interpreter_content_hash(tree.path()).expect("updated tree must hash");
    assert_eq!(
        first, changed,
        "phase orchestration and the interpret engine must remain outside the hash"
    );
}

#[test]
fn every_hash_input_is_watched_for_rebuilds() {
    // A hashed root or semantic file that is not watched means editing interpretation code does
    // not recompile the hash. A scan root is not hashed, but a cfg(test) declaration inside one
    // changes which files under a hashed root are excluded, so it has the same requirement.
    let workspace_root = workspace_root();
    let watched = crate::compute::watched_paths(&workspace_root);
    for relative_path in crate::compute::cfg_test_scan_roots()
        .iter()
        .chain(crate::compute::hashed_roots())
        .chain(semantic_source_files())
    {
        let path = workspace_root.join(relative_path);
        assert!(
            watched.contains(&path),
            "{relative_path} is a hash input but not watched"
        );
    }
}

#[test]
fn a_test_module_declared_in_the_write_parent_stays_out_of_the_hash() {
    // `write.rs` is the hashed root's parent module but lives outside it, so its `#[cfg(test)]`
    // declaration has to be seen or the test file lands inside the fence as production input.
    let tree = SampleTree::new();
    tree.write(
        "crates/interpret/src/write.rs",
        "#[cfg(test)]\nmod tests;\nmod identity_names;\n",
    );
    tree.write(
        "crates/interpret/src/write/tests.rs",
        "fn test_only_baseline() {}\n",
    );
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    tree.write(
        "crates/interpret/src/write/tests.rs",
        "fn test_only_change() {}\n",
    );

    let changed = interpreter_content_hash(tree.path()).expect("updated tree must hash");
    assert_eq!(
        first, changed,
        "a cfg(test) module must not rotate the hash"
    );
}

#[test]
fn write_conflict_policy_changes_the_hash() {
    // Which interpreted row wins a persistence conflict decides which identity, discovery, and
    // preimage rows the projections then read, so it is interpretation, not plumbing.
    for relative_path in [
        "crates/interpret/src/write/identity_names.rs",
        "crates/interpret/src/write.rs",
        "crates/interpret/src/recompute.rs",
    ] {
        let tree = SampleTree::new();
        let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

        tree.write(relative_path, "fn source_priority() -> u8 { 2 }\n");

        let changed = interpreter_content_hash(tree.path()).expect("updated tree must hash");
        assert_ne!(first, changed, "{relative_path} must rotate the hash");
    }
}

#[test]
fn semantic_dependencies_of_the_watched_roots_affect_the_hash() {
    for (relative_path, changed_source) in [
        (
            "crates/domain/src/normalization.rs",
            "pub fn normalize_name() -> bool { false }\n",
        ),
        (
            "crates/lookup/src/reverse_names.rs",
            "pub fn decode_reverse_names() -> bool { false }\n",
        ),
        (
            "crates/lookup/src/text_records.rs",
            "pub fn decode_text_records() -> bool { false }\n",
        ),
        (
            "crates/lookup/src/abi.rs",
            "pub fn namehash() -> bool { false }\n",
        ),
        (
            "crates/lookup/src/record_selector.rs",
            "pub struct RecordSelector;\n",
        ),
    ] {
        let tree = SampleTree::new();
        let first = interpreter_content_hash(tree.path()).expect("baseline must hash");
        tree.write(relative_path, changed_source);
        let changed = interpreter_content_hash(tree.path()).expect("updated tree must hash");
        assert_ne!(
            first, changed,
            "{relative_path} decides persisted rows and must rotate the interpreter hash"
        );
    }
}

#[test]
fn request_scoped_lookup_sources_do_not_change_hash() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    for relative_path in [
        "crates/lookup/src/engine.rs",
        "crates/lookup/src/types.rs",
        "crates/lookup/src/ccip/gateway.rs",
        "crates/lookup/src/store/indexed.rs",
        "crates/lookup/src/rpc.rs",
        "crates/domain/src/block_interval.rs",
    ] {
        tree.write(relative_path, "fn serving_only_change() {}\n");
    }

    let changed = interpreter_content_hash(tree.path()).expect("updated tree must hash");
    assert_eq!(
        first, changed,
        "request-scoped serving, transport, and ingest-range sources must stay outside the hash"
    );
}

#[test]
fn a_moved_semantic_source_fails_the_hash_instead_of_narrowing_it() {
    let tree = SampleTree::new();
    interpreter_content_hash(tree.path()).expect("baseline must hash");
    fs::remove_file(tree.path().join("crates/domain/src/normalization.rs"))
        .expect("semantic source must be removable");

    let error = interpreter_content_hash(tree.path())
        .expect_err("a missing semantic source must fail loudly");
    assert!(
        error
            .to_string()
            .contains("crates/domain/src/normalization.rs"),
        "missing semantic source must name the path: {error}"
    );
}

#[test]
fn checked_in_semantic_sources_are_hash_covered() {
    let workspace_root = workspace_root();
    let hashed = hashed_source_paths(&workspace_root)
        .expect("checked-in source paths must be collectable")
        .into_iter()
        .collect::<BTreeSet<_>>();

    for relative_path in semantic_source_files() {
        assert!(
            workspace_root.join(relative_path).is_file(),
            "semantic source {relative_path} must exist on disk"
        );
        assert!(
            hashed.contains(*relative_path),
            "semantic source {relative_path} is not content-hash covered"
        );
    }
}

#[test]
fn newly_added_adapter_and_project_modules_affect_the_hash() {
    let adapter_tree = SampleTree::new();
    let adapter_before =
        interpreter_content_hash(adapter_tree.path()).expect("adapter baseline must hash");
    adapter_tree.write(
        "crates/adapters/src/future_interpreter.rs",
        "fn interpret_future_event() {}\n",
    );
    let adapter_after =
        interpreter_content_hash(adapter_tree.path()).expect("new adapter module must hash");
    assert_ne!(
        adapter_before, adapter_after,
        "a new adapter source file must enter the hash automatically"
    );

    let project_tree = SampleTree::new();
    let project_before =
        interpreter_content_hash(project_tree.path()).expect("project baseline must hash");
    project_tree.write(
        "crates/project/src/future_projection.rs",
        "fn derive_future_projection() {}\n",
    );
    let project_after =
        interpreter_content_hash(project_tree.path()).expect("new project module must hash");
    assert_ne!(
        project_before, project_after,
        "a new project semantic source file must enter the hash automatically"
    );
}

#[test]
fn manifest_authority_source_changes_affect_the_hash() {
    let tree = SampleTree::new();
    let before =
        interpreter_content_hash(tree.path()).expect("manifest-authority baseline must hash");
    tree.write(
        "crates/manifests/src/schema_v2.rs",
        "fn persist_manifest_authority() {}\n",
    );
    let after = interpreter_content_hash(tree.path()).expect("manifest-authority source must hash");
    assert_ne!(
        before, after,
        "manifest authority persistence must affect the hash"
    );
}

#[test]
fn excluded_sources_are_insensitive_but_production_support_is_hashed() {
    let tree = SampleTree::new();
    let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

    tree.write("crates/project/src/tests.rs", "fn changed_test() {}\n");
    let excluded_change =
        interpreter_content_hash(tree.path()).expect("excluded sources must be inspectable");
    assert_eq!(
        first, excluded_change,
        "cfg(test)-only sources must be excluded"
    );

    tree.write(
        "crates/adapters/src/schema_v2/protocol.rs",
        "fn production_support_changed() {}\n",
    );
    let production_support_change =
        interpreter_content_hash(tree.path()).expect("production support must hash");
    assert_ne!(
        first, production_support_change,
        "production support files must not be blanket-excluded"
    );
}

#[test]
fn conventionally_named_source_is_hashed_without_a_cfg_test_gate() {
    let tree = SampleTree::new();
    tree.write(
        "crates/project/src/conventional.rs",
        "mod tests;\npub fn project() -> bool { true }\n",
    );
    tree.write(
        "crates/project/src/conventional/tests.rs",
        "pub fn interpret() -> bool { false }\n",
    );
    let first = interpreter_content_hash(tree.path()).expect("production module must hash");

    tree.write(
        "crates/project/src/conventional/tests.rs",
        "pub fn interpret() -> bool { true }\n",
    );
    let changed = interpreter_content_hash(tree.path()).expect("changed module must hash");
    assert_ne!(first, changed);
}

#[test]
fn descendants_of_a_cfg_test_module_are_excluded() {
    let tree = SampleTree::new();
    tree.write("crates/project/src/tests/mod.rs", "mod support;\n");
    tree.write(
        "crates/project/src/tests/support.rs",
        "pub fn fixture() -> bool { false }\n",
    );
    let first = interpreter_content_hash(tree.path()).expect("test module tree must hash");

    tree.write(
        "crates/project/src/tests/support.rs",
        "pub fn fixture() -> bool { true }\n",
    );
    let changed = interpreter_content_hash(tree.path()).expect("changed test helper must hash");
    assert_eq!(first, changed);
}

#[test]
fn every_project_source_on_disk_is_hash_covered() {
    let workspace_root = workspace_root();
    let hashed = hashed_source_paths(&workspace_root)
        .expect("checked-in source paths must be collectable")
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut disk_sources = Vec::new();
    collect_rust_files(
        &workspace_root.join("crates/project/src"),
        &mut disk_sources,
    );

    for source in disk_sources {
        let relative_path = workspace_relative(&workspace_root, &source);
        let excluded = excluded_source_reason(&workspace_root, &source)
            .expect("source exclusion must be inspectable");
        if excluded.is_none() {
            assert!(
                hashed.contains(&relative_path),
                "project source {relative_path} is not content-hash covered"
            );
        }
    }
}

#[test]
fn cfg_test_gated_sources_are_excluded_and_hashed_sources_are_not_test_gated() {
    let workspace_root = workspace_root();
    let hashed = hashed_source_paths(&workspace_root)
        .expect("checked-in source paths must be collectable")
        .into_iter()
        .collect::<BTreeSet<_>>();

    for (relative_path, parent_module, module_declaration, reason) in cfg_test_source_exclusions() {
        assert!(
            !reason.trim().is_empty(),
            "cfg(test) exclusion {relative_path} must have a justification"
        );
        assert!(
            !hashed.contains(relative_path),
            "cfg(test)-gated source {relative_path} must not be hashed"
        );
        assert_cfg_test_gated(&workspace_root.join(parent_module), module_declaration);
    }

    let gated_sources = discover_cfg_test_module_sources(&workspace_root);
    let accidentally_hashed = hashed
        .intersection(&gated_sources)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        accidentally_hashed.is_empty(),
        "cfg(test)-gated external modules entered the hash: {accidentally_hashed:?}"
    );
    assert!(
        hashed.contains("crates/adapters/src/schema_v2/protocol.rs"),
        "schema-v2 interpreter support must remain hashed"
    );
}

#[test]
fn manifest_parser_fails_loudly_below_the_checked_in_floor() {
    let tree = SampleTree::empty();
    tree.write("crates/adapters/src/lib.rs", "fn interpret() {}\n");
    tree.write(
        "manifests/mainnet/undersized.toml",
        concat!(
            "[[abi.events]]\n",
            "name = \"OnlyEvent\"\n",
            "fragment = \"event OnlyEvent()\"\n",
            "normalized_events = [\"OnlyEvent\"]\n",
        ),
    );

    let error =
        interpreter_content_hash(tree.path()).expect_err("an undersized manifest corpus must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("found 1 event blocks across 1 manifests"),
        "parser-floor failure must report the observed counts: {error}"
    );
}

struct SampleTree {
    root: PathBuf,
}

impl SampleTree {
    fn new() -> Self {
        let tree = Self::empty();
        tree.write(
            "crates/adapters/src/lib.rs",
            "pub fn interpret() -> bool { true }\n",
        );
        tree.write(
            "crates/adapters/src/schema_v2/protocol.rs",
            "fn production_support() {}\n",
        );
        tree.write(
            "crates/manifests/src/lib.rs",
            "pub fn admit() -> bool { true }\n",
        );
        tree.write(
            "crates/project/src/lib.rs",
            "#[cfg(test)]\nmod tests;\npub fn project() -> bool { true }\n",
        );
        tree.write(
            "crates/project/src/tests.rs",
            "fn test_only_baseline() {}\n",
        );
        tree.write(
            "crates/project/src/projection.rs",
            "fn derive_invalidations() {}\n",
        );
        tree.write(
            "crates/interpret/src/write/identity_names.rs",
            "fn source_priority() -> u8 { 1 }\n",
        );
        for relative_path in semantic_source_files() {
            tree.write(relative_path, "pub fn semantics() -> bool { true }\n");
        }
        tree.write_manifest_floor();
        tree
    }

    fn empty() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "bigname-content-hash-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("sample tree must be creatable");
        Self { root }
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
                    &format!("event Stable{index}0(bytes32 indexed node)"),
                    "[\"registry\"]",
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
        self.write_example_manifest_details(
            normalizer_version,
            "event Changed0(bytes32 indexed node)",
            "[\"registry\"]",
            normalized_events,
        );
    }

    fn write_example_manifest_details(
        &self,
        normalizer_version: &str,
        first_fragment: &str,
        first_emitter_roles: &str,
        first_normalized_events: &str,
    ) {
        self.write(
            "manifests/mainnet/example.toml",
            &manifest_document(
                normalizer_version,
                "Changed",
                first_fragment,
                first_emitter_roles,
                first_normalized_events,
                7,
            ),
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
    first_fragment: &str,
    first_emitter_roles: &str,
    first_normalized_events: &str,
    event_count: usize,
) -> String {
    let mut document = format!("normalizer_version = {normalizer_version:?}\n");
    for index in 0..event_count {
        let name = format!("{event_name}{index}");
        let fragment = if index == 0 {
            first_fragment.to_owned()
        } else {
            format!("event {name}(bytes32 indexed node)")
        };
        let emitter_roles = if index == 0 {
            first_emitter_roles
        } else {
            "[\"registry\"]"
        };
        let normalized_events = if index == 0 {
            first_normalized_events
        } else {
            "[\"RecordChanged\"]"
        };
        document.push_str(&format!(
            "[[abi.events]]\n\
             name = {name:?}\n\
             fragment = {fragment:?}\n\
             emitter_roles = {emitter_roles}\n\
             normalized_events = {normalized_events}\n"
        ));
    }
    document
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| {
            panic!(
                "failed to read an entry in {}: {error}",
                directory.display()
            )
        });
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn workspace_relative(workspace_root: &Path, path: &Path) -> String {
    path.strip_prefix(workspace_root)
        .unwrap_or_else(|_| {
            panic!(
                "{} must be below {}",
                path.display(),
                workspace_root.display()
            )
        })
        .to_string_lossy()
        .replace('\\', "/")
}

fn assert_cfg_test_gated(parent_module: &Path, module_declaration: &str) {
    let contents = fs::read_to_string(parent_module)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", parent_module.display()));
    let lines = contents.lines().collect::<Vec<_>>();
    let declaration_index = lines
        .iter()
        .position(|line| line.trim() == module_declaration)
        .unwrap_or_else(|| {
            panic!(
                "{} does not declare {module_declaration}",
                parent_module.display()
            )
        });
    let attributes = lines[..declaration_index]
        .iter()
        .rev()
        .map(|line| line.trim())
        .take_while(|line| line.is_empty() || line.starts_with("#["))
        .collect::<Vec<_>>();
    assert!(
        attributes.contains(&"#[cfg(test)]"),
        "{} declaration {module_declaration} is not #[cfg(test)]-gated",
        parent_module.display()
    );
}

fn discover_cfg_test_module_sources(workspace_root: &Path) -> BTreeSet<String> {
    let mut source_files = Vec::new();
    collect_rust_files(
        &workspace_root.join("crates/adapters/src"),
        &mut source_files,
    );
    collect_rust_files(
        &workspace_root.join("crates/manifests/src"),
        &mut source_files,
    );
    collect_rust_files(
        &workspace_root.join("crates/project/src"),
        &mut source_files,
    );
    collect_rust_files(
        &workspace_root.join("crates/interpret/src"),
        &mut source_files,
    );
    let mut gated_sources = BTreeSet::new();

    for parent_module in source_files {
        let contents = fs::read_to_string(&parent_module)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", parent_module.display()));
        let mut attributes = Vec::new();
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("#[") {
                attributes.push(trimmed.to_owned());
                continue;
            }
            if trimmed.is_empty() || trimmed.starts_with("//") {
                continue;
            }
            if trimmed.ends_with(';')
                && let Some(module_name) = external_module_name(trimmed)
                && attributes
                    .iter()
                    .any(|attribute| attribute == "#[cfg(test)]")
            {
                let explicit_path = attributes
                    .iter()
                    .find_map(|attribute| path_attribute(attribute));
                let module_path =
                    resolve_external_module(&parent_module, module_name, explicit_path.as_deref());
                gated_sources.insert(workspace_relative(workspace_root, &module_path));
            }
            attributes.clear();
        }
    }
    gated_sources
}

fn external_module_name(declaration: &str) -> Option<&str> {
    let words = declaration.split_whitespace().collect::<Vec<_>>();
    let mod_index = words.iter().position(|word| *word == "mod")?;
    words
        .get(mod_index + 1)
        .map(|name| name.trim_end_matches(';'))
}

fn path_attribute(attribute: &str) -> Option<String> {
    attribute
        .strip_prefix("#[path = \"")
        .and_then(|path| path.strip_suffix("\"]"))
        .map(str::to_owned)
}

fn resolve_external_module(
    parent_module: &Path,
    module_name: &str,
    explicit_path: Option<&str>,
) -> PathBuf {
    if let Some(explicit_path) = explicit_path {
        let path = parent_module
            .parent()
            .expect("module source must have a parent")
            .join(explicit_path);
        assert!(
            path.is_file(),
            "cfg(test) module path {} does not exist",
            path.display()
        );
        return path;
    }

    let parent_directory = parent_module
        .parent()
        .expect("module source must have a parent");
    let file_name = parent_module
        .file_name()
        .and_then(|name| name.to_str())
        .expect("module source must have a UTF-8 file name");
    let module_directory = if matches!(file_name, "lib.rs" | "main.rs" | "mod.rs") {
        parent_directory.to_owned()
    } else {
        parent_directory.join(
            parent_module
                .file_stem()
                .expect("module source must have a file stem"),
        )
    };
    let candidates = [
        module_directory.join(format!("{module_name}.rs")),
        module_directory.join(module_name).join("mod.rs"),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| {
            panic!(
                "could not resolve cfg(test) module {module_name} declared by {}",
                parent_module.display()
            )
        })
}
