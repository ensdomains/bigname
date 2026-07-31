use std::{io, path::Path};

include!(concat!(env!("OUT_DIR"), "/interpreter_content_hash.rs"));

/// Compute the interpreter content hash for a source tree.
pub fn interpreter_content_hash(workspace_root: impl AsRef<Path>) -> io::Result<String> {
    super::interpreter_content_hash_impl::compute(workspace_root.as_ref())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;
    use crate::interpreter_content_hash_impl::{
        cfg_test_source_exclusions, excluded_source_reason, hashed_source_paths,
    };

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
    fn hash_is_stable_and_changes_for_interpreter_projection_and_mapping_content() {
        let tree = SampleTree::new();

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
            "apps/worker/src/projection_apply/derive.rs",
            "fn derive_invalidations() { let changed = true; }\n",
        );
        let builder_change =
            interpreter_content_hash(tree.path()).expect("builder hash must compute");
        assert_ne!(
            first, builder_change,
            "invalidation derivation must affect the hash"
        );

        tree.write(
            "apps/worker/src/projection_apply/derive.rs",
            "fn derive_invalidations() {}\n",
        );
        tree.write_example_manifest(
            "ensip15@new",
            "event Changed(bytes32 indexed node)",
            "[\"registry\"]",
            "[\"RecordChanged\"]",
        );
        let normalizer_change =
            interpreter_content_hash(tree.path()).expect("normalizer hash must compute");
        assert_eq!(
            first, normalizer_change,
            "the normalizer version is owned by flag recomputation"
        );

        tree.write_example_manifest(
            "ensip15@old",
            "event Changed(bytes32 indexed node, address owner)",
            "[\"registry\"]",
            "[\"RecordChanged\"]",
        );
        let event_change = interpreter_content_hash(tree.path()).expect("event hash must compute");
        assert_ne!(
            first, event_change,
            "manifest ABI event fragments must affect the hash"
        );

        tree.write_example_manifest(
            "ensip15@old",
            "event Changed(bytes32 indexed node)",
            "[\"resolver\"]",
            "[\"RecordChanged\"]",
        );
        let emitter_mapping_change =
            interpreter_content_hash(tree.path()).expect("emitter mapping hash must compute");
        assert_ne!(
            first, emitter_mapping_change,
            "manifest emitter-role mappings must affect the hash"
        );

        tree.write_example_manifest(
            "ensip15@old",
            "event Changed(bytes32 indexed node)",
            "[\"registry\"]",
            "[\"ResolverChanged\"]",
        );
        let normalized_mapping_change =
            interpreter_content_hash(tree.path()).expect("normalized mapping hash must compute");
        assert_ne!(
            first, normalized_mapping_change,
            "manifest normalized-event mappings must affect the hash"
        );
    }

    #[test]
    fn newly_added_adapter_and_worker_modules_affect_the_hash() {
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

        let worker_tree = SampleTree::new();
        let worker_before =
            interpreter_content_hash(worker_tree.path()).expect("worker baseline must hash");
        worker_tree.write(
            "apps/worker/src/future_projection.rs",
            "fn build_future_projection() {}\n",
        );
        let worker_after =
            interpreter_content_hash(worker_tree.path()).expect("new worker module must hash");
        assert_ne!(
            worker_before, worker_after,
            "a new non-excluded worker source file must enter the hash automatically"
        );
    }

    #[test]
    fn manifest_authority_source_changes_affect_the_hash() {
        let tree = SampleTree::new();
        let before =
            interpreter_content_hash(tree.path()).expect("manifest-authority baseline must hash");
        tree.write(
            "crates/manifests/src/lib/discovery/reconciliation.rs",
            "fn reconcile_discovery_output() {}\n",
        );
        let after =
            interpreter_content_hash(tree.path()).expect("new manifest-authority source must hash");
        assert_ne!(
            before, after,
            "manifest discovery reconciliation changes interpreter output and must affect the hash"
        );
    }

    #[test]
    fn excluded_sources_are_insensitive_but_production_support_is_hashed() {
        let tree = SampleTree::new();
        let first = interpreter_content_hash(tree.path()).expect("baseline must hash");

        tree.write("apps/worker/src/cli.rs", "struct ChangedCli;\n");
        tree.write(
            "apps/worker/src/inspect/canonicality.rs",
            "fn changed_inspection() {}\n",
        );
        tree.write(
            "apps/worker/src/name_current/tests.rs",
            "fn changed_projection_test() {}\n",
        );
        tree.write(
            "crates/adapters/src/ens_v2_resolver/testsupport.rs",
            "fn changed_resolver_test_support() {}\n",
        );
        tree.write(
            "apps/worker/src/primary_name/projection/test_hooks.rs",
            "fn changed_projection_hook() {}\n",
        );
        tree.write(
            "apps/worker/src/primary_name/hydration/test_hooks.rs",
            "fn changed_hydration_hook() {}\n",
        );
        tree.write(
            "apps/worker/src/record_inventory/hydration_tests_support.rs",
            "fn changed_hydration_support() {}\n",
        );
        let excluded_change =
            interpreter_content_hash(tree.path()).expect("excluded sources must still hash");
        assert_eq!(
            first, excluded_change,
            "CLI, inspection, conventional test, and cfg(test)-only sources must be excluded"
        );

        tree.write(
            "crates/adapters/src/normalized_event_support.rs",
            "fn production_support_changed() {}\n",
        );
        let production_support_change =
            interpreter_content_hash(tree.path()).expect("production support must hash");
        assert_ne!(
            first, production_support_change,
            "production *_support.rs files must not be blanket-excluded"
        );
    }

    #[test]
    fn every_worker_source_on_disk_is_hashed_or_has_a_documented_exclusion() {
        let workspace_root = workspace_root();
        let hashed = hashed_source_paths(&workspace_root)
            .expect("checked-in source paths must be collectable")
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut disk_sources = Vec::new();
        collect_rust_files(&workspace_root.join("apps/worker/src"), &mut disk_sources);

        for source in disk_sources {
            let relative_path = workspace_relative(&workspace_root, &source);
            if hashed.contains(&relative_path) {
                assert!(
                    excluded_source_reason(&workspace_root, &source)
                        .expect("source exclusion must be inspectable")
                        .is_none(),
                    "hashed worker source {relative_path} must not also be excluded"
                );
            } else {
                let reason = excluded_source_reason(&workspace_root, &source)
                    .expect("source exclusion must be inspectable")
                    .unwrap_or_else(|| {
                        panic!(
                            "worker source {relative_path} is neither hashed nor explicitly excluded"
                        )
                    });
                assert!(
                    !reason.trim().is_empty(),
                    "worker source exclusion {relative_path} must have a justification"
                );
            }
        }

        assert!(
            hashed.contains("apps/worker/src/projection_apply/derive.rs"),
            "invalidation derivation is projection rebuild semantics and must be hashed"
        );
    }

    #[test]
    fn cfg_test_gated_sources_are_excluded_and_hashed_sources_are_not_test_gated() {
        let workspace_root = workspace_root();
        let hashed = hashed_source_paths(&workspace_root)
            .expect("checked-in source paths must be collectable")
            .into_iter()
            .collect::<BTreeSet<_>>();

        for (relative_path, parent_module, module_declaration, reason) in
            cfg_test_source_exclusions()
        {
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
            hashed.contains("crates/adapters/src/normalized_event_support.rs"),
            "production normalized-event support must remain hashed"
        );
    }

    #[test]
    fn manifest_parser_fails_loudly_below_the_checked_in_floor() {
        let tree = SampleTree::empty();
        tree.write("crates/adapters/src/lib.rs", "fn interpret() {}\n");
        tree.write("apps/worker/src/name_current.rs", "fn project() {}\n");
        tree.write(
            "manifests/mainnet/undersized.toml",
            concat!(
                "[[abi.events]]\n",
                "name = \"OnlyEvent\"\n",
                "fragment = \"event OnlyEvent()\"\n",
                "normalized_events = [\"OnlyEvent\"]\n",
            ),
        );

        let error = interpreter_content_hash(tree.path())
            .expect_err("an undersized manifest corpus must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
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
                "crates/adapters/src/normalized_event_support.rs",
                "fn production_support() {}\n",
            );
            tree.write(
                "apps/worker/src/name_current.rs",
                "fn build_projection() {}\n",
            );
            tree.write(
                "apps/worker/src/projection_apply/derive.rs",
                "fn derive_invalidations() {}\n",
            );
            tree.write_manifest_floor();
            tree
        }

        fn empty() -> Self {
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

        fn write_manifest_floor(&self) {
            self.write_example_manifest(
                "ensip15@old",
                "event Changed(bytes32 indexed node)",
                "[\"registry\"]",
                "[\"RecordChanged\"]",
            );
            for manifest_index in 0..15 {
                self.write(
                    &format!("manifests/generated/manifest-{manifest_index:02}.toml"),
                    &manifest_document(
                        "ensip15@old",
                        7,
                        &format!("Stable{manifest_index}"),
                        &format!("event Stable{manifest_index}(bytes32 indexed node)"),
                        "[\"registry\"]",
                        "[\"RecordChanged\"]",
                    ),
                );
            }
        }

        fn write_example_manifest(
            &self,
            normalizer_version: &str,
            fragment: &str,
            emitter_roles: &str,
            normalized_events: &str,
        ) {
            self.write(
                "manifests/mainnet/example.toml",
                &manifest_document(
                    normalizer_version,
                    6,
                    "Changed",
                    fragment,
                    emitter_roles,
                    normalized_events,
                ),
            );
        }
    }

    impl Drop for SampleTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn manifest_document(
        normalizer_version: &str,
        event_count: usize,
        first_name: &str,
        first_fragment: &str,
        first_emitter_roles: &str,
        first_normalized_events: &str,
    ) -> String {
        let mut document = format!("normalizer_version = {normalizer_version:?}\n");
        for event_index in 0..event_count {
            let (name, fragment, emitter_roles, normalized_events) = if event_index == 0 {
                (
                    first_name.to_owned(),
                    first_fragment.to_owned(),
                    first_emitter_roles.to_owned(),
                    first_normalized_events.to_owned(),
                )
            } else {
                (
                    format!("{first_name}{event_index}"),
                    format!("event {first_name}{event_index}()"),
                    "[\"registry\"]".to_owned(),
                    "[\"RecordChanged\"]".to_owned(),
                )
            };
            document.push_str(&format!(
                concat!(
                    "\n[[abi.events]]\n",
                    "name = {name:?}\n",
                    "fragment = {fragment:?}\n",
                    "emitter_roles = {emitter_roles}\n",
                    "normalized_events = {normalized_events}\n",
                ),
                name = name,
                fragment = fragment,
                emitter_roles = emitter_roles,
                normalized_events = normalized_events,
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
        collect_rust_files(&workspace_root.join("apps/worker/src"), &mut source_files);
        let mut gated_sources = BTreeSet::new();

        for parent_module in source_files {
            let contents = fs::read_to_string(&parent_module).unwrap_or_else(|error| {
                panic!("failed to read {}: {error}", parent_module.display())
            });
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
                    let module_path = resolve_external_module(
                        &parent_module,
                        module_name,
                        explicit_path.as_deref(),
                    );
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
}
