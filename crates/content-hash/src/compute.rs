use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

use alloy_primitives::{hex, keccak256};

use crate::{
    classes::{
        ADAPTER_SOURCE_ROOT, Class, MANIFEST_AUTHORITY_SOURCE_ROOT, PROJECT_SOURCE_ROOT, ROOTS,
        SEMANTIC_SOURCE_FILES,
    },
    source_paths, storage_families,
};

const MANIFEST_ROOT: &str = "manifests";
/// Watched and scanned whole, but only its composition files are hashed (`storage_families`).
const STORAGE_FAMILIES_SOURCE_ROOT: &str = storage_families::ROOT;
const MINIMUM_MANIFEST_EVENT_COUNT: usize = 111;
const MINIMUM_EVENT_MANIFEST_COUNT: usize = 16;
/// v4 since the projection inputs left it, which also keeps it unequal to every combined value.
const INTERPRETER_HASH_FORMAT: &[u8] = b"bigname-interpreter-content-v4\0";
const PROJECTION_HASH_FORMAT: &[u8] = b"bigname-projection-content-v1\0";
/// The key the projection hash embeds the interpreter hash under. No collected key can take it,
/// since every collected key has a `source:`, `manifest-event:` or `semantic-crate:` prefix.
const INTERPRETER_HASH_KEY: &str = "interpreter-content-hash";
const MANIFEST_PROFILE_HASH_FORMAT: &[u8] = b"bigname-manifest-profile-v1\0";

// `apps/phase-runner` is deliberately outside these roots: it may orchestrate phase work, but
// semantic interpretation or projection code must never live there.

#[allow(dead_code)]
struct CfgTestSourceExclusion {
    relative_path: &'static str,
    parent_module: &'static str,
    module_declaration: &'static str,
    reason: &'static str,
}

const CFG_TEST_SOURCE_EXCLUSIONS: &[CfgTestSourceExclusion] = &[];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Input {
    pub(crate) key: String,
    pub(crate) content: Vec<u8>,
    pub(crate) class: Class,
}

/// Both content hashes of a tree, with the inputs they were computed from.
pub(crate) struct Hashes {
    pub(crate) interpreter: String,
    pub(crate) projection: String,
    pub(crate) inputs: Vec<Input>,
}

impl Hashes {
    /// One `class\tkey\tkeccak256(content)` line per input, sorted by key, under a header.
    pub(crate) fn inputs_tsv(&self) -> String {
        let mut tsv = String::from("class\tkey\tcontent_keccak256\n");
        for input in &self.inputs {
            tsv.push_str(&format!(
                "{}\t{}\tkeccak256:{}\n",
                input.class.as_str(),
                input.key,
                hex::encode(keccak256(&input.content))
            ));
        }
        tsv
    }
}

pub(crate) fn semantic_paths() -> Vec<&'static str> {
    SEMANTIC_SOURCE_FILES
        .iter()
        .map(|entry| entry.path)
        .collect()
}

#[allow(dead_code)]
pub(crate) fn watched_paths(workspace_root: &Path) -> Vec<PathBuf> {
    let mut paths = ROOTS
        .iter()
        .map(|root| workspace_root.join(root.path))
        .collect::<Vec<_>>();
    paths.push(workspace_root.join(MANIFEST_ROOT));
    paths.push(workspace_root.join(STORAGE_FAMILIES_SOURCE_ROOT));
    // Not all hashed, but walked: a module declaration anywhere in these crates, or in a crate
    // holding a file hashed by name, can change which files are test-only or what a hashed file
    // compiles, so it has to trigger a rebuild. A target root or `#[path]` file can sit outside
    // `src/`, so every directory the walk reads is watched too. A walk that fails here fails the
    // hash below, and a failed build script always reruns.
    if let Ok(walked) =
        source_paths::walk_crates(workspace_root, CRATE_SOURCE_ROOTS, &semantic_paths())
    {
        for source_root in walked.roots.iter().map(|root| workspace_root.join(root)) {
            paths.extend(source_root.parent().map(|parent| parent.join("Cargo.toml")));
            paths.push(source_root);
        }
        paths.extend(
            walked
                .files
                .iter()
                .filter_map(|file| file.parent().map(Path::to_owned)),
        );
        // A data read from a hashed file is hashed, so its file is watched, and its directory too
        // in case it is created later. Every data read is watched, which only over-watches. An
        // absent optional `cfg_attr` or `#[path]` file, or an absent default file of a module
        // that resolves to none, may also be created later, which changes what the walk reaches.
        // An absent path is watched through its nearest existing directory, because cargo reruns
        // the build script on every build for a watched path that does not exist.
        for file in walked.data_reads.keys().filter(|file| file.is_file()) {
            paths.push(file.clone());
        }
        for file in walked.data_reads.keys().chain(&walked.missing) {
            let directory = file
                .ancestors()
                .skip(1)
                .find(|directory| directory.is_dir());
            paths.extend(directory.map(Path::to_owned));
        }
    }
    paths.extend(
        SEMANTIC_SOURCE_FILES
            .iter()
            .map(|entry| workspace_root.join(entry.path)),
    );
    paths.push(workspace_root.join(crate::lockfile::LOCKFILE));
    paths.sort();
    paths.dedup();
    paths
}

/// The interpreter hash covers every input that is not project-class. The projection hash covers
/// the project-class inputs and the interpreter hash itself, so every interpreter rotation also
/// rotates the projection hash.
pub(crate) fn compute(workspace_root: &Path) -> io::Result<Hashes> {
    let mut inputs = collect_inputs(workspace_root)?;
    inputs.sort();
    let (mut interpreter_inputs, mut projection_inputs): (Vec<_>, Vec<_>) = inputs
        .iter()
        .cloned()
        .partition(|input| input.class.feeds_interpreter_hash());
    let interpreter = hash_inputs(INTERPRETER_HASH_FORMAT, &mut interpreter_inputs);
    projection_inputs.push(Input {
        key: INTERPRETER_HASH_KEY.to_owned(),
        content: interpreter.as_bytes().to_vec(),
        class: Class::Project,
    });
    let projection = hash_inputs(PROJECTION_HASH_FORMAT, &mut projection_inputs);
    Ok(Hashes {
        interpreter,
        projection,
        inputs,
    })
}

pub(crate) fn manifest_profile_hash(manifest_root: &Path) -> io::Result<String> {
    let mut files = Vec::new();
    collect_files_with_extension(manifest_root, OsStr::new("toml"), &mut files)?;
    files.sort();
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "manifest profile {} contains no TOML manifests",
                manifest_root.display()
            ),
        ));
    }

    let mut inputs = Vec::with_capacity(files.len());
    for path in files {
        let key = relative_key(manifest_root, &path)?;
        let contents = fs::read_to_string(&path)?;
        let mut filtered = Vec::new();
        for line in contents.lines() {
            let trimmed = line.trim();
            if assignment_name(trimmed) != Some("normalizer_version") {
                filtered.extend_from_slice(line.as_bytes());
                filtered.push(b'\n');
            }
        }
        inputs.push(Input {
            key: format!("manifest:{key}"),
            content: filtered,
            class: Class::Interpret,
        });
    }

    Ok(hash_inputs(MANIFEST_PROFILE_HASH_FORMAT, &mut inputs))
}

pub(crate) fn is_hidden_directory_name(name: &OsStr) -> bool {
    name.as_encoded_bytes().starts_with(b".")
}

fn hash_inputs(format: &[u8], inputs: &mut [Input]) -> String {
    inputs.sort();

    let mut encoded = Vec::new();
    encoded.extend_from_slice(format);
    append_usize(&mut encoded, inputs.len());
    for input in inputs.iter() {
        append_bytes(&mut encoded, input.key.as_bytes());
        append_bytes(&mut encoded, &input.content);
    }

    format!("keccak256:{}", hex::encode(keccak256(encoded)))
}

/// The crates holding a hashed root. Each module tree is walked from the crate's library and
/// binary target roots to find the files compiled only under `cfg(test)`.
const CRATE_SOURCE_ROOTS: &[&str] = &[
    ADAPTER_SOURCE_ROOT,
    MANIFEST_AUTHORITY_SOURCE_ROOT,
    PROJECT_SOURCE_ROOT,
    "crates/interpret/src",
    "crates/storage/src",
];

fn collect_inputs(workspace_root: &Path) -> io::Result<Vec<Input>> {
    let mut inputs = Vec::new();
    let walked = source_paths::walk_crates(workspace_root, CRATE_SOURCE_ROOTS, &semantic_paths())?;
    let cfg_test_sources = &walked.test_only;
    // Manifest declarations select interpretation inputs and supply authority for derived identity
    // and discovery rows, so the manifest root is scanned whole like the others.
    for root in ROOTS {
        collect_rust_sources(
            workspace_root,
            &workspace_root.join(root.path),
            root.class,
            cfg_test_sources,
            &mut inputs,
        )?;
    }
    let project_source_root = workspace_root.join(PROJECT_SOURCE_ROOT);
    if project_source_root.exists() {
        let mut project_sql_sources = Vec::new();
        collect_files_with_extension(
            &project_source_root,
            OsStr::new("sql"),
            &mut project_sql_sources,
        )?;
        for path in project_sql_sources {
            collect_file(workspace_root, &path, Class::Project, &mut inputs)?;
        }
    }
    collect_manifest_event_blocks(workspace_root, &mut inputs)?;
    collect_semantic_sources(workspace_root, &mut inputs)?;
    storage_families::collect(workspace_root, cfg_test_sources, &mut inputs)?;
    // This decides what joins from the complete set of hashed sources, so it must run after every
    // collector that adds a `source:` input.
    collect_data_reads(&walked, &mut inputs)?;
    crate::lockfile::collect_semantic_crate_fingerprints(workspace_root, &mut inputs)?;
    Ok(inputs)
}

/// Files a hashed file compiles in with `include_str!` or `include_bytes!`. They are part of that
/// file's semantics whatever their extension or directory, so each joins the hash once. A data
/// read from an unhashed file, such as interpret's input loader, stays out with its reader. An
/// `include!` from a hashed file must reach a hashed source, or the build fails, and so must a
/// module the walk records for a hashed file.
fn collect_data_reads(walked: &source_paths::Walked, inputs: &mut Vec<Input>) -> io::Result<()> {
    let hashed = inputs
        .iter()
        .map(|input| (input.key.clone(), input.class))
        .collect::<BTreeMap<_, _>>();
    let class_of = |key: &str| hashed.get(&format!("source:{key}")).copied();
    let is_hashed = |key: &str| class_of(key).is_some();
    walked.refuse_unhashed_inclusions(is_hashed)?;
    walked.refuse_unhashed_modules(is_hashed)?;
    let data_inputs = walked.data_inputs(class_of)?;
    for (key, file, class) in data_inputs {
        let key = format!("source:{key}");
        if !hashed.contains_key(&key) {
            inputs.push(Input {
                key,
                content: fs::read(file)?,
                class,
            });
        }
    }
    Ok(())
}

fn collect_semantic_sources(workspace_root: &Path, inputs: &mut Vec<Input>) -> io::Result<()> {
    for entry in SEMANTIC_SOURCE_FILES {
        let relative_path = entry.path;
        let path = workspace_root.join(relative_path);
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "the content hashes require semantic source {relative_path}; if those \
                     semantics moved, update SEMANTIC_SOURCE_FILES in the same change"
                ),
            ));
        }
        collect_file(workspace_root, &path, entry.class, inputs)?;
    }
    Ok(())
}

fn collect_rust_sources(
    workspace_root: &Path,
    directory: &Path,
    class: Class,
    cfg_test_sources: &BTreeSet<String>,
    inputs: &mut Vec<Input>,
) -> io::Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(workspace_root, &path, class, cfg_test_sources, inputs)?;
        } else if path.extension() == Some(OsStr::new("rs"))
            && source_exclusion(workspace_root, &path, cfg_test_sources)?.is_none()
        {
            collect_file(workspace_root, &path, class, inputs)?;
        }
    }
    Ok(())
}

pub(crate) fn source_exclusion(
    workspace_root: &Path,
    path: &Path,
    cfg_test_sources: &BTreeSet<String>,
) -> io::Result<Option<&'static str>> {
    let relative_path = relative_key(workspace_root, path)?;
    if let Some(exclusion) = CFG_TEST_SOURCE_EXCLUSIONS
        .iter()
        .find(|exclusion| exclusion.relative_path == relative_path)
    {
        return Ok(Some(exclusion.reason));
    }

    if cfg_test_sources.contains(&relative_path) {
        return Ok(Some("cfg(test)-gated external module"));
    }

    Ok(None)
}

pub(crate) fn collect_file(
    workspace_root: &Path,
    path: &Path,
    class: Class,
    inputs: &mut Vec<Input>,
) -> io::Result<()> {
    let key = relative_key(workspace_root, path)?;
    inputs.push(Input {
        key: format!("source:{key}"),
        content: fs::read(path)?,
        class,
    });
    Ok(())
}

fn collect_manifest_event_blocks(workspace_root: &Path, inputs: &mut Vec<Input>) -> io::Result<()> {
    let manifest_root = workspace_root.join(MANIFEST_ROOT);
    let mut files = Vec::new();
    let mut profile_entries = fs::read_dir(&manifest_root)?.collect::<Result<Vec<_>, _>>()?;
    profile_entries.sort_by_key(|entry| entry.file_name());
    for entry in profile_entries {
        let name = entry.file_name();
        let path = entry.path();
        if path.is_dir() {
            if !is_hidden_directory_name(&name) {
                collect_files_with_extension(&path, OsStr::new("toml"), &mut files)?;
            }
        } else if path.extension() == Some(OsStr::new("toml")) {
            files.push(path);
        }
    }
    files.sort();

    let mut event_count = 0usize;
    let mut manifest_count = 0usize;
    for path in files {
        let relative_path = relative_key(workspace_root, &path)?;
        let contents = fs::read_to_string(&path)?;
        let mut current_event = None;
        let mut event_index = 0usize;
        let mut manifest_has_events = false;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                if let Some(event) = current_event.take() {
                    finish_manifest_event(&relative_path, event_index, event, inputs)?;
                }
                if trimmed == "[[abi.events]]" {
                    event_index += 1;
                    event_count += 1;
                    manifest_has_events = true;
                    let mut event = ManifestEventBlock::default();
                    event.push(trimmed);
                    current_event = Some(event);
                }
                continue;
            }
            if let Some(event) = current_event.as_mut()
                && !trimmed.is_empty()
                && !trimmed.starts_with('#')
            {
                event.push(trimmed);
                if assignment_name(trimmed) == Some("fragment") {
                    event.has_fragment = true;
                }
            }
        }
        if let Some(event) = current_event {
            finish_manifest_event(&relative_path, event_index, event, inputs)?;
        }
        if manifest_has_events {
            manifest_count += 1;
        }
    }

    if event_count < MINIMUM_MANIFEST_EVENT_COUNT || manifest_count < MINIMUM_EVENT_MANIFEST_COUNT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "manifest ABI event parser found {event_count} event blocks across \
                 {manifest_count} manifests; expected at least {MINIMUM_MANIFEST_EVENT_COUNT} \
                 event blocks across {MINIMUM_EVENT_MANIFEST_COUNT} manifests"
            ),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ManifestEventBlock {
    content: Vec<u8>,
    has_fragment: bool,
}

impl ManifestEventBlock {
    fn push(&mut self, line: &str) {
        self.content.extend_from_slice(line.as_bytes());
        self.content.push(b'\n');
    }
}

fn finish_manifest_event(
    relative_path: &str,
    event_index: usize,
    event: ManifestEventBlock,
    inputs: &mut Vec<Input>,
) -> io::Result<()> {
    if !event.has_fragment {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "manifest ABI event block {event_index} in {relative_path} has no fragment field"
            ),
        ));
    }
    inputs.push(Input {
        key: format!("manifest-event:{relative_path}:{event_index}"),
        content: event.content,
        class: Class::Interpret,
    });
    Ok(())
}

pub(crate) fn assignment_name(line: &str) -> Option<&str> {
    line.split_once('=').map(|(name, _)| name.trim())
}
fn collect_files_with_extension(
    directory: &Path,
    extension: &OsStr,
    files: &mut Vec<PathBuf>,
) -> io::Result<()> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files_with_extension(&path, extension, files)?;
        } else if path.extension() == Some(extension) {
            files.push(path);
        }
    }
    Ok(())
}

pub(crate) fn relative_key(workspace_root: &Path, path: &Path) -> io::Result<String> {
    path.strip_prefix(workspace_root)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} is outside workspace root {}",
                    path.display(),
                    workspace_root.display()
                ),
            )
        })
}

fn append_bytes(output: &mut Vec<u8>, value: &[u8]) {
    append_usize(output, value.len());
    output.extend_from_slice(value);
}

fn append_usize(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(&(value as u64).to_be_bytes());
}

#[cfg(test)]
pub(crate) fn semantic_source_files() -> Vec<&'static str> {
    semantic_paths()
}

#[cfg(test)]
pub(crate) fn hashed_source_paths(workspace_root: &Path) -> io::Result<Vec<String>> {
    collect_inputs(workspace_root).map(|inputs| {
        inputs
            .into_iter()
            .filter_map(|input| input.key.strip_prefix("source:").map(str::to_owned))
            .collect()
    })
}

#[cfg(test)]
pub(crate) fn classed_inputs(workspace_root: &Path) -> io::Result<Vec<Input>> {
    compute(workspace_root).map(|hashes| hashes.inputs)
}

#[cfg(test)]
pub(crate) fn module_keys(
    workspace_root: &Path,
) -> io::Result<std::collections::BTreeMap<(String, Vec<String>), String>> {
    source_paths::walk_crates(workspace_root, CRATE_SOURCE_ROOTS, &semantic_paths())?.module_keys()
}

#[cfg(test)]
pub(crate) fn crate_source_roots() -> &'static [&'static str] {
    CRATE_SOURCE_ROOTS
}

#[cfg(test)]
pub(crate) fn cfg_test_source_set(workspace_root: &Path) -> io::Result<BTreeSet<String>> {
    source_paths::walk_crates(workspace_root, CRATE_SOURCE_ROOTS, &semantic_paths())
        .map(|walked| walked.test_only)
}

#[cfg(test)]
pub(crate) fn excluded_source_reason(
    workspace_root: &Path,
    path: &Path,
) -> io::Result<Option<&'static str>> {
    source_exclusion(workspace_root, path, &cfg_test_source_set(workspace_root)?)
}

#[cfg(test)]
pub(crate) fn cfg_test_source_exclusions()
-> impl Iterator<Item = (&'static str, &'static str, &'static str, &'static str)> {
    CFG_TEST_SOURCE_EXCLUSIONS.iter().map(|exclusion| {
        (
            exclusion.relative_path,
            exclusion.parent_module,
            exclusion.module_declaration,
            exclusion.reason,
        )
    })
}
