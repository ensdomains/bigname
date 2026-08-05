use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

use alloy_primitives::{hex, keccak256};

use crate::source_paths;

const ADAPTER_SOURCE_ROOT: &str = "crates/adapters/src";
const MANIFEST_AUTHORITY_SOURCE_ROOT: &str = "crates/manifests/src";
const MANIFEST_ROOT: &str = "manifests";
const PROJECT_SOURCE_ROOT: &str = "crates/project/src";
const WORKER_SOURCE_ROOT: &str = "apps/worker/src";
const MINIMUM_MANIFEST_EVENT_COUNT: usize = 111;
const MINIMUM_EVENT_MANIFEST_COUNT: usize = 16;
const HASH_FORMAT: &[u8] = b"bigname-interpreter-content-v3\0";
const MANIFEST_PROFILE_HASH_FORMAT: &[u8] = b"bigname-manifest-profile-v1\0";

// `apps/phase-runner` is deliberately outside these roots: it may orchestrate phase work, but
// semantic interpretation or projection code must never live there.

struct SourceExclusion {
    relative_path: &'static str,
    includes_descendants: bool,
    reason: &'static str,
}

const WORKER_SOURCE_EXCLUSIONS: &[SourceExclusion] = &[
    // The binary entrypoint only parses the CLI and starts runtime wiring.
    SourceExclusion {
        relative_path: "main.rs",
        includes_descendants: false,
        reason: "worker binary entrypoint wiring",
    },
    // clap declarations select commands but do not interpret or project indexed facts.
    SourceExclusion {
        relative_path: "cli.rs",
        includes_descendants: false,
        reason: "worker CLI declarations",
    },
    // Command dispatch and its submodules only connect command-line requests to owned behavior.
    SourceExclusion {
        relative_path: "commands.rs",
        includes_descendants: false,
        reason: "worker CLI command dispatch",
    },
    SourceExclusion {
        relative_path: "commands",
        includes_descendants: true,
        reason: "worker CLI command handlers",
    },
    // Tracing, metrics, and listener setup do not change interpreter output.
    SourceExclusion {
        relative_path: "runtime.rs",
        includes_descendants: false,
        reason: "worker runtime and observability wiring",
    },
    // The healthcheck reads service state but does not derive or apply indexed state.
    SourceExclusion {
        relative_path: "healthcheck.rs",
        includes_descendants: false,
        reason: "worker healthcheck wiring",
    },
    // Inspect commands are read-only operational views over already persisted state.
    SourceExclusion {
        relative_path: "inspect.rs",
        includes_descendants: false,
        reason: "worker inspection command wiring",
    },
    SourceExclusion {
        relative_path: "inspect",
        includes_descendants: true,
        reason: "worker read-only inspection implementations",
    },
];

#[allow(dead_code)]
struct CfgTestSourceExclusion {
    relative_path: &'static str,
    parent_module: &'static str,
    module_declaration: &'static str,
    reason: &'static str,
}

const CFG_TEST_SOURCE_EXCLUSIONS: &[CfgTestSourceExclusion] = &[
    // Projection rebuild hooks are compiled only for worker tests.
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/primary_name/projection/test_hooks.rs",
        parent_module: "apps/worker/src/primary_name/projection.rs",
        module_declaration: "pub(crate) mod test_hooks;",
        reason: "cfg(test)-gated primary-name projection hooks",
    },
    // Hydration hooks are compiled only for worker tests.
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/primary_name/hydration/test_hooks.rs",
        parent_module: "apps/worker/src/primary_name/hydration.rs",
        module_declaration: "pub(crate) mod test_hooks;",
        reason: "cfg(test)-gated primary-name hydration hooks",
    },
    // Record hydration seed helpers are compiled only for worker tests.
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/record_inventory/hydration_tests_support.rs",
        parent_module: "apps/worker/src/record_inventory/hydration.rs",
        module_declaration: "pub(super) mod tests_support;",
        reason: "cfg(test)-gated record-inventory hydration support",
    },
    // The staging fingerprint exists only to regression-test the durable staging contract.
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/replay/staging/fingerprint.rs",
        parent_module: "apps/worker/src/replay/staging.rs",
        module_declaration: "pub(crate) mod fingerprint;",
        reason: "cfg(test)-gated projection staging fingerprint",
    },
];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Input {
    key: String,
    content: Vec<u8>,
}

#[allow(dead_code)]
pub(crate) fn watched_paths(workspace_root: &Path) -> Vec<PathBuf> {
    vec![
        workspace_root.join(ADAPTER_SOURCE_ROOT),
        workspace_root.join(MANIFEST_AUTHORITY_SOURCE_ROOT),
        workspace_root.join(MANIFEST_ROOT),
        workspace_root.join(PROJECT_SOURCE_ROOT),
        workspace_root.join(WORKER_SOURCE_ROOT),
    ]
}

pub(crate) fn compute(workspace_root: &Path) -> io::Result<String> {
    let mut inputs = collect_inputs(workspace_root)?;
    Ok(hash_inputs(HASH_FORMAT, &mut inputs))
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

fn collect_inputs(workspace_root: &Path) -> io::Result<Vec<Input>> {
    let mut inputs = Vec::new();
    let cfg_test_sources = source_paths::cfg_test_sources(
        workspace_root,
        &[
            ADAPTER_SOURCE_ROOT,
            MANIFEST_AUTHORITY_SOURCE_ROOT,
            PROJECT_SOURCE_ROOT,
            WORKER_SOURCE_ROOT,
        ],
    )?;
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(ADAPTER_SOURCE_ROOT),
        &cfg_test_sources,
        &mut inputs,
    )?;
    // Manifest declarations select interpretation inputs and supply authority for derived
    // identity and discovery rows. Scan the whole production source tree so a manifest-authority
    // change cannot silently change interpreter output without changing the content hash.
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(MANIFEST_AUTHORITY_SOURCE_ROOT),
        &cfg_test_sources,
        &mut inputs,
    )?;
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(WORKER_SOURCE_ROOT),
        &cfg_test_sources,
        &mut inputs,
    )?;
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(PROJECT_SOURCE_ROOT),
        &cfg_test_sources,
        &mut inputs,
    )?;
    collect_manifest_event_blocks(workspace_root, &mut inputs)?;
    Ok(inputs)
}

fn collect_rust_sources(
    workspace_root: &Path,
    directory: &Path,
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
            collect_rust_sources(workspace_root, &path, cfg_test_sources, inputs)?;
        } else if path.extension() == Some(OsStr::new("rs"))
            && source_exclusion(workspace_root, &path, cfg_test_sources)?.is_none()
        {
            collect_file(workspace_root, &path, inputs)?;
        }
    }
    Ok(())
}

fn source_exclusion(
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

    let source_relative_path = if let Some(path) = relative_path.strip_prefix(ADAPTER_SOURCE_ROOT) {
        path.trim_start_matches('/')
    } else if let Some(path) = relative_path.strip_prefix(MANIFEST_AUTHORITY_SOURCE_ROOT) {
        path.trim_start_matches('/')
    } else if let Some(path) = relative_path.strip_prefix(WORKER_SOURCE_ROOT) {
        path.trim_start_matches('/')
    } else if let Some(path) = relative_path.strip_prefix(PROJECT_SOURCE_ROOT) {
        path.trim_start_matches('/')
    } else {
        return Ok(None);
    };
    if relative_path.starts_with(WORKER_SOURCE_ROOT) {
        return Ok(WORKER_SOURCE_EXCLUSIONS
            .iter()
            .find(|exclusion| exclusion.matches(source_relative_path))
            .map(|exclusion| exclusion.reason));
    }
    Ok(None)
}

fn collect_file(workspace_root: &Path, path: &Path, inputs: &mut Vec<Input>) -> io::Result<()> {
    let key = relative_key(workspace_root, path)?;
    inputs.push(Input {
        key: format!("source:{key}"),
        content: fs::read(path)?,
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
    });
    Ok(())
}

fn assignment_name(line: &str) -> Option<&str> {
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

fn relative_key(workspace_root: &Path, path: &Path) -> io::Result<String> {
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

impl SourceExclusion {
    fn matches(&self, relative_path: &str) -> bool {
        relative_path == self.relative_path
            || (self.includes_descendants
                && relative_path
                    .strip_prefix(self.relative_path)
                    .is_some_and(|suffix| suffix.starts_with('/')))
    }
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
pub(crate) fn excluded_source_reason(
    workspace_root: &Path,
    path: &Path,
) -> io::Result<Option<&'static str>> {
    let cfg_test_sources = source_paths::cfg_test_sources(
        workspace_root,
        &[
            ADAPTER_SOURCE_ROOT,
            MANIFEST_AUTHORITY_SOURCE_ROOT,
            PROJECT_SOURCE_ROOT,
            WORKER_SOURCE_ROOT,
        ],
    )?;
    source_exclusion(workspace_root, path, &cfg_test_sources)
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
