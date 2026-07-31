use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

use alloy_primitives::{hex, keccak256};

const ADAPTER_SOURCE_ROOT: &str = "crates/adapters/src";
const MANIFEST_AUTHORITY_SOURCE_ROOT: &str = "crates/manifests/src";
const MANIFEST_ROOT: &str = "manifests";
const WORKER_SOURCE_ROOT: &str = "apps/worker/src";
const MINIMUM_MANIFEST_EVENT_COUNT: usize = 111;
const MINIMUM_EVENT_MANIFEST_COUNT: usize = 16;
const HASH_FORMAT: &[u8] = b"bigname-interpreter-content-v3\0";

struct SourceExclusion {
    relative_path: &'static str,
    includes_descendants: bool,
    reason: &'static str,
}

const WORKER_SOURCE_EXCLUSIONS: &[SourceExclusion] = &[
    SourceExclusion {
        relative_path: "main.rs",
        includes_descendants: false,
        reason: "worker binary entrypoint wiring",
    },
    SourceExclusion {
        relative_path: "cli.rs",
        includes_descendants: false,
        reason: "worker CLI declarations",
    },
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
    SourceExclusion {
        relative_path: "runtime.rs",
        includes_descendants: false,
        reason: "worker runtime and observability wiring",
    },
    SourceExclusion {
        relative_path: "healthcheck.rs",
        includes_descendants: false,
        reason: "worker healthcheck wiring",
    },
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

struct CfgTestSourceExclusion {
    relative_path: &'static str,
    reason: &'static str,
}

const CFG_TEST_SOURCE_EXCLUSIONS: &[CfgTestSourceExclusion] = &[
    CfgTestSourceExclusion {
        relative_path: "crates/adapters/src/ens_v2_resolver/testsupport.rs",
        reason: "cfg(test)-gated ENSv2 resolver test support",
    },
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/primary_name/projection/test_hooks.rs",
        reason: "cfg(test)-gated primary-name projection hooks",
    },
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/primary_name/hydration/test_hooks.rs",
        reason: "cfg(test)-gated primary-name hydration hooks",
    },
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/record_inventory/hydration_tests_support.rs",
        reason: "cfg(test)-gated record-inventory hydration support",
    },
    CfgTestSourceExclusion {
        relative_path: "apps/worker/src/replay/staging/fingerprint.rs",
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
        workspace_root.join(WORKER_SOURCE_ROOT),
    ]
}

pub(crate) fn compute(workspace_root: &Path) -> io::Result<String> {
    let mut inputs = collect_inputs(workspace_root)?;
    inputs.sort();

    let mut encoded = Vec::new();
    encoded.extend_from_slice(HASH_FORMAT);
    append_usize(&mut encoded, inputs.len());
    for input in inputs {
        append_bytes(&mut encoded, input.key.as_bytes());
        append_bytes(&mut encoded, &input.content);
    }

    Ok(format!("keccak256:{}", hex::encode(keccak256(encoded))))
}

fn collect_inputs(workspace_root: &Path) -> io::Result<Vec<Input>> {
    let mut inputs = Vec::new();
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(ADAPTER_SOURCE_ROOT),
        &mut inputs,
    )?;
    // Registry interpretation delegates discovery and identity writes to the manifest crate.
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(MANIFEST_AUTHORITY_SOURCE_ROOT),
        &mut inputs,
    )?;
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(WORKER_SOURCE_ROOT),
        &mut inputs,
    )?;
    collect_manifest_event_blocks(workspace_root, &mut inputs)?;
    Ok(inputs)
}

fn collect_rust_sources(
    workspace_root: &Path,
    directory: &Path,
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
            collect_rust_sources(workspace_root, &path, inputs)?;
        } else if path.extension() == Some(OsStr::new("rs"))
            && source_exclusion(workspace_root, &path)?.is_none()
        {
            collect_file(workspace_root, &path, inputs)?;
        }
    }
    Ok(())
}

fn source_exclusion(workspace_root: &Path, path: &Path) -> io::Result<Option<&'static str>> {
    let relative_path = relative_key(workspace_root, path)?;
    if let Some(exclusion) = CFG_TEST_SOURCE_EXCLUSIONS
        .iter()
        .find(|exclusion| exclusion.relative_path == relative_path)
    {
        return Ok(Some(exclusion.reason));
    }

    let source_relative_path = if let Some(path) = relative_path.strip_prefix(ADAPTER_SOURCE_ROOT) {
        path.trim_start_matches('/')
    } else if let Some(path) = relative_path.strip_prefix(MANIFEST_AUTHORITY_SOURCE_ROOT) {
        path.trim_start_matches('/')
    } else if let Some(path) = relative_path.strip_prefix(WORKER_SOURCE_ROOT) {
        path.trim_start_matches('/')
    } else {
        return Ok(None);
    };
    let source_path = Path::new(source_relative_path);
    // Conventionally named external test modules are not production hash inputs.
    if source_path
        .components()
        .any(|component| component.as_os_str() == OsStr::new("tests"))
        || source_path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
    {
        return Ok(Some("conventionally named external test module"));
    }

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
    collect_files_with_extension(&manifest_root, OsStr::new("toml"), &mut files)?;
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
