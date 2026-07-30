use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

use alloy_primitives::{hex, keccak256};

const ADAPTER_SOURCE_ROOT: &str = "crates/adapters/src";
const MANIFEST_ROOT: &str = "manifests";
const WORKER_SOURCE_ROOT: &str = "apps/worker/src";
const PROJECTION_MODULES: &[&str] = &[
    "address_names",
    "children",
    "name_current",
    "permissions",
    "primary_name",
    "record_inventory",
    "resolver",
];
const SHARED_PROJECTION_SOURCES: &[&str] = &["projection_json.rs"];
const HASH_FORMAT: &[u8] = b"bigname-interpreter-content-v1\0";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Input {
    key: String,
    content: Vec<u8>,
}

#[allow(dead_code)]
pub(crate) fn watched_paths(workspace_root: &Path) -> Vec<PathBuf> {
    vec![
        workspace_root.join(ADAPTER_SOURCE_ROOT),
        workspace_root.join(MANIFEST_ROOT),
        workspace_root.join(WORKER_SOURCE_ROOT),
    ]
}

pub(crate) fn compute(workspace_root: &Path) -> io::Result<String> {
    let mut inputs = Vec::new();
    collect_rust_sources(
        workspace_root,
        &workspace_root.join(ADAPTER_SOURCE_ROOT),
        &mut inputs,
    )?;
    collect_projection_sources(workspace_root, &mut inputs)?;
    collect_manifest_event_fragments(workspace_root, &mut inputs)?;
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

fn collect_projection_sources(workspace_root: &Path, inputs: &mut Vec<Input>) -> io::Result<()> {
    let worker_root = workspace_root.join(WORKER_SOURCE_ROOT);
    for module in PROJECTION_MODULES {
        let module_file = worker_root.join(format!("{module}.rs"));
        collect_file(workspace_root, &module_file, inputs)?;
        collect_rust_sources(workspace_root, &worker_root.join(module), inputs)?;
    }
    for relative_path in SHARED_PROJECTION_SOURCES {
        collect_file(workspace_root, &worker_root.join(relative_path), inputs)?;
    }
    Ok(())
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
            if path.file_name() == Some(OsStr::new("tests")) {
                continue;
            }
            collect_rust_sources(workspace_root, &path, inputs)?;
        } else if path.extension() == Some(OsStr::new("rs")) && !is_test_source(&path) {
            collect_file(workspace_root, &path, inputs)?;
        }
    }
    Ok(())
}

fn is_test_source(path: &Path) -> bool {
    path.file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
}

fn collect_file(workspace_root: &Path, path: &Path, inputs: &mut Vec<Input>) -> io::Result<()> {
    let key = relative_key(workspace_root, path)?;
    inputs.push(Input {
        key: format!("source:{key}"),
        content: fs::read(path)?,
    });
    Ok(())
}

fn collect_manifest_event_fragments(
    workspace_root: &Path,
    inputs: &mut Vec<Input>,
) -> io::Result<()> {
    let manifest_root = workspace_root.join(MANIFEST_ROOT);
    let mut files = Vec::new();
    collect_files_with_extension(&manifest_root, OsStr::new("toml"), &mut files)?;
    files.sort();

    for path in files {
        let relative_path = relative_key(workspace_root, &path)?;
        let contents = fs::read_to_string(&path)?;
        let mut in_event = false;
        let mut event_index = 0usize;
        for line in contents.lines() {
            let line = line.trim();
            if line.starts_with("[[") {
                in_event = line == "[[abi.events]]";
                if in_event {
                    event_index += 1;
                }
                continue;
            }
            if !in_event {
                continue;
            }
            let Some(fragment) = line.strip_prefix("fragment").and_then(|rest| {
                let (separator, value) = rest.split_once('=')?;
                separator.trim().is_empty().then_some(value.trim())
            }) else {
                continue;
            };
            inputs.push(Input {
                key: format!("manifest-event:{relative_path}:{event_index}"),
                content: fragment.as_bytes().to_vec(),
            });
        }
    }
    Ok(())
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
