//! Lockfile fingerprints of crates whose releases can change persisted interpretation or
//! projection output without changing a watched source file. This includes the Alloy decode
//! stack and the Serde stack used by the authoritative projected-topology serializer. Only the
//! named crates participate, so an unrelated dependency bump does not force re-derivation. The
//! source field is load-bearing for git-patched dependencies: a git revision carries no checksum,
//! so without it two revisions of the same version would fingerprint identically.

use std::{collections::BTreeMap, fs, io, path::Path};

use crate::compute::{Input, assignment_name};

pub(crate) const LOCKFILE: &str = "Cargo.lock";

/// The required set must be present — a missing entry fails the build rather than silently
/// narrowing the fingerprint — while the macro support crates are included when the lock
/// happens to carry them.
const REQUIRED_SEMANTIC_CRATES: &[&str] = &[
    "alloy-dyn-abi",
    "alloy-primitives",
    "alloy-sol-macro",
    "alloy-sol-type-parser",
    "alloy-sol-types",
    "serde",
    "serde_core",
    "serde_derive",
    "serde_json",
];
const OPTIONAL_SEMANTIC_CRATES: &[&str] = &["alloy-sol-macro-expander", "alloy-sol-macro-input"];

/// Adds one input per semantic dependency found in the workspace lockfile.
pub(crate) fn collect_semantic_crate_fingerprints(
    workspace_root: &Path,
    inputs: &mut Vec<Input>,
) -> io::Result<()> {
    let path = workspace_root.join(LOCKFILE);
    if !path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "interpreter content hash requires semantic-crate fingerprints from {LOCKFILE}; if \
                 the lockfile moved, update LOCKFILE in the same change"
            ),
        ));
    }
    let packages = parse_lockfile_packages(&fs::read_to_string(&path)?);
    let mut fingerprints: BTreeMap<&str, Vec<(&str, &str, &str)>> = BTreeMap::new();
    for (name, version, source, checksum) in &packages {
        if REQUIRED_SEMANTIC_CRATES.contains(&name.as_str())
            || OPTIONAL_SEMANTIC_CRATES.contains(&name.as_str())
        {
            fingerprints.entry(name.as_str()).or_default().push((
                version.as_str(),
                source.as_str(),
                checksum.as_str(),
            ));
        }
    }
    for required in REQUIRED_SEMANTIC_CRATES {
        if !fingerprints.contains_key(required) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "interpreter content hash requires a {LOCKFILE} entry for semantic crate \
                     {required}; if persisted output no longer depends on it, update \
                     REQUIRED_SEMANTIC_CRATES in the same change"
                ),
            ));
        }
    }
    for (name, mut entries) in fingerprints {
        entries.sort_unstable();
        let mut content = Vec::new();
        for (version, source, checksum) in entries {
            content.extend_from_slice(version.as_bytes());
            content.push(b' ');
            content.extend_from_slice(source.as_bytes());
            content.push(b' ');
            content.extend_from_slice(checksum.as_bytes());
            content.push(b'\n');
        }
        inputs.push(Input {
            key: format!("semantic-crate:{name}"),
            content,
        });
    }
    Ok(())
}

/// Line-parses `[[package]]` stanzas into (name, version, source, checksum). A git dependency
/// has no checksum but pins its revision in `source`; a path dependency has neither and
/// contributes empty fields.
fn parse_lockfile_packages(contents: &str) -> Vec<(String, String, String, String)> {
    let mut packages = Vec::new();
    let mut in_package = false;
    let mut name = None;
    let mut version = None;
    let mut source = None;
    let mut checksum = None;
    let mut finish = |name: &mut Option<String>,
                      version: &mut Option<String>,
                      source: &mut Option<String>,
                      checksum: &mut Option<String>| {
        let source = source.take().unwrap_or_default();
        let checksum = checksum.take().unwrap_or_default();
        if let (Some(name), Some(version)) = (name.take(), version.take()) {
            packages.push((name, version, source, checksum));
        }
    };
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            if in_package {
                finish(&mut name, &mut version, &mut source, &mut checksum);
            }
            in_package = trimmed == "[[package]]";
            continue;
        }
        if !in_package {
            continue;
        }
        let value = || {
            trimmed
                .split_once('=')
                .map(|(_, value)| value.trim().trim_matches('"').to_owned())
        };
        match assignment_name(trimmed) {
            Some("name") => name = value(),
            Some("version") => version = value(),
            Some("source") => source = value(),
            Some("checksum") => checksum = value(),
            _ => {}
        }
    }
    if in_package {
        finish(&mut name, &mut version, &mut source, &mut checksum);
    }
    packages
}

#[cfg(test)]
pub(crate) fn semantic_crate_lists() -> (&'static [&'static str], &'static [&'static str]) {
    (REQUIRED_SEMANTIC_CRATES, OPTIONAL_SEMANTIC_CRATES)
}

#[cfg(test)]
pub(crate) fn semantic_crate_fingerprints(
    workspace_root: &Path,
) -> io::Result<Vec<(String, String, String, String)>> {
    let packages = parse_lockfile_packages(&fs::read_to_string(workspace_root.join(LOCKFILE))?);
    Ok(packages
        .into_iter()
        .filter(|(name, _, _, _)| {
            REQUIRED_SEMANTIC_CRATES.contains(&name.as_str())
                || OPTIONAL_SEMANTIC_CRATES.contains(&name.as_str())
        })
        .collect())
}
