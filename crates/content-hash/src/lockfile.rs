//! Lockfile fingerprints of the crates whose releases decide how the adapters turn raw log
//! words into persisted event bodies: the strict `SolEvent` decode, the tolerant retry's masked
//! word, and the address and integer types decoded values land in. A version bump of any of
//! them can change persisted interpretation without touching a watched source file, so each
//! one's lockfile fingerprint (version + checksum) is a hash input. Only the named crates
//! participate — an unrelated dependency bump must not rotate the hash and force a
//! re-derivation.

use std::{collections::BTreeMap, fs, io, path::Path};

use crate::compute::{Input, assignment_name};

pub(crate) const LOCKFILE: &str = "Cargo.lock";

/// The required set must be present — a missing entry fails the build rather than silently
/// narrowing the fingerprint — while the macro support crates are included when the lock
/// happens to carry them.
const REQUIRED_DECODE_CRATES: &[&str] = &[
    "alloy-dyn-abi",
    "alloy-primitives",
    "alloy-sol-macro",
    "alloy-sol-type-parser",
    "alloy-sol-types",
];
const OPTIONAL_DECODE_CRATES: &[&str] = &["alloy-sol-macro-expander", "alloy-sol-macro-input"];

/// Adds one input per decode-semantic crate found in the workspace lockfile.
pub(crate) fn collect_decode_crate_fingerprints(
    workspace_root: &Path,
    inputs: &mut Vec<Input>,
) -> io::Result<()> {
    let path = workspace_root.join(LOCKFILE);
    if !path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "interpreter content hash requires decode-crate fingerprints from {LOCKFILE}; if \
                 the lockfile moved, update LOCKFILE in the same change"
            ),
        ));
    }
    let packages = parse_lockfile_packages(&fs::read_to_string(&path)?);
    let mut fingerprints: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for (name, version, checksum) in &packages {
        if REQUIRED_DECODE_CRATES.contains(&name.as_str())
            || OPTIONAL_DECODE_CRATES.contains(&name.as_str())
        {
            fingerprints
                .entry(name.as_str())
                .or_default()
                .push((version.as_str(), checksum.as_str()));
        }
    }
    for required in REQUIRED_DECODE_CRATES {
        if !fingerprints.contains_key(required) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "interpreter content hash requires a {LOCKFILE} entry for decode crate \
                     {required}; if the adapters no longer decode through it, update \
                     REQUIRED_DECODE_CRATES in the same change"
                ),
            ));
        }
    }
    for (name, mut entries) in fingerprints {
        entries.sort_unstable();
        let mut content = Vec::new();
        for (version, checksum) in entries {
            content.extend_from_slice(version.as_bytes());
            content.push(b' ');
            content.extend_from_slice(checksum.as_bytes());
            content.push(b'\n');
        }
        inputs.push(Input {
            key: format!("decode-crate:{name}"),
            content,
        });
    }
    Ok(())
}

/// Line-parses `[[package]]` stanzas into (name, version, checksum); a path or git dependency
/// has no checksum and contributes an empty one.
fn parse_lockfile_packages(contents: &str) -> Vec<(String, String, String)> {
    let mut packages = Vec::new();
    let mut in_package = false;
    let mut name = None;
    let mut version = None;
    let mut checksum = None;
    let mut finish =
        |name: &mut Option<String>, version: &mut Option<String>, checksum: &mut Option<String>| {
            let checksum = checksum.take().unwrap_or_default();
            if let (Some(name), Some(version)) = (name.take(), version.take()) {
                packages.push((name, version, checksum));
            }
        };
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            if in_package {
                finish(&mut name, &mut version, &mut checksum);
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
            Some("checksum") => checksum = value(),
            _ => {}
        }
    }
    if in_package {
        finish(&mut name, &mut version, &mut checksum);
    }
    packages
}

#[cfg(test)]
pub(crate) fn decode_crate_lists() -> (&'static [&'static str], &'static [&'static str]) {
    (REQUIRED_DECODE_CRATES, OPTIONAL_DECODE_CRATES)
}

#[cfg(test)]
pub(crate) fn decode_crate_fingerprints(
    workspace_root: &Path,
) -> io::Result<Vec<(String, String, String)>> {
    let packages = parse_lockfile_packages(&fs::read_to_string(workspace_root.join(LOCKFILE))?);
    Ok(packages
        .into_iter()
        .filter(|(name, _, _)| {
            REQUIRED_DECODE_CRATES.contains(&name.as_str())
                || OPTIONAL_DECODE_CRATES.contains(&name.as_str())
        })
        .collect())
}
