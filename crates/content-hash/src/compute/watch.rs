//! The paths the build script watches for rebuilds, and the guard that keeps each one from
//! rerunning every build.

use std::{
    io,
    path::{Path, PathBuf},
};

use super::{
    ADAPTER_SOURCE_ROOT, CRATE_SOURCE_ROOTS, INTERPRET_WRITE_SOURCE_ROOT,
    MANIFEST_AUTHORITY_SOURCE_ROOT, MANIFEST_ROOT, PROJECT_SOURCE_ROOT, SEMANTIC_SOURCE_FILES,
    STORAGE_FAMILIES_SOURCE_ROOT, source_paths,
};

/// Every path the build script watches, each with where it comes from.
#[allow(dead_code)]
fn watched_sources(workspace_root: &Path) -> Vec<(PathBuf, String)> {
    let mut paths = [
        ADAPTER_SOURCE_ROOT,
        MANIFEST_AUTHORITY_SOURCE_ROOT,
        MANIFEST_ROOT,
        PROJECT_SOURCE_ROOT,
        STORAGE_FAMILIES_SOURCE_ROOT,
        INTERPRET_WRITE_SOURCE_ROOT,
    ]
    .iter()
    .map(|root| (workspace_root.join(root), "a hashed root".to_owned()))
    .collect::<Vec<_>>();
    // Not all hashed, but walked: a module declaration anywhere in these crates, or in a crate
    // holding a file hashed by name, can change which files are test-only or what a hashed file
    // compiles, so it has to trigger a rebuild. A target root or `#[path]` file can sit outside
    // `src/`, so every directory the walk reads is watched too. A walk that fails here fails the
    // hash below, and a failed build script always reruns.
    if let Ok(walked) =
        source_paths::walk_crates(workspace_root, CRATE_SOURCE_ROOTS, SEMANTIC_SOURCE_FILES)
    {
        for source_root in walked.roots.iter().map(|root| workspace_root.join(root)) {
            if let Some(crate_directory) = source_root.parent() {
                let manifest = crate_directory.join("Cargo.toml");
                paths.push((manifest, "a walked crate's manifest".to_owned()));
            }
            paths.push((source_root, "a walked source root".to_owned()));
        }
        for file in &walked.files {
            if let Some(directory) = file.parent() {
                let source = format!("the directory of walked {}", file.display());
                paths.push((directory.to_owned(), source));
            }
        }
        // A data read is watched as its file. An absent data read, an absent optional
        // `cfg_attr`, `#[path]` or `include!` file, or an absent default file of a module that
        // resolves to none may be created later, which changes the hash or what the walk
        // reaches. An absent path is watched through its nearest existing directory, because
        // cargo reruns the build script on every build for a watched path that does not exist.
        for file in walked.data_reads.keys().filter(|file| file.is_file()) {
            paths.push((file.clone(), "a data read".to_owned()));
        }
        let absent_reads = walked.data_reads.keys().filter(|file| !file.is_file());
        for file in absent_reads.chain(walked.missing.keys()) {
            let directory = file
                .ancestors()
                .skip(1)
                .find(|directory| directory.is_dir());
            if let Some(directory) = directory {
                let source = format!("the absent {}", file.display());
                paths.push((directory.to_owned(), source));
            }
        }
    }
    for relative_path in SEMANTIC_SOURCE_FILES {
        let path = workspace_root.join(relative_path);
        paths.push((path, "a file hashed by name".to_owned()));
    }
    let lockfile = workspace_root.join(crate::lockfile::LOCKFILE);
    paths.push((lockfile, "the lockfile".to_owned()));
    paths.sort();
    paths.dedup_by(|later, earlier| later.0 == earlier.0);
    paths
}

/// Every path the build script watches.
#[allow(dead_code)]
pub(crate) fn watched_paths(workspace_root: &Path) -> Vec<PathBuf> {
    let sources = watched_sources(workspace_root);
    sources.into_iter().map(|(path, _)| path).collect()
}

/// Every path the build script watches, once each is known not to rerun every build. Cargo scans
/// a watched directory whole, so a path must lie strictly inside the workspace root, outside the
/// target directory, and must not hold the build script's own output directory, which it writes
/// on every run. The target directory is `target_dir`, and also whatever holds the profile
/// directory above `OUT_DIR`'s `build`, as `<target>/<profile>` or `<target>/<triple>/<profile>`,
/// so a target directory set only in Cargo's config is covered too.
#[allow(dead_code)]
pub(crate) fn guarded_watched_paths(
    workspace_root: &Path,
    out_dir: &Path,
    target_dir: &Path,
) -> io::Result<Vec<PathBuf>> {
    let root = source_paths::lexically_normal(workspace_root);
    let out_dir = source_paths::lexically_normal(out_dir);
    let target_dir = source_paths::lexically_normal(&root.join(target_dir));
    let derived_target = (out_dir.ancestors())
        .find(|directory| directory.file_name().is_some_and(|name| name == "build"))
        .and_then(Path::parent)
        .and_then(Path::parent)
        .map(Path::to_owned);
    let mut paths = Vec::new();
    for (path, source) in watched_sources(workspace_root) {
        let normal = source_paths::lexically_normal(&path);
        let inside = normal != root && normal.starts_with(&root);
        let in_target = normal.starts_with(&target_dir)
            || derived_target
                .as_ref()
                .is_some_and(|target| normal.starts_with(target));
        if !inside || out_dir.starts_with(&normal) || in_target {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} would be watched for {source}, but it is not strictly inside the \
                     workspace or it holds the build output, so every build would rerun the \
                     interpreter content hash",
                    path.display()
                ),
            ));
        }
        paths.push(path);
    }
    Ok(paths)
}
