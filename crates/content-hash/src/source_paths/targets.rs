//! The production Cargo targets of a walked crate, read from its manifest and `src/`.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use super::paths::{lexically_normal, parent};

/// The root files of a crate's production targets: `[lib]` and `[[bin]]` paths from its
/// `Cargo.toml`, or `src/lib.rs`, plus `src/main.rs`, `src/bin/*.rs` and `src/bin/*/main.rs`. A
/// path the manifest selects is kept whatever its extension. Tests, examples and benches are not
/// production targets.
///
/// A build script can set cfgs, environment and generated code for its crate, and none of it is
/// hashed. So a walked crate's manifest must set `package.build = false`, which also keeps Cargo
/// from running a `build.rs` created later.
pub(super) fn targets(source_root: &Path) -> io::Result<Vec<PathBuf>> {
    let crate_directory = parent(source_root)?;
    let manifest_path = crate_directory.join("Cargo.toml");
    if !manifest_path.is_file() {
        // A manifest higher up can compile this `src/` through a `[lib]` path, with roots and a
        // build script the walk never reads.
        if source_root.exists() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} has no Cargo.toml beside it, so the content hash cannot tell which package \
                     compiles it",
                    source_root.display()
                ),
            ));
        }
        return Ok(Vec::new());
    }
    let manifest = fs::read_to_string(&manifest_path)?
        .parse::<toml::Table>()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("could not parse {}: {error}", manifest_path.display()),
            )
        })?;
    let build = manifest
        .get("package")
        .and_then(|package| package.get("build"));
    if build != Some(&toml::Value::Boolean(false)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} does not set package.build = false, and the content hash cannot cover a \
                 build script of a walked crate",
                manifest_path.display()
            ),
        ));
    }
    let selected = |path: &str| crate_directory.join(path);
    let mut discovered = vec![source_root.join("main.rs")];
    let mut roots = vec![match manifest.get("lib").and_then(target_path) {
        Some(path) => selected(path),
        None => source_root.join("lib.rs"),
    }];
    let bins = manifest.get("bin").and_then(toml::Value::as_array);
    roots.extend(
        bins.into_iter()
            .flatten()
            .filter_map(target_path)
            .map(selected),
    );
    let bin_directory = source_root.join("bin");
    if bin_directory.is_dir() {
        let mut entries = fs::read_dir(&bin_directory)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            discovered.push(if path.is_dir() {
                path.join("main.rs")
            } else {
                path
            });
        }
    }
    // Cargo discovers only `.rs` files by itself.
    roots.extend(
        discovered
            .into_iter()
            .filter(|root| root.extension().is_some_and(|extension| extension == "rs")),
    );
    let mut roots = roots
        .iter()
        .map(|root| lexically_normal(root))
        .filter(|root| root.is_file())
        .collect::<Vec<_>>();
    roots.dedup();
    Ok(roots)
}

fn target_path(target: &toml::Value) -> Option<&str> {
    target.get("path").and_then(toml::Value::as_str)
}
