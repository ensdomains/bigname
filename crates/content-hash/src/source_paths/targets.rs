//! The production Cargo targets of a walked crate, read from its manifest and `src/`.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use super::paths::{lexically_normal, parent};

/// The root files of a crate's production targets.
pub(super) struct Targets {
    /// `[lib]` and `[[bin]]` paths from its `Cargo.toml`, or `src/lib.rs`, plus `src/main.rs`,
    /// `src/bin/*.rs` and `src/bin/*/main.rs`. A path the manifest selects is kept whatever its
    /// extension. Tests, examples and benches are not production targets.
    pub(super) roots: Vec<PathBuf>,
    /// The `package.build` path, or a `build.rs` beside the manifest unless `build = false`.
    pub(super) build_script: Option<PathBuf>,
}

pub(super) fn targets(source_root: &Path) -> io::Result<Targets> {
    let crate_directory = parent(source_root)?;
    let manifest_path = crate_directory.join("Cargo.toml");
    let manifest = if manifest_path.is_file() {
        fs::read_to_string(&manifest_path)?
            .parse::<toml::Table>()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("could not parse {}: {error}", manifest_path.display()),
                )
            })?
    } else {
        toml::Table::new()
    };
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
    let build_script = match manifest
        .get("package")
        .and_then(|package| package.get("build"))
    {
        Some(toml::Value::String(path)) => Some(selected(path)),
        Some(toml::Value::Boolean(false)) => None,
        None | Some(toml::Value::Boolean(true)) => Some(crate_directory.join("build.rs")),
        Some(other) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} sets package.build to {other}, which the content hash cannot read",
                    manifest_path.display()
                ),
            ));
        }
    };
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
    Ok(Targets {
        roots,
        build_script: build_script
            .map(|script| lexically_normal(&script))
            .filter(|script| script.is_file()),
    })
}

fn target_path(target: &toml::Value) -> Option<&str> {
    target.get("path").and_then(toml::Value::as_str)
}
