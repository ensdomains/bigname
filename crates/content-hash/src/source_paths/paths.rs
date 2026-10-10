//! Path folding and the errors the walk shares with its findings.

use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

pub(super) fn parent(file: &Path) -> io::Result<PathBuf> {
    file.parent()
        .map(Path::to_owned)
        .ok_or_else(|| invalid_path(file))
}

/// Folds `.` and `..` so two spellings of one file compare equal. A `..` at the root stays at
/// the root, as the filesystem resolves it.
pub(crate) fn lexically_normal(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir if normal.file_name().is_some() => {
                normal.pop();
            }
            Component::ParentDir if normal.has_root() => {}
            other => normal.push(other),
        }
    }
    normal
}

/// Reads a walked file, naming it when it cannot be read as UTF-8 text.
pub(super) fn read_source(file: &Path) -> io::Result<String> {
    fs::read_to_string(file).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("could not read {}: {error}", file.display()),
        )
    })
}

pub(super) fn unparsed(file: &Path, error: &syn::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("could not parse {}: {error}", file.display()),
    )
}

pub(super) fn computed_include(file: &Path, include: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{include}! in {} has a computed path, which the content hash cannot follow",
            file.display()
        ),
    )
}

pub(super) fn unresolved_module(parent_module: &Path, module_name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "could not resolve module {module_name} declared by {}",
            parent_module.display()
        ),
    )
}

fn invalid_path(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} is not a module file path", path.display()),
    )
}

pub(super) fn relative_key(workspace_root: &Path, path: &Path) -> io::Result<String> {
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

pub(super) fn mixed_module(
    file: &Path,
    gated: &super::Site,
    production: &super::Site,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{} is reached both from test-only {} ({}) and from {} ({})",
            file.display(),
            gated.parent.display(),
            gated.name,
            production.parent.display(),
            production.name
        ),
    )
}
