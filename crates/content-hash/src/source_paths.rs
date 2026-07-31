use std::{
    collections::BTreeSet,
    fs, io,
    path::{Path, PathBuf},
};

pub(super) fn cfg_test_sources(
    workspace_root: &Path,
    source_roots: &[&str],
) -> io::Result<BTreeSet<String>> {
    let mut source_files = Vec::new();
    for source_root in source_roots {
        collect_rust_files(&workspace_root.join(source_root), &mut source_files)?;
    }

    let mut gated_sources = BTreeSet::new();
    for parent_module in source_files {
        let contents = fs::read_to_string(&parent_module)?;
        let mut attributes = Vec::new();
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("#[") {
                attributes.push(trimmed.to_owned());
                continue;
            }
            if trimmed.is_empty() || trimmed.starts_with("//") {
                continue;
            }
            if trimmed.ends_with(';')
                && attributes
                    .iter()
                    .any(|attribute| attribute == "#[cfg(test)]")
                && let Some(module_name) = external_module_name(trimmed)
            {
                let explicit_path = attributes
                    .iter()
                    .find_map(|attribute| path_attribute(attribute));
                let module_path =
                    resolve_external_module(&parent_module, module_name, explicit_path.as_deref())?;
                gated_sources.insert(relative_key(workspace_root, &module_path)?);
            }
            attributes.clear();
        }
    }
    let directly_gated = gated_sources.clone();
    for relative_path in directly_gated {
        let module_path = workspace_root.join(relative_path);
        let module_directory = if module_path.file_name().is_some_and(|name| name == "mod.rs") {
            module_path.parent().map(Path::to_owned)
        } else {
            module_path
                .parent()
                .zip(module_path.file_stem())
                .map(|(parent, stem)| parent.join(stem))
        };
        let Some(module_directory) = module_directory else {
            continue;
        };
        let mut descendants = Vec::new();
        collect_rust_files(&module_directory, &mut descendants)?;
        for descendant in descendants {
            gated_sources.insert(relative_key(workspace_root, &descendant)?);
        }
    }
    Ok(gated_sources)
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    Ok(())
}

fn external_module_name(declaration: &str) -> Option<&str> {
    let words = declaration.split_whitespace().collect::<Vec<_>>();
    let mod_index = words.iter().position(|word| *word == "mod")?;
    words
        .get(mod_index + 1)
        .map(|name| name.trim_end_matches(';'))
}

fn path_attribute(attribute: &str) -> Option<String> {
    attribute
        .strip_prefix("#[path = \"")
        .and_then(|path| path.strip_suffix("\"]"))
        .map(str::to_owned)
}

fn resolve_external_module(
    parent_module: &Path,
    module_name: &str,
    explicit_path: Option<&str>,
) -> io::Result<PathBuf> {
    if let Some(explicit_path) = explicit_path {
        return require_file(
            parent_module
                .parent()
                .ok_or_else(|| invalid_module(parent_module, module_name))?
                .join(explicit_path),
            parent_module,
            module_name,
        );
    }

    let parent_directory = parent_module
        .parent()
        .ok_or_else(|| invalid_module(parent_module, module_name))?;
    let file_name = parent_module
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_module(parent_module, module_name))?;
    let module_directory = if matches!(file_name, "lib.rs" | "main.rs" | "mod.rs") {
        parent_directory.to_owned()
    } else {
        parent_directory.join(
            parent_module
                .file_stem()
                .ok_or_else(|| invalid_module(parent_module, module_name))?,
        )
    };
    [
        module_directory.join(format!("{module_name}.rs")),
        module_directory.join(module_name).join("mod.rs"),
    ]
    .into_iter()
    .find(|candidate| candidate.is_file())
    .ok_or_else(|| invalid_module(parent_module, module_name))
}

fn require_file(path: PathBuf, parent_module: &Path, module_name: &str) -> io::Result<PathBuf> {
    if path.is_file() {
        Ok(path)
    } else {
        Err(invalid_module(parent_module, module_name))
    }
}

fn invalid_module(parent_module: &Path, module_name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "could not resolve cfg(test) module {module_name} declared by {}",
            parent_module.display()
        ),
    )
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
