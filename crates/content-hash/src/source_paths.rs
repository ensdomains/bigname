use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
};

use syn::{Expr, Item, ext::IdentExt, visit::Visit};

#[path = "source_paths/paths.rs"]
mod paths;
#[path = "source_paths/syntax.rs"]
mod syntax;
#[path = "source_paths/targets.rs"]
mod targets;
#[path = "source_paths/walked.rs"]
mod walked;

pub(super) use walked::Walked;

use paths::{
    computed_include, lexically_normal, parent, read_source, relative_key, unparsed,
    unresolved_module,
};

use syntax::{
    Collector, INCLUDE, cfg_attr_path, include_paths, is_cfg_test, named, path_value,
    refuse_unfollowable, scope_items,
};

/// Walks each crate's module tree from its production Cargo targets as rustc resolves it. A file
/// leaves the hash only when every route to it passes a `#[cfg(test)]` declaration. A file
/// reached both from test-only code and from production code is an error, and so is each shape
/// the walk refuses in `docs/storage.md`.
///
/// A file that production code reads with `include_str!` or `include_bytes!` is never test-only.
/// When the file reading it is hashed, it is hashed too, whatever its extension or directory.
/// That holds for a call in code, including one nested in another macro's arguments. Attribute
/// token streams are not scanned, so `#![doc = include_str!("shared.rs")]` keeps nothing in the
/// hash.
///
/// `semantic_files` are hashed by name. The roots include each of their crates, so a compiled one
/// is reached in rustc's own context and held to the same rules. The files a production module
/// declared from one resolves to, and in turn their own modules, like those a production
/// `#[path]` reaches, must be hashed sources. A file hashed by name that the walk never reaches
/// is not compiled, so nothing it declares is checked.
pub(super) fn walk_crates(
    workspace_root: &Path,
    crate_source_roots: &[&str],
    semantic_files: &[&str],
) -> io::Result<Walked> {
    let normal_root = lexically_normal(workspace_root);
    let by_name = semantic_files
        .iter()
        .map(|file| normal_root.join(file))
        .collect();
    // Only the source roots are hashed as trees, so a production file outside them, such as a
    // `[lib]` path or a `#[path]` module outside `src/`, must be hashed by name. Each crate that
    // holds a file hashed by name is walked too.
    let mut source_roots = crate_source_roots
        .iter()
        .map(|root| (*root).to_owned())
        .collect::<Vec<_>>();
    for file in semantic_files {
        let Some(end) = file.find("/src/") else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{file} is hashed by name but is not under a crate's src/, so the walk cannot \
                     reach it"
                ),
            ));
        };
        let root = file[..end + 4].to_owned();
        if !source_roots.contains(&root) {
            source_roots.push(root);
        }
    }
    let roots = source_roots
        .iter()
        .map(|root| normal_root.join(root))
        .collect::<Vec<_>>();
    let walk = walk(&roots, by_name)?;
    let test_only = walk
        .reached
        .iter()
        .filter(|(file, (test_only, _))| *test_only && !walk.pinned.contains_key(*file))
        .map(|(file, _)| relative_key(&normal_root, file))
        .collect::<io::Result<_>>()?;
    // A module file that is not Rust is not hashed with its root either, such as a `[lib]` path
    // to `src/root.txt`, whatever else reaches it, an `include!` among them.
    let outside = walk
        .reached
        .iter()
        .filter(|(file, (test_only, _))| {
            let rust = file.extension().is_some_and(|extension| extension == "rs");
            let module = walk.compiled.contains(*file);
            !test_only && (!roots.iter().any(|root| file.starts_with(root)) || (module && !rust))
        })
        .map(|(file, (_, site))| (file.clone(), (site.parent.clone(), site.name.clone())))
        .collect();
    Ok(Walked {
        test_only,
        data_reads: walk.pinned,
        files: walk.reached.into_keys().collect(),
        inclusions: walk.inclusions,
        modules: walk.modules,
        outside,
        missing: walk.missing,
        roots: source_roots,
        normal_root,
    })
}

fn walk(roots: &[PathBuf], by_name: BTreeSet<PathBuf>) -> io::Result<Walk> {
    let mut walk = Walk {
        by_name,
        ..Walk::default()
    };
    let mut pending = Vec::new();
    for root in roots {
        for file in targets::targets(root)? {
            let directory = parent(&file)?;
            let site = Site::crate_root(&file);
            pending.push(Module {
                by_name: walk.by_name.contains(&file),
                file,
                directory,
                test_only: false,
                site,
                required: true,
            });
        }
    }
    walk.drain(&mut pending)?;
    Ok(walk)
}

/// Where a file was reached from: the declaring file and the declaration, such as `mod x`.
#[derive(Clone)]
struct Site {
    parent: PathBuf,
    name: String,
}

impl Site {
    fn crate_root(file: &Path) -> Self {
        Self {
            parent: file.to_owned(),
            name: "crate root".to_owned(),
        }
    }

    fn module(parent: &Path, name: &str) -> Self {
        Self {
            parent: parent.to_owned(),
            name: format!("mod {name}"),
        }
    }
}

/// A module file to read, with the directory its plain `mod x;` children resolve in.
struct Module {
    file: PathBuf,
    directory: PathBuf,
    test_only: bool,
    site: Site,
    /// False under a cfg that may be off, where a declared file need not exist.
    required: bool,
    /// Whether the file is hashed by name or declared from one, so its production modules must
    /// be hashed sources.
    by_name: bool,
}

#[derive(Default)]
struct Walk {
    /// Every file reached, lexically normalized, with whether it is test-only and the first
    /// declaration that reached it.
    reached: BTreeMap<PathBuf, (bool, Site)>,
    /// Every file production code reaches as a crate root or a module, on any visit, whatever
    /// reached it first.
    compiled: BTreeSet<PathBuf>,
    /// Each file with the directory its children resolve in, walked once per pair: one file can
    /// be declared twice with different child directories.
    walked: BTreeSet<(PathBuf, PathBuf, bool, bool)>,
    /// Each `include!` target with the including file's directory, followed once per pair. It is
    /// kept apart from `walked`, so an ordinary module visit of the same file never skips it.
    included: BTreeSet<(PathBuf, PathBuf, bool)>,
    /// Files that production code reads as data, which are never test-only, with the files that
    /// read them.
    pinned: BTreeMap<PathBuf, BTreeSet<PathBuf>>,
    /// Files production code includes with `include!`, with the files that include them.
    inclusions: BTreeMap<PathBuf, BTreeSet<PathBuf>>,
    /// Files a production `#[path]` decides, directly or as an inline module's directory, or a
    /// file hashed by name declares, with each declaring file and declaration.
    modules: BTreeMap<PathBuf, BTreeSet<(PathBuf, String)>>,
    /// Absent files an optional `cfg_attr`, `#[path]` or `include!` path names, and the absent
    /// default files of a module that resolves to none. Their nearest existing directories are
    /// watched, so creating one reruns the hash.
    missing: BTreeSet<PathBuf>,
    /// The files hashed by name.
    by_name: BTreeSet<PathBuf>,
}

impl Walk {
    fn drain(&mut self, pending: &mut Vec<Module>) -> io::Result<()> {
        while let Some(module) = pending.pop() {
            self.reach(&module.file, module.test_only, &module.site)?;
            if !module.test_only {
                self.compiled.insert(module.file.clone());
            }
            let walk_key = (
                module.file.clone(),
                module.directory.clone(),
                module.required,
                module.by_name,
            );
            if !self.walked.insert(walk_key) {
                continue;
            }
            let source = read_source(&module.file)?;
            let parsed =
                syn::parse_file(&source).map_err(|error| unparsed(&module.file, &error))?;
            let scope = Scope {
                directory: module.directory.clone(),
                inline: false,
                explicit: false,
                test_only: module.test_only,
                required: module.required,
            };
            self.items(&module, &parsed.items, &scope, pending)?;
        }
        Ok(())
    }

    /// Queues the external modules declared in `items`, including those nested in blocks.
    fn items(
        &mut self,
        module: &Module,
        items: &[Item],
        scope: &Scope,
        pending: &mut Vec<Module>,
    ) -> io::Result<()> {
        let found = scope_items(items);
        refuse_unfollowable(&module.file, &found)?;
        for invocation in &found.macros {
            for (include, path) in include_paths(invocation) {
                let path = path.map_err(|()| computed_include(&module.file, include))?;
                self.follow(
                    &module.file,
                    include,
                    &path,
                    scope.test_only,
                    scope.required,
                )?;
            }
        }
        for (declaration, in_block) in found.modules {
            let gate = declaration.attrs.iter().any(is_cfg_test);
            // rustc drops a module whose other cfg is off before it looks for the file, and
            // everything inside it with it.
            let other_cfg = declaration
                .attrs
                .iter()
                .any(|attribute| named(attribute.path(), "cfg") && !is_cfg_test(attribute));
            let test_only = scope.test_only || gate;
            let required = scope.required && !other_cfg;
            let explicit_path = declaration
                .attrs
                .iter()
                .find_map(|attribute| path_value(&attribute.meta));
            let cfg_attr_paths = declaration
                .attrs
                .iter()
                .flat_map(cfg_attr_path)
                .collect::<Vec<_>>();
            let name = declaration.ident.unraw().to_string();
            let site = Site::module(&module.file, &name);
            // A `#[path]` at the top of a file is relative to the file's own directory, without
            // the offset a non-mod-rs file gives its plain children. Inside an inline module it is
            // relative to that module's directory.
            let path_base = if scope.inline {
                scope.directory.clone()
            } else {
                parent(&module.file)?
            };
            if let Some((_, content)) = &declaration.content {
                if !cfg_attr_paths.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "unsupported cfg_attr path on inline module {name} declared by {}",
                            module.file.display()
                        ),
                    ));
                }
                // An inline module is a directory, named by its `#[path]` when it has one. A
                // block owns no directory, so one declared in a block drops the file's offset.
                let directory = match &explicit_path {
                    Some(explicit_path) => path_base.join(explicit_path),
                    None if in_block => path_base.join(&name),
                    None => scope.directory.join(&name),
                };
                let inner = Scope {
                    explicit: scope.explicit || explicit_path.is_some(),
                    directory,
                    inline: true,
                    test_only,
                    required,
                };
                self.items(module, content, &inner, pending)?;
                continue;
            }
            // A `cfg_attr` path applies only when its predicate holds, so its file is optional.
            // It is test-only exactly when the predicate is `test`.
            for (test_predicate, path) in &cfg_attr_paths {
                let file = lexically_normal(&path_base.join(path));
                if file.is_file() {
                    if !(test_only || *test_predicate) {
                        self.declare(&file, &site);
                    }
                    pending.push(Module {
                        by_name: module.by_name || self.by_name.contains(&file),
                        directory: parent(&file)?,
                        file,
                        test_only: test_only || *test_predicate,
                        site: site.clone(),
                        // The predicate may be off, so the file's own children need not exist.
                        required: false,
                    });
                } else {
                    self.missing.insert(file);
                }
            }
            let resolved = match explicit_path {
                // rustc treats a `#[path]` file like a `mod.rs`.
                Some(explicit_path) => {
                    let file = lexically_normal(&path_base.join(explicit_path));
                    if file.is_file() {
                        if !test_only {
                            self.declare(&file, &site);
                        }
                        Some((parent(&file)?, file))
                    } else {
                        self.missing.insert(file);
                        None
                    }
                }
                None => {
                    let candidates = [
                        scope.directory.join(format!("{name}.rs")),
                        scope.directory.join(&name).join("mod.rs"),
                    ]
                    .map(|candidate| lexically_normal(&candidate));
                    let found = candidates.iter().find(|candidate| candidate.is_file());
                    if found.is_none() {
                        // Creating one later changes what the walk reaches.
                        self.missing.extend(candidates.iter().cloned());
                    }
                    found.map(|file| (scope.directory.join(&name), file.clone()))
                }
            };
            let Some((child_directory, file)) = resolved else {
                if required && cfg_attr_paths.is_empty() {
                    return Err(unresolved_module(&module.file, &name));
                }
                continue;
            };
            // A child of an inline module with a `#[path]` resolves wherever that path points, and
            // a child of a file hashed by name is part of its semantics.
            if (scope.explicit || module.by_name) && !test_only {
                self.declare(&file, &site);
            }
            pending.push(Module {
                by_name: module.by_name || self.by_name.contains(&file),
                file,
                directory: lexically_normal(&child_directory),
                test_only,
                site,
                required,
            });
        }
        Ok(())
    }

    /// Records a production route to an existing `file` that a `#[path]` decides, or that a file
    /// hashed by name declares, which must be a hashed source.
    fn declare(&mut self, file: &Path, site: &Site) {
        let declaration = (site.parent.clone(), site.name.clone());
        self.modules
            .entry(file.to_owned())
            .or_default()
            .insert(declaration);
    }

    /// Records that `file` is reached with `test_only` status, refusing a mixed one.
    fn reach(&mut self, file: &Path, test_only: bool, site: &Site) -> io::Result<()> {
        match self.reached.get(file) {
            Some((reached_test_only, reached_site)) if *reached_test_only != test_only => {
                let (gated, production) = if *reached_test_only {
                    (reached_site, site)
                } else {
                    (site, reached_site)
                };
                Err(mixed_module(file, gated, production))
            }
            Some(_) => Ok(()),
            None => {
                self.reached
                    .insert(file.to_owned(), (test_only, site.clone()));
                Ok(())
            }
        }
    }

    /// Follows one literal include path from `includer`. `include!` text is walked. The target of
    /// a production `include_str!` or `include_bytes!` is recorded with the file that reads it.
    fn follow(
        &mut self,
        includer: &Path,
        include: &str,
        path: &str,
        test_only: bool,
        required: bool,
    ) -> io::Result<()> {
        if include == INCLUDE {
            return self.include(includer, path, test_only, required);
        }
        if !test_only {
            self.pinned
                .entry(lexically_normal(&parent(includer)?.join(path)))
                .or_default()
                .insert(includer.to_owned());
        }
        Ok(())
    }

    /// Follows a literal `include!` from `includer`. The included text is reached in the
    /// includer's cfg context. It may not declare modules, which would resolve in the includer's
    /// module, and its own `include!` paths are relative to it.
    fn include(
        &mut self,
        includer: &Path,
        path: &str,
        test_only: bool,
        required: bool,
    ) -> io::Result<()> {
        let file = lexically_normal(&parent(includer)?.join(path));
        if !file.is_file() {
            if required {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "could not resolve include!({path:?}) in {}",
                        includer.display()
                    ),
                ));
            }
            // Creating it later changes what the walk reaches.
            self.missing.insert(file);
            return Ok(());
        }
        let site = Site {
            parent: includer.to_owned(),
            name: format!("include!({path:?})"),
        };
        self.reach(&file, test_only, &site)?;
        if !test_only {
            self.inclusions
                .entry(file.clone())
                .or_default()
                .insert(includer.to_owned());
        }
        if !self
            .included
            .insert((file.clone(), parent(includer)?, required))
        {
            return Ok(());
        }
        let source = read_source(&file)?;
        // The text is items or one expression, depending on where it is included.
        let parsed = syn::parse_file(&source);
        let expression;
        let found = match &parsed {
            Ok(parsed) => scope_items(&parsed.items),
            Err(file_error) => {
                expression =
                    syn::parse_str::<Expr>(&source).map_err(|_| unparsed(&file, file_error))?;
                let mut collector = Collector::default();
                collector.visit_expr(&expression);
                collector.found
            }
        };
        if let Some((declaration, _)) = found.modules.first() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "include! target {} declares mod {}, which the content hash cannot resolve",
                    file.display(),
                    declaration.ident
                ),
            ));
        }
        refuse_unfollowable(&file, &found)?;
        for invocation in &found.macros {
            for (include, path) in include_paths(invocation) {
                let path = path.map_err(|()| computed_include(&file, include))?;
                self.follow(&file, include, &path, test_only, required)?;
            }
        }
        Ok(())
    }
}

/// The module context declarations resolve in: the directory plain children resolve in, whether
/// it is an inline module, whether an inline module's `#[path]` set that directory, and what the
/// enclosing modules carry down.
struct Scope {
    directory: PathBuf,
    inline: bool,
    explicit: bool,
    test_only: bool,
    required: bool,
}

fn mixed_module(file: &Path, gated: &Site, production: &Site) -> io::Error {
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
