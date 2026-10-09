//! What the module walk found, and the checks that need the hashed source set to decide.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
};

use syn::{Expr, Item, ext::IdentExt, visit::Visit};

use super::{
    paths::{computed_include, lexically_normal, parent, relative_key, unparsed},
    syntax::{
        Collector, Found, INCLUDE, cfg_attr_path, include_paths, is_cfg_test, named, path_value,
        refuse_unfollowable, scope_items,
    },
};

/// File stems whose plain child modules resolve beside the file.
const MOD_RS: [Option<&str>; 3] = [Some("mod"), Some("lib"), Some("main")];

/// Where a scanned file's declarations resolve, as rustc resolves them: the directory plain
/// children resolve in, the directory a `#[path]` is relative to, and whether no non-test cfg may
/// turn the scope off, so an `include!` target in it must exist.
struct ScanScope {
    directory: PathBuf,
    base: PathBuf,
    required: bool,
}

/// What the walk found across the crates that hold hashed sources.
pub(crate) struct Walked {
    /// The sources compiled only under `cfg(test)`, as workspace-relative keys.
    pub(crate) test_only: BTreeSet<String>,
    /// Every file production code reads with `include_str!` or `include_bytes!`, with the files
    /// that read it.
    pub(crate) data_reads: BTreeMap<PathBuf, BTreeSet<PathBuf>>,
    /// Every file the walk reads, so a rebuild can watch them.
    pub(crate) files: Vec<PathBuf>,
    /// Every file production code includes with `include!`, with the files that include it.
    pub(super) inclusions: BTreeMap<PathBuf, BTreeSet<PathBuf>>,
    /// Every file a production `#[path]` decides, directly or as an inline module's directory, and
    /// every file an out-of-line module of a file hashed by name may resolve to, with each
    /// declaring file and declaration.
    pub(super) modules: BTreeMap<PathBuf, BTreeSet<(PathBuf, String)>>,
    /// Every production file the walk reaches outside the walked source roots, with the
    /// declaring file and declaration that first reached it.
    pub(super) outside: BTreeMap<PathBuf, (PathBuf, String)>,
    /// Absent files an optional `cfg_attr` path names, in the walk or a file hashed by name. Their
    /// nearest existing directories are watched, so creating one reruns the hash.
    pub(crate) missing: BTreeSet<PathBuf>,
    pub(super) normal_root: PathBuf,
}

impl Walked {
    /// Each existing file read as data by a hashed file, with its workspace-relative key.
    /// `hashed` says whether a workspace-relative key is a hashed input. A read from a hashed file
    /// of a file outside the workspace is an error, because the hash could not name it.
    pub(crate) fn data_inputs(
        &self,
        hashed: impl Fn(&str) -> bool,
    ) -> io::Result<Vec<(String, &Path)>> {
        let mut inputs = Vec::new();
        for (file, readers) in &self.data_reads {
            let mut read_by_hashed_file = false;
            for reader in readers {
                read_by_hashed_file |= hashed(&relative_key(&self.normal_root, reader)?);
            }
            if read_by_hashed_file && file.is_file() {
                inputs.push((relative_key(&self.normal_root, file)?, file.as_path()));
            }
        }
        Ok(inputs)
    }

    /// Refuses an `include!` from a hashed file whose target is not itself a hashed source, such
    /// as a file that is not Rust or sits outside the hashed roots. The included text is part of
    /// the hashed file, and the data reads inside it are attributed to the target, so neither
    /// would reach the hash.
    pub(crate) fn refuse_unhashed_inclusions(
        &self,
        hashed: impl Fn(&str) -> bool,
    ) -> io::Result<()> {
        for (file, includers) in &self.inclusions {
            let target = relative_key(&self.normal_root, file)?;
            for includer in includers {
                let includer = relative_key(&self.normal_root, includer)?;
                if hashed(&includer) && !hashed(&target) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "include! in {includer} reads {target}, which is not a hashed source, \
                             so the content hash cannot cover it"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Refuses a module declared from a hashed file whose file is not itself a hashed source,
    /// such as one a `#[path]` reaches outside the hashed roots, and any production file outside
    /// the walked source roots that is not hashed, such as a `[lib]` path outside `src/`. Its code
    /// would compile without reaching the hash.
    pub(crate) fn refuse_unhashed_modules(&self, hashed: impl Fn(&str) -> bool) -> io::Result<()> {
        for (file, declarations) in &self.modules {
            let target = relative_key(&self.normal_root, file)?;
            for (declarer, declaration) in declarations {
                let declarer = relative_key(&self.normal_root, declarer)?;
                if hashed(&declarer) && !hashed(&target) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{declaration} in {declarer} reaches {target}, which is not a hashed \
                             source, so the content hash cannot cover it"
                        ),
                    ));
                }
            }
        }
        for (file, (declarer, declaration)) in &self.outside {
            let target = relative_key(&self.normal_root, file)?;
            if !hashed(&target) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{target} is compiled outside the walked source roots, reached from {} \
                         ({declaration}), and is not a hashed source, so the content hash cannot \
                         cover it",
                        relative_key(&self.normal_root, declarer)?
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Scans the files hashed by name for the include macros the walk follows, and records what
    /// they read or include as reached from a hashed file. An `include!` target is scanned in turn,
    /// as the reader of its own data reads. A `macro_rules!` body or macro invocation the walk
    /// cannot follow is refused, as in the walk. Each out-of-line module resolves to one file, as
    /// rustc resolves it, which is recorded, as it must be a hashed source, and scanned in turn.
    /// Inline modules are scanned, and `#[cfg(test)]` modules are skipped.
    pub(super) fn scan_semantic_files(&mut self, files: &[PathBuf]) -> io::Result<()> {
        let mut scanned = BTreeSet::new();
        for file in files.iter().filter(|file| file.is_file()) {
            // A crate root or `mod.rs` owns its directory, and `foo.rs` owns `foo/`.
            let base = parent(file)?;
            let directory = match file.file_stem() {
                Some(stem) if !MOD_RS.contains(&stem.to_str()) => base.join(stem),
                _ => base,
            };
            self.scan_module(file, directory, &mut scanned)?;
        }
        Ok(())
    }

    /// Scans one module file once, with the directory its plain children resolve in.
    fn scan_module(
        &mut self,
        file: &Path,
        directory: PathBuf,
        scanned: &mut BTreeSet<PathBuf>,
    ) -> io::Result<()> {
        if !scanned.insert(file.to_owned()) {
            return Ok(());
        }
        let source = fs::read_to_string(file)?;
        let parsed = syn::parse_file(&source).map_err(|error| unparsed(file, &error))?;
        let scope = ScanScope {
            directory,
            base: parent(file)?,
            required: true,
        };
        self.scan_items(file, &parsed.items, &scope, scanned)
    }

    fn scan_items(
        &mut self,
        file: &Path,
        items: &[Item],
        scope: &ScanScope,
        scanned: &mut BTreeSet<PathBuf>,
    ) -> io::Result<()> {
        let found = scope_items(items);
        for (declaration, in_block) in &found.modules {
            if declaration.attrs.iter().any(is_cfg_test) {
                continue;
            }
            let explicit = declaration
                .attrs
                .iter()
                .find_map(|attribute| path_value(&attribute.meta));
            let name = declaration.ident.unraw().to_string();
            if let Some((_, content)) = &declaration.content {
                let directory = lexically_normal(&match &explicit {
                    Some(explicit) => scope.base.join(explicit),
                    None if *in_block => scope.base.join(&name),
                    None => scope.directory.join(&name),
                });
                // rustc drops a module whose other cfg is off, and its include targets with it.
                let other_cfg = declaration
                    .attrs
                    .iter()
                    .any(|attribute| named(attribute.path(), "cfg") && !is_cfg_test(attribute));
                let inner = ScanScope {
                    base: directory.clone(),
                    directory,
                    required: scope.required && !other_cfg,
                };
                self.scan_items(file, content, &inner, scanned)?;
                continue;
            }
            // Each target with the directory its own plain children resolve in. A `#[path]` file is
            // treated like a `mod.rs`.
            let beside = |target: PathBuf| -> io::Result<(PathBuf, PathBuf)> {
                Ok((parent(&target)?, target))
            };
            let main = match &explicit {
                Some(explicit) => beside(lexically_normal(&scope.base.join(explicit)))?,
                None => {
                    let file = lexically_normal(&scope.directory.join(format!("{name}.rs")));
                    let nested = lexically_normal(&scope.directory.join(&name).join("mod.rs"));
                    let file = if !file.is_file() && nested.is_file() {
                        nested
                    } else {
                        file
                    };
                    (lexically_normal(&scope.directory.join(&name)), file)
                }
            };
            let mut targets = vec![main];
            for (test_predicate, path) in declaration.attrs.iter().flat_map(cfg_attr_path) {
                if !test_predicate {
                    targets.push(beside(lexically_normal(&scope.base.join(path)))?);
                }
            }
            // A target that resolves nowhere is recorded too, so it fails as unhashed.
            if targets.iter().any(|(_, target)| target.is_file()) {
                // An absent alternative is watched, in case it is created later.
                for (_, target) in targets.iter().filter(|(_, target)| !target.is_file()) {
                    self.missing.insert(target.clone());
                }
                targets.retain(|(_, target)| target.is_file());
            } else {
                targets.truncate(1);
            }
            for (directory, target) in targets {
                let declaration = (file.to_owned(), format!("mod {name}"));
                self.modules
                    .entry(target.clone())
                    .or_default()
                    .insert(declaration);
                if target.is_file() {
                    self.scan_module(&target, directory, scanned)?;
                }
            }
        }
        self.scan_found(file, &found, scope.required, scanned)
    }

    fn scan_found(
        &mut self,
        file: &Path,
        found: &Found<'_>,
        required: bool,
        scanned: &mut BTreeSet<PathBuf>,
    ) -> io::Result<()> {
        refuse_unfollowable(file, found)?;
        for invocation in &found.macros {
            for (include, path) in include_paths(invocation) {
                let path = path.map_err(|()| computed_include(file, include))?;
                let target = lexically_normal(&parent(file)?.join(path));
                if include == INCLUDE {
                    // Under a cfg that may be off, as in the walk, the target need not exist.
                    if !required && !target.is_file() {
                        continue;
                    }
                    self.inclusions
                        .entry(target.clone())
                        .or_default()
                        .insert(file.to_owned());
                    self.scan_included(&target, required, scanned)?;
                } else {
                    self.data_reads
                        .entry(target)
                        .or_default()
                        .insert(file.to_owned());
                }
            }
        }
        Ok(())
    }

    /// Scans an `include!` target once, as items or one expression, refusing module declarations
    /// as the walk does. A missing target is left to the hashed-source check, which refuses it.
    fn scan_included(
        &mut self,
        file: &Path,
        required: bool,
        scanned: &mut BTreeSet<PathBuf>,
    ) -> io::Result<()> {
        if !file.is_file() || !scanned.insert(file.to_owned()) {
            return Ok(());
        }
        let source = fs::read_to_string(file)?;
        let parsed = syn::parse_file(&source);
        let expression;
        let found = match &parsed {
            Ok(parsed) => scope_items(&parsed.items),
            Err(file_error) => {
                expression =
                    syn::parse_str::<Expr>(&source).map_err(|_| unparsed(file, file_error))?;
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
        self.scan_found(file, &found, required, scanned)
    }
}
