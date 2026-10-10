//! What the module walk found, and the checks that need the hashed source set to decide.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
};

use super::paths::relative_key;

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
    /// every file a production module declared in or below a file hashed by name resolves to,
    /// with each declaring file and declaration.
    pub(super) modules: BTreeMap<PathBuf, BTreeSet<(PathBuf, String)>>,
    /// Every production file the walk reaches outside the walked source roots, and every
    /// production root or module file that is not Rust, with the declaring file and declaration
    /// that first reached it.
    pub(super) outside: BTreeMap<PathBuf, (PathBuf, String)>,
    /// Every production out-of-line module declaration that can resolve to a file.
    pub(super) slots: Vec<Slot>,
    /// Each file hashed by name that no production route reaches, with why.
    pub(super) unreached: Vec<(PathBuf, &'static str)>,
    /// Absent files an optional `cfg_attr`, `#[path]` or `include!` path names, and the absent
    /// default files of a module that resolves to none. Their nearest existing directories are
    /// watched, so creating one reruns the hash.
    pub(crate) missing: BTreeMap<PathBuf, PathBuf>,
    /// The workspace-relative source roots walked: the crates whose sources are hashed, and each
    /// crate that holds a file hashed by name.
    pub(crate) roots: Vec<String>,
    pub(super) normal_root: PathBuf,
}

/// A production module declaration: the files it can resolve to in production, from its default
/// location, `#[path]` and `cfg_attr` paths, and its default locations, in its scope's directory
/// and in that directory as if no `#[path]` on the route had moved it.
pub(crate) struct Slot {
    pub(super) declarer: PathBuf,
    pub(super) declaration: String,
    pub(super) possible: BTreeSet<PathBuf>,
    pub(super) defaults: BTreeSet<PathBuf>,
}

impl Walked {
    /// A file's workspace-relative key, or its full path when it lies outside the workspace,
    /// which no hashed key matches.
    fn name(&self, file: &Path) -> String {
        relative_key(&self.normal_root, file).unwrap_or_else(|_| file.display().to_string())
    }

    /// Refuses an absent path the build cannot watch for: one whose nearest existing directory
    /// is the workspace root, which holds the target directory the build itself writes, or lies
    /// outside the workspace. Each absent path is watched through that directory.
    pub(super) fn refuse_unwatchable(&self) -> io::Result<()> {
        let absent_reads = (self.data_reads.iter())
            .filter(|(file, _)| !file.is_file())
            .filter_map(|(file, readers)| Some((file, readers.first()?)));
        for (file, declarer) in self.missing.iter().chain(absent_reads) {
            let directory = file
                .ancestors()
                .skip(1)
                .find(|directory| directory.is_dir());
            if directory.is_some_and(|directory| {
                directory != self.normal_root && directory.starts_with(&self.normal_root)
            }) {
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} names {}, which does not exist, and its nearest existing directory is the \
                     workspace root or outside it, so the build cannot watch for it",
                    self.name(declarer),
                    file.display()
                ),
            ));
        }
        Ok(())
    }

    /// Each existing file read as data by a hashed file, with its workspace-relative key.
    /// `hashed` says whether a workspace-relative key is a hashed input. A read from a hashed file
    /// of a file outside the workspace is an error, because the hash could not name it.
    pub(crate) fn data_inputs(
        &self,
        hashed: impl Fn(&str) -> bool,
    ) -> io::Result<Vec<(String, &Path)>> {
        let mut inputs = Vec::new();
        for (file, readers) in &self.data_reads {
            let hashed_reader = readers.iter().find(|reader| hashed(&self.name(reader)));
            let Some(reader) = hashed_reader.filter(|_| file.is_file()) else {
                continue;
            };
            let key = relative_key(&self.normal_root, file).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} reads {}, which is outside the workspace, so the content hash cannot \
                         name it",
                        self.name(reader),
                        file.display()
                    ),
                )
            })?;
            inputs.push((key, file.as_path()));
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
            let target = self.name(file);
            for includer in includers {
                let includer = self.name(includer);
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
            let target = self.name(file);
            for (declarer, declaration) in declarations {
                let declarer = self.name(declarer);
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
        // A declaration that can resolve to a hashed file, or whose default location is hashed,
        // is a semantic slot: whoever declares it, every file it can resolve to must be hashed.
        for slot in &self.slots {
            let semantic = (slot.possible.iter().chain(&slot.defaults))
                .any(|file| file.is_file() && hashed(&self.name(file)));
            for file in slot.possible.iter().filter(|_| semantic) {
                if !hashed(&self.name(file)) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{} in {} can resolve to {}, which is not a hashed source, although \
                             another file it can resolve to or its default location is hashed, \
                             so the content hash cannot cover it",
                            slot.declaration,
                            self.name(&slot.declarer),
                            self.name(file)
                        ),
                    ));
                }
            }
        }
        for (file, (declarer, declaration)) in &self.outside {
            let target = self.name(file);
            if !hashed(&target) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{target} is compiled outside the walked source roots or is not Rust, \
                         reached from {} ({declaration}), and is not a hashed source, so the \
                         content hash cannot cover it",
                        self.name(declarer)
                    ),
                ));
            }
        }
        // A file hashed by name that production never compiles leaves its semantics wherever the
        // module that should hold it points, which the list does not name.
        if let Some((file, how)) = self.unreached.first() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} is hashed by name but {how}, so the content hash cannot tell where its \
                     semantics compile",
                    self.name(file)
                ),
            ));
        }
        Ok(())
    }
}
