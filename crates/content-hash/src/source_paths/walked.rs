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
    /// Absent files an optional `cfg_attr`, `#[path]` or `include!` path names, and the absent
    /// default files of a module that resolves to none. Their nearest existing directories are
    /// watched, so creating one reruns the hash.
    pub(crate) missing: BTreeSet<PathBuf>,
    /// The workspace-relative source roots walked: the crates whose sources are hashed, and each
    /// crate that holds a file hashed by name.
    pub(crate) roots: Vec<String>,
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
                        "{target} is compiled outside the walked source roots or is not Rust, \
                         reached from {} ({declaration}), and is not a hashed source, so the \
                         content hash cannot cover it",
                        relative_key(&self.normal_root, declarer)?
                    ),
                ));
            }
        }
        Ok(())
    }
}
