use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::model::RawSourceManifest;
use crate::{
    LoadedManifest, ManifestLoadStatus, ManifestLoadSummary, ManifestRepository, SourceManifest,
};
pub fn load_repository(root: impl AsRef<Path>) -> Result<ManifestRepository> {
    let root = root.as_ref();
    let display_root = canonicalize_for_logging(root);

    if !root.exists() {
        return Ok(ManifestRepository {
            root: display_root.clone(),
            manifests: Vec::new(),
            summary: ManifestLoadSummary {
                root: display_root,
                status: ManifestLoadStatus::MissingRoot,
                namespace_count: 0,
                source_family_count: 0,
                manifest_count: 0,
            },
        });
    }

    if !root.is_dir() {
        return Ok(ManifestRepository {
            root: display_root.clone(),
            manifests: Vec::new(),
            summary: ManifestLoadSummary {
                root: display_root,
                status: ManifestLoadStatus::InvalidRoot,
                namespace_count: 0,
                source_family_count: 0,
                manifest_count: 0,
            },
        });
    }

    let mut manifests = Vec::new();
    let mut namespace_count = 0;
    let mut source_family_count = 0;

    for namespace in read_dir_sorted(root)
        .with_context(|| format!("failed to read manifests root {}", root.display()))?
    {
        if !namespace
            .file_type()
            .with_context(|| format!("failed to inspect {}", namespace.path().display()))?
            .is_dir()
        {
            continue;
        }

        namespace_count += 1;
        let namespace_name = namespace.file_name().to_string_lossy().into_owned();

        for source_family in read_dir_sorted(&namespace.path()).with_context(|| {
            format!(
                "failed to read namespace directory {}",
                namespace.path().display()
            )
        })? {
            if !source_family
                .file_type()
                .with_context(|| format!("failed to inspect {}", source_family.path().display()))?
                .is_dir()
            {
                continue;
            }

            source_family_count += 1;
            let source_family_name = source_family.file_name().to_string_lossy().into_owned();

            for manifest in read_dir_sorted(&source_family.path()).with_context(|| {
                format!(
                    "failed to read source family directory {}",
                    source_family.path().display()
                )
            })? {
                if !manifest
                    .file_type()
                    .with_context(|| format!("failed to inspect {}", manifest.path().display()))?
                    .is_file()
                {
                    continue;
                }

                if manifest.path().extension().and_then(|part| part.to_str()) != Some("toml") {
                    continue;
                }

                manifests.push(load_manifest_file(
                    root,
                    &manifest.path(),
                    &namespace_name,
                    &source_family_name,
                )?);
            }
        }
    }

    let manifest_count = manifests.len();
    let status = if manifests.is_empty() {
        ManifestLoadStatus::Empty
    } else {
        ManifestLoadStatus::Loaded
    };

    Ok(ManifestRepository {
        root: display_root.clone(),
        manifests,
        summary: ManifestLoadSummary {
            root: display_root,
            status,
            namespace_count,
            source_family_count,
            manifest_count,
        },
    })
}

fn load_manifest_file(
    root: &Path,
    path: &Path,
    namespace_name: &str,
    source_family_name: &str,
) -> Result<LoadedManifest> {
    let relative_path = path
        .strip_prefix(root)
        .with_context(|| {
            format!(
                "manifest path {} is not under repository root {}",
                path.display(),
                root.display()
            )
        })?
        .to_path_buf();
    let version_tag = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("manifest path {} is missing a file stem", path.display()))?;
    let raw_manifest = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest file {}", path.display()))?;
    let manifest: SourceManifest = toml::from_str::<RawSourceManifest>(&raw_manifest)
        .with_context(|| format!("failed to parse manifest TOML {}", path.display()))?
        .into();

    validate_manifest_metadata(
        &manifest,
        path,
        &relative_path,
        namespace_name,
        source_family_name,
    )?;

    Ok(LoadedManifest {
        path: path.to_path_buf(),
        relative_path,
        version_tag,
        manifest,
    })
}

fn validate_manifest_metadata(
    manifest: &SourceManifest,
    path: &Path,
    relative_path: &Path,
    namespace_name: &str,
    source_family_name: &str,
) -> Result<()> {
    let depth = relative_path.iter().count();
    if depth != 3 {
        bail!(
            "manifest path {} must match manifests/<namespace>/<source_family>/<version>.toml",
            path.display()
        );
    }

    if manifest.namespace != namespace_name {
        bail!(
            "manifest namespace {} does not match directory {} for {}",
            manifest.namespace,
            namespace_name,
            path.display()
        );
    }

    if manifest.source_family != source_family_name {
        bail!(
            "manifest source_family {} does not match directory {} for {}",
            manifest.source_family,
            source_family_name,
            path.display()
        );
    }

    for root in &manifest.roots {
        validate_start_block_fits_i64(root.start_block, "root", &root.name, path)?;
    }

    for contract in &manifest.contracts {
        validate_start_block_fits_i64(contract.start_block, "contract", &contract.role, path)?;
    }

    Ok(())
}

fn validate_start_block_fits_i64(
    start_block: Option<u64>,
    declaration_kind: &str,
    declaration_name: &str,
    path: &Path,
) -> Result<()> {
    if let Some(start_block) = start_block
        && i64::try_from(start_block).is_err()
    {
        bail!(
            "manifest {declaration_kind} {declaration_name} in {} has start_block {start_block} that does not fit into BIGINT",
            path.display()
        );
    }

    Ok(())
}

fn read_dir_sorted(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)
        .with_context(|| format!("failed to read directory {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to iterate directory {}", path.display()))?;
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn canonicalize_for_logging(root: &Path) -> PathBuf {
    fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf())
}

pub(crate) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}
