use std::{collections::BTreeSet, path::Path};

use anyhow::{Result, bail};

use crate::{DEFAULT_VERIFIED_AUTHORITY_ARMS, SourceManifest, VERIFIED_AUTHORITY_ARMS};

pub(super) fn validate_verified_authority_arms(
    manifest: &SourceManifest,
    path: &Path,
) -> Result<()> {
    let Some(arms) = &manifest.verified_authority_arms else {
        return Ok(());
    };
    if manifest.source_family != "ens_execution" {
        bail!(
            "manifest {} declares verified_authority_arms, which only source family ens_execution may declare",
            path.display()
        );
    }
    if arms.is_empty() {
        bail!(
            "manifest {} declares empty verified_authority_arms; omit the field to admit the default {:?}",
            path.display(),
            DEFAULT_VERIFIED_AUTHORITY_ARMS
        );
    }
    let mut seen = BTreeSet::new();
    for arm in arms {
        if !VERIFIED_AUTHORITY_ARMS.contains(&arm.as_str()) {
            bail!(
                "manifest {} declares unknown verified authority arm {arm:?}; expected one of {:?}",
                path.display(),
                VERIFIED_AUTHORITY_ARMS
            );
        }
        if !seen.insert(arm.as_str()) {
            bail!(
                "manifest {} duplicates verified authority arm {arm:?}",
                path.display()
            );
        }
    }
    Ok(())
}
pub(super) fn validate_start_block_fits_i64(
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
