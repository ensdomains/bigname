use std::{collections::BTreeSet, path::Path};

use anyhow::{Result, bail};

use crate::{ResolverReadFeature, SourceManifest};

pub(super) fn validate_read_features(manifest: &SourceManifest, path: &Path) -> Result<()> {
    for implementation in &manifest.resolver_implementations {
        validate_unique(
            &implementation.read_features,
            "resolver implementation",
            &implementation.role,
            path,
        )?;
    }

    for contract in &manifest.contracts {
        validate_unique(&contract.read_features, "contract", &contract.role, path)?;
        if !contract.read_features.is_empty() && !contract.role.contains("resolver") {
            bail!(
                "manifest contract {} in {} declares resolver read features on a non-resolver role",
                contract.role,
                path.display()
            );
        }
        if !contract.read_features.is_empty() && contract.proxy_kind != "none" {
            bail!(
                "manifest proxy contract {} in {} must declare implementation-sensitive read features on resolver_implementations",
                contract.role,
                path.display()
            );
        }
    }
    Ok(())
}

fn validate_unique(
    features: &[ResolverReadFeature],
    declaration_kind: &str,
    role: &str,
    path: &Path,
) -> Result<()> {
    let mut unique = BTreeSet::new();
    for feature in features {
        if !unique.insert(*feature) {
            bail!(
                "manifest {declaration_kind} {role} in {} duplicates read feature {:?}",
                path.display(),
                feature
            );
        }
    }
    Ok(())
}
