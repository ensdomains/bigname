use std::{collections::BTreeSet, path::Path};

use anyhow::{Result, bail};

use crate::{SourceManifest, is_repeatable_contract_role};

/// A singleton role is declared at most once per manifest. A repeatable role
/// ([`crate::REPEATABLE_CONTRACT_ROLES`]) may be declared once per address, never twice at one
/// address, because each declaration is the instance at that address.
pub(super) fn validate_contract_roles(
    manifest: &SourceManifest,
    version_tag: &str,
    path: &Path,
) -> Result<()> {
    let mut singletons = BTreeSet::new();
    let mut instances = BTreeSet::new();
    for contract in &manifest.contracts {
        if is_repeatable_contract_role(&contract.role) {
            if !instances.insert((
                contract.role.as_str(),
                contract.address.to_ascii_lowercase(),
            )) {
                bail!(
                    "source family {} manifest version {} in {} declares contract role {} address {} more than once",
                    manifest.source_family,
                    version_tag,
                    path.display(),
                    contract.role,
                    contract.address.to_ascii_lowercase()
                );
            }
        } else if !singletons.insert(contract.role.as_str()) {
            bail!(
                "source family {} manifest version {} in {} duplicates contract role {}",
                manifest.source_family,
                version_tag,
                path.display(),
                contract.role
            );
        }
    }
    Ok(())
}
