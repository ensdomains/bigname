use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_domain::vocabulary::parse_alloy_evm_address;

use crate::{ENSV1_MIRROR_REGISTRY_CORRELATION_KEY, ENSV1_MIRROR_RESOLVER_ROLE, LoadedManifest};

/// A declared ENSv1 mirror resolver is an exact `ens_v2_resolver_l1` instance that reads one
/// ENSv1 registry; the manifest names that registry so the loader can hold it to the active
/// `ens_v1_registry_l1` declaration Project serves the mirrored records from. A family may declare
/// several mirror instances; every one is validated and they share the family's correlation.
pub(super) fn validate_mirror_declarations(manifests: &[LoadedManifest]) -> Result<()> {
    for loaded in manifests {
        let manifest = &loaded.manifest;
        let mirrors: Vec<_> = manifest
            .contracts
            .iter()
            .filter(|contract| contract.role == ENSV1_MIRROR_RESOLVER_ROLE)
            .collect();
        if mirrors.is_empty() {
            continue;
        }
        if manifest.source_family != "ens_v2_resolver_l1" {
            bail!(
                "manifest {} declares {ENSV1_MIRROR_RESOLVER_ROLE} outside ens_v2_resolver_l1",
                loaded.relative_path.display(),
            );
        }
        let mut addresses = BTreeSet::new();
        for mirror in &mirrors {
            if mirror.proxy_kind != "none" || !mirror.read_features.is_empty() {
                bail!(
                    "manifest {} must declare {ENSV1_MIRROR_RESOLVER_ROLE} with proxy_kind = \"none\" and no read_features; the mirror stores no records and its getter behavior belongs to the mirrored ENSv1 resolver",
                    loaded.relative_path.display(),
                );
            }
            if !addresses.insert(mirror.address.to_ascii_lowercase()) {
                bail!(
                    "manifest {} declares {ENSV1_MIRROR_RESOLVER_ROLE} address {} more than once",
                    loaded.relative_path.display(),
                    mirror.address.to_ascii_lowercase(),
                );
            }
        }
        let correlation_address = manifest
            .correlation_addresses
            .get(ENSV1_MIRROR_REGISTRY_CORRELATION_KEY)
            .with_context(|| {
                format!(
                    "manifest {} declares {ENSV1_MIRROR_RESOLVER_ROLE} without correlation address {ENSV1_MIRROR_REGISTRY_CORRELATION_KEY}",
                    loaded.relative_path.display(),
                )
            })?;
        let correlation_address = parse_alloy_evm_address(correlation_address)
            .context("validated mirror correlation address did not parse")?;
        if !manifest.rollout_status.is_active() {
            continue;
        }
        let Some(registry_family) = manifests.iter().find(|candidate| {
            candidate.manifest.rollout_status.is_active()
                && candidate.manifest.namespace == manifest.namespace
                && candidate.manifest.chain == manifest.chain
                && candidate.manifest.source_family == "ens_v1_registry_l1"
        }) else {
            continue;
        };
        let declared_address = registry_family
            .manifest
            .contracts
            .iter()
            .find(|contract| contract.role == "registry")
            .map(|contract| contract.address.as_str())
            .with_context(|| {
                format!(
                    "active ens_v1_registry_l1 manifest {} paired with mirror declaration {} does not declare contract role registry",
                    registry_family.relative_path.display(),
                    loaded.relative_path.display(),
                )
            })?;
        let declared_address = parse_alloy_evm_address(declared_address)
            .context("validated ENSv1 registry contract address did not parse")?;
        if correlation_address != declared_address {
            bail!(
                "manifest {} declares correlation address {ENSV1_MIRROR_REGISTRY_CORRELATION_KEY} {correlation_address}, but active ens_v1_registry_l1 manifest {} declares registry {declared_address}",
                loaded.relative_path.display(),
                registry_family.relative_path.display(),
            );
        }
    }

    Ok(())
}
