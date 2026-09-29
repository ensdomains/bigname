use std::{collections::BTreeSet, path::Path};

use anyhow::{Result, bail};

use bigname_domain::vocabulary::parse_alloy_evm_address;

use super::normalize_address;
use crate::{
    DEFAULT_VERIFIED_AUTHORITY_ARMS, MANAGED_UNIVERSAL_RESOLVER_ROLE, SourceManifest,
    UNIVERSAL_RESOLVER_ROLE, VERIFIED_AUTHORITY_ARMS,
};

/// `universal_resolver_implementations` is an `ens_execution` declaration of valid, distinct
/// addresses, and needs the client-facing `universal_resolver` proxy whose chain it is matched
/// against.
pub(super) fn validate_universal_resolver_implementations(
    manifest: &SourceManifest,
    path: &Path,
) -> Result<()> {
    if manifest.source_family == "ens_execution" {
        let mut proxies = BTreeSet::new();
        for contract in manifest.contracts.iter().filter(|contract| {
            matches!(
                contract.role.as_str(),
                UNIVERSAL_RESOLVER_ROLE | MANAGED_UNIVERSAL_RESOLVER_ROLE
            )
        }) {
            if !proxies.insert(normalize_address(&contract.address)) {
                bail!(
                    "manifest {} overlaps Universal Resolver proxy roles at address {}",
                    path.display(),
                    contract.address
                );
            }
        }
    }
    if manifest.universal_resolver_implementations.is_empty() {
        return Ok(());
    }
    if manifest.source_family != "ens_execution" {
        bail!(
            "manifest {} declares universal_resolver_implementations, which only source family ens_execution may declare",
            path.display()
        );
    }
    if !manifest
        .contracts
        .iter()
        .any(|contract| contract.role == UNIVERSAL_RESOLVER_ROLE)
    {
        bail!(
            "manifest {} declares universal_resolver_implementations without a {UNIVERSAL_RESOLVER_ROLE} contract",
            path.display()
        );
    }
    let mut seen = BTreeSet::new();
    for address in &manifest.universal_resolver_implementations {
        if parse_alloy_evm_address(address).is_err() {
            bail!(
                "manifest {} has invalid universal resolver implementation address {address}",
                path.display()
            );
        }
        if manifest.contracts.iter().any(|contract| {
            matches!(
                contract.role.as_str(),
                UNIVERSAL_RESOLVER_ROLE | MANAGED_UNIVERSAL_RESOLVER_ROLE
            ) && normalize_address(&contract.address) == normalize_address(address)
        }) {
            bail!(
                "manifest {} universal resolver implementation address {address} overlaps declared Universal Resolver proxy",
                path.display()
            );
        }
        if !seen.insert(normalize_address(address)) {
            bail!(
                "manifest {} duplicates universal resolver implementation address {address}",
                path.display()
            );
        }
    }
    Ok(())
}

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
