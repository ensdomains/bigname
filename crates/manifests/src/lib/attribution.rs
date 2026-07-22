use std::collections::BTreeMap;

use anyhow::{Result, bail};

use crate::{LoadedManifest, normalize_address};

pub fn is_block_derived_preimage_source_family(source_family: &str) -> bool {
    matches!(
        source_family,
        "ens_v1_registrar_l1"
            | "basenames_base_registrar"
            | "ens_v1_wrapper_l1"
            | "ens_v2_root_l1"
            | "ens_v2_registry_l1"
            | "ens_v2_registrar_l1"
            | "ens_v2_resolver_l1"
    )
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DeclarationRank {
    ManifestRoot,
    ManifestContract,
}

impl DeclarationRank {
    const fn source_rank(self) -> i32 {
        match self {
            Self::ManifestRoot => 0,
            Self::ManifestContract => 1,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::ManifestRoot => "manifest root",
            Self::ManifestContract => "manifest contract",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct AttributionDeclaration<'a> {
    loaded_manifest: &'a LoadedManifest,
    declaration_kind: &'static str,
    declaration_name: &'a str,
    start_block: Option<u64>,
}

pub(crate) fn validate_block_derived_preimage_attribution(
    manifests: &[LoadedManifest],
) -> Result<()> {
    let mut declarations_by_chain_address_and_rank =
        BTreeMap::<(String, String, DeclarationRank), Vec<AttributionDeclaration<'_>>>::new();

    for loaded_manifest in manifests
        .iter()
        .filter(|loaded_manifest| loaded_manifest.manifest.rollout_status.is_active())
    {
        let manifest = &loaded_manifest.manifest;
        for root in &manifest.roots {
            declarations_by_chain_address_and_rank
                .entry((
                    manifest.chain.clone(),
                    normalize_address(&root.address),
                    DeclarationRank::ManifestRoot,
                ))
                .or_default()
                .push(AttributionDeclaration {
                    loaded_manifest,
                    declaration_kind: "root",
                    declaration_name: &root.name,
                    start_block: root.start_block,
                });
        }
        for contract in &manifest.contracts {
            declarations_by_chain_address_and_rank
                .entry((
                    manifest.chain.clone(),
                    normalize_address(&contract.address),
                    DeclarationRank::ManifestContract,
                ))
                .or_default()
                .push(AttributionDeclaration {
                    loaded_manifest,
                    declaration_kind: "contract role",
                    declaration_name: &contract.role,
                    start_block: contract.start_block,
                });
        }
    }

    for ((chain, address, rank), declarations) in declarations_by_chain_address_and_rank {
        for (index, left) in declarations.iter().enumerate() {
            for right in &declarations[index + 1..] {
                if std::ptr::eq(left.loaded_manifest, right.loaded_manifest) {
                    continue;
                }

                let left_manifest = &left.loaded_manifest.manifest;
                let right_manifest = &right.loaded_manifest.manifest;
                if !is_block_derived_preimage_source_family(&left_manifest.source_family)
                    && !is_block_derived_preimage_source_family(&right_manifest.source_family)
                {
                    continue;
                }

                let overlap_from = left
                    .start_block
                    .unwrap_or(0)
                    .max(right.start_block.unwrap_or(0));
                bail!(
                    "active manifest declarations could assign one block-derived preimage log to two sources for chain {chain}, address {address}: source family {} {} {} in {} and source family {} {} {} in {} are both {} declarations (runtime priority {}) with open-ended intervals overlapping from block {overlap_from}; remove one overlapping declaration or mark one manifest version non-active",
                    left_manifest.source_family,
                    left.declaration_kind,
                    left.declaration_name,
                    left.loaded_manifest.relative_path.display(),
                    right_manifest.source_family,
                    right.declaration_kind,
                    right.declaration_name,
                    right.loaded_manifest.relative_path.display(),
                    rank.label(),
                    rank.source_rank(),
                );
            }
        }
    }

    Ok(())
}
