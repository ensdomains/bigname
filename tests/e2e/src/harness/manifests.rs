use std::collections::HashMap;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result};
use toml::Value;

/// Generate a temporary manifest profile for the local chain by copying
/// every version file of the shipped mainnet ENSv1 family manifests and
/// re-pointing each declared root and contract role at the locally deployed
/// address with its real deploy block. Rollout statuses, capability flags,
/// ABI declarations, and discovery rules are preserved verbatim, so the
/// generated profile carries the shipped semantics — including the active
/// registry v3 admission with its old-registry role and discovery rules.
/// Roles a scenario does not deploy are re-pointed at deterministic
/// placeholder addresses (no code, no logs). Nothing under the checked-in
/// `manifests/` tree changes.
///
/// Every `v*.toml` in each family directory must be mirrored: families
/// version their manifests in place and the newest active version is the
/// one that admits watch targets — mirroring only `v1.toml` silently drops
/// shipped admission (that exact defect once produced a false "production
/// does not watch the registry" finding).
///
/// The root directory is named `manifests-e2e` so the derived deployment
/// profile is `e2e`; chain identity stays `ethereum-mainnet`, matching the
/// provider label the harness hands the indexer.
pub struct LocalProfile {
    pub root: PathBuf,
}

const FAMILIES: &[&str] = &[
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_resolver_l1",
    "ens_v1_reverse_l1",
    "ens_v1_wrapper_l1",
];
const ENS_EXECUTION_FAMILY: &str = "ens_execution";

const BASE_NAMESPACES: &[&str] = &[
    "basenames_base_registry",
    "basenames_base_registrar",
    "basenames_base_resolver",
    "basenames_base_primary",
];

const ENS_V2_SEPOLIA_FAMILIES: &[&str] = &[
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_registrar_l1",
    "ens_v2_resolver_l1",
];

struct FamilySpec {
    profile_root: &'static str,
    chain_combo: &'static str,
    namespace_group: &'static str,
    family: &'static str,
}

pub fn generate_local_profile(
    scratch_dir: &Path,
    repo_root: &Path,
    // keyed by `[[contracts]].role` and `[[roots]].name`
    local_targets: &HashMap<&str, (Address, u64)>,
) -> Result<LocalProfile> {
    let mut family_names = FAMILIES.to_vec();
    if local_targets.contains_key("universal_resolver") {
        family_names.push(ENS_EXECUTION_FAMILY);
    }
    let families = family_names.into_iter().map(|family| FamilySpec {
        profile_root: "mainnet",
        chain_combo: "ethereum",
        namespace_group: "ens",
        family,
    });
    generate_profile_from_families(
        scratch_dir,
        "manifests-e2e",
        repo_root,
        local_targets,
        families,
    )
}

pub fn generate_local_basenames_profile(
    scratch_dir: &Path,
    repo_root: &Path,
    // keyed by `[[contracts]].role` and `[[roots]].name`
    local_targets: &HashMap<&str, (Address, u64)>,
) -> Result<LocalProfile> {
    let families = BASE_NAMESPACES.iter().map(|family| FamilySpec {
        profile_root: "mainnet",
        chain_combo: "base",
        namespace_group: "basenames",
        family,
    });
    generate_profile_from_families(
        scratch_dir,
        "manifests-e2e",
        repo_root,
        local_targets,
        families,
    )
}

pub fn generate_local_sepolia_profile(
    scratch_dir: &Path,
    repo_root: &Path,
    // keyed by `[[contracts]].role` and `[[roots]].name`
    local_targets: &HashMap<&str, (Address, u64)>,
) -> Result<LocalProfile> {
    let families = ENS_V2_SEPOLIA_FAMILIES.iter().map(|family| FamilySpec {
        profile_root: "sepolia",
        chain_combo: "ethereum",
        namespace_group: "ens",
        family,
    });
    generate_profile_from_families(
        scratch_dir,
        "manifests-sepolia",
        repo_root,
        local_targets,
        families,
    )
}

fn generate_profile_from_families(
    scratch_dir: &Path,
    generated_root: &str,
    repo_root: &Path,
    local_targets: &HashMap<&str, (Address, u64)>,
    families: impl IntoIterator<Item = FamilySpec>,
) -> Result<LocalProfile> {
    let root = scratch_dir.join(generated_root);
    for spec in families {
        let family_dir = repo_root
            .join("manifests")
            .join(spec.profile_root)
            .join(spec.chain_combo)
            .join(spec.namespace_group)
            .join(spec.family);
        let out_dir = root
            .join(spec.chain_combo)
            .join(spec.namespace_group)
            .join(spec.family);
        std::fs::create_dir_all(&out_dir)?;
        let mut mirrored = 0usize;
        for entry in std::fs::read_dir(&family_dir)
            .with_context(|| format!("read shipped family dir {family_dir:?}"))?
        {
            let path = entry?.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !file_name.starts_with('v') || !file_name.ends_with(".toml") {
                continue;
            }
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("read shipped manifest {path:?}"))?;
            let mut doc: Value = raw.parse().with_context(|| format!("parse {path:?}"))?;
            patch_targets(&mut doc, local_targets)?;
            std::fs::write(out_dir.join(file_name), toml::to_string(&doc)?)?;
            mirrored += 1;
        }
        anyhow::ensure!(
            mirrored > 0,
            "no version files mirrored for {}",
            spec.family
        );
    }
    Ok(LocalProfile { root })
}

fn patch_targets(doc: &mut Value, local_targets: &HashMap<&str, (Address, u64)>) -> Result<()> {
    for (section, key) in [("roots", "name"), ("contracts", "role")] {
        let Some(entries) = doc.get_mut(section).and_then(Value::as_array_mut) else {
            continue;
        };
        for entry in entries {
            let Some(label) = entry.get(key).and_then(Value::as_str).map(str::to_owned) else {
                continue;
            };
            let (address, start_block) = local_targets
                .get(label.as_str())
                .copied()
                .unwrap_or_else(|| (placeholder_address(&label), 0));
            let table = entry
                .as_table_mut()
                .context("manifest entry is not a table")?;
            table.insert("address".into(), Value::String(format!("{address:#x}")));
            table.insert("start_block".into(), Value::Integer(start_block as i64));
            table.remove("code_hash");
            if table.contains_key("implementation") {
                let implementation_label = format!("{label}_implementation");
                let implementation = local_targets
                    .get(implementation_label.as_str())
                    .map(|(address, _)| *address)
                    .unwrap_or_else(|| placeholder_address(&implementation_label));
                table.insert(
                    "implementation".into(),
                    Value::String(format!("{implementation:#x}")),
                );
            }
        }
    }
    Ok(())
}

fn placeholder_address(label: &str) -> Address {
    Address::from_slice(&keccak256(format!("bigname-e2e-placeholder:{label}"))[12..])
}
