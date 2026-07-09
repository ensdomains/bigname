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

pub fn generate_local_profile(
    scratch_dir: &Path,
    repo_root: &Path,
    // keyed by `[[contracts]].role` and `[[roots]].name`
    local_targets: &HashMap<&str, (Address, u64)>,
) -> Result<LocalProfile> {
    let root = scratch_dir.join("manifests-e2e");
    for family in FAMILIES {
        let family_dir = repo_root
            .join("manifests/mainnet/ethereum/ens")
            .join(family);
        let out_dir = root.join("ethereum/ens").join(family);
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
        anyhow::ensure!(mirrored > 0, "no version files mirrored for {family}");
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
        }
    }
    Ok(())
}

fn placeholder_address(label: &str) -> Address {
    Address::from_slice(&keccak256(format!("bigname-e2e-placeholder:{label}"))[12..])
}
