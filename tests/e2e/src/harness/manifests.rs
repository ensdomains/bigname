use std::collections::HashMap;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result};
use toml::Value;

/// Generate a temporary manifest profile for the local chain by copying the
/// shipped mainnet ENSv1 family manifests and re-pointing each declared root
/// and contract role at the locally deployed address with its real deploy
/// block. Roles the scenario does not deploy are re-pointed at deterministic
/// placeholder addresses (no code, no logs) so family shape and ABI
/// admission stay identical to the shipped profile.
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
        let shipped = repo_root
            .join("manifests/mainnet/ethereum/ens")
            .join(family)
            .join("v1.toml");
        let raw = std::fs::read_to_string(&shipped)
            .with_context(|| format!("read shipped manifest {shipped:?}"))?;
        let mut doc: Value = raw.parse().with_context(|| format!("parse {shipped:?}"))?;

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

        let out_dir = root.join("ethereum/ens").join(family);
        std::fs::create_dir_all(&out_dir)?;
        std::fs::write(out_dir.join("v1.toml"), toml::to_string(&doc)?)?;
    }
    Ok(LocalProfile { root })
}

fn placeholder_address(label: &str) -> Address {
    Address::from_slice(&keccak256(format!("bigname-e2e-placeholder:{label}"))[12..])
}
