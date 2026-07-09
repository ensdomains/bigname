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
    generate_local_profile_with_activation(scratch_dir, repo_root, local_targets, &[])
}

/// Same as [`generate_local_profile`], but forces `rollout_status = "active"`
/// for the named source families. The shipped mainnet profile currently
/// leaves `ens_v1_registry_l1` as a deprecated bootstrap seed with no start
/// block, so registry-driven facts (subname ownership, declared resolver
/// bindings, registry-owner changes) are not ingested by a faithful mirror.
/// Scenarios that exercise those paths opt in here — an explicit divergence
/// from the shipped profile, not a reproduction of it.
pub fn generate_local_profile_with_activation(
    scratch_dir: &Path,
    repo_root: &Path,
    local_targets: &HashMap<&str, (Address, u64)>,
    activate_families: &[&str],
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

        if activate_families.contains(family) {
            let table = doc.as_table_mut().context("manifest doc is not a table")?;
            table.insert("rollout_status".into(), Value::String("active".into()));
            if *family == "ens_v1_registry_l1" {
                append_registry_abi_events(table)?;
            }
        }

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

/// The unwrapped-authority adapter requires these registry event fragments
/// in the *active* manifest ABI; the shipped seed carries none. Fragments
/// match the pinned upstream registry interface
/// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L6 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L9 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L15 @ ens_v1@91c966f).
fn append_registry_abi_events(table: &mut toml::map::Map<String, Value>) -> Result<()> {
    let fragments = [
        (
            "NewOwner",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        ),
        (
            "Transfer",
            "event Transfer(bytes32 indexed node, address owner)",
        ),
        (
            "NewResolver",
            "event NewResolver(bytes32 indexed node, address resolver)",
        ),
        ("NewTTL", "event NewTTL(bytes32 indexed node, uint64 ttl)"),
    ];
    let events: Vec<Value> = fragments
        .into_iter()
        .map(|(name, fragment)| {
            let mut entry = toml::map::Map::new();
            entry.insert("name".into(), Value::String(name.into()));
            entry.insert("fragment".into(), Value::String(fragment.into()));
            entry.insert(
                "emitter_roles".into(),
                Value::Array(vec![Value::String("registry".into())]),
            );
            Value::Table(entry)
        })
        .collect();
    let abi = table
        .entry("abi")
        .or_insert_with(|| Value::Table(toml::map::Map::new()))
        .as_table_mut()
        .context("abi section is not a table")?;
    abi.insert("events".into(), Value::Array(events));
    Ok(())
}
