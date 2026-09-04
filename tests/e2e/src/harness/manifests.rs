use std::collections::HashMap;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, keccak256};
use anyhow::{Context, Result};
use toml::Value;

/// Generate a temporary
/// [deployment profile](../../../../docs/glossary.md#deployment-profile) for
/// the local chain by copying
/// every version file of the shipped mainnet ENSv1 family manifests and
/// re-pointing each declared root and contract role at the locally deployed
/// address with its real deploy block. Rollout statuses, capability flags,
/// ABI declarations, and discovery rules are preserved verbatim, so the
/// generated deployment profile carries the shipped semantics — including the active
/// registry v3 admission with its old-registry role and discovery rules.
/// Optional authored root `code_hash` pins are deliberately removed after
/// target substitution because a production hash does not describe the local
/// deployment or a placeholder. The checked-in deployment profiles currently declare no
/// such pins, but this harness does not test production code-hash drift pins.
/// ENSv1 resolver-profile admission remains exact-address classification from
/// the generated declared list; local bytecode identity does not widen it.
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
/// deployment profile is `e2e`; chain identity stays `ethereum-mainnet`, matching the
/// chain label the fixture harness records for the phase-runner.
pub struct LocalProfile {
    pub root: PathBuf,
}

impl LocalProfile {
    /// Present a generated local deployment profile under a non-production chain label.
    /// The production ingest plan deliberately rejects JSON-RPC for
    /// `ethereum-mainnet`; provider-fault scenarios use this alias so they can
    /// exercise the shipped RPC ingest engine without weakening that contract.
    pub fn retarget_chain(&self, from: &str, to: &str) -> Result<()> {
        fn visit(directory: &Path, from: &str, to: &str, changed: &mut usize) -> Result<()> {
            for entry in std::fs::read_dir(directory)? {
                let path = entry?.path();
                if path.is_dir() {
                    visit(&path, from, to, changed)?;
                    continue;
                }
                if path.extension().and_then(|extension| extension.to_str()) != Some("toml") {
                    continue;
                }
                let raw = std::fs::read_to_string(&path)?;
                let mut document: Value = raw.parse()?;
                let Some(chain) = document.get_mut("chain") else {
                    continue;
                };
                if chain.as_str() != Some(from) {
                    continue;
                }
                *chain = Value::String(to.to_owned());
                std::fs::write(&path, toml::to_string(&document)?)?;
                *changed += 1;
            }
            Ok(())
        }

        let mut changed = 0;
        visit(&self.root, from, to, &mut changed)?;
        anyhow::ensure!(
            changed > 0,
            "generated deployment profile contained no {from} manifests"
        );
        Ok(())
    }
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

const ENS_V1_SEPOLIA_MIGRATION_FAMILIES: &[&str] = &[
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_resolver_l1",
    "ens_v1_wrapper_l1",
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
        None,
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
        None,
        families,
    )
}

const MAINNET_GLUE_FAMILIES: &[&str] = &["basenames_l1_compat", "basenames_execution"];

/// Mirror the eleven non-`ens_execution` mainnet families into one generated
/// root: five ENSv1 intake families, four Basenames base-mainnet families, and
/// the two ethereum-chain glue families (`basenames_l1_compat`,
/// `basenames_execution`) that no single-protocol scenario mirrors. The
/// checked-in shadow `ens_execution` family is intentionally omitted here and
/// exercised by the separate verified-resolution scenario. Glue roles a
/// scenario does not deploy get placeholder addresses like any other
/// undeployed role.
pub fn generate_local_mainnet_composed_profile(
    scratch_dir: &Path,
    repo_root: &Path,
    ens_targets: &HashMap<&str, (Address, u64)>,
    basenames_targets: &HashMap<&str, (Address, u64)>,
) -> Result<LocalProfile> {
    let profile = generate_profile_from_families(
        scratch_dir,
        "manifests-e2e",
        repo_root,
        ens_targets,
        None,
        FAMILIES.iter().map(|family| FamilySpec {
            profile_root: "mainnet",
            chain_combo: "ethereum",
            namespace_group: "ens",
            family,
        }),
    )?;
    generate_profile_from_families(
        scratch_dir,
        "manifests-e2e",
        repo_root,
        basenames_targets,
        None,
        BASE_NAMESPACES.iter().map(|family| FamilySpec {
            profile_root: "mainnet",
            chain_combo: "base",
            namespace_group: "basenames",
            family,
        }),
    )?;
    let glue_targets = HashMap::new();
    generate_profile_from_families(
        scratch_dir,
        "manifests-e2e",
        repo_root,
        &glue_targets,
        None,
        MAINNET_GLUE_FAMILIES.iter().map(|family| FamilySpec {
            profile_root: "mainnet",
            chain_combo: "ethereum",
            namespace_group: "basenames",
            family,
        }),
    )?;
    Ok(profile)
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
        None,
        families,
    )
}

pub fn generate_local_sepolia_migration_profile(
    scratch_dir: &Path,
    repo_root: &Path,
    ens_v1_targets: &HashMap<&str, (Address, u64)>,
    ens_v2_targets: &HashMap<&str, (Address, u64)>,
    migration_targets: &HashMap<&str, (Address, u64)>,
    correlation_addresses: &HashMap<&str, Address>,
) -> Result<LocalProfile> {
    let family = |family| FamilySpec {
        profile_root: "sepolia",
        chain_combo: "ethereum",
        namespace_group: "ens",
        family,
    };
    let profile = generate_profile_from_families(
        scratch_dir,
        "manifests-sepolia",
        repo_root,
        ens_v1_targets,
        None,
        ENS_V1_SEPOLIA_MIGRATION_FAMILIES
            .iter()
            .copied()
            .map(family),
    )?;
    generate_profile_from_families(
        scratch_dir,
        "manifests-sepolia",
        repo_root,
        ens_v2_targets,
        None,
        ENS_V2_SEPOLIA_FAMILIES.iter().copied().map(family),
    )?;
    generate_profile_from_families(
        scratch_dir,
        "manifests-sepolia",
        repo_root,
        migration_targets,
        Some(correlation_addresses),
        std::iter::once(family("ens_v2_migration_l1")),
    )?;
    Ok(profile)
}

fn generate_profile_from_families(
    scratch_dir: &Path,
    generated_root: &str,
    repo_root: &Path,
    local_targets: &HashMap<&str, (Address, u64)>,
    correlation_addresses: Option<&HashMap<&str, Address>>,
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
            if spec.family == "ens_v2_migration_l1" {
                let correlations = correlation_addresses.context(
                    "ens_v2_migration_l1 requires local correlation-address substitutions",
                )?;
                patch_correlation_addresses(&mut doc, correlations)?;
            }
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

fn patch_correlation_addresses(
    doc: &mut Value,
    substitutions: &HashMap<&str, Address>,
) -> Result<()> {
    for required in ["ens_v1_name_wrapper", "ens_v1_base_registrar"] {
        anyhow::ensure!(
            substitutions.contains_key(required),
            "missing required migration correlation address {required}"
        );
    }
    let table = doc
        .get_mut("correlation_addresses")
        .context("migration manifest is missing [correlation_addresses]")?
        .as_table_mut()
        .context("migration manifest [correlation_addresses] is not a table")?;
    for (key, address) in substitutions {
        if table.contains_key(*key) {
            table.insert((*key).to_owned(), Value::String(format!("{address:#x}")));
        }
    }
    Ok(())
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
