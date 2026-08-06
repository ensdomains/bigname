use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput,
    RawLogInput,
};
use bigname_manifests::{LoadedManifest, load_repository};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

/// One checked-in manifest file admitted into a generated scenario.
pub struct SourceSlot {
    pub family: &'static str,
    pub version_file: &'static str,
}

/// One admitted address inside a scenario, carrying the emitter role the manifest declares.
pub struct RoleSlot {
    pub family: &'static str,
    pub role: &'static str,
}

pub struct World {
    pub label: &'static str,
    pub namespace: &'static str,
    pub chain_id: &'static str,
    pub deployment_epoch: &'static str,
    pub address_base: u64,
    pub sources: &'static [SourceSlot],
    pub roles: &'static [RoleSlot],
}

pub const ENS_V1_MAINNET: World = World {
    label: "ens_v1_mainnet",
    namespace: "ens",
    chain_id: "ethereum-mainnet",
    deployment_epoch: "ens_v1",
    address_base: 0x0001_0000,
    sources: &[
        SourceSlot {
            family: "ens_v1_registry_l1",
            version_file: "v3.toml",
        },
        SourceSlot {
            family: "ens_v1_registrar_l1",
            version_file: "v1.toml",
        },
        SourceSlot {
            family: "ens_v1_wrapper_l1",
            version_file: "v1.toml",
        },
        SourceSlot {
            family: "ens_v1_resolver_l1",
            version_file: "v1.toml",
        },
        SourceSlot {
            family: "ens_v1_reverse_l1",
            version_file: "v1.toml",
        },
    ],
    roles: &[
        RoleSlot {
            family: "ens_v1_registry_l1",
            role: "registry",
        },
        RoleSlot {
            family: "ens_v1_registrar_l1",
            role: "registrar",
        },
        RoleSlot {
            family: "ens_v1_registrar_l1",
            role: "legacy_registrar_controller",
        },
        RoleSlot {
            family: "ens_v1_registrar_l1",
            role: "wrapped_registrar_controller",
        },
        RoleSlot {
            family: "ens_v1_registrar_l1",
            role: "unwrapped_registrar_controller",
        },
        RoleSlot {
            family: "ens_v1_wrapper_l1",
            role: "name_wrapper",
        },
        RoleSlot {
            family: "ens_v1_resolver_l1",
            role: "public_resolver",
        },
        RoleSlot {
            family: "ens_v1_reverse_l1",
            role: "reverse_registrar",
        },
    ],
};

pub const ENS_V2_SEPOLIA: World = World {
    label: "ens_v2_sepolia",
    namespace: "ens",
    chain_id: "ethereum-sepolia",
    deployment_epoch: "ens_v2_sepolia_post_audit",
    address_base: 0x0002_0000,
    sources: &[
        SourceSlot {
            family: "ens_v2_root_l1",
            version_file: "v2.toml",
        },
        SourceSlot {
            family: "ens_v2_registry_l1",
            version_file: "v2.toml",
        },
        SourceSlot {
            family: "ens_v2_registrar_l1",
            version_file: "v3.toml",
        },
        SourceSlot {
            family: "ens_v2_resolver_l1",
            version_file: "v2.toml",
        },
    ],
    roles: &[
        RoleSlot {
            family: "ens_v2_root_l1",
            role: "root_registry",
        },
        RoleSlot {
            family: "ens_v2_registry_l1",
            role: "registry",
        },
        RoleSlot {
            family: "ens_v2_registrar_l1",
            role: "registrar",
        },
        RoleSlot {
            family: "ens_v2_resolver_l1",
            role: "resolver",
        },
    ],
};

pub struct Wiring {
    pub chain_id: String,
    manifests: Vec<ManifestInput>,
    discovery_rules: Vec<DiscoveryRuleInput>,
    admissions: Vec<AddressAdmissionInput>,
    addresses: BTreeMap<(&'static str, &'static str), String>,
    instances: BTreeMap<String, Uuid>,
}

impl Wiring {
    pub fn build(world: &World, checked_in: &[LoadedManifest]) -> Result<Self> {
        let mut manifests = Vec::new();
        let mut discovery_rules = Vec::new();
        let mut manifest_ids = BTreeMap::new();
        for (index, slot) in world.sources.iter().enumerate() {
            let manifest_id = i64::try_from(index + 1)?;
            let loaded = find_checked_in(world, slot, checked_in)?;
            let source = &loaded.manifest;
            let mut payload = serde_json::to_value(source)?;
            payload["manifest_version"] = Value::from(1);
            manifests.push(ManifestInput {
                manifest_id,
                manifest_version: 1,
                namespace: world.namespace.to_owned(),
                source_family: slot.family.to_owned(),
                chain_id: world.chain_id.to_owned(),
                deployment_label: world.deployment_epoch.to_owned(),
                normalizer_version: source.normalizer_version.clone(),
                payload_json: serde_json::to_string(&payload)?,
            });
            discovery_rules.extend(
                source
                    .discovery_rules
                    .iter()
                    .map(|rule| DiscoveryRuleInput {
                        manifest_id,
                        edge_kind: rule.edge_kind.clone(),
                        from_role: Some(rule.from_role.clone()),
                        admission: rule.admission.clone(),
                    }),
            );
            manifest_ids.insert(slot.family, manifest_id);
        }

        let mut addresses = BTreeMap::new();
        let mut instances = BTreeMap::new();
        let mut admissions = Vec::new();
        for (index, slot) in world.roles.iter().enumerate() {
            let manifest_id = *manifest_ids
                .get(slot.family)
                .with_context(|| format!("role {} has no admitted manifest", slot.role))?;
            let offset = world.address_base + u64::try_from(index + 1)?;
            let address = format!("0x{offset:040x}");
            let instance =
                Uuid::from_u128(u128::from(world.address_base) << 64 | u128::from(offset));
            admissions.push(AddressAdmissionInput {
                address: address.clone(),
                contract_instance_id: instance,
                source_manifest_id: Some(manifest_id),
                role: Some(slot.role.to_owned()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            });
            instances.insert(address.clone(), instance);
            addresses.insert((slot.family, slot.role), address);
        }

        Ok(Self {
            chain_id: world.chain_id.to_owned(),
            manifests,
            discovery_rules,
            admissions,
            addresses,
            instances,
        })
    }

    pub fn address(&self, family: &'static str, role: &'static str) -> &str {
        self.addresses
            .get(&(family, role))
            .unwrap_or_else(|| panic!("world has no admitted {family}/{role} address"))
    }

    /// Contract identities the manifest already declares; discovery edges may point out of them
    /// without the batch re-emitting a contract-instance row.
    pub fn declared_instances(&self) -> Vec<Uuid> {
        self.instances.values().copied().collect()
    }

    pub fn batch_input(&self, blocks: &[BlockSpec], logs: &[GeneratedLog]) -> Result<BatchInput> {
        let mut raw_blocks = Vec::with_capacity(blocks.len());
        for block in blocks {
            raw_blocks.push(RawBlockInput {
                chain_id: self.chain_id.clone(),
                block_hash: block.hash.clone(),
                block_number: block.number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)?,
                canonicality_state: "canonical".to_owned(),
            });
        }
        let mut raw_logs = Vec::with_capacity(logs.len());
        for log in logs {
            let block = blocks
                .get(log.block_index)
                .with_context(|| format!("generated log references block {}", log.block_index))?;
            raw_logs.push(RawLogInput {
                chain_id: self.chain_id.clone(),
                block_hash: block.hash.clone(),
                block_number: block.number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)?,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: log.transaction_hash.clone(),
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                emitting_address: log.emitter.clone(),
                topics: log.topics.clone(),
                data: log.data.clone(),
            });
        }
        raw_logs.sort_by(|left, right| {
            (left.block_number, left.transaction_index, left.log_index).cmp(&(
                right.block_number,
                right.transaction_index,
                right.log_index,
            ))
        });
        Ok(BatchInput {
            chain_id: self.chain_id.clone(),
            manifests: self.manifests.clone(),
            discovery_rules: self.discovery_rules.clone(),
            admissions: self.admissions.clone(),
            prior_events: Vec::new(),
            blocks: raw_blocks,
            raw_logs,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BlockSpec {
    pub number: i64,
    pub hash: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct GeneratedLog {
    pub block_index: usize,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub emitter: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
}

fn find_checked_in<'a>(
    world: &World,
    slot: &SourceSlot,
    checked_in: &'a [LoadedManifest],
) -> Result<&'a LoadedManifest> {
    let suffix = Path::new(slot.family).join(slot.version_file);
    let mut matches = checked_in.iter().filter(|loaded| {
        loaded.manifest.namespace == world.namespace
            && loaded.manifest.source_family == slot.family
            && loaded.manifest.chain == world.chain_id
            && loaded.manifest.deployment_epoch == world.deployment_epoch
            && loaded.relative_path.ends_with(&suffix)
    });
    let found = matches
        .next()
        .with_context(|| format!("no checked-in manifest for {}", slot.family))?;
    if matches.next().is_some() {
        bail!("more than one checked-in manifest for {}", slot.family);
    }
    Ok(found)
}

/// Topic0 of every event the world's checked-in manifests declare, mapped to the log topic count
/// that event produces. The lane asserts its own fragments against this, so neither a mistyped
/// signature nor a wrong `indexed` marking can silently drop coverage.
pub fn declared_event_topics(
    world: &World,
    checked_in: &[LoadedManifest],
) -> Result<BTreeMap<String, usize>> {
    let mut declared = BTreeMap::new();
    for slot in world.sources {
        let loaded = find_checked_in(world, slot, checked_in)?;
        for event in &loaded.manifest.abi.events {
            let Some(topic0) = event.topic0()? else {
                continue;
            };
            let parsed = event.parsed_event()?;
            let indexed = parsed.inputs.iter().filter(|input| input.indexed).count();
            if let Some(existing) = declared.insert(topic0.to_ascii_lowercase(), indexed + 1)
                && existing != indexed + 1
            {
                bail!(
                    "{} declares topic0 {topic0} with {existing} topics and with {} topics",
                    world.label,
                    indexed + 1
                );
            }
        }
    }
    Ok(declared)
}

pub fn checked_in_manifests() -> Result<Vec<LoadedManifest>> {
    let root = workspace_root()?.join("manifests");
    let mut manifests = Vec::new();
    for profile in ["mainnet", "sepolia"] {
        manifests.extend(
            load_repository(root.join(profile))?
                .manifests()
                .iter()
                .cloned(),
        );
    }
    Ok(manifests)
}

pub fn workspace_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("adapters crate must be two directories below the workspace root")
}
