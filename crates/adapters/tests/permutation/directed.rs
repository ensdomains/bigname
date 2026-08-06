use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput, RawLogInput,
};
use bigname_manifests::LoadedManifest;
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

/// The production lease-release corpus behind the binding foreign-key crash: a registrar lease
/// that lapses at a bare block boundary and hands authority back to the registry.
const BINDING_FK_RELEASE: &str = include_str!("../fixtures/interpreters/binding-fk-release.json");

#[derive(Deserialize)]
struct Fixture {
    case: Case,
    batches: Vec<BlockRange>,
    expected: Expected,
}

#[derive(Clone, Copy, Deserialize)]
struct BlockRange {
    from_block: i64,
    to_block: i64,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    manifests: Vec<FixtureManifest>,
    blocks: Vec<Block>,
    logs: Vec<Log>,
}

#[derive(Deserialize)]
struct FixtureManifest {
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    file_path: String,
    role: String,
    address: String,
    contract_instance_id: Uuid,
}

#[derive(Deserialize)]
struct Block {
    hash: String,
    number: i64,
    timestamp: i64,
}

#[derive(Deserialize)]
struct Log {
    chain: String,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    topics: Vec<String>,
    data: String,
}

#[derive(Deserialize)]
struct Expected {
    logical_name_id: String,
    resource_id: Uuid,
    surface_binding_id: Uuid,
    release_block_number: i64,
}

pub struct Directed {
    pub id: String,
    pub input: BatchInput,
    pub declared_instances: Vec<Uuid>,
    /// The physical batch boundaries the corpus was captured with, one block each.
    pub batches: Vec<std::ops::Range<usize>>,
    expected: Expected,
}

impl Directed {
    pub fn lease_release(checked_in: &[LoadedManifest]) -> Result<Self> {
        let Fixture {
            case,
            batches,
            expected,
        } = serde_json::from_str(BINDING_FK_RELEASE)?;
        let chain_id = case
            .manifests
            .first()
            .context("lease-release fixture has no manifest")?
            .chain
            .clone();
        let mut manifests = Vec::new();
        let mut discovery_rules = Vec::new();
        let mut admissions = Vec::new();
        let mut declared_instances = Vec::new();
        for (index, entry) in case.manifests.iter().enumerate() {
            let manifest_id = i64::try_from(index + 1)?;
            let loaded = find_checked_in(entry, checked_in)?;
            let source = &loaded.manifest;
            let mut payload = serde_json::to_value(source)?;
            payload["manifest_version"] = Value::from(1);
            manifests.push(ManifestInput {
                manifest_id,
                manifest_version: 1,
                namespace: entry.namespace.clone(),
                source_family: entry.source_family.clone(),
                chain_id: entry.chain.clone(),
                deployment_label: entry.deployment_epoch.clone(),
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
            admissions.push(AddressAdmissionInput {
                address: entry.address.to_ascii_lowercase(),
                contract_instance_id: entry.contract_instance_id,
                source_manifest_id: Some(manifest_id),
                role: Some(entry.role.clone()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            });
            declared_instances.push(entry.contract_instance_id);
        }
        let mut blocks = Vec::new();
        for block in &case.blocks {
            blocks.push(RawBlockInput {
                chain_id: chain_id.clone(),
                block_hash: block.hash.clone(),
                block_number: block.number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)?,
                canonicality_state: "canonical".to_owned(),
            });
        }
        let mut raw_logs = Vec::new();
        for log in &case.logs {
            let block = case
                .blocks
                .iter()
                .find(|block| block.hash == log.block_hash)
                .with_context(|| format!("fixture log references block {}", log.block_hash))?;
            raw_logs.push(RawLogInput {
                chain_id: log.chain.clone(),
                block_hash: log.block_hash.clone(),
                block_number: log.block_number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)?,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: log.transaction_hash.clone(),
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                emitting_address: log.emitting_address.to_ascii_lowercase(),
                topics: log.topics.clone(),
                data: alloy_primitives::hex::decode(
                    log.data.strip_prefix("0x").unwrap_or(&log.data),
                )?,
            });
        }
        raw_logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
        let mut ranges = Vec::with_capacity(batches.len());
        for batch in &batches {
            let start = blocks
                .iter()
                .position(|block| block.block_number >= batch.from_block)
                .with_context(|| format!("fixture batch {} has no blocks", batch.from_block))?;
            let end = blocks
                .iter()
                .rposition(|block| block.block_number <= batch.to_block)
                .with_context(|| format!("fixture batch {} has no blocks", batch.to_block))?;
            ranges.push(start..end + 1);
        }
        if ranges.iter().map(|range| range.len()).sum::<usize>() != blocks.len() {
            bail!("fixture batches do not cover every block exactly once");
        }
        Ok(Self {
            id: case.id,
            batches: ranges,
            input: BatchInput {
                chain_id,
                manifests,
                discovery_rules,
                admissions,
                prior_events: Vec::new(),
                blocks,
                raw_logs,
            },
            declared_instances,
            expected,
        })
    }

    /// Guards the directed case against silently degenerating into a sequence that never releases.
    pub fn assert_release_reached(&self, outputs: &[BatchOutput]) -> Result<()> {
        let released = outputs
            .iter()
            .flat_map(|output| &output.normalized_events)
            .any(|event| {
                event.event_kind == "AuthorityEpochChanged"
                    && event.block_number == Some(self.expected.release_block_number)
                    && event.logical_name_id.as_deref()
                        == Some(self.expected.logical_name_id.as_str())
                    && event.resource_id == Some(self.expected.resource_id)
            });
        if !released {
            bail!(
                "{}: lapsed lease never settled at a block boundary",
                self.id
            );
        }
        let bound = outputs
            .iter()
            .flat_map(|output| &output.surface_bindings)
            .any(|binding| binding.surface_binding_id == self.expected.surface_binding_id);
        if !bound {
            bail!(
                "{}: release never opened its registry fallback binding",
                self.id
            );
        }
        Ok(())
    }
}

fn find_checked_in<'a>(
    entry: &FixtureManifest,
    checked_in: &'a [LoadedManifest],
) -> Result<&'a LoadedManifest> {
    let path = std::path::Path::new(&entry.file_path);
    let version = path
        .file_name()
        .context("fixture manifest path has no version file")?;
    let suffix = std::path::Path::new(&entry.source_family).join(version);
    let mut matches = checked_in.iter().filter(|loaded| {
        loaded.manifest.namespace == entry.namespace
            && loaded.manifest.source_family == entry.source_family
            && loaded.manifest.chain == entry.chain
            && loaded.manifest.deployment_epoch == entry.deployment_epoch
            && loaded.relative_path.ends_with(&suffix)
    });
    let found = matches.next().with_context(|| {
        format!(
            "fixture manifest {} has no checked-in match",
            entry.file_path
        )
    })?;
    if matches.next().is_some() {
        bail!(
            "fixture manifest {} has more than one checked-in match",
            entry.file_path
        );
    }
    Ok(found)
}
