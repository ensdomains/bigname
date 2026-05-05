use std::{
    collections::BTreeMap,
    hash::Hasher,
    path::{Path, PathBuf},
};

use anyhow::{Result, bail};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Visitor},
};
use uuid::Uuid;

use crate::REACHABLE_FROM_ROOT_ADMISSION;

#[path = "model/abi.rs"]
mod abi;

pub use abi::{ParsedManifestAbiEvent, ParsedManifestAbiFunction};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestRepository {
    pub(crate) root: PathBuf,
    pub(crate) manifests: Vec<LoadedManifest>,
    pub(crate) summary: ManifestLoadSummary,
}

impl ManifestRepository {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn manifests(&self) -> &[LoadedManifest] {
        &self.manifests
    }

    pub fn summary(&self) -> &ManifestLoadSummary {
        &self.summary
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedManifest {
    pub path: PathBuf,
    pub relative_path: PathBuf,
    pub version_tag: String,
    pub manifest: SourceManifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestLoadSummary {
    pub root: PathBuf,
    pub status: ManifestLoadStatus,
    pub namespace_count: usize,
    pub source_family_count: usize,
    pub manifest_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestLoadStatus {
    Loaded,
    Empty,
    MissingRoot,
    InvalidRoot,
}

impl ManifestLoadStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "loaded",
            Self::Empty => "empty",
            Self::MissingRoot => "missing_root",
            Self::InvalidRoot => "invalid_root",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestSyncSummary {
    pub status: ManifestSyncStatus,
    pub synced_manifest_count: usize,
    pub active_manifest_count: usize,
    pub root_count: usize,
    pub contract_count: usize,
    pub capability_count: usize,
    pub discovery_rule_count: usize,
    pub removed_manifest_count: usize,
    pub cleared_discovery_edge_count: usize,
}

impl ManifestSyncSummary {
    pub(crate) fn skipped(status: ManifestSyncStatus) -> Self {
        Self {
            status,
            synced_manifest_count: 0,
            active_manifest_count: 0,
            root_count: 0,
            contract_count: 0,
            capability_count: 0,
            discovery_rule_count: 0,
            removed_manifest_count: 0,
            cleared_discovery_edge_count: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestSyncStatus {
    Synced,
    SkippedMissingRoot,
    SkippedInvalidRoot,
}

impl ManifestSyncStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::SkippedMissingRoot => "skipped_missing_root",
            Self::SkippedInvalidRoot => "skipped_invalid_root",
        }
    }
}

#[derive(Clone, Debug)]
pub struct WatchedContract {
    pub chain: String,
    pub source_family: String,
    pub address: String,
    pub contract_instance_id: Uuid,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
    pub active_from_block_number: Option<i64>,
    pub active_to_block_number: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WatchedContractSource {
    ManifestRoot,
    ManifestContract,
    DiscoveryEdge,
}

impl WatchedContractSource {
    pub(crate) fn from_db_value(value: &str) -> Result<Self> {
        match value {
            "manifest_root" => Ok(Self::ManifestRoot),
            "manifest_contract" => Ok(Self::ManifestContract),
            "discovery_edge" => Ok(Self::DiscoveryEdge),
            _ => bail!("unsupported watched contract source {value}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractSummary {
    pub unique_contract_count: usize,
    pub source_entry_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
    pub chains: Vec<WatchedContractChainSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedChainPlan {
    pub chain: String,
    pub addresses: Vec<String>,
    pub manifest_root_entry_count: usize,
    pub manifest_contract_entry_count: usize,
    pub discovery_edge_entry_count: usize,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WatchedTargetIdentity {
    pub contract_instance_id: Uuid,
}

impl From<Uuid> for WatchedTargetIdentity {
    fn from(contract_instance_id: Uuid) -> Self {
        Self {
            contract_instance_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchedSourceSelectorKind {
    WholeActiveWatchedChain,
    SourceFamily,
    WatchedTargetSet,
}

impl WatchedSourceSelectorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WholeActiveWatchedChain => "whole_active_watched_chain",
            Self::SourceFamily => "source_family",
            Self::WatchedTargetSet => "watched_target_set",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchedSourceSelector {
    WholeActiveWatchedChain,
    SourceFamily(String),
    WatchedTargetSet(Vec<WatchedTargetIdentity>),
}

impl WatchedSourceSelector {
    pub const fn kind(&self) -> WatchedSourceSelectorKind {
        match self {
            Self::WholeActiveWatchedChain => WatchedSourceSelectorKind::WholeActiveWatchedChain,
            Self::SourceFamily(_) => WatchedSourceSelectorKind::SourceFamily,
            Self::WatchedTargetSet(_) => WatchedSourceSelectorKind::WatchedTargetSet,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WatchedBackfillTarget {
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub effective_from_block: i64,
    pub effective_to_block: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedSourceSelectorPlan {
    pub chain: String,
    pub selector_kind: WatchedSourceSelectorKind,
    pub source_family: Option<String>,
    pub requested_watched_targets: Vec<WatchedTargetIdentity>,
    pub selected_targets: Vec<WatchedBackfillTarget>,
    pub watched_chain_plan: WatchedChainPlan,
}

impl WatchedSourceSelectorPlan {
    pub fn source_identity_payload(&self) -> serde_json::Value {
        let mut payload = self.source_identity_payload_without_hash();
        if let serde_json::Value::Object(fields) = &mut payload {
            fields.insert(
                "source_identity_hash".to_owned(),
                serde_json::Value::String(self.source_identity_hash()),
            );
        }
        payload
    }

    pub fn source_identity_hash(&self) -> String {
        let payload = serde_json::to_string(&self.source_identity_payload_without_hash())
            .expect("watched source identity payload must be serializable");
        let mut hasher = StableFnv64::default();
        hasher.write(payload.as_bytes());
        format!("fnv1a64:{:016x}", hasher.finish())
    }

    fn source_identity_payload_without_hash(&self) -> serde_json::Value {
        serde_json::json!({
            "selector_kind": self.selector_kind.as_str(),
            "source_family": self.source_family,
            "requested_watched_targets": self.requested_watched_targets,
            "selected_targets": self.selected_targets,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ManifestBootstrapTarget {
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub effective_from_block: i64,
    pub effective_to_block: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileAdmission {
    pub chain: String,
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
    pub active_from_block_number: Option<i64>,
    pub active_to_block_number: Option<i64>,
    pub profile: String,
    pub fact_family: String,
    pub status: String,
    pub admission_basis: String,
    pub observed_code_hash: Option<String>,
    pub matched_code_hash: Option<String>,
    pub matched_contract_instance_id: Option<Uuid>,
}

#[derive(Default)]
struct StableFnv64(u64);

impl Hasher for StableFnv64 {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.0 == 0 {
            self.0 = 0xcbf29ce484222325;
        }

        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractChainSummary {
    pub chain: String,
    pub unique_contract_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveManifestVersion {
    pub manifest_version: u64,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub normalizer_version: String,
    pub capability_flags: BTreeMap<String, CapabilityFlag>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceManifestSnapshot {
    pub manifests: Vec<ActiveManifestVersion>,
    pub last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceManifest {
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub rollout_status: RolloutStatus,
    pub normalizer_version: String,
    pub capability_flags: BTreeMap<String, CapabilityFlag>,
    pub roots: Vec<ManifestRoot>,
    pub contracts: Vec<ManifestContract>,
    pub discovery_rules: Vec<DiscoveryRule>,
    #[serde(default, skip_serializing_if = "ManifestAbi::is_empty")]
    pub abi: ManifestAbi,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
    Draft,
    Shadow,
    Active,
    Deprecated,
}

impl RolloutStatus {
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Shadow => "shadow",
            Self::Active => "active",
            Self::Deprecated => "deprecated",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySupportStatus {
    Unsupported,
    Shadow,
    Supported,
}

impl CapabilitySupportStatus {
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Shadow => "shadow",
            Self::Supported => "supported",
        }
    }

    pub(crate) fn from_db_value(value: &str) -> Result<Self> {
        match value {
            "unsupported" => Ok(Self::Unsupported),
            "shadow" => Ok(Self::Shadow),
            "supported" => Ok(Self::Supported),
            _ => bail!("unsupported capability status {value}"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityFlag {
    pub status: CapabilitySupportStatus,
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestAbi {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<ManifestAbiEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub calls: Vec<ManifestAbiCall>,
}

impl ManifestAbi {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.calls.is_empty()
    }

    pub fn event_topic0s(&self) -> Result<Vec<String>> {
        let mut topic0s = self
            .events
            .iter()
            .map(|event| event.parsed_event_view().map(|parsed| parsed.topic0()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        topic0s.sort();
        topic0s.dedup();
        Ok(topic0s)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestAbiEvent {
    pub name: String,
    pub fragment: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emitter_roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub normalized_events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<CapabilitySupportStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestAbiCall {
    pub name: String,
    pub fragment: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<CapabilitySupportStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestRoot {
    pub name: String,
    pub address: String,
    pub code_hash: Option<String>,
    pub abi_ref: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_start_block")]
    pub start_block: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestContract {
    pub role: String,
    pub address: String,
    pub proxy_kind: String,
    pub implementation: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_start_block")]
    pub start_block: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRule {
    pub edge_kind: String,
    pub from_role: String,
    #[serde(deserialize_with = "deserialize_authored_discovery_rule_admission")]
    pub admission: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct RawSourceManifest {
    manifest_version: u64,
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    rollout_status: RolloutStatus,
    normalizer_version: String,
    capability_flags: BTreeMap<String, RawCapabilityFlag>,
    roots: Vec<ManifestRoot>,
    contracts: Vec<ManifestContract>,
    discovery_rules: Vec<DiscoveryRule>,
    #[serde(default)]
    abi: ManifestAbi,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
enum RawCapabilityFlag {
    Status(CapabilitySupportStatus),
    Detailed(CapabilityFlag),
}

impl From<RawSourceManifest> for SourceManifest {
    fn from(value: RawSourceManifest) -> Self {
        Self {
            manifest_version: value.manifest_version,
            namespace: value.namespace,
            source_family: value.source_family,
            chain: value.chain,
            deployment_epoch: value.deployment_epoch,
            rollout_status: value.rollout_status,
            normalizer_version: value.normalizer_version,
            capability_flags: value
                .capability_flags
                .into_iter()
                .map(|(name, flag)| {
                    let flag = match flag {
                        RawCapabilityFlag::Status(status) => CapabilityFlag {
                            status,
                            notes: None,
                        },
                        RawCapabilityFlag::Detailed(flag) => flag,
                    };
                    (name, flag)
                })
                .collect(),
            roots: value.roots,
            contracts: value.contracts,
            discovery_rules: value.discovery_rules,
            abi: value.abi,
        }
    }
}

fn deserialize_optional_start_block<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionalStartBlockVisitor;

    impl<'de> Visitor<'de> for OptionalStartBlockVisitor {
        type Value = Option<u64>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a non-negative integer start_block")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(StartBlockVisitor).map(Some)
        }
    }

    struct StartBlockVisitor;

    impl Visitor<'_> for StartBlockVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a non-negative integer start_block")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            u64::try_from(value)
                .map_err(|_| E::custom("start_block must be a non-negative integer"))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value)
        }
    }

    deserializer.deserialize_option(OptionalStartBlockVisitor)
}

fn deserialize_authored_discovery_rule_admission<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let admission = String::deserialize(deserializer)?;
    if admission == REACHABLE_FROM_ROOT_ADMISSION {
        Ok(admission)
    } else {
        Err(de::Error::custom(format!(
            "unsupported authored discovery_rules[].admission \"{admission}\"; expected \"{REACHABLE_FROM_ROOT_ADMISSION}\""
        )))
    }
}
