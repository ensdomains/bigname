use anyhow::{Result, bail};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileAdmission {
    pub chain: String,
    pub source_family: String,
    /// The persistent contract identity when classification came from a
    /// manifest declaration or discovery edge. Match-all resolver logs and
    /// registry resolver pointers can supply an address-only classification
    /// input, so this is absent for those inputs.
    pub contract_instance_id: Option<Uuid>,
    pub address: String,
    pub source: Option<WatchedContractSource>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractChainSummary {
    pub chain: String,
    pub unique_contract_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
}
