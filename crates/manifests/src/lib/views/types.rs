use uuid::Uuid;

use crate::WatchedContractSource;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ManifestCodeHashObservation {
    pub chain: String,
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
    pub block_hash: String,
    pub block_number: i64,
    pub code_hash: String,
    pub code_byte_length: i64,
    pub canonicality_state: String,
}
