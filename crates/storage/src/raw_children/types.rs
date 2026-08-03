use crate::CanonicalityState;

/// Persisted exact transaction fact anchored to one observed block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawTransaction {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub from_address: String,
    pub to_address: Option<String>,
    pub canonicality_state: CanonicalityState,
}

/// Persisted exact receipt fact anchored to one observed block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawReceipt {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_address: Option<String>,
    pub status: Option<bool>,
    pub gas_used: Option<i64>,
    pub cumulative_gas_used: Option<i64>,
    pub logs_bloom: Option<Vec<u8>>,
    pub canonicality_state: CanonicalityState,
}

/// Persisted exact log fact anchored to one observed block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawLog {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub emitting_address: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
    pub canonicality_state: CanonicalityState,
}
