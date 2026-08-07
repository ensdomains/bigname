use serde::{Deserialize, Serialize};
use sqlx::types::time::OffsetDateTime;

/// Persisted lineage snapshot for one chain block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainLineageBlock {
    pub chain_id: String,
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub block_number: i64,
    pub block_timestamp: OffsetDateTime,
    pub logs_bloom: Option<Vec<u8>>,
    pub transactions_root: Option<String>,
    pub receipts_root: Option<String>,
    pub state_root: Option<String>,
    pub canonicality_state: CanonicalityState,
}

/// Persisted canonicality marker for a lineage row.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, sqlx::Type)]
#[sqlx(type_name = "canonicality_state", rename_all = "lowercase")]
pub enum CanonicalityState {
    Observed,
    Canonical,
    Safe,
    Finalized,
    Orphaned,
}

impl CanonicalityState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Canonical => "canonical",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
            Self::Orphaned => "orphaned",
        }
    }

    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "observed" => Ok(Self::Observed),
            "canonical" => Ok(Self::Canonical),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            "orphaned" => Ok(Self::Orphaned),
            _ => anyhow::bail!("unknown canonicality state {value}"),
        }
    }
}
