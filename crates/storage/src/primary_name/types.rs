use anyhow::{Result, bail};
use serde_json::Value;

/// Persisted declared claim-state for one address, coin_type, and namespace tuple.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryNameCurrentRow {
    pub address: String,
    pub namespace: String,
    pub coin_type: String,
    pub claim_status: PrimaryNameClaimStatus,
    pub raw_claim_name: Option<String>,
    pub claim_provenance: Value,
}

/// Persisted exact-tuple declared claim-state plus claimed-name source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryNameCurrentSnapshot {
    pub row: PrimaryNameCurrentRow,
    pub normalized_claim_name: Option<String>,
    pub claim_name_is_normalized: bool,
}

/// Stable storage representation for projection-owned declared primary-name status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrimaryNameClaimStatus {
    Success,
    NotFound,
    Unsupported,
    InvalidName,
}

impl PrimaryNameClaimStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::InvalidName => "invalid_name",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "success" => Ok(Self::Success),
            "not_found" => Ok(Self::NotFound),
            "unsupported" => Ok(Self::Unsupported),
            "invalid_name" => Ok(Self::InvalidName),
            _ => bail!("unknown primary_names_current claim_status {value}"),
        }
    }
}

pub(super) fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}
