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
    /// Set when a coin type `60` read serves the `default.reverse` claim although the
    /// `addr.reverse` node has a nonzero resolver. The projection may not hold that resolver's
    /// live name (an unadmitted or event-silent resolver), so the served name need not be the
    /// one the chain answers.
    pub default_past_resolver: bool,
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
}
