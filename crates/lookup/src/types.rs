use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{LookupError, RecordSelector, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupRequest {
    pub logical_name_id: String,
    pub records: Vec<RecordSelector>,
    /// The name to execute in place of the indexed name's own: another path that reaches the
    /// same ENSv2 token (an alias path, docs/glossary.md#alias-path). The calls go through the
    /// Universal Resolver with this name, so the chain resolves the path requested, and no
    /// indexed answer is compared with them.
    pub requested_path: Option<LookupPath>,
}

/// A name a lookup executes, as the Universal Resolver takes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupPath {
    pub name: String,
    pub dns_name: Vec<u8>,
    pub node: [u8; 32],
}

impl LookupRequest {
    pub fn new(
        logical_name_id: impl Into<String>,
        record_keys: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self> {
        let mut records = record_keys
            .into_iter()
            .map(|key| RecordSelector::parse(key.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        records.sort_by(|left, right| left.record_key.cmp(&right.record_key));
        records.dedup_by(|left, right| left.record_key == right.record_key);
        if records.is_empty() {
            return Err(LookupError::unsupported(
                "verified lookup requires at least one supported record selector",
            ));
        }
        Ok(Self {
            logical_name_id: logical_name_id.into(),
            records,
            requested_path: None,
        })
    }

    /// This request executed for `path` instead of the indexed name's own path.
    pub fn at_path(mut self, path: LookupPath) -> Self {
        self.requested_path = Some(path);
        self
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupRecordStatus {
    Success,
    NotFound,
    Unsupported,
    ExecutionFailed,
}

impl LookupRecordStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::ExecutionFailed => "execution_failed",
        }
    }

    pub(crate) const fn is_comparable(self) -> bool {
        matches!(self, Self::Success | Self::NotFound)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerAction {
    None,
    Written,
    Cleared,
    SkippedCcip,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LookupRecordResult {
    pub record_key: String,
    pub record_family: String,
    pub selector_key: Option<String>,
    pub status: LookupRecordStatus,
    pub value: Option<Value>,
    pub failure_reason: Option<String>,
    pub unsupported_reason: Option<String>,
    pub ccip_read: bool,
    pub ledger_action: LedgerAction,
}

impl LookupRecordResult {
    pub(crate) fn comparison_value(&self) -> Value {
        let mut value = serde_json::json!({ "status": self.status.as_str() });
        if let Some(answer) = &self.value {
            value["value"] = answer.clone();
        }
        value
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LookupPosition {
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LookupResponse {
    pub logical_name_id: String,
    pub name: String,
    pub resolver_chain_id: String,
    pub resolver_address: String,
    pub entrypoint_chain_id: String,
    pub entrypoint_address: String,
    /// The resolver chain's family publication the lookup captured, within the publication lag
    /// tolerance of the stored head.
    pub authoritative_position: LookupPosition,
    /// Exact hash-pinned block used for the live call.
    pub execution_position: LookupPosition,
    pub observed_positions: Value,
    pub records: Vec<LookupRecordResult>,
}
