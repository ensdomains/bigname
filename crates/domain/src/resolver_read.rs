use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const ENSIP19_DEFAULT_COIN_TYPE: u64 = 1 << 31;
pub const ETH_COIN_TYPE: u64 = 60;
pub const ENSIP19_DEFAULT_RECORD_KEY: &str = "addr:2147483648";
const ZERO20_HEX: &str = "0x0000000000000000000000000000000000000000";
/// Reason an indexed read reports when the inventory row's coverage is not authoritative and the
/// row names no reason of its own.
pub const INDEXED_INVENTORY_NOT_AUTHORITATIVE_REASON: &str =
    "indexed_record_inventory_not_authoritative";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolverReadFeature {
    Ensip19DefaultAddress,
    /// The resolver implements `IExtendedResolver`, so callers forward `resolve(name, data)` and
    /// its answer for a name is resolver-defined rather than a node-keyed storage read.
    /// (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L66-L70 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L108-L116 @ ens_v1@91c966f)
    Ensip10ExtendedResolver,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolverReadRule {
    Ensip19DefaultAddress { source_record_key: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexedRecordStatus {
    Success,
    NotFound,
    Unsupported,
    ExecutionFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedRecordDerivation {
    pub rule: ResolverReadFeature,
    pub source_record_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedRecordAnswer {
    pub status: IndexedRecordStatus,
    pub value: Option<Value>,
    pub unsupported_reason: Option<String>,
    pub failure_reason: Option<String>,
    pub derivation: Option<IndexedRecordDerivation>,
}

pub const fn ensip19_chain_from_coin_type(coin_type: u64) -> u32 {
    if coin_type == ETH_COIN_TYPE {
        return 1;
    }
    let candidate = coin_type ^ ENSIP19_DEFAULT_COIN_TYPE;
    if candidate < ENSIP19_DEFAULT_COIN_TYPE {
        candidate as u32
    } else {
        0
    }
}

pub const fn ensip19_default_fallback_target(coin_type: u64) -> bool {
    ensip19_chain_from_coin_type(coin_type) > 0
}

/// Answer one record key from a projected record inventory row.
///
/// The row's coverage gates the whole read. An inventory serves values, derived answers, and
/// authoritative absence only while its coverage is authoritative (`status` `full` or `projected`
/// with no `unsupported_reason`). An `unsupported` row, such as a name behind a resolver whose
/// implementation is not an admitted profile, may retain entries for diagnostics, but they are not
/// answers: every key reports the row's own reason instead
/// (`docs/api-v2-routes.md` § `GET /v1/names/{name}/records`).
pub fn evaluate_indexed_record(
    entries: &Value,
    provenance: &Value,
    coverage: &Value,
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
) -> IndexedRecordAnswer {
    if !coverage_is_authoritative(coverage) {
        return unsupported(
            coverage_unsupported_reason(coverage)
                .unwrap_or(INDEXED_INVENTORY_NOT_AUTHORITATIVE_REASON),
        );
    }

    if let Some(entry) = find_entry(entries, record_key, record_family, selector_key) {
        let exact = answer_from_entry(entry, record_family);
        if exact.status != IndexedRecordStatus::NotFound
            || (record_key == "addr:60"
                && provenance["exact_nonempty_not_found_record_keys"]
                    .as_array()
                    .is_some_and(|keys| keys.iter().any(|key| key.as_str() == Some(record_key))))
        {
            return exact;
        }
    }

    let eligible_coin_type = (record_family == "addr")
        .then(|| selector_key?.parse::<u64>().ok())
        .flatten()
        .is_some_and(ensip19_default_fallback_target);
    if eligible_coin_type && has_ensip19_rule(provenance) {
        let derivation = Some(IndexedRecordDerivation {
            rule: ResolverReadFeature::Ensip19DefaultAddress,
            source_record_key: ENSIP19_DEFAULT_RECORD_KEY.to_owned(),
        });
        if let Some(source) = find_entry(
            entries,
            ENSIP19_DEFAULT_RECORD_KEY,
            "addr",
            Some("2147483648"),
        ) {
            let mut answer = answer_from_entry(source, record_family);
            // A derived answer must use the requested getter's verified decode. The legacy
            // addr(bytes32) path converts the coin-60 bytes to address(0), while the multicoin
            // path returns the non-empty bytes unchanged. Exact stored entries keep their shape.
            // (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
            // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
            if selector_key == Some("60")
                && answer.status == IndexedRecordStatus::Success
                && answer.value.as_ref().and_then(Value::as_str) == Some(ZERO20_HEX)
            {
                answer = not_found();
            }
            return match answer.status {
                IndexedRecordStatus::Success | IndexedRecordStatus::NotFound => {
                    IndexedRecordAnswer {
                        derivation,
                        ..answer
                    }
                }
                IndexedRecordStatus::Unsupported | IndexedRecordStatus::ExecutionFailed => {
                    unsupported("ensip19_default_address_source_unavailable")
                }
            };
        }
        return IndexedRecordAnswer {
            derivation,
            ..not_found()
        };
    }

    not_found()
}

impl IndexedRecordStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::ExecutionFailed => "execution_failed",
        }
    }
}

impl IndexedRecordAnswer {
    pub fn comparison_value(&self) -> Value {
        let mut answer = serde_json::json!({"status": self.status.as_str()});
        if let Some(value) = &self.value {
            answer["value"] = value.clone();
        }
        answer
    }
}

fn find_entry<'a>(
    entries: &'a Value,
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
) -> Option<&'a Value> {
    entries.as_array().and_then(|entries| {
        entries
            .iter()
            .find(|entry| {
                entry.get("record_key").and_then(Value::as_str) == Some(record_key)
                    || (entry.get("record_family").and_then(Value::as_str) == Some(record_family)
                        && entry.get("selector_key").and_then(Value::as_str) == selector_key)
            })
            .or_else(|| {
                (record_key == "avatar").then(|| {
                    entries.iter().find(|entry| {
                        entry.get("record_key").and_then(Value::as_str) == Some("text:avatar")
                    })
                })?
            })
    })
}

fn answer_from_entry(entry: &Value, record_family: &str) -> IndexedRecordAnswer {
    match entry.get("status").and_then(Value::as_str) {
        Some("success") => match indexed_value(entry, record_family) {
            Some(value) => IndexedRecordAnswer {
                status: IndexedRecordStatus::Success,
                value: Some(value),
                unsupported_reason: None,
                failure_reason: None,
                derivation: None,
            },
            None => unsupported("indexed_record_value_malformed"),
        },
        Some("not_found") => IndexedRecordAnswer {
            failure_reason: entry
                .get("failure_reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
            ..not_found()
        },
        Some("unsupported") => unsupported(
            entry
                .get("unsupported_reason")
                .and_then(Value::as_str)
                .unwrap_or("record_not_supported"),
        ),
        Some("execution_failed" | "failed") => IndexedRecordAnswer {
            status: IndexedRecordStatus::ExecutionFailed,
            value: None,
            unsupported_reason: None,
            failure_reason: Some(
                entry
                    .get("failure_reason")
                    .and_then(Value::as_str)
                    .unwrap_or("record_read_failed")
                    .to_owned(),
            ),
            derivation: None,
        },
        _ => unsupported("indexed_record_entry_malformed"),
    }
}

fn indexed_value(entry: &Value, record_family: &str) -> Option<Value> {
    let value = entry.get("value")?;
    let value = value
        .get("value")
        .or_else(|| value.get("bytes"))
        .unwrap_or(value);
    let text = value.as_str()?;
    Some(Value::String(if record_family == "addr" {
        text.to_ascii_lowercase()
    } else {
        text.to_owned()
    }))
}

fn has_ensip19_rule(provenance: &Value) -> bool {
    provenance
        .get("read_rules")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|rule| {
            serde_json::from_value::<ResolverReadRule>(rule.clone()).is_ok_and(|rule| {
                matches!(
                    rule,
                    ResolverReadRule::Ensip19DefaultAddress { ref source_record_key }
                        if source_record_key == ENSIP19_DEFAULT_RECORD_KEY
                )
            })
        })
}

/// Whether a record inventory row's coverage lets its entries answer: coverage `status` is `full`
/// or `projected` and the row carries no `unsupported_reason` key. Any other coverage, including a
/// null reason, fails closed. Routes that render inventory values outside [`evaluate_indexed_record`]
/// use the same test so one row is either serving or unsupported everywhere.
pub fn coverage_is_authoritative(coverage: &Value) -> bool {
    coverage.get("unsupported_reason").is_none()
        && matches!(
            coverage.get("status").and_then(Value::as_str),
            Some("full" | "projected")
        )
}

fn coverage_unsupported_reason(coverage: &Value) -> Option<&str> {
    coverage
        .get("unsupported_reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
}

fn not_found() -> IndexedRecordAnswer {
    IndexedRecordAnswer {
        status: IndexedRecordStatus::NotFound,
        value: None,
        unsupported_reason: None,
        failure_reason: None,
        derivation: None,
    }
}

fn unsupported(reason: &str) -> IndexedRecordAnswer {
    IndexedRecordAnswer {
        status: IndexedRecordStatus::Unsupported,
        value: None,
        unsupported_reason: Some(reason.to_owned()),
        failure_reason: None,
        derivation: None,
    }
}

#[cfg(test)]
mod tests;
