use bigname_storage::{NameCurrentRow, SelectedSnapshot};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use crate::v2::support::{
    direct_json_field, record_json_path, record_json_string_at_paths,
    record_network_from_chain_positions,
};
use crate::v2::vocab::{PARTIAL_SERVE_UNSUPPORTED_REASON, RegistrationStatus};
use crate::v2::{chains::slug_to_numeric, format_timestamp};

pub(in crate::v2) fn has_current_registration(status: RegistrationStatus) -> bool {
    !matches!(
        status,
        RegistrationStatus::Released | RegistrationStatus::Unregistered
    )
}

pub(in crate::v2) fn row_has_current_registration(row: &NameCurrentRow) -> bool {
    has_current_registration(
        super::name_registration_fields(Some(row), &row.namespace).registration_status,
    ) || bigname_storage::name_current_has_event_linked_registry_serving(row)
}

pub(in crate::v2) fn identity_row_has_current_registration(
    row: &bigname_storage::IdentityNameCurrentRow,
) -> bool {
    has_current_registration(
        super::identity_name_registration_fields(Some(row), &row.namespace).registration_status,
    ) || bigname_storage::identity_name_current_has_event_linked_registry_serving(row)
}

/// Whether the row's declared resolver is served. A `current_authority_not_projected` row keeps
/// retained pointer evidence out of the response unless an event-linked registry serving resource
/// (an ENSv2 TLD's root-registry pointer) selected it.
pub(in crate::v2) fn row_serves_resolver(row: &NameCurrentRow) -> bool {
    row_has_current_registration(row)
        && (string_field(row.coverage.get("unsupported_reason")).as_deref()
            != Some(PARTIAL_SERVE_UNSUPPORTED_REASON)
            || bigname_storage::name_current_has_event_linked_registry_serving(row))
}

pub(in crate::v2) fn identity_row_serves_resolver(
    row: &bigname_storage::IdentityNameCurrentRow,
) -> bool {
    identity_row_has_current_registration(row)
        && (string_field(row.coverage.get("unsupported_reason")).as_deref()
            != Some(PARTIAL_SERVE_UNSUPPORTED_REASON)
            || bigname_storage::identity_name_current_has_event_linked_registry_serving(row))
}

pub(super) fn json_chain_id(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.parse::<u64>().ok().or_else(|| slug_to_numeric(value)),
        _ => None,
    }
}

pub(super) fn response_chain_id(selected_snapshot: &SelectedSnapshot) -> Option<u64> {
    selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find_map(|position| slug_to_numeric(&position.chain_id))
}

pub(super) fn network(row: &NameCurrentRow) -> String {
    network_from_parts(&row.namespace, &row.chain_positions)
}

pub(in crate::v2) fn network_from_parts(namespace: &str, chain_positions: &Value) -> String {
    record_network_from_chain_positions(namespace, chain_positions, direct_json_field)
}

pub(in crate::v2) fn chain_id_from_positions(chain_positions: &Value) -> Option<u64> {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .find_map(|(_, value)| {
            value
                .get("chain_id")
                .and_then(value_to_string)
                .and_then(|value| slug_to_numeric(&value))
        })
}

pub(super) fn object_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|value| value.is_object())
}

pub(in crate::v2) fn json_string_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    record_json_string_at_paths(value, paths, direct_json_field)
}

pub(super) fn json_address_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    json_string_at_paths(value, paths).map(|value| value.to_ascii_lowercase())
}

pub(super) fn json_timestamp_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    for path in paths {
        let Some(value) = record_json_path(value, path, direct_json_field) else {
            continue;
        };
        match value {
            Value::String(value) if !value.trim().is_empty() => match quoted_seconds(value) {
                Some(QuotedSeconds::InRange(seconds)) => {
                    if let Some(timestamp) = format_unix_timestamp(seconds) {
                        return Some(timestamp);
                    }
                }
                Some(QuotedSeconds::OutOfRange) => {}
                None => return Some(value.clone()),
            },
            Value::Number(number) => {
                if let Some(timestamp) = number.as_i64().and_then(format_unix_timestamp) {
                    return Some(timestamp);
                }
            }
            _ => {}
        }
    }
    None
}

pub(in crate::v2) fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(value_to_string)
}

pub(in crate::v2) fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(super) fn json_value_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        _ => true,
    }
}

const LAST_TIMESTAMP_SECOND: u64 = 253_402_300_799;

#[derive(Debug, PartialEq, Eq)]
enum QuotedSeconds {
    /// The whole seconds of a value inside 1970..=9999.
    InRange(i64),
    /// A number before 1970 or after 9999-12-31T23:59:59Z, which reads as unknown.
    OutOfRange,
}

/// A quoted seconds value (`"1735689600"`, `"-1"`, `"1735689600.5"`) reads like the number it
/// spells, or `None` when the string is not a decimal number. The range check uses the complete
/// value, sign and fraction included, the same bounds the collection routes' SQL applies; a value
/// inside the range then keeps only its whole seconds, as Project's `to_char(to_timestamp(..),
/// 'SS')` presents it.
fn quoted_seconds(value: &str) -> Option<QuotedSeconds> {
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, value),
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) if !fraction.is_empty() => (whole, fraction),
        Some(_) => return None,
        None => (unsigned, ""),
    };
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) {
        return None;
    }
    let has_fraction = fraction.bytes().any(|byte| byte != b'0');
    let Ok(whole) = whole.parse::<u64>() else {
        return Some(QuotedSeconds::OutOfRange);
    };
    if (negative && (whole > 0 || has_fraction))
        || whole > LAST_TIMESTAMP_SECOND
        || (whole == LAST_TIMESTAMP_SECOND && has_fraction)
    {
        return Some(QuotedSeconds::OutOfRange);
    }
    Some(i64::try_from(whole).map_or(QuotedSeconds::OutOfRange, QuotedSeconds::InRange))
}

/// A seconds value outside 1970..=9999 reads as unknown, the same rule the collection routes'
/// expiry reads and Project's formatted `control.expiry` apply.
fn format_unix_timestamp(timestamp: i64) -> Option<String> {
    if !u64::try_from(timestamp).is_ok_and(|timestamp| timestamp <= LAST_TIMESTAMP_SECOND) {
        return None;
    }
    let value = OffsetDateTime::from_unix_timestamp(timestamp).ok()?;
    Some(format_timestamp(value))
}

pub(super) fn has_name_binding(row: &NameCurrentRow) -> bool {
    row.surface_binding_id.is_some() || row.resource_id.is_some() || row.binding_kind.is_some()
}

pub(in crate::v2) fn declared_token_id(row: &NameCurrentRow) -> Option<String> {
    declared_token_id_from_parts(
        &row.declared_summary,
        &row.namespace,
        &row.normalized_name,
        None,
    )
}

pub(in crate::v2) fn identity_declared_token_id(
    row: &bigname_storage::IdentityNameCurrentRow,
) -> Option<String> {
    row.resource_id?;
    let labelhash = row.labelhash.as_deref().filter(|value| {
        row.labelhash_count
            .is_none_or(|label_count| label_count == 2)
            && !value.trim().is_empty()
    });
    declared_token_id_from_parts(
        &row.declared_summary,
        &row.namespace,
        &row.normalized_name,
        labelhash,
    )
}

fn declared_token_id_from_parts(
    summary: &Value,
    namespace: &str,
    normalized_name: &str,
    labelhash: Option<&str>,
) -> Option<String> {
    json_string_at_paths(
        summary,
        &[
            &["authority", "token_id"],
            &["registration", "token_id"],
            &["registration", "upstream_resource"],
            &["control", "token_id"],
        ],
    )
    .or_else(|| eth_2ld_labelhash_token_id(namespace, normalized_name, labelhash))
}

fn eth_2ld_labelhash_token_id(
    namespace: &str,
    normalized_name: &str,
    labelhash: Option<&str>,
) -> Option<String> {
    if namespace != "ens" {
        return None;
    }
    let mut labels = normalized_name.split('.');
    let label = labels.next()?;
    if labels.next() != Some("eth") || labels.next().is_some() || label.trim().is_empty() {
        return None;
    }
    let labelhash = labelhash.map(str::to_owned).unwrap_or_else(|| {
        format!(
            "0x{}",
            alloy_primitives::hex::encode(alloy_primitives::keccak256(label.as_bytes()))
        )
    });
    let hex = labelhash.strip_prefix("0x").unwrap_or(&labelhash);
    alloy_primitives::U256::from_str_radix(hex, 16)
        .ok()
        .map(|value| value.to_string())
}

#[cfg(test)]
mod quoted_seconds_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn quoted_seconds_range_checks_the_whole_value_then_keeps_whole_seconds() {
        for (quoted, expected) in [
            ("1735689600.5", Some("2025-01-01T00:00:00Z")),
            ("-0.5", None),
            ("-1", None),
            ("253402300799", Some("9999-12-31T23:59:59Z")),
            ("253402300799.9", None),
            ("253402300800", None),
            ("0", Some("1970-01-01T00:00:00Z")),
        ] {
            let summary = json!({"registration": {"expiry": quoted}});
            assert_eq!(
                json_timestamp_at_paths(&summary, &[&["registration", "expiry"]]).as_deref(),
                expected,
                "{quoted}"
            );
        }
        // Not a number: served as the stored string.
        for text in ["abc", "1.", ".5", "1e9", "--1"] {
            assert_eq!(quoted_seconds(text), None, "{text}");
        }
        let summary = json!({"registration": {"expiry": "abc"}});
        assert_eq!(
            json_timestamp_at_paths(&summary, &[&["registration", "expiry"]]).as_deref(),
            Some("abc")
        );
    }
}
