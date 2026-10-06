//! The existing public scalar conversions, shared without changing source precedence.
use serde_json::Value;

pub fn object_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|value| value.is_object())
}

pub fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(value_to_string)
}

pub fn json_value_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        _ => true,
    }
}

fn path<'a>(mut value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    for key in path {
        value = value.get(key)?;
    }
    Some(value)
}

pub fn json_address_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|keys| path(value, keys).and_then(value_to_string))
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_ascii_lowercase())
}

pub fn json_timestamp_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|keys| seconds_timestamp(path(value, keys)?))
}

/// Public timestamps keep exact integral Unix seconds, independent of calendar range.
pub fn seconds_timestamp(value: &Value) -> Option<String> {
    crate::UnixSeconds::from_json(value).map(|value| value.unix_timestamp().to_string())
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn finite_seconds_are_exact_independent_of_calendar_or_json_safe_integer_range() {
        for seconds in [
            0,
            1_735_689_600,
            253_402_300_800,
            9_007_199_254_740_993,
            u64::MAX,
        ] {
            for value in [json!(seconds), json!(seconds.to_string())] {
                assert_eq!(seconds_timestamp(&value), Some(seconds.to_string()));
            }
        }
        assert_eq!(
            seconds_timestamp(&json!("2025-01-01T00:00:00.123Z")),
            Some("1735689600".into())
        );
        assert_eq!(
            seconds_timestamp(&json!("1735689600.123")),
            Some("1735689600".into())
        );
        for malformed in ["abc", "1.", ".5", "--1"] {
            assert_eq!(seconds_timestamp(&json!(malformed)), None);
        }
    }
}
