//! Field-by-field comparison of a served JSON block with its shadow. Numbers compare by exact
//! value, so a jsonb `1000` and a Rust `1000` agree however each was produced, and two integers
//! past 2^53 that a float would merge stay apart.
use serde_json::{Number, Value};

/// One field whose served and shadow values differ.
#[derive(Clone, Debug, PartialEq)]
pub struct Difference {
    pub field: String,
    pub served: Value,
    pub shadow: Value,
}

/// JSON equality with numbers compared by their exact value.
pub fn same(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => same_number(left, right),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| same(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, value)| right.get(key).is_some_and(|other| same(value, other)))
        }
        _ => left == right,
    }
}

/// An integer held exactly, whether serde_json stored it signed or unsigned.
fn integer(number: &Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

/// Two JSON numbers by exact value: integers as integers, never through a float, so on-chain
/// values past 2^53 stay apart; a float equals an integer only when it holds exactly that
/// integer, so a jsonb `1000.0` equals `1000`.
fn same_number(left: &Number, right: &Number) -> bool {
    match (integer(left), integer(right)) {
        (Some(left), Some(right)) => left == right,
        (Some(whole), None) | (None, Some(whole)) => {
            let float = if integer(left).is_some() { right } else { left };
            float.as_f64().is_some_and(|float| {
                float.fract() == 0.0 && float.abs() < 2f64.powi(126) && float as i128 == whole
            })
        }
        (None, None) => left.as_f64() == right.as_f64(),
    }
}

/// A field of a block by its `/`-separated path, null when absent.
pub fn field<'a>(block: &'a Value, path: &str) -> &'a Value {
    static NULL: Value = Value::Null;
    path.split('/')
        .try_fold(block, |value, part| value.get(part))
        .unwrap_or(&NULL)
}

/// The listed fields whose values differ between `served` and `shadow`.
pub fn differences(served: &Value, shadow: &Value, fields: &[&str]) -> Vec<Difference> {
    fields
        .iter()
        .filter_map(|path| {
            let (left, right) = (field(served, path), field(shadow, path));
            (!same(left, right)).then(|| Difference {
                field: (*path).to_owned(),
                served: left.clone(),
                shadow: right.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn numbers_compare_by_value_and_absent_reads_as_null() {
        assert!(same(
            &json!(1000),
            &serde_json::from_str::<Value>("1000.0").unwrap()
        ));
        let served = json!({"a": 1, "b": {"c": null}});
        let shadow = json!({"a": 1.0});
        assert!(differences(&served, &shadow, &["a", "b/c", "d"]).is_empty());
        assert_eq!(differences(&served, &json!({"a": 2}), &["a"]).len(), 1);
    }

    #[test]
    fn integers_past_two_to_the_fifty_third_compare_exactly() {
        let (low, high) = (
            json!(9_007_199_254_740_992u64),
            json!(9_007_199_254_740_993u64),
        );
        assert!(!same(&low, &high));
        assert!(same(
            &high,
            &serde_json::from_str::<Value>("9007199254740993").unwrap()
        ));
        assert!(!same(
            &json!(-9_007_199_254_740_993i64),
            &json!(-9_007_199_254_740_992i64)
        ));
        // A float equals an integer only when it holds exactly that integer.
        assert!(!same(&json!(9_007_199_254_740_992.0f64), &high));
        assert!(same(&json!(9_007_199_254_740_992.0f64), &low));
        assert!(!same(&json!(1000.5), &json!(1000)));
    }
}
