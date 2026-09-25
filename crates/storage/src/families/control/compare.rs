//! Field-by-field comparison of a served JSON block with its shadow. Numbers compare by value,
//! so a jsonb `1000` and a Rust `1000` agree however each was produced.
use serde_json::Value;

/// One field whose served and shadow values differ.
#[derive(Clone, Debug, PartialEq)]
pub struct Difference {
    pub field: String,
    pub served: Value,
    pub shadow: Value,
}

/// JSON equality with numbers compared by their decimal value.
pub fn same(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            left == right
                || match (left.as_f64(), right.as_f64()) {
                    (Some(left), Some(right)) => left == right,
                    _ => left.to_string() == right.to_string(),
                }
        }
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
}
