use serde_json::{Value, json};

use crate::RecordSelector;

pub(crate) fn answer(entries: &Value, selector: &RecordSelector) -> Value {
    let entry = entries.as_array().and_then(|entries| {
        entries
            .iter()
            .find(|entry| entry_matches_selector(entry, selector))
            .or_else(|| {
                if selector.record_key == "avatar" {
                    entries.iter().find(|entry| {
                        entry.get("record_key").and_then(Value::as_str) == Some("text:avatar")
                    })
                } else {
                    None
                }
            })
    });
    let Some(entry) = entry else {
        return json!({ "status": "not_found" });
    };
    let status = entry
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unsupported");
    let mut answer = json!({ "status": status });
    if status == "success"
        && let Some(value) = indexed_value(entry, selector)
    {
        answer["value"] = value;
    }
    answer
}

fn entry_matches_selector(entry: &Value, selector: &RecordSelector) -> bool {
    entry.get("record_key").and_then(Value::as_str) == Some(&selector.record_key)
        || (entry.get("record_family").and_then(Value::as_str) == Some(&selector.record_family)
            && entry.get("selector_key").and_then(Value::as_str)
                == selector.selector_key.as_deref())
}

fn indexed_value(entry: &Value, selector: &RecordSelector) -> Option<Value> {
    let value = entry.get("value")?;
    let value = value
        .get("value")
        .or_else(|| value.get("bytes"))
        .unwrap_or(value);
    let text = value.as_str()?;
    Some(Value::String(if selector.record_family == "addr" {
        text.to_ascii_lowercase()
    } else {
        text.to_owned()
    }))
}
