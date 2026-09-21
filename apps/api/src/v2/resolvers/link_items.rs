//! Product shape of a resolver record link: one node bound to a non-zero record.

use serde_json::{Map, Value, json};

use crate::v2::{V2Error, V2Result};

pub(super) fn compact_resolver_link_item(item: &Value) -> V2Result<Value> {
    let Some(object) = item.as_object() else {
        return Err(mapping_error());
    };
    let mut compact = Map::new();
    compact.insert(
        "record_id".to_owned(),
        json!(required_string(object, "record_id")?),
    );
    compact.insert(
        "namehash".to_owned(),
        json!(required_string(object, "namehash")?),
    );
    compact.insert(
        "default".to_owned(),
        json!(
            object
                .get("default")
                .and_then(Value::as_bool)
                .ok_or_else(mapping_error)?
        ),
    );
    if let Some(namespace) = object.get("namespace").and_then(Value::as_str) {
        compact.insert("namespace".to_owned(), json!(namespace));
    }
    if let Some(name) = object.get("name").and_then(Value::as_str) {
        let normalized = bigname_domain::normalization::normalize_name(name)
            .map_err(|_| V2Error::internal_error("failed to normalize resolver link name"))?;
        compact.insert("name".to_owned(), json!(normalized.normalized_name));
        compact.insert(
            "display_name".to_owned(),
            json!(normalized.canonical_display_name),
        );
    }
    let position = object
        .get("chain_position")
        .and_then(Value::as_object)
        .ok_or_else(mapping_error)?;
    compact.insert(
        "link_event".to_owned(),
        json!({
            "block_number": position.get("block_number").and_then(Value::as_u64).ok_or_else(mapping_error)?,
            "timestamp": required_string(position, "timestamp")?,
            "transaction_hash": required_string(position, "transaction_hash")?,
            "log_index": position.get("log_index").and_then(Value::as_u64).ok_or_else(mapping_error)?,
        }),
    );
    Ok(Value::Object(compact))
}

fn required_string(object: &Map<String, Value>, key: &str) -> V2Result<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(mapping_error)
}

fn mapping_error() -> V2Error {
    V2Error::internal_error("failed to map resolver link item")
}
