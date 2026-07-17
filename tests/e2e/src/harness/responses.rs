use std::collections::BTreeSet;

use anyhow::Result;
use serde_json::Value;

use super::pipeline::ApiServer;

pub fn pointer(body: &Value, path: &str) -> Value {
    body.pointer(path)
        .unwrap_or_else(|| panic!("response is missing JSON pointer {path}: {body}"))
        .clone()
}

pub fn data_array(body: &Value) -> Vec<Value> {
    body.pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub fn selector_keys(body: &Value) -> BTreeSet<String> {
    body.pointer("/declared_state/record_inventory/selectors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("record_key").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

pub async fn exact_name(api: &ApiServer, namespace: &str, name: &str) -> Result<Value> {
    let (status, body) = api
        .get_json(&format!("/v1/names/{namespace}/{name}"))
        .await?;
    assert_eq!(
        status, 200,
        "{namespace} exact-name lookup for {name} failed: {body}"
    );
    Ok(body)
}

pub async fn primary_name(
    api: &ApiServer,
    namespace: &str,
    coin_type: u64,
    address: &str,
    mode: &str,
) -> Result<Value> {
    let (status, body) = api
        .get_json(&format!(
            "/v1/primary-names/{address}?namespace={namespace}&coin_type={coin_type}&mode={mode}"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "{namespace} primary-name lookup for {address} mode={mode} failed: {body}"
    );
    Ok(body)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::pointer;

    #[test]
    fn pointer_preserves_explicit_null() {
        let body = json!({"data": {"value": null}});

        assert_eq!(pointer(&body, "/data/value"), Value::Null);
    }

    #[test]
    #[should_panic(expected = "response is missing JSON pointer /data/value")]
    fn pointer_rejects_missing_path() {
        let body = json!({"data": {}});

        let _ = pointer(&body, "/data/value");
    }
}
