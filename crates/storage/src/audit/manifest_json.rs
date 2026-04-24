use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use uuid::Uuid;

pub(super) fn serialize_json_object(context: &str, value: &Value) -> Result<String> {
    ensure_json_object(context, value)?;
    serde_json::to_string(value).with_context(|| format!("failed to serialize {context}"))
}

pub(super) fn ensure_json_object(context: &str, value: &Value) -> Result<()> {
    if !value.is_object() {
        bail!("{context} must be a JSON object");
    }
    Ok(())
}

pub(super) fn json_object(value: Value) -> Result<Map<String, Value>> {
    match value {
        Value::Object(object) => Ok(object),
        _ => bail!("manifest drift alert JSON material must be an object"),
    }
}

pub(super) fn merge_json_object(
    state: &mut Map<String, Value>,
    context: &str,
    value: Value,
) -> Result<()> {
    for (key, value) in
        json_object(value).with_context(|| format!("{context} must be an object"))?
    {
        state.insert(key, value);
    }
    Ok(())
}

pub(super) fn insert_json<T>(state: &mut Map<String, Value>, key: &str, value: T)
where
    T: Into<Value>,
{
    state.insert(key.to_owned(), value.into());
}

pub(super) fn insert_optional_json<T>(state: &mut Map<String, Value>, key: &str, value: Option<T>)
where
    T: Into<Value>,
{
    if let Some(value) = value {
        insert_json(state, key, value);
    }
}

pub(super) fn insert_uuid(state: &mut Map<String, Value>, key: &str, value: Option<Uuid>) {
    if let Some(value) = value {
        insert_json(state, key, value.to_string());
    }
}
