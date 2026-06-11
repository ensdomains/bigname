use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

pub(crate) fn json_field<'a>(value: &'a Value, field_name: &str) -> Option<&'a Value> {
    value.as_object()?.get(field_name)
}

pub(crate) fn json_string_field(value: Option<&Value>) -> Option<String> {
    value?.as_str().map(str::to_owned)
}

pub(crate) fn required_object<'a>(
    value: Option<&'a Value>,
    context: &str,
) -> Result<&'a Map<String, Value>> {
    value
        .and_then(Value::as_object)
        .with_context(|| format!("{context} must be a JSON object"))
}

pub(crate) fn required_array<'a>(
    value: Option<&'a Value>,
    context: &str,
) -> Result<&'a Vec<Value>> {
    value
        .and_then(Value::as_array)
        .with_context(|| format!("{context} must be a JSON array"))
}

pub(crate) fn ensure_only_allowed_fields(
    object: &Map<String, Value>,
    allowed_fields: &[&str],
    context: &str,
) -> Result<()> {
    for key in object.keys() {
        if !allowed_fields
            .iter()
            .any(|allowed| allowed == &key.as_str())
        {
            bail!("{context} must not set field {key}");
        }
    }

    Ok(())
}

pub(crate) fn required_string<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{context} must include non-empty string field {field_name}"))
}

pub(crate) fn required_nonempty_string_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<String> {
    Ok(required_string(object, field_name, context)?.to_owned())
}

pub(crate) fn optional_nonempty_string_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<Option<String>> {
    match object.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => bail!("{context} field {field_name} must be null or a non-empty string"),
    }
}

pub(crate) fn required_coin_type_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<String> {
    match object.get(field_name) {
        Some(Value::String(value))
            if !value.is_empty() && value.as_bytes().iter().all(u8::is_ascii_digit) =>
        {
            canonical_decimal_coin_type(value, context, field_name)
        }
        Some(Value::Number(value)) if value.as_u64().is_some() => {
            Ok(value.as_u64().expect("as_u64 was checked").to_string())
        }
        _ => bail!("{context} field {field_name} must be decimal coin_type text or number"),
    }
}

fn canonical_decimal_coin_type(value: &str, context: &str, field_name: &str) -> Result<String> {
    let coin_type = value
        .parse::<u64>()
        .with_context(|| format!("{context} field {field_name} must fit in u64"))?;
    Ok(coin_type.to_string())
}

pub(crate) fn ensure_absent(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<()> {
    if object.contains_key(field_name) {
        bail!("{context} must not set field {field_name}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn required_coin_type_field_canonicalizes_decimal_text() -> Result<()> {
        let object = json!({
            "coin_type": "060",
        });
        let object = object.as_object().expect("test payload must be an object");

        assert_eq!(
            required_coin_type_field(object, "coin_type", "test payload")?,
            "60"
        );
        Ok(())
    }

    #[test]
    fn required_coin_type_field_accepts_numeric_zero() -> Result<()> {
        let object = json!({
            "coin_type": 0,
        });
        let object = object.as_object().expect("test payload must be an object");

        assert_eq!(
            required_coin_type_field(object, "coin_type", "test payload")?,
            "0"
        );
        Ok(())
    }
}
