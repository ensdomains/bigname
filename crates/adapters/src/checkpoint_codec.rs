use alloy_primitives::hex;
use anyhow::{Context, Result, bail};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::fmt::Display;

pub(crate) struct JsonbCheckpointCodec {
    escaped_string_key: &'static str,
    escaped_object_key: &'static str,
}

impl JsonbCheckpointCodec {
    pub(crate) const fn new(
        escaped_string_key: &'static str,
        escaped_object_key: &'static str,
    ) -> Self {
        Self {
            escaped_string_key,
            escaped_object_key,
        }
    }

    pub(crate) fn encode(&self, value: Value) -> Value {
        match value {
            Value::String(value) if value.contains('\0') => {
                let mut object = Map::new();
                object.insert(
                    self.escaped_string_key.to_owned(),
                    Value::String(hex::encode(value.as_bytes())),
                );
                Value::Object(object)
            }
            Value::Array(values) => {
                Value::Array(values.into_iter().map(|value| self.encode(value)).collect())
            }
            Value::Object(values)
                if values.keys().any(|key| {
                    key.contains('\0')
                        || key == self.escaped_string_key
                        || key == self.escaped_object_key
                }) =>
            {
                let entries = values
                    .into_iter()
                    .map(|(key, value)| {
                        Value::Array(vec![
                            Value::String(hex::encode(key.as_bytes())),
                            self.encode(value),
                        ])
                    })
                    .collect();
                let mut object = Map::new();
                object.insert(self.escaped_object_key.to_owned(), Value::Array(entries));
                Value::Object(object)
            }
            Value::Object(values) => Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, self.encode(value)))
                    .collect(),
            ),
            value => value,
        }
    }

    pub(crate) fn decode(&self, value: Value) -> Result<Value> {
        match value {
            Value::Object(mut values)
                if values.len() == 1 && values.contains_key(self.escaped_string_key) =>
            {
                let encoded = values
                    .remove(self.escaped_string_key)
                    .context("missing checkpoint escaped string")?;
                let Value::String(encoded) = encoded else {
                    bail!("checkpoint escaped string payload is not a string");
                };
                Ok(Value::String(decode_utf8_hex(&encoded)?))
            }
            Value::Object(mut values)
                if values.len() == 1 && values.contains_key(self.escaped_object_key) =>
            {
                let entries = values
                    .remove(self.escaped_object_key)
                    .context("missing checkpoint escaped object")?;
                let Value::Array(entries) = entries else {
                    bail!("checkpoint escaped object payload is not an array");
                };
                let mut object = Map::new();
                for entry in entries {
                    let Value::Array(mut pair) = entry else {
                        bail!("checkpoint escaped object entry is not an array");
                    };
                    if pair.len() != 2 {
                        bail!("checkpoint escaped object entry does not contain two values");
                    }
                    let value = pair.pop().expect("two-value pair has a value");
                    let key = pair.pop().expect("two-value pair has a key");
                    let Value::String(key) = key else {
                        bail!("checkpoint escaped object key is not a string");
                    };
                    let key = decode_utf8_hex(&key)?;
                    if object.insert(key, self.decode(value)?).is_some() {
                        bail!("checkpoint escaped object contains a duplicate key");
                    }
                }
                Ok(Value::Object(object))
            }
            Value::Object(values) => Ok(Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| Ok((key, self.decode(value)?)))
                    .collect::<Result<Map<_, _>>>()?,
            )),
            Value::Array(values) => Ok(Value::Array(
                values
                    .into_iter()
                    .map(|value| self.decode(value))
                    .collect::<Result<Vec<_>>>()?,
            )),
            value => Ok(value),
        }
    }

    pub(crate) fn encode_serde<T, C>(&self, value: &T, context: C) -> Result<Value>
    where
        T: Serialize + ?Sized,
        C: Display + Send + Sync + 'static,
    {
        let value = serde_json::to_value(value).context(context)?;
        Ok(self.encode(value))
    }

    pub(crate) fn decode_serde<T, C, D>(
        &self,
        value: Value,
        codec_context: C,
        payload_context: D,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        C: Display + Send + Sync + 'static,
        D: Display + Send + Sync + 'static,
    {
        let value = self.decode(value).context(codec_context)?;
        serde_json::from_value(value).context(payload_context)
    }
}

fn decode_utf8_hex(value: &str) -> Result<String> {
    let bytes = hex::decode(value).context("checkpoint escaped string hex is invalid")?;
    String::from_utf8(bytes).context("checkpoint escaped string is not valid UTF-8")
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use super::*;

    const CODEC: JsonbCheckpointCodec =
        JsonbCheckpointCodec::new("__checkpoint_string_v1", "__checkpoint_object_v1");

    #[test]
    fn codec_preserves_existing_safe_value_encoding() -> Result<()> {
        let payload = json!({
            "record": "before\0after",
            "nested": ["plain", {"number": 1}],
        });
        let expected = json!({
            "record": {"__checkpoint_string_v1": "6265666f7265006166746572"},
            "nested": ["plain", {"number": 1}],
        });

        let encoded = CODEC.encode(payload.clone());

        assert_eq!(encoded, expected);
        assert_eq!(CODEC.decode(encoded)?, payload);
        Ok(())
    }

    #[test]
    fn codec_round_trips_nul_in_object_keys() -> Result<()> {
        let payload = json!({"ordinary": {"key\0suffix": "value\0suffix"}});

        let encoded = CODEC.encode(payload.clone());
        let encoded_json = serde_json::to_string(&encoded)?;

        assert!(!encoded_json.contains("\\u0000"));
        assert_eq!(CODEC.decode(encoded)?, payload);
        Ok(())
    }

    #[test]
    fn codec_wraps_ordinary_objects_that_contain_envelope_keys() -> Result<()> {
        for payload in [
            json!({"__checkpoint_string_v1": "ordinary text"}),
            json!({"__checkpoint_object_v1": ["ordinary", "array"]}),
        ] {
            let encoded = CODEC.encode(payload.clone());

            assert!(encoded.as_object().is_some_and(|object| {
                object.len() == 1 && object.contains_key("__checkpoint_object_v1")
            }));
            assert_eq!(CODEC.decode(encoded)?, payload);
        }
        Ok(())
    }

    #[test]
    fn serde_helpers_preserve_encoding_and_error_contexts() -> Result<()> {
        let payload = json!({"record": "before\0after"});
        let expected = json!({
            "record": {"__checkpoint_string_v1": "6265666f7265006166746572"}
        });

        let encoded = CODEC.encode_serde(&payload, "encode context")?;
        assert_eq!(encoded, expected);
        assert_eq!(
            CODEC.decode_serde::<Value, _, _>(encoded, "codec context", "payload context",)?,
            payload
        );

        let codec_error = CODEC
            .decode_serde::<Value, _, _>(
                json!({"__checkpoint_string_v1": 1}),
                "codec context",
                "payload context",
            )
            .expect_err("invalid envelope must fail");
        assert_eq!(
            format!("{codec_error:#}"),
            "codec context: checkpoint escaped string payload is not a string"
        );

        let payload_error = CODEC
            .decode_serde::<u64, _, _>(
                Value::String("not a number".to_owned()),
                "codec context",
                "payload context",
            )
            .expect_err("invalid payload must fail");
        assert!(format!("{payload_error:#}").starts_with("payload context: invalid type"));
        Ok(())
    }
}
