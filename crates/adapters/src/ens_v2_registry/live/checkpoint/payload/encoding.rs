use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use serde_json::{Map, Value};

const ESCAPED_STRING_KEY: &str = "__bigname_live_checkpoint_utf8_hex_v1";

pub(super) fn encode_value<T: Serialize>(value: &T) -> Result<Value> {
    let value =
        serde_json::to_value(value).context("failed to encode ENSv2 live checkpoint payload")?;
    Ok(escape_nul_strings(value))
}

pub(super) fn decode_value<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    let value = unescape_nul_strings(value)?;
    serde_json::from_value(value).context("failed to decode ENSv2 live checkpoint payload")
}

fn escape_nul_strings(value: Value) -> Value {
    match value {
        Value::String(value) if value.contains('\0') => {
            let mut object = Map::new();
            object.insert(
                ESCAPED_STRING_KEY.to_owned(),
                Value::String(hex_encode(value.as_bytes())),
            );
            Value::Object(object)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(escape_nul_strings).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, escape_nul_strings(value)))
                .collect(),
        ),
        value => value,
    }
}

fn unescape_nul_strings(value: Value) -> Result<Value> {
    match value {
        Value::Object(mut values)
            if values.len() == 1 && values.contains_key(ESCAPED_STRING_KEY) =>
        {
            let encoded = values
                .remove(ESCAPED_STRING_KEY)
                .context("missing ENSv2 live checkpoint escaped string")?;
            let Value::String(encoded) = encoded else {
                bail!("ENSv2 live checkpoint escaped string is not text");
            };
            Ok(Value::String(
                String::from_utf8(hex_decode(&encoded)?)
                    .context("ENSv2 live checkpoint escaped string is not UTF-8")?,
            ))
        }
        Value::Object(values) => Ok(Value::Object(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, unescape_nul_strings(value)?)))
                .collect::<Result<Map<_, _>>>()?,
        )),
        Value::Array(values) => Ok(Value::Array(
            values
                .into_iter()
                .map(unescape_nul_strings)
                .collect::<Result<Vec<_>>>()?,
        )),
        value => Ok(value),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.len() % 2 == 0,
        "ENSv2 live checkpoint escaped string hex has odd length"
    );
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => bail!("ENSv2 live checkpoint escaped string contains invalid hex"),
    }
}
