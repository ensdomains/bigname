use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::checkpoint_codec::JsonbCheckpointCodec;

const CHECKPOINT_CODEC: JsonbCheckpointCodec = JsonbCheckpointCodec::new(
    "__bigname_live_checkpoint_utf8_hex_v1",
    "__bigname_live_checkpoint_object_utf8_hex_v1",
);

pub(super) fn encode_value<T: Serialize>(value: &T) -> Result<Value> {
    let value =
        serde_json::to_value(value).context("failed to encode ENSv2 live checkpoint payload")?;
    Ok(CHECKPOINT_CODEC.encode(value))
}

pub(super) fn decode_value<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    let value = CHECKPOINT_CODEC
        .decode(value)
        .context("failed to decode ENSv2 live checkpoint JSONB encoding")?;
    serde_json::from_value(value).context("failed to decode ENSv2 live checkpoint payload")
}
