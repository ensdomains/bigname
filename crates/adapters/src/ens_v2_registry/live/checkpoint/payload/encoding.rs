use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::checkpoint_codec::JsonbCheckpointCodec;

const CHECKPOINT_CODEC: JsonbCheckpointCodec = JsonbCheckpointCodec::new(
    "__bigname_live_checkpoint_utf8_hex_v1",
    "__bigname_live_checkpoint_object_utf8_hex_v1",
);

pub(super) fn encode_value<T: Serialize>(value: &T) -> Result<Value> {
    CHECKPOINT_CODEC.encode_serde(value, "failed to encode ENSv2 live checkpoint payload")
}

pub(super) fn decode_value<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    CHECKPOINT_CODEC.decode_serde(
        value,
        "failed to decode ENSv2 live checkpoint JSONB encoding",
        "failed to decode ENSv2 live checkpoint payload",
    )
}
