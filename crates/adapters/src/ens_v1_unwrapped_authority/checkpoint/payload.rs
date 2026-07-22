use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::{
    CHECKPOINT_CODEC, EnsV1UnwrappedAuthoritySyncSummary, UnwrappedAuthorityReplayFlushedEvents,
};

pub(in crate::ens_v1_unwrapped_authority) fn encode_item<T>(value: &T) -> Result<Value>
where
    T: serde::Serialize + ?Sized,
{
    CHECKPOINT_CODEC.encode_serde(
        value,
        "failed to encode unwrapped-authority checkpoint item",
    )
}

pub(in crate::ens_v1_unwrapped_authority) fn decode_item<T>(
    value: Value,
    item_kind: &str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    CHECKPOINT_CODEC.decode_serde(
        value,
        "failed to decode unwrapped-authority checkpoint JSONB encoding",
        format!("failed to decode unwrapped-authority checkpoint item {item_kind}"),
    )
}

pub(super) fn summary_payload(summary: &EnsV1UnwrappedAuthoritySyncSummary) -> Value {
    json!({
        "scanned_log_count": summary.scanned_log_count,
        "matched_log_count": summary.matched_log_count,
        "total_name_surface_count": summary.total_name_surface_count,
        "total_resource_count": summary.total_resource_count,
        "total_surface_binding_count": summary.total_surface_binding_count,
        "total_normalized_event_count": summary.total_normalized_event_count,
        "total_normalized_event_inserted_count": summary.total_normalized_event_inserted_count,
        "by_kind": summary.by_kind,
    })
}

pub(super) fn summary_from_payload(payload: &Value) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    Ok(EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count: usize_field(payload, "scanned_log_count")?,
        matched_log_count: usize_field(payload, "matched_log_count")?,
        total_name_surface_count: usize_field(payload, "total_name_surface_count")?,
        total_resource_count: usize_field(payload, "total_resource_count")?,
        total_surface_binding_count: usize_field(payload, "total_surface_binding_count")?,
        total_normalized_event_count: usize_field(payload, "total_normalized_event_count")?,
        total_normalized_event_inserted_count: usize_field(
            payload,
            "total_normalized_event_inserted_count",
        )?,
        by_kind: serde_json::from_value(
            payload.get("by_kind").cloned().unwrap_or_else(|| json!({})),
        )
        .context("checkpoint summary by_kind is invalid")?,
    })
}

pub(super) fn flushed_events_from_payload(
    payload: &Value,
) -> Result<UnwrappedAuthorityReplayFlushedEvents> {
    Ok(UnwrappedAuthorityReplayFlushedEvents {
        total_count: optional_usize_field(payload, "flushed_normalized_event_count")?,
        inserted_count: optional_usize_field(payload, "flushed_normalized_event_inserted_count")?,
        by_kind: payload
            .get("flushed_by_kind")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .context("checkpoint flushed_by_kind is invalid")?
            .unwrap_or_default(),
    })
}

fn usize_field(payload: &Value, field: &str) -> Result<usize> {
    let value = payload
        .get(field)
        .and_then(Value::as_i64)
        .with_context(|| format!("checkpoint summary is missing i64 field {field}"))?;
    usize::try_from(value)
        .with_context(|| format!("checkpoint summary field {field} overflows usize"))
}

fn optional_usize_field(payload: &Value, field: &str) -> Result<usize> {
    let Some(value) = payload.get(field).and_then(Value::as_i64) else {
        return Ok(0);
    };
    usize::try_from(value).with_context(|| format!("checkpoint field {field} overflows usize"))
}
