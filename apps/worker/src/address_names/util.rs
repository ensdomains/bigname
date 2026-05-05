use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
use serde_json::Value;
use sqlx::types::time::{OffsetDateTime, UtcOffset};

pub(super) fn normalize_address(value: impl AsRef<str>) -> String {
    value.as_ref().to_ascii_lowercase()
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    CanonicalityState::parse(value)
}

pub(super) fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    SurfaceBindingKind::parse(value)
}

pub(super) fn canonicality_rank(state: CanonicalityState) -> u8 {
    state.rank()
}

pub(super) fn weakest_canonicality(
    states: impl Iterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    CanonicalityState::weakest(states)
}

pub(super) fn chain_slot(chain_id: &str) -> String {
    if chain_id.starts_with("ethereum") {
        "ethereum".to_owned()
    } else if chain_id.starts_with("base") {
        "base".to_owned()
    } else {
        chain_id.to_owned()
    }
}

pub(super) fn format_timestamp(timestamp: OffsetDateTime) -> String {
    let timestamp = timestamp.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
    )
}

pub(super) fn json_str(value: &Value, path: &[&str]) -> Option<String> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

pub(super) fn dedupe_json_values(values: impl IntoIterator<Item = Value>) -> Result<Vec<Value>> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();

    for value in values {
        let key = serde_json::to_string(&value).context("failed to serialize JSON for dedupe")?;
        if seen.insert(key) {
            unique.push(value);
        }
    }

    Ok(unique)
}
