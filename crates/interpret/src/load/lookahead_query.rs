use bigname_adapters::schema_v2::{
    PriorEventInput,
    seam::{
        INTERPRETER_STATE_KEY, STATE_SCOPE_KEY, SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY,
        retained_event_state_key, retained_prior_state_key,
    },
};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::{PgConnection, types::Uuid};
use time::OffsetDateTime;

use crate::{InterpretError, Result};

// These are experiment limits, independent of the process environment. SQL suppresses
// an oversized payload before sending it; streaming also bounds the accumulated batch.
const MAX_EVENT_BYTES: i64 = 64 * 1024 * 1024;
const ENS_GRACE_PERIOD_SECS: i64 = 90 * 24 * 60 * 60;
const EVENTS: &str = include_str!("lookahead/events.sql");
const DUE_NAMES: &str = include_str!("lookahead/due_names.sql");

type EventRow = (Option<Value>, i64, Option<OffsetDateTime>);

pub(super) async fn events(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    names: &[String],
    resources: &[Uuid],
    limit: usize,
) -> Result<Vec<PriorEventInput>> {
    events_with_byte_limit(
        connection,
        chain,
        before,
        names,
        resources,
        limit,
        MAX_EVENT_BYTES,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn events_with_byte_limit(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    names: &[String],
    resources: &[Uuid],
    limit: usize,
    byte_limit: i64,
) -> Result<Vec<PriorEventInput>> {
    if names.is_empty() && resources.is_empty() {
        return Ok(Vec::new());
    }
    let query = EVENTS
        .replace("{state_key}", INTERPRETER_STATE_KEY)
        .replace("{state_scope}", STATE_SCOPE_KEY)
        .replace("{clear_marker}", SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY);
    let mut rows = sqlx::query_as::<_, EventRow>(&query)
        .bind(chain)
        .bind(before)
        .bind(names)
        .bind(resources)
        .bind(probe_limit(limit)?)
        .bind(byte_limit)
        .fetch(connection);
    let mut result = Vec::new();
    let mut bytes = 0_i64;
    while let Some((body, size, timestamp)) = rows
        .try_next()
        .await
        .map_err(|error| InterpretError::database("failed to load lookahead prior events", error))?
    {
        if result.len() == limit {
            return Err(cap_error("event count", limit));
        }
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| cap_error("event bytes", byte_limit))?;
        if bytes > byte_limit || body.is_none() {
            return Err(cap_error("event bytes", byte_limit));
        }
        result.push(decode_event(
            body.expect("checked payload size"),
            timestamp,
        )?);
    }
    Ok(result)
}

pub(super) async fn due_names(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    predecessor: Option<OffsetDateTime>,
    last: OffsetDateTime,
    limit: usize,
) -> Result<Vec<String>> {
    let mut rows = sqlx::query_scalar::<_, Option<String>>(DUE_NAMES)
        .bind(chain)
        .bind(before)
        .bind(predecessor.map(OffsetDateTime::unix_timestamp))
        .bind(last.unix_timestamp())
        .bind(probe_limit(limit)?)
        .bind(ENS_GRACE_PERIOD_SECS)
        .bind(MAX_EVENT_BYTES)
        .fetch(connection);
    let mut names = Vec::new();
    let mut bytes = 0_usize;
    while let Some(name) = rows.try_next().await.map_err(|error| {
        InterpretError::database("failed to load lookahead expiry candidates", error)
    })? {
        if names.len() == limit {
            return Err(cap_error("expiry candidate count", limit));
        }
        let name = name.ok_or_else(|| cap_error("expiry candidate bytes", MAX_EVENT_BYTES))?;
        bytes = bytes.saturating_add(name.len());
        if bytes > MAX_EVENT_BYTES as usize {
            return Err(cap_error("expiry candidate bytes", MAX_EVENT_BYTES));
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}

fn probe_limit(limit: usize) -> Result<i64> {
    limit
        .checked_add(1)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| {
            InterpretError::configuration("lookahead row limit cannot represent its overflow probe")
        })
}

fn cap_error(kind: &str, limit: impl std::fmt::Display) -> InterpretError {
    InterpretError::data_integrity(format!(
        "lookahead {kind} exceeds limit {limit}; refusing incomplete prior state"
    ))
}

fn decode_event(
    mut body: Value,
    block_timestamp: Option<OffsetDateTime>,
) -> Result<PriorEventInput> {
    macro_rules! field {
        ($name:expr) => {
            serde_json::from_value(body[$name].take()).map_err(|error| {
                InterpretError::data_integrity(format!("invalid lookahead {}: {error}", $name))
            })?
        };
    }
    let interpreter_state_key: Option<String> = field!(INTERPRETER_STATE_KEY);
    let event_identity: String = field!("event_identity");
    let after_state = body["after_state"].take();
    Ok(PriorEventInput {
        retained_state_key: retained_event_state_key(
            retained_prior_state_key(interpreter_state_key.as_deref(), &event_identity),
            &after_state,
        ),
        chain_id: field!("chain_id"),
        namespace: field!("namespace"),
        logical_name_id: field!("logical_name_id"),
        resource_id: field!("resource_id"),
        event_kind: field!("event_kind"),
        source_family: field!("source_family"),
        manifest_version: field!("manifest_version"),
        source_manifest_id: field!("source_manifest_id"),
        emitting_address: field!("emitting_address"),
        state_scope: field!(STATE_SCOPE_KEY),
        block_timestamp,
        after_state,
    })
}

#[cfg(test)]
#[path = "lookahead_query_tests.rs"]
mod tests;
