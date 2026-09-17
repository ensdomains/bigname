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

// Neither query has a row or byte limit. A batch that legitimately needs a large working
// set (many names falling due at one timestamp, say) must load all of it; memory is the
// operator's concern, managed through `BIGNAME_INTERPRET_BLOCKS_PER_BATCH`.
const ENS_GRACE_PERIOD_SECS: i64 = 90 * 24 * 60 * 60;
const EVENTS: &str = include_str!("lookahead/events.sql");
const DUE_NAMES: &str = include_str!("lookahead/due_names.sql");

type EventRow = (Value, Option<OffsetDateTime>);

pub(super) async fn events(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    names: &[String],
    resources: &[Uuid],
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
        .fetch(connection);
    let mut result = Vec::new();
    while let Some((body, timestamp)) = rows
        .try_next()
        .await
        .map_err(|error| InterpretError::database("failed to load lookahead prior events", error))?
    {
        result.push(decode_event(body, timestamp)?);
    }
    Ok(result)
}

pub(super) async fn due_names(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    predecessor: Option<OffsetDateTime>,
    last: OffsetDateTime,
) -> Result<Vec<String>> {
    let mut names: Vec<String> = sqlx::query_scalar(DUE_NAMES)
        .bind(chain)
        .bind(before)
        .bind(predecessor.map(OffsetDateTime::unix_timestamp))
        .bind(last.unix_timestamp())
        .bind(ENS_GRACE_PERIOD_SECS)
        .fetch_all(connection)
        .await
        .map_err(|error| {
            InterpretError::database("failed to load lookahead expiry candidates", error)
        })?;
    names.sort();
    Ok(names)
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
