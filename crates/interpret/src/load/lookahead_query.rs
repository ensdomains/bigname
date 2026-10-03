use bigname_adapters::schema_v2::{
    PriorEventInput, PriorWritePosition,
    seam::{
        INTERPRETER_STATE_KEY, LOG_INDEX_KEY, STATE_SCOPE_KEY,
        SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY, TRANSACTION_INDEX_KEY, retained_event_state_key,
        retained_prior_state_key,
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
pub(super) const V2_KEYS: &str = include_str!("lookahead/v2_keys.sql");
const DUE_NAMES: &str = include_str!("lookahead/due_names.sql");
const V2_DUE_KEYS: &str = include_str!("lookahead/v2_due_keys.sql");
const V2_LATEST_TOPOLOGY: &str = include_str!("lookahead/v2_latest_topology.sql");
const RETAINED_FAMILIES: &str = include_str!("lookahead/retained_families.sql");
const V2_REGISTRY_DELTA: &str = include_str!("lookahead/v2_registry_delta.sql");

type EventRow = (Value, Option<OffsetDateTime>);

/// A loaded event with the position the restore must apply it in.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct OrderedEvent {
    pub(super) order: (i64, i64),
    pub(super) event: PriorEventInput,
}

#[cfg(test)]
pub(super) async fn events(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    names: &[String],
    resources: &[Uuid],
) -> Result<Vec<PriorEventInput>> {
    Ok(
        ordered_events(connection, chain, before, names, resources, &[])
            .await?
            .into_iter()
            .map(|ordered| ordered.event)
            .collect(),
    )
}

/// The latest event of every state key among the events of `names` (every covered family),
/// of `resources` (ENSv1-model events naming no name) and of the ENSv2 state keys `v2_keys`,
/// in restore order.
pub(super) async fn ordered_events(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    names: &[String],
    resources: &[Uuid],
    v2_keys: &[String],
) -> Result<Vec<OrderedEvent>> {
    if names.is_empty() && resources.is_empty() && v2_keys.is_empty() {
        return Ok(Vec::new());
    }
    let query = substitute(EVENTS);
    let mut rows = sqlx::query_as::<_, EventRow>(&query)
        .bind(chain)
        .bind(before)
        .bind(names)
        .bind(resources)
        .bind(v2_keys)
        .fetch(connection);
    let mut result = Vec::new();
    while let Some((body, timestamp)) = rows
        .try_next()
        .await
        .map_err(|error| InterpretError::database("failed to load lookahead prior events", error))?
    {
        result.push(decode_ordered(body, timestamp)?);
    }
    Ok(result)
}

/// Every readable event in `[from, before)` filed under one of the whole-registry keys
/// `registries`, with its key, in the order `lookahead/v2_registry_delta.sql` documents.
pub(super) async fn v2_registry_delta(
    connection: &mut PgConnection,
    chain: &str,
    from: i64,
    before: i64,
    registries: &[String],
) -> Result<Vec<(String, OrderedEvent)>> {
    let query = substitute(V2_REGISTRY_DELTA);
    let mut rows = sqlx::query_as::<_, (String, Value, Option<OffsetDateTime>)>(&query)
        .bind(chain)
        .bind(from)
        .bind(before)
        .bind(registries)
        .fetch(connection);
    let mut result = Vec::new();
    while let Some((registry, body, timestamp)) = rows.try_next().await.map_err(|error| {
        InterpretError::database("failed to load retained ENSv2 registry events", error)
    })? {
        result.push((registry, decode_ordered(body, timestamp)?));
    }
    Ok(result)
}

pub(super) fn substitute(statement: &str) -> String {
    statement
        .replace("{v2_keys}", V2_KEYS.trim_end())
        .replace("{state_key}", INTERPRETER_STATE_KEY)
        .replace("{state_scope}", STATE_SCOPE_KEY)
        .replace("{clear_marker}", SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY)
        .replace("{transaction_index}", TRANSACTION_INDEX_KEY)
        .replace("{log_index}", LOG_INDEX_KEY)
}

fn decode_ordered(mut body: Value, timestamp: Option<OffsetDateTime>) -> Result<OrderedEvent> {
    let normalized_event_id = body["normalized_event_id"].as_i64().ok_or_else(|| {
        InterpretError::data_integrity("lookahead prior event has no normalized event id")
    })?;
    let block_number = body["block_number"].as_i64().ok_or_else(|| {
        InterpretError::data_integrity("lookahead prior event has no block number")
    })?;
    if let Some(fields) = body.as_object_mut() {
        fields.remove("normalized_event_id");
    }
    Ok(OrderedEvent {
        order: (block_number, normalized_event_id),
        event: decode_event(body, timestamp)?,
    })
}

/// The ENSv2 state keys of the tokens whose expiry lies in `(start, end]`; see
/// `lookahead/v2_due_keys.sql`.
pub(super) async fn v2_due_keys(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    (start, end): (i64, i64),
) -> Result<Vec<String>> {
    sqlx::query_scalar(&V2_DUE_KEYS.replace("{state_scope}", STATE_SCOPE_KEY))
        .bind(chain)
        .bind(before)
        .bind(start)
        .bind(end)
        .fetch_all(connection)
        .await
        .map_err(|error| InterpretError::database("failed to load due ENSv2 tokens", error))
}

/// The timestamp of the latest ENSv2 registry event before `before`.
pub(super) async fn v2_latest_topology(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
) -> Result<Option<OffsetDateTime>> {
    sqlx::query_scalar(V2_LATEST_TOPOLOGY)
        .bind(chain)
        .bind(before)
        .fetch_optional(connection)
        .await
        .map_err(|error| {
            InterpretError::database("failed to load the latest ENSv2 topology timestamp", error)
        })
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

/// The source families of the chain's manifests in a rollout state other than `active` or
/// `deprecated`, each with that state, ordered by family then state. `manifest_versions`
/// holds a few rows per chain, so this reads no index.
pub(super) async fn other_manifest_families(
    connection: &mut PgConnection,
    chain: &str,
) -> Result<Vec<(String, String)>> {
    sqlx::query_as(
        "SELECT DISTINCT source_family, rollout_status
         FROM manifest_versions
         WHERE chain_id = $1 AND rollout_status NOT IN ('active', 'deprecated')
         ORDER BY source_family, rollout_status",
    )
    .bind(chain)
    .fetch_all(connection)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load manifests outside interpretation", error)
    })
}

/// The first of `families`, in name order, with a readable event on the chain before
/// `before`; see `lookahead/retained_families.sql` for its cost.
pub(super) async fn first_retained_family(
    connection: &mut PgConnection,
    chain: &str,
    before: i64,
    families: &[String],
) -> Result<Option<String>> {
    if families.is_empty() {
        return Ok(None);
    }
    sqlx::query_scalar(RETAINED_FAMILIES)
        .bind(chain)
        .bind(before)
        .bind(families)
        .fetch_optional(connection)
        .await
        .map_err(|error| InterpretError::database("failed to probe retained families", error))
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
        write_position: PriorWritePosition::from_parts(
            field!("block_number"),
            field!(TRANSACTION_INDEX_KEY),
            field!(LOG_INDEX_KEY),
        ),
        after_state,
    })
}

#[cfg(test)]
#[path = "lookahead_query_tests.rs"]
mod tests;
