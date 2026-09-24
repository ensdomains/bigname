//! F12, reverse tuples and claims. A tuple (address, coin type, namespace) keeps its latest
//! ReverseChanged (reverse node, source event, claim provenance) and, separately, its latest
//! direct claim, a RecordChanged whose `primary_claim_source` names the tuple (primary_names.rs,
//! `latest_reverse` and `latest_claim`). A node keeps its latest name record or version change,
//! the claim a ReverseClaimed tuple selects through the node (`node_claim`). Each claim event keeps
//! its normalization result, stored once (`stage_claim_normalization`). The served stage only
//! normalizes a name record at a node some ReverseClaimed points to; the family keeps every name
//! record's result, since a later ReverseClaimed can select it. Hydration results stay with the
//! served rows until the per-block publication commits them here.
use bigname_domain::normalization::normalize_name;
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, current, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null},
    store::{Row, RowSet},
    tables,
};
use crate::Result;

fn tuple_key(state: &Value) -> Option<[Value; 3]> {
    Some([
        json!(raw_lower(state, "address")?),
        json!(raw_text(state, "coin_type")?),
        json!(raw_text(state, "namespace")?),
    ])
}

/// The tuple a ReverseChanged writes.
fn reverse_tuple(event: &BlockEvent) -> Option<Row> {
    (event.event_kind == "ReverseChanged").then_some(())?;
    Some(key_of(&tables::REVERSE_TUPLE, tuple_key(&event.after)?))
}

/// The tuple a direct claim writes.
fn claim_tuple(event: &BlockEvent) -> Option<Row> {
    (event.event_kind == "RecordChanged").then_some(())?;
    Some(key_of(
        &tables::REVERSE_TUPLE,
        tuple_key(event.after.get("primary_claim_source")?)?,
    ))
}

fn name_record(event: &BlockEvent) -> bool {
    event.event_kind == "RecordChanged"
        && raw_text(&event.after, "source_event").as_deref() == Some("NameChanged")
        && raw_text(&event.after, "record_key").as_deref() == Some("name")
}

/// The node a name record or version change addresses.
fn node_claim(event: &BlockEvent) -> Option<Row> {
    (name_record(event) || event.event_kind == "RecordVersionChanged").then_some(())?;
    Some(key_of(
        &tables::REVERSE_NODE_CLAIM,
        [
            json!(event.namespace),
            json!(raw_lower(&event.after, "node")?),
        ],
    ))
}

/// The claim events whose normalization is kept: direct claims and name records.
fn claim(chain: &Value, event: &BlockEvent) -> Option<Row> {
    (claim_tuple(event).is_some() || name_record(event)).then_some(())?;
    Some(key_of(
        &tables::CLAIM_NORMALIZATION,
        [chain.clone(), json!(event.position.event_identity)],
    ))
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let tuples = events
        .iter()
        .filter_map(|event| reverse_tuple(event).or_else(|| claim_tuple(event)))
        .collect();
    load_rows(transaction, rows, &tables::REVERSE_TUPLE, tuples).await?;
    let nodes = events.iter().filter_map(node_claim).collect();
    load_rows(transaction, rows, &tables::REVERSE_NODE_CLAIM, nodes).await?;
    let claims = events
        .iter()
        .filter_map(|event| claim(&chain, event))
        .collect();
    load_rows(transaction, rows, &tables::CLAIM_NORMALIZATION, claims).await?;

    for event in events {
        let table = &tables::REVERSE_TUPLE;
        if let Some(key) = reverse_tuple(event) {
            let mut row = current(rows, table, &key);
            set(&mut row, "chain_id", context.chain_id);
            set(
                &mut row,
                "reverse_node",
                text_or_null(raw_lower(&event.after, "reverse_node")),
            );
            set(
                &mut row,
                "source_event",
                text_or_null(raw_text(&event.after, "source_event")),
            );
            set(
                &mut row,
                "claim_provenance",
                event
                    .after
                    .get("claim_provenance")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            set(&mut row, "reverse_position", event.position.to_json());
            put(rows, table, row, event)?;
        }
        if let Some(key) = claim_tuple(event) {
            let mut row = current(rows, table, &key);
            set(&mut row, "chain_id", context.chain_id);
            set(
                &mut row,
                "raw_name",
                event.after.get("raw_name").cloned().unwrap_or(Value::Null),
            );
            set(
                &mut row,
                "raw_name_bytes",
                event
                    .after
                    .get("raw_name_bytes")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
            set(
                &mut row,
                "claim_event_identity",
                event.position.event_identity.clone(),
            );
            set(&mut row, "claim_position", event.position.to_json());
            put(rows, table, row, event)?;
        }
        if let Some(key) = node_claim(event) {
            // The served read takes the node's latest name record or version change at the
            // resolver it points to; the row keeps the latest one with its resolver.
            let table = &tables::REVERSE_NODE_CLAIM;
            let mut row = current(rows, table, &key);
            let after = &event.after;
            set(&mut row, "chain_id", context.chain_id);
            set(
                &mut row,
                "resolver_address",
                text_or_null(raw_lower(after, "resolver")),
            );
            set(
                &mut row,
                "raw_name",
                after.get("raw_name").cloned().unwrap_or(Value::Null),
            );
            let bytes = after.get("raw_name_bytes").cloned().unwrap_or(Value::Null);
            set(&mut row, "raw_name_bytes", bytes);
            put(rows, table, row, event)?;
        }
        if let Some(key) = claim(&chain, event) {
            let table = &tables::CLAIM_NORMALIZATION;
            let mut row = current(rows, table, &key);
            let (status, normalized, reason) = normalization(&event.after);
            set(&mut row, "status", status);
            set(&mut row, "normalized_name", text_or_null(normalized));
            set(&mut row, "reason", text_or_null(reason.map(str::to_owned)));
            put(rows, table, row, event)?;
        }
    }
    Ok(())
}

/// The claim classification primary_names.rs applies (`classify_claim`): not found without a
/// name, unsupported when only undecodable bytes remain, invalid when the name does not
/// normalize. The normalized form is kept on success.
fn normalization(after: &Value) -> (&'static str, Option<String>, Option<&'static str>) {
    let raw = match after.get("raw_name") {
        Some(Value::String(name)) => Some(name.as_str()),
        _ => None,
    };
    let has_bytes = after.get("raw_name_bytes").is_some()
        || after.get("raw_name").is_some_and(Value::is_object);
    let Some(raw) = raw else {
        return if has_bytes {
            ("unsupported", None, Some("claim_name_not_decodable"))
        } else {
            ("not_found", None, None)
        };
    };
    if raw.chars().all(char::is_whitespace) {
        return ("not_found", None, None);
    }
    match normalize_name(raw) {
        Ok(normalized) => ("success", Some(normalized.normalized_name), None),
        Err(_) => ("invalid_name", None, None),
    }
}
