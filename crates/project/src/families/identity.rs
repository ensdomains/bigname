//! F1, name identity. A name keeps its latest MigrationApplied (migration path, evidence and
//! time; children.rs `parent_boundaries`) and the start of each authority arm's latest epoch (the
//! latest AuthorityEpochChanged per arm). Every binding candidate is kept as it was created,
//! selected or not: its identity row from `surface_bindings`, the facts of the SurfaceBound that
//! opened it (state-derived, authority kind, and for a NameWrapper binding the registrar lease it
//! recorded, the node, transaction and emitter; name_authority/stage.rs), and for a
//! registry-only binding the binding it replaced and the lease that stands for it, to begin with
//! the replaced binding's resource (stage.rs, `registry_only_handoffs`).
mod lease;

use std::collections::BTreeMap;

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

pub(super) use self::lease::successor_grant;
use super::{
    input::{BlockEvent, Position},
    reduce::{
        Context, current, in_family, key_of, load_rows, namehash_of, put, raw_lower, raw_text, set,
        text_or_null,
    },
    store::{Row, RowSet},
    tables,
};
use crate::{ProjectError, Result};

/// The authority arm a source family belongs to.
fn arm(source_family: &str) -> &'static str {
    if source_family.starts_with("basenames_") {
        "basenames"
    } else if source_family.starts_with("ens_v2_") {
        "ens_v2"
    } else {
        "ens_v1"
    }
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    names(transaction, context, events, rows).await?;
    candidates(transaction, context, events, rows).await
}

async fn names(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::NAME_STATE;
    let relevant: Vec<(&BlockEvent, &str)> = events
        .iter()
        .filter(|event| {
            (event.event_kind == "MigrationApplied" && event.source_family == "ens_v2_migration_l1")
                || event.event_kind == "AuthorityEpochChanged"
        })
        .filter_map(|event| Some((event, event.logical_name_id.as_deref()?)))
        .collect();
    let keys = relevant
        .iter()
        .map(|(event, name)| key_of(table, [json!(event.namespace), json!(name)]))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, name) in relevant {
        let mut row = current(
            rows,
            table,
            &key_of(table, [json!(event.namespace), json!(name)]),
        );
        set(&mut row, "chain_id", context.chain_id);
        if event.event_kind == "MigrationApplied" {
            set(
                &mut row,
                "migration_path",
                text_or_null(raw_text(&event.after, "migration_path")),
            );
            set(
                &mut row,
                "migration_evidence",
                event.after.get("evidence").cloned().unwrap_or(Value::Null),
            );
            set(&mut row, "migration_position", event.position.to_json());
            set(&mut row, "migrated_at", context.block.timestamp.clone());
        } else {
            let mut starts = row
                .get("authority_start_positions")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let mut start = event.position.to_json();
            start["authority_kind"] = json!(raw_text(&event.after, "authority_kind"));
            start["authority_key"] = json!(raw_text(&event.after, "authority_key"));
            start["owner"] = json!(control_owner(&event.after));
            start["resource_id"] = json!(event.resource_id);
            starts.insert(arm(&event.source_family).to_owned(), start);
            set(&mut row, "authority_start_positions", Value::Object(starts));
        }
        row.entry("authority_start_positions")
            .or_insert_with(|| json!({}));
        put(rows, table, row, event)?;
    }
    Ok(())
}

/// The registry owner an authority event reports to the served control block
/// (name_current/build.sql:650-663): none when the owner word is unmasked, else the registry
/// owner, else the owner, lower-cased.
fn control_owner(after: &Value) -> Option<String> {
    if raw_text(after, "owner_word_unmasked").as_deref() == Some("true") {
        return None;
    }
    raw_lower(after, "registry_owner").or_else(|| raw_lower(after, "owner"))
}

/// The transaction and log index a binding row's provenance records, both or neither.
fn provenance_index(binding: &Row) -> (Option<i64>, Option<i64>) {
    let provenance = binding.get("provenance").cloned().unwrap_or(Value::Null);
    let index = |field: &str| raw_text(&provenance, field).and_then(|text| text.parse().ok());
    match (index("transaction_index"), index("log_index")) {
        (Some(transaction), Some(log)) => (Some(transaction), Some(log)),
        _ => (None, None),
    }
}

/// The SurfaceBound that opened a binding: the block's SurfaceBound of the binding's name and
/// resource at the transaction and log index the binding's provenance records, both null for a
/// binding the block itself synthesised. When the event names its binding id it must be this
/// one. One log opens at most one binding of a name and resource, so at most one event
/// matches; if an adapter ever emitted two, the first in the canonical order is taken.
fn opening_event<'a>(events: &'a [BlockEvent], binding: &Row) -> Option<&'a BlockEvent> {
    let binding_value = Value::Object(binding.clone());
    let text = |field: &str| raw_text(&binding_value, field);
    let (name, resource, id) = (
        text("logical_name_id"),
        text("resource_id"),
        text("surface_binding_id"),
    );
    let (transaction, log) = provenance_index(binding);
    events.iter().find(|event| {
        event.event_kind == "SurfaceBound"
            && event.logical_name_id == name
            && event.resource_id == resource
            && event.position.transaction_index == transaction
            && event.position.log_index == log
            && raw_text(&event.after, "surface_binding_id").is_none_or(|bound| Some(bound) == id)
    })
}

/// A binding's position in the canonical event order: its opening SurfaceBound's position, else
/// its own block and provenance index with the identity `binding:<surface binding id>`. The
/// fallback applies to any binding without a matching opener in the block, whether the adapter
/// dropped the SurfaceBound or an opener fails the match above; the candidate is still stored,
/// without the opener-derived fields (`normalized_event_id`, `state_derived`, the authority
/// metadata), and nothing is reported.
fn binding_position(binding: &Row, opening: Option<&BlockEvent>) -> Position {
    if let Some(event) = opening {
        return event.position.clone();
    }
    let (transaction_index, log_index) = provenance_index(binding);
    Position {
        block_number: binding
            .get("block_number")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        transaction_index,
        log_index,
        event_identity: format!(
            "binding:{}",
            raw_text(&Value::Object(binding.clone()), "surface_binding_id").unwrap_or_default()
        ),
    }
}

/// The order candidates are compared in: the canonical event order of their positions, then
/// the surface binding id for two bindings one event opened.
fn candidate_order(row: &Row) -> (Option<Position>, String) {
    (
        Position::of_row(row),
        row.get("surface_binding_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    )
}

async fn candidates(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::BINDING_CANDIDATE;
    let bindings: Vec<Value> = sqlx::query_scalar(
        "/* project:families.identity.block_bindings */ SELECT to_jsonb(binding)
         FROM surface_bindings binding
         WHERE binding.chain_id = $1 AND binding.block_number = $2 AND binding.block_hash = $3
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(context.chain_id)
    .bind(context.block.number)
    .bind(&context.block.hash)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the block's surface bindings", error))
    .map_err(in_family(table.name))?;
    let bindings: Vec<Row> = bindings
        .into_iter()
        .filter_map(|value| match value {
            Value::Object(row) => Some(row),
            _ => None,
        })
        .collect();
    // An epoch that turns an earlier binding registry-only arrives in a later block than the
    // binding: the name's earlier candidates of that resource become registry-only then.
    let epochs: Vec<&BlockEvent> = events
        .iter()
        .filter(|event| {
            event.event_kind == "AuthorityEpochChanged"
                && raw_text(&event.after, "authority_kind").as_deref() == Some("registry_only")
                && event.logical_name_id.is_some()
                && event.resource_id.is_some()
        })
        .collect();
    if bindings.is_empty() && epochs.is_empty() {
        return Ok(());
    }
    let mut names: Vec<String> = bindings
        .iter()
        .filter_map(|binding| raw_text(&Value::Object(binding.clone()), "logical_name_id"))
        .chain(
            epochs
                .iter()
                .filter_map(|event| event.logical_name_id.clone()),
        )
        .collect();
    names.sort();
    names.dedup();
    // The names' earlier candidates, for a registry-only binding's predecessor.
    let earlier: Vec<Value> = sqlx::query_scalar(
        "/* project:families.identity.name_candidates */ SELECT to_jsonb(candidate)
         FROM project_binding_candidate candidate
         WHERE candidate.chain_id = $1 AND candidate.logical_name_id = ANY($2)",
    )
    .bind(context.chain_id)
    .bind(&names)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the names' binding candidates", error))
    .map_err(in_family(table.name))?;
    let mut by_name: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for candidate in earlier.into_iter().filter_map(|value| match value {
        Value::Object(row) => Some(row),
        _ => None,
    }) {
        let name = candidate
            .get("logical_name_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        by_name.entry(name).or_default().push(candidate);
    }
    let keys = bindings
        .iter()
        .map(|binding| {
            key_of(
                table,
                [binding
                    .get("surface_binding_id")
                    .cloned()
                    .unwrap_or(Value::Null)],
            )
        })
        .chain(
            by_name
                .values()
                .flatten()
                .map(|candidate| key_of(table, [candidate["surface_binding_id"].clone()])),
        )
        .collect();
    load_rows(transaction, rows, table, keys).await?;

    let mut ordered: Vec<(Position, String, Row, Option<&BlockEvent>)> = bindings
        .into_iter()
        .map(|binding| {
            let opening = opening_event(events, &binding);
            let position = binding_position(&binding, opening);
            let id =
                raw_text(&Value::Object(binding.clone()), "surface_binding_id").unwrap_or_default();
            (position, id, binding, opening)
        })
        .collect();
    ordered.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    for (position, _, binding, opening) in ordered {
        let mut row = candidate_row(context, &binding, &position, opening);
        let name = text_of(&row, "logical_name_id");
        let resource = text_of(&row, "resource_id");
        let registry_only = epochs.iter().any(|event| {
            event.logical_name_id.as_deref() == Some(name.as_str())
                && event.resource_id.as_deref() == Some(resource.as_str())
        });
        if registry_only {
            handoff(&mut row, by_name.get(&name).map(Vec::as_slice));
        }
        by_name.entry(name).or_default().push(row.clone());
        rows.put(table, row).map_err(in_family(table.name))?;
    }
    for event in epochs {
        let name = event.logical_name_id.clone().unwrap_or_default();
        let earlier: Vec<Row> = by_name.get(&name).cloned().unwrap_or_default();
        for candidate in earlier.iter().filter(|candidate| {
            candidate.get("resource_id").and_then(Value::as_str) == event.resource_id.as_deref()
                && candidate.get("registry_only").and_then(Value::as_bool) != Some(true)
        }) {
            let mut row = candidate.clone();
            handoff(&mut row, Some(&earlier));
            if let Some(slot) = by_name.get_mut(&name).and_then(|rows| {
                rows.iter_mut()
                    .find(|other| other["surface_binding_id"] == row["surface_binding_id"])
            }) {
                *slot = row.clone();
            }
            rows.put(table, row).map_err(in_family(table.name))?;
        }
    }
    Ok(())
}

fn text_of(row: &Row, column: &str) -> String {
    row.get(column)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn candidate_row(
    context: &Context<'_>,
    binding: &Row,
    position: &Position,
    bound: Option<&BlockEvent>,
) -> Row {
    let binding_value = Value::Object(binding.clone());
    let text = |field: &str| raw_text(&binding_value, field);
    let name = text("logical_name_id").unwrap_or_default();
    let mut row = Row::new();
    set(
        &mut row,
        "surface_binding_id",
        text("surface_binding_id").unwrap_or_default(),
    );
    set(&mut row, "logical_name_id", name.clone());
    set(
        &mut row,
        "namespace",
        name.split_once(':').map_or("", |(namespace, _)| namespace),
    );
    set(&mut row, "chain_id", context.chain_id);
    set(
        &mut row,
        "authority_arm",
        text("authority_arm").unwrap_or_default(),
    );
    set(
        &mut row,
        "resource_id",
        text("resource_id").unwrap_or_default(),
    );
    set(
        &mut row,
        "binding_kind",
        text("binding_kind").unwrap_or_default(),
    );
    set(
        &mut row,
        "canonicality_state",
        text("canonicality_state").unwrap_or_default(),
    );
    set(
        &mut row,
        "active_from",
        binding.get("active_from").cloned().unwrap_or(Value::Null),
    );
    set(
        &mut row,
        "surface_namehash",
        text_or_null(namehash_of(&name)),
    );
    position.write_columns(&mut row);
    set(
        &mut row,
        "normalized_event_id",
        json!(bound.map(|event| event.normalized_event_id)),
    );
    set(
        &mut row,
        "state_derived",
        bound
            .and_then(|event| event.after.get("state_derived"))
            .and_then(Value::as_bool)
            .map_or(Value::Null, Value::Bool),
    );
    set(
        &mut row,
        "authority_kind",
        text_or_null(bound.and_then(|event| raw_text(&event.after, "authority_kind"))),
    );
    set(
        &mut row,
        "authority_key",
        text_or_null(bound.and_then(|event| raw_text(&event.after, "authority_key"))),
    );
    set(
        &mut row,
        "bound_owner",
        text_or_null(bound.and_then(|event| control_owner(&event.after))),
    );
    let wrapper = bound.filter(|event| event.source_family == "ens_v1_wrapper_l1");
    set(
        &mut row,
        "wrapped_registrar_resource_id",
        text_or_null(
            wrapper.and_then(|event| raw_text(&event.after, "wrapped_registrar_resource_id")),
        ),
    );
    set(
        &mut row,
        "node",
        text_or_null(wrapper.and_then(|event| raw_lower(&event.after, "node"))),
    );
    set(
        &mut row,
        "transaction_hash",
        text_or_null(wrapper.and_then(|event| event.transaction_hash.clone())),
    );
    set(
        &mut row,
        "emitting_address",
        text_or_null(wrapper.and_then(BlockEvent::emitting_address)),
    );
    set(
        &mut row,
        "surface_bound_position",
        bound.map_or(Value::Null, |event| event.position.to_json()),
    );
    set(&mut row, "registry_only", false);
    for column in [
        "predecessor_resource_id",
        "predecessor_position",
        "predecessor_wrapped_registrar_resource_id",
        "predecessor_node",
        "lease_resource_id",
        "lease_position",
    ] {
        set(&mut row, column, Value::Null);
    }
    row
}

/// Mark a candidate registry-only and record its handoff (stage.rs:47-135): the latest earlier
/// candidate of the name and arm is the predecessor, with the wrapper lease and node it recorded
/// when it is a NameWrapper binding; the lease stands for the predecessor's resource at the
/// predecessor's position until a later registrar grant replaces it (`successor_grant`).
fn handoff(row: &mut Row, earlier: Option<&[Row]>) {
    set(row, "registry_only", true);
    let order = candidate_order(row);
    let arm = row.get("authority_arm").cloned();
    let predecessor = earlier
        .into_iter()
        .flatten()
        .filter(|candidate| {
            candidate.get("authority_arm").cloned() == arm && candidate_order(candidate) < order
        })
        .max_by_key(|candidate| candidate_order(candidate));
    let field = |column: &str| {
        predecessor
            .and_then(|candidate| candidate.get(column).cloned())
            .unwrap_or(Value::Null)
    };
    let position = predecessor
        .and_then(Position::of_row)
        .map_or(Value::Null, |position| position.to_json());
    set(row, "predecessor_resource_id", field("resource_id"));
    set(row, "predecessor_position", position.clone());
    set(
        row,
        "predecessor_wrapped_registrar_resource_id",
        field("wrapped_registrar_resource_id"),
    );
    set(row, "predecessor_node", field("node"));
    set(row, "lease_resource_id", field("resource_id"));
    set(row, "lease_position", position);
}
