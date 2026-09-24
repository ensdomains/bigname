//! F1, name identity. A name keeps its latest MigrationApplied (migration path, evidence and
//! time; children.rs `parent_boundaries`) and the start of each authority arm's latest epoch (the
//! latest AuthorityEpochChanged per arm). Every binding candidate is kept as it was created,
//! selected or not: its identity row from `surface_bindings`, the facts of the SurfaceBound that
//! opened it (state-derived, authority kind, and for a NameWrapper binding the registrar lease it
//! recorded, the node, transaction and emitter; name_authority/stage.rs), and for a
//! registry-only binding the binding it replaced and the lease that stands for it, to begin with
//! the replaced binding's resource (stage.rs, `registry_only_handoffs`).
use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

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

/// The candidate position of a binding row: its block and the transaction and log its
/// provenance records, both or neither.
fn binding_position(binding: &Map<String, Value>) -> (i64, Option<i64>, Option<i64>) {
    let provenance = binding.get("provenance").cloned().unwrap_or(Value::Null);
    let index = |field: &str| raw_text(&provenance, field).and_then(|text| text.parse().ok());
    let (transaction, log) = match (index("transaction_index"), index("log_index")) {
        (Some(transaction), Some(log)) => (Some(transaction), Some(log)),
        _ => (None, None),
    };
    (
        binding
            .get("block_number")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        transaction,
        log,
    )
}

/// The order registry-only handoffs compare candidates in: block, then transaction and log with
/// a missing one first, then the binding id.
fn candidate_order(row: &Map<String, Value>) -> (i64, i64, i64, String) {
    let field = |name: &str| row.get(name).and_then(Value::as_i64);
    (
        field("block_number").unwrap_or_default(),
        field("transaction_index").unwrap_or(-1),
        field("log_index").unwrap_or(-1),
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
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY binding.surface_binding_id",
    )
    .bind(context.chain_id)
    .bind(context.block.number)
    .bind(&context.block.hash)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the block's surface bindings", error))
    .map_err(in_family(table.name))?;
    if bindings.is_empty() {
        return Ok(());
    }
    let names: Vec<String> = bindings
        .iter()
        .filter_map(|binding| raw_text(binding, "logical_name_id"))
        .collect();
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
        .collect();
    load_rows(transaction, rows, table, keys).await?;

    let mut ordered: Vec<Row> = bindings
        .into_iter()
        .filter_map(|value| match value {
            Value::Object(row) => Some(row),
            _ => None,
        })
        .collect();
    ordered.sort_by_key(|binding| {
        let (block, transaction, log) = binding_position(binding);
        (
            block,
            transaction.unwrap_or(-1),
            log.unwrap_or(-1),
            raw_text(&Value::Object(binding.clone()), "surface_binding_id"),
        )
    });
    for binding in ordered {
        let row = candidate_row(context, events, &binding, &by_name);
        let name = row
            .get("logical_name_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        by_name.entry(name).or_default().push(row.clone());
        rows.put(table, row).map_err(in_family(table.name))?;
    }
    Ok(())
}

fn candidate_row(
    context: &Context<'_>,
    events: &[BlockEvent],
    binding: &Row,
    by_name: &BTreeMap<String, Vec<Row>>,
) -> Row {
    let binding_value = Value::Object(binding.clone());
    let text = |field: &str| raw_text(&binding_value, field);
    let name = text("logical_name_id").unwrap_or_default();
    let resource = text("resource_id").unwrap_or_default();
    let authority_arm = text("authority_arm").unwrap_or_default();
    let (block, transaction, log) = binding_position(binding);
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
    set(&mut row, "authority_arm", authority_arm.clone());
    set(&mut row, "resource_id", resource.clone());
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
    set(&mut row, "block_number", block);
    set(&mut row, "transaction_index", json!(transaction));
    set(&mut row, "log_index", json!(log));
    set(
        &mut row,
        "event_identity",
        text("surface_binding_id").unwrap_or_default(),
    );

    let bound = events.iter().rev().find(|event| {
        event.event_kind == "SurfaceBound"
            && event.logical_name_id.as_deref() == Some(name.as_str())
            && event.resource_id.as_deref() == Some(resource.as_str())
    });
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

    let registry_only = events.iter().any(|event| {
        event.event_kind == "AuthorityEpochChanged"
            && event.logical_name_id.as_deref() == Some(name.as_str())
            && event.resource_id.as_deref() == Some(resource.as_str())
            && raw_text(&event.after, "authority_kind").as_deref() == Some("registry_only")
    });
    set(&mut row, "registry_only", registry_only);
    let order = candidate_order(&row);
    let predecessor = registry_only
        .then(|| {
            by_name
                .get(&name)
                .into_iter()
                .flatten()
                .filter(|candidate| {
                    candidate.get("authority_arm").and_then(Value::as_str)
                        == Some(authority_arm.as_str())
                        && candidate_order(candidate) < order
                })
                .max_by_key(|candidate| candidate_order(candidate))
        })
        .flatten();
    let predecessor_resource =
        predecessor.and_then(|candidate| candidate.get("resource_id").cloned());
    set(
        &mut row,
        "predecessor_resource_id",
        predecessor_resource.clone().unwrap_or(Value::Null),
    );
    set(
        &mut row,
        "predecessor_position",
        predecessor
            .and_then(Position::of_row)
            .map_or(Value::Null, |position| position.to_json()),
    );
    set(
        &mut row,
        "lease_resource_id",
        predecessor_resource.unwrap_or(Value::Null),
    );
    set(&mut row, "lease_position", Value::Null);
    row
}
