//! F6 and F7, resolver records. A node-keyed write belongs to a partition by the arm the record
//! inventory admits it through (record_inventory.rs, `attributed_events`): a named write to the
//! named arm by its logical name; an unnamed ENSv1 or Basenames resolver write to the native arm
//! by node and source family; an unnamed ENSv2 resolver write to the guarded arm by node, source
//! family, namespace and declaration manifest. The partition keeps its latest version event; each
//! record key keeps its latest write and, for the `AddrChanged` half of a coin-60 pair, the
//! `AddressChanged` half one log earlier. Record-id writes keep the latest value per record id
//! and key; a resolver link keeps the latest record id per node, `0` included, and belongs to
//! the resolver that emitted it only when its payload names that resolver.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    keys,
    reduce::{Context, current, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null},
    store::{Row, RowSet},
    tables,
};
use crate::Result;

const NATIVE_FAMILIES: [&str; 2] = ["ens_v1_resolver_l1", "basenames_base_resolver"];
const GUARDED_FAMILY: &str = "ens_v2_resolver_l1";

/// The partition a node-keyed record event belongs to: arm and arm identity.
fn partition(event: &BlockEvent, node: &str) -> Option<(&'static str, String)> {
    if let Some(name) = &event.logical_name_id {
        return Some(("named", name.clone()));
    }
    let family = event.source_family.as_str();
    if NATIVE_FAMILIES.contains(&family) {
        return Some(("native", format!("{node}|{family}")));
    }
    (family == GUARDED_FAMILY).then(|| {
        (
            "guarded",
            format!(
                "{node}|{family}|{}|{}",
                event.namespace,
                event
                    .source_manifest_id
                    .map(|id| id.to_string())
                    .unwrap_or_default()
            ),
        )
    })
}

/// The status the record inventory derives from a write's payload, before its read-time
/// coin-60 zero-address rule.
fn status(after: &Value) -> &'static str {
    let blank =
        |value: Option<String>| value.is_some_and(|value| value.is_empty() || value == "0x");
    if after.get("value").is_some() {
        let family = raw_text(after, "record_family");
        let cleared = blank(raw_text(after, "value"))
            || blank(
                after
                    .get("value")
                    .and_then(|value| raw_text(value, "bytes")),
            );
        if matches!(family.as_deref(), Some("contenthash" | "addr")) && cleared {
            "not_found"
        } else {
            "success"
        }
    } else if after.get("contenthash_hex").is_some() {
        if blank(raw_text(after, "contenthash_hex")) {
            "not_found"
        } else {
            "success"
        }
    } else if after.get("address_bytes_hex").is_some() {
        if blank(raw_text(after, "address_bytes_hex")) {
            "not_found"
        } else {
            "success"
        }
    } else {
        "unsupported"
    }
}

/// The payload columns a record write sets on its value row.
fn write_value(row: &mut Row, event: &BlockEvent) {
    let after = &event.after;
    set(row, "status", status(after));
    set(
        row,
        "value",
        after.get("value").cloned().unwrap_or(Value::Null),
    );
    for field in [
        "record_family",
        "selector_key",
        "contenthash_hex",
        "address_bytes_hex",
        "source_event",
        "storage_model",
    ] {
        set(row, field, text_or_null(raw_text(after, field)));
    }
    set(row, "source_family", event.source_family.clone());
    set(row, "namespace", event.namespace.clone());
    set(row, "source_manifest_id", json!(event.source_manifest_id));
    // A name record's claim input as the event carried it (reverse claims read it).
    for field in ["raw_name", "raw_name_bytes"] {
        set(row, field, after.get(field).cloned().unwrap_or(Value::Null));
    }
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let writes: Vec<&BlockEvent> = events
        .iter()
        .filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "RecordChanged" | "RecordVersionChanged"
            )
        })
        .collect();
    // Load every row the block's writes and links address.
    let (mut partitions, mut values, mut ids, mut links) = (vec![], vec![], vec![], vec![]);
    for event in &writes {
        let Some(resolver) = keys::record_resolver(event) else {
            continue;
        };
        if raw_text(&event.after, "storage_model").as_deref() == Some("resolver_record_id") {
            if let (Some(id), Some(key)) = (
                raw_text(&event.after, "resolver_record_id"),
                raw_text(&event.after, "record_key"),
            ) {
                ids.push(key_of(
                    &tables::RECORD_ID_VALUE,
                    [chain.clone(), json!(resolver), json!(id), json!(key)],
                ));
            }
            continue;
        }
        let Some(node) = raw_lower(&event.after, "node") else {
            continue;
        };
        let Some((arm, identity)) = partition(event, &node) else {
            continue;
        };
        partitions.push(key_of(
            &tables::NODE_RECORD_PARTITION,
            [chain.clone(), json!(resolver), json!(arm), json!(identity)],
        ));
        if let Some(key) = raw_text(&event.after, "record_key") {
            values.push(key_of(
                &tables::NODE_RECORD_VALUE,
                [
                    chain.clone(),
                    json!(resolver),
                    json!(arm),
                    json!(identity),
                    json!(key),
                ],
            ));
        }
    }
    for event in events
        .iter()
        .filter(|event| event.event_kind == "ResolverRecordLinked")
    {
        if let (Some(resolver), Some(node)) =
            (keys::link_resolver(event), raw_lower(&event.after, "node"))
        {
            links.push(key_of(
                &tables::RESOLVER_LINK,
                [chain.clone(), json!(resolver), json!(node)],
            ));
        }
    }
    load_rows(
        transaction,
        rows,
        &tables::NODE_RECORD_PARTITION,
        partitions,
    )
    .await?;
    load_rows(transaction, rows, &tables::NODE_RECORD_VALUE, values).await?;
    load_rows(transaction, rows, &tables::RECORD_ID_VALUE, ids).await?;
    load_rows(transaction, rows, &tables::RESOLVER_LINK, links).await?;

    for event in events {
        match event.event_kind.as_str() {
            "RecordChanged" | "RecordVersionChanged" => write(rows, &chain, event)?,
            "ResolverRecordLinked" => link(rows, &chain, event)?,
            _ => {}
        }
    }
    Ok(())
}

fn write(rows: &mut RowSet, chain: &Value, event: &BlockEvent) -> Result<()> {
    let Some(resolver) = keys::record_resolver(event) else {
        return Ok(());
    };
    let after = &event.after;
    let record_key = raw_text(after, "record_key");
    if raw_text(after, "storage_model").as_deref() == Some("resolver_record_id") {
        let (Some(id), Some(record_key), true) = (
            raw_text(after, "resolver_record_id"),
            record_key,
            event.event_kind == "RecordChanged",
        ) else {
            return Ok(());
        };
        let table = &tables::RECORD_ID_VALUE;
        let mut row = current(
            rows,
            table,
            &key_of(
                table,
                [chain.clone(), json!(resolver), json!(id), json!(record_key)],
            ),
        );
        write_value(&mut row, event);
        return put(rows, table, row, event);
    }
    let Some(node) = raw_lower(after, "node") else {
        return Ok(());
    };
    let Some((arm, identity)) = partition(event, &node) else {
        return Ok(());
    };
    let base = [chain.clone(), json!(resolver), json!(arm), json!(identity)];

    let table = &tables::NODE_RECORD_PARTITION;
    let mut row = current(rows, table, &key_of(table, base.clone()));
    set(&mut row, "node", node.clone());
    set(
        &mut row,
        "logical_name_id",
        text_or_null(event.logical_name_id.clone()),
    );
    set(&mut row, "source_family", event.source_family.clone());
    set(&mut row, "namespace", event.namespace.clone());
    set(
        &mut row,
        "source_manifest_id",
        json!(event.source_manifest_id),
    );
    if event.event_kind == "RecordVersionChanged" {
        set(&mut row, "version_position", event.position.to_json());
    }
    row.entry("version_position").or_insert(Value::Null);
    put(rows, table, row, event)?;

    let (Some(record_key), "RecordChanged") = (record_key, event.event_kind.as_str()) else {
        return Ok(());
    };
    let table = &tables::NODE_RECORD_VALUE;
    let key = key_of(table, base.into_iter().chain([json!(record_key)]));
    let previous = current(rows, table, &key);
    // The AddrChanged half of a coin-60 pair keeps the AddressChanged half written one log
    // earlier in the same transaction.
    let sibling = (raw_text(after, "source_event").as_deref() == Some("AddrChanged")
        && record_key == "addr:60"
        && previous.get("source_event").and_then(Value::as_str) == Some("AddressChanged")
        && previous.get("block_number") == Some(&json!(event.position.block_number))
        && previous.get("transaction_index") == Some(&json!(event.position.transaction_index))
        && event
            .position
            .log_index
            .zip(previous.get("log_index").and_then(Value::as_i64))
            .is_some_and(|(log, earlier)| earlier + 1 == log))
    .then(|| {
        let column = |name: &str| previous.get(name).cloned().unwrap_or(Value::Null);
        [
            column("value"),
            super::input::Position::of_row(&previous)
                .map_or(Value::Null, |position| position.to_json()),
            column("status"),
            column("address_bytes_hex"),
        ]
    });
    let mut row = previous;
    write_value(&mut row, event);
    // The served record of the pair is the AddressChanged half (record_inventory.rs,
    // `ranked_records`): its status and both payload shapes stay beside this half's own.
    let [sibling_value, sibling_position, sibling_status, sibling_hex] =
        sibling.unwrap_or([Value::Null, Value::Null, Value::Null, Value::Null]);
    set(&mut row, "sibling_value", sibling_value);
    set(&mut row, "sibling_position", sibling_position);
    set(&mut row, "sibling_status", sibling_status);
    set(&mut row, "sibling_address_bytes_hex", sibling_hex);
    set(&mut row, "node", node);
    set(
        &mut row,
        "logical_name_id",
        text_or_null(event.logical_name_id.clone()),
    );
    set(
        &mut row,
        "resource_id",
        text_or_null(event.resource_id.clone()),
    );
    put(rows, table, row, event)
}

fn link(rows: &mut RowSet, chain: &Value, event: &BlockEvent) -> Result<()> {
    let (Some(resolver), Some(node)) =
        (keys::link_resolver(event), raw_lower(&event.after, "node"))
    else {
        return Ok(());
    };
    let table = &tables::RESOLVER_LINK;
    let mut row = current(
        rows,
        table,
        &key_of(table, [chain.clone(), json!(resolver), json!(node)]),
    );
    set(
        &mut row,
        "record_id",
        raw_text(&event.after, "resolver_record_id").unwrap_or_else(|| "0".to_owned()),
    );
    set(
        &mut row,
        "storage_model",
        text_or_null(raw_text(&event.after, "storage_model")),
    );
    put(rows, table, row, event)
}
