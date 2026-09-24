//! Undo of one family block restores every family row and the marker byte for byte: rows the
//! block created are removed, rows it changed or cleared get their before-image back, and rows it
//! deleted return.
use anyhow::Result;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    FamilyOptions, block,
    guard_tests::{CHAIN, database, hash},
    marker,
    store::{Row, RowSet},
    tables::{self, TableSpec},
    undo,
};
use crate::Marker;

fn at(number: i64) -> Marker {
    Marker {
        number,
        hash: hash(number),
    }
}

fn row(value: Value) -> Row {
    let Value::Object(row) = value else {
        unreachable!("rows are objects")
    };
    row
}

fn position(number: i64, log: i64) -> Value {
    json!({
        "chain_id": CHAIN,
        "block_number": number,
        "transaction_index": 0,
        "log_index": log,
        "event_identity": format!("event-{number}-{log}"),
        "normalized_event_id": number * 100 + log,
    })
}

fn with(mut base: Value, fields: Value) -> Row {
    if let (Value::Object(base), Value::Object(fields)) = (&mut base, fields) {
        base.extend(fields);
    }
    row(base)
}

fn alias(number: i64, log: i64, active: bool) -> Row {
    with(
        position(number, log),
        json!({"logical_name_id": "ens:alias.eth", "active": active,
               "to_logical_name_id": if active { json!("ens:target.eth") } else { Value::Null }}),
    )
}

fn link(number: i64, log: i64, node: &str, record_id: &str) -> Row {
    with(
        position(number, log),
        json!({"resolver_address": "0x00000000000000000000000000000000000000r1",
               "node": node, "record_id": record_id}),
    )
}

fn subregistry(number: i64, log: i64) -> Row {
    with(
        position(number, log),
        json!({"logical_name_id": "ens:parent.eth",
               "subregistry_address": "0x0000000000000000000000000000000000000001"}),
    )
}

/// One block that writes `after` over each key (or deletes it when `after` is `None`).
async fn publish(
    pool: &PgPool,
    number: i64,
    predecessor: Option<&Marker>,
    changes: Vec<(&'static TableSpec, Row, Option<Row>)>,
) -> Result<()> {
    let plan = block::Plan {
        predecessor,
        contiguous: true,
        bootstrap: false,
    };
    let (mut transaction, prior, header) = block::open(pool, CHAIN, number, &plan).await?;
    let mut rows = RowSet::default();
    for (table, key, after) in changes {
        rows.load(&mut transaction, table, [key.clone()]).await?;
        match after {
            Some(after) => rows.put(table, after)?,
            None => rows.delete(table, &key)?,
        }
    }
    block::publish(
        transaction,
        CHAIN,
        &header,
        &prior,
        &rows,
        &plan,
        (None, None),
        &FamilyOptions::new("undo"),
    )
    .await?;
    Ok(())
}

/// Every family table as ordered JSON text, and the marker without its sequence.
async fn snapshot(pool: &PgPool) -> Result<Vec<(String, String)>> {
    let mut snapshot = Vec::new();
    for name in tables::JOURNALLED
        .iter()
        .map(|table| table.name)
        .chain(tables::DERIVED)
    {
        let rows: String = sqlx::query_scalar(&format!(
            "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), '[]')::text
             FROM {name} t"
        ))
        .fetch_one(pool)
        .await?;
        snapshot.push((name.to_owned(), rows));
    }
    let marker: Option<String> = sqlx::query_scalar(
        "SELECT (to_jsonb(m) - 'sequence')::text FROM project_family_marker m WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_optional(pool)
    .await?;
    snapshot.push(("marker".to_owned(), marker.unwrap_or_default()));
    Ok(snapshot)
}

fn key(table: &'static TableSpec, row: &Row) -> Row {
    table
        .key
        .iter()
        .map(|column| ((*column).to_owned(), row[*column].clone()))
        .collect()
}

#[tokio::test]
async fn undoing_a_block_restores_every_family_row_and_the_marker_byte_for_byte() -> Result<()> {
    let (database, pool) = database().await?;
    let (name_alias, resolver_link, parent) = (
        &tables::NAME_ALIAS,
        &tables::RESOLVER_LINK,
        &tables::PARENT_SUBREGISTRY,
    );
    publish(
        &pool,
        10,
        None,
        vec![
            (
                name_alias,
                key(name_alias, &alias(10, 1, true)),
                Some(alias(10, 1, true)),
            ),
            (
                resolver_link,
                key(resolver_link, &link(10, 2, "n1", "7")),
                Some(link(10, 2, "n1", "7")),
            ),
            (
                parent,
                key(parent, &subregistry(10, 3)),
                Some(subregistry(10, 3)),
            ),
        ],
    )
    .await?;
    let before = snapshot(&pool).await?;
    let sequence = marker::read(&pool, CHAIN).await?.sequence;

    // Block 11 clears the alias and the first link, creates a second link and deletes the
    // parent's subregistry row.
    publish(
        &pool,
        11,
        Some(&at(10)),
        vec![
            (
                name_alias,
                key(name_alias, &alias(11, 1, false)),
                Some(alias(11, 1, false)),
            ),
            (
                resolver_link,
                key(resolver_link, &link(11, 2, "n1", "0")),
                Some(link(11, 2, "n1", "0")),
            ),
            (
                resolver_link,
                key(resolver_link, &link(11, 3, "n2", "8")),
                Some(link(11, 3, "n2", "8")),
            ),
            (parent, key(parent, &subregistry(10, 3)), None),
        ],
    )
    .await?;
    assert_ne!(
        snapshot(&pool).await?,
        before,
        "block 11 changed the families"
    );

    let restored = undo::undo_block(&pool, CHAIN, &at(11)).await?;
    assert_eq!(restored.and_then(|marker| marker.current), Some(at(10)));
    assert_eq!(
        snapshot(&pool).await?,
        before,
        "undo restores block 10's families"
    );
    assert_eq!(
        marker::read(&pool, CHAIN).await?.sequence,
        sequence + 2,
        "the block and its undo each advance the sequence"
    );
    let journalled: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT block_number FROM project_family_undo WHERE chain_id = $1 ORDER BY 1",
    )
    .bind(CHAIN)
    .fetch_all(&pool)
    .await?;
    assert_eq!(journalled, vec![10], "the undone block's journal is gone");

    // Undoing block 10 as well empties every table and the marker.
    let empty = undo::undo_block(&pool, CHAIN, &at(10)).await?;
    assert_eq!(empty.and_then(|marker| marker.current), None);
    for (name, rows) in snapshot(&pool).await? {
        if name != "marker" {
            assert_eq!(rows, "[]", "{name} is empty after undoing its only block");
        }
    }

    // A block the journal does not hold cannot be undone and leaves everything as it was.
    publish(&pool, 10, None, Vec::new()).await?;
    sqlx::query("DELETE FROM project_family_undo WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(&pool)
        .await?;
    assert_eq!(undo::undo_block(&pool, CHAIN, &at(10)).await?, None);
    assert_eq!(marker::read(&pool, CHAIN).await?.current, Some(at(10)));
    database.cleanup().await
}
