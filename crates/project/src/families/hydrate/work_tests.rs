//! The changes that say which hydration retry scheduling one block clears.
use anyhow::Result;
use serde_json::{Value, json};

use super::{Images, changed_images};
use crate::families::{
    guard_tests::database,
    store::{Row, RowSet},
    tables::{self, TableSpec},
};

fn row(value: Value) -> Row {
    serde_json::from_value(value).expect("a row is an object")
}

/// The key columns of `row`, as `RowSet::load` takes them.
fn key(table: &TableSpec, row: &Row) -> Row {
    table
        .key
        .iter()
        .map(|column| ((*column).to_owned(), row[*column].clone()))
        .collect()
}

fn tuple(address: &str) -> Row {
    row(
        json!({"address": address, "coin_type": "60", "namespace": "ens",
               "chain_id": "ethereum-mainnet", "source_event": "ReverseClaimed"}),
    )
}

fn value(record_key: &str) -> Row {
    row(json!({"chain_id": "ethereum-mainnet",
               "resolver_address": "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41",
               "arm": "ens_v1", "arm_identity": "0x11", "record_key": record_key,
               "value": record_key}))
}

// A rebuild range applies its blocks in one working set. The second block's images hold the rows
// that block put and the row it deleted after the first block put it. They hold nothing the first
// block wrote and the second left alone.
#[tokio::test]
async fn hydration_images_of_a_range_block_hold_only_that_blocks_changes() -> Result<()> {
    let (database, pool) = database().await?;
    let mut transaction = pool.begin().await?;
    let mut rows = RowSet::default();
    let (tuples, values) = (&tables::REVERSE_TUPLE, &tables::NODE_RECORD_VALUE);
    let first_tuple = tuple("0x0000000000000000000000000000000000000001");
    let second_tuple = tuple("0x0000000000000000000000000000000000000002");
    let (first_value, second_value) = (value("text:a"), value("text:b"));
    let removed_value = value("text:c");
    rows.load(
        &mut transaction,
        tuples,
        [&first_tuple, &second_tuple].map(|entry| key(tuples, entry)),
    )
    .await?;
    rows.load(
        &mut transaction,
        values,
        [&first_value, &second_value, &removed_value].map(|entry| key(values, entry)),
    )
    .await?;

    rows.put(tuples, first_tuple)?;
    rows.put(values, first_value)?;
    rows.put(values, removed_value.clone())?;
    rows.end_block();

    rows.put(tuples, second_tuple.clone())?;
    rows.put(values, second_value.clone())?;
    rows.delete(values, &key(values, &removed_value))?;

    // A deletion contributes its earlier row and its key.
    let expected = Images::from([
        (
            values.name,
            vec![
                Value::Object(second_value),
                Value::Object(removed_value.clone()),
                Value::Object(key(values, &removed_value)),
            ],
        ),
        (tuples.name, vec![Value::Object(second_tuple)]),
    ]);
    assert_eq!(changed_images(&rows), expected);
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
