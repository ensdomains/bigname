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

fn classification(kind: &str, status: &str, reason: Option<&str>, manifest: Option<i64>) -> Value {
    json!({"chain_id": "ethereum-mainnet",
           "resolver_address": "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41",
           "classification": {"kind": kind}, "support_status": status,
           "unsupported_reason": reason, "manifest_id": manifest})
}

// A text overlay is served only while its resolver is supported and the overlay's admission
// equals the four classification fields (storage `families/records/text_hydration.rs`), and a
// selector is eligible for a read only while its resolver is supported (`text_eligible.sql`).
// So a change between two unsupported rows, or between no row and an unsupported one, changes
// no selector's hydration work, and any change with a supported side re-derives it. The images
// are the block's: the before-image and the row the block wrote, or the key alone for a
// deletion. The stored row is still the one before the block.
#[tokio::test]
async fn a_classification_change_re_derives_text_hydration_work_only_when_a_side_is_supported()
-> Result<()> {
    let unsupported = classification("a", "unsupported", Some("resolver_not_declared"), Some(1));
    let supported = classification("a", "supported", None, Some(1));
    let cases: [(&str, Option<Value>, Option<Value>, bool); 10] = [
        (
            "unsupported re-filed with a new reason, manifest and classification",
            Some(classification(
                "b",
                "unsupported",
                Some("resolver_manifest_not_active"),
                None,
            )),
            Some(unsupported.clone()),
            false,
        ),
        (
            "unsupported, manifest only",
            Some(classification(
                "a",
                "unsupported",
                Some("resolver_not_declared"),
                Some(2),
            )),
            Some(unsupported.clone()),
            false,
        ),
        (
            "unsupported row inserted",
            None,
            Some(unsupported.clone()),
            false,
        ),
        (
            "unsupported row deleted",
            Some(unsupported.clone()),
            None,
            false,
        ),
        (
            "supported row inserted",
            None,
            Some(supported.clone()),
            true,
        ),
        ("supported row deleted", Some(supported.clone()), None, true),
        (
            "support withdrawn",
            Some(supported.clone()),
            Some(unsupported.clone()),
            true,
        ),
        (
            "supported, manifest re-stamped",
            Some(classification("a", "supported", None, Some(2))),
            Some(supported.clone()),
            true,
        ),
        (
            "supported, classification changed",
            Some(classification("b", "supported", None, Some(1))),
            Some(supported.clone()),
            true,
        ),
        (
            "supported, nothing changed",
            Some(supported.clone()),
            Some(supported.clone()),
            false,
        ),
    ];
    let (database, pool) = database().await?;
    let mut wrong = Vec::new();
    let mut selector = value("text:a");
    selector.insert("arm".into(), json!("named"));
    let classifications = &tables::RESOLVER_CLASSIFICATION;
    for (case, before, after, re_derives) in cases {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            "INSERT INTO project_node_record_value (chain_id, resolver_address, arm,
                 arm_identity, record_key, block_number, event_identity, status, source_family,
                 namespace)
             VALUES ($1, $2, $3, $4, $5, 1, 'set', 'unsupported', 'ens_v1_resolver_l1', 'ens')",
        )
        .bind(selector["chain_id"].as_str())
        .bind(selector["resolver_address"].as_str())
        .bind(selector["arm"].as_str())
        .bind(selector["arm_identity"].as_str())
        .bind(selector["record_key"].as_str())
        .execute(&mut *transaction)
        .await?;
        let mut images = Vec::new();
        if let Some(stored) = &before {
            sqlx::query(
                "INSERT INTO project_resolver_classification (chain_id, resolver_address,
                     block_number, event_identity, classification, support_status,
                     unsupported_reason, manifest_id)
                 VALUES ($1 ->> 'chain_id', $1 ->> 'resolver_address', 1, 'classified',
                         $1 -> 'classification', $1 ->> 'support_status',
                         $1 ->> 'unsupported_reason', ($1 ->> 'manifest_id')::bigint)",
            )
            .bind(stored)
            .execute(&mut *transaction)
            .await?;
            images.push(stored.clone());
        }
        match after {
            Some(after) => images.push(after),
            None => images.push(Value::Object(key(
                classifications,
                &row(before.clone().expect("a deletion has its before-image")),
            ))),
        }
        let keys = super::text_keys(
            &mut transaction,
            "ethereum-mainnet",
            &Images::from([(classifications.name, images)]),
        )
        .await?;
        let expected = if re_derives {
            vec![Value::Object(key(&tables::NODE_RECORD_VALUE, &selector))]
        } else {
            vec![]
        };
        if keys != expected {
            wrong.push(format!("{case}: {keys:?}"));
        }
        transaction.rollback().await?;
    }
    database.cleanup().await?;
    assert!(wrong.is_empty(), "wrong text hydration keys: {wrong:#?}");
    Ok(())
}
