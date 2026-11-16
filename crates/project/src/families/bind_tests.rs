//! A block's journal, keys and rows bound as jsonb arrays in pieces store what one array per
//! statement stores: the family rows, the journal, the statistics and the state after an undo.
use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    FamilyOptions, bind_log, block,
    guard_tests::{CHAIN, NO_INTERPRET, database},
    hydrate, marker,
    store::{self, Change, Row, RowSet},
    tables::{self, TableSpec},
    undo_tests::{RESOLVER, at, key, row, snapshot, undo_one},
};
use crate::{ErrorKind, Marker};

/// The bind budget the tests set, small enough that their fixtures need several pieces.
pub(super) const PIECE: usize = 64 * 1024;

/// Every bind fits the test's budget, or holds one element too large for any piece.
pub(super) fn assert_pieces(binds: &[bind_log::Bind]) {
    for bind in binds {
        assert!(
            bind.bytes <= PIECE || bind.elements == 1,
            "{bind:?} is over the {PIECE}-byte budget"
        );
    }
}

fn elements(binds: &[bind_log::Bind]) -> BTreeMap<&'static str, usize> {
    let mut elements = BTreeMap::new();
    for bind in binds {
        *elements.entry(bind.tag).or_default() += bind.elements;
    }
    elements
}

fn count(binds: &[bind_log::Bind], tag: &str) -> usize {
    binds.iter().filter(|bind| bind.tag == tag).count()
}

/// A follow transaction open on `number`, past its fences.
pub(super) async fn open_block(pool: &PgPool, number: i64) -> Result<block::Opened> {
    let sequence = marker::read(pool, CHAIN).await?.sequence;
    let manifests = super::manifests::History::default();
    let plan = block::Plan {
        predecessor: None,
        sequence,
        contiguous: true,
        bootstrap: false,
        revision: &NO_INTERPRET,
        role: block::Role::Follow,
        manifests: &manifests,
        head: None,
    };
    Ok(block::open(pool, CHAIN, number, &plan).await?)
}

/// 4 to 12 KiB of filler, its length varied so no piece ends on a row count.
fn filler(seed: usize) -> String {
    "x".repeat(4_096 + (seed * 2_731) % 8_192)
}

fn link(number: i64, i: usize, record: &str) -> Row {
    row(json!({
        "chain_id": CHAIN,
        "resolver_address": RESOLVER,
        "node": format!("0xnode{i:03}"),
        "block_number": number,
        "transaction_index": 0,
        "log_index": i,
        "event_identity": format!("event-{number}-{i}-{}", filler(i + record.len() * 7)),
        "normalized_event_id": null,
        "record_id": record,
        "storage_model": null,
    }))
}

fn subregistry(number: i64, i: usize, seed: usize) -> Row {
    row(json!({
        "chain_id": CHAIN,
        "logical_name_id": format!("ens:parent{i:03}.eth"),
        "block_number": number,
        "transaction_index": 0,
        "log_index": i,
        "event_identity": format!("event-{number}-{i}-{}", filler(i + seed)),
        "normalized_event_id": null,
        "subregistry_address": format!("0x{i:040x}"),
    }))
}

type Changes = Vec<(&'static TableSpec, Row, Option<Row>)>;

fn put(changes: &mut Changes, table: &'static TableSpec, after: Row) {
    changes.push((table, key(table, &after), Some(after)));
}

/// Block 10 creates 40 resolver links and 20 subregistry pointers, each with a 4 to 12 KiB
/// field.
fn wide_10() -> Changes {
    let mut changes = Vec::new();
    for i in 0..40 {
        put(&mut changes, &tables::RESOLVER_LINK, link(10, i, "1"));
    }
    for i in 0..20 {
        put(
            &mut changes,
            &tables::PARENT_SUBREGISTRY,
            subregistry(10, i, 0),
        );
    }
    changes
}

/// Block 11 changes 38 links and every pointer, deletes link 38, keeps link 39 and creates
/// link 40, so its journal holds about 500 KiB of before-images.
fn wide_11() -> Changes {
    let mut changes = Vec::new();
    for i in 0..38 {
        put(&mut changes, &tables::RESOLVER_LINK, link(11, i, "22"));
    }
    let deleted = link(10, 38, "1");
    changes.push((
        &tables::RESOLVER_LINK,
        key(&tables::RESOLVER_LINK, &deleted),
        None,
    ));
    put(&mut changes, &tables::RESOLVER_LINK, link(11, 40, "22"));
    for i in 0..20 {
        put(
            &mut changes,
            &tables::PARENT_SUBREGISTRY,
            subregistry(11, i, 5),
        );
    }
    changes
}

/// One block that writes `after` over each key, or deletes it when `after` is `None`, loading
/// each table's keys in one call. Returns what the block wrote.
async fn publish(
    pool: &PgPool,
    number: i64,
    predecessor: Option<&Marker>,
    changes: Changes,
) -> Result<block::BlockStats> {
    let sequence = marker::read(pool, CHAIN).await?.sequence;
    let manifests = super::manifests::History::default();
    let plan = block::Plan {
        predecessor,
        sequence,
        contiguous: true,
        bootstrap: false,
        revision: &NO_INTERPRET,
        role: block::Role::Follow,
        manifests: &manifests,
        head: None,
    };
    let mut opened = block::open(pool, CHAIN, number, &plan).await?;
    let mut rows = RowSet::default();
    for table in [&tables::RESOLVER_LINK, &tables::PARENT_SUBREGISTRY] {
        let keys: Vec<Row> = changes
            .iter()
            .filter(|(spec, _, _)| spec.name == table.name)
            .map(|(_, key, _)| key.clone())
            .collect();
        rows.load(&mut opened.transaction, table, keys).await?;
    }
    for (table, key, after) in changes {
        match after {
            Some(after) => rows.put(table, after)?,
            None => rows.delete(table, &key)?,
        }
    }
    let options = FamilyOptions::new("bind");
    let (_, stats) = block::publish(opened, CHAIN, &rows, &plan, &options).await?;
    Ok(stats)
}

type Journal = Vec<(String, String, Option<Value>)>;

/// What one run of the wide blocks leaves.
struct Wide {
    after_10: Vec<(String, Value)>,
    after_11: Vec<(String, Value)>,
    journal_11: Journal,
    rows_11: BTreeMap<&'static str, u64>,
    undo_rows_11: u64,
    undone: Vec<(String, Value)>,
    binds: Vec<bind_log::Bind>,
}

/// Publish blocks 10 and 11 on a fresh database, then undo block 11, under `budget`.
async fn wide(budget: Option<usize>) -> Result<Wide> {
    bind_log::set_budget(budget);
    let (database, pool) = database().await?;
    bind_log::start();
    publish(&pool, 10, None, wide_10()).await?;
    let after_10 = snapshot(&pool).await?;
    let stats = publish(&pool, 11, Some(&at(10)), wide_11()).await?;
    let after_11 = snapshot(&pool).await?;
    let journal_11: Journal = sqlx::query_as(
        "SELECT family, key, before_image FROM project_family_undo
         WHERE chain_id = $1 AND block_number = 11 ORDER BY family, key",
    )
    .bind(CHAIN)
    .fetch_all(&pool)
    .await?;
    assert_eq!(undo_one(&pool, 10).await?, Some(at(10)));
    let binds = bind_log::take();
    let undone = snapshot(&pool).await?;
    bind_log::set_budget(None);
    database.cleanup().await?;
    Ok(Wide {
        after_10,
        after_11,
        journal_11,
        rows_11: stats.rows,
        undo_rows_11: stats.undo_rows,
        undone,
        binds,
    })
}

/// The snapshot without the catalogue stamp's publication sequence, which an undo advances.
fn unsequenced(mut snapshot: Vec<(String, Value)>) -> Vec<(String, Value)> {
    for (name, rows) in &mut snapshot {
        if name == tables::HISTORY_MARKER.name {
            for row in rows.as_array_mut().into_iter().flatten() {
                if let Some(row) = row.as_object_mut() {
                    row.remove("publication_sequence");
                }
            }
        }
    }
    snapshot
}

#[tokio::test]
async fn a_block_bound_in_pieces_stores_what_a_block_bound_whole_stores() -> Result<()> {
    let whole = wide(None).await?;
    let pieces = wide(Some(PIECE)).await?;
    assert_eq!(pieces.after_10, whole.after_10, "block 10's families");
    assert_eq!(pieces.after_11, whole.after_11, "block 11's families");
    assert_eq!(pieces.journal_11, whole.journal_11, "block 11's journal");
    assert_eq!(
        (&pieces.rows_11, pieces.undo_rows_11),
        (&whole.rows_11, whole.undo_rows_11),
        "block 11's statistics"
    );
    assert_eq!(pieces.undone, whole.undone, "the families after the undo");
    assert_eq!(
        elements(&pieces.binds),
        elements(&whole.binds),
        "every element is bound once"
    );
    assert_pieces(&pieces.binds);
    for tag in ["families.block.insert_journal", "families.store.insert"] {
        assert!(
            count(&pieces.binds, tag) > count(&whole.binds, tag),
            "{tag} is bound in pieces"
        );
    }
    assert_eq!(
        unsequenced(whole.undone),
        unsequenced(whole.after_10),
        "the undo restores block 10's families"
    );
    Ok(())
}

fn entry(family: &str, key: &str, image: Value) -> Value {
    json!({"family": family, "key": key, "before_image": image})
}

/// A before-image of 3 to 6 KiB, named by `tag`.
fn image(tag: &str, seed: usize) -> Value {
    json!({"tag": tag, "filler": "y".repeat(3_000 + (seed * 1_237) % 3_000)})
}

fn summary_key(i: usize) -> String {
    json!([CHAIN, format!("ens:n{i:03}.eth")]).to_string()
}

fn summaries(range: std::ops::Range<usize>, tag: &str) -> Vec<Value> {
    range
        .map(|i| entry("project_name_summary", &summary_key(i), image(tag, i)))
        .collect()
}

fn lookup_records(prefix: &str) -> Vec<Value> {
    (0..30)
        .map(|i| {
            let key = format!("{prefix}{i:02}");
            entry("project_lookup_record", &key, image(prefix, i))
        })
        .collect()
}

/// What four journal calls on one block leave.
struct Calls {
    counts: Vec<u64>,
    journal: Vec<(String, String, Option<String>)>,
    binds: Vec<bind_log::Bind>,
}

/// Journal summary keys 0 to 39, then 20 to 59, then two disjoint sets of lookup record keys,
/// on one block, as the summary and resolution-path refreshes and two lookup pages do.
async fn calls(budget: Option<usize>) -> Result<Calls> {
    bind_log::set_budget(budget);
    let (database, pool) = database().await?;
    let mut opened = open_block(&pool, 10).await?;
    bind_log::start();
    let mut counts = Vec::new();
    for journal in [
        summaries(0..40, "first"),
        summaries(20..60, "second"),
        lookup_records("a"),
        lookup_records("b"),
    ] {
        counts.push(
            block::insert_journal(&mut opened.transaction, CHAIN, &opened.block, journal).await?,
        );
    }
    let binds = bind_log::take();
    let journal: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT family, key, before_image ->> 'tag' FROM project_family_undo
         WHERE chain_id = $1 AND block_number = 10 ORDER BY family, key",
    )
    .bind(CHAIN)
    .fetch_all(&mut *opened.transaction)
    .await?;
    // A family without the dedupe arm journalled twice in one block fails on the key.
    let link_entry = |tag| vec![entry("project_resolver_link", "[\"link\"]", image(tag, 0))];
    block::insert_journal(
        &mut opened.transaction,
        CHAIN,
        &opened.block,
        link_entry("first"),
    )
    .await?;
    let error = block::insert_journal(
        &mut opened.transaction,
        CHAIN,
        &opened.block,
        link_entry("next"),
    )
    .await
    .expect_err("a key journalled twice in one block is refused");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
    drop(opened);
    bind_log::set_budget(None);
    database.cleanup().await?;
    Ok(Calls {
        counts,
        journal,
        binds,
    })
}

#[tokio::test]
async fn dedupe_keys_an_earlier_statement_journalled_keep_its_image_in_every_piece() -> Result<()> {
    let whole = calls(None).await?;
    let pieces = calls(Some(PIECE)).await?;
    assert_eq!(
        pieces.counts,
        [40_u64, 20, 30, 30],
        "the second call journals its 20 new keys"
    );
    assert_eq!(pieces.counts, whole.counts);
    assert_eq!(pieces.journal, whole.journal, "the same journal");
    let summary: Vec<(String, Option<String>)> = pieces
        .journal
        .iter()
        .filter(|(family, _, _)| family == "project_name_summary")
        .map(|(_, key, tag)| (key.clone(), tag.clone()))
        .collect();
    let expected: Vec<(String, Option<String>)> = (0..60)
        .map(|i| {
            let tag = if i < 40 { "first" } else { "second" };
            (summary_key(i), Some(tag.to_owned()))
        })
        .collect();
    assert_eq!(summary, expected, "a key keeps its first image");
    assert_pieces(&pieces.binds);
    assert!(
        count(&pieces.binds, "families.block.insert_journal") > whole.binds.len(),
        "the calls are bound in pieces"
    );
    Ok(())
}

#[tokio::test]
async fn a_journal_that_repeats_a_key_is_refused_before_any_statement() -> Result<()> {
    bind_log::set_budget(Some(PIECE));
    let (database, pool) = database().await?;
    // A summary key repeated pieces apart, which the arm of a later piece would skip, and a
    // link key repeated next to itself.
    let mut repeated_summary = summaries(0..30, "first");
    repeated_summary.push(entry(
        "project_name_summary",
        &summary_key(0),
        image("again", 0),
    ));
    let repeated_link = vec![
        entry("project_resolver_link", "[\"link\"]", image("first", 0)),
        entry("project_resolver_link", "[\"link\"]", image("again", 0)),
    ];
    for journal in [repeated_summary, repeated_link] {
        let mut opened = open_block(&pool, 10).await?;
        bind_log::start();
        let error = block::insert_journal(&mut opened.transaction, CHAIN, &opened.block, journal)
            .await
            .expect_err("a journal that repeats a key is refused");
        assert_eq!(bind_log::take(), Vec::new(), "nothing is bound");
        assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
        assert!(error.to_string().contains("repeats"), "{error}");
    }
    bind_log::set_budget(None);
    database.cleanup().await
}

#[tokio::test]
async fn an_element_over_the_budget_is_bound_alone_and_stored_whole() -> Result<()> {
    bind_log::set_budget(Some(PIECE));
    let (database, pool) = database().await?;
    let mut opened = open_block(&pool, 10).await?;
    let large = json!({"filler": "z".repeat(200 * 1024)});
    let journal = vec![
        entry("project_resolver_link", "[\"a\"]", image("small", 0)),
        entry("project_resolver_link", "[\"b\"]", large.clone()),
        entry("project_resolver_link", "[\"c\"]", image("small", 1)),
    ];
    bind_log::start();
    let journalled =
        block::insert_journal(&mut opened.transaction, CHAIN, &opened.block, journal).await?;
    let binds = bind_log::take();
    let stored: Option<Value> = sqlx::query_scalar(
        "SELECT before_image FROM project_family_undo WHERE chain_id = $1 AND key = '[\"b\"]'",
    )
    .bind(CHAIN)
    .fetch_one(&mut *opened.transaction)
    .await?;
    assert_eq!(journalled, 3_u64);
    assert_eq!(stored, Some(large), "the large image is stored whole");
    assert_eq!(
        binds.iter().map(|bind| bind.elements).collect::<Vec<_>>(),
        [1_usize, 1, 1],
        "each element is a piece of its own"
    );
    assert_pieces(&binds);
    drop(opened);
    bind_log::set_budget(None);
    database.cleanup().await
}

fn link_key(i: usize) -> Row {
    row(json!({"chain_id": CHAIN, "resolver_address": RESOLVER, "node": format!("node-{i}")}))
}

/// Load 2,000 resolver link keys, every other one stored, in one call.
async fn load(budget: Option<usize>) -> Result<(Vec<Option<Row>>, Vec<bind_log::Bind>)> {
    bind_log::set_budget(budget);
    let (database, pool) = database().await?;
    sqlx::query(
        "INSERT INTO project_resolver_link (chain_id, resolver_address, node, block_number,
             event_identity, record_id)
         SELECT $1, $2, 'node-' || i, 10, 'event-' || i, i::text
         FROM generate_series(0, 1998, 2) i",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(&pool)
    .await?;
    let mut opened = open_block(&pool, 10).await?;
    let mut rows = RowSet::default();
    bind_log::start();
    rows.load(
        &mut opened.transaction,
        &tables::RESOLVER_LINK,
        (0..2_000).map(link_key),
    )
    .await?;
    let binds = bind_log::take();
    let loaded = (0..2_000)
        .map(|i| rows.get(&tables::RESOLVER_LINK, &link_key(i)).cloned())
        .collect();
    drop(opened);
    bind_log::set_budget(None);
    database.cleanup().await?;
    Ok((loaded, binds))
}

#[tokio::test]
async fn loading_keys_in_pieces_loads_the_same_rows() -> Result<()> {
    let (whole, _) = load(None).await?;
    let (pieces, binds) = load(Some(PIECE)).await?;
    assert_eq!(pieces, whole, "the same rows");
    assert_eq!(
        pieces.iter().map(Option::is_some).collect::<Vec<_>>(),
        (0..2_000).map(|i| i % 2 == 0).collect::<Vec<_>>(),
        "every stored row is found"
    );
    assert!(binds.len() > 1, "the keys are bound in pieces");
    assert_pieces(&binds);
    Ok(())
}

#[tokio::test]
async fn a_row_repeated_in_another_piece_fails_on_its_key_as_it_does_whole() -> Result<()> {
    let (database, pool) = database().await?;
    for budget in [None, Some(PIECE)] {
        bind_log::set_budget(budget);
        let mut opened = open_block(&pool, 10).await?;
        let mut rows: Vec<Value> = (0..20).map(|i| Value::Object(link(10, i, "1"))).collect();
        rows.push(Value::Object(link(10, 0, "2")));
        let error = store::replace(
            &mut opened.transaction,
            &tables::RESOLVER_LINK,
            Vec::new(),
            rows,
        )
        .await
        .expect_err("a repeated key is refused");
        assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
    }
    bind_log::set_budget(None);
    database.cleanup().await
}

/// The tables a registration's release writes (by trace: lifecycle, name state, binding
/// candidates, the address fold, grants and name history) are not hydration sources, so the
/// hydration binds carry none of their rows.
#[test]
fn rows_a_release_writes_carry_no_hydration_bind() {
    let row = Row::new();
    for table in [
        &tables::NAME_STATE,
        &tables::BINDING_CANDIDATE,
        &tables::LIFECYCLE_KEY_STATE,
        &tables::LIFECYCLE_TRIPLE_SUMMARY,
        &tables::LIFECYCLE_ASSOCIATION,
        &tables::LIFECYCLE_EVENT,
        &tables::ADDRESS_NAME_FOLD,
        &tables::GRANT,
        &tables::NAME_HISTORY,
    ] {
        let change = Change {
            table,
            key: "[]",
            before: None,
            after: Some(&row),
        };
        assert!(
            hydrate::work::images(&[change]).is_empty(),
            "{} feeds a hydration bind",
            table.name
        );
    }
}

/// The bind budget in code.
const BUDGET: usize = 16 * 1024 * 1024;

/// A JSON array of copies of `element`, as long as fits `BUDGET` bytes.
fn full_piece(element: &str) -> String {
    let copies = (BUDGET - 1) / (element.len() + 1);
    let mut text = String::with_capacity(BUDGET);
    text.push('[');
    for copy in 0..copies {
        if copy > 0 {
            text.push(',');
        }
        text.push_str(element);
    }
    text.push(']');
    assert!(text.len() <= BUDGET && text.len() + element.len() + 1 > BUDGET);
    text
}

/// One-digit numbers under one-letter keys, and in an array, take the most stored jsonb bytes
/// for their text. A full piece of each is accepted, under PostgreSQL's 268,435,455-byte limit
/// on a jsonb array's elements.
#[tokio::test]
async fn a_full_piece_of_the_densest_rows_is_accepted() -> Result<()> {
    let (database, pool) = database().await?;
    let digits = format!("{{\"a\":[{}]}}", ["1"; 4_096].join(","));
    for element in [
        "{\"a\":1}",
        "{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5,\"f\":6,\"g\":7,\"h\":8}",
        digits.as_str(),
    ] {
        let stored: i32 = sqlx::query_scalar("SELECT pg_column_size($1::jsonb)")
            .bind(full_piece(element))
            .fetch_one(&pool)
            .await?;
        assert!(
            i64::from(stored) < 268_435_455,
            "{stored} stored bytes for {element}"
        );
    }
    database.cleanup().await
}
