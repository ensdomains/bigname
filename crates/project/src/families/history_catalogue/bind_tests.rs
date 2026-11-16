//! The catalogue's journal and its source lookup bind in pieces and keep their results.
use std::collections::BTreeSet;

use anyhow::Result;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    super::{
        bind_log,
        bind_tests::{PIECE, assert_pieces, open_block},
        guard_tests::{CHAIN, database},
        tables::HISTORY_ANCHOR,
    },
    prepare, write,
};

fn anchor(i: usize, mask: i64) -> Value {
    json!({
        "chain_id": CHAIN,
        "address": format!("0x{i:040x}"),
        "namespace": "ens",
        "anchor_kind": 0,
        "anchor_id": format!("ens:a{i:03}.eth"),
        "current_mask": mask,
        "historical_mask": 0,
        "current_resource_id": null,
        "first_bucket": null,
        "last_bucket": null,
        "bucket_range": "empty",
        "event_mask": 0,
        "key_bloom": "0".repeat(256),
    })
}

/// What two catalogue writes of the same 300 anchors on one block leave.
struct Anchors {
    counts: Vec<(u64, u64)>,
    journal: Vec<(String, Option<Value>)>,
    masks: Vec<i16>,
    binds: Vec<bind_log::Bind>,
}

/// Write 300 new anchors, then change all of them, on one block, as membership and then the
/// source envelopes do.
async fn anchors(budget: Option<usize>) -> Result<Anchors> {
    bind_log::set_budget(budget);
    let (database, pool) = database().await?;
    let mut opened = open_block(&pool, 10).await?;
    let first: Vec<Value> = (0..300).map(|i| anchor(i, 1)).collect();
    let second: Vec<Value> = (0..300).map(|i| anchor(i, 2)).collect();
    let (transaction, block) = (&mut opened.transaction, &opened.block);
    bind_log::start();
    let counts = vec![
        write::replace(
            transaction,
            CHAIN,
            block,
            &HISTORY_ANCHOR,
            Vec::new(),
            first.clone(),
        )
        .await?,
        write::replace(transaction, CHAIN, block, &HISTORY_ANCHOR, first, second).await?,
    ];
    let binds = bind_log::take();
    let journal: Vec<(String, Option<Value>)> = sqlx::query_as(
        "SELECT key, before_image FROM project_family_undo
         WHERE chain_id = $1 AND family = 'project_address_history_anchor' ORDER BY key",
    )
    .bind(CHAIN)
    .fetch_all(&mut **transaction)
    .await?;
    let masks: Vec<i16> = sqlx::query_scalar(
        "SELECT current_mask FROM project_address_history_anchor
         WHERE chain_id = $1 ORDER BY anchor_id",
    )
    .bind(CHAIN)
    .fetch_all(&mut **transaction)
    .await?;
    drop(opened);
    bind_log::set_budget(None);
    database.cleanup().await?;
    Ok(Anchors {
        counts,
        journal,
        masks,
        binds,
    })
}

#[tokio::test]
async fn catalogue_rows_journalled_in_pieces_keep_their_first_image() -> Result<()> {
    let whole = anchors(None).await?;
    let pieces = anchors(Some(PIECE)).await?;
    assert_eq!(
        pieces.counts,
        [(300_u64, 300_u64), (300, 0)],
        "the second write journals nothing new"
    );
    assert_eq!(pieces.counts, whole.counts);
    assert_eq!(pieces.journal, whole.journal, "the same journal");
    assert_eq!(pieces.journal.len(), 300);
    assert!(
        pieces.journal.iter().all(|(_, image)| image.is_none()),
        "every anchor keeps the image from before its first change"
    );
    assert_eq!(pieces.masks, [2_i16; 300], "the second write's rows stand");
    assert_eq!(pieces.masks, whole.masks);
    assert_pieces(&pieces.binds);
    let journals = |binds: &[bind_log::Bind]| {
        binds
            .iter()
            .filter(|bind| bind.tag == "history.insert_journal")
            .count()
    };
    assert!(
        journals(&pieces.binds) > journals(&whole.binds),
        "the second journal is bound in pieces"
    );
    Ok(())
}

/// Find the resources of 2,500 changed sources in one call, two of every three with an edge.
async fn source_resources(
    budget: Option<usize>,
) -> Result<(BTreeSet<Uuid>, BTreeSet<Uuid>, Vec<bind_log::Bind>)> {
    bind_log::set_budget(budget);
    let (database, pool) = database().await?;
    sqlx::query(
        "INSERT INTO project_history_source_edge (chain_id, resource_id, source_kind, source_key,
             pointer_event_identity, pointer_resolver, node)
         SELECT $1, md5('resource-' || i)::uuid, 2, 'key-' || i, 'pointer-' || i, '', '0x' || i
         FROM generate_series(0, 2499) i WHERE i % 3 <> 0",
    )
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    let sources: Vec<Value> = (0..2_500)
        .map(|i| {
            json!({"source_kind": 2, "source_key": format!("key-{i}"), "resolver_address": "",
                   "key_bloom": "0".repeat(256)})
        })
        .collect();
    let mut opened = open_block(&pool, 10).await?;
    bind_log::start();
    let found = prepare::dependent_resources(&mut opened.transaction, CHAIN, sources)
        .await?
        .into_iter()
        .collect();
    let binds = bind_log::take();
    let edges: Vec<Uuid> =
        sqlx::query_scalar("SELECT resource_id FROM project_history_source_edge")
            .fetch_all(&mut *opened.transaction)
            .await?;
    drop(opened);
    bind_log::set_budget(None);
    database.cleanup().await?;
    Ok((found, edges.into_iter().collect(), binds))
}

#[tokio::test]
async fn source_resources_found_in_pieces_are_the_same_set() -> Result<()> {
    let (whole, _, _) = source_resources(None).await?;
    let (pieces, edges, binds) = source_resources(Some(PIECE)).await?;
    assert_eq!(pieces, whole, "the same resources");
    assert_eq!(pieces, edges, "every edge's resource");
    assert_eq!(pieces.len(), 1_666);
    assert!(binds.len() > 1, "the sources are bound in pieces");
    assert_pieces(&binds);
    Ok(())
}
