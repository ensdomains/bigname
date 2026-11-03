//! The work list's preimage observation arm: it derives again only the stored rows whose name
//! still spells an observed label as `[labelhash]`, found by `project_child_listing_unhealed_idx`.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

use super::{WORK_LIST, list};
use crate::families::{
    guard_tests::{CHAIN, database, executed_scans, hash, reads_through},
    input::BlockHeader,
};

const OBSERVED: &str = "ens:0x0b";
const HELD: &str = "ens:0x0c";
const UNRELATED: &str = "ens:0x0d";
const SPELLED: &str = "ens:0x0e";

fn block(number: i64) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash(number),
        predecessor_hash: Some(hash(number - 1)),
        timestamp_seconds: 1_800_000_000 + number * 12,
        timestamp: json!(null),
        journal: true,
    }
}

/// `x.eth` observed at block 12, with no raw bytes until then; an unspelled `.eth` name that
/// carries only `eth` of its labels; three stored rows: one whose name still spells `x` as a
/// placeholder, one whose name spells nothing but its own unknown label, and one fully spelled
/// whose ENSv1 edge carries `x`.
async fn seed(pool: &PgPool) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ('{OBSERVED}', 'ens', '0x0b', ARRAY['0xa1', '0xe7'], 'fixture', 'active',
                 '{CHAIN}', '{block0}', 0, 'canonical'),
                ('{UNRELATED}', 'ens', '0x0d', ARRAY['0xd1', '0xe7'], 'fixture', 'active',
                 '{CHAIN}', '{block0}', 0, 'canonical');
         INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, logical_name_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state)
         VALUES ('observed:x', 'ens', 'PreimageObserved', 'ens_v1_registry_l1', 1, '{CHAIN}',
                 '{OBSERVED}', 12, '{block12}', '0xtx12', 0, 0, 'raw_log_preimage_observation',
                 'canonical');
         INSERT INTO project_child_edge_candidate (chain_id, namespace, parent_node,
             child_node, authority_arm, block_number, transaction_index, log_index,
             event_identity, owner, labelhash, source_family)
         VALUES ('{CHAIN}', 'ens', '0xe7', '0x0e', 'ens_v1', 1, 0, 0, 'edge:spelled',
                 '0x00000000000000000000000000000000000000a1', '0xa1', 'ens_v1_registry_l1');
         INSERT INTO project_child_listing (chain_id, parent_logical_name_id,
             child_logical_name_id, namespace, display_key, display_name, namehash,
             lifecycle_shadow, wrapper_held, released_lease, expires_key, registered_key,
             unhealed_labelhashes)
         VALUES ('{CHAIN}', 'ens:0xf1', '{HELD}', 'ens', 'c.[a1].f', 'c.[a1].f', '0x0c',
                 FALSE, FALSE, FALSE, -1, -1, ARRAY['0xa1']),
                ('{CHAIN}', 'ens:0xe7', '{UNRELATED}', 'ens', '[d1].eth', '[d1].eth', '0x0d',
                 FALSE, FALSE, FALSE, -1, -1, ARRAY['0xd1']),
                ('{CHAIN}', 'ens:0xe7', '{SPELLED}', 'ens', 'x.eth', 'x.eth', '0x0e',
                 FALSE, FALSE, FALSE, -1, -1, '{{}}');
         ANALYZE project_child_listing;",
        block0 = hash(0),
        block12 = hash(12),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

// The heal fan-out rows (TYR-455): observing `x.eth` derives again the stored row that still
// spells `x` as `[labelhash]` and the observed name itself. It derives no unspelled `.eth` name
// that only shares `eth`, and no fully spelled row, even one whose edge carries `x`. The lookup
// reads the stored rows through the unhealed-label index, never by a scan of the family.
#[tokio::test]
async fn an_observation_derives_again_only_the_rows_that_spell_its_labels_as_unknown()
-> Result<()> {
    let (database, pool) = database().await?;
    seed(&pool).await?;
    let mut transaction = pool.begin().await?;
    let (children, parents) = list(&mut transaction, CHAIN, &block(12), 11, (&[], &[])).await?;
    ensure!(
        children == [OBSERVED, HELD] && parents.is_empty(),
        "the work list is {children:?} and parents {parents:?}"
    );

    raw_sql("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;
    let header = block(12);
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {WORK_LIST}"))
        .bind(CHAIN)
        .bind(header.number)
        .bind(11_i64)
        .bind(header.timestamp_seconds)
        .bind(Vec::<String>::new())
        .bind(Vec::<String>::new())
        .fetch_one(&mut *transaction)
        .await?;
    let mut scans = Vec::new();
    executed_scans(&plan[0]["Plan"], "", &mut scans);
    let listing: Vec<&Value> = scans
        .iter()
        .map(|(_, node)| *node)
        .filter(|node| node["Relation Name"] == "project_child_listing")
        .collect();
    ensure!(
        listing
            .iter()
            .any(|node| reads_through(node, &["project_child_listing_unhealed_idx"])),
        "no lookup of the unhealed-label index: {plan}"
    );
    ensure!(
        listing.iter().all(|node| node["Node Type"] != "Seq Scan"),
        "the work list scans the listing family: {plan}"
    );
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
