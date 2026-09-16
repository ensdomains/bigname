use super::{
    RethDbProvider, RethDbReader,
    convert::{address_hex, hash_hex},
};
use crate::{
    fetching::{FetchedBatch, fetch_selected_facts},
    manifest::WatchFilter,
    provider::{ChainProvider, Log},
};
use alloy_primitives::{Address, B256};
use std::{
    collections::BTreeSet,
    sync::{Arc, OnceLock},
};

#[path = "payloads_fixture.rs"]
mod fixture;
use fixture::Fixture;

fn selected(f: &Fixture) -> Vec<Log> {
    f.reader
        .logs(
            &f.blocks,
            &[hash_hex(B256::repeat_byte(1))],
            &[address_hex(Address::repeat_byte(7))],
            &[],
        )
        .unwrap()
}
fn filter(f: &Fixture) -> WatchFilter {
    WatchFilter::watching(
        &address_hex(Address::repeat_byte(7)),
        0,
        f.blocks.len() as i64 - 1,
        &[hash_hex(B256::repeat_byte(1))],
    )
}
fn provider(f: &Fixture) -> ChainProvider {
    ChainProvider::RethDb(RethDbProvider {
        reader: Arc::new(RethDbReader {
            chain: f.reader.chain.clone(),
            datadir: f.reader.datadir.clone(),
            factory: OnceLock::from(Ok(f.reader.factory().unwrap())),
        }),
    })
}
fn reference(f: &Fixture, selected: &[Log]) -> FetchedBatch {
    let hashes = selected
        .iter()
        .map(|log| log.transaction_hash.as_str())
        .collect::<BTreeSet<_>>();
    let mut result = FetchedBatch {
        blocks: f.reader.headers(&f.blocks).unwrap(),
        ..Default::default()
    };
    for bundle in f.reader.bundles(&f.blocks).unwrap() {
        result.transactions.extend(
            bundle
                .transactions
                .into_iter()
                .filter(|tx| hashes.contains(tx.hash.as_str())),
        );
        result.receipts.extend(
            bundle
                .receipts
                .into_iter()
                .filter(|r| hashes.contains(r.transaction_hash.as_str())),
        );
        result.logs.extend(
            bundle
                .logs
                .into_iter()
                .filter(|log| hashes.contains(log.transaction_hash.as_str())),
        );
    }
    result
        .transactions
        .sort_by(|a, b| (&a.block_hash, &a.hash).cmp(&(&b.block_hash, &b.hash)));
    result.receipts.sort_by(|a, b| {
        (&a.block_hash, &a.transaction_hash).cmp(&(&b.block_hash, &b.transaction_hash))
    });
    result
}
fn equal(a: &FetchedBatch, b: &FetchedBatch) {
    assert_eq!(a.blocks, b.blocks);
    assert_eq!(a.transactions, b.transactions);
    assert_eq!(a.receipts, b.receipts);
    assert_eq!(a.logs, b.logs);
}

#[tokio::test]
async fn sparse_payloads_preserve_complete_selected_facts_and_sender_recovery() {
    let f = Fixture::new(3, 201, 1, true);
    let logs = selected(&f);
    assert!(
        f.reader
            .transaction_payloads(&f.blocks, &logs)
            .unwrap()
            .bundle_blocks
            .is_empty()
    );
    let actual = fetch_selected_facts(&provider(&f), &f.blocks, logs.clone(), &filter(&f))
        .await
        .unwrap();
    equal(&reference(&f, &logs), &actual);
    assert_eq!(actual.transactions.len(), 3);
    assert_eq!(actual.logs.len(), 9); // Includes the unrelated companion log of each receipt.
    assert!(
        actual
            .receipts
            .iter()
            .all(|r| r.gas_used.as_deref() == Some("21000"))
    );
    assert!(actual.logs.iter().all(|l| l.transaction_index == 200));
    assert!(actual.logs.iter().all(|l| l.log_index >= 200));
}

#[tokio::test]
async fn density_boundary_and_dense_fallback_preserve_facts() {
    for (transactions, count, sparse) in [
        (31, 1, false),
        (32, 1, true),
        (64, 2, true),
        (64, 3, false),
        (200, 200, false),
    ] {
        let f = Fixture::new(2, transactions, count, false);
        let logs = selected(&f);
        assert_eq!(
            f.reader
                .transaction_payloads(&f.blocks, &logs)
                .unwrap()
                .bundle_blocks
                .is_empty(),
            sparse
        );
        let actual = fetch_selected_facts(&provider(&f), &f.blocks, logs.clone(), &filter(&f))
            .await
            .unwrap();
        equal(&reference(&f, &logs), &actual);
    }
}

#[tokio::test]
async fn sparse_payloads_reject_incomplete_or_mismatched_facts() {
    let good = Fixture::new(1, 64, 1, false);
    let logs = selected(&good);
    for fault in ["receipt", "transaction", "gas"] {
        let f = Fixture::with_fault(1, 64, 1, false, Some(fault));
        assert!(
            fetch_selected_facts(&provider(&f), &f.blocks, logs.clone(), &filter(&f))
                .await
                .is_err(),
            "{fault}"
        );
    }
    let mut missing = logs.clone();
    missing.pop();
    assert!(
        fetch_selected_facts(&provider(&good), &good.blocks, missing, &filter(&good))
            .await
            .is_err()
    );
    let mut bad_hash = logs.clone();
    bad_hash[0].transaction_hash = hash_hex(B256::repeat_byte(254));
    assert!(
        fetch_selected_facts(&provider(&good), &good.blocks, bad_hash, &filter(&good))
            .await
            .is_err()
    );
    let mut bad_index = logs.clone();
    bad_index[0].transaction_index = 64;
    assert!(
        fetch_selected_facts(&provider(&good), &good.blocks, bad_index, &filter(&good))
            .await
            .is_err()
    );
    let mut bad_block = good.blocks.clone();
    bad_block[0].hash = hash_hex(B256::repeat_byte(254));
    assert!(
        fetch_selected_facts(&provider(&good), &bad_block, logs, &filter(&good))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn dense_fallback_still_rejects_logs_outside_the_resolved_window() {
    let f = Fixture::new(2, 32, 2, false);
    let mut logs = selected(&f);
    let mut outside = logs[0].clone();
    outside.block_hash = hash_hex(B256::repeat_byte(254));
    outside.block_number = 100;
    logs.push(outside);
    assert!(f.reader.transaction_payloads(&f.blocks, &logs).is_err());
    assert!(
        fetch_selected_facts(&provider(&f), &f.blocks, logs, &filter(&f))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mixed_density_window_preserves_all_facts_and_keeps_sparse_blocks_selective() {
    let selections = (0..256)
        .map(|block| [2, 2, 5, 15, 40, 1, 0, 200][block % 8])
        .collect::<Vec<_>>();
    let f = Fixture::mixed(200, &selections);
    let logs = selected(&f);
    let reads = f.reader.transaction_payloads(&f.blocks, &logs).unwrap();
    // 128 sparse blocks retain only 320 transaction payloads instead of 25,600;
    // the 96 busy blocks keep bundle reads, and 32 empty blocks need neither.
    assert_eq!(reads.transactions.len(), 320);
    assert_eq!(reads.bundle_blocks.len(), 96);
    assert!(
        reads
            .transactions
            .iter()
            .all(|tx| selections[tx.transaction.block_number as usize] <= 6)
    );
    assert!(
        reads
            .bundle_blocks
            .iter()
            .all(|block| selections[block.number as usize] > 6)
    );
    let actual = fetch_selected_facts(&provider(&f), &f.blocks, logs.clone(), &filter(&f))
        .await
        .unwrap();
    equal(&reference(&f, &logs), &actual);
    assert_eq!(actual.blocks.len(), 256);
    assert_eq!(actual.transactions.len(), selections.iter().sum::<usize>());
    assert_eq!(actual.logs.len(), actual.transactions.len() * 3);
}
