use super::super::{RethDbReader, convert::hash_hex};
use crate::provider::ResolvedBlock;
use alloy_consensus::transaction::SignerRecoverable as _;
use alloy_consensus::{Header, TxLegacy};
use alloy_primitives::{Address, B256, Bloom, Bytes, Log as EthLog, Signature, TxKind, U256};
use reth_ethereum::{
    Receipt, TransactionSigned,
    provider::{
        ProviderFactory, StaticFileSegment, StaticFileWriter,
        db::{
            Database, init_db, tables,
            transaction::{DbTx, DbTxMut},
        },
        providers::{RocksDBProvider, StaticFileProvider},
    },
};
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

pub(super) struct Fixture {
    pub reader: RethDbReader,
    pub blocks: Vec<ResolvedBlock>,
    path: PathBuf,
}

pub(super) fn event(address: u8, topic: u8, topic1: u8) -> EthLog {
    EthLog::new_unchecked(
        Address::repeat_byte(address),
        vec![B256::repeat_byte(topic), B256::repeat_byte(topic1)],
        Bytes::from(vec![topic; 48]),
    )
}

impl Fixture {
    // Real MDBX headers/body indices/senders plus transaction and receipt static files.
    // Writer APIs: (upstream: .refs/reth/crates/storage/provider/src/providers/static_file/writer.rs:L1208 @ reth@189c0df3).
    pub fn new(blocks: u64, transactions: usize, selected: usize, missing: bool) -> Self {
        Self::with_fault(blocks, transactions, selected, missing, None)
    }
    pub fn with_fault(
        blocks: u64,
        transactions: usize,
        selected: usize,
        missing: bool,
        fault: Option<&str>,
    ) -> Self {
        Self::build(
            blocks,
            transactions,
            &vec![selected; blocks as usize],
            missing,
            fault,
        )
    }
    pub fn mixed(transactions: usize, selected: &[usize]) -> Self {
        Self::build(selected.len() as u64, transactions, selected, false, None)
    }
    fn build(
        blocks: u64,
        transactions: usize,
        selections: &[usize],
        missing: bool,
        fault: Option<&str>,
    ) -> Self {
        let path = std::env::temp_dir().join(format!(
            "bigname-reth-payload-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(path.join("static_files")).unwrap();
        let db = init_db(path.join("db"), Default::default()).unwrap();
        let files = StaticFileProvider::<reth_ethereum::EthPrimitives>::read_write(
            path.join("static_files"),
        )
        .unwrap();
        let mut headers = files.get_writer(0, StaticFileSegment::Headers).unwrap();
        let mut txs = files
            .get_writer(0, StaticFileSegment::Transactions)
            .unwrap();
        let mut receipts = files.get_writer(0, StaticFileSegment::Receipts).unwrap();
        let write = db.tx_mut().unwrap();
        let mut resolved = Vec::new();
        let mut parent_hash = B256::ZERO;
        for number in 0..blocks {
            let selected = selections[number as usize];
            txs.increment_block(number).unwrap();
            receipts.increment_block(number).unwrap();
            let first = number * transactions as u64;
            write
                .put::<tables::BlockBodyIndices>(
                    number,
                    reth_ethereum::provider::db::models::StoredBlockBodyIndices {
                        first_tx_num: first,
                        tx_count: transactions as u64,
                    },
                )
                .unwrap();
            let mut bloom = Bloom::ZERO;
            for index in 0..transactions {
                let id = first + index as u64;
                let tx = TransactionSigned::new_unhashed(
                    TxLegacy {
                        nonce: id,
                        gas_limit: 100_000,
                        gas_price: 20_000_000_000,
                        to: if index % 7 == 0 {
                            TxKind::Create
                        } else {
                            TxKind::Call(Address::repeat_byte(19))
                        },
                        value: U256::from(id + 1),
                        input: Bytes::from(vec![(id % 251) as u8; 300]),
                        ..Default::default()
                    }
                    .into(),
                    Signature::new(U256::from(1), U256::from(2), false),
                );
                if !(fault == Some("transaction") && index + 1 == transactions) {
                    txs.append_transaction(id, &tx).unwrap();
                }
                if missing && index % 5 == 0 {
                    // Exercise signer recovery when the stored sender is absent.
                } else {
                    write
                        .put::<tables::TransactionSenders>(
                            id,
                            tx.recover_signer_unchecked().unwrap(),
                        )
                        .unwrap();
                }
                let is_selected = selected > 0
                    && (0..selected).any(|slot| {
                        index == ((slot + 1) * transactions / selected).saturating_sub(1)
                    });
                let mut logs = vec![event(7, 2, 99)];
                if is_selected {
                    logs.extend([event(7, 1, 10), event(7, 1, 10)]);
                }
                bloom.accrue_logs(&logs);
                if !(fault == Some("receipt") && index + 1 == transactions) {
                    receipts
                        .append_receipt(
                            id,
                            &Receipt {
                                success: index % 11 != 0,
                                cumulative_gas_used: if fault == Some("gas")
                                    && index + 1 == transactions
                                {
                                    1
                                } else {
                                    (index as u64 + 1) * 21_000
                                },
                                logs,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                }
            }
            let header = Header {
                number,
                parent_hash,
                timestamp: 1_600_000_000 + number * 12,
                gas_limit: 30_000_000,
                gas_used: transactions as u64 * 21_000,
                logs_bloom: bloom,
                ..Default::default()
            };
            let hash = header.hash_slow();
            headers.append_header(&header, &hash).unwrap();
            write.put::<tables::CanonicalHeaders>(number, hash).unwrap();
            write.put::<tables::HeaderNumbers>(hash, number).unwrap();
            resolved.push(ResolvedBlock {
                number: number as i64,
                hash: hash_hex(hash),
            });
            parent_hash = hash;
        }
        headers.commit().unwrap();
        txs.commit().unwrap();
        receipts.commit().unwrap();
        drop(headers);
        drop(txs);
        drop(receipts);
        write.commit().unwrap();
        let factory = ProviderFactory::new(
            db,
            reth_ethereum::chainspec::MAINNET.clone(),
            files,
            RocksDBProvider::builder(path.join("rocksdb"))
                .with_default_tables()
                .build()
                .unwrap(),
            reth_ethereum::tasks::Runtime::test(),
        )
        .unwrap();
        Self {
            reader: RethDbReader {
                chain: "ethereum-mainnet".into(),
                datadir: path.clone(),
                factory: OnceLock::from(Ok(Arc::new(factory))),
            },
            blocks: resolved,
            path,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.reader.factory.take();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
