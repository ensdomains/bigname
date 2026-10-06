use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use alloy_primitives::{Address, B256, LogData, keccak256};
use alloy_sol_types::SolEvent;
use anyhow::{Result, ensure};
use bigname_manifests::ManifestRepository;
use serde::{Deserialize, Serialize};

use super::manifests;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RawLog {
    pub(super) emitter: String,
    pub(super) topics: Vec<String>,
    pub(super) data_hex: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RawTransaction {
    pub(super) block: i64,
    pub(super) transaction_index: i64,
    pub(super) first_log_index: i64,
    pub(super) from: String,
    pub(super) to: String,
    pub(super) logs: Vec<RawLog>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(super) struct RawCounts {
    pub(super) transactions: u64,
    pub(super) logs: u64,
    pub(super) by_signature: BTreeMap<String, u64>,
    pub(super) by_emitter: BTreeMap<String, u64>,
    pub(super) first_block: i64,
    pub(super) last_block: i64,
    pub(super) raw_jsonl_bytes: u64,
    /// h[0]=zero; h[n+1]=keccak256(h[n] || exact JSONL bytes including newline).
    pub(super) rolling_keccak256: String,
}

pub(super) struct Writer<'a> {
    output: BufWriter<File>,
    repository: &'a ManifestRepository,
    validated: BTreeSet<(String, String)>,
    digest: B256,
    pub(super) counts: RawCounts,
    block: i64,
    index: i64,
    next_log_index: i64,
}

impl<'a> Writer<'a> {
    pub(super) fn new(path: &Path, repository: &'a ManifestRepository) -> Result<Self> {
        Ok(Self {
            output: BufWriter::new(File::create_new(path)?),
            repository,
            validated: BTreeSet::new(),
            digest: B256::ZERO,
            counts: RawCounts::default(),
            block: 0,
            index: 0,
            next_log_index: 0,
        })
    }

    pub(super) fn log<E: SolEvent>(&mut self, role: &str, event: E) -> Result<RawLog> {
        self.encoded(role, event.encode_log_data())
    }

    fn encoded(&mut self, role: &str, event: LogData) -> Result<RawLog> {
        let emitter = manifests::address(role);
        let topics = event
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        let topic = topics.first().expect("fixture events are not anonymous");
        let key = (format!("{emitter:#x}"), topic.clone());
        if self.validated.insert(key) {
            manifests::require_event(self.repository, emitter, topic, topics.len())?;
        }
        Ok(RawLog {
            emitter: format!("{emitter:#x}"),
            topics,
            data_hex: alloy_primitives::hex::encode(event.data),
        })
    }

    /// At most 32 transactions in ordinary synthetic blocks. The structural
    /// epoch explicitly chooses 512-NewOwner blocks with `transaction_at`.
    pub(super) fn transaction(
        &mut self,
        from: Address,
        to: Address,
        logs: Vec<RawLog>,
    ) -> Result<()> {
        let block = if self.index >= 32 {
            self.block + 1
        } else {
            self.block
        };
        self.transaction_at(block, from, to, logs)
    }

    pub(super) fn begin_epoch(&mut self, first_block: i64) -> Result<()> {
        ensure!(first_block > self.block, "fixture epochs must advance");
        self.block = first_block;
        self.index = 0;
        self.next_log_index = 0;
        Ok(())
    }

    pub(super) fn transaction_at(
        &mut self,
        block: i64,
        from: Address,
        to: Address,
        logs: Vec<RawLog>,
    ) -> Result<()> {
        ensure!(
            block >= self.block && block > 0 && !logs.is_empty(),
            "invalid fixture transaction"
        );
        if block != self.block {
            self.index = 0;
            self.next_log_index = 0;
            self.block = block;
        }
        for log in &logs {
            *self
                .counts
                .by_signature
                .entry(log.topics[0].clone())
                .or_default() += 1;
            *self
                .counts
                .by_emitter
                .entry(log.emitter.clone())
                .or_default() += 1;
        }
        self.counts.logs += logs.len() as u64;
        self.counts.transactions += 1;
        if self.counts.first_block == 0 {
            self.counts.first_block = block;
        }
        self.counts.last_block = block;
        let first_log_index = self.next_log_index;
        self.next_log_index += logs.len() as i64;
        let transaction = RawTransaction {
            block,
            transaction_index: self.index,
            first_log_index,
            from: format!("{from:#x}"),
            to: format!("{to:#x}"),
            logs,
        };
        let mut bytes = serde_json::to_vec(&transaction)?;
        bytes.push(b'\n');
        let mut hash_input = Vec::with_capacity(32 + bytes.len());
        hash_input.extend_from_slice(self.digest.as_slice());
        hash_input.extend_from_slice(&bytes);
        self.digest = keccak256(hash_input);
        self.counts.raw_jsonl_bytes += bytes.len() as u64;
        self.output.write_all(&bytes)?;
        self.index += 1;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<RawCounts> {
        self.output.flush()?;
        self.output.get_ref().sync_all()?;
        self.counts.rolling_keccak256 = format!("{:#x}", self.digest);
        Ok(self.counts)
    }
}

pub(super) fn owner(ordinal: u32) -> Address {
    let mut bytes = [0_u8; 20];
    bytes[0] = 0x11;
    bytes[16..].copy_from_slice(&(ordinal % 100 + 1).to_be_bytes());
    Address::from(bytes)
}

pub(super) fn alternate_owner() -> Address {
    Address::repeat_byte(0x22)
}
