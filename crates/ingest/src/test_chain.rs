//! A synthetic chain served over a counting JSON-RPC transport.
//!
//! The ingest fetch path is judged on two things at once: the facts it stores and the
//! requests it spends getting them. This double serves a small deterministic chain, answers
//! every method the provider can issue, and counts them by method, so a test can assert
//! both. Individual faults are injected through [`Tamper`] to prove that each cross-check
//! actually fires, and with the right severity.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use alloy_primitives::{Bloom, BloomInput, hex};
use anyhow::Result as AnyResult;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::provider::{ChainProvider, ResolvedBlock};

pub(crate) const WATCHED_ADDRESS: &str = "0x1111111111111111111111111111111111111111";
pub(crate) const NOISE_ADDRESS: &str = "0x2222222222222222222222222222222222222222";
pub(crate) const WATCHED_TOPIC: &str =
    "0xaaaa000000000000000000000000000000000000000000000000000000000001";
pub(crate) const NOISE_TOPIC: &str =
    "0xbbbb000000000000000000000000000000000000000000000000000000000002";

/// One fault to inject, so a test can prove a single cross-check fires on its own.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum Tamper {
    #[default]
    None,
    /// A block header whose bloom omits the watched log's address.
    BloomOmitsWatchedAddress(i64),
    /// A receipt whose copy of the watched log carries different data.
    ReceiptLogDiffers(i64),
    /// A receipt log the range query never reported, though the filter admits it.
    RangeQueryDropsWatchedLog(i64),
    /// A receipt that claims a block this window did not resolve.
    ReceiptBlockHashMoved(i64),
    /// A receipt the provider no longer has.
    NullReceipt(i64),
    /// A transaction the provider no longer has.
    NullTransaction(i64),
}

#[derive(Clone, Debug)]
pub(crate) struct TestLog {
    pub(crate) log_index: i64,
    pub(crate) address: String,
    pub(crate) topics: Vec<String>,
    pub(crate) data: String,
}

#[derive(Clone, Debug)]
pub(crate) struct TestTransaction {
    pub(crate) hash: String,
    pub(crate) index: i64,
    pub(crate) logs: Vec<TestLog>,
}

#[derive(Clone, Debug)]
pub(crate) struct TestBlock {
    pub(crate) number: i64,
    pub(crate) hash: String,
    pub(crate) parent_hash: String,
    pub(crate) transactions: Vec<TestTransaction>,
}

impl TestBlock {
    fn logs(&self) -> impl Iterator<Item = (&TestTransaction, &TestLog)> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.logs.iter().map(move |log| (transaction, log)))
    }

    fn bloom(&self, omit_watched_address: bool) -> String {
        let mut bloom = Bloom::ZERO;
        for (_, log) in self.logs() {
            if !(omit_watched_address && log.address == WATCHED_ADDRESS) {
                bloom.accrue(BloomInput::Raw(&decode_hex(&log.address)));
            }
            for topic in &log.topics {
                bloom.accrue(BloomInput::Raw(&decode_hex(topic)));
            }
        }
        hex::encode_prefixed(bloom.as_slice())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TestChain {
    pub(crate) blocks: Vec<TestBlock>,
}

impl TestChain {
    /// A chain of `count` blocks from `first`, where every `logged_every`-th block carries
    /// a watched transaction alongside unwatched noise.
    pub(crate) fn synthetic(first: i64, count: i64, logged_every: i64) -> Self {
        let blocks = (first..first + count)
            .map(|number| {
                let mut transactions = vec![TestTransaction {
                    hash: hash_of("noise", number),
                    index: 0,
                    logs: vec![TestLog {
                        log_index: 0,
                        address: NOISE_ADDRESS.to_owned(),
                        topics: vec![NOISE_TOPIC.to_owned()],
                        data: "0x00".to_owned(),
                    }],
                }];
                if number % logged_every == 0 {
                    transactions.push(TestTransaction {
                        hash: hash_of("watched", number),
                        index: 1,
                        logs: vec![
                            TestLog {
                                log_index: 1,
                                address: WATCHED_ADDRESS.to_owned(),
                                topics: vec![WATCHED_TOPIC.to_owned(), hash_of("arg", number)],
                                data: format!("0x{number:064x}"),
                            },
                            TestLog {
                                log_index: 2,
                                address: NOISE_ADDRESS.to_owned(),
                                topics: vec![NOISE_TOPIC.to_owned()],
                                data: "0x11".to_owned(),
                            },
                            // A second watched log in the same transaction: dropping it
                            // from a range query still leaves the transaction selected,
                            // which is the only way a completeness gap can be observed.
                            TestLog {
                                log_index: 3,
                                address: WATCHED_ADDRESS.to_owned(),
                                topics: vec![WATCHED_TOPIC.to_owned(), hash_of("tail", number)],
                                data: format!("0x{:064x}", number + 1),
                            },
                        ],
                    });
                }
                TestBlock {
                    number,
                    hash: hash_of("block", number),
                    parent_hash: hash_of("block", number - 1),
                    transactions,
                }
            })
            .collect();
        Self { blocks }
    }

    pub(crate) fn resolved(&self) -> Vec<ResolvedBlock> {
        self.blocks
            .iter()
            .map(|block| ResolvedBlock {
                number: block.number,
                hash: block.hash.clone(),
            })
            .collect()
    }

    fn block(&self, number: i64) -> Option<&TestBlock> {
        self.blocks.iter().find(|block| block.number == number)
    }

    fn block_by_hash(&self, hash: &str) -> Option<&TestBlock> {
        self.blocks.iter().find(|block| block.hash == hash)
    }

    fn transaction(&self, hash: &str) -> Option<(&TestBlock, &TestTransaction)> {
        self.blocks.iter().find_map(|block| {
            block
                .transactions
                .iter()
                .find(|transaction| transaction.hash == hash)
                .map(|transaction| (block, transaction))
        })
    }
}

/// Counts requests by method, distinguishing the heavy variants ingest must stop issuing.
#[derive(Default)]
pub(crate) struct RequestCounts(Mutex<BTreeMap<String, usize>>);

impl RequestCounts {
    fn record(&self, label: &str) {
        *self
            .0
            .lock()
            .expect("counter lock")
            .entry(label.to_owned())
            .or_default() += 1;
    }

    pub(crate) fn get(&self, label: &str) -> usize {
        self.0
            .lock()
            .expect("counter lock")
            .get(label)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<String, usize> {
        self.0.lock().expect("counter lock").clone()
    }
}

pub(crate) struct TestNode {
    chain: TestChain,
    tamper: Tamper,
    pub(crate) counts: Arc<RequestCounts>,
}

impl TestNode {
    fn respond(&self, call: &Value) -> Value {
        let id = call.get("id").cloned().unwrap_or_else(|| json!(1));
        let method = call
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = call
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let result = match method {
            "eth_getBlockByNumber" => {
                self.counts.record("eth_getBlockByNumber");
                self.block_by_number(&params)
            }
            "eth_getBlockByHash" => {
                let full = params.get(1).and_then(Value::as_bool).unwrap_or_default();
                self.counts.record(if full {
                    BLOCK_BODY
                } else {
                    "eth_getBlockByHash"
                });
                self.block_by_hash(&params, full)
            }
            "eth_getLogs" => {
                let filter = params.first().cloned().unwrap_or(Value::Null);
                self.counts.record(if filter.get("blockHash").is_some() {
                    EXACT_BLOCK_LOGS
                } else {
                    "eth_getLogs"
                });
                self.logs(&filter)
            }
            "eth_getBlockReceipts" => {
                self.counts.record(BLOCK_RECEIPTS);
                self.block_receipts(&params)
            }
            "eth_getTransactionReceipt" => {
                self.counts.record("eth_getTransactionReceipt");
                self.transaction_receipt(&params)
            }
            "eth_getTransactionByHash" => {
                self.counts.record("eth_getTransactionByHash");
                self.transaction(&params)
            }
            _ => Value::Null,
        };
        json!({ "jsonrpc": "2.0", "id": id, "result": result })
    }

    fn block_by_number(&self, params: &[Value]) -> Value {
        let number = params
            .first()
            .and_then(Value::as_str)
            .and_then(parse_quantity);
        number
            .and_then(|number| self.chain.block(number))
            .map_or(Value::Null, |block| self.header(block, false))
    }

    fn block_by_hash(&self, params: &[Value], full: bool) -> Value {
        params
            .first()
            .and_then(Value::as_str)
            .and_then(|hash| self.chain.block_by_hash(hash))
            .map_or(Value::Null, |block| self.header(block, full))
    }

    fn header(&self, block: &TestBlock, full: bool) -> Value {
        let omit = self.tamper == Tamper::BloomOmitsWatchedAddress(block.number);
        let mut header = json!({
            "hash": block.hash,
            "parentHash": block.parent_hash,
            "number": format!("0x{:x}", block.number),
            "timestamp": format!("0x{:x}", 1_700_000_000 + block.number),
            "logsBloom": block.bloom(omit),
        });
        if full {
            header["transactions"] = Value::Array(
                block
                    .transactions
                    .iter()
                    .map(|transaction| self.transaction_value(block, transaction))
                    .collect(),
            );
        }
        header
    }

    fn transaction_value(&self, block: &TestBlock, transaction: &TestTransaction) -> Value {
        json!({
            "hash": transaction.hash,
            "blockHash": block.hash,
            "blockNumber": format!("0x{:x}", block.number),
            "transactionIndex": format!("0x{:x}", transaction.index),
            "from": NOISE_ADDRESS,
            "to": WATCHED_ADDRESS,
            "input": format!("0x{:08x}", block.number),
            "value": "0x0",
        })
    }

    fn log_value(&self, block: &TestBlock, transaction: &TestTransaction, log: &TestLog) -> Value {
        json!({
            "blockHash": block.hash,
            "blockNumber": format!("0x{:x}", block.number),
            "transactionHash": transaction.hash,
            "transactionIndex": format!("0x{:x}", transaction.index),
            "logIndex": format!("0x{:x}", log.log_index),
            "address": log.address,
            "topics": log.topics,
            "data": log.data,
        })
    }

    fn receipt_value(&self, block: &TestBlock, transaction: &TestTransaction) -> Value {
        let block_hash = if self.tamper == Tamper::ReceiptBlockHashMoved(block.number) {
            hash_of("reorged", block.number)
        } else {
            block.hash.clone()
        };
        let logs = transaction
            .logs
            .iter()
            .map(|log| {
                let mut value = self.log_value(block, transaction, log);
                value["blockHash"] = json!(block_hash);
                if self.tamper == Tamper::ReceiptLogDiffers(block.number)
                    && log.address == WATCHED_ADDRESS
                {
                    value["data"] = json!("0xdeadbeef");
                }
                value
            })
            .collect::<Vec<_>>();
        let mut bloom = Bloom::ZERO;
        for log in &transaction.logs {
            bloom.accrue(BloomInput::Raw(&decode_hex(&log.address)));
            for topic in &log.topics {
                bloom.accrue(BloomInput::Raw(&decode_hex(topic)));
            }
        }
        json!({
            "transactionHash": transaction.hash,
            "blockHash": block_hash,
            "blockNumber": format!("0x{:x}", block.number),
            "transactionIndex": format!("0x{:x}", transaction.index),
            "contractAddress": Value::Null,
            "status": "0x1",
            "cumulativeGasUsed": "0x5208",
            "gasUsed": "0x5208",
            "logsBloom": hex::encode_prefixed(bloom.as_slice()),
            "logs": logs,
        })
    }

    fn block_receipts(&self, params: &[Value]) -> Value {
        params
            .first()
            .and_then(Value::as_str)
            .and_then(|hash| self.chain.block_by_hash(hash))
            .map_or(Value::Null, |block| {
                Value::Array(
                    block
                        .transactions
                        .iter()
                        .map(|transaction| self.receipt_value(block, transaction))
                        .collect(),
                )
            })
    }

    fn transaction_receipt(&self, params: &[Value]) -> Value {
        let Some((block, transaction)) = params
            .first()
            .and_then(Value::as_str)
            .and_then(|hash| self.chain.transaction(hash))
        else {
            return Value::Null;
        };
        if self.tamper == Tamper::NullReceipt(block.number) && transaction.index == 1 {
            return Value::Null;
        }
        self.receipt_value(block, transaction)
    }

    fn transaction(&self, params: &[Value]) -> Value {
        let Some((block, transaction)) = params
            .first()
            .and_then(Value::as_str)
            .and_then(|hash| self.chain.transaction(hash))
        else {
            return Value::Null;
        };
        if self.tamper == Tamper::NullTransaction(block.number) && transaction.index == 1 {
            return Value::Null;
        }
        self.transaction_value(block, transaction)
    }

    fn logs(&self, filter: &Value) -> Value {
        let addresses = filter
            .get("address")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_ascii_lowercase)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let topic0s = filter
            .pointer("/topics/0")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_ascii_lowercase)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let blocks = if let Some(hash) = filter.get("blockHash").and_then(Value::as_str) {
            self.chain
                .block_by_hash(hash)
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            let from = filter
                .get("fromBlock")
                .and_then(Value::as_str)
                .and_then(parse_quantity)
                .unwrap_or(i64::MIN);
            let to = filter
                .get("toBlock")
                .and_then(Value::as_str)
                .and_then(parse_quantity)
                .unwrap_or(i64::MAX);
            self.chain
                .blocks
                .iter()
                .filter(|block| (from..=to).contains(&block.number))
                .collect()
        };
        let exact_block = filter.get("blockHash").is_some();
        let mut values = Vec::new();
        for block in blocks {
            let dropping = self.tamper == Tamper::RangeQueryDropsWatchedLog(block.number);
            for (transaction, log) in block.logs() {
                let matches = exact_block
                    || ((addresses.is_empty()
                        || addresses.contains(&log.address.to_ascii_lowercase()))
                        && (topic0s.is_empty()
                            || log.topics.first().is_some_and(|topic| {
                                topic0s.contains(&topic.to_ascii_lowercase())
                            })));
                // Only the trailing watched log is dropped, so the transaction is still
                // selected and its receipt still exposes the gap.
                let dropped = dropping && !exact_block && log.log_index == 3;
                if matches && !dropped {
                    values.push(self.log_value(block, transaction, log));
                }
            }
        }
        Value::Array(values)
    }
}

/// Label for the whole-block body fetch the per-transaction path must never issue.
pub(crate) const BLOCK_BODY: &str = "eth_getBlockByHash(full)";
/// Label for the whole-block receipt fetch the per-transaction path must never issue.
pub(crate) const BLOCK_RECEIPTS: &str = "eth_getBlockReceipts";
/// Label for the exact-block log fetch the per-transaction path must never issue.
pub(crate) const EXACT_BLOCK_LOGS: &str = "eth_getLogs(blockHash)";

pub(crate) struct TestEndpoint {
    pub(crate) provider: ChainProvider,
    pub(crate) counts: Arc<RequestCounts>,
}

/// Serves `chain` over HTTP JSON-RPC until the test process ends.
pub(crate) async fn serve(chain: TestChain, tamper: Tamper) -> AnyResult<TestEndpoint> {
    let counts = Arc::new(RequestCounts::default());
    let node = Arc::new(TestNode {
        chain,
        tamper,
        counts: Arc::clone(&counts),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let node = Arc::clone(&node);
            tokio::spawn(async move {
                while let Some(body) = read_request_body(&mut socket).await {
                    let response =
                        serde_json::from_str::<Value>(&body).map_or(Value::Null, |request| {
                            match request {
                                Value::Array(calls) => Value::Array(
                                    calls.iter().map(|call| node.respond(call)).collect(),
                                ),
                                single => node.respond(&single),
                            }
                        });
                    let payload = response.to_string();
                    let http = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\n\r\n{payload}",
                        payload.len()
                    );
                    if socket.write_all(http.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    Ok(TestEndpoint {
        provider: ChainProvider::new("test-chain", "rpc", &endpoint)?,
        counts,
    })
}

async fn read_request_body(socket: &mut tokio::net::TcpStream) -> Option<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let headers_end = find_headers_end(&buffer);
        if let Some(headers_end) = headers_end {
            let length = content_length(&buffer[..headers_end])?;
            if buffer.len() >= headers_end + length {
                return String::from_utf8(buffer[headers_end..headers_end + length].to_vec()).ok();
            }
        }
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

fn find_headers_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

fn content_length(headers: &[u8]) -> Option<usize> {
    for line in String::from_utf8_lossy(headers).lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            return value.trim().parse().ok();
        }
    }
    None
}

fn parse_quantity(value: &str) -> Option<i64> {
    i64::from_str_radix(value.strip_prefix("0x")?, 16).ok()
}

pub(crate) fn hash_of(label: &str, number: i64) -> String {
    let tag = label.bytes().map(u64::from).sum::<u64>();
    format!("0x{tag:032x}{number:032x}")
}

fn decode_hex(value: &str) -> Vec<u8> {
    hex::decode(value).expect("test fixture hex")
}
