use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use alloy_primitives::{Address, Bytes};
use alloy_sol_types::SolValue;
use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use bigname_lookup::ChainRpcUrls;
use serde_json::{Value, json};

/// The fixture endpoint. A block answers every call with one name (`answer(block, Some(..))`) or
/// fails every aggregate sent at it, as an endpoint without that block's state does
/// (`answer(block, None)`, the default). The faults below shape single aggregates and calls at
/// the blocks that do answer.
#[derive(Clone, Default)]
struct Responses {
    values: Arc<Mutex<BTreeMap<String, Option<String>>>>,
    calls: Arc<Mutex<Vec<(String, usize)>>>,
    probes: Arc<Mutex<usize>>,
    before_reply: Arc<Mutex<Option<(sqlx::PgPool, String)>>>,
    faults: Arc<Mutex<Faults>>,
}

#[derive(Clone, Default)]
struct Faults {
    /// An aggregate holding a call whose target or calldata contains one of these fails whole.
    poisoned: Vec<String>,
    /// A call whose target or calldata contains one of these fails inside an answered aggregate.
    failed_calls: Vec<String>,
    /// An aggregate of more calls than this fails whole.
    limit: Option<usize>,
    /// Every answer waits this long first.
    delay: Option<std::time::Duration>,
}

pub struct Rpc {
    endpoint: String,
    responses: Responses,
    server: tokio::task::JoinHandle<()>,
}

/// Lower-case hex without `0x`, as the fixture matches targets and calldata.
pub fn plain_hex(value: &str) -> String {
    value.trim_start_matches("0x").to_ascii_lowercase()
}

impl Rpc {
    pub async fn new() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/", listener.local_addr()?);
        let responses = Responses::default();
        let app = Router::new()
            .route("/", post(respond))
            .with_state(responses.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(Self {
            endpoint,
            responses,
            server,
        })
    }
    pub fn urls(&self) -> ChainRpcUrls {
        ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={}", self.endpoint)]).unwrap()
    }
    pub fn answer(&self, block: i64, name: Option<&str>) {
        self.responses
            .values
            .lock()
            .unwrap()
            .insert(super::hash(block), name.map(str::to_owned));
    }
    /// As [`Self::answer`], for a block that is not one of the fixture's own.
    #[allow(dead_code)]
    pub fn answer_hash(&self, hash: &str, name: Option<&str>) {
        self.responses
            .values
            .lock()
            .unwrap()
            .insert(hash.to_owned(), name.map(str::to_owned));
    }
    /// The hydration aggregates received, failed ones included: block hash and call count.
    pub fn calls(&self) -> Vec<(String, usize)> {
        self.responses.calls.lock().unwrap().clone()
    }
    /// The one-call aggregates Project sent to learn whether the endpoint serves a block.
    #[allow(dead_code)]
    pub fn probes(&self) -> usize {
        *self.responses.probes.lock().unwrap()
    }
    #[allow(dead_code)]
    pub fn poison(&self, hex: &str) {
        self.responses
            .faults
            .lock()
            .unwrap()
            .poisoned
            .push(plain_hex(hex));
    }
    #[allow(dead_code)]
    pub fn fail_call(&self, hex: &str) {
        self.responses
            .faults
            .lock()
            .unwrap()
            .failed_calls
            .push(plain_hex(hex));
    }
    #[allow(dead_code)]
    pub fn limit(&self, calls: Option<usize>) {
        self.responses.faults.lock().unwrap().limit = calls;
    }
    #[allow(dead_code)]
    pub fn delay(&self, delay: std::time::Duration) {
        self.responses.faults.lock().unwrap().delay = Some(delay);
    }
    #[allow(dead_code)]
    pub fn clear_faults(&self) {
        *self.responses.faults.lock().unwrap() = Faults::default();
    }
    #[allow(dead_code)] // The text integration binary shares this helper without its race hook.
    pub fn before_reply(&self, pool: &sqlx::PgPool, sql: &str) {
        *self.responses.before_reply.lock().unwrap() = Some((pool.clone(), sql.to_owned()));
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Make `block` the highest readable block of the fixture's mainnet lineage: the fixture blocks
/// above it leave the readable lineage and those up to it are on it. A block a test orphaned
/// itself is therefore readable again after this.
pub async fn head(pool: &sqlx::PgPool, block: i64) -> Result<()> {
    for (state, comparison, from) in [
        ("orphaned", ">", "canonical"),
        ("canonical", "<=", "orphaned"),
    ] {
        sqlx::query(&format!(
            "UPDATE chain_lineage SET canonicality_state = '{state}'
             WHERE chain_id = 'ethereum-mainnet' AND block_number {comparison} $1
               AND block_hash = '0x' || lpad(to_hex(block_number), 64, '0')
               AND canonicality_state = '{from}'"
        ))
        .bind(block)
        .execute(pool)
        .await?;
    }
    Ok(())
}

const MULTICALL3: &str = "ca11bde05977b3631167028862be2a173976ca11";

fn error(request: &Value, message: &str) -> Json<Value> {
    Json(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":message}}))
}

async fn respond(State(state): State<Responses>, Json(request): Json<Value>) -> Json<Value> {
    let hash = request["params"][1]["blockHash"]
        .as_str()
        .unwrap()
        .to_owned();
    let data = alloy_primitives::hex::decode(request["params"][0]["data"].as_str().unwrap())
        .expect("aggregate3 calldata");
    let (calls,) =
        <(Vec<(Address, bool, Bytes)>,)>::abi_decode_params(&data[4..]).expect("aggregate3 calls");
    let calls: Vec<String> = calls
        .iter()
        .map(|(target, _, calldata)| {
            format!(
                "{}{}",
                alloy_primitives::hex::encode(target),
                alloy_primitives::hex::encode(calldata)
            )
        })
        .collect();
    let probe = calls.len() == 1 && calls[0].starts_with(MULTICALL3);
    if probe {
        *state.probes.lock().unwrap() += 1;
    } else {
        state
            .calls
            .lock()
            .unwrap()
            .push((hash.clone(), calls.len()));
    }
    let hook = state.before_reply.lock().unwrap().take();
    if let Some((pool, statement)) = hook {
        // Fixture pools hold one connection. This also proves the prepare transaction was
        // released before RPC: the callback can use that connection before sending its reply.
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            sqlx::raw_sql(&statement).execute(&pool),
        )
        .await
        .expect("preparation must release its connection before RPC")
        .expect("fixture concurrent publication input change");
    }
    let name = state.values.lock().unwrap().get(&hash).cloned().flatten();
    let Some(name) = name else {
        return error(&request, "fixture RPC failure");
    };
    let faults = state.faults.lock().unwrap().clone();
    if let Some(delay) = faults.delay {
        tokio::time::sleep(delay).await;
    }
    let holds = |call: &String, patterns: &[String]| patterns.iter().any(|hex| call.contains(hex));
    if faults.limit.is_some_and(|limit| calls.len() > limit) {
        return error(&request, "fixture aggregate too large");
    }
    if calls.iter().any(|call| holds(call, &faults.poisoned)) {
        return error(&request, "fixture aggregate content failure");
    }
    let inner = (name,).abi_encode_params();
    let results: Vec<(bool, Bytes)> = calls
        .iter()
        .map(|call| {
            if holds(call, &faults.failed_calls) || (probe && call.starts_with(MULTICALL3)) {
                (false, Bytes::new())
            } else {
                (true, Bytes::from(inner.clone()))
            }
        })
        .collect();
    let encoded = (results,).abi_encode_params();
    Json(
        json!({"jsonrpc":"2.0","id":request["id"],"result":format!("0x{}",alloy_primitives::hex::encode(encoded))}),
    )
}
