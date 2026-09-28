use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use alloy_primitives::Bytes;
use alloy_sol_types::SolValue;
use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use bigname_lookup::ChainRpcUrls;
use serde_json::{Value, json};

#[derive(Clone, Default)]
struct Responses {
    values: Arc<Mutex<BTreeMap<String, Option<String>>>>,
    calls: Arc<Mutex<Vec<(String, usize)>>>,
}

pub struct Rpc {
    endpoint: String,
    responses: Responses,
    server: tokio::task::JoinHandle<()>,
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
    pub fn calls(&self) -> Vec<(String, usize)> {
        self.responses.calls.lock().unwrap().clone()
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn respond(State(state): State<Responses>, Json(request): Json<Value>) -> Json<Value> {
    let hash = request["params"][1]["blockHash"]
        .as_str()
        .unwrap()
        .to_owned();
    let data = request["params"][0]["data"].as_str().unwrap();
    let count = data.matches("691f3431").count();
    state.calls.lock().unwrap().push((hash.clone(), count));
    let name = state.values.lock().unwrap().get(&hash).cloned().flatten();
    let Some(name) = name else {
        return Json(
            json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"fixture RPC failure"}}),
        );
    };
    let inner = (name,).abi_encode_params();
    let results = vec![(true, Bytes::from(inner)); count];
    let encoded = (results,).abi_encode_params();
    Json(
        json!({"jsonrpc":"2.0","id":request["id"],"result":format!("0x{}",alloy_primitives::hex::encode(encoded))}),
    )
}
