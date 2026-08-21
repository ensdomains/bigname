use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry};
use serde_json::Value;

#[derive(Default)]
struct RawQueryScript {
    responses: VecDeque<Vec<Value>>,
    queries: Vec<String>,
}

type SharedScript = Arc<Mutex<RawQueryScript>>;

static RAW_QUERIES: ScopedTestHookRegistry<String, SharedScript> = ScopedTestHookRegistry::new();

pub(crate) struct RawQueryScriptGuard {
    _registration: ScopedTestHookGuard<String, SharedScript>,
    script: SharedScript,
}

impl RawQueryScriptGuard {
    pub(crate) fn queries(&self) -> Vec<String> {
        self.script
            .lock()
            .expect("scripted Coinbase SQL raw-query lock")
            .queries
            .clone()
    }
}

pub(crate) fn install(
    endpoint: &str,
    responses: impl IntoIterator<Item = Vec<Value>>,
) -> RawQueryScriptGuard {
    let script = Arc::new(Mutex::new(RawQueryScript {
        responses: responses.into_iter().collect(),
        queries: Vec::new(),
    }));
    RawQueryScriptGuard {
        _registration: RAW_QUERIES.install(endpoint.to_owned(), Arc::clone(&script)),
        script,
    }
}

pub(crate) fn installed(endpoint: &str) -> bool {
    RAW_QUERIES.get_cloned(&endpoint.to_owned()).is_some()
}

pub(crate) fn response(endpoint: &str, sql: &str) -> Option<Vec<Value>> {
    let script = RAW_QUERIES.get_cloned(&endpoint.to_owned())?;
    let mut script = script.lock().expect("scripted Coinbase SQL raw-query lock");
    script.queries.push(sql.to_owned());
    Some(script.responses.pop_front().unwrap_or_else(|| {
        panic!("scripted Coinbase SQL raw-query responses exhausted for {endpoint}: {sql}")
    }))
}
