use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const MAX_HTTP_REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// Observable fault classes supported by the local JSON-RPC proxy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    DropLogs,
    DelayTimeout,
    Truncate,
    ErrorOnce,
    DropReceipts,
    PrunedState,
}

/// One deterministic proxy mutation. Rules are evaluated in insertion order,
/// and at most one rule is applied to an HTTP response. This makes consecutive
/// retry faults reproducible even when the provider internally retries a call.
#[derive(Clone, Debug)]
pub struct FaultSpec {
    kind: FaultKind,
    remaining: Option<usize>,
    action: FaultAction,
}

#[derive(Clone, Debug)]
enum FaultAction {
    DropLogs {
        transaction_hash: String,
        count: usize,
    },
    DelayTimeout {
        transaction_hash: String,
        delay: Duration,
    },
    Truncate {
        transaction_hash: String,
        trailing_bytes: usize,
    },
    JsonRpcError {
        transaction_hash: String,
        code: i64,
        message: String,
    },
    GetCodeJsonRpcError {
        address: String,
        block_hash: String,
        code: i64,
        message: String,
    },
    DropReceipts {
        transaction_hash: String,
        count: usize,
    },
    PrunedCode {
        address: String,
        block_hash: String,
        requested_block_number: u64,
    },
}

impl FaultSpec {
    pub fn drop_logs_once(transaction_hash: impl Into<String>, count: usize) -> Self {
        Self::once(
            FaultKind::DropLogs,
            FaultAction::DropLogs {
                transaction_hash: normalize_hex(transaction_hash),
                count,
            },
        )
    }

    /// Delay a matching log response, then return HTTP 504. The response delay
    /// is deliberately caller-controlled so scenarios remain fast and stable.
    pub fn delay_timeout_once(transaction_hash: impl Into<String>, delay: Duration) -> Self {
        Self::once(
            FaultKind::DelayTimeout,
            FaultAction::DelayTimeout {
                transaction_hash: normalize_hex(transaction_hash),
                delay,
            },
        )
    }

    pub fn delay_timeout_until_cleared(
        transaction_hash: impl Into<String>,
        delay: Duration,
    ) -> Self {
        Self {
            kind: FaultKind::DelayTimeout,
            remaining: None,
            action: FaultAction::DelayTimeout {
                transaction_hash: normalize_hex(transaction_hash),
                delay,
            },
        }
    }

    pub fn truncate_once(transaction_hash: impl Into<String>, trailing_bytes: usize) -> Self {
        Self::once(
            FaultKind::Truncate,
            FaultAction::Truncate {
                transaction_hash: normalize_hex(transaction_hash),
                trailing_bytes,
            },
        )
    }

    pub fn error_once(
        transaction_hash: impl Into<String>,
        code: i64,
        message: impl Into<String>,
    ) -> Self {
        Self::once(
            FaultKind::ErrorOnce,
            FaultAction::JsonRpcError {
                transaction_hash: normalize_hex(transaction_hash),
                code,
                message: message.into(),
            },
        )
    }

    pub fn get_code_error_once(
        address: impl Into<String>,
        block_hash: impl Into<String>,
        code: i64,
        message: impl Into<String>,
    ) -> Self {
        Self::once(
            FaultKind::ErrorOnce,
            FaultAction::GetCodeJsonRpcError {
                address: normalize_hex(address),
                block_hash: normalize_hex(block_hash),
                code,
                message: message.into(),
            },
        )
    }

    pub fn drop_receipts_once(transaction_hash: impl Into<String>, count: usize) -> Self {
        Self::once(
            FaultKind::DropReceipts,
            FaultAction::DropReceipts {
                transaction_hash: normalize_hex(transaction_hash),
                count,
            },
        )
    }

    /// Return the exact JSON-RPC error recognized by the historical-state
    /// fallback path whenever the selected address and block hash are queried.
    pub fn pruned_get_code(
        address: impl Into<String>,
        block_hash: impl Into<String>,
        requested_block_number: u64,
    ) -> Self {
        Self {
            kind: FaultKind::PrunedState,
            remaining: None,
            action: FaultAction::PrunedCode {
                address: normalize_hex(address),
                block_hash: normalize_hex(block_hash),
                requested_block_number,
            },
        }
    }

    fn once(kind: FaultKind, action: FaultAction) -> Self {
        Self {
            kind,
            remaining: Some(1),
            action,
        }
    }
}

fn normalize_hex(value: impl Into<String>) -> String {
    value.into().to_ascii_lowercase()
}

#[derive(Debug)]
struct FaultRule {
    spec: FaultSpec,
    hits: usize,
}

#[derive(Clone, Debug)]
struct ObservedCall {
    method: String,
    params: Value,
}

#[derive(Default)]
struct ProxyState {
    rules: Mutex<Vec<FaultRule>>,
    calls: Mutex<Vec<ObservedCall>>,
    errors: Mutex<Vec<String>>,
    changed: tokio::sync::Notify,
}

/// A small in-process HTTP proxy used only by the e2e harness. Scenario driver
/// RPC stays connected directly to Anvil; only the production indexer receives
/// this proxy URL.
pub struct FaultProxy {
    pub url: String,
    state: Arc<ProxyState>,
    task: JoinHandle<()>,
}

impl FaultProxy {
    pub async fn spawn(upstream_url: &str) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind provider-fault proxy")?;
        let address = listener
            .local_addr()
            .context("read provider-fault proxy address")?;
        let upstream_url = upstream_url.to_owned();
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .context("build provider-fault upstream client")?;
        let state = Arc::new(ProxyState::default());
        let task = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(connection) => connection,
                        Err(error) => {
                            record_error(&state, format!("provider-fault accept failed: {error}"));
                            break;
                        }
                    };
                    let upstream_url = upstream_url.clone();
                    let client = client.clone();
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        if let Err(error) =
                            serve_connection(stream, &client, &upstream_url, &state).await
                        {
                            if is_client_disconnect(&error) {
                                return;
                            }
                            record_error(
                                &state,
                                format!("provider-fault connection failed: {error:#}"),
                            );
                        }
                    });
                }
            })
        };

        Ok(Self {
            url: format!("http://{address}"),
            state,
            task,
        })
    }

    pub fn add_fault(&self, spec: FaultSpec) {
        self.state
            .rules
            .lock()
            .expect("provider-fault rules lock must not be poisoned")
            .push(FaultRule { spec, hits: 0 });
        self.state.changed.notify_waiters();
    }

    pub fn add_faults(&self, specs: impl IntoIterator<Item = FaultSpec>) {
        for spec in specs {
            self.add_fault(spec);
        }
    }

    pub fn clear_faults(&self, kind: FaultKind) {
        let mut rules = self
            .state
            .rules
            .lock()
            .expect("provider-fault rules lock must not be poisoned");
        for rule in rules.iter_mut().filter(|rule| rule.spec.kind == kind) {
            rule.spec.remaining = Some(0);
        }
        self.state.changed.notify_waiters();
    }

    pub fn hit_count(&self, kind: FaultKind) -> usize {
        self.state
            .rules
            .lock()
            .expect("provider-fault rules lock must not be poisoned")
            .iter()
            .filter(|rule| rule.spec.kind == kind)
            .map(|rule| rule.hits)
            .sum()
    }

    pub async fn wait_for_hits(
        &self,
        kind: FaultKind,
        expected: usize,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.state.changed.notified();
            let actual = self.hit_count(kind);
            if actual >= expected {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                bail!(
                    "provider-fault proxy observed {actual} {kind:?} hits, expected at least {expected} within {timeout:?}"
                );
            }
        }
    }

    pub fn get_code_request_count(&self, address: &str, block_hash: &str) -> usize {
        let address = address.to_ascii_lowercase();
        let block_hash = block_hash.to_ascii_lowercase();
        self.state
            .calls
            .lock()
            .expect("provider-fault calls lock must not be poisoned")
            .iter()
            .filter(|call| {
                call.method == "eth_getCode"
                    && get_code_params_match(&call.params, &address, &block_hash)
            })
            .count()
    }

    pub fn total_request_count(&self) -> usize {
        self.state
            .calls
            .lock()
            .expect("provider-fault calls lock must not be poisoned")
            .len()
    }

    pub fn assert_healthy(&self) -> Result<()> {
        let errors = self
            .state
            .errors
            .lock()
            .expect("provider-fault errors lock must not be poisoned");
        if errors.is_empty() {
            Ok(())
        } else {
            bail!("provider-fault proxy errors:\n{}", errors.join("\n"))
        }
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn record_error(state: &ProxyState, error: String) {
    state
        .errors
        .lock()
        .expect("provider-fault errors lock must not be poisoned")
        .push(error);
    state.changed.notify_waiters();
}

fn is_client_disconnect(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
            )
        })
    })
}

async fn serve_connection(
    mut stream: TcpStream,
    client: &reqwest::Client,
    upstream_url: &str,
    state: &ProxyState,
) -> Result<()> {
    let request_body = read_http_request_body(&mut stream).await?;
    let request: Value =
        serde_json::from_slice(&request_body).context("decode provider-fault JSON-RPC request")?;
    record_calls(state, &request);

    let upstream = client
        .post(upstream_url)
        .header("content-type", "application/json")
        .body(request_body)
        .send()
        .await
        .context("forward provider-fault request to Anvil")?;
    let status = upstream.status();
    let upstream_body = upstream
        .bytes()
        .await
        .context("read provider-fault upstream response")?;
    let mut response: Value = serde_json::from_slice(&upstream_body)
        .context("decode provider-fault upstream JSON-RPC response")?;
    let directive = apply_next_fault(state, &request, &mut response)?;

    let mut response_status = status.as_u16();
    let mut response_body = serde_json::to_vec(&response)?;
    let mut declared_length = response_body.len();
    let mut delay = Duration::ZERO;
    match directive {
        Some(FaultDirective::DelayTimeout { wait }) => {
            delay = wait;
            response_status = 504;
            response_body = b"provider-fault injected gateway timeout".to_vec();
            declared_length = response_body.len();
        }
        Some(FaultDirective::Truncate { trailing_bytes }) => {
            let retained = response_body.len().saturating_sub(trailing_bytes).max(1);
            response_body.truncate(retained);
        }
        Some(FaultDirective::Mutated) | None => {}
    }

    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    write_http_response(
        &mut stream,
        response_status,
        declared_length,
        &response_body,
    )
    .await
}

async fn read_http_request_body(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    loop {
        if let Some(header_end) = find_header_end(&buffer) {
            let content_length = parse_content_length(&buffer[..header_end])?;
            let body_start = header_end + 4;
            let request_end = body_start
                .checked_add(content_length)
                .context("provider-fault request length overflow")?;
            if request_end > MAX_HTTP_REQUEST_BYTES {
                bail!("provider-fault request exceeds {MAX_HTTP_REQUEST_BYTES} bytes");
            }
            if buffer.len() >= request_end {
                return Ok(buffer[body_start..request_end].to_vec());
            }
        } else if buffer.len() > MAX_HTTP_REQUEST_BYTES {
            bail!("provider-fault request headers exceed {MAX_HTTP_REQUEST_BYTES} bytes");
        }

        let mut chunk = [0_u8; 8192];
        let read = stream
            .read(&mut chunk)
            .await
            .context("read provider-fault HTTP request")?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "provider-fault client closed before sending a complete request",
            )
            .into());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &[u8]) -> Result<usize> {
    let headers =
        std::str::from_utf8(headers).context("provider-fault HTTP headers are not UTF-8")?;
    for line in headers.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse()
                .context("parse provider-fault content-length");
        }
    }
    bail!("provider-fault request omitted content-length")
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    declared_length: usize,
    body: &[u8],
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        504 => "Gateway Timeout",
        _ => "Upstream Response",
    };
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {declared_length}\r\nconnection: close\r\n\r\n"
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .context("write provider-fault response headers")?;
    stream
        .write_all(body)
        .await
        .context("write provider-fault response body")?;
    stream
        .shutdown()
        .await
        .context("close provider-fault response")
}

fn record_calls(state: &ProxyState, request: &Value) {
    let mut calls = state
        .calls
        .lock()
        .expect("provider-fault calls lock must not be poisoned");
    visit_values(request, &mut |call| {
        let Some(method) = call.get("method").and_then(Value::as_str) else {
            return;
        };
        calls.push(ObservedCall {
            method: method.to_owned(),
            params: call.get("params").cloned().unwrap_or(Value::Null),
        });
    });
    state.changed.notify_waiters();
}

enum FaultDirective {
    Mutated,
    DelayTimeout { wait: Duration },
    Truncate { trailing_bytes: usize },
}

fn apply_next_fault(
    state: &ProxyState,
    request: &Value,
    response: &mut Value,
) -> Result<Option<FaultDirective>> {
    let mut rules = state
        .rules
        .lock()
        .expect("provider-fault rules lock must not be poisoned");
    for rule in rules.iter_mut() {
        if rule.spec.remaining == Some(0) {
            continue;
        }
        let Some(directive) = apply_fault_action(&rule.spec.action, request, response)? else {
            continue;
        };
        if let Some(remaining) = rule.spec.remaining.as_mut() {
            *remaining -= 1;
        }
        rule.hits += 1;
        state.changed.notify_waiters();
        return Ok(Some(directive));
    }
    Ok(None)
}

fn apply_fault_action(
    action: &FaultAction,
    request: &Value,
    response: &mut Value,
) -> Result<Option<FaultDirective>> {
    match action {
        FaultAction::DropLogs {
            transaction_hash,
            count,
        } => {
            let removed =
                drop_matching_results(request, response, "eth_getLogs", transaction_hash, *count);
            if removed == 0 {
                return Ok(None);
            }
            if removed != *count {
                bail!(
                    "drop-logs fault removed {removed} logs for {transaction_hash}, expected {count}"
                );
            }
            Ok(Some(FaultDirective::Mutated))
        }
        FaultAction::DelayTimeout {
            transaction_hash,
            delay,
        } => Ok(
            response_contains_transaction(request, response, "eth_getLogs", transaction_hash)
                .then_some(FaultDirective::DelayTimeout { wait: *delay }),
        ),
        FaultAction::Truncate {
            transaction_hash,
            trailing_bytes,
        } => Ok(
            response_contains_transaction(request, response, "eth_getLogs", transaction_hash)
                .then_some(FaultDirective::Truncate {
                    trailing_bytes: *trailing_bytes,
                }),
        ),
        FaultAction::JsonRpcError {
            transaction_hash,
            code,
            message,
        } => {
            if replace_log_result_with_error(request, response, transaction_hash, *code, message) {
                Ok(Some(FaultDirective::Mutated))
            } else {
                Ok(None)
            }
        }
        FaultAction::GetCodeJsonRpcError {
            address,
            block_hash,
            code,
            message,
        } => {
            let ids = request_ids_matching(request, "eth_getCode", |call| {
                call.get("params")
                    .is_some_and(|params| get_code_params_match(params, address, block_hash))
            });
            if ids.is_empty() {
                return Ok(None);
            }
            let replaced = replace_response_ids_with_error(response, &ids, *code, message);
            if replaced != ids.len() {
                bail!(
                    "get-code error fault matched {} requests but replaced {replaced} responses",
                    ids.len()
                );
            }
            Ok(Some(FaultDirective::Mutated))
        }
        FaultAction::DropReceipts {
            transaction_hash,
            count,
        } => {
            let removed = drop_matching_results(
                request,
                response,
                "eth_getBlockReceipts",
                transaction_hash,
                *count,
            );
            if removed == 0 {
                return Ok(None);
            }
            if removed != *count {
                bail!(
                    "drop-receipts fault removed {removed} receipts for {transaction_hash}, expected {count}"
                );
            }
            Ok(Some(FaultDirective::Mutated))
        }
        FaultAction::PrunedCode {
            address,
            block_hash,
            requested_block_number,
        } => {
            let ids = request_ids_matching(request, "eth_getCode", |call| {
                call.get("params")
                    .is_some_and(|params| get_code_params_match(params, address, block_hash))
            });
            if ids.is_empty() {
                return Ok(None);
            }
            let state_boundary = requested_block_number
                .checked_add(1)
                .context("pruned-state boundary overflow")?;
            let replaced = replace_response_ids_with_error(
                response,
                &ids,
                -32603,
                &format!("state at block #{state_boundary} is pruned"),
            );
            if replaced == 0 {
                bail!("pruned-state fault matched a request but no response id");
            }
            Ok(Some(FaultDirective::Mutated))
        }
    }
}

fn get_code_params_match(params: &Value, address: &str, block_hash: &str) -> bool {
    let Some(params) = params.as_array() else {
        return false;
    };
    params
        .first()
        .and_then(Value::as_str)
        .is_some_and(|actual| actual.eq_ignore_ascii_case(address))
        && params.get(1).is_some_and(|selection| {
            selection
                .get("blockHash")
                .and_then(Value::as_str)
                .or_else(|| selection.as_str())
                .is_some_and(|actual| actual.eq_ignore_ascii_case(block_hash))
        })
}

fn response_contains_transaction(
    request: &Value,
    response: &Value,
    method: &str,
    transaction_hash: &str,
) -> bool {
    let ids = request_ids_matching(request, method, |_| true);
    let mut found = false;
    visit_values(response, &mut |item| {
        if response_id_is_selected(item, &ids)
            && result_contains_transaction(item, transaction_hash)
        {
            found = true;
        }
    });
    found
}

fn drop_matching_results(
    request: &Value,
    response: &mut Value,
    method: &str,
    transaction_hash: &str,
    count: usize,
) -> usize {
    let ids = request_ids_matching(request, method, |_| true);
    let mut remaining = count;
    visit_values_mut(response, &mut |item| {
        if remaining == 0 || !response_id_is_selected(item, &ids) {
            return;
        }
        let Some(results) = item.get_mut("result").and_then(Value::as_array_mut) else {
            return;
        };
        results.retain(|result| {
            if remaining > 0 && value_transaction_matches(result, transaction_hash) {
                remaining -= 1;
                false
            } else {
                true
            }
        });
    });
    count - remaining
}

fn replace_log_result_with_error(
    request: &Value,
    response: &mut Value,
    transaction_hash: &str,
    code: i64,
    message: &str,
) -> bool {
    let ids = request_ids_matching(request, "eth_getLogs", |_| true);
    let mut replaced = false;
    visit_values_mut(response, &mut |item| {
        if replaced
            || !response_id_is_selected(item, &ids)
            || !result_contains_transaction(item, transaction_hash)
        {
            return;
        }
        let id = item.get("id").cloned().unwrap_or(Value::Null);
        *item = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message},
        });
        replaced = true;
    });
    replaced
}

fn replace_response_ids_with_error(
    response: &mut Value,
    ids: &BTreeSet<String>,
    code: i64,
    message: &str,
) -> usize {
    let mut replaced = 0;
    visit_values_mut(response, &mut |item| {
        if !response_id_is_selected(item, ids) {
            return;
        }
        let id = item.get("id").cloned().unwrap_or(Value::Null);
        *item = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message},
        });
        replaced += 1;
    });
    replaced
}

fn request_ids_matching(
    request: &Value,
    method: &str,
    mut predicate: impl FnMut(&Map<String, Value>) -> bool,
) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    visit_values(request, &mut |call| {
        let Some(call) = call.as_object() else {
            return;
        };
        if call.get("method").and_then(Value::as_str) == Some(method)
            && predicate(call)
            && let Some(id) = call.get("id").and_then(id_key)
        {
            ids.insert(id);
        }
    });
    ids
}

fn response_id_is_selected(item: &Value, ids: &BTreeSet<String>) -> bool {
    item.get("id")
        .and_then(id_key)
        .is_some_and(|id| ids.contains(&id))
}

fn id_key(id: &Value) -> Option<String> {
    serde_json::to_string(id).ok()
}

fn result_contains_transaction(item: &Value, transaction_hash: &str) -> bool {
    item.get("result")
        .and_then(Value::as_array)
        .is_some_and(|values| {
            values
                .iter()
                .any(|value| value_transaction_matches(value, transaction_hash))
        })
}

fn value_transaction_matches(value: &Value, transaction_hash: &str) -> bool {
    value
        .get("transactionHash")
        .and_then(Value::as_str)
        .is_some_and(|actual| actual.eq_ignore_ascii_case(transaction_hash))
}

fn visit_values(value: &Value, visitor: &mut impl FnMut(&Value)) {
    match value {
        Value::Array(values) => {
            for value in values {
                visitor(value);
            }
        }
        _ => visitor(value),
    }
}

fn visit_values_mut(value: &mut Value, visitor: &mut impl FnMut(&mut Value)) {
    match value {
        Value::Array(values) => {
            for value in values {
                visitor(value);
            }
        }
        _ => visitor(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_request() -> Value {
        json!({"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{}]})
    }

    fn log_response(transaction_hash: &str) -> Value {
        json!({
            "jsonrpc":"2.0",
            "id":1,
            "result":[{"transactionHash":transaction_hash,"logIndex":"0x0"}]
        })
    }

    #[test]
    fn ordered_one_shot_faults_advance_one_response_at_a_time() -> Result<()> {
        let transaction_hash = "0xabc";
        let state = ProxyState::default();
        {
            let mut rules = state.rules.lock().expect("rules lock");
            for spec in [
                FaultSpec::error_once(transaction_hash, -32005, "retry"),
                FaultSpec::delay_timeout_once(transaction_hash, Duration::from_millis(1)),
                FaultSpec::truncate_once(transaction_hash, 1),
            ] {
                rules.push(FaultRule { spec, hits: 0 });
            }
        }

        let first = apply_next_fault(&state, &log_request(), &mut log_response(transaction_hash))?;
        assert!(matches!(first, Some(FaultDirective::Mutated)));
        let second = apply_next_fault(&state, &log_request(), &mut log_response(transaction_hash))?;
        assert!(matches!(second, Some(FaultDirective::DelayTimeout { .. })));
        let third = apply_next_fault(&state, &log_request(), &mut log_response(transaction_hash))?;
        assert!(matches!(third, Some(FaultDirective::Truncate { .. })));
        let fourth = apply_next_fault(&state, &log_request(), &mut log_response(transaction_hash))?;
        assert!(fourth.is_none());
        Ok(())
    }

    #[test]
    fn pruned_code_rule_targets_only_one_address_and_block_hash() -> Result<()> {
        let address = "0x0000000000000000000000000000000000000001";
        let block_hash = format!("0x{:064x}", 42);
        let request = json!([
            {"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":[address,{"blockHash":block_hash}]},
            {"jsonrpc":"2.0","id":2,"method":"eth_getCode","params":["0x0000000000000000000000000000000000000002",{"blockHash":block_hash}]}
        ]);
        let mut response = json!([
            {"jsonrpc":"2.0","id":1,"result":"0x6000"},
            {"jsonrpc":"2.0","id":2,"result":"0x6001"}
        ]);
        let action = FaultSpec::pruned_get_code(address, &block_hash, 42).action;

        assert!(matches!(
            apply_fault_action(&action, &request, &mut response)?,
            Some(FaultDirective::Mutated)
        ));
        assert_eq!(response[0]["error"]["code"], -32603);
        assert_eq!(
            response[0]["error"]["message"],
            "state at block #43 is pruned"
        );
        assert_eq!(response[1]["result"], "0x6001");
        Ok(())
    }

    #[tokio::test]
    async fn clean_eof_before_complete_request_is_a_client_disconnect() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = TcpStream::connect(listener.local_addr()?).await?;
        let (mut server, _) = listener.accept().await?;
        client
            .write_all(b"POST / HTTP/1.1\r\ncontent-length: 10\r\n\r\npartial")
            .await?;
        client.shutdown().await?;

        let error = read_http_request_body(&mut server)
            .await
            .expect_err("an incomplete request ending in clean EOF must fail");
        assert!(is_client_disconnect(&error));
        Ok(())
    }
}
