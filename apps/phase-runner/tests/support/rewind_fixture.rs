use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub const CHAIN: &str = "ethereum-sepolia";
pub const HEAD: i64 = 511;
pub const SAFE: i64 = 64;
pub const FINALIZED: i64 = 32;
pub const ANCESTOR: i64 = 128;
pub const CHECKPOINT: i64 = 255;
pub const POLL: Duration = Duration::from_millis(25);
pub const DEADLINE: Duration = Duration::from_secs(60);

pub fn hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}

pub fn save(directory: &Path, name: &str, value: &Value) -> Result<()> {
    fs::write(directory.join(name), serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

pub struct Gate {
    entries: Mutex<Vec<Instant>>,
    release: CancellationToken,
}

impl Gate {
    pub fn blocked(&self) -> bool {
        !self
            .entries
            .lock()
            .expect("gate entries poisoned")
            .is_empty()
    }

    pub fn require_held(&self) -> Result<()> {
        let entries = self.entries.lock().expect("gate entries poisoned");
        ensure!(
            entries.len() == 1,
            "gate must hold exactly one request: {entries:?}"
        );
        ensure!(
            entries[0].elapsed() < Duration::from_secs(10),
            "gate exceeded ten-second budget"
        );
        ensure!(!self.release.is_cancelled(), "gate was already released");
        Ok(())
    }

    pub fn release(&self) {
        self.release.cancel();
    }
}

struct RpcState {
    gate: Mutex<Option<Arc<Gate>>>,
    trace: Mutex<File>,
    unexpected: Mutex<Vec<String>>,
    stage: Mutex<&'static str>,
}

pub struct RpcFixture {
    pub endpoint: String,
    state: Arc<RpcState>,
    server: JoinHandle<std::io::Result<()>>,
}

impl RpcFixture {
    pub async fn start(directory: &Path) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(RpcState {
            gate: Mutex::new(None),
            trace: Mutex::new(File::create(directory.join("rpc.jsonl"))?),
            unexpected: Mutex::new(Vec::new()),
            stage: Mutex::new("setup"),
        });
        let router = Router::new()
            .route("/", post(rpc))
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        Ok(Self {
            endpoint,
            state,
            server,
        })
    }

    pub fn stage(&self, name: &'static str) {
        *self.state.stage.lock().expect("RPC stage poisoned") = name;
    }

    pub fn arm(&self, name: &'static str) -> Arc<Gate> {
        self.stage(name);
        let gate = Arc::new(Gate {
            entries: Mutex::new(Vec::new()),
            release: CancellationToken::new(),
        });
        *self.state.gate.lock().expect("RPC gate poisoned") = Some(Arc::clone(&gate));
        gate
    }

    pub fn disarm(&self) {
        if let Some(gate) = self.state.gate.lock().expect("RPC gate poisoned").take() {
            gate.release();
        }
    }

    pub fn require_expected_requests(&self) -> Result<()> {
        let unexpected = self
            .state
            .unexpected
            .lock()
            .expect("RPC diagnostics poisoned");
        ensure!(
            unexpected.is_empty(),
            "unsupported fixture requests: {unexpected:?}"
        );
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.disarm();
        self.server.abort();
        match (&mut self.server).await {
            Err(error) if error.is_cancelled() => Ok(()),
            result => {
                result.context("RPC server task failed")??;
                Ok(())
            }
        }
    }
}

impl Drop for RpcFixture {
    fn drop(&mut self) {
        self.disarm();
        self.server.abort();
    }
}

async fn rpc(State(state): State<Arc<RpcState>>, Json(request): Json<Value>) -> Json<Value> {
    let stage = *state.stage.lock().expect("RPC stage poisoned");
    let calls = request
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![request.clone()]);
    let mut replies = Vec::with_capacity(calls.len());
    for call in calls {
        // Header 256 belongs to the second load window. Numeric end probes for 511
        // must remain available to the first batch, including during redo.
        let gate = state.gate.lock().expect("RPC gate poisoned").clone();
        let held = call["method"] == "eth_getBlockByHash"
            && call["params"][0].as_str() == Some(hash(CHECKPOINT + 1).as_str());
        if held && let Some(gate) = gate {
            gate.entries
                .lock()
                .expect("gate entries poisoned")
                .push(Instant::now());
            gate.release.cancelled().await;
        }
        let result = reply(&call);
        let response = match result {
            Ok(value) => json!({"jsonrpc": "2.0", "id": call["id"], "result": value}),
            Err(error) => {
                state
                    .unexpected
                    .lock()
                    .expect("RPC diagnostics poisoned")
                    .push(error.to_string());
                json!({"jsonrpc": "2.0", "id": call["id"], "error": {
                    "code": -32602, "message": error.to_string()
                }})
            }
        };
        let trace = json!({"stage": stage, "request": call, "response": response});
        let written = writeln!(state.trace.lock().expect("RPC trace poisoned"), "{trace}");
        if let Err(error) = written {
            state
                .unexpected
                .lock()
                .expect("RPC diagnostics poisoned")
                .push(error.to_string());
        }
        replies.push(response);
    }
    Json(if request.is_array() {
        Value::Array(replies)
    } else {
        replies.remove(0)
    })
}

fn reply(call: &Value) -> Result<Value> {
    let selector = call["params"][0].as_str();
    let number = match call["method"].as_str() {
        Some("eth_getBlockByNumber") => match selector.context("missing block selector")? {
            "latest" => HEAD,
            "safe" => SAFE,
            "finalized" => FINALIZED,
            value => i64::from_str_radix(value.trim_start_matches("0x"), 16)?,
        },
        Some("eth_getBlockByHash") => {
            let selected = selector.context("missing block hash")?;
            (0..=HEAD)
                .find(|number| hash(*number) == selected)
                .context("unknown block hash")?
        }
        Some("eth_getLogs") => return Ok(json!([])),
        method => bail!("unexpected RPC method {method:?}"),
    };
    ensure!((0..=HEAD).contains(&number), "unexpected block {number}");
    Ok(json!({
        "number": format!("0x{number:x}"), "hash": hash(number),
        "parentHash": hash(number - 1),
        "timestamp": format!("0x{:x}", 1_600_000_000 + 12 * number),
        "logsBloom": format!("0x{}", "00".repeat(256)), "transactions": []
    }))
}

pub struct OwnedChild {
    child: Child,
    pub log: PathBuf,
    status: Option<ExitStatus>,
}

impl OwnedChild {
    pub fn start(mut command: Command, directory: &Path, label: &str) -> Result<Self> {
        let log = directory.join(format!("{label}.log"));
        let output = File::create(&log)?;
        let child = command
            .stdin(Stdio::null())
            .stdout(output.try_clone()?)
            .stderr(output)
            .spawn()
            .with_context(|| format!("failed to start {label}"))?;
        let owned = Self {
            child,
            log,
            status: None,
        };
        eprintln!(
            "owned {label} pid {}: {}",
            owned.child.id(),
            owned.log.display()
        );
        Ok(owned)
    }

    pub fn running(&mut self) -> Result<bool> {
        if self.status.is_none() {
            self.status = self.child.try_wait()?;
        }
        Ok(self.status.is_none())
    }

    pub async fn wait(&mut self) -> Result<ExitStatus> {
        let until = Instant::now() + DEADLINE;
        while self.running()? {
            ensure!(
                Instant::now() < until,
                "child did not exit; see {}",
                self.log.display()
            );
            tokio::time::sleep(POLL).await;
        }
        let status = self.status.expect("child was reaped");
        fs::write(
            self.log.with_extension("exit.json"),
            json!({
                "pid": self.child.id(), "code": status.code(), "status": status.to_string()
            })
            .to_string(),
        )?;
        Ok(status)
    }

    pub async fn interrupt(&mut self) -> Result<()> {
        ensure!(
            self.running()?,
            "child exited before SIGINT; see {}",
            self.log.display()
        );
        let mut command = Command::new("/bin/kill");
        command.args(["-INT", &self.child.id().to_string()]);
        let directory = self.log.parent().context("child log has no directory")?;
        let label = format!("signal-{}", self.child.id());
        let mut signal = Self::start(command, directory, &label)?;
        ensure!(signal.wait().await?.success(), "SIGINT delivery failed");
        Ok(())
    }

    pub async fn terminate(&mut self) -> Result<()> {
        if self.running()? {
            self.child.kill().context("failed to kill owned child")?;
        }
        self.wait().await?;
        Ok(())
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Explicit asynchronous cleanup below is responsible for bounded reaping.
        // This is only a last resort if setup or the test task unwinds.
        if self.status.is_none() {
            let _ = self.child.kill();
            let _ = self.child.try_wait();
        }
    }
}

pub async fn wait_for_gate(child: &mut OwnedChild, gate: &Gate) -> Result<()> {
    let until = Instant::now() + DEADLINE;
    while !gate.blocked() {
        ensure!(
            child.running()?,
            "child exited before header gate; see {}",
            child.log.display()
        );
        ensure!(
            Instant::now() < until,
            "second-window header gate was not reached"
        );
        tokio::time::sleep(POLL).await;
    }
    gate.require_held()
}

pub async fn snapshot(pool: &sqlx::PgPool, directory: &Path, name: &str) -> Result<Value> {
    let mut snapshot = serde_json::Map::new();
    for table in [
        "chain_heads",
        "chain_lineage",
        "chain_header_audit",
        "raw_transactions",
        "raw_receipts",
        "raw_logs",
        "chain_phase_state",
        "ingest_cursors",
    ] {
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), '[]'::jsonb)
                FROM bigname_phase.{table} t WHERE chain_id = $1"
        );
        let rows: Value = sqlx::query_scalar(&sql).bind(CHAIN).fetch_one(pool).await?;
        snapshot.insert(table.to_owned(), rows);
    }
    let snapshot = Value::Object(snapshot);
    save(directory, name, &snapshot)?;
    Ok(snapshot)
}

pub async fn no_writer_sessions(pool: &sqlx::PgPool, application: &str) -> Result<()> {
    let until = Instant::now() + DEADLINE;
    loop {
        let sessions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
                 AND pid <> pg_backend_pid() AND application_name = $1",
        )
        .bind(application)
        .fetch_one(pool)
        .await?;
        let locked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'
                 AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .fetch_one(pool)
        .await?;
        if sessions == 0 && locked == 0 {
            return Ok(());
        }
        ensure!(
            Instant::now() < until,
            "owned writer sessions or advisory locks remain"
        );
        tokio::time::sleep(POLL).await;
    }
}

pub fn require_resumed_headers(directory: &std::path::Path) -> Result<()> {
    let mut loaded = std::collections::BTreeSet::new();
    for line in fs::read_to_string(directory.join("rpc.jsonl"))?.lines() {
        let event: Value = serde_json::from_str(line)?;
        if event["stage"] == "repair" && event["request"]["method"] == "eth_getBlockByHash" {
            let selected = event["request"]["params"][0]
                .as_str()
                .context("missing repair hash")?;
            loaded.insert(
                (0..=HEAD)
                    .find(|n| hash(*n) == selected)
                    .context("unknown repair header")?,
            );
        }
    }
    ensure!(
        loaded == ((CHECKPOINT + 1)..=HEAD).collect(),
        "repair did not load exactly the remaining header suffix: {loaded:?}"
    );
    Ok(())
}
