use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::process::{Child, Command};

const PIPELINE_BINARY_SPECS: [(&str, &str); 3] = [
    ("bigname-api", "apps/api/Cargo.toml"),
    ("bigname-indexer", "apps/indexer/Cargo.toml"),
    ("bigname-worker", "apps/worker/Cargo.toml"),
];

#[derive(Debug)]
struct PipelineBinaries {
    api: PathBuf,
    indexer: PathBuf,
    worker: PathBuf,
}

static PIPELINE_BINARIES: tokio::sync::OnceCell<std::result::Result<PipelineBinaries, String>> =
    tokio::sync::OnceCell::const_new();
static PROCESS_LOG_SEQ: AtomicU64 = AtomicU64::new(0);

const DEFAULT_READY_TIMEOUT_SECS: u64 = 600;
const DEFAULT_COMMAND_TIMEOUT_SECS: u64 = 600;

#[cfg(unix)]
mod unix_process {
    use std::io;

    const SIGKILL: i32 = 9;
    #[cfg(test)]
    const ESRCH: i32 = 3;

    unsafe extern "C" {
        #[link_name = "kill"]
        fn c_kill(pid: i32, signal: i32) -> i32;
        fn getpgrp() -> i32;
    }

    pub fn kill_process_group(process_group: u32) -> io::Result<()> {
        let process_group = positive_pid(process_group)?;
        // SAFETY: getpgrp takes no arguments and has no memory-safety
        // preconditions.
        let harness_process_group = unsafe { getpgrp() };
        if process_group == harness_process_group {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing to signal the harness process group",
            ));
        }
        // SAFETY: a negative, nonzero pid addresses one Unix process group.
        // The equality guard above prevents signaling the harness group.
        if unsafe { c_kill(-process_group, SIGKILL) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(test)]
    pub fn process_exists(pid: u32) -> io::Result<bool> {
        let pid = positive_pid(pid)?;
        // SAFETY: signal 0 performs existence/permission checking only.
        if unsafe { c_kill(pid, 0) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }

    #[cfg(test)]
    pub fn kill_process(pid: u32) -> io::Result<()> {
        let pid = positive_pid(pid)?;
        // SAFETY: pid is a validated positive process id and SIGKILL has no
        // userspace memory-safety preconditions.
        if unsafe { c_kill(pid, SIGKILL) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn positive_pid(pid: u32) -> io::Result<i32> {
        let pid = i32::try_from(pid)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id exceeds i32"))?;
        if pid == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process id must be positive",
            ));
        }
        Ok(pid)
    }
}

#[derive(Clone, Copy, Debug)]
enum TimeoutTerminationTarget {
    DirectChild,
    #[cfg(unix)]
    ProcessGroup(u32),
}

/// Build the three pipeline binaries once, then launch the exact artifacts
/// Cargo reports. Direct launches keep later scenario startup independent of
/// the workspace target lock while still honoring its effective target dir.
async fn pipeline_binaries(repo_root: &Path) -> Result<&'static PipelineBinaries> {
    match PIPELINE_BINARIES
        .get_or_init(|| async {
            build_pipeline_binaries(repo_root)
                .await
                .map_err(|error| format!("{error:#}"))
        })
        .await
    {
        Ok(binaries) => Ok(binaries),
        Err(error) => bail!("{error}"),
    }
}

async fn build_pipeline_binaries(repo_root: &Path) -> Result<PipelineBinaries> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut command = Command::new(cargo);
    command.current_dir(repo_root).args([
        "build",
        "--locked",
        "--message-format=json-render-diagnostics",
        "--package",
        "bigname-api",
        "--package",
        "bigname-indexer",
        "--package",
        "bigname-worker",
        "--bins",
    ]);
    let stdout = run_to_completion(command, "one-time pipeline binary build").await?;

    parse_pipeline_binaries(repo_root, stdout.as_bytes())
}

fn normalize_cargo_path(repo_root: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn parse_pipeline_binaries(repo_root: &Path, stdout: &[u8]) -> Result<PipelineBinaries> {
    let mut api = None;
    let mut indexer = None;
    let mut worker = None;

    for (line_index, line) in stdout.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        let message: Value = serde_json::from_slice(line).with_context(|| {
            format!("parse Cargo JSON message on stdout line {}", line_index + 1)
        })?;
        if message.get("reason").and_then(Value::as_str) != Some("compiler-artifact") {
            continue;
        }
        let Some(target) = message.get("target") else {
            continue;
        };
        let is_binary = target
            .get("kind")
            .and_then(Value::as_array)
            .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some("bin")));
        if !is_binary {
            continue;
        }
        let Some(name) = target.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(manifest_path) = message.get("manifest_path").and_then(Value::as_str) else {
            continue;
        };
        let manifest_path = normalize_cargo_path(repo_root, manifest_path);
        let Some((_, expected_manifest)) = PIPELINE_BINARY_SPECS
            .iter()
            .find(|(expected, path)| name == *expected && manifest_path == repo_root.join(*path))
        else {
            continue;
        };

        let executable = match message.get("executable") {
            Some(Value::String(path)) => normalize_cargo_path(repo_root, path),
            Some(Value::Null) => {
                bail!("Cargo reported a null executable for {name} from {expected_manifest}")
            }
            Some(_) => {
                bail!("Cargo reported a non-string executable for {name} from {expected_manifest}")
            }
            None => bail!("Cargo omitted the executable for {name} from {expected_manifest}"),
        };

        let slot = match name {
            "bigname-api" => &mut api,
            "bigname-indexer" => &mut indexer,
            "bigname-worker" => &mut worker,
            _ => unreachable!("pipeline binary specification matched an unknown name"),
        };
        if slot.replace(executable).is_some() {
            bail!("Cargo reported duplicate executable artifacts for {name}");
        }
    }

    let mut missing = Vec::new();
    if api.is_none() {
        missing.push("bigname-api");
    }
    if indexer.is_none() {
        missing.push("bigname-indexer");
    }
    if worker.is_none() {
        missing.push("bigname-worker");
    }
    if !missing.is_empty() {
        bail!(
            "Cargo build did not report executable artifacts for {}",
            missing.join(", ")
        );
    }

    Ok(PipelineBinaries {
        api: api.expect("checked above"),
        indexer: indexer.expect("checked above"),
        worker: worker.expect("checked above"),
    })
}

fn pipeline_command(repo_root: &Path, executable: &Path) -> Command {
    let mut command = Command::new(executable);
    command.current_dir(repo_root);
    // E2e corpora are tiny, but every spawned binary defaults to a
    // 10-connection pool; under suite parallelism that exhausts the shared
    // test postgres (max_connections 100) and surfaces as pool-acquire
    // timeouts in unrelated tests.
    command.env("BIGNAME_DATABASE_MAX_CONNECTIONS", "4");
    command
}

async fn run_to_completion(command: Command, what: &str) -> Result<String> {
    let timeout_secs = timeout_secs_from_env(
        "BIGNAME_E2E_COMMAND_TIMEOUT_SECS",
        DEFAULT_COMMAND_TIMEOUT_SECS,
    )?;
    run_to_completion_with_timeout(command, what, timeout_secs).await
}

async fn run_to_completion_with_timeout(
    mut command: Command,
    what: &str,
    timeout_secs: u64,
) -> Result<String> {
    let (stdout_path, stdout_file) = create_process_log_file("command-stdout", what)?;
    let (stderr_path, stderr_file) = match create_process_log_file("command-stderr", what) {
        Ok(log) => log,
        Err(error) => {
            std::fs::remove_file(&stdout_path).ok();
            return Err(error);
        }
    };
    isolate_bounded_command(&mut command);
    command
        .kill_on_drop(true)
        .stdout(std::process::Stdio::from(stdout_file))
        .stderr(std::process::Stdio::from(stderr_file));
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            std::fs::remove_file(&stdout_path).ok();
            std::fs::remove_file(&stderr_path).ok();
            return Err(error).with_context(|| format!("spawn {what}"));
        }
    };
    let termination_target = timeout_termination_target(&child);
    let status = match tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait()).await {
        Ok(status) => status.with_context(|| format!("wait for {what}"))?,
        Err(_) => {
            let stop_note = stop_and_reap_timed_out_child(&mut child, termination_target).await;
            bail!(
                "{what} exceeded the configured BIGNAME_E2E_COMMAND_TIMEOUT_SECS deadline of {timeout_secs}s ({stop_note}); stdout log {stdout_path:?}, stderr log {stderr_path:?}; stdout tail (reversed):\n{}\nstderr tail (reversed):\n{}",
                process_log_tail(&stdout_path),
                process_log_tail(&stderr_path)
            );
        }
    };
    let stdout = read_process_log(&stdout_path);
    if !status.success() {
        bail!(
            "{what} failed ({status}); stdout log {stdout_path:?}, stderr log {stderr_path:?}; stdout tail (reversed):\n{}\nstderr tail (reversed):\n{}",
            process_log_tail(&stdout_path),
            process_log_tail(&stderr_path)
        );
    }
    std::fs::remove_file(stdout_path).ok();
    std::fs::remove_file(stderr_path).ok();
    Ok(stdout)
}

#[cfg(unix)]
fn isolate_bounded_command(command: &mut Command) {
    // PGID 0 asks the child to become leader of a new process group. Its
    // descendants inherit that group unless they explicitly leave it.
    command.process_group(0);
}

#[cfg(not(unix))]
fn isolate_bounded_command(_command: &mut Command) {}

fn timeout_termination_target(child: &Child) -> TimeoutTerminationTarget {
    #[cfg(unix)]
    if let Some(process_group) = child.id() {
        return TimeoutTerminationTarget::ProcessGroup(process_group);
    }
    TimeoutTerminationTarget::DirectChild
}

async fn stop_and_reap_timed_out_child(
    child: &mut Child,
    target: TimeoutTerminationTarget,
) -> String {
    let stop_note = request_timeout_stop(child, target);
    let reap = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    match reap {
        Ok(Ok(status)) => format!("{stop_note}; process stopped and reaped with {status}"),
        Ok(Err(wait_error)) => {
            format!("{stop_note}; process reap failed: {wait_error}")
        }
        Err(_) => format!("{stop_note}; process was not reaped within 5s"),
    }
}

fn request_timeout_stop(child: &mut Child, target: TimeoutTerminationTarget) -> String {
    match target {
        TimeoutTerminationTarget::DirectChild => match child.start_kill() {
            Ok(()) => "direct child termination requested".to_string(),
            Err(error) => format!("direct child termination failed: {error}"),
        },
        #[cfg(unix)]
        TimeoutTerminationTarget::ProcessGroup(process_group) => {
            match unix_process::kill_process_group(process_group) {
                Ok(()) => format!("process group {process_group} termination requested"),
                Err(group_error) => match child.start_kill() {
                    Ok(()) => format!(
                        "process group {process_group} termination failed ({group_error}); direct child termination requested"
                    ),
                    Err(child_error) => format!(
                        "process group {process_group} termination failed ({group_error}); direct child termination failed ({child_error})"
                    ),
                },
            }
        }
    }
}

fn timeout_secs_from_env(variable: &str, default: u64) -> Result<u64> {
    match std::env::var(variable) {
        Ok(value) => parse_timeout_secs(variable, &value),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {variable}")),
    }
}

fn parse_timeout_secs(variable: &str, value: &str) -> Result<u64> {
    let seconds = value
        .parse::<u64>()
        .with_context(|| format!("{variable} must be a positive integer number of seconds"))?;
    if seconds == 0 {
        bail!("{variable} must be greater than zero");
    }
    Ok(seconds)
}

pub(super) fn ready_timeout_secs() -> Result<u64> {
    timeout_secs_from_env("BIGNAME_E2E_READY_TIMEOUT_SECS", DEFAULT_READY_TIMEOUT_SECS)
}

pub(super) fn deadline_after(seconds: u64, what: &str) -> Result<tokio::time::Instant> {
    tokio::time::Instant::now()
        .checked_add(Duration::from_secs(seconds))
        .with_context(|| format!("{what} timeout is too large"))
}

pub(super) async fn await_with_readiness_deadline<F>(
    deadline: tokio::time::Instant,
    ready_timeout_secs: u64,
    what: impl Into<String>,
    future: F,
) -> Result<F::Output>
where
    F: std::future::Future,
{
    let what = what.into();
    match tokio::time::timeout_at(deadline, future).await {
        Ok(output) => Ok(output),
        Err(_) => bail!("{what} exceeded the configured {ready_timeout_secs}s readiness deadline"),
    }
}

async fn await_supervised_readiness<T, F>(
    child: &mut Child,
    log_path: &Path,
    process_name: &str,
    deadline: tokio::time::Instant,
    ready_timeout_secs: u64,
    what: impl Into<String>,
    future: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    match await_with_readiness_deadline(deadline, ready_timeout_secs, what, future).await {
        Ok(result) => result,
        Err(error) => {
            let stop_note =
                stop_and_reap_timed_out_child(child, TimeoutTerminationTarget::DirectChild).await;
            Err(error.context(format!(
                "{process_name} stopped after readiness timeout ({stop_note}); log tail (reversed) from {log_path:?}:\n{}",
                process_log_tail(log_path)
            )))
        }
    }
}

async fn stop_after_readiness_deadline(
    child: &mut Child,
    log_path: &Path,
    timeout_secs: u64,
    message: &str,
) -> Result<()> {
    let stop_note =
        stop_and_reap_timed_out_child(child, TimeoutTerminationTarget::DirectChild).await;
    bail!(
        "{message} within the configured {timeout_secs}s readiness deadline ({stop_note}); log tail (reversed) from {log_path:?}:\n{}",
        process_log_tail(log_path)
    )
}

fn sanitize_log_label(label: &str) -> String {
    let label = label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let label = label.trim_matches('-');
    if label.is_empty() {
        "process".to_string()
    } else {
        label.to_string()
    }
}

fn create_process_log_file(process_kind: &str, label: &str) -> Result<(PathBuf, std::fs::File)> {
    let label = sanitize_log_label(label);
    for _ in 0..1000 {
        let sequence = PROCESS_LOG_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bigname-e2e-{process_kind}-{}-{label}-{sequence}.log",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("create process log at {path:?}"));
            }
        }
    }
    bail!("could not allocate a unique {process_kind} log path")
}

fn read_process_log(log_path: &Path) -> String {
    String::from_utf8_lossy(&std::fs::read(log_path).unwrap_or_default()).into_owned()
}

fn process_log_tail(log_path: &Path) -> String {
    let log = read_process_log(log_path);
    log.lines().rev().take(60).collect::<Vec<_>>().join("\n")
}

async fn stop_supervised_child(child: Child, what: &str, log_path: &Path) -> Result<()> {
    stop_supervised_child_with_pre_kill_delay(child, what, log_path, None).await
}

async fn stop_supervised_child_with_pre_kill_delay(
    mut child: Child,
    what: &str,
    log_path: &Path,
    pre_kill_delay: Option<Duration>,
) -> Result<()> {
    if let Some(status) = child.try_wait()? {
        bail!(
            "{what} exited before requested stop ({status}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }

    if let Some(delay) = pre_kill_delay {
        tokio::time::sleep(delay).await;
    }
    if let Err(kill_error) = child.start_kill() {
        let status = child
            .wait()
            .await
            .with_context(|| format!("failed to reap {what} after stop failed"))?;
        bail!(
            "{what} exited while stop was requested ({status}; stop error: {kill_error}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }
    let status = child
        .wait()
        .await
        .with_context(|| format!("failed to reap {what} after stop"))?;
    if !exited_from_requested_kill(&status) {
        bail!(
            "{what} exited independently while stop was requested ({status}); log tail (reversed) from {log_path:?}:\n{}",
            process_log_tail(log_path)
        );
    }
    Ok(())
}

fn exited_from_requested_kill(status: &std::process::ExitStatus) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        // Tokio's start_kill is SIGKILL on Unix. An ordinary nonzero exit or
        // another fatal signal therefore remains distinguishable from the
        // harness-requested stop even if it races with start_kill.
        status.signal() == Some(9)
    }

    #[cfg(not(unix))]
    {
        !status.success()
    }
}

pub type ChainRpcUrl<'a> = (&'a str, &'a str);

pub struct ChainCheckpointTarget<'a> {
    pub chain_rpc_urls: &'a [ChainRpcUrl<'a>],
    pub chain: &'a str,
    pub target_block: u64,
    pub extra_ready_sql: Option<&'a str>,
}

pub struct ChainBackfillTarget<'a> {
    pub chain_rpc_urls: &'a [ChainRpcUrl<'a>],
    pub chain: &'a str,
    pub block_range: std::ops::RangeInclusive<u64>,
    pub idempotency_key: &'a str,
}

fn format_chain_rpc_urls(chain_rpc_urls: &[ChainRpcUrl<'_>]) -> String {
    chain_rpc_urls
        .iter()
        .map(|(chain, url)| format!("{chain}={url}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// A supervised `indexer run` process. The caller decides when the live session
/// has reached enough checkpoints/readiness and then stops it explicitly.
pub struct IndexerRunSession {
    child: Child,
    log_path: PathBuf,
}

/// A supervised production `worker run` process. Most scenarios use the
/// deterministic one-shot projection replay command; this session exists for
/// the smaller set that must prove bootstrap handoff and continuous
/// invalidation/apply behavior while intake and the API remain live.
pub struct WorkerRunSession {
    child: Child,
    log_path: PathBuf,
}

impl WorkerRunSession {
    pub async fn start(repo_root: &Path, database_url: &str, log_label: &str) -> Result<Self> {
        let worker = &pipeline_binaries(repo_root).await?.worker;
        let (log_path, log_file) = create_process_log_file("worker", log_label)?;
        let child = pipeline_command(repo_root, worker)
            .args([
                "run",
                "--database-url",
                database_url,
                "--poll-interval-secs",
                "1",
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()
            .context("spawn worker run")?;
        Ok(Self { child, log_path })
    }

    pub async fn wait_for_sql(&mut self, pool: &sqlx::PgPool, sql: &str) -> Result<()> {
        let ready_timeout_secs = ready_timeout_secs()?;
        let deadline = deadline_after(ready_timeout_secs, "worker readiness")?;
        loop {
            self.bail_if_exited()?;
            let ready = await_supervised_readiness(
                &mut self.child,
                &self.log_path,
                "worker run",
                deadline,
                ready_timeout_secs,
                "worker readiness SQL query",
                async { Ok(sqlx::query_scalar::<_, bool>(sql).fetch_one(pool).await?) },
            )
            .await?;
            if ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return stop_after_readiness_deadline(
                    &mut self.child,
                    &self.log_path,
                    ready_timeout_secs,
                    "worker run did not satisfy readiness SQL",
                )
                .await;
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(250)),
            )
            .await;
        }
    }

    pub async fn stop(self) -> Result<()> {
        stop_supervised_child(self.child, "worker run", &self.log_path).await
    }

    fn bail_if_exited(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "worker run exited early ({status}); log tail (reversed) from {:?}:\n{}",
                self.log_path,
                self.log_tail()
            );
        }
        Ok(())
    }

    fn log_tail(&self) -> String {
        process_log_tail(&self.log_path)
    }
}

impl IndexerRunSession {
    pub async fn start(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_url: &str,
        log_label: &str,
    ) -> Result<Self> {
        Self::start_with_chain_rpc_urls(
            repo_root,
            database_url,
            manifests_root,
            &[("ethereum-mainnet", chain_rpc_url)],
            log_label,
        )
        .await
    }

    pub async fn start_with_chain_rpc_urls(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
        log_label: &str,
    ) -> Result<Self> {
        Self::start_with_chain_rpc_urls_and_adapter_sync_mode(
            repo_root,
            database_url,
            manifests_root,
            chain_rpc_urls,
            log_label,
            None,
        )
        .await
    }

    /// Start the production loop with live poll adapter sync enabled from the
    /// first poll. This keeps automatic normalized replay catch-up out of the
    /// assertion path so a test cannot pass after a later repair cycle.
    pub async fn start_with_live_poll_adapter_sync(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
        log_label: &str,
    ) -> Result<Self> {
        Self::start_with_chain_rpc_urls_and_adapter_sync_mode(
            repo_root,
            database_url,
            manifests_root,
            chain_rpc_urls,
            log_label,
            Some("auto"),
        )
        .await
    }

    async fn start_with_chain_rpc_urls_and_adapter_sync_mode(
        repo_root: &Path,
        database_url: &str,
        manifests_root: &Path,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
        log_label: &str,
        adapter_sync_mode: Option<&str>,
    ) -> Result<Self> {
        let indexer = &pipeline_binaries(repo_root).await?.indexer;
        // Output goes to a log file, not a pipe: the run loop can out-write an
        // undrained pipe buffer and block, deadlocking the session.
        let (log_path, log_file) = create_process_log_file("indexer", log_label)?;
        let chain_rpc_urls = format_chain_rpc_urls(chain_rpc_urls);
        let mut command = pipeline_command(repo_root, indexer);
        command
            .args(["run", "--database-url", database_url, "--manifests-root"])
            .arg(manifests_root)
            .args([
                "--chain-rpc-url",
                &chain_rpc_urls,
                "--poll-interval-secs",
                "1",
                // Scenario readiness often waits on a full-closure authority
                // sync round; the default 30s cadence just slows tests down.
                "--normalized-replay-catchup-poll-interval-secs",
                "2",
            ]);
        if let Some(adapter_sync_mode) = adapter_sync_mode {
            command.args(["--hash-pinned-adapter-sync", adapter_sync_mode]);
            command.env("BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_ENABLED", "false");
        }
        let child = command
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()
            .context("spawn indexer run")?;

        Ok(Self { child, log_path })
    }

    pub async fn wait_for_first_checkpoint(&mut self, pool: &sqlx::PgPool) -> Result<i64> {
        self.wait_for_first_chain_checkpoint(pool, "ethereum-mainnet")
            .await
    }

    pub async fn wait_for_first_chain_checkpoint(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
    ) -> Result<i64> {
        let ready_timeout_secs = ready_timeout_secs()?;
        let deadline = deadline_after(ready_timeout_secs, "indexer readiness")?;
        loop {
            self.bail_if_exited()?;
            let checkpoint = await_supervised_readiness(
                &mut self.child,
                &self.log_path,
                "indexer run",
                deadline,
                ready_timeout_secs,
                format!("indexer canonical checkpoint query for {chain}"),
                canonical_checkpoint(pool, chain),
            )
            .await?;
            if let Some(block) = checkpoint {
                return Ok(block);
            }
            self.bail_if_timed_out(
                deadline,
                ready_timeout_secs,
                "indexer run did not write a canonical checkpoint",
            )
            .await?;
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(500)),
            )
            .await;
        }
    }

    pub async fn wait_for_checkpoint(
        &mut self,
        pool: &sqlx::PgPool,
        target_block: u64,
        extra_ready_sql: Option<&str>,
    ) -> Result<()> {
        self.wait_for_chain_checkpoint(pool, "ethereum-mainnet", target_block, extra_ready_sql)
            .await
    }

    pub async fn wait_for_chain_checkpoint(
        &mut self,
        pool: &sqlx::PgPool,
        chain: &str,
        target_block: u64,
        extra_ready_sql: Option<&str>,
    ) -> Result<()> {
        // The binary is built before the session starts, so this deadline
        // measures intake readiness rather than workspace build-lock waits.
        let ready_timeout_secs = ready_timeout_secs()?;
        let deadline = deadline_after(ready_timeout_secs, "indexer readiness")?;
        loop {
            self.bail_if_exited()?;
            let checkpoint = await_supervised_readiness(
                &mut self.child,
                &self.log_path,
                "indexer run",
                deadline,
                ready_timeout_secs,
                format!("indexer canonical checkpoint query for {chain}"),
                canonical_checkpoint(pool, chain),
            )
            .await?;
            if checkpoint.is_some_and(|block| block >= target_block as i64) {
                let extra_ready = match extra_ready_sql {
                    Some(sql) => {
                        await_supervised_readiness(
                            &mut self.child,
                            &self.log_path,
                            "indexer run",
                            deadline,
                            ready_timeout_secs,
                            format!("indexer extra readiness SQL query for {chain}"),
                            async { Ok(sqlx::query_scalar::<_, bool>(sql).fetch_one(pool).await?) },
                        )
                        .await?
                    }
                    None => true,
                };
                if extra_ready {
                    return Ok(());
                }
            }
            self.bail_if_timed_out(
                deadline,
                ready_timeout_secs,
                &format!("indexer run did not reach canonical checkpoint {target_block}"),
            )
            .await?;
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(500)),
            )
            .await;
        }
    }

    pub async fn stop(self) -> Result<()> {
        stop_supervised_child(self.child, "indexer run", &self.log_path).await
    }

    fn bail_if_exited(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!(
                "indexer run exited early ({status}); log tail (reversed) from {:?}:\n{}",
                self.log_path,
                self.log_tail()
            );
        }
        Ok(())
    }

    async fn bail_if_timed_out(
        &mut self,
        deadline: tokio::time::Instant,
        timeout_secs: u64,
        message: &str,
    ) -> Result<()> {
        if tokio::time::Instant::now() >= deadline {
            return stop_after_readiness_deadline(
                &mut self.child,
                &self.log_path,
                timeout_secs,
                message,
            )
            .await;
        }
        Ok(())
    }

    fn log_tail(&self) -> String {
        process_log_tail(&self.log_path)
    }
}

async fn canonical_checkpoint(pool: &sqlx::PgPool, chain: &str) -> Result<Option<i64>> {
    Ok(bigname_storage::load_chain_checkpoint(pool, chain)
        .await?
        .and_then(|checkpoint| checkpoint.canonical_block_number))
}

pub async fn indexer_backfill(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    chain_rpc_url: &str,
    from_block: u64,
    to_block: u64,
    idempotency_key: &str,
) -> Result<String> {
    let chain_rpc_urls = [("ethereum-mainnet", chain_rpc_url)];
    indexer_backfill_with_chain_rpc_urls(
        repo_root,
        database_url,
        manifests_root,
        ChainBackfillTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: "ethereum-mainnet",
            block_range: from_block..=to_block,
            idempotency_key,
        },
    )
    .await
}

pub async fn indexer_backfill_with_chain_rpc_urls(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    target: ChainBackfillTarget<'_>,
) -> Result<String> {
    indexer_backfill_with_chain_rpc_urls_and_watch_targets(
        repo_root,
        database_url,
        manifests_root,
        target,
        &[],
    )
    .await
}

pub async fn indexer_backfill_watched_target_with_chain_rpc_urls(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    target: ChainBackfillTarget<'_>,
    watch_target: sqlx::types::Uuid,
) -> Result<String> {
    indexer_backfill_with_chain_rpc_urls_and_watch_targets(
        repo_root,
        database_url,
        manifests_root,
        target,
        &[watch_target],
    )
    .await
}

async fn indexer_backfill_with_chain_rpc_urls_and_watch_targets(
    repo_root: &Path,
    database_url: &str,
    manifests_root: &Path,
    target: ChainBackfillTarget<'_>,
    watch_targets: &[sqlx::types::Uuid],
) -> Result<String> {
    let indexer = &pipeline_binaries(repo_root).await?.indexer;
    let chain_rpc_urls = format_chain_rpc_urls(target.chain_rpc_urls);
    let from_block = target.block_range.start().to_string();
    let to_block = target.block_range.end().to_string();
    let mut command = pipeline_command(repo_root, indexer);
    command
        .args([
            "backfill",
            "--database-url",
            database_url,
            "--manifests-root",
        ])
        .arg(manifests_root)
        .args([
            "--chain-rpc-url",
            &chain_rpc_urls,
            "--chain",
            target.chain,
            "--from-block",
            &from_block,
            "--to-block",
            &to_block,
            "--idempotency-key",
            target.idempotency_key,
        ]);
    for watch_target in watch_targets {
        command.arg("--watch-target").arg(watch_target.to_string());
    }
    run_to_completion(command, "indexer backfill").await
}

/// Live intake session: the real `indexer run` poll loop against the local
/// chain. Unlike `backfill`, this is the path that promotes canonical chain
/// checkpoints, which snapshot-selecting API reads require. The process is
/// killed once the canonical checkpoint reaches the target block and the
/// caller's readiness query (a parameterless SQL statement returning one
/// boolean) is true — the latter guards against stopping intake before
/// adapters finish deriving the scenario's normalized events.
pub async fn indexer_run_until_checkpoint(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_url: &str,
    target_block: u64,
    extra_ready_sql: Option<&str>,
) -> Result<()> {
    let chain_rpc_urls = [("ethereum-mainnet", chain_rpc_url)];
    indexer_run_until_chain_checkpoint(
        repo_root,
        database_url,
        pool,
        manifests_root,
        ChainCheckpointTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: "ethereum-mainnet",
            target_block,
            extra_ready_sql,
        },
    )
    .await
}

pub async fn indexer_run_until_chain_checkpoint(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    target: ChainCheckpointTarget<'_>,
) -> Result<()> {
    let mut session = IndexerRunSession::start_with_chain_rpc_urls(
        repo_root,
        database_url,
        manifests_root,
        target.chain_rpc_urls,
        &target.target_block.to_string(),
    )
    .await?;
    session
        .wait_for_chain_checkpoint(
            pool,
            target.chain,
            target.target_block,
            target.extra_ready_sql,
        )
        .await?;
    session.stop().await
}

/// Run one live session over multiple chains and wait each chain's
/// canonical checkpoint sequentially; `extra_ready_sql` gates the final
/// wait.
pub async fn indexer_run_until_chain_checkpoints(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_urls: &[ChainRpcUrl<'_>],
    targets: &[(&str, u64)],
    extra_ready_sql: Option<&str>,
) -> Result<()> {
    let label = targets
        .iter()
        .map(|(_, block)| block.to_string())
        .collect::<Vec<_>>()
        .join("-");
    let mut session = IndexerRunSession::start_with_chain_rpc_urls(
        repo_root,
        database_url,
        manifests_root,
        chain_rpc_urls,
        &label,
    )
    .await?;
    for (index, (chain, target_block)) in targets.iter().enumerate() {
        let extra = if index + 1 == targets.len() {
            extra_ready_sql
        } else {
            None
        };
        session
            .wait_for_chain_checkpoint(pool, chain, *target_block, extra)
            .await?;
    }
    session.stop().await
}

pub struct RestartCompletion {
    pub target_block: u64,
    pub extra_ready_sql: Option<String>,
}

pub async fn indexer_run_restart_after_first_checkpoint<F, Fut>(
    repo_root: &Path,
    database_url: &str,
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    chain_rpc_url: &str,
    after_first_checkpoint: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<RestartCompletion>>,
{
    let mut first_session = IndexerRunSession::start(
        repo_root,
        database_url,
        manifests_root,
        chain_rpc_url,
        "restart-first",
    )
    .await?;
    first_session.wait_for_first_checkpoint(pool).await?;
    first_session.stop().await?;

    let completion = after_first_checkpoint().await?;
    indexer_run_until_checkpoint(
        repo_root,
        database_url,
        pool,
        manifests_root,
        chain_rpc_url,
        completion.target_block,
        completion.extra_ready_sql.as_deref(),
    )
    .await
}

/// Full-range normalized-event replay from stored raw facts — the operator
/// command that rebuilds adapter state under a complete closure boundary.
/// The active corpus registers under the `mainnet` profile (the generated
/// root mirrors the shipped mainnet manifests), so that is the profile the
/// replay must name.
pub async fn indexer_replay_normalized_events(
    repo_root: &Path,
    database_url: &str,
    to_block: u64,
) -> Result<String> {
    let indexer = &pipeline_binaries(repo_root).await?.indexer;
    let mut command = pipeline_command(repo_root, indexer);
    command.args([
        "replay",
        "normalized-events",
        "--database-url",
        database_url,
        "--deployment-profile",
        "mainnet",
        "--chain",
        "ethereum-mainnet",
        "--from-block",
        "0",
        "--to-block",
        &to_block.to_string(),
    ]);
    run_to_completion(command, "indexer replay normalized-events").await
}

pub async fn worker_replay_all_current_projections(
    repo_root: &Path,
    database_url: &str,
) -> Result<String> {
    let worker = &pipeline_binaries(repo_root).await?.worker;
    let mut command = pipeline_command(repo_root, worker);
    command.args([
        "replay",
        "all-current-projections",
        "--database-url",
        database_url,
    ]);
    run_to_completion(command, "worker replay all-current-projections").await
}

/// The shipped API binary serving HTTP on a local port; killed on drop.
pub struct ApiServer {
    _child: Child,
    pub base_url: String,
    http: reqwest::Client,
}

impl ApiServer {
    pub async fn start(repo_root: &Path, database_url: &str) -> Result<Self> {
        Self::start_with_chain_rpc_urls(repo_root, database_url, &[]).await
    }

    pub async fn start_with_chain_rpc_urls(
        repo_root: &Path,
        database_url: &str,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
    ) -> Result<Self> {
        let api = &pipeline_binaries(repo_root).await?.api;
        let ready_timeout_secs = ready_timeout_secs()?;
        let deadline = deadline_after(ready_timeout_secs, "API readiness")?;
        // The free port is released before the API binds it, so a parallel
        // external process can still steal it in the window. Harness-managed
        // Anvil/API starts share the startup lock only until the child listener
        // is observed; health waits and retries do not block unrelated starts.
        let mut last_error = None;
        for attempt in 1..=3 {
            match Self::try_start(
                repo_root,
                api,
                database_url,
                chain_rpc_urls,
                deadline,
                ready_timeout_secs,
            )
            .await
            {
                Ok(server) => return Ok(server),
                Err(error) => {
                    last_error =
                        Some(error.context(format!("API startup attempt {attempt}/3 failed")));
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                }
            }
        }
        let error = last_error.expect("at least one API start attempt ran");
        if tokio::time::Instant::now() >= deadline {
            return Err(error.context(format!(
                "API did not become ready within the configured {ready_timeout_secs}s readiness deadline"
            )));
        }
        Err(error)
    }

    async fn try_start(
        repo_root: &Path,
        api: &Path,
        database_url: &str,
        chain_rpc_urls: &[ChainRpcUrl<'_>],
        deadline: tokio::time::Instant,
        ready_timeout_secs: u64,
    ) -> Result<Self> {
        let _startup_guard = await_with_readiness_deadline(
            deadline,
            ready_timeout_secs,
            "API local-server startup lock wait",
            super::lock_local_server_start(),
        )
        .await?;
        let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let bind_addr = format!("127.0.0.1:{port}");
        let mut command = pipeline_command(repo_root, api);
        command.args([
            "serve",
            "--bind-addr",
            &bind_addr,
            "--database-url",
            database_url,
        ]);
        if !chain_rpc_urls.is_empty() {
            let chain_rpc_urls = format_chain_rpc_urls(chain_rpc_urls);
            command.args(["--chain-rpc-url", &chain_rpc_urls]);
        }
        let child = command
            .kill_on_drop(true)
            .spawn()
            .context("spawn bigname-api serve")?;
        let mut server = Self {
            _child: child,
            base_url: format!("http://{bind_addr}"),
            http: reqwest::Client::new(),
        };
        server
            .wait_until_listener_bound(port, deadline, ready_timeout_secs)
            .await?;
        drop(_startup_guard);
        server.wait_healthy(deadline, ready_timeout_secs).await?;
        Ok(server)
    }

    async fn wait_until_listener_bound(
        &mut self,
        port: u16,
        deadline: tokio::time::Instant,
        ready_timeout_secs: u64,
    ) -> Result<()> {
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        loop {
            if let Some(status) = self._child.try_wait()? {
                bail!(
                    "API exited before binding its listener at {} ({status})",
                    self.base_url
                );
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                bail!(
                    "API did not bind its listener at {} within the configured {ready_timeout_secs}s readiness deadline",
                    self.base_url
                );
            }
            let connect_timeout = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(20));
            if std::net::TcpStream::connect_timeout(&address, connect_timeout).is_ok() {
                if let Some(status) = self._child.try_wait()? {
                    bail!(
                        "API exited while binding its listener at {} ({status})",
                        self.base_url
                    );
                }
                return Ok(());
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(10)),
            )
            .await;
        }
    }

    async fn wait_healthy(
        &mut self,
        deadline: tokio::time::Instant,
        ready_timeout_secs: u64,
    ) -> Result<()> {
        // The binary is built before startup, so this loop measures process
        // health rather than workspace build-lock waits.
        loop {
            if let Some(status) = self._child.try_wait()? {
                bail!(
                    "API exited before becoming healthy at {} ({status})",
                    self.base_url
                );
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "API did not become healthy at {} within the configured {ready_timeout_secs}s readiness deadline",
                    self.base_url
                );
            }
            let response = await_with_readiness_deadline(
                deadline,
                ready_timeout_secs,
                format!("API health request at {}/healthz", self.base_url),
                self.http.get(format!("{}/healthz", self.base_url)).send(),
            )
            .await?;
            if response.is_ok_and(|response| response.status().is_success()) {
                if let Some(status) = self._child.try_wait()? {
                    bail!(
                        "API exited while its health endpoint responded at {} ({status})",
                        self.base_url
                    );
                }
                return Ok(());
            }
            tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(250)),
            )
            .await;
        }
    }

    pub async fn get_json(&self, path: &str) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let response = self
            .http
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .with_context(|| format!("GET {path}"))?;
        let status = response.status();
        let body = response
            .json()
            .await
            .with_context(|| format!("GET {path} body"))?;
        Ok((status, body))
    }

    pub async fn post_json(
        &self,
        path: &str,
        request: &serde_json::Value,
    ) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .json(request)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        let status = response.status();
        let body = response
            .json()
            .await
            .with_context(|| format!("POST {path} body"))?;
        Ok((status, body))
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::*;

    fn artifact_message(name: &str, manifest_path: &Path, executable: Option<Value>) -> String {
        let mut message = serde_json::json!({
            "reason": "compiler-artifact",
            "manifest_path": manifest_path,
            "target": {
                "kind": ["bin"],
                "name": name,
            },
        });
        if let Some(executable) = executable {
            message["executable"] = executable;
        }
        message.to_string()
    }

    fn expected_manifest(repo_root: &Path, name: &str) -> PathBuf {
        let (_, manifest) = PIPELINE_BINARY_SPECS
            .iter()
            .find(|(expected, _)| *expected == name)
            .expect("known pipeline binary");
        repo_root.join(*manifest)
    }

    #[test]
    fn parses_exact_pipeline_artifacts_and_normalizes_relative_executables() -> Result<()> {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-root");
        let custom_target = std::env::temp_dir()
            .join("custom target")
            .join("aarch64-unknown-linux-gnu")
            .join("debug");
        let api = custom_target.join("bigname-api");
        let indexer = custom_target.join("bigname-indexer");
        let relative_worker = PathBuf::from("relative-target/debug/bigname-worker");
        let messages = [
            // An exact binary name from another manifest must not be selected.
            artifact_message(
                "bigname-api",
                &repo_root.join("decoy/Cargo.toml"),
                Some(Value::Null),
            ),
            artifact_message(
                "bigname-api",
                &expected_manifest(&repo_root, "bigname-api"),
                Some(Value::String(api.to_string_lossy().into_owned())),
            ),
            artifact_message(
                "bigname-indexer",
                &expected_manifest(&repo_root, "bigname-indexer"),
                Some(Value::String(indexer.to_string_lossy().into_owned())),
            ),
            artifact_message(
                "bigname-worker",
                &expected_manifest(&repo_root, "bigname-worker"),
                Some(Value::String(
                    relative_worker.to_string_lossy().into_owned(),
                )),
            ),
            serde_json::json!({"reason": "build-finished", "success": true}).to_string(),
        ]
        .join("\n");

        let binaries = parse_pipeline_binaries(&repo_root, messages.as_bytes())?;
        assert_eq!(binaries.api, api);
        assert_eq!(binaries.indexer, indexer);
        assert_eq!(binaries.worker, repo_root.join(relative_worker));
        Ok(())
    }

    #[test]
    fn rejects_missing_pipeline_artifact() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-missing");
        let messages = [
            artifact_message(
                "bigname-api",
                &expected_manifest(&repo_root, "bigname-api"),
                Some(Value::String("api".to_string())),
            ),
            artifact_message(
                "bigname-indexer",
                &expected_manifest(&repo_root, "bigname-indexer"),
                Some(Value::String("indexer".to_string())),
            ),
        ]
        .join("\n");

        let error = parse_pipeline_binaries(&repo_root, messages.as_bytes())
            .expect_err("a missing worker artifact must fail");
        assert!(format!("{error:#}").contains("bigname-worker"));
    }

    #[test]
    fn rejects_null_or_omitted_pipeline_executable() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-null");
        for (executable, expected) in [
            (Some(Value::Null), "null executable"),
            (None, "omitted the executable"),
        ] {
            let message = artifact_message(
                "bigname-api",
                &expected_manifest(&repo_root, "bigname-api"),
                executable,
            );
            let error = parse_pipeline_binaries(&repo_root, message.as_bytes())
                .expect_err("a missing executable path must fail");
            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn rejects_duplicate_pipeline_artifacts() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-artifact-parser-duplicate");
        let api = artifact_message(
            "bigname-api",
            &expected_manifest(&repo_root, "bigname-api"),
            Some(Value::String("api".to_string())),
        );
        let messages = format!("{api}\n{api}");

        let error = parse_pipeline_binaries(&repo_root, messages.as_bytes())
            .expect_err("duplicate API artifacts must fail");
        assert!(format!("{error:#}").contains("duplicate executable artifacts for bigname-api"));
    }

    #[test]
    fn pipeline_command_uses_exact_binary_cwd_and_pool_limit() {
        let repo_root = std::env::temp_dir().join("bigname-e2e-command-root");
        let executable = std::env::temp_dir()
            .join("custom target")
            .join("debug")
            .join("bigname-api");
        let command = pipeline_command(&repo_root, &executable);
        let command = command.as_std();

        assert_eq!(command.get_program(), executable.as_os_str());
        assert_eq!(command.get_current_dir(), Some(repo_root.as_path()));
        assert_eq!(command.get_args().count(), 0);
        assert_eq!(
            command
                .get_envs()
                .find(|(name, _)| *name == OsStr::new("BIGNAME_DATABASE_MAX_CONNECTIONS"))
                .and_then(|(_, value)| value),
            Some(OsStr::new("4"))
        );
    }

    #[test]
    fn timeout_configuration_requires_positive_integer_seconds() {
        for variable in [
            "BIGNAME_E2E_READY_TIMEOUT_SECS",
            "BIGNAME_E2E_COMMAND_TIMEOUT_SECS",
        ] {
            assert_eq!(parse_timeout_secs(variable, "17").unwrap(), 17);
            for invalid in ["0", "-1", "1.5", "not-a-number"] {
                let error = parse_timeout_secs(variable, invalid)
                    .expect_err("invalid timeout must fail explicitly");
                assert!(format!("{error:#}").contains(variable), "{error:#}");
            }
        }
    }

    #[test]
    fn process_log_files_are_unique_for_repeated_labels() -> Result<()> {
        let (first_path, first_file) = create_process_log_file("indexer", "same/label")?;
        let (second_path, second_file) = create_process_log_file("indexer", "same/label")?;
        drop(first_file);
        drop(second_file);

        assert_ne!(first_path, second_path);
        assert!(
            first_path
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.contains("same-label"))
        );
        std::fs::remove_file(first_path).ok();
        std::fs::remove_file(second_path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn one_shot_command_deadline_stops_and_reaps_the_child() -> Result<()> {
        let label = "unit-one-shot-timeout";
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "printf 'deliberate-timeout-stdout\\n'; printf 'deliberate-timeout-stderr\\n' >&2; exec sleep 30",
        ]);

        let error = run_to_completion_with_timeout(command, label, 1)
            .await
            .expect_err("a long-running one-shot command must time out");
        let message = format!("{error:#}");
        assert!(
            message.contains(
                "exceeded the configured BIGNAME_E2E_COMMAND_TIMEOUT_SECS deadline of 1s"
            ),
            "{message}"
        );
        assert!(message.contains("stopped and reaped"), "{message}");
        assert!(message.contains("deliberate-timeout-stdout"), "{message}");
        assert!(message.contains("deliberate-timeout-stderr"), "{message}");

        let pid = std::process::id().to_string();
        for entry in std::fs::read_dir(std::env::temp_dir())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("bigname-e2e-command-")
                && name.contains(&pid)
                && name.contains(label)
            {
                std::fs::remove_file(entry.path()).ok();
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn one_shot_command_deadline_terminates_descendants() -> Result<()> {
        let label = "unit-one-shot-descendant-timeout";
        let (pid_path, pid_file) = create_process_log_file("descendant-pid", label)?;
        drop(pid_file);

        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 30 & child=$!; printf '%s\\n' \"$child\" > \"$1\"; wait")
            .arg("timeout-tree")
            .arg(&pid_path);

        let error = run_to_completion_with_timeout(command, label, 1)
            .await
            .expect_err("a command with a live descendant must time out");
        let message = format!("{error:#}");
        assert!(message.contains("process group"), "{message}");

        let descendant_pid = std::fs::read_to_string(&pid_path)?
            .trim()
            .parse::<u32>()
            .context("parse descendant pid")?;
        let mut descendant_exists = unix_process::process_exists(descendant_pid)?;
        for _ in 0..100 {
            if !descendant_exists {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            descendant_exists = unix_process::process_exists(descendant_pid)?;
        }
        if descendant_exists {
            unix_process::kill_process(descendant_pid).ok();
        }
        assert!(
            !descendant_exists,
            "descendant process {descendant_pid} survived the command timeout"
        );

        std::fs::remove_file(pid_path).ok();
        let harness_pid = std::process::id().to_string();
        for entry in std::fs::read_dir(std::env::temp_dir())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("bigname-e2e-command-")
                && name.contains(&harness_pid)
                && name.contains(label)
            {
                std::fs::remove_file(entry.path()).ok();
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn expired_readiness_deadline_bounds_a_pending_probe() {
        let expired = tokio::time::Instant::now() - Duration::from_millis(1);
        let error = await_with_readiness_deadline(
            expired,
            17,
            "deliberately pending readiness probe",
            std::future::pending::<()>(),
        )
        .await
        .expect_err("a readiness probe must not outlive its deadline");
        let message = format!("{error:#}");
        assert!(
            message.contains(
                "deliberately pending readiness probe exceeded the configured 17s readiness deadline"
            ),
            "{message}"
        );
    }

    #[tokio::test]
    async fn supervised_readiness_timeout_stops_and_reaps_child() -> Result<()> {
        let (log_path, log_file) =
            create_process_log_file("readiness-child", "unit-readiness-timeout")?;
        let mut child = Command::new("sh")
            .args(["-c", "exec sleep 30"])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;
        let expired = tokio::time::Instant::now() - Duration::from_millis(1);

        let error = await_supervised_readiness(
            &mut child,
            &log_path,
            "test readiness child",
            expired,
            23,
            "deliberately pending SQL readiness probe",
            std::future::pending::<Result<()>>(),
        )
        .await
        .expect_err("a timed-out readiness probe must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains(
                "deliberately pending SQL readiness probe exceeded the configured 23s readiness deadline"
            ),
            "{message}"
        );
        assert!(message.contains("stopped and reaped"), "{message}");
        assert!(child.id().is_none(), "the readiness child was not reaped");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stop_reports_a_child_that_crashed_after_readiness() -> Result<()> {
        let log_path =
            std::env::temp_dir().join(format!("bigname-e2e-stop-crash-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path)?;
        let child = Command::new("sh")
            .args(["-c", "echo deliberate-child-crash >&2; exit 17"])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let error = stop_supervised_child(child, "test child", &log_path)
            .await
            .expect_err("an already-crashed child must make stop fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited before requested stop"),
            "{message}"
        );
        assert!(message.contains("deliberate-child-crash"), "{message}");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stop_reports_a_child_that_crashes_between_status_check_and_kill() -> Result<()> {
        let log_path =
            std::env::temp_dir().join(format!("bigname-e2e-stop-race-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path)?;
        let child = Command::new("sh")
            .args([
                "-c",
                "sleep 0.2; echo deliberate-stop-race-crash >&2; exit 17",
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .spawn()?;

        let error = stop_supervised_child_with_pre_kill_delay(
            child,
            "test child",
            &log_path,
            Some(Duration::from_millis(500)),
        )
        .await
        .expect_err("a child crash racing with stop must not be accepted as a requested kill");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited independently while stop was requested"),
            "{message}"
        );
        assert!(message.contains("deliberate-stop-race-crash"), "{message}");
        std::fs::remove_file(log_path).ok();
        Ok(())
    }
}
