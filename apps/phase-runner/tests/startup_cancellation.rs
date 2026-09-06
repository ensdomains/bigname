//! A stop that arrives while the runner is still starting must exit the process,
//! not be absorbed until the supervisor escalates to SIGKILL. Start-up holds no
//! phase loop yet, and manifest synchronization waits on a blocking
//! `pg_advisory_lock`, so this drives the real binary against a held lock.
#![cfg(unix)]

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use tokio::{process::Command, time::Instant};
use url::Url;

const MANIFEST_STARTUP_LOCK_NAME: &str = "phase-runner:manifest-startup";

#[tokio::test]
async fn a_stop_during_start_up_exits_instead_of_waiting_for_sigkill() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("startup_cancellation").pool_max_connections(2),
    )
    .await?;
    let mut writer = Url::parse(&database_url_from_env())?;
    writer.set_path(&format!("/{}", database.database_name()));
    let writer_url = writer.to_string();

    // Hold the manifest synchronization lock, so the runner's own start-up blocks
    // inside `sync_loaded_manifests` exactly as it would beside a concurrent runner.
    let mut holder = PgConnection::connect_with(&writer_url.parse::<PgConnectOptions>()?).await?;
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
        .bind(MANIFEST_STARTUP_LOCK_NAME)
        .execute(&mut holder)
        .await?;

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_phase-runner"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("BIGNAME_") {
            command.env_remove(key);
        }
    }
    let mut child = command
        .current_dir(&root)
        .arg("run")
        .args([
            "--chain",
            "ethereum-sepolia",
            "--source",
            "ethereum-sepolia:startupcancel:drpc:ethereum_head:0=STARTUP_CANCEL_RPC_URL",
            "--manifests-root",
            "manifests/sepolia",
            "--metrics-bind-addr",
            "127.0.0.1:0",
        ])
        .env("BIGNAME_DATABASE_URL", &writer_url)
        .env(
            "BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL",
            &writer_url,
        )
        .env("STARTUP_CANCEL_RPC_URL", "http://127.0.0.1:1")
        .env("RUST_LOG", "phase_runner=info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("spawn phase-runner")?;

    // Wait until the runner is actually parked on the advisory lock; signalling
    // before that would prove nothing about the blocking wait.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
        )
        .fetch_one(database.pool())
        .await?;
        if waiting > 0 {
            break;
        }
        if let Some(status) = child.try_wait()? {
            bail!("the runner exited before it reached the manifest lock: {status}");
        }
        if Instant::now() >= deadline {
            bail!("the runner never blocked on the manifest synchronization lock");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let pid = child.id().context("the runner has no pid")?;
    let killed = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await
        .context("raise SIGTERM")?;
    assert!(killed.success(), "kill -TERM failed: {killed}");

    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "the runner absorbed SIGTERM during start-up and had to be killed; \
                 a stop must not wait on the manifest lock"
            )
        })??;
    assert!(
        status.success(),
        "a stop during start-up must exit cleanly, got {status}"
    );

    sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
        .bind(MANIFEST_STARTUP_LOCK_NAME)
        .execute(&mut holder)
        .await?;
    database.cleanup().await?;
    Ok(())
}
