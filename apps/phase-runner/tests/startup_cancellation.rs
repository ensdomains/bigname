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
    let holder_url = writer.to_string();
    // Tag the runner's own connections so the lock probe below can tell this
    // process's waiter apart from other tests sharing the cluster.
    let application_name = format!("startup_cancellation_{}", std::process::id());
    writer
        .query_pairs_mut()
        .append_pair("application_name", &application_name);
    let runner_url = writer.to_string();

    // Hold the manifest synchronization lock, so the runner's own start-up blocks
    // inside `sync_loaded_manifests` exactly as it would beside a concurrent runner.
    let mut holder = PgConnection::connect_with(&holder_url.parse::<PgConnectOptions>()?).await?;
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
        .bind(MANIFEST_STARTUP_LOCK_NAME)
        .execute(&mut holder)
        .await?;

    // Noise: an unrelated advisory-lock waiter in the same cluster, standing in for a
    // concurrently running test. The probe below must not mistake it for this runner.
    const NOISE_KEY: i64 = 0x7374_6172_7475_7001;
    let mut noise_holder =
        PgConnection::connect_with(&holder_url.parse::<PgConnectOptions>()?).await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(NOISE_KEY)
        .execute(&mut noise_holder)
        .await?;
    // Two of them, so an unscoped probe cannot accidentally agree with the scoped
    // count of one and slip through as a false pass.
    let mut noise_waiters = Vec::new();
    for _ in 0..2 {
        let noise_url = holder_url.clone();
        noise_waiters.push(tokio::spawn(async move {
            let mut connection =
                PgConnection::connect_with(&noise_url.parse::<PgConnectOptions>()?).await?;
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(NOISE_KEY)
                .execute(&mut connection)
                .await?;
            anyhow::Ok(())
        }));
    }
    let noise_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let parked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
        )
        .fetch_one(database.pool())
        .await?;
        if parked >= 2 {
            break;
        }
        if Instant::now() >= noise_deadline {
            bail!("the unrelated waiters never parked");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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
        .env("BIGNAME_DATABASE_URL", &runner_url)
        .env(
            "BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL",
            &runner_url,
        )
        .env("STARTUP_CANCEL_RPC_URL", "http://127.0.0.1:1")
        .env("RUST_LOG", "phase_runner=info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("spawn phase-runner")?;

    // Wait until this runner is actually parked on the advisory lock; signalling
    // before that would prove nothing about the blocking wait. Scope the probe to
    // this database and this runner's own connections: the cluster is shared with
    // other tests, and some of them park advisory-lock waiters of their own. Every
    // other advisory lock the runner takes is `pg_try_advisory_lock`, which never
    // waits, so an ungranted one on these connections is the manifest-startup lock.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks lock_row
             JOIN pg_stat_activity activity ON activity.pid = lock_row.pid
             WHERE lock_row.locktype = 'advisory'
               AND NOT lock_row.granted
               AND activity.datname = current_database()
               AND activity.application_name = $1",
        )
        .bind(&application_name)
        .fetch_one(database.pool())
        .await?;
        if waiting > 0 {
            assert_eq!(
                waiting, 1,
                "the scoped probe must see only this runner's manifest-lock waiter"
            );
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

    // The filter is the point: the cluster now holds this runner's waiter and the
    // unrelated one above, and only the scoped count may see the runner's.
    let cluster_wide: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(
        cluster_wide > 2,
        "the unrelated waiters should be visible cluster-wide alongside the runner's, got {cluster_wide}"
    );

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
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(NOISE_KEY)
        .execute(&mut noise_holder)
        .await?;
    for waiter in noise_waiters {
        waiter.await??;
    }
    database.cleanup().await?;
    Ok(())
}
