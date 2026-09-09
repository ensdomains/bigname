#![cfg(unix)]

use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::{process::Command, time::Instant};

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, pipeline, repo_root};
use pipeline::unix_process::{Signal, signal_process_group};

#[tokio::test]
async fn api_signals_drain_accepted_indexed_name_read() -> Result<()> {
    let mode = std::env::var("BIGNAME_E2E_SHUTDOWN_MODE").unwrap_or("direct".into());
    ensure!(
        matches!(mode.as_str(), "direct" | "compose"),
        "unsupported shutdown mode: {mode}"
    );
    let root = repo_root();
    let mut git = Command::new("git");
    git.current_dir(&root).args(["rev-parse", "HEAD"]);
    let sha = pipeline::run_to_completion(git, "shutdown source identity").await?;
    let sha = if mode == "compose" {
        std::env::var("BIGNAME_E2E_SHUTDOWN_SOURCE_SHA")
            .context("validated image source SHA required")?
    } else {
        sha
    };
    let sha = sha.trim();
    ensure!(
        sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid shutdown source SHA"
    );
    let evidence = std::env::var_os("BIGNAME_E2E_SHUTDOWN_EVIDENCE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("bigname-api-shutdown-{}", std::process::id()))
        });
    std::fs::create_dir_all(&evidence)?;
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let owner = rpc.accounts().await?[1];
    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "alice",
        owner,
        365 * 86400,
        deployment.public_resolver.address,
    )
    .await?;
    let run = support::ingest_and_serve(&anvil, &deployment, None).await?;
    let role = format!("api_shutdown_{}", std::process::id());
    // Use the documented serving grants, never schema-wide SELECT or ownership.
    let docs = std::fs::read_to_string(root.join("docs/deployment.md"))?;
    let grants = docs
        .split("CREATE ROLE bigname_api\n")
        .nth(1)
        .context("API grants missing")?
        .split("```")
        .next()
        .context("API grants terminator missing")?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&run.db.pool)
        .await?;
    let admin = PgPool::connect(&std::env::var("BIGNAME_DATABASE_URL")?).await?;
    sqlx::raw_sql(
        &format!("CREATE ROLE bigname_api\n{grants}")
            .replace("'<secret>'", "'shutdown-test-only'")
            .replace("ON DATABASE bigname ", &format!("ON DATABASE {database} "))
            .replace("bigname_api", &role),
    )
    .execute(&run.db.pool)
    .await?;
    let result = async {
        let mut api_url = reqwest::Url::parse(&run.db.url)?;
        api_url
            .set_username(&role)
            .map_err(|_| anyhow::anyhow!("API username"))?;
        api_url
            .set_password(Some("shutdown-test-only"))
            .map_err(|_| anyhow::anyhow!("API password"))?;
        let api_pool = PgPool::connect(api_url.as_str()).await?;
        for query in [
            "SELECT * FROM bigname_phase.raw_logs LIMIT 0",
            "UPDATE bigname_phase.name_current SET raw_name=raw_name WHERE false",
        ] {
            let error = sqlx::query(query)
                .execute(&api_pool)
                .await
                .err()
                .context("API privilege must be denied")?;
            ensure!(
                error.as_database_error().and_then(|e| e.code()).as_deref() == Some("42501"),
                "{error}"
            );
        }
        api_pool.close().await;
        let before = durable_rows(&run.db.pool).await?;
        save(&evidence, "rows-before", &before)?;
        let private_binary = if mode == "direct" {
            Some(support::TempDir::create()?)
        } else {
            None
        };
        let binary = if let Some(directory) = private_binary.as_ref() {
            let mut build = Command::new("cargo");
            build
                .current_dir(&root)
                .args([
                    "build",
                    "--locked",
                    "--message-format=json-render-diagnostics",
                    "--package",
                    "bigname-api",
                    "--bin",
                    "bigname-api",
                ])
                .env("BIGNAME_BUILD_SHA", sha);
            // Inherit Cargo's target settings so restored dependencies remain reusable.
            let lock_started = Instant::now();
            let build_lock = pipeline::lock_api_build(&root).await?;
            let lock_elapsed = lock_started.elapsed();
            let build_started = Instant::now();
            let output = pipeline::run_to_completion(build, "shutdown API build").await?;
            let build_elapsed = build_started.elapsed();
            let mut executable = None;
            for line in output.lines() {
                let message: Value = serde_json::from_str(line)?;
                if message["reason"] == "compiler-artifact"
                    && message["target"]["name"] == "bigname-api"
                    && message["target"]["kind"]
                        .as_array()
                        .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
                    && message["manifest_path"].as_str().map(Path::new)
                        == Some(root.join("apps/api/Cargo.toml").as_path())
                {
                    let path = message["executable"]
                        .as_str()
                        .context("Cargo omitted the shutdown API executable")?;
                    ensure!(
                        executable.replace(root.join(path)).is_none(),
                        "Cargo reported duplicate shutdown API executables"
                    );
                }
            }
            let executable = executable.context("Cargo did not produce the shutdown API")?;
            let snapshot = directory.path().join("bigname-api");
            // Hash and run a test-owned copy, never the mutable Cargo output path.
            std::fs::copy(&executable, &snapshot)?;
            drop(build_lock);
            save(
                &evidence,
                "native-build-timing",
                &json!({"source_sha": sha, "lock_wait_ms": lock_elapsed.as_millis(),
                    "build_ms": build_elapsed.as_millis()}),
            )?;
            snapshot
        } else {
            root.join("target")
                .join(format!("shutdown-api-{sha}/debug/bigname-api"))
        };
        let mut digest = Command::new("sha256sum");
        digest.arg(&binary);
        let binary_hash = if mode == "direct" {
            pipeline::run_to_completion(digest, "shutdown API digest").await?
        } else {
            std::env::var("BIGNAME_E2E_SHUTDOWN_IMAGE")?
        };
        save(
            &evidence,
            "identity",
            &json!({"source_sha":sha,"artifact":binary_hash,"mode":mode,
        "database":database,"fixture_phase_extents_seeded":true}),
        )?;
        let base = std::env::var_os("BIGNAME_E2E_SHUTDOWN_BASE").is_some();
        let mut result = Ok(());
        for (signal, name) in [
            (Signal::Interrupt, "sigint"),
            (Signal::Terminate, "sigterm"),
        ] {
            if mode == "compose" && name == "sigint" {
                continue;
            }
            if let Err(error) = drain(
                &binary,
                api_url.as_str(),
                &run.db.pool,
                sha,
                signal,
                name,
                base,
                &evidence,
                &mode,
            )
            .await
            {
                result = Err(error);
                break;
            }
        }
        let after = durable_rows(&run.db.pool).await?;
        save(&evidence, "rows-after", &after)?;
        ensure!(before == after, "API shutdown changed durable rows");
        result
    }
    .await;
    let cleanup = run.db.cleanup().await;
    let role_cleanup = sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup?;
    role_cleanup?;
    result
}

async fn durable_rows(pool: &PgPool) -> Result<Value> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename FROM pg_tables WHERE schemaname='bigname_phase' ORDER BY tablename",
    )
    .fetch_all(pool)
    .await?;
    let parts = tables.iter().map(|table| format!("SELECT '{table}' AS name, (SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]') FROM bigname_phase.\"{table}\" r) AS rows"))
        .collect::<Vec<_>>().join(" UNION ALL ");
    Ok(sqlx::query_scalar(&format!(
        "SELECT jsonb_object_agg(name,rows) FROM ({parts}) r"
    ))
    .fetch_one(pool)
    .await?)
}

fn save(directory: &Path, name: &str, value: &Value) -> Result<()> {
    std::fs::write(
        directory.join(format!("{name}.json")),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

async fn response(url: String) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(90))
        .build()?;
    let response = client.get(url).send().await?;
    ensure!(
        response.status() == 200,
        "indexed HTTP status {}",
        response.status()
    );
    Ok(response.json().await?)
}

#[allow(clippy::too_many_arguments)] // One explicit invocation per process/signal case.
async fn drain(
    binary: &Path,
    database_url: &str,
    pool: &PgPool,
    sha: &str,
    signal: Signal,
    name: &str,
    base: bool,
    evidence: &Path,
    mode: &str,
) -> Result<()> {
    let extended = std::env::var_os("BIGNAME_E2E_SHUTDOWN_EXTENDED").is_some();
    ensure!(
        !extended || mode == "compose",
        "extended drain requires Compose"
    );
    let _start = crate::harness::lock_local_server_start().await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let metrics = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let metrics_address = metrics.local_addr()?;
    let log_path = evidence.join(format!("{name}.log"));
    let log = std::fs::File::create(&log_path)?;
    let mut command = if mode == "direct" {
        let mut command = Command::new(binary);
        command.arg("serve");
        command
    } else {
        let mut command = Command::new(repo_root().join("scripts/test-container-shutdown"));
        command
            .args([
                "--serve",
                database_url,
                &address.to_string(),
                &metrics_address.to_string(),
            ])
            .arg(evidence);
        command
    };
    command
        .env("BIGNAME_DATABASE_URL", database_url)
        .env("BIGNAME_API_BIND_ADDR", address.to_string())
        .env("BIGNAME_API_METRICS_BIND_ADDR", metrics_address.to_string())
        .env("BIGNAME_LOG_JSON", "1")
        .env("RUST_LOG", "info")
        .kill_on_drop(true);
    if extended {
        command
            .env("BIGNAME_API_REQUEST_TIMEOUT_MS", "60000")
            .env("BIGNAME_API_DB_STATEMENT_TIMEOUT_MS", "55000")
            .env("BIGNAME_API_STOP_GRACE_MS", "75000");
    }
    if mode == "direct" && name == "sigint" {
        let path = evidence.join("occupied-bind.log");
        let log = std::fs::File::create(&path)?;
        let mut probe = command.stdout(log.try_clone()?).stderr(log).spawn()?;
        let status = match tokio::time::timeout(Duration::from_secs(10), probe.wait()).await {
            Ok(status) => status?,
            Err(_) => {
                probe.start_kill()?;
                tokio::time::timeout(Duration::from_secs(5), probe.wait()).await??;
                anyhow::bail!("occupied-bind probe timed out");
            }
        };
        let log = std::fs::read_to_string(path)?;
        save(
            evidence,
            "occupied-bind",
            &json!({"exit":status.to_string(),"log":log}),
        )?;
        ensure!(
            !status.success() && log.contains("failed to bind the API listener"),
            "wrong bind failure: {log}"
        );
        for forbidden in [
            "API booted",
            "graceful_shutdown",
            "shutdown signal received",
        ] {
            ensure!(!log.contains(forbidden), "bind error mislabeled: {log}");
        }
    }
    drop((listener, metrics));
    let mut child = command
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .process_group(0)
        .kill_on_drop(true)
        .spawn()?;
    let pid = child.id().context("API pid")?;
    let mut stop = None;
    let result = async {
        let deadline = Instant::now() + Duration::from_secs(30);
        let boot = loop {
            let lines = std::fs::read_to_string(&log_path)?;
            if let Some(boot) = lines.lines().filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .find(|v| v["fields"]["message"] == "API booted") { break boot; }
            ensure!(child.try_wait()?.is_none(), "API exited before boot: {lines}");
            ensure!(Instant::now() < deadline, "API boot deadline");
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        drop(_start);
        ensure!(boot["fields"]["build_sha"] == sha, "wrong API build");
        ensure!(boot["fields"]["request_timeout_ms"] == if extended { 60000 } else { 30000 } && boot["fields"]["db_statement_timeout_ms"] == if extended { 55000 } else { 25000 }, "timeouts changed");
        let hashes: Vec<String> = sqlx::query_scalar("SELECT input_content_hash FROM chain_phase_state WHERE phase_name IN ('interpret','project') AND phase_status='completed'")
            .fetch_all(pool).await?;
        ensure!(hashes.len() == 2 && hashes.iter().all(|h| boot["fields"]["interpreter_content_hash"] == *h), "producer/API generation mismatch");
        let url = format!("http://{address}/v2/names/alice.eth?source=indexed");
        let baseline = response(url.clone()).await?;
        ensure!(baseline["data"]["name"] == "alice.eth" && baseline["meta"]["source"] == "indexed", "wrong indexed identity");
        save(evidence, &format!("{name}-baseline"), &baseline)?;
        let mut gate = pool.begin().await?;
        sqlx::query("LOCK TABLE bigname_phase.name_current IN ACCESS EXCLUSIVE MODE").execute(&mut *gate).await?;
        let owner: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *gate).await?;
        let started: String = sqlx::query_scalar("SELECT clock_timestamp()::text").fetch_one(pool).await?;
        let request = response(url);
        tokio::pin!(request);
        let deadline = Instant::now() + Duration::from_secs(10);
        let waiting: Value = loop {
            let waiting: Value = sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(r)),'[]') FROM (SELECT a.pid,a.application_name,a.query_start,a.wait_event_type,a.wait_event,l.relation::regclass::text,l.mode,l.granted FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() AND a.application_name='bigname-api' AND a.query_start >= $1::text::timestamptz AND l.relation='bigname_phase.name_current'::regclass AND NOT l.granted) r")
                .bind(&started).fetch_one(pool).await?;
            if waiting.as_array().is_some_and(|v| !v.is_empty()) { break waiting; }
            ensure!(Instant::now() < deadline, "request never entered real relation lock");
            tokio::select! { value = &mut request => anyhow::bail!("request ended before lock: {value:?}"), _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
        };
        tokio::select! { biased; value = &mut request => anyhow::bail!("request ended before signal: {value:?}"), _ = std::future::ready(()) => {} }
        save(evidence, &format!("{name}-lock"), &json!({"owner":owner,"started":started,"waiting":waiting}))?;
        let sent = Instant::now();
        if mode == "direct" { signal_process_group(pid, signal)?; } else {
            let project=std::fs::read_to_string(evidence.join("project"))?;
            stop=Some(Command::new("docker").args(["compose","-p",project.trim(),"-f"]).arg(evidence.join("compose.json")).args(["stop","api"]).kill_on_drop(true).spawn()?);
        }
        let deadline = sent + Duration::from_secs(5);
        loop {
            let log = std::fs::read_to_string(&log_path)?;
            let accepted = log.lines().filter_map(|line| serde_json::from_str::<Value>(line).ok()).any(|v|
                v["fields"]["signal"] == name && v["fields"]["action"] == "graceful_shutdown"
                || base && name == "sigint" && v["fields"]["message"] == "shutdown signal received");
            if accepted { break; }
            if let Some(status) = child.try_wait()? {
                gate.rollback().await?;
                let outcome = request.await;
                save(evidence, &format!("{name}-early-exit"), &json!({"exit":status.to_string(),"request_error":outcome.as_ref().err().map(ToString::to_string),"accepted_signal":false}))?;
                anyhow::bail!("API exited without {name} acceptance: {status}; request={outcome:?}");
            }
            ensure!(Instant::now() < deadline, "no {name} acceptance");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            if tokio::net::TcpStream::connect(address).await.is_err() { break; }
            ensure!(Instant::now() < deadline, "API listener remains open after {name}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::select! { biased; value = &mut request => anyhow::bail!("request ended before release: {value:?}"), _ = std::future::ready(()) => {} }
        if extended {
            tokio::select! { value = &mut request => anyhow::bail!("request ended during extended drain: {value:?}"), _ = tokio::time::sleep_until(sent + Duration::from_secs(48)) => {} }
            ensure!(child.try_wait()?.is_none(), "API died before extended lock release");
        }
        gate.commit().await?;
        let body = tokio::time::timeout(Duration::from_secs(10), request).await??;
        ensure!(body == baseline, "drained response differs from baseline");
        let status = tokio::time::timeout(Duration::from_secs(10), child.wait()).await??;
        save(evidence, &format!("{name}-result"), &json!({"response":body,"exit":status.to_string(),"elapsed_seconds":sent.elapsed().as_secs_f64()}))?;
        ensure!(status.success() && sent.elapsed() < Duration::from_secs(if extended { 75 } else { 45 }), "non-graceful API exit: {status}");
        Ok(())
    }.await;
    let stopped: Result<()> = async {
        if let Some(mut stop) = stop {
            match tokio::time::timeout(Duration::from_secs(50), stop.wait()).await {
                Ok(status) => ensure!(status?.success(), "Compose stop failed"),
                Err(_) => {
                    stop.kill().await?;
                    anyhow::bail!("Compose stop timed out");
                }
            }
        }
        Ok(())
    }
    .await;
    let container_cleanup: Result<()> = async {
        if mode == "compose" && evidence.join("project").exists() {
            let project = std::fs::read_to_string(evidence.join("project"))?;
            let mut cleanup = Command::new("docker");
            cleanup
                .args(["compose", "-p", project.trim(), "-f"])
                .arg(evidence.join("compose.json"))
                .args(["down", "--volumes", "--remove-orphans"]);
            tokio::time::timeout(
                Duration::from_secs(50),
                pipeline::run_to_completion(cleanup, "shutdown container cleanup"),
            )
            .await??;
        }
        Ok(())
    }
    .await;
    let child_cleanup: Result<()> = async {
        if child.try_wait()?.is_none() {
            signal_process_group(pid, Signal::Kill)?;
            tokio::time::timeout(Duration::from_secs(5), child.wait()).await??;
        }
        Ok(())
    }
    .await;
    stopped
        .and(container_cleanup)
        .and(child_cleanup)
        .and(result)
}
