//! Ordinary Sepolia CLI proof. Only database/role setup uses administrative SQL;
//! all lineage, cursors, phase lifecycle and redo state come from the binary.

#[path = "support/rewind_fixture.rs"]
mod rewind_fixture;

use std::{fs, net::SocketAddr, path::PathBuf, process::Command, time::Instant};

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use url::Url;
use uuid::Uuid;

use rewind_fixture::{
    ANCESTOR, CHAIN, CHECKPOINT, DEADLINE, FINALIZED, Gate, HEAD, OwnedChild, POLL, RpcFixture,
    SAFE, hash, no_writer_sessions, require_resumed_headers, save, snapshot, wait_for_gate,
};

struct Fixture {
    database: TestDatabase,
    writer_url: String,
    reader_url: String,
    reader_role: String,
    directory: PathBuf,
    root: PathBuf,
    rpc: RpcFixture,
    children: Vec<OwnedChild>,
}

impl Fixture {
    async fn create() -> Result<Self> {
        let directory = std::env::var_os("BIGNAME_REWIND_EVIDENCE_DIR")
            .map_or_else(std::env::temp_dir, PathBuf::from)
            .join(format!("redo554-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&directory)?;
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()?;
        let profile_hash =
            bigname_content_hash::manifest_profile_hash(root.join("manifests/sepolia"))?;
        ensure!(
            bigname_content_hash::HASHED_MANIFEST_PROFILES
                .iter()
                .any(|(name, hash)| { *name == "sepolia" && *hash == profile_hash }),
            "shipped Sepolia profile is not covered by this build"
        );
        save(
            &directory,
            "identity.json",
            &json!({
                "binary": env!("CARGO_BIN_EXE_phase-runner"), "build_sha": phase_runner::BUILD_SHA,
                "interpreter_hash": bigname_content_hash::INTERPRETER_CONTENT_HASH,
                "profile": "manifests/sepolia", "profile_hash": profile_hash,
                "source": "ethereum-sepolia:redo554:drpc:ethereum_head:0=REDO554_RPC_URL"
            }),
        )?;
        let mut writer = Url::parse(&database_url_from_env())?;
        let rpc = RpcFixture::start(&directory).await?;
        let database =
            TestDatabase::create(TestDatabaseConfig::new("redo554_cli").pool_max_connections(2))
                .await?;
        writer.set_path(&format!("/{}", database.database_name()));
        let reader_role = format!("redo554_{}", Uuid::new_v4().simple());
        writer
            .query_pairs_mut()
            .append_pair("application_name", &reader_role);
        Ok(Self {
            database,
            writer_url: writer.to_string(),
            reader_url: String::new(),
            reader_role,
            directory,
            root,
            rpc,
            children: Vec::new(),
        })
    }

    fn start(&mut self, label: &str, operation: &str, args: &[&str]) -> Result<usize> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_phase-runner"));
        // Exclude inherited source/profile/capacity overrides from this controlled run.
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("BIGNAME_") {
                command.env_remove(key);
            }
        }
        command
            .current_dir(&self.root)
            .arg(operation)
            .args(args)
            .env("BIGNAME_DATABASE_URL", &self.writer_url)
            .env("RUST_LOG", "phase_runner=info")
            .env("REDO554_RPC_URL", &self.rpc.endpoint);
        if matches!(operation, "run" | "redo") {
            command.args([
                "--chain",
                CHAIN,
                "--source",
                "ethereum-sepolia:redo554:drpc:ethereum_head:0=REDO554_RPC_URL",
                "--manifests-root",
                "manifests/sepolia",
                "--metrics-bind-addr",
                "127.0.0.1:0",
                "--live-poll-ms",
                "25",
            ]);
        }
        if operation == "run" {
            command.env(
                "BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL",
                &self.reader_url,
            );
        }
        let index = self.children.len();
        self.children
            .push(OwnedChild::start(command, &self.directory, label)?);
        Ok(index)
    }

    async fn initialize(&mut self) -> Result<()> {
        let init = self.start("init", "init-schema", &[])?;
        ensure!(
            self.children[init].wait().await?.success(),
            "ordinary init-schema failed"
        );
        let role = &self.reader_role;
        let password = Uuid::new_v4().simple().to_string();
        let db = self.database.database_name();
        // All interpolated identifiers/passwords are generated alphanumeric values.
        let sql = format!(
            "CREATE ROLE {role} LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB
             NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
             REVOKE CREATE ON SCHEMA public FROM PUBLIC;
             REVOKE CREATE ON DATABASE \"{db}\" FROM PUBLIC;
             GRANT CONNECT ON DATABASE \"{db}\" TO {role};
             GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {role};
             GRANT USAGE ON SCHEMA bigname_phase TO {role};
             GRANT SELECT ON ALL TABLES IN SCHEMA bigname_phase TO {role};"
        );
        let mut transaction = self.database.pool().begin().await?;
        sqlx::raw_sql(&sql).execute(&mut *transaction).await?;
        transaction.commit().await?;
        let mut reader = Url::parse(&self.writer_url)?;
        reader
            .set_username(role)
            .map_err(|()| anyhow::anyhow!("reader URL cannot hold username"))?;
        reader
            .set_password(Some(&password))
            .map_err(|()| anyhow::anyhow!("reader URL cannot hold password"))?;
        self.reader_url = reader.to_string();
        let identity: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object('database', current_database(), 'database_oid',
                (SELECT oid::text FROM pg_database WHERE datname = current_database()),
                'system_identifier', (SELECT system_identifier::text FROM pg_control_system()))",
        )
        .fetch_one(self.database.pool())
        .await?;
        save(&self.directory, "database-identity.json", &identity)
    }

    async fn cleanup(mut self) -> Result<()> {
        let mut failures = Vec::new();
        for child in &mut self.children {
            if let Err(error) = child.terminate().await {
                failures.push(error.to_string());
            }
        }
        self.rpc.disarm();
        if let Err(error) = no_writer_sessions(self.database.pool(), &self.reader_role).await {
            failures.push(error.to_string());
        }
        let role = &self.reader_role;
        let role_cleanup: Result<()> = async {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
                    .bind(role)
                    .fetch_one(self.database.pool())
                    .await?;
            if exists {
                sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
                    .execute(self.database.pool())
                    .await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = role_cleanup {
            failures.push(error.to_string());
        }
        if let Err(error) = self.rpc.stop().await {
            failures.push(error.to_string());
        }
        if let Err(error) = self.database.cleanup().await {
            failures.push(error.to_string());
        }
        save(
            &self.directory,
            "cleanup.json",
            &json!({"failures": failures}),
        )?;
        ensure!(failures.is_empty(), "fixture cleanup failed: {failures:?}");
        Ok(())
    }
}

fn phase<'a>(snapshot: &'a Value, name: &str) -> Result<&'a Value> {
    snapshot["chain_phase_state"]
        .as_array()
        .context("missing phase rows")?
        .iter()
        .find(|row| row["phase_name"] == name)
        .context("phase row absent")
}

fn require_heads(snapshot: &Value, latest: i64) -> Result<()> {
    let heads = &snapshot["chain_heads"][0];
    for (key, number) in [("latest", latest), ("safe", SAFE), ("finalized", FINALIZED)] {
        ensure!(
            heads[format!("{key}_block_number")] == number,
            "wrong {key} height: {heads}"
        );
        ensure!(
            heads[format!("{key}_block_hash")] == hash(number),
            "wrong {key} hash"
        );
    }
    Ok(())
}

async fn observe_cancellation(child: &mut OwnedChild, gate: &Gate) -> Result<()> {
    let log = fs::read_to_string(&child.log)?;
    let address: SocketAddr = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|line| {
            line["fields"]["metrics_bind_addr"]
                .as_str()
                .map(str::to_owned)
        })
        .context("metrics listener address absent from child log")?
        .parse()?;
    let connect = || {
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            TcpStream::connect(address),
        )
    };
    let connection = connect()
        .await
        .context("metrics connect timed out")?
        .context("metrics listener was not accepting before SIGINT")?;
    drop(connection);
    child.interrupt().await?;
    loop {
        gate.require_held()?;
        ensure!(
            child.running()?,
            "setup child exited before final batch was released"
        );
        match connect().await.context("metrics connect timed out")? {
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => break,
            Err(error) => return Err(error).context("unexpected metrics connection error"),
            Ok(connection) => drop(connection),
        }
        tokio::time::sleep(POLL).await;
    }
    ensure!(
        !fs::read_to_string(&child.log)?.contains("metrics listener exited"),
        "metrics failed independently of cancellation"
    );
    Ok(())
}

async fn scenario(fixture: &mut Fixture) -> Result<()> {
    fixture
        .initialize()
        .await
        .context("prerequisite: schema and SELECT-only role")?;
    let normal_gate = fixture.rpc.arm("normal-setup");
    let normal = fixture.start("normal-setup", "run", &[])?;
    wait_for_gate(&mut fixture.children[normal], &normal_gate)
        .await
        .context("prerequisite: final normal Ingest window")?;
    observe_cancellation(&mut fixture.children[normal], &normal_gate).await?;
    save(
        &fixture.directory,
        "setup-cancellation.json",
        &json!({"metrics_closed_while_child_alive": true}),
    )?;
    normal_gate.require_held()?;
    normal_gate.release();
    ensure!(
        fixture.children[normal].wait().await?.success(),
        "normal setup did not exit cleanly"
    );
    fixture.rpc.disarm();
    no_writer_sessions(fixture.database.pool(), &fixture.reader_role).await?;
    let setup = snapshot(fixture.database.pool(), &fixture.directory, "setup.json").await?;
    require_heads(&setup, HEAD)?;
    let ingest = phase(&setup, "ingest")?;
    ensure!(
        ingest["phase_status"] == "completed"
            && ingest["live_handoff_block_number"] == HEAD
            && ingest["redo_in_progress"] == false,
        "normal Ingest did not complete without redo ownership: {ingest}"
    );
    for name in ["interpret", "project", "verify", "live"] {
        ensure!(
            phase(&setup, name)?["phase_status"] == "idle",
            "setup started downstream phase {name}"
        );
    }

    let redo_args = [
        "--phase",
        "ingest",
        "--from-block",
        "0",
        "--to-block",
        "511",
    ];
    let redo_gate = fixture.rpc.arm("interrupted-redo");
    let redo = fixture.start("interrupted-redo", "redo", &redo_args)?;
    wait_for_gate(&mut fixture.children[redo], &redo_gate)
        .await
        .context("prerequisite: real second redo window")?;
    let checkpoint = snapshot(
        fixture.database.pool(),
        &fixture.directory,
        "committed-checkpoint.json",
    )
    .await?;
    let ingest = phase(&checkpoint, "ingest")?;
    ensure!(
        ingest["redo_in_progress"] == true
            && ingest["redo_from_block_number"] == 0
            && ingest["redo_to_block_number"] == HEAD
            && ingest["redo_current_block_number"] == CHECKPOINT
            && ingest["redo_current_block_hash"] == hash(CHECKPOINT)
            && ingest["redo_target_block_number"] == HEAD
            && ingest["redo_target_block_hash"] == hash(HEAD)
            && ingest["redo_attempt_generation"]
                .as_i64()
                .is_some_and(|generation| generation > 0)
            && ingest["redo_manifest_authority_fingerprint"]
                .as_str()
                .is_some()
            && !ingest["last_error"]
                .as_str()
                .unwrap_or_default()
                .starts_with("required downstream redo"),
        "prerequisite: ordinary redo has no real non-final committed checkpoint: {ingest}"
    );
    redo_gate.require_held()?;
    fixture.children[redo].terminate().await?;
    fixture.rpc.disarm();
    no_writer_sessions(fixture.database.pool(), &fixture.reader_role).await?;
    fixture.rpc.require_expected_requests()?;
    let before = snapshot(
        fixture.database.pool(),
        &fixture.directory,
        "before-refusal.json",
    )
    .await?;
    require_heads(&before, HEAD)?;
    let ancestor_hash = hash(ANCESTOR);
    let rewind_args = [
        "--chain",
        CHAIN,
        "--ancestor-block",
        "128",
        "--ancestor-hash",
        &ancestor_hash,
    ];
    let rewind = fixture.start("rewind-refusal", "rewind", &rewind_args)?;
    let refused = fixture.children[rewind].wait().await?;
    no_writer_sessions(fixture.database.pool(), &fixture.reader_role).await?;
    let after = snapshot(
        fixture.database.pool(),
        &fixture.directory,
        "after-refusal.json",
    )
    .await?;
    // On unchanged production source this is the controlling failing-first assertion.
    ensure!(
        !refused.success(),
        "#554 baseline failure: ordinary rewind orphaned a retained redo end; evidence in {}",
        fixture.directory.display()
    );
    let error = fs::read_to_string(&fixture.children[rewind].log)?.to_lowercase();
    for required in [
        "interrupted",
        "128",
        "configured sources",
        "redo --chain ethereum-sepolia --phase ingest --from-block 0 --to-block 511",
    ] {
        ensure!(
            error.contains(required),
            "rewind failed for another reason: missing {required}; see child log"
        );
    }
    ensure!(before == after, "refused rewind mutated retained state");

    fixture.rpc.stage("repair");
    let retry = fixture.start("repair", "redo", &redo_args)?;
    ensure!(
        fixture.children[retry].wait().await?.success(),
        "ordinary covering repair failed"
    );
    no_writer_sessions(fixture.database.pool(), &fixture.reader_role).await?;
    require_resumed_headers(&fixture.directory)?;
    let repaired = snapshot(fixture.database.pool(), &fixture.directory, "repaired.json").await?;
    ensure!(
        phase(&repaired, "ingest")?["redo_in_progress"] == false,
        "repair retained Ingest work"
    );
    fixture.rpc.stage("downstream");
    let downstream = fixture.start("downstream", "run", &[])?;
    let until = Instant::now() + DEADLINE;
    loop {
        let done: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM bigname_phase.chain_phase_state
            WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
            AND current_block_number = $2 AND NOT redo_in_progress",
        )
        .bind(CHAIN)
        .bind(HEAD)
        .fetch_one(fixture.database.pool())
        .await?;
        if done == 2 {
            break;
        }
        ensure!(
            fixture.children[downstream].running()?,
            "ordinary downstream run exited before processing suffix"
        );
        ensure!(
            Instant::now() < until,
            "ordinary downstream processing did not finish"
        );
        tokio::time::sleep(POLL).await;
    }
    fixture.children[downstream].interrupt().await?;
    ensure!(
        fixture.children[downstream].wait().await?.success(),
        "downstream run did not stop cleanly"
    );
    no_writer_sessions(fixture.database.pool(), &fixture.reader_role).await?;
    let resolved = snapshot(
        fixture.database.pool(),
        &fixture.directory,
        "before-successful-rewind.json",
    )
    .await?;
    require_heads(&resolved, HEAD)?;
    let ordinary = fixture.start("rewind-after-repair", "rewind", &rewind_args)?;
    ensure!(
        fixture.children[ordinary].wait().await?.success(),
        "identical rewind failed after ordinary repair"
    );
    no_writer_sessions(fixture.database.pool(), &fixture.reader_role).await?;
    let rewound = snapshot(
        fixture.database.pool(),
        &fixture.directory,
        "after-successful-rewind.json",
    )
    .await?;
    require_heads(&rewound, ANCESTOR)?;
    let old_epoch = resolved["chain_heads"][0]["lineage_orphaning_epoch"]
        .as_i64()
        .context("epoch absent")?;
    ensure!(
        rewound["chain_heads"][0]["lineage_orphaning_epoch"] == old_epoch + 1,
        "orphaning epoch did not advance once"
    );
    for row in rewound["chain_lineage"]
        .as_array()
        .context("lineage absent")?
    {
        if row["block_number"]
            .as_i64()
            .context("lineage height absent")?
            > ANCESTOR
        {
            ensure!(
                row["canonicality_state"] == "orphaned",
                "displaced suffix remains readable"
            );
        }
    }
    for table in [
        "chain_header_audit",
        "raw_transactions",
        "raw_receipts",
        "raw_logs",
    ] {
        ensure!(
            resolved[table] == rewound[table],
            "rewind changed immutable {table}"
        );
    }
    for name in ["interpret", "project"] {
        let row = phase(&rewound, name)?;
        ensure!(
            row["redo_in_progress"] == true
                && row["redo_from_block_number"] == ANCESTOR + 1
                && row["redo_to_block_number"] == HEAD,
            "rewind did not stamp the actual {name} suffix: {row}"
        );
    }
    ensure!(
        phase(&resolved, "verify")? == phase(&rewound, "verify")?,
        "rewind changed Verify below the displaced suffix"
    );
    fixture.rpc.require_expected_requests()?;
    save(
        &fixture.directory,
        "result.json",
        &json!({"ordinary_checkpoint_refusal_repair_and_rewind": "passed"}),
    )
}

#[tokio::test]
async fn ordinary_ingest_checkpoint_refuses_end_orphaning_rewind_then_repairs() -> Result<()> {
    let mut fixture = Fixture::create().await?;
    eprintln!("#554 evidence: {}", fixture.directory.display());
    let result = tokio::time::timeout(std::time::Duration::from_secs(300), scenario(&mut fixture))
        .await
        .context("ordinary CLI scenario exceeded its five-minute deadline")
        .and_then(|result| result);
    let recorded = save(
        &fixture.directory,
        "scenario-result.json",
        &json!({
            "success": result.is_ok(), "error": result.as_ref().err().map(|error| format!("{error:#}"))
        }),
    );
    let cleanup = fixture.cleanup().await;
    recorded?;
    match (result, cleanup) {
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("cleanup also failed: {cleanup:#}")))
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}
