//! One PostgreSQL-native same-release restore followed by ordinary indexing.
//! Selected evidence checks do not establish independent whole-database equality.
use alloy_primitives::{Address, U256, hex, keccak256};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, ensure};
use bigname_e2e::harness::{
    anvil::{Anvil, GENESIS_TIMESTAMP},
    ens_v1,
    ens_v2::{self, RegisterEthName},
    manifests::generate_local_sepolia_profile,
    pipeline::SequentialFixtureReplay,
    rpc::RpcClient,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{
    ConnectOptions, Connection,
    postgres::{PgConnectOptions, PgConnection},
};
use std::{
    fs::{self, File},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    process::{Child, Command},
    time::{Instant, sleep, timeout, timeout_at},
};

const CHAIN: &str = "ethereum-sepolia";
const LABEL: &str = "restore640";
const DURATION: u64 = 2_592_000;
// getExpiry (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/PermissionedRegistry.sol:L296 @ ens_v2_sepolia_20260629@ccaeb58b).
// getOwner (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/PermissionedRegistry.sol:L310 @ ens_v2_sepolia_20260629@ccaeb58b).
sol! {
    function getExpiry(uint256 anyId) external view returns (uint64);
    function getOwner(uint256 anyId) external view returns (address);
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    repo_root: PathBuf,
    evidence_dir: PathBuf,
    container_name: String,
    admin_url: String,
    admin_password: String,
    original_database: String,
    restored_database: String,
    writer_password: String,
    reader_password: String,
    command_timeout_secs: u64,
    shutdown_timeout_secs: u64,
    readiness_timeout_secs: u64,
    progress_timeout_secs: u64,
    poll_secs: u64,
}
fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
fn save(path: &Path, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn number(v: &Value) -> Result<u64> {
    v.as_u64()
        .or_else(|| v.as_str()?.parse().ok())
        .context("missing exact integer")
}
fn quantity(v: &Value) -> Result<u64> {
    Ok(u64::from_str_radix(
        v.as_str().context("RPC quantity")?.trim_start_matches("0x"),
        16,
    )?)
}
fn array<'a>(v: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    v[key].as_array().with_context(|| format!("missing {key}"))
}
fn one<'a>(v: &'a Value, key: &str) -> Result<&'a Value> {
    let a = array(v, key)?;
    ensure!(a.len() == 1, "expected one {key}");
    Ok(&a[0])
}
fn canonical(v: &Value) -> bool {
    matches!(v.as_str(), Some("canonical" | "safe" | "finalized"))
}
fn equal(a: &Path, b: &Path) -> Result<()> {
    ensure!(
        fs::read(a)? == fs::read(b)?,
        "selected restore comparison differs: {a:?} {b:?}"
    );
    Ok(())
}
// Identity formulas from crates/adapters/src/schema_v2/common.rs:10,19,49.
fn uuid(seed: &str) -> String {
    let mut b = keccak256(seed.as_bytes()).0;
    b[6] = (b[6] & 15) | 80;
    b[8] = (b[8] & 63) | 128;
    let s = hex::encode(&b[..16]);
    format!(
        "{}-{}-{}-{}-{}",
        &s[..8],
        &s[8..12],
        &s[12..16],
        &s[16..20],
        &s[20..32]
    )
}
impl Config {
    fn timeout(&self) -> Duration {
        Duration::from_secs(self.command_timeout_secs)
    }
    fn check(&self) -> Result<()> {
        ensure!(
            self.original_database == "r640_original" && self.restored_database == "r640_restored",
            "unexpected database names"
        );
        ensure!(
            self.repo_root.is_absolute() && self.evidence_dir.is_absolute(),
            "absolute paths required"
        );
        ensure!(
            !self.container_name.is_empty()
                && self
                    .container_name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "container name"
        );
        for secret in [
            &self.admin_password,
            &self.writer_password,
            &self.reader_password,
        ] {
            ensure!(
                secret.len() >= 16 && secret.bytes().all(|b| b.is_ascii_hexdigit()),
                "expected disposable hex password"
            );
        }
        for secs in [
            self.command_timeout_secs,
            self.shutdown_timeout_secs,
            self.readiness_timeout_secs,
            self.progress_timeout_secs,
            self.poll_secs,
        ] {
            ensure!(
                secs > 0 && secs <= 86_400,
                "positive bounded deadline required"
            );
        }
        let options: PgConnectOptions = self.admin_url.parse()?;
        ensure!(
            options.get_host() == "127.0.0.1" && options.get_username() == "postgres",
            "published admin endpoint"
        );
        Ok(())
    }
    fn redact(&self, s: &str) -> String {
        [
            &self.admin_password,
            &self.writer_password,
            &self.reader_password,
            &self.admin_url,
        ]
        .into_iter()
        .fold(s.into(), |v, k| v.replace(k, "[redacted]"))
    }
    fn url(&self, db: &str, role: &str, password: &str) -> Result<String> {
        let options: PgConnectOptions = self.admin_url.parse()?;
        Ok(options
            .database(db)
            .username(role)
            .password(password)
            .to_url_lossy()
            .to_string())
    }
    async fn connect(&self, url: &str) -> Result<PgConnection> {
        timeout(
            Duration::from_secs(self.readiness_timeout_secs),
            PgConnection::connect(url),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(self.redact(&e.to_string())))
    }
    async fn container_alive(&self) -> Result<()> {
        let out = timeout(
            self.timeout(),
            Command::new("docker")
                .args([
                    "inspect",
                    "--format",
                    "{{.State.Running}}",
                    &self.container_name,
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        ensure!(
            out.status.success() && out.stdout == b"true\n",
            "owned PostgreSQL container stopped"
        );
        Ok(())
    }
    async fn pg(
        &self,
        db: &str,
        tool: &str,
        args: &[&str],
        input: Option<&Path>,
        tag: &str,
    ) -> Result<PathBuf> {
        self.container_alive().await?;
        let out = self.evidence_dir.join(format!("{tag}.out"));
        let err = self.evidence_dir.join(format!("{tag}.err"));
        let mut c = Command::new("docker");
        c.env("PGPASSWORD", &self.admin_password)
            .args([
                "exec",
                "-i",
                "-e",
                "PGPASSWORD",
                &self.container_name,
                tool,
                "--host=127.0.0.1",
                "--username=postgres",
                "--dbname",
                db,
            ])
            .args(args)
            .stdin(match input {
                Some(p) => Stdio::from(File::open(p)?),
                None => Stdio::null(),
            })
            .stdout(File::create(&out)?)
            .stderr(File::create(&err)?)
            .kill_on_drop(true);
        let started = now();
        let mut child = c.spawn()?;
        let status = match timeout(self.timeout(), child.wait()).await {
            Ok(s) => Some(s?),
            Err(_) => {
                child.start_kill()?;
                let _ = timeout(
                    Duration::from_secs(self.shutdown_timeout_secs),
                    child.wait(),
                )
                .await;
                None
            }
        };
        fs::write(&err, self.redact(&fs::read_to_string(&err)?))?;
        save(
            &self.evidence_dir.join(format!("{tag}.timing.json")),
            &json!({"tool":tool,"args":args,"database":db,"started_ms":started,"ended_ms":now(),"status":status.map(|s|s.to_string()),"timed_out":status.is_none()}),
        )?;
        ensure!(
            status.is_some_and(|s| s.success()),
            "native {tag} failed; timeout requires owned-cluster cleanup"
        );
        Ok(out)
    }
    async fn sql(
        &self,
        db: &str,
        mode: &str,
        tag: &str,
        e: Option<&Value>,
        roles: bool,
    ) -> Result<PathBuf> {
        let input = self.evidence_dir.join("private").join(format!("{tag}.sql"));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&input)?;
        writeln!(file, "\\set mode '{mode}'")?;
        if roles {
            writeln!(
                file,
                "\\set database '{db}'\n\\set writer 'r640_writer'\n\\set reader 'r640_reader'\n\\set writer_password '{}'\n\\set reader_password '{}'",
                self.writer_password, self.reader_password
            )?;
        }
        if let Some(e) = e {
            for key in [
                "chain",
                "name",
                "transaction",
                "registration_transaction",
                "F0",
                "source_key",
            ] {
                let value = e[key].as_str().context("SQL expectation")?;
                ensure!(
                    !value.contains(['\n', '\r', '\0', '\'', '\\']),
                    "unsafe SQL expectation"
                );
                writeln!(file, "\\set {key} '{value}'")?;
            }
        }
        let name = if roles { "roles.sql" } else { "checkpoint.sql" };
        file.write_all(&fs::read(
            self.repo_root
                .join("work/640-same-release-restore")
                .join(name),
        )?)?;
        drop(file);
        self.pg(
            db,
            "psql",
            &["-X", "-qAt", "-v", "ON_ERROR_STOP=1"],
            Some(&input),
            tag,
        )
        .await
    }
}

async fn rpc(
    cfg: &Config,
    client: &RpcClient,
    method: &str,
    params: Value,
    tag: &str,
) -> Result<Value> {
    let result = timeout(cfg.timeout(), client.call(method, params.clone())).await??;
    save(
        &cfg.evidence_dir.join(format!("{tag}.rpc.json")),
        &json!({"method":method,"params":params,"result":result}),
    )?;
    Ok(result)
}
async fn expiry(
    cfg: &Config,
    client: &RpcClient,
    registry: Address,
    hash: &Value,
    expected_owner: Address,
    tag: &str,
) -> Result<u64> {
    let mut results = Vec::new();
    let label_id = ens_v2::label_id(LABEL);
    for (field, data) in [
        ("expiry", getExpiryCall { anyId: label_id }.abi_encode()),
        ("owner", getOwnerCall { anyId: label_id }.abi_encode()),
    ] {
        let value=rpc(cfg,client,"eth_call",json!([{"to":registry,"data":hex::encode_prefixed(data)},{"blockHash":hash,"requireCanonical":true}]),&format!("{tag}-{field}")).await?;
        results.push(hex::decode(value.as_str().context("getter bytes")?)?);
    }
    ensure!(
        getOwnerCall::abi_decode_returns(&results[1])? == expected_owner,
        "onchain owner changed"
    );
    Ok(getExpiryCall::abi_decode_returns(&results[0])?)
}
async fn registration_receipt(
    cfg: &Config,
    client: &RpcClient,
    block: u64,
    registrar: Address,
    account: Address,
) -> Result<Value> {
    let full = rpc(
        cfg,
        client,
        "eth_getBlockByNumber",
        json!([format!("{block:#x}"), true]),
        "registration-block",
    )
    .await?;
    let candidates = array(&full, "transactions")?
        .iter()
        .filter(|t| {
            t["to"].as_str().and_then(|v| v.parse::<Address>().ok()) == Some(registrar)
                && t["from"].as_str().and_then(|v| v.parse::<Address>().ok()) == Some(account)
        })
        .collect::<Vec<_>>();
    ensure!(candidates.len() == 1, "registration transaction ambiguity");
    let r = rpc(
        cfg,
        client,
        "eth_getTransactionReceipt",
        json!([candidates[0]["hash"]]),
        "registration-receipt",
    )
    .await?;
    ensure!(
        quantity(&r["status"])? == 1
            && r["blockHash"] == full["hash"]
            && r["transactionHash"] == candidates[0]["hash"],
        "registration receipt failed or mismatched"
    );
    Ok(r)
}

fn validate(v: &Value, e: &Value) -> Result<()> {
    let p = one(v, "projections")?;
    ensure!(
        p["raw_name"] == "restore640.eth"
            && p["namespace"] == "ens"
            && p["logical_name_id"] == e["logical_name_id"]
            && p["resource_id"] == e["resource_uuid"]
            && p["support_status"] == "supported",
        "name identity/support"
    );
    let reg = &p["declared_summary"]["registration"];
    ensure!(
        reg["status"] == "active"
            && reg["registrant"] == e["account"]
            && number(&reg["expiry"])? == number(&e["expiry"])?,
        "registration owner/expiry"
    );
    let receipt = one(v, "receipts")?;
    let tx = one(v, "transactions")?;
    let expected = &e["receipt"];
    ensure!(
        quantity(&expected["status"])? == 1 && receipt["status"] == true,
        "failed receipt"
    );
    for item in [receipt, tx] {
        ensure!(
            item["transaction_hash"] == expected["transactionHash"]
                && item["block_hash"] == expected["blockHash"]
                && number(&item["block_number"])? == quantity(&expected["blockNumber"])?
                && number(&item["transaction_index"])? == quantity(&expected["transactionIndex"])?,
            "raw provenance"
        );
    }
    ensure!(
        array(v, "lineage")?.iter().any(
            |b| b["block_hash"] == expected["blockHash"] && canonical(&b["canonicality_state"])
        ),
        "canonical receipt block"
    );
    let logs = array(v, "logs")?;
    ensure!(!logs.is_empty(), "no selected logs");
    for log in logs {
        ensure!(
            log["transaction_hash"] == expected["transactionHash"]
                && log["block_hash"] == expected["blockHash"],
            "log provenance"
        );
        ensure!(
            array(expected, "logs")?
                .iter()
                .any(
                    |r| quantity(&r["logIndex"]).ok() == number(&log["log_index"]).ok()
                        && r["address"] == log["emitting_address"]
                        && r["topics"] == log["topics"]
                        && r["data"].as_str().map(str::to_owned)
                            == log["data"].as_str().map(|s| s.replace("\\x", "0x"))
                ),
            "selected log bytes"
        );
    }
    let kinds: &[&str] = if e["stage"] == "H1" {
        &["ExpiryChanged", "RegistrationRenewed"]
    } else {
        &["RegistrationGranted"]
    };
    for kind in kinds {
        ensure!(
            array(v, "events")?.iter().any(|r| r["event_kind"] == *kind
                && r["resource_id"] == e["resource_uuid"]
                && r["transaction_hash"] == expected["transactionHash"]
                && canonical(&r["canonicality_state"])
                && logs.iter().any(
                    |l| l["log_index"] == r["log_index"] && l["block_hash"] == r["block_hash"]
                )),
            "canonical normalized event missing: {kind}"
        );
    }
    ensure!(
        array(v, "history")?
            .iter()
            .any(|r| r["event_kind"] == "RegistrationGranted"
                && r["transaction_hash"] == e["registration_transaction"]
                && r["resource_id"] == e["resource_uuid"]
                && canonical(&r["canonicality_state"])),
        "retained registration history"
    );
    let phases = array(v, "phases")?;
    ensure!(phases.len() == 5, "five phases required");
    for name in ["ingest", "interpret", "project", "verify", "live"] {
        let matching = phases
            .iter()
            .filter(|p| p["phase_name"] == name)
            .collect::<Vec<_>>();
        ensure!(matching.len() == 1, "missing/duplicate phase");
        let phase = matching[0];
        ensure!(
            phase["last_error"].is_null()
                && phase["redo_in_progress"] == false
                && phase["redo_mode"].is_null()
                && phase["phase_status"] != "failed",
            "phase error/redo"
        );
        let at_finality = matches!(name, "verify" | "ingest");
        let target = number(&e[if at_finality { "F0" } else { "head" }])?;
        ensure!(
            number(&phase["current_block_number"])? >= target,
            "phase progress"
        );
        ensure!(
            array(v, "lineage")?
                .iter()
                .any(|b| b["block_hash"] == phase["current_block_hash"]
                    && canonical(&b["canonicality_state"])),
            "phase current hash"
        );
        if name == "verify" {
            ensure!(
                phase["phase_status"] == "completed"
                    && phase["verification_level"] == "quick_synced"
                    && phase["target_block_hash"] == e["F0_hash"]
                    && phase["current_block_hash"] == e["F0_hash"]
                    && number(&phase["target_block_number"])? == number(&e["F0"])?,
                "frozen provider-trusted Verify"
            );
        }
    }
    let cursor = one(v, "cursors")?;
    ensure!(
        cursor["source_key"] == "restore640"
            && cursor["source_kind"] == "drpc"
            && cursor["seed_basis"] == "ethereum_head"
            && number(&cursor["start_block_number"])? == 0
            && number(&cursor["last_processed_block_number"])? >= number(&e["finite_ingest_head"])?,
        "ordinary source cursor"
    );
    Ok(())
}

async fn stop_writer(cfg: &Config, writer: &mut Option<Child>, tag: &str) -> Result<()> {
    let Some(mut child) = writer.take() else {
        return Ok(());
    };
    let pid = child.id();
    let premature = child.try_wait()?;
    let signaled = if premature.is_none() {
        timeout(
            Duration::from_secs(cfg.shutdown_timeout_secs),
            Command::new("kill")
                .args(["-INT", &pid.context("writer pid")?.to_string()])
                .kill_on_drop(true)
                .status(),
        )
        .await
        .is_ok_and(|r| r.is_ok_and(|s| s.success()))
    } else {
        false
    };
    let mut forced = false;
    let exit = match timeout(Duration::from_secs(cfg.shutdown_timeout_secs), child.wait()).await {
        Ok(s) => s?,
        Err(_) => {
            forced = true;
            child.start_kill()?;
            timeout(Duration::from_secs(cfg.shutdown_timeout_secs), child.wait())
                .await
                .context("writer did not reap")??
        }
    };
    for suffix in ["stdout", "stderr"] {
        let path = cfg.evidence_dir.join(format!("{tag}.{suffix}"));
        fs::write(&path, cfg.redact(&fs::read_to_string(&path)?))?;
    }
    save(
        &cfg.evidence_dir.join(format!("{tag}.exit.json")),
        &json!({"pid":pid,"at_ms":now(),"status":exit.to_string(),"premature":premature.map(|s|s.to_string()),"signaled":signaled,"forced":forced}),
    )?;
    ensure!(
        premature.is_none() && signaled && !forced && exit.success(),
        "writer did not stop cleanly"
    );
    Ok(())
}
fn start_writer(
    cfg: &Config,
    binary: &Path,
    profile: &Path,
    db: &str,
    url: &str,
    tag: &str,
) -> Result<Child> {
    let mut c = Command::new(binary);
    c.current_dir(&cfg.repo_root)
        .env(
            "BIGNAME_DATABASE_URL",
            cfg.url(db, "r640_writer", &cfg.writer_password)?,
        )
        .env(
            "BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL",
            cfg.url(db, "r640_reader", &cfg.reader_password)?,
        )
        .env("BIGNAME_RESTORE_RPC", url)
        .args([
            "run",
            "--chain",
            CHAIN,
            "--source",
            "ethereum-sepolia:restore640:drpc:ethereum_head:0=BIGNAME_RESTORE_RPC",
            "--metrics-bind-addr",
            "127.0.0.1:0",
            "--live-poll-ms",
            &(cfg.poll_secs * 1000).to_string(),
            "--manifests-root",
        ])
        .arg(profile)
        .stdout(File::create(
            cfg.evidence_dir.join(format!("{tag}.stdout")),
        )?)
        .stderr(File::create(
            cfg.evidence_dir.join(format!("{tag}.stderr")),
        )?)
        .kill_on_drop(true);
    let child = c.spawn()?;
    save(
        &cfg.evidence_dir.join(format!("{tag}.start.json")),
        &json!({"pid":child.id(),"at_ms":now(),"database":db,"rpc_url":url,"binary":binary,"profile":profile,"poll_secs":cfg.poll_secs}),
    )?;
    Ok(child)
}
async fn observe(
    cfg: &Config,
    client: &RpcClient,
    writer: &mut Child,
    db: &str,
    e: &Value,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(cfg.progress_timeout_secs);
    let mut previous = false;
    let mut iteration = 0;
    timeout_at(deadline, async {
    loop {
        ensure!(Instant::now() < deadline, "healthy resting checkpoint deadline");
        ensure!(
            writer.try_wait()?.is_none(),
            "writer exited during observation"
        );
        let head = rpc(
            cfg,
            client,
            "eth_getBlockByNumber",
            json!(["latest", false]),
            &format!("{}-head-{iteration}", e["stage"].as_str().context("stage")?),
        )
        .await?;
        ensure!(
            head["hash"] == e["head_hash"],
            "chain moved during resting observation"
        );
        let tag = format!(
            "{}-observation-{iteration}",
            e["stage"].as_str().context("stage")?
        );
        ensure!(Instant::now() < deadline, "healthy resting checkpoint deadline");
        let path = cfg.sql(db, "checkpoint", &tag, Some(e), false).await?;
        let snapshot = read_json(&path)?;
        let checked = validate(&snapshot, e);
        save(
            &path.with_extension("validation.json"),
            &json!({"at_ms":now(),"healthy":checked.is_ok(),"violation":checked.as_ref().err().map(|e|e.to_string())}),
        )?;
        ensure!(Instant::now() < deadline, "healthy resting checkpoint deadline");
        if checked.is_ok() && previous {
            return Ok(());
        }
        previous = checked.is_ok();
        sleep(Duration::from_secs(cfg.poll_secs).min(deadline.saturating_duration_since(Instant::now()))).await;
        iteration += 1;
    }
    }).await.context("healthy resting checkpoint deadline")?
}
async fn connections(cfg: &Config, db: &str, tag: &str) -> Result<()> {
    let mut identities = Vec::new();
    for (role, password) in [
        ("r640_writer", &cfg.writer_password),
        ("r640_reader", &cfg.reader_password),
    ] {
        let mut c = cfg.connect(&cfg.url(db, role, password)?).await?;
        let identity:(String,i64,String,String)=timeout(cfg.timeout(),sqlx::query_as("SELECT current_database(),(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),system_identifier::text,current_user FROM pg_control_system()").fetch_one(&mut c)).await??;
        ensure!(
            identity.0 == db && identity.3 == role,
            "direct connection identity"
        );
        identities.push(json!({"database":identity.0,"oid":identity.1.to_string(),"cluster":identity.2,"role":identity.3}));
        c.close().await?;
    }
    ensure!(
        identities[0]["oid"] == identities[1]["oid"]
            && identities[0]["cluster"] == identities[1]["cluster"],
        "writer/reader mismatch"
    );
    save(
        &cfg.evidence_dir.join(format!("{tag}-connections.json")),
        &json!(identities),
    )
}
async fn selected(cfg: &Config, db: &str, tag: &str, e: &Value) -> Result<Vec<PathBuf>> {
    let schema = cfg
        .pg(
            db,
            "pg_dump",
            &["--schema-only", "--restrict-key=restore640comparison"],
            None,
            &format!("{tag}-schema"),
        )
        .await?;
    let ledger = cfg
        .sql(db, "migrations", &format!("{tag}-ledger"), None, false)
        .await?;
    let expected=bigname_storage::MIGRATOR.iter().map(|m|json!({"version":m.version.to_string(),"checksum":hex::encode(&m.checksum),"success":true})).collect::<Vec<_>>();
    ensure!(
        read_json(&ledger)?["versions"] == json!(expected),
        "compiled migration set mismatch"
    );
    let records = cfg
        .sql(db, "selected", &format!("{tag}-selected"), Some(e), false)
        .await?;
    validate(&read_json(&records)?, e)?;
    Ok(vec![schema, ledger, records])
}
async fn init_original(cfg: &Config, binary: &Path) -> Result<()> {
    let mut c = cfg
        .connect(&cfg.url(&cfg.original_database, "r640_writer", &cfg.writer_password)?)
        .await?;
    timeout(cfg.timeout(), bigname_storage::MIGRATOR.run(&mut c)).await??;
    c.close().await?;
    let mut child = Command::new(binary)
        .arg("init-schema")
        .env(
            "BIGNAME_DATABASE_URL",
            cfg.url(&cfg.original_database, "r640_writer", &cfg.writer_password)?,
        )
        .stdout(File::create(cfg.evidence_dir.join("init.stdout"))?)
        .stderr(File::create(cfg.evidence_dir.join("init.stderr"))?)
        .kill_on_drop(true)
        .spawn()?;
    let status = match timeout(cfg.timeout(), child.wait()).await {
        Ok(s) => Some(s?),
        Err(_) => {
            child.start_kill()?;
            let _ = timeout(Duration::from_secs(cfg.shutdown_timeout_secs), child.wait()).await;
            None
        }
    };
    let err = cfg.evidence_dir.join("init.stderr");
    fs::write(&err, cfg.redact(&fs::read_to_string(&err)?))?;
    save(
        &cfg.evidence_dir.join("init-status.json"),
        &json!({"status":status.map(|s|s.to_string()),"timed_out":status.is_none()}),
    )?;
    ensure!(
        status.is_some_and(|s| s.success()),
        "original init-schema failed"
    );
    cfg.sql(
        &cfg.original_database,
        "application",
        "original-application-grants",
        None,
        true,
    )
    .await?;
    Ok(())
}

async fn exercise(
    cfg: &Config,
    chain: &mut Option<Anvil>,
    lease: &mut Option<SequentialFixtureReplay>,
    writer: &mut Option<Child>,
    writer_tag: &mut String,
) -> Result<()> {
    cfg.container_alive().await?;
    let mut admin = cfg.connect(&cfg.admin_url).await?;
    timeout(cfg.timeout(), sqlx::query("SELECT 1").execute(&mut admin)).await??;
    cfg.container_alive().await?;
    save(
        &cfg.evidence_dir.join("published-readiness.json"),
        &json!({"authenticated_sql":true,"at_ms":now()}),
    )?;
    cfg.sql("postgres", "cluster", "cluster-roles", None, true)
        .await?;
    *chain = Some(Anvil::spawn_ethereum_sepolia().await?);
    let anvil = chain.as_ref().context("owned Anvil")?;
    let client = anvil.client();
    let id = rpc(cfg, &client, "eth_chainId", json!([]), "chain-id").await?;
    ensure!(quantity(&id)? == 11155111, "chain identity");
    save(
        &cfg.evidence_dir.join("fixture-context.json"),
        &json!({"chain":CHAIN,"rpc_url":anvil.url,"genesis_timestamp":GENESIS_TIMESTAMP,"label":LABEL,"duration":DURATION,"anvil_cleanup":"public harness destructor; no independent exit-status claim"}),
    )?;
    let (d, account, name, receipt, setup_head) = timeout(cfg.timeout(), async {
        let d = ens_v2::deploy_ens_v2(&client, &cfg.repo_root).await?;
        save(
            &cfg.evidence_dir.join("deployment-targets.json"),
            &json!(d.manifest_targets()),
        )?;
        let accounts = client.accounts().await?;
        let account = *accounts.get(1).context("nonzero fixture account")?;
        ensure!(account != Address::ZERO, "zero account");
        let name = ens_v2::register_eth_name(
            &client,
            &d,
            RegisterEthName {
                from: account,
                label: LABEL,
                owner: account,
                duration_secs: DURATION,
                subregistry: Address::ZERO,
                resolver: Address::ZERO,
            },
        )
        .await?;
        let receipt = registration_receipt(
            cfg,
            &client,
            name.register_block,
            d.eth_registrar.address,
            account,
        )
        .await?;
        ens_v2::grant_roles(
            &client,
            d.eth_registry.address,
            d.deployer,
            name.token_id,
            U256::from(1) << ens_v2::ROLE_RENEW,
            account,
        )
        .await?;
        let setup_head = client.block_number().await?;
        client.mine(64).await?;
        Ok::<_, anyhow::Error>((d, account, name, receipt, setup_head))
    })
    .await
    .context("H0 fixture deadline exceeded")??;
    let frozen = rpc(
        cfg,
        &client,
        "eth_getBlockByNumber",
        json!(["finalized", false]),
        "F0",
    )
    .await?;
    ensure!(
        quantity(&frozen["number"])? >= setup_head,
        "actual finalized head does not cover setup"
    );
    let h0 = rpc(
        cfg,
        &client,
        "eth_getBlockByNumber",
        json!(["latest", false]),
        "H0-head",
    )
    .await?;
    let e0 = expiry(
        cfg,
        &client,
        d.eth_registry.address,
        &h0["hash"],
        account,
        "H0-expiry",
    )
    .await?;
    ensure!(
        e0 > quantity(&h0["timestamp"])?,
        "registration expired at H0"
    );
    let profile =
        generate_local_sepolia_profile(&cfg.evidence_dir, &cfg.repo_root, &d.manifest_targets())?;
    *lease = Some(
        SequentialFixtureReplay::start_with_chain_rpc_urls(
            &cfg.repo_root,
            &cfg.url(&cfg.original_database, "r640_writer", &cfg.writer_password)?,
            &profile.root,
            &[(CHAIN, &anvil.url)],
        )
        .await?,
    );
    let binary = lease.as_ref().context("binary lease")?.binary_path();
    let retained = cfg.evidence_dir.join("phase-runner");
    let mut copy = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o700)
        .open(&retained)?;
    copy.write_all(&fs::read(binary)?)?;
    drop(copy);
    equal(binary, &retained)?;
    let contract = uuid(&format!("contract:{CHAIN}:{:#x}", d.eth_registry.address));
    let resource = uuid(&format!(
        "ens-v2-resource:{CHAIN}:{contract}:{:#066x}",
        name.resource_id
    ));
    let mut e = json!({"stage":"H0","chain":CHAIN,"name":"restore640.eth","source_key":"restore640","account":format!("{account:#x}"),"expiry":e0.to_string(),"resource_uuid":resource,"logical_name_id":format!("ens:{:#x}",ens_v1::namehash("restore640.eth")),"registration_transaction":receipt["transactionHash"],"transaction":receipt["transactionHash"],"receipt":receipt,"F0":quantity(&frozen["number"])?.to_string(),"F0_hash":frozen["hash"],"head":quantity(&h0["number"])?.to_string(),"head_hash":h0["hash"]});
    // Ingest retains the finite H0 boundary while Live advances the published head.
    e["finite_ingest_head"] = e["head"].clone();
    timeout(cfg.timeout(),sqlx::query("CREATE DATABASE r640_original OWNER r640_writer TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'").execute(&mut admin)).await??;
    cfg.sql(
        &cfg.original_database,
        "database",
        "original-prerequisites",
        None,
        true,
    )
    .await?;
    let original_identity = read_json(
        &cfg.sql(
            &cfg.original_database,
            "identity",
            "original-identity",
            None,
            false,
        )
        .await?,
    )?;
    init_original(cfg, binary).await?;
    connections(cfg, &cfg.original_database, "H0").await?;
    *writer_tag = "H0-writer".into();
    *writer = Some(start_writer(
        cfg,
        binary,
        &profile.root,
        &cfg.original_database,
        &anvil.url,
        writer_tag,
    )?);
    save(&cfg.evidence_dir.join("H0-expectations.json"), &e)?;
    observe(
        cfg,
        &client,
        writer.as_mut().context("writer")?,
        &cfg.original_database,
        &e,
    )
    .await?;
    stop_writer(cfg, writer, writer_tag).await?;
    let baseline = selected(cfg, &cfg.original_database, "stopped-H0", &e).await?;
    let size: i64 = timeout(
        cfg.timeout(),
        sqlx::query_scalar("SELECT pg_database_size('r640_original')").fetch_one(&mut admin),
    )
    .await??;
    save(
        &cfg.evidence_dir.join("database-size.json"),
        &json!({"bytes":size,"head":e["head"],"F0":e["F0"],"at_ms":now()}),
    )?;
    let archive = cfg
        .pg(
            &cfg.original_database,
            "pg_dump",
            &["--format=custom"],
            None,
            "backup",
        )
        .await?;
    cfg.pg(
        &cfg.original_database,
        "pg_restore",
        &["--list"],
        Some(&archive),
        "archive-list",
    )
    .await?;
    save(
        &cfg.evidence_dir.join("archive-size.json"),
        &json!({"bytes":fs::metadata(&archive)?.len()}),
    )?;
    timeout(cfg.timeout(),sqlx::query("CREATE DATABASE r640_restored OWNER r640_writer TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'").execute(&mut admin)).await??;
    cfg.sql(
        &cfg.restored_database,
        "database",
        "restored-prerequisites",
        None,
        true,
    )
    .await?;
    let empty = read_json(
        &cfg.sql(
            &cfg.restored_database,
            "empty",
            "destination-empty",
            None,
            false,
        )
        .await?,
    )?;
    for key in ["application_schemas", "relations", "routines", "types"] {
        ensure!(
            array(&empty, key)?.is_empty(),
            "destination is not empty: {key}"
        );
    }
    let extensions = array(&empty, "extensions")?;
    ensure!(
        extensions.len() == 1 && extensions[0]["name"] == "plpgsql",
        "unexpected destination extension"
    );
    let restored_identity = read_json(
        &cfg.sql(
            &cfg.restored_database,
            "identity",
            "restored-identity",
            None,
            false,
        )
        .await?,
    )?;
    ensure!(
        original_identity["db_oid"] != restored_identity["db_oid"]
            && original_identity["db_name"] != restored_identity["db_name"]
            && original_identity["cluster_id"] == restored_identity["cluster_id"],
        "restore must use a distinct database in the same cluster"
    );
    cfg.pg(
        &cfg.restored_database,
        "pg_restore",
        &[
            "--single-transaction",
            "--exit-on-error",
            "--role=r640_writer",
        ],
        Some(&archive),
        "restore",
    )
    .await?;
    let restored = selected(cfg, &cfg.restored_database, "restored-H0", &e).await?;
    for (a, b) in baseline.iter().zip(&restored) {
        equal(a, b)?;
    }
    connections(cfg, &cfg.restored_database, "restored-H0").await?;
    let e1 = e0.checked_add(DURATION).context("expiry overflow")?;
    let renewal = timeout(cfg.timeout(), async {
        ensure!(
            e0 > client.block_timestamp().await? as u64,
            "registration expired before renewal"
        );
        ens_v2::renew_in_registry(&client, d.eth_registry.address, account, name.token_id, e1).await
    })
    .await
    .context("H1 renewal deadline exceeded")??;
    ensure!(
        renewal.status_ok && renewal.block_number > quantity(&h0["number"])?,
        "renewal failed or not newer than frozen H0"
    );
    let r = rpc(
        cfg,
        &client,
        "eth_getTransactionReceipt",
        json!([renewal.tx_hash]),
        "renewal-receipt",
    )
    .await?;
    ensure!(
        quantity(&r["status"])? == 1 && r["transactionHash"] != e["registration_transaction"],
        "renewal receipt identity"
    );
    ensure!(
        expiry(
            cfg,
            &client,
            d.eth_registry.address,
            &r["blockHash"],
            account,
            "H1-expiry"
        )
        .await?
            == e1,
        "renewed onchain expiry"
    );
    let h1 = rpc(
        cfg,
        &client,
        "eth_getBlockByNumber",
        json!(["latest", false]),
        "H1-head",
    )
    .await?;
    e["stage"] = json!("H1");
    e["receipt"] = r.clone();
    e["transaction"] = r["transactionHash"].clone();
    e["head"] = json!(quantity(&h1["number"])?.to_string());
    e["head_hash"] = h1["hash"].clone();
    e["expiry"] = json!(e1.to_string());
    save(&cfg.evidence_dir.join("H1-expectations.json"), &e)?;
    equal(binary, &retained)?;
    *writer_tag = "H1-writer".into();
    let restart = now();
    *writer = Some(start_writer(
        cfg,
        binary,
        &profile.root,
        &cfg.restored_database,
        &anvil.url,
        writer_tag,
    )?);
    observe(
        cfg,
        &client,
        writer.as_mut().context("restored writer")?,
        &cfg.restored_database,
        &e,
    )
    .await?;
    save(
        &cfg.evidence_dir.join("recovery-timing.json"),
        &json!({"restart_ms":restart,"healthy_ms":now(),"scope":"selected fixture; not production RTO"}),
    )?;
    stop_writer(cfg, writer, writer_tag).await?;
    selected(cfg, &cfg.restored_database, "stopped-H1", &e).await?;
    equal(binary, &retained)?;
    admin.close().await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    ensure!(args.len() == 1, "supply one protected configuration path");
    let cfg: Config = serde_json::from_slice(&fs::read(&args[0])?)?;
    cfg.check()?;
    let (mut chain, mut lease, mut writer, mut tag) = (None, None, None, String::new());
    let result = exercise(&cfg, &mut chain, &mut lease, &mut writer, &mut tag).await;
    let cleanup = stop_writer(&cfg, &mut writer, &tag).await;
    drop(chain);
    save(
        &cfg.evidence_dir.join("driver-result.json"),
        &json!({"success":result.is_ok()&&cleanup.is_ok(),"error":result.as_ref().err().map(|e|cfg.redact(&format!("{e:#}"))),"writer_cleanup":cleanup.as_ref().err().map(|e|cfg.redact(&format!("{e:#}"))),"anvil_cleanup":"harness destructor invoked; no independent exit-status evidence","finished_ms":now()}),
    )?;
    drop(lease);
    result.map_err(|e| anyhow::anyhow!(cfg.redact(&format!("{e:#}"))))?;
    cleanup
}
