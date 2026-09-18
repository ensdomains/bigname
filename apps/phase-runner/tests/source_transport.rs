#[allow(dead_code)]
mod support;
use std::collections::BTreeMap;

use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::VerificationProvider;
use phase_runner::{
    config::{SeedBasis, SourceConfig, SourceRole},
    phase::PhaseName,
    phase_lock::PhaseLock,
    source_transport::{transition, transition_with_readers},
};
use serde_json::{Value, json};
use support::ScratchDatabase;
use tokio::net::TcpListener;
use uuid::Uuid;

const SEPOLIA: &str = "ethereum-sepolia";
const CONTRACT: &str = "0x0000000000000000000000000000000000000004";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[tokio::test]
async fn active_writer_refuses_transport_change_before_provider_access() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_lock").await?;
    let old = source("drpc", "http://127.0.0.1:1")?;
    let new = source("reth_db", "/missing/reth")?;
    for phase in PhaseName::ALL {
        let lock = PhaseLock::acquire(db.writer_connect_options(), SEPOLIA, phase).await?;
        let error = transition(&db.runner(), &old, &new).await.unwrap_err();
        assert!(
            error.to_string().contains("stop all phase writers"),
            "{error:#}"
        );
        lock.release().await?;
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ingest_cursors")
        .fetch_one(db.pool())
        .await?;
    assert_eq!(count, 0);
    db.cleanup().await
}

#[tokio::test]
async fn direct_reader_without_a_datadir_is_refused_and_changes_nothing() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_no_datadir").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::None).await?;
    let (rpc, server) = NodeDouble::through(6).spawn().await?;
    let datadir = std::env::temp_dir().join(format!("source-transport-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&datadir)?;
    let before = snapshot(db.pool()).await?;

    let error = transition(
        &db.runner(),
        &source("drpc", &rpc)?,
        &source("reth_db", datadir.to_str().expect("utf-8 temp path"))?,
    )
    .await
    .expect_err("an empty directory is not a Reth datadir");

    assert!(format!("{error:#}").contains("is missing"), "{error:#}");
    assert_eq!(before, snapshot(db.pool()).await?);
    server.abort();
    std::fs::remove_dir_all(datadir)?;
    db.cleanup().await
}

#[tokio::test]
async fn matching_node_interfaces_change_only_the_stored_source_kind() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_success").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::None).await?;
    let node = NodeDouble::through(6).with_watched_log(6);
    let before = snapshot(db.pool()).await?;

    let receipt = switch(&db, "drpc", &node, "reth_db", &node).await?;

    assert_eq!(receipt["from_kind"], "drpc");
    assert_eq!(receipt["to_kind"], "reth_db");
    assert_eq!(receipt["previous_cursor"], before["cursors"][0]);
    assert_eq!(receipt["ingest_phase"], before["phases"][0]);
    assert_eq!(
        receipt["checked_boundaries"],
        json!([
            {"block": 5, "hash": block_hash(5)},
            {"block": 5, "hash": block_hash(5)}
        ]),
        "the cursor boundary and the Ingest phase boundary are both checked"
    );
    assert_eq!(receipt["next_block"], 6);
    assert_eq!(receipt["next_block_hash"], block_hash(6));
    assert_eq!(receipt["next_block_log_count"], 1);
    assert_eq!(
        snapshot(db.pool()).await?,
        with_stored_kind(before, "reth_db"),
        "only the stored source kind may change; progress, retained hashes, phase state, \
         lineage, raw logs and manifests stay as they were"
    );
    db.cleanup().await
}

#[tokio::test]
async fn reverse_switch_restores_the_http_source_kind() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_reverse").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::None).await?;
    let node = NodeDouble::through(6).with_watched_log(6);
    let original = snapshot(db.pool()).await?;

    switch(&db, "drpc", &node, "reth_db", &node).await?;
    let error = switch(&db, "drpc", &node, "reth_db", &node)
        .await
        .expect_err("the stored kind is no longer drpc");
    assert!(
        error.to_string().contains("stored source kind differs"),
        "{error:#}"
    );
    let receipt = switch(&db, "reth_db", &node, "drpc", &node).await?;

    assert_eq!(receipt["from_kind"], "reth_db");
    assert_eq!(receipt["to_kind"], "drpc");
    assert_eq!(snapshot(db.pool()).await?, original);
    db.cleanup().await
}

#[tokio::test]
async fn retained_boundary_hash_mismatch_is_refused() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_boundary").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::None).await?;
    let node = NodeDouble::through(6);
    let forked = node.clone().with_hash(5, block_hash(1_005));
    let before = snapshot(db.pool()).await?;

    // The new interface is on another fork at the retained boundary.
    let error = switch(&db, "drpc", &node, "reth_db", &forked)
        .await
        .expect_err("the two interfaces disagree at the retained boundary");
    assert!(
        error
            .to_string()
            .contains("retained boundary differs at block 5"),
        "{error:#}"
    );
    // Both interfaces agree with each other but not with the hash Ingest retained.
    let error = switch(&db, "drpc", &forked, "reth_db", &forked)
        .await
        .expect_err("the node does not hold the retained boundary block");
    assert!(
        error
            .to_string()
            .contains("retained boundary differs at block 5"),
        "{error:#}"
    );

    assert_eq!(before, snapshot(db.pool()).await?);
    db.cleanup().await
}

#[tokio::test]
async fn next_block_mismatch_is_refused() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_next_block").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::None).await?;
    let node = NodeDouble::through(6).with_watched_log(6);
    let before = snapshot(db.pool()).await?;

    let other_hash = NodeDouble::through(6)
        .with_hash(6, block_hash(1_006))
        .with_watched_log(6);
    let missing_log = NodeDouble::through(6);
    for other in [&other_hash, &missing_log] {
        let error = switch(&db, "drpc", &node, "reth_db", other)
            .await
            .expect_err("the two interfaces disagree about the next block");
        assert!(
            error.to_string().contains("next block data differs"),
            "{error:#}"
        );
    }

    assert_eq!(before, snapshot(db.pool()).await?);
    db.cleanup().await
}

#[tokio::test]
async fn redo_in_progress_checks_the_redo_position_and_keeps_the_redo() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_redo").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::ResumedAt(3)).await?;
    let node = NodeDouble::through(6)
        .with_watched_log(4)
        .with_watched_log(6);
    let before = snapshot(db.pool()).await?;

    // The redo resumes at block 4, so that is the block whose logs must match.
    let disagrees_at_redo_position = NodeDouble::through(6).with_watched_log(6);
    let error = switch(&db, "drpc", &node, "reth_db", &disagrees_at_redo_position)
        .await
        .expect_err("the two interfaces disagree about the block the redo reads next");
    assert!(
        error.to_string().contains("next block data differs"),
        "{error:#}"
    );
    let forked_at_redo_boundary = node.clone().with_hash(3, block_hash(1_003));
    let error = switch(&db, "drpc", &node, "reth_db", &forked_at_redo_boundary)
        .await
        .expect_err("the two interfaces disagree at the redo's retained boundary");
    assert!(
        error
            .to_string()
            .contains("retained boundary differs at block 3"),
        "{error:#}"
    );
    assert_eq!(before, snapshot(db.pool()).await?);

    let receipt = switch(&db, "drpc", &node, "reth_db", &node).await?;

    assert_eq!(receipt["next_block"], 4);
    assert_eq!(receipt["next_block_log_count"], 1);
    assert_eq!(
        receipt["checked_boundaries"][2],
        json!({"block": 3, "hash": block_hash(3)})
    );
    assert_eq!(
        snapshot(db.pool()).await?,
        with_stored_kind(before, "reth_db"),
        "the redo range, its position and its retained hash are untouched"
    );
    db.cleanup().await
}

#[tokio::test]
async fn redo_that_has_not_started_checks_its_first_block() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_redo_start").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::NotStarted).await?;
    let node = NodeDouble::through(6).with_watched_log(2);
    let before = snapshot(db.pool()).await?;

    let receipt = switch(&db, "drpc", &node, "reth_db", &node).await?;

    assert_eq!(receipt["next_block"], 2);
    assert_eq!(receipt["next_block_log_count"], 1);
    assert_eq!(
        snapshot(db.pool()).await?,
        with_stored_kind(before, "reth_db")
    );
    db.cleanup().await
}

#[tokio::test]
async fn direct_reader_floor_above_the_declared_start_refuses_unfinished_normal_ingest()
-> Result<()> {
    let db = ScratchDatabase::create("source_transport_floor_normal").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::None).await?;
    // Ingest still has catch-up work: it stands at block 5 with a target of 8.
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', finished_at = NULL,
             target_block_number = 8, target_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(SEPOLIA)
    .bind(block_hash(8))
    .execute(db.pool())
    .await?;
    let node = NodeDouble::through(8).with_watched_log(6);
    let before = snapshot(db.pool()).await?;

    // The node has pruned history below block 3. The retained boundary (5) and the next
    // block (6) are both readable, so every comparison passes; only the floor differs.
    let error = switch_to_direct_reader_with_floor(&db, &node, 3)
        .await
        .expect_err("resumed normal Ingest plans from the declared start block 0");

    let message = format!("{error:#}");
    assert!(
        message.contains("cannot serve the Ingest work that resumes after this change")
            && message.contains("keeps history from block 3 only")
            && message.contains("0..=head"),
        "{message}"
    );
    assert_eq!(before, snapshot(db.pool()).await?);

    // A node that still holds the declared range is admitted.
    switch_to_direct_reader_with_floor(&db, &node, 0).await?;
    assert_eq!(
        snapshot(db.pool()).await?,
        with_stored_kind(before, "reth_db")
    );
    db.cleanup().await
}

#[tokio::test]
async fn direct_reader_floor_is_judged_on_the_remaining_redo_suffix() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_floor_redo").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::ResumedAt(3)).await?;
    let node = NodeDouble::through(6)
        .with_watched_log(4)
        .with_watched_log(6);
    let before = snapshot(db.pool()).await?;

    // The redo of 2..=5 resumes at block 4; a floor of 5 leaves block 4 unreadable.
    let error = switch_to_direct_reader_with_floor(&db, &node, 5)
        .await
        .expect_err("the redo still has to read block 4");
    let message = format!("{error:#}");
    assert!(
        message.contains("keeps history from block 5 only") && message.contains("4..=5"),
        "{message}"
    );
    assert_eq!(before, snapshot(db.pool()).await?);

    // Blocks 2 and 3 are already re-read, so a floor of 4 admits what remains.
    switch_to_direct_reader_with_floor(&db, &node, 4).await?;
    assert_eq!(
        snapshot(db.pool()).await?,
        with_stored_kind(before, "reth_db")
    );
    db.cleanup().await
}

#[tokio::test]
async fn direct_reader_floor_is_judged_on_the_whole_range_of_an_unstarted_redo() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_floor_redo_start").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::NotStarted).await?;
    let node = NodeDouble::through(6).with_watched_log(2);
    let before = snapshot(db.pool()).await?;

    let error = switch_to_direct_reader_with_floor(&db, &node, 3)
        .await
        .expect_err("the redo of 2..=5 has not read block 2 yet");
    let message = format!("{error:#}");
    assert!(
        message.contains("keeps history from block 3 only") && message.contains("2..=5"),
        "{message}"
    );
    assert_eq!(before, snapshot(db.pool()).await?);

    switch_to_direct_reader_with_floor(&db, &node, 2).await?;
    assert_eq!(
        snapshot(db.pool()).await?,
        with_stored_kind(before, "reth_db")
    );
    db.cleanup().await
}

#[tokio::test]
async fn direct_reader_floor_is_judged_on_the_live_suffix_after_ingest_completed() -> Result<()> {
    let db = ScratchDatabase::create("source_transport_floor_live").await?;
    seed_watch_set(db.pool()).await?;
    seed_ingest(db.pool(), "drpc", Redo::None).await?;
    // Ingest is complete: it handed block 5 to live follow, which reads from block 6.
    sqlx::query(
        "UPDATE chain_phase_state
         SET live_handoff_block_number = 5, live_handoff_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(SEPOLIA)
    .bind(block_hash(5))
    .execute(db.pool())
    .await?;
    let node = NodeDouble::through(6).with_watched_log(6);
    let before = snapshot(db.pool()).await?;

    let error = switch_to_direct_reader_with_floor(&db, &node, 7)
        .await
        .expect_err("live follow reads block 6 next");
    let message = format!("{error:#}");
    assert!(
        message.contains("keeps history from block 7 only") && message.contains("6..=head"),
        "{message}"
    );
    assert_eq!(before, snapshot(db.pool()).await?);

    switch_to_direct_reader_with_floor(&db, &node, 6).await?;
    assert_eq!(
        snapshot(db.pool()).await?,
        with_stored_kind(before, "reth_db")
    );
    db.cleanup().await
}

/// Runs the production switch with each descriptor read through an HTTP node double. The
/// direct database reader needs a real Reth datadir, so `direct` stands in for it; the locks,
/// cursor checks, comparisons and update are the ones `transition` runs.
async fn switch(
    db: &ScratchDatabase,
    from_kind: &str,
    from_node: &NodeDouble,
    to_kind: &str,
    to_node: &NodeDouble,
) -> Result<Value> {
    switch_with_readers(db, from_kind, from_node, to_kind, to_node, |source| {
        Ok(VerificationProvider::new(
            SEPOLIA,
            "drpc",
            source.endpoint(),
        )?)
    })
    .await
}

/// [`switch`] from `drpc` to `reth_db` where the double standing in for the direct reader
/// reports a retention floor, as a pruned datadir would.
async fn switch_to_direct_reader_with_floor(
    db: &ScratchDatabase,
    node: &NodeDouble,
    floor: i64,
) -> Result<Value> {
    switch_with_readers(db, "drpc", node, "reth_db", node, |source| {
        let provider = VerificationProvider::new(SEPOLIA, "drpc", source.endpoint())?;
        Ok(if source.source_kind == "reth_db" {
            provider.with_declared_retention_floor(floor)
        } else {
            provider
        })
    })
    .await
}

async fn switch_with_readers(
    db: &ScratchDatabase,
    from_kind: &str,
    from_node: &NodeDouble,
    to_kind: &str,
    to_node: &NodeDouble,
    open_reader: impl Fn(&SourceConfig) -> Result<VerificationProvider>,
) -> Result<Value> {
    let (from_rpc, from_server) = from_node.spawn().await?;
    let (to_rpc, to_server) = to_node.spawn().await?;
    let result = transition_with_readers(
        &db.runner(),
        &source(from_kind, &from_rpc)?,
        &source(to_kind, &to_rpc)?,
        open_reader,
    )
    .await;
    from_server.abort();
    to_server.abort();
    result
}

fn with_stored_kind(mut snapshot: Value, kind: &str) -> Value {
    snapshot["cursors"][0]["source_kind"] = json!(kind);
    snapshot
}

fn source(kind: &str, endpoint: &str) -> Result<SourceConfig> {
    Ok(SourceConfig::new_with_role(
        SEPOLIA,
        "node",
        kind,
        SeedBasis::EthereumHead,
        0,
        SourceRole::Intake,
        endpoint,
    )?)
}

fn block_hash(number: i64) -> String {
    format!("0x{:064x}", 0xb10c_0000_i64 + number)
}

/// One execution node as its HTTP interface reports it: canonical hashes by height and
/// the logs of each block.
#[derive(Clone)]
struct NodeDouble {
    hashes: BTreeMap<i64, String>,
    logs: BTreeMap<i64, Vec<Value>>,
}

impl NodeDouble {
    fn through(head: i64) -> Self {
        Self {
            hashes: (0..=head)
                .map(|number| (number, block_hash(number)))
                .collect(),
            logs: BTreeMap::new(),
        }
    }

    fn with_hash(mut self, number: i64, hash: String) -> Self {
        self.hashes.insert(number, hash);
        self
    }

    fn with_watched_log(mut self, number: i64) -> Self {
        let log = json!({
            "blockHash": self.hashes[&number],
            "blockNumber": format!("{number:#x}"),
            "transactionHash": format!("0x{:064x}", 0x7a00_i64 + number),
            "transactionIndex": "0x0",
            "logIndex": "0x0",
            "address": CONTRACT,
            "topics": [TRANSFER_TOPIC],
            "data": "0x"
        });
        self.logs.entry(number).or_default().push(log);
        self
    }

    async fn spawn(&self) -> Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let node = self.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/", post(answer)).with_state(node),
            )
            .await
            .expect("source transport node double");
        });
        Ok((format!("http://{address}/"), server))
    }

    fn respond(&self, request: &Value) -> Value {
        let id = request.get("id").cloned().unwrap_or(json!(1));
        let number = |value: &Value| {
            let quantity = value.as_str().expect("hex quantity");
            i64::from_str_radix(quantity.trim_start_matches("0x"), 16).expect("block number")
        };
        let result = match request["method"].as_str().unwrap_or_default() {
            "eth_getBlockByNumber" => {
                let number = number(&request["params"][0]);
                self.hashes.get(&number).map_or(Value::Null, |hash| {
                    json!({
                        "hash": hash,
                        "parentHash": block_hash(number - 1),
                        "number": format!("{number:#x}"),
                        "timestamp": format!("{number:#x}")
                    })
                })
            }
            "eth_getLogs" => {
                let filter = &request["params"][0];
                let (from, to) = (number(&filter["fromBlock"]), number(&filter["toBlock"]));
                let logs = self
                    .logs
                    .range(from..=to)
                    .flat_map(|(_, logs)| logs.clone());
                Value::Array(logs.collect())
            }
            method => panic!("unexpected source transport RPC method {method}"),
        };
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    }
}

async fn answer(State(node): State<NodeDouble>, Json(request): Json<Value>) -> Json<Value> {
    Json(match request.as_array() {
        Some(batch) => Value::Array(batch.iter().map(|item| node.respond(item)).collect()),
        None => node.respond(&request),
    })
}

/// Whether the seeded Ingest phase also carries an explicit redo of blocks 2..=5.
enum Redo {
    None,
    /// Installed but no block re-read yet.
    NotStarted,
    /// Re-read through this block.
    ResumedAt(i64),
}

/// An Ingest pass that finished through block 5, so the cursor's next block is 6.
async fn seed_ingest(pool: &sqlx::PgPool, stored_kind: &str, redo: Redo) -> Result<()> {
    support::seed_lineage(pool, SEPOLIA, 5).await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
            chain_id, source_key, source_kind, seed_basis, start_block_number,
            next_block_number, target_block_number, last_processed_block_number,
            last_processed_block_hash
         ) VALUES ($1, 'node', $2, 'ethereum_head', 0, 6, 5, 5, $3)",
    )
    .bind(SEPOLIA)
    .bind(stored_kind)
    .bind(block_hash(5))
    .execute(pool)
    .await?;
    for phase in PhaseName::ALL {
        let ingest = phase == PhaseName::Ingest;
        sqlx::query(
            "INSERT INTO chain_phase_state (
                chain_id, phase_name, phase_status, current_block_number, current_block_hash,
                target_block_number, target_block_hash, started_at, finished_at
             ) VALUES (
                $1, $2, $3, $4, $5, $4, $5,
                CASE WHEN $3 = 'completed' THEN now() END,
                CASE WHEN $3 = 'completed' THEN now() END
             )",
        )
        .bind(SEPOLIA)
        .bind(phase.as_str())
        .bind(if ingest { "completed" } else { "idle" })
        .bind(ingest.then_some(5_i64))
        .bind(ingest.then(|| block_hash(5)))
        .execute(pool)
        .await?;
    }
    let resumed_at = match redo {
        Redo::None => return Ok(()),
        Redo::NotStarted => None,
        Redo::ResumedAt(number) => Some(number),
    };
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', finished_at = NULL, redo_in_progress = true,
             redo_mode = 'redo', redo_previous_phase_status = 'completed',
             redo_previous_started_at = started_at, redo_previous_finished_at = finished_at,
             redo_from_block_number = 2, redo_to_block_number = 5,
             redo_current_block_number = $2, redo_current_block_hash = $3
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(SEPOLIA)
    .bind(resumed_at)
    .bind(resumed_at.map(block_hash))
    .execute(pool)
    .await?;
    Ok(())
}

/// Everything the command is documented to preserve, plus the cursor it may edit.
async fn snapshot(pool: &sqlx::PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
            'cursors', (SELECT jsonb_agg(to_jsonb(c) ORDER BY source_key) FROM ingest_cursors c),
            'phases', (SELECT jsonb_agg(to_jsonb(p) ORDER BY phase_name) FROM chain_phase_state p),
            'lineage', (SELECT jsonb_agg(to_jsonb(l) ORDER BY block_number, block_hash)
                        FROM chain_lineage l),
            'raw_logs', (SELECT count(*) FROM raw_logs),
            'manifests', (SELECT jsonb_agg(to_jsonb(m) ORDER BY manifest_id)
                          FROM manifest_versions m)
        )",
    )
    .fetch_one(pool)
    .await?)
}

async fn seed_watch_set(pool: &sqlx::PgPool) -> Result<()> {
    let contract_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
         VALUES ($1, $2, 'contract', '{}'::jsonb)",
    )
    .bind(contract_id)
    .bind(SEPOLIA)
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "test",
        "source_family": "test_events",
        "chain": SEPOLIA,
        "deployment_epoch": "test",
        "rollout_status": "active",
        "normalizer_version": "test",
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": {"events": [{
            "name": "Transfer",
            "fragment": "event Transfer(address indexed from,address indexed to,uint256 value)",
            "emitter_roles": [],
            "normalized_events": []
        }]}
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id, deployment_label,
            rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'test', 'test_events', $1, 'test', 'active', 'test', $2, $3::jsonb)
         RETURNING manifest_id",
    )
    .bind(SEPOLIA)
    .bind(format!("tests/{SEPOLIA}.toml"))
    .bind(payload.to_string())
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name, contract_instance_id,
            declared_address, role, proxy_kind
         ) VALUES ($1, $2, 'contract', 'test', $3, $4, 'test', 'none')",
    )
    .bind(manifest_id)
    .bind(SEPOLIA)
    .bind(contract_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(contract_id)
    .bind(SEPOLIA)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}
