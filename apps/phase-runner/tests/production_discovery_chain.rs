//! Discovery repair over a chain of announced ENSv2 registries, through the production phases.
//! Each block announces one registry with `RegistryCreated()`, and the registry announced in the
//! block before mounts it with `SubregistryUpdated`, so every Interpret batch of one block admits
//! one more registry whose own logs Ingest has to fetch. One registry also emits `Upgraded` naming
//! a declared resolver implementation, and its resolver record follows in the last block. The
//! chain is longer than the repair loop's pass limit (one pass per active discovery rule plus a
//! scheduling margin), so a repair that admitted one registry per pass would stop with the
//! loop's exhaustion error. The run must instead converge in at most three passes.
#[allow(dead_code)]
mod support;

use std::sync::{Arc, Mutex};

use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, SourceRole, TimingConfig},
    error::RunnerResult,
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    phase::{
        AfterProgressFuture, CompletedPhaseFuture, LoopbackPhase, Phase, PhaseBatchOutcome,
        PhaseContext, PhaseFuture, PhaseName, PhaseProgress, PhaseSet, RunMode, VerificationLevel,
    },
    runner::PhaseRunner,
};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, chain_double};

const CHAIN: &str = "ethereum-sepolia";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
/// The declared registry, which mounts the first announced registry.
const REGISTRY: &str = "0x0000000000000000000000000000000000000047";
const IMPLEMENTATION: &str = "0x0000000000000000000000000000000000000077";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const SENDER: &str = "0x00000000000000000000000000000000000000a2";
/// Announced registries. The fixture has three active discovery rules, so the repair loop allows
/// eleven passes. One registry per pass would need twelve.
const CHAIN_LENGTH: i64 = 12;
/// The announced registry that also announces itself as a resolver.
const RESOLVER_INDEX: i64 = 6;
/// The last block, which carries that resolver's record.
const HEAD: i64 = CHAIN_LENGTH + 1;
const MAXIMUM_REPAIR_PASSES: usize = 3;

sol! {
    event RegistryCreated();
    event LabelRegistered(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        address owner,
        uint64 expiry,
        address indexed sender
    );
    event SubregistryUpdated(
        uint256 indexed tokenId,
        address indexed subregistry,
        address indexed sender
    );
    event Upgraded(address indexed implementation);
    event TextChanged(
        bytes32 indexed node,
        string indexed indexedKey,
        string key,
        string value
    );
}

#[tokio::test]
async fn announced_registry_chain_converges_within_three_repair_passes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_chain").await?;
    seed_manifests(scratch.pool()).await?;
    let logs = fixture_logs()?;
    let (endpoint, server) = spawn_rpc(logs).await?;
    let sequence = Arc::new(Mutex::new(Vec::new()));
    let capacity = CapacityConfig {
        interpret_blocks_per_batch: std::num::NonZeroU32::MIN,
        ..CapacityConfig::default()
    };
    let phases = PhaseSet::new([
        recorded(
            Arc::new(IngestPhase::new(scratch.pool().clone())),
            &sequence,
        ),
        recorded(
            Arc::new(InterpretPhase::from_capacity(
                scratch.pool().clone(),
                &capacity,
            )),
            &sequence,
        ),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(QuickVerifyPhase),
        Arc::new(CompleteLivePhase),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-discovery-chain",
        fast_timing(),
    )?;
    let chain = ChainConfig::new(
        CHAIN,
        vec![SourceConfig::new_with_role(
            CHAIN,
            "intake",
            "drpc",
            SeedBasis::EthereumHead,
            0,
            SourceRole::Intake,
            endpoint,
        )?],
        true,
    )?;

    let outcome = runner.run_chain(&chain, CancellationToken::new()).await;
    let sequence = sequence.lock().expect("sequence lock").clone();
    println!("discovery chain phase runs: {sequence:?}");
    if let Err(error) = &outcome {
        panic!("the announced registry chain did not converge: {error}");
    }

    let interpret_batches = sequence
        .iter()
        .filter(|(phase, _)| *phase == PhaseName::Interpret)
        .count();
    assert!(
        interpret_batches >= HEAD as usize,
        "each block must be its own Interpret batch, saw {interpret_batches}"
    );
    let ingest_redos = runs(&sequence, PhaseName::Ingest, "redo");
    let interpret_redos = runs(&sequence, PhaseName::Interpret, "redo");
    println!(
        "discovery chain repair: {ingest_redos} Ingest redo runs, {interpret_redos} Interpret redo runs"
    );
    assert!(
        ingest_redos <= MAXIMUM_REPAIR_PASSES,
        "{ingest_redos} Ingest repair passes"
    );
    assert!(
        interpret_redos <= MAXIMUM_REPAIR_PASSES,
        "{interpret_redos} Interpret repair passes"
    );

    let (announcements, mounts, record): (i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM discovery_edges
              WHERE chain_id = $1 AND edge_kind = 'registry_announcement'),
             (SELECT count(*) FROM raw_logs
              WHERE chain_id = $1 AND lower(topics[1]) = lower($2)),
             (SELECT count(*) FROM raw_logs
              WHERE chain_id = $1 AND lower(emitting_address) = lower($3)
                AND lower(topics[1]) = lower($4))",
    )
    .bind(CHAIN)
    .bind(format!("{:#x}", SubregistryUpdated::SIGNATURE_HASH))
    .bind(registry(RESOLVER_INDEX))
    .bind(format!("{:#x}", TextChanged::SIGNATURE_HASH))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(announcements, CHAIN_LENGTH, "every registry is admitted");
    assert_eq!(
        mounts, CHAIN_LENGTH,
        "every mount, each logged by an announced registry but the first, is fetched"
    );
    assert_eq!(record, 1, "the discovered resolver's record is fetched");
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state WHERE chain_id = $1 AND redo_in_progress",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(pending, 0);

    server.abort();
    scratch.cleanup().await
}

/// The number of separate runs of `phase` in `mode`: batches of one run are adjacent.
fn runs(sequence: &[(PhaseName, &'static str)], phase: PhaseName, mode: &str) -> usize {
    let mut count = 0;
    let mut previous = None;
    for entry in sequence {
        let matches = entry.0 == phase && entry.1 == mode;
        if matches && previous != Some(true) {
            count += 1;
        }
        previous = Some(matches);
    }
    count
}

fn registry(index: i64) -> String {
    format!("0x{:040x}", 0xc000 + index)
}

fn block_hash(number: i64) -> String {
    format!("0x{:064x}", 0x100 + number)
}

fn transaction_hash(number: i64) -> String {
    format!("0x{:064x}", 0x200 + number)
}

fn versioned_token(label: &str) -> U256 {
    let mut bytes = *keccak256(label.as_bytes());
    bytes[28..].copy_from_slice(&1u32.to_be_bytes());
    U256::from_be_bytes(bytes)
}

fn raw_namehash(labels: &[&[u8]]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        let mut bytes = [0u8; 64];
        bytes[..32].copy_from_slice(node.as_slice());
        bytes[32..].copy_from_slice(keccak256(label).as_slice());
        keccak256(bytes)
    })
}

/// Block `i` announces registry `i`, and the declared registry (for `i` = 1) or registry `i - 1`
/// registers a label and mounts registry `i` under it.
fn fixture_logs() -> Result<Vec<Value>> {
    let mut logs = Vec::new();
    for number in 1..=CHAIN_LENGTH {
        let mut block = vec![(registry(number), RegistryCreated {}.encode_log_data())];
        let parent = if number == 1 {
            REGISTRY.to_owned()
        } else {
            registry(number - 1)
        };
        let label = format!("link{number}");
        let token = versioned_token(&label);
        block.push((
            parent.clone(),
            LabelRegistered {
                tokenId: token,
                labelHash: keccak256(label.as_bytes()),
                label: label.clone(),
                owner: OWNER.parse()?,
                expiry: 10_000,
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ));
        block.push((
            parent,
            SubregistryUpdated {
                tokenId: token,
                subregistry: registry(number).parse()?,
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ));
        if number == RESOLVER_INDEX {
            block.push((
                registry(number),
                Upgraded {
                    implementation: IMPLEMENTATION.parse()?,
                }
                .encode_log_data(),
            ));
        }
        logs.extend(
            block
                .into_iter()
                .enumerate()
                .map(|(index, (address, log))| encoded_log(&address, number, index as i64, log)),
        );
    }
    logs.push(encoded_log(
        &registry(RESOLVER_INDEX),
        HEAD,
        0,
        TextChanged {
            node: raw_namehash(&[b"link1", b"eth"]),
            indexedKey: keccak256(b"url"),
            key: "url".into(),
            value: "https://example.test".into(),
        }
        .encode_log_data(),
    ));
    Ok(logs)
}

fn encoded_log(address: &str, number: i64, index: i64, log: alloy_primitives::LogData) -> Value {
    json!({
        "blockHash": block_hash(number),
        "blockNumber": format!("0x{number:x}"),
        "transactionHash": transaction_hash(number),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{index:x}"),
        "address": address,
        "topics": log.topics().iter().map(|topic| format!("{topic:#x}")).collect::<Vec<_>>(),
        "data": format!("0x{}", alloy_primitives::hex::encode(log.data)),
    })
}

async fn spawn_rpc(logs: Vec<Value>) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(rpc))
                .with_state(Arc::new(logs)),
        )
        .await
        .expect("discovery chain RPC server");
    });
    Ok((format!("http://{address}/"), server))
}

async fn rpc(State(logs): State<Arc<Vec<Value>>>, Json(request): Json<Value>) -> Json<Value> {
    let answer = |request: &Value| {
        chain_double::node_shaped(request, &|inner: &Value| respond(&logs, inner))
    };
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(requests.iter().map(&answer).collect()));
    }
    Json(answer(&request))
}

/// A node serving blocks 0 through `HEAD`, all finalized. `chain_double::node_shaped` adds the
/// header blooms, the receipts' logs and the per-transaction methods.
fn respond(logs: &[Value], request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let full = params.get(1) == Some(&Value::Bool(true));
    let by_hash = |hash: Option<&Value>| {
        let hash = hash.and_then(Value::as_str)?;
        (0..=HEAD).find(|number| block_hash(*number).eq_ignore_ascii_case(hash))
    };
    let result = match request["method"].as_str().unwrap_or_default() {
        "eth_getBlockByNumber" => params
            .first()
            .and_then(Value::as_str)
            .and_then(|selector| match selector {
                "latest" | "safe" | "finalized" => Some(HEAD),
                number => quantity(number).filter(|number| (0..=HEAD).contains(number)),
            })
            .map(|number| block(number, full)),
        "eth_getBlockByHash" => by_hash(params.first()).map(|number| block(number, full)),
        "eth_getLogs" => Some(Value::Array(filter_logs(
            logs,
            &params.first().cloned().unwrap_or_default(),
        ))),
        "eth_getBlockReceipts" => by_hash(params.first()).map(|number| {
            if number == 0 {
                json!([])
            } else {
                json!([{
                    "transactionHash": transaction_hash(number),
                    "blockHash": block_hash(number),
                    "blockNumber": format!("0x{number:x}"),
                    "transactionIndex": "0x0",
                    "status": "0x1",
                    "cumulativeGasUsed": "0x5208",
                    "gasUsed": "0x5208",
                    "logsBloom": "0x",
                }])
            }
        }),
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn block(number: i64, full: bool) -> Value {
    let parent = if number == 0 {
        format!("0x{}", "00".repeat(32))
    } else {
        block_hash(number - 1)
    };
    let transactions = if number == 0 {
        json!([])
    } else if full {
        json!([{
            "hash": transaction_hash(number),
            "blockHash": block_hash(number),
            "blockNumber": format!("0x{number:x}"),
            "transactionIndex": "0x0",
            "from": SENDER,
            "to": REGISTRY,
            "input": "0x",
            "value": "0x0",
        }])
    } else {
        json!([transaction_hash(number)])
    };
    json!({
        "hash": block_hash(number),
        "parentHash": parent,
        "number": format!("0x{number:x}"),
        "timestamp": format!("0x{:x}", number + 100),
        "logsBloom": "0x",
        "transactions": transactions,
    })
}

fn filter_logs(logs: &[Value], filter: &Value) -> Vec<Value> {
    let block = filter.get("blockHash").and_then(Value::as_str);
    let from = filter
        .get("fromBlock")
        .and_then(Value::as_str)
        .and_then(quantity)
        .unwrap_or(0);
    let to = filter
        .get("toBlock")
        .and_then(Value::as_str)
        .and_then(quantity)
        .unwrap_or(i64::MAX);
    let addresses = values(filter.get("address"));
    let topics = values(filter.pointer("/topics/0"));
    let matches = |wanted: &[String], actual: Option<&str>| {
        wanted.is_empty()
            || wanted
                .iter()
                .any(|value| value.eq_ignore_ascii_case(actual.unwrap_or_default()))
    };
    logs.iter()
        .filter(|log| {
            let number = log["blockNumber"]
                .as_str()
                .and_then(quantity)
                .unwrap_or_default();
            block.is_none_or(|hash| log["blockHash"].as_str() == Some(hash))
                && (from..=to).contains(&number)
                && matches(&addresses, log["address"].as_str())
                && matches(&topics, log.pointer("/topics/0").and_then(Value::as_str))
        })
        .cloned()
        .collect()
}

fn values(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(value)) => vec![value.clone()],
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn quantity(value: &str) -> Option<i64> {
    i64::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}

async fn seed_manifests(pool: &sqlx::PgPool) -> Result<()> {
    let rules = [
        ("subregistry", "reachable_from_root"),
        ("resolver", "reachable_from_root"),
        ("registry_announcement", "reachable_from_root"),
    ];
    let registry_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": CHAIN,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{"role": "registry", "address": REGISTRY, "proxy_kind": "none", "start_block": 0}],
        "discovery_rules": rules
            .iter()
            .map(|(edge_kind, admission)| json!({"edge_kind": edge_kind, "from_role": "registry", "admission": admission}))
            .collect::<Vec<_>>(),
        "abi": {"events": [
            {
                "name": "RegistryCreated",
                "fragment": "event RegistryCreated()",
                "emitter_roles": [],
                "normalized_events": ["SurfaceUnbound", "RegistrationReleased", "RegistryCreated"]
            },
            {
                "name": "LabelRegistered",
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["SurfaceUnbound", "RegistrationReleased", "RegistrationGranted", "PreimageObserved", "ResolverChanged", "SubregistryChanged"]
            },
            {
                "name": "SubregistryUpdated",
                "fragment": "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["SurfaceUnbound", "RegistrationReleased", "SubregistryChanged", "PreimageObserved", "SurfaceBound", "RegistrationGranted", "AuthorityTransferred", "ExpiryChanged", "ResolverChanged"]
            }
        ], "calls": []}
    });
    let resolver_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_resolver_l1",
        "chain": CHAIN,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "resolver_implementations": [{"role": "permissioned_resolver", "address": IMPLEMENTATION}],
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": {"events": [
            {
                "name": "Upgraded",
                "fragment": "event Upgraded(address indexed implementation)",
                "emitter_roles": [],
                "normalized_events": ["Upgraded"]
            },
            {
                "name": "TextChanged",
                "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            }
        ], "calls": []}
    });
    let registry_manifest = insert_manifest(
        pool,
        "ens_v2_registry_l1",
        "tests/chain-registry.toml",
        registry_payload,
    )
    .await?;
    insert_manifest(
        pool,
        "ens_v2_resolver_l1",
        "tests/chain-resolver.toml",
        resolver_payload,
    )
    .await?;
    let instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(instance)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name, contract_instance_id,
             declared_address, role, proxy_kind, start_block_number
         ) VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)",
    )
    .bind(registry_manifest)
    .bind(CHAIN)
    .bind(instance)
    .bind(REGISTRY)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}')",
    )
    .bind(instance)
    .bind(CHAIN)
    .bind(REGISTRY)
    .bind(registry_manifest)
    .execute(pool)
    .await?;
    for (edge_kind, admission) in rules {
        sqlx::query(
            "INSERT INTO manifest_discovery_rules (
                 manifest_id, edge_kind, from_role, admission, rule_payload
             ) VALUES ($1, $2, 'registry', $3, $4)",
        )
        .bind(registry_manifest)
        .bind(edge_kind)
        .bind(admission)
        .bind(json!({"edge_kind": edge_kind, "from_role": "registry", "admission": admission}))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn insert_manifest(
    pool: &sqlx::PgPool,
    family: &str,
    path: &str,
    payload: Value,
) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', $1, $2, 'fixture', 'active', $3, $4, $5)
         RETURNING manifest_id",
    )
    .bind(family)
    .bind(CHAIN)
    .bind(NORMALIZER)
    .bind(path)
    .bind(payload)
    .fetch_one(pool)
    .await?)
}

fn fast_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: std::time::Duration::from_millis(1),
        maximum_backoff: std::time::Duration::from_millis(4),
        live_poll_interval: std::time::Duration::from_millis(1),
    }
}

/// Records each batch's phase and mode before running it.
struct RecordedPhase {
    inner: Arc<dyn Phase>,
    sequence: Arc<Mutex<Vec<(PhaseName, &'static str)>>>,
}

fn recorded(
    inner: Arc<dyn Phase>,
    sequence: &Arc<Mutex<Vec<(PhaseName, &'static str)>>>,
) -> Arc<dyn Phase> {
    Arc::new(RecordedPhase {
        inner,
        sequence: Arc::clone(sequence),
    })
}

impl Phase for RecordedPhase {
    fn name(&self) -> PhaseName {
        self.inner.name()
    }

    fn preflight(&self, chain: &str, sources: &[SourceConfig], mode: &RunMode) -> RunnerResult<()> {
        self.inner.preflight(chain, sources, mode)
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        self.sequence
            .lock()
            .expect("sequence lock")
            .push((context.phase, context.mode.as_str()));
        self.inner.run_batch(context)
    }

    fn revalidates_completed(&self, chain: &str, sources: &[SourceConfig]) -> RunnerResult<bool> {
        self.inner.revalidates_completed(chain, sources)
    }

    fn revalidate_completed(&self, context: PhaseContext) -> CompletedPhaseFuture<'_> {
        self.inner.revalidate_completed(context)
    }

    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        self.inner.after_progress_recorded(chain_id)
    }

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.inner.has_after_progress_work(chain_id)
    }
}

struct CompleteLivePhase;

impl Phase for CompleteLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { Ok(PhaseBatchOutcome::Complete(PhaseProgress::default())) })
    }
}

struct QuickVerifyPhase;

impl Phase for QuickVerifyPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async {
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                verification_level: Some(VerificationLevel::QuickSynced),
                ..PhaseProgress::default()
            }))
        })
    }
}
