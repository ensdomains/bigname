#[allow(dead_code)]
mod support;

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use alloy_primitives::{Address, U256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::{
    BatchRequest as IngestRequest, Engine as IngestEngine, LiveBatchRequest,
    Marker as IngestMarker, SourceCursor, SourceDescriptor, load_persisted_watch_filter,
};
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, Marker as InterpretMarker,
    RunMode as InterpretRunMode,
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_storage::{
    AddressNameRelation, EventHistoryAddressFilter, EventHistoryFilter, HistoryScope,
    HistorySummaryMode, load_address_history, load_event_history_page,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use support::ScratchDatabase;

const CHAIN: &str = "ethereum-sepolia";
const ANNOUNCEMENT_BLOCK: i64 = 11_163_420;
const LATER_BLOCK: i64 = ANNOUNCEMENT_BLOCK + 1;
const ORPHANED_ANNOUNCEMENT_HASH: &str =
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff42";
const ORPHANED_EVENT_IDENTITY: &str = "migration-restart:orphaned-event";
const FACTORY: &str = "0x118bc31a50d559f7015a8da26d54b3b030cdb70f";
const LOCKED_CONTROLLER: &str = "0x681802eff57b83edce99d688c023ab1284495176";
const PROXY: &str = "0x0000000000000000000000000000000000000045";
const PARENT: &str = "0x544d3e88f4ab566e7a3f9229daab5caad98e233a";

sol! {
    event RegistryCreated();
    event ProxyDeployed(
        address indexed sender,
        address indexed proxyAddress,
        uint256 salt,
        address implementation
    );
    event ParentUpdated(address indexed parent, string label, address indexed sender);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event TransferSingle(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256 id,
        uint256 value
    );
    event TransferBatch(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256[] ids,
        uint256[] values
    );
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
}

const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";

#[derive(Clone, Copy, Debug)]
enum RestartLane {
    FullHistoricalReplay,
    LiveFollow,
}

#[derive(Debug, Eq, PartialEq)]
struct LaneSnapshot {
    later_raw_count: i64,
    ordinary_event: (String, String, Vec<String>, Value),
    candidate_association: (String, String),
    candidate_factory_event_count: i64,
    discovery_association_count: i64,
}

#[tokio::test]
async fn migration_registry_restart_retains_later_facts_in_replay_and_live_follow() -> Result<()> {
    let replay = run_lane(RestartLane::FullHistoricalReplay).await?;
    let live = run_lane(RestartLane::LiveFollow).await?;

    assert_eq!(replay, live);
    assert_eq!(replay.later_raw_count, 1);
    assert_eq!(replay.ordinary_event.1, "activated");
    assert!(replay.ordinary_event.2.is_empty());
    assert_eq!(
        replay.candidate_association.0,
        "migration_registry_creation"
    );
    assert_eq!(replay.candidate_association.1, "candidate");
    assert_eq!(replay.candidate_factory_event_count, 1);
    assert_eq!(replay.discovery_association_count, 1);
    Ok(())
}

#[tokio::test]
async fn candidate_address_relation_cannot_create_a_product_history_selector() -> Result<()> {
    let scratch = seed_candidate_address_history("migration_candidate_address_product").await?;
    let rows = load_address_history(
        scratch.pool(),
        CANDIDATE_ADDRESS,
        Some("ens"),
        Some(AddressNameRelation::EffectiveController),
        HistoryScope::Surface,
        true,
    )
    .await?;
    assert!(
        rows.is_empty(),
        "candidate authority evidence must be filtered before it can select activated history"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn candidate_address_relation_creates_a_diagnostic_history_selector() -> Result<()> {
    let scratch = seed_candidate_address_history("migration_candidate_address_diagnostic").await?;
    let page = load_event_history_page(
        scratch.pool(),
        EventHistoryFilter {
            namespace: Some("ens".to_owned()),
            address: Some(EventHistoryAddressFilter {
                address: CANDIDATE_ADDRESS.to_owned(),
                relation: Some(AddressNameRelation::EffectiveController),
            }),
            ..EventHistoryFilter::default()
        },
        true,
        None,
        20,
        HistorySummaryMode::None,
        true,
    )
    .await?;
    assert_eq!(
        page.rows
            .iter()
            .map(|row| (
                row.event_identity.as_str(),
                row.consumer_visibility.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("activated-same-name-history", "activated"),
            ("candidate-address-anchor", "candidate"),
        ]
    );

    scratch.cleanup().await
}

const CANDIDATE_ADDRESS: &str = "0x00000000000000000000000000000000000000cc";
const CANDIDATE_LOGICAL_NAME: &str = "ens:candidate-history-anchor";

async fn seed_candidate_address_history(name: &str) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(name).await?;
    for (number, hash) in [(1_i64, "candidate-anchor-1"), (2, "candidate-anchor-2")] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
        )
        .bind(CHAIN)
        .bind(hash)
        .bind(number)
        .execute(scratch.pool())
        .await?;
    }
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'candidate-history-anchor', ARRAY['candidate-history-anchor'],
             decode('', 'hex'), 'candidate-history-anchor', ARRAY['candidate-history-anchor'],
             'test', 'active', $2, 'candidate-anchor-1', 1, 'canonical'
         )",
    )
    .bind(CANDIDATE_LOGICAL_NAME)
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state,
             after_state, migration_correlation_ids, consumer_visibility
         ) VALUES
             ('candidate-address-anchor', 'ens', $1, 'AuthorityTransferred',
              'ens_v1_registry_l1', 1, $2, 1, 'candidate-anchor-1', 'candidate-tx-1',
              0, 0, 'ens_v1_unwrapped_authority', 'canonical',
              jsonb_build_object('owner', $3), ARRAY['candidate-correlation'], 'candidate'),
             ('activated-same-name-history', 'ens', $1, 'RecordChanged',
              'ens_v1_registry_l1', 1, $2, 2, 'candidate-anchor-2', 'candidate-tx-2',
              0, 0, 'ens_v1_unwrapped_authority', 'canonical',
              '{}'::jsonb, ARRAY[]::text[], 'activated')",
    )
    .bind(CANDIDATE_LOGICAL_NAME)
    .bind(CHAIN)
    .bind(CANDIDATE_ADDRESS)
    .execute(scratch.pool())
    .await?;
    Ok(scratch)
}

/// The Sepolia ENSv1 registry and wrapper admissions make the logs a migrated child's ENSv1
/// cleanup is carried by ingestible on this deployment profile. Before them the migration manifest
/// named a NameWrapper correlation address that no admitted source watched, so a child's cleanup
/// could never be observed and no child boundary could derive from it.
///
/// This pins ingestibility of those inputs only. It does not claim an ENSv1 authority arm for .eth
/// second-level authority-transition boundaries: that still needs the ENSv1 registrar family,
/// which this profile does not admit.
#[tokio::test]
async fn sepolia_profile_watches_the_ens_v1_cleanup_and_registry_surfaces() -> Result<()> {
    let scratch = ScratchDatabase::create("sepolia_ens_v1_admission_watch").await?;
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(scratch.pool(), &load_repository(manifest_root)?).await?;

    let watch = load_persisted_watch_filter(
        scratch.pool(),
        CHAIN,
        ANNOUNCEMENT_BLOCK,
        ANNOUNCEMENT_BLOCK,
    )
    .await?;

    // Both branches a migrated child's ENSv1 control can end on: the wrapper token parked in the
    // Graveyard still wrapped, and the node unwrapped into it.
    for (label, topic) in [
        ("NameUnwrapped", NameUnwrapped::SIGNATURE_HASH),
        ("TransferSingle", TransferSingle::SIGNATURE_HASH),
        ("TransferBatch", TransferBatch::SIGNATURE_HASH),
    ] {
        assert!(
            watch.includes(NAME_WRAPPER, &format!("{topic:#x}"), ANNOUNCEMENT_BLOCK),
            "ENSv1 cleanup surface {label} must be ingestible on the Sepolia profile"
        );
    }

    assert!(
        watch.includes(
            ENS_REGISTRY,
            &format!("{:#x}", NewOwner::SIGNATURE_HASH),
            ANNOUNCEMENT_BLOCK
        ),
        "the ENSv1 registry ownership surface must be ingestible on the Sepolia profile"
    );

    scratch.cleanup().await
}

async fn run_lane(lane: RestartLane) -> Result<LaneSnapshot> {
    let suffix = match lane {
        RestartLane::FullHistoricalReplay => "replay",
        RestartLane::LiveFollow => "live",
    };
    let scratch = ScratchDatabase::create(&format!("migration_registry_restart_{suffix}")).await?;
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(scratch.pool(), &load_repository(manifest_root)?).await?;

    let rpc = MigrationRpc::spawn(ANNOUNCEMENT_BLOCK).await?;
    let source = SourceDescriptor {
        key: "rpc".to_owned(),
        kind: "rpc".to_owned(),
        start_block: ANNOUNCEMENT_BLOCK,
        endpoint: rpc.endpoint.clone(),
    };

    let first_ingest = IngestEngine::new(scratch.pool().clone())
        .run_batch(IngestRequest {
            chain_id: CHAIN.to_owned(),
            sources: vec![source.clone()],
            cursors: Vec::new(),
            redo_range: None,
            resume_current: None,
        })
        .await?;
    assert!(first_ingest.complete);
    assert_eq!(first_ingest.current.number, ANNOUNCEMENT_BLOCK);
    mark_finalized(scratch.pool(), ANNOUNCEMENT_BLOCK).await?;
    publish_head(scratch.pool(), ANNOUNCEMENT_BLOCK).await?;

    let first_interpret = InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: ANNOUNCEMENT_BLOCK,
            to_block: ANNOUNCEMENT_BLOCK,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(first_interpret.complete);

    let edge_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM discovery_edges edge
         JOIN contract_instance_addresses address
           ON address.contract_instance_id = edge.to_contract_instance_id
          AND address.chain_id = edge.chain_id
         WHERE edge.chain_id = $1
           AND edge.edge_kind = 'registry_announcement'
           AND lower(address.address) = lower($2)
           AND edge.deactivated_at IS NULL",
    )
    .bind(CHAIN)
    .bind(PROXY)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edge_count, 1, "block N must persist the ordinary edge");

    // This loader intentionally excludes the retained-log and same-window announcement
    // supplements. Inclusion here proves that a fresh Ingest instance traverses the edge.
    let persisted_watch =
        load_persisted_watch_filter(scratch.pool(), CHAIN, LATER_BLOCK, LATER_BLOCK).await?;
    let parent_topic = format!("{:#x}", ParentUpdated::SIGNATURE_HASH);
    assert!(persisted_watch.includes(PROXY, &parent_topic, LATER_BLOCK));

    rpc.head.store(LATER_BLOCK, Ordering::SeqCst);
    match lane {
        RestartLane::FullHistoricalReplay => {
            let later_ingest = IngestEngine::new(scratch.pool().clone())
                .run_batch(IngestRequest {
                    chain_id: CHAIN.to_owned(),
                    sources: vec![source],
                    cursors: vec![SourceCursor {
                        key: "rpc".to_owned(),
                        next_block: LATER_BLOCK,
                        target_block: None,
                        last_processed: Some(first_ingest.current),
                        redo_loaded_boundary: None,
                    }],
                    redo_range: None,
                    resume_current: None,
                })
                .await?;
            assert!(later_ingest.complete);
            mark_finalized(scratch.pool(), LATER_BLOCK).await?;
            sqlx::query(
                "UPDATE migration_event_associations
                 SET evidence_refs = '[{\"stale_redo_evidence\":true}]'::jsonb
                 WHERE chain_id = $1 AND block_number = $2",
            )
            .bind(CHAIN)
            .bind(ANNOUNCEMENT_BLOCK)
            .execute(scratch.pool())
            .await?;
            seed_orphaned_discovery_association(scratch.pool()).await?;
            seed_orphaned_event_association(scratch.pool()).await?;
            InterpretEngine::new(scratch.pool().clone())
                .run_batch(InterpretRequest {
                    chain_id: CHAIN.to_owned(),
                    from_block: ANNOUNCEMENT_BLOCK,
                    to_block: LATER_BLOCK,
                    resume_current: None,
                    mode: InterpretRunMode::Redo,
                })
                .await?;
            let orphaned_association_count: i64 = sqlx::query_scalar(
                "SELECT count(*)
                 FROM migration_discovery_associations
                 WHERE chain_id = $1 AND block_hash = $2",
            )
            .bind(CHAIN)
            .bind(ORPHANED_ANNOUNCEMENT_HASH)
            .fetch_one(scratch.pool())
            .await?;
            assert_eq!(
                orphaned_association_count, 1,
                "redo must retain old-fork discovery evidence under its original lineage"
            );
            let orphaned_event_count: i64 = sqlx::query_scalar(
                "SELECT count(*)
                 FROM normalized_events
                 WHERE event_identity = $1",
            )
            .bind(ORPHANED_EVENT_IDENTITY)
            .fetch_one(scratch.pool())
            .await?;
            assert_eq!(
                orphaned_event_count, 0,
                "redo must still clear old-fork normalized events in its range"
            );
            let orphaned_event_association_count: i64 = sqlx::query_scalar(
                "SELECT count(*)
                 FROM migration_event_associations
                 WHERE event_identity = $1",
            )
            .bind(ORPHANED_EVENT_IDENTITY)
            .fetch_one(scratch.pool())
            .await?;
            assert_eq!(
                orphaned_event_association_count, 1,
                "redo must retain old-fork event-correlation evidence after parent cleanup"
            );
        }
        RestartLane::LiveFollow => {
            let later_ingest = IngestEngine::new(scratch.pool().clone())
                .run_live_batch(LiveBatchRequest {
                    chain_id: CHAIN.to_owned(),
                    sources: vec![source],
                    live_handoff: IngestMarker {
                        number: ANNOUNCEMENT_BLOCK,
                        hash: block_hash(ANNOUNCEMENT_BLOCK),
                    },
                })
                .await?;
            assert!(later_ingest.caught_up);
            mark_finalized(scratch.pool(), LATER_BLOCK).await?;
            InterpretEngine::new(scratch.pool().clone())
                .run_batch(InterpretRequest {
                    chain_id: CHAIN.to_owned(),
                    from_block: ANNOUNCEMENT_BLOCK,
                    to_block: LATER_BLOCK,
                    resume_current: Some(InterpretMarker {
                        number: ANNOUNCEMENT_BLOCK,
                        hash: block_hash(ANNOUNCEMENT_BLOCK),
                    }),
                    mode: InterpretRunMode::Normal,
                })
                .await?;
        }
    }

    let later_raw_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs
         WHERE chain_id = $1 AND block_number = $2
           AND lower(emitting_address) = lower($3)",
    )
    .bind(CHAIN)
    .bind(LATER_BLOCK)
    .bind(PROXY)
    .fetch_one(scratch.pool())
    .await?;
    let ordinary_event: (String, String, Vec<String>, Value) = sqlx::query_as(
        "SELECT event_identity, consumer_visibility, migration_correlation_ids, after_state
         FROM normalized_events
         WHERE chain_id = $1 AND block_number = $2
           AND event_kind = 'ParentChanged'",
    )
    .bind(CHAIN)
    .bind(LATER_BLOCK)
    .fetch_one(scratch.pool())
    .await?;
    let candidate_association: (String, String) = sqlx::query_as(
        "SELECT association.correlation_kind, association.consumer_visibility
         FROM migration_event_associations association
         WHERE association.event_identity = $1",
    )
    .bind(&ordinary_event.0)
    .fetch_one(scratch.pool())
    .await?;
    let candidate_factory_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = $2
           AND event_kind = 'ContractDiscovered'
           AND consumer_visibility = 'candidate'",
    )
    .bind(CHAIN)
    .bind(ANNOUNCEMENT_BLOCK)
    .fetch_one(scratch.pool())
    .await?;
    let discovery_association_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM migration_discovery_associations association
         JOIN chain_lineage lineage
           ON lineage.chain_id = association.chain_id
          AND lineage.block_hash = association.block_hash
          AND lineage.block_number = association.block_number
         WHERE association.chain_id = $1 AND association.block_number = $2
           AND lower(association.registry_address) = lower($3)
           AND association.consumer_visibility = 'candidate'
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(CHAIN)
    .bind(ANNOUNCEMENT_BLOCK)
    .bind(PROXY)
    .fetch_one(scratch.pool())
    .await?;

    let snapshot = LaneSnapshot {
        later_raw_count,
        ordinary_event,
        candidate_association,
        candidate_factory_event_count,
        discovery_association_count,
    };
    if matches!(lane, RestartLane::FullHistoricalReplay) {
        sqlx::query(
            "UPDATE migration_event_associations
             SET evidence_refs = '[{\"conflicting_evidence\":true}]'::jsonb
             WHERE chain_id = $1 AND block_number = $2",
        )
        .bind(CHAIN)
        .bind(ANNOUNCEMENT_BLOCK)
        .execute(scratch.pool())
        .await?;
        let error = InterpretEngine::new(scratch.pool().clone())
            .run_batch(InterpretRequest {
                chain_id: CHAIN.to_owned(),
                from_block: ANNOUNCEMENT_BLOCK,
                to_block: ANNOUNCEMENT_BLOCK,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
            .expect_err("an immutable association conflict must fail closed");
        assert!(
            error
                .to_string()
                .contains("already bound to different evidence")
        );
    }
    rpc.server.abort();
    scratch.cleanup().await?;
    Ok(snapshot)
}

async fn seed_orphaned_discovery_association(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp,
             canonicality_state
         ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'orphaned')",
    )
    .bind(CHAIN)
    .bind(ORPHANED_ANNOUNCEMENT_HASH)
    .bind(block_hash(ANNOUNCEMENT_BLOCK - 1))
    .bind(ANNOUNCEMENT_BLOCK)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO migration_discovery_associations (
             logical_edge_identity, migration_correlation_id, correlation_kind,
             registry_contract_instance_id, registry_address, source_manifest_id,
             evidence_refs, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, canonicality_state, consumer_visibility,
             interpreter_content_hash
         )
         SELECT logical_edge_identity, migration_correlation_id || ':orphaned', correlation_kind,
                registry_contract_instance_id, registry_address, source_manifest_id,
                evidence_refs, chain_id, block_number, $2,
                transaction_hash || ':orphaned', transaction_index, log_index,
                'orphaned', consumer_visibility, interpreter_content_hash
         FROM migration_discovery_associations
         WHERE chain_id = $1 AND block_number = $3
         LIMIT 1",
    )
    .bind(CHAIN)
    .bind(ORPHANED_ANNOUNCEMENT_HASH)
    .bind(ANNOUNCEMENT_BLOCK)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_orphaned_event_association(pool: &sqlx::PgPool) -> Result<()> {
    let inserted_parent = sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, source_manifest_id, chain_id, block_number,
             block_hash, transaction_hash, transaction_index, log_index, raw_fact_ref,
             derivation_kind, canonicality_state, before_state, after_state,
             migration_correlation_ids, consumer_visibility
         )
         SELECT $1, event.namespace, event.logical_name_id, event.resource_id, event.event_kind,
                event.source_family, event.manifest_version, event.source_manifest_id,
                event.chain_id, event.block_number, $2, event.transaction_hash || ':orphaned',
                event.transaction_index, event.log_index, event.raw_fact_ref,
                event.derivation_kind, 'orphaned', event.before_state, event.after_state,
                event.migration_correlation_ids, event.consumer_visibility
         FROM normalized_events event
         JOIN migration_event_associations association
           ON association.event_identity = event.event_identity
         WHERE event.chain_id = $3 AND event.block_number = $4
         LIMIT 1",
    )
    .bind(ORPHANED_EVENT_IDENTITY)
    .bind(ORPHANED_ANNOUNCEMENT_HASH)
    .bind(CHAIN)
    .bind(ANNOUNCEMENT_BLOCK)
    .execute(pool)
    .await?;
    assert_eq!(inserted_parent.rows_affected(), 1);

    let inserted_association = sqlx::query(
        "INSERT INTO migration_event_associations (
             event_identity, migration_correlation_id, correlation_kind, evidence_refs,
             chain_id, block_number, block_hash, transaction_hash, transaction_index,
             log_index, canonicality_state, consumer_visibility, interpreter_content_hash
         )
         SELECT $1, migration_correlation_id || ':orphaned', correlation_kind, evidence_refs,
                chain_id, block_number, $2, transaction_hash || ':orphaned', transaction_index,
                log_index, 'orphaned', consumer_visibility, interpreter_content_hash
         FROM migration_event_associations
         WHERE chain_id = $3 AND block_number = $4
         LIMIT 1",
    )
    .bind(ORPHANED_EVENT_IDENTITY)
    .bind(ORPHANED_ANNOUNCEMENT_HASH)
    .bind(CHAIN)
    .bind(ANNOUNCEMENT_BLOCK)
    .execute(pool)
    .await?;
    assert_eq!(inserted_association.rows_affected(), 1);
    Ok(())
}

async fn mark_finalized(pool: &sqlx::PgPool, through: i64) -> Result<()> {
    for (from, to) in [
        ("observed", "canonical"),
        ("canonical", "safe"),
        ("safe", "finalized"),
    ] {
        sqlx::query(
            "UPDATE chain_lineage
             SET canonicality_state = $3::canonicality_state
             WHERE chain_id = $1 AND block_number <= $2
               AND canonicality_state = $4::canonicality_state",
        )
        .bind(CHAIN)
        .bind(through)
        .bind(to)
        .bind(from)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn publish_head(pool: &sqlx::PgPool, number: i64) -> Result<()> {
    let hash = block_hash(number);
    sqlx::query(
        "INSERT INTO chain_heads (
             chain_id, latest_block_hash, latest_block_number,
             safe_block_hash, safe_block_number,
             finalized_block_hash, finalized_block_number
         ) VALUES ($1, $2, $3, $2, $3, $2, $3)
         ON CONFLICT (chain_id) DO UPDATE
         SET latest_block_hash = EXCLUDED.latest_block_hash,
             latest_block_number = EXCLUDED.latest_block_number,
             safe_block_hash = EXCLUDED.safe_block_hash,
             safe_block_number = EXCLUDED.safe_block_number,
             finalized_block_hash = EXCLUDED.finalized_block_hash,
             finalized_block_number = EXCLUDED.finalized_block_number,
             updated_at = now()",
    )
    .bind(CHAIN)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

struct MigrationRpc {
    endpoint: String,
    head: Arc<AtomicI64>,
    server: tokio::task::JoinHandle<()>,
}

impl MigrationRpc {
    async fn spawn(initial_head: i64) -> Result<Self> {
        let head = Arc::new(AtomicI64::new(initial_head));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_head = Arc::clone(&head);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/", post(rpc)).with_state(server_head),
            )
            .await
            .expect("migration restart RPC server");
        });
        Ok(Self {
            endpoint: format!("http://{address}/"),
            head,
            server,
        })
    }
}

async fn rpc(State(head): State<Arc<AtomicI64>>, Json(request): Json<Value>) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| rpc_response(request, head.load(Ordering::SeqCst)))
                .collect(),
        ));
    }
    Json(rpc_response(&request, head.load(Ordering::SeqCst)))
}

fn rpc_response(request: &Value, head: i64) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match request["method"].as_str().unwrap_or_default() {
        "eth_getBlockByNumber" => {
            let selected = params.first().and_then(Value::as_str).unwrap_or_default();
            let number = match selected {
                "latest" | "safe" | "finalized" => Some(head),
                value => rpc_quantity(Some(&Value::String(value.to_owned()))),
            };
            number
                .filter(|number| *number <= head)
                .map(|number| block(number, params.get(1) == Some(&Value::Bool(true))))
        }
        "eth_getBlockByHash" => {
            block_number_from_hash(params.first().and_then(Value::as_str).unwrap_or_default())
                .filter(|number| *number <= head)
                .map(|number| block(number, params.get(1) == Some(&Value::Bool(true))))
        }
        "eth_getLogs" => Some(Value::Array(range_logs(
            params.first().unwrap_or(&Value::Null),
            head,
        ))),
        "eth_getBlockReceipts" => {
            let selected = params.first().and_then(Value::as_str).unwrap_or_default();
            let number = block_number_from_hash(selected)
                .or_else(|| rpc_quantity(Some(&Value::String(selected.to_owned()))));
            number
                .filter(|number| *number <= head)
                .map(|number| json!([receipt(number)]))
        }
        _ => None,
    };
    json!({"jsonrpc":"2.0", "id":id, "result":result})
}

fn block(number: i64, full_transactions: bool) -> Value {
    let transaction_hash = transaction_hash(number);
    let transactions = if full_transactions {
        json!([{
            "hash":transaction_hash,
            "blockHash":block_hash(number),
            "blockNumber":format!("0x{number:x}"),
            "transactionIndex":"0x0",
            "from":LOCKED_CONTROLLER,
            "to":if number == ANNOUNCEMENT_BLOCK { FACTORY } else { PROXY },
            "input":"0x",
            "value":"0x0"
        }])
    } else {
        json!([transaction_hash])
    };
    json!({
        "hash":block_hash(number),
        "parentHash":block_hash(number - 1),
        "number":format!("0x{number:x}"),
        "timestamp":format!("0x{:x}", number + 1_000),
        "logsBloom":"0x",
        "transactions":transactions
    })
}

fn range_logs(filter: &Value, head: i64) -> Vec<Value> {
    let pinned = filter
        .get("blockHash")
        .and_then(Value::as_str)
        .and_then(block_number_from_hash);
    let from = pinned
        .or_else(|| rpc_quantity(filter.get("fromBlock")))
        .unwrap_or(ANNOUNCEMENT_BLOCK);
    let to = pinned
        .or_else(|| rpc_quantity(filter.get("toBlock")))
        .unwrap_or(head)
        .min(head);
    let addresses = filter
        .get("address")
        .map(string_filter_values)
        .unwrap_or_default();
    let topics = filter
        .pointer("/topics/0")
        .map(string_filter_values)
        .unwrap_or_default();
    fixture_logs()
        .into_iter()
        .filter(|log| {
            let number = rpc_quantity(log.get("blockNumber")).unwrap_or_default();
            let address = log
                .get("address")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let topic0 = log
                .pointer("/topics/0")
                .and_then(Value::as_str)
                .unwrap_or_default();
            (from..=to).contains(&number)
                && (addresses.is_empty()
                    || addresses
                        .iter()
                        .any(|expected| expected.eq_ignore_ascii_case(address)))
                && (topics.is_empty()
                    || topics
                        .iter()
                        .any(|expected| expected.eq_ignore_ascii_case(topic0)))
        })
        .collect()
}

fn fixture_logs() -> Vec<Value> {
    let sender = LOCKED_CONTROLLER
        .parse::<Address>()
        .expect("fixture address");
    let proxy = PROXY.parse::<Address>().expect("fixture address");
    let factory = FACTORY.parse::<Address>().expect("fixture address");
    let parent = PARENT.parse::<Address>().expect("fixture address");
    vec![
        encoded_log(
            RegistryCreated {}.encode_log_data(),
            PROXY,
            ANNOUNCEMENT_BLOCK,
            0,
        ),
        encoded_log(
            ProxyDeployed {
                sender,
                proxyAddress: proxy,
                salt: U256::from(17),
                implementation: factory,
            }
            .encode_log_data(),
            FACTORY,
            ANNOUNCEMENT_BLOCK,
            1,
        ),
        encoded_log(
            ParentUpdated {
                parent,
                label: "restart".to_owned(),
                sender,
            }
            .encode_log_data(),
            PROXY,
            LATER_BLOCK,
            0,
        ),
    ]
}

fn encoded_log(
    encoded: alloy_primitives::LogData,
    address: &str,
    block_number: i64,
    log_index: i64,
) -> Value {
    json!({
        "blockHash":block_hash(block_number),
        "blockNumber":format!("0x{block_number:x}"),
        "transactionHash":transaction_hash(block_number),
        "transactionIndex":"0x0",
        "logIndex":format!("0x{log_index:x}"),
        "address":address,
        "topics":encoded.topics().iter().map(|topic| format!("{topic:#x}")).collect::<Vec<_>>(),
        "data":format!("0x{}", alloy_primitives::hex::encode(encoded.data))
    })
}

fn receipt(block_number: i64) -> Value {
    json!({
        "transactionHash":transaction_hash(block_number),
        "blockHash":block_hash(block_number),
        "blockNumber":format!("0x{block_number:x}"),
        "transactionIndex":"0x0",
        "status":"0x1",
        "cumulativeGasUsed":"0x5208",
        "gasUsed":"0x5208",
        "logsBloom":"0x"
    })
}

fn string_filter_values(value: &Value) -> Vec<String> {
    value.as_array().map_or_else(
        || value.as_str().map(str::to_owned).into_iter().collect(),
        |values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        },
    )
}

fn rpc_quantity(value: Option<&Value>) -> Option<i64> {
    i64::from_str_radix(value?.as_str()?.trim_start_matches("0x"), 16).ok()
}

fn block_hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}

fn block_number_from_hash(hash: &str) -> Option<i64> {
    let encoded = i64::from_str_radix(hash.trim_start_matches("0x"), 16).ok()?;
    Some(encoded - 1)
}

fn transaction_hash(number: i64) -> String {
    format!("0x{:064x}", number + 10_000)
}
