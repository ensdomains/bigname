#[allow(dead_code)]
mod support;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::{
    VerificationBatch, VerificationLog, VerificationMarker, VerificationProviderKind, WatchFilter,
};
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, ErrorKind as InterpretErrorKind,
    Marker as InterpretMarker, RunMode as InterpretRunMode,
};
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, SourceRole, TimingConfig},
    error::{RunnerError, RunnerResult},
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    live_phase::LivePhase,
    phase::{
        CompletedPhaseFuture, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture,
        PhaseName, PhaseProgress, PhaseSet, RunMode, VerificationLevel,
    },
    project_phase::ProjectPhase,
    runner::PhaseRunner,
    state::PhaseStore,
    verify_phase::{
        VerificationReferenceFuture, VerificationReferenceProvider, VerificationSource, VerifyPhase,
    },
};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

const CHAIN: &str = "ethereum-sepolia";
const REGISTRY: &str = "0x0000000000000000000000000000000000000047";
const RESOLVER: &str = "0x0000000000000000000000000000000000000051";
const SECOND_RESOLVER: &str = "0x0000000000000000000000000000000000000052";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const SENDER: &str = "0x00000000000000000000000000000000000000a2";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
const BLOCK_0: &str = "0x0000000000000000000000000000000000000000000000000000000000000100";
const BLOCK_1: &str = "0x0000000000000000000000000000000000000000000000000000000000000101";
const BLOCK_2: &str = "0x0000000000000000000000000000000000000000000000000000000000000102";
const TX_1: &str = "0x0000000000000000000000000000000000000000000000000000000000000201";
const TX_2: &str = "0x0000000000000000000000000000000000000000000000000000000000000202";

sol! {
    event LabelRegistered(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        address owner,
        uint64 expiry,
        address indexed sender
    );
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event ResolverUpdated(
        uint256 indexed tokenId,
        address indexed resolver,
        address indexed sender
    );
    event TextChanged(
        bytes32 indexed node,
        string indexed indexedKey,
        string key,
        string value
    );
}

#[derive(Clone, Copy)]
enum Producer {
    Registry,
    Root,
}

impl Producer {
    fn family(self) -> &'static str {
        match self {
            Self::Registry => "ens_v2_registry_l1",
            Self::Root => "ens_v2_root_l1",
        }
    }

    fn role(self) -> &'static str {
        match self {
            Self::Registry => "registry",
            Self::Root => "root_registry",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Registry => "registry",
            Self::Root => "root",
        }
    }
}

#[tokio::test]
async fn fresh_rpc_walk_repairs_registry_and_root_discovery_before_compared_verify() -> Result<()> {
    for producer in [Producer::Registry, Producer::Root] {
        run_fresh_case(producer).await?;
    }
    Ok(())
}

async fn run_fresh_case(producer: Producer) -> Result<()> {
    let scratch =
        ScratchDatabase::create(&format!("production_discovery_ingest_{}", producer.label()))
            .await?;
    seed_manifest_configuration(scratch.pool(), producer).await?;
    let fixture = RpcFixture::spawn(producer).await?;
    let references = Arc::new(FixtureReferences::new(fixture.verification_logs()));
    let observations = Arc::new(Observations::default());
    let phases = PhaseSet::new([
        observed(
            Arc::new(IngestPhase::new(scratch.pool().clone())),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
        observed(
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
        observed(
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
        observed(
            Arc::new(VerifyPhase::with_reference_provider(
                scratch.verification_database(2).await?,
                references.clone(),
            )),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
        observed(
            Arc::new(CompleteLivePhase),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        format!("production-discovery-ingest-{}", producer.label()),
        fast_timing(),
    )?;
    let chain = ChainConfig::new(
        CHAIN,
        vec![
            SourceConfig::new_with_role(
                CHAIN,
                "intake",
                "drpc",
                SeedBasis::EthereumHead,
                0,
                SourceRole::Intake,
                fixture.endpoint.clone(),
            )?,
            SourceConfig::new_with_role(
                CHAIN,
                "independent-reference",
                "drpc",
                SeedBasis::EthereumHead,
                0,
                SourceRole::VerificationOnly,
                "http://independent-reference.invalid/",
            )?,
        ],
        true,
    )?;

    runner.run_chain(&chain, CancellationToken::new()).await?;

    assert!(
        observations
            .first_interpret_missing_resolver
            .load(Ordering::SeqCst),
        "the resolver record must be absent before first Interpret"
    );
    assert_eq!(fixture.resolver_range_requests.load(Ordering::SeqCst), 1);
    let raw_record: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs
         WHERE chain_id = $1 AND lower(emitting_address) = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(raw_record, 1, "the same run must fetch the missed RPC log");
    let normalized: (String, String, String) = sqlx::query_as(
        "SELECT event_kind, after_state ->> 'record_key', after_state ->> 'value'
         FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        normalized,
        (
            "RecordChanged".to_owned(),
            "text:url".to_owned(),
            "https://example.test".to_owned(),
        )
    );
    let inventory: Value = sqlx::query_scalar(
        "SELECT entries FROM record_inventory_current
         WHERE entries @> '[{\"record_key\":\"text:url\",\"value\":\"https://example.test\"}]'::jsonb",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        inventory
            .as_array()
            .is_some_and(|entries| entries.iter().any(|entry| {
                entry["record_key"] == "text:url"
                    && entry["value"] == "https://example.test"
                    && entry["status"] == "success"
            }))
    );
    let verified: (String, Option<String>, bool) = sqlx::query_as(
        "SELECT phase_status, verification_level, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        verified,
        ("completed".into(), Some("cross_checked".into()), false)
    );
    assert_eq!(references.calls.load(Ordering::SeqCst), 1);
    let ingest: (bool, i64) = sqlx::query_as(
        "SELECT redo_in_progress, redo_attempt_generation
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(ingest, (false, 2));

    let sequence = observations
        .sequence
        .lock()
        .expect("observation lock")
        .clone();
    assert_subsequence(
        &sequence,
        &[
            (PhaseName::Ingest, "normal"),
            (PhaseName::Interpret, "normal"),
            (PhaseName::Ingest, "redo"),
            (PhaseName::Interpret, "redo"),
            (PhaseName::Project, "normal"),
            (PhaseName::Verify, "normal"),
        ],
    );
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state
         WHERE chain_id = $1 AND redo_in_progress",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(pending, 0);
    assert_replay_stable_across_redo_batches(scratch.pool(), &fixture).await?;

    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_fetched_discovery_beyond_ingest_cursor_installs_and_drains_repair() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_ingest_live_suffix").await?;
    seed_manifest_configuration(scratch.pool(), Producer::Root).await?;
    seed_pre_live_spine(scratch.pool()).await?;
    let fixture = RpcFixture::spawn(Producer::Root).await?;
    let observations = Arc::new(Observations::default());
    let phases = PhaseSet::new([
        observed(
            Arc::new(IngestPhase::new(scratch.pool().clone())),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
        observed(
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
        observed(
            Arc::new(LoopbackPhase::new(PhaseName::Project)),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
        Arc::new(QuickVerifyPhase),
        observed(
            Arc::new(LivePhase::new(scratch.pool().clone())),
            scratch.pool().clone(),
            Arc::clone(&observations),
        ),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-discovery-ingest-live-suffix",
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
            fixture.endpoint.clone(),
        )?],
        true,
    )?;

    runner.run_chain(&chain, CancellationToken::new()).await?;

    assert_eq!(
        fixture.resolver_range_requests.load(Ordering::SeqCst),
        1,
        "discovery from the Live-loaded suffix must trigger one address-aware historical fetch"
    );
    let recovered: (i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM raw_logs
              WHERE chain_id = $1 AND lower(emitting_address) = lower($2)),
             (SELECT count(*) FROM normalized_events
              WHERE chain_id = $1 AND event_kind = 'RecordChanged')",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(recovered, (1, 1));
    let ingest: (bool, i64, Option<i64>) = sqlx::query_as(
        "SELECT state.redo_in_progress, state.redo_attempt_generation,
                cursor.last_processed_block_number
         FROM chain_phase_state state
         JOIN ingest_cursors cursor USING (chain_id)
         WHERE state.chain_id = $1 AND state.phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        ingest,
        (false, 2, Some(0)),
        "the repair must drain without pretending Live advanced the finite cursor"
    );
    assert_subsequence(
        &observations
            .sequence
            .lock()
            .expect("observation lock")
            .clone(),
        &[
            (PhaseName::Live, "normal"),
            (PhaseName::Interpret, "normal"),
            (PhaseName::Ingest, "redo"),
            (PhaseName::Interpret, "redo"),
            (PhaseName::Project, "normal"),
        ],
    );
    let snapshot_before: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT lower(address), lower(topic0), active_from_block_number,
                active_to_block_number
         FROM discovery_watch_admissions WHERE chain_id = $1
         ORDER BY 1, 2, 3, 4",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    assert!(!snapshot_before.is_empty());

    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: Some(InterpretMarker {
                number: 2,
                hash: BLOCK_2.to_owned(),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    let settled: (bool, i64) = sqlx::query_as(
        "SELECT redo_in_progress, redo_attempt_generation
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(settled, (false, 2));
    let snapshot_after: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT lower(address), lower(topic0), active_from_block_number,
                active_to_block_number
         FROM discovery_watch_admissions WHERE chain_id = $1
         ORDER BY 1, 2, 3, 4",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(snapshot_after, snapshot_before);
    assert_eq!(fixture.resolver_range_requests.load(Ordering::SeqCst), 1);

    fixture.server.abort();
    scratch.cleanup().await
}

async fn assert_replay_stable_across_redo_batches(
    pool: &sqlx::PgPool,
    fixture: &RpcFixture,
) -> Result<()> {
    let generation_before: i64 = sqlx::query_scalar(
        "SELECT redo_attempt_generation FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    let fetches_before = fixture.resolver_range_requests.load(Ordering::SeqCst);
    let snapshot_before: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT lower(address), lower(topic0), active_from_block_number,
                active_to_block_number
         FROM discovery_watch_admissions WHERE chain_id = $1
         ORDER BY 1, 2, 3, 4",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;

    let mut transaction = pool.begin().await?;
    for number in 3..=501 {
        let hash = extended_block_hash(number);
        let parent = if number == 3 {
            BLOCK_2.to_owned()
        } else {
            extended_block_hash(number - 1)
        };
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'finalized')",
        )
        .bind(CHAIN)
        .bind(hash)
        .bind(parent)
        .bind(number)
        .execute(&mut *transaction)
        .await?;
    }
    let tip = extended_block_hash(501);
    sqlx::query(
        "UPDATE chain_heads SET latest_block_hash = $2, latest_block_number = 501,
             safe_block_hash = $2, safe_block_number = 501,
             finalized_block_hash = $2, finalized_block_number = 501
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(&tip)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE ingest_cursors SET next_block_number = 502,
             target_block_number = 501, last_processed_block_number = 501,
             last_processed_block_hash = $2 WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(&tip)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET current_block_number = 501,
             current_block_hash = $2, target_block_number = 501,
             target_block_hash = $2, live_handoff_block_number = 501,
             live_handoff_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .bind(&tip)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    let one_batch = InterpretEngine::new(pool.clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: 0,
            to_block: 250,
            resume_current: None,
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(one_batch.complete);

    let interrupted = InterpretEngine::new(pool.clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(!interrupted.complete);
    assert_eq!(interrupted.current.number, 499);
    let resumed = InterpretEngine::new(pool.clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(interrupted.current),
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(resumed.complete);

    let ingest_after: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    assert_eq!(ingest_after, (generation_before, false));
    assert_eq!(
        fixture.resolver_range_requests.load(Ordering::SeqCst),
        fetches_before,
        "replaying identical discovery must not invoke historical intake"
    );
    let snapshot_after: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT lower(address), lower(topic0), active_from_block_number,
                active_to_block_number
         FROM discovery_watch_admissions WHERE chain_id = $1
         ORDER BY 1, 2, 3, 4",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;
    assert_eq!(snapshot_after, snapshot_before);
    Ok(())
}

#[tokio::test]
async fn settled_admission_is_idempotent_across_empty_restart_completion() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_ingest_restart").await?;
    seed_manifest_configuration(scratch.pool(), Producer::Root).await?;
    seed_completed_discovery_state(scratch.pool()).await?;
    let engine = InterpretEngine::new(scratch.pool().clone());
    let request = || InterpretRequest {
        chain_id: CHAIN.to_owned(),
        from_block: 0,
        to_block: 2,
        resume_current: Some(InterpretMarker {
            number: 2,
            hash: BLOCK_2.to_owned(),
        }),
        mode: InterpretRunMode::Normal,
    };
    engine.run_batch(request()).await?;
    let before: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::raw_sql(
        "CREATE FUNCTION reject_unchanged_admission_rewrite() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'unchanged snapshot was rewritten'; END $$;
         CREATE TRIGGER reject_unchanged_admission_rewrite
         BEFORE INSERT OR DELETE ON discovery_watch_admissions
         FOR EACH ROW EXECUTE FUNCTION reject_unchanged_admission_rewrite();",
    )
    .execute(scratch.pool())
    .await?;
    engine.run_batch(request()).await?;
    let after: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        after, before,
        "identical restart finalization must be a no-op"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn changed_authority_or_lineage_conservatively_readmits_the_union() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_ingest_snapshot_scope").await?;
    seed_manifest_configuration(scratch.pool(), Producer::Root).await?;
    seed_completed_discovery_state(scratch.pool()).await?;
    let engine = InterpretEngine::new(scratch.pool().clone());
    let request = || InterpretRequest {
        chain_id: CHAIN.to_owned(),
        from_block: 0,
        to_block: 2,
        resume_current: Some(InterpretMarker {
            number: 2,
            hash: BLOCK_2.to_owned(),
        }),
        mode: InterpretRunMode::Normal,
    };
    engine.run_batch(request()).await?;
    let first: (i64, String, i64) = sqlx::query_as(
        "SELECT state.redo_attempt_generation,
                admission.manifest_authority_fingerprint,
                admission.lineage_orphaning_epoch
         FROM chain_phase_state state
         JOIN discovery_watch_admissions admission USING (chain_id)
         WHERE state.chain_id = $1 AND state.phase_name = 'ingest'
         LIMIT 1",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await
    .context("missing first authority-scoped admission")?;

    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload, '{manifest_version}', '2'::jsonb, false
         )
         WHERE chain_id = $1 AND source_family = 'ens_v2_resolver_l1'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    engine.run_batch(request()).await?;
    let authority_changed: (i64, String, i64) = sqlx::query_as(
        "SELECT state.redo_attempt_generation,
                admission.manifest_authority_fingerprint,
                admission.lineage_orphaning_epoch
         FROM chain_phase_state state
         JOIN discovery_watch_admissions admission USING (chain_id)
         WHERE state.chain_id = $1 AND state.phase_name = 'ingest'
         LIMIT 1",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await
    .context("missing authority-rotated admission")?;
    assert_eq!(authority_changed.0, first.0 + 1);
    assert_ne!(authority_changed.1, first.1);
    assert_eq!(authority_changed.2, first.2);

    sqlx::query(
        "UPDATE chain_heads
         SET lineage_orphaning_epoch = lineage_orphaning_epoch + 1
         WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    engine.run_batch(request()).await?;
    let lineage_changed: (i64, String, i64) = sqlx::query_as(
        "SELECT state.redo_attempt_generation,
                admission.manifest_authority_fingerprint,
                admission.lineage_orphaning_epoch
         FROM chain_phase_state state
         JOIN discovery_watch_admissions admission USING (chain_id)
         WHERE state.chain_id = $1 AND state.phase_name = 'ingest'
         LIMIT 1",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await
    .context("missing lineage-rotated admission")?;
    assert_eq!(lineage_changed.0, authority_changed.0 + 1);
    assert_eq!(lineage_changed.1, authority_changed.1);
    assert_eq!(lineage_changed.2, authority_changed.2 + 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn genuinely_new_same_range_tuple_advances_generation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_ingest_same_range_delta").await?;
    seed_manifest_configuration(scratch.pool(), Producer::Root).await?;
    seed_completed_discovery_state(scratch.pool()).await?;
    let engine = InterpretEngine::new(scratch.pool().clone());
    let request = || InterpretRequest {
        chain_id: CHAIN.to_owned(),
        from_block: 0,
        to_block: 2,
        resume_current: Some(InterpretMarker {
            number: 2,
            hash: BLOCK_2.to_owned(),
        }),
        mode: InterpretRunMode::Normal,
    };
    engine.run_batch(request()).await?;
    let before: i64 = sqlx::query_scalar(
        "SELECT redo_attempt_generation FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    add_discovered_resolver(scratch.pool(), SECOND_RESOLVER, 1).await?;
    engine.run_batch(request()).await?;
    let after: (i64, bool, i64, i64) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress,
                redo_from_block_number, redo_to_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, (before + 1, true, 1, 2));
    scratch.cleanup().await
}

#[tokio::test]
async fn cross_family_all_emitter_coverage_keeps_discovery_repair_idle() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_ingest_all_emitter").await?;
    seed_manifest_configuration(scratch.pool(), Producer::Root).await?;
    insert_manifest(
        scratch.pool(),
        "ens_v1_resolver_l1",
        "tests/all-emitter-resolver.toml",
        json!({
            "manifest_version": 1,
            "namespace": "ens",
            "source_family": "ens_v1_resolver_l1",
            "chain": CHAIN,
            "deployment_epoch": "fixture",
            "rollout_status": "active",
            "normalizer_version": NORMALIZER,
            "capability_flags": {},
            "roots": [],
            "contracts": [],
            "discovery_rules": [],
            "abi": {"events": [{
                "name": "TextChanged",
                "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            }], "calls": []}
        }),
    )
    .await?;
    seed_completed_discovery_state(scratch.pool()).await?;

    let before: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: Some(InterpretMarker {
                number: 2,
                hash: BLOCK_2.to_owned(),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    let after: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, before);
    let (edges, admissions): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM discovery_edges WHERE chain_id = $1),
                (SELECT count(*) FROM discovery_watch_admissions WHERE chain_id = $1)",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edges, 1, "the resolver discovery edge still materializes");
    assert_eq!(
        admissions, 1,
        "the complete admission snapshot retains tuples already covered from all emitters"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn transient_required_ingest_stamp_failure_is_retryable_and_atomic() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_ingest_transient_stamp").await?;
    seed_manifest_configuration(scratch.pool(), Producer::Root).await?;
    seed_completed_discovery_state(scratch.pool()).await?;
    sqlx::raw_sql(
        "CREATE FUNCTION reject_discovery_ingest_stamp_once() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN
             RAISE EXCEPTION 'fixture transient stamp failure' USING ERRCODE = '40001';
         END $$;
         CREATE TRIGGER reject_discovery_ingest_stamp_once
         BEFORE UPDATE ON chain_phase_state
         FOR EACH ROW
         WHEN (NEW.phase_name = 'ingest' AND NEW.redo_in_progress)
         EXECUTE FUNCTION reject_discovery_ingest_stamp_once();",
    )
    .execute(scratch.pool())
    .await?;

    let error = InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: Some(InterpretMarker {
                number: 2,
                hash: BLOCK_2.to_owned(),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("the injected stamp failure must abort Interpret finalization");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(
        error
            .to_string()
            .contains("fixture transient stamp failure")
    );
    let ingest: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(ingest, (0, false));
    let snapshots: i64 =
        sqlx::query_scalar("SELECT count(*) FROM discovery_watch_admissions WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(snapshots, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn admission_snapshot_and_required_ingest_stamp_roll_back_together() -> Result<()> {
    let scratch = ScratchDatabase::create("production_discovery_ingest_atomic_rollback").await?;
    seed_manifest_configuration(scratch.pool(), Producer::Root).await?;
    seed_completed_discovery_state(scratch.pool()).await?;
    sqlx::raw_sql(
        "CREATE FUNCTION reject_discovery_admission() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture snapshot failure'; END $$;
         CREATE TRIGGER reject_discovery_admission
         BEFORE INSERT ON discovery_watch_admissions
         FOR EACH ROW EXECUTE FUNCTION reject_discovery_admission();",
    )
    .execute(scratch.pool())
    .await?;

    let error = InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: Some(InterpretMarker {
                number: 2,
                hash: BLOCK_2.to_owned(),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("snapshot write failure must abort Interpret finalization");
    assert!(error.to_string().contains("fixture snapshot failure"));
    let ingest: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(ingest, (0, false));
    let snapshots: i64 =
        sqlx::query_scalar("SELECT count(*) FROM discovery_watch_admissions WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(snapshots, 0);
    scratch.cleanup().await
}

#[derive(Default)]
struct Observations {
    sequence: Mutex<Vec<(PhaseName, &'static str)>>,
    first_interpret_checked: AtomicBool,
    first_interpret_missing_resolver: AtomicBool,
}

struct ObservedPhase {
    inner: Arc<dyn Phase>,
    pool: sqlx::PgPool,
    observations: Arc<Observations>,
}

fn observed(
    inner: Arc<dyn Phase>,
    pool: sqlx::PgPool,
    observations: Arc<Observations>,
) -> Arc<dyn Phase> {
    Arc::new(ObservedPhase {
        inner,
        pool,
        observations,
    })
}

impl Phase for ObservedPhase {
    fn name(&self) -> PhaseName {
        self.inner.name()
    }

    fn preflight(&self, chain: &str, sources: &[SourceConfig], mode: &RunMode) -> RunnerResult<()> {
        self.inner.preflight(chain, sources, mode)
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.observations
                .sequence
                .lock()
                .expect("observation lock")
                .push((context.phase, context.mode.as_str()));
            if context.phase == PhaseName::Interpret
                && matches!(context.mode, RunMode::Normal)
                && !self
                    .observations
                    .first_interpret_checked
                    .swap(true, Ordering::SeqCst)
            {
                let present: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM raw_logs
                     WHERE chain_id = $1 AND lower(emitting_address) = lower($2))",
                )
                .bind(&context.chain_id)
                .bind(RESOLVER)
                .fetch_one(&self.pool)
                .await
                .map_err(|error| {
                    RunnerError::data_integrity(format!(
                        "failed to observe first Interpret raw corpus: {error}"
                    ))
                })?;
                self.observations
                    .first_interpret_missing_resolver
                    .store(!present, Ordering::SeqCst);
            }
            self.inner.run_batch(context).await
        })
    }

    fn revalidates_completed(&self, chain: &str, sources: &[SourceConfig]) -> RunnerResult<bool> {
        self.inner.revalidates_completed(chain, sources)
    }

    fn revalidate_completed(&self, context: PhaseContext) -> CompletedPhaseFuture<'_> {
        self.inner.revalidate_completed(context)
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

fn assert_subsequence(
    actual: &[(PhaseName, &'static str)],
    expected: &[(PhaseName, &'static str)],
) {
    let mut cursor = 0;
    for item in actual {
        if expected.get(cursor) == Some(item) {
            cursor += 1;
        }
    }
    assert_eq!(
        cursor,
        expected.len(),
        "missing phase order {expected:?} in {actual:?}"
    );
}

struct FixtureReferences {
    logs: Vec<VerificationLog>,
    calls: AtomicUsize,
}

impl FixtureReferences {
    fn new(logs: Vec<VerificationLog>) -> Self {
        Self {
            logs,
            calls: AtomicUsize::new(0),
        }
    }
}

impl VerificationReferenceProvider for FixtureReferences {
    fn preflight(&self, source: &VerificationSource) -> RunnerResult<()> {
        if source.provider_kind() == VerificationProviderKind::IndependentRpc
            && source.verification_level() == VerificationLevel::CrossChecked
        {
            Ok(())
        } else {
            Err(RunnerError::data_integrity(
                "fixture requires Compared verification",
            ))
        }
    }

    fn fetch<'a>(
        &'a self,
        _source: &'a VerificationSource,
        filter: WatchFilter,
        from: i64,
        to: i64,
    ) -> VerificationReferenceFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let logs = self
            .logs
            .iter()
            .filter(|log| {
                (from..=to).contains(&log.block_number)
                    && filter.includes(&log.address, &log.topics[0], log.block_number)
            })
            .cloned()
            .collect();
        Box::pin(async move {
            Ok(VerificationBatch {
                end: VerificationMarker {
                    number: to,
                    hash: block_hash(to).to_owned(),
                },
                logs,
                rpc_request_count: 1,
            })
        })
    }
}

struct RpcFixture {
    endpoint: String,
    logs: Vec<Value>,
    resolver_range_requests: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct RpcState {
    logs: Vec<Value>,
    resolver_range_requests: Arc<AtomicUsize>,
}

impl RpcFixture {
    async fn spawn(producer: Producer) -> Result<Self> {
        let logs = fixture_logs(producer)?;
        let resolver_range_requests = Arc::new(AtomicUsize::new(0));
        let state = RpcState {
            logs: logs.clone(),
            resolver_range_requests: Arc::clone(&resolver_range_requests),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/", post(rpc)).with_state(state),
            )
            .await
            .expect("discovery RPC server");
        });
        Ok(Self {
            endpoint: format!("http://{address}/"),
            logs,
            resolver_range_requests,
            server,
        })
    }

    fn verification_logs(&self) -> Vec<VerificationLog> {
        self.logs
            .iter()
            .map(verification_log)
            .collect::<Result<Vec<_>>>()
            .expect("valid logs")
    }
}

async fn rpc(State(state): State<RpcState>, Json(request): Json<Value>) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| rpc_response(request, &state))
                .collect(),
        ));
    }
    Json(rpc_response(&request, &state))
}

fn rpc_response(request: &Value, state: &RpcState) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match request["method"].as_str().unwrap_or_default() {
        "eth_getBlockByNumber" => {
            params
                .first()
                .and_then(Value::as_str)
                .and_then(|selector| match selector {
                    "latest" | "safe" | "finalized" | "0x2" => {
                        Some(block(2, params.get(1) == Some(&Value::Bool(true))))
                    }
                    "0x1" => Some(block(1, params.get(1) == Some(&Value::Bool(true)))),
                    "0x0" => Some(block(0, params.get(1) == Some(&Value::Bool(true)))),
                    _ => None,
                })
        }
        "eth_getBlockByHash" => params.first().and_then(Value::as_str).and_then(|hash| {
            [0, 1, 2]
                .into_iter()
                .find(|number| block_hash(*number) == hash)
                .map(|number| block(number, params.get(1) == Some(&Value::Bool(true))))
        }),
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            let selects_resolver = filter_values(filter.get("address"))
                .iter()
                .any(|address| address.eq_ignore_ascii_case(RESOLVER));
            let text_topic = format!("{:#x}", TextChanged::SIGNATURE_HASH);
            let selects_text = filter_values(filter.pointer("/topics/0"))
                .iter()
                .any(|topic| topic.eq_ignore_ascii_case(&text_topic));
            if filter.get("fromBlock").is_some() && selects_resolver && selects_text {
                state.resolver_range_requests.fetch_add(1, Ordering::SeqCst);
            }
            Some(Value::Array(filter_logs(&state.logs, &filter)))
        }
        "eth_getBlockReceipts" => params
            .first()
            .and_then(Value::as_str)
            .map(|hash| match hash {
                BLOCK_0 => json!([]),
                BLOCK_1 => json!([receipt(1, TX_1)]),
                BLOCK_2 => json!([receipt(2, TX_2)]),
                _ => Value::Null,
            }),
        _ => None,
    };
    json!({"jsonrpc":"2.0","id":id,"result":result})
}

fn block(number: i64, full: bool) -> Value {
    let (hash, parent, transaction, to) = match number {
        0 => (BLOCK_0, format!("0x{}", "00".repeat(32)), None, REGISTRY),
        1 => (BLOCK_1, BLOCK_0.to_owned(), Some(TX_1), REGISTRY),
        _ => (BLOCK_2, BLOCK_1.to_owned(), Some(TX_2), RESOLVER),
    };
    let transactions = transaction.map_or_else(|| json!([]), |transaction| if full { json!([{"hash":transaction,"blockHash":hash,"blockNumber":format!("0x{number:x}"),"transactionIndex":"0x0","from":SENDER,"to":to,"input":"0x","value":"0x0"}]) } else { json!([transaction]) });
    json!({"hash":hash,"parentHash":parent,"number":format!("0x{number:x}"),"timestamp":format!("0x{:x}", number + 100),"logsBloom":"0x","transactions":transactions})
}

fn receipt(number: i64, transaction: &str) -> Value {
    json!({"transactionHash":transaction,"blockHash":block_hash(number),"blockNumber":format!("0x{number:x}"),"transactionIndex":"0x0","status":"0x1","cumulativeGasUsed":"0x5208","gasUsed":"0x5208","logsBloom":"0x"})
}

fn fixture_logs(producer: Producer) -> Result<Vec<Value>> {
    let token = versioned_token("box", 1);
    let registry = [
        LabelRegistered {
            tokenId: token,
            labelHash: keccak256(b"box"),
            label: "box".into(),
            owner: OWNER.parse()?,
            expiry: 10_000,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
        TokenResource {
            tokenId: token,
            resource: U256::from(374),
        }
        .encode_log_data(),
        ResolverUpdated {
            tokenId: token,
            resolver: RESOLVER.parse()?,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
    ];
    let mut logs = registry
        .into_iter()
        .enumerate()
        .map(|(index, log)| encoded_log(REGISTRY, 1, TX_1, index as i64, log))
        .collect::<Vec<_>>();
    logs.push(encoded_log(
        RESOLVER,
        2,
        TX_2,
        0,
        TextChanged {
            node: match producer {
                Producer::Registry => raw_namehash(&[b"box", b"eth"]),
                Producer::Root => raw_namehash(&[b"box"]),
            },
            indexedKey: keccak256(b"url"),
            key: "url".into(),
            value: "https://example.test".into(),
        }
        .encode_log_data(),
    ));
    Ok(logs)
}

fn encoded_log(
    address: &str,
    number: i64,
    transaction: &str,
    index: i64,
    log: alloy_primitives::LogData,
) -> Value {
    json!({"blockHash":block_hash(number),"blockNumber":format!("0x{number:x}"),"transactionHash":transaction,"transactionIndex":"0x0","logIndex":format!("0x{index:x}"),"address":address,"topics":log.topics().iter().map(|topic| format!("{topic:#x}")).collect::<Vec<_>>(),"data":format!("0x{}", alloy_primitives::hex::encode(log.data))})
}

fn filter_logs(logs: &[Value], filter: &Value) -> Vec<Value> {
    let block_hash_filter = filter.get("blockHash").and_then(Value::as_str);
    let from = quantity(filter.get("fromBlock")).unwrap_or(0);
    let to = quantity(filter.get("toBlock")).unwrap_or(i64::MAX);
    let addresses = filter_values(filter.get("address"));
    let topics = filter_values(filter.pointer("/topics/0"));
    logs.iter()
        .filter(|log| {
            let number = quantity(log.get("blockNumber")).unwrap_or_default();
            block_hash_filter.is_none_or(|hash| log["blockHash"] == hash)
                && (from..=to).contains(&number)
                && (addresses.is_empty()
                    || addresses.iter().any(|address| {
                        address.eq_ignore_ascii_case(log["address"].as_str().unwrap_or_default())
                    }))
                && (topics.is_empty()
                    || topics.iter().any(|topic| {
                        topic.eq_ignore_ascii_case(
                            log.pointer("/topics/0")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                        )
                    }))
        })
        .cloned()
        .collect()
}

fn filter_values(value: Option<&Value>) -> Vec<String> {
    value.map_or_else(Vec::new, |value| {
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
    })
}

fn quantity(value: Option<&Value>) -> Option<i64> {
    i64::from_str_radix(value?.as_str()?.trim_start_matches("0x"), 16).ok()
}
fn block_hash(number: i64) -> &'static str {
    match number {
        0 => BLOCK_0,
        1 => BLOCK_1,
        _ => BLOCK_2,
    }
}

fn extended_block_hash(number: i64) -> String {
    format!("0x{:064x}", 0x1_0000_i64 + number)
}

fn verification_log(log: &Value) -> Result<VerificationLog> {
    Ok(VerificationLog {
        block_hash: log["blockHash"].as_str().context("block hash")?.to_owned(),
        block_number: quantity(log.get("blockNumber")).context("block number")?,
        transaction_hash: log["transactionHash"]
            .as_str()
            .context("transaction hash")?
            .to_owned(),
        transaction_index: quantity(log.get("transactionIndex")).context("transaction index")?,
        log_index: quantity(log.get("logIndex")).context("log index")?,
        address: log["address"].as_str().context("address")?.to_owned(),
        topics: log["topics"]
            .as_array()
            .context("topics")?
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        data: alloy_primitives::hex::decode(
            log["data"]
                .as_str()
                .context("data")?
                .trim_start_matches("0x"),
        )?,
    })
}

async fn seed_manifest_configuration(pool: &sqlx::PgPool, producer: Producer) -> Result<()> {
    let instance = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1, $2, 'contract')").bind(instance).bind(CHAIN).execute(pool).await?;
    let source_payload = json!({"manifest_version":1,"namespace":"ens","source_family":producer.family(),"chain":CHAIN,"deployment_epoch":"fixture","rollout_status":"active","normalizer_version":NORMALIZER,"capability_flags":{},"roots":[],"contracts":[{"role":producer.role(),"address":REGISTRY,"proxy_kind":"none","start_block":0}],"discovery_rules":[{"edge_kind":"resolver","from_role":producer.role(),"admission":"reachable_from_root"}],"abi":{"events":[{"name":"LabelRegistered","fragment":"event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)","emitter_roles":[producer.role()],"normalized_events":["RegistrationGranted","PreimageObserved"]},{"name":"TokenResource","fragment":"event TokenResource(uint256 indexed tokenId, uint256 indexed resource)","emitter_roles":[producer.role()],"normalized_events":["TokenResourceLinked"]},{"name":"ResolverUpdated","fragment":"event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)","emitter_roles":[producer.role()],"normalized_events":["ResolverChanged"]}],"calls":[]}});
    let resolver_payload = json!({"manifest_version":1,"namespace":"ens","source_family":"ens_v2_resolver_l1","chain":CHAIN,"deployment_epoch":"fixture","rollout_status":"active","normalizer_version":NORMALIZER,"capability_flags":{},"roots":[],"contracts":[],"discovery_rules":[],"abi":{"events":[{"name":"TextChanged","fragment":"event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)","emitter_roles":[],"normalized_events":["RecordChanged"]}],"calls":[]}});
    let source_id = insert_manifest(
        pool,
        producer.family(),
        &format!("tests/{}-source.toml", producer.label()),
        source_payload,
    )
    .await?;
    insert_manifest(
        pool,
        "ens_v2_resolver_l1",
        &format!("tests/{}-resolver.toml", producer.label()),
        resolver_payload,
    )
    .await?;
    sqlx::query("INSERT INTO manifest_contract_instances (manifest_id, chain_id, declaration_kind, declaration_name, contract_instance_id, declared_address, role, proxy_kind, start_block_number) VALUES ($1,$2,'contract',$3,$4,$5,$3,'none',0)").bind(source_id).bind(CHAIN).bind(producer.role()).bind(instance).bind(REGISTRY).execute(pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number, source_manifest_id, provenance) VALUES ($1,$2,$3,0,$4,'{}')").bind(instance).bind(CHAIN).bind(REGISTRY).bind(source_id).execute(pool).await?;
    sqlx::query("INSERT INTO manifest_discovery_rules (manifest_id, edge_kind, from_role, admission, rule_payload) VALUES ($1,'resolver',$2,'reachable_from_root',$3)").bind(source_id).bind(producer.role()).bind(json!({"edge_kind":"resolver","from_role":producer.role(),"admission":"reachable_from_root"})).execute(pool).await?;
    Ok(())
}

async fn insert_manifest(
    pool: &sqlx::PgPool,
    family: &str,
    path: &str,
    payload: Value,
) -> Result<i64> {
    Ok(sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id, deployment_label, rollout_status, normalizer_version, file_path, manifest_payload) VALUES (1,'ens',$1,$2,'fixture','active',$3,$4,$5) RETURNING manifest_id").bind(family).bind(CHAIN).bind(NORMALIZER).bind(path).bind(payload).fetch_one(pool).await?)
}

async fn seed_completed_discovery_state(pool: &sqlx::PgPool) -> Result<()> {
    for number in 0..=2 {
        sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state) VALUES ($1,$2,$3,$4,to_timestamp($4),'finalized')").bind(CHAIN).bind(block_hash(number)).bind((number>0).then(|| block_hash(number-1))).bind(number).execute(pool).await?;
    }
    sqlx::query("INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number, safe_block_hash, safe_block_number, finalized_block_hash, finalized_block_number) VALUES ($1,$2,2,$2,2,$2,2)").bind(CHAIN).bind(BLOCK_2).execute(pool).await?;
    for phase in PhaseName::ALL {
        sqlx::query("INSERT INTO chain_phase_state (chain_id, phase_name, phase_status, current_block_number, current_block_hash, target_block_number, target_block_hash, input_content_hash, started_at, finished_at) VALUES ($1,$2,'completed',2,$3,2,$3,CASE WHEN $2 IN ('interpret','project') THEN $4 END,now(),now())").bind(CHAIN).bind(phase.as_str()).bind(BLOCK_2).bind(phase_runner::INTERPRETER_CONTENT_HASH).execute(pool).await?;
    }
    sqlx::query("INSERT INTO ingest_cursors (chain_id, source_key, source_kind, seed_basis, start_block_number, next_block_number, target_block_number, last_processed_block_number, last_processed_block_hash) VALUES ($1,'intake','drpc','ethereum_head',0,3,2,2,$2)").bind(CHAIN).bind(BLOCK_2).execute(pool).await?;
    add_discovered_resolver(pool, RESOLVER, 1).await
}

async fn seed_pre_live_spine(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, NULL, 0, to_timestamp(0), 'finalized')",
    )
    .bind(CHAIN)
    .bind(BLOCK_0)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (
             chain_id, latest_block_hash, latest_block_number,
             safe_block_hash, safe_block_number,
             finalized_block_hash, finalized_block_number
         ) VALUES ($1, $2, 0, $2, 0, $2, 0)",
    )
    .bind(CHAIN)
    .bind(BLOCK_0)
    .execute(pool)
    .await?;
    PhaseStore::new(pool.clone())
        .initialize_chain(CHAIN)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = 0,
             current_block_hash = $2, target_block_number = 0,
             target_block_hash = $2,
             live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN 0 END,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $2 END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $3 END,
             verification_level = CASE WHEN phase_name = 'verify' THEN 'quick_synced' END,
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project', 'verify')",
    )
    .bind(CHAIN)
    .bind(BLOCK_0)
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'intake', 'drpc', 'ethereum_head', 0, 1, 0, 0, $2)",
    )
    .bind(CHAIN)
    .bind(BLOCK_0)
    .execute(pool)
    .await?;
    Ok(())
}

async fn add_discovered_resolver(pool: &sqlx::PgPool, address: &str, from: i64) -> Result<()> {
    let source: (i64, Uuid) = sqlx::query_as("SELECT manifest.manifest_id, declaration.contract_instance_id FROM manifest_versions manifest JOIN manifest_contract_instances declaration ON declaration.manifest_id=manifest.manifest_id WHERE manifest.chain_id=$1 AND manifest.source_family='ens_v2_root_l1'").bind(CHAIN).fetch_one(pool).await?;
    let target = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1,$2,'contract')").bind(target).bind(CHAIN).execute(pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number, active_from_block_hash, source_manifest_id, provenance) VALUES ($1,$2,$3,$4,$5,$6,'{}')").bind(target).bind(CHAIN).bind(address).bind(from).bind(block_hash(from)).bind(source.0).execute(pool).await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source, admission_basis, source_manifest_id, active_from_block_number, active_from_block_hash, canonicality_state) VALUES ($1,'resolver',$2,$3,'event','reachable_from_root',$4,$5,$6,'finalized')").bind(CHAIN).bind(source.1).bind(target).bind(source.0).bind(from).bind(block_hash(from)).execute(pool).await?;
    Ok(())
}

fn fast_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: std::time::Duration::from_millis(1),
        maximum_backoff: std::time::Duration::from_millis(4),
        live_poll_interval: std::time::Duration::from_millis(1),
    }
}
fn versioned_token(label: &str, version: u32) -> U256 {
    let mut bytes = *keccak256(label.as_bytes());
    bytes[28..].copy_from_slice(&version.to_be_bytes());
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
