#[allow(dead_code)]
mod support;

use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::{Address, U256, hex, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::{
    BASE_COINBASE_SEAM_BLOCK, BatchRequest, Engine, ErrorKind as IngestErrorKind, SourceDescriptor,
    VerificationBatch, VerificationLog, VerificationMarker, VerificationProviderKind, WatchFilter,
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use phase_runner::{
    capacity::CapacityGuard,
    config::{
        CapacityConfig, ChainConfig, RuntimeConfig, SeedBasis, SourceConfig, SourceRole,
        TimingConfig,
    },
    error::{ErrorKind, RunnerError, RunnerResult},
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName,
        PhaseProgress, PhaseSet, SourceProgress, VerificationLevel,
    },
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
    verify_phase::{
        VerificationReferenceFuture, VerificationReferenceProvider, VerificationSource, VerifyPhase,
    },
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use support::ScratchDatabase;

const BLOCK_0: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const BLOCK_1: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
const BLOCK_1_REORG: &str = "0x0000000000000000000000000000000000000000000000000000000000000012";
const BLOCK_1_SECOND_REORG: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000022";
const FORK_BLOCK_0: &str = "0x0000000000000000000000000000000000000000000000000000000000000032";
const BLOCK_2: &str = "0x0000000000000000000000000000000000000000000000000000000000000007";
const TRANSACTION: &str = "0x0000000000000000000000000000000000000000000000000000000000000003";
const REORG_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000013";
const SECOND_REORG_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000023";
const WIDENED_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000033";
const WIDENED_REORG_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000043";
const WIDENED_SECOND_REORG_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000053";
const ANNOUNCEMENT_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000008";
const REGISTRATION_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000009";
const DECLARED_APPROVAL_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000061";
const FOREIGN_APPROVAL_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000062";
const CONTEXT_APPROVAL_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000063";
const CONTRACT: &str = "0x0000000000000000000000000000000000000004";
const SENDER: &str = "0x0000000000000000000000000000000000000005";
const SIBLING_CONTRACT: &str = "0x0000000000000000000000000000000000000006";
const ANNOUNCED_REGISTRY: &str = "0x0000000000000000000000000000000000000045";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const SIBLING_TOPIC: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
const BASE: &str = "base-mainnet";
const SEPOLIA: &str = "ethereum-sepolia";

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
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
}

#[tokio::test]
async fn verification_only_source_never_reaches_finite_ingest_or_cursors() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_production_ingest").await?;
    let chain_id = "rpc-ingest-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(true, false).await?;
    let (verify_endpoint, verify_server, verify_requests) = spawn_crash_window_rpc(false).await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![
            SourceConfig::new_with_role(
                chain_id,
                "rpc",
                "rpc",
                SeedBasis::NewSignatureRange,
                0,
                SourceRole::Intake,
                endpoint,
            )?,
            SourceConfig::new_with_role(
                chain_id,
                "verify",
                "rpc",
                SeedBasis::NewSignatureRange,
                0,
                SourceRole::VerificationOnly,
                verify_endpoint,
            )?,
        ],
        false,
    )?;
    let database = scratch.runner();
    let phases = PhaseSet::with_ingest(Arc::new(IngestPhase::new(database.pool().clone())))?;
    let runner = PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-ingest-test",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, task_cancellation).await });

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let phase_state: Option<(String, Option<String>)> = sqlx::query_as(
                "
                SELECT phase_status, last_error
                FROM chain_phase_state
                WHERE chain_id = $1
                  AND phase_name = 'ingest'
                ",
            )
            .bind(chain_id)
            .fetch_optional(scratch.pool())
            .await?;
            match phase_state {
                Some((status, _)) if status == "completed" => {
                    return Ok::<_, anyhow::Error>(());
                }
                Some((status, reason)) if status == "failed" => {
                    anyhow::bail!(
                        "production ingest failed: {}",
                        reason.unwrap_or_else(|| "no failure reason".to_owned())
                    );
                }
                _ => {}
            }
            if task.is_finished() {
                let result = (&mut task)
                    .await
                    .context("production ingest task panicked")?;
                result.context("production ingest task exited before completion")?;
                anyhow::bail!("production ingest task exited before ingest completed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("production ingest did not complete")??;
    cancellation.cancel();
    task.await??;
    server.abort();
    verify_server.abort();

    let lineage_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM chain_lineage WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    let raw_log_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    let raw_logs: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "
        SELECT emitting_address, data
        FROM raw_logs
        WHERE chain_id = $1
        ORDER BY log_index
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    let transaction: (Vec<u8>, String) = sqlx::query_as(
        "
        SELECT input, value::text
        FROM raw_transactions
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let cursor: (i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT next_block_number, target_block_number, last_processed_block_number
        FROM ingest_cursors
        WHERE chain_id = $1
          AND source_key = 'rpc'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let cursor_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_cursors WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    let state: (Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT current_block_number, live_handoff_block_number
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'ingest'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let head: (
        i64,
        String,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "
            SELECT latest_block_number,
                   latest_block_hash,
                   safe_block_number,
                   safe_block_hash,
                   finalized_block_number,
                   finalized_block_hash
            FROM chain_heads
            WHERE chain_id = $1
            ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let finalized_lineage_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM chain_lineage
        WHERE chain_id = $1
          AND canonicality_state = 'finalized'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(lineage_count, 2);
    assert_eq!(raw_log_count, 2);
    assert_eq!(
        raw_logs,
        vec![
            (CONTRACT.to_owned(), Vec::new()),
            (SIBLING_CONTRACT.to_owned(), vec![0x12, 0x34]),
        ]
    );
    assert_eq!(transaction, (vec![0xde, 0xad], "7".to_owned()));
    assert_eq!(cursor, (2, Some(1), Some(1)));
    assert_eq!(cursor_count, 1);
    assert_eq!(state, (Some(1), Some(1)));
    assert_eq!(
        head,
        (
            1,
            BLOCK_1.to_owned(),
            Some(1),
            Some(BLOCK_1.to_owned()),
            Some(1),
            Some(BLOCK_1.to_owned()),
        )
    );
    assert_eq!(finalized_lineage_count, 2);
    assert_eq!(verify_requests.load(Ordering::SeqCst), 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn address_scoped_approvals_follow_raw_intake_and_transaction_context_boundaries()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_address_scoped_approval").await?;
    let chain_id = "rpc-address-scoped-approval";
    let manifests = ApprovalManifestFixture::new(chain_id)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let (endpoint, server) = spawn_approval_rpc().await?;
    let source = SourceDescriptor {
        key: "rpc".to_owned(),
        kind: "rpc".to_owned(),
        start_block: 0,
        endpoint: endpoint.clone(),
    };
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let database = scratch.runner();
    let phases = PhaseSet::new([
        Arc::new(IngestPhase::new(database.pool().clone())) as Arc<dyn Phase>,
        Arc::new(InterpretPhase::new(database.pool().clone())),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    ])?;
    let runner = PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "address-scoped-approval-intake",
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, task_cancellation).await });
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status: Option<String> = sqlx::query_scalar(
                "SELECT phase_status FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name = 'interpret'",
            )
            .bind(chain_id)
            .fetch_optional(scratch.pool())
            .await?;
            if status.as_deref() == Some("completed") {
                return Ok::<_, anyhow::Error>(());
            }
            if task.is_finished() {
                anyhow::bail!("phase runner exited before approval interpretation completed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("approval raw-intake fixture did not complete")??;
    cancellation.cancel();
    task.await??;

    type RawLogRow = (i64, String, String, Vec<String>, Vec<u8>);
    let logs: Vec<RawLogRow> = sqlx::query_as(
        "SELECT log_index, emitting_address, transaction_hash, topics, data
         FROM raw_logs WHERE chain_id = $1 ORDER BY log_index",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        logs.iter().map(|row| row.0).collect::<Vec<_>>(),
        [0, 2, 3],
        "the unrelated foreign transaction must be absent while same-transaction context remains"
    );
    assert_eq!(logs[0].1, CONTRACT);
    assert_eq!(logs[0].2, DECLARED_APPROVAL_TRANSACTION);
    assert_eq!(logs[1].1, CONTRACT);
    assert_eq!(logs[1].2, CONTEXT_APPROVAL_TRANSACTION);
    assert_eq!(logs[2].1, SIBLING_CONTRACT);
    assert_eq!(logs[2].2, CONTEXT_APPROVAL_TRANSACTION);
    assert_eq!(logs[0].3, approval_topics());
    assert_eq!(logs[1].3, approval_topics());
    assert_eq!(logs[2].3, approval_topics());
    assert_eq!(logs[0].4, approval_data(true));
    assert_eq!(logs[1].4, approval_data(false));
    assert_eq!(logs[2].4, approval_data(true));

    let transactions: Vec<String> = sqlx::query_scalar(
        "SELECT transaction_hash FROM raw_transactions
         WHERE chain_id = $1 ORDER BY transaction_index",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        transactions,
        [
            DECLARED_APPROVAL_TRANSACTION.to_owned(),
            CONTEXT_APPROVAL_TRANSACTION.to_owned()
        ]
    );
    let receipt_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_receipts WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(receipt_count, 2);
    let approval_derived_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND raw_fact_ref ->> 'kind' = 'raw_log'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        approval_derived_count, 0,
        "approval logs must not produce normalized output; manifest sync may still emit its independent boundary event"
    );

    Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            sources: vec![source],
            cursors: Vec::new(),
            redo_range: Some((0, 1)),
            resume_current: None,
        })
        .await?;
    let repeated_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM raw_logs WHERE chain_id = $1),
             (SELECT count(*) FROM raw_transactions WHERE chain_id = $1),
             (SELECT count(*) FROM raw_receipts WHERE chain_id = $1)",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        repeated_counts,
        (3, 2, 2),
        "raw-fact writes must deduplicate"
    );

    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_boundary_redo_reconciles_cursor_before_normal_restart() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_boundary_redo_cursor").await?;
    let chain_id = "rpc-ingest-boundary-redo";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, hash_epoch, server) = spawn_hash_switchable_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "boundary-redo-runner")?;

    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1,
    )
    .await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1.to_owned()),
            Some(BLOCK_1.to_owned()),
            Some(BLOCK_1.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1.to_owned()),
        )
    );

    hash_epoch.store(1, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1_REORG.to_owned()),
            Some(BLOCK_1_REORG.to_owned()),
            Some(BLOCK_1.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1_REORG.to_owned()),
        ),
        "the redo must reconcile the cursor while preserving the old handoff until restart"
    );

    run_until_ingest_handoff(runner, configured_chain, scratch.pool(), BLOCK_1_REORG).await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1_REORG.to_owned()),
            Some(BLOCK_1_REORG.to_owned()),
            Some(BLOCK_1_REORG.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1_REORG.to_owned()),
        )
    );
    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn resumed_ingest_redo_requires_lineage_before_reconciling_cursor() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_redo_lineage_guard").await?;
    let chain_id = "rpc-ingest-redo-lineage-guard";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, hash_epoch, server) = spawn_hash_switchable_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "redo-lineage-normal-runner")?;
    run_until_ingest_handoff(runner, configured_chain.clone(), scratch.pool(), BLOCK_1).await?;

    hash_epoch.store(1, Ordering::SeqCst);
    let interrupting_ingest = Arc::new(InterruptAfterCompletedIngestBatch {
        inner: IngestPhase::new(scratch.pool().clone()),
        interrupt_next_batch: AtomicBool::new(false),
    });
    let interrupted_runner = production_ingest_runner_with_phase(
        scratch.runner(),
        "redo-lineage-interrupted-runner",
        interrupting_ingest,
    )?;
    let interruption = interrupted_runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the wrapper must interrupt after durable final-batch progress");
    assert_eq!(interruption.kind(), ErrorKind::DataIntegrity);
    assert!(interruption.to_string().contains("forced interruption"));
    let resumable: (
        bool,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT phase.redo_in_progress,
                    phase.redo_current_block_number,
                    phase.redo_current_block_hash,
                    cursor.last_processed_block_number,
                    cursor.last_processed_block_hash
             FROM chain_phase_state phase
             JOIN ingest_cursors cursor USING (chain_id)
             WHERE phase.chain_id = $1
               AND phase.phase_name = 'ingest'
               AND cursor.source_key = 'rpc'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        resumable,
        (
            true,
            Some(1),
            Some(BLOCK_1_REORG.to_owned()),
            Some(1),
            Some(BLOCK_1.to_owned()),
        )
    );
    assert_eq!(
        load_boundary_lineage_hashes(scratch.pool(), chain_id).await?,
        vec![BLOCK_1.to_owned(), BLOCK_1_REORG.to_owned()]
    );

    hash_epoch.store(2, Ordering::SeqCst);
    let resumed_runner = production_ingest_runner(scratch.runner(), "redo-lineage-resume-runner")?;
    let resume_error = resumed_runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the resumed redo must reject a target without matching loaded evidence");
    assert_eq!(resume_error.kind(), ErrorKind::DataIntegrity);
    let message = resume_error.to_string();
    for expected in [BLOCK_1_REORG, BLOCK_1_SECOND_REORG, "rerun the Ingest redo"] {
        assert!(
            message.contains(expected),
            "missing {expected:?} in {message}"
        );
    }
    assert_eq!(
        load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?,
        (true, None, None, Some(BLOCK_1.to_owned()),),
        "a failed resume must clear its progress and preserve the truthful cursor"
    );
    assert_eq!(
        load_boundary_lineage_hashes(scratch.pool(), chain_id).await?,
        vec![BLOCK_1.to_owned(), BLOCK_1_REORG.to_owned()]
    );

    let published_head: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(published_head, (1, BLOCK_1.to_owned()));

    resumed_runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        load_boundary_lineage_hashes(scratch.pool(), chain_id).await?,
        vec![
            BLOCK_1.to_owned(),
            BLOCK_1_REORG.to_owned(),
            BLOCK_1_SECOND_REORG.to_owned(),
        ]
    );
    run_until_ingest_handoff(
        resumed_runner,
        configured_chain,
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
        )
    );
    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn resumed_ingest_redo_reloads_a_divergent_boundary_under_the_current_watch_set() -> Result<()>
{
    let scratch = ScratchDatabase::create("phase_runner_ingest_redo_watch_boundary").await?;
    let chain_id = "rpc-ingest-redo-watch-boundary";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, rpc_state, server) = spawn_watch_plan_boundary_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "redo-watch-boundary-runner")?;

    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1_SECOND_REORG, CONTRACT,).await?,
        1,
        "the initial watch set must load its selected fork-C fact"
    );
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        0,
        "the initial watch set must not select the widened address"
    );

    rpc_state.hash_epoch.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1,
    )
    .await?;
    widen_watch_set(scratch.pool(), chain_id).await?;

    let interrupting_ingest = Arc::new(InterruptAfterCompletedIngestBatch {
        inner: IngestPhase::new(scratch.pool().clone()),
        interrupt_next_batch: AtomicBool::new(false),
    });
    let interrupted_runner = production_ingest_runner_with_phase(
        scratch.runner(),
        "redo-watch-boundary-interrupted-runner",
        interrupting_ingest,
    )?;
    let interruption = interrupted_runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the wrapper must interrupt after durable final-batch progress");
    assert!(interruption.to_string().contains("forced interruption"));
    assert_eq!(
        load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?,
        (
            true,
            Some(BLOCK_1.to_owned()),
            Some(BLOCK_1.to_owned()),
            Some(BLOCK_1.to_owned()),
        )
    );
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1, SIBLING_CONTRACT).await?,
        1,
        "the interrupted redo must durably load fork A under the widened watch set"
    );

    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    let resumed_runner =
        production_ingest_runner(scratch.runner(), "redo-watch-boundary-resumed-runner")?;
    let resume_result = resumed_runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await;
    let silently_resumed = resume_result.is_ok();
    if silently_resumed {
        run_until_ingest_handoff(
            resumed_runner.clone(),
            configured_chain.clone(),
            scratch.pool(),
            BLOCK_1_SECOND_REORG,
        )
        .await?;
    }
    let cursor_after_resume = load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?;
    let published_after_resume: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let widened_fact_after_resume = raw_log_count(
        scratch.pool(),
        chain_id,
        BLOCK_1_SECOND_REORG,
        SIBLING_CONTRACT,
    )
    .await?;
    let boundary_calls_after_resume = rpc_state.boundary_log_calls.load(Ordering::SeqCst);
    let resume_error = resume_result.expect_err(&format!(
        "a resumed empty redo silently reconciled to the old fork: cursor={cursor_after_resume:?}, \
         published={published_after_resume:?}, boundary_get_logs={boundary_calls_after_resume}, \
         widened_raw_logs={widened_fact_after_resume}"
    ));
    assert_eq!(resume_error.kind(), ErrorKind::DataIntegrity);
    let message = resume_error.to_string();
    for expected in [
        chain_id,
        "block 1",
        BLOCK_1,
        BLOCK_1_SECOND_REORG,
        "rerun the Ingest redo",
        "current watch plan",
    ] {
        assert!(
            message.contains(expected),
            "missing {expected:?} in {message}"
        );
    }
    assert_eq!(boundary_calls_after_resume, 0);
    assert_eq!(
        cursor_after_resume,
        (true, None, None, Some(BLOCK_1.to_owned())),
        "the failed resume must stay marked in progress, clear redo progress, and preserve the truthful cursor"
    );
    assert_eq!(published_after_resume, (1, BLOCK_1.to_owned()));
    assert_eq!(widened_fact_after_resume, 0);

    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    resumed_runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert!(rpc_state.boundary_log_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        1,
        "the fresh redo must load the widened fact on the newly canonical fork"
    );
    run_until_ingest_handoff(
        resumed_runner,
        configured_chain,
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
        )
    );
    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn required_ingest_redo_demotes_an_overlapping_completed_verify_attestation() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_demotes_verify").await?;
    let manifests = IngestWatchManifestFixture::new(BASE)?;
    let tip = BASE_COINBASE_SEAM_BLOCK;
    manifests.write_from(false, tip)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let chain = ChainConfig::new(
        BASE,
        vec![
            SourceConfig::new_with_role(
                BASE,
                "coinbase-history",
                "coinbase_sql",
                SeedBasis::BaseSeam,
                BASE_COINBASE_SEAM_BLOCK,
                SourceRole::Intake,
                "https://coinbase.invalid",
            )?,
            SourceConfig::new_with_role(
                BASE,
                "drpc-intake",
                "drpc",
                SeedBasis::BaseSeam,
                BASE_COINBASE_SEAM_BLOCK,
                SourceRole::Intake,
                "https://drpc-intake.invalid",
            )?,
            SourceConfig::new_with_role(
                BASE,
                "drpc-reference",
                "drpc",
                SeedBasis::BaseSeam,
                BASE_COINBASE_SEAM_BLOCK,
                SourceRole::VerificationOnly,
                "https://drpc.invalid",
            )?,
        ],
        true,
    )?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(BASE).await?;
    store
        .ensure_ingest_sources(BASE, &chain.intake_sources())
        .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'finalized')",
    )
    .bind(BASE)
    .bind(verify_attestation_hash(tip))
    .bind(verify_attestation_hash(tip - 1))
    .bind(tip)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (
             chain_id, latest_block_hash, latest_block_number,
             safe_block_hash, safe_block_number,
             finalized_block_hash, finalized_block_number
         ) VALUES ($1, $2, $3, $2, $3, $2, $3)",
    )
    .bind(BASE)
    .bind(verify_attestation_hash(tip))
    .bind(tip)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address
         ) VALUES ($1, $2, $3, $4, 0, $5)",
    )
    .bind(BASE)
    .bind(verify_attestation_hash(tip))
    .bind(tip)
    .bind(verify_attestation_transaction(false))
    .bind(SENDER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data
         ) VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)",
    )
    .bind(BASE)
    .bind(verify_attestation_hash(tip))
    .bind(tip)
    .bind(verify_attestation_transaction(false))
    .bind(CONTRACT)
    .bind(vec![TRANSFER_TOPIC.to_owned()])
    .bind(Vec::<u8>::new())
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE ingest_cursors
         SET next_block_number = CASE
                 WHEN source_key = 'coinbase-history' THEN $2 + 1
                 ELSE $3 + 1
             END,
             target_block_number = CASE
                 WHEN source_key = 'coinbase-history' THEN $2
                 ELSE $3
             END,
             last_processed_block_number = CASE
                 WHEN source_key = 'coinbase-history' THEN $2
                 ELSE $3
             END,
             last_processed_block_hash = CASE
                 WHEN source_key = 'coinbase-history' THEN $4
                 ELSE $5
             END
         WHERE chain_id = $1
           AND source_key IN ('coinbase-history', 'drpc-intake')",
    )
    .bind(BASE)
    .bind(BASE_COINBASE_SEAM_BLOCK)
    .bind(tip)
    .bind(verify_attestation_hash(BASE_COINBASE_SEAM_BLOCK))
    .bind(verify_attestation_hash(tip))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = $2,
             current_block_hash = $3, target_block_number = $2,
             target_block_hash = $3, input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4
             END,
             live_handoff_block_number = CASE
                 WHEN phase_name = 'ingest' THEN $2
             END,
             live_handoff_block_hash = CASE
                 WHEN phase_name = 'ingest' THEN $3
             END,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(BASE)
    .bind(tip)
    .bind(verify_attestation_hash(tip))
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;
    let references = Arc::new(AttestationReferences::default());
    let live_calls = Arc::new(AtomicUsize::new(0));
    let phases = PhaseSet::new([
        Arc::new(RawFactChangeIngestPhase {
            pool: scratch.pool().clone(),
        }) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            scratch.verification_database(2).await?,
            references.clone(),
        )),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&live_calls),
        }),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-ingest-verify-attestation",
        test_timing(),
    )?;
    runner.run_chain(&chain, CancellationToken::new()).await?;
    assert_eq!(references.calls.load(Ordering::SeqCst), 1);
    let verified: (String, Option<String>, i64) = sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        verified,
        (
            "completed".to_owned(),
            Some("cross_checked".to_owned()),
            BASE_COINBASE_SEAM_BLOCK,
        )
    );
    manifests.write_from(true, tip)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let authority_marker: String = sqlx::query_scalar(
        "SELECT input_content_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    let (_, authority_generation) = authority_marker
        .rsplit_once(':')
        .context("manifest-authority marker is missing its generation token")?;
    let authority_generation = authority_generation.to_owned();
    let required: (i64, i64) = sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(required, (tip, tip));
    runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(required.0, required.1)?,
            CancellationToken::new(),
        )
        .await?;
    let attested_runner = runner
        .clone()
        .with_watch_set_coverage_attestation(BASE, authority_generation);
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            BASE,
            &verify_attestation_hash(tip),
            SIBLING_CONTRACT,
        )
        .await?,
        1,
        "the required Ingest redo must load the widened raw extent"
    );
    attested_runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(required.0, required.1)?,
            CancellationToken::new(),
        )
        .await?;
    let demoted: (String, Option<String>, bool, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, verification_level, redo_in_progress,
                redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        demoted,
        (
            "running".to_owned(),
            Some("cross_checked".to_owned()),
            true,
            Some(tip),
            Some(tip),
        ),
        "changing an attested raw extent must stamp Verify before redo completion commits"
    );
    let mismatch = runner
        .run_chain(&chain, CancellationToken::new())
        .await
        .expect_err("Verify must reread the changed raw-fact extent");
    assert_eq!(
        mismatch.kind(),
        ErrorKind::VerificationMismatch,
        "unexpected restart failure: {mismatch}"
    );
    assert_eq!(
        references.calls.load(Ordering::SeqCst),
        2,
        "normal restart must execute Verify against the changed raw-fact set"
    );
    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn explicit_ingest_redo_clears_a_manifest_widening_obligation_and_unblocks_derivation()
-> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_manifest_widening_required").await?;
    let chain_id = "rpc-ingest-manifest-widening-required";
    let manifests = IngestWatchManifestFixture::new(chain_id)?;
    manifests.write(false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let (endpoint, rpc_state, server) = spawn_watch_plan_boundary_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "manifest-widening-required-runner")?;
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1,
    )
    .await?;
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1, SIBLING_CONTRACT).await?,
        0,
        "the narrow watch plan must not retain the future address-family fact"
    );
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'idle', verification_level = NULL,
             current_block_number = NULL, current_block_hash = NULL,
             target_block_number = NULL, target_block_hash = NULL,
             input_content_hash = NULL, live_handoff_block_number = NULL,
             live_handoff_block_hash = NULL, last_error = NULL,
             started_at = NULL, finished_at = NULL
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project', 'verify', 'live')",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    manifests.write(true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let required: (i64, i64) = sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'
           AND redo_in_progress",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(required, (0, 1));
    let blocked = runner
        .run_chain(&configured_chain, CancellationToken::new())
        .await
        .expect_err("normal derivation must not auto-run a costly required Ingest redo");
    for expected in [chain_id, "--phase ingest", "--from-block 0", "--to-block 1"] {
        assert!(
            blocked.to_string().contains(expected),
            "missing {expected:?} in {blocked}"
        );
    }

    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    let sibling_error = runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a required redo on a sibling fork must not cover the readable fork");
    assert_eq!(sibling_error.kind(), ErrorKind::DataIntegrity);
    assert!(sibling_error.to_string().contains("readable"));
    rpc_state.hash_epoch.store(0, Ordering::SeqCst);

    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .context("the exact explicit Ingest redo must clear the widening obligation")?;
    assert!(rpc_state.boundary_log_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1, SIBLING_CONTRACT).await?,
        1,
        "the explicit redo must fetch the newly watched fact"
    );
    let still_required: bool = sqlx::query_scalar(
        "SELECT redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!still_required);

    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task_chain = configured_chain.clone();
    let task_runner = runner.clone();
    let mut task =
        tokio::spawn(async move { task_runner.run_chain(&task_chain, task_cancellation).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status: String = sqlx::query_scalar(
                "SELECT phase_status FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name = 'interpret'",
            )
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
            if status == "completed" {
                cancellation.cancel();
                return Ok::<_, anyhow::Error>(());
            }
            if task.is_finished() {
                let result = (&mut task).await.context("post-redo runner panicked")?;
                result.context("derivation stayed blocked after the required Ingest redo")?;
                anyhow::bail!("post-redo runner exited before Interpret completed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("derivation did not resume after the required Ingest redo")??;
    task.await??;

    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_required_ingest_redo_resumes_its_bound_checkpoint() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_required_ingest_retry_resume").await?;
    let chain_id = "rpc-required-ingest-retry-resume";
    let manifests = IngestWatchManifestFixture::new(chain_id)?;
    manifests.write(false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let (endpoint, _rpc_state, server) = spawn_watch_plan_boundary_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    run_until_ingest_handoff(
        production_ingest_runner(scratch.runner(), "required-ingest-retry-seed")?,
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1,
    )
    .await?;

    manifests.write(true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    assert_eq!(
        sqlx::query_as::<_, (Option<i64>, Option<Value>, Option<String>)>(
            "SELECT redo_current_block_number, redo_source_boundary_markers,
                    redo_manifest_authority_fingerprint
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'ingest'",
        )
        .bind(chain_id)
        .fetch_one(scratch.pool())
        .await?,
        (None, None, None),
        "a sync-stamped required Ingest redo must begin without resumable evidence"
    );

    let observed = Arc::new(Mutex::new(Vec::new()));
    production_ingest_runner_with_phase(
        scratch.runner(),
        "required-ingest-retry",
        Arc::new(ProgressThenTransientIngestPhase::new(Arc::clone(&observed))),
    )?
    .redo(
        &configured_chain,
        RedoPhase::Phase(PhaseName::Ingest),
        BlockRange::new(0, 1)?,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        *observed
            .lock()
            .expect("resume observation lock must not be poisoned"),
        vec![(Some(0), Some(0))],
        "the retry must receive the saved checkpoint and per-source boundary marker"
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
        )
        .bind(chain_id)
        .fetch_one(scratch.pool())
        .await?
    );

    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn pre_upgrade_range_end_redo_checkpoint_requires_loaded_boundary_evidence() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_redo_pre_upgrade_boundary").await?;
    let chain_id = "rpc-ingest-redo-pre-upgrade-boundary";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, rpc_state, server) = spawn_watch_plan_boundary_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "redo-pre-upgrade-boundary-runner")?;

    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1_SECOND_REORG, CONTRACT).await?,
        1,
        "the historical fork must have retained lineage and its narrow-watch fact"
    );

    rpc_state.hash_epoch.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1,
    )
    .await?;
    widen_watch_set(scratch.pool(), chain_id).await?;
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        0,
        "the old fork must not contain the fact admitted by the widened watch set"
    );

    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             redo_in_progress = true,
             redo_attempt_generation = 0,
             redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_last_error = NULL,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 1,
             redo_to_block_number = 1,
             redo_current_block_number = 1,
             redo_current_block_hash = $2,
             redo_target_block_number = 1,
             redo_target_block_hash = $2,
             redo_source_boundary_markers = NULL,
             redo_manifest_authority_fingerprint = $3,
             last_error = NULL,
             finished_at = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .bind(BLOCK_1_SECOND_REORG)
    .bind(active_manifest_watch_plan_fingerprint(scratch.pool(), chain_id).await?)
    .execute(scratch.pool())
    .await?;
    assert_eq!(
        load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?,
        (
            true,
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1.to_owned()),
        ),
        "the seeded checkpoint must match the format written before source markers existed"
    );
    assert_eq!(
        sqlx::query_as::<_, (bool, i64, bool)>(
            "SELECT redo_source_boundary_markers IS NULL, redo_attempt_generation,
                    redo_manifest_authority_fingerprint = $2
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'ingest'",
        )
        .bind(chain_id)
        .bind(active_manifest_watch_plan_fingerprint(scratch.pool(), chain_id).await?)
        .fetch_one(scratch.pool())
        .await?,
        (true, 0, true),
        "the source-marker checkpoint must retain the current manifest/watch-plan fingerprint and the generation default"
    );

    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    let resume_result = runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await;
    let cursor_after_resume = load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?;
    let published_after_resume: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let boundary_calls_after_resume = rpc_state.boundary_log_calls.load(Ordering::SeqCst);
    let resume_error = resume_result.expect_err(&format!(
        "a pre-upgrade checkpoint resumed without load-derived evidence: \
         cursor={cursor_after_resume:?}, published={published_after_resume:?}, \
         boundary_get_logs={boundary_calls_after_resume}"
    ));
    assert_eq!(resume_error.kind(), ErrorKind::DataIntegrity);
    let message = resume_error.to_string();
    for expected in [
        chain_id,
        "source rpc",
        "block 1",
        "no load-derived boundary marker was persisted",
        "rerun the Ingest redo",
        "current watch plan",
    ] {
        assert!(
            message.contains(expected),
            "missing {expected:?} in {message}"
        );
    }
    assert_eq!(boundary_calls_after_resume, 0);
    assert_eq!(
        cursor_after_resume,
        (true, None, None, Some(BLOCK_1.to_owned())),
        "the failed resume must clear resumable progress and preserve the truthful cursor"
    );
    assert_eq!(published_after_resume, (1, BLOCK_1.to_owned()));

    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert!(rpc_state.boundary_log_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        1,
        "the fresh redo must fetch the widened fact before adopting the old fork"
    );
    run_until_ingest_handoff(
        runner,
        configured_chain,
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
        )
    );
    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_redo_checkpoint_does_not_cross_a_manifest_authority_change() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_redo_manifest_authority").await?;
    let chain_id = "rpc-ingest-redo-manifest-authority";
    let manifests = IngestWatchManifestFixture::new(chain_id)?;
    manifests.write(false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let w0_fingerprint = active_manifest_watch_plan_fingerprint(scratch.pool(), chain_id).await?;
    let (endpoint, rpc_state, server) = spawn_watch_plan_boundary_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "redo-manifest-authority-runner")?;
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1,
    )
    .await?;
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1, CONTRACT).await?,
        1
    );
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1, SIBLING_CONTRACT).await?,
        0
    );

    let interrupting_ingest = Arc::new(InterruptAfterCompletedIngestBatch {
        inner: IngestPhase::new(scratch.pool().clone()),
        interrupt_next_batch: AtomicBool::new(false),
    });
    let interrupted_runner = production_ingest_runner_with_phase(
        scratch.runner(),
        "redo-manifest-authority-interrupted-runner",
        interrupting_ingest,
    )?;
    for expected_interruption in 1..=2 {
        let interruption = interrupted_runner
            .redo(
                &configured_chain,
                RedoPhase::Phase(PhaseName::Ingest),
                BlockRange::new(1, 1)?,
                CancellationToken::new(),
            )
            .await
            .expect_err("the wrapper must interrupt after durable final-batch progress");
        assert!(interruption.to_string().contains("forced interruption"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*)
                 FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name = 'ingest'
                   AND redo_in_progress
                   AND redo_current_block_number = 1
                   AND redo_current_block_hash = $2
                   AND redo_source_boundary_markers -> 'rpc' ->> 'hash' = $2
                   AND redo_manifest_authority_fingerprint = $3",
            )
            .bind(chain_id)
            .bind(BLOCK_1)
            .bind(&w0_fingerprint)
            .fetch_one(scratch.pool())
            .await?,
            1,
            "interruption {expected_interruption} must persist numeric and per-source evidence"
        );

        rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
        if expected_interruption == 1 {
            runner
                .redo(
                    &configured_chain,
                    RedoPhase::Phase(PhaseName::Ingest),
                    BlockRange::new(1, 1)?,
                    CancellationToken::new(),
                )
                .await?;
            assert_eq!(
                rpc_state.boundary_log_calls.load(Ordering::SeqCst),
                0,
                "unchanged active manifest/watch-plan inputs must resume the durable checkpoint"
            );
        }
    }

    manifests.write(true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let w1_fingerprint = active_manifest_watch_plan_fingerprint(scratch.pool(), chain_id).await?;
    assert_ne!(
        w0_fingerprint, w1_fingerprint,
        "adding a watched root must change the per-chain manifest/watch-plan fingerprint"
    );
    assert_eq!(
        load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?,
        (true, None, None, Some(BLOCK_1.to_owned())),
        "manifest sync must discard the W0 checkpoint before an operator can resume it"
    );
    let required: (i64, i64, String) = sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!((required.0, required.1), (0, 1));
    assert!(required.2.starts_with("required downstream redo:"));

    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    let narrow_retry = runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the old 1..=1 attempt must not undercut the widened required range");
    for expected in ["--phase ingest", "--from-block 0", "--to-block 1"] {
        assert!(narrow_retry.to_string().contains(expected));
    }
    assert_eq!(rpc_state.boundary_log_calls.load(Ordering::SeqCst), 0);

    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert!(rpc_state.boundary_log_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1, SIBLING_CONTRACT).await?,
        1,
        "completion under W1 must follow a real W1 boundary load"
    );

    let missing_stamp_interruption = interrupted_runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the wrapper must persist another completed-batch checkpoint");
    assert!(
        missing_stamp_interruption
            .to_string()
            .contains("forced interruption")
    );
    sqlx::query(
        "UPDATE chain_phase_state
         SET redo_manifest_authority_fingerprint = NULL
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    let missing_stamp_error = runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("an active checkpoint without an authority fingerprint must fail closed");
    assert_eq!(missing_stamp_error.kind(), ErrorKind::DataIntegrity);
    assert!(
        missing_stamp_error
            .to_string()
            .contains("redo authority changed")
    );
    assert_eq!(rpc_state.boundary_log_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?,
        (true, None, None, Some(BLOCK_1.to_owned())),
        "a pre-fingerprint active redo must discard resumable evidence and preserve its cursor"
    );

    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert!(rpc_state.boundary_log_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1, SIBLING_CONTRACT).await?,
        1
    );
    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn fresh_ingest_redo_rejects_a_loaded_boundary_that_diverges_from_target() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_redo_loaded_boundary").await?;
    let chain_id = "rpc-ingest-redo-loaded-boundary";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, rpc_state, server) = spawn_watch_plan_boundary_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "redo-loaded-boundary-runner")?;

    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1_SECOND_REORG, CONTRACT).await?,
        1
    );
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        0
    );

    rpc_state.hash_epoch.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1,
    )
    .await?;
    widen_watch_set(scratch.pool(), chain_id).await?;

    rpc_state.script_boundary_epochs([2, 1, 1, 2, 2]);
    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    let redo_result = runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await;
    let state_after_redo = load_redo_and_cursor_hashes(scratch.pool(), chain_id).await?;
    let scripted_calls_after_redo = 5 - rpc_state.scripted_epochs_remaining();
    let b_fact_after_redo =
        raw_log_count(scratch.pool(), chain_id, BLOCK_1_REORG, SIBLING_CONTRACT).await?;
    let c_fact_after_redo = raw_log_count(
        scratch.pool(),
        chain_id,
        BLOCK_1_SECOND_REORG,
        SIBLING_CONTRACT,
    )
    .await?;
    if redo_result.is_ok() {
        run_until_ingest_handoff(
            runner.clone(),
            configured_chain.clone(),
            scratch.pool(),
            BLOCK_1_SECOND_REORG,
        )
        .await?;
    }
    let published_after_redo: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let redo_error = redo_result.expect_err(&format!(
        "redo reported the pre-load target instead of the loaded boundary: \
         state={state_after_redo:?}, scripted_boundary_calls={scripted_calls_after_redo}, \
         loaded_b_fact={b_fact_after_redo}, reported_c_fact={c_fact_after_redo}, \
         published={published_after_redo:?}"
    ));
    assert_eq!(redo_error.kind(), ErrorKind::DataIntegrity);
    let message = redo_error.to_string();
    for expected in [
        chain_id,
        "block 1",
        BLOCK_1_REORG,
        BLOCK_1_SECOND_REORG,
        "rerun the Ingest redo",
        "current watch plan",
    ] {
        assert!(
            message.contains(expected),
            "missing {expected:?} in {message}"
        );
    }
    assert_eq!(scripted_calls_after_redo, 3);
    assert_eq!(
        state_after_redo,
        (true, None, None, Some(BLOCK_1.to_owned()))
    );
    assert_eq!(published_after_redo, (1, BLOCK_1.to_owned()));
    assert_eq!(b_fact_after_redo, 1);
    assert_eq!(c_fact_after_redo, 0);

    rpc_state.clear_boundary_script();
    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    rpc_state.boundary_log_calls.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert!(rpc_state.boundary_log_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        1,
        "the consistent rerun must store the widened fact for the boundary it reports"
    );
    run_until_ingest_handoff(
        runner,
        configured_chain,
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
        )
    );
    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn fresh_ingest_redo_reports_the_boundary_returned_by_the_load() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_redo_loaded_report").await?;
    let chain_id = "rpc-ingest-redo-loaded-report";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, rpc_state, server) = spawn_watch_plan_boundary_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner(scratch.runner(), "redo-loaded-report-runner")?;

    rpc_state.hash_epoch.store(2, Ordering::SeqCst);
    run_until_ingest_handoff(
        runner.clone(),
        configured_chain.clone(),
        scratch.pool(),
        BLOCK_1_SECOND_REORG,
    )
    .await?;
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        0
    );

    rpc_state.hash_epoch.store(0, Ordering::SeqCst);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    widen_watch_set(scratch.pool(), chain_id).await?;

    rpc_state.script_boundary_epochs([1, 1, 1, 2, 2]);
    runner
        .redo(
            &configured_chain,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        load_ingest_boundary_state(scratch.pool(), chain_id).await?,
        (
            "completed".to_owned(),
            Some(BLOCK_1_REORG.to_owned()),
            Some(BLOCK_1_REORG.to_owned()),
            Some(BLOCK_1_SECOND_REORG.to_owned()),
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1_REORG.to_owned()),
        ),
        "redo completion must report the marker returned by its boundary load"
    );
    assert_eq!(rpc_state.scripted_epochs_remaining(), 2);
    assert_eq!(
        raw_log_count(scratch.pool(), chain_id, BLOCK_1_REORG, SIBLING_CONTRACT).await?,
        1
    );
    assert_eq!(
        raw_log_count(
            scratch.pool(),
            chain_id,
            BLOCK_1_SECOND_REORG,
            SIBLING_CONTRACT,
        )
        .await?,
        0
    );

    server.abort();
    scratch.cleanup().await
}

struct InterruptAfterCompletedIngestBatch {
    inner: IngestPhase,
    interrupt_next_batch: AtomicBool,
}

type RedoResumeObservation = Arc<Mutex<Vec<(Option<i64>, Option<i64>)>>>;

struct ProgressThenTransientIngestPhase {
    attempts: AtomicUsize,
    observed: RedoResumeObservation,
    loopback: LoopbackPhase,
}

impl ProgressThenTransientIngestPhase {
    fn new(observed: RedoResumeObservation) -> Self {
        Self {
            attempts: AtomicUsize::new(0),
            observed,
            loopback: LoopbackPhase::new(PhaseName::Ingest),
        }
    }
}

impl Phase for ProgressThenTransientIngestPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt > 1 {
            let boundary = context
                .resume
                .ingest_cursors
                .iter()
                .find(|cursor| cursor.source_key == "rpc")
                .and_then(|cursor| cursor.redo_loaded_boundary.as_ref())
                .map(|marker| marker.number);
            self.observed
                .lock()
                .expect("resume observation lock must not be poisoned")
                .push((
                    context.resume.current.as_ref().map(|marker| marker.number),
                    boundary,
                ));
            return self.loopback.run_batch(context);
        }
        Box::pin(async move {
            if attempt == 1 {
                return Err(RunnerError::transient("forced transient Ingest retry"));
            }
            let current = phase_runner::heads::BlockMarker::new(0, BLOCK_0)?;
            let target = context
                .available_heads
                .expect("fixture requires readable Ingest heads")
                .latest;
            Ok(PhaseBatchOutcome::Continue(PhaseProgress {
                current: Some(current.clone()),
                target: Some(target.clone()),
                source_progress: vec![phase_runner::phase::SourceProgress {
                    source_key: "rpc".to_owned(),
                    current: Some(current.clone()),
                    target: Some(target),
                    redo_loaded_boundary: Some(current),
                }],
                ..PhaseProgress::default()
            }))
        })
    }
}

struct IngestWatchManifestFixture {
    root: PathBuf,
    chain: String,
}

impl IngestWatchManifestFixture {
    fn new(chain: &str) -> Result<Self> {
        let root = std::env::temp_dir().join(format!(
            "bigname-ingest-manifest-authority-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(root.join("test/test_events"))?;
        Ok(Self {
            root,
            chain: chain.to_owned(),
        })
    }

    fn write(&self, include_widened_address: bool) -> Result<()> {
        self.write_from(include_widened_address, 0)
    }

    fn write_from(&self, include_widened_address: bool, start_block: i64) -> Result<()> {
        let widened_root = if include_widened_address {
            format!(
                r#"
[[roots]]
name = "source_b"
address = "{SIBLING_CONTRACT}"
start_block = {start_block}
"#,
            )
        } else {
            "roots = []\n".to_owned()
        };
        let manifest = format!(
            r#"
manifest_version = 1
namespace = "test"
source_family = "test_events"
chain = "{}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "{NORMALIZER}"
discovery_rules = []
{widened_root}

[capability_flags]

[[contracts]]
role = "source_a"
address = "{CONTRACT}"
proxy_kind = "none"
start_block = {start_block}

[[abi.events]]
name = "Transfer"
fragment = "event Transfer(address indexed from, address indexed to, uint256 value)"
emitter_roles = ["source_a"]
normalized_events = []
status = "supported"
"#,
            self.chain
        );
        fs::write(self.root.join("test/test_events/v1.toml"), manifest)?;
        Ok(())
    }
}

impl Drop for IngestWatchManifestFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct ApprovalManifestFixture {
    root: PathBuf,
}

impl ApprovalManifestFixture {
    fn new(chain: &str) -> Result<Self> {
        let root = std::env::temp_dir().join(format!(
            "bigname-approval-intake-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        fs::create_dir_all(root.join("ens/ens_v1_registry_l1"))?;
        fs::write(
            root.join("ens/ens_v1_registry_l1/v1.toml"),
            format!(
                r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_registry_l1"
chain = "{chain}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "{NORMALIZER}"
roots = []
discovery_rules = []

[capability_flags]

[[contracts]]
role = "registry"
address = "{CONTRACT}"
proxy_kind = "none"
start_block = 0

[[abi.events]]
name = "ApprovalForAll"
fragment = "event ApprovalForAll(address indexed owner, address indexed operator, bool approved)"
emitter_roles = ["registry"]
normalized_events = []
"#
            ),
        )?;
        Ok(Self { root })
    }
}

impl Drop for ApprovalManifestFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl Phase for InterruptAfterCompletedIngestBatch {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if self.interrupt_next_batch.swap(false, Ordering::SeqCst) {
                return Err(RunnerError::data_integrity(
                    "forced interruption after durable final-batch redo progress",
                ));
            }
            match self.inner.run_batch(context).await? {
                PhaseBatchOutcome::Complete(progress) => {
                    self.interrupt_next_batch.store(true, Ordering::SeqCst);
                    Ok(PhaseBatchOutcome::Continue(progress))
                }
                outcome => Ok(outcome),
            }
        })
    }
}

fn production_ingest_runner(
    database: phase_runner::database::RunnerDatabase,
    instance_id: &str,
) -> Result<PhaseRunner> {
    production_ingest_runner_with_phase(
        database.clone(),
        instance_id,
        Arc::new(IngestPhase::new(database.pool().clone())),
    )
}

fn production_ingest_runner_with_phase(
    database: phase_runner::database::RunnerDatabase,
    instance_id: &str,
    ingest: Arc<dyn Phase>,
) -> Result<PhaseRunner> {
    let phases = PhaseSet::new([
        ingest,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Project)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Verify)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Live)) as Arc<dyn Phase>,
    ])?;
    Ok(PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        instance_id,
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?)
}

async fn run_until_ingest_handoff(
    runner: PhaseRunner,
    chain: ChainConfig,
    pool: &sqlx::PgPool,
    expected_hash: &str,
) -> Result<()> {
    let chain_id = chain.chain_id.clone();
    let recovery_runner = runner.clone();
    let recovery_chain = chain.clone();
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task = tokio::spawn(async move { runner.run_chain(&chain, task_cancellation).await });
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let state: Option<(String, Option<String>, bool)> = sqlx::query_as(
                "SELECT ingest.phase_status, ingest.live_handoff_block_hash,
                        EXISTS (
                            SELECT 1 FROM chain_phase_state pending
                            WHERE pending.chain_id = ingest.chain_id
                              AND pending.redo_in_progress
                        ) AS has_pending_redo
                 FROM chain_phase_state ingest
                 WHERE ingest.chain_id = $1 AND ingest.phase_name = 'ingest'",
            )
            .bind(&chain_id)
            .fetch_optional(pool)
            .await?;
            match state {
                Some((status, Some(handoff), false))
                    if status == "completed" && handoff == expected_hash =>
                {
                    return Ok::<_, anyhow::Error>(());
                }
                _ => {}
            }
            if task.is_finished() {
                let result = (&mut task)
                    .await
                    .context("production ingest restart task panicked")?;
                result.context("production ingest restart exited before convergence")?;
                anyhow::bail!("production ingest restart exited before convergence");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("production ingest restart did not converge")??;
    cancellation.cancel();
    task.await??;
    let recovery_cancellation = CancellationToken::new();
    recovery_cancellation.cancel();
    recovery_runner
        .run_chain(&recovery_chain, recovery_cancellation)
        .await?;
    Ok(())
}

type IngestBoundaryState = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

async fn load_ingest_boundary_state(
    pool: &sqlx::PgPool,
    chain_id: &str,
) -> Result<IngestBoundaryState> {
    Ok(sqlx::query_as(
        "SELECT phase.phase_status,
                phase.current_block_hash,
                phase.target_block_hash,
                phase.live_handoff_block_hash,
                cursor.next_block_number,
                cursor.target_block_number,
                cursor.last_processed_block_number,
                cursor.last_processed_block_hash
         FROM chain_phase_state phase
         JOIN ingest_cursors cursor USING (chain_id)
         WHERE phase.chain_id = $1
           AND phase.phase_name = 'ingest'
           AND cursor.source_key = 'rpc'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?)
}

async fn load_boundary_lineage_hashes(pool: &sqlx::PgPool, chain_id: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT block_hash
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number = 1
         ORDER BY block_hash",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?)
}

async fn load_redo_and_cursor_hashes(
    pool: &sqlx::PgPool,
    chain_id: &str,
) -> Result<(bool, Option<String>, Option<String>, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT phase.redo_in_progress, phase.redo_current_block_hash,
                phase.redo_target_block_hash,
                cursor.last_processed_block_hash
         FROM chain_phase_state phase
         JOIN ingest_cursors cursor USING (chain_id)
         WHERE phase.chain_id = $1
           AND phase.phase_name = 'ingest'
           AND cursor.source_key = 'rpc'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?)
}

async fn active_manifest_watch_plan_fingerprint(
    pool: &sqlx::PgPool,
    chain_id: &str,
) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT encode(
             public.digest(
                 COALESCE(
                     jsonb_agg(
                         manifest_payload - 'normalizer_version'
                         ORDER BY namespace, source_family
                     )::text,
                     '[]'
                 ),
                 'sha256'
             ),
             'hex'
         )
         FROM manifest_versions
         WHERE chain_id = $1 AND rollout_status = 'active'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?)
}

async fn raw_log_count(
    pool: &sqlx::PgPool,
    chain_id: &str,
    block_hash: &str,
    address: &str,
) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs
         WHERE chain_id = $1 AND block_hash = $2
           AND lower(emitting_address) = lower($3)",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(address)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn source_kind_change_after_first_batch_crash_is_rejected_at_phase_entry() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_crash_kind_change").await?;
    seed_watch_set(scratch.pool(), SEPOLIA).await?;

    let (drpc_endpoint, drpc_server, drpc_requests) = spawn_crash_window_rpc(false).await?;
    let first_live_calls = Arc::new(AtomicUsize::new(0));
    let first_runner = crash_window_runner(&scratch, Arc::clone(&first_live_calls))?;
    let first_error = first_runner
        .run_chain(
            &sepolia_ingest_chain("drpc", &drpc_endpoint)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the fixture must stop after the raw-fact commit");
    assert_eq!(first_error.kind(), ErrorKind::DataIntegrity);
    assert!(drpc_requests.load(Ordering::SeqCst) > 0);
    assert_eq!(first_live_calls.load(Ordering::SeqCst), 0);
    drop(first_runner);
    drpc_server.abort();

    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);
    let identity_after_crash = ingest_identity(scratch.pool()).await?;

    let (rpc_endpoint, rpc_server, rpc_requests) = spawn_crash_window_rpc(true).await?;
    let restarted_live_calls = Arc::new(AtomicUsize::new(0));
    let restarted = complete_ingest_runner(&scratch, Arc::clone(&restarted_live_calls)).await?;
    let error = restarted
        .run_chain(
            &sepolia_ingest_chain("rpc", &rpc_endpoint)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a persisted dRPC source identity must reject an in-place RPC relabel");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("source kind"), "{error}");
    assert!(error.to_string().contains("explicit reset"), "{error}");
    assert_eq!(rpc_requests.load(Ordering::SeqCst), 0);
    assert_eq!(restarted_live_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        identity_after_crash,
        Some((
            "drpc".to_owned(),
            "ethereum_head".to_owned(),
            0,
            0,
            None,
            None,
            None,
        )),
        "phase entry must persist provenance before the first provider write"
    );
    assert_eq!(
        ingest_identity(scratch.pool()).await?,
        identity_after_crash,
        "the rejected kind change must leave the original identity intact"
    );
    let verify: (String, Option<String>) = sqlx::query_as(
        "SELECT phase_status, verification_level
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    assert_ne!(
        verify,
        ("completed".to_owned(), Some("quick_synced".to_owned()))
    );
    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);

    drop(restarted);
    rpc_server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn source_key_change_after_first_batch_crash_is_rejected_at_phase_entry() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_crash_source_replacement").await?;
    seed_watch_set(scratch.pool(), SEPOLIA).await?;

    let (drpc_endpoint, drpc_server, drpc_requests) = spawn_crash_window_rpc(false).await?;
    let first_runner = crash_window_runner(&scratch, Arc::new(AtomicUsize::new(0)))?;
    first_runner
        .run_chain(
            &sepolia_ingest_chain_with(
                "original",
                "drpc",
                SeedBasis::EthereumHead,
                0,
                &drpc_endpoint,
            )?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the fixture must stop after the raw-fact commit");
    assert!(drpc_requests.load(Ordering::SeqCst) > 0);
    drop(first_runner);
    drpc_server.abort();
    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);

    let (replacement_endpoint, replacement_server, replacement_requests) =
        spawn_crash_window_rpc(true).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let restarted = complete_ingest_runner(&scratch, Arc::clone(&live_calls)).await?;
    let error = restarted
        .run_chain(
            &sepolia_ingest_chain_with(
                "replacement",
                "drpc",
                SeedBasis::EthereumHead,
                0,
                &replacement_endpoint,
            )?,
            CancellationToken::new(),
        )
        .await
        .expect_err("retained facts cannot be assigned to a replacement source identity");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("explicit reset"), "{error}");
    assert_eq!(replacement_requests.load(Ordering::SeqCst), 0);
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);
    let identities: Vec<(String, String, Option<i64>)> = sqlx::query_as(
        "SELECT source_key, source_kind, last_processed_block_number
         FROM ingest_cursors WHERE chain_id = $1 ORDER BY source_key",
    )
    .bind(SEPOLIA)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        identities,
        vec![("original".to_owned(), "drpc".to_owned(), None)]
    );
    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);

    drop(restarted);
    replacement_server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn persisted_seed_basis_and_start_block_are_checked_before_provider_writes() -> Result<()> {
    for (prefix, restarted_seed, restarted_start) in [
        (
            "production_ingest_crash_seed_change",
            SeedBasis::NewSignatureRange,
            0,
        ),
        (
            "production_ingest_crash_start_change",
            SeedBasis::EthereumHead,
            1,
        ),
    ] {
        let scratch = ScratchDatabase::create(prefix).await?;
        seed_watch_set(scratch.pool(), SEPOLIA).await?;

        let (first_endpoint, first_server, _) = spawn_crash_window_rpc(false).await?;
        let first_runner = crash_window_runner(&scratch, Arc::new(AtomicUsize::new(0)))?;
        first_runner
            .run_chain(
                &sepolia_ingest_chain_with(
                    "intake",
                    "drpc",
                    SeedBasis::EthereumHead,
                    0,
                    &first_endpoint,
                )?,
                CancellationToken::new(),
            )
            .await
            .expect_err("the fixture must stop after the raw-fact commit");
        drop(first_runner);
        first_server.abort();

        let (restart_endpoint, restart_server, restart_requests) =
            spawn_crash_window_rpc(false).await?;
        let live_calls = Arc::new(AtomicUsize::new(0));
        let restarted = complete_ingest_runner(&scratch, Arc::clone(&live_calls)).await?;
        let error = restarted
            .run_chain(
                &sepolia_ingest_chain_with(
                    "intake",
                    "drpc",
                    restarted_seed,
                    restarted_start,
                    &restart_endpoint,
                )?,
                CancellationToken::new(),
            )
            .await
            .expect_err("persisted seed identity must be checked at phase entry");
        assert_eq!(error.kind(), ErrorKind::DataIntegrity);
        assert!(error.to_string().contains("seed configuration"), "{error}");
        assert_eq!(restart_requests.load(Ordering::SeqCst), 0);
        assert_eq!(live_calls.load(Ordering::SeqCst), 0);
        assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);
        assert_eq!(
            ingest_identity(scratch.pool()).await?,
            Some((
                "drpc".to_owned(),
                "ethereum_head".to_owned(),
                0,
                0,
                None,
                None,
                None,
            ))
        );

        drop(restarted);
        restart_server.abort();
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn cursorless_legacy_raw_facts_require_reset_before_source_initialization() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_legacy_cursorless_facts").await?;
    seed_watch_set(scratch.pool(), SEPOLIA).await?;

    let (rpc_endpoint, rpc_server, rpc_requests) = spawn_crash_window_rpc(false).await?;
    Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: SEPOLIA.to_owned(),
            sources: vec![SourceDescriptor {
                key: "intake".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint: rpc_endpoint,
            }],
            cursors: Vec::new(),
            redo_range: None,
            resume_current: None,
        })
        .await?;
    assert!(rpc_requests.load(Ordering::SeqCst) > 0);
    rpc_server.abort();
    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);
    assert_eq!(ingest_identity(scratch.pool()).await?, None);

    let (drpc_endpoint, drpc_server, drpc_requests) = spawn_crash_window_rpc(true).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let restarted = complete_ingest_runner(&scratch, Arc::clone(&live_calls)).await?;
    let error = restarted
        .run_chain(
            &sepolia_ingest_chain("drpc", &drpc_endpoint)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("cursorless legacy raw facts must require an explicit reset");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("durable ingest data"), "{error}");
    assert!(error.to_string().contains("explicit reset"), "{error}");
    assert_eq!(drpc_requests.load(Ordering::SeqCst), 0);
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);
    assert_eq!(ingest_identity(scratch.pool()).await?, None);
    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);

    drop(restarted);
    drpc_server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_ingest_rejects_a_configured_source_without_a_persisted_cursor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_completed_missing_cursor").await?;
    let chain_id = "completed-ingest-source-rotation";
    seed_completed_ingest(
        &scratch,
        SourceConfig::new(
            chain_id,
            "original",
            "rpc",
            SeedBasis::BaseSeam,
            0,
            "https://original.invalid",
        )?,
    )
    .await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = completed_ingest_skip_runner(&scratch, Arc::clone(&live_calls))?;
    let replacement = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "replacement",
            "rpc",
            SeedBasis::BaseSeam,
            0,
            "https://replacement.invalid",
        )?],
        true,
    )?;

    let result = runner
        .run_chain(&replacement, CancellationToken::new())
        .await;
    let observed_live_calls = live_calls.load(Ordering::SeqCst);

    drop(runner);
    scratch.cleanup().await?;
    let error = result.expect_err("completed Ingest must require every configured source cursor");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("matching cursor"), "{error}");
    assert_eq!(observed_live_calls, 0);
    Ok(())
}

#[tokio::test]
async fn completed_ingest_rejects_a_removed_persisted_source_before_downstream_phases() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_ingest_completed_removed_source").await?;
    let chain_id = "completed-ingest-source-removal";
    let kept = SourceConfig::new(
        chain_id,
        "kept",
        "rpc",
        SeedBasis::BaseSeam,
        0,
        "https://kept.invalid",
    )?;
    let removed = SourceConfig::new(
        chain_id,
        "removed",
        "rpc",
        SeedBasis::BaseSeam,
        0,
        "https://removed.invalid",
    )?;
    seed_completed_ingest_sources(&scratch, &[kept.clone(), removed]).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = completed_ingest_skip_runner(&scratch, Arc::clone(&live_calls))?;
    let reduced = ChainConfig::new(chain_id, vec![kept], true)?;

    let result = runner.run_chain(&reduced, CancellationToken::new()).await;
    let observed_live_calls = live_calls.load(Ordering::SeqCst);

    drop(runner);
    scratch.cleanup().await?;
    let error = result.expect_err("completed Ingest must reject removal of a persisted source");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("persisted ingest source keys"),
        "{error}"
    );
    assert_eq!(observed_live_calls, 0);
    Ok(())
}

#[tokio::test]
async fn completed_ingest_rejects_seed_and_start_drift_before_downstream_phases() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_completed_seed_drift").await?;
    let chain_id = "completed-ingest-seed-drift";
    seed_completed_ingest(
        &scratch,
        SourceConfig::new(
            chain_id,
            "source",
            "rpc",
            SeedBasis::BaseSeam,
            0,
            "https://source.invalid",
        )?,
    )
    .await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = completed_ingest_skip_runner(&scratch, Arc::clone(&live_calls))?;
    let changed = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "source",
            "rpc",
            SeedBasis::NewSignatureRange,
            10,
            "https://source.invalid",
        )?],
        true,
    )?;

    let result = runner.run_chain(&changed, CancellationToken::new()).await;
    let observed_live_calls = live_calls.load(Ordering::SeqCst);

    drop(runner);
    scratch.cleanup().await?;
    let error = result.expect_err("completed Ingest must revalidate its persisted seed identity");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("seed configuration"), "{error}");
    assert_eq!(observed_live_calls, 0);
    Ok(())
}

#[tokio::test]
async fn cursorless_lineage_and_header_audit_require_reset_before_source_initialization()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_cursorless_lineage").await?;
    let chain_id = "cursorless-lineage-chain";
    sqlx::raw_sql(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES (
             'cursorless-lineage-chain', '0xcursorless-lineage', 0,
             to_timestamp(0), 'canonical'
         );
         INSERT INTO chain_header_audit (chain_id, block_hash, state_root)
         VALUES ('cursorless-lineage-chain', '0xcursorless-lineage', '0xstate-root')",
    )
    .execute(scratch.pool())
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    let source = SourceConfig::new(
        chain_id,
        "source",
        "drpc",
        SeedBasis::EthereumHead,
        0,
        "https://source.invalid",
    )?;

    let result = store.ensure_ingest_sources(chain_id, &[source]).await;
    let cursor_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_cursors WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;

    scratch.cleanup().await?;
    let error = result.expect_err("cursorless lineage must remain bound to its original provider");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("durable ingest data"), "{error}");
    assert_eq!(cursor_count, 0);
    Ok(())
}

#[tokio::test]
async fn same_kind_restart_after_first_batch_crash_refetches_and_completes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_crash_same_kind").await?;
    seed_watch_set(scratch.pool(), SEPOLIA).await?;

    let (first_endpoint, first_server, _) = spawn_crash_window_rpc(false).await?;
    let first_runner = crash_window_runner(&scratch, Arc::new(AtomicUsize::new(0)))?;
    first_runner
        .run_chain(
            &sepolia_ingest_chain("drpc", &first_endpoint)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the fixture must stop after the raw-fact commit");
    drop(first_runner);
    first_server.abort();
    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);

    let (restart_endpoint, restart_server, restart_requests) =
        spawn_crash_window_rpc(false).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let restarted = complete_ingest_runner(&scratch, Arc::clone(&live_calls)).await?;
    restarted
        .run_chain(
            &sepolia_ingest_chain("drpc", &restart_endpoint)?,
            CancellationToken::new(),
        )
        .await?;
    assert!(
        restart_requests.load(Ordering::SeqCst) > 0,
        "the restart must refetch the uncheckpointed range"
    );
    assert_eq!(stored_log_indexes(scratch.pool()).await?, vec![0, 1]);
    assert_eq!(
        ingest_identity(scratch.pool()).await?,
        Some((
            "drpc".to_owned(),
            "ethereum_head".to_owned(),
            0,
            2,
            Some(1),
            Some(1),
            Some(BLOCK_1.to_owned()),
        ))
    );
    let verify: (String, Option<String>) = sqlx::query_as(
        "SELECT phase_status, verification_level
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        verify,
        ("completed".to_owned(), Some("quick_synced".to_owned()))
    );
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);

    drop(restarted);
    restart_server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn readded_ingest_settled_before_its_first_cursor_starts_normally() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_readd_before_cursor").await?;
    seed_watch_set(scratch.pool(), SEPOLIA).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(SEPOLIA).await?;
    store
        .start_phase(
            SEPOLIA,
            PhaseName::Ingest,
            &phase_runner::phase::RunMode::Normal,
        )
        .await?;
    let retained_chain = ChainConfig::new(
        "retained-chain",
        vec![SourceConfig::new(
            "retained-chain",
            "rpc",
            "rpc",
            SeedBasis::EthereumHead,
            0,
            "http://unused.invalid",
        )?],
        false,
    )?;
    let runtime = RuntimeConfig::new(
        "production-ingest-settlement",
        vec![retained_chain],
        CapacityConfig::default(),
        test_timing(),
    )?;
    let settlement_cancellation = CancellationToken::new();
    settlement_cancellation.cancel();
    Arc::new(production_ingest_runner(
        scratch.runner(),
        "production-ingest-settlement",
    )?)
    .run(&runtime, settlement_cancellation)
    .await?;

    let (endpoint, server, requests) = spawn_crash_window_rpc(false).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = complete_ingest_runner(&scratch, Arc::clone(&live_calls)).await?;
    runner
        .run_chain(
            &sepolia_ingest_chain("drpc", &endpoint)?,
            CancellationToken::new(),
        )
        .await?;

    assert!(requests.load(Ordering::SeqCst) > 0);
    assert!(ingest_identity(scratch.pool()).await?.is_some());
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    drop(runner);
    server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn fresh_sources_persist_any_kind_before_ingest_runs() -> Result<()> {
    for (prefix, chain_id, kind) in [
        ("production_ingest_fresh_rpc", "fresh-rpc-chain", "rpc"),
        ("production_ingest_fresh_drpc", "fresh-drpc-chain", "drpc"),
    ] {
        let scratch = ScratchDatabase::create(prefix).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let phases = PhaseSet::new([
            Arc::new(ObserveFreshIdentityPhase {
                pool: scratch.pool().clone(),
                calls: Arc::clone(&calls),
            }) as Arc<dyn Phase>,
            Arc::new(UnexpectedPhase::new(PhaseName::Interpret)),
            Arc::new(UnexpectedPhase::new(PhaseName::Project)),
            Arc::new(UnexpectedPhase::new(PhaseName::Verify)),
            Arc::new(UnexpectedPhase::new(PhaseName::Live)),
        ])?;
        let runner = PhaseRunner::new(
            scratch.runner(),
            phases,
            CapacityGuard::system(CapacityConfig::default()),
            format!("fresh-source-{kind}"),
            test_timing(),
        )?;
        let chain = ChainConfig::new(
            chain_id,
            vec![SourceConfig::new(
                chain_id,
                "intake",
                kind,
                SeedBasis::EthereumHead,
                0,
                "https://unused.invalid",
            )?],
            true,
        )?;
        let error = runner
            .run_chain(&chain, CancellationToken::new())
            .await
            .expect_err("the observing phase stops after checking its pre-write identity");
        assert!(
            error.to_string().contains("fresh identity observed"),
            "{error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let identity: (String, String, i64, i64, Option<i64>) = sqlx::query_as(
            "SELECT source_kind, seed_basis, start_block_number,
                    next_block_number, last_processed_block_number
             FROM ingest_cursors WHERE chain_id = $1 AND source_key = 'intake'",
        )
        .bind(chain_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(
            identity,
            (kind.to_owned(), "ethereum_head".to_owned(), 0, 0, None)
        );
        let raw_fact_count: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM raw_logs WHERE chain_id = $1)
                  + (SELECT count(*) FROM raw_transactions WHERE chain_id = $1)",
        )
        .bind(chain_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(raw_fact_count, 0);
        drop(runner);
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn fresh_multi_source_initialization_recovers_from_one_empty_persisted_cursor() -> Result<()>
{
    let scratch =
        ScratchDatabase::create("production_ingest_partial_source_initialization").await?;
    let chain_id = "partial-source-initialization-chain";
    let sources = vec![
        SourceConfig::new(
            chain_id,
            "first",
            "rpc",
            SeedBasis::BaseSeam,
            0,
            "https://first.invalid",
        )?,
        SourceConfig::new(
            chain_id,
            "second",
            "rpc",
            SeedBasis::BaseSeam,
            0,
            "https://second.invalid",
        )?,
    ];
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain_id).await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number
         ) VALUES ($1, 'first', 'rpc', 'base_seam', 0, 0)",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    store.ensure_ingest_sources(chain_id, &sources).await?;
    let persisted: Vec<String> = sqlx::query_scalar(
        "SELECT source_key FROM ingest_cursors WHERE chain_id = $1 ORDER BY source_key",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;

    scratch.cleanup().await?;
    assert_eq!(persisted, vec!["first".to_owned(), "second".to_owned()]);
    Ok(())
}

#[tokio::test]
async fn fresh_sepolia_rejects_invalid_intake_shape_before_raw_facts_are_written() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_fresh_sepolia_rpc").await?;
    seed_watch_set(scratch.pool(), SEPOLIA).await?;
    let (rpc_endpoint, rpc_server, rpc_requests) = spawn_crash_window_rpc(false).await?;
    let runner = crash_window_runner(&scratch, Arc::new(AtomicUsize::new(0)))?;

    let result = runner
        .run_chain(
            &sepolia_ingest_chain("rpc", &rpc_endpoint)?,
            CancellationToken::new(),
        )
        .await;
    let raw_log_indexes = stored_log_indexes(scratch.pool()).await?;
    let observed_rpc_requests = rpc_requests.load(Ordering::SeqCst);

    drop(runner);
    rpc_server.abort();
    scratch.cleanup().await?;
    assert!(
        raw_log_indexes.is_empty(),
        "invalid Sepolia intake persisted raw logs: {raw_log_indexes:?}"
    );
    let error = result.expect_err("invalid Sepolia intake must fail before Ingest");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(
        error.to_string(),
        "chain ethereum-sepolia intake descriptors [ethereum-sepolia:intake] violate the required \
         shape: exactly one dRPC intake-capable source with ethereum_head seed basis and start \
         block 0"
    );
    assert_eq!(observed_rpc_requests, 0);
    Ok(())
}

#[tokio::test]
async fn fresh_sepolia_rejects_equal_role_endpoints_before_cursor_or_raw_fact_writes() -> Result<()>
{
    let scratch =
        ScratchDatabase::create("production_ingest_fresh_sepolia_equal_endpoints").await?;
    seed_watch_set(scratch.pool(), SEPOLIA).await?;
    let (endpoint, rpc_server, rpc_requests) = spawn_crash_window_rpc(false).await?;
    let endpoint_alias = endpoint.trim_end_matches('/').to_owned();
    let runner = crash_window_runner(&scratch, Arc::new(AtomicUsize::new(0)))?;
    let chain = ChainConfig::new(
        SEPOLIA,
        vec![
            SourceConfig::new_with_role(
                SEPOLIA,
                "sepolia-intake",
                "drpc",
                SeedBasis::EthereumHead,
                0,
                SourceRole::Intake,
                endpoint.clone(),
            )?,
            SourceConfig::new_with_role(
                SEPOLIA,
                "sepolia-verify",
                "drpc",
                SeedBasis::EthereumHead,
                0,
                SourceRole::VerificationOnly,
                endpoint_alias.clone(),
            )?,
        ],
        true,
    )?;

    let error = runner
        .run_chain(&chain, CancellationToken::new())
        .await
        .expect_err("equal intake and verification endpoints must fail before Ingest");
    let cursor_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_cursors WHERE chain_id = $1")
            .bind(SEPOLIA)
            .fetch_one(scratch.pool())
            .await?;
    let raw_fact_count: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM raw_logs WHERE chain_id = $1)
              + (SELECT count(*) FROM raw_transactions WHERE chain_id = $1)
              + (SELECT count(*) FROM raw_receipts WHERE chain_id = $1)",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    let observed_rpc_requests = rpc_requests.load(Ordering::SeqCst);

    drop(runner);
    rpc_server.abort();
    scratch.cleanup().await?;
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("sepolia-intake"), "{error}");
    assert!(error.to_string().contains("sepolia-verify"), "{error}");
    assert!(!error.to_string().contains(&endpoint), "{error}");
    assert!(!error.to_string().contains(&endpoint_alias), "{error}");
    assert_eq!(cursor_count, 0);
    assert_eq!(raw_fact_count, 0);
    assert_eq!(observed_rpc_requests, 0);
    Ok(())
}

#[tokio::test]
async fn cold_catch_up_fetches_events_after_registry_announcement() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_registry_announcement").await?;
    let chain_id = "rpc-registry-announcement-test";
    seed_announcement_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_announcement_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        true,
    )?;
    let database = scratch.runner();
    let phases = PhaseSet::new([
        Arc::new(IngestPhase::new(database.pool().clone())),
        Arc::new(InterpretPhase::new(database.pool().clone())),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    ])?;
    let runner = PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "registry-announcement-catch-up-test",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, task_cancellation).await });

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status: Option<String> = sqlx::query_scalar(
                "
                SELECT phase_status
                FROM chain_phase_state
                WHERE chain_id = $1
                  AND phase_name = 'interpret'
                ",
            )
            .bind(chain_id)
            .fetch_optional(scratch.pool())
            .await?;
            if status.as_deref() == Some("completed") {
                return Ok::<_, anyhow::Error>(());
            }
            if task.is_finished() {
                anyhow::bail!("phase runner exited before registry interpretation completed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("registry announcement catch-up did not complete")??;
    cancellation.cancel();
    task.await??;
    server.abort();

    let raw_logs: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, emitting_address
        FROM raw_logs
        WHERE chain_id = $1
        ORDER BY block_number, log_index
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        raw_logs,
        [
            (1, ANNOUNCED_REGISTRY.to_owned()),
            (2, ANNOUNCED_REGISTRY.to_owned()),
        ],
        "cold intake must retain the announced registry's later event"
    );
    let event_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(event_count, 1);
    let announcement: (String, Uuid, Uuid, i64) = sqlx::query_as(
        "
        SELECT edge_kind,
               from_contract_instance_id,
               to_contract_instance_id,
               active_from_block_number
        FROM discovery_edges
        WHERE chain_id = $1
          AND edge_kind = 'registry_announcement'
          AND deactivated_at IS NULL
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(announcement.0, "registry_announcement");
    assert_eq!(announcement.1, announcement.2);
    assert_eq!(announcement.3, 1);
    let ingest_redo: (i64, bool) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        ingest_redo,
        (0, false),
        "the native same-window announcement supplement must not enqueue historical repair"
    );

    let registered_topic = format!("{:#x}", LabelRegistered::SIGNATURE_HASH);
    let watch = bigname_ingest::load_watch_filter(scratch.pool(), chain_id, 0, 5).await?;
    assert!(!watch.includes(ANNOUNCED_REGISTRY, &registered_topic, 0));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &registered_topic, 1));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &registered_topic, 5));
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_rejects_a_provider_without_checkpoint_heads() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_missing_checkpoints").await?;
    let chain_id = "rpc-missing-checkpoints-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(false, false).await?;
    let outcome = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            sources: vec![SourceDescriptor {
                key: "rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint,
            }],
            cursors: Vec::new(),
            redo_range: None,
            resume_current: None,
        })
        .await;
    server.abort();

    let error = outcome.expect_err("ingest must require safe and finalized checkpoints");
    assert_eq!(error.kind(), IngestErrorKind::DataIntegrity);
    assert!(error.to_string().contains("checkpoint"));
    scratch.cleanup().await
}

#[tokio::test]
async fn block_hash_pinned_log_mismatch_is_terminal_data_integrity() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_block_hash_log_mismatch").await?;
    let chain_id = "rpc-block-hash-log-mismatch-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(true, true).await?;
    let outcome = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            sources: vec![SourceDescriptor {
                key: "rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint,
            }],
            cursors: Vec::new(),
            redo_range: None,
            resume_current: None,
        })
        .await;
    server.abort();

    let error = outcome.expect_err("blockHash-pinned log mismatch must fail ingest");
    assert_eq!(error.kind(), IngestErrorKind::DataIntegrity);
    assert!(error.to_string().contains("outside blockHash-pinned block"));
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_retries_a_fork_straddled_resolved_window() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_fork_straddled_window").await?;
    let chain_id = "rpc-ingest-fork-straddled-window";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, rpc_state, server) = spawn_fork_straddle_rpc().await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let request = || BatchRequest {
        chain_id: chain_id.to_owned(),
        sources: vec![SourceDescriptor {
            key: "rpc".to_owned(),
            kind: "rpc".to_owned(),
            start_block: 0,
            endpoint: endpoint.clone(),
        }],
        cursors: Vec::new(),
        redo_range: None,
        resume_current: None,
    };

    rpc_state.script_straddled_attempts(1);
    let error = engine
        .run_batch(request())
        .await
        .expect_err("a fork-straddled resolved window must be retried");
    assert_eq!(error.kind(), IngestErrorKind::Transient);
    assert!(error.to_string().contains("between blocks 0 and 1"));

    rpc_state.script_straddled_attempts(3);
    let persistent = tokio::time::timeout(Duration::from_secs(3), async {
        let mut last_error = None;
        for _ in 0..3 {
            last_error = Some(
                engine
                    .run_batch(request())
                    .await
                    .expect_err("persistent fork churn must keep surfacing"),
            );
        }
        last_error.expect("bounded persistent retry produced an error")
    })
    .await
    .context("persistent fork-straddle retries must stay bounded by the caller")?;
    assert_eq!(persistent.kind(), IngestErrorKind::Transient);
    assert!(persistent.to_string().contains("between blocks 0 and 1"));

    rpc_state.script_straddled_attempts(1);
    let chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let runner = production_ingest_runner_with_phase(
        scratch.runner(),
        "fork-straddle-retry-runner",
        Arc::new(IngestPhase::with_engine(engine)),
    )?;
    run_until_ingest_handoff(runner, chain, scratch.pool(), BLOCK_1).await?;
    assert!(rpc_state.window_resolves() >= 12);

    server.abort();
    scratch.cleanup().await
}

struct CrashAfterCommitIngest {
    inner: IngestPhase,
}

impl Phase for CrashAfterCommitIngest {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let _committed = self.inner.run_batch(context).await?;
            Err(RunnerError::data_integrity(
                "fixture crash after raw-fact commit and before cursor persistence",
            ))
        })
    }
}

struct ObserveFreshIdentityPhase {
    pool: sqlx::PgPool,
    calls: Arc<AtomicUsize>,
}

impl Phase for ObserveFreshIdentityPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let source = &context.sources[0];
            let identity: Option<(String, String, i64, i64, Option<i64>)> = sqlx::query_as(
                "SELECT source_kind, seed_basis, start_block_number,
                        next_block_number, last_processed_block_number
                 FROM ingest_cursors WHERE chain_id = $1 AND source_key = $2",
            )
            .bind(&source.chain_id)
            .bind(&source.source_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| {
                RunnerError::data_integrity(format!(
                    "fresh identity observation failed to query its cursor: {error}"
                ))
            })?;
            if identity
                != Some((
                    source.source_kind.clone(),
                    source.seed_basis.as_str().to_owned(),
                    source.start_block_number,
                    source.start_block_number,
                    None,
                ))
            {
                return Err(RunnerError::data_integrity(format!(
                    "fresh source identity was not persisted before Ingest: {identity:?}"
                )));
            }
            Err(RunnerError::data_integrity("fresh identity observed"))
        })
    }
}

struct UnexpectedPhase {
    name: PhaseName,
}

impl UnexpectedPhase {
    const fn new(name: PhaseName) -> Self {
        Self { name }
    }
}

impl Phase for UnexpectedPhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            Err(RunnerError::data_integrity(format!(
                "phase {} unexpectedly ran",
                self.name
            )))
        })
    }
}

struct CountingLivePhase {
    calls: Arc<AtomicUsize>,
}

impl Phase for CountingLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        })
    }
}

struct UnexpectedReferences;

impl VerificationReferenceProvider for UnexpectedReferences {
    fn preflight(&self, _source: &VerificationSource) -> RunnerResult<()> {
        Err(RunnerError::data_integrity(
            "provider-trusted verification selected a reference during preflight",
        ))
    }

    fn fetch<'a>(
        &'a self,
        _source: &'a VerificationSource,
        _filter: WatchFilter,
        _from_block: i64,
        _to_block: i64,
    ) -> VerificationReferenceFuture<'a> {
        Box::pin(async {
            Err::<VerificationBatch, _>(RunnerError::data_integrity(
                "provider-trusted verification fetched an independent reference",
            ))
        })
    }
}

#[derive(Default)]
struct AttestationReferences {
    calls: AtomicUsize,
}
impl VerificationReferenceProvider for AttestationReferences {
    fn preflight(&self, source: &VerificationSource) -> RunnerResult<()> {
        if source.provider_kind() == VerificationProviderKind::IndependentRpc
            && source.verification_level() == VerificationLevel::CrossChecked
        {
            return Ok(());
        }
        Err(RunnerError::data_integrity(
            "attestation fixture requires independent cross-check verification",
        ))
    }

    fn fetch<'a>(
        &'a self,
        _source: &'a VerificationSource,
        _filter: WatchFilter,
        from_block: i64,
        to_block: i64,
    ) -> VerificationReferenceFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let tip = BASE_COINBASE_SEAM_BLOCK;
            let logs = (from_block..=to_block)
                .contains(&tip)
                .then(|| verify_attestation_log(false))
                .into_iter()
                .collect();
            Ok(VerificationBatch {
                end: VerificationMarker {
                    number: to_block,
                    hash: verify_attestation_hash(to_block),
                },
                logs,
                rpc_request_count: 1,
            })
        })
    }
}
struct RawFactChangeIngestPhase {
    pool: sqlx::PgPool,
}
impl Phase for RawFactChangeIngestPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let marker = context
                .available_heads
                .as_ref()
                .map(|heads| heads.latest.clone())
                .ok_or_else(|| RunnerError::data_integrity("raw reload has no readable head"))?;
            assert!(
                sqlx::query_scalar::<_, bool>("SELECT redo_in_progress FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'")
                    .bind(&context.chain_id)
                    .fetch_one(&self.pool)
                    .await
                    .expect("Verify state must remain readable during Ingest redo"),
                "Verify must be demoted before an Ingest redo can change raw facts"
            );
            sqlx::query(
                "INSERT INTO raw_transactions (
                     chain_id, block_hash, block_number, transaction_hash,
                     transaction_index, from_address
                 ) VALUES ($1, $2, $3, $4, 1, $5)
                 ON CONFLICT DO NOTHING",
            )
            .bind(&context.chain_id)
            .bind(&marker.hash)
            .bind(marker.number)
            .bind(verify_attestation_transaction(true))
            .bind(SENDER)
            .execute(&self.pool)
            .await
            .map_err(|error| {
                RunnerError::data_integrity(format!("failed to load raw transaction: {error}"))
            })?;
            sqlx::query(
                "INSERT INTO raw_logs (
                     chain_id, block_hash, block_number, transaction_hash,
                     transaction_index, log_index, emitting_address, topics, data
                 ) VALUES ($1, $2, $3, $4, 1, 1, $5, $6, $7)
                 ON CONFLICT DO NOTHING",
            )
            .bind(&context.chain_id)
            .bind(&marker.hash)
            .bind(marker.number)
            .bind(verify_attestation_transaction(true))
            .bind(SIBLING_CONTRACT)
            .bind(vec![TRANSFER_TOPIC.to_owned()])
            .bind(vec![1_u8])
            .execute(&self.pool)
            .await
            .map_err(|error| {
                RunnerError::data_integrity(format!("failed to load raw log: {error}"))
            })?;
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker.clone()),
                live_handoff: Some(marker.clone()),
                source_progress: context
                    .sources
                    .iter()
                    .map(|source| SourceProgress {
                        source_key: source.source_key.clone(),
                        current: Some(marker.clone()),
                        target: Some(marker.clone()),
                        redo_loaded_boundary: Some(marker.clone()),
                    })
                    .collect(),
                ..PhaseProgress::default()
            }))
        })
    }
}

fn crash_window_runner(
    scratch: &ScratchDatabase,
    live_calls: Arc<AtomicUsize>,
) -> Result<PhaseRunner> {
    let phases = PhaseSet::new([
        Arc::new(CrashAfterCommitIngest {
            inner: IngestPhase::new(scratch.pool().clone()),
        }) as Arc<dyn Phase>,
        Arc::new(UnexpectedPhase::new(PhaseName::Interpret)),
        Arc::new(UnexpectedPhase::new(PhaseName::Project)),
        Arc::new(UnexpectedPhase::new(PhaseName::Verify)),
        Arc::new(CountingLivePhase { calls: live_calls }),
    ])?;
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-ingest-crash-window",
        test_timing(),
    )?)
}

fn completed_ingest_skip_runner(
    scratch: &ScratchDatabase,
    live_calls: Arc<AtomicUsize>,
) -> Result<PhaseRunner> {
    let phases = PhaseSet::new([
        Arc::new(UnexpectedPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(CountingLivePhase { calls: live_calls }),
    ])?;
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-ingest-completed-skip",
        test_timing(),
    )?)
}

async fn seed_completed_ingest(scratch: &ScratchDatabase, source: SourceConfig) -> Result<()> {
    seed_completed_ingest_sources(scratch, std::slice::from_ref(&source)).await
}

async fn seed_completed_ingest_sources(
    scratch: &ScratchDatabase,
    sources: &[SourceConfig],
) -> Result<()> {
    let store = PhaseStore::new(scratch.pool().clone());
    let chain_id = &sources[0].chain_id;
    store.initialize_chain(chain_id).await?;
    store.ensure_ingest_sources(chain_id, sources).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = 0,
             current_block_hash = '0xcompleted-ingest', target_block_number = 0,
             target_block_hash = '0xcompleted-ingest',
             live_handoff_block_number = 0,
             live_handoff_block_hash = '0xcompleted-ingest',
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    Ok(())
}

async fn complete_ingest_runner(
    scratch: &ScratchDatabase,
    live_calls: Arc<AtomicUsize>,
) -> Result<PhaseRunner> {
    let phases = PhaseSet::new([
        Arc::new(IngestPhase::new(scratch.pool().clone())) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            scratch.verification_database(2).await?,
            Arc::new(UnexpectedReferences),
        )),
        Arc::new(CountingLivePhase { calls: live_calls }),
    ])?;
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-ingest-crash-window-restart",
        test_timing(),
    )?)
}

fn sepolia_ingest_chain(source_kind: &str, endpoint: &str) -> RunnerResult<ChainConfig> {
    sepolia_ingest_chain_with("intake", source_kind, SeedBasis::EthereumHead, 0, endpoint)
}

fn sepolia_ingest_chain_with(
    source_key: &str,
    source_kind: &str,
    seed_basis: SeedBasis,
    start_block: i64,
    endpoint: &str,
) -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        SEPOLIA,
        vec![SourceConfig::new(
            SEPOLIA,
            source_key,
            source_kind,
            seed_basis,
            start_block,
            endpoint,
        )?],
        true,
    )
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(2),
    }
}

type IngestIdentity = (
    String,
    String,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

async fn ingest_identity(pool: &sqlx::PgPool) -> Result<Option<IngestIdentity>> {
    Ok(sqlx::query_as(
        "SELECT source_kind, seed_basis, start_block_number, next_block_number,
                target_block_number, last_processed_block_number,
                last_processed_block_hash
         FROM ingest_cursors WHERE chain_id = $1 AND source_key = 'intake'",
    )
    .bind(SEPOLIA)
    .fetch_optional(pool)
    .await?)
}

async fn stored_log_indexes(pool: &sqlx::PgPool) -> Result<Vec<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT log_index FROM raw_logs
         WHERE chain_id = $1 ORDER BY block_number, log_index",
    )
    .bind(SEPOLIA)
    .fetch_all(pool)
    .await?)
}

async fn seed_watch_set(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    let contract_id = Uuid::new_v4();
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        ",
    )
    .bind(contract_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "test",
        "source_family": "test_events",
        "chain": chain_id,
        "deployment_epoch": "test",
        "rollout_status": "active",
        "normalizer_version": "test",
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": {
            "events": [{
                "name": "Transfer",
                "fragment": "event Transfer(address indexed from,address indexed to,uint256 value)",
                "emitter_roles": [],
                "normalized_events": []
            }]
        }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain_id,
            deployment_label,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES (1, 'test', 'test_events', $1, 'test', 'active', 'test', $2, $3::jsonb)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(format!("tests/{chain_id}.toml"))
    .bind(payload.to_string())
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id,
            chain_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, $2, 'contract', 'test', $3, $4, 'test', 'none')
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(contract_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn widen_watch_set(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    let contract_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind, provenance
         ) VALUES ($1, $2, 'contract', '{}'::jsonb)",
    )
    .bind(contract_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "test",
        "source_family": "widened_test_events",
        "chain": chain_id,
        "deployment_epoch": "test",
        "rollout_status": "active",
        "normalizer_version": "test",
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": {
            "events": [{
                "name": "Transfer",
                "fragment": "event Transfer(address indexed from,address indexed to,uint256 value)",
                "emitter_roles": [],
                "normalized_events": []
            }]
        }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'test', 'widened_test_events', $1, 'test', 'active', 'test', $2, $3::jsonb)
         RETURNING manifest_id",
    )
    .bind(chain_id)
    .bind(format!("tests/{chain_id}-widened.toml"))
    .bind(payload.to_string())
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind
         ) VALUES ($1, $2, 'contract', 'widened', $3, $4, 'widened', 'none')",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_id)
    .bind(SIBLING_CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(contract_id)
    .bind(chain_id)
    .bind(SIBLING_CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_announcement_watch_set(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    let anchor_id = Uuid::new_v4();
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        ",
    )
    .bind(anchor_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [{
            "edge_kind": "registry_announcement",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [
            {
                "name": "RegistryCreated",
                "fragment": "event RegistryCreated()",
                "emitter_roles": [],
                "normalized_events": ["RegistryCreated"]
            },
            {
                "name": "LabelRegistered",
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["RegistrationGranted"]
            }
        ], "calls": [] }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version,
            file_path, manifest_payload
        )
        VALUES (1, 'ens', 'ens_v2_registry_l1', $1, 'fixture',
                'active', $2, $3, $4)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(NORMALIZER)
    .bind(format!("tests/{chain_id}.toml"))
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4,
                'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(anchor_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(anchor_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'registry_announcement', 'registry', 'reachable_from_root')
        ",
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn spawn_announcement_rpc() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(announcement_rpc)))
            .await
            .expect("announcement test RPC server");
    });
    Ok((format!("http://{address}/"), server))
}

async fn announcement_rpc(Json(request): Json<Value>) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(announcement_rpc_response)
                .collect::<Vec<_>>(),
        ));
    }
    Json(announcement_rpc_response(&request))
}

fn announcement_rpc_response(request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match method {
        "eth_getBlockByNumber" => {
            let selection = params.first().and_then(Value::as_str).unwrap_or_default();
            match selection {
                "latest" | "safe" | "finalized" | "0x2" => Some(announcement_block(
                    2,
                    params.get(1) == Some(&Value::Bool(true)),
                )),
                "0x1" => Some(announcement_block(
                    1,
                    params.get(1) == Some(&Value::Bool(true)),
                )),
                "0x0" => Some(announcement_block(
                    0,
                    params.get(1) == Some(&Value::Bool(true)),
                )),
                _ => None,
            }
        }
        "eth_getBlockByHash" => {
            let full = params.get(1) == Some(&Value::Bool(true));
            match params.first().and_then(Value::as_str).unwrap_or_default() {
                BLOCK_0 => Some(announcement_block(0, full)),
                BLOCK_1 => Some(announcement_block(1, full)),
                BLOCK_2 => Some(announcement_block(2, full)),
                _ => None,
            }
        }
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            match filter.get("blockHash").and_then(Value::as_str) {
                Some(BLOCK_0) => Some(json!([])),
                Some(BLOCK_1) => Some(json!([announcement_log()])),
                Some(BLOCK_2) => Some(json!([registration_log()])),
                Some(_) => None,
                None => Some(Value::Array(announcement_range_logs(&filter))),
            }
        }
        "eth_getBlockReceipts" => {
            match params.first().and_then(Value::as_str).unwrap_or_default() {
                BLOCK_0 => Some(json!([])),
                BLOCK_1 => Some(json!([announcement_receipt(1, ANNOUNCEMENT_TRANSACTION)])),
                BLOCK_2 => Some(json!([announcement_receipt(2, REGISTRATION_TRANSACTION)])),
                _ => None,
            }
        }
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn announcement_block(number: i64, full_transactions: bool) -> Value {
    let (hash, parent_hash, transaction_hash) = match number {
        0 => (
            BLOCK_0,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            None,
        ),
        1 => (BLOCK_1, BLOCK_0, Some(ANNOUNCEMENT_TRANSACTION)),
        _ => (BLOCK_2, BLOCK_1, Some(REGISTRATION_TRANSACTION)),
    };
    let transactions = transaction_hash.map_or_else(
        || json!([]),
        |transaction_hash| {
            if full_transactions {
                json!([{
                    "hash": transaction_hash,
                    "blockHash": hash,
                    "blockNumber": format!("0x{number:x}"),
                    "transactionIndex": "0x0",
                    "from": SENDER,
                    "to": ANNOUNCED_REGISTRY,
                    "input": "0x",
                    "value": "0x0"
                }])
            } else {
                json!([transaction_hash])
            }
        },
    );
    json!({
        "hash": hash,
        "parentHash": parent_hash,
        "number": format!("0x{number:x}"),
        "timestamp": format!("0x{:x}", number + 200),
        "logsBloom": "0x",
        "transactions": transactions
    })
}

fn announcement_range_logs(filter: &Value) -> Vec<Value> {
    let from = rpc_quantity(filter.get("fromBlock")).unwrap_or_default();
    let to = rpc_quantity(filter.get("toBlock")).unwrap_or(i64::MAX);
    let addresses = filter
        .get("address")
        .map(string_filter_values)
        .unwrap_or_default();
    let topics = filter
        .pointer("/topics/0")
        .map(string_filter_values)
        .unwrap_or_default();
    [announcement_log(), registration_log()]
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

fn announcement_log() -> Value {
    encoded_rpc_log(
        RegistryCreated {}.encode_log_data(),
        1,
        BLOCK_1,
        ANNOUNCEMENT_TRANSACTION,
    )
}

fn registration_log() -> Value {
    encoded_rpc_log(
        LabelRegistered {
            tokenId: U256::from(1),
            labelHash: keccak256(b"alice"),
            label: "alice".to_owned(),
            owner: SENDER.parse::<Address>().expect("valid fixture owner"),
            expiry: 10_000,
            sender: SENDER.parse::<Address>().expect("valid fixture sender"),
        }
        .encode_log_data(),
        2,
        BLOCK_2,
        REGISTRATION_TRANSACTION,
    )
}

fn encoded_rpc_log(
    encoded: alloy_primitives::LogData,
    block_number: i64,
    block_hash: &str,
    transaction_hash: &str,
) -> Value {
    json!({
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": ANNOUNCED_REGISTRY,
        "topics": encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>(),
        "data": format!("0x{}", alloy_primitives::hex::encode(encoded.data))
    })
}

fn announcement_receipt(block_number: i64, transaction_hash: &str) -> Value {
    let block_hash = if block_number == 1 { BLOCK_1 } else { BLOCK_2 };
    json!({
        "transactionHash": transaction_hash,
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "transactionIndex": "0x0",
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "logsBloom": "0x"
    })
}

#[derive(Clone)]
struct CrashWindowRpcState {
    omit_second_log: bool,
    requests: Arc<AtomicUsize>,
}

async fn spawn_crash_window_rpc(
    omit_second_log: bool,
) -> Result<(String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let requests = Arc::new(AtomicUsize::new(0));
    let state = CrashWindowRpcState {
        omit_second_log,
        requests: Arc::clone(&requests),
    };
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(crash_window_rpc))
                .with_state(state),
        )
        .await
        .expect("crash-window RPC server");
    });
    Ok((format!("http://{address}/"), server, requests))
}

async fn crash_window_rpc(
    State(state): State<CrashWindowRpcState>,
    Json(request): Json<Value>,
) -> Json<Value> {
    state.requests.fetch_add(1, Ordering::SeqCst);
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| crash_window_rpc_response(request, state.omit_second_log))
                .collect(),
        ));
    }
    Json(crash_window_rpc_response(&request, state.omit_second_log))
}

fn crash_window_rpc_response(request: &Value, omit_second_log: bool) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match method {
        "eth_getBlockByNumber" => {
            let selection = params.first().and_then(Value::as_str).unwrap_or_default();
            match selection {
                "latest" | "safe" | "finalized" | "0x1" => {
                    Some(block(1, params.get(1) == Some(&Value::Bool(true))))
                }
                "0x0" => Some(block(0, params.get(1) == Some(&Value::Bool(true)))),
                _ => None,
            }
        }
        "eth_getBlockByHash" => match params.first().and_then(Value::as_str).unwrap_or_default() {
            BLOCK_0 => Some(block(0, params.get(1) == Some(&Value::Bool(true)))),
            BLOCK_1 => Some(block(1, params.get(1) == Some(&Value::Bool(true)))),
            _ => None,
        },
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            match filter.get("blockHash").and_then(Value::as_str) {
                Some(BLOCK_0) => Some(json!([])),
                _ => Some(Value::Array(crash_window_logs(omit_second_log))),
            }
        }
        "eth_getBlockReceipts" => Some(json!([receipt()])),
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn crash_window_logs(omit_second_log: bool) -> Vec<Value> {
    let mut logs = vec![raw_log()];
    if !omit_second_log {
        logs.push(json!({
            "blockHash": BLOCK_1,
            "blockNumber": "0x1",
            "transactionHash": TRANSACTION,
            "transactionIndex": "0x0",
            "logIndex": "0x1",
            "address": CONTRACT,
            "topics": [
                TRANSFER_TOPIC,
                format!("0x{}", "00".repeat(32)),
                format!("0x{}", "00".repeat(32))
            ],
            "data": "0x1234"
        }));
    }
    logs
}

async fn spawn_hash_switchable_rpc() -> Result<(String, Arc<AtomicU8>, tokio::task::JoinHandle<()>)>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let hash_epoch = Arc::new(AtomicU8::new(0));
    let server_state = Arc::clone(&hash_epoch);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(hash_switchable_rpc))
                .with_state(server_state),
        )
        .await
        .expect("hash-switchable test RPC server");
    });
    Ok((format!("http://{address}/"), hash_epoch, server))
}

#[derive(Clone)]
struct WatchPlanBoundaryRpcState {
    hash_epoch: Arc<AtomicU8>,
    boundary_log_calls: Arc<AtomicUsize>,
    scripted_boundary_epochs: Arc<Mutex<VecDeque<u8>>>,
}

impl WatchPlanBoundaryRpcState {
    fn script_boundary_epochs(&self, epochs: impl IntoIterator<Item = u8>) {
        *self
            .scripted_boundary_epochs
            .lock()
            .expect("boundary epoch script lock") = epochs.into_iter().collect();
    }

    fn scripted_epochs_remaining(&self) -> usize {
        self.scripted_boundary_epochs
            .lock()
            .expect("boundary epoch script lock")
            .len()
    }

    fn clear_boundary_script(&self) {
        self.scripted_boundary_epochs
            .lock()
            .expect("boundary epoch script lock")
            .clear();
    }

    fn response_epoch(&self, request: &Value) -> u8 {
        let fallback = self.hash_epoch.load(Ordering::SeqCst);
        let boundary_number = request["method"] == "eth_getBlockByNumber"
            && request.pointer("/params/0").and_then(Value::as_str) == Some("0x1");
        if !boundary_number {
            return fallback;
        }
        let epoch = self
            .scripted_boundary_epochs
            .lock()
            .expect("boundary epoch script lock")
            .pop_front()
            .unwrap_or(fallback);
        self.hash_epoch.store(epoch, Ordering::SeqCst);
        epoch
    }
}

async fn spawn_watch_plan_boundary_rpc() -> Result<(
    String,
    WatchPlanBoundaryRpcState,
    tokio::task::JoinHandle<()>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let state = WatchPlanBoundaryRpcState {
        hash_epoch: Arc::new(AtomicU8::new(0)),
        boundary_log_calls: Arc::new(AtomicUsize::new(0)),
        scripted_boundary_epochs: Arc::new(Mutex::new(VecDeque::new())),
    };
    let server_state = state.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(watch_plan_boundary_rpc))
                .with_state(server_state),
        )
        .await
        .expect("watch-plan boundary RPC server");
    });
    Ok((format!("http://{address}/"), state, server))
}

fn verify_attestation_hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}

fn verify_attestation_transaction(sibling: bool) -> String {
    let offset = i64::from(sibling) + 100;
    format!("0x{:064x}", BASE_COINBASE_SEAM_BLOCK + offset)
}

fn verify_attestation_log(sibling: bool) -> VerificationLog {
    let tip = BASE_COINBASE_SEAM_BLOCK;
    VerificationLog {
        block_hash: verify_attestation_hash(tip),
        block_number: tip,
        transaction_hash: verify_attestation_transaction(sibling),
        transaction_index: i64::from(sibling),
        log_index: i64::from(sibling),
        address: if sibling { SIBLING_CONTRACT } else { CONTRACT }.to_owned(),
        topics: vec![TRANSFER_TOPIC.to_owned()],
        data: Vec::new(),
    }
}

async fn watch_plan_boundary_rpc(
    State(state): State<WatchPlanBoundaryRpcState>,
    Json(request): Json<Value>,
) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| watch_plan_boundary_rpc_response(request, &state))
                .collect(),
        ));
    }
    Json(watch_plan_boundary_rpc_response(&request, &state))
}

fn watch_plan_boundary_rpc_response(request: &Value, state: &WatchPlanBoundaryRpcState) -> Value {
    let hash_epoch = state.response_epoch(request);
    let mut response = hash_switchable_rpc_response(request, hash_epoch);
    let method = request["method"].as_str().unwrap_or_default();
    let epoch_boundary = match hash_epoch {
        0 => BLOCK_1,
        1 => BLOCK_1_REORG,
        2 => BLOCK_1_SECOND_REORG,
        other => panic!("unsupported hash epoch {other}"),
    };
    if matches!(method, "eth_getBlockByNumber" | "eth_getBlockByHash")
        && response["result"]["number"] == "0x1"
    {
        let returned_hash = response["result"]["hash"]
            .as_str()
            .expect("boundary block hash")
            .to_owned();
        let (_, widened_transaction) = watch_plan_transactions(&returned_hash);
        add_widened_transaction(
            &mut response,
            &returned_hash,
            widened_transaction,
            request.pointer("/params/1") == Some(&Value::Bool(true)),
        );
        return response;
    }
    if method == "eth_getBlockReceipts"
        && request.pointer("/params/0").and_then(Value::as_str) != Some(BLOCK_0)
    {
        let requested_hash = request
            .pointer("/params/0")
            .and_then(Value::as_str)
            .unwrap_or(epoch_boundary);
        let (primary_transaction, widened_transaction) = watch_plan_transactions(requested_hash);
        let mut widened_receipt = switchable_receipt(requested_hash, widened_transaction);
        widened_receipt["transactionIndex"] = json!("0x1");
        response["result"] = json!([
            switchable_receipt(requested_hash, primary_transaction),
            widened_receipt,
        ]);
        return response;
    }
    if method != "eth_getLogs" {
        return response;
    }
    let filter = request.pointer("/params/0").cloned().unwrap_or_default();
    let block_hash = filter
        .get("blockHash")
        .and_then(Value::as_str)
        .unwrap_or(epoch_boundary);
    if block_hash == BLOCK_0 {
        return response;
    }
    let selects_boundary = filter.get("blockHash").is_some()
        || (rpc_quantity(filter.get("fromBlock")).unwrap_or_default() <= 1
            && rpc_quantity(filter.get("toBlock")).unwrap_or(i64::MAX) >= 1);
    if !selects_boundary {
        response["result"] = json!([]);
        return response;
    }
    state.boundary_log_calls.fetch_add(1, Ordering::SeqCst);
    let addresses = filter
        .get("address")
        .map(string_filter_values)
        .unwrap_or_default();
    let transaction_hash = match block_hash {
        BLOCK_1 => TRANSACTION,
        BLOCK_1_REORG => REORG_TRANSACTION,
        BLOCK_1_SECOND_REORG => SECOND_REORG_TRANSACTION,
        _ => return response,
    };
    let includes = |address: &str| {
        addresses.is_empty()
            || addresses
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(address))
    };
    let mut logs = Vec::new();
    if includes(CONTRACT) {
        logs.push(switchable_log(block_hash, transaction_hash));
    }
    if includes(SIBLING_CONTRACT) {
        let (_, widened_transaction) = watch_plan_transactions(block_hash);
        logs.push(switchable_widened_log(block_hash, widened_transaction));
    }
    response["result"] = Value::Array(logs);
    response
}

fn watch_plan_transactions(block_hash: &str) -> (&'static str, &'static str) {
    match block_hash {
        BLOCK_1 => (TRANSACTION, WIDENED_TRANSACTION),
        BLOCK_1_REORG => (REORG_TRANSACTION, WIDENED_REORG_TRANSACTION),
        BLOCK_1_SECOND_REORG => (SECOND_REORG_TRANSACTION, WIDENED_SECOND_REORG_TRANSACTION),
        other => panic!("unsupported watch-plan block hash {other}"),
    }
}

fn add_widened_transaction(
    response: &mut Value,
    block_hash: &str,
    transaction_hash: &str,
    full: bool,
) {
    let transaction = if full {
        json!({
            "hash": transaction_hash,
            "blockHash": block_hash,
            "blockNumber": "0x1",
            "transactionIndex": "0x1",
            "from": SENDER,
            "to": SIBLING_CONTRACT,
            "input": "0xbeef",
            "value": "0x0"
        })
    } else {
        json!(transaction_hash)
    };
    response["result"]["transactions"]
        .as_array_mut()
        .expect("boundary block transactions")
        .push(transaction);
}

async fn hash_switchable_rpc(
    State(hash_epoch): State<Arc<AtomicU8>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let hash_epoch = hash_epoch.load(Ordering::SeqCst);
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| hash_switchable_rpc_response(request, hash_epoch))
                .collect::<Vec<_>>(),
        ));
    }
    Json(hash_switchable_rpc_response(&request, hash_epoch))
}

fn hash_switchable_rpc_response(request: &Value, hash_epoch: u8) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let (boundary_hash, transaction_hash) = match hash_epoch {
        0 => (BLOCK_1, TRANSACTION),
        1 => (BLOCK_1_REORG, REORG_TRANSACTION),
        2 => (BLOCK_1_SECOND_REORG, SECOND_REORG_TRANSACTION),
        other => panic!("unsupported hash epoch {other}"),
    };
    let result = match method {
        "eth_getBlockByNumber" => {
            let selection = params.first().and_then(Value::as_str).unwrap_or_default();
            match selection {
                "latest" | "0x1" => Some(switchable_block(
                    1,
                    boundary_hash,
                    transaction_hash,
                    params.get(1) == Some(&Value::Bool(true)),
                )),
                "safe" | "finalized" | "0x0" => {
                    Some(block(0, params.get(1) == Some(&Value::Bool(true))))
                }
                _ => None,
            }
        }
        "eth_getBlockByHash" => {
            let full = params.get(1) == Some(&Value::Bool(true));
            match params.first().and_then(Value::as_str).unwrap_or_default() {
                BLOCK_0 => Some(block(0, full)),
                BLOCK_1 => Some(switchable_block(1, BLOCK_1, TRANSACTION, full)),
                BLOCK_1_REORG => Some(switchable_block(1, BLOCK_1_REORG, REORG_TRANSACTION, full)),
                BLOCK_1_SECOND_REORG => Some(switchable_block(
                    1,
                    BLOCK_1_SECOND_REORG,
                    SECOND_REORG_TRANSACTION,
                    full,
                )),
                _ => None,
            }
        }
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            match filter.get("blockHash").and_then(Value::as_str) {
                Some(BLOCK_0) => Some(json!([])),
                Some(BLOCK_1) => Some(json!([switchable_log(BLOCK_1, TRANSACTION)])),
                Some(BLOCK_1_REORG) => {
                    Some(json!([switchable_log(BLOCK_1_REORG, REORG_TRANSACTION)]))
                }
                Some(BLOCK_1_SECOND_REORG) => Some(json!([switchable_log(
                    BLOCK_1_SECOND_REORG,
                    SECOND_REORG_TRANSACTION
                )])),
                _ => Some(json!([])),
            }
        }
        "eth_getBlockReceipts" => match params.first().and_then(Value::as_str) {
            Some(BLOCK_1) => Some(json!([switchable_receipt(BLOCK_1, TRANSACTION)])),
            Some(BLOCK_1_REORG) => Some(json!([switchable_receipt(
                BLOCK_1_REORG,
                REORG_TRANSACTION
            )])),
            Some(BLOCK_1_SECOND_REORG) => Some(json!([switchable_receipt(
                BLOCK_1_SECOND_REORG,
                SECOND_REORG_TRANSACTION
            )])),
            Some(BLOCK_0) => Some(json!([])),
            _ => None,
        },
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn switchable_block(
    number: i64,
    hash: &str,
    transaction_hash: &str,
    full_transactions: bool,
) -> Value {
    let transactions = if full_transactions {
        json!([{
            "hash": transaction_hash,
            "blockHash": hash,
            "blockNumber": format!("0x{number:x}"),
            "transactionIndex": "0x0",
            "from": SENDER,
            "to": CONTRACT,
            "input": "0xdead",
            "value": "0x7"
        }])
    } else {
        json!([transaction_hash])
    };
    json!({
        "hash": hash,
        "parentHash": BLOCK_0,
        "number": format!("0x{number:x}"),
        "timestamp": format!("0x{:x}", number + 100),
        "logsBloom": "0x",
        "transactions": transactions
    })
}

fn switchable_log(block_hash: &str, transaction_hash: &str) -> Value {
    json!({
        "blockHash": block_hash,
        "blockNumber": "0x1",
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": CONTRACT,
        "topics": [
            TRANSFER_TOPIC,
            format!("0x{}", "00".repeat(32)),
            format!("0x{}", "00".repeat(32))
        ],
        "data": "0x"
    })
}

fn switchable_widened_log(block_hash: &str, transaction_hash: &str) -> Value {
    json!({
        "blockHash": block_hash,
        "blockNumber": "0x1",
        "transactionHash": transaction_hash,
        "transactionIndex": "0x1",
        "logIndex": "0x1",
        "address": SIBLING_CONTRACT,
        "topics": [
            TRANSFER_TOPIC,
            format!("0x{}", "00".repeat(32)),
            format!("0x{}", "00".repeat(32))
        ],
        "data": "0x1234"
    })
}

fn switchable_receipt(block_hash: &str, transaction_hash: &str) -> Value {
    json!({
        "transactionHash": transaction_hash,
        "blockHash": block_hash,
        "blockNumber": "0x1",
        "transactionIndex": "0x0",
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "logsBloom": "0x"
    })
}

async fn spawn_rpc(
    checkpoint_support: bool,
    mismatched_block_hash_log: bool,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(rpc))
                .with_state((checkpoint_support, mismatched_block_hash_log)),
        )
        .await
        .expect("test RPC server");
    });
    Ok((format!("http://{address}/"), server))
}

async fn spawn_approval_rpc() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(approval_rpc)))
            .await
            .expect("approval test RPC server");
    });
    Ok((format!("http://{address}/"), server))
}

async fn approval_rpc(Json(request): Json<Value>) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests.iter().map(approval_rpc_response).collect(),
        ));
    }
    Json(approval_rpc_response(&request))
}

fn approval_rpc_response(request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match method {
        "eth_getBlockByNumber" => {
            let selection = params.first().and_then(Value::as_str).unwrap_or_default();
            match selection {
                "latest" | "safe" | "finalized" | "0x1" => {
                    Some(approval_block(1, params.get(1) == Some(&Value::Bool(true))))
                }
                "0x0" => Some(approval_block(0, params.get(1) == Some(&Value::Bool(true)))),
                _ => None,
            }
        }
        "eth_getBlockByHash" => {
            let hash = params.first().and_then(Value::as_str).unwrap_or_default();
            match hash {
                BLOCK_0 => Some(approval_block(0, params.get(1) == Some(&Value::Bool(true)))),
                BLOCK_1 => Some(approval_block(1, params.get(1) == Some(&Value::Bool(true)))),
                _ => None,
            }
        }
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            if filter.get("address").is_some() && filter.get("fromBlock").is_some() {
                Some(json!([approval_log(0), approval_log(2)]))
            } else {
                match filter.get("blockHash").and_then(Value::as_str) {
                    Some(BLOCK_0) => Some(json!([])),
                    Some(BLOCK_1) => Some(json!([
                        approval_log(0),
                        approval_log(1),
                        approval_log(2),
                        approval_log(3)
                    ])),
                    _ => Some(json!([])),
                }
            }
        }
        "eth_getBlockReceipts" => Some(json!([
            approval_receipt(DECLARED_APPROVAL_TRANSACTION, 0),
            approval_receipt(FOREIGN_APPROVAL_TRANSACTION, 1),
            approval_receipt(CONTEXT_APPROVAL_TRANSACTION, 2)
        ])),
        _ => None,
    };
    json!({"jsonrpc":"2.0", "id":id, "result":result})
}

fn approval_block(number: i64, full_transactions: bool) -> Value {
    let mut value = block(number, false);
    if number == 1 {
        value["transactions"] = if full_transactions {
            Value::Array(
                [
                    DECLARED_APPROVAL_TRANSACTION,
                    FOREIGN_APPROVAL_TRANSACTION,
                    CONTEXT_APPROVAL_TRANSACTION,
                ]
                .into_iter()
                .enumerate()
                .map(|(index, hash)| {
                    json!({
                        "hash":hash,
                        "blockHash":BLOCK_1,
                        "blockNumber":"0x1",
                        "transactionIndex":format!("0x{index:x}"),
                        "from":SENDER,
                        "to":CONTRACT,
                        "input":"0x",
                        "value":"0x0"
                    })
                })
                .collect(),
            )
        } else {
            json!([
                DECLARED_APPROVAL_TRANSACTION,
                FOREIGN_APPROVAL_TRANSACTION,
                CONTEXT_APPROVAL_TRANSACTION
            ])
        };
    }
    value
}

fn approval_topics() -> Vec<String> {
    vec![
        format!("{}", ApprovalForAll::SIGNATURE_HASH),
        format!("0x{}", "00".repeat(12)) + &CONTRACT[2..],
        format!("0x{}", "00".repeat(12)) + &SENDER[2..],
    ]
}

fn approval_data(approved: bool) -> Vec<u8> {
    let mut data = vec![0; 32];
    data[31] = u8::from(approved);
    data
}

fn approval_log(index: i64) -> Value {
    let (transaction_hash, transaction_index, address, approved) = match index {
        0 => (DECLARED_APPROVAL_TRANSACTION, 0, CONTRACT, true),
        1 => (FOREIGN_APPROVAL_TRANSACTION, 1, SIBLING_CONTRACT, false),
        2 => (CONTEXT_APPROVAL_TRANSACTION, 2, CONTRACT, false),
        3 => (CONTEXT_APPROVAL_TRANSACTION, 2, SIBLING_CONTRACT, true),
        _ => unreachable!("approval fixture log index"),
    };
    json!({
        "blockHash":BLOCK_1,
        "blockNumber":"0x1",
        "transactionHash":transaction_hash,
        "transactionIndex":format!("0x{transaction_index:x}"),
        "logIndex":format!("0x{index:x}"),
        "address":address,
        "topics":approval_topics(),
        "data":format!("0x{}", hex::encode(approval_data(approved)))
    })
}

fn approval_receipt(transaction_hash: &str, transaction_index: i64) -> Value {
    json!({
        "transactionHash":transaction_hash,
        "blockHash":BLOCK_1,
        "blockNumber":"0x1",
        "transactionIndex":format!("0x{transaction_index:x}"),
        "status":"0x1",
        "cumulativeGasUsed":"0x5208",
        "gasUsed":"0x5208",
        "logsBloom":"0x"
    })
}

#[derive(Clone)]
struct ForkStraddleRpcState {
    remaining_straddled_resolves: Arc<AtomicUsize>,
    window_resolves: Arc<AtomicUsize>,
}

impl ForkStraddleRpcState {
    fn script_straddled_attempts(&self, attempts: usize) {
        self.remaining_straddled_resolves
            .store(attempts.saturating_mul(2), Ordering::SeqCst);
    }

    fn claim_straddled_resolve(&self) -> bool {
        self.window_resolves.fetch_add(1, Ordering::SeqCst);
        self.remaining_straddled_resolves
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    fn window_resolves(&self) -> usize {
        self.window_resolves.load(Ordering::SeqCst)
    }
}

async fn spawn_fork_straddle_rpc()
-> Result<(String, ForkStraddleRpcState, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let state = ForkStraddleRpcState {
        remaining_straddled_resolves: Arc::new(AtomicUsize::new(0)),
        window_resolves: Arc::new(AtomicUsize::new(0)),
    };
    let server_state = state.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(fork_straddle_rpc))
                .with_state(server_state),
        )
        .await
        .expect("fork-straddle test RPC server");
    });
    Ok((format!("http://{address}/"), state, server))
}

async fn fork_straddle_rpc(
    State(state): State<ForkStraddleRpcState>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let straddled = request.as_array().is_some_and(|requests| {
        let resolves_window = requests.len() == 2
            && requests
                .iter()
                .all(|request| request["method"] == "eth_getBlockByNumber")
            && requests
                .iter()
                .any(|request| request.pointer("/params/0") == Some(&json!("0x0")))
            && requests
                .iter()
                .any(|request| request.pointer("/params/0") == Some(&json!("0x1")));
        resolves_window && state.claim_straddled_resolve()
    });
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| fork_straddle_rpc_response(request, straddled))
                .collect(),
        ));
    }
    Json(fork_straddle_rpc_response(&request, false))
}

fn fork_straddle_rpc_response(request: &Value, straddled: bool) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let selection = request
        .pointer("/params/0")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let full = request.pointer("/params/1") == Some(&Value::Bool(true));
    let result = match method {
        "eth_getBlockByNumber" => match selection {
            "0x0" => Some(block(0, full)),
            "0x1" if straddled => Some(straddled_block_1(full)),
            "latest" | "safe" | "finalized" | "0x1" => Some(block(1, full)),
            _ => None,
        },
        "eth_getBlockByHash" if selection == BLOCK_0 => Some(block(0, full)),
        "eth_getBlockByHash" if selection == BLOCK_1 => Some(block(1, full)),
        "eth_getBlockByHash" if selection == BLOCK_1_REORG => Some(straddled_block_1(full)),
        "eth_getLogs" | "eth_getBlockReceipts" => Some(json!([])),
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn straddled_block_1(full_transactions: bool) -> Value {
    let mut value = block(1, full_transactions);
    value["hash"] = json!(BLOCK_1_REORG);
    value["parentHash"] = json!(FORK_BLOCK_0);
    value["transactions"] = json!([]);
    value
}

async fn rpc(
    State((checkpoint_support, mismatched_block_hash_log)): State<(bool, bool)>,
    Json(request): Json<Value>,
) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| rpc_response(request, checkpoint_support, mismatched_block_hash_log))
                .collect::<Vec<_>>(),
        ));
    }
    Json(rpc_response(
        &request,
        checkpoint_support,
        mismatched_block_hash_log,
    ))
}

fn rpc_response(
    request: &Value,
    checkpoint_support: bool,
    mismatched_block_hash_log: bool,
) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match method {
        "eth_getBlockByNumber" => {
            let selection = params.first().and_then(Value::as_str).unwrap_or_default();
            match selection {
                "latest" | "0x1" => Some(block(1, params.get(1) == Some(&Value::Bool(true)))),
                "0x0" => Some(block(0, params.get(1) == Some(&Value::Bool(true)))),
                "safe" | "finalized" if !checkpoint_support => {
                    return json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32602, "message": "unsupported block tag"}
                    });
                }
                "safe" | "finalized" => Some(block(1, false)),
                _ => None,
            }
        }
        "eth_getBlockByHash" => {
            let hash = params.first().and_then(Value::as_str).unwrap_or_default();
            match hash {
                BLOCK_0 => Some(block(0, params.get(1) == Some(&Value::Bool(true)))),
                BLOCK_1 => Some(block(1, params.get(1) == Some(&Value::Bool(true)))),
                _ => None,
            }
        }
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            match filter.get("blockHash").and_then(Value::as_str) {
                Some(BLOCK_0) => Some(json!([])),
                Some(BLOCK_1) if mismatched_block_hash_log => {
                    Some(json!([block_hash_mismatched_log()]))
                }
                Some(BLOCK_1) => Some(json!([raw_log(), sibling_log()])),
                _ => Some(json!([raw_log()])),
            }
        }
        "eth_getBlockReceipts" => Some(json!([receipt()])),
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn block(number: i64, full_transactions: bool) -> Value {
    let (hash, parent_hash, transactions) = if number == 0 {
        (
            BLOCK_0,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            json!([]),
        )
    } else {
        (
            BLOCK_1,
            BLOCK_0,
            if full_transactions {
                json!([{
                    "hash": TRANSACTION,
                    "blockHash": BLOCK_1,
                    "blockNumber": "0x1",
                    "transactionIndex": "0x0",
                    "from": SENDER,
                    "to": CONTRACT,
                    "input": "0xdead",
                    "value": "0x7"
                }])
            } else {
                json!([TRANSACTION])
            },
        )
    };
    json!({
        "hash": hash,
        "parentHash": parent_hash,
        "number": format!("0x{number:x}"),
        "timestamp": format!("0x{:x}", number + 100),
        "logsBloom": "0x",
        "transactions": transactions
    })
}

fn raw_log() -> Value {
    json!({
        "blockHash": BLOCK_1,
        "blockNumber": "0x1",
        "transactionHash": TRANSACTION,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": CONTRACT,
        "topics": [
            TRANSFER_TOPIC,
            format!("0x{}", "00".repeat(32)),
            format!("0x{}", "00".repeat(32))
        ],
        "data": "0x"
    })
}

fn block_hash_mismatched_log() -> Value {
    let mut log = raw_log();
    log["blockHash"] = json!(BLOCK_0);
    log
}

fn sibling_log() -> Value {
    json!({
        "blockHash": BLOCK_1,
        "blockNumber": "0x1",
        "transactionHash": TRANSACTION,
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "address": SIBLING_CONTRACT,
        "topics": [SIBLING_TOPIC],
        "data": "0x1234"
    })
}

fn receipt() -> Value {
    json!({
        "transactionHash": TRANSACTION,
        "blockHash": BLOCK_1,
        "blockNumber": "0x1",
        "transactionIndex": "0x0",
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "logsBloom": "0x"
    })
}
