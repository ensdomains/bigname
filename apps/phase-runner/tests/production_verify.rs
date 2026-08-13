#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::keccak256;
use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use bigname_ingest::{
    BASE_COINBASE_SEAM_BLOCK, VerificationBatch, VerificationLog, VerificationMarker,
    VerificationProvider, VerificationProviderKind, WatchFilter,
};
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    database::VerificationDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers},
    phase::{
        BlockRange, IngestCursor, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext,
        PhaseFuture, PhaseName, PhaseProgress, PhaseSet, SourceProgress, VerificationLevel,
    },
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
    verify_phase::{
        VerificationReferenceFuture, VerificationReferenceProvider, VerificationSource, VerifyPhase,
    },
};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::{net::TcpListener, sync::Notify};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use support::ScratchDatabase;

const BASE: &str = "base-mainnet";
const ETHEREUM: &str = "ethereum-mainnet";
const SEPOLIA: &str = "ethereum-sepolia";
const CONTRACT: &str = "0x00000000000000000000000000000000000000aa";
const MULTI_BATCH_VERIFY_TARGET: i64 = 131_073;
const FIRST_VERIFY_BATCH_END: i64 = 131_071;

#[tokio::test]
async fn verifier_rejects_a_database_role_with_write_privileges() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_write_role").await?;
    let writer_database = scratch.runner();
    let result = VerificationDatabase::connect_with_options(
        scratch.writer_connect_options(),
        &writer_database,
        1,
    )
    .await;
    let rejected = result
        .as_ref()
        .is_err_and(|error| error.kind() == ErrorKind::Configuration);
    drop(result);
    assert!(
        rejected,
        "verification must reject a role that can write application tables"
    );

    let assumed_role_result = VerificationDatabase::connect_with_options(
        scratch.writer_assuming_verification_role_options().await?,
        &writer_database,
        1,
    )
    .await;
    assert!(
        assumed_role_result
            .as_ref()
            .is_err_and(|error| error.kind() == ErrorKind::Configuration),
        "verification must reject a privileged session user that assumes the reader role"
    );
    drop(assumed_role_result);

    let reader_options = scratch.verification_connect_options().await?;
    let verification_database =
        VerificationDatabase::connect_with_options(reader_options.clone(), &writer_database, 1)
            .await?;
    let reader_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(reader_options)
        .await?;
    let system_identifier: String =
        sqlx::query_scalar("SELECT system_identifier::text FROM pg_control_system()")
            .fetch_one(&reader_pool)
            .await?;
    assert!(!system_identifier.is_empty());
    let write_error = sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET updated_at = updated_at
         WHERE false",
    )
    .execute(&reader_pool)
    .await
    .expect_err("the verification role must not be able to update phase state");
    let write_error_code = write_error
        .as_database_error()
        .and_then(|error| error.code())
        .expect("permission failure must have a PostgreSQL error code");
    assert_eq!(write_error_code.as_ref(), "42501");
    reader_pool.close().await;
    drop(verification_database);
    scratch.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn verifier_rejects_a_reader_connected_to_a_different_database() -> Result<()> {
    let writer = ScratchDatabase::create("production_verify_writer_identity").await?;
    let other = ScratchDatabase::create("production_verify_other_identity").await?;
    let writer_database = writer.runner();
    let result = VerificationDatabase::connect_with_options(
        other.verification_connect_options().await?,
        &writer_database,
        2,
    )
    .await;
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.kind() == ErrorKind::Configuration),
        "verification must reject a reader for a different cluster/database identity"
    );

    drop(result);
    writer.cleanup().await?;
    other.cleanup().await
}

#[tokio::test]
async fn clean_sweeps_advance_finalized_extents_for_drpc_and_reth() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_clean").await?;
    for chain in [BASE, ETHEREUM] {
        seed_chain(scratch.pool(), chain, 8, 7, 5, 1).await?;
    }
    let reference = Arc::new(FixtureReferences::new([
        reference_log(BASE, 1),
        reference_log(ETHEREUM, 1),
    ]));
    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;

    runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await?;
    runner
        .run_chain(&ethereum_chain()?, CancellationToken::new())
        .await?;

    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT chain_id, verification_level, current_block_number, target_block_number
         FROM chain_phase_state
         WHERE chain_id = ANY($1) AND phase_name = 'verify'
         ORDER BY chain_id",
    )
    .bind(vec![BASE, ETHEREUM])
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        rows,
        vec![
            (BASE.to_owned(), "cross_checked".to_owned(), 5, 5),
            (ETHEREUM.to_owned(), "node_checked".to_owned(), 5, 5),
        ]
    );
    assert_eq!(
        reference.calls(),
        vec![
            ReferenceCall {
                chain_id: BASE.to_owned(),
                provider_kind: VerificationProviderKind::IndependentRpc,
                level: VerificationLevel::CrossChecked,
                from: 0,
                to: 5,
            },
            ReferenceCall {
                chain_id: ETHEREUM.to_owned(),
                provider_kind: VerificationProviderKind::LocalReth,
                level: VerificationLevel::NodeChecked,
                from: 0,
                to: 5,
            },
        ]
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_provider_trusted_verify_reports_quick_synced_without_reference_calls() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_verify_sepolia").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let phases = PhaseSet::new([
        Arc::new(UnexpectedPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(UnexpectedPhase::new(PhaseName::Interpret)),
        Arc::new(UnexpectedPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            scratch.verification_database(2).await?,
            Arc::new(UnexpectedReferences),
        )),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&live_calls),
        }),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-sepolia-positive",
        test_timing(),
    )?;

    runner
        .run_chain(&sepolia_chain()?, CancellationToken::new())
        .await?;
    let state: (String, String, i64, i64) = sqlx::query_as(
        "SELECT phase_status, verification_level,
                current_block_number, target_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        ("completed".to_owned(), "quick_synced".to_owned(), 5, 5)
    );
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_provider_trusted_verify_finishes_before_live_when_serial_flag_is_omitted()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_forced_serial").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    let verify_gate = VerificationGate::default();
    let live_entered = Arc::new(Notify::new());
    let phases = PhaseSet::new([
        Arc::new(UnexpectedPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(UnexpectedPhase::new(PhaseName::Interpret)),
        Arc::new(UnexpectedPhase::new(PhaseName::Project)),
        Arc::new(GatedQuickSyncVerifyPhase {
            gate: verify_gate.clone(),
        }),
        Arc::new(SignalingLivePhase {
            entered: Arc::clone(&live_entered),
        }),
    ])?;
    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-sepolia-forced-serial",
        test_timing(),
    )?);
    let mut chain = ChainConfig::new(
        SEPOLIA,
        vec![SourceConfig::new(
            SEPOLIA,
            "drpc-intake",
            "drpc",
            SeedBasis::EthereumHead,
            0,
            "https://drpc.invalid",
        )?],
        false,
    )?;
    chain.verify_before_live = false;

    let task = {
        let runner = Arc::clone(&runner);
        tokio::spawn(async move { runner.run_chain(&chain, CancellationToken::new()).await })
    };
    tokio::time::timeout(Duration::from_secs(5), verify_gate.entered.notified()).await?;
    let live_started_before_verify_completed =
        tokio::time::timeout(Duration::from_millis(250), live_entered.notified())
            .await
            .is_ok();
    verify_gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task).await???;

    drop(runner);
    scratch.cleanup().await?;
    assert!(
        !live_started_before_verify_completed,
        "Live started while provider-trusted Verify was still running"
    );
    Ok(())
}

#[tokio::test]
async fn completed_sepolia_ingest_revalidates_source_binding_and_recovers() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_restart").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;

    let first_live_calls = Arc::new(AtomicUsize::new(0));
    let first_runner = sepolia_verifier_runner(&scratch, Arc::clone(&first_live_calls)).await?;
    first_runner
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(first_live_calls.load(Ordering::SeqCst), 1);
    drop(first_runner);
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(SEPOLIA)
    .execute(scratch.pool())
    .await?;

    let rotated_live_calls = Arc::new(AtomicUsize::new(0));
    let rotated_runner = sepolia_verifier_runner(&scratch, Arc::clone(&rotated_live_calls)).await?;
    let error = rotated_runner
        .run_chain(
            &sepolia_chain_with_key("rotated-drpc-intake")?,
            CancellationToken::new(),
        )
        .await
        .expect_err("completed Sepolia Ingest must revalidate the configured source key");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("matching cursor"), "{error}");
    assert_eq!(rotated_live_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        PhaseStore::new(scratch.pool().clone())
            .status(SEPOLIA, PhaseName::Ingest)
            .await?,
        phase_runner::state::PhaseStatus::Failed
    );
    drop(rotated_runner);

    let recovered_live_calls = Arc::new(AtomicUsize::new(0));
    let recovered = sepolia_verifier_runner(&scratch, Arc::clone(&recovered_live_calls)).await?;
    recovered
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(recovered_live_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        PhaseStore::new(scratch.pool().clone())
            .status(SEPOLIA, PhaseName::Ingest)
            .await?,
        phase_runner::state::PhaseStatus::Completed
    );

    drop(recovered);
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_sepolia_verify_survives_live_finality_past_ingest_cursor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_live_restart").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;

    let live_calls = Arc::new(AtomicUsize::new(0));
    let first_runner = sepolia_verifier_runner_with_live(
        &scratch,
        Arc::new(AdvancingLivePhase {
            pool: scratch.pool().clone(),
            from: 9,
            through: 12,
            calls: Arc::clone(&live_calls),
        }),
    )
    .await?;
    first_runner
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    drop(first_runner);

    assert_eq!(
        verify_state(scratch.pool()).await?,
        ("completed".to_owned(), "quick_synced".to_owned(), 5, 5)
    );
    let cursor = ingest_cursor_row(scratch.pool(), "drpc-intake").await?;
    assert_eq!(
        cursor,
        (
            "drpc".to_owned(),
            "ethereum_head".to_owned(),
            0,
            9,
            Some(8),
            Some(8),
            Some(block_hash(SEPOLIA, 8)),
        )
    );
    let highest_finalized: i64 = sqlx::query_scalar(
        "SELECT max(block_number) FROM chain_lineage
         WHERE chain_id = $1 AND canonicality_state = 'finalized'",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(highest_finalized, 12);

    for attempt in 1..=2 {
        let restart_live_calls = Arc::new(AtomicUsize::new(0));
        let restarted = sepolia_verifier_runner(&scratch, Arc::clone(&restart_live_calls)).await?;
        restarted
            .run_chain(
                &sepolia_chain_with_key("drpc-intake")?,
                CancellationToken::new(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("restart {attempt} must reach Live without extending Verify: {error}")
            });
        assert_eq!(restart_live_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            verify_state(scratch.pool()).await?,
            ("completed".to_owned(), "quick_synced".to_owned(), 5, 5)
        );
        assert_eq!(
            ingest_cursor_row(scratch.pool(), "drpc-intake").await?,
            cursor
        );
        drop(restarted);
    }

    let rotated_live_calls = Arc::new(AtomicUsize::new(0));
    let rotated = sepolia_verifier_runner(&scratch, Arc::clone(&rotated_live_calls)).await?;
    let error = rotated
        .run_chain(
            &sepolia_chain_with_key("rotated-drpc-intake")?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a rotated source without a durable cursor must still fail closed");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("matching cursor"), "{error}");
    assert_eq!(rotated_live_calls.load(Ordering::SeqCst), 0);

    drop(rotated);
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_sepolia_verify_survives_a_reorg_of_the_frozen_ingest_tip() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_reorged_ingest_tip").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;

    let first_live_calls = Arc::new(AtomicUsize::new(0));
    let first_runner = sepolia_verifier_runner(&scratch, Arc::clone(&first_live_calls)).await?;
    first_runner
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(first_live_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        verify_state(scratch.pool()).await?,
        ("completed".to_owned(), "quick_synced".to_owned(), 5, 5)
    );
    drop(first_runner);

    let replacement_tip = format!("{SEPOLIA}-winning-block-8");
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_number = 7, latest_block_hash = $2
         WHERE chain_id = $1",
    )
    .bind(SEPOLIA)
    .bind(block_hash(SEPOLIA, 7))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_lineage
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(SEPOLIA)
    .bind(block_hash(SEPOLIA, 8))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 8, to_timestamp(8), 'canonical')",
    )
    .bind(SEPOLIA)
    .bind(&replacement_tip)
    .bind(block_hash(SEPOLIA, 7))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_number = 8, latest_block_hash = $2,
             lineage_orphaning_epoch = lineage_orphaning_epoch + 1
         WHERE chain_id = $1",
    )
    .bind(SEPOLIA)
    .bind(&replacement_tip)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_hash = $2, target_block_hash = $2
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(SEPOLIA)
    .bind(&replacement_tip)
    .execute(scratch.pool())
    .await?;

    let restarted_live_calls = Arc::new(AtomicUsize::new(0));
    let restarted = sepolia_verifier_runner(&scratch, Arc::clone(&restarted_live_calls)).await?;
    restarted
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(restarted_live_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        verify_state(scratch.pool()).await?,
        ("completed".to_owned(), "quick_synced".to_owned(), 5, 5)
    );

    drop(restarted);
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_completed_sepolia_revalidation_records_failure_and_recovers() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_corrupt_completion_target").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;

    let first_live_calls = Arc::new(AtomicUsize::new(0));
    let first_runner = sepolia_verifier_runner(&scratch, Arc::clone(&first_live_calls)).await?;
    first_runner
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(first_live_calls.load(Ordering::SeqCst), 1);
    drop(first_runner);

    sqlx::query(
        "UPDATE chain_phase_state
         SET target_block_hash = 'corrupted-completion-target'
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .execute(scratch.pool())
    .await?;

    let restart_live_calls = Arc::new(AtomicUsize::new(0));
    let restarted = sepolia_verifier_runner(&scratch, Arc::clone(&restart_live_calls)).await?;
    let error = restarted
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await
        .expect_err("completed Verify must retain its finalized target identity");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert_eq!(restart_live_calls.load(Ordering::SeqCst), 0);
    drop(restarted);
    let failed_state: (String, String, i64, String, i64, String, Option<String>) = sqlx::query_as(
        "SELECT phase_status, verification_level,
                current_block_number, current_block_hash,
                target_block_number, target_block_hash, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(failed_state.0, "failed");
    assert_eq!(failed_state.1, "quick_synced");
    assert_eq!((failed_state.2, failed_state.4), (5, 5));
    assert_eq!(failed_state.3, block_hash(SEPOLIA, 5));
    assert_eq!(failed_state.5, "corrupted-completion-target");
    assert!(
        failed_state.6.as_deref().is_some_and(|error| {
            error.starts_with("completed phase validation failed: ")
                && error.contains("finalized lineage")
        }),
        "failed revalidation must retain its cause: {failed_state:?}"
    );

    sqlx::query(
        "UPDATE chain_phase_state
         SET target_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .bind(block_hash(SEPOLIA, 5))
    .execute(scratch.pool())
    .await?;

    let recovered_live_calls = Arc::new(AtomicUsize::new(0));
    let recovered = sepolia_verifier_runner(&scratch, Arc::clone(&recovered_live_calls)).await?;
    recovered
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(recovered_live_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        verify_state(scratch.pool()).await?,
        ("completed".to_owned(), "quick_synced".to_owned(), 5, 5)
    );

    drop(recovered);
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_completion_rejects_a_changed_frozen_target_identity() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_frozen_target_identity").await?;
    seed_sparse_verify_boundaries(scratch.pool()).await?;
    seed_ingest_cursor(
        scratch.pool(),
        SEPOLIA,
        "drpc-intake",
        MULTI_BATCH_VERIFY_TARGET,
    )
    .await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, MULTI_BATCH_VERIFY_TARGET).await?;

    run_first_verify_batch(&scratch).await?;
    let after_first = verify_marker_state(scratch.pool()).await?;
    assert_eq!(
        after_first,
        (
            "failed".to_owned(),
            Some(FIRST_VERIFY_BATCH_END),
            Some(block_hash(SEPOLIA, FIRST_VERIFY_BATCH_END)),
            Some(MULTI_BATCH_VERIFY_TARGET),
            Some(block_hash(SEPOLIA, MULTI_BATCH_VERIFY_TARGET)),
        )
    );

    sqlx::query(
        "UPDATE chain_phase_state
         SET target_block_hash = 'changed-frozen-target'
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .execute(scratch.pool())
    .await?;

    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = sepolia_verifier_runner(&scratch, Arc::clone(&live_calls)).await?;
    let error = runner
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await
        .expect_err("Verify must reject a final marker that differs from its frozen target");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("frozen target"), "{error}");
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        verify_marker_state(scratch.pool()).await?,
        (
            "failed".to_owned(),
            Some(FIRST_VERIFY_BATCH_END),
            Some(block_hash(SEPOLIA, FIRST_VERIFY_BATCH_END)),
            Some(MULTI_BATCH_VERIFY_TARGET),
            Some("changed-frozen-target".to_owned()),
        ),
        "the rejected final batch must not record completion progress"
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_completion_accepts_an_intact_multi_batch_target() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_intact_multi_batch_target").await?;
    seed_sparse_verify_boundaries(scratch.pool()).await?;
    seed_ingest_cursor(
        scratch.pool(),
        SEPOLIA,
        "drpc-intake",
        MULTI_BATCH_VERIFY_TARGET,
    )
    .await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, MULTI_BATCH_VERIFY_TARGET).await?;

    run_first_verify_batch(&scratch).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = sepolia_verifier_runner(&scratch, Arc::clone(&live_calls)).await?;
    runner
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        verify_marker_state(scratch.pool()).await?,
        (
            "completed".to_owned(),
            Some(MULTI_BATCH_VERIFY_TARGET),
            Some(block_hash(SEPOLIA, MULTI_BATCH_VERIFY_TARGET)),
            Some(MULTI_BATCH_VERIFY_TARGET),
            Some(block_hash(SEPOLIA, MULTI_BATCH_VERIFY_TARGET)),
        )
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn progressed_ingest_cursor_rejects_source_kind_change_before_verify() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_source_kind_restart").await?;
    let original_chain = sepolia_chain_with_kind("intake", "drpc")?;
    PhaseStore::new(scratch.pool().clone())
        .ensure_ingest_sources(SEPOLIA, &original_chain.sources)
        .await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;

    let stage_a_live_calls = Arc::new(AtomicUsize::new(0));
    let stage_a = PhaseSet::new([
        Arc::new(PartialThenStopIngestPhase {
            batches: AtomicUsize::new(0),
        }) as Arc<dyn Phase>,
        Arc::new(UnexpectedPhase::new(PhaseName::Interpret)),
        Arc::new(UnexpectedPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            scratch.verification_database(2).await?,
            Arc::new(UnexpectedReferences),
        )),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&stage_a_live_calls),
        }),
    ])?;
    let stage_a_runner = PhaseRunner::new(
        scratch.runner(),
        stage_a,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-source-kind-stage-a",
        test_timing(),
    )?;
    let stage_a_error = stage_a_runner
        .run_chain(&original_chain, CancellationToken::new())
        .await
        .expect_err("the fixture must stop after persisting the dRPC prefix");
    assert_eq!(stage_a_error.kind(), ErrorKind::DataIntegrity);
    assert_eq!(stage_a_live_calls.load(Ordering::SeqCst), 0);
    drop(stage_a_runner);

    let before = ingest_cursor_row(scratch.pool(), "intake").await?;
    let phase_before = ingest_phase_state(scratch.pool()).await?;
    assert_eq!(
        before,
        (
            "drpc".to_owned(),
            "ethereum_head".to_owned(),
            0,
            5,
            Some(8),
            Some(4),
            Some(block_hash(SEPOLIA, 4)),
        )
    );

    let observed_resume = Arc::new(Mutex::new(Vec::new()));
    let resumed_ingest_calls = Arc::new(AtomicUsize::new(0));
    let stage_b_live_calls = Arc::new(AtomicUsize::new(0));
    let stage_b = PhaseSet::new([
        Arc::new(ResumingIngestPhase {
            observed_resume: Arc::clone(&observed_resume),
            calls: Arc::clone(&resumed_ingest_calls),
        }) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            scratch.verification_database(2).await?,
            Arc::new(UnexpectedReferences),
        )),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&stage_b_live_calls),
        }),
    ])?;
    let stage_b_runner = PhaseRunner::new(
        scratch.runner(),
        stage_b,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-source-kind-stage-b",
        test_timing(),
    )?;
    let result = stage_b_runner
        .run_chain(
            &sepolia_chain_with_kind("intake", "rpc")?,
            CancellationToken::new(),
        )
        .await;
    let after = ingest_cursor_row(scratch.pool(), "intake").await?;
    let verify = verify_state_optional(scratch.pool()).await?;
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.kind() == ErrorKind::DataIntegrity),
        "source kind change must fail closed; result={result:?}, cursor={after:?}, verify={verify:?}, live_calls={}",
        stage_b_live_calls.load(Ordering::SeqCst),
    );
    assert_eq!(
        after, before,
        "the rejected resume must not mutate its cursor"
    );
    assert_eq!(
        resumed_ingest_calls.load(Ordering::SeqCst),
        0,
        "source compatibility must fail before resumed Ingest runs"
    );
    assert_eq!(
        ingest_phase_state(scratch.pool()).await?,
        phase_before,
        "source compatibility must fail before phase progress changes"
    );
    assert_eq!(stage_b_live_calls.load(Ordering::SeqCst), 0);
    assert!(
        verify.as_ref().is_none_or(
            |state| state.0 != "completed" || state.1.as_deref() != Some("quick_synced")
        ),
        "the rejected mixed-provenance cursor must not earn quick_synced"
    );
    assert!(
        observed_resume
            .lock()
            .expect("resume observation lock")
            .is_empty(),
        "the incompatible cursor must not be handed to Ingest"
    );

    drop(stage_b_runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_sepolia_ingest_rejects_unreviewed_source_shape_before_interpret() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_completed_ingest_shape").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor_with_kind(scratch.pool(), SEPOLIA, "rpc-intake", "rpc", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'idle',
             current_block_number = NULL, current_block_hash = NULL,
             target_block_number = NULL, target_block_hash = NULL,
             input_content_hash = NULL, started_at = NULL, finished_at = NULL
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(SEPOLIA)
    .execute(scratch.pool())
    .await?;

    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let project_calls = Arc::new(AtomicUsize::new(0));
    let phases = PhaseSet::new([
        Arc::new(UnexpectedPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(CountingCompletePhase {
            name: PhaseName::Interpret,
            calls: Arc::clone(&interpret_calls),
        }),
        Arc::new(CountingCompletePhase {
            name: PhaseName::Project,
            calls: Arc::clone(&project_calls),
        }),
        Arc::new(VerifyPhase::with_reference_provider(
            scratch.verification_database(2).await?,
            Arc::new(UnexpectedReferences),
        )),
        Arc::new(UnexpectedPhase::new(PhaseName::Live)),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-completed-ingest-shape",
        test_timing(),
    )?;
    let error = runner
        .run_chain(
            &sepolia_chain_with_kind("rpc-intake", "rpc")?,
            CancellationToken::new(),
        )
        .await
        .expect_err("completed Sepolia Ingest must reject an unreviewed source shape");
    let observed_calls = (
        interpret_calls.load(Ordering::SeqCst),
        project_calls.load(Ordering::SeqCst),
    );
    let ingest_status = PhaseStore::new(scratch.pool().clone())
        .status(SEPOLIA, PhaseName::Ingest)
        .await?;

    drop(runner);
    scratch.cleanup().await?;
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(
        error.to_string().contains("one dRPC intake source"),
        "{error}"
    );
    assert_eq!(
        observed_calls,
        (0, 0),
        "source-shape validation must run before Interpret or Project"
    );
    assert_eq!(ingest_status, phase_runner::state::PhaseStatus::Failed);
    Ok(())
}

#[tokio::test]
async fn completed_compared_verify_remains_attested_across_endpoint_rotation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_compared_restart").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 1)]));
    let first_runner =
        verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    first_runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await?;
    assert_eq!(reference.calls().len(), 1);
    drop(first_runner);
    reference.clear_calls();

    let restarted_runner =
        verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    restarted_runner
        .run_chain(&base_chain_with_endpoints()?, CancellationToken::new())
        .await?;
    assert!(
        reference.calls().is_empty(),
        "completed compared verification must not rescan after endpoint rotation"
    );
    let state: (String, String, i64) = sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        ("completed".to_owned(), "cross_checked".to_owned(), 5)
    );

    drop(restarted_runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_verify_rejects_a_level_stronger_than_the_current_reference_can_earn()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_completed_level_cap").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    seed_completed_spine_prerequisites(scratch.pool(), BASE, 5).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', verification_level = 'node_checked',
             current_block_number = 5, current_block_hash = $2,
             target_block_number = 5, target_block_hash = $2,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .bind(block_hash(BASE, 5))
    .execute(scratch.pool())
    .await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = verifier_runner(
        &scratch,
        Arc::new(FixtureReferences::new([])),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&live_calls),
        }),
    )
    .await?;

    let result = runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await;
    let observed_live_calls = live_calls.load(Ordering::SeqCst);

    drop(runner);
    scratch.cleanup().await?;
    let error = result.expect_err("completed Verify must re-cap its retained trust level");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("earns at most cross_checked"),
        "{error}"
    );
    assert_eq!(observed_live_calls, 0);
    Ok(())
}

#[tokio::test]
async fn completed_verify_rejects_a_missing_retained_verification_level() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_completed_missing_level").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    seed_completed_spine_prerequisites(scratch.pool(), BASE, 5).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', verification_level = NULL,
             current_block_number = 5, current_block_hash = $2,
             target_block_number = 5, target_block_hash = $2,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .bind(block_hash(BASE, 5))
    .execute(scratch.pool())
    .await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = verifier_runner(
        &scratch,
        Arc::new(FixtureReferences::new([])),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&live_calls),
        }),
    )
    .await?;

    let result = runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await;
    let observed_live_calls = live_calls.load(Ordering::SeqCst);

    drop(runner);
    scratch.cleanup().await?;
    let error = result.expect_err("completed Verify must retain its verification level");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("verification level"), "{error}");
    assert_eq!(observed_live_calls, 0);
    Ok(())
}

#[tokio::test]
async fn sepolia_requires_its_configured_intake_cursor_through_finality() -> Result<()> {
    for (prefix, cursor_key, cursor_through, message) in [
        ("changed_key", "previous-drpc-intake", 8, "matching cursor"),
        ("below_finality", "drpc-intake", 4, "durable ingest cursor"),
    ] {
        let scratch =
            ScratchDatabase::create(&format!("production_verify_sepolia_cursor_{prefix}")).await?;
        seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
        seed_ingest_cursor(scratch.pool(), SEPOLIA, cursor_key, cursor_through).await?;
        seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
        let live_calls = Arc::new(AtomicUsize::new(0));
        let phases = PhaseSet::new([
            Arc::new(UnexpectedPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
            Arc::new(UnexpectedPhase::new(PhaseName::Interpret)),
            Arc::new(UnexpectedPhase::new(PhaseName::Project)),
            Arc::new(VerifyPhase::with_reference_provider(
                scratch.verification_database(2).await?,
                Arc::new(UnexpectedReferences),
            )),
            Arc::new(CountingLivePhase {
                calls: Arc::clone(&live_calls),
            }),
        ])?;
        let runner = PhaseRunner::new(
            scratch.runner(),
            phases,
            CapacityGuard::system(CapacityConfig::default()),
            format!("production-verify-sepolia-cursor-{prefix}"),
            test_timing(),
        )?;

        let error = runner
            .run_chain(&sepolia_chain()?, CancellationToken::new())
            .await
            .expect_err("Sepolia verification must be bound to covered configured intake");
        assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{prefix}");
        assert!(error.to_string().contains(message), "{error}");
        assert_eq!(live_calls.load(Ordering::SeqCst), 0, "{prefix}");

        drop(runner);
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn sepolia_rejects_a_cursor_whose_numeric_extent_ends_below_finality() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_inconsistent_cursor").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    sqlx::query(
        "UPDATE ingest_cursors
         SET target_block_number = 4,
             last_processed_block_number = 4,
             last_processed_block_hash = $2
         WHERE chain_id = $1 AND source_key = 'drpc-intake'",
    )
    .bind(SEPOLIA)
    .bind(block_hash(SEPOLIA, 4))
    .execute(scratch.pool())
    .await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = sepolia_verifier_runner(&scratch, Arc::clone(&live_calls)).await?;

    let error = runner
        .run_chain(&sepolia_chain()?, CancellationToken::new())
        .await
        .expect_err("Sepolia verification must reject cursor extent below finality");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("durable ingest cursor"),
        "{error}"
    );
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_rejects_a_cursor_whose_tip_diverged_below_finality() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_divergent_cursor").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    let mut parent_hash = block_hash(SEPOLIA, 4);
    for number in 5..=8 {
        let fork_hash = format!("{SEPOLIA}-losing-block-{number}");
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'orphaned')",
        )
        .bind(SEPOLIA)
        .bind(&fork_hash)
        .bind(&parent_hash)
        .bind(number)
        .execute(scratch.pool())
        .await?;
        parent_hash = fork_hash;
    }
    sqlx::query(
        "UPDATE ingest_cursors
         SET last_processed_block_hash = $2
         WHERE chain_id = $1 AND source_key = 'drpc-intake'",
    )
    .bind(SEPOLIA)
    .bind(parent_hash)
    .execute(scratch.pool())
    .await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = sepolia_verifier_runner(&scratch, Arc::clone(&live_calls)).await?;

    let error = runner
        .run_chain(&sepolia_chain()?, CancellationToken::new())
        .await
        .expect_err("Sepolia verification must reject a tip that forked below finality");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("durable ingest cursor"),
        "{error}"
    );
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_rejects_an_observed_cursor_tip() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_observed_cursor").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    let observed_tip = format!("{SEPOLIA}-observed-block-8");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 8, to_timestamp(8), 'observed')",
    )
    .bind(SEPOLIA)
    .bind(&observed_tip)
    .bind(block_hash(SEPOLIA, 7))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE ingest_cursors
         SET last_processed_block_hash = $2
         WHERE chain_id = $1 AND source_key = 'drpc-intake'",
    )
    .bind(SEPOLIA)
    .bind(observed_tip)
    .execute(scratch.pool())
    .await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = sepolia_verifier_runner(&scratch, Arc::clone(&live_calls)).await?;

    let error = runner
        .run_chain(&sepolia_chain()?, CancellationToken::new())
        .await
        .expect_err("Sepolia verification must reject an observed-only cursor tip");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("durable ingest cursor"),
        "{error}"
    );
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_rejects_unreviewed_intake_source_shapes_before_verification() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_source_shape").await?;
    let phase = VerifyPhase::with_reference_provider(
        scratch.verification_database(1).await?,
        Arc::new(UnexpectedReferences),
    );
    for (label, sources) in [
        ("empty", vec![]),
        (
            "wrong kind",
            vec![SourceConfig::new(
                SEPOLIA,
                "rpc-intake",
                "rpc",
                SeedBasis::EthereumHead,
                0,
                "https://rpc.invalid",
            )?],
        ),
        (
            "wrong start",
            vec![SourceConfig::new(
                SEPOLIA,
                "drpc-intake",
                "drpc",
                SeedBasis::EthereumHead,
                1,
                "https://drpc.invalid",
            )?],
        ),
        (
            "wrong seed basis",
            vec![SourceConfig::new(
                SEPOLIA,
                "drpc-intake",
                "drpc",
                SeedBasis::NewSignatureRange,
                0,
                "https://drpc.invalid",
            )?],
        ),
        (
            "multiple",
            vec![
                SourceConfig::new(
                    SEPOLIA,
                    "drpc-one",
                    "drpc",
                    SeedBasis::EthereumHead,
                    0,
                    "https://one.invalid",
                )?,
                SourceConfig::new(
                    SEPOLIA,
                    "drpc-two",
                    "drpc",
                    SeedBasis::EthereumHead,
                    0,
                    "https://two.invalid",
                )?,
            ],
        ),
    ] {
        let error = phase
            .preflight(SEPOLIA, &sources, &phase_runner::phase::RunMode::Normal)
            .expect_err(label);
        assert_eq!(error.kind(), ErrorKind::Configuration, "{label}");
    }
    drop(phase);
    scratch.cleanup().await
}

#[tokio::test]
async fn resumed_normal_verification_retains_the_weaker_extent_level() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_resumed_level").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 1)]));
    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'failed',
             current_block_number = 2,
             current_block_hash = $2,
             target_block_number = 5,
             target_block_hash = $3,
             verification_level = 'quick_synced',
             last_error = 'fixture interruption'
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .bind(block_hash(BASE, 2))
    .bind(block_hash(BASE, 5))
    .execute(scratch.pool())
    .await?;
    drop(runner);
    reference.clear_calls();

    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await?;
    let state: (String, String, i64) = sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        ("completed".to_owned(), "quick_synced".to_owned(), 5)
    );
    assert_eq!(
        reference.calls(),
        vec![ReferenceCall {
            chain_id: BASE.to_owned(),
            provider_kind: VerificationProviderKind::IndependentRpc,
            level: VerificationLevel::CrossChecked,
            from: 3,
            to: 5,
        }]
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_ingest_rejects_a_moved_start_before_verification() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_durable_start").await?;
    seed_chain(scratch.pool(), ETHEREUM, 5, 5, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(ETHEREUM, 1)]));
    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    runner
        .run_chain(&ethereum_chain()?, CancellationToken::new())
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'idle',
             current_block_number = NULL,
             current_block_hash = NULL,
             target_block_number = NULL,
             target_block_hash = NULL,
             verification_level = NULL,
             started_at = NULL,
             finished_at = NULL,
             last_error = NULL
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(ETHEREUM)
    .execute(scratch.pool())
    .await?;
    drop(runner);
    reference.clear_calls();
    reference.set_log_data(ETHEREUM, 2);

    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    let error = runner
        .run_chain(&ethereum_chain_with_start(5)?, CancellationToken::new())
        .await
        .expect_err("completed Ingest must reject a moved source start");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("seed configuration"), "{error}");
    assert!(
        reference.calls().is_empty(),
        "Verify must not run after completed Ingest rejects source drift"
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn drpc_query_count_matches_actual_rpc_requests() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_drpc_query_count").await?;
    seed_watch_manifest(scratch.pool(), BASE).await?;
    let filter = bigname_ingest::load_watch_filter(scratch.pool(), BASE, 0, 5).await?;
    let (endpoint, requests, server) = spawn_verification_rpc(0).await?;
    let provider = VerificationProvider::new(BASE, "drpc", &endpoint)?;

    let batch = provider.fetch(filter.clone(), 0, 5).await?;
    let actual_requests = requests.load(Ordering::SeqCst);
    assert_eq!(actual_requests, 3);
    assert_eq!(batch.rpc_request_count, actual_requests);
    server.abort();

    let (endpoint, requests, server) = spawn_verification_rpc(1).await?;
    let provider = VerificationProvider::new(BASE, "drpc", &endpoint)?;
    let batch = provider.fetch(filter, 0, 5).await?;
    let actual_requests = requests.load(Ordering::SeqCst);
    assert_eq!(actual_requests, 4);
    assert_eq!(batch.rpc_request_count, actual_requests);
    server.abort();

    scratch.cleanup().await
}

#[tokio::test]
async fn base_cross_check_stops_at_the_drpc_ingest_seam() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_base_seam_cap").await?;
    seed_chain(scratch.pool(), BASE, 8, 7, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 1)]));
    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;

    runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await?;

    let state: (String, i64, i64) = sqlx::query_as(
        "SELECT verification_level, current_block_number, target_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("cross_checked".to_owned(), 5, 5));
    assert_eq!(reference.calls()[0].to, 5);
    let error = runner
        .redo(
            &base_chain(true)?,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(BASE_COINBASE_SEAM_BLOCK, BASE_COINBASE_SEAM_BLOCK + 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a dRPC redo must not claim independent evidence after the ingest seam");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    let redo_in_progress: bool = sqlx::query_scalar(
        "SELECT redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!redo_in_progress);

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn base_drpc_seam_cannot_be_moved_by_verify_redo_configuration() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_base_fixed_seam").await?;
    seed_chain(scratch.pool(), BASE, 8, 7, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 1)]));
    let runner = verifier_runner(&scratch, reference, Arc::new(CompleteLivePhase)).await?;

    runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await?;
    let error = runner
        .redo(
            &base_chain_with_drpc_start(true, BASE_COINBASE_SEAM_BLOCK + 1)?,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(2, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("verify redo must not accept a caller-moved Base dRPC seam");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    let redo_in_progress: bool = sqlx::query_scalar(
        "SELECT redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!redo_in_progress);

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn mismatch_is_fatal_durable_and_restartable_after_wipe_and_resync() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_mismatch").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 2)]));
    let runner = verifier_runner(&scratch, reference, Arc::new(CompleteLivePhase)).await?;
    let chain = base_chain(true)?;

    let error = runner
        .run_chain(&chain, CancellationToken::new())
        .await
        .expect_err("different stored and reference log bytes must stop verification");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    assert!(!error.is_retryable());
    for context in ["block 2", "raw_logs[0].data", "ours=0x01", "reference=0x02"] {
        assert!(
            error.to_string().contains(context),
            "missing mismatch context {context:?}: {error}"
        );
    }
    let failed: (String, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT phase_status, current_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(failed.0, "failed");
    assert_eq!(failed.1, None, "a mismatching batch must not advance");
    let durable = failed.2.expect("fatal mismatch context must be durable");
    assert!(durable.starts_with("verification mismatch: "));
    assert!(durable.contains("block 2 field raw_logs[0].data"));

    wipe_and_resync_log(scratch.pool(), BASE, 2).await?;
    runner.run_chain(&chain, CancellationToken::new()).await?;
    let repaired: (String, String, i64, Option<String>) = sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        repaired,
        ("completed".to_owned(), "cross_checked".to_owned(), 5, None,)
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn mismatch_marks_a_caught_up_live_phase_failed() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_completed_live_mismatch").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let gate = VerificationGate::default();
    let reference = Arc::new(FixtureReferences::gated(
        [reference_log(BASE, 2)],
        gate.clone(),
    ));
    let database = scratch.runner();
    let verification_database = scratch.verification_database(2).await?;
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            verification_database,
            reference,
        )),
        Arc::new(CompleteLivePhase),
    )?;
    let runner = Arc::new(PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-completed-live-mismatch",
        TimingConfig {
            live_poll_interval: Duration::from_secs(1),
            ..test_timing()
        },
    )?);
    let task = {
        let runner = Arc::clone(&runner);
        tokio::spawn(async move {
            runner
                .run_chain(&base_chain(false)?, CancellationToken::new())
                .await
        })
    };

    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    wait_for_phase_position(scratch.pool(), BASE, PhaseName::Live, "completed", None).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.release.notify_one();

    let error = tokio::time::timeout(Duration::from_secs(5), task)
        .await??
        .expect_err("the verification mismatch must stop the chain");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    let state: (String, Option<String>) = sqlx::query_as(
        "SELECT phase_status, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state.0, "failed");
    assert!(
        state
            .1
            .as_deref()
            .is_some_and(|reason| reason.contains("block 2 field raw_logs[0].data"))
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn mismatch_finishing_after_live_still_records_the_live_stop() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_live_first_mismatch").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let gate = VerificationGate::default();
    let reference = Arc::new(FixtureReferences::gated(
        [reference_log(BASE, 2)],
        gate.clone(),
    ));
    let runner = Arc::new(verifier_runner(&scratch, reference, Arc::new(CompleteLivePhase)).await?);
    let task = {
        let runner = Arc::clone(&runner);
        tokio::spawn(async move {
            runner
                .run_chain(&base_chain(false)?, CancellationToken::new())
                .await
        })
    };

    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    wait_for_phase_position(scratch.pool(), BASE, PhaseName::Live, "completed", None).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.release.notify_one();

    let error = tokio::time::timeout(Duration::from_secs(5), task)
        .await??
        .expect_err("the late verification mismatch must stop the chain");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    let state: (String, Option<String>) = sqlx::query_as(
        "SELECT phase_status, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state.0, "failed");
    assert!(
        state
            .1
            .as_deref()
            .is_some_and(|reason| reason.contains("block 2 field raw_logs[0].data"))
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn mismatch_finishing_after_live_error_remains_the_fatal_context() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_live_error_mismatch").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let gate = VerificationGate::default();
    let reference = Arc::new(FixtureReferences::gated(
        [reference_log(BASE, 2)],
        gate.clone(),
    ));
    let runner = Arc::new(verifier_runner(&scratch, reference, Arc::new(FailingLivePhase)).await?);
    let task = {
        let runner = Arc::clone(&runner);
        tokio::spawn(async move {
            runner
                .run_chain(&base_chain(false)?, CancellationToken::new())
                .await
        })
    };

    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    wait_for_phase_position(scratch.pool(), BASE, PhaseName::Live, "failed", None).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.release.notify_one();

    let error = tokio::time::timeout(Duration::from_secs(5), task)
        .await??
        .expect_err("a late mismatch must remain the fatal chain error");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    assert!(error.to_string().contains("fixture live failed first"));
    let last_error: String = sqlx::query_scalar(
        "SELECT last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert!(last_error.contains("block 2 field raw_logs[0].data"));

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn drpc_backed_base_cannot_persist_node_checked() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_label_cap").await?;
    seed_ingest_identities(scratch.pool(), BASE).await?;
    seed_lineage_and_heads(scratch.pool(), BASE, 5, 5, 5).await?;
    let phases = PhaseSet::new([
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(ClaimingVerifyPhase {
            level: VerificationLevel::NodeChecked,
        }),
        Arc::new(CompleteLivePhase),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-label-cap",
        test_timing(),
    )?;

    let error = runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await
        .expect_err("the persistence boundary must reject a dRPC node-check claim");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("chain-specific verification path earns at most cross_checked")
    );
    let state: (String, Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("failed".to_owned(), None, None));

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_no_comparison_cannot_report_cross_checked_or_enter_live() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_sepolia_level_guard").await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 5).await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 5, 5, 5).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let phases = PhaseSet::new([
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(ClaimingVerifyPhase {
            level: VerificationLevel::CrossChecked,
        }),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&live_calls),
        }),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verify-sepolia-level-guard",
        test_timing(),
    )?;

    let error = runner
        .run_chain(&sepolia_chain()?, CancellationToken::new())
        .await
        .expect_err("a provider-trusted Sepolia pass cannot claim an independent comparison");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("chain-specific verification path earns at most quick_synced")
    );
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);
    let state: (String, Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("failed".to_owned(), None, None));

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_redo_rechecks_the_requested_range_and_persists_its_level() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_redo").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 1)]));
    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    let chain = base_chain(true)?;
    runner.run_chain(&chain, CancellationToken::new()).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET verification_level = 'quick_synced'
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .execute(scratch.pool())
    .await?;
    reference.clear_calls();
    reference.set_log_data(BASE, 2);

    let mismatch = runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(2, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a redo mismatch must retain resumable redo state");
    assert_eq!(mismatch.kind(), ErrorKind::VerificationMismatch);
    let failed_redo: (bool, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_current_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert!(failed_redo.0);
    assert_eq!(failed_redo.1, None);
    assert!(
        failed_redo
            .2
            .as_deref()
            .is_some_and(|error| error.contains("block 2 field raw_logs[0].data"))
    );

    wipe_and_resync_log(scratch.pool(), BASE, 2).await?;

    runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(2, 3)?,
            CancellationToken::new(),
        )
        .await?;

    assert_eq!(
        reference.calls(),
        vec![
            ReferenceCall {
                chain_id: BASE.to_owned(),
                provider_kind: VerificationProviderKind::IndependentRpc,
                level: VerificationLevel::CrossChecked,
                from: 2,
                to: 3,
            },
            ReferenceCall {
                chain_id: BASE.to_owned(),
                provider_kind: VerificationProviderKind::IndependentRpc,
                level: VerificationLevel::CrossChecked,
                from: 2,
                to: 3,
            },
        ]
    );
    let state: (String, String, i64, bool) = sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        ("completed".to_owned(), "quick_synced".to_owned(), 5, false)
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_verify_redo_rejects_an_extra_persisted_ingest_source() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_redo_extra_ingest_source").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = sepolia_verifier_runner(&scratch, Arc::clone(&live_calls)).await?;
    let chain = sepolia_chain_with_key("drpc-intake")?;
    runner.run_chain(&chain, CancellationToken::new()).await?;
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);

    seed_ingest_cursor(scratch.pool(), SEPOLIA, "retired-drpc-intake", 8).await?;
    let result = runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(0, 5)?,
            CancellationToken::new(),
        )
        .await;

    let error = result.expect_err("Verify redo must reject an extra persisted intake source");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("persisted ingest source keys"),
        "{error}"
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_sepolia_verify_redo_revalidates_the_retained_completion_target() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_partial_redo_retained_target").await?;
    seed_lineage_and_heads(scratch.pool(), SEPOLIA, 8, 7, 5).await?;
    seed_ingest_cursor(scratch.pool(), SEPOLIA, "drpc-intake", 8).await?;
    seed_completed_spine_prerequisites(scratch.pool(), SEPOLIA, 8).await?;
    let live_calls = Arc::new(AtomicUsize::new(0));
    let runner = sepolia_verifier_runner(&scratch, Arc::clone(&live_calls)).await?;
    let chain = sepolia_chain()?;
    runner.run_chain(&chain, CancellationToken::new()).await?;

    let mut parent_hash = block_hash(SEPOLIA, 3);
    for number in 4..=8 {
        let fork_hash = format!("{SEPOLIA}-partial-redo-fork-{number}");
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'orphaned')",
        )
        .bind(SEPOLIA)
        .bind(&fork_hash)
        .bind(&parent_hash)
        .bind(number)
        .execute(scratch.pool())
        .await?;
        parent_hash = fork_hash;
    }
    sqlx::query(
        "UPDATE ingest_cursors
         SET last_processed_block_hash = $2
         WHERE chain_id = $1 AND source_key = 'drpc-intake'",
    )
    .bind(SEPOLIA)
    .bind(parent_hash)
    .execute(scratch.pool())
    .await?;

    let error = runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a partial redo must revalidate the retained full Verify target");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("finalized block 5"), "{error}");
    let redo_in_progress: bool = sqlx::query_scalar(
        "SELECT redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(scratch.pool())
    .await?;
    assert!(redo_in_progress);

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn full_verify_redo_updates_the_full_extent_level() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_full_redo_level").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 1)]));
    let runner = verifier_runner(&scratch, reference, Arc::new(CompleteLivePhase)).await?;
    let chain = base_chain(true)?;
    runner.run_chain(&chain, CancellationToken::new()).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET verification_level = 'quick_synced'
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .execute(scratch.pool())
    .await?;

    runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(0, 5)?,
            CancellationToken::new(),
        )
        .await?;

    let state: (String, i64, bool) = sqlx::query_as(
        "SELECT verification_level, current_block_number, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("cross_checked".to_owned(), 5, false));

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn base_reth_reference_is_rejected_before_verify_redo_begins() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_base_reth_unsupported").await?;
    seed_chain(scratch.pool(), BASE, 5, 5, 5, 1).await?;
    let reference = Arc::new(FixtureReferences::new([reference_log(BASE, 1)]));
    let runner = verifier_runner(&scratch, reference.clone(), Arc::new(CompleteLivePhase)).await?;
    runner
        .run_chain(&base_chain(true)?, CancellationToken::new())
        .await?;
    reference.clear_calls();

    let error = runner
        .redo(
            &base_chain_with_reth_reference()?,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(0, 5)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("Base local-reth verification must fail during configuration validation");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(
        error.to_string().contains("base-mainnet with reth_db"),
        "{error}"
    );
    assert!(
        error.to_string().contains("OP Stack") && error.to_string().contains("Ethereum-primitives"),
        "{error}"
    );
    assert!(reference.calls().is_empty());
    let state: (String, String, bool) = sqlx::query_as(
        "SELECT phase_status, verification_level, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        ("completed".to_owned(), "cross_checked".to_owned(), false)
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_stays_at_finality_while_live_advances_to_head() -> Result<()> {
    let scratch = ScratchDatabase::create("production_verify_live_pair").await?;
    seed_chain(scratch.pool(), BASE, 10, 7, 5, 1).await?;
    let gate = VerificationGate::default();
    let reference = Arc::new(FixtureReferences::gated(
        [reference_log(BASE, 1)],
        gate.clone(),
    ));
    let live_advanced = Arc::new(Notify::new());
    let runner = Arc::new(
        verifier_runner(
            &scratch,
            reference.clone(),
            Arc::new(HeadFollowingLivePhase {
                advanced: Arc::clone(&live_advanced),
            }),
        )
        .await?,
    );
    let cancellation = CancellationToken::new();
    let task = {
        let runner = Arc::clone(&runner);
        let cancellation = cancellation.clone();
        tokio::spawn(async move { runner.run_chain(&base_chain(false)?, cancellation).await })
    };

    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    tokio::time::timeout(Duration::from_secs(5), live_advanced.notified()).await?;
    wait_for_phase_position(scratch.pool(), BASE, PhaseName::Live, "running", Some(10)).await?;
    let during_verify: (String, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(during_verify, ("running".to_owned(), None));

    gate.release.notify_one();
    wait_for_phase_position(
        scratch.pool(),
        BASE,
        PhaseName::Verify,
        "completed",
        Some(5),
    )
    .await?;
    let live_position: i64 = sqlx::query_scalar(
        "SELECT current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(BASE)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(live_position, 10);
    assert_eq!(reference.calls()[0].to, 5);

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    drop(runner);
    scratch.cleanup().await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReferenceCall {
    chain_id: String,
    provider_kind: VerificationProviderKind,
    level: VerificationLevel,
    from: i64,
    to: i64,
}

#[derive(Clone, Default)]
struct VerificationGate {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[derive(Default)]
struct FixtureState {
    logs: BTreeMap<String, Vec<VerificationLog>>,
    calls: Vec<ReferenceCall>,
}

struct FixtureReferences {
    state: Mutex<FixtureState>,
    gate: Option<VerificationGate>,
}

impl FixtureReferences {
    fn new(logs: impl IntoIterator<Item = VerificationLog>) -> Self {
        Self::with_gate(logs, None)
    }

    fn gated(logs: impl IntoIterator<Item = VerificationLog>, gate: VerificationGate) -> Self {
        Self::with_gate(logs, Some(gate))
    }

    fn with_gate(
        logs: impl IntoIterator<Item = VerificationLog>,
        gate: Option<VerificationGate>,
    ) -> Self {
        let mut state = FixtureState::default();
        for log in logs {
            state
                .logs
                .entry(chain_from_hash(&log.block_hash).to_owned())
                .or_default()
                .push(log);
        }
        Self {
            state: Mutex::new(state),
            gate,
        }
    }

    fn calls(&self) -> Vec<ReferenceCall> {
        self.state.lock().expect("fixture state lock").calls.clone()
    }

    fn clear_calls(&self) {
        self.state.lock().expect("fixture state lock").calls.clear();
    }

    fn set_log_data(&self, chain_id: &str, data: u8) {
        for log in self
            .state
            .lock()
            .expect("fixture state lock")
            .logs
            .get_mut(chain_id)
            .expect("fixture chain logs")
        {
            log.data = vec![data];
        }
    }
}

impl VerificationReferenceProvider for FixtureReferences {
    fn preflight(&self, source: &VerificationSource) -> RunnerResult<()> {
        match (source.provider_kind(), source.verification_level()) {
            (VerificationProviderKind::IndependentRpc, VerificationLevel::CrossChecked)
            | (VerificationProviderKind::LocalReth, VerificationLevel::NodeChecked) => Ok(()),
            (kind, level) => Err(RunnerError::data_integrity(format!(
                "fixture observed incoherent verification mapping {kind:?} => {level:?}"
            ))),
        }
    }

    fn fetch<'a>(
        &'a self,
        source: &'a VerificationSource,
        _filter: WatchFilter,
        from_block: i64,
        to_block: i64,
    ) -> VerificationReferenceFuture<'a> {
        let chain_id = source.chain_id().to_owned();
        let (logs, gate) = {
            let mut state = self.state.lock().expect("fixture state lock");
            state.calls.push(ReferenceCall {
                chain_id: chain_id.clone(),
                provider_kind: source.provider_kind(),
                level: source.verification_level(),
                from: from_block,
                to: to_block,
            });
            let logs = state
                .logs
                .get(&chain_id)
                .into_iter()
                .flatten()
                .filter(|log| (from_block..=to_block).contains(&log.block_number))
                .cloned()
                .collect();
            (logs, self.gate.clone())
        };
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            Ok(VerificationBatch {
                end: VerificationMarker {
                    number: to_block,
                    hash: block_hash(&chain_id, to_block),
                },
                logs,
                rpc_request_count: 0,
            })
        })
    }
}

struct UnexpectedReferences;

impl VerificationReferenceProvider for UnexpectedReferences {
    fn preflight(&self, _source: &VerificationSource) -> RunnerResult<()> {
        Err(RunnerError::data_integrity(
            "Sepolia provider-trusted verification selected a reference during preflight",
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
            Err(RunnerError::data_integrity(
                "Sepolia provider-trusted verification fetched an independent reference",
            ))
        })
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
                "completed prerequisite phase {} unexpectedly ran",
                self.name
            )))
        })
    }
}

struct CountingLivePhase {
    calls: Arc<AtomicUsize>,
}

struct CountingCompletePhase {
    name: PhaseName,
    calls: Arc<AtomicUsize>,
}

impl Phase for CountingCompletePhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        })
    }
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

struct GatedQuickSyncVerifyPhase {
    gate: VerificationGate,
}

impl Phase for GatedQuickSyncVerifyPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
            let marker = context
                .available_heads
                .as_ref()
                .and_then(|heads| heads.finalized.clone())
                .ok_or_else(|| RunnerError::data_integrity("verify fixture has no finality"))?;
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker),
                verification_level: Some(VerificationLevel::QuickSynced),
                ..PhaseProgress::default()
            }))
        })
    }
}

struct SignalingLivePhase {
    entered: Arc<Notify>,
}

impl Phase for SignalingLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.entered.notify_one();
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        })
    }
}

struct StopAfterFirstVerifyBatch {
    inner: VerifyPhase,
    batches: AtomicUsize,
}

impl Phase for StopAfterFirstVerifyBatch {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn preflight(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
        mode: &phase_runner::phase::RunMode,
    ) -> RunnerResult<()> {
        self.inner.preflight(chain_id, sources, mode)
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        if self.batches.fetch_add(1, Ordering::SeqCst) > 0 {
            return Box::pin(async {
                Err(RunnerError::data_integrity(
                    "fixture stop between Verify batches",
                ))
            });
        }
        self.inner.run_batch(context)
    }
}

struct AdvancingLivePhase {
    pool: sqlx::PgPool,
    from: i64,
    through: i64,
    calls: Arc<AtomicUsize>,
}

impl Phase for AdvancingLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            for number in self.from..=self.through {
                sqlx::query(
                    "INSERT INTO chain_lineage (
                         chain_id, block_hash, parent_hash, block_number, block_timestamp
                     ) VALUES ($1, $2, $3, $4, to_timestamp($4))
                     ON CONFLICT (chain_id, block_hash) DO NOTHING",
                )
                .bind(SEPOLIA)
                .bind(block_hash(SEPOLIA, number))
                .bind(block_hash(SEPOLIA, number - 1))
                .bind(number)
                .execute(&self.pool)
                .await
                .map_err(|error| {
                    RunnerError::data_integrity(format!(
                        "live fixture failed to store lineage: {error}"
                    ))
                })?;
            }
            let marker = BlockMarker::new(self.through, block_hash(SEPOLIA, self.through))?;
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker.clone()),
                heads: Some(HeadMarkers {
                    latest: marker.clone(),
                    safe: Some(marker.clone()),
                    finalized: Some(marker),
                }),
                ..PhaseProgress::default()
            }))
        })
    }
}

struct PartialThenStopIngestPhase {
    batches: AtomicUsize,
}

impl Phase for PartialThenStopIngestPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if self.batches.fetch_add(1, Ordering::SeqCst) > 0 {
                return Err(RunnerError::data_integrity("fixture stop mid-ingest"));
            }
            let current = BlockMarker::new(4, block_hash(SEPOLIA, 4))?;
            let target = BlockMarker::new(8, block_hash(SEPOLIA, 8))?;
            Ok(PhaseBatchOutcome::Continue(PhaseProgress {
                current: Some(current.clone()),
                target: Some(target.clone()),
                source_progress: vec![SourceProgress {
                    source_key: context.sources[0].source_key.clone(),
                    current: Some(current),
                    target: Some(target),
                }],
                ..PhaseProgress::default()
            }))
        })
    }
}

struct ResumingIngestPhase {
    observed_resume: Arc<Mutex<Vec<IngestCursor>>>,
    calls: Arc<AtomicUsize>,
}

impl Phase for ResumingIngestPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self
                .observed_resume
                .lock()
                .expect("resume observation lock") = context.resume.ingest_cursors.to_vec();
            let end = BlockMarker::new(8, block_hash(SEPOLIA, 8))?;
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(end.clone()),
                target: Some(end.clone()),
                live_handoff: Some(end.clone()),
                source_progress: vec![SourceProgress {
                    source_key: context.sources[0].source_key.clone(),
                    current: Some(end.clone()),
                    target: Some(end),
                }],
                ..PhaseProgress::default()
            }))
        })
    }
}

struct FailingLivePhase;

impl Phase for FailingLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { Err(RunnerError::data_integrity("fixture live failed first")) })
    }
}

struct HeadFollowingLivePhase {
    advanced: Arc<Notify>,
}

impl Phase for HeadFollowingLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let marker = context
                .available_heads
                .as_ref()
                .map(|heads| heads.latest.clone())
                .ok_or_else(|| RunnerError::data_integrity("live fixture has no head"))?;
            self.advanced.notify_one();
            Ok(PhaseBatchOutcome::Idle(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker),
                ..PhaseProgress::default()
            }))
        })
    }
}

struct ClaimingVerifyPhase {
    level: VerificationLevel,
}

impl Phase for ClaimingVerifyPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let marker = context
                .available_heads
                .as_ref()
                .and_then(|heads| heads.finalized.clone())
                .ok_or_else(|| RunnerError::data_integrity("verify fixture has no finality"))?;
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker),
                verification_level: Some(self.level),
                ..PhaseProgress::default()
            }))
        })
    }
}

async fn verifier_runner(
    scratch: &ScratchDatabase,
    reference: Arc<dyn VerificationReferenceProvider>,
    live: Arc<dyn Phase>,
) -> Result<PhaseRunner> {
    let database = scratch.runner();
    let verification_database = scratch.verification_database(2).await?;
    let phases = PhaseSet::new([
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            verification_database,
            reference,
        )),
        live,
    ])?;
    Ok(PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verifier",
        test_timing(),
    )?)
}

async fn sepolia_verifier_runner(
    scratch: &ScratchDatabase,
    live_calls: Arc<AtomicUsize>,
) -> Result<PhaseRunner> {
    sepolia_verifier_runner_with_live(scratch, Arc::new(CountingLivePhase { calls: live_calls }))
        .await
}

async fn sepolia_verifier_runner_with_live(
    scratch: &ScratchDatabase,
    live: Arc<dyn Phase>,
) -> Result<PhaseRunner> {
    let phases = PhaseSet::new([
        Arc::new(UnexpectedPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(VerifyPhase::with_reference_provider(
            scratch.verification_database(2).await?,
            Arc::new(UnexpectedReferences),
        )),
        live,
    ])?;
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verifier-sepolia-restart",
        test_timing(),
    )?)
}

async fn run_first_verify_batch(scratch: &ScratchDatabase) -> Result<()> {
    let live_calls = Arc::new(AtomicUsize::new(0));
    let phases = PhaseSet::new([
        Arc::new(UnexpectedPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(StopAfterFirstVerifyBatch {
            inner: VerifyPhase::with_reference_provider(
                scratch.verification_database(2).await?,
                Arc::new(UnexpectedReferences),
            ),
            batches: AtomicUsize::new(0),
        }),
        Arc::new(CountingLivePhase {
            calls: Arc::clone(&live_calls),
        }),
    ])?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-verifier-first-multi-batch",
        test_timing(),
    )?;
    let error = runner
        .run_chain(
            &sepolia_chain_with_key("drpc-intake")?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the fixture must stop between Verify batches");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(2),
    }
}

fn base_chain(verify_before_live: bool) -> RunnerResult<ChainConfig> {
    base_chain_with_drpc_start(verify_before_live, BASE_COINBASE_SEAM_BLOCK)
}

fn base_chain_with_drpc_start(
    verify_before_live: bool,
    drpc_start: i64,
) -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        BASE,
        vec![
            SourceConfig::new(
                BASE,
                "coinbase-history",
                "coinbase_sql",
                SeedBasis::BaseSeam,
                0,
                "https://coinbase.invalid",
            )?,
            SourceConfig::new(
                BASE,
                "drpc-reference",
                "drpc",
                SeedBasis::BaseSeam,
                drpc_start,
                "https://drpc.invalid",
            )?,
        ],
        verify_before_live,
    )
}

fn base_chain_with_endpoints() -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        BASE,
        vec![
            SourceConfig::new(
                BASE,
                "coinbase-history",
                "coinbase_sql",
                SeedBasis::BaseSeam,
                0,
                "https://rotated-coinbase.invalid",
            )?,
            SourceConfig::new(
                BASE,
                "drpc-reference",
                "drpc",
                SeedBasis::BaseSeam,
                BASE_COINBASE_SEAM_BLOCK,
                "https://rotated-drpc.invalid",
            )?,
        ],
        true,
    )
}

fn base_chain_with_reth_reference() -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        BASE,
        vec![
            SourceConfig::new(
                BASE,
                "coinbase-history",
                "coinbase_sql",
                SeedBasis::BaseSeam,
                0,
                "https://coinbase.invalid",
            )?,
            SourceConfig::new(
                BASE,
                "reth-reference",
                "reth_db",
                SeedBasis::BaseSeam,
                0,
                "/fixture/reth",
            )?,
        ],
        true,
    )
}

fn ethereum_chain() -> RunnerResult<ChainConfig> {
    ethereum_chain_with_start(0)
}

fn ethereum_chain_with_start(start: i64) -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        ETHEREUM,
        vec![SourceConfig::new(
            ETHEREUM,
            "reth-reference",
            "reth_db",
            SeedBasis::EthereumHead,
            start,
            "/fixture/reth",
        )?],
        false,
    )
}

fn sepolia_chain() -> RunnerResult<ChainConfig> {
    sepolia_chain_with_key("drpc-intake")
}

fn sepolia_chain_with_key(source_key: &str) -> RunnerResult<ChainConfig> {
    sepolia_chain_with_kind(source_key, "drpc")
}

fn sepolia_chain_with_kind(source_key: &str, source_kind: &str) -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        SEPOLIA,
        vec![SourceConfig::new(
            SEPOLIA,
            source_key,
            source_kind,
            SeedBasis::EthereumHead,
            0,
            "https://drpc.invalid",
        )?],
        true,
    )
}

async fn seed_ingest_cursor(
    pool: &sqlx::PgPool,
    chain_id: &str,
    source_key: &str,
    through: i64,
) -> Result<()> {
    seed_ingest_cursor_with_kind(pool, chain_id, source_key, "drpc", through).await
}

async fn seed_ingest_cursor_with_kind(
    pool: &sqlx::PgPool,
    chain_id: &str,
    source_key: &str,
    source_kind: &str,
    through: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number, target_block_number,
             last_processed_block_number, last_processed_block_hash
         ) VALUES ($1, $2, $3, 'ethereum_head', 0, $4 + 1, $4, $4, $5)",
    )
    .bind(chain_id)
    .bind(source_key)
    .bind(source_kind)
    .bind(through)
    .bind(block_hash(chain_id, through))
    .execute(pool)
    .await?;
    Ok(())
}

type IngestCursorRow = (
    String,
    String,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

async fn ingest_cursor_row(pool: &sqlx::PgPool, source_key: &str) -> Result<IngestCursorRow> {
    Ok(sqlx::query_as(
        "SELECT source_kind, seed_basis, start_block_number, next_block_number,
                target_block_number, last_processed_block_number, last_processed_block_hash
         FROM ingest_cursors WHERE chain_id = $1 AND source_key = $2",
    )
    .bind(SEPOLIA)
    .bind(source_key)
    .fetch_one(pool)
    .await?)
}

async fn verify_state(pool: &sqlx::PgPool) -> Result<(String, String, i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number, target_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(pool)
    .await?)
}

async fn verify_state_optional(
    pool: &sqlx::PgPool,
) -> Result<Option<(String, Option<String>, Option<i64>, Option<i64>)>> {
    Ok(sqlx::query_as(
        "SELECT phase_status, verification_level, current_block_number, target_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_optional(pool)
    .await?)
}

async fn ingest_phase_state(
    pool: &sqlx::PgPool,
) -> Result<(
    String,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
)> {
    Ok(sqlx::query_as(
        "SELECT phase_status, current_block_number, current_block_hash,
                target_block_number, target_block_hash, last_error
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(SEPOLIA)
    .fetch_one(pool)
    .await?)
}

type VerifyMarkerState = (
    String,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
);

async fn verify_marker_state(pool: &sqlx::PgPool) -> Result<VerifyMarkerState> {
    Ok(sqlx::query_as(
        "SELECT phase_status, current_block_number, current_block_hash,
                target_block_number, target_block_hash
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(SEPOLIA)
    .fetch_one(pool)
    .await?)
}

async fn seed_sparse_verify_boundaries(pool: &sqlx::PgPool) -> Result<()> {
    for number in [FIRST_VERIFY_BATCH_END, MULTI_BATCH_VERIFY_TARGET] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'finalized')",
        )
        .bind(SEPOLIA)
        .bind(block_hash(SEPOLIA, number))
        .bind(block_hash(SEPOLIA, number - 1))
        .bind(number)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (
             chain_id, latest_block_hash, latest_block_number,
             safe_block_hash, safe_block_number,
             finalized_block_hash, finalized_block_number
         ) VALUES ($1, $2, $3, $2, $3, $2, $3)",
    )
    .bind(SEPOLIA)
    .bind(block_hash(SEPOLIA, MULTI_BATCH_VERIFY_TARGET))
    .bind(MULTI_BATCH_VERIFY_TARGET)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_completed_spine_prerequisites(
    pool: &sqlx::PgPool,
    chain_id: &str,
    through: i64,
) -> Result<()> {
    let store = PhaseStore::new(pool.clone());
    store.initialize_chain(chain_id).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2,
             current_block_hash = $3,
             target_block_number = $2,
             target_block_hash = $3,
             input_content_hash = $4,
             live_handoff_block_number = CASE
                 WHEN phase_name = 'ingest' THEN $2 ELSE NULL
             END,
             live_handoff_block_hash = CASE
                 WHEN phase_name = 'ingest' THEN $3 ELSE NULL
             END,
             started_at = now(),
             finished_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(chain_id)
    .bind(through)
    .bind(block_hash(chain_id, through))
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_chain(
    pool: &sqlx::PgPool,
    chain_id: &str,
    latest: i64,
    safe: i64,
    finalized: i64,
    log_data: u8,
) -> Result<()> {
    seed_ingest_identities(pool, chain_id).await?;
    seed_lineage_and_heads(pool, chain_id, latest, safe, finalized).await?;
    seed_watch_manifest(pool, chain_id).await?;
    insert_log(pool, chain_id, log_data).await
}

async fn seed_ingest_identities(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    let sources = match chain_id {
        BASE => vec![
            ("coinbase-history", "coinbase_sql", "base_seam", 0),
            (
                "drpc-reference",
                "drpc",
                "base_seam",
                BASE_COINBASE_SEAM_BLOCK,
            ),
        ],
        ETHEREUM => vec![("reth-reference", "reth_db", "ethereum_head", 0)],
        _ => Vec::new(),
    };
    for (source_key, source_kind, seed_basis, start) in sources {
        sqlx::query(
            "INSERT INTO ingest_cursors (
                 chain_id, source_key, source_kind, seed_basis,
                 start_block_number, next_block_number
             ) VALUES ($1, $2, $3, $4, $5, $5)",
        )
        .bind(chain_id)
        .bind(source_key)
        .bind(source_kind)
        .bind(seed_basis)
        .bind(start)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_lineage_and_heads(
    pool: &sqlx::PgPool,
    chain_id: &str,
    latest: i64,
    safe: i64,
    finalized: i64,
) -> Result<()> {
    for number in 0..=latest {
        let state = if number <= finalized {
            "finalized"
        } else if number <= safe {
            "safe"
        } else {
            "canonical"
        };
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             )
             VALUES ($1, $2, $3, $4, to_timestamp($4), $5::canonicality_state)",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, number))
        .bind((number > 0).then(|| block_hash(chain_id, number - 1)))
        .bind(number)
        .bind(state)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (
             chain_id, latest_block_hash, latest_block_number,
             safe_block_hash, safe_block_number,
             finalized_block_hash, finalized_block_number
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, latest))
    .bind(latest)
    .bind(block_hash(chain_id, safe))
    .bind(safe)
    .bind(block_hash(chain_id, finalized))
    .bind(finalized)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_watch_manifest(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    let contract_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind, provenance
         )
         VALUES ($1, $2, 'contract', '{}'::jsonb)",
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
        "abi": { "events": [{
            "name": "Transfer",
            "fragment": "event Transfer(address indexed from,address indexed to,uint256 value)",
            "emitter_roles": [],
            "normalized_events": []
        }]}
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         )
         VALUES (1, 'test', 'test_events', $1, 'test', 'active', 'test', $2, $3)
         RETURNING manifest_id",
    )
    .bind(chain_id)
    .bind(format!("tests/verify-{chain_id}.toml"))
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         )
         VALUES ($1, $2, 'contract', 'test', $3, $4, 'test', 'none', 0)",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address,
             active_from_block_number, source_manifest_id, provenance
         )
         VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(contract_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_log(pool: &sqlx::PgPool, chain_id: &str, data: u8) -> Result<()> {
    let transaction_hash = transaction_hash(chain_id);
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address
         )
         VALUES ($1, $2, 2, $3, 0, $4)",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 2))
    .bind(&transaction_hash)
    .bind("0x0000000000000000000000000000000000000001")
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data
         )
         VALUES ($1, $2, 2, $3, 0, 0, $4, $5, $6)",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 2))
    .bind(transaction_hash)
    .bind(CONTRACT)
    .bind(vec![transfer_topic0()])
    .bind(vec![data])
    .execute(pool)
    .await?;
    Ok(())
}

async fn wipe_and_resync_log(pool: &sqlx::PgPool, chain_id: &str, data: u8) -> Result<()> {
    sqlx::query("DELETE FROM raw_logs WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM raw_transactions WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    insert_log(pool, chain_id, data).await
}

fn reference_log(chain_id: &str, data: u8) -> VerificationLog {
    VerificationLog {
        block_hash: block_hash(chain_id, 2),
        block_number: 2,
        transaction_hash: transaction_hash(chain_id),
        transaction_index: 0,
        log_index: 0,
        address: CONTRACT.to_owned(),
        topics: vec![transfer_topic0()],
        data: vec![data],
    }
}

fn block_hash(chain_id: &str, number: i64) -> String {
    format!("{chain_id}-block-{number}")
}

fn transaction_hash(chain_id: &str) -> String {
    format!("{chain_id}-transaction-2")
}

fn chain_from_hash(hash: &str) -> &str {
    hash.strip_suffix("-block-2")
        .expect("fixture block hash must end in its block number")
}

fn transfer_topic0() -> String {
    format!("{:#x}", keccak256("Transfer(address,address,uint256)"))
}

#[derive(Clone)]
struct VerificationRpcState {
    requests: Arc<AtomicUsize>,
    transient_failures: Arc<AtomicUsize>,
}

async fn spawn_verification_rpc(
    transient_failures: usize,
) -> Result<(String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let requests = Arc::new(AtomicUsize::new(0));
    let state = VerificationRpcState {
        requests: Arc::clone(&requests),
        transient_failures: Arc::new(AtomicUsize::new(transient_failures)),
    };
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(verification_rpc))
                .with_state(state),
        )
        .await
        .expect("verification query-count RPC server");
    });
    Ok((format!("http://{address}/"), requests, server))
}

async fn verification_rpc(
    State(state): State<VerificationRpcState>,
    Json(request): Json<Value>,
) -> Response {
    state.requests.fetch_add(1, Ordering::SeqCst);
    if state
        .transient_failures
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "retry fixture").into_response();
    }
    if let Some(batch) = request.as_array() {
        return Json(Value::Array(
            batch.iter().map(verification_rpc_response).collect(),
        ))
        .into_response();
    }
    Json(verification_rpc_response(&request)).into_response()
}

fn verification_rpc_response(request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let result = match request["method"].as_str().unwrap_or_default() {
        "eth_getBlockByNumber" => json!({
            "hash": format!("0x{:064x}", 6),
            "parentHash": format!("0x{:064x}", 5),
            "number": "0x5",
            "timestamp": "0x5"
        }),
        "eth_getLogs" => json!([]),
        method => panic!("unexpected verification RPC method {method}"),
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

async fn wait_for_phase_position(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    expected_status: &str,
    expected_current: Option<i64>,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state: Option<(String, Option<i64>)> = sqlx::query_as(
                "SELECT phase_status, current_block_number
                 FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name = $2",
            )
            .bind(chain_id)
            .bind(phase.as_str())
            .fetch_optional(pool)
            .await?;
            if state
                .as_ref()
                .is_some_and(|state| state.0 == expected_status && state.1 == expected_current)
            {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}
