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
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName,
        PhaseProgress, PhaseSet, VerificationLevel,
    },
    runner::{PhaseRunner, RedoPhase},
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
const CONTRACT: &str = "0x00000000000000000000000000000000000000aa";

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
             verification_level = 'cross_checked',
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
        .run_chain(&base_reth_chain()?, CancellationToken::new())
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
        ("completed".to_owned(), "cross_checked".to_owned(), 5)
    );
    assert_eq!(
        reference.calls(),
        vec![ReferenceCall {
            chain_id: BASE.to_owned(),
            provider_kind: VerificationProviderKind::LocalReth,
            level: VerificationLevel::NodeChecked,
            from: 3,
            to: 5,
        }]
    );

    drop(runner);
    scratch.cleanup().await
}

#[tokio::test]
async fn normal_verification_uses_the_durable_ingest_start_not_restart_configuration() -> Result<()>
{
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
        .expect_err("verification must include the durable ingest extent before the moved start");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    assert_eq!(reference.calls()[0].from, 0);

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
    seed_lineage_and_heads(scratch.pool(), BASE, 5, 5, 5).await?;
    let phases = PhaseSet::new([
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)) as Arc<dyn Phase>,
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(NodeClaimingVerifyPhase),
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
            .contains("dRPC is capped at cross_checked")
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

struct CompleteLivePhase;

impl Phase for CompleteLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { Ok(PhaseBatchOutcome::Complete(PhaseProgress::default())) })
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

struct NodeClaimingVerifyPhase;

impl Phase for NodeClaimingVerifyPhase {
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
                verification_level: Some(VerificationLevel::NodeChecked),
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

fn base_reth_chain() -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        BASE,
        vec![SourceConfig::new(
            BASE,
            "reth-reference",
            "reth_db",
            SeedBasis::BaseSeam,
            0,
            "/fixture/base-reth",
        )?],
        true,
    )
}

async fn seed_chain(
    pool: &sqlx::PgPool,
    chain_id: &str,
    latest: i64,
    safe: i64,
    finalized: i64,
    log_data: u8,
) -> Result<()> {
    seed_lineage_and_heads(pool, chain_id, latest, safe, finalized).await?;
    seed_watch_manifest(pool, chain_id).await?;
    insert_log(pool, chain_id, log_data).await
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
